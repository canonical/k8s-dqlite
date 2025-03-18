package sqllog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/limited"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	otelName         = "sqllog"
	SupersededCount  = 100
	compactBatchSize = 1000
	pollBatchSize    = 500
)

var (
	otelTracer trace.Tracer
	otelMeter  metric.Meter
	compactCnt metric.Int64Counter
)

func init() {
	var err error
	otelTracer = otel.Tracer(otelName)
	otelMeter = otel.Meter(otelName)

	compactCnt, err = otelMeter.Int64Counter(fmt.Sprintf("%s.compact", otelName), metric.WithDescription("Number of compact requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
}

type Driver interface {
	List(ctx context.Context, key, rangeEnd []byte, limit, revision int64) (*sql.Rows, error)
	ListTTL(ctx context.Context, revision int64) (*sql.Rows, error)
	Count(ctx context.Context, key, rangeEnd []byte, revision int64) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	AfterPrefix(ctx context.Context, key, rangeEnd []byte, fromRevision, toRevision int64) (*sql.Rows, error)
	After(ctx context.Context, startRevision, limit int64) (*sql.Rows, error)
	Create(ctx context.Context, key []byte, value []byte, lease int64) (int64, bool, error)
	Update(ctx context.Context, key []byte, value []byte, prevRev, lease int64) (int64, bool, error)
	Delete(ctx context.Context, key []byte, revision int64) (int64, bool, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, int64, error)
	Compact(ctx context.Context, revision int64) error
	Fill(ctx context.Context, revision int64) error
	IsFill(key []byte) bool
	GetSize(ctx context.Context) (int64, error)
	Close() error
}

type SQLLog struct {
	mu sync.Mutex

	config *SQLLogConfig

	stop    func()
	started bool

	watcherGroups map[*watcherGroup]*watcherGroup
	pollRevision  int64

	notify chan int64
	wg     sync.WaitGroup
}

type SQLLogConfig struct {
	// Driver is the SQL driver to use to query the database.
	Driver Driver

	// CompactInterval is interval between database compactions performed by k8s-dqlite.
	CompactInterval time.Duration

	// PollInterval is the event poll interval used by k8s-dqlite.
	PollInterval time.Duration

	// WatchQueryTimeout is the timeout on the after query in the poll loop.
	WatchQueryTimeout time.Duration
}

func New(config *SQLLogConfig) *SQLLog {
	return &SQLLog{
		config:        config,
		notify:        make(chan int64, 100),
		watcherGroups: make(map[*watcherGroup]*watcherGroup),
	}
}

func (s *SQLLog) Start(startCtx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	_, _, err := s.Create(startCtx, []byte("/registry/health"), []byte(`{"health":"true"}`), 0)
	if err != nil {
		return err
	}

	ctx, stop := context.WithCancel(context.Background())
	if err := s.startWatch(ctx); err != nil {
		stop()
		return err
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.ttl(ctx)
	}()

	s.stop = stop
	s.started = true
	return nil
}

func (s *SQLLog) Stop() error {
	s.mu.Lock()

	if !s.started {
		return nil
	}
	s.stop()
	s.stop, s.started = nil, false
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}

func (s *SQLLog) Close() error {
	stopErr := s.Stop()
	closeErr := s.config.Driver.Close()

	return errors.Join(stopErr, closeErr)
}

func (s *SQLLog) compactStart(ctx context.Context) error {
	currentRevision, err := s.config.Driver.CurrentRevision(ctx)

	rows, err := s.config.Driver.AfterPrefix(ctx, []byte("compact_rev_key"), []byte("compact_rev_key\x00"), 0, currentRevision)
	if err != nil {
		return err
	}

	events, err := ScanAll(rows, scanEvent)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		_, _, err := s.Create(ctx, []byte("compact_rev_key"), nil, 0)
		return err
	} else if len(events) == 1 {
		return nil
	}

	// this is to work around a bug in which we ended up with two compact_rev_key rows
	maxRev := int64(0)
	maxID := int64(0)
	for _, event := range events {
		if event.PrevKv != nil && event.PrevKv.ModRevision > maxRev {
			maxRev = event.PrevKv.ModRevision
			maxID = event.Kv.ModRevision
		}
	}

	for _, event := range events {
		if event.Kv.ModRevision == maxID {
			continue
		}
		if err := s.config.Driver.DeleteRevision(ctx, event.Kv.ModRevision); err != nil {
			return err
		}
	}

	return nil
}

// DoCompact makes a single compaction run when called. It is intended to be called
// from test functions that have access to the backend.
func (s *SQLLog) DoCompact(ctx context.Context) (err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.DoCompact", otelName))
	compactCnt.Add(ctx, 1)
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	if err := s.compactStart(ctx); err != nil {
		return fmt.Errorf("failed to initialise compaction: %v", err)
	}

	// When executing compaction as a background operation
	// it's best not to take too much time away from query
	// operation and similar. As such, we do compaction in
	// small batches. Given that this logic runs every second,
	// on regime it should take usually just a couple batches
	// to keep the pace.
	start, target, err := s.config.Driver.GetCompactRevision(ctx)
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Int64("start", start))
	// NOTE: Upstream is ignoring the last 1000 revisions, however that causes the following CNCF conformance test to fail.
	// This is because of low activity, where the created list is part of the last 1000 revisions and is not compacted.
	// Link to failing test: https://github.com/kubernetes/kubernetes/blob/f2cfbf44b1fb482671aedbfff820ae2af256a389/test/e2e/apimachinery/chunking.go#L144
	// To address this, we only ignore the last 100 revisions instead
	target -= SupersededCount
	span.SetAttributes(attribute.Int64("target", target))
	for start < target {
		batchRevision := start + compactBatchSize
		if batchRevision > target {
			batchRevision = target
		}
		if err := s.config.Driver.Compact(ctx, batchRevision); err != nil {
			return err
		}
		start = batchRevision
	}
	return nil
}

func (s *SQLLog) WatcherGroup(ctx context.Context) (limited.WatcherGroup, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	wg := &watcherGroup{
		ctx:             ctx,
		driver:          s.config.Driver,
		currentRevision: s.pollRevision,
		watchers:        make(map[int64]*watcher),
		updates:         make(chan limited.WatcherGroupUpdate, 100),
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.watcherGroups, wg)
		close(wg.updates)
	})
	wg.stop = stop
	s.watcherGroups[wg] = wg
	logrus.Debugf("created new WatcherGroup.")
	return wg, nil
}

func (s *SQLLog) List(ctx context.Context, key, rangeEnd []byte, limit, revision int64) (int64, []*limited.KeyValue, error) {
	var err error

	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.List", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("rangeEnd", string(rangeEnd)),
		attribute.Int64("limit", limit),
		attribute.Int64("revision", revision),
	)

	compactRevision, currentRevision, err := s.config.Driver.GetCompactRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, nil, limited.ErrCompacted
	}

	rows, err := s.config.Driver.List(ctx, key, rangeEnd, limit, revision)
	if err != nil {
		return 0, nil, err
	}

	result, err := ScanAll(rows, scanKeyValue)
	if err != nil {
		return 0, nil, err
	}

	return currentRevision, result, err
}

func (s *SQLLog) ttl(ctx context.Context) {
	run := func(ctx context.Context, key []byte, revision int64, timeout time.Duration) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
			s.Delete(ctx, key, revision)
		}
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		startRevision, err := s.config.Driver.CurrentRevision(ctx)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		rows, err := s.config.Driver.ListTTL(ctx, startRevision)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		var (
			key             []byte
			revision, lease int64
		)
		for rows.Next() {
			if err := rows.Scan(&revision, &key, &lease); err != nil {
				logrus.Errorf("failed to read old events for ttl: %v", err)
				return
			}
			go run(ctx, key, revision, time.Duration(lease)*time.Second)
		}

		// TODO needs to be restarted, never cancelled/dropped
		group, err := s.WatcherGroup(ctx)
		if err != nil {
			logrus.Errorf("failed to create watch groupw for ttl: %v", err)
			return
		}
		group.Watch(1, []byte{0}, []byte{255}, startRevision)

		for group := range group.Updates() {
			for _, watcher := range group.Watchers() {
				for _, event := range watcher.Events {
					if event.Kv.Lease > 0 {
						go run(ctx, []byte(event.Kv.Key), event.Kv.ModRevision, time.Duration(event.Kv.Lease)*time.Second)
					}
				}
			}
		}
	}()
}

func (s *SQLLog) startWatch(ctx context.Context) error {
	if err := s.compactStart(ctx); err != nil {
		return err
	}

	polllInitialRevision, _, err := s.config.Driver.GetCompactRevision(ctx)
	if err != nil {
		return err
	}
	s.pollRevision = polllInitialRevision

	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	s.wg.Add(2)

	go func() {
		defer s.wg.Done()

		t := time.NewTicker(s.config.CompactInterval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := s.DoCompact(ctx); err != nil {
					logrus.WithError(err).Trace("compaction failed")
				}
			}
		}
	}()

	go func() {
		defer s.wg.Done()
		s.poll(ctx)
	}()

	return nil
}

func (s *SQLLog) poll(ctx context.Context) {
	var (
		skip        int64
		skipTime    time.Time
		waitForMore = true
	)

	wait := time.NewTicker(s.config.PollInterval)
	defer wait.Stop()

	for {
		if waitForMore {
			select {
			case <-ctx.Done():
				return
			case check := <-s.notify:
				if check <= s.pollRevision {
					continue
				}
			case <-wait.C:
			}
		}
		waitForMore = true
		events, err := s.getLatestEvents(ctx, s.pollRevision)
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, context.Canceled) {
				logrus.Errorf("fail to get latest events: %v", err)
			}
			continue
		}

		waitForMore = len(events) < 100

		rev := s.pollRevision
		var (
			sequential []*limited.Event
		)

		for _, event := range events {
			next := rev + 1
			// Ensure that we are notifying events in a sequential fashion. For example if we find row 4 before 3
			// we don't want to notify row 4 because 3 is essentially dropped forever.
			if event.Kv.ModRevision != next {
				logrus.Tracef("MODREVISION GAP: expected %v, got %v", next, event.Kv.ModRevision)
				if canSkipRevision(next, skip, skipTime) {
					// This situation should never happen, but we have it here as a fallback just for unknown reasons
					// we don't want to pause all watches forever
					logrus.Errorf("GAP %s, revision=%d, delete=%v, next=%d", event.Kv.Key, event.Kv.ModRevision, event.Type == mvccpb.DELETE, next)
				} else if skip != next {
					// This is the first time we have encountered this missing revision, so record time start
					// and trigger a quick retry for simple out of order events
					skip = next
					skipTime = time.Now()
					s.notifyWatcherPoll(next)
					break
				} else {
					if err := s.config.Driver.Fill(ctx, next); err == nil {
						logrus.Debugf("FILL, revision=%d, err=%v", next, err)
						s.notifyWatcherPoll(next)
					} else {
						logrus.Debugf("FILL FAILED, revision=%d, err=%v", next, err)
					}
					break
				}
			}

			// we have done something now that we should save the last revision.  We don't save here now because
			// the next loop could fail leading to saving the reported revision without reporting it.  In practice this
			// loop right now has no error exit so the next loop shouldn't fail, but if we for some reason add a method
			// that returns error, that would be a tricky bug to find.  So instead we only save the last revision at
			// the same time we write to the channel.
			rev = event.Kv.ModRevision
			if s.config.Driver.IsFill([]byte(event.Kv.Key)) {
				logrus.Debugf("NOT TRIGGER FILL %s, revision=%d, delete=%v", event.Kv.Key, event.Kv.ModRevision, event.Type == mvccpb.DELETE)
			} else {
				sequential = append(sequential, event)
				logrus.Debugf("TRIGGERED %s, revision=%d, delete=%v", event.Kv.Key, event.Kv.ModRevision, event.Type == mvccpb.DELETE)
			}
		}

		s.publishEvents(rev, sequential)
	}
}

func (s *SQLLog) publishEvents(currentRevision int64, events []*limited.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pollRevision = currentRevision

	for _, w := range s.watcherGroups {
		if !w.publish(currentRevision, events) {
			if w.stop() {
				delete(s.watcherGroups, w)
				close(w.updates)
			}
		}
	}
}

func (s *SQLLog) getLatestEvents(ctx context.Context, last int64) ([]*limited.Event, error) {
	watchCtx, cancel := context.WithTimeout(ctx, s.config.WatchQueryTimeout)
	defer cancel()

	rows, err := s.config.Driver.After(watchCtx, last, pollBatchSize)
	if err != nil {
		return nil, err
	}

	events, err := ScanAll(rows, scanEvent)
	if err != nil {
		return nil, err
	}
	return events, nil
}

func canSkipRevision(rev, skip int64, skipTime time.Time) bool {
	return rev == skip && time.Since(skipTime) > time.Second
}

func (s *SQLLog) Count(ctx context.Context, key, rangeEnd []byte, revision int64) (int64, int64, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Count", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("rangeEnd", string(rangeEnd)),
		attribute.Int64("revision", revision),
	)

	compactRevision, currentRevision, err := s.config.Driver.GetCompactRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, 0, limited.ErrCompacted
	}
	count, err := s.config.Driver.Count(ctx, key, rangeEnd, revision)
	if err != nil {
		return 0, 0, err
	}
	return currentRevision, count, nil
}

func (s *SQLLog) Create(ctx context.Context, key, value []byte, lease int64) (int64, bool, error) {
	rev, created, err := s.config.Driver.Create(ctx, key, value, lease)
	if err != nil {
		return 0, false, err
	}
	if created {
		s.notifyWatcherPoll(rev)
	}
	return rev, created, nil
}

func (s *SQLLog) Delete(ctx context.Context, key []byte, revision int64) (rev int64, deleted bool, err error) {
	rev, deleted, err = s.config.Driver.Delete(ctx, key, revision)
	if err != nil {
		return 0, false, err
	}
	if deleted {
		s.notifyWatcherPoll(rev)
	}
	return rev, deleted, nil
}

func (s *SQLLog) Update(ctx context.Context, key []byte, value []byte, prevRev, lease int64) (rev int64, updated bool, err error) {
	rev, updated, err = s.config.Driver.Update(ctx, key, value, prevRev, lease)
	if err != nil {
		return 0, false, err
	}
	if updated {
		s.notifyWatcherPoll(rev)
	}
	return rev, updated, nil
}

func (s *SQLLog) notifyWatcherPoll(revision int64) {
	select {
	case s.notify <- revision:
	default:
	}
}

func (s *SQLLog) DbSize(ctx context.Context) (int64, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.DbSize", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	size, err := s.config.Driver.GetSize(ctx)
	span.SetAttributes(attribute.Int64("size", size))
	return size, err
}

func ScanAll[T any](rows *sql.Rows, scanOne func(*sql.Rows) (T, error)) ([]T, error) {
	var result []T
	defer rows.Close()

	for rows.Next() {
		item, err := scanOne(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, item)
	}

	return result, nil
}

func scanKeyValue(rows *sql.Rows) (*limited.KeyValue, error) {
	kv := &limited.KeyValue{}
	err := rows.Scan(
		&kv.ModRevision,
		&kv.Key,
		&kv.CreateRevision,
		&kv.Lease,
		&kv.Value,
	)
	if err != nil {
		return nil, err
	}
	return kv, nil
}

func scanEvent(rows *sql.Rows) (*limited.Event, error) {
	// Bundle in a single allocation
	row := &struct {
		event            limited.Event
		kv, prevKv       limited.KeyValue
		created, deleted bool
	}{}
	event := &row.event

	err := rows.Scan(
		&row.kv.ModRevision,
		&row.kv.Key,
		&row.created,
		&row.deleted,
		&row.kv.CreateRevision,
		&row.prevKv.ModRevision,
		&row.kv.Lease,
		&row.kv.Value,
		&row.prevKv.Value,
	)
	if err != nil {
		return nil, err
	}

	event.Kv = &row.kv
	if row.deleted {
		row.event.Type = mvccpb.DELETE
	}
	if row.created {
		event.Kv.CreateRevision = event.Kv.ModRevision
	} else {
		event.PrevKv = &row.prevKv
		event.PrevKv.Key = event.Kv.Key
		event.PrevKv.CreateRevision = event.Kv.CreateRevision
		event.PrevKv.Lease = event.Kv.Lease
	}
	return event, nil
}

type watcherGroup struct {
	mu              sync.Mutex
	ctx             context.Context
	driver          Driver
	currentRevision int64
	watchers        map[int64]*watcher
	updates         chan limited.WatcherGroupUpdate
	stop            func() bool
}

type watcher struct {
	watchId       int64
	key, rangeEnd []byte
	initialized   bool
	events        []*limited.Event
}

type watcherGroupUpdate struct {
	revision int64
	updates  []limited.WatcherUpdate
}

func (w *watcherGroupUpdate) Revision() int64                   { return w.revision }
func (w *watcherGroupUpdate) Watchers() []limited.WatcherUpdate { return w.updates }

func (w *watcherGroup) publish(currentRevision int64, events []*limited.Event) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.currentRevision = currentRevision
	updates := []limited.WatcherUpdate{}
	for watchId, watcher := range w.watchers {
		events := watcher.update(events)
		if len(events) > 0 {
			updates = append(updates, limited.WatcherUpdate{
				WatcherId: watchId,
				Events:    events,
			})
		}
	}

	groupUpdate := &watcherGroupUpdate{
		revision: currentRevision,
		updates:  updates,
	}
	select {
	case w.updates <- groupUpdate:
		return true
	default:
		return false
	}
}

func (w *watcher) update(events []*limited.Event) []*limited.Event {
	if !w.initialized {
		w.events = appendEvents(events, w.events, w.key, w.rangeEnd)
		return nil
	} else if len(w.events) > 0 {
		initialEvents := w.events
		w.events = nil
		return appendEvents(events, initialEvents, w.key, w.rangeEnd)
	}

	filtered := make([]*limited.Event, 0, len(events))
	return appendEvents(events, filtered, w.key, w.rangeEnd)
}

func appendEvents(src, dst []*limited.Event, key, rangeEnd []byte) []*limited.Event {
	for _, event := range src {
		if bytes.Compare(key, event.Kv.Key) <= 0 && bytes.Compare(rangeEnd, event.Kv.Key) > 0 {
			dst = append(dst, event)
		}
	}
	return dst
}

func (w *watcherGroup) Watch(watchId int64, key, rangeEnd []byte, startRevision int64) error {
	ctx, span := otelTracer.Start(w.ctx, fmt.Sprintf("%s.Watch", otelName))
	defer span.End()

	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("rangeEnd", string(rangeEnd)),
		attribute.Int64("startRevision", startRevision),
	)

	watcher := &watcher{
		watchId:  watchId,
		key:      key,
		rangeEnd: rangeEnd,
	}

	w.mu.Lock()
	w.watchers[watchId] = watcher
	if startRevision > 0 {
		currentRevision := w.currentRevision
		w.mu.Unlock()

		// TODO: check that this has a time limit
		initialEvents, err := w.initialEvents(ctx, key, rangeEnd, startRevision-1, currentRevision)
		if err != nil {
			w.mu.Lock()
			delete(w.watchers, watchId)
			w.mu.Unlock()
			if !errors.Is(err, context.Canceled) {
				span.RecordError(err)
				logrus.Errorf("Failed to list %s for revision %d: %v", key, startRevision, err)
				// We return an error message that the api-server understands: limited.ErrGRPCUnhealthy
				if err != limited.ErrCompacted {
					err = limited.ErrGRPCUnhealthy
				}
			}
			return err
		}

		w.mu.Lock()
		watcher.events = append(initialEvents, watcher.events...)
	}
	watcher.initialized = true
	w.mu.Unlock()

	return nil
}

func (w *watcherGroup) initialEvents(ctx context.Context, key, rangeEnd []byte, fromRevision, toRevision int64) ([]*limited.Event, error) {
	rows, err := w.driver.AfterPrefix(ctx, key, rangeEnd, fromRevision, toRevision)
	if err != nil {
		return nil, err
	}
	return ScanAll(rows, scanEvent)
}

func (w *watcherGroup) Unwatch(watchId int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.watchers, watchId)
}

func (w *watcherGroup) Updates() <-chan limited.WatcherGroupUpdate { return w.updates }

var _ limited.WatcherGroup = &watcherGroup{}
