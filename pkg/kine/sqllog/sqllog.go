package sqllog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/broadcaster"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/sirupsen/logrus"
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

type Dialect interface {
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (*sql.Rows, error)
	ListTTL(ctx context.Context, revision int64) (*sql.Rows, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	AfterPrefix(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error)
	After(ctx context.Context, rev, limit int64) (*sql.Rows, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, bool, error)
	Update(ctx context.Context, key string, value []byte, prevRev, lease int64) (int64, bool, error)
	Delete(ctx context.Context, key string, revision int64) (int64, bool, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, int64, error)
	Compact(ctx context.Context, revision int64) error
	Fill(ctx context.Context, revision int64) error
	IsFill(key string) bool
	GetSize(ctx context.Context) (int64, error)
	GetCompactInterval() time.Duration
	GetWatchQueryTimeout() time.Duration
	GetPollInterval() time.Duration
	Close() error
}

type SQLLog struct {
	mu      sync.Mutex
	stop    func()
	started bool

	d           Dialect
	broadcaster broadcaster.Broadcaster[[]*server.Event]
	notify      chan int64
	currentRev  int64
	wg          sync.WaitGroup
}

func New(d Dialect) *SQLLog {
	return &SQLLog{
		d:      d,
		notify: make(chan int64, 1024),
	}
}

func (s *SQLLog) Start(startCtx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return nil
	}

	_, _, err := s.Create(startCtx, "/registry/health", []byte(`{"health":"true"}`), 0)
	if err != nil {
		return err
	}

	ctx, stop := context.WithCancel(context.Background())
	err = s.broadcaster.Start(ctx, s.startWatch)
	if err != nil {
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
	defer s.mu.Unlock()

	if !s.started {
		return nil
	}

	s.stop()
	s.wg.Wait()
	s.stop, s.started = nil, false
	return nil
}

func (s *SQLLog) Close() error {
	stopErr := s.Stop()
	closeErr := s.d.Close()

	return errors.Join(stopErr, closeErr)
}

func (s *SQLLog) compactStart(ctx context.Context) error {
	rows, err := s.d.AfterPrefix(ctx, "compact_rev_key", 0, 0)
	if err != nil {
		return err
	}

	events, err := ScanAll(rows, scanEvent)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		_, _, err := s.Create(ctx, "compact_rev_key", []byte(""), 0)
		return err
	} else if len(events) == 1 {
		return nil
	}

	// this is to work around a bug in which we ended up with two compact_rev_key rows
	maxRev := int64(0)
	maxID := int64(0)
	for _, event := range events {
		if event.PrevKV != nil && event.PrevKV.ModRevision > maxRev {
			maxRev = event.PrevKV.ModRevision
			maxID = event.KV.ModRevision
		}
	}

	for _, event := range events {
		if event.KV.ModRevision == maxID {
			continue
		}
		if err := s.d.DeleteRevision(ctx, event.KV.ModRevision); err != nil {
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
	start, target, err := s.d.GetCompactRevision(ctx)
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
		if err := s.d.Compact(ctx, batchRevision); err != nil {
			return err
		}
		start = batchRevision
	}
	return nil
}

func (s *SQLLog) CurrentRevision(ctx context.Context) (int64, error) {
	if s.currentRev != 0 {
		return s.currentRev, nil
	}
	return s.d.CurrentRevision(ctx)
}

func (s *SQLLog) After(ctx context.Context, prefix string, revision, limit int64) (int64, []*server.Event, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.After", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.Int64("revision", revision),
		attribute.Int64("limit", limit),
	)

	compactRevision, currentRevision, err := s.d.GetCompactRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, nil, server.ErrCompacted
	}

	rows, err := s.d.AfterPrefix(ctx, prefix, revision, limit)
	if err != nil {
		return 0, nil, err
	}

	result, err := ScanAll(rows, scanEvent)
	if err != nil {
		return 0, nil, err
	}
	return currentRevision, result, err
}

func (s *SQLLog) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*server.KeyValue, error) {
	var err error

	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.List", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.String("startKey", startKey),
		attribute.Int64("limit", limit),
		attribute.Int64("revision", revision),
	)

	compactRevision, currentRevision, err := s.d.GetCompactRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, nil, server.ErrCompacted
	}

	// It's assumed that when there is a start key that that key exists.
	if strings.HasSuffix(prefix, "/") {
		// In the situation of a list start the startKey will not exist so set to ""
		if prefix == startKey {
			startKey = ""
		}
	} else {
		// Also if this isn't a list there is no reason to pass startKey
		startKey = ""
	}

	rows, err := s.d.List(ctx, prefix, startKey, limit, revision)
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
	run := func(ctx context.Context, key string, revision int64, timeout time.Duration) {
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

		startRevision, err := s.d.CurrentRevision(ctx)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		rows, err := s.d.ListTTL(ctx, startRevision)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		var (
			key             string
			revision, lease int64
		)
		for rows.Next() {
			if err := rows.Scan(&revision, &key, &lease); err != nil {
				logrus.Errorf("failed to read old events for ttl: %v", err)
				return
			}
			go run(ctx, key, revision, time.Duration(lease)*time.Second)
		}

		wr, err := s.Watch(ctx, "/", startRevision)
		if err != nil {
			logrus.Errorf("failed to watch events for ttl: %v", err)
			return
		}

		for events := range wr.Events {
			for _, event := range events {
				if event.KV.Lease > 0 {
					go run(ctx, event.KV.Key, event.KV.ModRevision, time.Duration(event.KV.Lease)*time.Second)
				}
			}
		}
	}()
}

func (s *SQLLog) Watch(ctx context.Context, key string, startRevision int64) (server.WatchResult, error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Watch", otelName))
	defer span.End()
	span.SetAttributes(
		attribute.String("key", key),
		attribute.Int64("startRevision", startRevision),
	)

	res := make(chan []*server.Event, 100)
	wr := server.WatchResult{Events: res}

	values, err := s.broadcaster.Subscribe(ctx)
	if err != nil {
		return wr, err
	}

	if startRevision > 0 {
		startRevision = startRevision - 1
	}

	initialRevision, initialEvents, err := s.After(ctx, key, startRevision, 0)
	if err != nil {
		span.RecordError(err)
		if err == server.ErrCompacted {
			compact, _, _ := s.d.GetCompactRevision(ctx)
			wr.CompactRevision = compact
			wr.CurrentRevision = initialRevision
		}
	}

	if len(initialEvents) > 0 {
		res <- initialEvents
	}

	s.wg.Add(1)
	go func() {
		defer func() {
			close(res)
			s.wg.Done()
		}()

		for events := range values {
			filtered := filterEvents(events, key, initialRevision)
			if len(filtered) > 0 {
				res <- filtered
			}
		}
	}()

	return wr, nil
}

func filterEvents(events []*server.Event, key string, startRevision int64) []*server.Event {
	filteredEventList := make([]*server.Event, 0, len(events))
	checkPrefix := strings.HasSuffix(key, "/")

	for _, event := range events {
		if event.KV.ModRevision <= startRevision {
			continue
		}
		if !(checkPrefix && strings.HasPrefix(event.KV.Key, key)) && event.KV.Key != key {
			continue
		}
		filteredEventList = append(filteredEventList, event)
	}

	return filteredEventList
}

func (s *SQLLog) startWatch(ctx context.Context) (chan []*server.Event, error) {
	if err := s.compactStart(ctx); err != nil {
		return nil, err
	}

	pollStart, _, err := s.d.GetCompactRevision(ctx)
	if err != nil {
		return nil, err
	}

	c := make(chan []*server.Event)
	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	s.wg.Add(2)

	go func() {
		defer s.wg.Done()

		t := time.NewTicker(s.d.GetCompactInterval())

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
		s.poll(ctx, c, pollStart)
	}()

	return c, nil
}

func (s *SQLLog) poll(ctx context.Context, result chan []*server.Event, pollStart int64) {
	s.currentRev = pollStart
	var (
		skip        int64
		skipTime    time.Time
		waitForMore = true
	)

	wait := time.NewTicker(s.d.GetPollInterval())
	defer wait.Stop()
	defer close(result)

	for {
		if waitForMore {
			select {
			case <-ctx.Done():
				return
			case check := <-s.notify:
				if check <= s.currentRev {
					continue
				}
			case <-wait.C:
			}
		}
		waitForMore = true
		watchCtx, cancel := context.WithTimeout(ctx, s.d.GetWatchQueryTimeout())
		defer cancel()

		rows, err := s.d.After(watchCtx, s.currentRev, pollBatchSize)
		if err != nil {
			if !errors.Is(err, context.DeadlineExceeded) {
				logrus.Errorf("fail to list latest changes: %v", err)
			}
			continue
		}

		events, err := ScanAll(rows, scanEvent)
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		if len(events) == 0 {
			continue
		}

		waitForMore = len(events) < 100

		rev := s.currentRev
		var (
			sequential []*server.Event
			saveLast   bool
		)

		for _, event := range events {
			next := rev + 1
			// Ensure that we are notifying events in a sequential fashion. For example if we find row 4 before 3
			// we don't want to notify row 4 because 3 is essentially dropped forever.
			if event.KV.ModRevision != next {
				logrus.Tracef("MODREVISION GAP: expected %v, got %v", next, event.KV.ModRevision)
				if canSkipRevision(next, skip, skipTime) {
					// This situation should never happen, but we have it here as a fallback just for unknown reasons
					// we don't want to pause all watches forever
					logrus.Errorf("GAP %s, revision=%d, delete=%v, next=%d", event.KV.Key, event.KV.ModRevision, event.Delete, next)
				} else if skip != next {
					// This is the first time we have encountered this missing revision, so record time start
					// and trigger a quick retry for simple out of order events
					skip = next
					skipTime = time.Now()
					s.notifyWatcherPoll(next)
					break
				} else {
					if err := s.d.Fill(ctx, next); err == nil {
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
			saveLast = true
			rev = event.KV.ModRevision
			if s.d.IsFill(event.KV.Key) {
				logrus.Debugf("NOT TRIGGER FILL %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
			} else {
				sequential = append(sequential, event)
				logrus.Debugf("TRIGGERED %s, revision=%d, delete=%v", event.KV.Key, event.KV.ModRevision, event.Delete)
			}
		}

		if saveLast {
			s.currentRev = rev
			if len(sequential) > 0 {
				result <- sequential
			}
		}
	}
}

func canSkipRevision(rev, skip int64, skipTime time.Time) bool {
	return rev == skip && time.Since(skipTime) > time.Second
}

func (s *SQLLog) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Count", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("prefix", prefix),
		attribute.String("startKey", startKey),
		attribute.Int64("revision", revision),
	)

	compactRevision, currentRevision, err := s.d.GetCompactRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, 0, server.ErrCompacted
	}
	count, err := s.d.Count(ctx, prefix, startKey, revision)
	if err != nil {
		return 0, 0, err
	}
	return currentRevision, count, nil
}

func (s *SQLLog) Create(ctx context.Context, key string, value []byte, lease int64) (int64, bool, error) {
	rev, created, err := s.d.Create(ctx, key, value, lease)
	if err != nil {
		return 0, false, err
	}
	if created {
		s.notifyWatcherPoll(rev)
	}
	return rev, created, nil
}

func (s *SQLLog) Delete(ctx context.Context, key string, revision int64) (rev int64, deleted bool, err error) {
	rev, deleted, err = s.d.Delete(ctx, key, revision)
	if err != nil {
		return 0, false, err
	}
	if deleted {
		s.notifyWatcherPoll(rev)
	}
	return rev, deleted, nil
}

func (s *SQLLog) Update(ctx context.Context, key string, value []byte, prevRev, lease int64) (rev int64, updated bool, err error) {
	rev, updated, err = s.d.Update(ctx, key, value, prevRev, lease)
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
	size, err := s.d.GetSize(ctx)
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

func scanKeyValue(rows *sql.Rows) (*server.KeyValue, error) {
	kv := &server.KeyValue{}
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

func scanEvent(rows *sql.Rows) (*server.Event, error) {
	event := &server.Event{
		KV:     &server.KeyValue{},
		PrevKV: &server.KeyValue{},
	}

	err := rows.Scan(
		&event.KV.ModRevision,
		&event.KV.Key,
		&event.Create,
		&event.Delete,
		&event.KV.CreateRevision,
		&event.PrevKV.ModRevision,
		&event.KV.Lease,
		&event.KV.Value,
		&event.PrevKV.Value,
	)
	if err != nil {
		return nil, err
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	} else {
		event.PrevKV.Key = event.KV.Key
		event.PrevKV.CreateRevision = event.KV.CreateRevision
		event.PrevKV.Lease = event.KV.Lease
	}

	return event, nil
}
