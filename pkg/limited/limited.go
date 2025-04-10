package limited

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.opentelemetry.io/otel/attribute"
)

const (
	SupersededCount  = 100
	compactBatchSize = 1000
	pollBatchSize    = 500
)

type LimitedServerConfig struct {
	// Driver is the SQL driver to use to query the database.
	Driver Driver

	// CompactInterval is interval between database compactions performed by k8s-dqlite.
	CompactInterval time.Duration

	// PollInterval is the event poll interval used by k8s-dqlite.
	PollInterval time.Duration

	// WatchQueryTimeout is the timeout on the after query in the poll loop.
	WatchQueryTimeout time.Duration
}

type LimitedServer struct {
	config *LimitedServerConfig

	mu sync.Mutex
	wg sync.WaitGroup

	stop func()

	started bool

	watcherGroups map[*watcherGroup]*watcherGroup
	pollRevision  int64

	notify chan int64
}

func NewLimitedServer(config *LimitedServerConfig) *LimitedServer {
	return &LimitedServer{
		config:        config,
		notify:        make(chan int64, 100),
		watcherGroups: make(map[*watcherGroup]*watcherGroup),
	}
}

func (l *LimitedServer) notifyWatcherPoll(revision int64) {
	select {
	case l.notify <- revision:
	default:
	}
}

func (l *LimitedServer) Start(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.started {
		return nil
	}

	_, _, err := l.internalCreate(ctx, []byte("/registry/health"), []byte(`{"health":"true"}`), 0)
	if err != nil {
		return err
	}

	ctx, stop := context.WithCancel(context.Background())
	if err := l.startWatch(ctx); err != nil {
		stop()
		return err
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()
		l.ttl(ctx)
	}()

	l.stop = stop
	l.started = true
	return nil
}

func (l *LimitedServer) compactStart(ctx context.Context) error {
	currentRevision, err := l.config.Driver.CurrentRevision(ctx)

	rows, err := l.config.Driver.AfterPrefix(ctx, []byte("compact_rev_key"), []byte("compact_rev_key\x00"), 0, currentRevision)
	if err != nil {
		return err
	}

	events, err := ScanAll(rows, scanEvent)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		_, _, err := l.internalCreate(ctx, []byte("compact_rev_key"), nil, 0)
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
		if err := l.config.Driver.DeleteRevision(ctx, event.Kv.ModRevision); err != nil {
			return err
		}
	}

	return nil
}

// DoCompact makes a single compaction run when called. It is intended to be called
// from test functions that have access to the backend.
func (l *LimitedServer) DoCompact(ctx context.Context) (err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.DoCompact", otelName))
	compactCnt.Add(ctx, 1)
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	if err := l.compactStart(ctx); err != nil {
		return fmt.Errorf("failed to initialise compaction: %v", err)
	}

	// When executing compaction as a background operation
	// it's best not to take too much time away from query
	// operation and similar. As such, we do compaction in
	// small batches. Given that this logic runs every second,
	// on regime it should take usually just a couple batches
	// to keep the pace.
	start, target, err := l.config.Driver.GetCompactRevision(ctx)
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
		if err := l.config.Driver.Compact(ctx, batchRevision); err != nil {
			return err
		}
		start = batchRevision
	}
	return nil
}

func (l *LimitedServer) WatcherGroup(ctx context.Context) (WatcherGroup, error) {
	var err error

	watcherGroupCnt.Add(ctx, 1)
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.WatcherGroup", otelName))
	span.SetAttributes(attribute.Int64("currentRevision", l.pollRevision))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	l.mu.Lock()
	defer l.mu.Unlock()

	wg := &watcherGroup{
		ctx:             ctx,
		driver:          l.config.Driver,
		currentRevision: l.pollRevision,
		watchers:        make(map[int64]*watcher),
		updates:         make(chan WatcherGroupUpdate, 100),
	}
	stop := context.AfterFunc(ctx, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		delete(l.watcherGroups, wg)
		close(wg.updates)
	})
	wg.stop = stop
	l.watcherGroups[wg] = wg
	logrus.Debugf("created new WatcherGroup.")
	return wg, nil
}

func (l *LimitedServer) ttl(ctx context.Context) {
	run := func(ctx context.Context, key []byte, revision int64, timeout time.Duration) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(timeout):
			l.internalDelete(ctx, key, revision)
		}
	}

	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		startRevision, err := l.config.Driver.CurrentRevision(ctx)
		if err != nil {
			logrus.Errorf("failed to read old events for ttl: %v", err)
			return
		}

		rows, err := l.config.Driver.ListTTL(ctx, startRevision)
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
		group, err := l.WatcherGroup(ctx)
		if err != nil {
			logrus.Errorf("failed to create watch group for ttl: %v", err)
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

func (l *LimitedServer) startWatch(ctx context.Context) error {
	if err := l.compactStart(ctx); err != nil {
		return err
	}

	pollInitialRevision, _, err := l.config.Driver.GetCompactRevision(ctx)
	if err != nil {
		return err
	}
	l.pollRevision = pollInitialRevision

	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	l.wg.Add(2)

	go func() {
		defer l.wg.Done()

		t := time.NewTicker(l.config.CompactInterval)

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := l.DoCompact(ctx); err != nil {
					logrus.WithError(err).Trace("compaction failed")
				}
			}
		}
	}()

	go func() {
		defer l.wg.Done()
		l.poll(ctx)
	}()

	return nil
}

func (l *LimitedServer) poll(ctx context.Context) {
	ticker := time.NewTicker(l.config.PollInterval)
	defer ticker.Stop()

	waitForMore := true
	for {
		if waitForMore {
			select {
			case <-ctx.Done():
				return
			case check := <-l.notify:
				if check <= l.pollRevision {
					continue
				}
			case <-ticker.C:
			}
		}
		events, err := l.getLatestEvents(ctx, l.pollRevision)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			logrus.Errorf("fail to get latest events: %v", err)
			continue
		}

		waitForMore = len(events) < 100
		l.publishEvents(events)
	}
}

func (l *LimitedServer) publishEvents(events []*Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(events) > 0 {
		l.pollRevision = events[len(events)-1].Kv.ModRevision
	}
	for _, w := range l.watcherGroups {
		if !w.publish(l.pollRevision, events) {
			if w.stop() {
				delete(l.watcherGroups, w)
				close(w.updates)
			}
		}
	}
}

func (l *LimitedServer) getLatestEvents(ctx context.Context, pollRevision int64) ([]*Event, error) {
	var err error
	watchCtx, cancel := context.WithTimeout(ctx, l.config.WatchQueryTimeout)
	defer cancel()

	watchCtx, span := otelTracer.Start(watchCtx, fmt.Sprintf("%s.getLatestEvents", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("pollRevision", pollRevision))

	rows, err := l.config.Driver.After(watchCtx, pollRevision, pollBatchSize)
	if err != nil {
		return nil, err
	}

	events, err := ScanAll(rows, scanEvent)
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Int("events", len(events)))
	return events, nil
}

func (l *LimitedServer) Stop() error {
	l.mu.Lock()

	if !l.started {
		return nil
	}
	l.stop()
	l.stop, l.started = nil, false
	l.mu.Unlock()

	l.wg.Wait()
	return nil
}

func (l *LimitedServer) Close() error {
	stopErr := l.Stop()
	closeErr := l.config.Driver.Close()

	return errors.Join(stopErr, closeErr)
}

func (l *LimitedServer) Range(ctx context.Context, r *etcdserverpb.RangeRequest) (*RangeResponse, error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Range", otelName))
	defer span.End()
	if len(r.RangeEnd) == 0 {
		return l.get(ctx, r)
	}
	return l.list(ctx, r)
}

func txnHeader(rev int64) *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		Revision: rev,
	}
}

func (l *LimitedServer) Txn(ctx context.Context, txn *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if put := isCreate(txn); put != nil {
		return l.create(ctx, put)
	}
	if rev, key, ok := isDelete(txn); ok {
		return l.delete(ctx, key, rev)
	}
	if rev, key, value, lease, ok := isUpdate(txn); ok {
		return l.update(ctx, rev, key, value, lease)
	}
	if isCompact(txn) {
		return l.compact(ctx)
	}
	return nil, fmt.Errorf("unsupported transaction: %v", txn)
}

type ResponseHeader struct {
	Revision int64
}

type RangeResponse struct {
	Header *etcdserverpb.ResponseHeader
	Kvs    []*KeyValue
	More   bool
	Count  int64
}
