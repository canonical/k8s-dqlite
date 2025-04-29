package sqlite

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/backend/v1/internal/backend"
	"github.com/canonical/k8s-dqlite/pkg/limited"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	backendOtelName  = "backend"
	SupersededCount  = 100
	compactBatchSize = 1000
	pollBatchSize    = 500
)

var (
	backendOtelTracer trace.Tracer
	backendOtelMeter  metric.Meter
	backendCompactCnt metric.Int64Counter
	watcherGroupCnt   metric.Int64Counter
)

func init() {
	var err error
	backendOtelTracer = otel.Tracer(backendOtelName)
	backendOtelMeter = otel.Meter(backendOtelName)

	backendCompactCnt, err = backendOtelMeter.Int64Counter(fmt.Sprintf("%s.compact", backendOtelName), metric.WithDescription("Number of compact requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
	watcherGroupCnt, err = backendOtelMeter.Int64Counter(fmt.Sprintf("%s.watcherGroup", backendOtelName), metric.WithDescription("Number of watcherGroup requests"))
	if err != nil {
		logrus.WithError(err).Warning("Otel failed to create create counter")
	}
}

type Backend struct {
	mu sync.Mutex

	Config backend.Config

	Driver backend.Driver

	stop    func()
	started bool

	WatcherGroups map[*WatcherGroup]*WatcherGroup
	pollRevision  int64

	Notify chan int64
	wg     sync.WaitGroup
}

type BackendConfig struct {
	backend.Config
	//DriverConfig is the sqlite driver config
	DriverConfig *DriverConfig
}

func NewBackend(ctx context.Context, config *BackendConfig) (limited.Backend, error) {
	driver, err := NewDriver(ctx, config.DriverConfig)
	if err != nil {
		return nil, err
	}

	return &Backend{
		Config:        config.Config,
		Driver:        driver,
		Notify:        make(chan int64, 100),
		WatcherGroups: make(map[*WatcherGroup]*WatcherGroup),
	}, nil
}

func (s *Backend) Start(startCtx context.Context) error {
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

func (s *Backend) Stop() error {
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

func (s *Backend) Close() error {
	stopErr := s.Stop()
	closeErr := s.Driver.Close()

	return errors.Join(stopErr, closeErr)
}

func (s *Backend) compactStart(ctx context.Context) error {
	currentRevision, err := s.Driver.CurrentRevision(ctx)

	rows, err := s.Driver.AfterPrefix(ctx, []byte("compact_rev_key"), []byte("compact_rev_key\x00"), 0, currentRevision)
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
		if err := s.Driver.DeleteRevision(ctx, event.Kv.ModRevision); err != nil {
			return err
		}
	}

	return nil
}

// DoCompact makes a single compaction run when called. It is intended to be called
// from test functions that have access to the backend.
func (s *Backend) DoCompact(ctx context.Context) (err error) {
	ctx, span := backendOtelTracer.Start(ctx, fmt.Sprintf("%s.DoCompact", backendOtelName))
	backendCompactCnt.Add(ctx, 1)
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
	start, target, err := s.Driver.GetCompactRevision(ctx)
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
		if err := s.Driver.Compact(ctx, batchRevision); err != nil {
			return err
		}
		start = batchRevision
	}
	return nil
}

func (s *Backend) GetCompactRevision(ctx context.Context) (int64, int64, error) {
	return s.Driver.GetCompactRevision(ctx)
}

func (s *Backend) List(ctx context.Context, key, rangeEnd []byte, limit, revision int64) (int64, []*limited.KeyValue, error) {
	var err error

	ctx, span := backendOtelTracer.Start(ctx, fmt.Sprintf("%s.List", backendOtelName))
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

	compactRevision, currentRevision, err := s.Driver.GetCompactRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, nil, limited.ErrCompacted
	}

	rows, err := s.Driver.List(ctx, key, rangeEnd, limit, revision)
	if err != nil {
		return 0, nil, err
	}

	result, err := ScanAll(rows, scanKeyValue)
	if err != nil {
		return 0, nil, err
	}

	return currentRevision, result, err
}

func (s *Backend) startWatch(ctx context.Context) error {
	if err := s.compactStart(ctx); err != nil {
		return err
	}

	pollInitialRevision, _, err := s.Driver.GetCompactRevision(ctx)
	if err != nil {
		return err
	}
	s.pollRevision = pollInitialRevision

	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	s.wg.Add(2)

	go func() {
		defer s.wg.Done()

		t := time.NewTicker(s.Config.CompactInterval)

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

func (s *Backend) Count(ctx context.Context, key, rangeEnd []byte, revision int64) (int64, int64, error) {
	var err error
	ctx, span := backendOtelTracer.Start(ctx, fmt.Sprintf("%s.Count", backendOtelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("rangeEnd", string(rangeEnd)),
		attribute.Int64("revision", revision),
	)

	compactRevision, currentRevision, err := s.Driver.GetCompactRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	if revision == 0 || revision > currentRevision {
		revision = currentRevision
	} else if revision < compactRevision {
		return currentRevision, 0, limited.ErrCompacted
	}
	count, err := s.Driver.Count(ctx, key, rangeEnd, revision)
	if err != nil {
		return 0, 0, err
	}
	return currentRevision, count, nil
}

func (s *Backend) Create(ctx context.Context, key, value []byte, lease int64) (int64, bool, error) {
	rev, created, err := s.Driver.Create(ctx, key, value, lease)
	if err != nil {
		return 0, false, err
	}
	if created {
		s.notifyWatcherPoll(rev)
	}
	return rev, created, nil
}

func (s *Backend) Delete(ctx context.Context, key []byte, revision int64) (rev int64, deleted bool, err error) {
	rev, deleted, err = s.Driver.Delete(ctx, key, revision)
	if err != nil {
		return 0, false, err
	}
	if deleted {
		s.notifyWatcherPoll(rev)
	}
	return rev, deleted, nil
}

func (s *Backend) Update(ctx context.Context, key []byte, value []byte, prevRev, lease int64) (rev int64, updated bool, err error) {
	rev, updated, err = s.Driver.Update(ctx, key, value, prevRev, lease)
	if err != nil {
		return 0, false, err
	}
	if updated {
		s.notifyWatcherPoll(rev)
	}
	return rev, updated, nil
}

func (s *Backend) DbSize(ctx context.Context) (int64, error) {
	var err error
	ctx, span := backendOtelTracer.Start(ctx, fmt.Sprintf("%s.DbSize", backendOtelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	size, err := s.Driver.GetSize(ctx)
	span.SetAttributes(attribute.Int64("size", size))
	return size, err
}
