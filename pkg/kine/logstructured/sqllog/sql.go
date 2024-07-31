package sqllog

import (
	"context"
	"database/sql"
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
	SupersededCount = 100
	otelName        = "sqllog"
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

type SQLLog struct {
	d           Dialect
	broadcaster broadcaster.Broadcaster
	ctx         context.Context
	notify      chan int64
	wg          sync.WaitGroup
}

func New(d Dialect) *SQLLog {
	l := &SQLLog{
		d:      d,
		notify: make(chan int64, 1024),
	}
	return l
}

type Dialect interface {
	ListCurrent(ctx context.Context, prefix, startKey string, limit int64, includeDeleted bool) (*sql.Rows, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (*sql.Rows, error)
	CountCurrent(ctx context.Context, prefix, startKey string) (int64, int64, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	AfterPrefix(ctx context.Context, prefix string, rev, limit int64) (*sql.Rows, error)
	After(ctx context.Context, rev, limit int64) (*sql.Rows, error)
	Insert(ctx context.Context, key string, create, delete bool, createRevision, previousRevision int64, ttl int64, value, prevValue []byte) (int64, error)
	GetRevision(ctx context.Context, revision int64) (*sql.Rows, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, int64, error)
	SetCompactRevision(ctx context.Context, revision int64) error
	Fill(ctx context.Context, revision int64) error
	IsFill(key string) bool
	GetSize(ctx context.Context) (int64, error)
	GetCompactInterval() time.Duration
	GetPollInterval() time.Duration
	Close()
}

func (s *SQLLog) Start(ctx context.Context) (err error) {
	s.ctx = ctx
	context.AfterFunc(ctx, func() {
		s.d.Close()
	})
	return s.broadcaster.Start(s.startWatch)
}

func (s *SQLLog) Wait() {
	s.wg.Wait()
}

func (s *SQLLog) compactStart(ctx context.Context) error {
	rows, err := s.d.AfterPrefix(ctx, "compact_rev_key", 0, 0)
	if err != nil {
		return err
	}

	events, err := RowsToEvents(rows)
	if err != nil {
		return err
	}

	if len(events) == 0 {
		_, err := s.Append(ctx, &server.Event{
			Create: true,
			KV: &server.KeyValue{
				Key:   "compact_rev_key",
				Value: []byte(""),
			},
		})
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
func (s *SQLLog) DoCompact(ctx context.Context) error {
	if err := s.compactStart(ctx); err != nil {
		return fmt.Errorf("failed to initialise compaction: %v", err)
	}

	nextEnd, _ := s.d.CurrentRevision(ctx)
	_, err := s.compactor(ctx, nextEnd)

	return err
}

func (s *SQLLog) compactor(ctx context.Context, nextEnd int64) (int64, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.compactor", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("nextEnd", nextEnd))

	currentRev, err := s.d.CurrentRevision(ctx)
	span.SetAttributes(attribute.Int64("currentRev", currentRev))
	if err != nil {
		logrus.Errorf("failed to get current revision: %v", err)
		return nextEnd, fmt.Errorf("failed to get current revision: %v", err)
	}

	cursor, _, err := s.d.GetCompactRevision(ctx)
	if err != nil {
		logrus.Errorf("failed to get compact revision: %v", err)
		return nextEnd, fmt.Errorf("failed to get compact revision: %v", err)
	}
	span.SetAttributes(attribute.Int64("cursor", cursor))

	end := nextEnd
	nextEnd = currentRev

	// NOTE(neoaggelos): Ignoring the last 1000 revisions causes the following CNCF conformance test to fail.
	// This is because of low activity, where the created list is part of the last 1000 revisions and is not compacted.
	// Link to failing test: https://github.com/kubernetes/kubernetes/blob/f2cfbf44b1fb482671aedbfff820ae2af256a389/test/e2e/apimachinery/chunking.go#L144
	// To address this, we only ignore the last 100 revisions instead
	end = end - SupersededCount

	savedCursor := cursor
	// Purposefully start at the current and redo the current as
	// it could have failed before actually compacting
	compactCnt.Add(ctx, 1)
	span.AddEvent(fmt.Sprintf("start compaction from %d to %d", cursor, end))
	for ; cursor <= end; cursor++ {
		rows, err := s.d.GetRevision(ctx, cursor)
		if err != nil {
			logrus.Errorf("failed to get revision %d: %v", cursor, err)
			return nextEnd, fmt.Errorf("failed to get revision %d: %v", cursor, err)
		}

		events, err := RowsToEvents(rows)
		if err != nil {
			logrus.Errorf("failed to convert to events: %v", err)
			return nextEnd, fmt.Errorf("failed to convert to events: %v", err)
		}

		if len(events) == 0 {
			continue
		}

		event := events[0]

		if event.KV.Key == "compact_rev_key" {
			span.AddEvent("skip compact_rev_key")
			// don't compact the compact key
			continue
		}

		setRev := false
		if event.PrevKV != nil && event.PrevKV.ModRevision != 0 {
			if savedCursor != cursor {
				if err := s.d.SetCompactRevision(ctx, cursor); err != nil {
					span.AddEvent(fmt.Sprintf("failed to record compact revision: %v", err))
					logrus.Errorf("failed to record compact revision: %v", err)
					return nextEnd, fmt.Errorf("failed to record compact revision: %v", err)
				}
				savedCursor = cursor
				setRev = true
			}

			if err := s.d.DeleteRevision(ctx, event.PrevKV.ModRevision); err != nil {
				span.AddEvent(fmt.Sprintf("failed to delete revision %d", event.PrevKV.ModRevision))
				logrus.Errorf("failed to delete revision %d: %v", event.PrevKV.ModRevision, err)
				return nextEnd, fmt.Errorf("failed to delete revision %d: %v", event.PrevKV.ModRevision, err)
			}
		}

		if event.Delete {
			if !setRev && savedCursor != cursor {
				if err := s.d.SetCompactRevision(ctx, cursor); err != nil {
					logrus.Errorf("failed to record compact revision: %v", err)
					return nextEnd, fmt.Errorf("failed to record compact revision: %v", err)
				}
				savedCursor = cursor
			}

			if err := s.d.DeleteRevision(ctx, cursor); err != nil {
				logrus.Errorf("failed to delete current revision %d: %v", cursor, err)
				return nextEnd, fmt.Errorf("failed to delete current revision %d: %v", cursor, err)
			}
		}
	}

	if savedCursor != cursor {
		if err := s.d.SetCompactRevision(ctx, cursor); err != nil {
			logrus.Errorf("failed to record compact revision: %v", err)
			return nextEnd, fmt.Errorf("failed to record compact revision: %v", err)
		}
	}
	span.SetAttributes(attribute.Int64("new-nextEnd", nextEnd), attribute.Int64("cursor-ended:", cursor))
	return nextEnd, nil
}

func (s *SQLLog) compact() {
	var nextEnd int64
	t := time.NewTicker(s.d.GetCompactInterval())
	nextEnd, _ = s.d.CurrentRevision(s.ctx)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}

		nextEnd, _ = s.compactor(s.ctx, nextEnd)
	}
}

func (s *SQLLog) CurrentRevision(ctx context.Context) (int64, error) {
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
	rows, err := s.d.AfterPrefix(ctx, prefix, revision, limit)
	if err != nil {
		return 0, nil, err
	}

	result, err := RowsToEvents(rows)
	if err != nil {
		return 0, nil, err
	}

	compact, rev, err := s.d.GetCompactRevision(ctx)

	if err != nil {
		return 0, nil, err
	}

	if revision > 0 && revision < compact {
		return rev, result, server.ErrCompacted
	}

	return rev, result, err
}

func (s *SQLLog) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted bool) (int64, []*server.Event, error) {
	var (
		rows *sql.Rows
		err  error
	)
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
		attribute.Bool("includeDeleted", includeDeleted),
	)

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

	if revision == 0 {
		rows, err = s.d.ListCurrent(ctx, prefix, startKey, limit, includeDeleted)
	} else {
		rows, err = s.d.List(ctx, prefix, startKey, limit, revision, includeDeleted)
	}
	if err != nil {
		return 0, nil, err
	}

	result, err := RowsToEvents(rows)
	if err != nil {
		return 0, nil, err
	}

	compact, rev, err := s.d.GetCompactRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	if revision > 0 && revision < compact {
		return rev, result, server.ErrCompacted
	}

	select {
	case s.notify <- rev:
	default:
	}

	return rev, result, err
}

func RowsToEvents(rows *sql.Rows) ([]*server.Event, error) {
	var result []*server.Event
	defer rows.Close()

	for rows.Next() {
		event := &server.Event{}
		if err := scan(rows, event); err != nil {
			return nil, err
		}
		result = append(result, event)
	}

	return result, nil
}

func (s *SQLLog) Watch(ctx context.Context, prefix string) <-chan []*server.Event {
	res := make(chan []*server.Event, 100)
	values, err := s.broadcaster.Subscribe(ctx)
	if err != nil {
		return nil
	}

	checkPrefix := strings.HasSuffix(prefix, "/")

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(res)

		for i := range values {
			events, ok := filter(i, checkPrefix, prefix)
			if ok {
				res <- events
			}
		}
	}()

	return res
}

func filter(events interface{}, checkPrefix bool, prefix string) ([]*server.Event, bool) {
	eventList := events.([]*server.Event)
	filteredEventList := make([]*server.Event, 0, len(eventList))

	for _, event := range eventList {
		if (checkPrefix && strings.HasPrefix(event.KV.Key, prefix)) || event.KV.Key == prefix {
			filteredEventList = append(filteredEventList, event)
		}
	}

	return filteredEventList, len(filteredEventList) > 0
}

func (s *SQLLog) startWatch() (chan interface{}, error) {
	if err := s.compactStart(s.ctx); err != nil {
		return nil, err
	}

	pollStart, _, err := s.d.GetCompactRevision(s.ctx)
	if err != nil {
		return nil, err
	}

	c := make(chan interface{})
	// start compaction and polling at the same time to watch starts
	// at the oldest revision, but compaction doesn't create gaps
	s.wg.Add(2)

	go func() {
		defer s.wg.Done()
		s.compact()
	}()

	go func() {
		defer s.wg.Done()
		s.poll(c, pollStart)
	}()

	return c, nil
}

func (s *SQLLog) poll(result chan interface{}, pollStart int64) {
	var (
		last        = pollStart
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
			case <-s.ctx.Done():
				return
			case check := <-s.notify:
				if check <= last {
					continue
				}
			case <-wait.C:
			}
		}
		waitForMore = true

		rows, err := s.d.After(s.ctx, last, 500)
		if err != nil {
			logrus.Errorf("fail to list latest changes: %v", err)
			continue
		}

		events, err := RowsToEvents(rows)
		if err != nil {
			logrus.Errorf("fail to convert rows changes: %v", err)
			continue
		}

		if len(events) == 0 {
			continue
		}

		waitForMore = len(events) < 100

		rev := last
		var (
			sequential []*server.Event
			saveLast   bool
		)

		for _, event := range events {
			next := rev + 1
			// Ensure that we are notifying events in a sequential fashion. For example if we find row 4 before 3
			// we don't want to notify row 4 because 3 is essentially dropped forever.
			if event.KV.ModRevision != next {
				if canSkipRevision(next, skip, skipTime) {
					// This situation should never happen, but we have it here as a fallback just for unknown reasons
					// we don't want to pause all watches forever
					logrus.Errorf("GAP %s, revision=%d, delete=%v, next=%d", event.KV.Key, event.KV.ModRevision, event.Delete, next)
				} else if skip != next {
					// This is the first time we have encountered this missing revision, so record time start
					// and trigger a quick retry for simple out of order events
					skip = next
					skipTime = time.Now()
					select {
					case s.notify <- next:
					default:
					}
					break
				} else {
					if err := s.d.Fill(s.ctx, next); err == nil {
						logrus.Debugf("FILL, revision=%d, err=%v", next, err)
						select {
						case s.notify <- next:
						default:
						}
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
			last = rev
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
	if revision == 0 {
		return s.d.CountCurrent(ctx, prefix, startKey)
	}

	return s.d.Count(ctx, prefix, startKey, revision)
}

func (s *SQLLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.Append", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	e := *event
	if e.KV == nil {
		e.KV = &server.KeyValue{}
	}
	if e.PrevKV == nil {
		e.PrevKV = &server.KeyValue{}
	}

	rev, err := s.d.Insert(ctx, e.KV.Key,
		e.Create,
		e.Delete,
		e.KV.CreateRevision,
		e.PrevKV.ModRevision,
		e.KV.Lease,
		e.KV.Value,
		e.PrevKV.Value,
	)
	span.SetAttributes(attribute.Int64("revision", rev))

	if err != nil {
		return 0, err
	}
	select {
	case s.notify <- rev:
	default:
	}
	return rev, nil
}

func scan(rows *sql.Rows, event *server.Event) error {
	event.KV = &server.KeyValue{}
	event.PrevKV = &server.KeyValue{}

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
		return err
	}

	if event.Create {
		event.KV.CreateRevision = event.KV.ModRevision
		event.PrevKV = nil
	}

	return nil
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
