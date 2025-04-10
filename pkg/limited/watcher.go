package limited

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.opentelemetry.io/otel/attribute"
)

type watcherGroupUpdate struct {
	revision int64
	updates  []WatcherUpdate
}

func (w *watcherGroupUpdate) Revision() int64           { return w.revision }
func (w *watcherGroupUpdate) Watchers() []WatcherUpdate { return w.updates }

type watcher struct {
	watchId       int64
	key, rangeEnd []byte
	initialized   bool
	events        []*Event
}

func (w *watcher) update(events []*Event) []*Event {
	if !w.initialized {
		w.events = appendEvents(events, w.events, w.key, w.rangeEnd)
		return nil
	} else if len(w.events) > 0 {
		initialEvents := w.events
		w.events = nil
		return appendEvents(events, initialEvents, w.key, w.rangeEnd)
	}

	filtered := make([]*Event, 0, len(events))
	return appendEvents(events, filtered, w.key, w.rangeEnd)
}

type watcherGroup struct {
	mu              sync.Mutex
	ctx             context.Context
	driver          Driver
	currentRevision int64
	watchers        map[int64]*watcher
	updates         chan WatcherGroupUpdate
	stop            func() bool
}

func (w *watcherGroup) publish(currentRevision int64, events []*Event) bool {
	_, span := otelTracer.Start(w.ctx, fmt.Sprintf("%s.publish", otelName))
	span.SetAttributes(attribute.Int64("currentRevision", currentRevision))
	span.SetAttributes(attribute.Int("events", len(events)))
	span.SetAttributes(attribute.Int("watchers", len(w.watchers)))
	span.SetAttributes(attribute.Int("updates", len(w.updates)))
	defer span.End()
	w.mu.Lock()
	defer w.mu.Unlock()

	w.currentRevision = currentRevision
	updates := []WatcherUpdate{}
	for watchId, watcher := range w.watchers {
		events := watcher.update(events)
		if len(events) > 0 {
			updates = append(updates, WatcherUpdate{
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

func (w *watcherGroup) Watch(watchId int64, key, rangeEnd []byte, startRevision int64) error {
	var err error
	ctx, span := otelTracer.Start(w.ctx, fmt.Sprintf("%s.Watch", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("rangeEnd", string(rangeEnd)),
		attribute.Int64("startRevision", startRevision),
		attribute.Int64("watchId", watchId),
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

		compactRev, rev, err := w.driver.GetCompactRevision(ctx)
		if err != nil {
			logrus.Errorf("Failed to get compact revision: %v", err)
			return err
		}
		if startRevision < compactRev {
			return &CompactedError{CompactRevision: compactRev, CurrentRevision: rev}
		}

		initialEvents, err := w.initialEvents(ctx, key, rangeEnd, startRevision-1, currentRevision)
		if err != nil {
			w.mu.Lock()
			delete(w.watchers, watchId)
			w.mu.Unlock()
			if !errors.Is(err, context.Canceled) {
				logrus.Errorf("Failed to list %s for revision %d: %v", key, startRevision, err)
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

func (w *watcherGroup) initialEvents(ctx context.Context, key, rangeEnd []byte, fromRevision, toRevision int64) ([]*Event, error) {
	var err error
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.initialEvents", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(
		attribute.String("key", string(key)),
		attribute.String("rangeEnd", string(rangeEnd)),
		attribute.Int64("fromRevision", fromRevision),
		attribute.Int64("toRevision", toRevision),
	)

	rows, err := w.driver.AfterPrefix(ctx, key, rangeEnd, fromRevision, toRevision)
	if err != nil {
		return nil, err
	}

	events, err := ScanAll(rows, scanEvent)
	span.SetAttributes(attribute.Int("events", len(events)))
	return events, err
}

func (w *watcherGroup) Unwatch(watchId int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.watchers, watchId)
}

func (w *watcherGroup) Updates() <-chan WatcherGroupUpdate { return w.updates }

var _ WatcherGroup = &watcherGroup{}

func appendEvents(src, dst []*Event, key, rangeEnd []byte) []*Event {
	for _, event := range src {
		if bytes.Compare(key, event.Kv.Key) <= 0 && bytes.Compare(rangeEnd, event.Kv.Key) > 0 {
			dst = append(dst, event)
		}
	}
	return dst
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

func scanEvent(rows *sql.Rows) (*Event, error) {
	// Bundle in a single allocation
	row := &struct {
		event            Event
		kv, prevKv       KeyValue
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

func scanKeyValue(rows *sql.Rows) (*KeyValue, error) {
	kv := &KeyValue{}
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
