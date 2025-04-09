package sqllog

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/canonical/k8s-dqlite/pkg/limited"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type watcherGroupUpdate struct {
	revision int64
	updates  []limited.WatcherUpdate
}

func (w *watcherGroupUpdate) Revision() int64                   { return w.revision }
func (w *watcherGroupUpdate) Watchers() []limited.WatcherUpdate { return w.updates }

type watcher struct {
	watchId       int64
	key, rangeEnd []byte
	initialized   bool
	events        []*limited.Event
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

type watcherGroup struct {
	mu              sync.Mutex
	ctx             context.Context
	driver          Driver
	currentRevision int64
	watchers        map[int64]*watcher
	updates         chan limited.WatcherGroupUpdate
	stop            func() bool
}

func (w *watcherGroup) publish(currentRevision int64, events []*limited.Event) bool {
	_, span := otelTracer.Start(w.ctx, fmt.Sprintf("%s.publish", otelName))
	span.SetAttributes(attribute.Int64("currentRevision", currentRevision))
	span.SetAttributes(attribute.Int("events", len(events)))
	span.SetAttributes(attribute.Int("watchers", len(w.watchers)))
	span.SetAttributes(attribute.Int("updates", len(w.updates)))
	defer span.End()
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
			return &limited.CompactedError{CompactRevision: compactRev, CurrentRevision: rev}
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

func (w *watcherGroup) initialEvents(ctx context.Context, key, rangeEnd []byte, fromRevision, toRevision int64) ([]*limited.Event, error) {
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

func (w *watcherGroup) Updates() <-chan limited.WatcherGroupUpdate { return w.updates }

var _ limited.WatcherGroup = &watcherGroup{}
