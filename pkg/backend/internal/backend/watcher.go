package internal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/limited"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

type WatcherGroup struct {
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
	startRevision int64
	initialized   bool
	events        []*limited.Event
}

type watcherGroupUpdate struct {
	revision int64
	updates  []limited.WatcherUpdate
}

func (w *watcherGroupUpdate) Revision() int64                   { return w.revision }
func (w *watcherGroupUpdate) Watchers() []limited.WatcherUpdate { return w.updates }

func (w *WatcherGroup) publish(currentRevision int64, events []*limited.Event) bool {
	_, span := backendOtelTracer.Start(w.ctx, fmt.Sprintf("%s.publish", backendOtelName))
	span.SetAttributes(attribute.Int64("currentRevision", currentRevision))
	span.SetAttributes(attribute.Int("events", len(events)))
	span.SetAttributes(attribute.Int("updates", len(w.updates)))
	defer span.End()
	w.mu.Lock()
	defer w.mu.Unlock()
	span.SetAttributes(attribute.Int("watchers", len(w.watchers)))

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
		w.events = w.appendEvents(w.events, events)
		return nil
	}
	return w.appendEvents(nil, events)
}

// appendEvents appends the events in the watcher's key range from its start
// revision on; the poll can be behind a watch that starts ahead of it.
func (w *watcher) appendEvents(dst, src []*limited.Event) []*limited.Event {
	for _, event := range appendEvents(src, nil, w.key, w.rangeEnd) {
		if event.Kv.ModRevision >= w.startRevision {
			dst = append(dst, event)
		}
	}
	return dst
}

// Watch adds a watcher and returns its events from startRevision up to
// revision; updates published later carry only its newer events.
func (w *WatcherGroup) Watch(watchId int64, key, rangeEnd []byte, startRevision int64) (revision int64, events []*limited.Event, err error) {
	ctx, span := backendOtelTracer.Start(w.ctx, fmt.Sprintf("%s.Watch", backendOtelName))
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
		watchId:       watchId,
		key:           key,
		rangeEnd:      rangeEnd,
		startRevision: startRevision,
	}

	w.mu.Lock()
	w.watchers[watchId] = watcher
	if startRevision > 0 {
		currentRevision := w.currentRevision
		w.mu.Unlock()

		events, err = w.loadInitialEvents(ctx, key, rangeEnd, startRevision, currentRevision)
		if err != nil {
			w.Unwatch(watchId)
			return 0, nil, err
		}

		w.mu.Lock()
	}
	events = append(events, watcher.events...)
	watcher.events = nil
	watcher.initialized = true
	revision = w.currentRevision
	w.mu.Unlock()

	return revision, events, nil
}

func (w *WatcherGroup) loadInitialEvents(ctx context.Context, key, rangeEnd []byte, startRevision, toRevision int64) ([]*limited.Event, error) {
	compactRev, rev, err := w.driver.GetCompactRevision(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get compact revision: %w", err)
	}
	if startRevision < compactRev {
		return nil, &limited.CompactedError{CompactRevision: compactRev, CurrentRevision: rev}
	}

	events, err := w.initialEvents(ctx, key, rangeEnd, startRevision-1, toRevision)
	if err != nil && !errors.Is(err, context.Canceled) {
		err = fmt.Errorf("failed to list %s for revision %d: %w", key, startRevision, err)
	}
	return events, err
}

func (w *WatcherGroup) initialEvents(ctx context.Context, key, rangeEnd []byte, fromRevision, toRevision int64) ([]*limited.Event, error) {
	var err error
	ctx, span := backendOtelTracer.Start(ctx, fmt.Sprintf("%s.initialEvents", backendOtelName))
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

func (w *WatcherGroup) Unwatch(watchId int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	delete(w.watchers, watchId)
}

func (w *WatcherGroup) Updates() <-chan limited.WatcherGroupUpdate { return w.updates }

var _ limited.WatcherGroup = &WatcherGroup{}

func (s *Backend) notifyWatcherPoll(revision int64) {
	select {
	case s.Notify <- revision:
	default:
	}
}

func (s *Backend) poll(ctx context.Context) {
	ticker := time.NewTicker(s.Config.PollInterval)
	defer ticker.Stop()

	waitForMore := true
	for {
		if waitForMore {
			select {
			case <-ctx.Done():
				return
			case check := <-s.Notify:
				if check <= s.pollRevision {
					continue
				}
			case <-ticker.C:
			}
		}
		events, err := s.getLatestEvents(ctx, s.pollRevision)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			logrus.Errorf("fail to get latest events: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(s.Config.PollInterval):
			}
			continue
		}

		waitForMore = len(events) < 100
		s.publishEvents(events)
	}
}

func (s *Backend) publishEvents(events []*limited.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(events) > 0 {
		s.pollRevision = events[len(events)-1].Kv.ModRevision
	}
	for _, w := range s.WatcherGroups {
		if !w.publish(s.pollRevision, events) {
			if w.stop() {
				delete(s.WatcherGroups, w)
				close(w.updates)
			}
		}
	}
}

func (s *Backend) getLatestEvents(ctx context.Context, pollRevision int64) ([]*limited.Event, error) {
	var err error
	watchCtx, cancel := context.WithTimeout(ctx, s.Config.WatchQueryTimeout)
	defer cancel()

	watchCtx, span := backendOtelTracer.Start(watchCtx, fmt.Sprintf("%s.getLatestEvents", backendOtelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.Int64("pollRevision", pollRevision))

	rows, err := s.Driver.After(watchCtx, pollRevision, pollBatchSize)
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

func (s *Backend) WatcherGroup(ctx context.Context) (limited.WatcherGroup, error) {
	var err error

	watcherGroupCnt.Add(ctx, 1)
	ctx, span := backendOtelTracer.Start(ctx, fmt.Sprintf("%s.WatcherGroup", backendOtelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()
	span.SetAttributes(attribute.Int64("currentRevision", s.pollRevision))

	wg := &WatcherGroup{
		ctx:             ctx,
		driver:          s.Driver,
		currentRevision: s.pollRevision,
		watchers:        make(map[int64]*watcher),
		updates:         make(chan limited.WatcherGroupUpdate, 100),
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.WatcherGroups, wg)
		close(wg.updates)
	})
	wg.stop = stop
	s.WatcherGroups[wg] = wg
	logrus.Debugf("created a new WatcherGroup")
	return wg, nil
}
