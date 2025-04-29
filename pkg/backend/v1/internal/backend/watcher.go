package backend

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

func (w *WatcherGroup) Watch(watchId int64, key, rangeEnd []byte, startRevision int64) error {
	var err error
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
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return
			}
			logrus.Errorf("fail to get latest events: %v", err)
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
	span.SetAttributes(attribute.Int64("currentRevision", s.pollRevision))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

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
	logrus.Debugf("created new WatcherGroup.")
	return wg, nil
}
