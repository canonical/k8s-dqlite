package limited

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	watchID int64
)

// explicit interface check
var _ etcdserverpb.WatchServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	w := watcher{
		server:  ws,
		backend: s.limited.backend,
		watches: make(map[int64]*watch),
	}
	defer w.Close()

	w.pollProgressNotify(ws.Context(), s.limited.notifyInterval)

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if cr := msg.GetCreateRequest(); cr != nil {
			w.Start(ws.Context(), cr)
		}
		if cr := msg.GetCancelRequest(); cr != nil {
			logrus.Tracef("WATCH CANCEL REQ id=%d", cr.WatchId)
			w.Cancel(cr.WatchId)
		}
		if pr := msg.GetProgressRequest(); pr != nil {
			w.Progress(ws.Context())
		}
	}
}

// pollProgressNotify periodically sends progress notifications to all watchers.
func (w *watcher) pollProgressNotify(ctx context.Context, interval time.Duration) {
	go func() {
		tick := time.NewTicker(interval)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				if err := w.progressTimeout(ctx); err != nil {
					logrus.Errorf("Failed to send progress notification: %v", err)
				}
			}
		}
	}()
}

type watcher struct {
	mu sync.Mutex

	wg            sync.WaitGroup
	backend       Backend
	server        etcdserverpb.Watch_WatchServer
	watches       map[int64]*watch
	syncRevision  int64
	unsyncedCount int
}

type watch struct {
	currentRevision int64
	busy            bool
	stop            func()
	progressRequest chan struct{}
}

func (w *watcher) updateRevision(watch *watch, revision int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.unsyncedCount > 0 {
		if w.syncRevision == revision {
			w.unsyncedCount--
		}
		if w.unsyncedCount == 0 {
			w.server.Send(&etcdserverpb.WatchResponse{
				// TODO
			})
		}
	}
	watch.currentRevision = revision
}

func (w *watcher) Start(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	if r.WatchId != clientv3.AutoWatchID {
		logrus.Errorf("WATCH START id=%d ignoring request with client-provided id", r.WatchId)
		if err := w.server.Send(&etcdserverpb.WatchResponse{
			Header:       &etcdserverpb.ResponseHeader{},
			WatchId:      r.WatchId,
			Canceled:     true,
			CancelReason: "not supported", // FIXME: check which cancel reason to send, maybe close the connection?
		}); err != nil {
			logrus.WithError(err).Errorf("WATCH START failed to send fail response")
		}
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	id := atomic.AddInt64(&watchID, 1)
	if err := w.server.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		Created: true,
		WatchId: id,
	}); err != nil {
		logrus.WithError(err).Errorf("WATCH START failed to send create response")
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	watch := &watch{
		stop: cancel,
	}
	if r.ProgressNotify {
		watch.progressRequest = make(chan struct{})
	}
	w.watches[id] = watch

	logrus.Tracef("WATCH START id=%d, key=%s, revision=%d, progressNotify=%v", id, string(r.Key), r.StartRevision, r.ProgressNotify)
	w.wg.Add(1)
	go func() {
		defer func() {
			w.wg.Done()
			w.removeWatch(id)
			if watch.progressRequest != nil {
				close(watch.progressRequest)
			}
		}()

		rangeEnd := r.RangeEnd
		if len(rangeEnd) == 0 {
			rangeEnd = append(r.Key, 0)
		}

		watchCh, err := w.backend.Watch(ctx, r.Key, rangeEnd, r.StartRevision)
		if err != nil {
			logrus.WithError(err).Errorf("WATCH START failed to start watch")
			compactRev, revision, revisionErr := w.backend.GetCompactRevision(w.server.Context())
			if revisionErr != nil {
				logrus.WithError(revisionErr).Errorf("WATCH START failed to fetch compact revision")
			}
			if err := w.server.Send(&etcdserverpb.WatchResponse{
				Header:          txnHeader(revision),
				Canceled:        true,
				CancelReason:    err.Error(),
				CompactRevision: compactRev,
				WatchId:         id,
			}); err != nil {
				logrus.WithError(err).Errorf("WATCH START failed to send stop response")
			}
			return
		}

		watch.currentRevision = r.StartRevision
		shouldNotify := true
		for {
			select {
			case <-ctx.Done():
				logrus.Debugf("WATCH CLOSE id=%d, key=%s", id, string(r.Key))
				err := w.server.Send(&etcdserverpb.WatchResponse{
					Header:   &etcdserverpb.ResponseHeader{},
					Canceled: true,
					WatchId:  watchID,
				})
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					logrus.WithError(err).Errorf("WATCH CLOSE cannot send close response")
				}
				return
			case watchData := <-watchCh:
				w.updateRevision(watch, watchData.CurrentRevision)
				if len(watchData.Events) > 0 {
					wr := &etcdserverpb.WatchResponse{
						Header:  txnHeader(watch.currentRevision),
						WatchId: id,
						Events:  toEvents(watchData.Events...),
					}
					if err := w.server.Send(wr); err != nil {
						logrus.WithError(err).Errorf("WATCH SEND cannot send event notification")
						return
					}
					logrus.Tracef("WATCH SEND id=%d, key=%s, revision=%d, events=%d, size=%d", id, string(r.Key), watch.currentRevision, len(wr.Events), wr.Size())
					shouldNotify = false
				}

			case <-watch.progressRequest:
				if shouldNotify {
					wr := &etcdserverpb.WatchResponse{
						Header:  txnHeader(watch.currentRevision),
						WatchId: id,
					}
					logrus.Tracef("WATCH SEND id=%d, key=%s, revision=%d, events=%d, size=%d", id, string(r.Key), watch.currentRevision, len(wr.Events), wr.Size())
					if err := w.server.Send(wr); err != nil {
						logrus.WithError(err).Errorf("WATCH SEND couldn't send")
						return
					}
				}
				shouldNotify = true
			}
		}
	}()
}

func (w *watcher) removeWatch(id int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.watches, id)
}

func toEvents(events ...*Event) []*mvccpb.Event {
	ret := make([]*mvccpb.Event, 0, len(events))
	for _, e := range events {
		ret = append(ret, toEvent(e))
	}
	return ret
}

func toEvent(event *Event) *mvccpb.Event {
	e := &mvccpb.Event{
		Kv:     toKV(event.KV),
		PrevKv: toKV(event.PrevKV),
	}
	if event.Delete {
		e.Type = mvccpb.DELETE
		e.Kv.Value = nil
	} else {
		e.Type = mvccpb.PUT
	}

	return e
}

func (w *watcher) Cancel(watchID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if watch, ok := w.watches[watchID]; ok {
		watch.stop()
	}
}

func (w *watcher) Close() {
	logrus.Tracef("WATCH SERVER CLOSE")
	w.mu.Lock()
	for _, watch := range w.watches {
		watch.stop()
	}
	w.mu.Unlock()
	w.wg.Wait()
}

// Progress sends a progress report if all watchers are synced.
func (w *watcher) Progress(ctx context.Context) {
	w.mu.Lock()
	defer w.mu.Unlock()

	logrus.Tracef("WATCH REQUEST PROGRESS")

	syncRevision := int64(0)
	for _, watch := range w.watches {
		if syncRevision < watch.currentRevision {
			syncRevision = watch.currentRevision
		}
	}

	unsyncedCount := 0
	for _, watch := range w.watches {
		if watch.currentRevision < syncRevision {
			unsyncedCount++
		}
	}

	if unsyncedCount == 0 {
		w.server.Send(&etcdserverpb.WatchResponse{
			Header:  txnHeader(syncRevision),
			WatchId: clientv3.InvalidWatchID,
		})
	} else {
		w.syncRevision = syncRevision
		w.unsyncedCount = unsyncedCount
	}
}

func (w *watcher) progressTimeout(ctx context.Context) error {
	logrus.Tracef("WATCH PROGRESS TICK")

	w.mu.Lock()
	defer w.mu.Unlock()

	// Send revision to all synced channels
	for _, watch := range w.watches {
		select {
		case watch.progressRequest <- struct{}{}:
		default:
		}
	}
	return nil
}
