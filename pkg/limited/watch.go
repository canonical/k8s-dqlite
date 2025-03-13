package limited

import (
	"context"
	"strings"
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
		server:   ws,
		backend:  s.limited.backend,
		watches:  map[int64]func(){},
		progress: map[int64]chan<- struct{}{},
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
			w.Cancel(cr.WatchId, nil)
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
				if err := w.ProgressIfSynced(ctx); err != nil {
					logrus.Errorf("Failed to send progress notification: %v", err)
				}
			}
		}
	}()
}

type watcher struct {
	sync.Mutex

	wg       sync.WaitGroup
	backend  Backend
	server   etcdserverpb.Watch_WatchServer
	watches  map[int64]func()
	progress map[int64]chan<- struct{}
}

func (w *watcher) Start(ctx context.Context, r *etcdserverpb.WatchCreateRequest) {
	if r.WatchId != clientv3.AutoWatchID {
		logrus.Errorf("WATCH START id=%d ignoring request with client-provided id", r.WatchId)
		return
	}

	w.Lock()
	defer w.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	id := atomic.AddInt64(&watchID, 1)

	w.watches[id] = cancel
	w.wg.Add(1)

	startRevision := r.StartRevision

	var progressCh chan struct{}
	if r.ProgressNotify {
		progressCh = make(chan struct{})
		w.progress[id] = progressCh
	}

	logrus.Tracef("WATCH START id=%d, key=%s, revision=%d, progressNotify=%v", id, string(r.Key), startRevision, r.ProgressNotify)

	go func() {
		defer w.wg.Done()
		if err := w.server.Send(&etcdserverpb.WatchResponse{
			Header:  &etcdserverpb.ResponseHeader{},
			Created: true,
			WatchId: id,
		}); err != nil {
			w.Cancel(id, err)
			return
		}

		wn, err := w.backend.Watch(ctx, id, r.Key, startRevision, r.RangeEnd)
		if err != nil {
			logrus.Errorf("Failed to start watch: %v", err)
			w.Cancel(id, err)
			return
		}

		trace := logrus.IsLevelEnabled(logrus.TraceLevel)
		outer := true
		watchCurrentRev := startRevision //TODO  maybe compact rev?
		key := string(r.Key)
		for outer {
			var reads int
			var events []*Event
			var revision int64

			// Wait for events or progress notifications
			select {
			case events = <-wn.Events:
				if len(events) > 0 {
					watchCurrentRev = wn.CurrentRevision
				}
				filteredEventList := filterEvents(events, key, startRevision)
				if len(filteredEventList) == 0 {
					continue
				}

				// We received events; batch any additional queued events
				reads++
				inner := true
				for inner {
					select {
					case e, ok := <-wn.Events:
						reads++
						events = append(events, e...)
						if !ok {
							// channel is closed, break out of both loops
							inner = false
							outer = false
						}
					default:
						// No more events in the queue, we can exit the inner loop
						inner = false
					}
				}
			case <-progressCh:
				// Received progress update without events
				revision = watchCurrentRev
			}

			// Determine the highest revision among the collected events
			if len(events) > 0 {
				revision = events[len(events)-1].KV.ModRevision
				if trace {
					for _, event := range events {
						logrus.Tracef("WATCH READ id=%d, key=%s, revision=%d", id, event.KV.Key, event.KV.ModRevision)
					}
				}
			}

			// Send response, even if this is a progress-only response and no events occured
			if revision >= startRevision {
				wr := &etcdserverpb.WatchResponse{
					Header:  txnHeader(revision),
					WatchId: id,
					Events:  toEvents(events...),
				}
				logrus.Tracef("WATCH SEND id=%d, key=%s, revision=%d, events=%d, size=%d, reads=%d", id, string(r.Key), revision, len(wr.Events), wr.Size(), reads)
				if err := w.server.Send(wr); err != nil {
					w.Cancel(id, err)
				}
			}
		}

		logrus.Debugf("WATCH CLOSE id=%d, key=%s", id, string(r.Key))
	}()
}

func filterEvents(events []*Event, key string, startRevision int64) []*Event {
	filteredEventList := make([]*Event, 0, len(events))
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

func (w *watcher) Cancel(watchID int64, err error) {
	w.Lock()
	if progressCh, ok := w.progress[watchID]; ok {
		close(progressCh)
		delete(w.progress, watchID)
	}
	if cancel, ok := w.watches[watchID]; ok {
		cancel()
		delete(w.watches, watchID)
	}
	w.Unlock()

	revision := int64(0)
	compactRev := int64(0)
	reason := ""
	if err != nil {
		reason = err.Error()
		if err == ErrCompacted {
			// the requested start revision is compacted.  Pass the current and and compact
			// revision to the client via the cancel response, along with the correct error message.
			compactRev, revision, err = w.backend.GetCompactRevision(w.server.Context())
			if err != nil {
				logrus.Errorf("Failed to get compact and current revision for cancel response: %v", err)
			}
		}
	}
	logrus.Tracef("WATCH CANCEL id=%d, reason=%s, compactRev=%d", watchID, reason, compactRev)

	serr := w.server.Send(&etcdserverpb.WatchResponse{
		Header:          txnHeader(revision),
		Canceled:        true,
		CancelReason:    reason,
		WatchId:         watchID,
		CompactRevision: compactRev,
	})
	if serr != nil && err != nil && !clientv3.IsConnCanceled(serr) {
		logrus.Errorf("WATCH Failed to send cancel response for watchID %d: %v", watchID, serr)
	}
}

func (w *watcher) Close() {
	logrus.Tracef("WATCH SERVER CLOSE")
	w.Lock()
	for id, progressCh := range w.progress {
		close(progressCh)
		delete(w.progress, id)
	}
	for id, cancel := range w.watches {
		cancel()
		delete(w.watches, id)
	}
	w.Unlock()
	w.wg.Wait()
}

// Progress sends a progress report if all watchers are synced.
// Ref: https://github.com/etcd-io/etcd/blob/v3.5.11/server/mvcc/watchable_store.go#L500-L504
func (w *watcher) Progress(ctx context.Context) {
	w.Lock()
	defer w.Unlock()

	logrus.Tracef("WATCH REQUEST PROGRESS")

	// All synced watchers will be blocked in the outer loop and able to receive on the progress channel.
	// If any cannot be sent to, then it is not synced and has pending events to be sent.
	// Send revision 0, as we don't actually want the watchers to send a progress response if they do receive.
	for id, progressCh := range w.progress {
		select {
		case progressCh <- struct{}{}:
		default:
			logrus.Tracef("WATCH SEND PROGRESS FAILED NOT SYNCED id=%d ", id)
			return
		}
	}

	// If all watchers are synced, send a broadcast progress notification with the latest revision.
	id := int64(clientv3.InvalidWatchID)
	rev, err := w.backend.CurrentRevision(ctx)
	if err != nil {
		logrus.Errorf("Failed to get current revision for ProgressNotify: %v", err)
		return
	}

	logrus.Tracef("WATCH SEND PROGRESS id=%d, revision=%d", id, rev)
	go w.server.Send(&etcdserverpb.WatchResponse{Header: txnHeader(rev), WatchId: id})
}

// ProgressIfSynced sends a progress report on any channels that are synced and blocked on the outer loop
func (w *watcher) ProgressIfSynced(ctx context.Context) error {
	logrus.Tracef("WATCH PROGRESS TICK")

	w.Lock()
	defer w.Unlock()

	// Send revision to all synced channels
	for _, progressCh := range w.progress {
		select {
		case progressCh <- struct{}{}:
		default:
		}
	}
	return nil
}
