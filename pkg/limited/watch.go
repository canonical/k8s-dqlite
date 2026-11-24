package limited

import (
	"sync"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// explicit interface check
var _ etcdserverpb.WatchServer = (*KVServerBridge)(nil)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	eg, ctx := errgroup.WithContext(ws.Context())
	eg.SetLimit(3)

	watcherGroup, err := s.limited.backend.WatcherGroup(ctx)
	if err != nil {
		return err
	}

	stream := &watchStream{
		watcherGroup:       watcherGroup,
		notifyInterval:     s.limited.notifyInterval,
		server:             ws,
		sendProgressReport: make(map[int64]bool),
	}

	eg.Go(stream.ServeUpdates)
	eg.Go(stream.ServeProgress)
	eg.Go(stream.ServeRequests)

	return eg.Wait()
}

type watchStream struct {
	mu                 sync.Mutex
	watcherGroup       WatcherGroup
	notifyInterval     time.Duration
	revision           int64
	server             etcdserverpb.Watch_WatchServer
	sendProgressReport map[int64]bool
	limited            LimitedServer

	// progressFloor is the highest revision of catch-up events sent by Create.
	// Updates queued before them may hold older events for other watchers, so
	// progress below it is deferred until ServeUpdates reaches it.
	progressFloor    int64
	deferredProgress bool
}

func (ws *watchStream) ServeRequests() error {
	nextWatcherId := int64(1)
	for {
		msg, err := ws.server.Recv()
		if err != nil {
			return err
		}

		if cr := msg.GetCreateRequest(); cr != nil {
			if cr.WatchId != clientv3.AutoWatchID {
				return unsupported("WatchId")
			}
			rangeEnd := cr.RangeEnd
			if len(rangeEnd) == 0 {
				rangeEnd = append(cr.Key, 0)
			}
			if err := ws.Create(nextWatcherId, cr.Key, rangeEnd, cr.StartRevision); err != nil {
				return err
			}
			nextWatcherId++
		}

		if cr := msg.GetCancelRequest(); cr != nil {
			logrus.Tracef("WATCH CANCEL REQ id=%d", cr.WatchId)
			ws.Close(cr.WatchId)
		}

		if pr := msg.GetProgressRequest(); pr != nil {
			if err := ws.RequestProgress(); err != nil {
				logrus.WithError(err).Errorf("couldn't send progress response")
				return err
			}
		}
	}
}

func (ws *watchStream) ServeUpdates() error {
	for update := range ws.watcherGroup.Updates() {
		if err := ws.sendUpdates(update.Revision(), update.Watchers()); err != nil {
			return err
		}
	}
	return nil
}

func (ws *watchStream) sendUpdates(revision int64, updates []WatcherUpdate) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	ws.revision = revision
	for _, watcherUpdate := range updates {
		ws.sendProgressReport[watcherUpdate.WatcherId] = false
		err := ws.server.Send(&etcdserverpb.WatchResponse{
			Header:  txnHeader(ws.revision),
			WatchId: watcherUpdate.WatcherId,
			Events:  watcherUpdate.Events,
		})
		if err != nil {
			logrus.WithError(err).Errorf("couldn't send watch response")
			return err
		}
	}
	if ws.deferredProgress && ws.canSendProgress() {
		ws.deferredProgress = false
		return ws.sendStreamProgress()
	}
	return nil
}

func (ws *watchStream) ServeProgress() error {
	ticker := time.NewTicker(ws.notifyInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := ws.sendProgress(); err != nil {
				return err
			}
		case <-ws.server.Context().Done():
			return nil
		}
	}
}

func (ws *watchStream) sendProgress() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if !ws.canSendProgress() {
		return nil
	}
	for id, sendProgressReport := range ws.sendProgressReport {
		if sendProgressReport {
			err := ws.server.Send(&etcdserverpb.WatchResponse{
				Header:  txnHeader(ws.revision),
				WatchId: id,
			})
			if err != nil {
				logrus.WithError(err).Errorf("couldn't send watch response")
				return err
			}
		} else {
			ws.sendProgressReport[id] = true
		}
	}
	return nil
}

func (ws *watchStream) Create(id int64, key, rangeEnd []byte, startRevision int64) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	revision, events, err := ws.watcherGroup.Watch(id, key, rangeEnd, startRevision)
	if err != nil {
		var reason string
		header := txnHeader(ws.revision)
		if compactErr, ok := err.(*CompactedError); ok {
			header = txnHeader(compactErr.CurrentRevision)
			reason = err.Error()
		} else {
			// Return an error message that the api-server understands:
			logrus.WithError(err).Errorf("WATCH Failed to create watcher: %v", err)
			reason = ErrGRPCUnhealthy.Error()
		}
		// clientv3 matches a create reply only by Created; without it the
		// create stays pending and blocks every later watch on the stream.
		return ws.server.Send(&etcdserverpb.WatchResponse{
			Header:       header,
			WatchId:      clientv3.InvalidWatchID,
			Canceled:     true,
			Created:      true,
			CancelReason: reason,
		})
	}
	ws.sendProgressReport[id] = true

	if err := ws.server.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		WatchId: id,
		Created: true,
	}); err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	// Send catch-up events now, so that no progress notification can get
	// ahead of them.
	ws.sendProgressReport[id] = false
	ws.progressFloor = max(ws.progressFloor, revision)
	return ws.server.Send(&etcdserverpb.WatchResponse{
		Header:  txnHeader(revision),
		WatchId: id,
		Events:  events,
	})
}

func (ws *watchStream) Close(id int64) {
	ws.watcherGroup.Unwatch(id)

	ws.mu.Lock()
	defer ws.mu.Unlock()
	delete(ws.sendProgressReport, id)
}

func (ws *watchStream) RequestProgress() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if !ws.canSendProgress() {
		ws.deferredProgress = true
		return nil
	}
	return ws.sendStreamProgress()
}

// canSendProgress reports whether all events up to ws.revision were sent to
// every watcher of the stream.
func (ws *watchStream) canSendProgress() bool {
	return ws.revision >= ws.progressFloor
}

func (ws *watchStream) sendStreamProgress() error {
	return ws.server.Send(&etcdserverpb.WatchResponse{
		Header:  txnHeader(ws.revision),
		WatchId: clientv3.InvalidWatchID,
	})
}
