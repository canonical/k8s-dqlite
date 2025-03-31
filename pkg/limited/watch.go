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
		ws.revision = update.Revision()
		if err := ws.sendUpdates(update.Watchers()); err != nil {
			return err
		}
	}
	return nil
}

func (ws *watchStream) sendUpdates(updates []WatcherUpdate) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, watcherUpdate := range updates {
		ws.sendProgressReport[watcherUpdate.WatcherId] = false
		err := ws.server.Send(&etcdserverpb.WatchResponse{
			Header:  txnHeader(ws.revision),
			WatchId: watcherUpdate.WatcherId,
			Events:  watcherUpdate.Events,
		})
		if err != nil {
			logrus.WithError(err).Errorf("couldn't send watch response.")
			return err
		}
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

	for id, sendProgressReport := range ws.sendProgressReport {
		if sendProgressReport {
			err := ws.server.Send(&etcdserverpb.WatchResponse{
				Header:  txnHeader(ws.revision),
				WatchId: id,
			})
			if err != nil {
				logrus.WithError(err).Errorf("couldn't send watch response.")
				return err
			}
		} else {
			sendProgressReport = true
		}
	}
	return nil
}

func (ws *watchStream) Create(id int64, key, rangeEnd []byte, startRevision int64) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if err := ws.watcherGroup.Watch(id, key, rangeEnd, startRevision); err != nil {
		if compactErr, ok := err.(*CompactedError); ok {
			return ws.server.Send(&etcdserverpb.WatchResponse{
				Header:       txnHeader(compactErr.CurrentRevision),
				WatchId:      clientv3.InvalidWatchID,
				Canceled:     true,
				Created:      true,
				CancelReason: err.Error(),
			})
		} else {
			// Return an error message that the api-server understands:
			logrus.WithError(err).Errorf("WATCH Failed to create watcher: %v", err)
			err = ErrGRPCUnhealthy
			return ws.server.Send(&etcdserverpb.WatchResponse{
				Header:       txnHeader(ws.revision),
				Canceled:     true,
				CancelReason: err.Error(),
			})
		}
	}
	ws.sendProgressReport[id] = true

	return ws.server.Send(&etcdserverpb.WatchResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		WatchId: id,
		Created: true,
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

	return ws.server.Send(&etcdserverpb.WatchResponse{
		Header:  txnHeader(ws.revision),
		WatchId: clientv3.InvalidWatchID,
	})
}
