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
		watcherGroup:   watcherGroup,
		notifyInterval: s.limited.notifyInterval,
		watches:        make(map[int64]*watch),
	}

	eg.Go(stream.ServeUpdates)
	eg.Go(stream.ServeProgress)
	eg.Go(stream.ServeRequests)

	return eg.Wait()
}

type watchStream struct {
	mu             sync.Mutex
	watcherGroup   WatcherGroup
	notifyInterval time.Duration
	revision       int64
	server         etcdserverpb.Watch_WatchServer
	watches        map[int64]*watch
}

type watch struct {
	reportProgress bool
}

func (ws *watchStream) ServeRequests() error {
	nextWatchId := int64(1)
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
			// TODO: return error on compaction error
			if err := ws.Create(nextWatchId, cr.Key, rangeEnd, cr.StartRevision); err != nil {
				return nil
			}
			nextWatchId++
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
		ws.revision = update.Revision
		if err := ws.sendUpdates(update.Updates); err != nil {
			return err
		}
	}
	return nil
}

func (ws *watchStream) sendUpdates(updates []WatcherUpdate) error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	for _, watcherUpdate := range updates {
		ws.watches[watcherUpdate.WatcherId].reportProgress = false
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

	for id, watch := range ws.watches {
		if watch.reportProgress {
			err := ws.server.Send(&etcdserverpb.WatchResponse{
				Header:  txnHeader(ws.revision),
				WatchId: id,
			})
			if err != nil {
				logrus.WithError(err).Errorf("couldn't send watch response.")
				return err
			}
		} else {
			watch.reportProgress = true
		}
	}
	return nil
}

func (ws *watchStream) Create(id int64, key, rangeEnd []byte, startRevision int64) error {
	if err := ws.watcherGroup.Watch(id, key, rangeEnd, startRevision); err != nil {
		return err
	}

	ws.mu.Lock()
	defer ws.mu.Unlock()
	ws.watches[id] = &watch{
		reportProgress: true,
	}
	return nil
}

func (ws *watchStream) Close(id int64) {
	ws.watcherGroup.Unwatch(id)

	ws.mu.Lock()
	defer ws.mu.Unlock()
	delete(ws.watches, id)
}

func (ws *watchStream) RequestProgress() error {
	ws.mu.Lock()
	defer ws.mu.Unlock()

	return ws.server.Send(&etcdserverpb.WatchResponse{
		Header:  txnHeader(ws.revision),
		WatchId: clientv3.InvalidWatchID,
	})
}
