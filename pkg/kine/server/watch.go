package server

import (
	"context"
	"errors"

	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	watchID int64
)

func (s *KVServerBridge) Watch(ws etcdserverpb.Watch_WatchServer) error {
	watcher := s.limited.backend.MakeWatcher()
	if err := watcher.Start(ws.Context()); err != nil {
		return err
	}
	defer watcher.Stop()

	for {
		msg, err := ws.Recv()
		if err != nil {
			return err
		}

		if create := msg.GetCreateRequest(); create != nil {
			listener := watcher.Watch(string(create.Key), string(create.RangeEnd), create.StartRevision)

			if err := ws.Send(&etcdserverpb.WatchResponse{
				Header:  &etcdserverpb.ResponseHeader{},
				Created: true,
				WatchId: listener.Id(),
			}); err != nil {
				listener.Close()
				if closeErr := listener.Err(); closeErr != nil {
					return errors.Join(err, closeErr)
				}
				return err
			}

			go func() {
				for events := range listener.Events() {
					if err := ws.Send(&etcdserverpb.WatchResponse{
						Header:  txnHeader(events[len(events)-1].KV.ModRevision),
						WatchId: listener.Id(),
						Events:  toEvents(events),
					}); err != nil {
						listener.Close()
						logrus.WithError(err).Error("cannot sent event notification")
						return
					}
				}

				err := listener.Err()
				if err == nil {
					return
				}
				if err == context.Canceled || err == context.DeadlineExceeded {
					return
				}
				if err == ErrCompacted {
					// TODO: send compacted error
				}
				logrus.WithError(err).Debug("error streaming events")
				if sendErr := ws.Send(&etcdserverpb.WatchResponse{
					Header:       &etcdserverpb.ResponseHeader{},
					Canceled:     true,
					CancelReason: err.Error(),
					WatchId:      watchID,
				}); sendErr != nil {
					logrus.WithError(err).Debug("cannot close watcher")
				}
			}()
		} else if cancel := msg.GetCancelRequest(); cancel != nil {
			logrus.Debugf("WATCH CANCEL REQ id=%d", cancel.GetWatchId())
			if l := watcher.Listener(cancel.GetWatchId()); l != nil {
				l.Close()
			}
		}
	}
}

func toEvents(events []*Event) []*mvccpb.Event {
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
	} else {
		e.Type = mvccpb.PUT
	}

	return e
}
