package limited

import (
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

var (
	ErrCompacted     = rpctypes.ErrGRPCCompacted
	ErrGRPCUnhealthy = rpctypes.ErrGRPCUnhealthy
)

type CompactedError struct {
	CompactRevision, CurrentRevision int64
}

func (e *CompactedError) Error() string { return ErrCompacted.Error() }

func (e *CompactedError) Is(target error) bool { return target == ErrCompacted }

type WatcherGroup interface {
	// Watch will add a watcher to the group. If startRevision is not 0, the first notification
	// containing an update for this watcher will also contain all events from startRevision
	// up to that notification.
	Watch(watcherId int64, key, rangeEnd []byte, startRevision int64) error
	Unwatch(watcherId int64)
	Updates() <-chan WatcherGroupUpdate
}

type WatcherGroupUpdate interface {
	Revision() int64
	Watchers() []WatcherUpdate
}

type WatcherUpdate struct {
	WatcherId int64
	Events    []*Event
}

type KeyValue = mvccpb.KeyValue
type Event = mvccpb.Event
