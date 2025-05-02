package limited

import (
	"context"
	"time"

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

type Backend interface {
	Start(ctx context.Context) error
	Stop() error
	Create(ctx context.Context, key, value []byte, lease int64) (int64, bool, error)
	Delete(ctx context.Context, key []byte, revision int64) (int64, bool, error)
	List(ctx context.Context, key, rangeEnd []byte, limit, revision int64) (int64, []*KeyValue, error)
	Count(ctx context.Context, key, rangeEnd []byte, revision int64) (int64, int64, error)
	Update(ctx context.Context, key, value []byte, revision, lease int64) (int64, bool, error)
	WatcherGroup(ctx context.Context) (WatcherGroup, error)
	DbSize(ctx context.Context) (int64, error)
	GetCompactRevision(ctx context.Context) (int64, int64, error)
	DoCompact(ctx context.Context) error
	Close() error
}

type Config struct {
	// CompactInterval is interval between database compactions performed by k8s-dqlite.
	CompactInterval time.Duration

	// PollInterval is the event poll interval used by k8s-dqlite.
	PollInterval time.Duration

	// WatchQueryTimeout is the timeout on the after query in the poll loop.
	WatchQueryTimeout time.Duration
}

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
