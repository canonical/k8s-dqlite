package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

var (
	ErrKeyExists = rpctypes.ErrGRPCDuplicateKey
	ErrCompacted = rpctypes.ErrGRPCCompacted
)

type Backend interface {
	Start(ctx context.Context) error
	Wait()
	Get(ctx context.Context, key, rangeEnd string, limit, revision int64) (int64, *KeyValue, error)
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, error)
	Delete(ctx context.Context, key string, revision int64) (int64, *KeyValue, bool, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*KeyValue, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *KeyValue, bool, error)
	Watch(ctx context.Context, key string, revision int64) <-chan []*Event
	DbSize(ctx context.Context) (int64, error)
	DoCompact(ctx context.Context) error
	MakeWatcher() Watcher
}

type KeyValue struct {
	Key            string
	CreateRevision int64
	ModRevision    int64
	Value          []byte
	Lease          int64
}

type Event struct {
	Delete bool
	Create bool
	KV     *KeyValue
	PrevKV *KeyValue
}

type Watcher interface {
	Start(ctx context.Context) error
	Listener(id int64) Listener
	Watch(rangeStart, rangeEnd string, revision int64) Listener
	Stop()
}

type Listener interface {
	Events() <-chan []*Event
	Id() int64
	RangeStart() string
	RangeEnd() string
	Close()
	Err() error
}
