package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

var (
	ErrKeyExists     = rpctypes.ErrGRPCDuplicateKey
	ErrCompacted     = rpctypes.ErrGRPCCompacted
	ErrGRPCUnhealthy = rpctypes.ErrGRPCUnhealthy
)

type Backend interface {
	Start(ctx context.Context) error
	Stop() error
	Create(ctx context.Context, key string, value []byte, lease int64) (int64, bool, error)
	Delete(ctx context.Context, key string, revision int64) (int64, bool, error)
	List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, []*KeyValue, error)
	Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error)
	Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, bool, error)
	Watch(ctx context.Context, key string, revision int64) (<-chan []*Event, error)
	DbSize(ctx context.Context) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	GetCompactRevision(ctx context.Context) (int64, int64, error)
	DoCompact(ctx context.Context) error
	Close() error
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
