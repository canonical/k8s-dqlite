package backend

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

type Driver interface {
	List(ctx context.Context, key, rangeEnd []byte, limit, revision int64) (*sql.Rows, error)
	ListTTL(ctx context.Context, revision int64) (*sql.Rows, error)
	Count(ctx context.Context, key, rangeEnd []byte, revision int64) (int64, error)
	CurrentRevision(ctx context.Context) (int64, error)
	AfterPrefix(ctx context.Context, key, rangeEnd []byte, fromRevision, toRevision int64) (*sql.Rows, error)
	After(ctx context.Context, startRevision, limit int64) (*sql.Rows, error)
	Create(ctx context.Context, key []byte, value []byte, lease int64) (int64, bool, error)
	Update(ctx context.Context, key []byte, value []byte, prevRev, lease int64) (int64, bool, error)
	Delete(ctx context.Context, key []byte, revision int64) (int64, bool, error)
	DeleteRevision(ctx context.Context, revision int64) error
	GetCompactRevision(ctx context.Context) (int64, int64, error)
	Compact(ctx context.Context, revision int64) error
	GetSize(ctx context.Context) (int64, error)
	Close() error
}

type Backend struct {
	mu sync.Mutex

	Config Config

	Driver Driver

	stop    func()
	started bool

	WatcherGroups map[*WatcherGroup]*WatcherGroup
	pollRevision  int64

	Notify chan int64
	wg     sync.WaitGroup
}

type Config struct {
	// CompactInterval is interval between database compactions performed by k8s-dqlite.
	CompactInterval time.Duration

	// PollInterval is the event poll interval used by k8s-dqlite.
	PollInterval time.Duration

	// WatchQueryTimeout is the timeout on the after query in the poll loop.
	WatchQueryTimeout time.Duration
}
