package sqllog

import (
	"context"
	"database/sql"
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
