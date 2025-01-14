package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const otelName = "prepared"

var otelTracer trace.Tracer

func init() {
	otelTracer = otel.Tracer(otelName)
}

type preparedDb[T Transaction] struct {
	underlying Wrapped[T]
	mu         sync.RWMutex
	cache      map[string]*sql.Stmt
}

// NewPrepared creates a new Interface that wraps the given database and
// uses a prepare cache to reduce the number of prepare calls. The cache
// is only used when calling ExecContext and QueryContext methods on the
// main instance or in a transaction.
func NewPrepared[T Transaction](db Wrapped[T]) Interface {
	return &preparedDb[T]{
		underlying: db,
		cache:      make(map[string]*sql.Stmt),
	}
}

func (db *preparedDb[T]) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
	ctx, span := otelTracer.Start(ctx, "DB.ExecContext")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	stmt, err := db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (db *preparedDb[T]) QueryContext(ctx context.Context, query string, args ...any) (rows *sql.Rows, err error) {
	ctx, span := otelTracer.Start(ctx, "DB.QueryContext")
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	stmt, err := db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (db *preparedDb[T]) PrepareContext(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	return db.underlying.PrepareContext(ctx, query)
}

func (db *preparedDb[T]) prepare(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.prepare", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()

	db.mu.RLock()
	stmt = db.cache[query]
	db.mu.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.underlying == nil {
		return nil, errDBClosed
	}

	// Given that some time has passed since the unlock of the read lock, and the lock of the
	// write lock, another goroutine might have already prepared this query, so we should check
	// again to avoid preparing the same query twice.
	stmt = db.cache[query]
	if stmt != nil {
		return stmt, nil
	}

	prepared, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	db.cache[query] = prepared
	return prepared, nil
}

func (db *preparedDb[T]) Conn(ctx context.Context) (*sql.Conn, error) {
	return db.underlying.Conn(ctx)
}

func (db *preparedDb[T]) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	tx, err := db.underlying.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &preparedTx[T]{
		Transaction: tx,
		db:          db,
	}, nil
}

func (db *preparedDb[T]) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	errs := []error{}
	for _, stmt := range db.cache {
		if err := stmt.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	db.cache = nil

	if err := db.underlying.Close(); err != nil {
		errs = append(errs, err)
	}
	db.underlying = nil

	return errors.Join(errs...)
}

type preparedTx[T Transaction] struct {
	Transaction
	db *preparedDb[T]
}

func (tx *preparedTx[T]) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	stmt, err := tx.db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.StmtContext(ctx, stmt), nil
}

func (tx *preparedTx[T]) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (tx *preparedTx[T]) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryContext(ctx, args...)
}
