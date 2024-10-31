package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const otelName = "prepared"

var otelTracer trace.Tracer

func init() {
	otelTracer = otel.Tracer(otelName)
}

type preparedDb struct {
	underlying Interface
	mu         sync.RWMutex
	store      map[string]*sql.Stmt
}

func NewPrepared(db Interface) Interface {
	return &preparedDb{
		underlying: db,
		store:      make(map[string]*sql.Stmt),
	}
}

func (db *preparedDb) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
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

func (db *preparedDb) QueryContext(ctx context.Context, query string, args ...any) (rows *sql.Rows, err error) {
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

func (db *preparedDb) PrepareContext(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	return db.underlying.PrepareContext(ctx, query)
}

func (db *preparedDb) prepare(ctx context.Context, query string) (stmt *sql.Stmt, err error) {
	ctx, span := otelTracer.Start(ctx, fmt.Sprintf("%s.prepare", otelName))
	defer func() {
		span.RecordError(err)
		span.End()
	}()
	span.SetAttributes(attribute.String("query", query))

	db.mu.RLock()
	stmt = db.store[query]
	db.mu.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.underlying == nil {
		return nil, errDBClosed
	}

	// Check again if the query was prepared during locking
	stmt = db.store[query]
	if stmt != nil {
		return stmt, nil
	}

	prepared, err := db.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	db.store[query] = prepared
	return prepared, nil
}

func (db *preparedDb) Conn(ctx context.Context) (*sql.Conn, error) {
	return db.underlying.Conn(ctx)
}

func (db *preparedDb) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	tx, err := db.underlying.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &preparedTx{
		Transaction: tx,
		db:          db,
	}, nil
}

func (db *preparedDb) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	errs := []error{}
	for _, stmt := range db.store {
		if err := stmt.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	db.store = nil

	if err := db.underlying.Close(); err != nil {
		errs = append(errs, err)
	}
	db.underlying = nil

	return errors.Join(errs...)
}

type preparedTx struct {
	Transaction
	db *preparedDb
}

func (tx *preparedTx) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	stmt, err := tx.db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return tx.StmtContext(ctx, stmt), nil
}

func (tx *preparedTx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (tx *preparedTx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return stmt.QueryContext(ctx, args...)
}
