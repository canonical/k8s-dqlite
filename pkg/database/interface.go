package database

import (
	"context"
	"database/sql"
	"errors"
)

var errDBClosed = errors.New("sql: database is closed")

type Interface interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error)
	Conn(ctx context.Context) (*sql.Conn, error)
	Close() error
}

type Transaction interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt
	Commit() error
	Rollback() error
}

type Wrapped[T Transaction] interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error)
	PrepareContext(ctx context.Context, query string) (*sql.Stmt, error)
	BeginTx(ctx context.Context, opts *sql.TxOptions) (T, error)
	Conn(ctx context.Context) (*sql.Conn, error)
	Close() error
}
