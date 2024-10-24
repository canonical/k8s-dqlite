package database

import (
	"context"
	"database/sql"
)

type wrappedTransaction sql.Tx

func (w *wrappedTransaction) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
	tx := (*sql.Tx)(w)
	return tx.ExecContext(ctx, query, args...)
}

func (w *wrappedTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	tx := (*sql.Tx)(w)
	return tx.PrepareContext(ctx, query)
}

func (w *wrappedTransaction) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	tx := (*sql.Tx)(w)
	return tx.QueryContext(ctx, query, args...)
}

func (w *wrappedTransaction) StmtContext(ctx context.Context, stmt *sql.Stmt) *sql.Stmt {
	tx := (*sql.Tx)(w)
	return tx.StmtContext(ctx, stmt)
}

func (w *wrappedTransaction) Rollback() error {
	tx := (*sql.Tx)(w)
	return tx.Rollback()
}

func (w *wrappedTransaction) Commit() error {
	tx := (*sql.Tx)(w)
	return tx.Commit()
}

type wrappedDb sql.DB

// Wrap returns a wrapped DB that does nothing
// different from the original db, but implements
// database.Interface.
func Wrap(db *sql.DB) Interface {
	return (*wrappedDb)(db)
}

func (w *wrappedDb) BeginTx(ctx context.Context, opts *sql.TxOptions) (Transaction, error) {
	db := (*sql.DB)(w)
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return (*wrappedTransaction)(tx), nil
}

func (w *wrappedDb) Conn(ctx context.Context) (*sql.Conn, error) {
	db := (*sql.DB)(w)
	return db.Conn(ctx)
}

func (w *wrappedDb) ExecContext(ctx context.Context, query string, args ...any) (result sql.Result, err error) {
	db := (*sql.DB)(w)
	return db.ExecContext(ctx, query, args...)
}

func (w *wrappedDb) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	db := (*sql.DB)(w)
	return db.PrepareContext(ctx, query)
}

func (w *wrappedDb) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	db := (*sql.DB)(w)
	return db.QueryContext(ctx, query, args...)
}

func (w *wrappedDb) Close() error {
	db := (*sql.DB)(w)
	return db.Close()
}
