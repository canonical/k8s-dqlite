package prepared

import (
	"context"
	"database/sql"
)

type Tx struct {
	db *DB
	tx *sql.Tx
}

func (tx *Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := tx.db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtContext(ctx, stmt).ExecContext(ctx, args...)
}

func (tx *Tx) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	stmt, err := tx.db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}

	return tx.tx.StmtContext(ctx, stmt).QueryContext(ctx, args...)
}

func (tx *Tx) Commit() error   { return tx.tx.Commit() }
func (tx *Tx) Rollback() error { return tx.tx.Rollback() }
