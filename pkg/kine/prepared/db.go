package prepared

import (
	"context"
	"database/sql"
	"errors"
	"sync"
)

type DB struct {
	underlying *sql.DB
	mu         sync.RWMutex
	store      map[string]*sql.Stmt
}

func New(db *sql.DB) *DB {
	return &DB{
		underlying: db,
		store:      make(map[string]*sql.Stmt),
	}
}

func (db *DB) Underlying() *sql.DB { return db.underlying }

func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	stmt, err := db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.ExecContext(ctx, args...)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	stmt, err := db.prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.QueryContext(ctx, args...)
}

func (db *DB) Close() error {
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

func (db *DB) prepare(ctx context.Context, query string) (*sql.Stmt, error) {
	db.mu.RLock()
	stmt := db.store[query]
	db.mu.RUnlock()
	if stmt != nil {
		return stmt, nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.underlying == nil {
		return nil, errors.New("database is closed")
	}

	// Check again if the query was prepared during locking
	stmt = db.store[query]
	if stmt != nil {
		return stmt, nil
	}

	prepared, err := db.underlying.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	db.store[query] = prepared
	return prepared, nil
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error) {
	tx, err := db.underlying.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &Tx{
		db: db,
		tx: tx,
	}, nil
}
