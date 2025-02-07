package dqlite

import (
	"context"
	"database/sql"
	"os"
	"strings"

	"github.com/canonical/go-dqlite/v2"
	"github.com/canonical/go-dqlite/v2/driver"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/sqllog"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func init() {
	// We assume SQLite will be used multi-threaded
	if err := dqlite.ConfigMultiThread(); err != nil {
		panic(errors.Wrap(err, "failed to set dqlite multithreaded mode"))
	}
}

func NewDriver(ctx context.Context, driverName, datasourceName string, connectionPoolConfig *generic.ConnectionPoolConfig) (sqllog.Driver, error) {
	logrus.Printf("New kine for dqlite")

	driver_, err := sqlite.NewDriver(ctx, driverName, datasourceName, connectionPoolConfig)
	if err != nil {
		return nil, errors.Wrap(err, "sqlite client")
	}

	conn, err := driver_.DB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if err := migrate(ctx, conn); err != nil {
		return nil, errors.Wrap(err, "failed to migrate DB from sqlite")
	}
	driver_.LockWrites = true
	driver_.Retry = func(err error) bool {
		// get the inner-most error if possible
		err = errors.Cause(err)

		if err, ok := err.(driver.Error); ok {
			return err.Code == driver.ErrBusy
		}

		if err == sqlite3.ErrLocked || err == sqlite3.ErrBusy {
			return true
		}

		if strings.Contains(err.Error(), "database is locked") {
			return true
		}

		if strings.Contains(err.Error(), "cannot start a transaction within a transaction") {
			return true
		}

		if strings.Contains(err.Error(), "bad connection") {
			return true
		}

		if strings.Contains(err.Error(), "checkpoint in progress") {
			return true
		}

		return false
	}

	return driver_, nil
}

func migrate(ctx context.Context, newDB *sql.Conn) (exitErr error) {
	row := newDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM kine")
	var count int64
	if err := row.Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	if _, err := os.Stat("./db/state.db"); err != nil {
		return nil
	}

	oldDB, err := sql.Open("sqlite3", "./db/state.db")
	if err != nil {
		return nil
	}
	defer oldDB.Close()

	oldData, err := oldDB.QueryContext(ctx, "SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value FROM kine")
	if err != nil {
		logrus.Errorf("failed to find old data to migrate: %v", err)
		return nil
	}
	defer oldData.Close()

	tx, err := newDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		if exitErr == nil {
			exitErr = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	for oldData.Next() {
		row := []interface{}{
			new(int),
			new(string),
			new(int),
			new(int),
			new(int),
			new(int),
			new(int),
			new([]byte),
			new([]byte),
		}
		if err := oldData.Scan(row...); err != nil {
			return err
		}

		if _, err := newDB.ExecContext(ctx, "INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value) values(?, ?, ?, ?, ?, ?, ?, ?, ?)",
			row...); err != nil {
			return err
		}
	}

	if err := oldData.Err(); err != nil {
		return err
	}

	return nil
}
