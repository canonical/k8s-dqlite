package dqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/canonical/go-dqlite/v2"
	"github.com/canonical/go-dqlite/v2/app"
	"github.com/canonical/go-dqlite/v2/driver"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
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

type Driver struct {
	*sqlite.Driver

	app *app.App
}

type DriverConfig struct {
	DB      database.Interface
	ErrCode func(error) string
	App     *app.App
}

func NewDriver(ctx context.Context, config *DriverConfig) (*Driver, error) {
	logrus.Printf("New driver for dqlite")

	if config.App == nil {
		return nil, fmt.Errorf("no go-dqlite app specified")
	}

	drv, err := sqlite.NewDriver(ctx, &sqlite.DriverConfig{
		DB:         config.DB,
		LockWrites: true,
		Retry:      dqliteRetry,
	})
	if err != nil {
		return nil, err
	}

	if err := migrate(ctx, config.DB); err != nil {
		return nil, errors.Wrap(err, "failed to migrate DB from sqlite")
	}

	return &Driver{
		Driver: drv,
		app:    config.App,
	}, nil
}

func (d *Driver) Compact(ctx context.Context, revision int64) (err error) {
	// Skip the compaction if we're not the leader.
	isLeader, err := d.isLocalNodeLeader(ctx)
	if err != nil {
		logrus.WithError(err).Warning("Couldn't determine whether the local node is the leader, allowing the compaction to proceed")
	} else if !isLeader {
		logrus.Trace("skipping compaction on follower node")
		return nil
	}
	return d.Driver.Compact(ctx, revision)
}

func dqliteRetry(err error) bool {
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

func (d *Driver) isLocalNodeLeader(ctx context.Context) (bool, error) {
	client, err := d.app.Client(ctx)
	if err != nil {
		return false, fmt.Errorf("couldn't obtain dqlite client: %w", err)
	}
	defer client.Close()

	leader, err := client.Leader(ctx)
	if err != nil {
		return false, fmt.Errorf("couldn't obtain dqlite leader info: %w", err)
	}

	return d.app.ID() == leader.ID, nil
}

// FIXME this might be very slow.
func migrate(ctx context.Context, db database.Interface) (exitErr error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if migrate, err := shouldMigrate(ctx, conn); err != nil {
		return err
	} else if !migrate {
		return nil
	}

	oldDB, err := sql.Open("sqlite3", "./db/state.db")
	if err != nil {
		return nil
	}
	defer oldDB.Close()

	oldData, err := oldDB.QueryContext(ctx, `
SELECT id, name, created, deleted, create_revision, prev_revision, lease, value, old_value
FROM kine
ORDER BY id ASC`)
	if err != nil {
		logrus.Errorf("failed to find old data to migrate: %v", err)
		return nil
	}
	defer oldData.Close()

	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO kine(id, name, created, deleted, create_revision, prev_revision, lease, value, old_value)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}

	oldRow := []interface{}{
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
	for oldData.Next() {
		if err := oldData.Scan(oldRow...); err != nil {
			return err
		}

		if _, err := stmt.ExecContext(ctx, oldRow...); err != nil {
			return err
		}
	}
	if err := oldData.Err(); err != nil {
		return err
	}

	return tx.Commit()
}

func shouldMigrate(ctx context.Context, conn *sql.Conn) (bool, error) {
	if _, err := os.Stat("./db/state.db"); err != nil {
		return false, nil
	}

	row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM kine")
	var count int64
	if err := row.Scan(&count); err != nil {
		return false, err
	}
	return count == 0, nil
}
