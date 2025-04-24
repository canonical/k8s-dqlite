package dqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/canonical/go-dqlite/v3"
	"github.com/canonical/go-dqlite/v3/app"
	"github.com/canonical/go-dqlite/v3/driver"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/drivers/sqlite"
	"github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

const ()

func init() {
	// We assume SQLite will be used multi-threaded
	if err := dqlite.ConfigMultiThread(); err != nil {
		panic(fmt.Errorf("failed to set dqlite multithreaded mode: %v", err))
	}
}

type Driver struct {
	*sqlite.Driver

	app *app.App
}

type DriverConfig struct {
	DB  database.Interface
	App *app.App
}

func NewDriver(ctx context.Context, config *DriverConfig) (*Driver, error) {
	logrus.Printf("New driver for dqlite")

	if config.App == nil {
		return nil, fmt.Errorf("no go-dqlite app specified")
	}

	drv, err := sqlite.NewDriver(ctx, &sqlite.DriverConfig{
		DB:    config.DB,
		Retry: dqliteRetry,
	})
	if err != nil {
		return nil, err
	}

	if err := migrate(ctx, config.DB); err != nil {
		return nil, fmt.Errorf("failed to migrate DB from sqlite: %w", err)
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
	var e driver.Error

	if errors.As(err, &e) {
		return e.Code == driver.ErrBusy
	}

	if errors.Is(err, sqlite3.ErrLocked) || errors.Is(err, sqlite3.ErrBusy) {
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
