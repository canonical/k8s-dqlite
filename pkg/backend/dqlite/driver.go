package dqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/canonical/go-dqlite/v3"
	"github.com/canonical/go-dqlite/v3/app"
	"github.com/canonical/go-dqlite/v3/driver"
	"github.com/canonical/k8s-dqlite/pkg/backend/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/mattn/go-sqlite3"
	"github.com/sirupsen/logrus"
)

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

	return &Driver{
		Driver: drv,
		app:    config.App,
	}, nil
}

func (d *Driver) Compact(ctx context.Context, revision int64) (err error) {
	// Skip the compaction if we're not the leader.
	isLeader, err := d.isLocalNodeLeader(ctx)
	if err != nil {
		logrus.WithError(err).Warning("couldn't determine whether the local node is the leader, allowing the compaction to proceed")
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
