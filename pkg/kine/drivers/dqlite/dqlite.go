//go:build dqlite

package dqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/driver"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
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

type opts struct {
	dsn        string
	driverName string // If not empty, use a pre-registered dqlite driver

	compactInterval time.Duration
	pollInterval    time.Duration
}

func New(ctx context.Context, datasourceName string, tlsInfo tls.Config) (server.Backend, error) {
	logrus.Printf("New kine for dqlite")
	opts, err := parseOpts(datasourceName)
	if err != nil {
		return nil, err
	}

	logrus.Printf("DriverName is %s.", opts.driverName)
	if opts.driverName == "" {
		return nil, fmt.Errorf("required option 'driver-name' not set in connection string")
	}

	backend, generic, err := sqlite.NewVariant(ctx, opts.driverName, opts.dsn)
	if err != nil {
		return nil, errors.Wrap(err, "sqlite client")
	}
	if err := migrate(ctx, generic.DB); err != nil {
		return nil, errors.Wrap(err, "failed to migrate DB from sqlite")
	}

	generic.LockWrites = true
	generic.Retry = func(err error) bool {
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
	generic.TranslateErr = func(err error) error {
		if strings.Contains(err.Error(), "UNIQUE constraint") {
			return server.ErrKeyExists
		}
		return err
	}

	generic.CompactInterval = opts.compactInterval
	generic.PollInterval = opts.pollInterval
	return backend, nil
}

func migrate(ctx context.Context, newDB *sql.DB) (exitErr error) {
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

func parseOpts(dsn string) (opts, error) {
	result := opts{
		dsn: dsn,
	}

	parts := strings.SplitN(dsn, "?", 2)
	if len(parts) == 1 {
		return result, nil
	}

	values, err := url.ParseQuery(parts[1])
	if err != nil {
		return result, err
	}

	for k, vs := range values {
		if len(vs) == 0 {
			continue
		}

		switch k {
		case "driver-name":
			result.driverName = vs[0]
		case "compact-interval":
			d, err := time.ParseDuration(vs[0])
			if err != nil {
				return opts{}, fmt.Errorf("failed to parse compact-interval duration value %q: %w", vs[0], err)
			}
			result.compactInterval = d
		case "poll-interval":
			d, err := time.ParseDuration(vs[0])
			if err != nil {
				return opts{}, fmt.Errorf("failed to parse poll-interval duration value %q: %w", vs[0], err)
			}
			result.pollInterval = d
		default:
			return opts{}, fmt.Errorf("unknown option %s=%v", k, vs)
		}
		delete(values, k)
	}

	if len(values) == 0 {
		result.dsn = parts[0]
	} else {
		result.dsn = fmt.Sprintf("%s?%s", parts[0], values.Encode())
	}

	return result, nil
}
