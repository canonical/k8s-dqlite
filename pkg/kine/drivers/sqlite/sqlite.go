package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/logstructured"
	"github.com/canonical/k8s-dqlite/pkg/kine/logstructured/sqllog"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type opts struct {
	dsn        string
	driverName string // If not empty, use a pre-registered dqlite driver

	compactInterval time.Duration
	pollInterval    time.Duration

	admissionControlPolicy                      string
	admissionControlPolicyLimitMaxConcurrentTxn int64
	admissionControlOnlyWriteQueries            bool
}

func New(ctx context.Context, dataSourceName string) (server.Backend, error) {
	backend, _, err := NewVariant(ctx, "sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}

	return backend, err
}

func NewVariant(ctx context.Context, driverName, dataSourceName string) (server.Backend, *generic.Generic, error) {
	const retryAttempts = 300

	opts, err := parseOpts(dataSourceName)
	if err != nil {
		return nil, nil, err
	}

	if driverName == "" {
		// Check if driver name is set via query parameters
		if opts.driverName == "" {
			return nil, nil, fmt.Errorf("required option 'driver-name' not set in connection string")
		}
		driverName = opts.driverName
	}
	logrus.Printf("DriverName is %s.", driverName)

	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&cache=shared"
	}

	dialect, err := generic.Open(ctx, driverName, opts.dsn, "?", false)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < retryAttempts; i++ {
		err = setup(ctx, dialect.DB.Underlying())
		if err == nil {
			break
		}
		logrus.Errorf("failed to setup db: %v", err)
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(time.Second):
		}
		time.Sleep(time.Second)
	}

	dialect.LastInsertID = true
	dialect.TranslateErr = func(err error) error {
		if err, ok := err.(sqlite3.Error); ok && err.ExtendedCode == sqlite3.ErrConstraintUnique {
			return server.ErrKeyExists
		}
		return err
	}
	dialect.GetSizeSQL = `SELECT (page_count - freelist_count) * page_size FROM pragma_page_count(), pragma_page_size(), pragma_freelist_count()`

	dialect.CompactInterval = opts.compactInterval
	dialect.PollInterval = opts.pollInterval
	dialect.AdmissionControlPolicy = generic.NewAdmissionControlPolicy(
		opts.admissionControlPolicy,
		opts.admissionControlOnlyWriteQueries,
		opts.admissionControlPolicyLimitMaxConcurrentTxn,
	)

	if driverName == "sqlite3" {
		dialect.Retry = func(err error) bool {
			if err, ok := err.(sqlite3.Error); ok {
				return err.Code == sqlite3.ErrBusy
			}
			return false
		}
	}

	return logstructured.New(sqllog.New(dialect)), dialect, nil
}

// setup performs table setup, which may include creation of the Kine table if
// it doesn't already exist, migrating key_value table contents to the Kine
// table if the key_value table exists, all in a single database transaction.
// changes are rolled back if an error occurs.
func setup(ctx context.Context, db *sql.DB) error {
	// Optimistically ask for the user_version without starting a transaction
	var currentSchemaVersion SchemaVersion

	row := db.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&currentSchemaVersion); err != nil {
		return err
	}

	if err := currentSchemaVersion.CompatibleWith(databaseSchemaVersion); err != nil {
		return err
	}
	if currentSchemaVersion >= databaseSchemaVersion {
		return nil
	}

	txn, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer txn.Rollback()

	if err := migrate(ctx, txn); err != nil {
		return errors.Wrap(err, "migration failed")
	}

	return txn.Commit()
}

// migrate tries to migrate from a version of the database
// to the target one.
func migrate(ctx context.Context, txn *sql.Tx) error {
	var currentSchemaVersion SchemaVersion

	row := txn.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&currentSchemaVersion); err != nil {
		return err
	}

	if err := currentSchemaVersion.CompatibleWith(databaseSchemaVersion); err != nil {
		return err
	}
	if currentSchemaVersion >= databaseSchemaVersion {
		return nil
	}

	switch currentSchemaVersion {
	case NewSchemaVersion(0, 0):
		if err := applySchemaV0_1(ctx, txn); err != nil {
			return err
		}
		fallthrough
	case NewSchemaVersion(0, 1):
		if err := applySchemaV0_2(ctx, txn); err != nil {
			return err
		}
	default:
		return nil
	}

	setUserVersionSQL := fmt.Sprintf(`PRAGMA user_version = %d`, databaseSchemaVersion)
	if _, err := txn.ExecContext(ctx, setUserVersionSQL); err != nil {
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
		case "admission-control-policy":
			result.admissionControlPolicy = vs[0]
		case "admission-control-policy-limit-max-concurrent-txn":
			d, err := strconv.ParseInt(vs[0], 10, 64)
			if err != nil {
				return opts{}, fmt.Errorf("failed to parse max-concurrent-txn value %q: %w", vs[0], err)
			}
			result.admissionControlPolicyLimitMaxConcurrentTxn = d
		case "admission-control-only-write-queries":
			d, err := strconv.ParseBool(vs[0])
			if err != nil {
				return opts{}, fmt.Errorf("failed to parse admission-control-only-writes value %q: %w", vs[0], err)
			}
			result.admissionControlOnlyWriteQueries = d
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
