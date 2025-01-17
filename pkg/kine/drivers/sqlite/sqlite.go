package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func New(ctx context.Context, driverName, dataSourceName string, connectionPoolConfig *generic.ConnectionPoolConfig) (*generic.Generic, error) {
	const retryAttempts = 300

	driver, err := generic.Open(ctx, driverName, dataSourceName, connectionPoolConfig)
	if err != nil {
		return nil, err
	}
	for i := 0; i < retryAttempts; i++ {
		err = func() error {
			conn, err := driver.DB.Conn(ctx)
			if err != nil {
				return err
			}
			defer conn.Close()
			return setup(ctx, conn)
		}()
		if err == nil {
			break
		}
		logrus.Errorf("failed to setup db: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
		time.Sleep(time.Second)
	}

	if driverName == "sqlite3" {
		driver.Retry = func(err error) bool {
			if err, ok := err.(sqlite3.Error); ok {
				return err.Code == sqlite3.ErrBusy
			}
			return false
		}
	}

	return driver, nil
}

// setup performs table setup, which may include creation of the Kine table if
// it doesn't already exist, migrating key_value table contents to the Kine
// table if the key_value table exists, all in a single database transaction.
// changes are rolled back if an error occurs.
func setup(ctx context.Context, db *sql.Conn) error {
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
	default:
		return nil
	}

	setUserVersionSQL := fmt.Sprintf(`PRAGMA user_version = %d`, databaseSchemaVersion)
	if _, err := txn.ExecContext(ctx, setUserVersionSQL); err != nil {
		return err
	}

	return nil
}
