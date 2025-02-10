package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/mattn/go-sqlite3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func NewDriver(ctx context.Context, config *DriverConfig) (*Driver, error) {
	const retryAttempts = 300

	if config == nil {
		return nil, errors.New("options cannot be nil")
	}
	if config.DB == nil {
		return nil, errors.New("db cannot be nil")
	}
	if config.ErrCode == nil {
		config.ErrCode = error.Error
	}
	if config.Retry == nil {
		config.Retry = func(err error) bool {
			if err, ok := err.(sqlite3.Error); ok {
				return err.Code == sqlite3.ErrBusy
			}
			return false
		}
	}

	for i := 0; i < retryAttempts; i++ {
		err := setup(ctx, config.DB)
		if err == nil {
			break
		}
		logrus.Errorf("failed to setup db: %v", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return &Driver{
		config: config,
	}, nil
}

// setup performs table setup, which may include creation of the Kine table if
// it doesn't already exist, migrating key_value table contents to the Kine
// table if the key_value table exists, all in a single database transaction.
// changes are rolled back if an error occurs.
func setup(ctx context.Context, db database.Interface) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Optimistically ask for the user_version without starting a transaction
	var currentSchemaVersion SchemaVersion

	row := conn.QueryRowContext(ctx, `PRAGMA user_version`)
	if err := row.Scan(&currentSchemaVersion); err != nil {
		return err
	}

	if err := currentSchemaVersion.CompatibleWith(databaseSchemaVersion); err != nil {
		return err
	}
	if currentSchemaVersion >= databaseSchemaVersion {
		return nil
	}

	txn, err := conn.BeginTx(ctx, nil)
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
