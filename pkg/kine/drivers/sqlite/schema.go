package sqlite

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

// The database version that designates whether table migration
// from the key_value table to the Kine table has been done.
// Databases with this version should not have the key_value table
// present anymore, and unexpired rows of the key_value table with
// the latest revisions must have been recorded in the Kine table
// already

const (
	databaseSchemaMajorVersion int16 = 1
	databaseSchemaMinorVersion int16 = 0
	mask                       int32 = 0xFFFF // Mask for 2 bytes (16 bits)
)

type schemaVersion int32

var (
	databaseSchemaVersion = ToSchemaVersion(databaseSchemaMajorVersion, databaseSchemaMinorVersion)
)

func ToSchemaVersion(major int16, minor int16) schemaVersion {
	return schemaVersion(int32(major)<<16 | int32(minor))
}

func (sv schemaVersion) Major() int16 {
	// Extract the high 16 bits
	return int16((int32(sv) >> 16) & mask)
}

func (sv schemaVersion) Minor() int16 {
	// Extract the lower 16 bits
	return int16(int32(sv) & mask)
}

func (sv schemaVersion) CanMigrate(targetSV schemaVersion) (bool, error) {
	// Check wether version is the same
	if sv.Major() == targetSV.Major() && sv.Minor() == targetSV.Minor() {
		return false, nil
	}
	// Major version must be the same
	if sv.Major() != targetSV.Major() {
		return false, errors.Errorf("can not migrate between different major versions: %d to %d", sv.Major(), targetSV.Major())
	}
	// Minor version must be greater
	if sv.Minor() > targetSV.Minor() {
		return false, errors.Errorf("can not rollback to earlier minor version: %d to %d", sv.Minor(), targetSV.Minor())
	}
	return true, nil
}

// applySchemaV1 moves the schema from version 0 to version 1,
// taking into account the possible unversioned schema from
// upstream kine.
func applySchemaV1(ctx context.Context, txn *sql.Tx) error {
	if kineTableExists, err := hasTable(ctx, txn, "kine"); err != nil {
		return err
	} else if !kineTableExists {
		// In this case the schema it's empty, so it is just
		// a matter of creating the table.
		createTableSQL := `
CREATE TABLE kine
(
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	created INTEGER,
	deleted INTEGER,
	create_revision INTEGER NOT NULL,
	prev_revision INTEGER,
	lease INTEGER,
	value BLOB,
	old_value BLOB
)`

		if _, err := txn.ExecContext(ctx, createTableSQL); err != nil {
			return err
		}
	} else {
		// The kine table already exists, so this is the case of
		// the unversioned schema that includes the wrong indexes.
		if _, err := txn.ExecContext(ctx, `DROP INDEX IF EXISTS kine_name_index`); err != nil {
			return err
		}
		if _, err := txn.ExecContext(ctx, `DROP INDEX IF EXISTS kine_name_prev_revision_uindex`); err != nil {
			return err
		}
	}

	if _, err := txn.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS kine_name_index ON kine (name, id)`); err != nil {
		return err
	}

	if _, err := txn.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS kine_name_prev_revision_uindex ON kine (prev_revision, name)`); err != nil {
		return err
	}

	return nil
}

// hasTable checks if a table exists.
func hasTable(ctx context.Context, txn *sql.Tx, tableName string) (bool, error) {
	// FIXME: why we can't use `pragma_table_list()`? Is dqlite/sqlite using
	// a very old sqlite version? `pragma_free_list()` works though...
	tableListSQL := `SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`
	row := txn.QueryRowContext(ctx, tableListSQL, tableName)
	var tableCount int
	if err := row.Scan(&tableCount); err != nil {
		return false, err
	}

	return tableCount != 0, nil
}
