package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

// The database version that designates whether table migration
// from the key_value table to the Kine table has been done.
// Databases with this version should not have the key_value table
// present anymore, and unexpired rows of the key_value table with
// the latest revisions must have been recorded in the Kine table
// already

type SchemaVersion int32

var (
	databaseSchemaVersion  = NewSchemaVersion(1, 0)
	ErrIncompatibleVersion = fmt.Errorf("incompatible schema version")
)

func NewSchemaVersion(major int16, minor int16) SchemaVersion {
	return SchemaVersion(int32(major)<<16 | int32(minor))
}

func (sv SchemaVersion) Major() int16 {
	// Extract the high 16 bits
	return int16((int32(sv) >> 16))
}

func (sv SchemaVersion) Minor() int16 {
	// Extract the lower 16 bits
	return int16(sv)
}

func (sv SchemaVersion) CompatibleWith(targetSV SchemaVersion) error {
	// Major version must be the same unless the schema version is 0.0
	if sv.Major() == 0 && sv.Minor() == 0 {
		return nil
	}
	if sv.Major() != targetSV.Major() {
		return ErrIncompatibleVersion
	}
	return nil
}

// applySchemaV0_1 moves the schema from version 0 to version 1,
// taking into account the possible unversioned schema from
// upstream kine.
func applySchemaV0_1(ctx context.Context, txn *sql.Tx) error {
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

// applySchemaV1_0 moves the schema from major version 0 to version 1.
func applySchemaV1_0(ctx context.Context, txn *sql.Tx) error {
	// Legacy nodes (v1..1.12 and older) panic if they try to migrate to any
	// new version, so we need a bump in the major version.
	// See https://github.com/canonical/k8s-dqlite/blob/v1.1.12/pkg/kine/drivers/sqlite/sqlite.go#L169
	if _, err := txn.ExecContext(ctx, `DROP INDEX IF EXISTS kine_name_index`); err != nil {
		return err
	}

	if _, err := txn.ExecContext(ctx, `DROP INDEX IF EXISTS kine_name_prev_revision_uindex`); err != nil {
		return err
	}

	if _, err := txn.ExecContext(ctx, `CREATE UNIQUE INDEX IF NOT EXISTS k8s_dqlite_name_del_index ON kine (name ASC, id DESC, deleted)`); err != nil {
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
