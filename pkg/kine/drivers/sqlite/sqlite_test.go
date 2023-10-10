package sqlite_test

import (
	"context"
	"database/sql"
	"os"
	"path"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
)

func setupV0(db *sql.DB) error {

	// Create the very old key_value table
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS kine
(
	id INTEGER primary key autoincrement,
	name INTEGER,
	created INTEGER,
	deleted INTEGER,
	prev_revision INTEGER,
	ttl INTEGER,
	value BLOB,
	old_value BLOB
)`); err != nil {
		return err
	}

	return nil
}

func TestMigration(t *testing.T) {
	const driver = "sqlite3"

	folder, err := os.MkdirTemp("", "kine_test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(folder)

	dbPath := path.Join(folder, "db.sqlite")
	db, err := sql.Open(driver, dbPath)
	defer db.Close()
	if err != nil {
		t.Fatal(err)
	}

	setupV0(db)

	ctx := context.Background()
	if _, err := sqlite.New(ctx, dbPath, generic.AdmissionControlPolicyConfig{}); err != nil {
		t.Fatal(err)
	}

	row := db.QueryRow(`
SELECT COUNT(*)
FROM sqlite_master
WHERE type = 'index'
	AND tbl_name = 'kine'
	AND name IN ('kine_name_index', 'kine_name_prev_revision_uindex')`)

	var indexes int
	if err := row.Scan(&indexes); err != nil {
		t.Error(err)
	}

	if indexes != 2 {
		t.Errorf("Expected 2 indexes, got %d", indexes)
	}
}
