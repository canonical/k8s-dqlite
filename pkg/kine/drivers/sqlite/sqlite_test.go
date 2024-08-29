package sqlite_test

import (
	"context"
	"database/sql"
	"path"
	"testing"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/sirupsen/logrus"
)

func init() {
	logrus.SetLevel(logrus.ErrorLevel)
}

func setupV0(db *sql.DB) error {
	// Create the very old key_value table
	if _, err := db.Exec(`
CREATE TABLE IF NOT EXISTS kine
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
)`); err != nil {
		return err
	}

	return nil
}

func TestMigration(t *testing.T) {
	const driver = "sqlite3"

	folder := t.TempDir()
	dbPath := path.Join(folder, "db.sqlite")
	db, err := sql.Open(driver, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if err := setupV0(db); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	connPoolConfig := generic.ConnectionPoolConfig{
		MaxIdle:     5,
		MaxOpen:     5,
		MaxLifetime: 60 * time.Second,
		MaxIdleTime: 0 * time.Second,
	}
	if _, err := sqlite.New(ctx, dbPath, connPoolConfig); err != nil {
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
