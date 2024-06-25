package sqlite_test

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/logstructured/sqllog"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	. "github.com/onsi/gomega"
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
	if _, err := sqlite.New(ctx, dbPath); err != nil {
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

func TestCompaction(t *testing.T) {
	ctx := context.Background()

	t.Run("SmallDatabaseDeleteEntry", func(t *testing.T) {
		g := NewWithT(t)
		backend, dialect, err := newBackend(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer dialect.DB.Close()

		addEntries(ctx, dialect, 2)
		deleteEntries(ctx, dialect, 1)

		initialSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		err = backend.DoCompact(ctx)
		g.Expect(err).To(BeNil())

		finalSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		// Expecting no compaction
		g.Expect(finalSize).To(BeNumerically("==", initialSize))
	})

	t.Run("LargeDatabaseDeleteFivePercent", func(t *testing.T) {
		g := NewWithT(t)
		backend, dialect, err := newBackend(ctx, t)
		if err != nil {
			t.Fatal(err)
		}
		defer dialect.DB.Close()

		addEntries(ctx, dialect, 10_000)
		deleteEntries(ctx, dialect, 500)

		initialSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		err = backend.DoCompact(ctx)
		g.Expect(err).To(BeNil())

		finalSize, err := backend.DbSize(ctx)
		g.Expect(err).To(BeNil())

		// Expecting compaction
		g.Expect(finalSize).To(BeNumerically("<", initialSize))
	})
}

func BenchmarkCompaction(b *testing.B) {
	b.StopTimer()
	ctx := context.Background()

	backend, dialect, err := newBackend(ctx, b)
	if err != nil {
		b.Fatal(err)
	}
	defer dialect.DB.Close()

	// Make sure there's enough rows deleted to have
	// b.N rows to compact.
	delCount := b.N + sqllog.SupersededCount

	// Also, make sure there's some uncollectable data, so
	// that the deleted rows are about 5% of the total.
	addCount := delCount * 20

	if err := addEntries(ctx, dialect, addCount); err != nil {
		b.Fatal(err)
	}
	if err := deleteEntries(ctx, dialect, delCount); err != nil {
		b.Fatal(err)
	}

	b.StartTimer()
	err = backend.DoCompact(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

func newBackend(ctx context.Context, tb testing.TB) (server.Backend, *generic.Generic, error) {
	dir := tb.TempDir()
	dataSource := path.Join(dir, "k8s.sqlite")
	return sqlite.NewVariant(ctx, "sqlite3", dataSource)
}

func addEntries(ctx context.Context, dialect *generic.Generic, count int) error {
	_, err := dialect.DB.ExecContext(ctx, `
WITH RECURSIVE gen_id AS(
	SELECT COALESCE(MAX(id), 0)+1 AS id FROM kine

	UNION ALL

	SELECT id + 1
	FROM gen_id
	WHERE id + 1 < ?
)
INSERT INTO kine
SELECT id, 'testkey-'||id, 1, 0, id, 0, 0, 'value-'||id, NULL FROM gen_id;
	`, count)
	return err
}

func deleteEntries(ctx context.Context, dialect *generic.Generic, count int) error {
	_, err := dialect.DB.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO kine(
	name, created, deleted, create_revision, prev_revision, lease, value, old_value
)
SELECT kv.name, 0, 1, kv.create_revision, kv.id, 0, kv.value, kv.value
FROM kine AS kv
JOIN (
	SELECT MAX(mkv.id) as id
	FROM kine mkv
	WHERE  'testkey-' <= mkv.name AND mkv.name < 'testkey.'
	GROUP BY mkv.name
) maxkv ON maxkv.id = kv.id
WHERE kv.deleted = 0
ORDER BY kv.name
LIMIT %d`, count))
	return err
}
