package drivers_test

import (
	"context"
	"path"
	"testing"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
)

func TestSQLiteCompaction(t *testing.T) {
	testCompaction(t, newSqliteBackend)
}

func BenchmarkSQLiteCompaction(b *testing.B) {
	benchmarkCompaction(b, newSqliteBackend)
}

func newSqliteBackend(ctx context.Context, tb testing.TB) (server.Backend, *generic.Generic, error) {
	dir := tb.TempDir()
	dataSource := path.Join(dir, "k8s.sqlite")
	return sqlite.NewVariant(ctx, "sqlite3", dataSource)
}
