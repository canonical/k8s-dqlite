//go:build dqlite

package drivers_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/dqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
)

func TestDqliteCompaction(t *testing.T) {
	testCompaction(t, newDqliteBackend)
}

func BenchmarkDqliteCompaction(b *testing.B) {
	benchmarkCompaction(b, newDqliteBackend)
}

var (
	nextIdx int
)

func newDqliteBackend(ctx context.Context, tb testing.TB) (server.Backend, *generic.Generic, error) {
	nextIdx++

	dir := tb.TempDir()
	app, err := app.New(dir, app.WithAddress(fmt.Sprintf("127.0.0.1:%d", 59090+nextIdx)))
	if err != nil {
		panic(fmt.Errorf("failed to create dqlite app: %w", err))
	}
	if err := app.Ready(ctx); err != nil {
		panic(fmt.Errorf("failed to initialize dqlite: %w", err))
	}
	tb.Cleanup(func() {
		app.Close()
	})

	return dqlite.NewVariant(ctx, fmt.Sprintf("dqlite://k8s-%d?driver-name=%s", nextIdx, app.Driver()))
}
