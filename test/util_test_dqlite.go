//go:build dqlite

package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
)

var (
	nextIdx int
)

func makeEndpointConfig(ctx context.Context, tb testing.TB) endpoint.Config {
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

	return endpoint.Config{
		Listener: fmt.Sprintf("unix://%s/listen.sock", dir),
		Endpoint: fmt.Sprintf("dqlite://k8s-%d?peer=%d:%s&driver-name=%s", nextIdx, app.ID(), app.Address(), app.Driver()),
	}
}
