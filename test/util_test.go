package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/canonical/go-dqlite/v2/app"
	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// newKine spins up a new instance of kine.
//
// newKine will create a sqlite or dqlite endpoint based on the provided backendType.
// Custom endpoint query parameters can be configured with the `qs` parameter (e.g. "admission-control-policy=limit")
//
// newKine will make the test fail on error
//
// newKine will return a Client as well as a configured etcd client for the kine instance
func newKine(ctx context.Context, tb testing.TB, backendType string, qs ...string) *clientv3.Client {
	var endpointConfig endpoint.Config
	switch backendType {
	case endpoint.SQLiteBackend:
		endpointConfig = startSqlite(ctx, tb)
	case endpoint.DQLiteBackend:
		endpointConfig = startDqlite(ctx, tb)
	default:
		tb.Fatalf("Testing %s backend not supported", backendType)
	}

	if !strings.Contains(endpointConfig.Endpoint, "?") {
		endpointConfig.Endpoint += "?"
	}
	for _, v := range qs {
		endpointConfig.Endpoint = fmt.Sprintf("%s&%s", endpointConfig.Endpoint, v)
	}
	config, backend, err := endpoint.ListenAndReturnBackend(ctx, endpointConfig)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		backend.Wait()
	})

	tlsConfig, err := config.TLSConfig.ClientConfig()
	if err != nil {
		tb.Fatal(err)
	}
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpointConfig.Listener},
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return client
}

func startSqlite(ctx context.Context, tb testing.TB) endpoint.Config {
	dir := tb.TempDir()
	return endpoint.Config{
		Listener: fmt.Sprintf("unix://%s/listen.sock", dir),
		Endpoint: fmt.Sprintf("sqlite://%s/data.db", dir),
	}
}

var nextPort = 59090

func startDqlite(ctx context.Context, tb testing.TB) endpoint.Config {
	nextPort++
	dir := tb.TempDir()

	app, err := app.New(dir, app.WithAddress(fmt.Sprintf("127.0.0.1:%d", nextPort)))
	if err != nil {
		tb.Fatalf("failed to create dqlite app: %v", err)
	}
	if err := app.Ready(ctx); err != nil {
		tb.Fatalf("failed to initialize dqlite: %v", err)
	}
	tb.Cleanup(func() {
		app.Close()
	})

	return endpoint.Config{
		Listener: fmt.Sprintf("unix://%s/listen.sock", dir),
		Endpoint: fmt.Sprintf("dqlite://k8s?driver-name=%s", app.Driver()),
	}
}
