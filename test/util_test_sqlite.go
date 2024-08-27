//go:build !dqlite

package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
)

func makeEndpointConfig(_ context.Context, tb testing.TB) endpoint.Config {
	dir := tb.TempDir()

	return endpoint.Config{
		Listener:         fmt.Sprintf("unix://%s/listen.sock", dir),
		Endpoint:         fmt.Sprintf("sqlite://%s/data.db", dir),
		PollAfterTimeout: 20 * time.Second,
	}
}
