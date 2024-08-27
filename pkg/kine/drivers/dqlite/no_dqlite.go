//go:build !dqlite

package dqlite

import (
	"context"
	"fmt"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
)

func New(ctx context.Context, datasourceName string, tlsInfo tls.Config, pollAfterTimeout time.Duration) (server.Backend, error) {
	return nil, fmt.Errorf("dqlite is not support, compile with \"-tags dqlite\"")
}
