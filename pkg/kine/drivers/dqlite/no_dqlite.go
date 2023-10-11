//go:build !dqlite

package dqlite

import (
	"context"
	"fmt"

	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
)

func New(ctx context.Context, datasourceName string, tlsInfo tls.Config) (server.Backend, error) {
	return nil, fmt.Errorf("dqlite is not support, compile with \"-tags dqlite\"")
}
