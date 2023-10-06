//go:build !dqlite

package dqlite

import (
	"context"
	"fmt"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
)

func New(ctx context.Context, datasourceName string, tlsInfo tls.Config, acPolicyConfig generic.AdmissionControlPolicyConfig) (server.Backend, error) {
	return nil, fmt.Errorf("dqlite is not support, compile with \"-tags dqlite\"")
}
