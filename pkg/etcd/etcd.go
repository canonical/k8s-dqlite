package etcd

import (
	"fmt"
	"os"
	"path/filepath"

	kine_tls "github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/canonical/k8s-dqlite/pkg/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type etcd struct {
	clientConfig clientv3.Config

	peerURL      string
	sentinelFile string

	config   *embed.Config
	instance *embed.Etcd

	mustStopCh chan struct{}
}

func New(storageDir string) (*etcd, error) {
	config, err := embed.ConfigFromFile(filepath.Join(storageDir, "etcd.yaml"))
	if err != nil {
		return nil, fmt.Errorf("failed to load etcd config: %w", err)
	}
	var registerConfig registerConfig
	if err := fileUnmarshal(&registerConfig, storageDir, "register.yaml"); err != nil {
		return nil, fmt.Errorf("failed to load register config: %w", err)
	}

	tlsConfig, err := kine_tls.Config{
		CAFile:   registerConfig.TrustedCAFile,
		CertFile: registerConfig.CertFile,
		KeyFile:  registerConfig.KeyFile,
	}.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client TLS config: %w", err)
	}

	if err := os.MkdirAll(config.Dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to ensure data directory: %w", err)
	}

	return &etcd{
		config: config,
		clientConfig: clientv3.Config{
			Endpoints: registerConfig.ClientURLs,
			TLS:       tlsConfig,
		},
		peerURL:      registerConfig.PeerURL,
		sentinelFile: filepath.Join(config.Dir, "sentinel"),
		mustStopCh:   make(chan struct{}, 1),
	}, nil
}

func (e *etcd) MustStop() <-chan struct{} {
	return e.mustStopCh
}

var _ server.Instance = &etcd{}
