package embedded

import (
	"fmt"
	"os"
	"path/filepath"

	kine_tls "github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/canonical/k8s-dqlite/pkg/server"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/server/v3/embed"
)

type embedded struct {
	clientConfig clientv3.Config

	peerURL      string
	sentinelFile string

	config   *embed.Config
	instance *embed.Etcd

	mustStopCh chan struct{}
}

func New(storageDir string) (*embedded, error) {
	config, err := embed.ConfigFromFile(filepath.Join(storageDir, "embedded.yaml"))
	if err != nil {
		return nil, fmt.Errorf("failed to load embedded config: %w", err)
	}
	var clusterConfig clusterConfig
	if err := fileUnmarshal(&clusterConfig, storageDir, "config.yaml"); err != nil {
		return nil, fmt.Errorf("failed to load cluster config: %w", err)
	}

	config.PeerTLSInfo.TrustedCAFile = clusterConfig.PeerCAFile
	config.PeerTLSInfo.CertFile = clusterConfig.PeerCertFile
	config.PeerTLSInfo.KeyFile = clusterConfig.PeerKeyFile

	config.ClientTLSInfo.TrustedCAFile = clusterConfig.CAFile
	config.ClientTLSInfo.CertFile = clusterConfig.CertFile
	config.ClientTLSInfo.KeyFile = clusterConfig.KeyFile

	tlsConfig, err := kine_tls.Config{
		CAFile:   clusterConfig.CAFile,
		CertFile: clusterConfig.CertFile,
		KeyFile:  clusterConfig.KeyFile,
	}.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client TLS config: %w", err)
	}

	if err := os.MkdirAll(config.Dir, 0700); err != nil {
		return nil, fmt.Errorf("failed to ensure data directory: %w", err)
	}

	return &embedded{
		config: config,
		clientConfig: clientv3.Config{
			Endpoints: clusterConfig.ClientURLs,
			TLS:       tlsConfig,
		},
		peerURL:      clusterConfig.PeerURL,
		sentinelFile: filepath.Join(config.Dir, "sentinel"),
		mustStopCh:   make(chan struct{}, 1),
	}, nil
}

func (e *embedded) MustStop() <-chan struct{} {
	return e.mustStopCh
}

var _ server.Instance = &embedded{}
