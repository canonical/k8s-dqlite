package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	kine_tls "github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/sirupsen/logrus"
)

// Server is the main k8s-dqlite server.
type Server struct {
	// app is the dqlite application driving the server.
	app *app.App

	// kineConfig is the configuration to use for starting kine against the dqlite application.
	kineConfig endpoint.Config
}

// expectedFilesDuringInitialization is a list of files that are allowed to exist when initializing the dqlite node.
// This is to prevent corruption that could occur by starting a new dqlite node when data already exists in the directory.
var expectedFilesDuringInitialization = map[string]struct{}{
	"cluster.crt":    {},
	"cluster.key":    {},
	"init.yaml":      {},
	"failure-domain": {},
	"tuning.yaml":    {},
}

// New creates a new instance of Server based on configuration.
func New(dir string, listen string, enableTLS bool, diskMode bool, clientSessionCacheSize uint) (*Server, error) {
	var (
		options         []app.Option
		kineConfig      endpoint.Config
		compactInterval *time.Duration
		pollInterval    *time.Duration
	)

	if mustInit, err := fileExists(dir, "init.yaml"); err != nil {
		return nil, fmt.Errorf("failed to check for init.yaml: %w", err)
	} else if mustInit {
		// handle init.yaml
		var init InitConfiguration

		// ensure we do not have existing state
		files, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("failed to list storage dir contents: %w", err)
		}
		for _, file := range files {
			if _, expected := expectedFilesDuringInitialization[file.Name()]; !expected {
				return nil, fmt.Errorf("data directory seems to have existing state '%s'. please remove the file and restart", file.Name())
			}
		}

		if err := fileUnmarshal(&init, dir, "init.yaml"); err != nil {
			return nil, fmt.Errorf("failed to read init.yaml: %w", err)
		}
		if init.Address == "" {
			return nil, fmt.Errorf("empty address in init.yaml")
		}

		// delete init.yaml from disk
		if err := os.Remove(filepath.Join(dir, "init.yaml")); err != nil {
			return nil, fmt.Errorf("failed to remove init.yaml after init: %w", err)
		}

		logrus.WithFields(logrus.Fields{"address": init.Address, "cluster": init.Cluster}).Print("Will initialize dqlite node")

		options = append(options, app.WithAddress(init.Address), app.WithCluster(init.Cluster))
	} else if mustUpdate, err := fileExists(dir, "update.yaml"); err != nil {
		return nil, fmt.Errorf("failed to check for update.yaml: %w", err)
	} else if mustUpdate {
		// handle update.yaml
		var (
			info   client.NodeInfo
			update UpdateConfiguration
		)

		// load info.yaml and update.yaml
		if err := fileUnmarshal(&update, dir, "update.yaml"); err != nil {
			return nil, fmt.Errorf("failed to read update.yaml: %w", err)
		}
		if update.Address == "" {
			return nil, fmt.Errorf("empty address in update.yaml")
		}
		if err := fileUnmarshal(&info, dir, "info.yaml"); err != nil {
			return nil, fmt.Errorf("failed to read info.yaml: %w", err)
		}

		logrus.WithFields(logrus.Fields{"old_address": info.Address, "new_address": update.Address}).Print("Will update address of dqlite node")

		// update node address
		info.Address = update.Address

		// reconfigure dqlite membership
		if err := dqlite.ReconfigureMembership(dir, []dqlite.NodeInfo{info}); err != nil {
			return nil, fmt.Errorf("failed to reconfigure dqlite membership for new address: %w", err)
		}

		// update info.yaml and cluster.yaml on disk
		if err := fileMarshal(info, dir, "info.yaml"); err != nil {
			return nil, fmt.Errorf("failed to write new address in info.yaml: %w", err)
		}
		if err := fileMarshal([]dqlite.NodeInfo{info}, dir, "cluster.yaml"); err != nil {
			return nil, fmt.Errorf("failed to write new address in cluster.yaml: %w", err)
		}

		// delete update.yaml from disk
		if err := os.Remove(filepath.Join(dir, "update.yaml")); err != nil {
			return nil, fmt.Errorf("failed to remove update.yaml after dqlite address update: %w", err)
		}
	}

	// handle failure-domain
	var failureDomain uint64
	if exists, err := fileExists(dir, "failure-domain"); err != nil {
		return nil, fmt.Errorf("failed to check failure-domain: %w", err)
	} else if exists {
		if err := fileUnmarshal(&failureDomain, dir, "failure-domain"); err != nil {
			return nil, fmt.Errorf("failed to parse failure-domain from file: %w", err)
		}
	}
	logrus.WithField("failure-domain", failureDomain).Print("Configure dqlite failure domain")
	options = append(options, app.WithFailureDomain(failureDomain))

	// handle TLS
	if enableTLS {
		logrus.Print("Enable TLS")
		crtFile := filepath.Join(dir, "cluster.crt")
		keyFile := filepath.Join(dir, "cluster.key")

		keypair, err := tls.LoadX509KeyPair(crtFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load keypair from cluster.crt and cluster.key: %w", err)
		}
		pool := x509.NewCertPool()
		for idx, cert := range keypair.Certificate {
			if !pool.AppendCertsFromPEM(cert) {
				return nil, fmt.Errorf("failed to add certificate %d to pool", idx)
			}
		}

		listen, dial := app.SimpleTLSConfig(keypair, pool)

		if clientSessionCacheSize > 0 {
			logrus.WithField("cache_size", clientSessionCacheSize).Print("Use TLS ClientSessionCache")
			dial.ClientSessionCache = tls.NewLRUClientSessionCache(int(clientSessionCacheSize))
		} else {
			logrus.Print("Disable TLS ClientSessionCache")
			dial.ClientSessionCache = nil
		}

		kineConfig.Config = kine_tls.Config{
			CertFile: crtFile,
			KeyFile:  keyFile,
		}

		options = append(options, app.WithTLS(listen, dial))
	}

	// handle tuning parameters
	if exists, err := fileExists(dir, "tuning.yaml"); err != nil {
		return nil, fmt.Errorf("failed to check for tuning.yaml: %w", err)
	} else if exists {
		var tuning TuningConfiguration
		if err := fileUnmarshal(&tuning, dir, "tuning.yaml"); err != nil {
			return nil, fmt.Errorf("failed to read tuning.yaml: %w", err)
		}

		if v := tuning.Snapshot; v != nil {
			logrus.WithFields(logrus.Fields{"threshold": v.Threshold, "trailing": v.Trailing}).Print("Configure dqlite raft snapshot parameters")
			options = append(options, app.WithSnapshotParams(dqlite.SnapshotParams{
				Threshold: v.Threshold,
				Trailing:  v.Trailing,
			}))
		}

		if v := tuning.NetworkLatency; v != nil {
			logrus.WithField("latency", *v).Print("Configure dqlite average one-way network latency")
			options = append(options, app.WithNetworkLatency(*v))
		}

		// these are set in the kine endpoint config below
		compactInterval = tuning.KineCompactInterval
		pollInterval = tuning.KinePollInterval
	}

	if diskMode {
		logrus.Print("Enable dqlite disk mode operation")
		options = append(options, app.WithDiskMode(true))

		// TODO: remove after disk mode is stable
		logrus.Warn("dqlite disk mode operation is current at an experimental state and MUST NOT be used in production. Expect data loss.")
	}

	app, err := app.New(dir, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create dqlite app: %w", err)
	}

	params := make(url.Values)
	params["driver-name"] = []string{app.Driver()}
	if v := compactInterval; v != nil {
		params["compact-interval"] = []string{fmt.Sprintf("%v", *v)}
	}
	if v := pollInterval; v != nil {
		params["poll-interval"] = []string{fmt.Sprintf("%v", *v)}
	}

	kineConfig.Listener = listen
	kineConfig.Endpoint = fmt.Sprintf("dqlite://k8s?%s", params.Encode())

	return &Server{
		app:        app,
		kineConfig: kineConfig,
	}, nil
}

// Start the dqlite node and the kine machinery.
func (s *Server) Start(ctx context.Context) error {
	if err := s.app.Ready(ctx); err != nil {
		return fmt.Errorf("failed to start dqlite app: %w", err)
	}
	logrus.WithFields(logrus.Fields{"id": s.app.ID(), "address": s.app.Address()}).Print("Started dqlite")

	logrus.WithField("config", s.kineConfig).Debug("Starting kine")
	if _, err := endpoint.Listen(ctx, s.kineConfig); err != nil {
		return fmt.Errorf("failed to start kine: %w", err)
	}
	logrus.WithFields(logrus.Fields{"address": s.kineConfig.Listener, "database": s.kineConfig.Endpoint}).Print("Started kine")
	return nil
}

// Shutdown cleans up any resources and attempts to hand-over and shutdown the dqlite application.
func (s *Server) Shutdown(ctx context.Context) error {
	logrus.Debug("Handing over dqlite leadership")
	if err := s.app.Handover(ctx); err != nil {
		logrus.WithError(err).Errorf("Failed to handover dqlite")
	}
	logrus.Debug("Closing dqlite application")
	if err := s.app.Close(); err != nil {
		return fmt.Errorf("failed to close dqlite app: %w", err)
	}
	return nil
}
