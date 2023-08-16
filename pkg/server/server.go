package server

import (
	"context"
	gotls "crypto/tls"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/k8s-dqlite/pkg/kine/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/canonical/k8s-dqlite/pkg/server/config"
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

// Server sets up a single dqlite node and serves the cluster management API.
type Server struct {
	dir        string // Data directory
	address    string // Network address
	app        *app.App
	cancelKine context.CancelFunc
}

var (
	defaultKineEp = "tcp://127.0.0.1:12379"
)

func New(dir string, listen string, enableTLS bool, diskMode bool, clientSessionCacheSize uint) (*Server, error) {
	// Check if we're initializing a new node (i.e. there's an init.yaml).
	// dir: the directory where data will be stored as well as where the init.yaml
	//       and certificates should be found
	// listen: kine listen endpoint could be a socket ("unix://<path>")
	//         or network ep ("tcp://127.0.0.1:12345")
	// enableTLS: true if we should enable tls communication
	cfg, err := config.Load(dir)
	if err != nil {
		return nil, err
	}

	if cfg.Update != nil {
		info := client.NodeInfo{}
		path := filepath.Join(dir, "info.yaml")
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(data, &info); err != nil {
			return nil, err
		}
		info.Address = cfg.Update.Address
		data, err = yaml.Marshal(info)
		if err != nil {
			return nil, err
		}
		if err := os.WriteFile(path, data, 0600); err != nil {
			return nil, err
		}
		nodes := []dqlite.NodeInfo{info}
		if err := dqlite.ReconfigureMembership(dir, nodes); err != nil {
			return nil, err
		}
		store, err := client.NewYamlNodeStore(filepath.Join(dir, "cluster.yaml"))
		if err != nil {
			return nil, err
		}
		if err := store.Set(context.Background(), nodes); err != nil {
			return nil, err
		}
		if err := os.Remove(filepath.Join(dir, "update.yaml")); err != nil {
			return nil, errors.Wrap(err, "remove update.yaml")
		}
	}

	options := []app.Option{
		app.WithFailureDomain(cfg.FailureDomain),
	}
	log.Printf("Failure domain set to %d", cfg.FailureDomain)
	if enableTLS {
		log.Printf("TLS enabled")

		listen, dial := app.SimpleTLSConfig(cfg.KeyPair, cfg.Pool)

		// Using a ClientSessionCache might cause incompatibilities and network resets
		// between go-dqlite 1.11.7 and 1.11.6 or older.
		if clientSessionCacheSize > 0 {
			log.Printf("Use TLS ClientSessionCache with size %d", clientSessionCacheSize)
			dial.ClientSessionCache = gotls.NewLRUClientSessionCache(int(clientSessionCacheSize))
		} else {
			log.Printf("Disable ClientSessionCache")
			dial.ClientSessionCache = nil
		}
		options = append(options, app.WithTLS(listen, dial))
	}

	// Possibly initialize our ID, address and initial node store content.
	if cfg.Init != nil {
		options = append(options, app.WithAddress(cfg.Init.Address), app.WithCluster(cfg.Init.Cluster))
	}

	// Tune raft snapshot parameters
	if v := cfg.DqliteTuning.Snapshot; v != nil {
		log.Printf("Raft snapshot parameters set to (threshold=%d, trailing=%d)", v.Threshold, v.Trailing)
		options = append(options, app.WithSnapshotParams(dqlite.SnapshotParams{
			Threshold: v.Threshold,
			Trailing:  v.Trailing,
		}))
	}

	// Tune network latency
	if v := cfg.DqliteTuning.NetworkLatency; v != nil {
		log.Printf("Network latency set to %v", *v)
		options = append(options, app.WithNetworkLatency(*v))
	}

	// Disk mode
	if diskMode {
		log.Printf("Enable dqlite disk mode")

		// TODO: remove after dqlite disk mode is stable
		log.Printf("WARNING: dqlite disk mode is current at an experimental state and SHOULD NOT be used in production. Expect data loss.")
		options = append(options, app.WithDiskMode(true))
	}

	app, err := app.New(dir, options...)
	if err != nil {
		return nil, err
	}
	if cfg.Init != nil {
		if err := os.Remove(filepath.Join(dir, "init.yaml")); err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	if err := app.Ready(ctx); err != nil {
		return nil, err
	}

	e := fmt.Sprintf("dqlite://k8s?driver-name=%s", app.Driver())

	// Tune kine compact interval
	if v := cfg.DqliteTuning.KineCompactInterval; v != nil {
		e = fmt.Sprintf("%s&compact-interval=%v", e, *v)
	}
	// Tune kine poll interval
	if v := cfg.DqliteTuning.KinePollInterval; v != nil {
		e = fmt.Sprintf("%s&poll-interval=%v", e, *v)
	}

	log.Printf("Connecting to kine endpoint: %s", e)

	config := endpoint.Config{
		Listener: listen,
		Endpoint: e,
	}

	if enableTLS {
		crt := filepath.Join(dir, "cluster.crt")
		key := filepath.Join(dir, "cluster.key")
		kineTls := tls.Config{
			CertFile: crt,
			KeyFile:  key,
		}
		config.Config = kineTls
	}

	kineCtx, cancelKine := context.WithCancel(context.Background())
	defer cancelKine()
	if _, err = endpoint.Listen(kineCtx, config); err != nil {
	if _, err := endpoint.Listen(kineCtx, config); err != nil {
		return nil, errors.Wrap(err, "kine")
	}

	s := &Server{
		dir:        dir,
		address:    cfg.Address,
		app:        app,
		cancelKine: cancelKine,
	}

	return s, nil
}

func (s *Server) Close(ctx context.Context) error {
	if s.cancelKine != nil {
		s.cancelKine()
	}
	s.app.Handover(ctx)
	if err := s.app.Close(); err != nil {
		return errors.Wrap(err, "stop dqlite app")
	}
	return nil
}
