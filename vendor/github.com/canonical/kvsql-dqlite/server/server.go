package server

import (
	"context"
	"fmt"
	"github.com/k3s-io/kine/pkg/drivers/generic"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite"
	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/canonical/kvsql-dqlite/server/config"
	"github.com/ghodss/yaml"
	"github.com/k3s-io/kine/pkg/endpoint"
	"github.com/k3s-io/kine/pkg/tls"
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

func New(dir string, listen string, enableTls bool, compact int64, eventPoll int64) (*Server, error) {
	// Check if we're initializing a new node (i.e. there's an init.yaml).
	// dir: the directory where data will be stored as well as where the init.yaml
	//       and certificates should be found
	// listen: kine listen endpoint could be a socket ("unix://<path>")
	//         or network ep ("tcp://127.0.0.1:12345")
	// enableTls: true if we should enable tls communication
	// compact: compaction interval in seconds
	// eventPoll: event poll interval in seconds
	cfg, err := config.Load(dir)
	if err != nil {
		return nil, err
	}

	if cfg.Update != nil {
		info := client.NodeInfo{}
		path := filepath.Join(dir, "info.yaml")
		data, err := ioutil.ReadFile(path)
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
		if err := ioutil.WriteFile(path, data, 0600); err != nil {
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
	if enableTls {
		options = append(options, app.WithTLS(app.SimpleTLSConfig(cfg.KeyPair, cfg.Pool)))
	}

	// Possibly initialize our ID, address and initial node store content.
	if cfg.Init != nil {
		options = append(options, app.WithAddress(cfg.Init.Address), app.WithCluster(cfg.Init.Cluster))
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

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Minute)
	defer cancel()

	if err := app.Ready(ctx); err != nil {
		return nil, err
	}

	// Connect to a single peer that is the current machine
	info := client.NodeInfo{}
	infoFile := filepath.Join(dir, "info.yaml")
	data, err := ioutil.ReadFile(infoFile)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	serverList := []client.NodeInfo{}
	serverList = append(serverList, info)
	data, err = yaml.Marshal(serverList)
	if err != nil {
		return  nil, err
	}
	localServerFile := filepath.Join(dir, "localnode.yaml")
	if err := ioutil.WriteFile(localServerFile, data, 0600); err != nil {
		return nil, err
	}

	peers := localServerFile

	ep := defaultKineEp
	if listen != "" {
		ep = listen
	}

	pool := generic.ConnectionPoolConfig{
		MaxIdle:     5,
		MaxOpen:     5,
		MaxLifetime: 60 * time.Second,
	}
	config := endpoint.Config{
		Listener:             ep,
		Endpoint:             fmt.Sprintf("dqlite://k8s?peer-file=%s&driver-name=%s&compact-interval=%d&event-poll-interval=%d", peers, app.Driver(), compact, eventPoll),
		ConnectionPoolConfig: pool,
	}

	if enableTls {
		crt := filepath.Join(dir, "cluster.crt")
		key := filepath.Join(dir, "cluster.key")
		kineTls := tls.Config{
			CertFile: crt,
			KeyFile: key,
		}
		config.Config = kineTls
	}

	kineCtx, cancelKine := context.WithCancel(context.Background())
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
