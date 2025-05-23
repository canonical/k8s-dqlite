package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/canonical/go-dqlite/v2"
	"github.com/canonical/go-dqlite/v2/app"
	"github.com/canonical/go-dqlite/v2/client"
	dqliteDriver "github.com/canonical/k8s-dqlite/pkg/backend/dqlite"
	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/endpoint"
	"github.com/canonical/k8s-dqlite/pkg/limited"
	k8s_dqlite_tls "github.com/canonical/k8s-dqlite/pkg/tls"
	"github.com/sirupsen/logrus"
)

// Server is the main k8s-dqlite server.
type Server struct {
	// app is the dqlite application driving the server.
	app *app.App

	backend limited.Backend

	serverConfig         *ServerConfig
	connectionPoolConfig *ConnectionPoolConfig

	// storageDir is the root directory used for dqlite storage.
	storageDir string
	// watchAvailableStorageMinBytes is the minimum required bytes that the server will expect to be
	// available on the storage directory. If not, it will handover the leader role and terminate.
	watchAvailableStorageMinBytes uint64
	// watchAvailableStorageInterval is the interval to check for available disk size. If set to
	// zero, then no checks will be performed.
	watchAvailableStorageInterval time.Duration
	// actionOnLowDisk is the action to perform in case the system is running low on disk.
	// One of "terminate", "handover", "none"
	actionOnLowDisk string

	// mustStopCh is used when the server must terminate.
	mustStopCh chan struct{}
}

type ServerConfig struct {
	ListenAddress string

	CompactInterval   time.Duration
	PollInterval      time.Duration
	WatchQueryTimeout time.Duration
	NotifyInterval    time.Duration

	TlsConfig k8s_dqlite_tls.Config
}

type ConnectionPoolConfig struct {
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

func (conf *ConnectionPoolConfig) apply(db *sql.DB) {
	const defaultMaxIdleConns = 2 // default from database/sql

	// behavior of database/sql - zero means defaultMaxIdleConns; negative means 0
	var maxIdle int
	if conf.MaxIdle < 0 {
		maxIdle = 0
	} else if conf.MaxIdle == 0 {
		maxIdle = defaultMaxIdleConns
	}

	logrus.Infof(
		"configuring database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%v, connMaxIdleTime=%v ",
		maxIdle,
		conf.MaxOpen,
		conf.MaxLifetime,
		conf.MaxIdleTime,
	)
	db.SetMaxIdleConns(conf.MaxIdle)
	db.SetMaxOpenConns(conf.MaxOpen)
	db.SetConnMaxLifetime(conf.MaxLifetime)
	db.SetConnMaxIdleTime(conf.MaxIdleTime)
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

const (
	defaultThreshold = 512
	minThreshold     = 384
	defaultTrailing  = 1024
	minTrailing      = 512
)

// New creates a new instance of Server based on configuration.
func New(
	dir string,
	listen string,
	enableTLS bool,
	diskMode bool,
	clientSessionCacheSize uint,
	minTLSVersion string,
	watchAvailableStorageInterval time.Duration,
	watchAvailableStorageMinBytes uint64,
	lowAvailableStorageAction string,
	connectionPoolConfig *ConnectionPoolConfig,
	watchQueryTimeout time.Duration,
	watchProgressNotifyInterval time.Duration,
) (*Server, error) {
	options := []app.Option{
		app.WithLogFunc(appLog),
	}
	serverConfig := &ServerConfig{
		ListenAddress: listen,

		CompactInterval:   5 * time.Minute,
		PollInterval:      1 * time.Second,
		WatchQueryTimeout: watchQueryTimeout,
		NotifyInterval:    watchProgressNotifyInterval,
	}

	switch lowAvailableStorageAction {
	case "none", "handover", "terminate":
	default:
		return nil, fmt.Errorf("unsupported low available storage action %v (supported values are none, handover, terminate)", lowAvailableStorageAction)
	}

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

		logrus.WithFields(logrus.Fields{"address": init.Address, "cluster": init.Cluster}).Print("will initialize dqlite node")

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

		logrus.WithFields(logrus.Fields{"old_address": info.Address, "new_address": update.Address}).Print("will update address of dqlite node")

		// update node address
		info.Address = update.Address

		// reconfigure dqlite membership
		if err := dqlite.ReconfigureMembershipExt(dir, []dqlite.NodeInfo{info}); err != nil {
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
	logrus.WithField("failure-domain", failureDomain).Print("configure dqlite failure domain")
	options = append(options, app.WithFailureDomain(failureDomain))

	// handle TLS
	if enableTLS {
		crtFile := filepath.Join(dir, "cluster.crt")
		keyFile := filepath.Join(dir, "cluster.key")

		keypair, err := tls.LoadX509KeyPair(crtFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load keypair from cluster.crt and cluster.key: %w", err)
		}
		crtPEM, err := os.ReadFile(crtFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read cluster.crt: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(crtPEM) {
			return nil, fmt.Errorf("failed to add certificate to pool")
		}

		listen, dial := app.SimpleTLSConfig(keypair, pool)

		if clientSessionCacheSize > 0 {
			logrus.WithField("cache_size", clientSessionCacheSize).Print("use TLS ClientSessionCache")
			dial.ClientSessionCache = tls.NewLRUClientSessionCache(int(clientSessionCacheSize))
		} else {
			logrus.Print("disable TLS ClientSessionCache")
			dial.ClientSessionCache = nil
		}

		switch minTLSVersion {
		case "tls10":
			listen.MinVersion = tls.VersionTLS10
		case "tls11":
			listen.MinVersion = tls.VersionTLS11
		case "", "tls12":
			minTLSVersion = "tls12"
			listen.MinVersion = tls.VersionTLS12
		case "tls13":
			listen.MinVersion = tls.VersionTLS13
		default:
			return nil, fmt.Errorf("unsupported TLS version %v (supported values are tls10, tls11, tls12, tls13)", minTLSVersion)
		}
		logrus.WithField("min_tls_version", minTLSVersion).Print("enable TLS")

		serverConfig.TlsConfig = k8s_dqlite_tls.Config{
			CertFile: crtFile,
			KeyFile:  keyFile,
		}
		options = append(options, app.WithTLS(listen, dial))
	}

	// handle tuning parameters
	// declare default
	snapshotParameters := dqlite.SnapshotParams{
		Threshold: defaultThreshold,
		Trailing:  defaultTrailing,
	}
	if exists, err := fileExists(dir, "tuning.yaml"); err != nil {
		return nil, fmt.Errorf("failed to check for tuning.yaml: %w", err)
	} else if exists {
		var tuning TuningConfiguration
		if err := fileUnmarshal(&tuning, dir, "tuning.yaml"); err != nil {
			return nil, fmt.Errorf("failed to read tuning.yaml: %w", err)
		}

		if v := tuning.Snapshot; v != nil {
			logrus.WithFields(logrus.Fields{"threshold": v.Threshold, "trailing": v.Trailing}).Print("initial dqlite raft snapshot parameters before adjustments")

			snapshotParameters.Threshold = v.Threshold
			snapshotParameters.Trailing = v.Trailing

			if v.Trailing < minTrailing {
				snapshotParameters.Trailing = minTrailing
				logrus.WithFields(logrus.Fields{"adjustedTrailing": snapshotParameters.Trailing}).Warning("trailing value is too low, setting to minimum value")
			}
			if v.Threshold == 0 {
				snapshotParameters.Threshold = v.Trailing * 3 / 4
				logrus.WithFields(logrus.Fields{"adjustedThreshold": snapshotParameters.Threshold}).Warning("threshold value is zero, setting to half of trailing value")
			} else if v.Threshold < minThreshold {
				snapshotParameters.Threshold = minThreshold
				logrus.WithFields(logrus.Fields{"adjustedThreshold": snapshotParameters.Threshold}).Warning("threshold value is too low, setting to minimum value")
			}
			if v.Threshold > v.Trailing {
				snapshotParameters.Threshold = v.Trailing
				logrus.WithFields(logrus.Fields{"adjustedThreshold": snapshotParameters.Threshold}).Warning("threshold value is higher than trailing value, setting to trailing value")
			}
		}

		if v := tuning.NetworkLatency; v != nil {
			logrus.WithField("latency", *v).Print("configure dqlite average one-way network latency")
			options = append(options, app.WithNetworkLatency(*v))
		}

		// these are set in the k8s-dqlite endpoint config below
		if tuning.K8sDqliteCompactInterval != nil {
			serverConfig.CompactInterval = *tuning.K8sDqliteCompactInterval
		}
		if tuning.K8sDqlitePollInterval != nil {
			serverConfig.PollInterval = *tuning.K8sDqlitePollInterval
		}
	}

	logrus.WithFields(logrus.Fields{"threshold": snapshotParameters.Threshold, "trailing": snapshotParameters.Trailing}).Print("configure dqlite raft snapshot parameters")
	options = append(options, app.WithSnapshotParams(snapshotParameters))

	if diskMode {
		logrus.Print("enable dqlite disk mode operation")
		options = append(options, app.WithDiskMode(true))

		// TODO: remove after disk mode is stable
		logrus.Warn("dqlite disk mode operation is current at an experimental state and MUST NOT be used in production. Expect data loss.")
	}

	// FIXME: this also starts dqlite. It should be moved to `Start`.
	app, err := app.New(dir, options...)
	if err != nil {
		return nil, fmt.Errorf("failed to create dqlite app: %w", err)
	}

	return &Server{
		app:                  app,
		serverConfig:         serverConfig,
		connectionPoolConfig: connectionPoolConfig,

		storageDir:                    dir,
		watchAvailableStorageMinBytes: watchAvailableStorageMinBytes,
		watchAvailableStorageInterval: watchAvailableStorageInterval,
		actionOnLowDisk:               lowAvailableStorageAction,

		mustStopCh: make(chan struct{}, 1),
	}, nil
}

func (s *Server) watchAvailableStorageSize(ctx context.Context) {
	logrus := logrus.WithField("dir", s.storageDir)

	if s.watchAvailableStorageInterval <= 0 {
		logrus.Info("disable periodic check for available disk size")
		return
	}

	logrus.WithField("interval", s.watchAvailableStorageInterval).Info("enable periodic check for available disk size")
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(s.watchAvailableStorageInterval):
			if err := checkAvailableStorageSize(s.storageDir, s.watchAvailableStorageMinBytes); err != nil {
				err := fmt.Errorf("periodic check for available disk storage failed: %w", err)

				switch s.actionOnLowDisk {
				case "none":
					logrus.WithError(err).Info("ignoring failed available disk storage check")
				case "handover":
					logrus.WithError(err).Info("handover dqlite leadership role")
					if err := s.app.Handover(ctx); err != nil {
						logrus.WithError(err).Warning("failed to handover dqlite leadership")
					}
				case "terminate":
					logrus.WithError(err).Error("terminating due to failed available disk storage check")
					s.mustStopCh <- struct{}{}
				}
			}
		}
	}
}

// MustStop returns a channel that can be used to check whether the server must stop.
func (s *Server) MustStop() <-chan struct{} {
	return s.mustStopCh
}

// Start the dqlite node and the k8s-dqlite machinery.
func (s *Server) Start(ctx context.Context) error {
	if err := s.app.Ready(ctx); err != nil {
		return fmt.Errorf("failed to start dqlite app: %w", err)
	}
	logrus.WithFields(logrus.Fields{"id": s.app.ID(), "address": s.app.Address()}).Print("started dqlite")

	logrus.WithField("config", s.serverConfig).Debug("starting k8s-dqlite server")

	db, err := s.robustOpenDb(ctx)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	backend, err := dqliteDriver.NewBackend(ctx, &dqliteDriver.BackendConfig{
		DriverConfig: &dqliteDriver.DriverConfig{
			DB:  database.NewPrepared(db),
			App: s.app,
		},
		Config: limited.Config{
			CompactInterval:   s.serverConfig.CompactInterval,
			PollInterval:      s.serverConfig.PollInterval,
			WatchQueryTimeout: s.serverConfig.WatchQueryTimeout,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to create dqlite backend: %w", err)
	}

	if err := backend.Start(ctx); err != nil {
		backend.Close()
		return fmt.Errorf("failed to start k8s-dqlite backend: %w", err)
	}

	s.backend = backend

	_, err = endpoint.Listen(ctx, &endpoint.EndpointConfig{
		ListenAddress: s.serverConfig.ListenAddress,
		Server:        limited.New(backend, s.serverConfig.NotifyInterval),
	})
	if err != nil {
		return fmt.Errorf("failed to start k8s-dqlite: %w", err)
	}

	go s.watchAvailableStorageSize(ctx)

	logrus.WithField("address", s.serverConfig.ListenAddress).Print("started k8s-dqlite server")
	return nil
}

func (s *Server) robustOpenDb(ctx context.Context) (*sql.DB, error) {
	var (
		db  *sql.DB
		err error
	)
	for i := 0; i < 300; i++ {
		db, err = s.openAndTestDb(ctx)
		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		return nil, err
	}

	s.connectionPoolConfig.apply(db)
	return db, nil
}

func (s *Server) openAndTestDb(ctx context.Context) (*sql.DB, error) {
	db, err := s.app.Open(ctx, "k8s")
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		logrus.Errorf("failed to ping connection: %v", err)
		db.Close()
		return nil, err
	}

	return db, nil
}

// Shutdown cleans up any resources and attempts to hand-over and shutdown the dqlite application.
func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.backend.Close(); err != nil {
		return err
	}

	logrus.Debug("handing over dqlite leadership")
	if err := s.app.Handover(ctx); err != nil {
		logrus.WithError(err).Errorf("failed to handover dqlite")
	}
	logrus.Debug("closing dqlite application")
	if err := s.app.Close(); err != nil {
		return fmt.Errorf("failed to close dqlite app: %w", err)
	}

	close(s.mustStopCh)
	return nil
}

func logrusLogLevel(level client.LogLevel) logrus.Level {
	switch level {
	case client.LogError:
		return logrus.ErrorLevel
	case client.LogWarn:
		return logrus.WarnLevel
	case client.LogInfo:
		return logrus.InfoLevel
	case client.LogDebug:
		return logrus.DebugLevel
	default:
		return logrus.TraceLevel
	}
}

func appLog(level client.LogLevel, format string, args ...interface{}) {
	logrus.StandardLogger().Logf(logrusLogLevel(level), format, args...)
}

var _ Instance = &Server{}
