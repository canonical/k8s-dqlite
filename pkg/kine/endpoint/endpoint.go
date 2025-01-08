package endpoint

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/database"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/dqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/server"
	"github.com/canonical/k8s-dqlite/pkg/kine/sqllog"
	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

const (
	KineSocket          = "unix://kine.sock"
	SQLiteBackend       = "sqlite"
	DQLiteBackend       = "dqlite"
	defaultMaxIdleConns = 2 // default from database/sql
)

type Config struct {
	GRPCServer           *grpc.Server
	Listener             string
	Endpoint             string
	ConnectionPoolConfig *ConnectionPoolConfig

	tls.Config
	NotifyInterval time.Duration
}

type ConnectionPoolConfig struct {
	MaxIdle     int
	MaxOpen     int
	MaxLifetime time.Duration
	MaxIdleTime time.Duration
}

func (conf *ConnectionPoolConfig) apply(db *sql.DB) {
	// behavior of database/sql - zero means defaultMaxIdleConns; negative means 0
	var maxIdle int
	if conf.MaxIdle < 0 {
		maxIdle = 0
	} else if conf.MaxIdle == 0 {
		maxIdle = defaultMaxIdleConns
	}

	logrus.Infof(
		"Configuring database connection pooling: maxIdleConns=%d, maxOpenConns=%d, connMaxLifetime=%v, connMaxIdleTime=%v ",
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

type ETCDConfig struct {
	Endpoints []string
	TLSConfig tls.Config
}

func Listen(ctx context.Context, config *Config) (ETCDConfig, error) {
	backend, err := openKineStorageBackend(ctx, config)
	if err != nil {
		return ETCDConfig{}, errors.Wrap(err, "building kine")
	}

	if err := backend.Start(ctx); err != nil {
		return ETCDConfig{}, errors.Wrap(err, "starting kine backend")
	}

	listen := config.Listener
	if listen == "" {
		listen = KineSocket
	}

	b := server.New(backend, config.NotifyInterval)
	grpcServer := grpcServer(config)
	b.Register(grpcServer)

	listener, err := createListener(listen)
	if err != nil {
		return ETCDConfig{}, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logrus.Errorf("unexpected server shutdown: %v", err)
		}
	}()
	context.AfterFunc(ctx, grpcServer.Stop)

	return ETCDConfig{
		Endpoints: []string{listen},
		TLSConfig: tls.Config{},
	}, nil
}

func createListener(listen string) (_ net.Listener, err error) {
	network, address := networkAndAddress(listen)

	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if chmodErr := os.Chmod(address, 0600); chmodErr != nil {
				err = chmodErr
			}
		}()
	}

	logrus.Infof("Kine listening on %s", listen)
	return net.Listen(network, address)
}

func networkAndAddress(str string) (string, string) {
	network, address, found := strings.Cut(str, "://")
	if found {
		return network, address
	}
	return "", str
}

func ListenAndReturnBackend(ctx context.Context, config *Config) (ETCDConfig, server.Backend, error) {
	backend, err := openKineStorageBackend(ctx, config)
	if err != nil {
		return ETCDConfig{}, nil, errors.Wrap(err, "building kine")
	}

	if err := backend.Start(ctx); err != nil {
		return ETCDConfig{}, nil, errors.Wrap(err, "starting kine backend")
	}

	listen := config.Listener
	if listen == "" {
		listen = KineSocket
	}

	b := server.New(backend, config.NotifyInterval)
	grpcServer := grpcServer(config)
	b.Register(grpcServer)

	listener, err := createListener(listen)
	if err != nil {
		return ETCDConfig{}, nil, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logrus.Errorf("Kine server shutdown: %v", err)
		}
		listener.Close()
	}()
	context.AfterFunc(ctx, grpcServer.Stop)

	return ETCDConfig{
		Endpoints: []string{listen},
		TLSConfig: tls.Config{},
	}, backend, nil
}

func grpcServer(config *Config) *grpc.Server {
	if config.GRPCServer != nil {
		return config.GRPCServer
	}
	gopts := []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             embed.DefaultGRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    embed.DefaultGRPCKeepAliveInterval,
			Timeout: embed.DefaultGRPCKeepAliveTimeout,
		}),
	}

	return grpc.NewServer(gopts...)
}

func openKineStorageBackend(ctx context.Context, config *Config) (server.Backend, error) {
	var (
		driver sqllog.Driver
		err    error
	)

	options, err := parseOpts(config.Endpoint)
	if err != nil {
		return nil, err
	}

	switch options.BackendType {
	case SQLiteBackend:
		driver, err = openSqlite(ctx, options.DataSourceName, config.ConnectionPoolConfig)
	case DQLiteBackend:
		driver, err = openDqlite(ctx, options.DriverName, options.DataSourceName, config.ConnectionPoolConfig)
	default:
		return nil, fmt.Errorf("backend type %s is not defined", options.BackendType)
	}
	if err != nil {
		return nil, err
	}

	return sqllog.New(&sqllog.SQLLogOptions{
		Driver:            driver,
		CompactInterval:   options.CompactInterval,
		PollInterval:      options.PollInterval,
		WatchQueryTimeout: options.WatchQueryTimeout,
	}), err
}

func openSqlite(ctx context.Context, dataSourceName string, connPoolConfig *ConnectionPoolConfig) (sqllog.Driver, error) {
	if dataSourceName == "" {
		if err := os.MkdirAll("./db", 0700); err != nil {
			return nil, err
		}
		dataSourceName = "./db/state.db?_journal=WAL&_synchronous=FULL&_foreign_keys=1"
	}
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	if connPoolConfig != nil {
		connPoolConfig.apply(db)
	}

	return sqlite.NewDriver(ctx, &sqlite.DriverOptions{
		DB: database.NewPrepared(db),
	})
}

func openDqlite(ctx context.Context, driverName, dataSourceName string, connPoolConfig *ConnectionPoolConfig) (sqllog.Driver, error) {
	var (
		db  *sql.DB
		err error
	)
	for i := 0; i < 300; i++ {
		db, err = openAndTest(ctx, driverName, dataSourceName)
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

	if connPoolConfig != nil {
		connPoolConfig.apply(db)
	}

	return dqlite.NewDriver(ctx, &dqlite.DriverOptions{
		DB: database.NewPrepared(db),
	})
}

func openAndTest(ctx context.Context, driverName, dataSourceName string) (*sql.DB, error) {
	db, err := sql.Open(driverName, dataSourceName)
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

type backendOptions struct {
	// BackendType is the type of backend to use.
	BackendType string

	// DriverName is the name of the database driver.
	DriverName string

	// DataSourceName is the uri for the database file.
	DataSourceName string

	CompactInterval   time.Duration
	PollInterval      time.Duration
	WatchQueryTimeout time.Duration
}

func parseOpts(rawUri string) (*backendOptions, error) {
	uri, err := url.Parse(rawUri)
	if err != nil {
		return nil, err
	}

	// The configuration of kine uses the schema as the
	// driver name. However that forces the URL to be
	// absolute, while using a relative path. As such,
	// the host, if present, should actually be part
	// of the path.
	uri.Path = path.Join(uri.Host, uri.Path)
	uri.Host = ""

	options, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, err
	}

	backendType := uri.Scheme
	if backendType == "" {
		backendType = SQLiteBackend
	}

	driverName := options.Get("driver-name")
	options.Del("driver-name")

	getDuration := func(name string, defaultValue time.Duration) (time.Duration, error) {
		defer options.Del(name)

		value := options.Get(name)
		if value == "" {
			return defaultValue, nil
		}
		duration, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("failed to parse compact-interval duration value %q: %w", value, err)
		}
		return duration, nil
	}

	compactInterval, err := getDuration("compact-interval", 5*time.Minute)
	if err != nil {
		return nil, err
	}

	pollInterval, err := getDuration("poll-interval", 1*time.Second)
	if err != nil {
		return nil, err
	}

	watchQueryTimeout, err := getDuration("watch-query-timeout", 20*time.Second)
	if err != nil {
		return nil, err
	}

	dataSource := &url.URL{
		Path:     uri.Path,
		RawQuery: options.Encode(),
	}

	return &backendOptions{
		BackendType:       backendType,
		DataSourceName:    dataSource.String(),
		DriverName:        driverName,
		CompactInterval:   compactInterval,
		PollInterval:      pollInterval,
		WatchQueryTimeout: watchQueryTimeout,
	}, nil
}
