package endpoint

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/dqlite"
	"github.com/canonical/k8s-dqlite/pkg/kine/drivers/generic"
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
	KineSocket    = "unix://kine.sock"
	SQLiteBackend = "sqlite"
	DQLiteBackend = "dqlite"
)

type Config struct {
	GRPCServer           *grpc.Server
	Listener             string
	Endpoint             string
	ConnectionPoolConfig generic.ConnectionPoolConfig
	tls.Config
	NotifyInterval time.Duration
}

type ETCDConfig struct {
	Endpoints []string
	TLSConfig tls.Config
}

func Listen(ctx context.Context, config Config) (ETCDConfig, error) {
	driver, dsn := ParseStorageEndpoint(config.Endpoint)
	backend, err := getKineStorageBackend(ctx, driver, dsn, config)
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

func createListener(listen string) (ret net.Listener, rerr error) {
	network, address := networkAndAddress(listen)

	if network == "unix" {
		if err := os.Remove(address); err != nil && !os.IsNotExist(err) {
			logrus.Warnf("failed to remove socket %s: %v", address, err)
		}
		defer func() {
			if err := os.Chmod(address, 0600); err != nil {
				rerr = err
			}
		}()
	}

	logrus.Infof("Kine listening on %s://%s", network, address)
	return net.Listen(network, address)
}

func ListenAndReturnBackend(ctx context.Context, config Config) (ETCDConfig, server.Backend, error) {
	driver, dsn := ParseStorageEndpoint(config.Endpoint)
	backend, err := getKineStorageBackend(ctx, driver, dsn, config)
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

func grpcServer(config Config) *grpc.Server {
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

func getKineStorageBackend(ctx context.Context, backendType, uri string, cfg Config) (server.Backend, error) {
	var (
		driver sqllog.Driver
		err    error
	)

	storageOptions, err := parseOpts(uri)
	if err != nil {
		return nil, err
	}

	switch backendType {
	case SQLiteBackend:
		dataSourceName := storageOptions.DataSourceName
		if dataSourceName == "" {
			if err := os.MkdirAll("./db", 0700); err != nil {
				return nil, err
			}
			dataSourceName = "./db/state.db?_journal=WAL&_synchronous=FULL&_foreign_keys=1"
		}
		driver, err = sqlite.New(ctx, "sqlite3", dataSourceName, &cfg.ConnectionPoolConfig)
	case DQLiteBackend:
		driver, err = dqlite.New(ctx, storageOptions.DriverName, storageOptions.DataSourceName, &cfg.ConnectionPoolConfig)
	default:
		return nil, fmt.Errorf("storage backend is not defined")
	}

	return sqllog.New(&sqllog.SQLLogOptions{
		Driver:            driver,
		CompactInterval:   storageOptions.CompactInterval,
		PollInterval:      storageOptions.PollInterval,
		WatchQueryTimeout: storageOptions.WatchQueryTimeout,
	}), err
}

func ParseStorageEndpoint(storageEndpoint string) (string, string) {
	network, address := networkAndAddress(storageEndpoint)
	if network == "" {
		network = SQLiteBackend
	}
	return network, address
}

func networkAndAddress(str string) (string, string) {
	parts := strings.SplitN(str, "://", 2)
	if len(parts) > 1 {
		return parts[0], parts[1]
	}
	return "", parts[0]
}

type backendOptions struct {
	// DataSourceName is the uri for the database file.
	DataSourceName string

	// DriverName is the name of the database driver. It is only used for dqlite.
	// If empty, a pre-registered default is used.
	DriverName string

	CompactInterval   time.Duration
	PollInterval      time.Duration
	WatchQueryTimeout time.Duration
}

func parseOpts(rawUri string) (*backendOptions, error) {
	result := &backendOptions{}

	uri, err := url.Parse(rawUri)
	if err != nil {
		return nil, err
	}

	options, err := url.ParseQuery(uri.RawQuery)
	if err != nil {
		return nil, err
	}

	if driverName := options.Get("driver-name"); driverName != "" {
		delete(options, "driver-name")
		result.DriverName = driverName
	}

	if compactInterval := options.Get("compact-interval"); compactInterval != "" {
		delete(options, "compact-interval")
		interval, err := time.ParseDuration(compactInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse compact-interval duration value %q: %w", compactInterval, err)
		}
		result.CompactInterval = interval
	}

	if pollInterval := options.Get("poll-interval"); pollInterval != "" {
		delete(options, "poll-interval")
		interval, err := time.ParseDuration(pollInterval)
		if err != nil {
			return nil, fmt.Errorf("failed to parse poll-interval duration value %q: %w", pollInterval, err)
		}
		result.PollInterval = interval
	}

	if watchQueryTimeout := options.Get("watch-query-timeout"); watchQueryTimeout != "" {
		delete(options, "watch-query-timeout")
		interval, err := time.ParseDuration(watchQueryTimeout)
		if err != nil {
			return nil, fmt.Errorf("failed to parse watch-query-timeout duration value %q: %w", watchQueryTimeout, err)
		}
		result.WatchQueryTimeout = interval
	}

	uri.RawQuery = url.Values(options).Encode()
	result.DataSourceName = uri.String()

	return result, nil
}
