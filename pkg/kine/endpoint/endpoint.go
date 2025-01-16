package endpoint

import (
	"context"
	"net"
	"os"
	"strings"

	"github.com/canonical/k8s-dqlite/pkg/kine/tls"
	"github.com/sirupsen/logrus"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

const (
	KineSocket = "unix://kine.sock"
)

type ETCDConfig struct {
	Endpoints []string
	TLSConfig tls.Config
}

type Server interface {
	etcdserverpb.LeaseServer
	etcdserverpb.WatchServer
	etcdserverpb.KVServer
	etcdserverpb.MaintenanceServer
}

type EndpointOptions struct {
	ListenAddress string

	Server Server
}

func Listen(ctx context.Context, options *EndpointOptions) (*ETCDConfig, error) {
	grpcServer := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             embed.DefaultGRPCKeepAliveMinTime,
			PermitWithoutStream: false,
		}),
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    embed.DefaultGRPCKeepAliveInterval,
			Timeout: embed.DefaultGRPCKeepAliveTimeout,
		}),
	)
	registerServer(grpcServer, options.Server)

	listenAddress := options.ListenAddress
	if listenAddress == "" {
		listenAddress = KineSocket
	}
	listener, err := createListener(listenAddress)
	if err != nil {
		return nil, err
	}

	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logrus.Errorf("Kine server shutdown: %v", err)
		}
		listener.Close()
	}()
	context.AfterFunc(ctx, grpcServer.Stop)

	return &ETCDConfig{
		Endpoints: []string{listenAddress},
		TLSConfig: tls.Config{},
	}, nil
}

func registerServer(grpc *grpc.Server, server Server) {
	etcdserverpb.RegisterLeaseServer(grpc, server)
	etcdserverpb.RegisterWatchServer(grpc, server)
	etcdserverpb.RegisterKVServer(grpc, server)
	etcdserverpb.RegisterMaintenanceServer(grpc, server)

	hsrv := health.NewServer()
	hsrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(grpc, hsrv)
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
