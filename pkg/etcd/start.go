package etcd

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap"
)

func (e *etcd) Start(ctx context.Context) error {
	if initialCluster, err := e.ensurePeerInCluster(ctx); err != nil {
		return fmt.Errorf("failed to initialize node: %w", err)
	} else if initialCluster != "" {
		e.config.GetLogger().Info("Set initial cluster", zap.String("initial-cluster", initialCluster))
		e.config.InitialCluster = initialCluster
		e.config.ClusterState = "existing"
	}

	if instance, err := embed.StartEtcd(e.config); err != nil {
		return fmt.Errorf("failed to start node: %w", err)
	} else {
		e.instance = instance
	}

	select {
	case <-ctx.Done():
		return fmt.Errorf("etcd did not start in time: %w", ctx.Err())
	case <-e.instance.Server.ReadyNotify():
		return nil
	}
}
