package sqlite

import (
	"context"

	internal "github.com/canonical/k8s-dqlite/pkg/backend/internal/backend"
	"github.com/canonical/k8s-dqlite/pkg/limited"
)

type BackendConfig struct {
	limited.Config
	//DriverConfig is the sqlite driver config
	DriverConfig *DriverConfig
}

func NewBackend(ctx context.Context, config *BackendConfig) (limited.Backend, error) {
	driver, err := NewDriver(ctx, config.DriverConfig)
	if err != nil {
		return nil, err
	}

	return &internal.Backend{
		Config:        config.Config,
		Driver:        driver,
		Notify:        make(chan int64, 100),
		WatcherGroups: make(map[*internal.WatcherGroup]*internal.WatcherGroup),
	}, nil
}
