package dqlite

import (
	"context"

	"github.com/canonical/k8s-dqlite/pkg/backend/v1/internal/backend"
	"github.com/canonical/k8s-dqlite/pkg/backend/v1/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/limited"
)

type BackendConfig struct {
	backend.Config
	//DriverConfig is the dqlite driver config
	DriverConfig *DriverConfig
}

func NewBackend(ctx context.Context, config *BackendConfig) (limited.Backend, error) {
	driver, err := NewDriver(ctx, config.DriverConfig)
	if err != nil {
		return nil, err
	}

	return sqlite.Backend{
		Config:        config.Config,
		Driver:        driver,
		Notify:        make(chan int64, 100),
		WatcherGroups: make(map[*sqlite.WatcherGroup]*sqlite.WatcherGroup),
	}, nil
}
