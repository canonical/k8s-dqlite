package dqlitev2

import (
	"context"

	"github.com/canonical/k8s-dqlite/pkg/backend/v2/internal/backend"
	sqlitev2 "github.com/canonical/k8s-dqlite/pkg/backend/v2/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/limited"
)

type BackendConfig struct {
	backend.Config
	//DriverConfig is the dqlite driver config
	DriverConfig *DriverConfig
}

type Backend struct {
	sqlitev2.Backend
}

func NewBackend(ctx context.Context, config *BackendConfig) (limited.Backend, error) {

	driver, err := NewDriver(ctx, config.DriverConfig)
	if err != nil {
		return nil, err
	}

	return &Backend{
		Backend: sqlitev2.Backend{
			Config:        config.Config,
			Driver:        driver,
			Notify:        make(chan int64, 100),
			WatcherGroups: make(map[*sqlitev2.WatcherGroup]*sqlitev2.WatcherGroup),
		},
	}, nil
}
