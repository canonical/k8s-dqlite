package dqlite

import (
	"context"

	"github.com/canonical/k8s-dqlite/pkg/drivers/sqlite"
	"github.com/canonical/k8s-dqlite/pkg/limited"
)

type BackendConfig struct {
	*sqlite.BaseBackendConfig
	//DriverConfig is the dqlite driver config
	DriverConfig *DriverConfig
}

type Backend struct {
	sqlite.Backend
}

func NewBackend(ctx context.Context, config *BackendConfig) (limited.Backend, error) {

	driver, err := NewDriver(ctx, config.DriverConfig)
	if err != nil {
		return nil, err
	}

	return &Backend{
		Backend: sqlite.Backend{
			Config:        config.BaseBackendConfig,
			Driver:        driver,
			Notify:        make(chan int64, 100),
			WatcherGroups: make(map[*sqlite.WatcherGroup]*sqlite.WatcherGroup),
		},
	}, nil
}
