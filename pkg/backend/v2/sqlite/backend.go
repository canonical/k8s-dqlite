package sqlite

import (
	"context"
	"time"

	internal "github.com/canonical/k8s-dqlite/pkg/backend/v2/internal/backend"
	"github.com/canonical/k8s-dqlite/pkg/limited"
)

type BackendConfig struct {
	limited.Config
	//DriverConfig is the sqlite driver config
	DriverConfig *DriverConfig
}

type Config struct {
	// CompactInterval is interval between database compactions performed by k8s-dqlite.
	CompactInterval time.Duration

	// PollInterval is the event poll interval used by k8s-dqlite.
	PollInterval time.Duration

	// WatchQueryTimeout is the timeout on the after query in the poll loop.
	WatchQueryTimeout time.Duration
}

func NewBackend(ctx context.Context, config *BackendConfig) (limited.Backend, error) {
	driver, err := NewDriver(ctx, config.DriverConfig)
	if err != nil {
		return nil, err
	}

	return internal.Backend{
		Config:        config.Config,
		Driver:        driver,
		Notify:        make(chan int64, 100),
		WatcherGroups: make(map[*internal.WatcherGroup]*internal.WatcherGroup),
	}, nil
}
