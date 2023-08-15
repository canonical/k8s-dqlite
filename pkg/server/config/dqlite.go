package config

import (
	"context"
	"os"
	"path/filepath"

	"github.com/canonical/go-dqlite/client"
	"github.com/pkg/errors"
)

// Migrate the legacy servers.sql SQLite database containing the addresses
// of the servers in the cluster.
func migrateNodeStore(dir string) error {
	// Possibly migrate from older path.
	legacyStorePath := filepath.Join(dir, "servers.sql")
	if _, err := os.Stat(legacyStorePath); err != nil {
		if !os.IsNotExist(err) {
			return errors.Wrap(err, "check if legacy node store exists")
		}
		return nil
	}

	legacyStore, err := client.DefaultNodeStore(legacyStorePath)
	if err != nil {
		return errors.Wrap(err, "open legacy node store")
	}
	servers, err := legacyStore.Get(context.Background())
	if err != nil {
		return errors.Wrap(err, "get servers from legacy node store")
	}

	store, err := client.NewYamlNodeStore(filepath.Join(dir, "cluster.yaml"))
	if err != nil {
		return errors.Wrap(err, "open node store")
	}
	if err := store.Set(context.Background(), servers); err != nil {
		return errors.Wrap(err, "migrate servers to new node store")
	}
	if err := os.Remove(legacyStorePath); err != nil {
		return errors.Wrap(err, "remove legacy store path")
	}

	return nil
}
