package migrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/canonical/k8s-dqlite/pkg/k8s_dqlite/client"
	k8s_dqlite_endpoint "github.com/canonical/k8s-dqlite/pkg/k8s_dqlite/endpoint"
	"github.com/sirupsen/logrus"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func keyFileName(dir string, index int) string {
	return filepath.Join(dir, fmt.Sprintf("%d.key", index))
}

func dataFileName(dir string, index int) string {
	return filepath.Join(dir, fmt.Sprintf("%d.data", index))
}

// BackupEtcd makes a back up of the contents of an etcd database into a filesystem directory.
func BackupEtcd(ctx context.Context, endpoint, dir string) error {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	if err != nil {
		return fmt.Errorf("failed to create etcd client: %w", err)
	}
	resp, err := client.Get(ctx, "", clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
	if err != nil {
		return fmt.Errorf("failed to list keys from etcd: %w", err)
	}
	client.Close()

	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to backup directory: %w", err)
	}
	for idx, kv := range resp.Kvs {
		logrus.WithFields(logrus.Fields{"index": idx, "key": string(kv.Key), "len": len(kv.Value)}).Print("Writing key")
		if err := os.WriteFile(keyFileName(dir, idx), kv.Key, 0640); err != nil {
			return fmt.Errorf("failed to write key file %d: %w", idx, err)
		}
		if err := os.WriteFile(dataFileName(dir, idx), kv.Value, 0640); err != nil {
			return fmt.Errorf("failed to write data file %d: %w", idx, err)
		}
	}
	return nil
}

// RestoreToDqlite restores database contents from backup directory to a k8s-dqlite database.
func RestoreToDqlite(ctx context.Context, endpoint, dir string) error {
	client, err := client.New(k8s_dqlite_endpoint.ETCDConfig{
		Endpoints: []string{endpoint},
	})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	idx := 0
	for {
		b, err := os.ReadFile(keyFileName(dir, idx))
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to read key file: %w", err)
			}
			logrus.WithField("entries", idx).Print("Completed database restore")
			return nil
		}
		key := string(b)

		value, err := os.ReadFile(dataFileName(dir, idx))
		if err != nil {
			return fmt.Errorf("failed to read value file: %w", err)
		}

		log := logrus.WithFields(logrus.Fields{"index": idx, "key": key})
		log.Debug("Restore key")
		if err := putKey(ctx, client, key, value); err != nil {
			log.Error("Failed to restore key")
		}

		idx++
	}
}

// BackupDqlite makes a back up of the contents of a k8s-dqlite database into a filesystem directory.
func BackupDqlite(ctx context.Context, endpoint, dir string) error {
	client, err := client.New(k8s_dqlite_endpoint.ETCDConfig{Endpoints: []string{endpoint}})
	if err != nil {
		return fmt.Errorf("failed to create k8s-dqlite client: %w", err)
	}
	resp, err := client.List(ctx, "/", 0)
	if err != nil {
		return fmt.Errorf("failed to list keys from dqlite: %w", err)
	}
	client.Close()

	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to backup directory: %w", err)
	}
	for idx, kv := range resp {
		logrus.WithFields(logrus.Fields{"index": idx, "key": string(kv.Key), "len": len(kv.Data)}).Print("Writing key")
		if err := os.WriteFile(keyFileName(dir, idx), kv.Key, 0640); err != nil {
			return fmt.Errorf("failed to write key file %d: %w", idx, err)
		}
		if err := os.WriteFile(dataFileName(dir, idx), kv.Data, 0640); err != nil {
			return fmt.Errorf("failed to write data file %d: %w", idx, err)
		}
	}
	return nil
}

// RestoreToEtcd restores database contents from backup directory to etcd.
func RestoreToEtcd(ctx context.Context, endpoint, dir string) error {
	client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}})
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	idx := 0
	for {
		b, err := os.ReadFile(keyFileName(dir, idx))
		if err != nil {
			if !os.IsNotExist(err) {
				return fmt.Errorf("failed to read key file: %w", err)
			}
			logrus.WithField("entries", idx).Print("Completed database restore")
			return nil
		}
		key := string(b)

		value, err := os.ReadFile(dataFileName(dir, idx))
		if err != nil {
			return fmt.Errorf("failed to read value file: %w", err)
		}

		log := logrus.WithFields(logrus.Fields{"index": idx, "key": key})
		log.Debug("Restore key")
		if _, err := client.Put(ctx, key, string(value)); err != nil {
			log.Error("Failed to restore key")
		}

		idx++
	}
}
