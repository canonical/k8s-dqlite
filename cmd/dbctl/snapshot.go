package dbctl

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	snapshotCmd = &cobra.Command{
		Use:   "snapshot",
		Short: "Manage cluster snapshots",
	}

	snapshotSaveCmd = &cobra.Command{
		Use:          "save [backup.db]",
		Short:        "Save a snapshot of the cluster",
		SilenceUsage: true,
		Args:         cobra.ExactArgs(1),
		RunE: command(func(ctx context.Context, client *clientv3.Client, args []string) (any, error) {
			reader, err := client.Snapshot(ctx)
			if err != nil {
				return nil, fmt.Errorf("failed to request snapshot: %w", err)
			}
			b, err := io.ReadAll(reader)
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve snapshot: %w", err)
			}
			if err := os.WriteFile(args[0], b, 0600); err != nil {
				return nil, fmt.Errorf("failed to write snapshot to %q: %w", args[0], err)
			}
			return map[string]any{"size": len(b), "file": args[0]}, nil
		}),
	}
)

func init() {
	// snapshot save
	snapshotCmd.AddCommand(snapshotSaveCmd)

}
