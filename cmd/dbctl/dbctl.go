package dbctl

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	flagStorageDir string

	Command = &cobra.Command{
		Use:   "dbctl",
		Short: "Interact with the embedded datastore",
	}
)

func init() {
	// convenient default
	defaultStorageDir := os.Getenv("STORAGE_DIR")
	if defaultStorageDir == "" {
		snapCommon := os.Getenv("SNAP_COMMON")
		if snapCommon == "" {
			snapCommon = "/var/snap/k8s/common"
		}
		defaultStorageDir = filepath.Join(snapCommon, "var", "lib", "etcd")
	}

	Command.PersistentFlags().StringVar(&flagStorageDir, "storage-dir", defaultStorageDir, "etcd state directory")

	Command.AddCommand(memberCmd)
	Command.AddCommand(snapshotCmd)
}
