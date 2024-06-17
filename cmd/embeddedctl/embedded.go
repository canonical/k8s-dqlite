package embeddedctl

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	flagStorageDir string

	Command = &cobra.Command{
		Use:   "embeddedctl",
		Short: "Interact with the embedded datastore",
	}
)

func init() {
	// convenient default
	defaultStorageDir := os.Getenv("EMBEDDED_DIR")
	if defaultStorageDir == "" {
		snapCommon := os.Getenv("SNAP_COMMON")
		if snapCommon == "" {
			snapCommon = "/var/snap/k8s/common"
		}
		defaultStorageDir = filepath.Join(snapCommon, "var", "lib", "k8s-dqlite")
	}

	Command.PersistentFlags().StringVar(&flagStorageDir, "storage-dir", defaultStorageDir, "k8s-dqlite state directory")

	Command.AddCommand(memberCmd)
	Command.AddCommand(snapshotCmd)
}
