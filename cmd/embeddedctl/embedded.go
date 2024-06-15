package embeddedctl

import (
	"os"

	"github.com/spf13/cobra"
)

var (
	flagStorageDir string

	Command = &cobra.Command{
		Use: "embeddedctl",
	}
)

func init() {
	Command.PersistentFlags().StringVar(&flagStorageDir, "storage-dir", os.Getenv("EMBEDDED_DIR"), "k8s-dqlite storage directory")

	Command.AddCommand(memberCmd)
}
