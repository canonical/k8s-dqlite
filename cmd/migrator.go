package cmd

import (
	"github.com/canonical/k8s-dqlite/pkg/migrator"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	migratorEndpoint string
	migratorMode     string
	migratorDbDir    string
	migratorDebug    bool

	migratorCmd = &cobra.Command{
		Use:   "migrator",
		Short: "Tool to migrate etcd to dqlite",
		Long: `
Copy data between etcd and kine (dqlite)

		k8s-dqlite migrator --mode [backup-etcd|restore-etcd|backup-dqlite|restore-dqlite] --endpoint [etcd or kine endpoint] --db-dir [dir to store entries]

`,
		Run: func(cmd *cobra.Command, args []string) {
			if migratorDebug {
				logrus.SetLevel(logrus.DebugLevel)
			}

			ctx := cmd.Context()

			logrus.WithFields(logrus.Fields{"mode": migratorMode, "endpoint": migratorEndpoint, "dir": migratorDbDir}).Print("Starting migrator")
			switch migratorMode {
			case "backup", "backup-etcd":
				if err := migrator.BackupEtcd(ctx, migratorEndpoint, migratorDbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to backup etcd")
				}
			case "restore", "restore-to-dqlite", "restore-dqlite":
				if err := migrator.RestoreToDqlite(ctx, migratorEndpoint, migratorDbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to restore to etcd")
				}
			case "backup-dqlite":
				if err := migrator.BackupDqlite(ctx, migratorEndpoint, migratorDbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to backup dqlite")
				}
			case "restore-to-etcd", "restore-etcd":
				if err := migrator.RestoreToEtcd(ctx, migratorEndpoint, migratorDbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to restore etcd")
				}
			}
		},
	}
)

func init() {
	migratorCmd.Flags().StringVar(&migratorEndpoint, "endpoint", "unix:///var/snap/microk8s/current/var/kubernetes/backend/kine.sock", "")
	migratorCmd.Flags().StringVar(&migratorMode, "mode", "backup", "")
	migratorCmd.Flags().StringVar(&migratorDbDir, "db-dir", "db", "")
	migratorCmd.Flags().BoolVar(&migratorDebug, "debug", false, "")
	rootCmd.AddCommand(migratorCmd)
}
