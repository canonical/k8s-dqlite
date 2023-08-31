package cmd

import (
	"github.com/canonical/k8s-dqlite/pkg/migrator"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	migratorCmdOpts struct {
		endpoint string
		mode     string
		dbDir    string
		debug    bool
	}

	migratorCmd = &cobra.Command{
		Use:   "migrator",
		Short: "Tool to migrate etcd to dqlite",
		Long: `
Copy data between etcd and kine (dqlite)

		k8s-dqlite migrator --mode [backup-etcd|restore-etcd|backup-dqlite|restore-dqlite] --endpoint [etcd or kine endpoint] --db-dir [dir to store entries]

`,
		Run: func(cmd *cobra.Command, args []string) {
			if migratorCmdOpts.debug {
				logrus.SetLevel(logrus.DebugLevel)
			}

			ctx := cmd.Context()

			logrus.WithFields(logrus.Fields{"mode": migratorCmdOpts.mode, "endpoint": migratorCmdOpts.endpoint, "dir": migratorCmdOpts.dbDir}).Print("Starting migrator")
			switch migratorCmdOpts.mode {
			case "backup", "backup-etcd":
				if err := migrator.BackupEtcd(ctx, migratorCmdOpts.endpoint, migratorCmdOpts.dbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to backup etcd")
				}
			case "restore", "restore-to-dqlite", "restore-dqlite":
				if err := migrator.RestoreToDqlite(ctx, migratorCmdOpts.endpoint, migratorCmdOpts.dbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to restore to etcd")
				}
			case "backup-dqlite":
				if err := migrator.BackupDqlite(ctx, migratorCmdOpts.endpoint, migratorCmdOpts.dbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to backup dqlite")
				}
			case "restore-to-etcd", "restore-etcd":
				if err := migrator.RestoreToEtcd(ctx, migratorCmdOpts.endpoint, migratorCmdOpts.dbDir); err != nil {
					logrus.WithError(err).Fatal("Failed to restore etcd")
				}
			}
		},
	}
)

func init() {
	migratorCmd.Flags().StringVar(&migratorCmdOpts.endpoint, "endpoint", "unix:///var/snap/microk8s/current/var/kubernetes/backend/kine.sock", "")
	migratorCmd.Flags().StringVar(&migratorCmdOpts.mode, "mode", "backup", "")
	migratorCmd.Flags().StringVar(&migratorCmdOpts.dbDir, "db-dir", "db", "")
	migratorCmd.Flags().BoolVar(&migratorCmdOpts.debug, "debug", false, "")
	rootCmd.AddCommand(migratorCmd)
}
