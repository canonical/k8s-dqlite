package cmd

import (
	"context"
	"crypto/tls"
	"errors"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"runtime/pprof"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

var (
	storageDir                  string
	listenAddress               string
	diskModeEnabled             bool
	connectionPoolConfig        = &server.ConnectionPoolConfig{}
	watchQueryTimeout           time.Duration
	watchProgressNotifyInterval time.Duration

	debugEnabled bool

	tlsEnabled    bool
	tlsMinVersion = NewEnumFlag(tls.VersionTLS12, []EnumValue[uint16]{
		{"tls10", tls.VersionTLS10},
		{"tls11", tls.VersionTLS11},
		{"tls12", tls.VersionTLS12},
		{"tls13", tls.VersionTLS13},
		{"", tls.VersionTLS12},
	})
	tlsClientSessionCacheSize uint

	lowAvailableStorageAction = NewEnumFlag(server.LowDiskActionNone, []EnumValue[server.LowDiskAction]{
		{"none", server.LowDiskActionNone},
		{"handover", server.LowDiskActionHandover},
		{"terminate", server.LowDiskActionTerminate},
	})
	watchAvailableStorageInterval time.Duration
	watchAvailableStorageMinBytes uint64

	profilingEnabled bool
	profilingAddress string
	profilingDir     string

	metricsEnabled bool
	metricsAddress string
	otelEnabled    bool
	otelAddress    string
)

func init() {
	rootCmd.Flags().StringVar(&storageDir, "storage-dir", "/var/tmp/k8s-dqlite", "directory with the dqlite datastore")
	rootCmd.Flags().StringVar(&listenAddress, "listen", "tcp://127.0.0.1:12379", "endpoint where dqlite should listen to")
	rootCmd.Flags().BoolVar(&diskModeEnabled, "disk-mode", false, "(experimental) run dqlite store in disk mode")
	rootCmd.Flags().DurationVar(&watchQueryTimeout, "watch-query-timeout", 20*time.Second, "Timeout for querying events in the watch poll loop. If timeout is reached, the poll loop will be re-triggered. The minimum value is 5 seconds.")
	rootCmd.Flags().DurationVar(&watchProgressNotifyInterval, "watch-progress-notify-interval", 5*time.Second, "Interval between periodic watch progress notifications. Default is 5s to ensure support for watch progress notifications.")

	rootCmd.Flags().BoolVar(&tlsEnabled, "enable-tls", true, "enable TLS")
	rootCmd.Flags().Var(tlsMinVersion, "min-tls-version", "Minimum TLS version for dqlite endpoint (tls10|tls11|tls12|tls13). Default is tls12")
	rootCmd.Flags().UintVar(&tlsClientSessionCacheSize, "tls-client-session-cache-size", 0, "ClientCacheSession size for dial TLS config")

	rootCmd.Flags().IntVar(&connectionPoolConfig.MaxIdle, "datastore-max-idle-connections", 5, "Maximum number of idle connections retained by datastore. If value = 0, the system default will be used. If value < 0, idle connections will not be reused.")
	rootCmd.Flags().IntVar(&connectionPoolConfig.MaxOpen, "datastore-max-open-connections", 5, "Maximum number of open connections used by datastore. If value <= 0, then there is no limit")
	rootCmd.Flags().DurationVar(&connectionPoolConfig.MaxLifetime, "datastore-connection-max-lifetime", 60*time.Second, "Maximum amount of time a connection may be reused. If value <= 0, then there is no limit.")
	rootCmd.Flags().DurationVar(&connectionPoolConfig.MaxIdleTime, "datastore-connection-max-idle-time", 0*time.Second, "Maximum amount of time a connection may be idle before being closed. If value <= 0, then there is no limit.")

	rootCmd.Flags().Var(lowAvailableStorageAction, "low-available-storage-action", "Action to perform in case the available storage is low. One of (none|handover|terminate). none means no action is performed. handover means the dqlite node will handover its leadership role, if any. terminate means this dqlite node will shutdown")
	rootCmd.Flags().DurationVar(&watchAvailableStorageInterval, "watch-storage-available-size-interval", 5*time.Second, "Interval to check if the disk is running low on space. Set to 0 to disable the periodic disk size check")
	rootCmd.Flags().Uint64Var(&watchAvailableStorageMinBytes, "watch-storage-available-size-min-bytes", 10*1024*1024, "Minimum required available disk size (in bytes) to continue operation. If available disk space gets below this threshold, then the --low-available-storage-action is performed")

	rootCmd.Flags().BoolVar(&debugEnabled, "debug", false, "debug logs")

	rootCmd.Flags().BoolVar(&profilingEnabled, "profiling", false, "enable debug pprof endpoint")
	rootCmd.Flags().StringVar(&profilingAddress, "profiling-listen", "127.0.0.1:4000", "listen address for pprof endpoint")
	rootCmd.Flags().StringVar(&profilingDir, "profiling-dir", "", "directory to use for profiling data")

	rootCmd.Flags().BoolVar(&metricsEnabled, "metrics", false, "enable metrics endpoint")
	rootCmd.Flags().BoolVar(&otelEnabled, "otel", false, "enable traces endpoint")
	rootCmd.Flags().StringVar(&otelAddress, "otel-listen", "127.0.0.1:4317", "listen address for OpenTelemetry endpoint")
	rootCmd.Flags().StringVar(&metricsAddress, "metrics-listen", "127.0.0.1:9042", "listen address for metrics endpoint")
}

var rootCmd = &cobra.Command{
	Use:   "k8s-dqlite",
	Short: "Dqlite for Kubernetes",
	Long:  `Kubernetes datastore based on dqlite`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		if debugEnabled {
			logrus.SetLevel(logrus.TraceLevel)
		}

		if profilingEnabled {
			go func() {
				logrus.WithField("address", profilingAddress).Print("Enable pprof endpoint")
				http.ListenAndServe(profilingAddress, nil)
			}()

			if profilingDir != "" {
				f, err := os.Create(filepath.Join(profilingDir, "cpu_profile.raw"))
				if err != nil {
					logrus.WithError(err).Fatal("Failed to create cpu profiling file.")
				}
				defer f.Close()
				err = pprof.StartCPUProfile(f)
				if err != nil {
					logrus.WithError(err).Fatal("Failed to setup cpu profiling.")
				}
				defer pprof.StopCPUProfile()
			}
		}

		var otelShutdown func(context.Context) error

		if otelEnabled {
			var err error
			logrus.WithField("address", otelAddress).Print("Enable otel endpoint")
			otelShutdown, err = setupOTelSDK(cmd.Context(), otelAddress)
			if err != nil {
				logrus.WithError(err).Warning("Failed to setup OpenTelemetry SDK")
			}
		}

		var metricsServer *http.Server

		if metricsEnabled {
			metricsServer := &http.Server{
				Addr:    metricsAddress,
				Handler: http.NewServeMux(),
			}
			mux, ok := metricsServer.Handler.(*http.ServeMux)
			if !ok {
				logrus.Fatal("Failed to create metrics endpoint")
			} else {
				mux.Handle("/metrics", promhttp.Handler())

				go func() {
					logrus.WithField("address", metricsAddress).Print("Enable metrics endpoint")
					if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
						logrus.WithError(err).Fatal("Failed to start metrics endpoint")
					}
				}()
			}
		}

		var tlsConfig *server.TlsConfig
		if tlsEnabled {
			tlsConfig = &server.TlsConfig{
				CertFile:               path.Join(storageDir, "cluster.crt"),
				KeyFile:                path.Join(storageDir, "cluster.key"),
				MinTlsVersion:          tlsMinVersion.Get(),
				ClientSessionCacheSize: int(tlsClientSessionCacheSize),
			}
		}

		instance, err := server.New(&server.ServerConfig{
			StorageDir:    storageDir,
			ListenAddress: listenAddress,
			DiskMode:      diskModeEnabled,

			LowDiskThresholdBytes: watchAvailableStorageMinBytes,
			LowDiskCheckInterval:  watchAvailableStorageInterval,
			LowDiskAction:         lowAvailableStorageAction.Get(),

			CompactInterval:   5 * time.Minute,
			PollInterval:      1 * time.Second,
			WatchQueryTimeout: watchQueryTimeout,
			NotifyInterval:    watchProgressNotifyInterval,

			ConnectionPoolConfig: connectionPoolConfig,
			TlsConfig:            tlsConfig,
		})
		if err != nil {
			logrus.WithError(err).Fatal("Failed to create server")
		}

		ctx, cancel := context.WithCancel(cmd.Context())
		if err := instance.Start(ctx); err != nil {
			logrus.WithError(err).Fatal("Server failed to start")
		}

		// Cancel context if we receive an exit signal
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, unix.SIGPWR)
		signal.Notify(ch, unix.SIGINT)
		signal.Notify(ch, unix.SIGQUIT)
		signal.Notify(ch, unix.SIGTERM)

		select {
		case <-ch:
		case <-instance.MustStop():
		}
		cancel()

		// Create a separate context with 30 seconds to cleanup
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := instance.Shutdown(stopCtx); err != nil {
			logrus.WithError(err).Fatal("Failed to shutdown server")
		}
		if otelEnabled && otelShutdown != nil {
			err = errors.Join(err, otelShutdown(stopCtx))
			if err != nil {
				logrus.WithError(err).Warning("Failed to shutdown OpenTelemetry SDK")
			}
		}
		if metricsServer != nil {
			if err := metricsServer.Shutdown(stopCtx); err != nil {
				logrus.WithError(err).Fatal("Failed to shutdown metrics endpoint")
			}
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the liteCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
