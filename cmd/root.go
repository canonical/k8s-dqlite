package cmd

import (
	"context"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
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
	rootCmdOpts struct {
		dir                       string
		listen                    string
		tls                       bool
		debug                     bool
		logLevel                  string
		profiling                 bool
		profilingAddress          string
		profilingDir              string
		diskMode                  bool
		clientSessionCacheSize    uint
		minTLSVersion             string
		metrics                   bool
		metricsAddress            string
		otel                      bool
		otelAddress               string
		otelDir                   string
		otelSpanNameFilter        string
		otelSpanMinDurationFilter string

		connectionPoolConfig server.ConnectionPoolConfig

		watchAvailableStorageInterval time.Duration
		watchAvailableStorageMinBytes uint64
		lowAvailableStorageAction     string

		etcdMode                    bool
		watchQueryTimeout           time.Duration
		watchProgressNotifyInterval time.Duration
	}

	rootCmd = &cobra.Command{
		Use:   "k8s-dqlite",
		Short: "Dqlite for Kubernetes",
		Long:  `Kubernetes datastore based on dqlite`,
		// Uncomment the following line if your bare application
		// has an action associated with it:
		Run: func(cmd *cobra.Command, args []string) {
			if rootCmdOpts.logLevel != "" {
				level, err := logrus.ParseLevel(rootCmdOpts.logLevel)
				if err != nil {
					logrus.WithError(err).Fatalf("failed to parse log level %s", rootCmdOpts.logLevel)
				} else {
					logrus.SetLevel(level)
				}
			}

			if rootCmdOpts.debug {
				logrus.SetLevel(logrus.TraceLevel)
			}

			if rootCmdOpts.profiling {
				go func() {
					logrus.WithField("address", rootCmdOpts.profilingAddress).Print("enable pprof endpoint")
					http.ListenAndServe(rootCmdOpts.profilingAddress, nil)
				}()

				if rootCmdOpts.profilingDir != "" {
					f, err := os.Create(filepath.Join(rootCmdOpts.profilingDir, "cpu_profile.raw"))
					if err != nil {
						logrus.WithError(err).Fatal("failed to create cpu profiling file.")
					}
					defer f.Close()
					err = pprof.StartCPUProfile(f)
					if err != nil {
						logrus.WithError(err).Fatal("failed to setup cpu profiling.")
					}
					defer pprof.StopCPUProfile()
				}
			}

			var otelShutdown func(context.Context) error

			if rootCmdOpts.otel {
				var err error
				if rootCmdOpts.otelAddress == "" && rootCmdOpts.otelDir == "" {
					logrus.Fatal("no otel address or directory specified.")
				}
				if rootCmdOpts.otelDir != "" {
					logrus.WithField("otel-dir", rootCmdOpts.otelDir).Print("dumping otel data to local directory.")
					if rootCmdOpts.otelAddress != "" {
						logrus.Warning("only one otel exporter allowed, ignoring otel endpoint.")
						rootCmdOpts.otelAddress = ""
					}
				} else {
					logrus.WithField("address", rootCmdOpts.otelAddress).Print("enabling otel endpoint")
				}

				var otelSpanMinDuration time.Duration
				if rootCmdOpts.otelSpanMinDurationFilter != "" {
					otelSpanMinDuration, err = time.ParseDuration(rootCmdOpts.otelSpanMinDurationFilter)
					if err != nil {
						logrus.Warningf("could not parse otel span duration %s: %v, defaulting to 10ms.", rootCmdOpts.otelSpanMinDurationFilter, err)
						otelSpanMinDuration = time.Duration(10) * time.Millisecond
					}
				}

				otelShutdown = setupOTelSDK(cmd.Context(), rootCmdOpts.otelAddress,
					rootCmdOpts.otelDir,
					rootCmdOpts.otelSpanNameFilter,
					otelSpanMinDuration)
			}

			var metricsServer *http.Server

			if rootCmdOpts.metrics {
				metricsServer := &http.Server{
					Addr:    rootCmdOpts.metricsAddress,
					Handler: http.NewServeMux(),
				}
				mux, ok := metricsServer.Handler.(*http.ServeMux)
				if !ok {
					logrus.Fatal("failed to create metrics endpoint")
				} else {
					mux.Handle("/metrics", promhttp.Handler())

					go func() {
						logrus.WithField("address", rootCmdOpts.metricsAddress).Print("enable metrics endpoint")
						if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
							logrus.WithError(err).Fatal("failed to start metrics endpoint")
						}
					}()
				}
			}

			instance, err := server.New(
				rootCmdOpts.dir,
				rootCmdOpts.listen,
				rootCmdOpts.tls,
				rootCmdOpts.diskMode,
				rootCmdOpts.clientSessionCacheSize,
				rootCmdOpts.minTLSVersion,
				rootCmdOpts.watchAvailableStorageInterval,
				rootCmdOpts.watchAvailableStorageMinBytes,
				rootCmdOpts.lowAvailableStorageAction,
				&rootCmdOpts.connectionPoolConfig,
				rootCmdOpts.watchQueryTimeout,
				rootCmdOpts.watchProgressNotifyInterval,
			)
			if err != nil {
				logrus.WithError(err).Fatal("failed to create server")
			}

			ctx, cancel := context.WithCancel(cmd.Context())
			if err := instance.Start(ctx); err != nil {
				logrus.WithError(err).Fatal("server failed to start")
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
				logrus.WithError(err).Fatal("failed to shutdown server")
			}
			if rootCmdOpts.otel && otelShutdown != nil {
				if err := otelShutdown(stopCtx); err != nil {
					logrus.WithError(err).Warning("failed to shutdown otel sdk")
				}
			}
			if metricsServer != nil {
				if err := metricsServer.Shutdown(stopCtx); err != nil {
					logrus.WithError(err).Fatal("failed to shutdown metrics endpoint")
				}
			}
		},
	}
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the liteCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&rootCmdOpts.dir, "storage-dir", "/var/tmp/k8s-dqlite", "directory with the dqlite datastore")
	rootCmd.Flags().StringVar(&rootCmdOpts.listen, "listen", "tcp://127.0.0.1:12379", "endpoint where dqlite should listen to")
	rootCmd.Flags().BoolVar(&rootCmdOpts.tls, "enable-tls", true, "enable TLS")
	rootCmd.Flags().BoolVar(&rootCmdOpts.debug, "debug", false, "debug logs")
	rootCmd.Flags().MarkDeprecated("debug", "use --log-level debug instead")
	rootCmd.Flags().StringVar(&rootCmdOpts.logLevel, "log-level", "error", "set the log level")
	rootCmd.Flags().BoolVar(&rootCmdOpts.profiling, "profiling", false, "enable debug pprof endpoint")
	rootCmd.Flags().StringVar(&rootCmdOpts.profilingAddress, "profiling-listen", "127.0.0.1:4000", "listen address for pprof endpoint")
	rootCmd.Flags().StringVar(&rootCmdOpts.profilingDir, "profiling-dir", "", "directory to use for profiling data")
	rootCmd.Flags().BoolVar(&rootCmdOpts.diskMode, "disk-mode", false, "(experimental) run dqlite store in disk mode")
	rootCmd.Flags().UintVar(&rootCmdOpts.clientSessionCacheSize, "tls-client-session-cache-size", 0, "ClientCacheSession size for dial TLS config")
	rootCmd.Flags().StringVar(&rootCmdOpts.minTLSVersion, "min-tls-version", "tls12", "Minimum TLS version for dqlite endpoint (tls10|tls11|tls12|tls13). Default is tls12")
	rootCmd.Flags().BoolVar(&rootCmdOpts.metrics, "metrics", false, "enable metrics endpoint")
	rootCmd.Flags().BoolVar(&rootCmdOpts.otel, "otel", false, "enable traces endpoint")
	rootCmd.Flags().StringVar(&rootCmdOpts.otelAddress, "otel-listen", "127.0.0.1:4317", "listen address for OpenTelemetry endpoint")
	rootCmd.Flags().StringVar(&rootCmdOpts.otelDir, "otel-dir", "", "dump OpenTelemetry metrics in the specified directory")
	rootCmd.Flags().StringVar(&rootCmdOpts.otelSpanNameFilter, "otel-span-name-filter", "", "drop OpenTelemetry trace spans that do not match the specified regex filter")
	rootCmd.Flags().StringVar(&rootCmdOpts.otelSpanMinDurationFilter, "otel-span-min-duration-filter", "", "drop OpenTelemetry trace spans below the specified time interval (e.g. 10ms)")
	rootCmd.Flags().StringVar(&rootCmdOpts.metricsAddress, "metrics-listen", "127.0.0.1:9042", "listen address for metrics endpoint")
	rootCmd.Flags().IntVar(&rootCmdOpts.connectionPoolConfig.MaxIdle, "datastore-max-idle-connections", 5, "Maximum number of idle connections retained by datastore. If value = 0, the system default will be used. If value < 0, idle connections will not be reused.")
	rootCmd.Flags().IntVar(&rootCmdOpts.connectionPoolConfig.MaxOpen, "datastore-max-open-connections", 5, "Maximum number of open connections used by datastore. If value <= 0, then there is no limit")
	rootCmd.Flags().DurationVar(&rootCmdOpts.connectionPoolConfig.MaxLifetime, "datastore-connection-max-lifetime", 60*time.Second, "Maximum amount of time a connection may be reused. If value <= 0, then there is no limit.")
	rootCmd.Flags().DurationVar(&rootCmdOpts.connectionPoolConfig.MaxIdleTime, "datastore-connection-max-idle-time", 0*time.Second, "Maximum amount of time a connection may be idle before being closed. If value <= 0, then there is no limit.")
	rootCmd.Flags().DurationVar(&rootCmdOpts.watchAvailableStorageInterval, "watch-storage-available-size-interval", 5*time.Second, "Interval to check if the disk is running low on space. Set to 0 to disable the periodic disk size check")
	rootCmd.Flags().Uint64Var(&rootCmdOpts.watchAvailableStorageMinBytes, "watch-storage-available-size-min-bytes", 10*1024*1024, "Minimum required available disk size (in bytes) to continue operation. If available disk space gets below this threshold, then the --low-available-storage-action is performed")
	rootCmd.Flags().StringVar(&rootCmdOpts.lowAvailableStorageAction, "low-available-storage-action", "none", "Action to perform in case the available storage is low. One of (none|handover|terminate). none means no action is performed. handover means the dqlite node will handover its leadership role, if any. terminate means this dqlite node will shutdown")
	rootCmd.Flags().DurationVar(&rootCmdOpts.watchQueryTimeout, "watch-query-timeout", 20*time.Second, "Timeout for querying events in the watch poll loop. If timeout is reached, the poll loop will be re-triggered. The minimum value is 5 seconds.")
	rootCmd.Flags().DurationVar(&rootCmdOpts.watchProgressNotifyInterval, "watch-progress-notify-interval", 5*time.Second, "Interval between periodic watch progress notifications. Default is 5s to ensure support for watch progress notifications.")

	rootCmd.AddCommand(&cobra.Command{
		Use:  "version",
		RunE: func(cmd *cobra.Command, args []string) error { return printVersions() },
	})
}
