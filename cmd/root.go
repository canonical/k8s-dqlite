package cmd

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"

	"github.com/canonical/k8s-dqlite/pkg/server"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
)

var (
	storageDir             string
	listenAddress          string
	enableTLS              bool
	debug                  bool
	enableProfiling        bool
	profilingListenAddress string
	diskMode               bool
	clientSessionCacheSize uint
	enableMetrics          bool
	metricsListenAddress   string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "k8s-dqlite",
	Short: "Dqlite for Kubernetes",
	Long:  `Kubernetes datastore based on dqlite`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		if debug {
			logrus.SetLevel(logrus.TraceLevel)
		}

		if enableProfiling {
			go func() {
				logrus.WithField("address", profilingListenAddress).Print("Enable pprof endpoint")
				http.ListenAndServe(profilingListenAddress, nil)
			}()
		}

		if enableMetrics {
			go func() {
				logrus.WithField("address", metricsListenAddress).Print("Enable metrics endpoint")
				mux := http.NewServeMux()
				mux.Handle("/metrics", promhttp.Handler())
				http.ListenAndServe(metricsListenAddress, mux)
			}()
		}

		server, err := server.New(storageDir, listenAddress, enableTLS, diskMode, clientSessionCacheSize)
		if err != nil {
			logrus.WithError(err).Fatal("Failed to create server")
		}

		ctx, cancel := context.WithCancel(cmd.Context())
		if err := server.Start(ctx); err != nil {
			logrus.WithError(err).Error("Server terminated")
		}

		// Cancel context if we receive an exit signal
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, unix.SIGPWR)
		signal.Notify(ch, unix.SIGINT)
		signal.Notify(ch, unix.SIGQUIT)
		signal.Notify(ch, unix.SIGTERM)

		<-ch
		cancel()

		// Create a separate context with 30 seconds to cleanup
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := server.Shutdown(stopCtx); err != nil {
			logrus.WithError(err).Fatal("Failed to shutdown server")
		}
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the liteCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.Flags().StringVar(&storageDir, "storage-dir", "/var/tmp/k8s-dqlite", "directory with the dqlite datastore")
	rootCmd.Flags().StringVar(&listenAddress, "listen", "tcp://127.0.0.1:12379", "endpoint where dqlite should listen to")
	rootCmd.Flags().BoolVar(&enableTLS, "enable-tls", true, "enable TLS")
	rootCmd.Flags().BoolVar(&debug, "debug", false, "debug logs")
	rootCmd.Flags().BoolVar(&enableProfiling, "profiling", false, "enable debug pprof endpoint")
	rootCmd.Flags().StringVar(&profilingListenAddress, "profiling-listen", "127.0.0.1:4000", "listen address for pprof endpoint")
	rootCmd.Flags().BoolVar(&diskMode, "disk-mode", false, "(experimental) run dqlite store in disk mode")
	rootCmd.Flags().UintVar(&clientSessionCacheSize, "tls-client-session-cache-size", 0, "ClientCacheSession size for dial TLS config")
	rootCmd.Flags().BoolVar(&enableMetrics, "metrics", true, "enable metrics endpoint")
	rootCmd.Flags().StringVar(&metricsListenAddress, "metrics-address", "127.0.0.1:9042", "listen address for metrics endpoint")
}
