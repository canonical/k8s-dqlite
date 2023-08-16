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
	log "github.com/sirupsen/logrus"
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
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "k8s-dqlite",
	Short: "Dqlite for Kubernetes",
	Long:  `Kubernetes datastore based on dqlite`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Starting dqlite")
		if debug {
			log.SetLevel(log.TraceLevel)
		}

		if enableProfiling {
			go func() {
				http.ListenAndServe(profilingListenAddress, nil)
			}()
		}

		server, err := server.New(storageDir, listenAddress, enableTLS, diskMode, clientSessionCacheSize)
		if err != nil {
			log.Fatalf("Failed to start server: %s\n", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, unix.SIGPWR)
		signal.Notify(ch, unix.SIGINT)
		signal.Notify(ch, unix.SIGQUIT)
		signal.Notify(ch, unix.SIGTERM)
		<-ch

		log.Printf("Shutting down\n")
		cancel()
		server.Close(ctx)

		log.Printf("Dqlite stopped\n")

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
}
