/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package app

import (
	"context"
	"fmt"
	"github.com/canonical/k8s-dqlite/app/options"
	"github.com/canonical/kvsql-dqlite/server"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"log"
	"os"
	"os/signal"
	"time"
)

var opts = options.NewOptions()

// liteCmd represents the base command when called without any subcommands
var dqliteCmd = &cobra.Command{
	Use:   "k8s-dqlite",
	Short: "Dqlite for Kubernetes",
	Long: `Kubernetes datastore based on dqlite`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("Starting dqlite")

		server, err := server.New(
			opts.StorageDir,
			opts.ListenEp,
			opts.EnableTls,
			)
		if err != nil {
			log.Fatalf("Failed to start server: %s\n", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		ch := make(chan os.Signal)
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
	if err := dqliteCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize()

	dqliteCmd.Flags().StringVar(&opts.StorageDir, "storage-dir", opts.StorageDir, "directory with the dqlite datastore")
	dqliteCmd.Flags().StringVar(&opts.ListenEp, "listen", opts.ListenEp, "endpoint where dqlite should listen to")
	dqliteCmd.Flags().BoolVar(&opts.EnableTls, "enable-tls", opts.EnableTls, "enable TlS")
}
