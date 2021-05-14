package main

import (
	"github.com/spf13/pflag"
	cliflag "k8s.io/component-base/cli/flag"
	"math/rand"
	"time"

	"github.com/canonical/k8s-dqlite/app"
	"k8s.io/component-base/logs"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	// TODO: once we switch everything over to Cobra commands, we can go back to calling
	// utilflag.InitFlags() (by removing its pflag.Parse() call). For now, we have to set the
	// normalize func and add the go flag set by hand.
	pflag.CommandLine.SetNormalizeFunc(cliflag.WordSepNormalizeFunc)
	// utilflag.InitFlags()
	logs.InitLogs()
	defer logs.FlushLogs()

	app.Execute()
}
