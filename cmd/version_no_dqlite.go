//go:build !dqlite

package cmd

import "fmt"

func printVersions() error {
	return fmt.Errorf("dqlite is not supported, compile with \"-tags dqlite\"")
}
