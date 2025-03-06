package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

/*
#include "dqlite.h"
#include "sqlite3.h"
#include "uv.h"

void print_dqlite_library_versions() {
	printf("sqlite: %s (compiled), %s (linked)\n",
		SQLITE_VERSION,
		sqlite3_version
	);

	printf(
		"dqlite: %d.%d.%d (compiled), %d.%d.%d (linked)\n",
		DQLITE_VERSION_NUMBER / (100*100),
		(DQLITE_VERSION_NUMBER / 100) % 100,
		DQLITE_VERSION_NUMBER % 100,
		dqlite_version_number() / (100 * 100),
		(dqlite_version_number() / 100) % 100,
		dqlite_version_number() % 100
	);

	printf("libuv: %d.%d.%d (compiled), %s (linked)\n",
		UV_VERSION_MAJOR, UV_VERSION_MINOR, UV_VERSION_PATCH,
		uv_version_string()
	);
}

*/
import "C"

func init() {
	rootCmd.AddCommand(&cobra.Command{
		Use:  "version",
		RunE: printVersions,
	})
}

func printVersions(cmd *cobra.Command, args []string) error {
	fmt.Println("go:", runtime.Version())
	C.print_dqlite_library_versions()
	return nil
}
