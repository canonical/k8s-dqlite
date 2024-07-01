//go:build dqlite

package cmd

import (
	"fmt"
	"runtime"
)

/*
#include "dqlite.h"
#include "raft.h"
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

	// TODO(neoaggelos): dynamic libraft does not have raft_version_number() symbol
	// printf("raft: %d.%d.%d (compiled), %d.%d.%d (linked)\n",
	// 	RAFT_VERSION_NUMBER / (100*100),
	// 	(RAFT_VERSION_NUMBER / 100) % 100,
	// 	RAFT_VERSION_NUMBER % 100,
	// 	raft_version_number() / (100 * 100),
	// 	(raft_version_number() / 100) % 100,
	// 	raft_version_number() % 100
	// );

	printf("libuv: %d.%d.%d (compiled), %s (linked)\n",
		UV_VERSION_MAJOR, UV_VERSION_MINOR, UV_VERSION_PATCH,
		uv_version_string()
	);
}

*/
import "C"

func printVersions() error {
	fmt.Println("go:", runtime.Version())
	C.print_dqlite_library_versions()
	return nil
}
