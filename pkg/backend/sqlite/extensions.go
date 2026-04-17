package sqlite

// #cgo LDFLAGS: -lsqlite3
// #include "extensions.h"
import "C"

import "github.com/mattn/go-sqlite3"

func init() {
	if err := C.sqlite3_enable_autovacuum_limit(10, 16); err != C.SQLITE_OK {
		panic(sqlite3.ErrNo(err))
	}
}
