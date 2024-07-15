package instrument

import (
	"time"
	_ "unsafe"

	"github.com/mattn/go-sqlite3"
)

// #cgo LDFLAGS: -lsqlite3
// #include "extension.h"
import "C"

// StartSQLiteMonitoring instruments sqlite to collect metrics
// on all connection opened after the call (i.e. existing connections
// will not be instrumented).
// Monitoring must be stopped by calling [StopSQLiteMonitoring].
func StartSQLiteMonitoring() error {
	if err := C.sqlite3_instrument(); err != sqlite3.SQLITE_OK {
		return sqlite3.ErrNo(err)
	}
	return nil
}

// StopSQLiteMonitoring removes instrumentation from sqlite.
// Existing connections will still be insturmented until they
// are closed.
func StopSQLiteMonitoring() { C.sqlite3_deinstrument() }

type SQLiteMetrics struct {
	// These fields follow the same order as the `struct sqlite3_metrics_s`.

	PagesCacheWrite uint64
	PagesCacheHit   uint64
	PagesCacheMiss  uint64
	PagesCacheSpill uint64

	TransactionReadTime  time.Duration
	TransactionWriteTime time.Duration
}

// ResetSQLiteMetrics returns the current metrics and
// resets their value to 0.
func ResetSQLiteMetrics() *SQLiteMetrics {
	var cstats C.sqlite3_metrics_t
	C.sqlite3_metrics(&cstats, 1)

	return convertMetrics(&cstats)
}

// FetchSQLiteMetrics returns the current metrics.
func FetchSQLiteMetrics() *SQLiteMetrics {
	var stats C.sqlite3_metrics_t
	C.sqlite3_metrics(&stats, 0)

	return convertMetrics(&stats)
}

func convertMetrics(stats *C.sqlite3_metrics_t) *SQLiteMetrics {
	return &SQLiteMetrics{
		PagesCacheWrite:      uint64(stats.pages_cache_write),
		PagesCacheHit:        uint64(stats.pages_cache_hit),
		PagesCacheMiss:       uint64(stats.pages_cache_miss),
		PagesCacheSpill:      uint64(stats.pages_cache_spill),
		TransactionReadTime:  time.Duration(stats.read_txn_time_ns) * time.Nanosecond,
		TransactionWriteTime: time.Duration(stats.write_txn_time_ns) * time.Nanosecond,
	}
}
