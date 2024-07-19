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
// on all subsequent connections (i.e. existing connections
// will not be instrumented).
// Monitoring must be stopped by calling [StopSQLiteMonitoring].
func StartSQLiteMonitoring() error {
	if err := C.sqlite3_instrument(); err != sqlite3.SQLITE_OK {
		return sqlite3.ErrNo(err)
	}
	return nil
}

// StopSQLiteMonitoring removes instrumentation from sqlite.
// After this call, no new connections are instrumented.
func StopSQLiteMonitoring() { C.sqlite3_deinstrument() }

type SQLiteMetrics struct {
	// These fields follow the same order as the `struct sqlite3_metrics_s`.

	PageCacheWrites uint64
	PageCacheHits   uint64
	PageCacheMisses uint64
	PageCacheSpills uint64

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
		PageCacheWrites:      uint64(stats.page_cache_writes),
		PageCacheHits:        uint64(stats.page_cache_hits),
		PageCacheMisses:      uint64(stats.page_cache_misses),
		PageCacheSpills:      uint64(stats.page_cache_spills),
		TransactionReadTime:  time.Duration(stats.read_txn_time_ns) * time.Nanosecond,
		TransactionWriteTime: time.Duration(stats.write_txn_time_ns) * time.Nanosecond,
	}
}
