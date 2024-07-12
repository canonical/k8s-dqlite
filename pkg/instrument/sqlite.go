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
// will not be instrumented). If monitoring is already in place, this
// call has no effects.
// Monitoring can be stopped using [StopSQLiteMonitoring].
func StartSQLiteMonitoring() error {
	if err := C.sqlite3_instrument(); err != sqlite3.SQLITE_OK {
		return sqlite3.ErrNo(err)
	}
	return nil
}

// StopSQLiteMonitoring removes instrumentation from sqlite.
// Existing connection will still be insturmented until they
// are closed. If monitoring is not in place, this call has
// no effects.
func StopSQLiteMonitoring() { C.sqlite3_uninstrument() }

type SQLiteMetrics struct {
	PagesCacheWrite uint64
	PagesCacheHit   uint64
	PagesCacheMiss  uint64
	PagesCacheSpill uint64

	ReadTransactionTime  time.Duration
	WriteTransactionTime time.Duration
}

// ResetSQLiteMetrics returns the current metrics and
// resets their value to 0.
func ResetSQLiteMetrics() *SQLiteMetrics { return fetchSQLiteMetrics(1) }

// FetchSQLiteMetrics returns the current metrics
func FetchSQLiteMetrics() *SQLiteMetrics { return fetchSQLiteMetrics(0) }

func fetchSQLiteMetrics(reset C.int) *SQLiteMetrics {
	var cstats C.sqlite3_metrics_t
	C.sqlite3_metrics(&cstats, reset)

	return &SQLiteMetrics{
		PagesCacheWrite:      uint64(cstats.pages_cache_write),
		PagesCacheHit:        uint64(cstats.pages_cache_hit),
		PagesCacheMiss:       uint64(cstats.pages_cache_miss),
		PagesCacheSpill:      uint64(cstats.pages_cache_spill),
		ReadTransactionTime:  time.Duration(cstats.read_txn_time_ns) * time.Nanosecond,
		WriteTransactionTime: time.Duration(cstats.write_txn_time_ns) * time.Nanosecond,
	}
}
