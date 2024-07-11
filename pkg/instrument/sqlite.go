package instrument

import (
	"time"
	_ "unsafe"

	"github.com/mattn/go-sqlite3"
)

// #cgo LDFLAGS: -lsqlite3
// #include "extension.h"
import "C"

func SQLite() (*SQLiteMonitor, error) {
	if err := C.sqlite3_instrument(); err != sqlite3.SQLITE_OK {
		return nil, sqlite3.ErrNo(err)
	}

	return &SQLiteMonitor{}, nil
}

type SQLiteMonitor struct{}

func (m *SQLiteMonitor) Fetch() *SQLiteStats { return fetchSQLiteStats(0) }
func (m *SQLiteMonitor) Reset() *SQLiteStats { return fetchSQLiteStats(1) }
func (m *SQLiteMonitor) Done()               { C.sqlite3_uninstrument() }

type SQLiteStats struct {
	PagesCacheWrite uint64
	PagesCacheHit   uint64
	PagesCacheMiss  uint64
	PagesCacheSpill uint64

	ReadTransactionTime  time.Duration
	WriteTransactionTime time.Duration
}

func fetchSQLiteStats(reset C.int) *SQLiteStats {
	var cstats C.sqlite3_stats_t
	C.sqlite3_stats(&cstats, reset)

	return &SQLiteStats{
		PagesCacheWrite:      uint64(cstats.pages_cache_write),
		PagesCacheHit:        uint64(cstats.pages_cache_hit),
		PagesCacheMiss:       uint64(cstats.pages_cache_miss),
		PagesCacheSpill:      uint64(cstats.pages_cache_spill),
		ReadTransactionTime:  time.Duration(cstats.read_txn_time_ns) * time.Nanosecond,
		WriteTransactionTime: time.Duration(cstats.write_txn_time_ns) * time.Nanosecond,
	}
}
