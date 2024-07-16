#include <sqlite3.h>
#include <stdint.h>

typedef int sqlite3_error;

typedef struct sqlite3_metrics_s {
    uint64_t pages_cache_write;
    uint64_t pages_cache_hit;
    uint64_t pages_cache_miss;
    uint64_t pages_cache_spill;

    uint64_t read_txn_time_ns;
    uint64_t write_txn_time_ns;
} sqlite3_metrics_t;

// sqlite3_instrument registers an auto extension which 
// instruments all new connections, causing them to collect
// sqlite performance metrics. Existing connections will will
// remain unaffected.
//
// This call inserts a trace hook to collect metrics. If this
// would clash with another hook, see the [sqlite3_collect_metrics]
// function instead.
sqlite3_error sqlite3_instrument();

// sqlite3_deinstrument stops instrumenting new connections.
// Existing connections are left unchanged.
void sqlite3_deinstrument();

// sqlite3_metrics copies the current performance metrics into
// its first argument.
// If reset is != 0, it resets the metrics.
sqlite3_error sqlite3_metrics(sqlite3_metrics_t* metrics, int reset);

// sqlite3_collect_metrics can be called to collect performance
// metrics during a SQLITE_TRACE_PROFILE event.
sqlite3_error sqlite3_collect_metrics(unsigned int event, void *pCtx, void *P, void *X);
