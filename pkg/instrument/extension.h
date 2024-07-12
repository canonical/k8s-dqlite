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

// sqlite3_instrument register an auto extension that 
// instruments all new connections to collect key sqlite
// performance metrics. Existing connections will not be 
// affected. This call uses the trace hook to collect data.
// Replacing the hook will remove instrumentation for the
// connection. In this case, it is possible to call 
// [sqlite3_collect_metrics] to keep the instrumentation
// active in a custom trace hook.
sqlite3_error sqlite3_instrument();

// sqlite3_uninstrument remove instrumentation connection
// and free the associated context.
// It will not restore previous commit or trace hook.
// If an error occurs, the conneciton is left unchanged
// and the instrumentation context is not freed.
void sqlite3_uninstrument();

// sqlite3_stats reads copies performance metrics into stats.
// It returns SQLITE_ERROR error if stats is null, SQLITE_OK
// otherwise.
// If reset is != 0, it resets each metric to 0.
sqlite3_error sqlite3_metrics(sqlite3_metrics_t* stats, int reset);

// sqlite3_collect_metrics is a trace hook that collects performance
// metrics during the SQLITE_TRACE_PROFILE event.
sqlite3_error sqlite3_collect_metrics(unsigned int event, void *pCtx, void *P, void *X);
