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
// affected.
// This call uses the trace hook to collect data. Replacing
// the hook will remove instrumentation for the connection. 
// In this case, it is possible to call [sqlite3_collect_metrics]
// to keep the instrumentation active in a custom trace hook.
sqlite3_error sqlite3_instrument();

// sqlite3_deinstrument removes instrumentation connection
// and free the associated context.
// It will not restore previous commit or trace hook.
// If an error occurs, the conneciton is left unchanged
// and the instrumentation context is not freed.
void sqlite3_deinstrument();

// sqlite3_metrics copies the current performance metrics into its first argument.
// If reset is != 0, it resets each metric to 0.
sqlite3_error sqlite3_metrics(sqlite3_metrics_t* metrics, int reset);

// sqlite3_collect_metrics is a trace hook that collects performance
// metrics during the SQLITE_TRACE_PROFILE event.
sqlite3_error sqlite3_collect_metrics(unsigned int event, void *pCtx, void *P, void *X);
