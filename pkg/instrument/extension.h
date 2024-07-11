#include <sqlite3.h>
#include <stdint.h>

typedef int sqlite3_error;

typedef struct sqlite3_stats_s {
    uint64_t pages_cache_write;
    uint64_t pages_cache_hit;
    uint64_t pages_cache_miss;
    uint64_t pages_cache_spill;

    uint64_t read_txn_time_ns;
    uint64_t write_txn_time_ns;
} sqlite3_stats_t;

// sqlite3_instrument instruments the connection represented
// by the first argument so it can take measurements of key
// sqlite performance metrics.
// It will replace both the commit and the trace hook for 
// the connection. If an error occurs, the connection is 
// not touched (i.e. neither the trace nor the commit hook
// are replaced).
sqlite3_error sqlite3_instrument();

// sqlite3_uninstrument remove instrumentation connection
// and free the associated context.
// It will not restore previous commit or trace hook.
// If an error occurs, the conneciton is left unchanged
// and the instrumentation context is not freed.
void sqlite3_uninstrument();

sqlite3_error sqlite3_stats(sqlite3_stats_t* stats, int reset);
