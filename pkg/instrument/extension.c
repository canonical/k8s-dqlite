#include "extension.h"

#include <sqlite3.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

static volatile sqlite3_metrics_t global_metrics = {0};


static sqlite3_error auto_instrument_connection(sqlite3 *connection, const char** pzErrMsg, const struct sqlite3_api_routines* pThunk) {
    return sqlite3_trace_v2(connection, SQLITE_TRACE_PROFILE|SQLITE_TRACE_CLOSE, sqlite3_collect_metrics, NULL);
}

sqlite3_error sqlite3_instrument() {
    return sqlite3_auto_extension((void (*)())(auto_instrument_connection));
}

void sqlite3_deinstrument() {
    sqlite3_cancel_auto_extension((void (*)())(auto_instrument_connection));
}

sqlite3_error sqlite3_metrics(sqlite3_metrics_t* metrics, int reset) {
    if (reset) {
        metrics->pages_cache_write = atomic_exchange(&global_metrics.pages_cache_write, 0);
        metrics->pages_cache_hit = atomic_exchange(&global_metrics.pages_cache_hit, 0);
        metrics->pages_cache_miss = atomic_exchange(&global_metrics.pages_cache_miss, 0);
        metrics->pages_cache_miss = atomic_exchange(&global_metrics.pages_cache_spill, 0);
        metrics->read_txn_time_ns = atomic_exchange(&global_metrics.read_txn_time_ns, 0);
        metrics->write_txn_time_ns = atomic_exchange(&global_metrics.write_txn_time_ns, 0);
    } else {
        metrics->pages_cache_write = atomic_load(&global_metrics.pages_cache_write);
        metrics->pages_cache_hit = atomic_load(&global_metrics.pages_cache_hit);
        metrics->pages_cache_miss = atomic_load(&global_metrics.pages_cache_miss);
        metrics->pages_cache_miss = atomic_load(&global_metrics.pages_cache_spill);
        metrics->read_txn_time_ns = atomic_load(&global_metrics.read_txn_time_ns);
        metrics->write_txn_time_ns = atomic_load(&global_metrics.write_txn_time_ns);
    }

    return SQLITE_OK;
}

sqlite3_error sqlite3_collect_metrics(unsigned int event, void *pCtx, void *P, void *X) {
    sqlite3_stmt* stmt;
    sqlite3* connection;

    int64_t stmt_time_ns;
    int cache_hit, cache_miss, cache_write, cache_spill, _;
    
    switch (event) {
    case SQLITE_TRACE_PROFILE:
        stmt = P;
        connection = sqlite3_db_handle(stmt);
        stmt_time_ns = *(sqlite3_int64*)(X);

        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_WRITE, &cache_write, &_, 1);
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_HIT, &cache_hit, &_, 1);
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_MISS, &cache_miss, &_, 1);
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_SPILL, &cache_spill, &_, 1);

        atomic_fetch_add(&global_metrics.pages_cache_write, cache_write);
        atomic_fetch_add(&global_metrics.pages_cache_hit, cache_hit);
        atomic_fetch_add(&global_metrics.pages_cache_miss, cache_miss);
        atomic_fetch_add(&global_metrics.pages_cache_spill, cache_spill);

        if (sqlite3_stmt_readonly(stmt)) {
            atomic_fetch_add(&global_metrics.read_txn_time_ns, stmt_time_ns);
        } else {
            atomic_fetch_add(&global_metrics.write_txn_time_ns, stmt_time_ns);
        }
        return SQLITE_OK;
    case SQLITE_TRACE_CLOSE:
        sqlite3_trace_v2(P, SQLITE_TRACE_PROFILE|SQLITE_TRACE_CLOSE, NULL, NULL);
        return SQLITE_OK;
    default:
        return SQLITE_OK;
    }
}
