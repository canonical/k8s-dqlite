#include "extension.h"

#include <sqlite3.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

static volatile sqlite3_stats_t global = {0};
    
static sqlite3_error collect_profile_stats(unsigned int event, void *pCtx, void *P, void *X) {
    if (event == SQLITE_TRACE_PROFILE) {
        sqlite3_stmt* stmt = P;
        sqlite3* connection = sqlite3_db_handle(stmt);

        int64_t stmt_time_ns = *(sqlite3_int64*)(X);
        int cache_hit, cache_miss, cache_write, cache_spill, _;
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_HIT, &cache_hit, &_, 1);
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_MISS, &cache_miss, &_, 1);
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_WRITE, &cache_write, &_, 1);
        sqlite3_db_status(connection, SQLITE_DBSTATUS_CACHE_SPILL, &cache_spill, &_, 1);

        atomic_fetch_add(&global.pages_cache_hit, cache_hit);
        atomic_fetch_add(&global.pages_cache_miss, cache_miss);
        atomic_fetch_add(&global.pages_cache_write, cache_write);
        atomic_fetch_add(&global.pages_cache_spill, cache_spill);

        if (sqlite3_stmt_readonly(stmt)) {
            atomic_fetch_add(&global.read_txn_time_ns, stmt_time_ns);
        } else {
            atomic_fetch_add(&global.write_txn_time_ns, stmt_time_ns);
        }
        
        return SQLITE_OK;
    }
    
    if (event == SQLITE_TRACE_CLOSE) {
        sqlite3_trace_v2(P, SQLITE_TRACE_PROFILE|SQLITE_TRACE_CLOSE, NULL, NULL);
    }
    
    return SQLITE_OK;
}

sqlite3_error sqlite3_stats(sqlite3_stats_t* stats, int reset) {
    if (stats == NULL) {
        return SQLITE_ERROR;
    }

    if (reset) {
        stats->pages_cache_write = atomic_exchange(&global.pages_cache_write, 0);
        stats->pages_cache_hit = atomic_exchange(&global.pages_cache_hit, 0);
        stats->pages_cache_miss = atomic_exchange(&global.pages_cache_miss, 0);
        stats->pages_cache_miss = atomic_exchange(&global.pages_cache_spill, 0);
        stats->read_txn_time_ns = atomic_exchange(&global.read_txn_time_ns, 0);
        stats->write_txn_time_ns = atomic_exchange(&global.write_txn_time_ns, 0);
    } else {
        stats->pages_cache_write = atomic_load(&global.pages_cache_write);
        stats->pages_cache_hit = atomic_load(&global.pages_cache_hit);
        stats->pages_cache_miss = atomic_load(&global.pages_cache_miss);
        stats->pages_cache_miss = atomic_load(&global.pages_cache_spill);
        stats->read_txn_time_ns = atomic_load(&global.read_txn_time_ns);
        stats->write_txn_time_ns = atomic_load(&global.write_txn_time_ns);
    }

    return SQLITE_OK;
}

static sqlite3_error auto_instrument_connection(sqlite3 *connection, const char** pzErrMsg, const struct sqlite3_api_routines* pThunk) {
    return sqlite3_trace_v2(connection, SQLITE_TRACE_PROFILE|SQLITE_TRACE_CLOSE, collect_profile_stats, NULL);
}

sqlite3_error sqlite3_instrument() {
    return sqlite3_auto_extension((void (*)())(auto_instrument_connection));
}

void sqlite3_uninstrument(sqlite3 *connection) {
    sqlite3_cancel_auto_extension((void (*)())(auto_instrument_connection));
}
