#include "strong_checkpoint.h"

#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>

#define UNUSED(x) (void)(x)


static sqlite3_error strong_checkpoint_hook(void *pCtx,sqlite3 *db, const char *zDb, int pages) {
    UNUSED(pCtx);

    if (pages > 5000) {
        return sqlite3_wal_checkpoint_v2(db, zDb, SQLITE_CHECKPOINT_TRUNCATE, 0, 0);
    } else if (pages > 1000) {
        return sqlite3_wal_checkpoint_v2(db, zDb, SQLITE_CHECKPOINT_PASSIVE, 0, 0);
    }

    return SQLITE_OK;
}

static sqlite3_error auto_instrument_connection(sqlite3 *connection, const char** pzErrMsg, const struct sqlite3_api_routines* pThunk) {
    (void)pzErrMsg;
    (void)pThunk;

    sqlite3_wal_hook(connection, strong_checkpoint_hook, 0);

    return SQLITE_OK;
}

sqlite3_error sqlite3_strong_checkpoint_register() {
    return sqlite3_auto_extension((void (*)())(auto_instrument_connection));
}
