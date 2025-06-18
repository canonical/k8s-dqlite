#include "extensions.h"

#include <assert.h>
#include <sqlite3.h>
#include <stdatomic.h>
#include <stdint.h>
#include <string.h>

#define UNUSED(x) (void)(x)

struct vacuum_config_s {
    size_t percent;
    size_t max_pages;
} vacuum_config;

static unsigned int autovacuum_pages_callback(void *pClientData,
                                              const char *zSchema,
                                              unsigned int nDbPage,
                                              unsigned int nFreePage,
                                              unsigned int nBytePerPage) {
    UNUSED(pClientData);
    UNUSED(zSchema);
    UNUSED(nBytePerPage);

    assert(nDbPage >= nFreePage);

    unsigned int keep_amount =
        (nDbPage - nFreePage) * vacuum_config.percent / 100;
    if (nFreePage < keep_amount) {
        return 0;
    }

    unsigned int free_amount = nFreePage - keep_amount;
    if (free_amount > vacuum_config.max_pages) {
        free_amount = vacuum_config.max_pages;
    }
    return free_amount;
}

static sqlite3_error auto_instrument_connection(
    sqlite3 *connection, const char **pzErrMsg,
    const struct sqlite3_api_routines *pThunk) {
    UNUSED(pzErrMsg);
    UNUSED(pThunk);

    return sqlite3_autovacuum_pages(connection, autovacuum_pages_callback, NULL,
                                    NULL);
}

sqlite3_error sqlite3_enable_autovacuum_limit(size_t percent,
                                              size_t max_pages) {
    vacuum_config.percent = percent;
    vacuum_config.max_pages = max_pages;

    return sqlite3_auto_extension((void (*)())(auto_instrument_connection));
}

void sqlite3_disable_autovacuum_limit() {
    sqlite3_cancel_auto_extension((void (*)())(auto_instrument_connection));
}
