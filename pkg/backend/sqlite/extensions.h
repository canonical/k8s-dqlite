#include <memory.h>  // for size_t
#include <sqlite3.h>
#include <stdint.h>

typedef int sqlite3_error;

// sqlite3_enable_autovacuum_limit limits the amount of autovacuumed
// pages to a percentage of the total database size. This is useful
// to decide on a compromise between performance and resource usage
//  - percent: percentage of database size to keep, so that the databse
//    so that the databse does not need to grow again
//  - max_pages: maximum number of pages to autovacuum
sqlite3_error sqlite3_enable_autovacuum_limit(size_t percent, size_t max_pages);

// sqlite3_disable_autovacuum_limit disables the autovacuum limit enabled
// by @sqlite3_enable_autovacuum_limit.
void sqlite3_disable_autovacuum_limit();
