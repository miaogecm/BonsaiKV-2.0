/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ log layer
 *
 * Hohai University
 */

#include "oplog.h"

logger_t *logger_create() {

}

void logger_destroy(logger_t *logger) {

}

logger_cli_t *logger_cli_create(logger_t *logger, perf_t *perf) {

}

void logger_cli_destroy(logger_cli_t *logger_cli) {

}

oplog_t logger_append(logger_cli_t *logger_cli, op_t op, const char *key, size_t key_len, void *val) {

}

op_t logger_get(logger_cli_t *logger_cli, oplog_t oplog, const char **key, size_t *key_len, void **val) {

}

oplog_range_t *logger_range_snap(logger_cli_t *logger_cli) {

}

void logger_range_destroy(oplog_range_t *range) {

}

void logger_range_prefetch(oplog_range_t *range) {

}

op_t logger_range_get(oplog_range_t *range, oplog_t log, const char **key, size_t *key_len, void **val) {

}
