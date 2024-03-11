/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ log layer
 *
 * Hohai University
 */

#ifndef OPLOG_H
#define OPLOG_H

#include "perf.h"
#include "k.h"

#include <unistd.h>
#include <stdint.h>

typedef enum {
    OP_PUT = 0,
    OP_DEL,
    NR_OP_TYPES
} op_t;
typedef struct logger logger_t;
typedef struct logger_cli logger_cli_t;
typedef struct logger_barrier logger_barrier_t;
typedef uint64_t oplog_t;

static const char *op_str[] = {
    [OP_PUT] = "put",
    [OP_DEL] = "del"
};

logger_t *logger_create(kc_t *kc, int nr_shards, lpma_t **lpmas, size_t lcb_size);
void logger_destroy(logger_t *logger);
logger_cli_t *logger_cli_create(logger_t *logger, perf_t *perf, size_t log_region_size, int id);
void logger_cli_destroy(logger_cli_t *logger_cli);

oplog_t logger_append(logger_cli_t *logger_cli, op_t op, k_t key, uint64_t valp, oplog_t depend);
op_t logger_get(logger_cli_t *logger_cli, oplog_t log, k_t *key, uint64_t *valp);
void logger_prefetch(logger_cli_t *logger_cli, oplog_t log);

logger_barrier_t *logger_snap_barrier(logger_cli_t *logger_cli, size_t *total);
op_t logger_get_within_barrier(logger_barrier_t *barrier, oplog_t log, k_t *key, uint64_t *valp);
void logger_prefetch_until_barrier(logger_barrier_t *barrier);
void logger_gc_before_barrier(logger_barrier_t *barrier);
void logger_destroy_barrier(logger_barrier_t *barrier);

#endif // OPLOG_H
