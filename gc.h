/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ GC/Checkpoint
 *
 * Hohai University
 */

#ifndef GC_H
#define GC_H

#include "perf.h"
#include "rpm.h"

typedef struct gc_cli gc_cli_t;

gc_cli_t *gc_cli_create(perf_t *perf, rpm_pool_t *pool);
void gc_cli_destroy(gc_cli_t *gc_cli);

#endif //GC_H
