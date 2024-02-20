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
#include "lpm.h"

typedef struct gc_cli gc_cli_t;

gc_cli_t *gc_cli_create(perf_t *perf, lpma_t *lpma_cli, rpma_cli_t *rpma_cli, kc_t *kc,
                        logger_cli_t *logger_cli, shim_cli_t *shim_cli, dcli_t *dcli);
void gc_cli_destroy(gc_cli_t *gc_cli);

#endif //GC_H
