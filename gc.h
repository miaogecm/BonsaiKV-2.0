/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ GC/Checkpoint
 *
 * Hohai University
 */

#ifndef GC_H
#define GC_H

#include "rpm.h"

typedef struct gc_cli gc_cli_t;

gc_cli_t *gc_cli_create(kc_t *kc,
                        logger_cli_t *logger_cli, shim_cli_t *shim_cli, dcli_t *dcli,
                        size_t pm_high_watermark, size_t pm_gc_size);
void gc_cli_destroy(gc_cli_t *gc_cli);

#endif //GC_H
