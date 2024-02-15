/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ GC/Checkpoint
 *
 * Hohai University
 */

#include "gc.h"

struct gc_cli {
    perf_t *perf;
    rpm_pool_t *pool;

    bool exit;
    pthread_t gc_thread;
};

static void *gc_thread(void *arg) {
    gc_cli_t *gc_cli = arg;

    pr_debug(5, "gc thread enter");

    while (!READ_ONCE(gc_cli->exit)) {

    }

    pr_debug(5, "gc thread exit");

    return NULL;
}

gc_cli_t *gc_cli_create(perf_t *perf, rpm_pool_t *pool) {
    gc_cli_t *gc_cli;

    gc_cli = calloc(1, sizeof(gc_cli_t));
    if (unlikely(!gc_cli)) {
        gc_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate gc_cli memory");
        goto out;
    }

    gc_cli->perf = perf;
    gc_cli->pool = pool;

out:
    return gc_cli;
}

void gc_cli_destroy(gc_cli_t *gc_cli) {

}
