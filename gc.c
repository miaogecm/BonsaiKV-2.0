/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ GC/Checkpoint
 *
 * Hohai University
 */

#include "oplog.h"
#include "shim.h"
#include "dset.h"
#include "gc.h"

struct gc_cli {
    perf_t *perf;
    lpma_t *lpma;
    rpma_cli_t *rpma_cli;

    kc_t *kc;

    shim_cli_t *shim_cli;
    logger_cli_t *logger_cli;
    dcli_t *dcli;

    bool exit;
    pthread_t gc_thread;
};

static int ingest_log(gc_cli_t *gc_cli, op_t op, k_t key, void *val, uint64_t hnode, uint64_t cnode) {
    int ret;

    pr_debug(30, "start ingest log with op=%d, k=%s, v=%p", op, k_str(gc_cli->kc, key), val);

    switch (op) {
        case OP_PUT:
            ret = dset_upsert(gc_cli->dcli, key, val, hnode);
            break;

        case OP_DEL:
            ret = dset_delete(gc_cli->dcli, key, hnode);
            break;

        default:
            bonsai_assert(0);
    }


}

static int scanner(uint64_t oplog, uint64_t hnode, uint64_t cnode, void *priv) {
    gc_cli_t *gc_cli = priv;
    void *val;
    k_t key;
    op_t op;
    int ret;

    op = logger_get(gc_cli->logger_cli, oplog, &key, &val);
    if (unlikely(op < 0)) {
        ret = op;
        pr_err("logger_get failed with %d(%s)", ret, strerror(-ret));
        goto out;
    }
    if (unlikely(op > NR_OP_TYPES)) {
        ret = -EINVAL;
        pr_err("invalid op type %d", op);
        goto out;
    }

    ret = ingest_log(gc_cli, op, key, val, hnode, code);

out:
    return ret;
}

static void ingest_until_barrier(gc_cli_t *gc_cli, logger_barrier_t *barrier) {
    /* prefetch logs into in-DRAM array */
    logger_prefetch_until_barrier(barrier);

    /* scan the shim layer to fetch and ingest each op */
    shim_scan(gc_cli->shim_cli, scanner, gc_cli);
}

static void *gc_thread(void *arg) {
    logger_barrier_t *barrier;
    gc_cli_t *gc_cli = arg;
    int nr_logs;

    pr_debug(5, "gc thread enter");

    while (!READ_ONCE(gc_cli->exit)) {
        /* snapshot current log tail */
        barrier = logger_snap_barrier(gc_cli->logger_cli, &nr_logs);
        if (unlikely(nr_logs == 0)) {
            continue;
        }

        pr_debug(20, "gc logs start (nr=%d)", nr_logs, logger_str_barrier(barrier));

        /* ingest logs before barrier */
        ingest_until_barrier(gc_cli, barrier);

        /* gc until current log tail */
        logger_gc_before_barrier(barrier);
        logger_destroy_barrier(barrier);
    }

    pr_debug(5, "gc thread exit");

    return NULL;
}

gc_cli_t *gc_cli_create(perf_t *perf, lpma_t *lpma, rpma_cli_t *rpma_cli, kc_t *kc,
                        logger_cli_t *logger_cli, shim_cli_t *shim_cli, dcli_t *dcli) {
    gc_cli_t *gc_cli;

    gc_cli = calloc(1, sizeof(gc_cli_t));
    if (unlikely(!gc_cli)) {
        gc_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate gc_cli memory");
        goto out;
    }

    gc_cli->perf = perf;
    gc_cli->lpma = lpma;
    gc_cli->rpma_cli = rpma_cli;

    gc_cli->kc = kc;

    gc_cli->logger_cli = logger_cli;
    gc_cli->shim_cli = shim_cli;
    gc_cli->dcli = dcli;

out:
    return gc_cli;
}

void gc_cli_destroy(gc_cli_t *gc_cli) {

}
