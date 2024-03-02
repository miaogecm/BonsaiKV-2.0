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

    kc_t *kc;

    shim_cli_t *shim_cli;
    logger_cli_t *logger_cli;
    dcli_t *dcli;

    bool exit;
    pthread_t gc_thread;

    size_t pm_high_watermark, pm_gc_size;
};

static int ingest_log(gc_cli_t *gc_cli, op_t op, dgroup_t dgroup, k_t key, uint64_t valp) {
    int ret;

    pr_debug(30, "start ingest log with op=%d, k=%s, v=%lx", op, k_str(gc_cli->kc, key), valp);

    switch (op) {
        case OP_PUT:
            ret = dset_upsert(gc_cli->dcli, dgroup, key, valp);
            break;

        case OP_DEL:
            ret = dset_delete(gc_cli->dcli, dgroup, key);
            break;

        default:
            bonsai_assert(0);
    }

    return ret;
}

static int scanner(uint64_t oplog, dgroup_t dgroup, void *priv) {
    gc_cli_t *gc_cli = priv;
    size_t gc_size;
    uint64_t valp;
    k_t key;
    op_t op;
    int ret;

    op = logger_get(gc_cli->logger_cli, oplog, &key, &valp);
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

    ret = ingest_log(gc_cli, op, dgroup, key, valp);
    if (unlikely(ret)) {
        pr_err("ingest_log failed with %d(%s)", ret, strerror(-ret));
        goto out;
    }

    /* invoke GC from LPM to RPM when LPM too large */
    if (unlikely(dset_get_pm_utilization(gc_cli->dcli) > gc_cli->pm_high_watermark)) {
        gc_size = gc_cli->pm_gc_size;
        ret = dset_gc(gc_cli->dcli, &gc_size);
        if (unlikely(ret)) {
            pr_err("dset_gc failed with %d(%s)", ret, strerror(-ret));
        }
    }

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
    size_t total;

    pr_debug(5, "gc thread enter");

    while (!READ_ONCE(gc_cli->exit)) {
        /* snapshot current log tail */
        barrier = logger_snap_barrier(gc_cli->logger_cli, &total);
        if (unlikely(total == 0)) {
            continue;
        }

        pr_debug(20, "gc logs start (size=%lu)", total);

        /* ingest logs before barrier */
        ingest_until_barrier(gc_cli, barrier);

        /* gc until current log tail */
        logger_gc_before_barrier(barrier);
        logger_destroy_barrier(barrier);
    }

    pr_debug(5, "gc thread exit");

    return NULL;
}

gc_cli_t *gc_cli_create(perf_t *perf, kc_t *kc,
                        logger_cli_t *logger_cli, shim_cli_t *shim_cli, dcli_t *dcli,
                        size_t pm_high_watermark, size_t pm_gc_size) {
    gc_cli_t *gc_cli;

    gc_cli = calloc(1, sizeof(gc_cli_t));
    if (unlikely(!gc_cli)) {
        gc_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate gc_cli memory");
        goto out;
    }

    gc_cli->perf = perf;

    gc_cli->kc = kc;

    gc_cli->logger_cli = logger_cli;
    gc_cli->shim_cli = shim_cli;
    gc_cli->dcli = dcli;

    gc_cli->pm_high_watermark = pm_high_watermark;
    gc_cli->pm_gc_size = pm_gc_size;

out:
    return gc_cli;
}

void gc_cli_destroy(gc_cli_t *gc_cli) {
    free(gc_cli);
}
