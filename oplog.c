/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ log layer
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include <sched.h>
#include <urcu.h>
#include <numa.h>

#include "oplog.h"
#include "lock.h"
#include "list.h"

#define NR_CLIS_MAX      1024

struct logger_dev {
    lpma_t *lpma;
    /* clients bind to this dev */
    int nr_clis;
};

struct logger {
    kc_t *kc;

    struct logger_dev *devs;
    int nr_devs;

    size_t lcb_size;
    int lcb_shift;

    spinlock_t lock;

    struct logger_cli **clis;
};

struct oplog_ptr {
    union {
        uint64_t val;
        struct {
            uint64_t cli_id : 16;
            uint64_t off : 48;
        };
    };
};

struct oplog_data {
    op_t op;
    uint32_t key_len;
    void *val;
    char key[];
};

struct lcb {
    struct rcu_head rcu;
    /* Logs between [@start, @tail) reside in LCB and not persisted */
    size_t start;
    char data[];
};

struct logger_cli {
    logger_t *logger;
    int id;

    perf_t *perf;

    /*
     * @head points to the beginning of log region
     * @tail points to the end of log region
     */
    size_t head, tail;

    struct lcb *lcb;
    size_t lcb_size;
    int lcb_shift;

    /*  */
    void *lcache;

    lpma_t *lpma;
    void *log_region;
    size_t log_region_size;
};

struct logger_cli_barrier {
    struct logger_cli *cli;

    size_t head_snap, tail_snap;

    void *prefetched;
};

struct logger_barrier {
    struct logger_cli *cli;

    struct logger_cli_barrier cli_barriers[];
};

logger_t *logger_create(kc_t *kc, int nr_devs, const char *dev_paths[], int lcb_shift) {
    logger_t *logger;
    lpma_t *lpma;
    int i;

    if (unlikely(nr_devs <= 0)) {
        logger = ERR_PTR(-EINVAL);
        pr_err("invalid nr_devs: %d", nr_devs);
        goto out;
    }

    logger = calloc(1, sizeof(*logger));
    if (unlikely(logger == NULL)) {
        logger = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger");
        goto out;
    }

    logger->kc = kc;

    logger->nr_devs = nr_devs;
    logger->devs = calloc(nr_devs, sizeof(*logger->devs));
    if (unlikely(logger->devs == NULL)) {
        free(logger);
        logger = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger->devs");
        goto out;
    }

    logger->lcb_shift = lcb_shift;
    logger->lcb_size = 1ul << lcb_shift;

    for (i = 0; i < nr_devs; i++) {
        lpma = lpma_create(1, &dev_paths[i], 0);
        if (unlikely(IS_ERR(lpma))) {
            logger = ERR_PTR(lpma);
            pr_err("failed to create lpma: %s", strerror(-PTR_ERR(lpma)));
            goto out;
        }

        logger->devs[i].lpma = lpma;
        logger->devs[i].nr_clis = 0;
    }

    logger->clis = calloc(NR_CLIS_MAX, sizeof(*logger->clis));
    if (unlikely(logger->clis == NULL)) {
        logger = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger->clis");
        goto out;
    }

    pr_debug(5, "created logger across %d PM devices, lcb_size=%luB", nr_devs, logger->lcb_size);

out:
    return logger;
}

void logger_destroy(logger_t *logger) {
    int i;

    for (i = 0; i < logger->nr_devs; i++) {
        lpma_destroy(logger->devs[i].lpma);
    }

    free(logger->devs);
    free(logger);
}

static inline bool is_local_socket(int socket) {
    return numa_node_of_cpu(sched_getcpu()) == socket;
}

static inline lpma_t *find_cli_dev(logger_t *logger) {
    struct logger_dev *dev;
    int i, nr_clis;

    spin_lock(&logger->lock);

    /* find local logger device with least load */
    dev = NULL;
    nr_clis = INT32_MAX;
    for (i = 0; i < logger->nr_devs; i++) {
        if (logger->devs[i].nr_clis < nr_clis && is_local_socket(lpma_socket(logger->devs[i].lpma))) {
            dev = &logger->devs[i];
            nr_clis = dev->nr_clis;
        }
    }

    if (unlikely(!dev)) {
        spin_unlock(&logger->lock);
        return NULL;
    }

    dev->nr_clis++;

    spin_unlock(&logger->lock);

    return dev->lpma;
}

logger_cli_t *logger_cli_create(logger_t *logger, perf_t *perf, int id, size_t log_region_size) {
    logger_cli_t *cli;
    uint64_t logs_off;

    cli = calloc(1, sizeof(*cli));
    if (unlikely(cli == NULL)) {
        cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger_cli");
        goto out;
    }

    cli->logger = logger;
    cli->id = id;

    cli->perf = perf;

    cli->lcb_shift = logger->lcb_shift;
    cli->lcb_size = logger->lcb_size;

    cli->lpma = find_cli_dev(logger);
    if (unlikely(!cli->lpma)) {
        cli = ERR_PTR(-ENODEV);
        pr_err("failed to find suitable logger device");
        goto out;
    }

    logs_off = lpma_alloc(cli->lpma, log_region_size);
    if (unlikely(IS_ERR(logs_off))) {
        cli = ERR_PTR(logs_off);
        pr_err("failed to allocate memory for logs: %s", strerror(-PTR_ERR(logs_off)));
        goto out;
    }
    cli->log_region = lpma_get_ptr(cli->lpma, logs_off);
    cli->log_region_size = log_region_size;

    logger->clis[id] = cli;

    pr_debug(10, "create logger client #%d (log region start=%p, size=%.2lfMB)",
             id, cli->log_region, (double) log_region_size / (1 << 20));

out:
    return cli;
}

void logger_cli_destroy(logger_cli_t *logger_cli) {
    free(logger_cli);
}

static void free_lcb(struct rcu_head *head) {
    struct lcb *lcb = container_of(head, struct lcb, rcu);
    free(lcb);
}

static int flush_lcb(logger_cli_t *logger_cli) {
    struct lcb *old_lcb, *new_lcb;
    size_t size, off;
    int ret = 0;

    /* flush data into pmem */
    off = logger_cli->lcb->start;
    size = logger_cli->tail - off;
    memcpy_nt(logger_cli->log_region + off, logger_cli->lcb, size);
    memory_sfence();

    /* allocate new LCB, we do not overwrite old LCB since some lock-free readers may accessing it */
    new_lcb = malloc(sizeof(struct lcb) + logger_cli->lcb_size);
    new_lcb->start = logger_cli->tail;
    if (unlikely(new_lcb == NULL)) {
        ret = -ENOMEM;
        pr_err("failed to allocate memory for lcb");
        goto out;
    }

    /* delay free old LCB (until no readers see it) */
    old_lcb = logger_cli->lcb;
    call_rcu(&old_lcb->rcu, free_lcb);

    /* replace current LCB */
    rcu_assign_pointer(logger_cli->lcb, new_lcb);

    pr_debug(20, "lcb flush, cli=%d, off=%lu, size=%lu, lcb:%p->%p",
             logger_cli->id, off, size, old_lcb, new_lcb);

out:
    return ret;
}

oplog_t logger_append(logger_cli_t *logger_cli, op_t op, k_t key, void *val) {
    struct oplog_data *log;
    struct oplog_ptr p;
    size_t lcb_used;
    int ret;

    /* generate new log pointer */
    p.cli_id = logger_cli->id;
    p.off = logger_cli->tail;

    lcb_used = logger_cli->tail - logger_cli->lcb->start;
    log = (void *) logger_cli->lcb->data + lcb_used;

    /* special case: LCB full */
    if (unlikely(lcb_used + sizeof(*log) + key.len > logger_cli->lcb_size)) {
        ret = flush_lcb(logger_cli);
        if (unlikely(ret)) {
            p.val = ret;
            pr_err("failed to flush lcb");
        } else {
            p.val = logger_append(logger_cli, op, key, val);
        }
        goto out;
    }

    /* copy log into LCB */
    log->op = op;
    log->key_len = key.len;
    log->val = val;
    memcpy(log->key, key.key, key.len);

    /* forward tail */
    logger_cli->tail += sizeof(*log) + key.len;

    pr_debug(30, "log append, cli=%d, off=%lu, key=%s, val=%p,",
             logger_cli->id, p.off, k_str(logger_cli->logger->kc, key), val);

out:
    return p.val;
}

op_t logger_get(logger_cli_t *logger_cli, oplog_t log, k_t *key, void **val) {
    struct oplog_ptr o = { .val = log };
    logger_cli_t *target_cli;
    struct oplog_data *log;
    struct lcb *lcb;
    op_t op;

    /* get the client of the oplog */
    target_cli = logger_cli->logger->clis[o.cli_id];
    bonsai_assert(target_cli);

    lcb = rcu_dereference(target_cli->lcb);

    /* log in PM or LCB? */
    if (likely(o.off < lcb->start)) {
        /* in PM */
        bonsai_assert(o.off < target_cli->log_region_size);
        log = target_cli->log_region + o.off;
    } else {
        /* in LCB */
        bonsai_assert(o.off - lcb->start < target_cli->lcb_size);
        log = (void *) lcb->data + (o.off - lcb->start);
    }

    /* get log pointer */
    op = log->op;
    key->key = log->key;
    key->len = log->key_len;
    *val = log->val;

out:
    return op;
}

static inline void logger_cpy(logger_cli_t *cli, void *dst, size_t head, size_t tail) {
    struct lcb *lcb;

    lcb = rcu_dereference(cli->lcb);

    /* copy in-PM logs to dst */
    bonsai_assert(lcb->start - head <= cli->log_region_size);
    memcpy(dst, cli->log_region + head, lcb->start - head);

    /* copy in-LCB logs to dst */
    bonsai_assert(tail - lcb->start <= cli->lcb_size);
    memcpy(dst + lcb->start - head, lcb->data, tail - lcb->start);
}

logger_barrier_t *logger_snap_barrier(logger_cli_t *logger_cli) {
    struct logger_cli_barrier *cb;
    logger_barrier_t *lb;
    int i;

    lb = malloc(sizeof(*lb) + sizeof(struct logger_cli_barrier) * NR_CLIS_MAX);
    if (unlikely(lb == NULL)) {
        lb = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger_barrier_t");
        goto out;
    }

    lb->cli = logger_cli;

    for (i = 0; i < NR_CLIS_MAX; i++) {
        cb = &lb->cli_barriers[i];

        cb->cli = logger_cli->logger->clis[i];
        if (!cb->cli) {
            continue;
        }

        /* snapshot current tail */
        cb->head_snap = cb->cli->head;
        cb->tail_snap = cb->cli->tail;
    }

out:
    return lb;
}

op_t logger_get_within_barrier(logger_barrier_t *barrier, oplog_t log, k_t *key, void **val) {
    struct oplog_ptr o = { .val = log };
    struct logger_cli_barrier *cb;
    struct oplog_data *data;
    op_t op;

    cb = &barrier->cli_barriers[o.cli_id];
    if (unlikely(!cb)) {
        /* no corresponding cli barrier */
        op = -ENOENT;
        goto out;
    }

    if (unlikely(log < cb->head_snap || log >= cb->tail_snap)) {
        /* not within range */
        op = -ENOENT;
        goto out;
    }

    if (unlikely(!cb->prefetched)) {
        /* not prefetched, slow path */
        op = logger_get(cb->cli, log, key, val);
        goto out;
    }

    /* read log in DRAM (fast path) */
    data = cb->prefetched + (log - cb->head_snap);
    op = data->op;
    key->key = data->key;
    key->len = data->key_len;
    *val = data->val;

out:
    return op;
}


void logger_destroy_barrier(logger_barrier_t *barrier) {
    struct logger_cli_barrier *cb;
    int i;

    for (i = 0; i < NR_CLIS_MAX; i++) {
        cb = &barrier->cli_barriers[i];
        if (!cb->cli) {
            continue;
        }

        if (cb->prefetched) {
            free(cb->prefetched);
        }
    }

    free(barrier);
}

void logger_prefetch_until_barrier(logger_barrier_t *barrier) {
    struct logger_cli_barrier *cb;
    int i;

    for (i = 0; i < NR_CLIS_MAX; i++) {
        cb = &barrier->cli_barriers[i];
        if (!cb->cli) {
            continue;
        }

        if (unlikely(cb->prefetched)) {
            continue;
        }

        cb->prefetched = malloc(cb->tail_snap - cb->head_snap);
        if (unlikely(!cb->prefetched)) {
            pr_warn("no enough memory for prefetch logs within logger cli barrier");
            continue;
        }

        logger_cpy(cb->cli, cb->prefetched, cb->head_snap, cb->tail_snap);
    }
}

void logger_gc_before_barrier(logger_barrier_t *barrier) {
    struct logger_cli_barrier *cb;
    int i;

    for (i = 0; i < NR_CLIS_MAX; i++) {
        cb = &barrier->cli_barriers[i];
        if (!cb->cli) {
            continue;
        }

        bonsai_assert(cb->cli->head <= cb->tail_snap);
        WRITE_ONCE(cb->cli->head, cb->tail_snap);
    }
}
