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

#include "utils.h"
#include "alloc.h"
#include "oplog.h"
#include "lock.h"
#include "list.h"
#include "pm.h"

#define NR_CLIS_MAX      1024

struct logger_shard {
    struct pm_dev *dev;
    allocator_t *allocator;
    /* clients bind to this shard */
    int nr_clis;
};

struct logger {
    kc_t *kc;

    struct logger_shard *shards;
    int nr_shards;

    size_t lcb_size;

    spinlock_t lock;

    struct logger_cli **clis;
};

struct oplog_ptr {
    union {
        uint64_t raw;
        struct {
            uint64_t cli_id : 16;
            uint64_t off : 48;
        };
    };
};

struct oplog_data {
    op_t op;
    uint32_t key_len;
    uint64_t valp;
    uint64_t depend;
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

    /*
     * @head points to the beginning of log region
     * @tail points to the end of log region
     */
    size_t head, tail;

    struct lcb *lcb;
    size_t lcb_size;

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

logger_t *logger_create(kc_t *kc, int nr_shards, const char *shard_devs[], size_t lcb_size) {
    logger_t *logger;
    int i;

    if (unlikely(nr_shards <= 0)) {
        logger = ERR_PTR(-EINVAL);
        pr_err("invalid nr_shards: %d", nr_shards);
        goto out;
    }

    logger = calloc(1, sizeof(*logger));
    if (unlikely(logger == NULL)) {
        logger = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger");
        goto out;
    }

    logger->kc = kc;

    logger->nr_shards = nr_shards;
    logger->shards = calloc(nr_shards, sizeof(*logger->shards));
    if (unlikely(logger->shards == NULL)) {
        free(logger);
        logger = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger->devs");
        goto out;
    }

    logger->lcb_size = lcb_size;

    for (i = 0; i < nr_shards; i++) {
        logger->shards[i].dev = pm_open_devs(nr_shards, shard_devs);
        if (unlikely(!logger->shards[i].dev)) {
            logger = ERR_PTR(-ENODEV);
            pr_err("failed to open PM device: %s", shard_devs[i]);
            goto out;
        }
        logger->shards[i].allocator = allocator_create(logger->shards[i].dev->size);
        if (unlikely(!logger->shards[i].allocator)) {
            logger = ERR_PTR(-ENOMEM);
            pr_err("failed to create allocator for PM device: %s", shard_devs[i]);
            goto out;
        }
        logger->shards[i].nr_clis = 0;
    }

    logger->clis = calloc(NR_CLIS_MAX, sizeof(*logger->clis));
    if (unlikely(logger->clis == NULL)) {
        logger = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for logger->clis");
        goto out;
    }

    pr_debug(5, "created logger across %d local PM areas, lcb_size=%luB", nr_shards, logger->lcb_size);

out:
    return logger;
}

void logger_destroy(logger_t *logger) {
    free(logger->shards);
    free(logger);
}

static inline bool is_local_socket(int socket) {
    return numa_node_of_cpu(sched_getcpu()) == socket;
}

static inline struct logger_shard *find_cli_shard(logger_t *logger) {
    struct logger_shard *shard;
    int i, nr_clis;

    spin_lock(&logger->lock);

    /* find local logger device with least load */
    shard = NULL;
    nr_clis = INT32_MAX;
    for (i = 0; i < logger->nr_shards; i++) {
        if (logger->shards[i].nr_clis < nr_clis && is_local_socket(logger->shards[i].dev->socket)) {
            shard = &logger->shards[i];
            nr_clis = shard->nr_clis;
        }
    }

    if (unlikely(!shard)) {
        spin_unlock(&logger->lock);
        return NULL;
    }

    shard->nr_clis++;

    spin_unlock(&logger->lock);

    return shard;
}

logger_cli_t *logger_cli_create(logger_t *logger, size_t log_region_size, int id) {
    struct logger_shard *shard;
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

    cli->lcb_size = logger->lcb_size;

    shard = find_cli_shard(logger);
    if (unlikely(!shard)) {
        cli = ERR_PTR(-ENODEV);
        pr_err("failed to find suitable logger shard");
        goto out;
    }

    logs_off = allocator_alloc(shard->allocator, log_region_size);
    if (unlikely(IS_ERR(logs_off))) {
        cli = ERR_PTR(logs_off);
        pr_err("failed to allocate memory for logs: %s", strerror(-PTR_ERR(logs_off)));
        goto out;
    }
    cli->log_region = shard->dev->start + logs_off;
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

oplog_t logger_append(logger_cli_t *logger_cli, op_t op, k_t key, uint64_t valp, oplog_t depend) {
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
            p.raw = ret;
            pr_err("failed to flush lcb");
        } else {
            p.raw = logger_append(logger_cli, op, key, valp, depend);
        }
        goto out;
    }

    /* copy log into LCB */
    log->op = op;
    log->key_len = key.len;
    log->valp = valp;
    log->depend = depend;
    memcpy(log->key, key.key, key.len);

    /* forward tail */
    logger_cli->tail += sizeof(*log) + key.len;

    pr_debug(30, "log append, cli=%d, off=%lu, key=%s, valp=%lx,",
             logger_cli->id, p.off, k_str(logger_cli->logger->kc, key), valp);

out:
    return p.raw;
}

op_t logger_get(logger_cli_t *logger_cli, oplog_t log, k_t *key, uint64_t *valp) {
    struct oplog_ptr o = { .raw = log };
    logger_cli_t *target_cli;
    struct oplog_data *data;
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
        data = target_cli->log_region + o.off;
    } else {
        /* in LCB */
        bonsai_assert(o.off - lcb->start < target_cli->lcb_size);
        data = (void *) lcb->data + (o.off - lcb->start);
    }

    /* get log pointer */
    op = data->op;
    key->key = data->key;
    key->len = data->key_len;
    *valp = data->valp;

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

bool logger_is_stale(logger_cli_t *logger_cli, oplog_t log) {
    struct oplog_ptr o = { .raw = log };
    return logger_cli->logger->clis[o.cli_id]->head > o.off;
}

logger_barrier_t *logger_snap_barrier(logger_cli_t *logger_cli, size_t *total) {
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

    *total = 0;

    for (i = 0; i < NR_CLIS_MAX; i++) {
        cb = &lb->cli_barriers[i];

        cb->cli = logger_cli->logger->clis[i];
        if (!cb->cli) {
            continue;
        }

        /* snapshot current tail */
        cb->head_snap = cb->cli->head;
        cb->tail_snap = cb->cli->tail;

        *total += cb->tail_snap - cb->head_snap;
    }

out:
    return lb;
}

op_t logger_get_within_barrier(logger_barrier_t *barrier, oplog_t log, k_t *key, uint64_t *valp) {
    struct oplog_ptr o = { .raw = log };
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
        op = logger_get(cb->cli, log, key, valp);
        goto out;
    }

    /* read log in DRAM (fast path) */
    data = cb->prefetched + (log - cb->head_snap);
    op = data->op;
    key->key = data->key;
    key->len = data->key_len;
    *valp = data->valp;

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
