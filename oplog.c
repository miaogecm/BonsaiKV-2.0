/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ log layer
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include <sched.h>
#include <numa.h>

#include "oplog.h"
#include "lock.h"

#define NR_CLIS_MAX      1024

struct logger_dev {
    lpma_t *lpma;
    /* clients bind to this dev */
    int nr_clis;
};

struct logger {
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

struct logger_cli {
    logger_t *logger;
    int id;

    perf_t *perf;

    /*
     * @head points to the beginning of log region
     * @tail points to the end of log region
     */
    size_t head, tail;

    /*
     * Logs between [ALIGN_DOWN(tail, lcb_size), tail) are in LCB and not persisted.
     */
    size_t lcb_size;
    int lcb_shift;
    void *lcb;

    /* @tail and @lcb should be updated atomically. */
    seqcount_t seq;

    lpma_t *lpma;
    void *log_region;
};

logger_t *logger_create(int nr_devs, const char *dev_paths[], int lcb_shift) {
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

    seqcount_init(&cli->seq);

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

    logger->clis[id] = cli;

    pr_debug(10, "create logger client #%d (log region start=%p, size=%.2lfMB)",
             id, cli->log_region, (double) log_region_size / (1 << 20));

out:
    return cli;
}

void logger_cli_destroy(logger_cli_t *logger_cli) {
    free(logger_cli);
}

static inline void flush_lcb(logger_cli_t *logger_cli) {

    logger_cli->tail
}

oplog_t logger_append(logger_cli_t *logger_cli, op_t op, const char *key, size_t key_len, void *val) {
    size_t ptail, lcb_used;
    struct oplog_data *log;
    struct oplog_ptr p;

    p.cli_id = logger_cli->id;
    p.off = logger_cli->tail;

    ptail = ALIGN_DOWN(logger_cli->tail, logger_cli->lcb_size);
    lcb_used = logger_cli->tail - ptail;
    log = logger_cli->lcb + lcb_used;

    if (unlikely(lcb_used + sizeof(*log) + key_len > logger_cli->lcb_size)) {
        flush_lcb(logger_cli);
    }

    log->op = op;
    log->key_len = key_len;
    log->val = val;
    memcpy(log->key, key, key_len);

    return p.val;
}

op_t logger_get(logger_cli_t *logger_cli, oplog_t oplog, const char **key, size_t *key_len, void **val) {

}
