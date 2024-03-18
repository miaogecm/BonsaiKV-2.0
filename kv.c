/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Top-layer key-value interface
 *
 * Hohai University
 */

#include <pthread.h>

#include "oplog.h"
#include "lock.h"
#include "list.h"
#include "shim.h"
#include "rpm.h"
#include "kv.h"
#include "gc.h"

#define GC_CLI_ID   4096

struct kv {
    rpma_t *rpma;
    index_t *index;
    shim_t *shim;
    logger_t *logger;
    dset_t *dset;

    spinlock_t lock;
    struct list_head clis;

    kv_cli_t *gc_cli;
    gc_cli_t *gc;
};

struct kv_cli {
    struct list_head head;

    int id;

    rpma_cli_t *rpma_cli;
    shim_cli_t *shim_cli;
    logger_cli_t *logger_cli;
    dcli_t *dcli;
};

struct kv_rm {
    rpma_svr_t *svr;
};

kv_t *kv_create(kv_conf_t *conf) {
    kv_cli_conf_t gc_cli_conf;
    kv_t *kv;

    kv = calloc(1, sizeof(*kv));
    if (unlikely(!kv)) {
        kv = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate kv");
        goto out;
    }

    kv->rpma = rpma_create(conf->rpma_host, conf->rpma_dev_ip, conf->rpma_interval_us);
    if (unlikely(IS_ERR(kv->rpma))) {
        kv = ERR_CAST(kv->rpma);
        pr_err("failed to create rpma");
        goto out;
    }

    kv->index = index_create();
    if (unlikely(IS_ERR(kv->index))) {
        kv = ERR_CAST(kv->index);
        pr_err("failed to create index");
        goto out;
    }

    kv->shim = shim_create(kv->index, conf->kc);
    if (unlikely(IS_ERR(kv->shim))) {
        kv = ERR_CAST(kv->shim);
        pr_err("failed to create shim");
        goto out;
    }

    kv->logger = logger_create(conf->kc, conf->logger_nr_shards, conf->logger_shard_devs, conf->logger_lcb_size);
    if (unlikely(IS_ERR(kv->logger))) {
        kv = ERR_CAST(kv->logger);
        pr_err("failed to create logger");
        goto out;
    }

    kv->dset = dset_create(conf->kc, conf->dset_bnode_size, conf->dset_dnode_size,
                           conf->dset_bdev, kv->rpma, conf->dset_max_gc_prefetch);
    if (unlikely(IS_ERR(kv->dset))) {
        kv = ERR_CAST(kv->dset);
        pr_err("failed to create dset");
        goto out;
    }

    spin_lock_init(&kv->lock);
    INIT_LIST_HEAD(&kv->clis);

    gc_cli_conf.logger_region_size = 0;
    gc_cli_conf.id = GC_CLI_ID;
    kv->gc_cli = kv_cli_create(kv, &gc_cli_conf);
    if (unlikely(IS_ERR(kv->gc_cli))) {
        kv = ERR_CAST(kv->gc_cli);
        pr_err("failed to create gc_cli");
        goto out;
    }
    kv->gc = gc_cli_create(conf->kc,
                           kv->gc_cli->logger_cli, kv->gc_cli->shim_cli, kv->gc_cli->dcli,
                           conf->pm_high_watermark, conf->pm_gc_size);
    if (unlikely(IS_ERR(kv->gc))) {
        kv = ERR_CAST(kv->gc);
        pr_err("failed to create gc");
        goto out;
    }

out:
    return kv;
}

void kv_destroy(kv_t *kv) {
    dset_destroy(kv->dset);
    logger_destroy(kv->logger);
    shim_destroy(kv->shim);
    index_destroy(kv->index);
    free(kv);
}

kv_cli_t *kv_cli_create(kv_t *kv, kv_cli_conf_t *conf) {
    kv_cli_t *kv_cli;

    kv_cli = calloc(1, sizeof(*kv_cli));
    if (unlikely(!kv_cli)) {
        kv_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate kv_cli");
        goto out;
    }

    INIT_LIST_HEAD(&kv_cli->head);
    spin_lock(&kv->lock);
    list_add_tail(&kv_cli->head, &kv->clis);
    spin_unlock(&kv->lock);

    kv_cli->id = conf->id;

    kv_cli->rpma_cli = rpma_cli_create(kv->rpma);
    if (unlikely(IS_ERR(kv_cli->rpma_cli))) {
        kv_cli = ERR_CAST(kv_cli->rpma_cli);
        pr_err("failed to create rpma_cli");
        goto out;
    }

    kv_cli->shim_cli = shim_create_cli(kv->shim, kv_cli->logger_cli);
    if (unlikely(IS_ERR(kv_cli->shim_cli))) {
        kv_cli = ERR_CAST(kv_cli->shim_cli);
        pr_err("failed to create shim_cli");
        goto out;
    }

    kv_cli->logger_cli = logger_cli_create(kv->logger, conf->logger_region_size, conf->id);
    if (unlikely(IS_ERR(kv_cli->logger_cli))) {
        kv_cli = ERR_CAST(kv_cli->logger_cli);
        pr_err("failed to create logger_cli");
        goto out;
    }

    kv_cli->dcli = dcli_create(kv->dset, kv_cli->shim_cli);
    if (unlikely(IS_ERR(kv_cli->dcli))) {
        kv_cli = ERR_CAST(kv_cli->dcli);
        pr_err("failed to create dcli");
        goto out;
    }

    shim_set_dcli(kv_cli->shim_cli, kv_cli->dcli);

out:
    return 0;
}

void kv_cli_destroy(kv_cli_t *kv_cli) {
    dcli_destroy(kv_cli->dcli);
    logger_cli_destroy(kv_cli->logger_cli);
    shim_destroy_cli(kv_cli->shim_cli);
    rpma_cli_destroy(kv_cli->rpma_cli);
    free(kv_cli);
}

int kv_put(kv_cli_t *kv_cli, k_t key, uint64_t valp) {
    oplog_t oplog;
    int ret;

    oplog = logger_append(kv_cli->logger_cli, OP_PUT, key, valp, 0);

    ret = shim_upsert(kv_cli->shim_cli, key, oplog);
    if (unlikely(ret)) {
        pr_err("shim_upsert failed with %d", ret);
    }

out:
    return ret;
}

int kv_get(kv_cli_t *kv_cli, k_t key, uint64_t *valp) {
    return shim_lookup(kv_cli->shim_cli, key, valp);
}

int kv_del(kv_cli_t *kv_cli, k_t key) {
    oplog_t oplog;
    int ret;

    oplog = logger_append(kv_cli->logger_cli, OP_DEL, key, 0, 0);

    ret = shim_upsert(kv_cli->shim_cli, key, oplog);
    if (unlikely(ret)) {
        pr_err("shim_upsert failed with %d", ret);
    }

out:
    return ret;
}

kv_rm_t *kv_rm_create(kv_rm_conf_t *conf) {
    kv_rm_t *kv_rm;

    kv_rm = calloc(1, sizeof(*kv_rm));
    if (unlikely(!kv_rm)) {
        kv_rm = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate kv_rm");
        goto out;
    }

    kv_rm->svr = rpma_svr_create(conf->rpma_conf);
    if (unlikely(IS_ERR(kv_rm->svr))) {
        kv_rm = ERR_CAST(kv_rm->svr);
        pr_err("failed to create rpma_svr");
        goto out;
    }

out:
    return kv_rm;
}

void kv_rm_destroy(kv_rm_t *kv_rm) {
    rpma_svr_destroy(kv_rm->svr);
    free(kv_rm);
}
