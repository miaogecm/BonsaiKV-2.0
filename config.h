/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Configure
 *
 * Hohai University
 */

#ifndef CONFIG_H
#define CONFIG_H

#include "k.h"

struct rpma_conf;

typedef struct kv_conf kv_conf_t;
typedef struct kv_cli_conf kv_cli_conf_t;
typedef struct kv_rm_conf kv_rm_conf_t;

struct kv_conf {
    /* key class */
    kc_t *kc;

    /* RPMA */
    const char *rpma_host;
    const char *rpma_dev_ip;
    int rpma_interval_us;

    /* log layer */
    size_t logger_lcb_size;
    int logger_nr_shards;
    const char **logger_shard_devs;

    /* data layer */
    size_t dset_bnode_size;
    size_t dset_dnode_size;
    const char *dset_bdev;
    int dset_max_gc_prefetch;

    /* gc config */
    bool auto_gc_logs;
    bool auto_gc_pm;
    size_t min_gc_size;
    size_t pm_high_watermark;
    size_t pm_gc_size;
};

struct kv_cli_conf {
    /* client ID */
    int id;

    /* log region size */
    size_t logger_region_size;
};

struct kv_rm_conf {
    struct rpma_conf *rpma_conf;
};

#endif //CONFIG_H
