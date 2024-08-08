/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Hohai University
 */

#include <stdio.h>

#include "../kv.h"
#include "../utils.h"
#include "../hash.h"

static int key_cmp(k_t a, k_t b) {
    uint64_t x = *(uint64_t *) a.key, y = *(uint64_t *) b.key;
    bonsai_assert(a.len == b.len == sizeof(uint64_t));
    if (x == y) {
        return 0;
    }
    return x < y ? -1 : 1;
}

static uint64_t key_hash(k_t key, int nbits) {
    bonsai_assert(key.len == sizeof(uint64_t));
    return hash_64(*(uint64_t *) key.key, nbits);
}

static int key_dump(k_t key, char *buf, int buflen) {
    bonsai_assert(key.len == sizeof(uint64_t));
    return snprintf(buf, buflen, "%lu", *(uint64_t *) key.key);
}

static uint64_t min_key = 0, max_key = UINT64_MAX;
static kc_t kc = {
    .min = { (char *) &min_key, sizeof(uint64_t) },
    .max = { (char *) &max_key, sizeof(uint64_t) },
    .max_len = sizeof(uint64_t),
    .cmp = key_cmp,
    .hash = key_hash,
    .dump = key_dump
};

static kv_conf_t kv_conf = {
    .kc = &kc,

    .logger_lcb_size = 4096,
    .logger_nr_shards = 6,
    .logger_shard_devs = (const char *[]) {
        "log_pm0", "log_pm1", "log_pm2",
        "log_pm3", "log_pm4", "log_pm5"
    },

    .dset_bnode_size = 2048,
    .dset_dnode_size = 8192,
    .dset_bdev = "data_pm",
    .dset_max_gc_prefetch = 4,

    .auto_gc_logs = true,
    .auto_gc_pm = false,
    .min_gc_size = 16 * 1024
};

static kv_cli_conf_t kv_cli_conf = {
    .id = 0,

    .rpma_dev_ip = "192.168.1.1",
    .rpma_host = "192.168.1.3:8888",

    .logger_region_size = 1024 * 1024 * 1024ul,
};

static const char *dump_file_path = "dump.json";
static int nr_entries = 65536;

static void dump_to_file(kv_cli_t *cli) {
    cJSON *dump;
    FILE *fh;

    fh = fopen(dump_file_path, "w");
    if (unlikely(!fh)) {
        pr_err("failed to open file %s: %s", dump_file_path, strerror(errno));
        abort();
    }

    dump = kv_dump(cli);
    if (unlikely(!dump)) {
        pr_err("failed to dump kv");
        abort();
    }

    fprintf(fh, "%s\n", cJSON_Print(dump));

    cJSON_Delete(dump);

    fclose(fh);
}

static void run_seq_test(kv_cli_t *cli) {
    uint64_t k = 0;
    int i, ret;

    for (i = 0; i < nr_entries; i++, k++) {
        ret = kv_put(cli, (k_t) { (char *) &k, sizeof(uint64_t) }, 0);
        if (unlikely(ret)) {
            pr_err("failed to insert key %lu: %s", k, strerror(-ret));
            abort();
        }
    }

    dump_to_file(cli);
}

int main() {
    kv_cli_t *cli;
    kv_t *kv;

    kv = kv_create(&kv_conf);

    cli = kv_cli_create(kv, &kv_cli_conf);

    run_seq_test(cli);

    kv_cli_destroy(cli);

    kv_destroy(kv);

    return 0;
}
