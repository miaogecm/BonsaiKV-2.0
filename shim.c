/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ shim layer
 *
 * Hohai University
 */

#include "shim.h"
#include "lock.h"
#include "bitmap.h"

#define INODE_FANOUT    46

struct shim {
    index_t *index;
};

struct shim_cli {

};

/* Each inode has 8 cache lines */
struct inode {
    uint32_t validmap;
    uint32_t deleted;

    inode_t *next;

    void *hnode;
    void *cnode;

    uint8_t fgprt[INODE_FANOUT];
    uint16_t rfence_len;

    spinlock_t lock;
    seqcount_t seq;

    void *logs[INODE_FANOUT];

    char rfence[];
};

shim_t *shim_create(index_t *index) {
    shim_t *shim;

    shim = calloc(1, sizeof(*shim));
    if (unlikely(!shim)) {
        shim = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate shim memory");
        goto out;
    }

    shim->index = index;

out:
    return shim;
}

void shim_destroy(shim_t *shim) {

}

shim_cli_t *shim_create_cli(shim_t *shim, perf_t *perf) {

}

void shim_destroy_cli(shim_cli_t *shim_cli) {

}

int shim_upsert(shim_cli_t *shim_cli, const char *key, size_t key_len, void *val) {

}

int shim_remove(shim_cli_t *shim_cli, const char *key, size_t key_len) {

}

void *shim_lookup(shim_cli_t *shim_cli, const char *key, size_t key_len) {

}
