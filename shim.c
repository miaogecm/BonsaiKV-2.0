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
#include "kv.h"

#define INODE_FANOUT    46

struct shim {
    index_t *index;
    shim_indexer logi, hnodei, cnodei;
    void *logi_ctx, *hnodei_ctx, *cnodei_ctx;
};

struct shim_cli {
    shim_t *shim;

    /* cache frequently-accessed fields in @shim (to reduce pointer chasing) */
    index_t *index;
    shim_indexer logi, hnodei, cnodei;
    void *logi_ctx, *hnodei_ctx, *cnodei_ctx;

    perf_t *perf;
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

void shim_set_logi(shim_t *shim, shim_indexer logi, void *logi_ctx) {
    shim->logi = logi;
    shim->logi_ctx = logi_ctx;
}

void shim_set_hnodei(shim_t *shim, shim_indexer hnodei, void *hnodei_ctx) {
    shim->hnodei = hnodei;
    shim->hnodei_ctx = hnodei_ctx;
}

void shim_set_cnodei(shim_t *shim, shim_indexer cnodei, void *cnodei_ctx) {
    shim->cnodei = cnodei;
    shim->cnodei_ctx = cnodei_ctx;
}

void shim_destroy(shim_t *shim) {
    index_destroy(shim->index);
    free(shim);
}

shim_cli_t *shim_create_cli(shim_t *shim, perf_t *perf) {
    shim_cli_t *shim_cli;

    shim_cli = calloc(1, sizeof(*shim_cli));
    if (unlikely(!shim_cli)) {
        shim_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate shim_cli memory");
        goto out;
    }

    shim_cli->shim = shim;

    shim_cli->index = shim->index;
    shim_cli->logi = shim->logi;
    shim_cli->hnodei = shim->hnodei;
    shim_cli->cnodei = shim->cnodei;
    shim_cli->logi_ctx = shim->logi_ctx;
    shim_cli->hnodei_ctx = shim->hnodei_ctx;
    shim_cli->cnodei_ctx = shim->cnodei_ctx;

    shim_cli->perf = perf;

out:
    return shim_cli;
}

void shim_destroy_cli(shim_cli_t *shim_cli) {
    free(shim_cli);
}

static inline bool key_within_rfence(inode_t *inode, const char *key, size_t key_len) {
    return !inode->next || memncmp(key, key_len, inode->rfence, inode->rfence_len) < 0;
}

static inline inode_t *iget_unlocked(shim_cli_t *shim_cli, const char *key, size_t key_len) {
    index_t *index = shim_cli->index;
    inode_t *inode;

    inode = index_find_first_ge(index, key, key_len);

    return inode;
}

static inline inode_t *iget_locked(shim_cli_t *shim_cli, const char *key, size_t key_len) {
    inode_t *inode, *next;

reget:
    inode = iget_unlocked(shim_cli, key, key_len);
    spin_lock(&inode->lock);
    if (unlikely(inode->deleted)) {
        spin_unlock(&inode->lock);
        goto reget;
    }

    while (unlikely(!key_within_rfence(inode, key, key_len))) {
        next = inode->next;
        bonsai_assert(next);
        spin_lock(&next->lock);
        /*
         * Don't worry. @target must be still alive now, as deletion needs
         * to lock both inode and its predecessor. We're holding the lock
         * of @target's predecessor.
         */
        spin_unlock(&inode->lock);
        inode = next;
    }

    return 0;
}

static inline void i_gc(shim_cli_t *shim_cli, inode_t *inode) {
}

static inline void i_split(shim_cli_t *shim_cli, inode_t *inode) {

}

/* TODO: SIMD-optimize */
static inline void *search_log(shim_cli_t *shim_cli, inode_t *inode, const char *key, size_t key_len, int *pos) {
    uint8_t fgprt = kv_key_fingerprint(key, key_len);
    void *val = ERR_PTR(-ENOENT);
    int i;

    *pos = INODE_FANOUT;

    for (i = 0; i < INODE_FANOUT; i++) {
        if (inode->fgprt[i] != fgprt) {
            continue;
        }

        val = shim_cli->logi(inode->logs[i], key, key_len, shim_cli->logi_ctx);
        if (unlikely(val == ERR_PTR(-ENOENT))) {
            continue;
        }

        *pos = i;
        break;
    }

    return val;
}

static inline void *search_hnode(shim_cli_t *shim_cli, inode_t *inode, const char *key, size_t key_len) {
    return shim_cli->hnodei(inode->hnode, key, key_len, shim_cli->hnodei_ctx);
}

static inline void *search_cnode(shim_cli_t *shim_cli, inode_t *inode, const char *key, size_t key_len) {
    return shim_cli->cnodei(inode->cnode, key, key_len, shim_cli->cnodei_ctx);
}

int shim_upsert(shim_cli_t *shim_cli, const char *key, size_t key_len, void *log) {
    unsigned long validmap;
    inode_t *inode;
    int pos, ret;
    void *val;

    inode = iget_locked(shim_cli, key, key_len);

    validmap = inode->validmap;

    val = search_log(shim_cli, inode, key, key_len, &pos);
    if (unlikely(!IS_ERR(val))) {
        /* Key exists, update */
        ret = -EEXIST;
    } else if (val == ERR_PTR(-ENOENT)) {
        pos = find_first_zero_bit(&validmap, INODE_FANOUT);

        if (unlikely(pos == INODE_FANOUT)) {
            /* TODO: add split and gc code */
            /* Inode full, need to split. */
            i_split(shim_cli, inode);

            validmap = inode->validmap;
            pos = find_first_zero_bit(&validmap, INODE_FANOUT);
            bonsai_assert(pos < INODE_FANOUT);
        }

        __set_bit(pos, &validmap);

        ret = 0;
    } else {
        ret = PTR_ERR(val);
        goto out;
    }

    inode->logs[pos] = log;
    inode->fgprt[pos] = kv_key_fingerprint(key, key_len);
    barrier();

    inode->validmap = validmap;

    spin_unlock(&inode->lock);

out:
    return ret;
}

void *shim_lookup(shim_cli_t *shim_cli, const char *key, size_t key_len) {
    inode_t *inode, *next;
    char rfence_buf[256];
    size_t rfence_len;
    unsigned int seq;
    void *val;
    int pos;

    inode = iget_unlocked(shim_cli, key, key_len);

retry:
    seq = read_seqcount_begin(&inode->seq);

    rfence_len = inode->rfence_len;
    memcpy(rfence_buf, inode->rfence, rfence_len);
    next = inode->next;

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    if (unlikely(memncmp(key, key_len, rfence_buf, rfence_len) >= 0)) {
        inode = next;
        goto retry;
    }

    val = search_log(shim_cli, inode, key, key_len, &pos);

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    /* collaborative tiered lookup */
    if (val == ERR_PTR(-ENOENT)) {
        val = search_hnode(shim_cli, inode, key, key_len);
        if (val == ERR_PTR(-ENOENT)) {
            val = search_cnode(shim_cli, inode, key, key_len);
        }
    }

    return val;
}
