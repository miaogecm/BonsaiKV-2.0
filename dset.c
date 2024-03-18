/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ scalable data layer
 *
 * The core data structure exposed by data layer is "dset" (a collection/set of nodes across different
 * storage tiers). This data structure relies on an upper layer index to map a key to its corresponding
 * nodes at different storage tiers. The nodes corresponding to a key is called as "node group" (dgroup).
 *
 * Hohai University
 */

#define _GNU_SOURCE

#define DSET_SOURCE

#include <urcu.h>

#include "atomic.h"
#include "alloc.h"
#include "dset.h"
#include "shim.h"
#include "pm.h"
#include "k.h"

#define BNULL           (-1ul)
#define TOMBSTONE       (-1ul)

/*
 * bnode/dnode = mnode(meta node) + enode(entry node) + fnode(fence node)
 */

typedef struct dgroup {
    size_t bnode;
    rpma_ptr_t dnode;
} dgroup_t;

struct entry {
    uint64_t valp;
    uint32_t k_len;
    uint32_t reserve;
    char key[];
};

struct mnode {
    union {
        struct {
            rpma_ptr_t dnext, dprev;
        };
        struct {
            size_t bnext, bprev;
        };
    };
    int nr_ents;
    bool ref;
    uint32_t lfence_len, rfence_len;
    uint8_t fgprt[];
};

struct enode {
    struct entry entries[0];
};

struct fnode {
    char fences[0];
};

struct dset {
    kc_t *kc;

    size_t sentinel_bnode;
    rpma_ptr_t sentinel_dnode;
    bool sentinel_created;

    size_t bnode_size, dnode_size;

    struct pm_dev *bdev;
    allocator_t *ba;
    rpma_t *rpma;

    size_t pm_utilization;

    size_t pivot_bnode;

    int max_gc_prefetch;
};

struct dcli {
    dset_t *dset;

    struct pm_dev *bdev;
    rpma_cli_t *rpma_cli;

    kc_t *kc;

    size_t bnode_size, bmnode_size;
    size_t dnode_size, dstrip_size;
    int bfanout, dfanout;

    unsigned seed;

    shim_cli_t *shim_cli;
};

dset_t *dset_create(kc_t *kc,
                    size_t bnode_size, size_t dnode_size,
                    const char *bdev, rpma_t *rpma,
                    int max_gc_prefetch) {
    dset_t *dset;

    dset = calloc(1, sizeof(*dset));
    if (unlikely(!dset)) {
        dset = ERR_PTR(-ENOMEM);
        pr_err("failed to alloc memory for dset struct");
        goto out;
    }

    dset->kc = kc;

    dset->bnode_size = bnode_size;
    dset->dnode_size = dnode_size;

    dset->bdev = pm_open_devs(1, &bdev);
    if (unlikely(IS_ERR(dset->bdev))) {
        dset = ERR_PTR(PTR_ERR(dset->bdev));
        pr_err("failed to open PM device %s: %s", bdev, strerror(-PTR_ERR(dset->bdev)));
        goto out;
    }
    dset->ba = allocator_create(dset->bdev->size);
    if (unlikely(IS_ERR(dset->ba))) {
        dset = ERR_PTR(PTR_ERR(dset->ba));
        pr_err("failed to create allocator for PM device %s: %s", bdev, strerror(-PTR_ERR(dset->ba)));
        goto out;
    }

    dset->rpma = rpma;

    dset->max_gc_prefetch = max_gc_prefetch;

    pr_debug(5, "dset created, bnode size: %lu, dnode size: %lu", bnode_size, dnode_size);

out:
    return dset;
}

void dset_destroy(dset_t *dset) {
    free(dset);
}

static inline uint8_t get_fgprt(dcli_t *dcli, k_t k) {
    return k_hash(dcli->kc, k) & 0xff;
}

static inline void *boff2ptr(dcli_t *dcli, size_t off) {
    if (off == BNULL) {
        return NULL;
    }
    bonsai_assert(off < dcli->bdev->size);
    return dcli->bdev->start + off;
}

static inline size_t bptr2off(dcli_t *dcli, void *ptr) {
    if (ptr == NULL) {
        return BNULL;
    }
    return ptr - dcli->bdev->start;
}

static inline void bread(dcli_t *dcli, void *buf, size_t len, size_t off) {
    memcpy(buf, boff2ptr(dcli, off), len);
}

static inline void bwrite(dcli_t *dcli, const void *buf, size_t len, size_t off) {
    memcpy(boff2ptr(dcli, off), buf, len);
}

static inline void *balloc(dcli_t *dcli, size_t size) {
    size_t off = allocator_alloc(dcli->dset->ba, size);
    return IS_ERR(off) ? ERR_PTR(off) : dcli->dset->bdev->start + off;
}

static inline void bfree(dcli_t *dcli, void *ptr, size_t size) {
    allocator_free(dcli->dset->ba, bptr2off(dcli, ptr), size);
}

static inline k_t e_key(dcli_t *dcli, const struct entry *de) {
    return (k_t) { .key = de->key, .len = de->k_len };
}

static inline int cmp_entry(dcli_t *dcli, const struct entry *d1, const struct entry *d2) {
    return k_cmp(dcli->kc, e_key(dcli, d1), e_key(dcli, d2));
}

static inline size_t sizeof_entry(dcli_t *dcli) {
    return dcli->kc->max_len + sizeof(struct entry);
}

static inline struct fnode *get_bfnode(dcli_t *dcli, struct mnode *mnode) {
    return (void *) mnode + dcli->bnode_size;
}

static inline struct enode *get_benode(dcli_t *dcli, struct mnode *mnode) {
    return (void *) mnode + sizeof(struct mnode) + dcli->bmnode_size;
}

static inline struct fnode *get_dfnode(dcli_t *dcli, struct mnode *mnode) {
    return (void *) mnode + dcli->dnode_size;
}

static inline struct enode *get_denode(dcli_t *dcli, struct mnode *mnode) {
    return (void *) mnode + sizeof(struct mnode) + dcli->dstrip_size;
}

static inline rpma_ptr_t get_dfnodep(dcli_t *dcli, rpma_ptr_t dnode) {
    return RPMA_PTR_OFF(dnode, dcli->dnode_size);
}

static inline rpma_ptr_t get_denodep(dcli_t *dcli, rpma_ptr_t dnode) {
    return RPMA_PTR_OFF(dnode, sizeof(struct mnode) + dcli->dstrip_size);
}

static inline rpma_ptr_t get_dentryp(dcli_t *dcli, rpma_ptr_t dnode, int nr) {
    return RPMA_PTR_OFF(dnode, sizeof(struct mnode) + dcli->dstrip_size + nr * sizeof_entry(dcli));
}

static inline k_t get_lfence(dcli_t *dcli, struct mnode *mnode, struct fnode *fnode) {
    return (k_t) { .key = fnode->fences, .len = mnode->lfence_len };
}

static inline k_t get_rfence(dcli_t *dcli, struct mnode *mnode, struct fnode *fnode) {
    return (k_t) { .key = fnode->fences + mnode->lfence_len, .len = mnode->rfence_len };
}

static inline struct entry *get_entry(dcli_t *dcli, struct enode *enode, int nr) {
    return (void *) enode->entries + nr * sizeof_entry(dcli);
}

static inline void cpy_entry(dcli_t *dcli, struct entry *dst, const struct entry *src) {
    memcpy(dst, src, sizeof_entry(dcli));
}

static inline int create_sentinel(dcli_t *dcli) {
    dset_t *dset = dcli->dset;
    size_t msize, fsize;
    struct mnode *mnode;
    struct fnode *fnode;
    dgroup_t dgroup;
    int ret = 0;

    fsize = sizeof(struct fnode) + dcli->kc->min.len + dcli->kc->max.len;

    /* alloc bnode */
    mnode = balloc(dcli, dcli->bnode_size + fsize);
    if (unlikely(IS_ERR(mnode))) {
        pr_err("failed to allocate sentinel bnode: %s", strerror(-PTR_ERR(mnode)));
        ret = PTR_ERR(mnode);
        goto out;
    }
    dset->sentinel_bnode = bptr2off(dcli, mnode);

    /* init bnode sentinel */
    fnode = get_bfnode(dcli, mnode);
    msize = sizeof(*mnode) + dcli->bfanout * sizeof(uint8_t);
    memset(mnode, 0, msize);
    mnode->bprev = mnode->bnext = BNULL;
    mnode->lfence_len = dcli->kc->min.len;
    mnode->rfence_len = dcli->kc->max.len;
    memcpy(fnode->fences, dcli->kc->min.key, dcli->kc->min.len);
    memcpy(fnode->fences + dcli->kc->min.len, dcli->kc->max.key, dcli->kc->max.len);
    flush_range(mnode, msize);
    flush_range(fnode, fsize);
    memory_sfence();

    /* init dnode sentinel */
    ret = rpma_alloc(dcli->rpma_cli, &dset->sentinel_dnode, dcli->dnode_size + fsize);
    if (unlikely(ret < 0)) {
        pr_err("failed to allocate memory for dnode sentinel: %s", strerror(-ret));
        goto out;
    }

    mnode = rpma_buf_alloc(dcli->rpma_cli, msize);
    if (unlikely(!mnode)) {
        pr_err("failed to allocate memory for dnode sentinel");
        ret = -ENOMEM;
        goto out;
    }
    msize = sizeof(*mnode) + dcli->dfanout * sizeof(uint8_t);
    memset(mnode, 0, msize);
    mnode->dprev = mnode->dnext = RPMA_NULL;

    /* write back dnode sentinel */
    ret = rpma_wr(dcli->rpma_cli, dset->sentinel_dnode, 0, mnode, msize);
    if (unlikely(ret < 0)) {
        pr_err("failed to write dnode sentinel: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_wr(dcli->rpma_cli, get_dfnodep(dcli, dset->sentinel_dnode), 0, fnode, fsize);
    if (unlikely(ret < 0)) {
        pr_err("failed to write dnode sentinel: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit dnode sentinel: %s", strerror(-ret));
    }

    rpma_buf_free(dcli->rpma_cli, mnode, msize);

    /* init dgroup map */
    dgroup.bnode = dset->sentinel_bnode;
    dgroup.dnode = dset->sentinel_dnode;
    ret = shim_update_dgroup(dcli->shim_cli, dcli->kc->min, dcli->kc->max, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to init dgroup map: %s", strerror(-ret));
        goto out;
    }

    /* update statistics */
    dset->pm_utilization += dcli->bnode_size + fsize;

out:
    return ret;
}

dcli_t *dcli_create(dset_t *dset, shim_cli_t *shim_cli) {
    size_t dstripe_size = UINT64_MAX;
    dcli_t *dcli;
    int ret, i;

    dcli = calloc(1, sizeof(*dcli));
    if (unlikely(!dcli)) {
        dcli = ERR_PTR(-ENOMEM);
        goto out;
    }

    dcli->dset = dset;

    dcli->bdev = dset->bdev;
    dcli->rpma_cli = rpma_cli_create(dset->rpma);
    if (unlikely(IS_ERR(dcli->rpma_cli))) {
        dcli = ERR_PTR(PTR_ERR(dcli->rpma_cli));
        pr_err("failed to create rpma client: %s", strerror(-PTR_ERR(dcli->rpma_cli)));
        goto out;
    }

    dcli->kc = dset->kc;

    dcli->dstrip_size = dstripe_size;

    if (unlikely(dset->dnode_size < dstripe_size)) {
        pr_err("dnode size %lu is smaller than stripe size %lu", dset->dnode_size, dstripe_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    if (unlikely(dset->dnode_size % dcli->dstrip_size != 0)) {
        pr_err("dnode size %lu is not a multiple of strip size %lu", dset->dnode_size, dcli->dstrip_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    dcli->bmnode_size = 256;
    dcli->bnode_size = dset->bnode_size;
    dcli->dnode_size = dset->dnode_size;

    dcli->bfanout = (dcli->bnode_size - dcli->bmnode_size) / sizeof_entry(dcli);
    dcli->dfanout = (dcli->dnode_size - dcli->dstrip_size) / sizeof_entry(dcli);

    if (unlikely(dcli->dfanout * sizeof(uint8_t) + sizeof(struct mnode) > dcli->dstrip_size)) {
        pr_err("dnode fanout %d is too large for stripe size %lu", dcli->dfanout, dcli->dstrip_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    dcli->shim_cli = shim_cli;

    /* create sentinel bnode and dnode if necessary */
    if (cmpxchg2(&dset->sentinel_created, false, true)) {
        ret = create_sentinel(dcli);
        if (unlikely(ret)) {
            pr_err("failed to create sentinel bnode / dnode: %s", strerror(-ret));
            free(dcli);
            dcli = ERR_PTR(ret);
        }
    }

out:
    return dcli;
}

void dcli_destroy(dcli_t *dcli) {
    free(dcli);
}

/* get mnode and enode */
static inline struct mnode *dnode_get_mnode_enode(dcli_t *dcli, rpma_ptr_t dnode) {
    struct mnode *mnode;
    size_t size;
    int ret;

    size = dcli->dnode_size;

    mnode = rpma_buf_alloc(dcli->rpma_cli, size);
    if (unlikely(IS_ERR(mnode))) {
        pr_err("failed to allocate memory for mnode: %s", strerror(-PTR_ERR(mnode)));
        goto out;
    }

    ret = rpma_rd(dcli->rpma_cli, dnode, 0, mnode, size);
    if (unlikely(ret < 0)) {
        pr_err("failed to read mnode: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, size);
        mnode = ERR_PTR(ret);
    }

    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit mnode read: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, dcli->dstrip_size);
        mnode = ERR_PTR(ret);
    }

out:
    return mnode;
}

static inline void dnode_put_mnode_enode(dcli_t *dcli, struct mnode *mnode) {
    rpma_buf_free(dcli->rpma_cli, mnode, dcli->dnode_size);
}

static inline struct mnode *dnode_get_mnode(dcli_t *dcli, rpma_ptr_t dnode) {
    struct mnode *mnode;
    size_t msize;
    int ret;

    msize = sizeof(struct mnode) + dcli->dfanout * sizeof(uint8_t);

    mnode = rpma_buf_alloc(dcli->rpma_cli, msize);
    if (unlikely(IS_ERR(mnode))) {
        pr_err("failed to allocate memory for mnode: %s", strerror(-PTR_ERR(mnode)));
        goto out;
    }

    ret = rpma_rd(dcli->rpma_cli, dnode, 0, mnode, msize);
    if (unlikely(ret < 0)) {
        pr_err("failed to read mnode: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, msize);
        mnode = ERR_PTR(ret);
    }

    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit mnode read: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, dcli->dstrip_size);
        mnode = ERR_PTR(ret);
    }

out:
    return mnode;
}

static inline void dnode_put_mnode(dcli_t *dcli, struct mnode *mnode) {
    size_t msize = sizeof(struct mnode) + dcli->dfanout * sizeof(uint8_t);
    rpma_buf_free(dcli->rpma_cli, mnode, msize);
}

static inline int dnode_get_enode_fnode(dcli_t *dcli, rpma_ptr_t dnode, struct mnode *mnode,
                                        struct enode **enodep, struct fnode **fnodep) {
    struct enode *enode;
    size_t size;
    int ret;

    size = dcli->dnode_size - dcli->dstrip_size + sizeof(struct fnode) + mnode->lfence_len + mnode->rfence_len;

    enode = rpma_buf_alloc(dcli->rpma_cli, size);
    if (unlikely(IS_ERR(enode))) {
        ret = PTR_ERR(enode);
        pr_err("failed to allocate memory for enode: %s", strerror(-PTR_ERR(enode)));
        goto out;
    }

    ret = rpma_rd(dcli->rpma_cli, get_denodep(dcli, dnode), 0, enode, size);
    if (unlikely(ret < 0)) {
        pr_err("failed to read enode: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, enode, size);
        enode = ERR_PTR(ret);
    }

    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit enode read: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, enode, dcli->dstrip_size);
        enode = ERR_PTR(ret);
    }

    *enodep = enode;
    *fnodep = (void *) enode + mnode->nr_ents * sizeof_entry(dcli);

out:
    return ret;
}

static inline void dnode_put_enode_fnode(dcli_t *dcli, struct mnode *mnode, struct enode *enode) {
    size_t size = dcli->dnode_size - dcli->dstrip_size + sizeof(struct fnode) + mnode->lfence_len + mnode->rfence_len;
    rpma_buf_free(dcli->rpma_cli, enode, size);
}

static int bnode_delete(dcli_t *dcli, size_t bnode, k_t key) {
    struct mnode *mnode;
    struct enode *enode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and enode address */
    mnode = boff2ptr(dcli, bnode);
    enode = get_benode(dcli, mnode);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < mnode->nr_ents; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            /* then do update */
            get_entry(dcli, enode, idx)->valp = TOMBSTONE;
            goto out;
        }
    }

    ret = -ENOENT;

out:
    return ret;
}

static int dnode_lookup(dcli_t *dcli, rpma_ptr_t dnode, uint64_t fgprt, k_t key, uint64_t *valp) {
    struct entry *entry;
    struct mnode *mnode;
    size_t msize;
    int ret, i;

    msize = sizeof(struct mnode) + dcli->dfanout * sizeof(uint8_t);

    mnode = rpma_buf_alloc(dcli->rpma_cli, msize);
    if (unlikely(IS_ERR(mnode))) {
        pr_err("failed to allocate memory for mnode: %s", strerror(-PTR_ERR(mnode)));
        ret = -ENOMEM;
        goto out;
    }

    ret = rpma_rd(dcli->rpma_cli, dnode, 0, mnode, msize);
    if (unlikely(ret < 0)) {
        pr_err("failed to read mnode: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, msize);
        mnode = ERR_PTR(ret);
    }

    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit mnode read: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, dcli->dstrip_size);
        mnode = ERR_PTR(ret);
    }

    entry = rpma_buf_alloc(dcli->rpma_cli, sizeof_entry(dcli));
    if (unlikely(IS_ERR(entry))) {
        pr_err("failed to allocate memory for entry: %s", strerror(-PTR_ERR(entry)));
        ret = -ENOMEM;
        goto out;
    }

    /* lookup in reversed order to catch latest data */
    *valp = TOMBSTONE;

    for (i = mnode->nr_ents - 1; i >= 0; i--) {
        if (fgprt == mnode->fgprt[i]) {
            ret = rpma_rd(dcli->rpma_cli, get_dentryp(dcli, dnode, i), 0, entry, sizeof_entry(dcli));
            if (unlikely(ret < 0)) {
                pr_err("failed to read entry: %s", strerror(-ret));
                rpma_buf_free(dcli->rpma_cli, entry, sizeof_entry(dcli));
                goto out;
            }

            ret = rpma_commit_sync(dcli->rpma_cli);
            if (unlikely(ret < 0)) {
                pr_err("failed to commit entry read: %s", strerror(-ret));
                rpma_buf_free(dcli->rpma_cli, entry, sizeof_entry(dcli));
                goto out;
            }

            *valp = entry->valp;
            break;
        }
    }

    if (unlikely(*valp == TOMBSTONE)) {
        ret = -ENOENT;
    }

    rpma_buf_free(dcli->rpma_cli, entry, sizeof_entry(dcli));
    rpma_buf_free(dcli->rpma_cli, mnode, msize);

out:
    return ret;
}

static int bnode_lookup(dcli_t *dcli, size_t bnode, uint64_t fgprt, k_t key, uint64_t *valp) {
    struct mnode *mnode;
    struct enode *enode;
    int idx, ret = 0;

    /* get mnode and enode address */
    mnode = boff2ptr(dcli, bnode);
    enode = get_benode(dcli, mnode);

    /* if key exists */
    for (idx = 0; idx < mnode->nr_ents; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            *valp = get_entry(dcli, enode, idx)->valp;
            if (*valp == TOMBSTONE) {
                ret = -ENOENT;
            }
            goto out;
        }
    }

    ret = -ERANGE;

out:
    return ret;
}

static int bnode_upsert(dcli_t *dcli, size_t bnode, k_t key, uint64_t valp) {
    struct entry *entry;
    struct mnode *mnode;
    struct enode *enode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and enode address */
    mnode = boff2ptr(dcli, bnode);
    enode = get_benode(dcli, mnode);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < mnode->nr_ents; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            /* then do update, and set @ref flag */
            get_entry(dcli, enode, idx)->valp = valp;
            mnode->ref = true;
            goto out;
        }
    }

    /* find valid index */
    idx = mnode->nr_ents;
    if (unlikely(idx == dcli->bfanout)) {
        /* run out of bnode space, need split */
        ret = -ENOMEM;
        goto out;
    }

    /* insert into it */
    entry = get_entry(dcli, enode, idx);
    entry->valp = valp;
    entry->k_len = key.len;
    memcpy(entry->key, key.key, key.len);
    mnode->fgprt[idx] = fgprt;

    /* make the insertion visible */
    WRITE_ONCE(mnode->nr_ents, mnode->nr_ents + 1);

out:
    return ret;
}

struct sort_task {
    dcli_t *dcli;
    struct enode *enode;
};

static int sort_cmp_entry(const void *a, const void *b, void *priv) {
    int p = *(int *)a, q = *(int *)b;
    struct sort_task *task = priv;
    return cmp_entry(task->dcli, get_entry(task->dcli, task->enode, p), get_entry(task->dcli, task->enode, q));
}

static int *get_order_arr(dcli_t *dcli, int *nr, struct mnode *mnode, struct enode *enode, bool dedup) {
    struct sort_task task = { .dcli = dcli, .enode = enode };
    int i, *order, *order_dedup, cnt;
    struct entry *e1, *e2;

    order = calloc(mnode->nr_ents, sizeof(*order));
    if (unlikely(!order)) {
        goto out;
    }

    *nr = 0;
    for (i = 0; i < mnode->nr_ents; i++) {
        if (mnode->fgprt[i]) {
            order[(*nr)++] = i;
        }
    }

    qsort_r(order, *nr, sizeof(*order), sort_cmp_entry, &task);

    if (dedup) {
        order_dedup = calloc(*nr, sizeof(*order_dedup));
        if (unlikely(!order_dedup)) {
            free(order);
            order = NULL;
            goto out;
        }

        cnt = 0;
        for (i = 0; i < *nr; i++) {
            if (i > 0 && mnode->fgprt[order[i]] == mnode->fgprt[order[i - 1]]) {
                e1 = get_entry(dcli, enode, order[i]);
                e2 = get_entry(dcli, enode, order[i - 1]);
                if (likely(k_cmp(dcli->kc, e_key(dcli, e1), e_key(dcli, e2)) == 0)) {
                    continue;
                }
            }
            order_dedup[cnt++] = order[i];
        }
        *nr = cnt;

        free(order);
        order = order_dedup;
    }

out:
    return order;
}

static inline void put_order_arr(int *order) {
    free(order);
}

/*
 * bnode split algorithm (two new nodes are combined into one box)
 *
 *                 ┌────────┐ (1) persist the new node's next and prev pointer
 *    ┌────────────┤        ├────────────┐
 *    │            │new     │            │
 *    │    xxxxxxx─►        ◄───────┐    │
 *    │    x       └────────┘       │    │
 *    │    x                        │    │
 *  ┌─▼────x─┐     ┌────────┐     ┌─┴────▼─┐
 *  │        │     │        │     │        │
 *  │        └─────►        └─────►        │
 *  │ prev   ◄─────┐ old    ◄─────┐ next   │
 *  └────────┘     └────────┘     └────────┘
 * (2) persist                  (3) change next->prev
 *     prev->next                   (no need to persist, can be recovered)
 *     (durable point)
 */
static int bnode_split(dcli_t *dcli, dgroup_t dgroup, size_t *new_bnode, k_t key, k_t split_key) {
    struct mnode *prev, *next, *mnode, *mleft, *mright;
    struct enode *enode, *eleft, *eright;
    struct fnode *fnode, *fleft, *fright;
    int *order, nr, ret = 0, i, pos;
    struct entry *entry;
    k_t lfence, rfence;
    size_t base;

    /* get mnode and enode address */
    mnode = boff2ptr(dcli, dgroup.bnode);
    enode = get_benode(dcli, mnode);
    fnode = get_bfnode(dcli, mnode);
    prev = boff2ptr(dcli, mnode->bprev);
    next = boff2ptr(dcli, mnode->bnext);

    /* get order array */
    order = get_order_arr(dcli, &nr, mnode, enode, false);
    if (unlikely(!order)) {
        ret = -ENOMEM;
        goto out;
    }

    /* get split key */
    if (split_key.key) {
        pos = nr / 2;
        entry = get_entry(dcli, enode, order[pos]);
        split_key = e_key(dcli, entry);
    } else {
        for (pos = 0; pos < nr; pos++) {
            entry = get_entry(dcli, enode, order[pos]);
            if (k_cmp(dcli->kc, key, e_key(dcli, entry)) <= 0) {
                break;
            }
        }
    }

    /* create new mnodes */
    base = dcli->bnode_size + sizeof(struct fnode) + split_key.len;
    mleft = balloc(dcli, base + mnode->lfence_len);
    mright = balloc(dcli, base + mnode->rfence_len);
    if (unlikely(IS_ERR(mleft) || IS_ERR(mright))) {
        pr_err("failed to allocate memory for new mnodes: %s / %s",
               strerror(-PTR_ERR(mleft)), strerror(-PTR_ERR(mright)));
        ret = -ENOMEM;
        goto out;
    }
    eleft = get_benode(dcli, mleft);
    eright = get_benode(dcli, mright);
    fleft = get_bfnode(dcli, mleft);
    fright = get_bfnode(dcli, mright);

    /* write links */
    mleft->bprev = mnode->bprev;
    mleft->bnext = bptr2off(dcli, mright);
    mright->bprev = bptr2off(dcli, mleft);
    mright->bnext = mnode->bnext;

    /* write new fingerprint and data */
    mleft->ref = mright->ref = mnode->ref;
    memset(mleft->fgprt, 0, dcli->bfanout);
    memset(mright->fgprt, 0, dcli->bfanout);
    for (i = 0; i < pos; i++) {
        mleft->fgprt[i] = mnode->fgprt[order[i]];
        cpy_entry(dcli, get_entry(dcli, eleft, i), get_entry(dcli, enode, order[i]));
    }
    for (i = pos; i < nr; i++) {
        mright->fgprt[i - pos] = mnode->fgprt[order[i]];
        cpy_entry(dcli, get_entry(dcli, eright, i - pos), get_entry(dcli, enode, order[i]));
    }
    mleft->nr_ents = pos;
    mright->nr_ents = nr - pos;
    mleft->ref = mright->ref = false;

    /* write fences */
    mleft->lfence_len = mnode->lfence_len;
    mleft->rfence_len = split_key.len;
    memcpy_nt(fleft->fences, fnode->fences, mnode->lfence_len);
    memcpy_nt(fleft->fences + mnode->lfence_len, split_key.key, split_key.len);
    mright->lfence_len = split_key.len;
    mright->rfence_len = mnode->rfence_len;
    memcpy_nt(fright->fences, split_key.key, split_key.len);
    memcpy_nt(fright->fences + split_key.len, fnode->fences + mnode->lfence_len, mnode->rfence_len);
    lfence = (k_t) { .key = fnode->fences, .len = mnode->lfence_len };
    rfence = (k_t) { .key = fnode->fences + mnode->lfence_len, .len = mnode->rfence_len };

    /* persist newly created nodes */
    flush_range(mleft, dcli->bnode_size);
    flush_range(mright, dcli->bnode_size);
    memory_sfence();

    /* changes to next->prev can be volatile, because prev pointers can be recovered */
    if (next) {
        next->bprev = bptr2off(dcli, mright);
    }

    /* persist the link (change prev->next), this is the durable point of this split */
    if (prev) {
        WRITE_ONCE(prev->bnext, bptr2off(dcli, mleft));
        flush_range(&prev->bnext, sizeof(prev->bnext));
    } else {
        dcli->dset->sentinel_bnode = bptr2off(dcli, mleft);
    }
    memory_sfence();

    /* change clock hand */
    if (dcli->dset->pivot_bnode == dgroup.bnode) {
        dcli->dset->pivot_bnode = bptr2off(dcli, mleft);
    }

    /* make new bnode visible to upper layer */
    dgroup.bnode = bptr2off(dcli, mleft);
    ret = shim_update_dgroup(dcli->shim_cli, lfence, split_key, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to update dgroup map: %s", strerror(-ret));
        goto out;
    }
    dgroup.bnode = bptr2off(dcli, mright);
    ret = shim_update_dgroup(dcli->shim_cli, split_key, rfence, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to update dgroup map: %s", strerror(-ret));
        goto out;
    }

    /* get the new bnode of key */
    if (k_cmp(dcli->kc, key, split_key) >= 0) {
        *new_bnode = bptr2off(dcli, mright);
    } else {
        *new_bnode = bptr2off(dcli, mleft);
    }

    /* TODO: GC old bnode */

    /* update statistics */
    dcli->dset->pm_utilization += 2 * (dcli->bnode_size + sizeof(struct fnode) + split_key.len);

    put_order_arr(order);

out:
    return ret;
}

static int prop_update_dnode(dcli_t *dcli, k_t s, k_t t, rpma_ptr_t dnode, rpma_ptr_t old_dnode) {
    k_t lfence, rfence, is, it;
    struct mnode *bmnode;
    struct fnode *bfnode;
    dgroup_t dgroup;
    size_t bnode;
    int ret;

    ret = shim_lookup_dgroup(dcli->shim_cli, s, &dgroup);
    if (unlikely(ret)) {
        pr_err("failed to lookup dgroup map: %s", strerror(-ret));
        goto out;
    }
    bnode = dgroup.bnode;

    for (; bnode != BNULL;) {
        bmnode = boff2ptr(dcli, bnode);
        bfnode = get_bfnode(dcli, bmnode);

        /* get bnode fences */
        lfence = get_lfence(dcli, bmnode, bfnode);
        rfence = get_rfence(dcli, bmnode, bfnode);

        /* get the overlapping part of [s, t) and [lfence, rfence) */
        is = s;
        it = t;
        if (k_cmp(dcli->kc, is, lfence) < 0) {
            is = lfence;
        }
        if (k_cmp(dcli->kc, it, rfence) > 0) {
            it = rfence;
        }

        /* no overlapping part, we stop */
        if (unlikely(k_cmp(dcli->kc, is, it) >= 0)) {
            break;
        }

        dgroup.bnode = bnode;
        dgroup.dnode = old_dnode;

        /* split if is > lfence */
        if (k_cmp(dcli->kc, is, lfence) > 0) {
            ret = bnode_split(dcli, dgroup, &bnode, dcli->kc->max, is);
            if (unlikely(ret)) {
                pr_err("failed to split bnode: %s", strerror(-ret));
                goto out;
            }
            dgroup.bnode = bnode;
        }

        /* split if it < rfence */
        if (k_cmp(dcli->kc, it, rfence) < 0) {
            ret = bnode_split(dcli, dgroup, &bnode, dcli->kc->min, it);
            if (unlikely(ret)) {
                pr_err("failed to split bnode: %s", strerror(-ret));
                goto out;
            }
            dgroup.bnode = bnode;
        }

        dgroup.dnode = dnode;
        ret = shim_update_dgroup(dcli->shim_cli, is, it, dgroup);
        if (unlikely(ret)) {
            pr_err("failed to update dgroup map: %s", strerror(-ret));
            goto out;
        }
    }

out:
    return ret;
}

static int dnode_split(dcli_t *dcli, rpma_ptr_t dnode, struct mnode *mnode) {
    struct enode *enode, *eleft, *eright;
    struct fnode *fnode, *fleft, *fright;
    int *order, nr, ret = 0, i, pos;
    k_t lfence, rfence, split_key;
    struct mnode *mleft, *mright;
    rpma_ptr_t left, right;
    struct entry *entry;
    size_t base;

    /* get enode/fnode */
    ret = dnode_get_enode_fnode(dcli, dnode, mnode, &enode, &fnode);
    if (unlikely(ret)) {
        pr_err("failed to get enode / fnode: %s", strerror(-ret));
        goto out;
    }

    /* get order array */
    order = get_order_arr(dcli, &nr, mnode, enode, true);
    if (unlikely(!order)) {
        ret = -ENOMEM;
        goto out;
    }

    /* get split key */
    pos = nr / 2;
    entry = get_entry(dcli, enode, order[pos]);
    split_key = e_key(dcli, entry);

    /* create new dnodes */
    base = dcli->dnode_size + sizeof(struct fnode) + split_key.len;
    ret = rpma_alloc(dcli->rpma_cli, &left, base + mnode->lfence_len);
    if (unlikely(ret < 0)) {
        pr_err("failed to allocate memory for left dnode: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_alloc(dcli->rpma_cli, &right, base + mnode->rfence_len);
    if (unlikely(ret < 0)) {
        pr_err("failed to allocate memory for right dnode: %s", strerror(-ret));
        goto out;
    }

    /* prepare buffers for new dnodes */
    mleft = rpma_buf_alloc(dcli->rpma_cli, base + mnode->lfence_len);
    mright = rpma_buf_alloc(dcli->rpma_cli, base + mnode->rfence_len);
    if (unlikely(IS_ERR(mleft) || IS_ERR(mright))) {
        pr_err("failed to allocate memory for new mnodes: %s / %s",
               strerror(-PTR_ERR(mleft)), strerror(-PTR_ERR(mright)));
        ret = -ENOMEM;
        goto out;
    }
    eleft = get_denode(dcli, mleft);
    eright = get_denode(dcli, mright);
    fleft = get_dfnode(dcli, mleft);
    fright = get_dfnode(dcli, mright);

    /* write links */
    mleft->dprev = mnode->dprev;
    mleft->dnext = right;
    mright->dprev = left;
    mright->dnext = mnode->dnext;

    /* write new fingerprint and data */
    memset(mleft->fgprt, 0, dcli->dfanout);
    memset(mright->fgprt, 0, dcli->dfanout);
    for (i = 0; i < pos; i++) {
        mleft->fgprt[i] = mnode->fgprt[order[i]];
        cpy_entry(dcli, get_entry(dcli, eleft, i), get_entry(dcli, enode, order[i]));
    }
    for (i = pos; i < nr; i++) {
        mright->fgprt[i - pos] = mnode->fgprt[order[i]];
        cpy_entry(dcli, get_entry(dcli, eright, i - pos), get_entry(dcli, enode, order[i]));
    }
    mleft->nr_ents = pos;
    mright->nr_ents = nr - pos;

    /* write fences */
    mleft->lfence_len = mnode->lfence_len;
    mleft->rfence_len = split_key.len;
    memcpy(fleft->fences, fnode->fences, mnode->lfence_len);
    memcpy(fleft->fences + mnode->lfence_len, split_key.key, split_key.len);
    mright->lfence_len = split_key.len;
    mright->rfence_len = mnode->rfence_len;
    memcpy(fright->fences, split_key.key, split_key.len);
    memcpy(fright->fences + split_key.len, fnode->fences + mnode->lfence_len, mnode->rfence_len);
    lfence = (k_t) { .key = fnode->fences, .len = mnode->lfence_len };
    rfence = (k_t) { .key = fnode->fences + mnode->lfence_len, .len = mnode->rfence_len };

    /* persist newly created nodes */
    ret = rpma_wr(dcli->rpma_cli, left, 0, mleft, base + mnode->lfence_len);
    if (unlikely(ret < 0)) {
        pr_err("failed to write left dnode: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_wr(dcli->rpma_cli, right, 0, mright, base + mnode->rfence_len);
    if (unlikely(ret < 0)) {
        pr_err("failed to write right dnode: %s", strerror(-ret));
        goto out;
    }
    if (mnode->dnext.rawp != RPMA_NULL.rawp) {
        ret = rpma_wr(dcli->rpma_cli, RPMA_PTR_OFF(mnode->dnext, offsetof(struct mnode, dprev)), 0, &right, sizeof(right));
        if (unlikely(ret < 0)) {
            pr_err("failed to write next->prev: %s", strerror(-ret));
            goto out;
        }
    }
    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit dnode write: %s", strerror(-ret));
        goto out;
    }

    /* persist the link (change prev->next), this is the durable point of this split */
    if (mnode->dprev.rawp != RPMA_NULL.rawp) {
        ret = rpma_wr(dcli->rpma_cli, RPMA_PTR_OFF(mnode->dprev, offsetof(struct mnode, dnext)), 0, &left, sizeof(left));
        if (unlikely(ret < 0)) {
            pr_err("failed to write prev->next: %s", strerror(-ret));
            goto out;
        }
        ret = rpma_commit_sync(dcli->rpma_cli);
        if (unlikely(ret < 0)) {
            pr_err("failed to commit dnode write: %s", strerror(-ret));
            goto out;
        }
    } else {
        dcli->dset->sentinel_dnode = left;
    }

    /* make new dnode visible to upper layer */
    ret = prop_update_dnode(dcli, lfence, split_key, left, dnode);
    if (unlikely(ret)) {
        pr_err("failed to update dnode: %s", strerror(-ret));
        goto out;
    }
    ret = prop_update_dnode(dcli, split_key, rfence, right, dnode);
    if (unlikely(ret)) {
        pr_err("failed to update dnode: %s", strerror(-ret));
        goto out;
    }

    /* TODO: GC old dnode */

    put_order_arr(order);

out:
    return ret;
}

int dset_upsert(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t valp) {
    size_t bnode = dgroup.bnode;
    int ret;

    ret = bnode_upsert(dcli, bnode, key, valp);
    if (unlikely(ret == -ENOMEM)) {
        /* bnode full, split and retry */
        ret = bnode_split(dcli, dgroup, &bnode, key, (k_t) { });
        if (unlikely(ret)) {
            pr_err("bnode split failed: %s", strerror(-ret));
            goto out;
        }
        ret = bnode_upsert(dcli, bnode, key, valp);
    }
    if (unlikely(ret)) {
        pr_err("dset upsert failed: %s", strerror(-ret));
        goto out;
    }

out:
    return ret;
}

int dset_delete(dcli_t *dcli, dgroup_t dgroup, k_t key) {
    return bnode_delete(dcli, dgroup.bnode, key);
}

int dset_lookup(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t *valp) {
    uint64_t fgprt = get_fgprt(dcli, key);
    int ret;

    ret = bnode_lookup(dcli, dgroup.bnode, fgprt, key, valp);
    if (ret == -ERANGE) {
        ret = dnode_lookup(dcli, dgroup.dnode, fgprt, key, valp);
    }

out:
    return ret;
}

size_t dset_get_pm_utilization(dcli_t *dcli) {
    return dcli->dset->pm_utilization;
}

static inline size_t choose_gc_target(dcli_t *dcli) {
    dset_t *dset = dcli->dset;
    struct mnode *mnode;
    size_t target;

    /* choose GC target based on clock pLRU algorithm to find the least recently updated node */
    target = dset->pivot_bnode;
    mnode = boff2ptr(dcli, target);
    while (mnode->ref) {
        mnode->ref = false;
        if (unlikely(mnode->bnext == BNULL)) {
            target = dset->sentinel_bnode;
        } else {
            target = mnode->bnext;
        }
        mnode = boff2ptr(dcli, target);
    }

    return target;
}

/*
 * ┌───────┐ ┌───────┐ ┌───────┐
 * │       │ │       │ │       │
 * │ bnode │ │ bnode │ │ bnode │
 * │       │ │       │ │       │
 * └───┬───┘ └───┬───┘ └───┬───┘
 *     │         │         │
 *     └─────────┼─────────┘ hardware gather
 *               │
 * ┌───────────┬─▼─────────────┐
 * │┼──────────┤     dnode     │
 * ││          │               │
 * ││  used    │   available   │
 * ││          │               │
 * │┼──────────┤               │
 * └───────────┴───────────────┘
 *
 *              ────────►
 *                append-only (out-of-place)
 */
static inline int gc_bnodes(dcli_t *dcli) {
    int ret, nr_gc_ents, nr_gc_bnodes;
    struct mnode *bmnode, *dmnode;
    rpma_ptr_t dnode, de_tail;
    k_t lfence, rfence;
    rpma_buf_t *bufs;
    dgroup_t dgroup;
    size_t target;

retry:
    /* choose GC target (strategy: the least recently updated node) */
    target = choose_gc_target(dcli);
    bmnode = boff2ptr(dcli, target);
    lfence = get_lfence(dcli, bmnode, get_bfnode(dcli, bmnode));
    rfence = get_rfence(dcli, bmnode, get_bfnode(dcli, bmnode));

    /* get the corresponding dnode */
    ret = shim_lookup_dgroup(dcli->shim_cli, lfence, &dgroup);
    if (unlikely(ret)) {
        pr_err("failed to lookup dgroup map: %s", strerror(-ret));
        goto out;
    }
    dnode = dgroup.dnode;
    dmnode = dnode_get_mnode(dcli, dnode);
    if (unlikely(IS_ERR(dmnode))) {
        pr_err("failed to get mnode: %s", strerror(-PTR_ERR(dmnode)));
        ret = PTR_ERR(dmnode);
        goto out;
    }
    de_tail = RPMA_PTR_OFF(get_denodep(dcli, dnode), dmnode->nr_ents * sizeof_entry(dcli));

    /* allocate data buffer pointers */
    bufs = calloc(dcli->dset->max_gc_prefetch + 1, sizeof(*bufs));
    if (unlikely(!bufs)) {
        pr_err("failed to allocate memory for data buffer pointers");
        ret = -ENOMEM;
        goto out;
    }

    /* get target bnodes (prefetch succeeding bnodes) and modify cmnode */
    nr_gc_bnodes = nr_gc_ents = 0;

    while (dgroup.dnode.rawp == dnode.rawp && nr_gc_ents + dmnode->nr_ents < dcli->dfanout) {
        bufs[nr_gc_bnodes].start = get_benode(dcli, bmnode);
        bufs[nr_gc_bnodes].size = sizeof_entry(dcli) * bmnode->nr_ents;

        memcpy(dmnode->fgprt + dmnode->nr_ents, bmnode->fgprt, sizeof(uint8_t) * bmnode->nr_ents);

        nr_gc_bnodes++;
        nr_gc_ents += bmnode->nr_ents;
    }

    bufs[nr_gc_bnodes] = (rpma_buf_t) { NULL, 0 };

    if (unlikely(nr_gc_bnodes == 0)) {
        ret = dnode_split(dcli, dnode, dmnode);
        if (unlikely(ret)) {
            pr_err("failed to split dnode: %s", strerror(-ret));
            goto out;
        }
        goto retry;
    }

    /* write dnode data */
    ret = rpma_wr_(dcli->rpma_cli, de_tail, bufs, 0);
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to GC data to dnode: %s", strerror(-ret));
        goto out;
    }

    /* write dnode metadata */
    dmnode->nr_ents += nr_gc_ents;
    ret = rpma_wr(dcli->rpma_cli, dnode, 0,
                  dmnode, sizeof(*dmnode) + dmnode->nr_ents * sizeof(uint8_t));
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to GC metadata to dnode: %s", strerror(-ret));
        goto out;
    }

    /* commit remote changes and wait */
    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to commit GC data to dnode");
        goto out;
    }

    free(bufs);

out:
    return ret;
}

int dset_gc(dcli_t *dcli, size_t *gc_size) {
    dset_t *dset = dcli->dset;
    size_t start;
    int ret = 0;

    start = dset->pm_utilization;
    while (dset->pm_utilization - start < *gc_size) {
        /* repeatedly GC bnodes until enough GC size */
        ret = gc_bnodes(dcli);
        if (unlikely(ret)) {
            pr_err("failed to GC bnodes: %s", strerror(-ret));
            goto out;
        }
    }

out:
    return ret;
}

static cJSON *bnode_dump(dcli_t *dcli, size_t bnode) {
    cJSON *out, *entries, *entry;
    struct mnode *mnode;
    struct enode *enode;
    struct fnode *fnode;
    int i;

    mnode = boff2ptr(dcli, bnode);
    enode = get_benode(dcli, mnode);
    fnode = get_bfnode(dcli, mnode);

    out = cJSON_CreateObject();

    cJSON_AddNumberToObject(out, "addr", bnode);
    cJSON_AddStringToObject(out, "lfence", k_str(dcli->kc, get_lfence(dcli, mnode, fnode)));
    cJSON_AddStringToObject(out, "rfence", k_str(dcli->kc, get_rfence(dcli, mnode, fnode)));

    entries = cJSON_CreateArray();

    for (i = 0; i < mnode->nr_ents; i++) {
        entry = cJSON_CreateObject();

        cJSON_AddStringToObject(entry, "key", get_entry(dcli, enode, i)->key);
        cJSON_AddNumberToObject(entry, "valp", get_entry(dcli, enode, i)->valp);

        cJSON_AddItemToArray(entries, entry);
    }

    cJSON_AddItemToObject(out, "entries", entries);

    return out;
}

static cJSON *dnode_dump(dcli_t *dcli, rpma_ptr_t dnode) {
    cJSON *out, *entries, *entry;
    struct mnode *mnode;
    struct enode *enode;
    struct fnode *fnode;
    int i;

    mnode = dnode_get_mnode(dcli, dnode);
    dnode_get_enode_fnode(dcli, dnode, mnode, &enode, &fnode);

    out = cJSON_CreateObject();

    cJSON_AddNumberToObject(out, "addr", dnode.rawp);
    cJSON_AddStringToObject(out, "lfence", k_str(dcli->kc, get_lfence(dcli, mnode, fnode)));
    cJSON_AddStringToObject(out, "rfence", k_str(dcli->kc, get_rfence(dcli, mnode, fnode)));

    entries = cJSON_CreateArray();

    for (i = 0; i < mnode->nr_ents; i++) {
        entry = cJSON_CreateObject();

        cJSON_AddStringToObject(entry, "key", get_entry(dcli, enode, i)->key);
        cJSON_AddNumberToObject(entry, "valp", get_entry(dcli, enode, i)->valp);

        cJSON_AddItemToArray(entries, entry);
    }

    cJSON_AddItemToObject(out, "entries", entries);

    dnode_put_enode_fnode(dcli, mnode, enode);
    dnode_put_mnode(dcli, mnode);

    return out;
}

static cJSON *bnodes_dump(dcli_t *dcli) {
    struct mnode *mnode;
    cJSON *bnodes;
    size_t bnode;

    bnodes = cJSON_CreateArray();

    bnode = dcli->dset->sentinel_bnode;
    while (bnode != BNULL) {
        cJSON_AddItemToArray(bnodes, bnode_dump(dcli, bnode));
        mnode = boff2ptr(dcli, bnode);
        bnode = mnode->bnext;
    }

    return bnodes;
}

static cJSON *dnodes_dump(dcli_t *dcli) {
    struct mnode *mnode;
    rpma_ptr_t dnode;
    cJSON *dnodes;

    dnodes = cJSON_CreateArray();

    dnode = dcli->dset->sentinel_dnode;
    while (dnode.rawp != RPMA_NULL.rawp) {
        cJSON_AddItemToArray(dnodes, dnode_dump(dcli, dnode));
        mnode = dnode_get_mnode(dcli, dnode);
        dnode = mnode->dnext;
        dnode_put_mnode(dcli, mnode);
    }

    return dnodes;
}

cJSON *dset_dump(dcli_t *dcli) {
    cJSON *out;

    out = cJSON_CreateObject();

    cJSON_AddItemToObject(out, "bnodes", bnodes_dump(dcli));
    cJSON_AddItemToObject(out, "dnodes", dnodes_dump(dcli));

    return out;
}

int dset_scan(dcli_t *dcli, dgroup_t dgroup) {
    rpma_ptr_t dnode = dgroup.dnode;
    struct mnode *mnode;
    int nr;

    mnode = dnode_get_mnode_enode(dcli, dnode);
    nr = mnode->nr_ents;
    dnode_put_mnode(dcli, mnode);

    return nr;
}
