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
    uint8_t fgprt[];
};

struct enode {
    struct entry entries[];
};

struct fnode {
    uint32_t lfence_len, rfence_len;
    char fences[];
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

    size_t pstage_sz;

    size_t pm_utilization;

    size_t pivot_bnode;

    int max_gc_prefetch;
};

struct dcli {
    dset_t *dset;

    dgroup_map_lookup_fn gm_lookuper;
    dgroup_map_update_fn gm_updator;
    void *priv;

    struct pm_dev *bdev;
    rpma_cli_t *rpma_cli;

    perf_t *perf;
    kc_t *kc;

    size_t bnode_size, bmnode_size;
    size_t dnode_size, dstrip_size;
    int bfanout, dfanout;

    size_t pstage_sz;

    unsigned seed;

    struct mnode *prefetched_mnode;
};

dset_t *dset_create(kc_t *kc,
                    size_t bnode_size, size_t dnode_size,
                    const char *bdev, rpma_t *rpma,
                    size_t pstage_sz, int max_gc_prefetch) {
    dset_t *dset;
    int i;

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

    dset->pstage_sz = pstage_sz;

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
    return (char *) ptr - dcli->bdev->start;
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

static inline k_t get_lfence(dcli_t *dcli, struct fnode *fnode) {
    return (k_t) { .key = fnode->fences, .len = fnode->lfence_len };
}

static inline k_t get_rfence(dcli_t *dcli, struct fnode *fnode) {
    return (k_t) { .key = fnode->fences + fnode->lfence_len, .len = fnode->rfence_len };
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
    fnode->lfence_len = dcli->kc->min.len;
    fnode->rfence_len = dcli->kc->max.len;
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
    ret = dcli->gm_updator(dcli->priv, dcli->kc->min, dcli->kc->max, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to init dgroup map: %s", strerror(-ret));
        goto out;
    }

    /* update statistics */
    dset->pm_utilization += dcli->bnode_size + fsize;

out:
    return ret;
}

dcli_t *dcli_create(dset_t *dset, dgroup_map_update_fn gm_updator, dgroup_map_lookup_fn gm_lookuper,
                    void *priv, perf_t *perf) {
    size_t dstripe_size = UINT64_MAX;
    dcli_t *dcli;
    int ret, i;

    dcli = calloc(1, sizeof(*dcli));
    if (unlikely(!dcli)) {
        dcli = ERR_PTR(-ENOMEM);
        goto out;
    }

    dcli->dset = dset;

    dcli->gm_lookuper = gm_lookuper;
    dcli->gm_updator = gm_updator;
    dcli->priv = priv;

    dcli->perf = perf;

    dcli->bdev = dset->bdev;
    dcli->rpma_cli = rpma_cli_create(dset->rpma, perf);
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

    /* TODO: flexible mnode size */
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

    dcli->pstage_sz = dset->pstage_sz;

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

static inline void dnode_put_mnode(dcli_t *dcli, rpma_ptr_t dnode, struct mnode *mnode) {
    size_t msize = sizeof(struct mnode) + dcli->dfanout * sizeof(uint8_t);
    rpma_buf_free(dcli->rpma_cli, mnode, msize);
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
            enode->entries[idx].valp = TOMBSTONE;
            goto out;
        }
    }

    ret = -ENOENT;

out:
    return ret;
}

static inline void bnode_prefetch(dcli_t *dcli, size_t bnode) {
    struct mnode *mnode;
    mnode = boff2ptr(dcli, bnode);
    prefetch_range(mnode, sizeof(*mnode) + dcli->bfanout * sizeof(uint8_t));
}

static inline int dnode_prefetch(dcli_t *dcli, rpma_ptr_t dnode) {
    struct mnode *mnode;
    size_t msize;
    int ret = 0;

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

    /* only commit, not sync for ACK */
    ret = rpma_commit(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit mnode read: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, dcli->dstrip_size);
        mnode = ERR_PTR(ret);
    }

    dcli->prefetched_mnode = mnode;

out:
    return ret;
}

static int dnode_lookup(dcli_t *dcli, rpma_ptr_t dnode, k_t key, uint64_t *valp) {
    struct mnode *mnode;
    size_t msize;
    int ret;

    msize = sizeof(struct mnode) + dcli->dfanout * sizeof(uint8_t);

    if (unlikely(!dcli->prefetched_mnode)) {
        dnode_prefetch(dcli, dnode);
    }

    ret = rpma_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit mnode read: %s", strerror(-ret));
        rpma_buf_free(dcli->rpma_cli, mnode, dcli->dstrip_size);
        mnode = ERR_PTR(ret);
    }

out:
    return mnode;
}

static int bnode_lookup(dcli_t *dcli, size_t bnode, k_t key, uint64_t *valp) {
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
            *valp = enode->entries[idx].valp;
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
            enode->entries[idx].valp = valp;
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
    enode->entries[idx].valp = valp;
    mnode->fgprt[idx] = fgprt;

    /* make the insertion visible */
    WRITE_ONCE(mnode->nr_ents, mnode->nr_ents + 1);

out:
    return ret;
}

static int sort_cmp_entry(const void *a, const void *b, void *priv) {
    dcli_t *dcli = priv;
    return cmp_entry(dcli, a, b);
}

static int *get_order_arr(dcli_t *dcli, int *nr, size_t bnode, struct mnode *mnode, struct enode *enode) {
    int i, *order;

    order = calloc(dcli->bfanout, sizeof(*order));
    if (unlikely(!order)) {
        return order;
    }

    *nr = 0;
    for (i = 0; i < mnode->nr_ents; i++) {
        if (mnode->fgprt[i]) {
            order[(*nr)++] = i;
        }
    }

    qsort_r(order, *nr, sizeof(*order), sort_cmp_entry, dcli);

    return order;
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
static int bnode_split_median(dcli_t *dcli, dgroup_t dgroup, size_t *new_bnode, k_t key) {
    struct mnode *prev, *next, *mnode, *mleft, *mright;
    struct enode *enode, *eleft, *eright;
    struct fnode *fnode, *fleft, *fright;
    int *order, nr, ret = 0, i, pos;
    k_t split_key, lfence, rfence;
    size_t base;

    /* get mnode and enode address */
    mnode = boff2ptr(dcli, dgroup.bnode);
    enode = get_benode(dcli, mnode);
    fnode = get_bfnode(dcli, mnode);
    prev = boff2ptr(dcli, mnode->bprev);
    next = boff2ptr(dcli, mnode->bnext);

    /* get order array */
    order = get_order_arr(dcli, &nr, dgroup.bnode, mnode, enode);
    if (unlikely(!order)) {
        ret = -ENOMEM;
        goto out;
    }

    /* get split key */
    pos = nr / 2;
    split_key = e_key(dcli, &enode->entries[order[pos]]);

    /* create new mnodes */
    base = dcli->bnode_size + sizeof(struct fnode) + split_key.len;
    mleft = balloc(dcli, base + fnode->lfence_len);
    mright = balloc(dcli, base + fnode->rfence_len);
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

    /* write new fingerprint and data */
    memset(mleft->fgprt, 0, dcli->bfanout);
    memset(mright->fgprt, 0, dcli->bfanout);
    for (i = 0; i < pos; i++) {
        mleft->fgprt[i] = mnode->fgprt[order[i]];
        eleft->entries[i] = enode->entries[order[i]];
    }
    for (i = pos; i < nr; i++) {
        mright->fgprt[i - pos] = mnode->fgprt[order[i]];
        eright->entries[i - pos] = enode->entries[order[i]];
    }
    mleft->nr_ents = pos;
    mright->nr_ents = nr - pos;
    mleft->ref = mright->ref = false;

    /* write fences */
    fleft->lfence_len = fnode->lfence_len;
    fleft->rfence_len = split_key.len;
    memcpy_nt(fleft->fences, fnode->fences, fnode->lfence_len);
    memcpy_nt(fleft->fences + fnode->lfence_len, split_key.key, split_key.len);
    fright->lfence_len = split_key.len;
    fright->rfence_len = fnode->rfence_len;
    memcpy_nt(fright->fences, split_key.key, split_key.len);
    memcpy_nt(fright->fences + split_key.len, fnode->fences + fnode->lfence_len, fnode->rfence_len);
    lfence = (k_t) { .key = fnode->fences, .len = fnode->lfence_len };
    rfence = (k_t) { .key = fnode->fences + fnode->lfence_len, .len = fnode->rfence_len };

    /* persist newly created nodes */
    mleft->bprev = mnode->bprev;
    mleft->bnext = bptr2off(dcli, mright);
    mright->bprev = bptr2off(dcli, mleft);
    mright->bnext = mnode->bnext;
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
        /* TODO: FIXME */
    }
    memory_sfence();

    /* make new bnode visible to upper layer */
    dgroup.bnode = bptr2off(dcli, mleft);
    ret = dcli->gm_updator(dcli->priv, lfence, split_key, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to update dgroup map: %s", strerror(-ret));
        goto out;
    }
    dgroup.bnode = bptr2off(dcli, mright);
    ret = dcli->gm_updator(dcli->priv, split_key, rfence, dgroup);
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

out:
    return ret;
}

int dset_upsert(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t valp) {
    size_t bnode = dgroup.bnode;
    int ret;

    ret = bnode_upsert(dcli, bnode, key, valp);
    if (unlikely(ret == -ENOMEM)) {
        /* bnode full, split and retry */
        ret = bnode_split_median(dcli, dgroup, &bnode, key);
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

void dset_prefetch(dcli_t *dcli, dgroup_t dgroup) {
    bnode_prefetch(dcli, dgroup.bnode);
    dnode_prefetch(dcli, dgroup.dnode);
}

int dset_lookup(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t *valp) {
    int ret;

    ret = bnode_lookup(dcli, dgroup.bnode, key, valp);
    if (ret == -ERANGE) {
        ret = dnode_lookup(dcli, dgroup.dnode, key, valp);
        goto out;
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
    struct mnode *hmnode, *cmnode;
    rpma_ptr_t dnode, denode;
    k_t lfence, rfence;
    rpma_buf_t *bufs;
    dgroup_t dgroup;
    size_t target;

    /* choose GC target (strategy: the least recently updated node) */
    target = choose_gc_target(dcli);
    hmnode = boff2ptr(dcli, target);
    lfence = get_lfence(dcli, get_bfnode(dcli, hmnode));
    rfence = get_rfence(dcli, get_bfnode(dcli, hmnode));

    /* get the corresponding dnode */
    ret = dcli->gm_lookuper(dcli->priv, &dgroup, lfence);
    if (unlikely(ret)) {
        pr_err("failed to lookup dgroup map: %s", strerror(-ret));
        goto out;
    }
    dnode = dgroup.dnode;
    cmnode = dnode_get_mnode(dcli, dnode);
    if (unlikely(IS_ERR(cmnode))) {
        pr_err("failed to get mnode: %s", strerror(-PTR_ERR(cmnode)));
        ret = PTR_ERR(cmnode);
        goto out;
    }

    /* allocate data buffer pointers */
    bufs = calloc(dcli->dset->max_gc_prefetch + 1, sizeof(*bufs));
    if (unlikely(!bufs)) {
        pr_err("failed to allocate memory for data buffer pointers");
        ret = -ENOMEM;
        goto out;
    }

    /* get target bnodes (prefetch succeeding bnodes) and modify cmnode */
    nr_gc_bnodes = nr_gc_ents = 0;

    while (dgroup.dnode.rawp == dnode.rawp && nr_gc_ents + cmnode->nr_ents < dcli->dfanout) {
        bufs[nr_gc_bnodes].start = get_benode(dcli, hmnode);
        bufs[nr_gc_bnodes].size = sizeof_entry(dcli) * hmnode->nr_ents;

        memcpy(cmnode->fgprt + cmnode->nr_ents, hmnode->fgprt, sizeof(uint8_t) * hmnode->nr_ents);
        cmnode->nr_ents += hmnode->nr_ents;

        nr_gc_bnodes++;
        nr_gc_ents += hmnode->nr_ents;
    }

    bufs[nr_gc_bnodes] = (rpma_buf_t) { NULL, 0 };

    if (unlikely(nr_gc_bnodes == 0)) {
        /* TODO: FIXME: handle dnode split case! */
    }

    /* write dnode data */
    denode = get_denodep(dcli, dnode);
    ret = rpma_wr_(dcli->rpma_cli, denode, bufs, 0);
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to GC data to dnode: %s", strerror(-ret));
        goto out;
    }

    /* write dnode metadata */
    ret = rpma_wr(dcli->rpma_cli, dnode, 0,
                  cmnode, sizeof(*cmnode) + cmnode->nr_ents * sizeof(uint8_t));
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
