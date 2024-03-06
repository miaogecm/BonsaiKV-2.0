/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ scalable data layer
 *
 * The core data structure exposed by data layer is "dset" (a collection/set of data nodes across different
 * storage tiers). This data structure relies on an upper layer index to map a key to its corresponding
 * dnodes at different storage tiers. The dnodes corresponding to a key is called as "data node group" (dgroup).
 *
 * Hohai University
 */

#define _GNU_SOURCE

#define DSET_SOURCE

#include <urcu.h>

#include "atomic.h"
#include "dset.h"
#include "k.h"

#define NO_OFF          (-1ul)
#define TOMBSTONE       (-1ul)

struct valp {
    union {
        struct {
            int is_inline : 1;
            union {
                struct {
                    size_t inline_len : 4;
                    uint64_t inline_val : 59;
                };
                struct {
                    int is_lpm : 1;
                    size_t len : 14;
                    size_t off : 48;
                };
            };
        };
        uint64_t rawp;
    };
};

struct nodep {
    union {
        struct {
            int dom : 16;
            uint64_t off : 48;
        };
        uint64_t rawp;
    };
};

typedef struct dgroup {
    /* dnodes across different storage tiers */
    struct nodep dnodes[2];
} dgroup_t;

struct dentry {
    struct valp valp;
    uint32_t k_len;
    uint32_t reserve;
    char dkey[];
};

struct mnode {
    size_t next_off, prev_off;
    int nr_ents;
    bool ref;
    uint8_t fgprt[];
};

struct dnode {
    struct dentry entries[];
};

struct fnode {
    uint32_t lfence_len, rfence_len;
    char fences[];
};

struct dset {
    kc_t *kc;

    struct nodep sentinel_hnode, sentinel_cnode;
    bool sentinel_created;

    size_t hnode_size, cnode_size;

    int nr_ldoms, nr_rdoms;
    lpma_t **dom_lpmas;
    rpma_t **dom_rpmas;
    rpma_t *rep_rpma;

    size_t pstage_sz;

    size_t pm_utilization;

    size_t pivot_hnode;

    int max_gc_prefetch;
};

struct dcli {
    dset_t *dset;

    dgroup_map_lookup_fn gm_lookuper;
    dgroup_map_update_fn gm_updator;
    void *priv;

    int nr_ldoms, nr_rdoms;
    rpma_cli_t **dom_rpma_clis;
    lpma_cli_t **dom_lpma_clis;
    rpma_cli_t *rep_rpma_cli;

    perf_t *perf;
    kc_t *kc;

    size_t hstrip_size, cstrip_size;
    size_t hnode_size, cnode_size;
    int hfanout, cfanout;

    size_t pstage_sz;

    unsigned seed;
};

/*
 * Remote Memory Layout
 *
 *  Dom0 RPMA          Dom1 RPMA         Rep RPMA
 * ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
 * │$$$$$$$$$$$$$│   │$$$$$$$$$$$$$│   │$$$$$$$$$$$$$│
 * │    dev0     │   │    dev1     │   │    dev0     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │▼▼▼▼▼▼▼▼▼▼▼▼▼│   │▼▼▼▼▼▼▼▼▼▼▼▼▼│   │$$$$$$$$$$$$$│
 * │    dev2     │   │    dev3     │   │    dev1     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │&&&&&&&&&&&&&│   │&&&&&&&&&&&&&│   │▼▼▼▼▼▼▼▼▼▼▼▼▼│
 * │    dev4     │   │    dev5     │   │    dev2     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │▼▼▼▼▼▼▼▼▼▼▼▼▼│
 * │    dev0     │   │    dev1     │   │    dev3     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │&&&&&&&&&&&&&│
 * │    dev2     │   │    dev3     │   │    dev4     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │&&&&&&&&&&&&&│
 * │    dev4     │   │    dev5     │   │    dev5     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │             │
 * │    dev0     │   │    dev1     │   │    dev0     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │             │
 * │    dev2     │   │    dev3     │   │    dev1     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │             │
 * │    dev4     │   │    dev5     │   │    dev2     │
 * │             │   │             │   │             │
 * ├─────────────┤   ├─────────────┤   ├─────────────┤
 * │             │   │             │   │             │
 * │    dev0     │   │    dev1     │   │    dev3     │
 * │             │   │             │   │             │
 * └─────────────┘   └─────────────┘   └─────────────┘
 */
dset_t *dset_create(kc_t *kc, size_t hnode_size, size_t cnode_size,
                    lpma_t **dom_lpmas, rpma_t **dom_rpmas, rpma_t *rep_rpma,
                    size_t pstage_sz, int max_gc_prefetch) {
    dset_t *dset;
    int i;

    if (unlikely(!dom_lpmas[0] || !dom_rpmas[0])) {
        dset = ERR_PTR(-EINVAL);
        pr_err("empty lpmas or rpmas");
        goto out;
    }

    dset = calloc(1, sizeof(*dset));
    if (unlikely(!dset)) {
        dset = ERR_PTR(-ENOMEM);
        pr_err("failed to alloc memory for dset struct");
        goto out;
    }

    dset->kc = kc;

    dset->hnode_size = hnode_size;
    dset->cnode_size = cnode_size;

    while (dom_lpmas[dset->nr_ldoms]) {
        dset->nr_ldoms++;
    }
    while (dom_rpmas[dset->nr_rdoms]) {
        dset->nr_rdoms++;
    }
    dset->dom_lpmas = calloc(dset->nr_ldoms, sizeof(*dom_lpmas));
    dset->dom_rpmas = calloc(dset->nr_rdoms, sizeof(*dom_rpmas));
    if (unlikely(!dset->dom_lpmas || !dset->dom_rpmas)) {
        dset = ERR_PTR(-ENOMEM);
        pr_err("failed to alloc memory for dom_pmas");
        goto out;
    }
    for (i = 0; i < dset->nr_ldoms; i++) {
        dset->dom_lpmas[i] = dom_lpmas[i];
    }
    for (i = 0; i < dset->nr_rdoms; i++) {
        dset->dom_rpmas[i] = dom_rpmas[i];
    }
    dset->rep_rpma = rep_rpma;

    dset->pstage_sz = pstage_sz;

    dset->max_gc_prefetch = max_gc_prefetch;

    pr_debug(5, "dset created, hnode size: %lu, cnode size: %lu", hnode_size, cnode_size);

out:
    return dset;
}

void dset_destroy(dset_t *dset) {
    free(dset);
}

static inline uint8_t get_fgprt(dcli_t *dcli, k_t k) {
    return k_hash(dcli->kc, k) & 0xff;
}

static inline int cmp_dentry(dcli_t *dcli, int dom, const struct dentry *d1, const struct dentry *d2) {
    size_t embed_len1 = min(d1->k_len, dcli->kc->typical_len), embed_len2 = min(d2->k_len, dcli->kc->typical_len);
    size_t ovf_len1 = d1->k_len - embed_len1, ovf_len2 = d2->k_len - embed_len2;
    char *o1, *o2;
    int cmp;

    /* compare embed part */
    cmp = memncmp(d1->dkey, embed_len1, d2->dkey, embed_len2);
    if (likely(cmp || (!ovf_len1 && !ovf_len2))) {
        return cmp;
    }

    /* same embed part, compare overflow part */
    if (!ovf_len1) {
        return -1;
    }
    if (!ovf_len2) {
        return 1;
    }

    /* both has overflow part */
    o1 = lpma_get_ptr(dcli->dom_lpma_clis[dom], d1->valp.off + d1->valp.len);
    o2 = lpma_get_ptr(dcli->dom_lpma_clis[dom], d2->valp.off + d2->valp.len);
    return memncmp(o1, ovf_len1, o2, ovf_len2);
}

static inline char *get_dentry_key(dcli_t *dcli, int dom, const struct dentry *de) {
    size_t ovf_off, ovf_len;
    char *key;

    if (likely(de->k_len <= dcli->kc->typical_len)) {
        return de->dkey;
    }

    key = malloc(de->k_len);
    if (unlikely(!key)) {
        pr_err("failed to allocate memory for dentry key");
        return NULL;
    }

    ovf_off = de->valp.off + de->valp.len;
    ovf_len = de->k_len - dcli->kc->typical_len;
    memcpy(key, de->dkey, dcli->kc->typical_len);
    lpma_rd(dcli->dom_lpma_clis[dom], key + dcli->kc->typical_len, ovf_off, ovf_len);

    return key;
}

static inline void put_dentry_key(dcli_t *dcli, const struct dentry *de, char *s) {
    if (likely(de->k_len <= dcli->kc->typical_len)) {
        return;
    }
    free(s);
}

static inline size_t sizeof_dentry(dcli_t *dcli) {
    return dcli->kc->typical_len + sizeof(struct dentry);
}

static inline k_t get_hnode_lfence(dcli_t *dcli, struct mnode *mnode) {
    struct fnode *fnode = (void *) mnode + dcli->hnode_size;
    return (k_t) { .key = fnode->fences, .len = fnode->lfence_len };
}

static inline k_t get_hnode_rfence(dcli_t *dcli, struct mnode *mnode) {
    struct fnode *fnode = (void *) mnode + dcli->hnode_size;
    return (k_t) { .key = fnode->fences + fnode->lfence_len, .len = fnode->rfence_len };
}

static inline int create_sentinel(dcli_t *dcli) {
    size_t msize, fsize, hoff, coff;
    dset_t *dset = dcli->dset;
    struct mnode *mnode;
    struct fnode *fnode;
    dgroup_t dgroup;
    int ret = 0;

    fsize = sizeof(struct fnode) + dcli->kc->min.len + dcli->kc->max.len;

    /* alloc sentinel memory */
    hoff = lpma_alloc(dcli->dom_lpma_clis[0], dcli->hnode_size + fsize);
    coff = rpma_alloc(dcli->dom_rpma_clis[0], dcli->cnode_size + fsize);
    if (unlikely(IS_ERR(hoff) || IS_ERR(coff))) {
        pr_err("failed to allocate sentinel hnode / cnode: %s / %s",
               strerror(-PTR_ERR(hoff)), strerror(-PTR_ERR(coff)));
        ret = -ENOMEM;
        goto out;
    }
    dset->sentinel_hnode.dom = dset->sentinel_cnode.dom = 0;
    dset->sentinel_hnode.off = hoff;
    dset->sentinel_cnode.off = coff;

    /* init hnode sentinel */
    mnode = lpma_get_ptr(dcli->dom_lpma_clis[0], hoff);
    fnode = lpma_get_ptr(dcli->dom_lpma_clis[0], hoff + dcli->hnode_size);
    msize = sizeof(*mnode) + dcli->hfanout * sizeof(uint8_t);
    memset(mnode, 0, msize);
    mnode->prev_off = mnode->next_off = NO_OFF;
    fnode->lfence_len = dcli->kc->min.len;
    fnode->rfence_len = dcli->kc->max.len;
    memcpy(fnode->fences, dcli->kc->min.key, dcli->kc->min.len);
    memcpy(fnode->fences + dcli->kc->min.len, dcli->kc->max.key, dcli->kc->max.len);
    lpma_flush(dcli->dom_lpma_clis[0], hoff, msize);
    lpma_flush(dcli->dom_lpma_clis[0], hoff + dcli->hnode_size, fsize);
    lpma_persist(dcli->dom_lpma_clis[0]);

    /* init cnode sentinel */
    mnode = rpma_buf_alloc(dcli->dom_rpma_clis[0], msize);
    if (unlikely(!mnode)) {
        pr_err("failed to allocate memory for cnode sentinel");
        ret = -ENOMEM;
        goto out;
    }
    msize = sizeof(*mnode) + dcli->cfanout * sizeof(uint8_t);
    memset(mnode, 0, msize);
    mnode->prev_off = mnode->next_off = NO_OFF;

    /* write back cnode sentinel */
    ret = rpma_wr(dcli->dom_rpma_clis[0], coff, 0, mnode, msize);
    if (unlikely(ret < 0)) {
        pr_err("failed to write cnode sentinel: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_wr(dcli->dom_rpma_clis[0], coff + dset->cnode_size, 0, fnode, fsize);
    if (unlikely(ret < 0)) {
        pr_err("failed to write cnode sentinel: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_commit_sync(dcli->dom_rpma_clis[0]);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit cnode sentinel: %s", strerror(-ret));
    }

    rpma_buf_free(dcli->dom_rpma_clis[0], mnode, msize);

    /* init dgroup map */
    dgroup.dnodes[0] = dset->sentinel_hnode;
    dgroup.dnodes[1] = dset->sentinel_cnode;
    ret = dcli->gm_updator(dcli->priv, dcli->kc->min, dcli->kc->max, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to init dgroup map: %s", strerror(-ret));
        goto out;
    }

    /* update statistics */
    dset->pm_utilization += dcli->hnode_size + fsize;

out:
    return ret;
}

dcli_t *dcli_create(dset_t *dset, dgroup_map_update_fn gm_updator, dgroup_map_lookup_fn gm_lookuper,
                    void *priv, perf_t *perf) {
    size_t hstripe_size = UINT64_MAX, cstripe_size = UINT64_MAX;
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

    dcli->nr_ldoms = dset->nr_ldoms;
    dcli->nr_rdoms = dset->nr_rdoms;
    for (i = 0; i < dset->nr_ldoms; i++) {
        dcli->dom_lpma_clis[i] = lpma_cli_create(dset->dom_lpmas[i], perf);
        if (unlikely(IS_ERR(dcli->dom_lpma_clis[i]))) {
            pr_err("failed to create dom_lpma_cli: %s", strerror(-PTR_ERR(dcli->dom_lpma_clis[i])));
            ret = PTR_ERR(dcli->dom_lpma_clis[i]);
            free(dcli);
            dcli = ERR_PTR(ret);
            goto out;
        }
        if (hstripe_size == UINT64_MAX) {
            hstripe_size = lpma_get_stripe_size(dcli->dom_lpma_clis[i]);
        }
        if (unlikely(hstripe_size != lpma_get_stripe_size(dcli->dom_lpma_clis[i]))) {
            pr_err("inconsistent stripe size across ldoms");
            ret = -EINVAL;
            free(dcli);
            dcli = ERR_PTR(ret);
            goto out;
        }
    }
    for (i = 0; i < dset->nr_rdoms; i++) {
        dcli->dom_rpma_clis[i] = rpma_cli_create(dset->dom_rpmas[i], perf);
        if (unlikely(IS_ERR(dcli->dom_rpma_clis[i]))) {
            pr_err("failed to create dom_rpma_cli: %s", strerror(-PTR_ERR(dcli->dom_rpma_clis[i])));
            ret = PTR_ERR(dcli->dom_rpma_clis[i]);
            free(dcli);
            dcli = ERR_PTR(ret);
            goto out;
        }
        if (cstripe_size == UINT64_MAX) {
            cstripe_size = rpma_get_stripe_size(dcli->dom_rpma_clis[i]);
        }
        if (unlikely(cstripe_size != rpma_get_stripe_size(dcli->dom_rpma_clis[i]))) {
            pr_err("inconsistent stripe size across rdoms");
            ret = -EINVAL;
            free(dcli);
            dcli = ERR_PTR(ret);
            goto out;
        }
    }
    dcli->rep_rpma_cli = rpma_cli_create(dset->rep_rpma, perf);
    if (unlikely(IS_ERR(dcli->rep_rpma_cli))) {
        pr_err("failed to create rep_rpma_cli: %s", strerror(-PTR_ERR(dcli->rep_rpma_cli)));
        ret = PTR_ERR(dcli->rep_rpma_cli);
        free(dcli);
        dcli = ERR_PTR(ret);
        goto out;
    }
    if (unlikely(cstripe_size != rpma_get_stripe_size(dcli->rep_rpma_cli))) {
        pr_err("inconsistent stripe size between rep_rpma and dom_rpma");
        ret = -EINVAL;
        free(dcli);
        dcli = ERR_PTR(ret);
        goto out;
    }

    dcli->kc = dset->kc;

    dcli->hstrip_size = hstripe_size;
    dcli->cstrip_size = cstripe_size;

    if (unlikely(dset->hnode_size < hstripe_size)) {
        pr_err("hnode size %lu is smaller than stripe size %lu", dset->hnode_size, hstripe_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    if (unlikely(dset->hnode_size % dcli->hstrip_size != 0)) {
        pr_err("hnode size %lu is not a multiple of strip size %lu", dset->hnode_size, dcli->hstrip_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    if (unlikely(dset->cnode_size < cstripe_size)) {
        pr_err("cnode size %lu is smaller than stripe size %lu", dset->cnode_size, cstripe_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    if (unlikely(dset->cnode_size % dcli->cstrip_size != 0)) {
        pr_err("cnode size %lu is not a multiple of strip size %lu", dset->cnode_size, dcli->cstrip_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    dcli->hnode_size = dset->hnode_size;
    dcli->cnode_size = dset->cnode_size;

    dcli->hfanout = (dcli->hnode_size - dcli->hstrip_size) / sizeof_dentry(dcli);
    dcli->cfanout = (dcli->cnode_size - dcli->cstrip_size) / sizeof_dentry(dcli);

    if (unlikely(dcli->hfanout * sizeof(uint8_t) + sizeof(struct mnode) > dcli->hstrip_size)) {
        pr_err("hnode fanout %d is too large for stripe size %lu", dcli->hfanout, dcli->hstrip_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    if (unlikely(dcli->cfanout * sizeof(uint8_t) + sizeof(struct mnode) > dcli->cstrip_size)) {
        pr_err("cnode fanout %d is too large for stripe size %lu", dcli->cfanout, dcli->cstrip_size);
        free(dcli);
        dcli = ERR_PTR(-EINVAL);
        goto out;
    }

    dcli->pstage_sz = dset->pstage_sz;

    /* create sentinel hnode and cnode if necessary */
    if (cmpxchg2(&dset->sentinel_created, false, true)) {
        ret = create_sentinel(dcli);
        if (unlikely(ret)) {
            pr_err("failed to create sentinel hnode / cnode: %s", strerror(-ret));
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

static inline struct mnode *cnode_get_mnode(dcli_t *dcli, struct nodep cnode) {
    struct mnode *mnode;
    size_t msize;
    int ret;

    msize = sizeof(struct mnode) + dcli->cfanout * sizeof(uint8_t);

    mnode = rpma_buf_alloc(dcli->dom_rpma_clis[cnode.dom], msize);
    if (unlikely(IS_ERR(mnode))) {
        pr_err("failed to allocate memory for mnode: %s", strerror(-PTR_ERR(mnode)));
        goto out;
    }

    ret = rpma_rd(dcli->dom_rpma_clis[cnode.dom], cnode.off, 0, mnode, msize);
    if (unlikely(ret < 0)) {
        pr_err("failed to read mnode: %s", strerror(-ret));
        rpma_buf_free(dcli->dom_rpma_clis[cnode.dom], mnode, dcli->cstrip_size);
        mnode = ERR_PTR(ret);
    }

    ret = rpma_commit_sync(dcli->dom_rpma_clis[cnode.dom]);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit mnode read: %s", strerror(-ret));
        rpma_buf_free(dcli->dom_rpma_clis[cnode.dom], mnode, dcli->cstrip_size);
        mnode = ERR_PTR(ret);
    }

out:
    return mnode;
}

static inline void cnode_put_mnode(dcli_t *dcli, struct nodep cnode, struct mnode *mnode) {
    size_t msize = sizeof(struct mnode) + dcli->cfanout * sizeof(uint8_t);
    rpma_buf_free(dcli->dom_rpma_clis[cnode.dom], mnode, msize);
}

static int hnode_delete(dcli_t *dcli, struct nodep hnode, k_t key) {
    struct mnode *mnode;
    struct dnode *dnode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->dom_lpma_clis[hnode.dom], hnode.off);
    dnode = lpma_get_ptr(dcli->dom_lpma_clis[hnode.dom], hnode.off + dcli->hstrip_size);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < mnode->nr_ents; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            /* then do update */
            dnode->entries[idx].valp.rawp = TOMBSTONE;
            goto out;
        }
    }

    ret = -ENOENT;

out:
    return ret;
}

static int hnode_lookup(dcli_t *dcli, struct nodep hnode, k_t key, uint64_t *valp) {
    struct mnode *mnode;
    struct dnode *dnode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->dom_lpma_clis[hnode.dom], hnode.off);
    dnode = lpma_get_ptr(dcli->dom_lpma_clis[hnode.dom], hnode.off + dcli->hstrip_size);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < mnode->nr_ents; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            *valp = dnode->entries[idx].valp.rawp;
            goto out;
        }
    }

    ret = -ENOENT;

out:
    return ret;
}

static int hnode_upsert(dcli_t *dcli, struct nodep hnode, k_t key, uint64_t valp) {
    struct mnode *mnode;
    struct dnode *dnode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->dom_lpma_clis[hnode.dom], hnode.off);
    dnode = lpma_get_ptr(dcli->dom_lpma_clis[hnode.dom], hnode.off + dcli->hstrip_size);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < mnode->nr_ents; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            /* then do update, and set @ref flag */
            dnode->entries[idx].valp.rawp = valp;
            mnode->ref = true;
            goto out;
        }
    }

    /* find valid index */
    idx = mnode->nr_ents;
    if (unlikely(idx == dcli->hfanout)) {
        /* run out of hnode space, need split */
        ret = -ENOMEM;
        goto out;
    }

    /* insert into it */
    dnode->entries[idx].valp.rawp = valp;
    mnode->fgprt[idx] = fgprt;

    /* make the insertion visible */
    WRITE_ONCE(mnode->nr_ents, mnode->nr_ents + 1);

out:
    return ret;
}

struct sort_task {
    dcli_t *dcli;
    int dom;
};

static int sort_cmp_dentry(const void *a, const void *b, void *priv) {
    struct sort_task *task = priv;
    return cmp_dentry(task->dcli, task->dom, a, b);
}

static int *get_order_arr(dcli_t *dcli, int *nr, struct nodep hnode, struct mnode *mnode, struct dnode *dnode) {
    struct sort_task task;
    int i, *order;

    order = calloc(dcli->hfanout, sizeof(*order));
    if (unlikely(!order)) {
        return order;
    }

    *nr = 0;
    for (i = 0; i < mnode->nr_ents; i++) {
        if (mnode->fgprt[i]) {
            order[(*nr)++] = i;
        }
    }

    task.dcli = dcli;
    task.dom = hnode.dom;
    qsort_r(order, *nr, sizeof(*order), sort_cmp_dentry, &task);

    return order;
}

static int hnode_split_median(dcli_t *dcli, dgroup_t dgroup, struct nodep *new_hnode, k_t key) {
    struct mnode *prev, *next, *mnode, *mleft, *mright;
    int *order, nr, ret = 0, i, pos, dom, ddom;
    struct dnode *dnode, *dleft, *dright;
    struct fnode *fnode, *fleft, *fright;
    size_t left_off, right_off, base;
    k_t split_key, lfence, rfence;

    dom = dgroup.dnodes[0].dom;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->dom_lpma_clis[dom], dgroup.dnodes[0].off);
    dnode = lpma_get_ptr(dcli->dom_lpma_clis[dom], dgroup.dnodes[0].off + dcli->hstrip_size);
    fnode = lpma_get_ptr(dcli->dom_lpma_clis[dom], dgroup.dnodes[0].off + dcli->hnode_size);
    prev = mnode->prev_off ? lpma_get_ptr(dcli->dom_lpma_clis[dom], mnode->prev_off) : NULL;
    next = mnode->next_off ? lpma_get_ptr(dcli->dom_lpma_clis[dom], mnode->next_off) : NULL;

    /* get order array */
    order = get_order_arr(dcli, &nr, dgroup.dnodes[0], mnode, dnode);
    if (unlikely(!order)) {
        ret = -ENOMEM;
        goto out;
    }

    /* get split key */
    pos = nr / 2;
    split_key.key = get_dentry_key(dcli, dom, &dnode->entries[order[pos]]);
    split_key.len = dnode->entries[order[pos]].k_len;

    /* create new mnodes */
    base = dcli->hnode_size + sizeof(struct fnode) + split_key.len;
    left_off = lpma_alloc(dcli->dom_lpma_clis[0], base + fnode->lfence_len);
    right_off = lpma_alloc(dcli->dom_lpma_clis[0], base + fnode->rfence_len);
    if (unlikely(IS_ERR(left_off) || IS_ERR(right_off))) {
        pr_err("failed to allocate memory for new mnodes: %s / %s",
               strerror(-PTR_ERR(left_off)), strerror(-PTR_ERR(right_off)));
        ret = -ENOMEM;
        goto out;
    }
    ddom = rand_r(&dcli->seed) % dcli->dset->nr_ldoms;
    mleft = lpma_get_ptr(dcli->dom_lpma_clis[ddom], left_off);
    mright = lpma_get_ptr(dcli->dom_lpma_clis[ddom], right_off);
    dleft = lpma_get_ptr(dcli->dom_lpma_clis[ddom], left_off + dcli->hstrip_size);
    dright = lpma_get_ptr(dcli->dom_lpma_clis[ddom], right_off + dcli->hstrip_size);
    fleft = lpma_get_ptr(dcli->dom_lpma_clis[ddom], left_off + dcli->hnode_size);
    fright = lpma_get_ptr(dcli->dom_lpma_clis[ddom], right_off + dcli->hnode_size);

    /* write new fingerprint and data */
    memset(mleft->fgprt, 0, dcli->hfanout);
    memset(mright->fgprt, 0, dcli->hfanout);
    for (i = 0; i < pos; i++) {
        mleft->fgprt[i] = mnode->fgprt[order[i]];
        dleft->entries[i] = dnode->entries[order[i]];
    }
    for (i = pos; i < nr; i++) {
        mright->fgprt[i - pos] = mnode->fgprt[order[i]];
        dright->entries[i - pos] = dnode->entries[order[i]];
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

    /* link new nodes and persist data */
    mleft->prev_off = mnode->prev_off;
    mleft->next_off = right_off;
    mright->prev_off = left_off;
    mright->next_off = mnode->next_off;
    lpma_flush(dcli->dom_lpma_clis[ddom], left_off, dcli->hnode_size);
    lpma_flush(dcli->dom_lpma_clis[ddom], right_off, dcli->hnode_size);
    if (next) {
        next->prev_off = right_off;
    }
    lpma_persist(dcli->dom_lpma_clis[ddom]);

    /* persist the link, this is the durable point of this split */
    if (prev) {
        WRITE_ONCE(prev->next_off, left_off);
        lpma_flush(dcli->dom_lpma_clis[ddom], mnode->prev_off, sizeof(*prev));
    } else {
        /* TODO: FIXME */
    }
    lpma_persist(dcli->dom_lpma_clis[ddom]);

    /* make new hnode visible to upper layer */
    dgroup.dnodes[0] = (struct nodep) { ddom, left_off };
    ret = dcli->gm_updator(dcli->priv, lfence, split_key, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to update dgroup map: %s", strerror(-ret));
        goto out;
    }
    dgroup.dnodes[0] = (struct nodep) { ddom, right_off };
    ret = dcli->gm_updator(dcli->priv, split_key, rfence, dgroup);
    if (unlikely(ret)) {
        pr_err("failed to update dgroup map: %s", strerror(-ret));
        goto out;
    }

    /* get the new hnode of key */
    if (unlikely(!split_key.key)) {
        ret = -ENOMEM;
        goto out;
    }
    if (k_cmp(dcli->kc, key, split_key) >= 0) {
        *new_hnode = (struct nodep) { ddom, right_off };
    } else {
        *new_hnode = (struct nodep) { ddom, left_off };
    }

    put_dentry_key(dcli, &dnode->entries[order[pos]], split_key.key);

    /* TODO: GC old hnode */

    /* update statistics */
    dcli->dset->pm_utilization += 2 * (dcli->hnode_size + sizeof(struct fnode) + split_key.len);

out:
    return ret;
}

int dset_upsert(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t valp) {
    struct nodep hnode = dgroup.dnodes[0];
    int ret;

    ret = hnode_upsert(dcli, hnode, key, valp);
    if (unlikely(ret == -ENOMEM)) {
        /* hnode full, split and retry */
        ret = hnode_split_median(dcli, dgroup, &hnode, key);
        if (unlikely(ret)) {
            pr_err("hnode split failed: %s", strerror(-ret));
            goto out;
        }
        ret = hnode_upsert(dcli, hnode, key, valp);
    }
    if (unlikely(ret)) {
        pr_err("dset upsert failed: %s", strerror(-ret));
        goto out;
    }

out:
    return ret;
}

int dset_delete(dcli_t *dcli, dgroup_t dgroup, k_t key) {
    return hnode_delete(dcli, dgroup.dnodes[0], key);
}

int dset_lookup(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t *valp) {
    return hnode_lookup(dcli, dgroup.dnodes[0], key, valp);
}

int dset_create_valp(dcli_t *dcli, uint64_t *valp, k_t key, const void *val, size_t len) {
    size_t off, persisted = 0, stage_len, ovf_len;
    struct valp vp;
    uint64_t data;
    int ret = 0;

    if (unlikely(len >= (1ul << 14))) {
        ret = -EINVAL;
        goto out;
    }

    /* inline case, value is small enough to hold within a 64bit pointer */
    if (key.len <= dcli->kc->typical_len) {
        data = *(uint64_t *) val;
        if (len < 8 || (len == 8 && data < (1ul << 60))) {
            vp.is_inline = 1;
            vp.inline_len = len;
            vp.inline_val = data;
            goto out;
        }
    }

    /* calculate key overflow length */
    ovf_len = key.len - min(key.len, dcli->kc->typical_len);

    /* alloc value memory in local PM */
    vp.off = off = lpma_alloc(dcli->lpma_cli, len + ovf_len);
    bonsai_assert(off < (1ul << 48));
    if (unlikely(IS_ERR(off))) {
        ret = -ENOMEM;
        pr_err("failed to alloc memory in local PM for value");
        goto out;
    }

    /* persist value with staging persist */
    while (persisted < len) {
        stage_len = min(len - persisted, dcli->pstage_sz);

        lpma_wr_nc(dcli->lpma_cli, off, val, stage_len);
        lpma_persist(dcli->lpma_cli);

        off += stage_len;
        val += stage_len;
        persisted += stage_len;
    }

    /* persist key overflow part */
    if (ovf_len) {
        lpma_wr_nc(dcli->lpma_cli, off, key.key + dcli->kc->typical_len, ovf_len);
        lpma_persist(dcli->lpma_cli);
    }

    vp.inline_val = 0;
    vp.is_lpm = 1;
    vp.len = len;

    /* update statistics */
    dcli->dset->pm_utilization += len + ovf_len;

out:
    *valp = vp.rawp;
    return ret;
}

static inline int get_val_lpm(dcli_t *dcli, void *buf, int bufsz, struct valp vp) {
    int read_len = min(bufsz, vp.len);
    bonsai_assert(vp.is_lpm);
    lpma_rd(dcli->lpma_cli, buf, vp.off, read_len);
    return read_len;
}

static inline int get_val_rpm(dcli_t *dcli, void *buf, int bufsz, struct valp vp) {
    int ret;

    ret = rpma_rd(dcli->rpma_cli, vp.off, 0, buf, min(bufsz, vp.len));
    if (unlikely(ret < 0)) {
        pr_err("failed to read value from remote PM: %s", strerror(-ret));
        goto out;
    }

    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit value read from remote PM: %s", strerror(-ret));
        goto out;
    }

out:
    return ret;
}

int dset_get_val(dcli_t *dcli, void *buf, int bufsz, uint64_t valp) {
    struct valp vp = { .rawp = valp };
    int ret = 0;

    if (unlikely(bufsz < sizeof(uint64_t))) {
        ret = -EINVAL;
        goto out;
    }

    if (vp.is_inline) {
        ret = vp.inline_len;
        *(uint64_t *) buf = vp.inline_val;
        goto out;
    }

    ret = vp.is_lpm ? get_val_lpm(dcli, buf, bufsz, vp) : get_val_rpm(dcli, buf, bufsz, vp);

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
    target = dset->pivot_hnode;
    mnode = lpma_get_ptr(dcli->lpma_cli, target);
    while (mnode->ref) {
        mnode->ref = false;
        if (unlikely(mnode->next_off == NO_OFF)) {
            target = dset->sentinel_hnode;
        } else {
            target = mnode->next_off;
        }
        mnode = lpma_get_ptr(dcli->lpma_cli, target);
    }

    return target;
}

/*
 * ┌───────┐ ┌───────┐ ┌───────┐
 * │       │ │       │ │       │
 * │ hnode │ │ hnode │ │ hnode │
 * │       │ │       │ │       │
 * └───┬───┘ └───┬───┘ └───┬───┘
 *     │         │         │
 *     └─────────┼─────────┘ hardware gather
 *               │
 * ┌───────────┬─▼─────────────┐
 * │┼──────────┤     cnode     │
 * ││          │               │
 * ││  used    │   available   │
 * ││          │               │
 * │┼──────────┤               │
 * └───────────┴───────────────┘
 *
 *              ────────►
 *                append-only (out-of-place)
 */
static inline int gc_hnodes(dcli_t *dcli) {
    size_t target, cnode_off, dnode_start_off;
    int ret, nr_gc_ents, nr_gc_hnodes;
    struct mnode *hmnode, *cmnode;
    k_t lfence, rfence;
    rpma_buf_t *bufs;
    dgroup_t dgroup;

    /* choose GC target (strategy: the least recently updated node) */
    target = choose_gc_target(dcli);
    hmnode = lpma_get_ptr(dcli->lpma_cli, target);
    lfence = get_hnode_lfence(dcli, hmnode);
    rfence = get_hnode_rfence(dcli, hmnode);

    /* get the corresponding cnode */
    ret = dcli->gm_lookuper(dcli->priv, &dgroup, lfence);
    if (unlikely(ret)) {
        pr_err("failed to lookup dgroup map: %s", strerror(-ret));
        goto out;
    }
    cnode_off = dgroup.dnodes[1];
    cmnode = cnode_get_mnode(dcli, cnode_off);
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

    /* get target hnodes (prefetch succeeding hnodes) and modify cmnode */
    nr_gc_hnodes = nr_gc_ents = 0;

    while (dgroup.dnodes[1] == cnode_off && nr_gc_ents + cmnode->nr_ents < dcli->cfanout) {
        bufs[nr_gc_hnodes].start = (void *) hmnode + dcli->hstrip_size;
        bufs[nr_gc_hnodes].size = sizeof_dentry(dcli) * hmnode->nr_ents;

        memcpy(cmnode->fgprt + cmnode->nr_ents, hmnode->fgprt, sizeof(uint8_t) * hmnode->nr_ents);
        cmnode->nr_ents += hmnode->nr_ents;

        nr_gc_hnodes++;
        nr_gc_ents += hmnode->nr_ents;
    }

    bufs[nr_gc_hnodes] = (rpma_buf_t) { NULL, 0 };

    if (unlikely(nr_gc_hnodes == 0)) {
        /* TODO: FIXME: handle cnode split case! */
    }

    /* write cnode data */
    dnode_start_off = cnode_off + dcli->cstrip_size + cmnode->nr_ents * sizeof_dentry(dcli);
    ret = rpma_wr_(dcli->rpma_cli, dnode_start_off, bufs, 0);
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to GC data to cnode: %s", strerror(-ret));
        goto out;
    }

    /* write cnode metadata */
    ret = rpma_wr(dcli->rpma_cli, cnode_off, 0, cmnode, sizeof(*cmnode) + cmnode->nr_ents * sizeof(uint8_t));
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to GC metadata to cnode: %s", strerror(-ret));
        goto out;
    }

    /* commit remote changes and wait */
    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        free(bufs);
        pr_err("failed to commit GC data to cnode");
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
        /* repeatedly GC hnodes until enough GC size */
        ret = gc_hnodes(dcli);
        if (unlikely(ret)) {
            pr_err("failed to GC hnodes: %s", strerror(-ret));
            goto out;
        }
    }

out:
    return ret;
}
