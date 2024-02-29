/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ scalable data layer
 *
 * Hohai University
 */

#define _GNU_SOURCE

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

struct dentry {
    struct valp valp;
    uint32_t k_len;
    uint32_t reserve;
    char dkey[];
};

struct mnode {
    size_t next_off, prev_off;
    /* "0" means empty slot */
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

    size_t sentinel_hnode, sentinel_cnode;
    bool sentinel_created;

    size_t hnode_size, cnode_size;

    lpma_t *lpma;
    rpma_t *rpma;

    size_t pstage_sz;
};

struct dcli {
    dset_t *dset;

    dset_chg_hint_cb_t chg_hint_cb;
    void *priv;

    rpma_cli_t *rpma_cli;
    lpma_cli_t *lpma_cli;
    perf_t *perf;
    kc_t *kc;

    size_t hstrip_size, cstrip_size;
    size_t hnode_size, cnode_size;
    int hfanout, cfanout;

    size_t pstage_sz;
};

dset_t *dset_create(kc_t *kc, size_t hnode_size, size_t cnode_size, lpma_t *lpma, rpma_t *rpma, size_t pstage_sz) {
    dset_t *dset;

    dset = calloc(1, sizeof(*dset));
    if (unlikely(!dset)) {
        dset = ERR_PTR(-ENOMEM);
        goto out;
    }

    dset->kc = kc;

    dset->hnode_size = hnode_size;
    dset->cnode_size = cnode_size;

    dset->lpma = lpma;
    dset->rpma = rpma;

    dset->pstage_sz = pstage_sz;

    pr_debug(5, "dset created, hnode size: %lu, cnode size: %lu", hnode_size, cnode_size);

out:
    return dset;
}

void dset_destroy(dset_t *dset) {
    free(dset);
}

static inline uint64_t get_fgprt(dcli_t *dcli, k_t k) {
    uint64_t hash = k_hash(dcli->kc, k);
    return !hash ? 1 : hash;
}

static inline int cmp_dentry(dcli_t *dcli, const struct dentry *d1, const struct dentry *d2) {
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
    o1 = lpma_get_ptr(dcli->lpma_cli, d1->valp.off + d1->valp.len);
    o2 = lpma_get_ptr(dcli->lpma_cli, d2->valp.off + d2->valp.len);
    return memncmp(o1, ovf_len1, o2, ovf_len2);
}

static inline char *get_dentry_key(dcli_t *dcli, const struct dentry *de) {
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
    lpma_rd(dcli->lpma_cli, key + dcli->kc->typical_len, ovf_off, ovf_len);

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

static inline int create_sentinel(dcli_t *dcli) {
    dset_t *dset = dcli->dset;
    size_t msize, fsize;
    struct mnode *mnode;
    struct fnode *fnode;
    int ret = 0;

    fsize = sizeof(struct fnode) + dcli->kc->min.len + dcli->kc->max.len;

    /* alloc sentinel memory */
    dset->sentinel_hnode = lpma_alloc(dcli->lpma_cli, dcli->hnode_size + fsize);
    dset->sentinel_cnode = rpma_alloc(dcli->rpma_cli, dcli->cnode_size + fsize);
    if (unlikely(IS_ERR(dset->sentinel_hnode) || IS_ERR(dset->sentinel_cnode))) {
        pr_err("failed to allocate sentinel hnode / cnode: %s / %s",
               strerror(-PTR_ERR(dset->sentinel_hnode)), strerror(-PTR_ERR(dset->sentinel_cnode)));
        ret = -ENOMEM;
        goto out;
    }

    /* init hnode sentinel */
    mnode = lpma_get_ptr(dcli->lpma_cli, dset->sentinel_hnode);
    fnode = lpma_get_ptr(dcli->lpma_cli, dset->sentinel_hnode + dcli->hnode_size);
    msize = sizeof(*mnode) + dcli->hfanout * sizeof(uint8_t);
    memset(mnode, 0, msize);
    mnode->prev_off = mnode->next_off = NO_OFF;
    fnode->lfence_len = dcli->kc->min.len;
    fnode->rfence_len = dcli->kc->max.len;
    memcpy(fnode->fences, dcli->kc->min.key, dcli->kc->min.len);
    memcpy(fnode->fences + dcli->kc->min.len, dcli->kc->max.key, dcli->kc->max.len);
    lpma_flush(dcli->lpma_cli, dset->sentinel_hnode, msize);
    lpma_flush(dcli->lpma_cli, dset->sentinel_hnode + dcli->hnode_size, fsize);
    lpma_persist(dcli->lpma_cli);

    /* init cnode sentinel */
    mnode = rpma_buf_alloc(dcli->rpma_cli, msize);
    if (unlikely(!mnode)) {
        pr_err("failed to allocate memory for cnode sentinel");
        ret = -ENOMEM;
        goto out;
    }
    msize = sizeof(*mnode) + dcli->cfanout * sizeof(uint8_t);
    memset(mnode, 0, msize);
    mnode->prev_off = mnode->next_off = NO_OFF;

    /* write back cnode sentinel */
    ret = rpma_wr(dcli->rpma_cli, dset->sentinel_cnode, 0, mnode, msize);
    if (unlikely(ret < 0)) {
        pr_err("failed to write cnode sentinel: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_wr(dcli->rpma_cli, dset->sentinel_cnode + dset->cnode_size, 0, fnode, fsize);
    if (unlikely(ret < 0)) {
        pr_err("failed to write cnode sentinel: %s", strerror(-ret));
        goto out;
    }
    ret = rpma_commit_sync(dcli->rpma_cli);
    if (unlikely(ret < 0)) {
        pr_err("failed to commit cnode sentinel: %s", strerror(-ret));
    }

out:
    return ret;
}

dcli_t *dcli_create(dset_t *dset, dset_chg_hint_cb_t chg_hint_cb, void *priv, perf_t *perf) {
    size_t hstripe_size, cstripe_size;
    dcli_t *dcli;
    int ret;

    dcli = calloc(1, sizeof(*dcli));
    if (unlikely(!dcli)) {
        dcli = ERR_PTR(-ENOMEM);
        goto out;
    }

    dcli->chg_hint_cb = chg_hint_cb;
    dcli->priv = priv;

    dcli->perf = perf;
    dcli->rpma_cli = rpma_cli_create(dset->rpma, perf);
    dcli->lpma_cli = lpma_cli_create(dset->lpma, perf);
    if (unlikely(IS_ERR(dcli->rpma_cli) || IS_ERR(dcli->lpma_cli))) {
        dcli = ERR_PTR(-ENOMEM);
        pr_err("failed to create rpma_cli / lpma_cli: %s / %s",
               strerror(-PTR_ERR(dcli->rpma_cli)), strerror(-PTR_ERR(dcli->lpma_cli)));
        goto out;
    }

    dcli->kc = dset->kc;

    hstripe_size = lpma_get_stripe_size(dcli->lpma_cli);
    cstripe_size = rpma_get_stripe_size(dcli->rpma_cli);

    dcli->hstrip_size = lpma_get_strip_size(dcli->lpma_cli);
    dcli->cstrip_size = rpma_get_strip_size(dcli->rpma_cli);

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

static int hnode_delete(dcli_t *dcli, size_t mnode_off, k_t key) {
    struct mnode *mnode;
    struct dnode *dnode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->lpma_cli, mnode_off);
    dnode = lpma_get_ptr(dcli->lpma_cli, mnode_off + dcli->hstrip_size);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < dcli->hfanout; idx++) {
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

static int hnode_lookup(dcli_t *dcli, size_t mnode_off, k_t key, uint64_t *valp) {
    struct mnode *mnode;
    struct dnode *dnode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->lpma_cli, mnode_off);
    dnode = lpma_get_ptr(dcli->lpma_cli, mnode_off + dcli->hstrip_size);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < dcli->hfanout; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            *valp = dnode->entries[idx].valp.rawp;
            goto out;
        }
    }

    ret = -ENOENT;

out:
    return ret;
}

static int hnode_upsert(dcli_t *dcli, size_t mnode_off, k_t key, uint64_t valp) {
    struct mnode *mnode;
    struct dnode *dnode;
    int idx, ret = 0;
    uint64_t fgprt;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->lpma_cli, mnode_off);
    dnode = lpma_get_ptr(dcli->lpma_cli, mnode_off + dcli->hstrip_size);

    /* if key exists */
    fgprt = get_fgprt(dcli, key);
    for (idx = 0; idx < dcli->hfanout; idx++) {
        if (mnode->fgprt[idx] == fgprt) {
            /* then do update */
            dnode->entries[idx].valp.rawp = valp;
            goto out;
        }
    }

    /* find valid index */
    for (idx = 0; idx < dcli->hfanout; idx++) {
        if (mnode->fgprt[idx] == 0) {
            break;
        }
    }
    if (unlikely(idx == dcli->hfanout)) {
        /* run out of hnode space, need split */
        ret = -ENOMEM;
        goto out;
    }

    /* insert into it */
    dnode->entries[idx].valp.rawp = valp;
    mnode->fgprt[idx] = fgprt;

out:
    return ret;
}

static int sort_cmp_dentry(const void *a, const void *b, void *priv) {
    dcli_t *dcli = priv;
    return cmp_dentry(dcli, a, b);
}

static int *get_order_arr(dcli_t *dcli, int *nr, struct mnode *mnode, struct dnode *dnode) {
    int i, *order;

    order = calloc(dcli->hfanout, sizeof(*order));
    if (unlikely(!order)) {
        return order;
    }

    *nr = 0;
    for (i = 0; i < dcli->hfanout; i++) {
        if (mnode->fgprt[i]) {
            order[(*nr)++] = i;
        }
    }

    qsort_r(order, *nr, sizeof(*order), sort_cmp_dentry, dcli);

    return order;
}

static int hnode_split_median(dcli_t *dcli, size_t *new_hnode, k_t key, dhint_t hint) {
    struct mnode *prev, *next, *mnode, *mleft, *mright;
    struct dnode *dnode, *dleft, *dright;
    struct fnode *fnode, *fleft, *fright;
    size_t left_off, right_off, base;
    int *order, nr, ret = 0, i, pos;
    k_t split_key, lfence, rfence;
    struct free_hnode_info *info;

    /* get mnode and dnode address */
    mnode = lpma_get_ptr(dcli->lpma_cli, hint.hints[0]);
    dnode = lpma_get_ptr(dcli->lpma_cli, hint.hints[0] + dcli->hstrip_size);
    fnode = lpma_get_ptr(dcli->lpma_cli, hint.hints[0] + dcli->hnode_size);
    prev = mnode->prev_off ? lpma_get_ptr(dcli->lpma_cli, mnode->prev_off) : NULL;
    next = mnode->next_off ? lpma_get_ptr(dcli->lpma_cli, mnode->next_off) : NULL;

    /* get order array */
    order = get_order_arr(dcli, &nr, mnode, dnode);
    if (unlikely(!order)) {
        ret = -ENOMEM;
        goto out;
    }

    /* get split key */
    pos = nr / 2;
    split_key.key = get_dentry_key(dcli, &dnode->entries[order[pos]]);
    split_key.len = dnode->entries[order[pos]].k_len;

    /* create new mnodes */
    base = dcli->hnode_size + sizeof(struct fnode) + split_key.len;
    left_off = lpma_alloc(dcli->lpma_cli, base + fnode->lfence_len);
    right_off = lpma_alloc(dcli->lpma_cli, base + fnode->rfence_len);

    right_off = lpma_alloc(dcli->lpma_cli, dcli->hnode_size);
    if (unlikely(IS_ERR(left_off) || IS_ERR(right_off))) {
        pr_err("failed to allocate memory for new mnodes: %s / %s",
               strerror(-PTR_ERR(left_off)), strerror(-PTR_ERR(right_off)));
        ret = -ENOMEM;
        goto out;
    }
    mleft = lpma_get_ptr(dcli->lpma_cli, left_off);
    mright = lpma_get_ptr(dcli->lpma_cli, right_off);
    dleft = lpma_get_ptr(dcli->lpma_cli, left_off + dcli->hstrip_size);
    dright = lpma_get_ptr(dcli->lpma_cli, right_off + dcli->hstrip_size);
    fleft = lpma_get_ptr(dcli->lpma_cli, left_off + dcli->hnode_size);
    fright = lpma_get_ptr(dcli->lpma_cli, right_off + dcli->hnode_size);

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
    lpma_flush(dcli->lpma_cli, left_off, dcli->hnode_size);
    lpma_flush(dcli->lpma_cli, right_off, dcli->hnode_size);
    if (next) {
        next->prev_off = right_off;
    }
    lpma_persist(dcli->lpma_cli);

    /* persist the link, this is the durable point of this split */
    if (prev) {
        WRITE_ONCE(prev->next_off, left_off);
        lpma_flush(dcli->lpma_cli, mnode->prev_off, sizeof(*prev));
    } else {
        /* TODO: FIXME */
    }
    lpma_persist(dcli->lpma_cli);

    /* make new hnode visible to upper layer */
    if (dcli->chg_hint_cb) {
        dcli->chg_hint_cb(dcli->priv, (dhint_t) { .hints = { left_off, hint.hints[1] } }, lfence, split_key);
        dcli->chg_hint_cb(dcli->priv, (dhint_t) { .hints = { right_off, hint.hints[1] } }, split_key, rfence);
    }

    /* get the new hnode of key */
    if (unlikely(!split_key.key)) {
        ret = -ENOMEM;
        goto out;
    }
    if (k_cmp(dcli->kc, key, split_key) >= 0) {
        *new_hnode = right_off;
    } else {
        *new_hnode = left_off;
    }

    put_dentry_key(dcli, &dnode->entries[order[pos]], split_key.key);

    /* TODO: GC old hnode */

out:
    return ret;
}

int dset_upsert(dcli_t *dcli, k_t key, uint64_t valp, dhint_t hint) {
    size_t hnode_off = hint.hints[0];
    int ret;

    ret = hnode_upsert(dcli, hnode_off, key, valp);
    if (unlikely(ret == -ENOMEM)) {
        /* hnode full, split and retry */
        ret = hnode_split_median(dcli, &hnode_off, key, hint);
        if (unlikely(ret)) {
            pr_err("hnode split failed: %s", strerror(-ret));
            goto out;
        }
        ret = hnode_upsert(dcli, hnode_off, key, valp);
    }
    if (unlikely(ret)) {
        pr_err("dset upsert failed: %s", strerror(-ret));
        goto out;
    }

out:
    return ret;
}

int dset_delete(dcli_t *dcli, k_t key, dhint_t hint) {
    return hnode_delete(dcli, hint.hints[0], key);
}

int dset_lookup(dcli_t *dcli, k_t key, uint64_t *valp, dhint_t hint) {
    return hnode_lookup(dcli, hint.hints[0], key, valp);
}

int dset_create_valp(dcli_t *dcli, uint64_t *valp, k_t key, const void *val, size_t len) {
    size_t off, persisted = 0, stage_len;
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

    /* alloc value memory in local PM */
    vp.off = off = lpma_alloc(dcli->lpma_cli, len + key.len);
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
    if (key.len > dcli->kc->typical_len) {
        lpma_wr_nc(dcli->lpma_cli, off, key.key + dcli->kc->typical_len, len - dcli->kc->typical_len);
        lpma_persist(dcli->lpma_cli);
    }

    vp.inline_val = 0;
    vp.is_lpm = 1;
    vp.len = len;

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
