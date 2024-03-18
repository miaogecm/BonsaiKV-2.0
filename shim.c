/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ shim layer
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include "shim.h"
#include "lock.h"
#include "bitmap.h"
#include "oplog.h"
#include "dset.h"
#include "kv.h"

#define INODE_FANOUT    46

struct shim {
    index_t *index;
    kc_t *kc;

    inode_t *sentinel;
};

struct shim_cli {
    shim_t *shim;

    /* cache frequently-accessed fields in @shim (to reduce pointer chasing) */
    index_t *index;
    kc_t *kc;

    logger_cli_t *logger_cli;
    dcli_t *dcli;
};

/* Each inode has 8 cache lines */
struct inode {
    uint32_t validmap;
    uint32_t deleted;

    inode_t *next;

    dgroup_t dgroup;

    uint8_t fgprt[INODE_FANOUT];
    uint8_t rfence_len;
    uint8_t lfence_len;

    spinlock_t lock;
    seqcount_t seq;

    uint64_t logs[INODE_FANOUT];

    char fences[];
};

static inline inode_t *create_sentinel(shim_t *shim) {
    inode_t *sentinel;

    sentinel = calloc(1, sizeof(*sentinel) + 1);
    if (unlikely(!sentinel)) {
        sentinel = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate sentinel memory");
        goto out;
    }

    sentinel->validmap = 0;
    sentinel->deleted = 0;
    sentinel->next = NULL;
    sentinel->dgroup = (dgroup_t) { };
    seqcount_init(&sentinel->seq);
    spin_lock_init(&sentinel->lock);

out:
    return sentinel;
}

shim_t *shim_create(index_t *index, kc_t *kc) {
    shim_t *shim;

    shim = calloc(1, sizeof(*shim));
    if (unlikely(!shim)) {
        shim = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate shim memory");
        goto out;
    }

    shim->index = index;

    shim->kc = kc;

    shim->sentinel = create_sentinel(shim);
    if (unlikely(IS_ERR(shim->sentinel))) {
        pr_err("failed to create sentinel");
    }

    pr_debug(5, "shim created");

out:
    return shim;
}

shim_cli_t *shim_create_cli(shim_t *shim, logger_cli_t *logger_cli) {
    shim_cli_t *shim_cli;

    shim_cli = calloc(1, sizeof(*shim_cli));
    if (unlikely(!shim_cli)) {
        shim_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate shim_cli memory");
        goto out;
    }

    shim_cli->shim = shim;

    shim_cli->index = shim->index;

    shim_cli->kc = shim->kc;

    shim_cli->logger_cli = logger_cli;

    pr_debug(10, "shim client created");

out:
    return shim_cli;
}

void shim_set_dcli(shim_cli_t *shim_cli, dcli_t *dcli) {
    shim_cli->dcli = dcli;
}

void shim_destroy_cli(shim_cli_t *shim_cli) {
    free(shim_cli);

    pr_debug(10, "shim client destroyed");
}

static inline k_t i_lfence(inode_t *inode) {
    return (k_t) { inode->fences, inode->lfence_len };
}

static inline k_t i_rfence(inode_t *inode) {
    return (k_t) { inode->fences + inode->lfence_len, inode->rfence_len };
}

static inline bool key_within_rfence(shim_cli_t *shim_cli, inode_t *inode, k_t key) {
    return !inode->next || k_cmp(shim_cli->kc, key, i_rfence(inode)) < 0;
}

static inline inode_t *iget_unlocked(shim_cli_t *shim_cli, k_t key) {
    index_t *index = shim_cli->index;
    inode_t *inode;

    inode = index_find_first_ge(index, key);

    return inode;
}

static inline inode_t *iget_locked(shim_cli_t *shim_cli, k_t key) {
    inode_t *inode, *next;

reget:
    inode = iget_unlocked(shim_cli, key);
    spin_lock(&inode->lock);
    if (unlikely(inode->deleted)) {
        spin_unlock(&inode->lock);
        goto reget;
    }

    while (unlikely(!key_within_rfence(shim_cli, inode, key))) {
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

    return inode;
}

struct log_info {
    k_t key;
    int pos;
};

static int sort_cmp(const void *a, const void *b, void *priv) {
    const struct log_info *la = a;
    const struct log_info *lb = b;
    shim_cli_t *cli = priv;

    return k_cmp(cli->kc, la->key, lb->key);
}

/*
 * Move @inode's keys within range [cut, fence) to another node N.
 * @inode's original responsible key range [l, fence) will be split to [l, cut) and [cut, fence).
 * Both @inode and its successor N will be locked.
 */
static inline void i_split(shim_cli_t *shim_cli, inode_t *inode, k_t cut) {
    struct log_info logs[INODE_FANOUT], *log;
    uint64_t valp, lvmp, rvmp, lmask = 0;
    int pos, cnt = 0;
    inode_t *new;
    k_t fence;

    /* collect log infos */
    for_each_set_bit(pos, &inode->validmap, INODE_FANOUT) {
        logger_get(shim_cli->logger_cli, inode->logs[pos], &logs[pos].key, &valp);
        logs[cnt++].pos = pos;
    }

    /* sort log infos */
    qsort_r(logs, INODE_FANOUT, sizeof(*logs), sort_cmp, shim_cli);

    /* set validmaps accordingly */
    if (cut.key) {
        /* use dedicated split key */
        for (pos = 0; pos < cnt; pos++) {
            log = &logs[pos];
            if (k_cmp(shim_cli->kc, log->key, cut) >= 0) {
                break;
            }
            __set_bit(log->pos, &lmask);
        }
        fence = cut;
    } else {
        /* use median */
        for (pos = 0; pos < cnt / 2; pos++) {
            __set_bit(logs[pos].pos, &lmask);
        }
        fence = logs[pos].key;
    }
    lvmp = lmask;
    rvmp = inode->validmap & ~lmask;

    /* alloc and init new node */
    new = calloc(1, sizeof(*new) + inode->rfence_len + 1);
    if (unlikely(!new)) {
        pr_err("failed to allocate inode memory");
        return;
    }
    new->validmap = rvmp;
    new->dgroup = inode->dgroup;
    new->deleted = 0;
    new->next = inode->next;
    new->lfence_len = fence.len;
    new->rfence_len = inode->rfence_len;
    memcpy(i_lfence(new).key, fence.key, fence.len);
    memcpy(i_rfence(new).key, i_rfence(inode).key, inode->rfence_len);
    memcpy(new->fgprt, inode->fgprt, sizeof(new->fgprt));
    memcpy(new->logs, inode->logs, sizeof(new->logs));
    seqcount_init(&new->seq);
    spin_lock(&new->lock);

    /* update @inode atomically */
    write_seqcount_begin(&inode->seq);
    inode->validmap = lvmp;
    inode->next = new;
    inode->rfence_len = fence.len;
    memcpy(i_rfence(inode).key, fence.key, fence.len);
    write_seqcount_end(&inode->seq);

    /* insert new node into upper index */
    index_upsert(shim_cli->index, fence, new);
}

static inline int search_log(shim_cli_t *shim_cli, inode_t *inode, uint32_t validmap,
                             k_t key, uint64_t *valp, int *pos) {
    uint8_t fgprt = k_fgprt(shim_cli->kc, key);
    int i, ret = -ERANGE;
    k_t log_key;
    op_t op;

    *pos = INODE_FANOUT;

    for_each_set_bit(i, &validmap, INODE_FANOUT) {
        if (inode->fgprt[i] != fgprt) {
            continue;
        }

        op = logger_get(shim_cli->logger_cli, inode->logs[i], &log_key, valp);

        if (k_cmp(shim_cli->kc, key, log_key) != 0) {
            /* not this key, hash collision */
            continue;
        }

        if (unlikely(op == OP_DEL)) {
            ret = -ENOENT;
            goto out;
        }

        ret = 0;
        *pos = i;
        break;
    }

out:
    return ret;
}

static inline int search_dset(shim_cli_t *shim_cli, inode_t *inode, k_t key, uint64_t *valp) {
    return dset_lookup(shim_cli->dcli, inode->dgroup, key, valp);
}

int shim_upsert(shim_cli_t *shim_cli, k_t key, oplog_t log) {
    unsigned long validmap;
    inode_t *inode, *next;
    uint64_t valp;
    int pos, ret;

    inode = iget_locked(shim_cli, key);

    validmap = inode->validmap;

    ret = search_log(shim_cli, inode, validmap, key, &valp, &pos);
    if (unlikely(!IS_ERR(ret))) {
        /* Key exists, update */
        ret = -EEXIST;
    } else if (ret == -ENOENT || ret == -ERANGE) {
        pos = find_first_zero_bit(&validmap, INODE_FANOUT);

        if (unlikely(pos == INODE_FANOUT)) {
            /* inode full, need to split */
            i_split(shim_cli, inode, (k_t) { });

            /* crab to correct inode */
            next = inode->next;
            if (k_cmp(shim_cli->kc, key, i_rfence(inode)) >= 0) {
                spin_unlock(&inode->lock);
                inode = next;
            } else {
                spin_unlock(&next->lock);
            }

            /* retry find valid position */
            validmap = inode->validmap;
            pos = find_first_zero_bit(&validmap, INODE_FANOUT);
            bonsai_assert(pos < INODE_FANOUT);
        }

        __set_bit(pos, &validmap);

        ret = 0;
    } else {
        goto out;
    }

    inode->logs[pos] = log;
    inode->fgprt[pos] = k_fgprt(shim_cli->kc, key);
    barrier();

    inode->validmap = validmap;

    spin_unlock(&inode->lock);

out:
    return ret;
}

int shim_lookup(shim_cli_t *shim_cli, k_t key, uint64_t *valp) {
    inode_t *inode, *next;
    char rfence_buf[256];
    uint32_t validmap;
    size_t rfence_len;
    unsigned int seq;
    int pos, ret;
    k_t rfence;

    inode = iget_unlocked(shim_cli, key);

retry:
    seq = read_seqcount_begin(&inode->seq);

    rfence_len = inode->rfence_len;
    memcpy(rfence_buf, i_rfence(inode).key, rfence_len);
    next = inode->next;

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    validmap = inode->validmap;

    rfence = (k_t) { rfence_buf, rfence_len };

    if (unlikely(k_cmp(shim_cli->kc, key, rfence) >= 0)) {
        inode = next;
        goto retry;
    }

    ret = search_log(shim_cli, inode, validmap, key, valp, &pos);

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    /* tiered lookup */
    if (ret == -ERANGE) {
        ret = search_dset(shim_cli, inode, key, valp);
    }

    return ret;
}

int shim_update_dgroup(shim_cli_t *shim_cli, k_t s, k_t t, dgroup_t dgroup) {
    k_t lfence, rfence, is, it;
    inode_t *inode, *next;

    inode = iget_locked(shim_cli, s);

    for (; inode;) {
        /* get inode fences */
        lfence = i_lfence(inode);
        rfence = i_rfence(inode);

        /* get the overlapping part of [s, t) and [lfence, rfence) */
        is = s;
        it = t;
        if (k_cmp(shim_cli->kc, is, lfence) < 0) {
            is = lfence;
        }
        if (k_cmp(shim_cli->kc, it, rfence) > 0) {
            it = rfence;
        }

        /* no overlapping part, we stop */
        if (unlikely(k_cmp(shim_cli->kc, is, it) >= 0)) {
            spin_unlock(&inode->lock);
            break;
        }

        /* dgroup already set */
        if (dgroup_is_eq(inode->dgroup, dgroup)) {
            goto next;
        }

        /* split if is > lfence */
        if (k_cmp(shim_cli->kc, is, lfence) > 0) {
            i_split(shim_cli, inode, is);
            next = inode->next;
            spin_unlock(&inode->lock);
            inode = next;
        }

        /* split if it < rfence */
        if (k_cmp(shim_cli->kc, it, rfence) < 0) {
            i_split(shim_cli, inode, it);
            spin_unlock(&inode->next->lock);
        }

        dgroup_copy(&inode->dgroup, dgroup);

next:
        next = inode->next;
        if (next) {
            spin_lock(&next->lock);
        }
        spin_unlock(&inode->lock);
        inode = next;
    }

    return 0;
}

int shim_lookup_dgroup(shim_cli_t *shim_cli, k_t key, dgroup_t *dgroup) {
    inode_t *inode, *next;
    char rfence_buf[256];
    size_t rfence_len;
    unsigned int seq;
    int ret = 0;
    k_t rfence;

    inode = iget_unlocked(shim_cli, key);

retry:
    seq = read_seqcount_begin(&inode->seq);

    rfence_len = inode->rfence_len;
    memcpy(rfence_buf, i_rfence(inode).key, rfence_len);
    next = inode->next;

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    *dgroup = inode->dgroup;

    rfence = (k_t) { rfence_buf, rfence_len };
    if (unlikely(k_cmp(shim_cli->kc, key, rfence) >= 0)) {
        inode = next;
        goto retry;
    }

    return ret;
}

void shim_scan_logs(shim_cli_t *shim_cli, shim_log_scanner scanner, void *priv) {
    inode_t *inode, *ibuf;
    uint64_t valp;
    unsigned seq;
    k_t key;
    int pos;

    ibuf = malloc(sizeof(*ibuf) + 2 * shim_cli->kc->max_len);
    bonsai_assert(ibuf);

    for (inode = shim_cli->shim->sentinel; inode; inode = ibuf->next) {
        /* snapshot the inode */
        do {
            seq = read_seqcount_begin(&inode->seq);
            memcpy(ibuf, inode, sizeof(*ibuf) + inode->lfence_len + inode->rfence_len);
        } while (unlikely(read_seqcount_retry(&inode->seq, seq)));

        /* scan logs */
        for_each_set_bit(pos, &ibuf->validmap, INODE_FANOUT) {
            logger_get(shim_cli->logger_cli, ibuf->logs[pos], &key, &valp);
            scanner(ibuf->logs[pos], ibuf->dgroup, priv);
        }
    }

    free(ibuf);
}

static void inode_gc(shim_cli_t *shim_cli, inode_t *inode) {
    int pos;

    /* clear stale logs */
    for_each_set_bit(pos, &inode->validmap, INODE_FANOUT) {
        if (logger_is_stale(shim_cli->logger_cli, inode->logs[pos])) {
            __clear_bit(pos, &inode->validmap);
        }
    }
}

/* merge inode into prev */
static void inode_merge(shim_cli_t *shim_cli, inode_t *prev, inode_t *inode) {
    uint64_t inode_bmp, prev_bmp;
    int pos, i;

    inode_bmp = inode->validmap;
    prev_bmp = prev->validmap;

    /* copy data to prev */
    for_each_set_bit(pos, &inode_bmp, INODE_FANOUT) {
        i = find_first_zero_bit(&prev_bmp, INODE_FANOUT);
        bonsai_assert(i < INODE_FANOUT);
        __set_bit(i, &prev_bmp);
        prev->logs[i] = inode->logs[pos];
        prev->fgprt[i] = inode->fgprt[pos];
    }

    /* update prev atomically */
    write_seqcount_begin(&prev->seq);
    prev->validmap = prev_bmp;
    prev->next = inode->next;
    prev->rfence_len = inode->rfence_len;
    memcpy(i_rfence(prev).key, i_rfence(inode).key, inode->rfence_len);
    write_seqcount_end(&prev->seq);

    /* remove inode from index */
    index_remove(shim_cli->index, i_lfence(inode));

    /* delay free inode */
    /* TODO: finish me */
}

void shim_gc(shim_cli_t *shim_cli) {
    inode_t *inode, *prev = NULL;

    inode = shim_cli->shim->sentinel;

    while (inode) {
        spin_lock(&inode->lock);

        /* gc current inode */
        inode_gc(shim_cli, inode);

        /* if can be merged with prev inode */
        if (prev &&
            hweight64(prev->validmap) + hweight64(inode->validmap) <= INODE_FANOUT &&
            dgroup_is_eq(prev->dgroup, inode->dgroup)) {
            inode_merge(shim_cli, prev, inode);
        }

        /* unlock prev inode, go to next node */
        if (prev) {
            spin_unlock(&prev->lock);
        }
        prev = inode;
        inode = inode->next;
    }

    if (prev) {
        spin_unlock(&prev->lock);
    }
}
