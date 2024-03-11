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
#include "oplog.h"
#include "kv.h"

#define INODE_FANOUT    46

struct shim {
    index_t *index;
    kc_t *kc;
};

struct shim_cli {
    shim_t *shim;

    /* cache frequently-accessed fields in @shim (to reduce pointer chasing) */
    index_t *index;
    kc_t *kc;

    perf_t *perf;

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
    uint16_t rfence_len;

    spinlock_t lock;
    seqcount_t seq;

    uint64_t logs[INODE_FANOUT];

    char rfence[];
};

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

out:
    return shim;
}

shim_cli_t *shim_create_cli(shim_t *shim, perf_t *perf, logger_cli_t *logger_cli) {
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

    shim_cli->perf = perf;

    shim_cli->logger_cli = logger_cli;

out:
    return shim_cli;
}

void shim_destroy_cli(shim_cli_t *shim_cli) {
    free(shim_cli);
}

static inline k_t i_rfence(inode_t *inode) {
    return (k_t) { inode->rfence, inode->rfence_len };
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

    return 0;
}

static inline void i_gc(shim_cli_t *shim_cli, inode_t *inode) {
}

static inline void i_split(shim_cli_t *shim_cli, inode_t *inode) {

}

/* TODO: SIMD-optimize */
static inline void prefetch_log(shim_cli_t *shim_cli, inode_t *inode, k_t key) {
    uint8_t fgprt = k_fgprt(shim_cli->kc, key);
    int i;

    for (i = 0; i < INODE_FANOUT; i++) {
        if (inode->fgprt[i] == fgprt) {
            logger_prefetch(shim_cli->logger_cli, inode->logs[i]);
        }
    }
}

/* TODO: SIMD-optimize */
static inline int search_log(shim_cli_t *shim_cli, inode_t *inode, k_t key, uint64_t *valp, int *pos) {
    uint8_t fgprt = k_fgprt(shim_cli->kc, key);
    int i, ret = -ERANGE;
    k_t log_key;
    op_t op;

    *pos = INODE_FANOUT;

    for (i = 0; i < INODE_FANOUT; i++) {
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
    return -ENOENT;
}

int shim_upsert(shim_cli_t *shim_cli, k_t key, oplog_t log) {
    unsigned long validmap;
    inode_t *inode;
    uint64_t valp;
    int pos, ret;

    inode = iget_locked(shim_cli, key);

    validmap = inode->validmap;

    ret = search_log(shim_cli, inode, key, &valp, &pos);
    if (unlikely(!IS_ERR(ret))) {
        /* Key exists, update */
        ret = -EEXIST;
    } else if (ret == -ENOENT || ret == -ERANGE) {
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
    size_t rfence_len;
    unsigned int seq;
    int pos, ret;
    k_t rfence;

    inode = iget_unlocked(shim_cli, key);

retry:
    seq = read_seqcount_begin(&inode->seq);

    rfence_len = inode->rfence_len;
    memcpy(rfence_buf, inode->rfence, rfence_len);
    next = inode->next;

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    rfence = (k_t) { rfence_buf, rfence_len };

    if (unlikely(k_cmp(shim_cli->kc, key, rfence) >= 0)) {
        inode = next;
        goto retry;
    }

    /* prefetch for pipelining */
    prefetch_log(shim_cli, inode, key);
    dset_prefetch(shim_cli->dcli, inode->dgroup);

    ret = search_log(shim_cli, inode, key, valp, &pos);

    if (unlikely(read_seqcount_retry(&inode->seq, seq))) {
        goto retry;
    }

    /* tiered lookup */
    if (ret == -ERANGE) {
        ret = search_dset(shim_cli, inode, key, valp);
    }

    return ret;
}
