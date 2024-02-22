/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ scalable data layer
 *
 * Hohai University
 */

#include "dset.h"

struct dentry {
    uint64_t val;
    char key[];
};

struct mnode {
    size_t next_off, prev_off;
    uint8_t fgprt[];
};

struct dnode {
    struct dentry entries[];
};

struct dset {
    kc_t *kc;

    size_t sentinel_off;
};

struct dcli {
    dset_t *dset;
};

dset_t *dset_create() {

}

void dset_destroy(dset_t *dset) {

}

dcli_t *dcli_create(dset_t *dset, dset_chg_hint_cb_t chg_hint_cb, void *priv, perf_t *perf, rpma_cli_t *rpma_cli) {

}

void dcli_destroy(dcli_t *dcli) {

}

int dset_upsert(dcli_t *dcli, k_t key, void *val, dhint_t hint) {

}

int dset_delete(dcli_t *dcli, k_t key, dhint_t hint) {

}

void *dset_lookup(dcli_t *dcli, k_t key, dhint_t hint) {

}
