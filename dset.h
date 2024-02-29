/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ scalable data layer
 *
 * Hohai University
 */

#ifndef DNODE_H
#define DNODE_H

#include <unistd.h>
#include <stdint.h>

#include "perf.h"
#include "rpm.h"
#include "lpm.h"
#include "k.h"

typedef struct dhint {
    uint64_t hints[2];
} dhint_t;
typedef struct dcli dcli_t;
typedef struct dset dset_t;
typedef void (*dset_chg_hint_cb_t)(void *priv, dhint_t new_hint, k_t s, k_t t);

dset_t *dset_create(kc_t *kc, size_t hnode_size, size_t cnode_size, lpma_t *lpma, rpma_t *rpma, size_t pstage_sz);
void dset_destroy(dset_t *dset);

dcli_t *dcli_create(dset_t *dset, dset_chg_hint_cb_t chg_hint_cb, void *priv, perf_t *perf);
void dcli_destroy(dcli_t *dcli);

int dset_upsert(dcli_t *dcli, k_t key, uint64_t valp, dhint_t hint);
int dset_delete(dcli_t *dcli, k_t key, dhint_t hint);
int dset_lookup(dcli_t *dcli, k_t key, uint64_t *valp, dhint_t hint);

int dset_create_valp(dcli_t *dcli, uint64_t *valp, k_t key, const void *val, size_t len);
int dset_get_val(dcli_t *dcli, void *buf, int bufsz, uint64_t valp);

#endif //DNODE_H
