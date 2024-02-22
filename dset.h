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
#include "k.h"

typedef struct dhint {
    uint64_t hints[2];
} dhint_t;
typedef struct dcli dcli_t;
typedef struct dset dset_t;
typedef void (*dset_chg_hint_cb_t)(void *priv, dhint_t new_hint, k_t s, k_t t);

dset_t *dset_create(kc_t *kc);
void dset_destroy(dset_t *dset);

dcli_t *dcli_create(dset_t *dset, dset_chg_hint_cb_t chg_hint_cb, void *priv, perf_t *perf, rpma_cli_t *rpma_cli);
void dcli_destroy(dcli_t *dcli);

int dset_upsert(dcli_t *dcli, k_t key, void *val, dhint_t hint);
int dset_delete(dcli_t *dcli, k_t key, dhint_t hint);
void *dset_lookup(dcli_t *dcli, k_t key, dhint_t hint);

#endif //DNODE_H
