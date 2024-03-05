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

#ifndef DNODE_H
#define DNODE_H

#include <unistd.h>
#include <stdint.h>

#include "perf.h"
#include "rpm.h"
#include "lpm.h"
#include "k.h"

#ifndef DSET_SOURCE
typedef struct dgroup {
    /* dnodes across different storage tiers */
    uint64_t dnodes[2];
} dgroup_t;
#endif
typedef struct dcli dcli_t;
typedef struct dset dset_t;
typedef int (*dgroup_map_update_fn)(void *priv, k_t s, k_t t, dgroup_t group);
typedef int (*dgroup_map_lookup_fn)(void *priv, dgroup_t *group, k_t k);

dset_t *dset_create(kc_t *kc, size_t hnode_size, size_t cnode_size,
                    lpma_t **dom_lpmas, rpma_t **dom_rpmas, rpma_t *rep_rpma,
                    size_t pstage_sz, int max_gc_prefetch);
void dset_destroy(dset_t *dset);

dcli_t *dcli_create(dset_t *dset, dgroup_map_update_fn gm_updator, dgroup_map_lookup_fn gm_lookuper,
                    void *priv, perf_t *perf, int dom_affinity);
void dcli_destroy(dcli_t *dcli);

int dset_upsert(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t valp);
int dset_delete(dcli_t *dcli, dgroup_t dgroup, k_t key);
int dset_lookup(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t *valp);

int dset_create_valp(dcli_t *dcli, uint64_t *valp, k_t key, const void *val, size_t len);
int dset_get_val(dcli_t *dcli, void *buf, int bufsz, uint64_t valp);

size_t dset_get_pm_utilization(dcli_t *dcli);

int dset_gc(dcli_t *dcli, size_t *gc_size);

#endif //DNODE_H
