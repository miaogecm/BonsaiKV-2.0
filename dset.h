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

#ifndef DSET_H
#define DSET_H

#include <unistd.h>
#include <stdint.h>

#include "perf.h"
#include "rpm.h"
#include "k.h"

#ifndef DSET_SOURCE
typedef struct dgroup {
    /* corresponding nodes across different storage tiers */
    uint64_t nodes[2];
} dgroup_t;
#else
typedef struct dgroup dgroup_t;
#endif
typedef struct dcli dcli_t;
typedef struct dset dset_t;
typedef int (*dgroup_map_update_fn)(void *priv, k_t s, k_t t, dgroup_t group);
typedef int (*dgroup_map_lookup_fn)(void *priv, dgroup_t *group, k_t k);

dset_t *dset_create(kc_t *kc,
                    size_t bnode_size, size_t cnode_size,
                    const char *bdev, rpma_t *rpma,
                    size_t pstage_sz, int max_gc_prefetch);
void dset_destroy(dset_t *dset);

dcli_t *dcli_create(dset_t *dset, dgroup_map_update_fn gm_updator, dgroup_map_lookup_fn gm_lookuper,
                    void *priv, perf_t *perf);
void dcli_destroy(dcli_t *dcli);

int dset_upsert(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t valp);
int dset_delete(dcli_t *dcli, dgroup_t dgroup, k_t key);
int dset_lookup(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t *valp);
void dset_prefetch(dcli_t *dcli, dgroup_t dgroup);

size_t dset_get_pm_utilization(dcli_t *dcli);

int dset_gc(dcli_t *dcli, size_t *gc_size);

#ifndef DSET_SOURCE
static inline bool dgroup_is_eq(dgroup_t a, dgroup_t b) {
    return a.nodes[0] == b.nodes[0] && a.nodes[1] == b.nodes[1];
}
#endif

#endif //DSET_H
