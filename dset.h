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

#include <cjson/cJSON.h>
#include <unistd.h>
#include <stdint.h>

#include "rpm.h"
#include "k.h"

#define DSET_INFO_SZ    4096

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

struct shim_cli;

dset_t *dset_create(kc_t *kc,
                    size_t bnode_size, size_t dnode_size,
                    const char *bdev, rpma_t *rpma,
                    int max_gc_prefetch);
void dset_destroy(dset_t *dset);

dcli_t *dcli_create(dset_t *dset, struct shim_cli *shim_cli);
void dcli_destroy(dcli_t *dcli);

int dset_upsert(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t valp);
int dset_delete(dcli_t *dcli, dgroup_t dgroup, k_t key);
int dset_lookup(dcli_t *dcli, dgroup_t dgroup, k_t key, uint64_t *valp);

size_t dset_get_pm_utilization(dcli_t *dcli);

int dset_gc(dcli_t *dcli, size_t *gc_size);

cJSON *dset_dump(dcli_t *dcli);

#ifndef DSET_SOURCE
static inline bool dgroup_is_eq(dgroup_t a, dgroup_t b) {
    return a.nodes[0] == b.nodes[0] && a.nodes[1] == b.nodes[1];
}

static inline void dgroup_copy(dgroup_t *dst, dgroup_t src) {
    dst->nodes[0] = src.nodes[0];
    dst->nodes[1] = src.nodes[1];
}
#endif

#endif //DSET_H
