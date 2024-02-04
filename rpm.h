/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Remote Persistent Memory
 *
 * Hohai University
 */

#ifndef RPM_H
#define RPM_H

#include <unistd.h>
#include <stdint.h>

#include "utils.h"

/*
 * Pool = MN1 + MN2 + ... + MNk
 *      = (MR11 + MR12 + ...) + (MR21 + MR22 + ...) + ... + (MRk1 + MRk2 + ...)
 * MR = BaseMR(start, size) | InterleavedMR(MR1, MR2, ..., MRn, strip_size)
 */

typedef struct rpm_pool rpm_pool_t;
typedef unsigned long rpm_flag_t;
typedef struct rpm_mn rpm_mn_t;
typedef uint64_t rpm_ptr_t;

/* Remote memory node */

rpm_mn_t *rpm_create_mn(int port);
int rpm_create_base_mr(rpm_mn_t *mn, int mr_id, void *start, size_t size);
int rpm_create_interleaved_mr(rpm_mn_t *mn, int mr_id, int nr_mrs, int sub_mr_ids[], size_t strip_size);
int rpm_start_mn(rpm_mn_t *mn);
void rpm_destroy_mn(rpm_mn_t *mn);

/* Local server */

rpm_pool_t *rpm_connect_pool(const char **hosts);
void rpm_destroy_pool(rpm_pool_t *pool);

void *rpm_alloc(rpm_pool_t *pool, size_t size);

int rpm_wr(rpm_pool_t *pool, rpm_ptr_t dst, const void *buf, size_t size, rpm_flag_t flag);
int rpm_rd(rpm_pool_t *pool, void *buf, rpm_ptr_t src, size_t size, rpm_flag_t flag);
int rpm_flush(rpm_pool_t *pool, rpm_ptr_t addr, size_t size, rpm_flag_t flag);

int rpm_commit(rpm_pool_t *pool);
int rpm_sync(rpm_pool_t *pool);

static inline int rpm_commit_sync(rpm_pool_t *pool) {
    int ret = rpm_commit(pool);
    if (ret) {
        return ret;
    }
    return rpm_sync(pool);
}

#endif //RPM_H
