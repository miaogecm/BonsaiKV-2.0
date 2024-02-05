/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ Performance Counters
 *
 * Hohai University
 */

#ifndef PERF_H
#define PERF_H

#include <stdlib.h>

#include "utils.h"

typedef struct perf perf_t;

struct counter_desc {
    const char *name, *desc;
};

#ifndef PERF_SOURCE
#define PERF_COUNTERS_BEGIN         enum {
#define PERF_COUNTER(name, desc)    COUNTER_##name,
#define PERF_COUNTERS_END           NR_PERF_COUNTERS };
#else
#define PERF_COUNTERS_BEGIN         struct counter_desc counter_descs[] = {
#define PERF_COUNTER(name, desc)    { #name, desc },
#define PERF_COUNTERS_END           };
#define NR_PERF_COUNTERS            ARRAY_LEN(counter_descs)
#endif

PERF_COUNTERS_BEGIN
PERF_COUNTER(hello, "aaa")
PERF_COUNTERS_END

extern struct counter_desc counter_descs[];

struct perf {
    long vals[NR_PERF_COUNTERS];
};

static inline perf_t *perf_create() {
    perf_t *perf;
    perf = calloc(1, sizeof(*perf));
    if (unlikely(!perf)) {
        perf = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate perf memory");
    }
    return perf;
}

static inline void perf_destroy(perf_t *perf) {
    free(perf);
}

static inline void perf_acc(perf_t *dst, perf_t *src) {
    int i;
    for (i = 0; i < NR_PERF_COUNTERS; i++) {
        dst->vals[i] += src->vals[i];
    }
}

#define perf_reset_all(perf)            memset((perf), 0, sizeof(*(perf)))
#define perf_counter(perf, counter)     ((perf)->vals[COUNTER_##counter])
#define perf_reset(perf, counter)       (perf_counter(perf, counter) = 0)
#define perf_add(perf, counter, n)      (perf_counter(perf, counter) += (n))
#define perf_inc(perf, counter)         (perf_counter(perf, counter)++)

#endif //PERF_H
