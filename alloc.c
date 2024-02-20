/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * General Memory Allcoator
 *
 * Hohai University
 */

#include <stdlib.h>

#include "atomic.h"
#include "alloc.h"
#include "utils.h"

/* TODO: current allocator is too simple */

struct allocator {
    size_t size;
    size_t used;
};

allocator_t *allocator_create(size_t size) {
    allocator_t *allocator;

    allocator = calloc(1, sizeof(*allocator));
    if (unlikely(!allocator)) {
        allocator = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for allocator");
        goto out;
    }

    allocator->size = size;
    allocator->used = 0;

    pr_debug(10, "init allocator, size=%.2fMB", (double) size / (1 << 20));

out:
    return allocator;
}

void allocator_destroy(allocator_t *allocator) {
    free(allocator);
}

size_t allocator_alloc(allocator_t *allocator, size_t size) {
    size_t off = xadd2(&allocator->used, size);
    return off + size <= allocator->size ? off : -ENOMEM;
}

void allocator_free(allocator_t *allocator, size_t off, size_t size) {
    /* FIXME: */
}
