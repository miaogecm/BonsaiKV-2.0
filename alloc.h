/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * General Memory Allcoator
 *
 * Hohai University
 */

#ifndef ALLOC_H
#define ALLOC_H

typedef struct allocator allocator_t;

allocator_t *allocator_create(size_t size);
void allocator_destroy(allocator_t *allocator);

size_t allocator_alloc(allocator_t *allocator, size_t size);
void allocator_free(allocator_t *allocator, size_t off, size_t size);

#endif //ALLOC_H
