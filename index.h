/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ volatile index layer
 *
 * Hohai University
 */

#ifndef INDEX_H
#define INDEX_H

#include <unistd.h>
#include <stdint.h>

#include "k.h"

typedef void index_t;

index_t *index_create();
void index_destroy(index_t *index);
int index_upsert(index_t *index, k_t key, void *val);
int index_remove(index_t *index, k_t key);
void *index_find_first_ge(index_t *index, k_t key);

#endif //INDEX_H
