/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ volatile index layer
 *
 * Hohai University
 */

#ifndef ILAYER_H
#define ILAYER_H

#include <unistd.h>
#include <stdint.h>

typedef void index_t;

index_t *index_create();
void index_destroy(index_t *index);
int index_insert(index_t *index, const char *key, size_t key_len, void *val);
int index_remove(index_t *index, const char *key, size_t key_len);
void *index_find_first_ge(index_t *index, const char *key, size_t key_len);

#endif //ILAYER_H
