/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ shim layer
 *
 * Hohai University
 */

#ifndef SHIM_H
#define SHIM_H

#include "index.h"
#include "perf.h"

typedef void *(*shim_indexer)(void *sub_index, const char *key, size_t key_len, void *priv);

typedef struct shim shim_t;
typedef struct shim_cli shim_cli_t;
typedef struct inode inode_t;

shim_t *shim_create(index_t *index);
void shim_set_logi(shim_t *shim, shim_indexer logi, void *logi_ctx);
void shim_set_hnodei(shim_t *shim, shim_indexer hnodei, void *hnodei_ctx);
void shim_set_cnodei(shim_t *shim, shim_indexer cnodei, void *cnodei_ctx);
void shim_destroy(shim_t *shim);

shim_cli_t *shim_create_cli(shim_t *shim, perf_t *perf);
void shim_destroy_cli(shim_cli_t *shim_cli);

int shim_upsert(shim_cli_t *shim_cli, const char *key, size_t key_len, void *log);
void *shim_lookup(shim_cli_t *shim_cli, const char *key, size_t key_len);

#endif //SHIM_H
