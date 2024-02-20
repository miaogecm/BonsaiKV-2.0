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
#include "k.h"

typedef void *(*shim_indexer)(uint64_t sub_index, k_t key, void *priv);
typedef bool (*shim_log_validator)(void *log, void *priv);
typedef int shim_log_scanner(uint64_t log, uint64_t hnode, uint64_t cnode, void *priv);

typedef struct shim shim_t;
typedef struct shim_cli shim_cli_t;
typedef struct inode inode_t;

shim_t *shim_create(index_t *index);
void shim_set_log_validator(shim_t *shim, shim_log_validator validator, void *priv);
void shim_set_logi(shim_t *shim, shim_indexer logi, void *priv);
void shim_set_hnodei(shim_t *shim, shim_indexer hnodei, void *priv);
void shim_set_cnodei(shim_t *shim, shim_indexer cnodei, void *priv);
void shim_destroy(shim_t *shim);

shim_cli_t *shim_create_cli(shim_t *shim, perf_t *perf);
void shim_destroy_cli(shim_cli_t *shim_cli);

int shim_upsert(shim_cli_t *shim_cli, k_t key, void *log);
void *shim_lookup(shim_cli_t *shim_cli, k_t key);
void shim_scan(shim_cli_t *shim_cli, shim_log_scanner scanner, void *priv);

#endif //SHIM_H
