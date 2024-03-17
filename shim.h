/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ shim layer
 *
 * Hohai University
 */

#ifndef SHIM_H
#define SHIM_H

#include "oplog.h"
#include "index.h"
#include "perf.h"
#include "dset.h"
#include "k.h"

typedef int shim_log_scanner(uint64_t log, dgroup_t dgroup, void *priv);

typedef struct shim shim_t;
typedef struct shim_cli shim_cli_t;
typedef struct inode inode_t;

shim_t *shim_create(index_t *index, kc_t *kc);
void shim_destroy(shim_t *shim);

shim_cli_t *shim_create_cli(shim_t *shim, perf_t *perf, logger_cli_t *logger_cli);
void shim_set_dcli(shim_cli_t *shim_cli, dcli_t *dcli);
void shim_destroy_cli(shim_cli_t *shim_cli);

int shim_upsert(shim_cli_t *shim_cli, k_t key, oplog_t log);
int shim_lookup(shim_cli_t *shim_cli, k_t key, uint64_t *valp);
void shim_scan(shim_cli_t *shim_cli, shim_log_scanner scanner, void *priv);

/* new dgroup must be different from the previous one */
int shim_update_dgroup(shim_cli_t *shim_cli, k_t s, k_t t, dgroup_t dgroup);
int shim_lookup_dgroup(shim_cli_t *shim_cli, k_t key, dgroup_t *dgroup);

#endif //SHIM_H
