/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Top-layer key-value interface
 *
 * Hohai University
 */

#ifndef KV_H
#define KV_H

#include "config.h"

typedef struct kv kv_t;
typedef struct kv_cli kv_cli_t;
typedef struct kv_rm kv_rm_t;

kv_t *kv_create(kv_conf_t *conf);
void kv_destroy(kv_t *kv);

kv_cli_t *kv_cli_create(kv_t *kv, kv_cli_conf_t *conf);
void kv_cli_destroy(kv_cli_t *kv_cli);

int kv_put(kv_cli_t *kv_cli, k_t key, uint64_t valp);
int kv_get(kv_cli_t *kv_cli, k_t key, uint64_t *valp);
int kv_del(kv_cli_t *kv_cli, k_t key);
int kv_scan(kv_cli_t *kv_cli, k_t key, int len);

cJSON *kv_dump(kv_cli_t *kv_cli);

kv_rm_t *kv_rm_create(kv_rm_conf_t *conf);
void kv_rm_destroy(kv_rm_t *kv_rm);

#endif //KV_H
