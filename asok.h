/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ Admin Socket
 *
 * Hohai University
 */

#ifndef ASOK_H
#define ASOK_H

#include "kv.h"

#include <cjson/cJSON.h>

typedef struct asok asok_t;

typedef int (*asok_cmd_handler_fn_t)(asok_t *asok, cJSON *out, const cJSON *cmd);

asok_t *asok_create(kv_t *kv, const char *sock_path);
int asok_register_handler(asok_t *asok, const char *prefix, asok_cmd_handler_fn_t fn);
void asok_destroy(asok_t *asok);

#endif //ASOK_H
