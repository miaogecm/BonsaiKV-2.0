/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Top-layer key-value interface
 *
 * Hohai University
 */

#include <pthread.h>

#include "utils.h"
#include "list.h"
#include "kv.h"

struct kv {
    pthread_mutex_t mgmt_thr_list_lock;
    struct list_head mgmt_thr_list;
};

uint8_t kv_key_fingerprint(const char *key, size_t len) {
    return 0;
}
