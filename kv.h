/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Top-layer key-value interface
 *
 * Hohai University
 */

#ifndef KV_H
#define KV_H

typedef struct kv kv_t;

uint8_t kv_key_fingerprint(const char *key, size_t len);

#endif //KV_H
