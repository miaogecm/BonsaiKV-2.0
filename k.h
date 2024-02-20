/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Key
 *
 * Hohai University
 */

#ifndef K_H
#define K_H

typedef struct kc kc_t;
typedef struct k k_t;

struct k {
    const char *key;
    uint16_t len;
};

struct kc {
    int (*cmp)(k_t k, k_t o);
    int (*dump)(k_t k, char *buf, int buflen);
    uint64_t (*hash)(k_t k);
    k_t min, max;
};

static inline uint64_t k_hash(kc_t *kc, k_t k) {
    return kc->hash(k);
}

static inline uint8_t k_fgprt(kc_t *kc, k_t k) {
    return k_hash(kc, k) & 0xff;
}

static int k_cmp(kc_t *kc, k_t k, k_t o) {
    return kc->cmp(k, o);
}

static int k_dump(kc_t *kc, k_t k, char *buf, int buflen) {
    return kc->dump(k, buf, buflen);
}

static char *k_str(kc_t *kc, k_t k) {
    static __thread char buf[UINT16_MAX];
    return !k_dump(kc, k, buf, sizeof(buf)) ? buf : NULL;
}

#endif //K_H
