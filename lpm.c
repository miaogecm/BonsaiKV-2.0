/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Local Persistent Memory Management
 *
 * LPMM provides LPMA (Local Persistent Memory Area) abstraction. A LPMA is a logically continuous memory
 * area starting from offset 0. However, it may be physically interleaved across multiple NVMM devices.
 *
 * Hohai University
 */

#include "atomic.h"
#include "alloc.h"
#include "utils.h"
#include "lpm.h"
#include "pm.h"

struct lpma {
    int nr_devs;
    struct pm_dev *devs;

    size_t strip_size, stripe_size;
    size_t size;

    allocator_t *allocator;

    int socket;
};

struct lpma_cli {
    lpma_t *lpma;
    perf_t *perf;

    /* cached fields to reduce one pointer chasing */
    int nr_devs;
    struct pm_dev *devs;

    size_t strip_size, stripe_size;
    size_t size;
};

lpma_t *lpma_create(int nr_devs, const char *dev_paths[], size_t strip_size) {
    lpma_t *lpma;
    int i;

    if (unlikely(nr_devs < 1)) {
        lpma = ERR_PTR(-EINVAL);
        pr_err("nr_devs must be greater than 0");
        goto out;
    }

    lpma = calloc(1, sizeof(*lpma));
    if (unlikely(lpma == NULL)) {
        lpma = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for lpma");
        goto out;
    }

    lpma->nr_devs = nr_devs;
    lpma->devs = pm_open_devs(nr_devs, dev_paths);
    if (unlikely(IS_ERR(lpma->devs))) {
        lpma = ERR_PTR(lpma->devs);
        pr_err("failed to open pm devices: %s", strerror(-PTR_ERR(lpma->devs)));
        goto out;
    }

    lpma->strip_size = nr_devs > 1 ? strip_size : 0;
    lpma->stripe_size = lpma->strip_size * nr_devs;
    lpma->size = 0;
    for (i = 0; i < nr_devs; i++) {
        lpma->size += lpma->devs[i].size;
    }

    lpma->allocator = allocator_create(lpma->size);
    if (unlikely(IS_ERR(lpma->allocator))) {
        lpma = ERR_PTR(lpma->allocator);
        pr_err("failed to create allocator: %s", strerror(-PTR_ERR(lpma->allocator)));
        goto out;
    }

    lpma->socket = lpma->devs[0].socket;
    for (i = 1; i < nr_devs; i++) {
        if (lpma->socket != lpma->devs[i].socket) {
            lpma->socket = -1;
            break;
        }
    }

    if (nr_devs > 1) {
        pr_debug(10, "create interleaved lpma on %d devices with strip size %lu", nr_devs, strip_size);
    } else {
        pr_debug(10, "create lpma @ %s", dev_paths[0]);
    }

out:
    return lpma;
}

void lpma_destroy(lpma_t *lpma) {
    pm_close_devs(lpma->nr_devs, lpma->devs);
    allocator_destroy(lpma->allocator);
    free(lpma);
}

lpma_t *lpma_cli_get_lpma(lpma_cli_t *lpma_cli) {
    return lpma_cli->lpma;
}

lpma_cli_t *lpma_cli_create(lpma_t *lpma, perf_t *perf) {
    lpma_cli_t *lpma_cli;

    lpma_cli = calloc(1, sizeof(*lpma_cli));
    if (unlikely(lpma_cli == NULL)) {
        lpma_cli = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for lpma_cli");
        goto out;
    }

    lpma_cli->lpma = lpma;
    lpma_cli->perf = perf;

    lpma_cli->stripe_size = lpma->stripe_size;
    lpma_cli->strip_size = lpma->strip_size;
    lpma_cli->size = lpma->size;
    lpma_cli->nr_devs = lpma->nr_devs;
    lpma_cli->devs = lpma->devs;

    pr_debug(20, "create lpma_cli on lpma @ %p", lpma);

out:
    return lpma_cli;
}

void lpma_cli_destroy(lpma_cli_t *lpma_cli) {
    free(lpma_cli);
}

static inline size_t va2pa(lpma_cli_t *lpma_cli, size_t *dev_off, size_t off) {
    size_t stripe_id, strip_id;

    stripe_id = off / lpma_cli->stripe_size;
    strip_id = (off % lpma_cli->stripe_size) / lpma_cli->strip_size;

    *dev_off = stripe_id * lpma_cli->strip_size + off % lpma_cli->strip_size;
    return strip_id;
}

void *lpma_get_ptr(lpma_cli_t *lpma_cli, size_t off) {
    size_t dev_off;
    int dev_id;

    if (lpma_cli->nr_devs == 1) {
        return lpma_cli->devs[0].start + off;
    }

    dev_id = va2pa(lpma_cli, &dev_off, off);

    return lpma_cli->devs[dev_id].start + dev_off;
}

static inline void do_wr(void *dst, const void *src, size_t size, bool cache) {
    if (cache) {
        memcpy(dst, src, size);
    } else {
        memcpy_nt(dst, src, size);
    }
}

static inline void do_lpma_wr_fastpath(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size, bool cache) {
    bonsai_assert(lpma_cli->nr_devs == 1);
    do_wr(lpma_cli->devs[0].start + dst, src, size, cache);
}

static inline void do_lpma_wr_slowpath(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size, bool cache) {
    size_t dev_off, strip_off, len;
    int dev_id;

    strip_off = dst % lpma_cli->strip_size;
    dev_id = va2pa(lpma_cli, &dev_off, dst);

    if (strip_off + size <= lpma_cli->strip_size) {
        do_wr(lpma_cli->devs[dev_id].start + dev_off, src, size, cache);
    } else {
        len = lpma_cli->strip_size - strip_off;
        do_wr(lpma_cli->devs[dev_id].start + dev_off, src, len, cache);
        do_lpma_wr_slowpath(lpma_cli, dst + len, src + len, size - len, cache);
    }
}

static inline void do_lpma_wr(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size, bool cache) {
    if (lpma_cli->nr_devs == 1) {
        do_lpma_wr_fastpath(lpma_cli, dst, src, size, cache);
        return;
    }
    do_lpma_wr_slowpath(lpma_cli, dst, src, size, cache);
}

void lpma_wr(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size) {
    return do_lpma_wr(lpma_cli, dst, src, size, true);
}

void lpma_wr_nc(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size) {
    return do_lpma_wr(lpma_cli, dst, src, size, false);
}

static inline void do_lpma_rd_fastpath(lpma_cli_t *lpma_cli, void *dst, size_t src, size_t size) {
    bonsai_assert(lpma_cli->nr_devs == 1);
    memcpy(dst, lpma_cli->devs[0].start + src, size);
}

static inline void do_lpma_rd_slowpath(lpma_cli_t *lpma_cli, void *dst, size_t src, size_t size) {
    size_t dev_off, strip_off, len;
    int dev_id;

    strip_off = src % lpma_cli->strip_size;
    dev_id = va2pa(lpma_cli, &dev_off, src);

    if (strip_off + size <= lpma_cli->strip_size) {
        memcpy(dst, lpma_cli->devs[dev_id].start + dev_off, size);
    } else {
        len = lpma_cli->strip_size - strip_off;
        memcpy(dst, lpma_cli->devs[dev_id].start + dev_off, len);
        do_lpma_rd_slowpath(lpma_cli, dst + len, src + len, size - len);
    }
}

static inline void do_lpma_prefetch_fastpath(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    bonsai_assert(lpma_cli->nr_devs == 1);
    prefetch_range(lpma_cli->devs[0].start + off, size);
}

static inline void do_lpma_prefetch_slowpath(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    size_t dev_off, strip_off, len;
    int dev_id;

    strip_off = off % lpma_cli->strip_size;
    dev_id = va2pa(lpma_cli, &dev_off, off);

    if (strip_off + size <= lpma_cli->strip_size) {
        prefetch_range(lpma_cli->devs[dev_id].start + dev_off, size);
    } else {
        len = lpma_cli->strip_size - strip_off;
        prefetch_range(lpma_cli->devs[dev_id].start + dev_off, len);
        do_lpma_prefetch_slowpath(lpma_cli, off + len, size - len);
    }
}

void lpma_rd(lpma_cli_t *lpma_cli, void *dst, size_t src, size_t size) {
    if (lpma_cli->nr_devs == 1) {
        do_lpma_rd_fastpath(lpma_cli, dst, src, size);
        return;
    }
    do_lpma_rd_slowpath(lpma_cli, dst, src, size);
}

void lpma_prefetch(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    if (lpma_cli->nr_devs == 1) {
        do_lpma_prefetch_fastpath(lpma_cli, off, size);
        return;
    }
    do_lpma_prefetch_slowpath(lpma_cli, off, size);
}

static inline void do_lpma_flush_fastpath(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    bonsai_assert(lpma_cli->nr_devs == 1);
    flush_range(lpma_cli->devs[0].start + off, size);
}

static inline void do_lpma_flush_slowpath(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    size_t dev_off, strip_off, len;
    int dev_id;

    strip_off = off % lpma_cli->strip_size;
    dev_id = va2pa(lpma_cli, &dev_off, off);

    if (strip_off + size <= lpma_cli->strip_size) {
        flush_range(lpma_cli->devs[dev_id].start + dev_off, size);
    } else {
        len = lpma_cli->strip_size - strip_off;
        flush_range(lpma_cli->devs[dev_id].start + dev_off, len);
        do_lpma_flush_slowpath(lpma_cli, off + len, size - len);
    }
}

void lpma_flush(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    if (lpma_cli->nr_devs == 1) {
        do_lpma_flush_fastpath(lpma_cli, off, size);
        return;
    }
    do_lpma_flush_slowpath(lpma_cli, off, size);
}

void lpma_persist() {
    memory_sfence();
}

size_t lpma_alloc(lpma_cli_t *lpma_cli, size_t size) {
    return allocator_alloc(lpma_cli->lpma->allocator, size);
}

void lpma_free(lpma_cli_t *lpma_cli, size_t off, size_t size) {
    return allocator_free(lpma_cli->lpma->allocator, off, size);
}

int lpma_socket(lpma_cli_t *lpma_cli) {
    return lpma_cli->lpma->socket;
}

size_t lpma_get_stripe_size(lpma_cli_t *lpma_cli) {
    return lpma_cli->stripe_size;
}

size_t lpma_get_strip_size(lpma_cli_t *lpma_cli) {
    return lpma_cli->strip_size;
}
