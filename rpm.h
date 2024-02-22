/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Local Persistent Memory Management
 *
 * RPMM provides RPMA (Remote Persistent Memory Area) abstraction. An RPMA is a logically continuous memory
 * area starting from offset 0. However, it may be physically interleaved across multiple NVMM devices in
 * a single memory node.
 *
 * Hohai University
 */

#ifndef RPM_H
#define RPM_H

#include <unistd.h>
#include <stdint.h>

#include "utils.h"
#include "perf.h"

/* Remote */

typedef struct rpma_svr rpma_svr_t;

rpma_svr_t *rpma_svr_create(const char *host, int nr_devs, const char *dev_paths[], size_t strip_size);
void rpma_svr_destroy(rpma_svr_t *svr);

/* Local */

typedef struct rpma rpma_t;
typedef struct rpma_cli rpma_cli_t;
typedef struct rpma_buf rpma_buf_t;
typedef unsigned long rpma_flag_t;

struct rpma_buf {
    void *start;
    size_t size;
};

rpma_t *rpma_create(const char *host, const char *dev_ip);

rpma_cli_t *rpma_cli_create(rpma_t *rpma, perf_t *perf);
void rpma_cli_destroy(rpma_cli_t *cli);

int rpma_add_mr(rpma_cli_t *cli, void *start, size_t size);
void *rpma_buf_alloc(rpma_cli_t *cli, size_t size);

int rpma_wr_(rpma_cli_t *cli, size_t dst, rpma_buf_t src[], rpma_flag_t flag);
int rpma_rd_(rpma_cli_t *cli, rpma_buf_t dst[], size_t src, rpma_flag_t flag);
int rpma_flush(rpma_cli_t *cli, size_t off, size_t size, rpma_flag_t flag);

int rpma_commit(rpma_cli_t *cli);
int rpma_sync(rpma_cli_t *cli);

static inline int rpma_commit_sync(rpma_cli_t *cli) {
    int ret = rpma_commit(cli);
    if (ret) {
        return ret;
    }
    return rpma_sync(cli);
}

#define rpma_buflist(cli, ...)          ((rpma_buf_t[]) { __VA_ARGS__, { NULL, 0 } })
#define rpma_wr(cli, dst, flag, ...)    rpma_wr_((cli), (dst), rpma_buflist((cli), __VA_ARGS__), (flag))
#define rpma_rd(cli, src, flag, ...)    rpma_rd_((cli), rpma_buflist((cli), __VA_ARGS__), (src), (flag))

size_t rpma_alloc(rpma_cli_t *cli, size_t size);
void rpma_free(rpma_cli_t *cli, size_t off, size_t size);

size_t rpma_get_strip_size(rpma_cli_t *cli);
size_t rpma_get_stripe_size(rpma_cli_t *cli);

#endif // RPM_H
