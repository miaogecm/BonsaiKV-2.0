/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Scalable & Hardware-accelerated Remote Persistent Memory Management
 *
 * RPMM provides RPMA (Remote Persistent Memory Area) abstraction. RPMA abstracts the whole remote memory
 * as a logically continuous memory area starting from offset 0. However, it may be physically interleaved
 * across multiple NVMM devices in a single memory node. Also, it can be physically divided into multiple
 * domains.
 *
 * RPMA Logical View
 *     0x00│   ┌──────────────┐
 *         │   │aaaaaaaaaaaaaa│
 *         │   │bbbbbbbbbbbbbb│
 *         │   │cccccccccccccc│
 *         │   │dddddddddddddd│
 *    size │   │              │
 *         ▼   └──────────────┘
 *
 * RPMA Physical View
 *     0x00│   ┌──────────────┐        ┌──────────────┐
 *         │   │aaaaaaaaaaaaaa│        │              │
 *         │   │dev0          │        │dev1          │     │
 *         │   ├──────────────┤        ├──────────────┤     │
 *         │   │bbbbbbbbbbbbbb│        │              │     │
 *         │   │dev2          │        │dev3          │     │    (2)
 *         │   ├──────────────┤  (1)   ├──────────────┤     │ interleaved
 *         │   │cccccccccccccc│replica │cccccccccccccc│     │
 *         │   │dev4          │        │dev5          │     │
 *         │   ├──────────────┤        ├──────────────┤     │
 *         │   │              │        │dddddddddddddd│     │
 *    size │   │dev6          │        │dev7          │
 *         ▼   └──────────────┘        └──────────────┘
 *                  dom0                    dom1
 *
 * Basic concepts:
 *   RPMA: Remote Persistent Memory Area
 *   Strip/Stripe
 *   (Home/Replica) Segment
 *   Directory: The per-dom directory maintains the caching state of each segment.
 *
 * An RPMA can be viewed as:
 *   (1) A logically continuous memory area starting from offset 0.
 *   (2) Interleaving of multiple STRIPEs, while each STRIPE consists of multiple STRIPS. Each
 *       STRIP belongs to one device.
 *   (3) Concatnation of multiple SEGMENTs. Each SEGMENT can be replicated across domains.
 *
 *                ┌──────────────┐        ┌──────────────┐ ────────────────────┐
 * segment ┌──────►aaaaaaaaaaaaaa│        │aaaaaaaaaaaaaa│                     │
 * segment │┼┼┼┼┼┤►bbbbbbbbbbbbbb│        │bbbbbbbbbbbbbb│                     │
 * segment └──────►cccccccccccccc│        │cccccccccccccc│                     │
 *                ┌──────────────┤        ├──────────────┤ ──┐                 │
 *                │dddddddddddddd│        │dddddddddddddd│   │ strip           │ stripe
 *                │eeeeeeeeeeeeee│        │              │   │                 │ (interleave
 *                │              │        │ffffffffffffff│   │                 │        set)
 *                ├──────────────┤        ├──────────────┤ ──┘                 │
 *  home   ──────►│CCCCCCCCCCCCCC│        │cccccccccccccc│                     │
 *  segment       │              │        │              │                     │
 *                │              │        │              │                     │
 *                ├──────────────┤        ├──────────────┤                     │
 *  replica ─────►│dddddddddddddd│        │DDDDDDDDDDDDDD│◄─── home segment    │
 *  segment       │              │        │              │                     │
 *                │              │        │              │ ────────────────────┘
 *                └──────────────┘        └──────────────┘
 *
 * Hohai University
 */

#ifndef RPM_H
#define RPM_H

#include <unistd.h>
#include <stdint.h>

#include "utils.h"

/* Remote */

/*
 * Topology:
 *
 * ┌───────────┐    ┌───────────┐
 * │           │    │           │
 * │ dom0 PMs  │    │ dom1 PMs  │
 * │           │    │           │
 * │           │    │           │
 * │           │    │           │
 * │           │    │           │
 * │           │    │           │
 * └──▲────▲───┘    └────▲───▲──┘
 *    │    │             │   │
 *    │    └┬──────────┬─┘   │
 *    │     │          │     │
 *    │     │          │     │
 * ┌──┴─────┴──┐    ┌──┴─────┴──┐
 * │ dom0 NIC  │    │ dom1 NIC  │
 * │           │    │           │
 * └───▲─────▲─┘    └───▲─────▲─┘
 *     │     │          │     │
 *     │     │          │     │
 *  ┌──┴─┐ ┌─┴─┐      ┌─┴─┐ ┌─┴─┐
 *  │    │ │   │      │   │ │   │
 *  │    │ │   │      │   │ │   │
 *  └────┘ └───┘      └───┘ └───┘
 *   cli0   cli1       cli2  cli3
 */

struct rpma_dom_conf {
    const char *host;
    const char **dev_paths;
};

struct rpma_conf {
    int nr_doms, nr_dev_per_dom;

    size_t strip_size, segment_size;
    int *permutes, nr_permutes;

    struct rpma_dom_conf *dom_confs;
};

typedef struct rpma_dom_conf rpma_dom_conf_t;
typedef struct rpma_conf rpma_conf_t;
typedef struct rpma_svr rpma_svr_t;

rpma_svr_t *rpma_svr_create(rpma_conf_t *rpma_conf);
void rpma_svr_destroy(rpma_svr_t *svr);

/* Local */

typedef struct rpma rpma_t;
typedef struct rpma_cli rpma_cli_t;
typedef struct rpma_buf rpma_buf_t;
typedef struct rpma_ptr rpma_ptr_t;
typedef unsigned long rpma_flag_t;

struct rpma_ptr {
    union {
        struct {
            uint16_t home;
            uint64_t off : 48;
        };
        uint64_t rawp;
    };
};

struct rpma_buf {
    void *start;
    size_t size;
};

rpma_t *rpma_create(const char *host, const char *dev_ip, int interval_usZ);
void rpma_destroy(rpma_t *rpma);

rpma_cli_t *rpma_cli_create(rpma_t *rpma);
void rpma_cli_destroy(rpma_cli_t *cli);

int rpma_add_mr(rpma_cli_t *cli, void *start, size_t size);
void *rpma_buf_alloc(rpma_cli_t *cli, size_t size);
void rpma_buf_free(rpma_cli_t *cli, void *buf, size_t size);

int rpma_wr_(rpma_cli_t *cli, rpma_ptr_t dst, rpma_buf_t src[], rpma_flag_t flag);
int rpma_rd_(rpma_cli_t *cli, rpma_buf_t dst[], rpma_ptr_t src, rpma_flag_t flag);
int rpma_flush(rpma_cli_t *cli, rpma_ptr_t dst, size_t size, rpma_flag_t flag);

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

int rpma_alloc_dom(rpma_cli_t *cli, rpma_ptr_t *ptr, size_t size, int dom);
int rpma_alloc(rpma_cli_t *cli, rpma_ptr_t *ptr, size_t size);
void rpma_free(rpma_cli_t *cli, rpma_ptr_t ptr, size_t size);

size_t rpma_get_strip_size(rpma_cli_t *cli);
size_t rpma_get_stripe_size(rpma_cli_t *cli);

#define RPMA_NULL                   ((rpma_ptr_t) { .rawp = UINT64_MAX })
#define RPMA_PTR_OFF(ptr, offset)   ((rpma_ptr_t) { .home = (ptr).home, .off = (ptr).off + (offset) })

#endif // RPM_H
