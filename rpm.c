/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Scalable & Hardware-accelerated Remote Persistent Memory Management
 *
 * RPMM provides RPMA (Remote Persistent Memory Area) abstraction. An RPMA is a logically continuous memory
 * area starting from offset 0. However, it may be physically interleaved across multiple NVMM devices in
 * a single memory node. Also, it can be physically divided into multiple domains.
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

#define _GNU_SOURCE

#include <infiniband/mlx5dv.h>
#include <rdma/rdma_cma.h>
#include <arpa/inet.h>
#include <sys/mman.h>
#include <pthread.h>

#include "atomic.h"
#include "alloc.h"
#include "list.h"
#include "perf.h"
#include "rpm.h"
#include "pm.h"

#define NR_MRS_MAX    32

#define MAX_QP_SR   128
#define MAX_QP_RR   128

#define MAX_SEND_SGE    32
#define MAX_RECV_SGE    32

#define MAX_INLINE_DATA    64

#define RETRY_CNT   14

#define MAX_CLI_NR_MRS  8

#define OP_BUF_SIZE     (1 * 1024 * 1024ul)
#define CLI_BUF_SIZE    (1 * 1024 * 1024ul)

struct rpma_svr {
    int nr_devs;
    struct pm_dev *devs;

    size_t strip_size, stripe_size;
    size_t size;

    in_addr_t ip;
    in_port_t port;

    pthread_t thread;
    pid_t tid;
    bool exit;

    struct ibv_mr **mrs;
};

struct wr_list {
    struct ibv_send_wr *head, *tail;
};

struct rpma {
    const char *dev_ip, *host;

    allocator_t *allocator;
    bool allocator_created;
};

struct rpma_cli {
    rpma_t *rpma;
    perf_t *perf;

    struct ibv_pd *pd;

    struct ibv_mr *mrs[MAX_CLI_NR_MRS];
    size_t op_buf_used;
    int nr_mrs;

    struct ibv_qp *qp;
    struct ibv_cq *cq;
    uint32_t rkey;

    size_t size;
    size_t strip_size, stripe_size;

    struct wr_list wr_list;
    int nr_cqe;

    allocator_t *cli_buf_allocator;
};

struct pdata {
    size_t size, strip_size, stripe_size;
    uint32_t rkey;
};

static inline struct ibv_qp *create_qp(rpma_svr_t *svr, struct rdma_cm_id *cli_id) {
    struct ibv_context *context = cli_id->verbs;
    struct mlx5dv_qp_init_attr mlx5_qp_attr;
    struct ibv_qp_init_attr_ex init_attr_ex;
    struct ibv_qp *qp;

    /* configure QP init attrs */
    memset(&mlx5_qp_attr, 0, sizeof(mlx5_qp_attr));
    memset(&init_attr_ex, 0, sizeof(init_attr_ex));

    /* enable QP support for interleaved MR */
    if (svr->nr_devs > 1) {
        mlx5_qp_attr.comp_mask |= MLX5DV_QP_INIT_ATTR_MASK_SEND_OPS_FLAGS;
        mlx5_qp_attr.send_ops_flags |= MLX5DV_QP_EX_WITH_MR_INTERLEAVED;
    }

    /* configure common QP init attrs */
    init_attr_ex.send_cq = cli_id->send_cq;
    init_attr_ex.recv_cq = cli_id->recv_cq;
    init_attr_ex.cap.max_send_wr = MAX_QP_SR;
    init_attr_ex.cap.max_recv_wr = MAX_QP_RR;
    init_attr_ex.cap.max_send_sge = MAX_SEND_SGE;
    init_attr_ex.cap.max_recv_sge = MAX_RECV_SGE;
    init_attr_ex.cap.max_inline_data = MAX_INLINE_DATA;
    init_attr_ex.qp_type = IBV_QPT_RC;
    init_attr_ex.pd = cli_id->pd;
    init_attr_ex.send_ops_flags |= IBV_QP_EX_WITH_RDMA_WRITE | IBV_QP_EX_WITH_RDMA_READ | IBV_QP_EX_WITH_SEND;
    init_attr_ex.send_ops_flags |= IBV_QP_EX_WITH_RDMA_WRITE_WITH_IMM | IBV_QP_EX_WITH_SEND_WITH_IMM;
    init_attr_ex.comp_mask = IBV_QP_INIT_ATTR_PD | IBV_QP_INIT_ATTR_SEND_OPS_FLAGS;

    /* create QP */
    qp = mlx5dv_create_qp(context, &init_attr_ex, &mlx5_qp_attr);
    if (unlikely(!qp)) {
        qp = ERR_PTR(-errno);
    }

    return qp;
}

static inline int create_mrs(rpma_svr_t *svr, struct rdma_cm_id *cli_id) {
    int i, flags, ret = 0;

    svr->mrs = calloc(svr->nr_devs, sizeof(*svr->mrs));
    if (unlikely(!svr->mrs)) {
        ret = -ENOMEM;
        pr_err("failed to allocate memory for MRs: %s", strerror(-ret));
        goto out;
    }

    flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    for (i = 0; i < svr->nr_devs; i++) {
        svr->mrs[i] = ibv_reg_mr(cli_id->pd, svr->devs[i].start, svr->devs[i].size, flags);
        if (unlikely(!svr->mrs[i])) {
            free(svr->mrs);
            ret = -errno;
            pr_err("failed to register MR: %s", strerror(-ret));
            goto out;
        }
    }

out:
    return ret;
}

static inline int get_rkey(rpma_svr_t *svr, struct rdma_cm_id *cli_id, uint32_t *rkey) {
    struct mlx5dv_mkey_init_attr mkey_init_attr;
    struct mlx5dv_mr_interleaved *strips;
    struct mlx5dv_mkey *dv_mkey;
    struct mlx5dv_qp_ex *dv_qp;
    struct ibv_qp_ex *qpx;
    size_t repeat_count;
    int i, ret = 0;
    int flags;

    /* get qp */
    bonsai_assert(cli_id->qp);
    qpx = ibv_qp_to_qp_ex(cli_id->qp);
    dv_qp = mlx5dv_qp_ex_from_ibv_qp_ex(qpx);

    /* if no PM MRs, create them */
    if (unlikely(!svr->mrs)) {
        ret = create_mrs(svr, cli_id);
        if (unlikely(ret)) {
            goto out;
        }
    }

    /* if only one device, no interleaving (indirect mkey) required */
    if (svr->nr_devs == 1) {
        *rkey = svr->mrs[0]->rkey;
        goto out;
    }

    /* configure mkey init attr */
    mkey_init_attr.create_flags = MLX5DV_MKEY_INIT_ATTR_FLAGS_INDIRECT;
    mkey_init_attr.max_entries = svr->nr_devs;
    mkey_init_attr.pd = cli_id->pd;

    /* create mkey */
    dv_mkey = mlx5dv_create_mkey(&mkey_init_attr);

    /* init strips info */
    strips = calloc(svr->nr_devs, sizeof(*strips));
    if (unlikely(!strips)) {
        ret = -ENOMEM;
        pr_err("failed to allocate memory for MR strips: %s", strerror(-ret));
        goto out;
    }
    for (i = 0; i < svr->nr_devs; i++) {
        strips[i].addr = (uintptr_t) svr->mrs[i]->addr;
        strips[i].bytes_count = svr->strip_size;
        strips[i].bytes_skip = svr->strip_size;
        strips[i].lkey = svr->mrs[i]->lkey;
    }
    repeat_count = svr->size / svr->stripe_size;
    flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;

    /* configure interleaved MR */
    ibv_wr_start(qpx);
    mlx5dv_wr_mr_interleaved(dv_qp, dv_mkey, flags, repeat_count, svr->nr_devs, strips);
    ret = ibv_wr_complete(qpx);
    if (unlikely(ret)) {
        ret = -errno;
        pr_err("failed to complete interleaved MR: %s", strerror(-ret));
        goto out;
    }

    *rkey = dv_mkey->rkey;

out:
    return ret;
}

static inline int handle_event_connect_request(rpma_svr_t *svr, struct rdma_cm_id *cli_id) {
    struct rdma_conn_param conn_param = { };
    struct pdata pdata;
    struct ibv_qp *qp;
    uint32_t rkey;
    int ret;

    pr_debug(10, "start handle event connect request");

    /* create qp */
    qp = create_qp(svr, cli_id);
    if (unlikely(IS_ERR(qp))) {
        ret = PTR_ERR(qp);
        goto out;
    }
    cli_id->qp = qp;

    /* get mkey */
    ret = get_rkey(svr, cli_id, &rkey);
    if (unlikely(ret)) {
        goto out;
    }

    /* prepare exchange information */
    pdata.rkey = rkey;
    pdata.size = svr->size;
    pdata.strip_size = svr->strip_size;
    pdata.stripe_size = svr->stripe_size;
    conn_param.private_data = &pdata;
    conn_param.private_data_len = sizeof(pdata);

    /* accept connection */
    ret = rdma_accept(cli_id, &conn_param);
    if (unlikely(ret)) {
        pr_err("failed to accept RDMA connection: %s", strerror(errno));
        goto out;
    }

    pr_debug(10, "connection accepted, rkey=%u, qp=%u", rkey, qp->qp_num);

out:
    return ret;
}

static inline int handle_event_established(rpma_svr_t *svr, struct rdma_cm_id *cli_id) {
    pr_debug(10, "handle event established");
}

static inline int parse_ip_port(const char *s, in_addr_t *ip_addr, in_port_t *port) {
    char *p;
    int ret;

    p = strchr(s, ':');
    if (unlikely(!p)) {
        ret = -EINVAL;
        pr_err("invalid IP:PORT format: %s", s);
        goto out;
    }

    *p = '\0';
    *ip_addr = inet_addr(s);
    if (unlikely(*ip_addr == INADDR_NONE)) {
        ret = -EINVAL;
        pr_err("invalid IP address: %s", s);
        goto out;
    }

    *port = atoi(p + 1);
    if (unlikely(*port <= 0 || *port > 65535)) {
        ret = -EINVAL;
        pr_err("invalid port number: %s", p + 1);
        goto out;
    }

    ret = 0;

out:
    return ret;
}

static void *cm_entry(void *arg) {
    struct rdma_event_channel *cm_chan;
    struct rdma_cm_id *svr_id, *cli_id;
    struct rdma_cm_event *event;
    struct sockaddr_in sin;
    rpma_svr_t *svr = arg;
    int ret;

    cm_chan = rdma_create_event_channel();
    if (unlikely(!cm_chan)) {
        pr_err("failed to create RDMA event channel: %s", strerror(errno));
        ret = -errno;
        goto out;
    }

    ret = rdma_create_id(cm_chan, &svr_id, NULL, RDMA_PS_TCP);
    if (unlikely(ret)) {
        ret = -errno;
        pr_err("failed to create RDMA listen ID: %s", strerror(errno));
        goto out_destroy_channel;
    }

    sin.sin_family = AF_INET;
    sin.sin_port = htons(svr->port);
    sin.sin_addr.s_addr = svr->ip;

    ret = rdma_bind_addr(svr_id, (struct sockaddr *) &sin);
    if (unlikely(ret)) {
        ret = -errno;
        pr_err("failed to bind RDMA address: %s", strerror(errno));
        goto out_destroy_id;
    }

    ret = rdma_listen(svr_id, 32);
    if (unlikely(ret)) {
        ret = -errno;
        pr_err("failed to listen RDMA server: %s", strerror(errno));
        goto out_destroy_id;
    }

    pr_debug(5, "RPMA CM created, listening on %s:%d", inet_ntoa(sin.sin_addr), ntohs(sin.sin_port));

    while (!READ_ONCE(svr->exit)) {
        ret = rdma_get_cm_event(cm_chan, &event);
        if (ret) {
            ret = -errno;
            pr_err("failed to get RDMA CM event: %s", strerror(errno));
            goto out_destroy_id;
        }
        ret = rdma_ack_cm_event(event);
        if (ret) {
            ret = -errno;
            pr_err("failed to acknowledge RDMA CM event: %s", strerror(errno));
            goto out_destroy_id;
        }

        cli_id = event->id;

        switch (event->event) {
            case RDMA_CM_EVENT_CONNECT_REQUEST:
                ret = handle_event_connect_request(svr, cli_id);
                break;

            case RDMA_CM_EVENT_ESTABLISHED:
                ret = handle_event_established(svr, cli_id);
                break;

            default:
                ret = -EINVAL;
                pr_err("unexpected RPMA CM event: %d", event->event);
        }

        if (unlikely(ret)) {
            pr_err("failed to accept RDMA connection: %s", strerror(-ret));
            goto out_destroy_id;
        }
    }

out_destroy_id:
    rdma_destroy_id(svr_id);

out_destroy_channel:
    rdma_destroy_event_channel(cm_chan);

out:
    return ret;
}

rpma_svr_t *rpma_svr_create(const char *host, int nr_devs, const char *dev_paths[], size_t strip_size) {
    rpma_svr_t *svr;
    int i, ret;

    svr = calloc(1, sizeof(*svr));
    if (unlikely(!svr)) {
        pr_err("failed to allocate memory for rpma_svr_t");
        svr = ERR_PTR(-ENOMEM);
        goto out;
    }

    /* open PM devices */
    svr->nr_devs = nr_devs;
    svr->devs = pm_open_devs(nr_devs, dev_paths);
    if (unlikely(IS_ERR(svr->devs))) {
        pr_err("failed to open PM devices: %s", strerror(-PTR_ERR(svr->devs)));
        svr = ERR_PTR(PTR_ERR(svr->devs));
        goto out;
    }

    /* calculate striping info */
    svr->strip_size = nr_devs > 1 ? strip_size : 0;
    svr->stripe_size = svr->strip_size * nr_devs;
    svr->size = 0;
    for (i = 0; i < nr_devs; i++) {
        svr->size += svr->devs[i].size;
    }

    /* get host info */
    ret = parse_ip_port(host, &svr->ip, &svr->port);
    if (unlikely(ret)) {
        free(svr);
        svr = ERR_PTR(ret);
        pr_err("failed to parse IP:PORT: %s", host);
        goto out;
    }

    /* create server connection management (cm) thread */
    ret = pthread_create(&svr->thread, NULL, cm_entry, svr);
    if (unlikely(ret)) {
        free(svr);
        svr = ERR_PTR(-ret);
        pr_err("failed to create rpma svr cm thread: %s", strerror(ret));
        goto out;
    }

    pthread_setname_np(svr->thread, "bonsai-rpmas");

    while (!READ_ONCE(svr->tid)) {
        cpu_relax();
    }

    if (nr_devs > 1) {
        pr_debug(5, "create interleaved rpma svr on %d devices with strip size %lu", nr_devs, strip_size);
    } else {
        pr_debug(5, "create rpma svr @ %s", dev_paths[0]);
    }

out:
    return svr;
}

void rpma_svr_destroy(rpma_svr_t *svr) {
    pr_debug(5, "destroy rpma svr");

    svr->exit = true;
    pthread_join(svr->thread, NULL);

    free(svr);
}

static inline void *huge_page_alloc(size_t size) {
    void *addr = mmap(NULL, size,
                      PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
    return addr != MAP_FAILED ? addr : NULL;
}

static inline int create_cli_buf(rpma_cli_t *cli, size_t size) {
    void *buf;
    int ret;

    buf = huge_page_alloc(size);
    if (unlikely(!buf)) {
        ret = -errno;
        pr_err("failed to allocate memory for client buffer: %s", strerror(-ret));
        goto out;
    }

    ret = rpma_add_mr(cli, buf, size);

out:
    return ret;
}

static inline int create_op_buf(rpma_cli_t *cli, size_t size) {
    void *buf;
    int ret;

    buf = huge_page_alloc(size);
    if (unlikely(!buf)) {
        ret = -errno;
        pr_err("failed to allocate memory for operand buffer: %s", strerror(-ret));
        goto out;
    }

    ret = rpma_add_mr(cli, buf, size);

out:
    return ret;
}

rpma_t *rpma_create(const char *host, const char *dev_ip) {
    rpma_t *rpma;

    rpma = calloc(1, sizeof(*rpma));
    if (unlikely(!rpma)) {
        pr_err("failed to allocate memory for rpma_t");
        rpma = ERR_PTR(-ENOMEM);
        goto out;
    }

    rpma->host = host;
    rpma->dev_ip = dev_ip;

out:
    return rpma;
}

void rpma_destroy(rpma_t *rpma) {
    free(rpma);
}

rpma_cli_t *rpma_cli_create(rpma_t *rpma, perf_t *perf) {
    struct rdma_conn_param conn_param = { };
    struct rdma_event_channel *cm_chan;
    struct ibv_qp_init_attr init_attr;
    struct rdma_cm_event *event;
    struct sockaddr_in sin;
    struct rdma_cm_id *id;
    struct pdata pdata;
    rpma_cli_t *cli;
    int ret;

    cli = calloc(1, sizeof(*cli));
    if (unlikely(!cli)) {
        pr_err("failed to allocate memory for rpma_cli_t");
        cli = ERR_PTR(-ENOMEM);
        goto out;
    }

    cli->rpma = rpma;
    cli->perf = perf;

    cm_chan = rdma_create_event_channel();
    if (unlikely(!cm_chan)) {
        pr_err("failed to create RDMA event channel: %s", strerror(errno));
        cli = ERR_PTR(-errno);
        goto out;
    }

    ret = rdma_create_id(cm_chan, &id, NULL, RDMA_PS_TCP);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to create RDMA listen ID: %s", strerror(errno));
    }

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port = htons(0);
    inet_pton(AF_INET, rpma->dev_ip, &sin.sin_addr);

    ret = rdma_bind_addr(id, (struct sockaddr *) &sin);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to bind RDMA address: %s", strerror(errno));
        goto out_destroy_id;
    }

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    ret = parse_ip_port(rpma->host, &sin.sin_addr.s_addr, &sin.sin_port);
    if (unlikely(ret)) {
        cli = ERR_PTR(ret);
        pr_err("failed to parse IP:PORT: %s", rpma->host);
        goto out_destroy_id;
    }

    ret = rdma_resolve_addr(id, NULL, (struct sockaddr *) &sin, 2000);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to resolve RDMA address: %s", strerror(errno));
        goto out_destroy_id;;
    }
    ret = rdma_get_cm_event(cm_chan, &event);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to get RDMA CM event: %s", strerror(errno));
        goto out_destroy_id;
    }
    if (unlikely(event->event != RDMA_CM_EVENT_ADDR_RESOLVED)) {
        cli = ERR_PTR(-EINVAL);
        pr_err("unexpected RDMA CM event: %d", event->event);
        goto out_destroy_id;;
    }
    rdma_ack_cm_event(event);

    ret = rdma_resolve_route(id, 2000);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to resolve RDMA route: %s", strerror(errno));
        goto out_destroy_id;
    }
    ret = rdma_get_cm_event(cm_chan, &event);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to get RDMA CM event: %s", strerror(errno));
        goto out_destroy_id;
    }
    if (unlikely(event->event != RDMA_CM_EVENT_ROUTE_RESOLVED)) {
        cli = ERR_PTR(-EINVAL);
        pr_err("unexpected RDMA CM event: %d", event->event);
        goto out_destroy_id;;
    }
    rdma_ack_cm_event(event);

    memset(&init_attr, 0, sizeof(init_attr));
    init_attr.cap.max_send_wr = MAX_QP_SR;
    init_attr.cap.max_recv_wr = MAX_QP_RR;
    init_attr.cap.max_send_sge = MAX_SEND_SGE;
    init_attr.cap.max_recv_sge = MAX_RECV_SGE;
    init_attr.cap.max_inline_data = MAX_INLINE_DATA;
    init_attr.send_cq = id->send_cq;
    init_attr.recv_cq = id->recv_cq;
    init_attr.qp_type = IBV_QPT_RC;
    ret = rdma_create_qp(id, id->pd, &init_attr);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to create RDMA QP: %s", strerror(errno));
        goto out_destroy_id;
    }

    conn_param.initiator_depth = 1;
    conn_param.retry_count = RETRY_CNT;
    ret = rdma_connect(id, &conn_param);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to connect RDMA server: %s", strerror(errno));
        goto out_destroy_id;
    }

    ret = rdma_get_cm_event(cm_chan, &event);
    if (unlikely(ret)) {
        cli = ERR_PTR(-errno);
        pr_err("failed to get RDMA CM event: %s", strerror(errno));
        goto out_destroy_id;
    }
    if (unlikely(event->event != RDMA_CM_EVENT_ESTABLISHED)) {
        cli = ERR_PTR(-EINVAL);
        pr_err("unexpected RDMA CM event: %d", event->event);
        goto out_destroy_id;
    }
    memcpy(&pdata, event->param.conn.private_data, sizeof(pdata));
    rdma_ack_cm_event(event);

    cli->pd = id->pd;
    cli->qp = id->qp;
    cli->cq = id->send_cq;
    cli->rkey = pdata.rkey;
    cli->size = pdata.size;
    cli->strip_size = pdata.strip_size;
    cli->stripe_size = pdata.stripe_size;

    ret = create_op_buf(cli, OP_BUF_SIZE);
    if (unlikely(ret)) {
        cli = ERR_PTR(ret);
        pr_err("failed to create operand buffer: %s", strerror(-ret));
        goto out_destroy_id;
    }

    ret = create_cli_buf(cli, CLI_BUF_SIZE);
    if (unlikely(ret)) {
        cli = ERR_PTR(ret);
        pr_err("failed to create client buffer: %s", strerror(-ret));
        goto out_destroy_id;
    }

    cli->cli_buf_allocator = allocator_create(CLI_BUF_SIZE);
    if (unlikely(IS_ERR(cli->cli_buf_allocator))) {
        cli = ERR_PTR(cli->cli_buf_allocator);
        pr_err("failed to create client buffer allocator: %s", strerror(-PTR_ERR(cli->cli_buf_allocator)));
        goto out_destroy_id;
    }

    if (cmpxchg2(&rpma->allocator_created, false, true)) {
        rpma->allocator = allocator_create(cli->size);
        if (unlikely(IS_ERR(rpma->allocator))) {
            cli = ERR_PTR(rpma->allocator);
            pr_err("failed to create RPMA allocator: %s", strerror(-PTR_ERR(rpma->allocator)));
            goto out_destroy_id;
        }
    }

    pr_debug(10, "rpma [%s] -> %s size=%lu,qpn=%d,rkey=%u)",
             rpma->dev_ip, rpma->host, cli->size, cli->qp->qp_num, cli->rkey);

out_destroy_id:
    rdma_destroy_id(id);

out_destroy_channel:
    rdma_destroy_event_channel(cm_chan);

out:
    return cli;
}

void rpma_cli_destroy(rpma_cli_t *cli) {
    /* TODO: support disconnect */
    free(cli);
}

int rpma_add_mr(rpma_cli_t *cli, void *start, size_t size) {
    int flags, ret = 0;
    struct ibv_mr *mr;

    flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    mr = ibv_reg_mr(cli->pd, start, size, flags);
    if (unlikely(!mr)) {
        ret = -errno;
        pr_err("failed to register MR: %s", strerror(-ret));
        goto out;
    }

    cli->mrs[cli->nr_mrs++] = mr;

out:
    return ret;
}

static inline struct ibv_mr *get_mr(rpma_cli_t *cli, void *addr, size_t size) {
    int i;
    for (i = 0; i < cli->nr_mrs; i++) {
        if (addr >= cli->mrs[i]->addr && addr + size <= cli->mrs[i]->addr + cli->mrs[i]->length) {
            return cli->mrs[i];
        }
    }
    return NULL;
}

void *rpma_buf_alloc(rpma_cli_t *cli, size_t size) {
    size_t off;

    off = allocator_alloc(cli->cli_buf_allocator, size);
    if (IS_ERR(off)) {
        pr_err("failed to allocate memory for client buffer: %s", strerror(-PTR_ERR(off)));
        return ERR_PTR(off);
    }

    return cli->mrs[1]->addr + off;
}

void rpma_buf_free(rpma_cli_t *cli, void *buf, size_t size) {
    size_t off;

    bonsai_assert(buf >= cli->mrs[1]->addr && buf < cli->mrs[1]->addr + cli->mrs[1]->length);

    off = buf - cli->mrs[1]->addr;
    allocator_free(cli->cli_buf_allocator, off, size);
}

static inline void *push_operand(rpma_cli_t *cli, void *start, size_t len) {
    void *p;

    if (unlikely(cli->op_buf_used + len > OP_BUF_SIZE)) {
        return ERR_PTR(-ENOMEM);
    }

    p = cli->mrs[0]->addr + cli->op_buf_used;
    cli->op_buf_used += len;

    if (start) {
        memcpy(p, start, len);
    }

    return p;
}

static inline void *get_operand(rpma_cli_t *cli, uint32_t *lkey, void *start, size_t len) {
    struct ibv_mr *mr;
    void *operand;

    mr = get_mr(cli, start, len);
    if (mr) {
        operand = start;
        goto out;
    }

    /* TODO: copy for read ops */
    operand = push_operand(cli, start, len);
    if (unlikely(IS_ERR(operand))) {
        goto out_err;
    }
    mr = cli->mrs[0];

out:
    *lkey = mr->lkey;

out_err:
    return operand;
}

static inline struct ibv_sge *get_sg_list(rpma_cli_t *cli, rpma_buf_t *buf, int *nr) {
    struct ibv_sge *sglist;
    int i, cnt = 0;
    void *addr;

    while (buf[cnt].start) {
        bonsai_assert(buf[cnt].size);
        cnt++;
    }
    bonsai_assert(!buf[cnt].size);
    *nr = cnt;

    sglist = calloc(cnt, sizeof(*sglist));
    if (unlikely(!sglist)) {
        goto out;
    }

    for (i = 0; i < cnt; i++) {
        addr = get_operand(cli, &sglist[i].lkey, buf[i].start, buf[i].size);
        sglist[i].addr = (uintptr_t) addr;
        sglist[i].length = buf[i].size;
        if (unlikely(IS_ERR(addr))) {
            free(sglist);
            sglist = ERR_PTR(PTR_ERR(addr));
            goto out;
        }
    }

out:
    return sglist;
}

static inline void insert_into_wr_list(rpma_cli_t *cli, struct ibv_send_wr *wr) {
    struct wr_list *wr_list = &cli->wr_list;
    if (!wr_list->head) {
        wr_list->head = wr;
    } else {
        wr_list->tail->next = wr;
    }
    bonsai_assert(!wr->next);
    wr_list->tail = wr;
}

int rpma_wr_(rpma_cli_t *cli, size_t dst, rpma_buf_t src[], rpma_flag_t flag) {
    struct ibv_send_wr *wr;
    int ret = 0, num_sge;
    struct ibv_sge *sgl;

    if (unlikely(dst >= cli->size)) {
        ret = -EINVAL;
        pr_err("invalid destination offset: %lu", dst);
        goto out;
    }

    sgl = get_sg_list(cli, src, &num_sge);
    if (unlikely(IS_ERR(sgl))) {
        ret = PTR_ERR(sgl);
        pr_err("failed to get sg list: %s", strerror(-ret));
        goto out;
    }

    wr = calloc(1, sizeof(*wr));
    if (unlikely(!wr)) {
        free(sgl);
        ret = -ENOMEM;
        pr_err("failed to allocate memory for send WR: %s", strerror(-ret));
        goto out;
    }

    wr->opcode = IBV_WR_RDMA_WRITE;
    wr->wr_id = 0;
    wr->sg_list = sgl;
    wr->num_sge = num_sge;
    wr->wr.rdma.remote_addr = dst;
    wr->wr.rdma.rkey = cli->rkey;

    insert_into_wr_list(cli, wr);

out:
    return ret;
}

int rpma_rd_(rpma_cli_t *cli, rpma_buf_t dst[], size_t src, rpma_flag_t flag) {
    struct ibv_send_wr *wr;
    int ret = 0, num_sge;
    struct ibv_sge *sgl;

    if (unlikely(src >= cli->size)) {
        ret = -EINVAL;
        pr_err("invalid source offset: %lu", src);
        goto out;
    }

    sgl = get_sg_list(cli, dst, &num_sge);
    if (unlikely(IS_ERR(sgl))) {
        ret = PTR_ERR(sgl);
        pr_err("failed to get sg list: %s", strerror(-ret));
        goto out;
    }

    wr = calloc(1, sizeof(*wr));
    if (unlikely(!wr)) {
        free(sgl);
        ret = -ENOMEM;
        pr_err("failed to allocate memory for send WR: %s", strerror(-ret));
        goto out;
    }

    wr->opcode = IBV_WR_RDMA_READ;
    wr->wr_id = 0;
    wr->sg_list = sgl;
    wr->num_sge = num_sge;
    wr->wr.rdma.remote_addr = src;
    wr->wr.rdma.rkey = cli->rkey;

    insert_into_wr_list(cli, wr);

out:
    return ret;
}

int rpma_flush(rpma_cli_t *cli, size_t off, size_t size, rpma_flag_t flag) {
    void *buf = push_operand(cli, NULL, 1);
    if (unlikely(IS_ERR(buf))) {
        return PTR_ERR(buf);
    }
    return rpma_rd(cli, off, flag, buf, 1);
}

int rpma_commit(rpma_cli_t *cli) {
    struct wr_list *wr_list = &cli->wr_list;
    struct ibv_send_wr *wr, *bad, *tmp;
    int ret = 0;

    /* only mark the last wr as signaled to reduce ACK msgs */
    wr_list->tail->send_flags |= IBV_SEND_SIGNALED;
    cli->nr_cqe++;

    /* use doorbell batching to reduce DMA doorbells */
    if (unlikely(ibv_post_send(cli->qp, wr_list->head, &bad))) {
        pr_err("failed to post wr");
        ret = -EINVAL;
    }

    for (wr = wr_list->head; wr; wr = tmp) {
        tmp = wr->next;
        free(wr->sg_list);
        free(wr);
    }

    wr_list->head = wr_list->tail = NULL;

    return ret;
}

int rpma_sync(rpma_cli_t *cli) {
    int total = 0, curr, ret = 0;
    struct ibv_wc wc = { };

    do {
        curr = ibv_poll_cq(cli->cq, 1, &wc);
        if (unlikely(curr < 0)) {
            pr_err("failed to poll CQ");
            ret = -EINVAL;
            goto out;
        }
        if (unlikely(wc.status != IBV_WC_SUCCESS)) {
            pr_err("work request failed: %s (wr_id: %lu)", ibv_wc_status_str(wc.status), wc.wr_id);
            ret = -EINVAL;
            goto out;
        }

        total += curr;
    } while (total < cli->nr_cqe);

    cli->nr_cqe = 0;
    cli->op_buf_used = 0;

out:
    return ret;
}

size_t rpma_alloc(rpma_cli_t *cli, size_t size) {
    return allocator_alloc(cli->rpma->allocator, size);
}

void rpma_free(rpma_cli_t *cli, size_t off, size_t size) {
    return allocator_free(cli->rpma->allocator, off, size);
}

size_t rpma_get_strip_size(rpma_cli_t *cli) {
    return cli->strip_size;
}

size_t rpma_get_stripe_size(rpma_cli_t *cli) {
    return cli->stripe_size;
}
