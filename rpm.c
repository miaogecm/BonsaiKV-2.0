/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Remote Persistent Memory
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include <rdma/rdma_cma.h>
#include <pthread.h>

#include "list.h"
#include "rpm.h"

#define NR_MRS_MAX    32

typedef enum {
    MR_BASE,
    MR_INTERLEAVED
} mr_type_t;

struct mr_desc {
    mr_type_t type;
};

struct mr_desc_base {
    struct mr_desc b;
    void *start;
    size_t size;
};

struct mr_desc_interleaved {
    struct mr_desc b;
    int nr_mrs;
    int sub_mr_ids[NR_MRS_MAX];
    size_t strip_size;
};

/* <mr_id, pd> -> ibv_mr */
struct base_mr_cache_item {
    int mr_id;
    struct ibv_pd *pd;

    struct ibv_mr *mr;

    struct list_head node;
};

struct rpm_mn {
    struct mr_desc *mr_descs[NR_MRS_MAX];

    /* We create base MR for each PD, rather than each connection */
    struct list_head base_mr_cache;

    int port;

    pthread_t thread;
    bool exit;
};

static inline int handle_event_connect_request(struct rpm_mn *mn, struct rdma_cm_id *cli_id) {
    int ret;

    pr_debug(10, "handle event connect request");


}

static inline int handle_event_established(struct rpm_mn *mn, struct rdma_cm_id *cli_id) {
    int ret;

    pr_debug(10, "handle event established");


}

static void *mn_daemon(void *arg) {
    struct rdma_event_channel *cm_chan;
    struct rdma_cm_id *svr_id, *cli_id;
    struct rdma_cm_event *event;
    struct rpm_mn *mn = arg;
    struct sockaddr_in sin;
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
    sin.sin_port = htons(mn->port);
    sin.sin_addr.s_addr = INADDR_ANY;

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

    pr_debug(5, "MN-side RDMA server created, listening on *:%d", mn->port);

    while (!READ_ONCE(mn->exit)) {
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
                ret = handle_event_connect_request(mn, cli_id);
                break;

            case RDMA_CM_EVENT_ESTABLISHED:
                ret = handle_event_established(mn, cli_id);
                break;

            default:
                ret = -EINVAL;
                pr_err("unexpected RDMA CM event: %d", event->event);
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

rpm_mn_t *rpm_create_mn(int port) {
    rpm_mn_t *mn;

    mn = calloc(1, sizeof(*mn));
    if (unlikely(!mn)) {
        mn = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate memory for rpm_mn_t struct");
        goto out;
    }

    mn->port = port;

out:
    return mn;
}
