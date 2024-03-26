/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Remote memory server
 *
 * Hohai University
 */

#include "../config.h"
#include "../rpm.h"

static int nr_threads = 1;
static size_t rd_size = 32;

static void *worker(void *arg) {
    rpma_cli_t *rpma_cli;
    rpma_t *rpma = arg;
    rpma_ptr_t ptr;
    void *buf;
    int ret;

    rpma_cli = rpma_cli_create(rpma);

    rpma_alloc(rpma_cli, &ptr, rd_size);

    buf = rpma_buf_alloc(rpma_cli, rd_size);

    ret = rpma_rd(rpma_cli, ptr, 0, buf, 8192);
    if (ret < 0) {
        pr_err("rpma_rd failed: %d", ret);
        return NULL;
    }

    ret = rpma_commit_sync(rpma_cli);
    if (ret < 0) {
        pr_err("rpma_commit_sync failed: %d", ret);
        return NULL;
    }

    rpma_buf_free(rpma_cli, buf, rd_size);

    return NULL;
}

int main() {
    pthread_t tid;
    rpma_t *rpma;
    int i;

    printf("net\n");

    rpma = rpma_create("192.168.1.3:8888", "192.168.1.1", 10);

    for (i = 0; i < nr_threads; i++) {
        pthread_create(&tid, NULL, worker, rpma);
    }

    while (true);

    return 0;
}
