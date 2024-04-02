/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Remote memory server
 *
 * Hohai University
 */

#include "../config.h"
#include "../rpm.h"

static size_t rd_size = 8192;
static int nr_threads = 18;

static void *worker(void *arg) {
    struct bench_timer timer;
    int ret, i, rep = 10000;
    rpma_cli_t *rpma_cli;
    rpma_t *rpma = arg;
    long elapsed_ns;
    rpma_ptr_t ptr;
    void *buf;

    bench_timer_init_freq();

    rpma_cli = rpma_cli_create(rpma);

    rpma_alloc(rpma_cli, &ptr, rd_size);

    ptr.home = 0;
    ptr.off = 0;

    buf = rpma_buf_alloc(rpma_cli, rd_size);

    while (true) {
        bench_timer_start(&timer);

        for (i = 0; i < rep; i++) {
            ret = rpma_rd(rpma_cli, ptr, 0, buf, rd_size);
            if (ret < 0) {
                pr_err("rpma_rd failed: %d", ret);
                return NULL;
            }

            ret = rpma_commit_sync(rpma_cli);
            if (ret < 0) {
                pr_err("rpma_commit_sync failed: %d", ret);
                return NULL;
            }
        }

        elapsed_ns = bench_timer_end(&timer);
        pr_info("BWT: %ld MB/s", rd_size * rep * 1000 / elapsed_ns);
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
