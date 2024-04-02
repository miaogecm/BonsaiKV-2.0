/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Remote memory server
 *
 * Hohai University
 */

#include "../config.h"
#include "../rpm.h"
#include "../kv.h"

static rpma_conf_t rpma_conf = {
    .nr_doms = 2,
    .nr_dev_per_dom = 1,
    .strip_size = 256,
    .nr_permutes = 1,
    .permutes = (int[]) { 0, 1, 2 },
    .segment_size = 32,
    .dom_confs = (rpma_dom_conf_t[]) {
        { "192.168.1.3:8888", (const char *[]) { "pm0", "pm1", "pm2" } },
        { "192.168.1.4:8888", (const char *[]) { "pm3", "pm4", "pm5" } },
    }
};

static kv_rm_conf_t kv_rm_conf = {
    .rpma_conf = &rpma_conf
};

bool terminated;

static void sig_handler(int signo) {
    terminated = true;
    pr_info("received signal %s, terminating ms...", strsignal(signo));
}

int main() {
    kv_rm_t *kv_rm;

    signal(SIGINT, sig_handler);

    kv_rm = kv_rm_create(&kv_rm_conf);

    while (!READ_ONCE(terminated)) {
        /* do nothing */
    }

    pr_debug(5, "start kv rm termination");

    // TODO: better
    exit(0);

    kv_rm_destroy(kv_rm);

    return 0;
}
