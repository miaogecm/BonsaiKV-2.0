/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Utility functions
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include <fcntl.h>

#include "utils.h"

int debug_level = 20;

int tsc_khz = 0;

static void bt_err_cb(void *data, const char *msg, int errnum) {
    pr_warn("err %s (%d)", msg, errnum);
}

static int bt_full_cb(void *data, uintptr_t pc, const char *filename, int lineno, const char *function) {
    pr_info(PT_UNDERLINE "%s" PT_RESET " [%s(%d)]", function, filename, lineno);
    return 0;
}

static void err_sig_handler(int sig) {
    pr_err("[!!!] program received signal %s", strsignal(sig));
    exit(1);
}

static void toggle_dbg_sig_handler(int sig) {
    debug_level = debug_level == 0 ? 20 : 0;
    pr_info("toggle debug mode, change to %d", debug_level);
}

const char *get_hostname() {
    static char hostname[512] = { 0 };
    static bool set = false;
    if (unlikely(!set)) {
        gethostname(hostname, sizeof(hostname));
        set = true;
    }
    return hostname;
}

const char *get_threadname() {
    static __thread char threadname[16] = { 0 };
    static __thread bool set = false;
    if (unlikely(!set)) {
        pthread_getname_np(pthread_self(), threadname, sizeof(threadname));
        set = true;
    }
    return threadname;
}

void reg_basic_sig_handler() {
    signal(SIGSEGV, err_sig_handler);
    signal(SIGTRAP, err_sig_handler);
    signal(SIGABRT, err_sig_handler);
    signal(SIGILL, err_sig_handler);
    signal(SIGFPE, err_sig_handler);
    signal(SIGBUS, err_sig_handler);
    signal(SIGUSR2, toggle_dbg_sig_handler);
}

void dump_stack() {
    struct backtrace_state *state = backtrace_create_state(NULL, BACKTRACE_SUPPORTS_THREADS, bt_err_cb, NULL);
    pr_info(PT_BOLD "========== dump stack ==========" PT_RESET);
    backtrace_full(state, 0, bt_full_cb, bt_err_cb, NULL);
}

unsigned int get_rand_seed() {
    unsigned int seed;
    int fd, err;

    fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) {
        pr_err("open /dev/urandom failed");
        exit(EXIT_FAILURE);
    }

    err = read(fd, &seed, sizeof(seed));
    if (err < 0) {
        pr_err("read /dev/urandom failed");
        exit(EXIT_FAILURE);
    }

    close(fd);

    return seed;
}

static void get_tsc_khz() {
    FILE *fp;
    fp = popen("gdb /dev/null /proc/kcore -ex 'x/uw 0x'$(grep '\\<tsc_khz\\>' /proc/kallsyms | cut -d' ' -f1) -batch 2>/dev/null | tail -n 1 | cut -f2", "r");
    fscanf(fp, "%d", &tsc_khz);
    pclose(fp);
}

void bench_timer_init_freq() {
    get_tsc_khz();
    pr_info("bench timer: TSC_KHZ=%d", tsc_khz);
}
