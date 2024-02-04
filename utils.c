/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Utility functions
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include "utils.h"

int debug_level;

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
