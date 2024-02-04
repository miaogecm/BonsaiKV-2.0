/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Utility functions
 *
 * Hohai University
 */

#ifndef BONSAIKV_UTILS_H
#define BONSAIKV_UTILS_H

#include <backtrace.h>
#include <backtrace-supported.h>
#include <signal.h>

#include <syscall.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <pthread.h>

extern int debug_level;

#define COLOR_BLACK         "\033[0;30m"
#define COLOR_RED           "\033[0;31m"
#define COLOR_GREEN         "\033[0;32m"
#define COLOR_YELLOW        "\033[0;33m"
#define COLOR_BLUE          "\033[0;34m"
#define COLOR_MAGENTA       "\033[0;35m"
#define COLOR_CYAN          "\033[0;36m"
#define COLOR_WHITE         "\033[0;37m"
#define COLOR_GRAY          "\033[0;90m"
#define COLOR_DEFAULT       "\033[0;39m"

#define COLOR_BOLD_BLACK    "\033[1;30m"
#define COLOR_BOLD_RED      "\033[1;31m"
#define COLOR_BOLD_GREEN    "\033[1;32m"
#define COLOR_BOLD_YELLOW   "\033[1;33m"
#define COLOR_BOLD_BLUE     "\033[1;34m"
#define COLOR_BOLD_MAGENTA  "\033[1;35m"
#define COLOR_BOLD_CYAN     "\033[1;36m"
#define COLOR_BOLD_WHITE    "\033[1;37m"
#define COLOR_BOLD_DEFAULT  "\033[1;39m"

#define PT_RESET            "\033[0m"
#define PT_BOLD             "\033[1m"
#define PT_UNDERLINE        "\033[4m"
#define PT_BLINKING         "\033[5m"
#define PT_INVERSE          "\033[7m"

#define CURSOR_PREV_LINE    "\033[A\033[K"

#define PR_PREFIX                   COLOR_GRAY "[bonsai:%s:%d (%s:%ld:%s)] " COLOR_DEFAULT
#define PR_PREFIX_FMT               __func__, __LINE__, get_hostname(), \
                                    syscall(SYS_gettid), get_threadname()
#define pr_color(color, fmt, ...)   printf(PR_PREFIX color fmt COLOR_DEFAULT "\n", PR_PREFIX_FMT, ##__VA_ARGS__)
#define pr_info(fmt, ...)           pr_color(COLOR_GREEN, fmt, ##__VA_ARGS__)
#define pr_warn(fmt, ...)           pr_color(COLOR_MAGENTA, fmt, ##__VA_ARGS__)
#define pr_emph(fmt, ...)           pr_color(COLOR_YELLOW, fmt, ##__VA_ARGS__)
#define pr_err(fmt, ...)            do { pr_color(COLOR_RED, fmt, ##__VA_ARGS__); dump_stack(); } while (0)
#define pr_debug(level, fmt, ...)   do { if (__builtin_expect((level) <= debug_level, 0)) pr_color(COLOR_BLUE, fmt, ##__VA_ARGS__); } while (0)

#define bonsai_assert(cond)         do { if (!(cond)) { pr_warn("assertion failed: %s", #cond); abort(); } } while (0)

#define likely(x)       __builtin_expect(!!(x), 1)
#define unlikely(x)     __builtin_expect(!!(x), 0)

#define ERR_PTR(x)      ((void *)(long)(x))
#define PTR_ERR(x)      ((long)(x))
#define IS_ERR(x)       ((unsigned long)(x) >= (unsigned long)-4095)

#define ALIGN_UP(x, a)          (((x) + (a) - 1) & ~((a) - 1))
#define ALIGN_DOWN(x, a)        ((x) & ~((a) - 1))
#define PTR_ALIGN_UP(x, a)      ((typeof(x))ALIGN_UP((unsigned long)(x), (a)))
#define PTR_ALIGN_DOWN(x, a)    ((typeof(x))ALIGN_DOWN((unsigned long)(x), (a)))
#define DIV_ROUND_UP(n, d)      (((n) + (d) - 1) / (d))

#define PAGE_SIZE       4096
#define CAS(x, old, new)    __sync_bool_compare_and_swap(&(x), (old), (new))
#define FAA(x, v)           __sync_fetch_and_add(&(x), (v))

#define cpu_relax()         asm volatile("pause\n": : :"memory")

#define CACHELINE_SIZE      64

#define barrier()           asm volatile("" ::: "memory")

#define inline              __always_inline

#define __packed            __attribute__((packed))

static inline void __read_once_size(const volatile void *p, void *res, int size) {
    switch (size) {
        case 1: *(uint8_t *)res = *(volatile uint8_t *)p; break;
        case 2: *(uint16_t *)res = *(volatile uint16_t *)p; break;
        case 4: *(uint32_t *)res = *(volatile uint32_t *)p; break;
        case 8: *(uint64_t *)res = *(volatile uint64_t *)p; break;
        default:
            barrier();
        __builtin_memcpy(res, (const void *)p, size);
        barrier();
    }
}

static inline void __write_once_size(volatile void *p, void *res, int size) {
    switch (size) {
        case 1: *(volatile uint8_t *)p = *(uint8_t *)res; break;
        case 2: *(volatile uint16_t *)p = *(uint16_t *)res; break;
        case 4: *(volatile uint32_t *)p = *(uint32_t *)res; break;
        case 8: *(volatile uint64_t *)p = *(uint64_t *)res; break;
        default:
            barrier();
        __builtin_memcpy((void *)p, (const void *)res, size);
        barrier();
    }
}

#define WRITE_ONCE(x, val) \
({							\
union { typeof(x) __val; char __c[1]; } __u =	\
{ .__val = (typeof(x)) (val) }; \
__write_once_size(&(x), __u.__c, sizeof(x));	\
__u.__val;					\
})

#define READ_ONCE(x)						\
({									\
union { typeof(x) __val; char __c[1]; } __u;			\
__read_once_size(&(x), __u.__c, sizeof(x));		\
__u.__val;							\
})

const char *get_hostname();
const char *get_threadname();

void reg_basic_sig_handler();
void dump_stack();

static inline pid_t current_tid() {
    return syscall(SYS_gettid);
}

#endif //BONSAIKV_UTILS_H
