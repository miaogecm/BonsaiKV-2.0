/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Local Persistent Memory Management
 *
 * LPMM provides LPMA (Local Persistent Memory Area) abstraction. A LPMA is a logically continuous memory
 * area starting from offset 0. However, it may be physically interleaved across multiple NVMM devices.
 *
 * Hohai University
 */

#ifndef LPM_H
#define LPM_H

#include "perf.h"

typedef struct lpma lpma_t;
typedef struct lpma_cli lpma_cli_t;

lpma_t *lpma_create(int nr_devs, const char *dev_paths[], size_t strip_size);
void lpma_destroy(lpma_t *lpma);

lpma_t *lpma_cli_get_lpma(lpma_cli_t *lpma_cli);
lpma_cli_t *lpma_cli_create(lpma_t *lpma, perf_t *perf);
void lpma_cli_destroy(lpma_cli_t *lpma_cli);

void *lpma_get_ptr(lpma_cli_t *lpma_cli, size_t off);

void lpma_wr(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size);
void lpma_wr_nc(lpma_cli_t *lpma_cli, size_t dst, const void *src, size_t size);
void lpma_rd(lpma_cli_t *lpma_cli, void *dst, size_t src, size_t size);

void lpma_prefetch(lpma_cli_t *lpma_cli, size_t off, size_t size);
void lpma_flush(lpma_cli_t *lpma_cli, size_t off, size_t size);
void lpma_persist(lpma_cli_t *lpma_cli);

size_t lpma_alloc(lpma_cli_t *lpma_cli, size_t size);
void lpma_free(lpma_cli_t *lpma_cli, size_t off, size_t size);

int lpma_socket(lpma_cli_t *lpma_cli);

size_t lpma_get_stripe_size(lpma_cli_t *lpma_cli);
size_t lpma_get_strip_size(lpma_cli_t *lpma_cli);

#endif //LPM_H
