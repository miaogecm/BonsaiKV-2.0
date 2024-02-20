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

#include <stdlib.h>

typedef struct lpma lpma_t;

lpma_t *lpma_create(int nr_devs, const char *dev_paths[], size_t strip_size);
void lpma_destroy(lpma_t *lpma);

void *lpma_get_ptr(lpma_t *lpma, size_t off);

void lpma_wr(lpma_t *lpma, size_t dst, void *src, size_t size);
void lpma_wr_nc(lpma_t *lpma, size_t dst, void *src, size_t size);
void lpma_rd(lpma_t *lpma, void *dst, size_t src, size_t size);

void lpma_prefetch(lpma_t *lpma, size_t off, size_t size);
void lpma_flush(lpma_t *lpma, size_t off, size_t size);
void lpma_persist(lpma_t *lpma);

size_t lpma_alloc(lpma_t *lpma, size_t size);
void lpma_free(lpma_t *lpma, size_t off, size_t size);

int lpma_socket(lpma_t *lpma);

#endif //LPM_H
