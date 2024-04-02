/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Persistent Memory Management
 *
 * Hohai University
 */

#ifndef PM_H
#define PM_H

#include <stdlib.h>

struct pm_dev {
    void *start;
    int socket;

    const char *name;
    size_t size;
    int fd;
};

struct pm_dev *pm_open_devs(int nr_devs, const char *dev_names[]);
void pm_close_devs(int nr_devs, struct pm_dev *devs);

#endif //PM_H
