/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Persistent Memory Management
 *
 * Hohai University
 */

#include <ndctl/libndctl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#include "utils.h"
#include "pm.h"

#define FOREACH_BUS_REGION_NAMESPACE(ctx, bus, region, ndns)	\
	        ndctl_bus_foreach(ctx, bus)				            \
	        ndctl_region_foreach(bus, region)			        \
        	ndctl_namespace_foreach(region, ndns)

static int get_device_socket(const char *path) {
    struct ndctl_namespace *ndns;
    struct ndctl_region *region;
    struct ndctl_bus *bus;
    struct ndctl_ctx *ctx;
    int ret;

    ret = ndctl_new(&ctx);
    if (unlikely(ret < 0)) {
        pr_err("failed to get ndctl");
        goto out;
    }

    FOREACH_BUS_REGION_NAMESPACE(ctx, bus, region, ndns) {
        if (strcmp(ndctl_namespace_get_devname(ndns), path) != 0) {
            continue;
        }

        ret = ndctl_namespace_get_numa_node(ndns);
        break;
    }

    ndctl_unref(ctx);
out:
    return ret;
}

static int open_dev(struct pm_dev *dev, const char *path) {
    struct stat sbuf;
    int ret = 0;

    if (unlikely(stat(path, &sbuf))) {
        ret = -errno;
        pr_err("failed to stat PM device %s: %s", path, strerror(-ret));
        goto out;
    }
    dev->size = sbuf.st_size;

    dev->socket = get_device_socket(path);
    if (unlikely(dev->socket < 0)) {
        ret = dev->socket;
        pr_err("failed to get PM device socket %s: %s", path, strerror(-ret));
        goto out;
    }

    dev->fd = open(path, O_RDWR);
    if (unlikely(dev->fd < 0)) {
        ret = -errno;
        pr_err("failed to open PM device %s: %s", path, strerror(-ret));
        goto out;
    }

    dev->start = mmap(NULL, sbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, dev->fd, 0);
    if (unlikely(dev->start == MAP_FAILED)) {
        ret = -errno;
        pr_err("failed to mmap PM device %s: %s", path, strerror(-ret));
    }

    pr_debug(10, "open PM device path=%s, size=%.2fMB, socket=%d, fd=%d, start=%p",
             path, (double) sbuf.st_size / (1 << 20), dev->socket, dev->fd, dev->start);

out:
    return ret;
}

static void close_dev(struct pm_dev *dev) {
    if (dev->start) {
        munmap(dev->start, dev->size);
    }
    if (dev->fd >= 0) {
        close(dev->fd);
    }
}

struct pm_dev *pm_open_devs(int nr_devs, const char *dev_paths[]) {
    struct pm_dev *devs;
    int i, ret;

    devs = calloc(nr_devs, sizeof(*devs));
    if (unlikely(!devs)) {
        devs = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate pm_dev struct");
        goto out;
    }

    for (i = 0; i < nr_devs; i++) {
        ret = open_dev(&devs[i], dev_paths[i]);
        if (unlikely(ret)) {
            free(devs);
            devs = ERR_PTR(ret);
            pr_err("failed to open PM device %s: %s", dev_paths[i], strerror(-ret));
            goto out;
        }
    }

out:
    return devs;
}

void pm_close_devs(int nr_devs, struct pm_dev *devs) {
    int i;

    for (i = 0; i < nr_devs; i++) {
        close_dev(&devs[i]);
    }
}
