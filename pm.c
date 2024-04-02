/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * Persistent Memory Management
 *
 * Hohai University
 */

#include <ndctl/libdaxctl.h>
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

static int get_device_info(const char *name, char *path, size_t *size, int *socket) {
    struct daxctl_region *dax_region;
    struct ndctl_namespace *ndns;
    struct ndctl_region *region;
    struct daxctl_dev *dax_dev;
    struct ndctl_bus *bus;
    struct ndctl_ctx *ctx;
    struct ndctl_dax *dax;
    int ret;

    ret = ndctl_new(&ctx);
    if (unlikely(ret < 0)) {
        pr_err("failed to get ndctl");
        goto out;
    }

    ret = -ENONET;

    FOREACH_BUS_REGION_NAMESPACE(ctx, bus, region, ndns) {
        if (strcmp(ndctl_namespace_get_alt_name(ndns), name) != 0) {
            continue;
        }

        dax = ndctl_namespace_get_dax(ndns);
        dax_region = ndctl_dax_get_daxctl_region(dax);
        dax_dev = daxctl_dev_get_first(dax_region);
        if (unlikely(!dax_dev)) {
            pr_err("failed to get PM device %s", name);
            goto out;
        }
        sprintf(path, "/dev/%s", daxctl_dev_get_devname(dax_dev));

        *size = daxctl_dev_get_size(dax_dev);

        *socket = ndctl_namespace_get_numa_node(ndns);

        ret = 0;
        break;
    }

    ndctl_unref(ctx);
out:
    return ret;
}

static int open_dev(struct pm_dev *dev, const char *name) {
    char path[256];
    int ret;

    ret = get_device_info(name, path, &dev->size, &dev->socket);
    if (unlikely(ret)) {
        pr_err("failed to get PM device info %s: %s", name, strerror(-ret));
        goto out;
    }

    dev->fd = open(path, O_RDWR);
    if (unlikely(dev->fd < 0)) {
        ret = -errno;
        pr_err("failed to open PM device %s: %s", path, strerror(-ret));
        goto out;
    }

    dev->start = mmap(NULL, dev->size, PROT_READ | PROT_WRITE, MAP_SHARED, dev->fd, 0);
    if (unlikely(dev->start == MAP_FAILED)) {
        ret = -errno;
        pr_err("failed to mmap PM device %s: %s", path, strerror(-ret));
    }

    dev->name = name;

    pr_debug(10, "open PM device name=%s path=%s, size=%.2fMB, socket=%d, fd=%d, start=%p",
             name, path, (double) dev->size / (1 << 20), dev->socket, dev->fd, dev->start);

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

struct pm_dev *pm_open_devs(int nr_devs, const char *dev_names[]) {
    struct pm_dev *devs;
    int i, ret;

    devs = calloc(nr_devs, sizeof(*devs));
    if (unlikely(!devs)) {
        devs = ERR_PTR(-ENOMEM);
        pr_err("failed to allocate pm_dev struct");
        goto out;
    }

    for (i = 0; i < nr_devs; i++) {
        ret = open_dev(&devs[i], dev_names[i]);
        if (unlikely(ret)) {
            free(devs);
            devs = ERR_PTR(ret);
            pr_err("failed to open PM device %s: %s", dev_names[i], strerror(-ret));
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
