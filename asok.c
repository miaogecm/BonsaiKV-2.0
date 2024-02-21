/*
 * BonsaiKV+: Scaling persistent in-memory key-value store for modern tiered, heterogeneous memory systems
 *
 * BonsaiKV+ Admin Socket
 *
 * Hohai University
 */

#define _GNU_SOURCE

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/param.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>
#include <poll.h>

#include "utils.h"
#include "list.h"
#include "asok.h"
#include "kv.h"

#define CMD_BUFSZ   1024ul

struct asok {
    kv_t *kv;

    pthread_t thread;
    pid_t tid;

    int sock_fd;

    struct list_head handlers;

    bool exit;
};

struct asok_handler {
    struct list_head node;
    const char *prefix;
    asok_cmd_handler_fn_t fn;
};

static inline int do_exec_cmd(asok_t *asok, const struct asok_handler *handler,
                              const cJSON *cmd_json, cJSON *out_json) {
    return handler->fn(asok, out_json, cmd_json);
}

static inline int exec_cmd(asok_t *asok, const char *cmd, cJSON *out_json) {
    struct asok_handler *handler;
    cJSON *cmd_json, *curr;
    const char *prefix;
    int ret = 0;

    /* parse command JSON */
    cmd_json = cJSON_Parse(cmd);
    if (unlikely(!cmd_json)) {
        pr_err("failed to parse command: %s", cmd);
        cJSON_AddStringToObject(out_json, "errmsg", "failed to parse command");
        ret = -EINVAL;
        goto out;
    }

    /* find command prefix */
    prefix = NULL;
    for (curr = cmd_json->child; curr; curr = curr->next) {
        if (cJSON_IsString(curr) && !strcmp(curr->string, "prefix")) {
            prefix = curr->valuestring;
            break;
        }
    }
    if (unlikely(!prefix)) {
        pr_err("command prefix not found: %s", cmd);
        cJSON_AddStringToObject(out_json, "errmsg", "prefix not found");
        ret = -EINVAL;
        goto out_delete_cmd_json;
    }
    pr_debug(20, "command prefix: %s", prefix);

    /* find command handler */
    ret = 1;

    list_for_each_entry(handler, &asok->handlers, node) {
        if (strcmp(prefix, handler->prefix) != 0) {
            continue;
        }

        pr_debug(20, "found handler for prefix %s", prefix);
        ret = do_exec_cmd(asok, handler, cmd_json, out_json);
        break;
    }

    if (ret == 1) {
        ret = -ENOENT;
        pr_err("handler not found for prefix %s", prefix);
        cJSON_AddStringToObject(out_json, "errmsg", "handler not found");
    }

out_delete_cmd_json:
    cJSON_Delete(cmd_json);

out:
    return ret;
}

static inline void do_accept(asok_t *asok) {
    char cmd[CMD_BUFSZ], *out_str;
    struct sockaddr_un addr;
    int conn_fd, pos, ret;
    cJSON *out_json;
    size_t len;

    pr_debug(20, "accept from sock fd %d", asok->sock_fd);

    conn_fd = accept4(asok->sock_fd, (struct sockaddr *) &addr, &(socklen_t) { sizeof(addr) }, SOCK_CLOEXEC);

    for (pos = 0; ; ) {
        ret = recv(conn_fd, &cmd[pos], 1, 0);
        if (unlikely(ret <= 0)) {
            if (ret < 0) {
                pr_err("recv failed: %s", strerror(errno));
            } else {
                pr_err("malformed command, received [%.*s]", pos, cmd);
            }
            goto out;
        }

        if (cmd[pos] == '\0' || cmd[pos] == '\n') {
            cmd[pos] = '\0';
            break;
        }

        if (++pos >= CMD_BUFSZ) {
            pr_err("command too long, maximum: %lu", CMD_BUFSZ);
            goto out;
        }
    }

    out_json = cJSON_CreateObject();

    pr_debug(20, "execute command %s", cmd);
    ret = exec_cmd(asok, cmd, out_json);
    if (unlikely(ret)) {
        pr_err("failed to execute command: %s (%s)", cmd, strerror(-ret));
    }

    cJSON_AddItemToObject(out_json, "ret", cJSON_CreateNumber(ret));
    cJSON_AddStringToObject(out_json, "errstr", strerror(-ret));

    out_str = cJSON_Print(out_json);
    if (unlikely(!out_str)) {
        pr_err("failed to print response to out_str");
        goto out_delete_out_json;
    }

    /* send via zero-terminated (JSON) string */
    len = strlen(out_str) + 1;
    bonsai_assert(out_str[len] == '\0');
    pr_debug(20, "send response (len: %lu): %s", len, out_str);
    ret = send(conn_fd, out_str, len, 0);
    if (unlikely(ret != len)) {
        pr_err("send failed: %s", strerror(errno));
    }

    free(out_str);

out_delete_out_json:
    cJSON_Delete(out_json);

out:
    close(conn_fd);
}

static void *asok_worker(void *arg) {
    struct pollfd fds[1];
    asok_t *asok = arg;

    asok->tid = current_tid();

    pr_debug(5, "asok worker start");

    while (!READ_ONCE(asok->exit)) {
        memset(fds, 0, sizeof(fds));
        fds[0].fd = asok->sock_fd;
        fds[0].events = POLLIN | POLLRDBAND;

        pr_debug(20, "waiting");
        if (unlikely(poll(fds, 1, -1) < 0)) {
            if (errno == EINTR) {
                continue;
            }
            pr_err("poll failed: %s", strerror(errno));
            break;
        }
        pr_debug(20, "awake");

        if (fds[0].revents & POLLIN) {
            do_accept(asok);
        }
    }

out:
    pr_debug(5, "asok worker exit");
    return NULL;
}

static inline int bind_and_listen(asok_t *asok, const char *sock_path) {
    size_t sock_path_len = strlen(sock_path);
    struct sockaddr_un addr = { 0 };
    int sock_fd, ret = 0;

    pr_debug(5, "bind %s", sock_path);

    if (unlikely(sock_path_len > sizeof(addr.sun_path) - 1)) {
        pr_err("socket path too long, maximum: %lu", sizeof(addr.sun_path) - 1);
        ret = -ENAMETOOLONG;
        goto out;
    }

    sock_fd = socket(PF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (unlikely(sock_fd < 0)) {
        ret = -errno;
        pr_err("failed to create asok: %s", strerror(-ret));
        goto out;
    }

    addr.sun_family = AF_UNIX;
    strcpy(addr.sun_path, sock_path);

    if (unlikely(bind(sock_fd, (struct sockaddr *) &addr, sizeof(addr)) != 0)) {
        ret = -errno;
        pr_err("failed to bind asok: %s", strerror(-ret));
        close(sock_fd);
        goto out;
    }

    pr_debug(5, "listen to sock fd %d", sock_fd);

    if (unlikely(listen(sock_fd, 5) != 0)) {
        ret = -errno;
        pr_err("failed to listen asok: %s", strerror(-ret));
        close(sock_fd);
        goto out;
    }

    asok->sock_fd = sock_fd;

out:
    return ret;
}

asok_t *asok_create(kv_t *kv, const char *sock_path) {
    asok_t *asok;
    int ret;

    asok = calloc(1, sizeof(asok_t));
    if (unlikely(!asok)) {
        asok = ERR_PTR(-ENOMEM);
        pr_err("failed to alloc mem for asok struct");
        goto out;
    }

    INIT_LIST_HEAD(&asok->handlers);

    ret = bind_and_listen(asok, sock_path);
    if (unlikely(ret)) {
        asok = ERR_PTR(ret);
        goto out;
    }

    ret = pthread_create(&asok->thread, NULL, asok_worker, asok);
    if (unlikely(ret)) {
        asok = ERR_PTR(-ret);
        pr_err("failed to create asok worker thread: %s", strerror(ret));
        goto out;
    }

    pthread_setname_np(asok->thread, "bonsai-asok");

    while (!READ_ONCE(asok->tid)) {
        cpu_relax();
    }

    pr_debug(5, "asok created, tid=%d", asok->tid);

out:
    return asok;
}

int asok_register_handler(asok_t *asok, const char *prefix, asok_cmd_handler_fn_t fn) {
    struct asok_handler *handler;
    int ret = 0;

    handler = calloc(1, sizeof(struct asok_handler));
    if (unlikely(!handler)) {
        pr_err("failed to allocate memory for asok handler");
        ret = -ENOMEM;
        goto out;
    }

    handler->prefix = prefix;
    handler->fn = fn;

    list_add_tail(&handler->node, &asok->handlers);

out:
    return ret;
}

void asok_destroy(asok_t *asok) {
    struct asok_handler *handler, *tmp;

    pr_debug(5, "destroy asok");

    asok->exit = true;
    pthread_join(asok->thread, NULL);

    list_for_each_entry_safe(handler, tmp, &asok->handlers, node) {
        list_del(&handler->node);
        free(handler);
    }
    free(asok);
}
