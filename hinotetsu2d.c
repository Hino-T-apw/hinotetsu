/*
 * hinotetsud - Hinotetsu Daemon (memcached compatible)
 * 
 * Usage:
 *   ./hinotetsud [-p port] [-m memory_mb] [-d]
 * 
 * Compile:
 *   gcc -O3 -o hinotetsud hinotetsud.c hinotetsu.c -lpthread
 */
// License: BUSL (Business Source License)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <pthread.h>
#include "hinotetsu2.h"
#include <sys/uio.h>   // writev
//#include "hinotetsu.h"

#define DEFAULT_PORT 11211
#define DEFAULT_MEMORY_MB 64
#define BUFFER_SIZE 65536

static Hinotetsu *g_db = NULL;
static volatile int g_running = 1;

void signal_handler(int sig) { (void)sig; g_running = 0; }

typedef struct { int fd; char buf[BUFFER_SIZE]; size_t buf_len; } Client;

void send_response(int fd, const char *msg) { write(fd, msg, strlen(msg)); }

void handle_set(Client *c, const char *line) {
    char key[256]; int flags, exptime, bytes;

    if (sscanf(line, "set %250s %d %d %d", key, &flags, &exptime, &bytes) != 4) {
        send_response(c->fd, "CLIENT_ERROR bad command line format\r\n");
        return;
    }

    // ★ 異常値ガード（超重要：ここが無いと segfault しやすい）
    if (bytes < 0 || bytes > (int)BUFFER_SIZE) {
        send_response(c->fd, "CLIENT_ERROR bad data chunk\r\n");
        return;
    }

    if (c->buf_len < (size_t)bytes + 2) {
        send_response(c->fd, "CLIENT_ERROR not enough data\r\n");
        return;
    }

    hinotetsu_lock(g_db);
    int ret = hinotetsu_set(g_db, key, strlen(key), c->buf, (size_t)bytes, exptime);
    hinotetsu_unlock(g_db);

    send_response(c->fd, ret == HINOTETSU_OK ? "STORED\r\n" : "SERVER_ERROR out of memory\r\n");

    size_t consumed = (size_t)bytes + 2;
    if (consumed > c->buf_len) { // ★ 念のため
        c->buf_len = 0;
        c->buf[0] = '\0';
        return;
    }
    memmove(c->buf, c->buf + consumed, c->buf_len - consumed);
    c->buf_len -= consumed;
    c->buf[c->buf_len] = '\0';
}

void handle_get(Client *c, char *line) {
    char key[256];
    if (sscanf(line, "get %250s", key) != 1) {
        send_response(c->fd, "CLIENT_ERROR bad command\r\n"); return;
    }

    // thread-local buffer (1 per client thread)
    static __thread char *tls_buf = NULL;
    static __thread size_t tls_cap = 0;

    size_t need = 0;
    int ret;

    // まずは現capで試す（足りなければ拡張して再試行）
    if (tls_cap == 0) {
        tls_cap = 4096;
        tls_buf = (char*)malloc(tls_cap);
        if (!tls_buf) { send_response(c->fd, "SERVER_ERROR out of memory\r\n"); return; }
    }

    hinotetsu_lock(g_db);
    ret = hinotetsu_get_into(g_db, key, strlen(key), tls_buf, tls_cap, &need);
    hinotetsu_unlock(g_db);

    if (ret == HINOTETSU_ERR_TOOSMALL) {
        // grow to need (round up)
        size_t new_cap = tls_cap;
        while (new_cap < need) new_cap <<= 1;
        char *nb = (char*)realloc(tls_buf, new_cap);
        if (!nb) { send_response(c->fd, "SERVER_ERROR out of memory\r\n"); return; }
        tls_buf = nb;
        tls_cap = new_cap;

        hinotetsu_lock(g_db);
        ret = hinotetsu_get_into(g_db, key, strlen(key), tls_buf, tls_cap, &need);
        hinotetsu_unlock(g_db);
    }

    if (ret != HINOTETSU_OK) {
        send_response(c->fd, "END\r\n");
        return;
    }

    // VALUE header
    char header[512];
    int hlen = snprintf(header, sizeof(header), "VALUE %s 0 %zu\r\n", key, need);
    if (hlen <= 0) { send_response(c->fd, "SERVER_ERROR\r\n"); return; }

    // writev: header + value + "\r\nEND\r\n"
    static const char trailer[] = "\r\nEND\r\n";
    struct iovec iov[3];
    iov[0].iov_base = header;
    iov[0].iov_len  = (size_t)hlen;
    iov[1].iov_base = tls_buf;
    iov[1].iov_len  = need;
    iov[2].iov_base = (void*)trailer;
    iov[2].iov_len  = sizeof(trailer) - 1;

    // best-effort writev
    ssize_t w = writev(c->fd, iov, 3);
    (void)w;
}


void handle_delete(Client *c, char *line) {
    char key[256];
    if (sscanf(line, "delete %250s", key) != 1) {
        send_response(c->fd, "CLIENT_ERROR bad command\r\n"); return;
    }
    hinotetsu_lock(g_db);
    int ret = hinotetsu_delete(g_db, key, strlen(key));
    hinotetsu_unlock(g_db);
    send_response(c->fd, ret == HINOTETSU_OK ? "DELETED\r\n" : "NOT_FOUND\r\n");
}

void handle_stats(Client *c) {
    HinotetsuStats stats;
    hinotetsu_lock(g_db);
    hinotetsu_stats(g_db, &stats);
    hinotetsu_unlock(g_db);
    
    char buf[2048];
    snprintf(buf, sizeof(buf),
        "STAT version %s\r\n"
        "STAT curr_items %zu\r\n"
        "STAT bytes %zu\r\n"
        "STAT limit_maxbytes %zu\r\n"
        "STAT get_hits %zu\r\n"
        "STAT get_misses %zu\r\n"
        "STAT bloom_bits %zu\r\n"
        "STAT bloom_fill_pct %.2f\r\n"
        "STAT storage_mode %s\r\n"
        "END\r\n",
        hinotetsu_version(), stats.count, stats.memory_used, stats.pool_size,
        stats.hits, stats.misses, stats.bloom_bits, stats.bloom_fill_rate,
        stats.mode == 0 ? "hash" : "rbtree");
    send_response(c->fd, buf);
}

void handle_flush(Client *c) {
    hinotetsu_lock(g_db);
    hinotetsu_flush(g_db);
    hinotetsu_unlock(g_db);
    send_response(c->fd, "OK\r\n");
}

int process_command(Client *c) {
    char *newline = strstr(c->buf, "\r\n");
    if (!newline) return 0;
    
    *newline = '\0';
    char *line = c->buf;
    size_t line_len = newline - c->buf + 2;
    //fprintf(stderr, "LINE='%s'\n", line);
    
    if (strncmp(line, "set ", 4) == 0) {
        int bytes = -1;
        if (sscanf(line, "set %*s %*d %*d %d", &bytes) == 1) {
            if (bytes < 0 || bytes > (int)BUFFER_SIZE) {
                send_response(c->fd, "CLIENT_ERROR bad data chunk\r\n");
                memmove(c->buf, c->buf + line_len, c->buf_len - line_len);
                c->buf_len -= line_len;
                c->buf[c->buf_len] = '\0';
                return 1;
            }
            if (c->buf_len < line_len + (size_t)bytes + 2) {
                *newline = '\r';
                return 0;
            }
        }

        // ★ memmove 前に set 行をコピー（これが本丸）
        char line_copy[1024];
        size_t copy_len = (size_t)(newline - c->buf); // "\r\n" の直前まで
        if (copy_len >= sizeof(line_copy)) copy_len = sizeof(line_copy) - 1;
        memcpy(line_copy, c->buf, copy_len);
        line_copy[copy_len] = '\0';

        // 行部分を先にバッファから落とす（残り先頭が data になる）
        memmove(c->buf, c->buf + line_len, c->buf_len - line_len);
        c->buf_len -= line_len;
        c->buf[c->buf_len] = '\0';

        // コピーした行で処理（lineが壊れない）
        handle_set(c, line_copy);
    }else if (strncmp(line, "get ", 4) == 0) {
        handle_get(c, line);
        memmove(c->buf, c->buf + line_len, c->buf_len - line_len); c->buf_len -= line_len;
    }
    else if (strncmp(line, "delete ", 7) == 0) {
        handle_delete(c, line);
        memmove(c->buf, c->buf + line_len, c->buf_len - line_len); c->buf_len -= line_len;
    }
    else if (strcmp(line, "stats") == 0) {
        handle_stats(c);
        memmove(c->buf, c->buf + line_len, c->buf_len - line_len); c->buf_len -= line_len;
    }
    else if (strcmp(line, "flush_all") == 0) {
        handle_flush(c);
        memmove(c->buf, c->buf + line_len, c->buf_len - line_len); c->buf_len -= line_len;
    }
    else if (strcmp(line, "quit") == 0) { return -1; }
    else {
        send_response(c->fd, "ERROR\r\n");
        memmove(c->buf, c->buf + line_len, c->buf_len - line_len); c->buf_len -= line_len;
    }
    return 1;
}

void *client_thread(void *arg) {
    Client *c = (Client*)arg;
    while (g_running) {
        ssize_t n = read(c->fd, c->buf + c->buf_len, BUFFER_SIZE - c->buf_len - 1);
        if (n <= 0) break;
        c->buf_len += n; c->buf[c->buf_len] = '\0';
        int result; while ((result = process_command(c)) > 0);
        if (result < 0) break;
    }
    close(c->fd); free(c); return NULL;
}

void daemonize(void) {
    pid_t pid = fork(); if (pid < 0) exit(1); if (pid > 0) exit(0);
    setsid();
    pid = fork(); if (pid < 0) exit(1); if (pid > 0) exit(0);
    chdir("/"); close(STDIN_FILENO); close(STDOUT_FILENO); close(STDERR_FILENO);
}

void print_banner(int port, int memory_mb) {
    printf("\n");
    printf("  ╦ ╦╦╔╗╔╔═╗╔╦╗╔═╗╔╦╗╔═╗╦ ╦\n");
    printf("  ╠═╣║║║║║ ║ ║ ║╣  ║ ╚═╗║ ║\n");
    printf("  ╩ ╩╩╝╚╝╚═╝ ╩ ╚═╝ ╩ ╚═╝╚═╝\n");
    printf("  High Performance Key-Value Store\n");
    printf("  Version %s\n\n", hinotetsu_version());
    printf("  Port: %d | Memory: %d MB\n\n", port, memory_mb);
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT, memory_mb = DEFAULT_MEMORY_MB, daemon_mode = 0, opt;
    
    while ((opt = getopt(argc, argv, "p:m:dh")) != -1) {
        switch (opt) {
            case 'p': port = atoi(optarg); break;
            case 'm': memory_mb = atoi(optarg); break;
            case 'd': daemon_mode = 1; break;
            case 'h': default:
                printf("Usage: %s [-p port] [-m memory_mb] [-d]\n", argv[0]);
                printf("  -p port       TCP port (default: %d)\n", DEFAULT_PORT);
                printf("  -m mb         Memory in MB (default: %d)\n", DEFAULT_MEMORY_MB);
                printf("  -d            Daemonize\n");
                return opt == 'h' ? 0 : 1;
        }
    }
    
    if (daemon_mode) daemonize();
    else print_banner(port, memory_mb);
    
    signal(SIGINT, signal_handler); signal(SIGTERM, signal_handler); signal(SIGPIPE, SIG_IGN);
    
    g_db = hinotetsu_open((size_t)memory_mb * 1024 * 1024);
    if (!g_db) { fprintf(stderr, "Failed to initialize Hinotetsu\n"); return 1; }
    
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int reuse = 1; setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_addr.s_addr = INADDR_ANY; addr.sin_port = htons(port);
    
    if (bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("bind"); return 1; }
    if (listen(server_fd, 128) < 0) { perror("listen"); return 1; }
    
    if (!daemon_mode) printf("Listening on port %d...\n\n", port);
    
    while (g_running) {
        fd_set fds; FD_ZERO(&fds); FD_SET(server_fd, &fds);
        struct timeval tv = {1, 0};
        if (select(server_fd + 1, &fds, NULL, NULL, &tv) > 0 && FD_ISSET(server_fd, &fds)) {
            int client_fd = accept(server_fd, NULL, NULL);
            if (client_fd >= 0) {
                Client *c = calloc(1, sizeof(Client)); c->fd = client_fd;
                pthread_t tid; pthread_attr_t attr;
                pthread_attr_init(&attr); pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
                pthread_create(&tid, &attr, client_thread, c); pthread_attr_destroy(&attr);
            }
        }
    }
    
    printf("\nShutting down Hinotetsu...\n");
    close(server_fd); hinotetsu_close(g_db);
    return 0;
}