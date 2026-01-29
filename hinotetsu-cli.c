/*
 * hinotetsu-cli - Hinotetsu Command Line Client
 * 
 * Usage:
 *   ./hinotetsu-cli [-h host] [-p port] [command] [args...]
 *   ./hinotetsu-cli -i   # interactive mode
 * 
 * Compile:
 *   gcc -O2 -o hinotetsu-cli hinotetsu-cli.c
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 11211
#define BUF_SIZE 65536

int connect_server(const char *host, int port) {
    struct hostent *he = gethostbyname(host);
    if (!he) { fprintf(stderr, "Cannot resolve: %s\n", host); return -1; }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return -1; }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET; addr.sin_port = htons(port);
    memcpy(&addr.sin_addr, he->h_addr, he->h_length);
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) { perror("connect"); close(fd); return -1; }
    return fd;
}

void send_cmd(int fd, const char *cmd) { write(fd, cmd, strlen(cmd)); }

int read_response(int fd, char *buf, size_t size) {
    size_t total = 0;
    while (total < size - 1) {
        ssize_t n = read(fd, buf + total, size - total - 1);
        if (n <= 0) break;
        total += n; buf[total] = '\0';
        if (strstr(buf, "END\r\n") || strstr(buf, "STORED\r\n") || strstr(buf, "DELETED\r\n") ||
            strstr(buf, "NOT_FOUND\r\n") || strstr(buf, "OK\r\n") || strstr(buf, "ERROR"))
            break;
    }
    return total;
}

void cmd_set(int fd, const char *key, const char *value, int ttl) {
    char cmd[BUF_SIZE];
    snprintf(cmd, sizeof(cmd), "set %s 0 %d %zu\r\n%s\r\n", key, ttl, strlen(value), value);
    send_cmd(fd, cmd);
    char buf[1024]; read_response(fd, buf, sizeof(buf)); printf("%s", buf);
}

void cmd_get(int fd, const char *key) {
    char cmd[1024]; snprintf(cmd, sizeof(cmd), "get %s\r\n", key);
    send_cmd(fd, cmd);
    char buf[BUF_SIZE]; read_response(fd, buf, sizeof(buf));
    if (strncmp(buf, "VALUE ", 6) == 0) {
        char *data = strstr(buf, "\r\n");
        if (data) { data += 2; char *end = strstr(data, "\r\nEND"); if (end) { *end = '\0'; printf("%s\n", data); return; } }
    }
    printf("(nil)\n");
}

void cmd_delete(int fd, const char *key) {
    char cmd[1024]; snprintf(cmd, sizeof(cmd), "delete %s\r\n", key);
    send_cmd(fd, cmd);
    char buf[1024]; read_response(fd, buf, sizeof(buf));
    printf("%s", strstr(buf, "DELETED") ? "OK\n" : "NOT_FOUND\n");
}

void cmd_stats(int fd) {
    send_cmd(fd, "stats\r\n");
    char buf[BUF_SIZE]; read_response(fd, buf, sizeof(buf)); printf("%s", buf);
}

void cmd_flush(int fd) {
    send_cmd(fd, "flush_all\r\n");
    char buf[1024]; read_response(fd, buf, sizeof(buf)); printf("%s", buf);
}

void interactive(const char *host, int port) {
    printf("\n  ╦ ╦╦╔╗╔╔═╗╔╦╗╔═╗╔╦╗╔═╗╦ ╦  CLI\n");
    printf("  ╠═╣║║║║║ ║ ║ ║╣  ║ ╚═╗║ ║\n");
    printf("  ╩ ╩╩╝╚╝╚═╝ ╩ ╚═╝ ╩ ╚═╝╚═╝\n\n");
    printf("Connected to %s:%d\n", host, port);
    printf("Commands: set, get, delete, stats, flush, quit\n\n");
    
    char line[BUF_SIZE];
    while (1) {
        printf("\033[1;36mhinotetsu>\033[0m ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;
        line[strcspn(line, "\r\n")] = '\0';
        if (strlen(line) == 0) continue;
        if (strcmp(line, "quit") == 0 || strcmp(line, "exit") == 0) break;
        
        int fd = connect_server(host, port);
        if (fd < 0) continue;
        
        char key[256], value[BUF_SIZE]; int ttl = 0;
        if (sscanf(line, "set %250s %[^\n]", key, value) >= 2) {
            char *last = strrchr(value, ' ');
            if (last && atoi(last + 1) > 0) { ttl = atoi(last + 1); *last = '\0'; }
            cmd_set(fd, key, value, ttl);
        }
        else if (sscanf(line, "get %250s", key) == 1) cmd_get(fd, key);
        else if (sscanf(line, "delete %250s", key) == 1) cmd_delete(fd, key);
        else if (strcmp(line, "stats") == 0) cmd_stats(fd);
        else if (strcmp(line, "flush") == 0) cmd_flush(fd);
        else printf("Unknown command\n");
        
        close(fd);
    }
    printf("Bye!\n");
}

int main(int argc, char **argv) {
    const char *host = DEFAULT_HOST; int port = DEFAULT_PORT, inter = 0, opt;
    
    while ((opt = getopt(argc, argv, "h:p:i?")) != -1) {
        switch (opt) {
            case 'h': host = optarg; break;
            case 'p': port = atoi(optarg); break;
            case 'i': inter = 1; break;
            default:
                printf("Usage: %s [-h host] [-p port] [-i] [cmd] [args]\n", argv[0]);
                printf("  set <key> <value> [ttl]\n  get <key>\n  delete <key>\n  stats\n  flush\n");
                return opt == '?' ? 0 : 1;
        }
    }
    
    if (inter) { interactive(host, port); return 0; }
    if (optind >= argc) { printf("Usage: %s [-h host] [-p port] [-i] <command> [args]\n", argv[0]); return 1; }
    
    int fd = connect_server(host, port);
    if (fd < 0) return 1;
    
    const char *cmd = argv[optind];
    if (strcmp(cmd, "set") == 0 && optind + 2 < argc)
        cmd_set(fd, argv[optind+1], argv[optind+2], (optind+3 < argc) ? atoi(argv[optind+3]) : 0);
    else if (strcmp(cmd, "get") == 0 && optind + 1 < argc) cmd_get(fd, argv[optind+1]);
    else if (strcmp(cmd, "delete") == 0 && optind + 1 < argc) cmd_delete(fd, argv[optind+1]);
    else if (strcmp(cmd, "stats") == 0) cmd_stats(fd);
    else if (strcmp(cmd, "flush") == 0) cmd_flush(fd);
    else printf("Unknown command\n");
    
    close(fd);
    return 0;
}