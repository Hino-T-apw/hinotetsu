// hinotetsu2d_uv.c
// libuv-based Hinotetsu daemon (memcached text protocol compatible subset)
// Cross-platform: Windows / macOS / Linux / FreeBSD
//
// Build (Unix-like):
//   gcc -O3 -std=c11 -o hinotetsu2d_uv hinotetsu2d_uv.c hinotetsu2.c -luv -lpthread
//
// Build (MSVC):
//   cl /O2 hinotetsu2d_uv.c hinotetsu2.c /I <libuv_include> /link <libuv_lib> ws2_32.lib iphlpapi.lib userenv.lib
//
// Run:
//   ./hinotetsu2d_uv -p 11211 -m 256
//
// Architecture:
//   Single-threaded event loop (like memcached) for maximum pipeline performance.
//   All KVS operations execute directly in the event loop - no worker threads.

#include <uv.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stddef.h>
#include <ctype.h>

#ifdef _WIN32
  #include <winsock2.h>
#else
  #include <signal.h>
#endif

#include "hinotetsu2.h"

#ifndef INBUF_INIT_CAP
#define INBUF_INIT_CAP (64 * 1024)
#endif

#ifndef MAX_LINE
#define MAX_LINE 4096
#endif

#ifndef MAX_KEY
#define MAX_KEY 250
#endif

#ifndef MAX_SET_BYTES
#define MAX_SET_BYTES (1024 * 1024)
#endif

#ifndef WRITE_BUF_INIT_CAP
#define WRITE_BUF_INIT_CAP (512 * 1024)
#endif

#ifndef FLUSH_THRESHOLD
#define FLUSH_THRESHOLD (256 * 1024)
#endif

// -----------------------------
// Global DB
// -----------------------------
static Hinotetsu* g_db = NULL;

// -----------------------------
// Reusable GET buffer (single-threaded, so global is fine)
// -----------------------------
static char* g_get_buf = NULL;
static size_t g_get_buf_cap = 0;

static char* ensure_get_buf(size_t need) {
  if (g_get_buf_cap < need) {
    size_t cap = g_get_buf_cap ? g_get_buf_cap : 4096;
    while (cap < need) cap <<= 1;
    char* nb = (char*)realloc(g_get_buf, cap);
    if (!nb) return NULL;
    g_get_buf = nb;
    g_get_buf_cap = cap;
  }
  return g_get_buf;
}

// -----------------------------
// Small helpers
// -----------------------------
static void die(const char* msg) {
  fprintf(stderr, "ERROR: %s\n", msg);
  exit(1);
}

static void* xmalloc(size_t n) {
  void* p = malloc(n);
  if (!p) die("out of memory");
  return p;
}

static void* xrealloc(void* p, size_t n) {
  void* q = realloc(p, n);
  if (!q) die("out of memory");
  return q;
}

static int find_crlf(const char* buf, size_t len) {
  for (size_t i = 0; i + 1 < len; i++) {
    if (buf[i] == '\r' && buf[i + 1] == '\n') return (int)i;
  }
  return -1;
}

// -----------------------------
// Safe string copy
// -----------------------------
static int safe_copy_key(char* dst, size_t dst_size, const char* src, size_t src_len) {
  if (src_len >= dst_size) return -1;
  memcpy(dst, src, src_len);
  dst[src_len] = '\0';
  return 0;
}

// -----------------------------
// Manual parser
// -----------------------------
static const char* skip_spaces(const char* p) {
  while (*p == ' ' || *p == '\t') p++;
  return p;
}

static const char* parse_token(const char* p, char* out, size_t out_size, size_t* out_len) {
  p = skip_spaces(p);
  size_t i = 0;
  while (*p && *p != ' ' && *p != '\t' && *p != '\r' && *p != '\n') {
    if (i < out_size - 1) out[i++] = *p;
    p++;
  }
  out[i] = '\0';
  if (out_len) *out_len = i;
  return p;
}

static const char* parse_uint(const char* p, int* out, int* ok) {
  p = skip_spaces(p);
  *ok = 0;
  if (!isdigit((unsigned char)*p) && *p != '-') return p;

  int neg = 0;
  if (*p == '-') { neg = 1; p++; }
  if (!isdigit((unsigned char)*p)) return p;

  long val = 0;
  while (isdigit((unsigned char)*p)) {
    val = val * 10 + (*p - '0');
    if (val > INT32_MAX) return p;
    p++;
  }

  *out = neg ? -(int)val : (int)val;
  *ok = 1;
  return p;
}

static int parse_set_cmd(const char* line, char* key, size_t key_size,
                         int* flags, int* exptime, int* bytes) {
  const char* p = line;
  char cmd[16];
  p = parse_token(p, cmd, sizeof(cmd), NULL);
  if (strcmp(cmd, "set") != 0) return -1;

  size_t key_len;
  p = parse_token(p, key, key_size, &key_len);
  if (key_len == 0 || key_len > MAX_KEY) return -1;

  int ok;
  p = parse_uint(p, flags, &ok);
  if (!ok) return -1;
  p = parse_uint(p, exptime, &ok);
  if (!ok) return -1;
  p = parse_uint(p, bytes, &ok);
  if (!ok || *bytes < 0) return -1;

  p = skip_spaces(p);
  if (*p != '\0') return -1;
  return 0;
}

static int parse_single_key_cmd(const char* line, const char* cmd_name, char* key, size_t key_size) {
  const char* p = line;
  char cmd[16];
  p = parse_token(p, cmd, sizeof(cmd), NULL);
  if (strcmp(cmd, cmd_name) != 0) return -1;

  size_t key_len;
  p = parse_token(p, key, key_size, &key_len);
  if (key_len == 0 || key_len > MAX_KEY) return -1;

  p = skip_spaces(p);
  if (*p != '\0') return -1;
  return 0;
}

// -----------------------------
// Connection object
// -----------------------------
typedef struct Conn Conn;

struct Conn {
  uv_tcp_t tcp;

  // Input buffer
  char* inbuf;
  size_t in_len;
  size_t in_cap;

  // Double-buffered output (swap on flush)
  char* outbuf[2];
  size_t out_len;
  size_t out_cap;
  int out_active;  // which buffer is being written (0 or 1)

  // Pending set state
  int pending_set;
  char pending_key[MAX_KEY + 1];
  int pending_flags;
  int pending_exptime;
  int pending_bytes;

  // Write state
  uv_write_t write_req;
  int writing;
  int closing;
};

// Forward declarations
static void conn_flush_output(Conn* c);
static void on_closed(uv_handle_t* handle);

// -----------------------------
// Output buffering (double-buffered)
// -----------------------------
static void conn_append_output(Conn* c, const char* data, size_t len) {
  if (c->closing) return;

  int idx = c->writing ? (1 - c->out_active) : c->out_active;
  char** buf = &c->outbuf[idx];

  if (c->out_len + len > c->out_cap) {
    size_t cap = c->out_cap ? c->out_cap : WRITE_BUF_INIT_CAP;
    while (cap < c->out_len + len) cap <<= 1;
    // Grow both buffers to same size
    c->outbuf[0] = (char*)xrealloc(c->outbuf[0], cap);
    c->outbuf[1] = (char*)xrealloc(c->outbuf[1], cap);
    c->out_cap = cap;
  }
  memcpy(*buf + c->out_len, data, len);
  c->out_len += len;

  // Flush when threshold reached
  if (c->out_len >= FLUSH_THRESHOLD && !c->writing) {
    conn_flush_output(c);
  }
}

static void conn_append_str(Conn* c, const char* s) {
  conn_append_output(c, s, strlen(s));
}

static void write_cb(uv_write_t* req, int status) {
  Conn* c = (Conn*)req->handle->data;
  c->writing = 0;

  if (status < 0 || c->closing) {
    if (!c->closing) {
      c->closing = 1;
      uv_close((uv_handle_t*)&c->tcp, on_closed);
    }
    return;
  }

  // If data accumulated in the other buffer while writing, flush it
  if (c->out_len > 0) {
    conn_flush_output(c);
  }
}

static void conn_flush_output(Conn* c) {
  if (c->closing || c->writing || c->out_len == 0) return;

  int idx = c->out_active;
  uv_buf_t b = uv_buf_init(c->outbuf[idx], (unsigned int)c->out_len);

  c->writing = 1;
  c->out_active = 1 - idx;  // Swap to other buffer
  size_t written_len = c->out_len;
  c->out_len = 0;  // Reset length for new buffer

  int rc = uv_write(&c->write_req, (uv_stream_t*)&c->tcp, &b, 1, write_cb);
  if (rc != 0) {
    c->writing = 0;
    c->out_len = written_len;  // Restore on error
    c->out_active = idx;
    c->closing = 1;
    uv_close((uv_handle_t*)&c->tcp, on_closed);
  }
}

// -----------------------------
// Connection lifecycle
// -----------------------------
static void close_conn(Conn* c) {
  if (!c) return;
  if (c->inbuf) free(c->inbuf);
  if (c->outbuf[0]) free(c->outbuf[0]);
  if (c->outbuf[1]) free(c->outbuf[1]);
  free(c);
}

static void on_closed(uv_handle_t* handle) {
  Conn* c = (Conn*)handle->data;
  close_conn(c);
}

static void alloc_cb(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf) {
  (void)handle;
  size_t n = suggested_size;
  if (n < 8192) n = 8192;
  buf->base = (char*)malloc(n);
  buf->len = (unsigned int)n;
}

// Command handlers (direct execution, no locks)
// -----------------------------
static void handle_set(Conn* c, const char* key, int flags, int exptime, const char* value, size_t vlen) {
  (void)flags;
  int ret = hinotetsu_set_nolock(g_db, key, strlen(key), value, vlen,
                                 (uint32_t)(exptime < 0 ? 0 : exptime));
  conn_append_str(c, ret == HINOTETSU_OK ? "STORED\r\n" : "SERVER_ERROR out of memory\r\n");
}

static void handle_get(Conn* c, const char* key) {
  size_t need = 0;
  char* buf = ensure_get_buf(4096);
  if (!buf) {
    conn_append_str(c, "SERVER_ERROR out of memory\r\n");
    return;
  }

  int ret = hinotetsu_get_into_nolock(g_db, key, strlen(key), buf, g_get_buf_cap, &need);

  if (ret == HINOTETSU_ERR_TOOSMALL) {
    buf = ensure_get_buf(need);
    if (!buf) {
      conn_append_str(c, "SERVER_ERROR out of memory\r\n");
      return;
    }
    ret = hinotetsu_get_into_nolock(g_db, key, strlen(key), buf, g_get_buf_cap, &need);
  }

  if (ret != HINOTETSU_OK) {
    conn_append_str(c, "END\r\n");
    return;
  }

  char header[512];
  int hlen = snprintf(header, sizeof(header), "VALUE %s 0 %zu\r\n", key, need);
  if (hlen <= 0 || (size_t)hlen >= sizeof(header)) {
    conn_append_str(c, "SERVER_ERROR\r\n");
    return;
  }

  conn_append_output(c, header, (size_t)hlen);
  conn_append_output(c, buf, need);
  conn_append_str(c, "\r\nEND\r\n");
}

static void handle_delete(Conn* c, const char* key) {
  int ret = hinotetsu_delete_nolock(g_db, key, strlen(key));
  conn_append_str(c, ret == HINOTETSU_OK ? "DELETED\r\n" : "NOT_FOUND\r\n");
}

static void handle_stats(Conn* c) {
  HinotetsuStats st;
  hinotetsu_stats_nolock(g_db, &st);

  char buf[2048];
  int n = snprintf(buf, sizeof(buf),
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
    hinotetsu_version(),
    st.count, st.memory_used, st.pool_size,
    st.hits, st.misses,
    st.bloom_bits, st.bloom_fill_rate,
    st.mode == 0 ? "hash" : "rbtree");

  if (n > 0 && (size_t)n < sizeof(buf)) {
    conn_append_output(c, buf, (size_t)n);
  } else {
    conn_append_str(c, "SERVER_ERROR\r\n");
  }
}

static void handle_flush(Conn* c) {
  hinotetsu_flush_nolock(g_db);
  conn_append_str(c, "OK\r\n");
}

// -----------------------------
// Parser
// -----------------------------
static void consume_prefix(Conn* c, size_t n) {
  if (n == 0) return;
  if (n >= c->in_len) { c->in_len = 0; return; }
  memmove(c->inbuf, c->inbuf + n, c->in_len - n);
  c->in_len -= n;
}

static void parse_and_dispatch(Conn* c) {
  if (c->closing) return;

  for (;;) {
    if (c->closing) return;

    // Handle pending SET data
    if (c->pending_set) {
      size_t need = (size_t)c->pending_bytes + 2;
      if (c->in_len < need) break;

      handle_set(c, c->pending_key, c->pending_flags, c->pending_exptime,
                 c->inbuf, (size_t)c->pending_bytes);
      consume_prefix(c, need);
      c->pending_set = 0;
      continue;
    }

    // Find command line
    int cr = find_crlf(c->inbuf, c->in_len);
    if (cr < 0) break;

    size_t line_len = (size_t)cr;
    if (line_len > MAX_LINE) {
      consume_prefix(c, line_len + 2);
      conn_append_str(c, "CLIENT_ERROR bad command line format\r\n");
      continue;
    }

    char line[MAX_LINE + 1];
    memcpy(line, c->inbuf, line_len);
    line[line_len] = '\0';
    consume_prefix(c, line_len + 2);

    // Parse command
    char cmd[16];
    parse_token(line, cmd, sizeof(cmd), NULL);

    if (strcmp(cmd, "set") == 0) {
      char key[MAX_KEY + 1];
      int flags = 0, exptime = 0, bytes = -1;

      if (parse_set_cmd(line, key, sizeof(key), &flags, &exptime, &bytes) != 0) {
        conn_append_str(c, "CLIENT_ERROR bad command line format\r\n");
        continue;
      }
      if (bytes < 0 || bytes > (int)MAX_SET_BYTES) {
        conn_append_str(c, "CLIENT_ERROR bad data chunk\r\n");
        continue;
      }

      c->pending_set = 1;
      safe_copy_key(c->pending_key, sizeof(c->pending_key), key, strlen(key));
      c->pending_flags = flags;
      c->pending_exptime = exptime;
      c->pending_bytes = bytes;
      continue;
    }
    else if (strcmp(cmd, "get") == 0) {
      char key[MAX_KEY + 1];
      if (parse_single_key_cmd(line, "get", key, sizeof(key)) != 0) {
        conn_append_str(c, "CLIENT_ERROR bad command\r\n");
        continue;
      }
      handle_get(c, key);
    }
    else if (strcmp(cmd, "delete") == 0) {
      char key[MAX_KEY + 1];
      if (parse_single_key_cmd(line, "delete", key, sizeof(key)) != 0) {
        conn_append_str(c, "CLIENT_ERROR bad command\r\n");
        continue;
      }
      handle_delete(c, key);
    }
    else if (strcmp(cmd, "stats") == 0) {
      const char* p = skip_spaces(line + 5);
      if (*p != '\0') {
        conn_append_str(c, "CLIENT_ERROR bad command\r\n");
        continue;
      }
      handle_stats(c);
    }
    else if (strcmp(cmd, "flush_all") == 0) {
      const char* p = skip_spaces(line + 9);
      if (*p != '\0') {
        conn_append_str(c, "CLIENT_ERROR bad command\r\n");
        continue;
      }
      handle_flush(c);
    }
    else if (strcmp(cmd, "quit") == 0) {
      c->closing = 1;
      uv_close((uv_handle_t*)&c->tcp, on_closed);
      return;
    }
    else {
      conn_append_str(c, "ERROR\r\n");
    }
  }

  // Flush accumulated output
  conn_flush_output(c);
}

static void read_cb(uv_stream_t* stream, ssize_t nread, const uv_buf_t* buf) {
  Conn* c = (Conn*)stream->data;

  if (nread <= 0) {
    free(buf->base);
    if (nread < 0 && !c->closing) {
      c->closing = 1;
      uv_close((uv_handle_t*)&c->tcp, on_closed);
    }
    return;
  }

  if (c->closing) {
    free(buf->base);
    return;
  }

  // Grow input buffer if needed
  if (c->in_len + (size_t)nread > c->in_cap) {
    size_t nc = c->in_cap ? c->in_cap : INBUF_INIT_CAP;
    while (nc < c->in_len + (size_t)nread) nc <<= 1;
    c->inbuf = (char*)xrealloc(c->inbuf, nc);
    c->in_cap = nc;
  }
  memcpy(c->inbuf + c->in_len, buf->base, (size_t)nread);
  c->in_len += (size_t)nread;

  free(buf->base);

  parse_and_dispatch(c);
}

// -----------------------------
// Accept callback
// -----------------------------
static void on_new_conn(uv_stream_t* server, int status) {
  if (status < 0) return;

  Conn* c = (Conn*)calloc(1, sizeof(Conn));
  if (!c) return;

  c->in_cap = INBUF_INIT_CAP;
  c->inbuf = (char*)malloc(c->in_cap);
  if (!c->inbuf) { free(c); return; }

  c->out_cap = WRITE_BUF_INIT_CAP;
  c->outbuf[0] = (char*)malloc(c->out_cap);
  c->outbuf[1] = (char*)malloc(c->out_cap);
  if (!c->outbuf[0] || !c->outbuf[1]) {
    if (c->outbuf[0]) free(c->outbuf[0]);
    if (c->outbuf[1]) free(c->outbuf[1]);
    free(c->inbuf);
    free(c);
    return;
  }
  c->out_active = 0;

  uv_tcp_init(uv_default_loop(), &c->tcp);
  c->tcp.data = c;

  // TCP optimizations
  uv_tcp_nodelay(&c->tcp, 1);

  // Increase send buffer size (reduces write blocking)
  uv_send_buffer_size((uv_handle_t*)&c->tcp, &(int){1024 * 1024});

  if (uv_accept(server, (uv_stream_t*)&c->tcp) == 0) {
    uv_read_start((uv_stream_t*)&c->tcp, alloc_cb, read_cb);
  } else {
    uv_close((uv_handle_t*)&c->tcp, on_closed);
  }
}

// -----------------------------
// CLI
// -----------------------------
static void usage(const char* argv0) {
  fprintf(stderr,
    "Usage: %s [-p port] [-m memory_mb]\n"
    "  -p port       TCP port (default: 11211)\n"
    "  -m mb         Memory in MB (default: 64)\n",
    argv0);
}

static void print_banner(int port, int memory_mb) {
  fprintf(stderr, "\n");
  fprintf(stderr, "  ╦ ╦╦╔╗╔╔═╗╔╦╗╔═╗╔╦╗╔═╗╦ ╦\n");
  fprintf(stderr, "  ╠═╣║║║║║ ║ ║ ║╣  ║ ╚═╗║ ║\n");
  fprintf(stderr, "  ╩ ╩╩╝╚═╚═╝ ╩ ╚═╝ ╩ ╚═╝╚═╝\n");
  fprintf(stderr, "  High Performance Key-Value Store (libuv)\n");
  fprintf(stderr, "  Version %s\n\n", hinotetsu_version());
  fprintf(stderr, "  Port: %d | Memory: %d MB\n\n", port, memory_mb);
}

int main(int argc, char** argv) {
  int port = 11211;
  int memory_mb = 64;

  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) port = atoi(argv[++i]);
    else if (strcmp(argv[i], "-m") == 0 && i + 1 < argc) memory_mb = atoi(argv[++i]);
    else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) { usage(argv[0]); return 0; }
    else { usage(argv[0]); return 1; }
  }

#ifndef _WIN32
  signal(SIGPIPE, SIG_IGN);
#endif

  size_t pool_bytes = (size_t)memory_mb * 1024u * 1024u;
  g_db = hinotetsu_open(pool_bytes);
  if (!g_db) die("Failed to initialize Hinotetsu");

  // Pre-allocate GET buffer
  g_get_buf = (char*)malloc(64 * 1024);
  g_get_buf_cap = g_get_buf ? 64 * 1024 : 0;

  uv_tcp_t server;
  uv_tcp_init(uv_default_loop(), &server);

  struct sockaddr_in addr4;
  if (uv_ip4_addr("0.0.0.0", port, &addr4) != 0) die("uv_ip4_addr failed");
  if (uv_tcp_bind(&server, (const struct sockaddr*)&addr4, 0) != 0) die("uv_tcp_bind failed");
  if (uv_listen((uv_stream_t*)&server, 1024, on_new_conn) != 0) die("uv_listen failed");

  print_banner(port, memory_mb);
  fprintf(stderr, "Listening on port %d...\n\n", port);

  uv_run(uv_default_loop(), UV_RUN_DEFAULT);

  if (g_get_buf) free(g_get_buf);
  hinotetsu_close(g_db);
  return 0;
}