// hinotetsu2.h
// Sharded, cache-friendly KV store (memcached-text compatible backend)
// License: BUSL (Business Source License)
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// Version
#define HINOTETSU_VERSION_STRING "2.2-sharded-getinto-slab-nolock"

// Defaults
#define HINOTETSU_DEFAULT_POOL_SIZE (64ULL * 1024ULL * 1024ULL)

// Error codes
#define HINOTETSU_OK           0
#define HINOTETSU_ERR_NOTFOUND 1
#define HINOTETSU_ERR_NOMEM    2
#define HINOTETSU_ERR_IO       3
#define HINOTETSU_ERR_TOOSMALL 4

// Tuning
#ifndef HINOTETSU_SHARDS
#define HINOTETSU_SHARDS 64u
#endif

#ifndef HINOTETSU_INIT_CAP
#define HINOTETSU_INIT_CAP (1u << 16) // 64K slots per shard
#endif

#ifndef HINOTETSU_SLAB_MIN_SHIFT
#define HINOTETSU_SLAB_MIN_SHIFT 6u
#endif

#ifndef HINOTETSU_SLAB_MAX_SHIFT
#define HINOTETSU_SLAB_MAX_SHIFT 12u
#endif

#ifndef HINOTETSU_SLAB_PAGE_SIZE
#define HINOTETSU_SLAB_PAGE_SIZE (64u * 1024u)
#endif

typedef struct Hinotetsu Hinotetsu;

typedef struct HinotetsuStats {
  size_t count;
  size_t memory_used;
  size_t pool_size;
  size_t hits;
  size_t misses;
  size_t bloom_bits;
  double bloom_fill_rate;
  int mode;
} HinotetsuStats;

// Core API (with locks - thread-safe)
Hinotetsu* hinotetsu_open(size_t pool_size_bytes);
void hinotetsu_close(Hinotetsu* db);

int hinotetsu_set(Hinotetsu* db,
                  const char* key, size_t klen,
                  const char* value, size_t vlen,
                  uint32_t ttl_seconds);

int hinotetsu_get(Hinotetsu* db,
                  const char* key, size_t klen,
                  char** out_value, size_t* out_vlen);

int hinotetsu_get_into(Hinotetsu* db,
                       const char* key, size_t klen,
                       char* dst, size_t dst_cap,
                       size_t* out_vlen);

int hinotetsu_delete(Hinotetsu* db, const char* key, size_t klen);

void hinotetsu_flush(Hinotetsu* db);
void hinotetsu_stats(Hinotetsu* db, HinotetsuStats* out);
const char* hinotetsu_version(void);

// ============================================================
// Lock-free API (for single-threaded use - e.g., libuv daemon)
// These skip pthread_rwlock for maximum performance.
// WARNING: NOT thread-safe! Use only from a single thread.
// ============================================================
int hinotetsu_set_nolock(Hinotetsu* db,
                         const char* key, size_t klen,
                         const char* value, size_t vlen,
                         uint32_t ttl_seconds);

int hinotetsu_get_into_nolock(Hinotetsu* db,
                              const char* key, size_t klen,
                              char* dst, size_t dst_cap,
                              size_t* out_vlen);

int hinotetsu_delete_nolock(Hinotetsu* db, const char* key, size_t klen);

void hinotetsu_flush_nolock(Hinotetsu* db);
void hinotetsu_stats_nolock(Hinotetsu* db, HinotetsuStats* out);

// Compatibility (no-ops)
void hinotetsu_lock(Hinotetsu* db);
void hinotetsu_unlock(Hinotetsu* db);

#ifdef __cplusplus
}
#endif