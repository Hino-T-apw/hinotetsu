// hinotetsu3.h
// Ultra-low-latency sharded KV store with incremental resize
// - Incremental hash table resize (no spike on grow)
// - Pre-warmed slab allocator
// - Memory pre-touch on startup
// License: BUSL (Business Source License)
#pragma once

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define HINOTETSU_VERSION_STRING "3.0-incremental"

// Error codes
#define HINOTETSU_OK           0
#define HINOTETSU_ERR_NOTFOUND 1
#define HINOTETSU_ERR_NOMEM    2
#define HINOTETSU_ERR_IO       3
#define HINOTETSU_ERR_TOOSMALL 4

// Tuning (override with -D at compile time)
#ifndef HINOTETSU_SHARDS
#define HINOTETSU_SHARDS 64u
#endif

#ifndef HINOTETSU_INIT_CAP
#define HINOTETSU_INIT_CAP (1u << 14)  // 16K slots per shard
#endif

#ifndef HINOTETSU_SLAB_MIN_SHIFT
#define HINOTETSU_SLAB_MIN_SHIFT 6u   // 64B
#endif

#ifndef HINOTETSU_SLAB_MAX_SHIFT
#define HINOTETSU_SLAB_MAX_SHIFT 12u  // 4KB
#endif

#ifndef HINOTETSU_SLAB_PAGE_SIZE
#define HINOTETSU_SLAB_PAGE_SIZE (64u * 1024u)
#endif

// Incremental resize: entries to migrate per operation
#ifndef HINOTETSU_MIGRATE_BATCH
#define HINOTETSU_MIGRATE_BATCH 16u
#endif

typedef struct Hinotetsu Hinotetsu;

typedef struct HinotetsuStats {
  size_t count;
  size_t memory_used;
  size_t pool_size;
  size_t hits;
  size_t misses;
  size_t resize_in_progress;  // number of shards currently resizing
  size_t bloom_bits;
  double bloom_fill_rate;
  int mode;
} HinotetsuStats;

// Core API (thread-safe with locks)
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

// Lock-free API (single-threaded use only)
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

// Compatibility
void hinotetsu_lock(Hinotetsu* db);
void hinotetsu_unlock(Hinotetsu* db);

#ifdef __cplusplus
}
#endif