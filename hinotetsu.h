/*
 * Hinotetsu - High Performance Key-Value Store Library
 * 
 * Features:
 *   - Hybrid Hash/RBTree auto-switching
 *   - Dynamic Bloom filter
 *   - Memory pool allocation
 *   - TTL support
 *   - Thread-safe
 * 
 * License: MIT
 */
#ifndef HINOTETSU_H
#define HINOTETSU_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Version */
#define HINOTETSU_VERSION_MAJOR 1
#define HINOTETSU_VERSION_MINOR 0
#define HINOTETSU_VERSION_PATCH 0
#define HINOTETSU_VERSION_STRING "1.0.0"

/* Configuration */
#define HINOTETSU_DEFAULT_POOL_SIZE    (256 * 1024 * 1024)
#define HINOTETSU_BUCKET_COUNT         (256 * 1024)
#define HINOTETSU_THRESHOLD            (HINOTETSU_BUCKET_COUNT * 4)
#define HINOTETSU_BLOOM_INIT_BITS      (1 << 20)
#define HINOTETSU_BLOOM_MAX_BITS       (1 << 26)

/* Error codes */
typedef enum {
    HINOTETSU_OK = 0,
    HINOTETSU_ERR_NOMEM = -1,
    HINOTETSU_ERR_NOTFOUND = -2,
    HINOTETSU_ERR_FULL = -3,
    HINOTETSU_ERR_IO = -4
} HinotetsuError;

/* Opaque handle */
typedef struct Hinotetsu Hinotetsu;

/* Statistics */
typedef struct {
    size_t count;
    size_t memory_used;
    size_t pool_size;
    size_t bloom_bits;
    double bloom_fill_rate;
    int mode;  /* 0=Hash, 1=RBTree */
    size_t hits;
    size_t misses;
} HinotetsuStats;

/* ===== Core API ===== */

/**
 * Create a new Hinotetsu instance
 * @param pool_size Memory pool size in bytes (0 for default 256MB)
 * @return Handle or NULL on error
 */
Hinotetsu *hinotetsu_open(size_t pool_size);

/**
 * Close and free Hinotetsu instance
 */
void hinotetsu_close(Hinotetsu *db);

/**
 * Store a key-value pair
 * @param ttl Time-to-live in seconds (0 = no expiration)
 */
int hinotetsu_set(Hinotetsu *db, const char *key, size_t klen,
                  const char *value, size_t vlen, uint32_t ttl);

/**
 * Retrieve a value
 * @param value Output: allocated buffer (caller must free)
 * @param vlen Output: value length
 */
int hinotetsu_get(Hinotetsu *db, const char *key, size_t klen,
                  char **value, size_t *vlen);

/**
 * Delete a key
 */
int hinotetsu_delete(Hinotetsu *db, const char *key, size_t klen);

/**
 * Check if key exists
 */
int hinotetsu_exists(Hinotetsu *db, const char *key, size_t klen);

/* ===== Convenience API (null-terminated strings) ===== */

int hinotetsu_set_str(Hinotetsu *db, const char *key, const char *value, uint32_t ttl);
char *hinotetsu_get_str(Hinotetsu *db, const char *key);
int hinotetsu_delete_str(Hinotetsu *db, const char *key);
int hinotetsu_exists_str(Hinotetsu *db, const char *key);

/* ===== TTL / Expiration ===== */

/**
 * Update TTL for existing key
 */
int hinotetsu_touch(Hinotetsu *db, const char *key, size_t klen, uint32_t ttl);

/* ===== Management ===== */

/**
 * Get statistics
 */
void hinotetsu_stats(Hinotetsu *db, HinotetsuStats *stats);

/**
 * Clear all data
 */
void hinotetsu_flush(Hinotetsu *db);

/**
 * Get version string
 */
const char *hinotetsu_version(void);

/* ===== Thread Safety ===== */

void hinotetsu_lock(Hinotetsu *db);
void hinotetsu_unlock(Hinotetsu *db);

#ifdef __cplusplus
}
#endif

#endif /* HINOTETSU_H */