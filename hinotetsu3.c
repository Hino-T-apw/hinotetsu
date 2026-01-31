// hinotetsu3.c
// Ultra-low-latency sharded KV store with incremental resize
// Key features:
// - Incremental hash table resize (migrate HINOTETSU_MIGRATE_BATCH entries per op)
// - Pre-warmed slab allocator with eager page allocation
// - Memory pre-touch on startup to avoid page faults
// License: BUSL (Business Source License)
#include "hinotetsu3.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef __linux__
#include <sys/mman.h>
#define USE_MMAP_ALLOC 1
#else
#define USE_MMAP_ALLOC 0
#endif

#define LOAD_FACTOR_NUM 7u
#define LOAD_FACTOR_DEN 10u
#define TOMBSTONE_PTR   ((Entry*)1)
#define VALUE_CLASS_BUMP 255u

// --------- slab helpers ----------
typedef struct SlabNode {
  struct SlabNode* next;
} SlabNode;

static inline uint32_t ceil_pow2_u32(uint32_t x) {
  if (x <= 1) return 1;
  x--;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  return x + 1;
}

static inline uint8_t class_for_size(size_t n) {
  uint32_t min_s = HINOTETSU_SLAB_MIN_SHIFT;
  uint32_t max_s = HINOTETSU_SLAB_MAX_SHIFT;
  if (n == 0) n = 1;
  uint32_t need = (uint32_t)n;
  uint32_t cap = ceil_pow2_u32(need);
  uint8_t shift = 0;
  while ((1u << shift) < cap) shift++;
  if (shift < min_s) shift = (uint8_t)min_s;
  if (shift > max_s) return VALUE_CLASS_BUMP;
  return shift;
}

static inline size_t class_size(uint8_t shift) {
  return (size_t)1u << shift;
}

// -------------------- data structures --------------------
typedef struct Entry {
  char* key;
  char* value;
  uint32_t klen;
  uint32_t vlen;
  uint32_t expire;
  uint8_t deleted;
  uint8_t vclass;
} Entry;

typedef struct Shard {
  pthread_rwlock_t lock;

  // Bump allocator
  uint8_t* pool;
  size_t pool_size;
  size_t pool_pos;

  // Current hash table
  Entry** tab;
  uint32_t cap;
  uint32_t used;
  uint32_t count;

  // Incremental resize state
  Entry** new_tab;       // NULL if not resizing
  uint32_t new_cap;
  uint32_t new_used;
  uint32_t migrate_pos;  // next index to migrate from old table

  // Slab freelists
  SlabNode* freelist[32];

  // Stats
  size_t hits;
  size_t misses;
} Shard;

struct Hinotetsu {
  Shard shards[HINOTETSU_SHARDS];
  size_t pool_size_total;
};

// -------------------- utils --------------------
static inline uint32_t now_sec(void) {
  return (uint32_t)time(NULL);
}

static inline int is_expired(const Entry* e, uint32_t now) {
  return (e->expire != 0 && e->expire <= now);
}

static inline uint64_t fnv1a64(const char* key, size_t len) {
  uint64_t h = 1469598103934665603ULL;
  for (size_t i = 0; i < len; i++) {
    h ^= (uint8_t)key[i];
    h *= 1099511628211ULL;
  }
  return h;
}

static inline uint32_t shard_id_for(uint64_t h) {
  return (uint32_t)(h & (HINOTETSU_SHARDS - 1u));
}

static inline uint32_t idx_for(uint64_t h, uint32_t cap) {
  return (uint32_t)(h & (uint64_t)(cap - 1u));
}

static inline void* pool_alloc(Shard* s, size_t n) {
  n = (n + 7u) & ~7u;
  if (s->pool_pos + n > s->pool_size) return NULL;
  void* p = s->pool + s->pool_pos;
  s->pool_pos += n;
  return p;
}

static inline int key_eq(const Entry* e, const char* key, size_t klen) {
  return (e && e != TOMBSTONE_PTR &&
          e->klen == (uint32_t)klen &&
          memcmp(e->key, key, klen) == 0);
}

// --------- slab allocator ----------
static inline void slab_push(Shard* s, uint8_t shift, void* p) {
  SlabNode* n = (SlabNode*)p;
  n->next = s->freelist[shift];
  s->freelist[shift] = n;
}

static void slab_refill(Shard* s, uint8_t shift) {
  size_t bsz = class_size(shift);
  size_t page = (size_t)HINOTETSU_SLAB_PAGE_SIZE;
  if (page < bsz * 8) page = bsz * 8;
  page = (page + 7u) & ~7u;

  uint8_t* mem = (uint8_t*)pool_alloc(s, page);
  if (!mem) return;

  size_t blocks = page / bsz;
  for (size_t i = 0; i < blocks; i++) {
    slab_push(s, shift, mem + i * bsz);
  }
}

// Pre-warm slab freelists
static void slab_prewarm(Shard* s) {
  for (uint8_t shift = HINOTETSU_SLAB_MIN_SHIFT; shift <= HINOTETSU_SLAB_MAX_SHIFT; shift++) {
    // Pre-allocate 4 pages per size class
    for (int i = 0; i < 4; i++) {
      slab_refill(s, shift);
    }
  }
}

static inline void* value_alloc(Shard* s, size_t n, uint8_t* out_class) {
  uint8_t shift = class_for_size(n);
  *out_class = shift;
  if (shift == VALUE_CLASS_BUMP) {
    return pool_alloc(s, n);
  }
  if (s->freelist[shift] == NULL) slab_refill(s, shift);
  SlabNode* head = s->freelist[shift];
  if (!head) return NULL;
  s->freelist[shift] = head->next;
  return (void*)head;
}

static inline void value_free(Shard* s, void* p, uint8_t vclass) {
  if (!p) return;
  if (vclass == VALUE_CLASS_BUMP) return;
  slab_push(s, vclass, p);
}

// --------- entry creation ----------
static Entry* entry_create_in_pool(Shard* s,
                                   const char* key, size_t klen,
                                   const char* val, size_t vlen,
                                   uint32_t ttl) {
  Entry* e = (Entry*)pool_alloc(s, sizeof(Entry));
  if (!e) return NULL;

  char* k = (char*)pool_alloc(s, klen);
  if (!k) return NULL;
  memcpy(k, key, klen);

  uint8_t vclass = VALUE_CLASS_BUMP;
  char* v = (char*)value_alloc(s, vlen, &vclass);
  if (!v) return NULL;
  memcpy(v, val, vlen);

  e->key = k;
  e->value = v;
  e->klen = (uint32_t)klen;
  e->vlen = (uint32_t)vlen;
  e->deleted = 0;
  e->vclass = vclass;
  e->expire = (ttl == 0) ? 0 : (now_sec() + ttl);
  return e;
}

// --------- incremental resize ----------

// Free hash table (handles both malloc and mmap)
static void free_table(Entry** tab, uint32_t cap) {
  if (!tab) return;
#if USE_MMAP_ALLOC
  munmap(tab, (size_t)cap * sizeof(Entry*));
#else
  free(tab);
#endif
}

// Insert entry into a table (used during migration)
static void table_insert(Entry** tab, uint32_t cap, Entry* e, uint32_t* used) {
  uint64_t h = fnv1a64(e->key, e->klen);
  uint32_t idx = idx_for(h, cap);
  while (tab[idx] != NULL && tab[idx] != TOMBSTONE_PTR) {
    idx = (idx + 1u) & (cap - 1u);
  }
  if (tab[idx] == NULL) (*used)++;
  tab[idx] = e;
}

// Start incremental resize
static void shard_start_resize(Shard* s) {
  if (s->new_tab) return;  // already resizing

  uint32_t new_cap = s->cap << 1u;
  if (new_cap < HINOTETSU_INIT_CAP) new_cap = HINOTETSU_INIT_CAP;

  size_t bytes = (size_t)new_cap * sizeof(Entry*);
  Entry** nt = NULL;

#if USE_MMAP_ALLOC
  // Use mmap with MAP_POPULATE to pre-fault pages
  nt = (Entry**)mmap(NULL, bytes, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
  if (nt == MAP_FAILED) nt = NULL;
#else
  nt = (Entry**)malloc(bytes);
  if (nt) memset(nt, 0, bytes);
#endif

  if (!nt) return;

  s->new_tab = nt;
  s->new_cap = new_cap;
  s->new_used = 0;
  s->migrate_pos = 0;
}

// Migrate a batch of entries from old to new table
static void shard_migrate_batch(Shard* s) {
  if (!s->new_tab) return;

  uint32_t now = now_sec();
  uint32_t migrated = 0;

  while (s->migrate_pos < s->cap && migrated < HINOTETSU_MIGRATE_BATCH) {
    Entry* e = s->tab[s->migrate_pos];
    s->migrate_pos++;

    if (!e || e == TOMBSTONE_PTR) continue;
    if (e->deleted || is_expired(e, now)) continue;

    table_insert(s->new_tab, s->new_cap, e, &s->new_used);
    migrated++;
  }

  // Check if migration complete
  if (s->migrate_pos >= s->cap) {
    uint32_t old_cap = s->cap;
    free_table(s->tab, old_cap);
    s->tab = s->new_tab;
    s->cap = s->new_cap;
    s->used = s->new_used;
    s->new_tab = NULL;
    s->new_cap = 0;
    s->new_used = 0;
    s->migrate_pos = 0;

    // Recount live entries
    uint32_t live = 0;
    for (uint32_t i = 0; i < s->cap; i++) {
      Entry* e = s->tab[i];
      if (e && e != TOMBSTONE_PTR && !e->deleted) live++;
    }
    s->count = live;
  }
}

// Check if resize needed and handle migration
static inline void shard_maybe_grow(Shard* s) {
  // Always do a migration batch if in progress
  if (s->new_tab) {
    shard_migrate_batch(s);
    return;
  }

  // Check if we need to start resize
  if (s->used + 1u > (uint32_t)((uint64_t)s->cap * LOAD_FACTOR_NUM / LOAD_FACTOR_DEN)) {
    shard_start_resize(s);
    if (s->new_tab) {
      shard_migrate_batch(s);
    }
  }
}

// Find slot - searches both old and new tables during resize
static uint32_t shard_find_slot(Shard* s, const char* key, size_t klen, uint64_t h,
                                int* found, Entry*** out_tab, uint32_t* out_cap) {
  // First check new table if resizing
  if (s->new_tab) {
    uint32_t cap = s->new_cap;
    uint32_t idx = idx_for(h, cap);
    uint32_t first_tomb = UINT32_MAX;

    for (uint32_t i = 0; i < cap; i++) {
      Entry* cur = s->new_tab[idx];
      if (cur == NULL) {
        // Not in new table, check old table
        break;
      }
      if (cur == TOMBSTONE_PTR) {
        if (first_tomb == UINT32_MAX) first_tomb = idx;
      } else if (key_eq(cur, key, klen)) {
        *found = 1;
        *out_tab = &s->new_tab;
        *out_cap = s->new_cap;
        return idx;
      }
      idx = (idx + 1u) & (cap - 1u);
    }
  }

  // Search old table
  uint32_t cap = s->cap;
  uint32_t idx = idx_for(h, cap);
  uint32_t first_tomb = UINT32_MAX;

  for (;;) {
    Entry* cur = s->tab[idx];
    if (cur == NULL) {
      *found = 0;
      *out_tab = s->new_tab ? &s->new_tab : &s->tab;
      *out_cap = s->new_tab ? s->new_cap : s->cap;
      return (first_tomb != UINT32_MAX) ? first_tomb : idx_for(h, *out_cap);
    }
    if (cur == TOMBSTONE_PTR) {
      if (first_tomb == UINT32_MAX) first_tomb = idx;
    } else if (key_eq(cur, key, klen)) {
      *found = 1;
      *out_tab = &s->tab;
      *out_cap = s->cap;
      return idx;
    }
    idx = (idx + 1u) & (cap - 1u);
  }
}

// Simplified find for insert (always into newest table)
static uint32_t find_insert_slot(Entry** tab, uint32_t cap, const char* key, size_t klen, uint64_t h, int* found) {
  uint32_t idx = idx_for(h, cap);
  uint32_t first_tomb = UINT32_MAX;

  for (;;) {
    Entry* cur = tab[idx];
    if (cur == NULL) {
      *found = 0;
      return (first_tomb != UINT32_MAX) ? first_tomb : idx;
    }
    if (cur == TOMBSTONE_PTR) {
      if (first_tomb == UINT32_MAX) first_tomb = idx;
    } else if (key_eq(cur, key, klen)) {
      *found = 1;
      return idx;
    }
    idx = (idx + 1u) & (cap - 1u);
  }
}

// ==================== INTERNAL (no lock) ====================

static int set_internal(Shard* s, uint64_t h,
                        const char* key, size_t klen,
                        const char* value, size_t vlen,
                        uint32_t ttl_seconds) {
  // Do migration work
  shard_maybe_grow(s);

  // Determine target table
  Entry** target_tab = s->new_tab ? s->new_tab : s->tab;
  uint32_t target_cap = s->new_tab ? s->new_cap : s->cap;
  uint32_t* target_used = s->new_tab ? &s->new_used : &s->used;

  // Check both tables for existing key
  int found_old = 0, found_new = 0;
  uint32_t idx_old = 0, idx_new = 0;

  if (s->new_tab) {
    idx_new = find_insert_slot(s->new_tab, s->new_cap, key, klen, h, &found_new);
  }
  idx_old = find_insert_slot(s->tab, s->cap, key, klen, h, &found_old);

  // Update existing entry if found
  Entry* existing = NULL;
  Entry** existing_tab = NULL;
  uint32_t existing_idx = 0;

  if (found_new) {
    existing = s->new_tab[idx_new];
    existing_tab = s->new_tab;
    existing_idx = idx_new;
  } else if (found_old) {
    existing = s->tab[idx_old];
    existing_tab = s->tab;
    existing_idx = idx_old;
  }

  if (existing && existing != TOMBSTONE_PTR) {
    uint8_t new_class = VALUE_CLASS_BUMP;
    char* v = (char*)value_alloc(s, vlen, &new_class);
    if (!v) return HINOTETSU_ERR_NOMEM;
    memcpy(v, value, vlen);

    value_free(s, existing->value, existing->vclass);

    existing->value = v;
    existing->vlen = (uint32_t)vlen;
    existing->vclass = new_class;
    existing->deleted = 0;
    existing->expire = (ttl_seconds == 0) ? 0 : (now_sec() + ttl_seconds);
    return HINOTETSU_OK;
  }

  // Create new entry
  Entry* e = entry_create_in_pool(s, key, klen, value, vlen, ttl_seconds);
  if (!e) return HINOTETSU_ERR_NOMEM;

  // Insert into target table
  int found = 0;
  uint32_t idx = find_insert_slot(target_tab, target_cap, key, klen, h, &found);

  if (target_tab[idx] == NULL) {
    target_tab[idx] = e;
    (*target_used)++;
  } else if (target_tab[idx] == TOMBSTONE_PTR) {
    target_tab[idx] = e;
  } else {
    return HINOTETSU_ERR_IO;
  }
  s->count++;
  return HINOTETSU_OK;
}

static int get_into_internal(Shard* s, uint64_t h,
                             const char* key, size_t klen,
                             char* dst, size_t dst_cap,
                             size_t* out_vlen) {
  // Do migration work (amortized)
  if (s->new_tab) shard_migrate_batch(s);

  // Search new table first
  Entry* e = NULL;
  uint32_t now = now_sec();

  if (s->new_tab) {
    uint32_t cap = s->new_cap;
    uint32_t idx = idx_for(h, cap);
    for (uint32_t i = 0; i < cap; i++) {
      Entry* cur = s->new_tab[idx];
      if (cur == NULL) break;
      if (cur != TOMBSTONE_PTR && key_eq(cur, key, klen)) {
        if (!cur->deleted && !is_expired(cur, now)) {
          e = cur;
        }
        break;
      }
      idx = (idx + 1u) & (cap - 1u);
    }
  }

  // Search old table if not found
  if (!e) {
    uint32_t cap = s->cap;
    uint32_t idx = idx_for(h, cap);
    for (;;) {
      Entry* cur = s->tab[idx];
      if (cur == NULL) break;
      if (cur != TOMBSTONE_PTR && key_eq(cur, key, klen)) {
        if (!cur->deleted && !is_expired(cur, now)) {
          e = cur;
        }
        break;
      }
      idx = (idx + 1u) & (cap - 1u);
    }
  }

  if (!e) {
    s->misses++;
    return HINOTETSU_ERR_NOTFOUND;
  }

  s->hits++;
  *out_vlen = e->vlen;
  if (e->vlen > dst_cap) return HINOTETSU_ERR_TOOSMALL;

  memcpy(dst, e->value, e->vlen);
  return HINOTETSU_OK;
}

static int delete_internal(Shard* s, uint64_t h, const char* key, size_t klen) {
  if (s->new_tab) shard_migrate_batch(s);

  uint32_t now = now_sec();
  Entry* e = NULL;
  Entry** tab = NULL;
  uint32_t idx = 0;

  // Search new table
  if (s->new_tab) {
    uint32_t cap = s->new_cap;
    idx = idx_for(h, cap);
    for (uint32_t i = 0; i < cap; i++) {
      Entry* cur = s->new_tab[idx];
      if (cur == NULL) break;
      if (cur != TOMBSTONE_PTR && key_eq(cur, key, klen)) {
        if (!cur->deleted && !is_expired(cur, now)) {
          e = cur;
          tab = s->new_tab;
        }
        break;
      }
      idx = (idx + 1u) & (cap - 1u);
    }
  }

  // Search old table
  if (!e) {
    uint32_t cap = s->cap;
    idx = idx_for(h, cap);
    for (;;) {
      Entry* cur = s->tab[idx];
      if (cur == NULL) break;
      if (cur != TOMBSTONE_PTR && key_eq(cur, key, klen)) {
        if (!cur->deleted && !is_expired(cur, now)) {
          e = cur;
          tab = s->tab;
        }
        break;
      }
      idx = (idx + 1u) & (cap - 1u);
    }
  }

  if (!e) return HINOTETSU_ERR_NOTFOUND;

  value_free(s, e->value, e->vclass);
  e->deleted = 1;
  tab[idx] = TOMBSTONE_PTR;
  if (s->count) s->count--;
  return HINOTETSU_OK;
}

// ==================== PUBLIC API ====================

Hinotetsu* hinotetsu_open(size_t pool_size_bytes) {
  if (pool_size_bytes == 0) pool_size_bytes = 64ULL * 1024ULL * 1024ULL;

  if ((HINOTETSU_SHARDS & (HINOTETSU_SHARDS - 1u)) != 0u) return NULL;
  if ((HINOTETSU_INIT_CAP & (HINOTETSU_INIT_CAP - 1u)) != 0u) return NULL;

  Hinotetsu* db = (Hinotetsu*)calloc(1, sizeof(Hinotetsu));
  if (!db) return NULL;

  db->pool_size_total = pool_size_bytes;

  size_t per = pool_size_bytes / (size_t)HINOTETSU_SHARDS;
  if (per < (1u << 20)) per = (1u << 20);

  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    pthread_rwlock_init(&s->lock, NULL);

    s->pool_size = per;
    s->pool_pos = 0;
    s->pool = (uint8_t*)malloc(s->pool_size);
    if (!s->pool) { hinotetsu_close(db); return NULL; }

    // Pre-touch memory to avoid page faults
    for (size_t j = 0; j < s->pool_size; j += 4096) {
      ((volatile uint8_t*)s->pool)[j] = 0;
    }

    s->cap = HINOTETSU_INIT_CAP;
    s->tab = (Entry**)calloc((size_t)s->cap, sizeof(Entry*));
    if (!s->tab) { hinotetsu_close(db); return NULL; }

    // Pre-touch hash table
    for (size_t j = 0; j < s->cap; j += 512) {
      ((volatile Entry**)s->tab)[j] = NULL;
    }

    s->used = 0;
    s->count = 0;
    s->hits = 0;
    s->misses = 0;

    s->new_tab = NULL;
    s->new_cap = 0;
    s->new_used = 0;
    s->migrate_pos = 0;

    memset(s->freelist, 0, sizeof(s->freelist));

    // Pre-warm slab allocator
    slab_prewarm(s);
  }

  return db;
}

void hinotetsu_close(Hinotetsu* db) {
  if (!db) return;
  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    free_table(s->tab, s->cap);
    free_table(s->new_tab, s->new_cap);
    if (s->pool) free(s->pool);
    pthread_rwlock_destroy(&s->lock);
  }
  free(db);
}

int hinotetsu_set(Hinotetsu* db,
                  const char* key, size_t klen,
                  const char* value, size_t vlen,
                  uint32_t ttl_seconds) {
  if (!db || !key || klen == 0) return HINOTETSU_ERR_IO;

  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  pthread_rwlock_wrlock(&s->lock);
  int ret = set_internal(s, h, key, klen, value, vlen, ttl_seconds);
  pthread_rwlock_unlock(&s->lock);
  return ret;
}

int hinotetsu_get(Hinotetsu* db,
                  const char* key, size_t klen,
                  char** out_value, size_t* out_vlen) {
  if (!db || !key || klen == 0 || !out_value || !out_vlen) return HINOTETSU_ERR_IO;

  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  char tmp[8192];
  size_t len = 0;

  pthread_rwlock_rdlock(&s->lock);
  int ret = get_into_internal(s, h, key, klen, tmp, sizeof(tmp), &len);
  pthread_rwlock_unlock(&s->lock);

  if (ret == HINOTETSU_ERR_TOOSMALL) {
    char* buf = (char*)malloc(len);
    if (!buf) return HINOTETSU_ERR_NOMEM;

    pthread_rwlock_rdlock(&s->lock);
    ret = get_into_internal(s, h, key, klen, buf, len, &len);
    pthread_rwlock_unlock(&s->lock);

    if (ret != HINOTETSU_OK) { free(buf); return ret; }
    *out_value = buf;
    *out_vlen = len;
    return HINOTETSU_OK;
  }

  if (ret != HINOTETSU_OK) return ret;

  char* out = (char*)malloc(len);
  if (!out) return HINOTETSU_ERR_NOMEM;
  memcpy(out, tmp, len);
  *out_value = out;
  *out_vlen = len;
  return HINOTETSU_OK;
}

int hinotetsu_get_into(Hinotetsu* db,
                       const char* key, size_t klen,
                       char* dst, size_t dst_cap,
                       size_t* out_vlen) {
  if (!db || !key || klen == 0 || !dst || !out_vlen) return HINOTETSU_ERR_IO;

  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  pthread_rwlock_rdlock(&s->lock);
  int ret = get_into_internal(s, h, key, klen, dst, dst_cap, out_vlen);
  pthread_rwlock_unlock(&s->lock);
  return ret;
}

int hinotetsu_delete(Hinotetsu* db, const char* key, size_t klen) {
  if (!db || !key || klen == 0) return HINOTETSU_ERR_IO;

  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  pthread_rwlock_wrlock(&s->lock);
  int ret = delete_internal(s, h, key, klen);
  pthread_rwlock_unlock(&s->lock);
  return ret;
}

void hinotetsu_flush(Hinotetsu* db) {
  if (!db) return;
  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    pthread_rwlock_wrlock(&s->lock);

    memset(s->tab, 0, (size_t)s->cap * sizeof(Entry*));
    if (s->new_tab) {
      free_table(s->new_tab, s->new_cap);
      s->new_tab = NULL;
    }
    s->new_cap = 0;
    s->new_used = 0;
    s->migrate_pos = 0;
    s->pool_pos = 0;
    s->used = 0;
    s->count = 0;
    s->hits = 0;
    s->misses = 0;
    memset(s->freelist, 0, sizeof(s->freelist));
    slab_prewarm(s);

    pthread_rwlock_unlock(&s->lock);
  }
}

void hinotetsu_stats(Hinotetsu* db, HinotetsuStats* out) {
  if (!db || !out) return;
  memset(out, 0, sizeof(*out));
  out->pool_size = db->pool_size_total;
  out->mode = 0;

  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    pthread_rwlock_rdlock(&s->lock);
    out->count += s->count;
    out->memory_used += s->pool_pos;
    out->hits += s->hits;
    out->misses += s->misses;
    if (s->new_tab) out->resize_in_progress++;
    pthread_rwlock_unlock(&s->lock);
  }
}

const char* hinotetsu_version(void) {
  return HINOTETSU_VERSION_STRING;
}

// ==================== NOLOCK API ====================

int hinotetsu_set_nolock(Hinotetsu* db,
                         const char* key, size_t klen,
                         const char* value, size_t vlen,
                         uint32_t ttl_seconds) {
  if (!db || !key || klen == 0) return HINOTETSU_ERR_IO;
  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];
  return set_internal(s, h, key, klen, value, vlen, ttl_seconds);
}

int hinotetsu_get_into_nolock(Hinotetsu* db,
                              const char* key, size_t klen,
                              char* dst, size_t dst_cap,
                              size_t* out_vlen) {
  if (!db || !key || klen == 0 || !dst || !out_vlen) return HINOTETSU_ERR_IO;
  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];
  return get_into_internal(s, h, key, klen, dst, dst_cap, out_vlen);
}

int hinotetsu_delete_nolock(Hinotetsu* db, const char* key, size_t klen) {
  if (!db || !key || klen == 0) return HINOTETSU_ERR_IO;
  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];
  return delete_internal(s, h, key, klen);
}

void hinotetsu_flush_nolock(Hinotetsu* db) {
  if (!db) return;
  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    memset(s->tab, 0, (size_t)s->cap * sizeof(Entry*));
    if (s->new_tab) { free_table(s->new_tab, s->new_cap); s->new_tab = NULL; }
    s->new_cap = 0;
    s->new_used = 0;
    s->migrate_pos = 0;
    s->pool_pos = 0;
    s->used = 0;
    s->count = 0;
    s->hits = 0;
    s->misses = 0;
    memset(s->freelist, 0, sizeof(s->freelist));
    slab_prewarm(s);
  }
}

void hinotetsu_stats_nolock(Hinotetsu* db, HinotetsuStats* out) {
  if (!db || !out) return;
  memset(out, 0, sizeof(*out));
  out->pool_size = db->pool_size_total;
  out->mode = 0;

  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    out->count += s->count;
    out->memory_used += s->pool_pos;
    out->hits += s->hits;
    out->misses += s->misses;
    if (s->new_tab) out->resize_in_progress++;
  }
}

void hinotetsu_lock(Hinotetsu* db) { (void)db; }
void hinotetsu_unlock(Hinotetsu* db) { (void)db; }