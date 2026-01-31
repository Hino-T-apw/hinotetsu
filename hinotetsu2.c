// hinotetsu2.c
// High-concurrency sharded KV store tuned for mixed throughput.
// - Per-shard open addressing hash table (linear probe)
// - Per-shard RWLock (optional - nolock API available)
// - Per-shard bump allocator for metadata/keys/pages
// - Per-shard slab allocator for VALUES (reuse on overwrite/delete)
// License: BUSL (Business Source License)
#include "hinotetsu2.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

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

  uint8_t* pool;
  size_t pool_size;
  size_t pool_pos;

  Entry** tab;
  uint32_t cap;
  uint32_t used;
  uint32_t count;

  SlabNode* freelist[32];

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

static uint32_t shard_find_slot(Shard* s, const char* key, size_t klen, uint64_t h, int* found) {
  uint32_t cap = s->cap;
  uint32_t idx = idx_for(h, cap);
  uint32_t first_tomb = UINT32_MAX;

  for (;;) {
    Entry* cur = s->tab[idx];
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

static void shard_resize(Shard* s, uint32_t new_cap) {
  Entry** old = s->tab;
  uint32_t old_cap = s->cap;

  Entry** nt = (Entry**)calloc((size_t)new_cap, sizeof(Entry*));
  if (!nt) return;

  s->tab = nt;
  s->cap = new_cap;
  s->used = 0;

  uint32_t now = now_sec();
  uint32_t new_count = 0;

  for (uint32_t i = 0; i < old_cap; i++) {
    Entry* e = old[i];
    if (!e || e == TOMBSTONE_PTR) continue;
    if (e->deleted || is_expired(e, now)) continue;

    uint64_t h = fnv1a64(e->key, e->klen);
    uint32_t idx = idx_for(h, new_cap);
    while (nt[idx] != NULL) idx = (idx + 1u) & (new_cap - 1u);
    nt[idx] = e;
    s->used++;
    new_count++;
  }

  s->count = new_count;
  free(old);
}

static inline void shard_maybe_grow(Shard* s) {
  if (s->used + 1u <= (uint32_t)((uint64_t)s->cap * LOAD_FACTOR_NUM / LOAD_FACTOR_DEN)) return;
  uint32_t new_cap = (s->cap ? (s->cap << 1u) : HINOTETSU_INIT_CAP);
  if (new_cap < HINOTETSU_INIT_CAP) new_cap = HINOTETSU_INIT_CAP;
  shard_resize(s, new_cap);
}

// ==================== INTERNAL (no lock) ====================

static int set_internal(Shard* s, uint64_t h,
                        const char* key, size_t klen,
                        const char* value, size_t vlen,
                        uint32_t ttl_seconds) {
  shard_maybe_grow(s);

  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);

  if (found) {
    Entry* e = s->tab[idx];
    if (!e || e == TOMBSTONE_PTR) return HINOTETSU_ERR_IO;

    uint8_t new_class = VALUE_CLASS_BUMP;
    char* v = (char*)value_alloc(s, vlen, &new_class);
    if (!v) return HINOTETSU_ERR_NOMEM;
    memcpy(v, value, vlen);

    value_free(s, e->value, e->vclass);

    e->value = v;
    e->vlen = (uint32_t)vlen;
    e->vclass = new_class;
    e->deleted = 0;
    e->expire = (ttl_seconds == 0) ? 0 : (now_sec() + ttl_seconds);
    return HINOTETSU_OK;
  }

  Entry* e = entry_create_in_pool(s, key, klen, value, vlen, ttl_seconds);
  if (!e) return HINOTETSU_ERR_NOMEM;

  if (s->tab[idx] == NULL) {
    s->tab[idx] = e;
    s->used++;
  } else if (s->tab[idx] == TOMBSTONE_PTR) {
    s->tab[idx] = e;
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
  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);
  if (!found) { s->misses++; return HINOTETSU_ERR_NOTFOUND; }

  Entry* e = s->tab[idx];
  uint32_t now = now_sec();
  if (!e || e == TOMBSTONE_PTR || e->deleted || is_expired(e, now)) {
    s->misses++;
    return HINOTETSU_ERR_NOTFOUND;
  }

  size_t len = e->vlen;
  s->hits++;

  *out_vlen = len;
  if (len > dst_cap) return HINOTETSU_ERR_TOOSMALL;

  memcpy(dst, e->value, len);
  return HINOTETSU_OK;
}

static int delete_internal(Shard* s, uint64_t h, const char* key, size_t klen) {
  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);
  if (!found) return HINOTETSU_ERR_NOTFOUND;

  Entry* e = s->tab[idx];
  if (!e || e == TOMBSTONE_PTR) return HINOTETSU_ERR_NOTFOUND;

  value_free(s, e->value, e->vclass);
  e->deleted = 1;
  s->tab[idx] = TOMBSTONE_PTR;
  if (s->count) s->count--;
  return HINOTETSU_OK;
}

// ==================== PUBLIC API (with locks) ====================

Hinotetsu* hinotetsu_open(size_t pool_size_bytes) {
  if (pool_size_bytes == 0) pool_size_bytes = HINOTETSU_DEFAULT_POOL_SIZE;

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

    s->cap = HINOTETSU_INIT_CAP;
    s->tab = (Entry**)calloc((size_t)s->cap, sizeof(Entry*));
    if (!s->tab) { hinotetsu_close(db); return NULL; }

    s->used = 0;
    s->count = 0;
    s->hits = 0;
    s->misses = 0;
    memset(s->freelist, 0, sizeof(s->freelist));
  }

  return db;
}

void hinotetsu_close(Hinotetsu* db) {
  if (!db) return;
  for (uint32_t i = 0; i < HINOTETSU_SHARDS; i++) {
    Shard* s = &db->shards[i];
    if (s->tab) free(s->tab);
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

  pthread_rwlock_rdlock(&s->lock);

  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);
  if (!found) { s->misses++; pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND; }

  Entry* e = s->tab[idx];
  uint32_t now = now_sec();
  if (!e || e == TOMBSTONE_PTR || e->deleted || is_expired(e, now)) {
    s->misses++; pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND;
  }

  size_t len = e->vlen;
  const char* src = e->value;
  s->hits++;

  // Copy under lock (fixed from original)
  char* out = (char*)malloc(len);
  if (!out) { pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOMEM; }
  memcpy(out, src, len);

  pthread_rwlock_unlock(&s->lock);

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
    s->pool_pos = 0;
    s->used = 0;
    s->count = 0;
    s->hits = 0;
    s->misses = 0;
    memset(s->freelist, 0, sizeof(s->freelist));
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
    pthread_rwlock_unlock(&s->lock);
  }
}

const char* hinotetsu_version(void) {
  return HINOTETSU_VERSION_STRING;
}

// ==================== NOLOCK API (single-threaded use) ====================

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
    s->pool_pos = 0;
    s->used = 0;
    s->count = 0;
    s->hits = 0;
    s->misses = 0;
    memset(s->freelist, 0, sizeof(s->freelist));
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
  }
}

// Compatibility no-ops
void hinotetsu_lock(Hinotetsu* db) { (void)db; }
void hinotetsu_unlock(Hinotetsu* db) { (void)db; }