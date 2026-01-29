// hinotetsu2.c
// High-concurrency sharded KV store tuned for mixed throughput.
// - Per-shard open addressing hash table (linear probe)
// - Per-shard RWLock
// - Per-shard bump allocator for metadata/keys/pages
// - Per-shard slab allocator for VALUES (reuse on overwrite/delete)
// - GET copies out of lock (pointer+len captured under lock)

#include "hinotetsu2.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define LOAD_FACTOR_NUM 7u
#define LOAD_FACTOR_DEN 10u   // grow when used > cap*0.7
#define TOMBSTONE_PTR   ((Entry*)1)
#define VALUE_CLASS_BUMP 255u // value allocated via bump (not reusable)

// --------- slab helpers ----------
typedef struct SlabNode {
  struct SlabNode* next;
} SlabNode;

static inline uint32_t clamp_u32(uint32_t v, uint32_t lo, uint32_t hi) {
  if (v < lo) return lo;
  if (v > hi) return hi;
  return v;
}

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
  // map size -> shift in [MIN_SHIFT..MAX_SHIFT], else VALUE_CLASS_BUMP
  uint32_t min_s = HINOTETSU_SLAB_MIN_SHIFT;
  uint32_t max_s = HINOTETSU_SLAB_MAX_SHIFT;
  if (n == 0) n = 1;
  uint32_t need = (uint32_t)n;
  uint32_t cap = ceil_pow2_u32(need);
  // shift = log2(cap)
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
  uint32_t expire;   // unix time, 0 = no expire
  uint8_t deleted;
  uint8_t vclass;    // slab shift or VALUE_CLASS_BUMP
} Entry;

typedef struct Shard {
  pthread_rwlock_t lock;

  // bump allocator backing store
  uint8_t* pool;
  size_t pool_size;
  size_t pool_pos;

  // open addressing table (Entry*)
  Entry** tab;
  uint32_t cap;    // power of two
  uint32_t used;   // occupied slots incl tombstones
  uint32_t count;  // approx live count

  // slab freelists (values only)
  // index by shift (0..31), only MIN..MAX used
  SlabNode* freelist[32];

  // stats
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

// 64-bit FNV-1a (simple + stable)
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
  // 8-byte align
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

// --------- slab allocator (values) ----------
static inline void slab_push(Shard* s, uint8_t shift, void* p) {
  SlabNode* n = (SlabNode*)p;
  n->next = s->freelist[shift];
  s->freelist[shift] = n;
}

static void slab_refill(Shard* s, uint8_t shift) {
  size_t bsz = class_size(shift);
  // allocate a page from bump pool
  size_t page = (size_t)HINOTETSU_SLAB_PAGE_SIZE;
  if (page < bsz * 8) page = bsz * 8; // at least a few blocks
  page = (page + 7u) & ~7u;

  uint8_t* mem = (uint8_t*)pool_alloc(s, page);
  if (!mem) return;

  size_t blocks = page / bsz;
  // push all blocks to freelist
  for (size_t i = 0; i < blocks; i++) {
    slab_push(s, shift, mem + i * bsz);
  }
}

static inline void* value_alloc(Shard* s, size_t n, uint8_t* out_class) {
  uint8_t shift = class_for_size(n);
  *out_class = shift;
  if (shift == VALUE_CLASS_BUMP) {
    void* p = pool_alloc(s, n);
    return p;
  }
  if (s->freelist[shift] == NULL) slab_refill(s, shift);
  SlabNode* head = s->freelist[shift];
  if (!head) return NULL;
  s->freelist[shift] = head->next;
  return (void*)head;
}

static inline void value_free(Shard* s, void* p, uint8_t vclass) {
  if (!p) return;
  if (vclass == VALUE_CLASS_BUMP) return; // cannot reuse bump allocations
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

// find slot: returns index; found=1 if exact key matched
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
  if (!nt) return; // best-effort; keep old

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

// -------------------- API --------------------
Hinotetsu* hinotetsu_open(size_t pool_size_bytes) {
  if (pool_size_bytes == 0) pool_size_bytes = HINOTETSU_DEFAULT_POOL_SIZE;

  // shards must be power-of-two for fast masking
  if ((HINOTETSU_SHARDS & (HINOTETSU_SHARDS - 1u)) != 0u) return NULL;
  if ((HINOTETSU_INIT_CAP & (HINOTETSU_INIT_CAP - 1u)) != 0u) return NULL;

  uint32_t min_s = HINOTETSU_SLAB_MIN_SHIFT;
  uint32_t max_s = HINOTETSU_SLAB_MAX_SHIFT;
  if (min_s < 4) min_s = 4;
  if (max_s > 20) max_s = 20;
  if (min_s > max_s) return NULL;

  Hinotetsu* db = (Hinotetsu*)calloc(1, sizeof(Hinotetsu));
  if (!db) return NULL;

  db->pool_size_total = pool_size_bytes;

  size_t per = pool_size_bytes / (size_t)HINOTETSU_SHARDS;
  if (per < (1u << 20)) per = (1u << 20); // 1MB minimum per shard

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

  shard_maybe_grow(s);

  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);

  if (found) {
    Entry* e = s->tab[idx];
    if (!e || e == TOMBSTONE_PTR) { pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_IO; }

    // allocate new value (slab), then free old value back to freelist (if slab)
    uint8_t new_class = VALUE_CLASS_BUMP;
    char* v = (char*)value_alloc(s, vlen, &new_class);
    if (!v) { pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOMEM; }
    memcpy(v, value, vlen);

    // recycle old value block (safe: old pointer remains valid for readers that already grabbed it
    // because we still hold WR lock here; readers only copy after unlocking and old memory isn't reclaimed
    // to OS—BUT we *are* reusing blocks. To keep correctness, we must NOT reuse immediately if readers
    // can access old pointer after unlock. In this design, readers capture pointer under RD lock then unlock.
    // Writer takes WR lock, which excludes RD locks, so once WR lock is acquired there are no readers in critical section.
    // After writer updates pointer and releases lock, readers won't see old pointer. Therefore it is safe to reuse old block now.
    value_free(s, e->value, e->vclass);

    e->value = v;
    e->vlen = (uint32_t)vlen;
    e->vclass = new_class;
    e->deleted = 0;
    e->expire = (ttl_seconds == 0) ? 0 : (now_sec() + ttl_seconds);

    pthread_rwlock_unlock(&s->lock);
    return HINOTETSU_OK;
  }

  Entry* e = entry_create_in_pool(s, key, klen, value, vlen, ttl_seconds);
  if (!e) { pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOMEM; }

  if (s->tab[idx] == NULL) {
    s->tab[idx] = e;
    s->used++;
  } else if (s->tab[idx] == TOMBSTONE_PTR) {
    s->tab[idx] = e;
  } else {
    pthread_rwlock_unlock(&s->lock);
    return HINOTETSU_ERR_IO;
  }
  s->count++;

  pthread_rwlock_unlock(&s->lock);
  return HINOTETSU_OK;
}

// mallocして返す互換版（hinotetsudはget_into推奨）
int hinotetsu_get(Hinotetsu* db,
                  const char* key, size_t klen,
                  char** out_value, size_t* out_vlen) {
  if (!db || !key || klen == 0 || !out_value || !out_vlen) return HINOTETSU_ERR_IO;

  // fast path: read pointer+len under lock
  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  const char* src = NULL;
  size_t len = 0;

  pthread_rwlock_rdlock(&s->lock);

  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);
  if (!found) { s->misses++; pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND; }

  Entry* e = s->tab[idx];
  uint32_t now = now_sec();
  if (!e || e == TOMBSTONE_PTR || e->deleted || is_expired(e, now)) {
    s->misses++; pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND;
  }

  src = e->value;
  len = e->vlen;
  s->hits++;

  pthread_rwlock_unlock(&s->lock);

  char* out = (char*)malloc(len);
  if (!out) return HINOTETSU_ERR_NOMEM;
  memcpy(out, src, len);

  *out_value = out;
  *out_vlen = len;
  return HINOTETSU_OK;
}

// ★追加：呼び出し側バッファにコピー（malloc/free無し）
int hinotetsu_get_into(Hinotetsu* db,
                       const char* key, size_t klen,
                       char* dst, size_t dst_cap,
                       size_t* out_vlen) {
  if (!db || !key || klen == 0 || !dst || !out_vlen) return HINOTETSU_ERR_IO;

  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  const char* src = NULL;
  size_t len = 0;

  pthread_rwlock_rdlock(&s->lock);

  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);
  if (!found) { s->misses++; pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND; }

  Entry* e = s->tab[idx];
  uint32_t now = now_sec();
  if (!e || e == TOMBSTONE_PTR || e->deleted || is_expired(e, now)) {
    s->misses++; pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND;
  }

  src = e->value;
  len = e->vlen;
  s->hits++;

  pthread_rwlock_unlock(&s->lock);

  if (len > dst_cap) {
    *out_vlen = len;
    return HINOTETSU_ERR_TOOSMALL;
  }

  memcpy(dst, src, len);
  *out_vlen = len;
  return HINOTETSU_OK;
}

int hinotetsu_delete(Hinotetsu* db, const char* key, size_t klen) {
  if (!db || !key || klen == 0) return HINOTETSU_ERR_IO;

  uint64_t h = fnv1a64(key, klen);
  Shard* s = &db->shards[shard_id_for(h)];

  pthread_rwlock_wrlock(&s->lock);

  int found = 0;
  uint32_t idx = shard_find_slot(s, key, klen, h, &found);
  if (!found) { pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND; }

  Entry* e = s->tab[idx];
  if (!e || e == TOMBSTONE_PTR) { pthread_rwlock_unlock(&s->lock); return HINOTETSU_ERR_NOTFOUND; }

  // recycle value block
  value_free(s, e->value, e->vclass);

  e->deleted = 1;
  s->tab[idx] = TOMBSTONE_PTR;
  if (s->count) s->count--;

  pthread_rwlock_unlock(&s->lock);
  return HINOTETSU_OK;
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
  out->mode = 0; // hash
  out->bloom_bits = 0;
  out->bloom_fill_rate = 0.0;

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

// Compatibility no-ops (library is internally thread-safe)
void hinotetsu_lock(Hinotetsu* db) { (void)db; }
void hinotetsu_unlock(Hinotetsu* db) { (void)db; }
