/*
 * Hinotetsu - Implementation
 */
#include "hinotetsu.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <sys/mman.h>

#define RED 0
#define BLACK 1
#define BLOOM_EXPAND_THRESHOLD 0.5

/* Entry */
typedef struct Entry {
    char *key;
    char *value;
    uint32_t klen;
    uint32_t vlen;
    uint32_t expire;
    uint8_t deleted;
    struct Entry *hash_next;
} Entry;

/* RBTree Node */
typedef struct RBNode {
    Entry *entry;
    int color;
    struct RBNode *left, *right, *parent;
} RBNode;

typedef struct {
    RBNode *root;
    RBNode *nil;
} RBTree;

/* Hinotetsu */
struct Hinotetsu {
    uint8_t *pool;
    size_t pool_size;
    size_t pool_pos;
    
    Entry **buckets;
    RBTree *tree;
    int mode;
    size_t count;
    size_t threshold;
    
    uint8_t *bloom;
    size_t bloom_bits;
    size_t bloom_set_bits;
    
    size_t hits;
    size_t misses;
    
    pthread_rwlock_t lock;
};

/* Hash Functions */
static inline uint32_t fnv1a(const char *key, size_t len) {
    uint32_t h = 2166136261u;
    for (size_t i = 0; i < len; i++)
        h = (h ^ (uint8_t)key[i]) * 16777619u;
    return h;
}

static inline uint32_t hash2(const char *k, size_t l) {
    uint32_t h = 0x5bd1e995;
    for (size_t i = 0; i < l; i++) h = ((h << 5) + h) ^ k[i];
    return h;
}

static inline uint32_t hash3(const char *k, size_t l) {
    uint32_t h = 0x811c9dc5;
    for (size_t i = 0; i < l; i++) h = (h * 31) + k[i];
    return h;
}

/* Bloom Filter */
static inline void bloom_set_bit(Hinotetsu *db, size_t pos) {
    size_t idx = pos >> 3;
    uint8_t mask = 1 << (pos & 7);
    if (!(db->bloom[idx] & mask)) {
        db->bloom[idx] |= mask;
        db->bloom_set_bits++;
    }
}

static inline int bloom_get_bit(Hinotetsu *db, size_t pos) {
    return db->bloom[pos >> 3] & (1 << (pos & 7));
}

static void bloom_add(Hinotetsu *db, const char *k, size_t l) {
    size_t bits = db->bloom_bits;
    bloom_set_bit(db, fnv1a(k, l) % bits);
    bloom_set_bit(db, hash2(k, l) % bits);
    bloom_set_bit(db, hash3(k, l) % bits);
}

static int bloom_maybe(Hinotetsu *db, const char *k, size_t l) {
    size_t bits = db->bloom_bits;
    return bloom_get_bit(db, fnv1a(k, l) % bits) &&
           bloom_get_bit(db, hash2(k, l) % bits) &&
           bloom_get_bit(db, hash3(k, l) % bits);
}

/* Key Comparison */
static inline int keycmp(const char *k1, size_t l1, const char *k2, size_t l2) {
    size_t min = l1 < l2 ? l1 : l2;
    int cmp = memcmp(k1, k2, min);
    if (cmp != 0) return cmp;
    return (l1 < l2) ? -1 : (l1 > l2) ? 1 : 0;
}

/* Memory Pool */
static void *pool_alloc(Hinotetsu *db, size_t size) {
    size = (size + 7) & ~7;
    if (db->pool_pos + size > db->pool_size) return NULL;
    void *p = db->pool + db->pool_pos;
    db->pool_pos += size;
    return p;
}

/* RBTree Operations */
static RBTree *rbtree_new(void) {
    RBTree *t = calloc(1, sizeof(RBTree));
    t->nil = calloc(1, sizeof(RBNode));
    t->nil->color = BLACK;
    t->root = t->nil;
    return t;
}

static void rb_left_rotate(RBTree *t, RBNode *x) {
    RBNode *y = x->right; x->right = y->left;
    if (y->left != t->nil) y->left->parent = x;
    y->parent = x->parent;
    if (x->parent == t->nil) t->root = y;
    else if (x == x->parent->left) x->parent->left = y;
    else x->parent->right = y;
    y->left = x; x->parent = y;
}

static void rb_right_rotate(RBTree *t, RBNode *x) {
    RBNode *y = x->left; x->left = y->right;
    if (y->right != t->nil) y->right->parent = x;
    y->parent = x->parent;
    if (x->parent == t->nil) t->root = y;
    else if (x == x->parent->right) x->parent->right = y;
    else x->parent->left = y;
    y->right = x; x->parent = y;
}

static void rb_insert_fixup(RBTree *t, RBNode *z) {
    while (z->parent->color == RED) {
        if (z->parent == z->parent->parent->left) {
            RBNode *y = z->parent->parent->right;
            if (y->color == RED) {
                z->parent->color = BLACK; y->color = BLACK;
                z->parent->parent->color = RED; z = z->parent->parent;
            } else {
                if (z == z->parent->right) { z = z->parent; rb_left_rotate(t, z); }
                z->parent->color = BLACK; z->parent->parent->color = RED;
                rb_right_rotate(t, z->parent->parent);
            }
        } else {
            RBNode *y = z->parent->parent->left;
            if (y->color == RED) {
                z->parent->color = BLACK; y->color = BLACK;
                z->parent->parent->color = RED; z = z->parent->parent;
            } else {
                if (z == z->parent->left) { z = z->parent; rb_right_rotate(t, z); }
                z->parent->color = BLACK; z->parent->parent->color = RED;
                rb_left_rotate(t, z->parent->parent);
            }
        }
    }
    t->root->color = BLACK;
}

static void rbtree_insert(RBTree *t, RBNode *z) {
    RBNode *y = t->nil, *x = t->root;
    while (x != t->nil) {
        y = x;
        int c = keycmp(z->entry->key, z->entry->klen, x->entry->key, x->entry->klen);
        if (c < 0) x = x->left;
        else if (c > 0) x = x->right;
        else {
            x->entry->value = z->entry->value;
            x->entry->vlen = z->entry->vlen;
            x->entry->expire = z->entry->expire;
            x->entry->deleted = 0;
            free(z); return;
        }
    }
    z->parent = y;
    if (y == t->nil) t->root = z;
    else if (keycmp(z->entry->key, z->entry->klen, y->entry->key, y->entry->klen) < 0) y->left = z;
    else y->right = z;
    z->left = z->right = t->nil; z->color = RED;
    rb_insert_fixup(t, z);
}

static RBNode *rbtree_search(RBTree *t, const char *k, size_t l) {
    RBNode *x = t->root;
    while (x != t->nil) {
        int c = keycmp(k, l, x->entry->key, x->entry->klen);
        if (c == 0) return x;
        x = (c < 0) ? x->left : x->right;
    }
    return NULL;
}

static void convert_to_rbtree(Hinotetsu *db) {
    for (size_t i = 0; i < HINOTETSU_BUCKET_COUNT; i++) {
        Entry *e = db->buckets[i];
        while (e) {
            if (!e->deleted) {
                RBNode *n = calloc(1, sizeof(RBNode));
                n->entry = e;
                rbtree_insert(db->tree, n);
            }
            e = e->hash_next;
        }
        db->buckets[i] = NULL;
    }
    db->mode = 1;
}

static int is_expired(Entry *e) {
    return e->expire && (uint32_t)time(NULL) > e->expire;
}

/* ===== Public API ===== */

const char *hinotetsu_version(void) {
    return HINOTETSU_VERSION_STRING;
}

Hinotetsu *hinotetsu_open(size_t pool_size) {
    if (pool_size == 0) pool_size = HINOTETSU_DEFAULT_POOL_SIZE;
    
    Hinotetsu *db = calloc(1, sizeof(Hinotetsu));
    if (!db) return NULL;
    
    db->pool_size = pool_size;
    db->pool = mmap(NULL, pool_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANON, -1, 0);
    if (db->pool == MAP_FAILED) {
        db->pool = malloc(pool_size);
        if (!db->pool) { free(db); return NULL; }
    }
    
    db->buckets = calloc(HINOTETSU_BUCKET_COUNT, sizeof(Entry*));
    db->tree = rbtree_new();
    db->threshold = HINOTETSU_THRESHOLD;
    
    db->bloom_bits = HINOTETSU_BLOOM_INIT_BITS;
    db->bloom = calloc(db->bloom_bits / 8, 1);
    
    pthread_rwlock_init(&db->lock, NULL);
    
    return db;
}

void hinotetsu_close(Hinotetsu *db) {
    if (!db) return;
    pthread_rwlock_destroy(&db->lock);
    free(db->buckets);
    free(db->bloom);
    free(db->tree->nil);
    free(db->tree);
    munmap(db->pool, db->pool_size);
    free(db);
}

void hinotetsu_lock(Hinotetsu *db) { pthread_rwlock_wrlock(&db->lock); }
void hinotetsu_unlock(Hinotetsu *db) { pthread_rwlock_unlock(&db->lock); }

int hinotetsu_set(Hinotetsu *db, const char *key, size_t klen, const char *value, size_t vlen, uint32_t ttl) {
    Entry *e = pool_alloc(db, sizeof(Entry));
    char *k = pool_alloc(db, klen + 1);
    char *v = pool_alloc(db, vlen + 1);
    if (!e || !k || !v) return HINOTETSU_ERR_NOMEM;
    
    memcpy(k, key, klen); k[klen] = '\0';
    memcpy(v, value, vlen); v[vlen] = '\0';
    e->key = k; e->value = v; e->klen = klen; e->vlen = vlen;
    e->expire = ttl ? (uint32_t)time(NULL) + ttl : 0;
    e->deleted = 0; e->hash_next = NULL;
    
    bloom_add(db, key, klen);
    
    if (db->mode == 0) {
        uint32_t bucket = fnv1a(key, klen) % HINOTETSU_BUCKET_COUNT;
        Entry *cur = db->buckets[bucket];
        while (cur) {
            if (cur->klen == klen && memcmp(cur->key, key, klen) == 0) {
                cur->value = v; cur->vlen = vlen; cur->expire = e->expire; cur->deleted = 0;
                return HINOTETSU_OK;
            }
            cur = cur->hash_next;
        }
        e->hash_next = db->buckets[bucket]; db->buckets[bucket] = e; db->count++;
        if (db->count >= db->threshold) convert_to_rbtree(db);
    } else {
        RBNode *n = calloc(1, sizeof(RBNode)); n->entry = e;
        rbtree_insert(db->tree, n); db->count++;
    }
    return HINOTETSU_OK;
}

int hinotetsu_set_str(Hinotetsu *db, const char *key, const char *value, uint32_t ttl) {
    return hinotetsu_set(db, key, strlen(key), value, strlen(value), ttl);
}

int hinotetsu_get(Hinotetsu *db, const char *key, size_t klen, char **value, size_t *vlen) {
    if (!bloom_maybe(db, key, klen)) { db->misses++; return HINOTETSU_ERR_NOTFOUND; }
    
    Entry *e = NULL;
    if (db->mode == 0) {
        e = db->buckets[fnv1a(key, klen) % HINOTETSU_BUCKET_COUNT];
        while (e) { if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) break; e = e->hash_next; }
    } else {
        RBNode *n = rbtree_search(db->tree, key, klen);
        if (n && !n->entry->deleted) e = n->entry;
    }
    
    if (!e || is_expired(e)) { if (e) e->deleted = 1; db->misses++; return HINOTETSU_ERR_NOTFOUND; }
    
    *value = malloc(e->vlen + 1); memcpy(*value, e->value, e->vlen + 1); *vlen = e->vlen;
    db->hits++;
    return HINOTETSU_OK;
}

char *hinotetsu_get_str(Hinotetsu *db, const char *key) {
    char *value; size_t vlen;
    return (hinotetsu_get(db, key, strlen(key), &value, &vlen) == HINOTETSU_OK) ? value : NULL;
}

int hinotetsu_delete(Hinotetsu *db, const char *key, size_t klen) {
    if (!bloom_maybe(db, key, klen)) return HINOTETSU_ERR_NOTFOUND;
    
    if (db->mode == 0) {
        Entry *e = db->buckets[fnv1a(key, klen) % HINOTETSU_BUCKET_COUNT];
        while (e) {
            if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) {
                e->deleted = 1; db->count--; return HINOTETSU_OK;
            }
            e = e->hash_next;
        }
    } else {
        RBNode *n = rbtree_search(db->tree, key, klen);
        if (n && !n->entry->deleted) { n->entry->deleted = 1; db->count--; return HINOTETSU_OK; }
    }
    return HINOTETSU_ERR_NOTFOUND;
}

int hinotetsu_delete_str(Hinotetsu *db, const char *key) {
    return hinotetsu_delete(db, key, strlen(key));
}

int hinotetsu_exists(Hinotetsu *db, const char *key, size_t klen) {
    if (!bloom_maybe(db, key, klen)) return 0;
    Entry *e = NULL;
    if (db->mode == 0) {
        e = db->buckets[fnv1a(key, klen) % HINOTETSU_BUCKET_COUNT];
        while (e) { if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) return !is_expired(e); e = e->hash_next; }
    } else {
        RBNode *n = rbtree_search(db->tree, key, klen);
        if (n && !n->entry->deleted) return !is_expired(n->entry);
    }
    return 0;
}

int hinotetsu_exists_str(Hinotetsu *db, const char *key) {
    return hinotetsu_exists(db, key, strlen(key));
}

int hinotetsu_touch(Hinotetsu *db, const char *key, size_t klen, uint32_t ttl) {
    Entry *e = NULL;
    if (db->mode == 0) {
        e = db->buckets[fnv1a(key, klen) % HINOTETSU_BUCKET_COUNT];
        while (e) { if (e->klen == klen && memcmp(e->key, key, klen) == 0 && !e->deleted) break; e = e->hash_next; }
    } else {
        RBNode *n = rbtree_search(db->tree, key, klen);
        if (n && !n->entry->deleted) e = n->entry;
    }
    if (e && !is_expired(e)) { e->expire = ttl ? (uint32_t)time(NULL) + ttl : 0; return HINOTETSU_OK; }
    return HINOTETSU_ERR_NOTFOUND;
}

void hinotetsu_stats(Hinotetsu *db, HinotetsuStats *stats) {
    stats->count = db->count;
    stats->memory_used = db->pool_pos;
    stats->pool_size = db->pool_size;
    stats->bloom_bits = db->bloom_bits;
    stats->bloom_fill_rate = (double)db->bloom_set_bits / db->bloom_bits * 100.0;
    stats->mode = db->mode;
    stats->hits = db->hits;
    stats->misses = db->misses;
}

void hinotetsu_flush(Hinotetsu *db) {
    memset(db->buckets, 0, HINOTETSU_BUCKET_COUNT * sizeof(Entry*));
    memset(db->bloom, 0, db->bloom_bits / 8);
    db->bloom_set_bits = 0; db->pool_pos = 0; db->count = 0; db->mode = 0;
    db->hits = db->misses = 0; db->tree->root = db->tree->nil;
}