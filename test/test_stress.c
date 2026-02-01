// test_stress.c
// Stress tests for Hinotetsu
// Inspired by memcached's stress/load tests

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include "test_helper.h"
#include "../hinotetsu3.h"

static Hinotetsu* db = NULL;

// Test: Insert many keys
int test_many_keys(void) {
    TEST_START("many_keys");

    const int NUM_KEYS = 10000;
    char key[32];
    char value[64];

    hinotetsu_flush(db);

    long long start = current_time_ms();

    for (int i = 0; i < NUM_KEYS; i++) {
        snprintf(key, sizeof(key), "stress_key_%d", i);
        snprintf(value, sizeof(value), "stress_value_%d_%d", i, i * 2);
        int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
        TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");
    }

    long long elapsed = current_time_ms() - start;
    printf("  Inserted %d keys in %lld ms (%.0f ops/sec)\n",
           NUM_KEYS, elapsed, NUM_KEYS * 1000.0 / elapsed);

    // Verify count
    HinotetsuStats stats;
    hinotetsu_stats(db, &stats);
    TEST_ASSERT_EQ(NUM_KEYS, stats.count, "Count should match inserted keys");

    // Verify random keys
    for (int i = 0; i < 100; i++) {
        int idx = rand() % NUM_KEYS;
        snprintf(key, sizeof(key), "stress_key_%d", idx);
        snprintf(value, sizeof(value), "stress_value_%d_%d", idx, idx * 2);

        char* out = NULL;
        size_t len = 0;
        int ret = hinotetsu_get(db, key, strlen(key), &out, &len);
        TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET should return OK");
        TEST_ASSERT_STR_EQ(value, out, len, "Value should match");
        free(out);
    }

    TEST_PASS();
}

// Test: Read performance
int test_read_performance(void) {
    TEST_START("read_performance");

    const int NUM_READS = 50000;
    char key[32];
    char buf[128];
    size_t len;

    // Prepare some keys
    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "read_key_%d", i);
        hinotetsu_set(db, key, strlen(key), "read_value", 10, 0);
    }

    long long start = current_time_ms();

    for (int i = 0; i < NUM_READS; i++) {
        int idx = i % 1000;
        snprintf(key, sizeof(key), "read_key_%d", idx);
        hinotetsu_get_into(db, key, strlen(key), buf, sizeof(buf), &len);
    }

    long long elapsed = current_time_ms() - start;
    printf("  Performed %d reads in %lld ms (%.0f ops/sec)\n",
           NUM_READS, elapsed, NUM_READS * 1000.0 / elapsed);

    TEST_PASS();
}

// Test: Mixed workload (read/write)
int test_mixed_workload(void) {
    TEST_START("mixed_workload");

    const int NUM_OPS = 20000;
    const int READ_RATIO = 80;  // 80% reads, 20% writes
    char key[32];
    char value[64];
    char buf[128];
    size_t len;

    hinotetsu_flush(db);

    // Seed with some keys
    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "mixed_%d", i);
        snprintf(value, sizeof(value), "init_value_%d", i);
        hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    }

    int reads = 0, writes = 0;
    long long start = current_time_ms();

    for (int i = 0; i < NUM_OPS; i++) {
        int idx = rand() % 1000;
        snprintf(key, sizeof(key), "mixed_%d", idx);

        if (rand() % 100 < READ_RATIO) {
            hinotetsu_get_into(db, key, strlen(key), buf, sizeof(buf), &len);
            reads++;
        } else {
            snprintf(value, sizeof(value), "updated_value_%d_%d", idx, i);
            hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
            writes++;
        }
    }

    long long elapsed = current_time_ms() - start;
    printf("  Performed %d ops (%d reads, %d writes) in %lld ms (%.0f ops/sec)\n",
           NUM_OPS, reads, writes, elapsed, NUM_OPS * 1000.0 / elapsed);

    TEST_PASS();
}

// Thread function for concurrent test
typedef struct {
    int thread_id;
    int num_ops;
    int errors;
} ThreadArg;

static void* thread_worker(void* arg) {
    ThreadArg* ta = (ThreadArg*)arg;
    char key[32];
    char value[64];
    char buf[128];
    size_t len;

    for (int i = 0; i < ta->num_ops; i++) {
        int idx = (ta->thread_id * ta->num_ops + i) % 10000;
        snprintf(key, sizeof(key), "concurrent_%d", idx);

        if (i % 2 == 0) {
            snprintf(value, sizeof(value), "value_%d_%d", ta->thread_id, i);
            int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
            if (ret != HINOTETSU_OK) ta->errors++;
        } else {
            hinotetsu_get_into(db, key, strlen(key), buf, sizeof(buf), &len);
            // GET may fail (key not yet set), that's OK
        }
    }

    return NULL;
}

// Test: Concurrent access (multi-threaded)
int test_concurrent_access(void) {
    TEST_START("concurrent_access");

    const int NUM_THREADS = 4;
    const int OPS_PER_THREAD = 5000;

    pthread_t threads[NUM_THREADS];
    ThreadArg args[NUM_THREADS];

    hinotetsu_flush(db);

    long long start = current_time_ms();

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].thread_id = i;
        args[i].num_ops = OPS_PER_THREAD;
        args[i].errors = 0;
        pthread_create(&threads[i], NULL, thread_worker, &args[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    long long elapsed = current_time_ms() - start;
    int total_ops = NUM_THREADS * OPS_PER_THREAD;
    int total_errors = 0;
    for (int i = 0; i < NUM_THREADS; i++) {
        total_errors += args[i].errors;
    }

    printf("  %d threads, %d ops total in %lld ms (%.0f ops/sec)\n",
           NUM_THREADS, total_ops, elapsed, total_ops * 1000.0 / elapsed);
    printf("  Errors: %d\n", total_errors);

    TEST_ASSERT_EQ(0, total_errors, "Should have no errors");

    TEST_PASS();
}

// Test: Delete stress
int test_delete_stress(void) {
    TEST_START("delete_stress");

    const int NUM_KEYS = 5000;
    char key[32];

    hinotetsu_flush(db);

    // Insert keys
    for (int i = 0; i < NUM_KEYS; i++) {
        snprintf(key, sizeof(key), "delete_stress_%d", i);
        hinotetsu_set(db, key, strlen(key), "value", 5, 0);
    }

    HinotetsuStats stats;
    hinotetsu_stats(db, &stats);
    TEST_ASSERT_EQ(NUM_KEYS, stats.count, "All keys should be inserted");

    // Delete all keys
    long long start = current_time_ms();
    for (int i = 0; i < NUM_KEYS; i++) {
        snprintf(key, sizeof(key), "delete_stress_%d", i);
        int ret = hinotetsu_delete(db, key, strlen(key));
        TEST_ASSERT_EQ(HINOTETSU_OK, ret, "DELETE should return OK");
    }
    long long elapsed = current_time_ms() - start;

    printf("  Deleted %d keys in %lld ms (%.0f ops/sec)\n",
           NUM_KEYS, elapsed, NUM_KEYS * 1000.0 / elapsed);

    hinotetsu_stats(db, &stats);
    TEST_ASSERT_EQ(0, stats.count, "All keys should be deleted");

    TEST_PASS();
}

// Test: Value size variations
int test_value_sizes(void) {
    TEST_START("value_sizes");

    const int sizes[] = {16, 64, 256, 1024, 4096};
    const int num_sizes = sizeof(sizes) / sizeof(sizes[0]);
    char key[32];

    hinotetsu_flush(db);

    for (int s = 0; s < num_sizes; s++) {
        int vlen = sizes[s];
        char* value = malloc(vlen);
        random_string(value, vlen);

        long long start = current_time_ms();
        int count = 1000;

        for (int i = 0; i < count; i++) {
            snprintf(key, sizeof(key), "size_%d_%d", vlen, i);
            int ret = hinotetsu_set(db, key, strlen(key), value, vlen - 1, 0);
            TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");
        }

        long long elapsed = current_time_ms() - start;
        printf("  %4d-byte values: %d ops in %lld ms (%.0f ops/sec)\n",
               vlen, count, elapsed, count * 1000.0 / elapsed);

        free(value);
    }

    TEST_PASS();
}

// Test: Key collision (same hash bucket stress)
int test_key_patterns(void) {
    TEST_START("key_patterns");

    hinotetsu_flush(db);

    // Sequential keys
    for (int i = 0; i < 1000; i++) {
        char key[32];
        snprintf(key, sizeof(key), "seq_%08d", i);
        hinotetsu_set(db, key, strlen(key), "value", 5, 0);
    }

    // Random-looking keys
    for (int i = 0; i < 1000; i++) {
        char key[32];
        snprintf(key, sizeof(key), "rnd_%08x", rand());
        hinotetsu_set(db, key, strlen(key), "value", 5, 0);
    }

    // UUID-like keys
    for (int i = 0; i < 1000; i++) {
        char key[48];
        snprintf(key, sizeof(key), "%08x-%04x-%04x-%04x-%08x%04x",
                rand(), rand() & 0xffff, rand() & 0xffff,
                rand() & 0xffff, rand(), rand() & 0xffff);
        hinotetsu_set(db, key, strlen(key), "value", 5, 0);
    }

    HinotetsuStats stats;
    hinotetsu_stats(db, &stats);
    printf("  Total keys: %zu, memory: %zu bytes\n", stats.count, stats.memory_used);

    TEST_PASS();
}

int main(void) {
    printf("Hinotetsu Stress Tests\n");
    printf("========================================\n");

    srand((unsigned)time(NULL));

    // Open database with larger pool (256MB for 64 shards + slab pre-warming)
    db = hinotetsu_open(256 * 1024 * 1024);  // 256MB
    if (!db) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    // Run tests
    RUN_TEST(test_many_keys);
    RUN_TEST(test_read_performance);
    RUN_TEST(test_mixed_workload);
    RUN_TEST(test_concurrent_access);
    RUN_TEST(test_delete_stress);
    RUN_TEST(test_value_sizes);
    RUN_TEST(test_key_patterns);

    hinotetsu_close(db);

    TEST_SUMMARY();
}
