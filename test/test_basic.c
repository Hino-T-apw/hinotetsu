// test_basic.c
// Basic SET/GET/DELETE tests for Hinotetsu
// Inspired by memcached's basic test suite

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "test_helper.h"
#include "../hinotetsu3.h"

static Hinotetsu* db = NULL;

// Test: Open and close database
int test_open_close(void) {
    TEST_START("open_close");

    Hinotetsu* test_db = hinotetsu_open(256 * 1024 * 1024);  // 256MB (needs 64 shards × 1MB min)
    TEST_ASSERT(test_db != NULL, "hinotetsu_open should return non-NULL");

    hinotetsu_close(test_db);
    TEST_PASS();
}

// Test: Version string
int test_version(void) {
    TEST_START("version");

    const char* ver = hinotetsu_version();
    TEST_ASSERT(ver != NULL, "version should not be NULL");
    TEST_ASSERT(strlen(ver) > 0, "version string should not be empty");
    printf("  Version: %s\n", ver);

    TEST_PASS();
}

// Test: Simple SET and GET
int test_set_get_simple(void) {
    TEST_START("set_get_simple");

    const char* key = "testkey";
    const char* value = "testvalue";

    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");

    char* out_value = NULL;
    size_t out_len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET should return OK");
    TEST_ASSERT_EQ(strlen(value), out_len, "Value length should match");
    TEST_ASSERT_STR_EQ(value, out_value, out_len, "Value content should match");

    free(out_value);
    TEST_PASS();
}

// Test: SET overwrites existing key
int test_set_overwrite(void) {
    TEST_START("set_overwrite");

    const char* key = "overwrite_key";
    const char* value1 = "first_value";
    const char* value2 = "second_value_longer";

    int ret = hinotetsu_set(db, key, strlen(key), value1, strlen(value1), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "First SET should return OK");

    ret = hinotetsu_set(db, key, strlen(key), value2, strlen(value2), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "Second SET should return OK");

    char* out_value = NULL;
    size_t out_len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET should return OK");
    TEST_ASSERT_EQ(strlen(value2), out_len, "Value length should match new value");
    TEST_ASSERT_STR_EQ(value2, out_value, out_len, "Value should be overwritten");

    free(out_value);
    TEST_PASS();
}

// Test: GET non-existent key
int test_get_notfound(void) {
    TEST_START("get_notfound");

    char* out_value = NULL;
    size_t out_len = 0;
    int ret = hinotetsu_get(db, "nonexistent_key_12345", 21, &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "GET should return NOTFOUND");
    TEST_ASSERT(out_value == NULL, "Value should be NULL on not found");

    TEST_PASS();
}

// Test: DELETE existing key
int test_delete(void) {
    TEST_START("delete");

    const char* key = "delete_test_key";
    const char* value = "delete_test_value";

    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");

    ret = hinotetsu_delete(db, key, strlen(key));
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "DELETE should return OK");

    char* out_value = NULL;
    size_t out_len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "GET after DELETE should return NOTFOUND");

    TEST_PASS();
}

// Test: DELETE non-existent key
int test_delete_notfound(void) {
    TEST_START("delete_notfound");

    int ret = hinotetsu_delete(db, "nonexistent_delete_key", 22);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "DELETE non-existent should return NOTFOUND");

    TEST_PASS();
}

// Test: hinotetsu_get_into (buffer copy API)
int test_get_into(void) {
    TEST_START("get_into");

    const char* key = "getinto_key";
    const char* value = "getinto_value_data";

    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");

    char buffer[64];
    size_t out_len = 0;
    ret = hinotetsu_get_into(db, key, strlen(key), buffer, sizeof(buffer), &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET_INTO should return OK");
    TEST_ASSERT_EQ(strlen(value), out_len, "Value length should match");
    TEST_ASSERT_STR_EQ(value, buffer, out_len, "Value content should match");

    TEST_PASS();
}

// Test: hinotetsu_get_into with small buffer
int test_get_into_toosmall(void) {
    TEST_START("get_into_toosmall");

    const char* key = "toosmall_key";
    const char* value = "this_is_a_longer_value_that_wont_fit";

    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");

    char buffer[10];  // Too small
    size_t out_len = 0;
    ret = hinotetsu_get_into(db, key, strlen(key), buffer, sizeof(buffer), &out_len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_TOOSMALL, ret, "GET_INTO should return TOOSMALL");
    TEST_ASSERT_EQ(strlen(value), out_len, "Should still report actual length");

    TEST_PASS();
}

// Test: Binary data (with null bytes)
int test_binary_data(void) {
    TEST_START("binary_data");

    const char* key = "binary_key";
    const char value[] = "\x00\x01\x02\x03\x04\x05\x00\x07";
    size_t vlen = 8;

    int ret = hinotetsu_set(db, key, strlen(key), value, vlen, 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET binary should return OK");

    char* out_value = NULL;
    size_t out_len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET binary should return OK");
    TEST_ASSERT_EQ(vlen, out_len, "Binary length should match");
    TEST_ASSERT(memcmp(value, out_value, vlen) == 0, "Binary content should match");

    free(out_value);
    TEST_PASS();
}

// Test: Empty value
int test_empty_value(void) {
    TEST_START("empty_value");

    const char* key = "empty_value_key";
    const char* value = "";

    int ret = hinotetsu_set(db, key, strlen(key), value, 0, 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET empty value should return OK");

    char* out_value = NULL;
    size_t out_len = 999;  // Set to non-zero to verify it's updated
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET empty value should return OK");
    TEST_ASSERT_EQ(0, out_len, "Empty value length should be 0");

    free(out_value);
    TEST_PASS();
}

// Test: Long key
int test_long_key(void) {
    TEST_START("long_key");

    char key[256];
    memset(key, 'k', 255);
    key[255] = '\0';
    const char* value = "long_key_value";

    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET with long key should return OK");

    char* out_value = NULL;
    size_t out_len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET with long key should return OK");
    TEST_ASSERT_STR_EQ(value, out_value, out_len, "Value should match");

    free(out_value);
    TEST_PASS();
}

// Test: Large value
int test_large_value(void) {
    TEST_START("large_value");

    const char* key = "large_value_key";
    size_t vlen = 4096;
    char* value = malloc(vlen);
    TEST_ASSERT(value != NULL, "malloc should succeed");
    random_string(value, vlen);

    int ret = hinotetsu_set(db, key, strlen(key), value, vlen - 1, 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET large value should return OK");

    char* out_value = NULL;
    size_t out_len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out_value, &out_len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET large value should return OK");
    TEST_ASSERT_EQ(vlen - 1, out_len, "Large value length should match");
    TEST_ASSERT_STR_EQ(value, out_value, out_len, "Large value content should match");

    free(value);
    free(out_value);
    TEST_PASS();
}

// Test: FLUSH
int test_flush(void) {
    TEST_START("flush");

    // Insert some keys
    hinotetsu_set(db, "flush1", 6, "v1", 2, 0);
    hinotetsu_set(db, "flush2", 6, "v2", 2, 0);
    hinotetsu_set(db, "flush3", 6, "v3", 2, 0);

    // Verify they exist
    char* out = NULL;
    size_t len = 0;
    int ret = hinotetsu_get(db, "flush1", 6, &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "Key should exist before flush");
    free(out);

    // Flush
    hinotetsu_flush(db);

    // Verify keys are gone
    ret = hinotetsu_get(db, "flush1", 6, &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "Key should not exist after flush");
    ret = hinotetsu_get(db, "flush2", 6, &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "Key should not exist after flush");
    ret = hinotetsu_get(db, "flush3", 6, &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "Key should not exist after flush");

    TEST_PASS();
}

// Test: STATS
int test_stats(void) {
    TEST_START("stats");

    // Flush and add some keys
    hinotetsu_flush(db);
    hinotetsu_set(db, "stat1", 5, "value1", 6, 0);
    hinotetsu_set(db, "stat2", 5, "value2", 6, 0);

    HinotetsuStats stats;
    hinotetsu_stats(db, &stats);

    TEST_ASSERT_EQ(2, stats.count, "Count should be 2");
    TEST_ASSERT(stats.memory_used > 0, "Memory used should be > 0");
    TEST_ASSERT(stats.pool_size > 0, "Pool size should be > 0");

    printf("  Stats: count=%zu, mem=%zu, pool=%zu, hits=%zu, misses=%zu\n",
           stats.count, stats.memory_used, stats.pool_size,
           stats.hits, stats.misses);

    TEST_PASS();
}

// Test: Hit/Miss statistics
int test_hit_miss_stats(void) {
    TEST_START("hit_miss_stats");

    hinotetsu_flush(db);

    HinotetsuStats stats_before;
    hinotetsu_stats(db, &stats_before);

    // Set a key
    hinotetsu_set(db, "hitkey", 6, "hitval", 6, 0);

    // Hit
    char* out = NULL;
    size_t len = 0;
    hinotetsu_get(db, "hitkey", 6, &out, &len);
    free(out);

    // Miss
    hinotetsu_get(db, "nokey", 5, &out, &len);

    HinotetsuStats stats_after;
    hinotetsu_stats(db, &stats_after);

    TEST_ASSERT(stats_after.hits > stats_before.hits, "Hits should increase");
    TEST_ASSERT(stats_after.misses > stats_before.misses, "Misses should increase");

    TEST_PASS();
}

int main(void) {
    printf("Hinotetsu Basic Tests\n");
    printf("========================================\n");

    srand((unsigned)time(NULL));

    // Open database for tests
    // Note: Each shard requires minimum 1MB, and there are 64 shards
    // Plus slab pre-warming consumes additional memory
    db = hinotetsu_open(256 * 1024 * 1024);  // 256MB
    if (!db) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    // Run tests
    RUN_TEST(test_open_close);
    RUN_TEST(test_version);
    RUN_TEST(test_set_get_simple);
    RUN_TEST(test_set_overwrite);
    RUN_TEST(test_get_notfound);
    RUN_TEST(test_delete);
    RUN_TEST(test_delete_notfound);
    RUN_TEST(test_get_into);
    RUN_TEST(test_get_into_toosmall);
    RUN_TEST(test_binary_data);
    RUN_TEST(test_empty_value);
    RUN_TEST(test_long_key);
    RUN_TEST(test_large_value);
    RUN_TEST(test_flush);
    RUN_TEST(test_stats);
    RUN_TEST(test_hit_miss_stats);

    hinotetsu_close(db);

    TEST_SUMMARY();
}
