// test_ttl.c
// TTL (Time-To-Live) expiration tests for Hinotetsu
// Inspired by memcached's expiration tests

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include "test_helper.h"
#include "../hinotetsu3.h"

static Hinotetsu* db = NULL;

// Test: SET with TTL - key should be accessible before expiration
int test_ttl_before_expire(void) {
    TEST_START("ttl_before_expire");

    const char* key = "ttl_key_1";
    const char* value = "ttl_value_1";

    // Set with 5 second TTL
    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 5);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET with TTL should return OK");

    // Immediately get - should succeed
    char* out = NULL;
    size_t len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET before TTL expires should return OK");
    TEST_ASSERT_STR_EQ(value, out, len, "Value should match");

    free(out);
    TEST_PASS();
}

// Test: SET with TTL - key should disappear after expiration
int test_ttl_after_expire(void) {
    TEST_START("ttl_after_expire");

    const char* key = "ttl_key_2";
    const char* value = "ttl_value_2";

    // Set with 1 second TTL
    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 1);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET with TTL should return OK");

    // Wait for expiration
    printf("  Waiting 2 seconds for TTL expiration...\n");
    sleep(2);

    // GET should fail
    char* out = NULL;
    size_t len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "GET after TTL expires should return NOTFOUND");

    TEST_PASS();
}

// Test: SET with TTL=0 means no expiration
int test_ttl_zero(void) {
    TEST_START("ttl_zero");

    const char* key = "ttl_key_0";
    const char* value = "ttl_value_0";

    // Set with TTL=0 (no expiration)
    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 0);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET with TTL=0 should return OK");

    // Wait a bit
    sleep(1);

    // Should still be accessible
    char* out = NULL;
    size_t len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET with TTL=0 should never expire");
    TEST_ASSERT_STR_EQ(value, out, len, "Value should match");

    free(out);
    TEST_PASS();
}

// Test: Update TTL by re-SET
int test_ttl_update(void) {
    TEST_START("ttl_update");

    const char* key = "ttl_update_key";
    const char* value1 = "value1";
    const char* value2 = "value2";

    // Set with 1 second TTL
    int ret = hinotetsu_set(db, key, strlen(key), value1, strlen(value1), 1);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "First SET should return OK");

    // Wait 0.5 seconds
    usleep(500000);

    // Update with new TTL (reset the clock)
    ret = hinotetsu_set(db, key, strlen(key), value2, strlen(value2), 3);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "Second SET should return OK");

    // Wait 1.5 seconds (would have expired if TTL wasn't reset)
    printf("  Waiting 1.5 seconds...\n");
    usleep(1500000);

    // Should still be accessible with new value
    char* out = NULL;
    size_t len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET should succeed after TTL update");
    TEST_ASSERT_STR_EQ(value2, out, len, "Value should be updated");

    free(out);
    TEST_PASS();
}

// Test: Multiple keys with different TTLs
int test_ttl_multiple_keys(void) {
    TEST_START("ttl_multiple_keys");

    // Set keys with different TTLs
    hinotetsu_set(db, "short_ttl", 9, "short", 5, 1);   // 1 second
    hinotetsu_set(db, "long_ttl", 8, "long", 4, 10);    // 10 seconds

    // Wait 2 seconds
    printf("  Waiting 2 seconds...\n");
    sleep(2);

    char* out = NULL;
    size_t len = 0;

    // Short TTL key should be gone
    int ret = hinotetsu_get(db, "short_ttl", 9, &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "Short TTL key should be expired");

    // Long TTL key should still exist
    ret = hinotetsu_get(db, "long_ttl", 8, &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "Long TTL key should still exist");
    free(out);

    TEST_PASS();
}

// Test: TTL with get_into
int test_ttl_get_into(void) {
    TEST_START("ttl_get_into");

    const char* key = "ttl_getinto";
    const char* value = "value_data";

    // Set with 1 second TTL
    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 1);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");

    // Get before expiration
    char buf[64];
    size_t len = 0;
    ret = hinotetsu_get_into(db, key, strlen(key), buf, sizeof(buf), &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET_INTO before expire should return OK");

    // Wait for expiration
    printf("  Waiting 2 seconds for TTL expiration...\n");
    sleep(2);

    // Get after expiration
    ret = hinotetsu_get_into(db, key, strlen(key), buf, sizeof(buf), &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "GET_INTO after expire should return NOTFOUND");

    TEST_PASS();
}

// Test: Large TTL value
int test_ttl_large(void) {
    TEST_START("ttl_large");

    const char* key = "large_ttl_key";
    const char* value = "large_ttl_value";

    // Set with very large TTL (1 year)
    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 365 * 24 * 3600);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET with large TTL should return OK");

    // Should be accessible
    char* out = NULL;
    size_t len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "GET with large TTL should return OK");

    free(out);
    TEST_PASS();
}

// Test: DELETE should work on TTL keys before expiration
int test_ttl_delete(void) {
    TEST_START("ttl_delete");

    const char* key = "ttl_delete_key";
    const char* value = "ttl_delete_value";

    // Set with 10 second TTL
    int ret = hinotetsu_set(db, key, strlen(key), value, strlen(value), 10);
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "SET should return OK");

    // Delete before expiration
    ret = hinotetsu_delete(db, key, strlen(key));
    TEST_ASSERT_EQ(HINOTETSU_OK, ret, "DELETE should return OK");

    // Verify it's gone
    char* out = NULL;
    size_t len = 0;
    ret = hinotetsu_get(db, key, strlen(key), &out, &len);
    TEST_ASSERT_EQ(HINOTETSU_ERR_NOTFOUND, ret, "GET after DELETE should return NOTFOUND");

    TEST_PASS();
}

int main(void) {
    printf("Hinotetsu TTL Tests\n");
    printf("========================================\n");
    printf("(These tests involve waiting for TTL expiration)\n");

    // Open database (256MB for 64 shards + slab pre-warming)
    db = hinotetsu_open(256 * 1024 * 1024);
    if (!db) {
        fprintf(stderr, "Failed to open database\n");
        return 1;
    }

    // Run tests
    RUN_TEST(test_ttl_before_expire);
    RUN_TEST(test_ttl_after_expire);
    RUN_TEST(test_ttl_zero);
    RUN_TEST(test_ttl_update);
    RUN_TEST(test_ttl_multiple_keys);
    RUN_TEST(test_ttl_get_into);
    RUN_TEST(test_ttl_large);
    RUN_TEST(test_ttl_delete);

    hinotetsu_close(db);

    TEST_SUMMARY();
}
