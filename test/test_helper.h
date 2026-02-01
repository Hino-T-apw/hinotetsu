// test_helper.h
// Simple test framework for Hinotetsu (inspired by memcached test style)
#ifndef TEST_HELPER_H
#define TEST_HELPER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static int test_count = 0;
static int test_passed = 0;
static int test_failed = 0;

#define TEST_START(name) \
    printf("\n=== TEST: %s ===\n", name); \
    test_count++

#define TEST_ASSERT(cond, msg) \
    do { \
        if (!(cond)) { \
            printf("  [FAIL] %s:%d: %s\n", __FILE__, __LINE__, msg); \
            test_failed++; \
            return 0; \
        } \
    } while(0)

#define TEST_ASSERT_EQ(expected, actual, msg) \
    do { \
        if ((expected) != (actual)) { \
            printf("  [FAIL] %s:%d: %s (expected: %ld, actual: %ld)\n", \
                   __FILE__, __LINE__, msg, (long)(expected), (long)(actual)); \
            test_failed++; \
            return 0; \
        } \
    } while(0)

#define TEST_ASSERT_STR_EQ(expected, actual, len, msg) \
    do { \
        if (memcmp((expected), (actual), (len)) != 0) { \
            printf("  [FAIL] %s:%d: %s\n", __FILE__, __LINE__, msg); \
            test_failed++; \
            return 0; \
        } \
    } while(0)

#define TEST_PASS() \
    do { \
        printf("  [PASS]\n"); \
        test_passed++; \
        return 1; \
    } while(0)

#define TEST_SUMMARY() \
    do { \
        printf("\n========================================\n"); \
        printf("Test Results: %d/%d passed", test_passed, test_count); \
        if (test_failed > 0) { \
            printf(" (%d failed)", test_failed); \
        } \
        printf("\n========================================\n"); \
        return test_failed == 0 ? 0 : 1; \
    } while(0)

#define RUN_TEST(test_func) \
    do { \
        test_func(); \
    } while(0)

// Utility: generate random string
static void random_string(char* buf, size_t len) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    for (size_t i = 0; i < len - 1; i++) {
        buf[i] = charset[rand() % (sizeof(charset) - 1)];
    }
    buf[len - 1] = '\0';
}

// Utility: get current time in milliseconds
static long long current_time_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

#endif // TEST_HELPER_H
