// test_protocol.c
// Memcached text protocol tests for Hinotetsu daemon
// Tests the server via TCP connection (requires running daemon)
//
// Usage: ./test_protocol [host] [port]
// Default: ./test_protocol 127.0.0.1 11211

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <errno.h>
#include "test_helper.h"

static int sock = -1;
static char recv_buf[65536];

// Connect to server
static int connect_to_server(const char* host, int port) {
    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return -1;
    }

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, host, &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        sock = -1;
        return -1;
    }

    return 0;
}

// Send command and receive response
static int send_cmd(const char* cmd, char* response, size_t resp_size) {
    if (send(sock, cmd, strlen(cmd), 0) < 0) {
        perror("send");
        return -1;
    }

    memset(response, 0, resp_size);
    ssize_t n = recv(sock, response, resp_size - 1, 0);
    if (n < 0) {
        perror("recv");
        return -1;
    }
    response[n] = '\0';
    return (int)n;
}

// Send SET command (with data block)
static int send_set(const char* key, const char* value, int exptime) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "set %s 0 %d %zu\r\n", key, exptime, strlen(value));

    if (send(sock, cmd, strlen(cmd), 0) < 0) return -1;
    if (send(sock, value, strlen(value), 0) < 0) return -1;
    if (send(sock, "\r\n", 2, 0) < 0) return -1;

    memset(recv_buf, 0, sizeof(recv_buf));
    ssize_t n = recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    if (n < 0) return -1;

    return 0;
}

// Test: Simple SET and GET
int test_protocol_set_get(void) {
    TEST_START("protocol_set_get");

    // SET
    int ret = send_set("proto_key1", "proto_value1", 0);
    TEST_ASSERT_EQ(0, ret, "send_set should succeed");
    TEST_ASSERT(strstr(recv_buf, "STORED") != NULL, "SET should return STORED");

    // GET
    send_cmd("get proto_key1\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "VALUE proto_key1") != NULL, "GET should return VALUE");
    TEST_ASSERT(strstr(recv_buf, "proto_value1") != NULL, "GET should contain value");
    TEST_ASSERT(strstr(recv_buf, "END") != NULL, "GET should end with END");

    TEST_PASS();
}

// Test: GET non-existent key
int test_protocol_get_miss(void) {
    TEST_START("protocol_get_miss");

    send_cmd("get nonexistent_protocol_key_xyz\r\n", recv_buf, sizeof(recv_buf));
    // For miss, just "END\r\n" without VALUE line
    TEST_ASSERT(strstr(recv_buf, "VALUE") == NULL, "GET miss should not return VALUE");
    TEST_ASSERT(strstr(recv_buf, "END") != NULL, "GET miss should return END");

    TEST_PASS();
}

// Test: DELETE
int test_protocol_delete(void) {
    TEST_START("protocol_delete");

    // Set a key
    send_set("proto_del_key", "delete_me", 0);

    // Delete it
    send_cmd("delete proto_del_key\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "DELETED") != NULL, "DELETE should return DELETED");

    // Verify it's gone
    send_cmd("get proto_del_key\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "VALUE") == NULL, "GET after DELETE should not find key");

    TEST_PASS();
}

// Test: DELETE non-existent
int test_protocol_delete_miss(void) {
    TEST_START("protocol_delete_miss");

    send_cmd("delete nonexistent_delete_key_xyz\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "NOT_FOUND") != NULL, "DELETE miss should return NOT_FOUND");

    TEST_PASS();
}

// Test: STATS command
int test_protocol_stats(void) {
    TEST_START("protocol_stats");

    send_cmd("stats\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "STAT") != NULL, "STATS should return STAT lines");
    TEST_ASSERT(strstr(recv_buf, "END") != NULL, "STATS should end with END");

    // Print some stats
    printf("  Stats response:\n");
    char* line = strtok(recv_buf, "\r\n");
    int count = 0;
    while (line && count < 10) {
        if (strncmp(line, "STAT ", 5) == 0) {
            printf("    %s\n", line);
            count++;
        }
        line = strtok(NULL, "\r\n");
    }

    TEST_PASS();
}

// Test: VERSION command
int test_protocol_version(void) {
    TEST_START("protocol_version");

    send_cmd("version\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "VERSION") != NULL, "VERSION should return VERSION string");
    printf("  %s", recv_buf);

    TEST_PASS();
}

// Test: Binary data in value
int test_protocol_binary_value(void) {
    TEST_START("protocol_binary_value");

    const char* key = "binary_proto_key";
    // Binary data (but no \r\n inside to avoid protocol confusion)
    const char value[] = "binary\x00\x01\x02data";
    size_t vlen = 15;

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "set %s 0 0 %zu\r\n", key, vlen);
    send(sock, cmd, strlen(cmd), 0);
    send(sock, value, vlen, 0);
    send(sock, "\r\n", 2, 0);

    memset(recv_buf, 0, sizeof(recv_buf));
    recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    TEST_ASSERT(strstr(recv_buf, "STORED") != NULL, "SET binary should return STORED");

    // GET it back
    snprintf(cmd, sizeof(cmd), "get %s\r\n", key);
    send(sock, cmd, strlen(cmd), 0);

    memset(recv_buf, 0, sizeof(recv_buf));
    recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    TEST_ASSERT(strstr(recv_buf, "VALUE") != NULL, "GET binary should return VALUE");

    TEST_PASS();
}

// Test: Large value
int test_protocol_large_value(void) {
    TEST_START("protocol_large_value");

    const char* key = "large_proto_key";
    size_t vlen = 8192;
    char* value = malloc(vlen);
    random_string(value, vlen);

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "set %s 0 0 %zu\r\n", key, vlen - 1);
    send(sock, cmd, strlen(cmd), 0);
    send(sock, value, vlen - 1, 0);
    send(sock, "\r\n", 2, 0);

    memset(recv_buf, 0, sizeof(recv_buf));
    recv(sock, recv_buf, sizeof(recv_buf) - 1, 0);
    TEST_ASSERT(strstr(recv_buf, "STORED") != NULL, "SET large should return STORED");

    free(value);
    TEST_PASS();
}

// Test: Multiple keys in single GET (multiget)
int test_protocol_multiget(void) {
    TEST_START("protocol_multiget");

    // Set several keys
    send_set("multi1", "val1", 0);
    send_set("multi2", "val2", 0);
    send_set("multi3", "val3", 0);

    // Multi-get
    send_cmd("get multi1 multi2 multi3\r\n", recv_buf, sizeof(recv_buf));

    // Should contain all values
    int found = 0;
    if (strstr(recv_buf, "multi1")) found++;
    if (strstr(recv_buf, "multi2")) found++;
    if (strstr(recv_buf, "multi3")) found++;

    TEST_ASSERT_EQ(3, found, "Multi-get should return all 3 keys");
    TEST_ASSERT(strstr(recv_buf, "END") != NULL, "Multi-get should end with END");

    TEST_PASS();
}

// Test: Flush all
int test_protocol_flush(void) {
    TEST_START("protocol_flush");

    // Set a key
    send_set("flush_test_key", "flush_value", 0);

    // Verify it exists
    send_cmd("get flush_test_key\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "VALUE") != NULL, "Key should exist before flush");

    // Flush
    send_cmd("flush_all\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "OK") != NULL, "FLUSH_ALL should return OK");

    // Verify key is gone
    send_cmd("get flush_test_key\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "VALUE") == NULL, "Key should not exist after flush");

    TEST_PASS();
}

// Test: Invalid command
int test_protocol_invalid(void) {
    TEST_START("protocol_invalid");

    send_cmd("invalid_command_xyz\r\n", recv_buf, sizeof(recv_buf));
    TEST_ASSERT(strstr(recv_buf, "ERROR") != NULL || strstr(recv_buf, "CLIENT_ERROR") != NULL,
                "Invalid command should return ERROR");

    TEST_PASS();
}

// Test: Pipeline multiple commands
int test_protocol_pipeline(void) {
    TEST_START("protocol_pipeline");

    // Send multiple SET commands in pipeline
    const char* pipeline =
        "set pipe1 0 0 4\r\nval1\r\n"
        "set pipe2 0 0 4\r\nval2\r\n"
        "set pipe3 0 0 4\r\nval3\r\n";

    send(sock, pipeline, strlen(pipeline), 0);

    // Read all responses
    memset(recv_buf, 0, sizeof(recv_buf));
    usleep(100000);  // Give server time to process
    recv(sock, recv_buf, sizeof(recv_buf) - 1, MSG_DONTWAIT);

    // Count STORED responses
    int stored_count = 0;
    char* p = recv_buf;
    while ((p = strstr(p, "STORED")) != NULL) {
        stored_count++;
        p++;
    }

    TEST_ASSERT_EQ(3, stored_count, "Pipeline should return 3 STORED");

    TEST_PASS();
}

int main(int argc, char* argv[]) {
    printf("Hinotetsu Protocol Tests\n");
    printf("========================================\n");

    const char* host = "127.0.0.1";
    int port = 11211;

    if (argc >= 2) host = argv[1];
    if (argc >= 3) port = atoi(argv[2]);

    printf("Connecting to %s:%d...\n", host, port);

    if (connect_to_server(host, port) < 0) {
        fprintf(stderr, "Failed to connect to server at %s:%d\n", host, port);
        fprintf(stderr, "Make sure hinotetsu daemon is running:\n");
        fprintf(stderr, "  ./hinotetsu3d\n");
        return 1;
    }

    printf("Connected!\n");
    srand((unsigned)time(NULL));

    // Run tests
    RUN_TEST(test_protocol_version);
    RUN_TEST(test_protocol_set_get);
    RUN_TEST(test_protocol_get_miss);
    RUN_TEST(test_protocol_delete);
    RUN_TEST(test_protocol_delete_miss);
    RUN_TEST(test_protocol_stats);
    RUN_TEST(test_protocol_binary_value);
    RUN_TEST(test_protocol_large_value);
    RUN_TEST(test_protocol_multiget);
    RUN_TEST(test_protocol_flush);
    RUN_TEST(test_protocol_invalid);
    RUN_TEST(test_protocol_pipeline);

    close(sock);

    TEST_SUMMARY();
}
