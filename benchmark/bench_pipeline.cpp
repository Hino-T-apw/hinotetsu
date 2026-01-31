// bench_pipeline.cpp
// True pipelining benchmark for memcached/redis compatible servers
// g++ -O3 -std=c++17 bench_pipeline.cpp -o bench_pipeline
//
// Usage:
//   ./bench_pipeline --host 127.0.0.1 --port 11211 --ops 200000 --pipeline 128
//   ./bench_pipeline --host 127.0.0.1 --port 6379 --ops 200000 --pipeline 128 --redis

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include <algorithm>

using Clock = std::chrono::steady_clock;

static void die(const std::string& msg) {
  std::cerr << "ERROR: " << msg << "\n";
  std::exit(1);
}

static uint64_t now_ns() {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::now().time_since_epoch()).count();
}

class Connection {
public:
  int fd = -1;
  std::string rbuf;
  std::string wbuf;
  size_t wpos = 0;

  ~Connection() { if (fd >= 0) ::close(fd); }

  void connect(const std::string& host, int port) {
    struct addrinfo hints{}, *res;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host.c_str(), std::to_string(port).c_str(), &hints, &res) != 0)
      die("getaddrinfo failed");

    for (auto* p = res; p; p = p->ai_next) {
      fd = ::socket(p->ai_family, p->ai_socktype, p->ai_protocol);
      if (fd < 0) continue;
      if (::connect(fd, p->ai_addr, p->ai_addrlen) == 0) break;
      ::close(fd);
      fd = -1;
    }
    freeaddrinfo(res);
    if (fd < 0) die("connect failed");

    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  }

  // Memcached protocol
  void queue_mc_set(const std::string& key, const std::string& value) {
    wbuf += "set " + key + " 0 0 " + std::to_string(value.size()) + "\r\n";
    wbuf += value + "\r\n";
  }

  void queue_mc_get(const std::string& key) {
    wbuf += "get " + key + "\r\n";
  }

  // Redis RESP protocol
  void queue_redis_set(const std::string& key, const std::string& value) {
    wbuf += "*3\r\n$3\r\nSET\r\n";
    wbuf += "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
    wbuf += "$" + std::to_string(value.size()) + "\r\n" + value + "\r\n";
  }

  void queue_redis_get(const std::string& key) {
    wbuf += "*2\r\n$3\r\nGET\r\n";
    wbuf += "$" + std::to_string(key.size()) + "\r\n" + key + "\r\n";
  }

  size_t try_read_mc_responses() {
    char tmp[65536];
    ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
    if (n > 0) rbuf.append(tmp, n);
    else if (n == 0) die("connection closed");
    else if (errno != EAGAIN && errno != EWOULDBLOCK) die("recv error");

    size_t count = 0;

    while (true) {
      size_t pos = rbuf.find("\r\n");
      if (pos == std::string::npos) break;

      std::string line = rbuf.substr(0, pos);

      if (line == "STORED" || line == "NOT_FOUND" || line == "DELETED" || line == "END") {
        rbuf.erase(0, pos + 2);
        count++;
        continue;
      }

      if (line.rfind("VALUE ", 0) == 0) {
        size_t last_sp = line.rfind(' ');
        if (last_sp == std::string::npos) break;
        size_t bytes = std::stoul(line.substr(last_sp + 1));

        size_t need = pos + 2 + bytes + 2;
        if (rbuf.size() < need) break;

        size_t end_start = need;
        size_t end_pos = rbuf.find("\r\n", end_start);
        if (end_pos == std::string::npos) break;

        rbuf.erase(0, end_pos + 2);
        count++;
        continue;
      }

      rbuf.erase(0, pos + 2);
    }

    return count;
  }

  size_t try_read_redis_responses() {
    char tmp[65536];
    ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
    if (n > 0) rbuf.append(tmp, n);
    else if (n == 0) die("connection closed");
    else if (errno != EAGAIN && errno != EWOULDBLOCK) die("recv error");

    size_t count = 0;

    while (!rbuf.empty()) {
      size_t crlf = rbuf.find("\r\n");
      if (crlf == std::string::npos) break;

      char type = rbuf[0];

      if (type == '+' || type == '-' || type == ':') {
        // Simple string (+OK), error (-ERR), integer (:123)
        rbuf.erase(0, crlf + 2);
        count++;
        continue;
      }

      if (type == '$') {
        // Bulk string: $len\r\ndata\r\n or $-1\r\n (nil)
        std::string len_str = rbuf.substr(1, crlf - 1);
        long len = std::stol(len_str);

        if (len < 0) {
          // Nil ($-1\r\n)
          rbuf.erase(0, crlf + 2);
          count++;
          continue;
        }

        // Need: $len\r\n + data + \r\n
        size_t need = crlf + 2 + (size_t)len + 2;
        if (rbuf.size() < need) break;

        rbuf.erase(0, need);
        count++;
        continue;
      }

      if (type == '*') {
        // Array header - skip it, elements counted separately
        rbuf.erase(0, crlf + 2);
        continue;
      }

      // Unknown type, skip line
      rbuf.erase(0, crlf + 2);
    }

    return count;
  }

  bool try_write() {
    if (wpos >= wbuf.size()) return true;

    ssize_t n = ::send(fd, wbuf.data() + wpos, wbuf.size() - wpos, 0);
    if (n > 0) wpos += n;
    else if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) die("send error");

    return wpos >= wbuf.size();
  }

  void reset_write() {
    wbuf.clear();
    wpos = 0;
  }
};

int main(int argc, char** argv) {
  std::string host = "127.0.0.1";
  int port = 11211;
  size_t total_ops = 200000;
  size_t pipeline = 128;
  size_t keyspace = 10000;
  size_t value_size = 256;
  bool redis_mode = false;

  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) die("missing arg value");
      return argv[++i];
    };
    if (a == "--host") host = next();
    else if (a == "--port") port = std::stoi(next());
    else if (a == "--ops") total_ops = std::stoull(next());
    else if (a == "--pipeline") pipeline = std::stoull(next());
    else if (a == "--keyspace") keyspace = std::stoull(next());
    else if (a == "--value-size") value_size = std::stoull(next());
    else if (a == "--redis") redis_mode = true;
    else if (a == "-h" || a == "--help") {
      std::cout << "Usage: bench_pipeline [options]\n"
                << "  --host HOST       (default: 127.0.0.1)\n"
                << "  --port PORT       (default: 11211)\n"
                << "  --ops N           total operations (default: 200000)\n"
                << "  --pipeline N      pipeline depth (default: 128)\n"
                << "  --keyspace N      number of keys (default: 10000)\n"
                << "  --value-size N    value size in bytes (default: 256)\n"
                << "  --redis           use Redis RESP protocol\n";
      return 0;
    }
  }

  std::vector<std::string> keys;
  keys.reserve(keyspace);
  for (size_t i = 0; i < keyspace; i++) {
    keys.push_back("key" + std::to_string(i));
  }
  std::string value(value_size, 'x');

  Connection conn;
  conn.connect(host, port);

  std::vector<double> latencies;
  latencies.reserve(total_ops / pipeline + 1);

  std::mt19937 rng(12345);
  std::uniform_int_distribution<size_t> key_dist(0, keys.size() - 1);

  size_t ops_done = 0;
  uint64_t total_ns = 0;

  std::cout << "Running benchmark: " << total_ops << " ops, pipeline=" << pipeline
            << ", protocol=" << (redis_mode ? "redis" : "memcached") << "\n";

  while (ops_done < total_ops) {
    size_t batch = std::min(pipeline, total_ops - ops_done);

    conn.reset_write();
    for (size_t i = 0; i < batch; i++) {
      const std::string& k = keys[key_dist(rng)];
      if ((ops_done + i) % 2 == 0) {
        if (redis_mode) conn.queue_redis_set(k, value);
        else conn.queue_mc_set(k, value);
      } else {
        if (redis_mode) conn.queue_redis_get(k);
        else conn.queue_mc_get(k);
      }
    }

    uint64_t t0 = now_ns();

    while (!conn.try_write()) {
      pollfd pfd{conn.fd, POLLOUT, 0};
      poll(&pfd, 1, 100);
    }

    size_t responses = 0;
    size_t stall_count = 0;
    while (responses < batch) {
      pollfd pfd{conn.fd, POLLIN, 0};
      poll(&pfd, 1, 1000);

      size_t before = responses;
      if (redis_mode) responses += conn.try_read_redis_responses();
      else responses += conn.try_read_mc_responses();

      if (responses == before) {
        stall_count++;
        if (stall_count > 5) {
          std::cerr << "DEBUG: stalled, responses=" << responses << "/" << batch
                    << ", rbuf size=" << conn.rbuf.size()
                    << ", first 200 bytes: [" << conn.rbuf.substr(0, 200) << "]\n";
          die("timeout waiting for responses");
        }
      } else {
        stall_count = 0;
      }
    }

    uint64_t t1 = now_ns();
    uint64_t batch_ns = t1 - t0;
    total_ns += batch_ns;

    double batch_ms = (double)batch_ns / 1e6;
    double per_op_ms = batch_ms / batch;
    latencies.push_back(per_op_ms);

    ops_done += batch;
  }

  std::sort(latencies.begin(), latencies.end());

  double sum = 0;
  for (double v : latencies) sum += v;
  double avg = sum / latencies.size();

  auto percentile = [&](double p) {
    size_t idx = (size_t)(latencies.size() * p);
    if (idx >= latencies.size()) idx = latencies.size() - 1;
    return latencies[idx];
  };

  double seconds = (double)total_ns / 1e9;
  double ops_per_sec = (double)total_ops / seconds;

  std::cout << "\n=== Results ===\n";
  std::cout << "Total ops:    " << total_ops << "\n";
  std::cout << "Time:         " << seconds << " s\n";
  std::cout << "Throughput:   " << ops_per_sec << " op/s\n";
  std::cout << "Avg latency:  " << avg << " ms/op\n";
  std::cout << "p50 latency:  " << percentile(0.50) << " ms/op\n";
  std::cout << "p95 latency:  " << percentile(0.95) << " ms/op\n";
  std::cout << "p99 latency:  " << percentile(0.99) << " ms/op\n";
  std::cout << "p999 latency: " << percentile(0.999) << " ms/op\n";
  std::cout << "max latency:  " << latencies.back() << " ms/op\n";

  return 0;
}