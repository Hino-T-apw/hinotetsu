echo "=== Hinotetsu ==="
./bench_pipeline --host 127.0.0.1 --port 11211 --ops 200000 --pipeline 128
./bench_pipeline --host 127.0.0.1 --port 11211 --ops 2000000 --pipeline 128

echo ""
echo "=== memcached ==="
./bench_pipeline --host 127.0.0.1 --port 11212 --ops 200000 --pipeline 128
./bench_pipeline --host 127.0.0.1 --port 11212 --ops 2000000 --pipeline 128

# Redis (--redis フラグを追加)
echo ""
echo "=== Redis ==="
./bench_pipeline --host 127.0.0.1 --port 6379 --ops 200000 --pipeline 128 --redis
./bench_pipeline --host 127.0.0.1 --port 6379 --ops 2000000 --pipeline 128 --redis