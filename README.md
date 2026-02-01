# hinotetsu
HinoTetsu is Faster than memcached or Redis Key-Value Store. Implemented by Claude Code.

# ファイル構成
```
hinotetsu/
├── hinotetsu.h       # ライブラリヘッダー
├── hinotetsu.c       # ライブラリ実装
├── hinotetsud.c      # デーモン
└── hinotetsu-cli.c   # CLIクライアント
```

# コンパイル
```
bash# ライブラリ
gcc -O3 -c hinotetsu.c -o hinotetsu.o
ar rcs libhinotetsu.a hinotetsu.o

# デーモン
gcc -O3 -o hinotetsud hinotetsud.c hinotetsu.c -lpthread

# クライアント
gcc -O2 -o hinotetsu-cli hinotetsu-cli.c
```

# 使い方
```
bash# サーバー起動
./hinotetsud -p 11211 -m 256

# 出力:
#   ╦ ╦╦╔╗╔╔═╗╔╦╗╔═╗╔╦╗╔═╗╦ ╦
#   ╠═╣║║║║║ ║ ║ ║╣  ║ ╚═╗║ ║
#   ╩ ╩╩╝╚╝╚═╝ ╩ ╚═╝ ╩ ╚═╝╚═╝
#   High Performance Key-Value Store
#   Version 1.0.0

# クライアント (インタラクティブ)
./hinotetsu-cli -i

hinotetsu> set name Alice
STORED
hinotetsu> get name
Alice
hinotetsu> stats
```

# API
```c
#include "hinotetsu.h"

Hinotetsu *db = hinotetsu_open(0);  // デフォルト256MB

hinotetsu_set_str(db, "key", "value", 0);     // TTLなし
hinotetsu_set_str(db, "session", "abc", 3600); // 1時間TTL

char *val = hinotetsu_get_str(db, "key");
free(val);
hinotetsu_close(db);
```


# TEST
  作成したテストファイル                                                        
```                                                                                
test/                                                                         
  ├── test_helper.h      # テストフレームワーク（マクロ定義）                   
  ├── test_basic.c       # 基本テスト（16テスト）                               
  ├── test_ttl.c         # TTLテスト（8テスト）                                 
  ├── test_stress.c      # ストレステスト（7テスト）                            
  ├── test_protocol.c    # memcachedプロトコルテスト（12テスト）                
  └── run_tests.sh       # テスト実行スクリプト                                 
```                                                                                
##  テスト内容
```
  ファイル: test_basic.c                                                        
  テスト内容: SET/GET/DELETE, get_into, バイナリデータ, 空値, 長いキー,         
  大きい値,                                                                     
    FLUSH, STATS                                                                
  ────────────────────────────────────────                                      
  ファイル: test_ttl.c                                                          
  テスト内容: TTL期限前/後, TTL=0, TTL更新, 複数キーの異なるTTL                 
  ────────────────────────────────────────                                      
  ファイル: test_stress.c                                                       
  テスト内容: 大量キー挿入, 読み取り性能, 混合ワークロード,                     
    マルチスレッド同時アクセス, 削除ストレス                                    
  ────────────────────────────────────────                                      
  ファイル: test_protocol.c                                                     
  テスト内容: memcachedプロトコル(SET/GET/DELETE/STATS/VERSION/FLUSH等)         
    ※デーモン起動が必要                                                         
```

## 使い方                                                                        
```                                                                                
  # 全テスト実行                                                                
  ./test/run_tests.sh                                                           
                                                                                
  # 個別実行                                                                    
  ./test/run_tests.sh basic                                                     
  ./test/run_tests.sh ttl                                                       
  ./test/run_tests.sh stress                                                    
                                                                                
  # プロトコルテスト（デーモン起動後）                                          
  ./hinotetsu3d &                                                               
  ./test/run_tests.sh protocol                                                  
```                                                                                
  memcachedのテストスタイルを参考に、シンプルなassertベースのフレームワークで実装しました。


Hinotetsu 完成！🔥


# Benchmark
```
=== Hinotetsu ===
Running benchmark: 200000 ops, pipeline=128, protocol=memcached

=== Results ===
Total ops:    200000
Time:         0.378557 s
Throughput:   528322 op/s
Avg latency:  0.00189256 ms/op
p50 latency:  0.0018197 ms/op
p95 latency:  0.00230304 ms/op
p99 latency:  0.00312544 ms/op
p999 latency: 0.00505984 ms/op
max latency:  0.00521249 ms/op
Running benchmark: 2000000 ops, pipeline=128, protocol=memcached

=== Results ===
Total ops:    2000000
Time:         3.85526 s
Throughput:   518771 op/s
Avg latency:  0.00192763 ms/op
p50 latency:  0.00181371 ms/op
p95 latency:  0.00240177 ms/op
p99 latency:  0.00356934 ms/op
p999 latency: 0.00601904 ms/op
max latency:  0.285051 ms/op

=== memcached ===
Running benchmark: 200000 ops, pipeline=128, protocol=memcached

=== Results ===
Total ops:    200000
Time:         1.46652 s
Throughput:   136378 op/s
Avg latency:  0.00733312 ms/op
p50 latency:  0.0072374 ms/op
p95 latency:  0.00838894 ms/op
p99 latency:  0.01071 ms/op
p999 latency: 0.0170617 ms/op
max latency:  0.0200535 ms/op
Running benchmark: 2000000 ops, pipeline=128, protocol=memcached

=== Results ===
Total ops:    2000000
Time:         14.9586 s
Throughput:   133702 op/s
Avg latency:  0.0074793 ms/op
p50 latency:  0.00707411 ms/op
p95 latency:  0.0108421 ms/op
p99 latency:  0.0140137 ms/op
p999 latency: 0.0279262 ms/op
max latency:  0.0477025 ms/op

=== Redis ===
Running benchmark: 200000 ops, pipeline=128, protocol=redis

=== Results ===
Total ops:    200000
Time:         1.37762 s
Throughput:   145177 op/s
Avg latency:  0.0068882 ms/op
p50 latency:  0.00628066 ms/op
p95 latency:  0.0109124 ms/op
p99 latency:  0.0140802 ms/op
p999 latency: 0.0202452 ms/op
max latency:  0.0357952 ms/op
Running benchmark: 2000000 ops, pipeline=128, protocol=redis

=== Results ===
Total ops:    2000000
Time:         11.264 s
Throughput:   177556 op/s
Avg latency:  0.00563201 ms/op
p50 latency:  0.0051937 ms/op
p95 latency:  0.00801777 ms/op
p99 latency:  0.0114615 ms/op
p999 latency: 0.0230171 ms/op
max latency:  0.532535 ms/op
```


## 200K ops（ウォームアップ）

| 指標 | Hinotetsu 3 | memcached | Redis |
|:-----|------------:|----------:|------:|
| **Throughput** | **528,322 op/s** | 136,378 op/s | 145,177 op/s |
| p99 | 0.003ms | 0.011ms | 0.014ms |
| max | 0.005ms | 0.020ms | 0.036ms |

## 2M ops（本番相当）

| 指標 | Hinotetsu 3 | memcached | Redis | vs memcached | vs Redis |
|:-----|------------:|----------:|------:|-------------:|---------:|
| **Throughput** | **518,771 op/s** | 133,702 op/s | 177,556 op/s | **3.9x** | **2.9x** |
| p50 | 0.0018ms | 0.0071ms | 0.0052ms | **3.9x** | **2.9x** |
| p95 | 0.0024ms | 0.0108ms | 0.0080ms | **4.5x** | **3.3x** |
| p99 | 0.0036ms | 0.0140ms | 0.0115ms | **3.9x** | **3.2x** |
| p999 | 0.0060ms | 0.0279ms | 0.0230ms | **4.6x** | **3.8x** |
| max | 0.285ms | 0.048ms | 0.533ms | - | **1.9x** |

## 結論

**Hinotetsu 3 は本物です：**

- **memcached の 3.9 倍のスループット**
- **Redis の 2.9 倍のスループット**
- **全パーセンタイルで 3〜4.6 倍高速**
- **50 万 op/s 超え**を安定して維持

# License

This project is licensed under the Business Source License 1.1.
See the LICENSE file for details.

