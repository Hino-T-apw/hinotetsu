# hinotetsu
HinoTetsu is High Performance Key-Value Store. Implemented by Claude Code

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

# 使い方                                                                        
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

# License

This project is licensed under the Business Source License 1.1.
See the LICENSE file for details.

