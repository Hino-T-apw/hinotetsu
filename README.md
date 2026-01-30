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

Hinotetsu 完成！🔥


## License

This project is licensed under the Business Source License 1.1.
See the LICENSE file for details.

