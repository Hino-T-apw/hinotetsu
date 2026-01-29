# ライブラリ
gcc -O3 -c hinotetsu.c -o hinotetsu.o
ar rcs libhinotetsu.a hinotetsu.o

# デーモン
gcc -O3 -o hinotetsud hinotetsud.c hinotetsu.c -lpthread

# クライアント
gcc -O2 -o hinotetsu-cli hinotetsu-cli.c