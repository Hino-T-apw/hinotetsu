#!/bin/bash
# run_tests.sh - Compile and run Hinotetsu tests
# Usage: ./run_tests.sh [test_name]
#   ./run_tests.sh          # Run all tests
#   ./run_tests.sh basic    # Run only basic tests
#   ./run_tests.sh ttl      # Run only TTL tests
#   ./run_tests.sh stress   # Run only stress tests
#   ./run_tests.sh protocol # Run protocol tests (requires running daemon)

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$SCRIPT_DIR/build"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Hinotetsu Test Suite"
echo "=========================================="
echo ""

# Create build directory
mkdir -p "$BUILD_DIR"

# Compile library first
echo -e "${YELLOW}Compiling hinotetsu3 library...${NC}"
gcc -O2 -c "$PROJECT_DIR/hinotetsu3.c" -o "$BUILD_DIR/hinotetsu3.o" -lpthread
echo "  Done."
echo ""

compile_test() {
    local name=$1
    echo -e "${YELLOW}Compiling test_$name...${NC}"
    gcc -O2 -o "$BUILD_DIR/test_$name" \
        "$SCRIPT_DIR/test_$name.c" \
        "$BUILD_DIR/hinotetsu3.o" \
        -lpthread
    echo "  Done."
}

run_test() {
    local name=$1
    echo ""
    echo -e "${GREEN}Running test_$name...${NC}"
    echo "------------------------------------------"
    if "$BUILD_DIR/test_$name"; then
        return 0
    else
        return 1
    fi
}

# Determine which tests to run
TESTS_TO_RUN=""
if [ -z "$1" ]; then
    TESTS_TO_RUN="basic ttl stress"
elif [ "$1" = "all" ]; then
    TESTS_TO_RUN="basic ttl stress"
elif [ "$1" = "protocol" ]; then
    TESTS_TO_RUN="protocol"
else
    TESTS_TO_RUN="$1"
fi

FAILED=0

for test in $TESTS_TO_RUN; do
    compile_test "$test"
    if ! run_test "$test"; then
        FAILED=1
    fi
done

# Protocol test needs special handling (requires running daemon)
if [ "$1" = "protocol" ]; then
    echo ""
    echo -e "${YELLOW}Note: Protocol tests require a running daemon${NC}"
    echo "Start the daemon with: ./hinotetsu3d"
    echo ""
fi

echo ""
echo "=========================================="
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}Some tests failed!${NC}"
fi
echo "=========================================="

exit $FAILED
