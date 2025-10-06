#!/bin/bash

set -e

# Check if in benchmark directory
if [ "$(basename "$(pwd)")" != "benchmark" ]; then
    cd benchmark/
fi

RUST_PID=""

cleanup() {
    if [ -n "$RUST_PID" ] && kill -0 $RUST_PID 2>/dev/null; then
        echo "Cleaning up Rust server (PID: $RUST_PID)..."
        kill $RUST_PID 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

wait_for_server() {
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if redis-cli ping >/dev/null 2>&1; then
            echo "Server is ready!"
            return 0
        fi
        sleep 1
    done
    echo "Server failed to start"
    exit 1
}

flush_db() {
    echo "Flushing database..."
    redis-cli flushall
}

load_data() {
    local prefix=$1
    echo "Loading data for $prefix..."
    memtier_benchmark --hide-histogram \
        --pipeline=10 \
        --clients=50 \
        --threads=6 \
        --key-maximum=1699396 \
        -n allkeys \
        --key-pattern=P:P \
        --ratio=1:0 \
        --data-size=1024
}

run_get_benchmark() {
    local prefix=$1
    echo "Running GET benchmark for $prefix..."
    memtier_benchmark \
        --pipeline=10 \
        --clients=50 \
        --threads=6 \
        --key-maximum=1699396 \
        --key-pattern=R:R \
        --ratio=0:1 \
        --data-size=1024 \
        --distinct-client-seed \
        --test-time 60 \
        --hdr-file-prefix="out/$prefix"
}

### Benchmark 1 (baseline)
echo "=== Benchmark 1: Redis Baseline ==="
brew services restart redis
wait_for_server
flush_db
load_data "baseline"
run_get_benchmark "baseline-get"
brew services stop redis

### Benchmark 2 (Rust implementation)
echo "=== Benchmark 2: Rust Implementation ==="
../your_program.sh >/dev/null 2>&1 &
RUST_PID=$!

wait_for_server
flush_db
load_data "redis-rs"
run_get_benchmark "redis-rs-get"

echo "Benchmark completed!"
