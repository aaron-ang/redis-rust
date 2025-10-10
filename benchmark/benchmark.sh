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
        wait $RUST_PID 2>/dev/null || true
        RUST_PID=""
    fi
}

trap cleanup INT TERM

wait_for_server() {
    echo "Waiting for server to be ready..."
    for i in {1..30}; do
        if redis-cli ping >/dev/null 2>&1; then
            echo "Server is ready!"
            return 0
        fi
        sleep 1
    done
    echo "Server failed to start within 30 seconds"
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
        --ratio=1:0 \
        --data-size=1024 \
        -n allkeys \
        --key-pattern=P:P
    
    # Wait for server to finish processing all queued commands
    echo "Waiting for data processing to complete..."
    sleep 5
    
    # Verify data was loaded
    local dbsize=$(redis-cli dbsize)
    echo "Database size: $dbsize"
    if [ "$dbsize" -lt 1699396 ]; then
        echo "Expected 1,699,396 keys but only got $dbsize"
        exit 1
    fi
}

run_throughput_benchmark() {
    local prefix=$1
    echo "Running throughput benchmark for $prefix..."
    memtier_benchmark \
        --pipeline=10 \
        --clients=50 \
        --threads=6 \
        --key-maximum=1699396 \
        --ratio=0:1 \
        --data-size=1024 \
        --distinct-client-seed \
        --test-time 60
}

run_latency_benchmark() {
    local prefix=$1
    echo "Running latency benchmark for $prefix..."
    memtier_benchmark \
        --pipeline=10 \
        --clients=50 \
        --threads=6 \
        --key-maximum=1699396 \
        --data-size=1024 \
        --distinct-client-seed \
        --test-time 60 \
        --hdr-file-prefix="out/$prefix"
}

### Benchmark 1 (Rust implementation)
echo "=== Benchmark 1: Rust Implementation ==="
../your_program.sh >/dev/null 2>&1 &
RUST_PID=$!
wait_for_server

flush_db
load_data "redis-rs"
run_throughput_benchmark "redis-rs"

flush_db
run_latency_benchmark "redis-rs"
cleanup

### Benchmark 2 (baseline)
echo "=== Benchmark 1: Redis Baseline ==="
brew services restart redis
wait_for_server

flush_db
load_data "baseline"
run_throughput_benchmark "baseline"

flush_db
run_latency_benchmark "baseline"
brew services stop redis

echo "Benchmark completed!"
