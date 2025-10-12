#!/bin/bash
set -euo pipefail

KEY_MAX="${KEY_MAX:-1699396}"
TEST_TIME="${TEST_TIME:-60}"
CLIENTS="${CLIENTS:-50}"
THREADS="${THREADS:-6}"
PIPELINE="${PIPELINE:-10}"

RUST_PID=""

cleanup() {
    if [[ -n "${RUST_PID}" ]] && kill -0 "${RUST_PID}" 2>/dev/null; then
        echo "Cleaning up server (PID: ${RUST_PID})..."
        kill "${RUST_PID}" 2>/dev/null || true
        wait "${RUST_PID}" 2>/dev/null || true
    fi
    RUST_PID=""
}

trap cleanup INT TERM EXIT

# Move into benchmark directory if not already there
if [ "$(basename "$PWD")" != "benchmark" ]; then
    if [ -d benchmark ]; then
        cd benchmark || exit 1
    else
        echo "Error: benchmark directory not found"
        exit 1
    fi
fi

wait_for_server() {
    echo "Waiting for Redis..."
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
    flush_db
    echo "Loading data for ${prefix}..."
    memtier_benchmark \
        --ipv4 \
        --hide-histogram \
        --pipeline="${PIPELINE}" \
        --clients="${CLIENTS}" \
        --threads="${THREADS}" \
        --key-maximum="${KEY_MAX}" \
        --ratio=1:0 \
        --data-size=1024 \
        --key-pattern=P:P \
        -n allkeys

    echo "Waiting for data processing to complete..."
    sleep 5

    local dbsize
    dbsize=$(redis-cli dbsize)
    echo "Database size: ${dbsize}"
    if [[ "${dbsize}" -lt "${KEY_MAX}" ]]; then
        echo "Expected ${KEY_MAX} keys but only got ${dbsize}"
        exit 1
    fi
    }

    run_throughput_benchmark() {
    local prefix=$1
    echo "Running throughput benchmark for ${prefix}..."
    memtier_benchmark \
        --ipv4 \
        --pipeline="${PIPELINE}" \
        --clients="${CLIENTS}" \
        --threads="${THREADS}" \
        --key-maximum="${KEY_MAX}" \
        --ratio=0:1 \
        --data-size=1024 \
        --distinct-client-seed \
        --test-time "${TEST_TIME}"
}

run_latency_benchmark() {
    local prefix=$1
    mkdir -p out
    echo "Running latency benchmark for ${prefix}..."
    memtier_benchmark \
        --ipv4 \
        --key-maximum="${KEY_MAX}" \
        --ratio=0:1 \
        --data-size=1024 \
        --distinct-client-seed \
        --test-time "${TEST_TIME}" \
        --hdr-file-prefix="out/${prefix}"
}

echo "=== Benchmark 1: Rust Implementation ==="
cargo run --release >/dev/null 2>&1 &
RUST_PID=$!
wait_for_server

load_data "redis-rs"
run_throughput_benchmark "redis-rs"
run_latency_benchmark "redis-rs"
cleanup

echo "=== Benchmark 2: Redis Baseline ==="
redis-server --save "" >/dev/null 2>&1 &
RUST_PID=$!
wait_for_server

load_data "baseline"
run_throughput_benchmark "baseline"
run_latency_benchmark "baseline"
cleanup

echo "Benchmark completed!"
