#!/bin/bash

set -e

export CARGO_PROFILE_RELEASE_DEBUG=true

if [ "$(basename "$(pwd)")" != "benchmark" ]; then
    cd benchmark/
fi

FLAME_PID=""

cleanup() {
    if [ -n "$FLAME_PID" ]; then
        echo "Stopping flamegraph process (PID: $FLAME_PID)..."
        kill -INT -"$FLAME_PID" 2>/dev/null || true
        wait "$FLAME_PID" 2>/dev/null || true
        FLAME_PID=""
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

turn_off_persistence() {
    echo "Turning off persistence for Redis server..."
    redis-cli config set save ""
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

echo "=== Generating Flamegraph for Rust Redis Implementation ==="

# Start flamegraph profiling for Rust implementation
echo "Starting flamegraph profiling for Rust implementation..."
setsid cargo flamegraph --output flamegraph-rs.svg &
FLAME_PID=$!

wait_for_server
run_latency_benchmark "redis-rs"
cleanup

echo "=== Generating Flamegraph for Redis Server ==="

# Start flamegraph profiling for Redis server
echo "Starting flamegraph profiling for Redis server..."
REDIS_SERVER_PATH=$(which redis-server)
setsid flamegraph -o flamegraph-redis-server.svg -- $REDIS_SERVER_PATH &
FLAME_PID=$!

wait_for_server
flush_db
turn_off_persistence
run_latency_benchmark "redis-server"
cleanup

echo "Flamegraph generation completed!"
echo "Generated files:"
echo "  - flamegraph-rs.svg (Rust implementation)"
echo "  - flamegraph-redis-server.svg (Redis server)"
