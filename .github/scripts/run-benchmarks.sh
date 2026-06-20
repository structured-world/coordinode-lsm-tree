#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS] [ITERATIONS]
#
# Single working-set sweep at NUM (default 500k). The head-to-head size sweep
# (1k/10k/70k vs RocksDB + SurrealKV) lives in the separate `compare-rocksdb`
# harness, not here — this dashboard tracks the single-engine trend only.

set -e

NUM=${1:-500000}
ITERATIONS=${2:-3}

cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
  --benchmark all --num "$NUM" --iterations "$ITERATIONS" \
  --github-json \
  > benchmark-results.json

echo "Results written to benchmark-results.json (num: $NUM)" >&2
