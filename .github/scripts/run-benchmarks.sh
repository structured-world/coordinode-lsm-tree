#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON:
# benchmark-results.json holds the bigger-is-better series (rates, yields),
# benchmark-costs.json the smaller-is-better ones (amplifications). The action
# fixes one direction per suite, so the two are stored as separate suites.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS] [ITERATIONS]
#
# Single working-set sweep at NUM (default 500k). The head-to-head size sweep
# (1k/10k/70k vs RocksDB + SurrealKV) lives in the separate `compare-rocksdb`
# harness, not here — this dashboard tracks the single-engine trend only.

set -e

NUM=${1:-500000}
ITERATIONS=${2:-3}

# The rate workloads, on the engine as it ships.
cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
  --benchmark all --num "$NUM" --iterations "$ITERATIONS" \
  --github-json \
  > benchmark-results.json

# The byte-counter workload needs the engine's read counters, which are
# atomics on every read path, so it runs from a separate `counters` build
# rather than slowing the rates above. Its yields join the same suite; its
# costs are the whole of the other one.
cargo run --release --manifest-path tools/db_bench/Cargo.toml --features counters -- \
  --benchmark mixed-layout --num "$NUM" --iterations "$ITERATIONS" \
  --github-json \
  --github-json-append benchmark-results.json \
  --github-json-costs benchmark-costs.json

echo "Results written to benchmark-results.json and benchmark-costs.json (num: $NUM)" >&2
