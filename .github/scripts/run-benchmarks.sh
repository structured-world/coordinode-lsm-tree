#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON, one file
# per dashboard suite: benchmark-results.json the rates (bigger is better,
# timed, one suite per host), benchmark-costs.json the bytes the engine counts
# (smaller is better, one suite for every host) and benchmark-timings.json the
# times measured on this host (smaller is better, one suite per host). The
# action fixes one direction per suite, and a time is no baseline for another
# host's, hence three.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS] [ITERATIONS]
#
# Single working-set sweep at NUM (default 500k). The head-to-head size sweep
# (1k/10k/70k vs RocksDB + SurrealKV) lives in the separate `compare-rocksdb`
# harness, not here — this dashboard tracks the single-engine trend only.
#
# Builds what it runs if it is not built yet; the workflow builds it first with
# build-benchmarks.sh, so here `cargo run` finds both binaries fresh.

set -e

NUM=${1:-500000}
ITERATIONS=${2:-3}

# The rate workloads, on the engine as it ships.
cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
  --benchmark all --num "$NUM" --iterations "$ITERATIONS" \
  --github-json \
  > benchmark-results.json

# The byte-counter workloads need the engine's counters, which are atomics on
# every read and compaction path, so they run from a separate `counters` build
# rather than slowing the rates above, in one run so their costs and timings
# share one pair of files. Their series are costs (bytes per emitted row,
# compaction bytes per written byte, debt) and times (scans to a first batch,
# compaction steps); any rate they publish is appended to the rate suite
# rather than replacing it. They live in their own target directory, the one
# build-benchmarks.sh builds them in.
cargo run --release --manifest-path tools/db_bench/Cargo.toml --features counters \
  --target-dir tools/db_bench/target/counters -- \
  --benchmark mixed-layout,leveled-sustained --num "$NUM" --iterations "$ITERATIONS" \
  --github-json \
  --github-json-append benchmark-results.json \
  --github-json-costs benchmark-costs.json \
  --github-json-timings benchmark-timings.json

echo "Results written to benchmark-results.json, benchmark-costs.json and benchmark-timings.json (num: $NUM)" >&2
