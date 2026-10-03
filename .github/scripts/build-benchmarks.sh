#!/bin/bash
# Build every benchmark binary the Benchmark workflow runs, before any of them
# measures anything: db_bench as it ships, db_bench with the `counters`
# feature, and the compare-rocksdb harness.
# Usage: .github/scripts/build-benchmarks.sh
#
# The three builds share no target directory, so they run side by side. Most of
# each one is the engine and the binary compiled with one codegen unit and thin
# LTO (the deterministic layout the dashboards need), which one rustc process
# does on about one core: one after another the three took ~9 min on the bench
# runner. A build never overlaps a measurement: the workflow runs the
# benchmarks only after this returns.
#
# The `counters` build goes to its own directory under db_bench's target, the
# one run-benchmarks.sh runs it from: in the same directory the two builds
# would wait on each other's lock.

set -euo pipefail

# Resolve the lockfiles first, one crate at a time, so the parallel builds
# below only read them.
cargo fetch --manifest-path tools/db_bench/Cargo.toml
cargo fetch --manifest-path tools/compare-rocksdb/Cargo.toml

cargo build --release --manifest-path tools/db_bench/Cargo.toml &
plain=$!
cargo build --release --manifest-path tools/db_bench/Cargo.toml --features counters \
  --target-dir tools/db_bench/target/counters &
counters=$!
cargo bench --manifest-path tools/compare-rocksdb/Cargo.toml --bench compare --no-run &
compare=$!

# Wait for all three before reporting, so one failure does not leave the other
# builds running into the next step.
status=0
wait "$plain" || status=1
wait "$counters" || status=1
wait "$compare" || status=1
exit "$status"
