#!/bin/bash
# Build every benchmark binary the Benchmark workflow runs, before any of them
# measures anything: db_bench and the compare-rocksdb harness.
# Usage: .github/scripts/build-benchmarks.sh
#
# The two builds share no target directory, so they run side by side. Most of
# each one is the engine and the binary compiled with one codegen unit and thin
# LTO (the deterministic layout the dashboards need), which one rustc process
# does on about one core. A build never overlaps a measurement: the workflow
# runs the benchmarks only after this returns.

set -euo pipefail

# Resolve the lockfiles first, one crate at a time, so the parallel builds
# below only read them.
cargo fetch --manifest-path tools/db_bench/Cargo.toml
cargo fetch --manifest-path tools/compare-rocksdb/Cargo.toml

cargo build --release --manifest-path tools/db_bench/Cargo.toml &
plain=$!
cargo bench --manifest-path tools/compare-rocksdb/Cargo.toml --bench compare --no-run &
compare=$!

# Wait for both before reporting, so one failure does not leave the other
# build running into the next step.
status=0
wait "$plain" || status=1
wait "$compare" || status=1
exit "$status"
