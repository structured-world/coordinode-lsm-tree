#!/bin/bash
# Run all db_bench workloads and produce github-action-benchmark JSON.
# Usage: .github/scripts/run-benchmarks.sh [NUM_OPS] [ITERATIONS]
#
# Sweeps two working-set sizes so the dashboard tracks both the default
# large set (NUM, default 500k) and a fixed 100k set that exceeds the default
# block cache. The 100k pass tags every entry name with " /100k" so the two
# series stay distinct (the default-size entries keep their original names, so
# their dashboard history is unbroken).

set -e

NUM=${1:-500000}
ITERATIONS=${2:-3}

run_pass() {
  # $1: --num value, $2: name suffix, $3: output file
  cargo run --release --manifest-path tools/db_bench/Cargo.toml -- \
    --benchmark all --num "$1" --iterations "$ITERATIONS" \
    --name-suffix "$2" --github-json \
    > "$3"
}

run_pass "$NUM" "" results-main.json
run_pass 100000 " /100k" results-100k.json

# Concatenate the two github-action-benchmark arrays into one.
jq -s 'add' results-main.json results-100k.json > benchmark-results.json
rm -f results-main.json results-100k.json

echo "Results written to benchmark-results.json (sizes: $NUM, 100000)" >&2
