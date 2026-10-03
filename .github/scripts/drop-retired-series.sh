#!/bin/bash
# Drop the points of retired series from the dashboard data file: every series
# a rule of the retired list names, in every suite the rule names, in every
# entry. An entry left with no series goes too. A retired series is one whose
# meaning changed (it got a new name) or that moved to another suite; its old
# points would otherwise stay in the file, and in the alert baseline of any
# series that still carries its name.
#
# Usage: drop-retired-series.sh <data.js> <retired-series.json>
#
# The list holds rules of the form {"suite": <regex>, "series": <regex>,
# "reason": <text>}. Idempotent: the exit status says whether the file changed
# (0 changed, 3 nothing to drop).

set -euo pipefail

file="$1"
rules="$2"

prefix='window.BENCHMARK_DATA = '
body="$(cat "$file")"
case "$body" in
  "$prefix"*) ;;
  *) echo "drop-retired-series: $file does not start with '$prefix'" >&2; exit 1 ;;
esac
json="${body#"$prefix"}"

# `--slurpfile` wraps the file's array in one more array. The `$` names are
# jq's own variables, which the shell must not expand.
# shellcheck disable=SC2016
retired='def retired($suite; $name):
  any($rules[0][]; . as $r | ($suite | test($r.suite)) and ($name | test($r.series)));'

count="$(jq --slurpfile rules "$rules" "$retired"'
  [.entries | to_entries[] | .key as $suite | .value[].benches[] | select(retired($suite; .name))]
  | length' <<<"$json")"
if [ "$count" -eq 0 ]; then
  echo "drop-retired-series: no retired series in $file" >&2
  exit 3
fi

dropped="$(jq --slurpfile rules "$rules" "$retired"'
  .entries |= with_entries(
    .key as $suite
    | .value |= (map(.benches |= map(select(retired($suite; .name) | not)))
                 | map(select(.benches | length > 0))))
  | .entries |= with_entries(select(.value | length > 0))' <<<"$json")"

printf '%s%s\n' "$prefix" "$dropped" > "$file"
echo "drop-retired-series: dropped $count points of retired series from $file" >&2
