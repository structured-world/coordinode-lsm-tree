#!/bin/bash
# Split the dashboard suites recorded before they carried a major version into
# one suite per version line: `lsm-tree db_bench · <os> · <runner>` becomes
# `lsm-tree db_bench <N>.x · <os> · <runner>`, and `lsm-tree db_bench costs`
# becomes `lsm-tree db_bench costs <N>.x`. A point belongs to the line whose
# first commit it descends from.
#
# Usage: split-bench-series.sh <data.js> <first commit of the new line> <old major> <new major>
#
# Run from a checkout with the full history of the measured branch, since each
# point's line is decided by ancestry. Idempotent: a data file with no
# unversioned suite is left untouched, and the exit status says whether it
# changed (0 changed, 3 nothing to split).

set -euo pipefail

file="$1"
boundary="$2"
old_major="$3"
new_major="$4"

prefix='window.BENCHMARK_DATA = '
body="$(cat "$file")"
case "$body" in
  "$prefix"*) ;;
  *) echo "split-bench-series: $file does not start with '$prefix'" >&2; exit 1 ;;
esac
json="${body#"$prefix"}"

# Unversioned suites: the costs suite and the per-host rate suites, named
# before a version line was part of the name.
unversioned='. == "lsm-tree db_bench costs" or startswith("lsm-tree db_bench · ")'
ids="$(jq -r "[.entries | to_entries[] | select(.key | $unversioned) | .value[].commit.id] | unique[]" <<<"$json")"
if [ -z "$ids" ]; then
  echo "split-bench-series: no unversioned suite in $file" >&2
  exit 3
fi

new_line_ids='[]'
while IFS= read -r id; do
  if ! git cat-file -e "$id^{commit}" 2>/dev/null; then
    echo "split-bench-series: commit $id of $file is not in this checkout's history" >&2
    exit 1
  fi
  if git merge-base --is-ancestor "$boundary" "$id"; then
    new_line_ids="$(jq -c --arg id "$id" '. + [$id]' <<<"$new_line_ids")"
  fi
done <<<"$ids"

# Rename by inserting "<N>.x" after the suite's fixed part; points of one
# unversioned suite go to the old or the new line by ancestry, and a line
# that already holds points keeps them, merged in date order.
split="$(jq --argjson newids "$new_line_ids" --arg old "$old_major.x" --arg new "$new_major.x" "
  def versioned(\$line):
    if . == \"lsm-tree db_bench costs\" then \"lsm-tree db_bench costs \" + \$line
    else \"lsm-tree db_bench \" + \$line + (.[(\"lsm-tree db_bench\" | length):]) end;
  .entries as \$all
  | .entries = (
      reduce (\$all | to_entries[]) as \$suite ({};
        if (\$suite.key | $unversioned) then
          (\$suite.value | map(select(.commit.id as \$c | \$newids | index(\$c)))) as \$newer
          | (\$suite.value | map(select(.commit.id as \$c | \$newids | index(\$c) | not))) as \$older
          | (\$suite.key | versioned(\$old)) as \$oldkey
          | (\$suite.key | versioned(\$new)) as \$newkey
          | .[\$oldkey] = (((.[\$oldkey] // []) + \$older) | sort_by(.date))
          | .[\$newkey] = (((.[\$newkey] // []) + \$newer) | sort_by(.date))
        else
          .[\$suite.key] = (((.[\$suite.key] // []) + \$suite.value) | sort_by(.date))
        end)
      | with_entries(select(.value | length > 0)))
" <<<"$json")"

printf '%s%s\n' "$prefix" "$split" > "$file"
echo "split-bench-series: split the unversioned suites of $file at $boundary into $old_major.x and $new_major.x" >&2
