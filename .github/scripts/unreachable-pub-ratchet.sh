#!/usr/bin/env bash
# Holds rustc's `unreachable_pub` warnings to a baseline that only goes down,
# and to none at all in the files a change touches. An item no other crate can
# reach is written `pub(crate)`; the crate is brought there file by file, as
# changes touch them, and the lint is denied in `src/lib.rs` once the count is
# zero.
#
# Usage: unreachable-pub-ratchet.sh [changed file...]
#
# The count is compared with `.github/unreachable-pub-baseline` under CI only:
# code behind a platform `cfg` is counted on the platform that compiles it, and
# the baseline is the Linux count the lint job sees. A count below the baseline
# fails too, so the progress a change made is written down and cannot be lost
# by the next one.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
baseline_file="$root/.github/unreachable-pub-baseline"
report=$(mktemp)
trap 'rm -f "$report"' EXIT

# `--all-targets` compiles the library twice (as a library and as its own test
# harness), so each warning is reported twice: one line per place. A file pulled
# into another target by `#[path]` is named through it (`examples/../tests/…`);
# the name is folded back to the file's own.
cargo clippy --all-features --all-targets --message-format=json -- -W unreachable_pub \
  | jq -r 'select(.reason == "compiler-message" and .message.code.code == "unreachable_pub")
      | .message.spans[] | select(.is_primary)
      | "\(.file_name | gsub("[^/]+/\\.\\./"; "")):\(.line_start):\(.column_start)"' \
  | sort -u >"$report"

total=$(wc -l <"$report" | tr -d ' ')
failed=0

for file in "$@"; do
  case "$file" in
    *.rs) ;;
    *) continue ;;
  esac
  while IFS=: read -r path line column; do
    echo "::error file=$path,line=$line,col=$column::unreachable \`pub\` item in a file this change touches: write it \`pub(crate)\`"
    failed=1
  done < <(awk -F: -v f="$file" '$1 == f' "$report")
done

echo "unreachable_pub warnings: $total"
if [ -n "${CI:-}" ]; then
  baseline=$(tr -d '[:space:]' <"$baseline_file")
  if [ "$total" -gt "$baseline" ]; then
    echo "::error file=.github/unreachable-pub-baseline::unreachable_pub warnings grew from $baseline to $total"
    failed=1
  elif [ "$total" -lt "$baseline" ]; then
    echo "::error file=.github/unreachable-pub-baseline::unreachable_pub warnings fell from $baseline to $total: write $total into the baseline to keep it"
    failed=1
  fi
fi

exit "$failed"
