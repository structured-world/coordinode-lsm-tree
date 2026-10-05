#!/bin/bash
# Push the gh-pages commit in the current directory, retrying a refused push.
# Usage: push-gh-pages.sh   (run inside a gh-pages clone with a commit to push)
#
# GitHub sometimes refuses a push on its own side ("fatal error in
# commit_refs"), and another workflow may have pushed gh-pages since the clone.
# Either way the next attempt rebases onto what is there and pushes again;
# three refusals in a row fail the step.

set -euo pipefail

for attempt in 1 2 3; do
  if git push origin gh-pages; then
    exit 0
  fi
  echo "push-gh-pages: attempt $attempt refused" >&2
  if [ "$attempt" -lt 3 ]; then
    sleep $((attempt * 5))
    git pull --rebase origin gh-pages
  fi
done
echo "::error::gh-pages refused the push three times"
exit 1
