#!/usr/bin/env bash
# Test wall-clock bounds are hang detectors, not performance assertions, and
# literal timeout durations must be at least 30 seconds. Intentionally short
# absence assertions and paused/virtual-time bounds must use the marker
# `// tight-timeout: <reason>` on the timeout line or the immediately preceding
# line. One marker covers one timeout call and the reason must be non-empty.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

problems=()
scanned_files=0
files=()

while IFS= read -r -d '' file; do
  scanned_files=$((scanned_files + 1))
  files+=("$file")
done < <(find crates apps -type f -name '*.rs' -print0 2>/dev/null)

scan_report=$(perl "$ROOT/scripts/test-timeout-lint.pl" "${files[@]}")
if [[ -n "$scan_report" ]]; then
  while IFS= read -r line; do problems+=("$line"); done <<<"$scan_report"
fi

if ((${#problems[@]} > 0)); then
  printf 'test-timeout-lint: %s\n' "${problems[@]}" >&2
  exit 1
fi

echo "test-timeout lint passed ($scanned_files Rust files)"
