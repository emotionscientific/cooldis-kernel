#!/usr/bin/env bash
# Test wall-clock bounds are hang detectors, not performance assertions, and
# inline timeout durations must be at least 30 seconds. Intentionally short
# absence assertions and paused/virtual-time bounds must use the marker
# `// tight-timeout: <reason>` on the timeout line or the immediately preceding
# line. The reason must be non-empty. This line-based lint catches inline
# from_secs/from_millis literals on the same line as a timeout call; multi-line
# call construction is a known limitation and remains a review responsibility.
set -euo pipefail

cd "$(dirname "$0")/.."

problems=()
scanned_files=0

while IFS= read -r -d '' file; do
  scanned_files=$((scanned_files + 1))
  scan_report=$(awk '
    function marker(line) {
      return line ~ /\/\/[[:space:]]*tight-timeout:[[:space:]]*[^[:space:]]/
    }

    function duration_value(call, value) {
      value = call
      sub(/^.*\(/, "", value)
      sub(/\).*$/, "", value)
      gsub(/_/, "", value)
      return value + 0
    }

    function sub_floor_timeout(line, rest, call) {
      if (line !~ /timeout[[:space:]]*\(/) {
        return 0
      }

      rest = line
      while (match(rest, /from_secs[[:space:]]*\([0-9_]+\)/)) {
        call = substr(rest, RSTART, RLENGTH)
        if (duration_value(call) < 30) {
          return 1
        }
        rest = substr(rest, RSTART + RLENGTH)
      }

      rest = line
      while (match(rest, /from_millis[[:space:]]*\([0-9_]+\)/)) {
        call = substr(rest, RSTART, RLENGTH)
        if (duration_value(call) < 30000) {
          return 1
        }
        rest = substr(rest, RSTART + RLENGTH)
      }

      return 0
    }

    /\/\/[[:space:]]*tight-timeout:/ && !marker($0) {
      print FILENAME ":" FNR ": tight-timeout marker requires a non-empty reason"
    }

    sub_floor_timeout($0) && !marker($0) && !marker(previous) {
      print FILENAME ":" FNR ": inline timeout is below the 30-second floor without a tight-timeout reason"
    }

    { previous = $0 }
  ' "$file")
  if [[ -n "$scan_report" ]]; then
    while IFS= read -r line; do problems+=("$line"); done <<<"$scan_report"
  fi
done < <(find crates apps -type f -name '*.rs' -print0 2>/dev/null)

if ((${#problems[@]} > 0)); then
  printf 'test-timeout-lint: %s\n' "${problems[@]}" >&2
  exit 1
fi

echo "test-timeout lint passed ($scanned_files Rust files)"
