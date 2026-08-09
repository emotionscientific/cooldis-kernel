#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALLOWLIST="$ROOT/scripts/verlet-name-allowlist.tsv"
PATTERN="cool""dis"
violations=0
source_pattern_regex=""

is_rust_source() {
  [[ "$1" == crates/*/src/*.rs || "$1" == crates/*/src/*/*.rs || "$1" == crates/*/src/*/*/*.rs ]]
}

if [[ ! -f "$ALLOWLIST" ]]; then
  printf 'error: missing Verlet rename allowlist: %s\n' "$ALLOWLIST" >&2
  exit 1
fi

while IFS=$'\t' read -r allowed_path reason extra; do
  [[ -z "$allowed_path" || "$allowed_path" == \#* ]] && continue
  if [[ "$allowed_path" == "@source-pattern" ]]; then
    if [[ -z "$reason" || -z "$extra" ]]; then
      printf 'error: source-pattern allowlist entry lacks a pattern or reason\n' >&2
      violations=1
      continue
    fi
    if ! git -C "$ROOT" grep -I -E -q "$reason" -- 'crates/*/src/*.rs' 'crates/*/src/**/*.rs'; then
      printf 'error: stale source-pattern allowlist entry has no match: %s\n' "$reason" >&2
      violations=1
    fi
    source_pattern_regex="${source_pattern_regex:+$source_pattern_regex|}($reason)"
    continue
  fi
  if [[ -z "$reason" ]]; then
    printf 'error: allowlist entry lacks a reason: %s\n' "$allowed_path" >&2
    violations=1
    continue
  fi
  if [[ ! -f "$ROOT/$allowed_path" ]]; then
    printf 'error: allowlist path does not exist: %s\n' "$allowed_path" >&2
    violations=1
    continue
  fi
  if ! LC_ALL=C grep -a -i -q "$PATTERN" "$ROOT/$allowed_path"; then
    printf 'error: stale allowlist entry has no legacy product token: %s\n' \
      "$allowed_path" >&2
    violations=1
  fi
done <"$ALLOWLIST"

while IFS= read -r -d '' tracked_path; do
  [[ -f "$ROOT/$tracked_path" ]] || continue
  if ! LC_ALL=C grep -a -i -q "$PATTERN" "$ROOT/$tracked_path"; then
    continue
  fi
  if is_rust_source "$tracked_path"; then
    while IFS= read -r source_line; do
      printf 'error: non-frozen legacy identifier in %s: %s\n' \
        "$tracked_path" "$source_line" >&2
      violations=1
    done < <(
      LC_ALL=C grep -a -i "$PATTERN" "$ROOT/$tracked_path" \
        | LC_ALL=C grep -a -E -v "$source_pattern_regex" \
        || true
    )
    continue
  fi
  if awk -F '\t' -v expected="$tracked_path" \
    '$1 == expected { found = 1 } END { exit !found }' "$ALLOWLIST"
  then
    continue
  fi

  printf 'error: non-allowlisted legacy product token in %s\n' "$tracked_path" >&2
  LC_ALL=C grep -a -H -i -n "$PATTERN" "$ROOT/$tracked_path" >&2 || true
  violations=1
done < <(git -C "$ROOT" ls-files -z --cached --others --exclude-standard)

if ((violations != 0)); then
  exit 1
fi

printf 'Verlet rename lint passed.\n'
