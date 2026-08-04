#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ALLOWLIST="$ROOT/scripts/verlet-name-allowlist.tsv"
PATTERN="cool""dis"
violations=0

if [[ ! -f "$ALLOWLIST" ]]; then
  printf 'error: missing Verlet rename allowlist: %s\n' "$ALLOWLIST" >&2
  exit 1
fi

while IFS=$'\t' read -r allowed_path reason; do
  [[ -z "$allowed_path" || "$allowed_path" == \#* ]] && continue
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
