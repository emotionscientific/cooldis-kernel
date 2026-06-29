#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_quiet() {
  printf '\n==> %s\n' "$*"
  "$@" >/dev/null
}

cd "$ROOT"

run "$ROOT/scripts/guard-rails.sh" tracked
run cargo fmt --all -- --check
run_quiet cargo metadata --locked --format-version 1 --no-deps

printf '\nCooldis remote CI sentinel passed.\n'
