#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

cd "$ROOT"

run "$ROOT/scripts/guard-rails.sh" staged
run cargo fmt --all -- --check
run cargo check --workspace --all-targets --locked

printf '\nCooldis pre-commit checks passed.\n'
