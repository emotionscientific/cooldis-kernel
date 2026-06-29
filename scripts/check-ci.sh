#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

cd "$ROOT"

run "$ROOT/scripts/check-pre-commit.sh"
run "$ROOT/scripts/check-pre-push.sh"
RUSTDOCFLAGS="-D warnings" run cargo doc --workspace --no-deps --locked

printf '\nCooldis CI checks passed.\n'
