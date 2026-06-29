#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

cd "$ROOT"

CLIPPY_GATE=(
  -A clippy::all
  -D clippy::correctness
  -D clippy::suspicious
  -D clippy::perf
)

run "$ROOT/scripts/guard-rails.sh" tracked
run cargo clippy --workspace --all-targets --locked -- "${CLIPPY_GATE[@]}"
run "$ROOT/scripts/verify.sh"

if [[ "${COOLDIS_PREPUSH_DOCS:-0}" == "1" ]]; then
  RUSTDOCFLAGS="-D warnings" run cargo doc --workspace --no-deps --locked
fi

printf '\nCooldis pre-push checks passed.\n'
