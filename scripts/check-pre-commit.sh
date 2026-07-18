#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

# Hooks must not use .cargo/config.toml's shared sibling target directly. During
# the EMO-490 incident, concurrent worktrees mixed fresh and stale rlibs there.
run_cargo() {
  printf '\n==> cargo %s\n' "$*"
  "$ROOT/scripts/cargo-lane.sh" "$@"
}

cd "$ROOT"

run "$ROOT/scripts/guard-rails.sh" staged
run_cargo fmt --all -- --check
run_cargo check --workspace --all-targets --locked

printf '\nCooldis pre-commit checks passed.\n'
