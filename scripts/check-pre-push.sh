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

run_verify() {
  printf '\n==> %s\n' "$ROOT/scripts/verify.sh"
  VERLET_VERIFY_MANAGED_CARGO=1 "$ROOT/scripts/verify.sh"
}

cd "$ROOT"

CLIPPY_GATE=(
  -A clippy::all
  -D clippy::correctness
  -D clippy::suspicious
  -D clippy::perf
)

run "$ROOT/scripts/guard-rails.sh" tracked
run_cargo clippy --workspace --all-targets --locked -- "${CLIPPY_GATE[@]}"
run_verify

if [[ "${VERLET_PREPUSH_DOCS:-0}" == "1" ]]; then
  RUSTDOCFLAGS="-D warnings" run_cargo doc --workspace --no-deps --locked
fi

printf '\nVerlet pre-push checks passed.\n'
