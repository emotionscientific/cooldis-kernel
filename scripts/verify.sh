#!/usr/bin/env bash
set -euo pipefail

# EMO-512: debug-build async poll frames are large, so deeply nested scenarios
# need more test-thread stack headroom than libtest's default.
export RUST_MIN_STACK=16777216

# The parallel daemon IO tests exhaust macOS's default 256-descriptor soft
# limit (EMFILE flakes in daemon_io tests); raise it before running anything.
soft_fd_limit="$(ulimit -n)"
if [[ "$soft_fd_limit" != "unlimited" && "$soft_fd_limit" -lt 8192 ]]; then
  ulimit -n 8192 || true
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT/scripts/env-compat.sh"
for env_name in \
  VERLET_VERIFY_MANAGED_CARGO \
  VERLET_VERIFY_LIVE_PLUGIN \
  VERLET_VERIFY_LIVE_S3
do
  verlet_env_promote "$env_name"
done

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

# Local verification shares the hook safety contract from EMO-490. CI keeps
# direct Cargo so its clean-runner target and cache behavior remain unchanged.
CARGO_RUNNER=(cargo)
if [[ -z "${CI:-}" || "${VERLET_VERIFY_MANAGED_CARGO:-0}" == "1" ]]; then
  CARGO_RUNNER=("$ROOT/scripts/cargo-lane.sh")
fi

run_cargo() {
  printf '\n==> cargo %s\n' "$*"
  "${CARGO_RUNNER[@]}" "$@"
}

cd "$ROOT"

run "$ROOT/scripts/threat-model-lint.sh"
run "$ROOT/scripts/verlet-name-lint.sh"
run "$ROOT/scripts/test-timeout-lint.sh"
run "$ROOT/scripts/guard-rails-test.sh"
run_cargo fmt --all -- --check
# Same lint set as scripts/release-v1-candidate.sh so the everyday lane
# cannot drift green while the release gate fails (EMO-459).
run_cargo clippy --workspace --all-targets --locked -- -A clippy::all -D clippy::correctness -D clippy::suspicious -D clippy::perf
run_cargo test --workspace --all-targets --locked
run_cargo run --locked --bin verlet-vbash-smoke
run_cargo run --locked --bin verlet-wasm-smoke

if [[ "${VERLET_VERIFY_LIVE_PLUGIN:-0}" == "1" ]]; then
  run_cargo run --locked --bin verlet-plugin-live-smoke
fi

if [[ "${VERLET_VERIFY_LIVE_S3:-0}" == "1" ]]; then
  run_cargo test --locked --test object_store_vfs_real_s3 -- --ignored
fi

printf '\nVerlet verification passed.\n'
