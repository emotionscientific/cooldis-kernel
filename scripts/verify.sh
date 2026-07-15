#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

cd "$ROOT"

run cargo fmt --all -- --check
# Same lint set as scripts/release-v1-candidate.sh so the everyday lane
# cannot drift green while the release gate fails (EMO-459).
run cargo clippy --workspace --all-targets --locked -- -A clippy::all -D clippy::correctness -D clippy::suspicious -D clippy::perf
run cargo test --workspace --all-targets --locked
run cargo run --locked --bin cooldis-vbash-smoke
run cargo run --locked --bin cooldis-wasm-smoke

if [[ "${COOLDIS_VERIFY_LIVE_PLUGIN:-0}" == "1" ]]; then
  run cargo run --locked --bin cooldis-plugin-live-smoke
fi

if [[ "${COOLDIS_VERIFY_LIVE_S3:-0}" == "1" ]]; then
  run cargo test --locked --test object_store_vfs_real_s3 -- --ignored
fi

printf '\nCooldis verification passed.\n'
