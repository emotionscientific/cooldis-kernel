#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

make_codex_stub() {
  local dir="$1"
  local stub="$dir/codex"
  mkdir -p "$dir"
  cat >"$stub" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
cat >/dev/null || true
printf 'Usage: codex exec [OPTIONS] PROMPT\nRun Codex non-interactively\n'
STUB
  chmod +x "$stub"
  printf '%s' "$stub"
}

cd "$ROOT"

run cargo fmt --all -- --check
# Same lint set as scripts/release-v1-candidate.sh so the everyday lane
# cannot drift green while the release gate fails (EMO-459).
run cargo clippy --workspace --all-targets --locked -- -A clippy::all -D clippy::correctness -D clippy::suspicious -D clippy::perf
run cargo test --workspace --all-targets --locked

if [[ -z "${COOLDIS_CODEX_BIN:-}" ]]; then
  if command -v codex >/dev/null 2>&1; then
    export COOLDIS_CODEX_BIN="$(command -v codex)"
  else
    export COOLDIS_CODEX_BIN="$(make_codex_stub "${TMPDIR:-/tmp}/cooldis-verify-codex-stub")"
  fi
fi

run cargo run --locked --bin cooldis-live-smoke
run cargo run --locked --bin cooldis-vbash-smoke
run cargo run --locked --bin cooldis-wasm-smoke

if [[ "${COOLDIS_VERIFY_LIVE_PLUGIN:-0}" == "1" ]]; then
  run cargo run --locked --bin cooldis-plugin-live-smoke
fi

if [[ "${COOLDIS_VERIFY_LIVE_S3:-0}" == "1" ]]; then
  run cargo test --locked --test object_store_vfs_real_s3 -- --ignored
fi

printf '\nCooldis verification passed.\n'
