#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source_url=https://models.dev/api.json
snapshot="$repo_root/crates/verlet-kernel/data/model-catalog.json"

usage() {
  printf 'usage: scripts/update-model-catalog.sh [SOURCE_URL] [--output FILE]\n'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --output)
      snapshot=${2:?--output requires a value}
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    -*)
      printf 'update-model-catalog: unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ "$source_url" != "https://models.dev/api.json" ]]; then
        printf 'update-model-catalog: unexpected extra argument: %s\n' "$1" >&2
        usage >&2
        exit 2
      fi
      source_url=$1
      shift
      ;;
  esac
done

for command_name in curl cargo; do
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "update-model-catalog: required command not found: $command_name" >&2
    exit 1
  fi
done

upstream=$(mktemp "${TMPDIR:-/tmp}/verlet-model-catalog.XXXXXX")
cleanup() {
  rm -f "$upstream"
}
trap cleanup EXIT

curl --fail --silent --show-error --location \
  --connect-timeout 5 --max-time 30 --max-filesize 8388608 \
  --output "$upstream" -- "$source_url"

# The snapshot is written by the same Rust normalization code the runtime
# refresh uses, so the checked-in data cannot drift from the runtime rules.
VERLET_MODEL_CATALOG_REGEN_INPUT="$upstream" \
  VERLET_MODEL_CATALOG_REGEN_OUTPUT="$snapshot" \
  "$repo_root/scripts/cargo-lane.sh" test \
  --manifest-path "$repo_root/Cargo.toml" -p verlet --lib \
  adapters::app_server::model_catalog::tests::regenerate_built_in_snapshot_from_env \
  -- --ignored --exact
