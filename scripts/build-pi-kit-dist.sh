#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="$ROOT/dist/pi-kit"
TARGET="wasm32-unknown-unknown"

usage() {
  cat <<'USAGE'
build-pi-kit-dist.sh - build a standalone Pi kit with prebuilt Wasm modules.

Usage:
  scripts/build-pi-kit-dist.sh [--out-dir DIR]

An existing output is replaced only after the complete kit has been staged.
The emitted kit uses bin_path in each member manifest, so
`verlet kit install DIR` does not invoke Cargo.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="${2:?--out-dir requires a value}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ "$OUT_DIR" != /* ]]; then
  OUT_DIR="$ROOT/$OUT_DIR"
fi
out_name="$(basename "$OUT_DIR")"
if [[ -z "$out_name" || "$out_name" == "/" \
  || "$out_name" == "." || "$out_name" == ".." ]]; then
  echo "refusing unsafe output directory: $OUT_DIR" >&2
  exit 1
fi
parent="$(dirname "$OUT_DIR")"
mkdir -p "$parent"
parent="$(cd "$parent" && pwd -P)"
OUT_DIR="$parent/$out_name"
if [[ "$OUT_DIR" == "/" || "$OUT_DIR" == "$ROOT" ]]; then
  echo "refusing unsafe output directory: $OUT_DIR" >&2
  exit 1
fi

modules=(read write edit search)
declare -a artifacts=()

cd "$ROOT"
for module in "${modules[@]}"; do
  manifest="agent-tools/wasm/$module/Cargo.toml"
  scripts/cargo-lane.sh build \
    --locked \
    --manifest-path "$manifest" \
    --target "$TARGET" \
    --release
  target_dir="$(
    scripts/cargo-lane.sh metadata \
      --locked \
      --manifest-path "$manifest" \
      --format-version 1 \
      --no-deps \
      | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
  )"
  if [[ -z "$target_dir" ]]; then
    echo "could not determine Cargo target directory for $module" >&2
    exit 1
  fi
  artifact="$target_dir/$TARGET/release/verlet_tool_wasm_${module}.wasm"
  if [[ ! -s "$artifact" ]]; then
    echo "built Wasm artifact is missing or empty: $artifact" >&2
    exit 1
  fi
  artifacts+=("$artifact")
done

stage="$(mktemp -d "$parent/.pi-kit-dist.XXXXXX")"
backup=
cleanup() {
  status=$?
  if [[ -n "$backup" && ( -e "$backup" || -L "$backup" ) \
    && ! -e "$OUT_DIR" && ! -L "$OUT_DIR" ]]; then
    if ! mv "$backup" "$OUT_DIR"; then
      echo "failed to restore previous output from $backup" >&2
    fi
  fi
  if [[ -d "$stage" ]]; then
    rm -rf "$stage"
  fi
  if [[ -n "$backup" && ( -e "$backup" || -L "$backup" ) ]]; then
    rm -rf "$backup"
  fi
  return "$status"
}
trap cleanup EXIT

cp -R agent-tools/pi-kit "$stage/pi-kit"
for index in "${!modules[@]}"; do
  module="${modules[$index]}"
  member="$stage/pi-kit/$module"
  mkdir -p "$member/bin"
  cp "${artifacts[$index]}" "$member/bin/pi-$module.wasm"
  sed \
    "s|^module_path = \"../../wasm/$module\"$|bin_path = \"bin/pi-$module.wasm\"|" \
    "$member/verlet.tool.toml" >"$member/verlet.tool.toml.tmp"
  mv "$member/verlet.tool.toml.tmp" "$member/verlet.tool.toml"
  bin_path_count="$(grep -c -F "bin_path = \"bin/pi-$module.wasm\"" "$member/verlet.tool.toml" || true)"
  if grep -F 'module_path =' "$member/verlet.tool.toml" >/dev/null \
    || [[ "$bin_path_count" != 1 ]]; then
    echo "failed to rewrite runtime.module_path to runtime.bin_path for $module" >&2
    exit 1
  fi
done

if [[ -e "$OUT_DIR" || -L "$OUT_DIR" ]]; then
  backup="$(mktemp -d "$parent/.pi-kit-previous.XXXXXX")"
  rmdir "$backup"
  mv "$OUT_DIR" "$backup"
fi
mv "$stage/pi-kit" "$OUT_DIR"
rmdir "$stage"
if [[ -n "$backup" ]]; then
  rm -rf "$backup"
  backup=
fi
trap - EXIT
printf 'pi kit distributable: %s\n' "$OUT_DIR"
