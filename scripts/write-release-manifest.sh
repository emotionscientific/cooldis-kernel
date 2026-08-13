#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${VERLET_RELEASE_OUT_DIR:-$ROOT/dist}"
REPO="${GITHUB_REPOSITORY:-emotionscientific/cooldis-kernel}"
TAG="${GITHUB_REF_NAME:-}"
CHANNEL="${VERLET_RELEASE_CHANNEL:-stable}"
BASE_URL=""

usage() {
  cat <<'USAGE'
write-release-manifest.sh - write latest.json for Verlet release assets.

Usage:
  scripts/write-release-manifest.sh [--out-dir DIR] [--repo OWNER/REPO] [--tag TAG] [--channel NAME] [--base-url URL]

The manifest is consumed by scripts/install.sh and future update tooling.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="${2:?--out-dir requires a value}"
      shift 2
      ;;
    --repo)
      REPO="${2:?--repo requires a value}"
      shift 2
      ;;
    --tag)
      TAG="${2:?--tag requires a value}"
      shift 2
      ;;
    --channel)
      CHANNEL="${2:?--channel requires a value}"
      shift 2
      ;;
    --base-url)
      BASE_URL="${2:?--base-url requires a value}"
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

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

sha256_from_file() {
  awk '{ print $1; exit }' "$1"
}

VERSION="$(
  sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/crates/verlet-kernel/Cargo.toml" | head -n 1
)"
if [[ -z "$VERSION" ]]; then
  echo "could not read verlet version from crates/verlet-kernel/Cargo.toml" >&2
  exit 1
fi

if [[ -z "$TAG" ]]; then
  TAG="v$VERSION"
fi

RELEASE_VERSION="$VERSION"
if [[ "$TAG" == v* ]]; then
  RELEASE_VERSION="${TAG#v}"
fi

if [[ -z "$BASE_URL" ]]; then
  BASE_URL="https://github.com/$REPO/releases/download/$TAG"
fi

OUT_DIR="$(cd "$OUT_DIR" && pwd)"
MANIFEST="$OUT_DIR/latest.json"
GENERATED_AT="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"

ARCHIVES=()
while IFS= read -r archive; do
  ARCHIVES+=("$archive")
done < <(find "$OUT_DIR" -maxdepth 1 -name "verlet-*.tar.gz" -type f | sort)
if [[ "${#ARCHIVES[@]}" -eq 0 ]]; then
  echo "no verlet release archives found under $OUT_DIR" >&2
  exit 1
fi

{
  printf '{\n'
  printf '  "schema": 1,\n'
  printf '  "name": "verlet",\n'
  printf '  "version": "%s",\n' "$(json_escape "$RELEASE_VERSION")"
  printf '  "channel": "%s",\n' "$(json_escape "$CHANNEL")"
  printf '  "repository": "%s",\n' "$(json_escape "$REPO")"
  printf '  "tag": "%s",\n' "$(json_escape "$TAG")"
  printf '  "generated_at": "%s",\n' "$(json_escape "$GENERATED_AT")"
  printf '  "install": { "name": "install.sh", "url": "%s/install.sh" },\n' "$(json_escape "$BASE_URL")"
  printf '  "artifacts": [\n'
  first=1
  for archive in "${ARCHIVES[@]}"; do
    name="$(basename "$archive")"
    checksum="$archive.sha256"
    if [[ ! -f "$checksum" ]]; then
      echo "missing checksum for $archive" >&2
      exit 1
    fi
    target="${name#verlet-$RELEASE_VERSION-}"
    target="${target%.tar.gz}"
    if [[ "$target" == "$name" || -z "$target" ]]; then
      echo "archive name does not match verlet-$RELEASE_VERSION-<target>.tar.gz: $name" >&2
      exit 1
    fi
    sha256="$(sha256_from_file "$checksum")"
    if [[ -z "$sha256" ]]; then
      echo "empty checksum file: $checksum" >&2
      exit 1
    fi
    if [[ "$first" -eq 0 ]]; then
      printf ',\n'
    fi
    first=0
    printf '    { "target": "%s", "name": "%s", "sha256": "%s", "url": "%s/%s" }' \
      "$(json_escape "$target")" \
      "$(json_escape "$name")" \
      "$(json_escape "$sha256")" \
      "$(json_escape "$BASE_URL")" \
      "$(json_escape "$name")"
  done
  printf '\n'
  printf '  ]\n'
  printf '}\n'
} >"$MANIFEST"

printf 'Wrote release manifest: %s\n' "$MANIFEST"
