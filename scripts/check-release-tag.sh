#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAG="${1:-${GITHUB_REF_NAME:-}}"

usage() {
  cat <<'USAGE'
check-release-tag.sh - validate a Cooldis release tag against the kernel version.

Usage:
  scripts/check-release-tag.sh v0.1.0
  scripts/check-release-tag.sh v0.1.0-rc.1

The tag must be v<crates/cooldis-kernel version>, optionally followed by a
SemVer prerelease suffix such as -rc.1.
USAGE
}

if [[ "${TAG:-}" == "--help" || "${TAG:-}" == "-h" ]]; then
  usage
  exit 0
fi

if [[ -z "$TAG" ]]; then
  echo "release tag is required" >&2
  usage >&2
  exit 2
fi

VERSION="$(
  sed -n 's/^version = "\(.*\)"/\1/p' "$ROOT/crates/cooldis-kernel/Cargo.toml" | head -n 1
)"
if [[ -z "$VERSION" ]]; then
  echo "could not read cooldis version from crates/cooldis-kernel/Cargo.toml" >&2
  exit 1
fi

case "$TAG" in
  "v$VERSION"|"v$VERSION"-*)
    ;;
  *)
    echo "release tag '$TAG' does not match kernel version '$VERSION'" >&2
    echo "expected v$VERSION or v$VERSION-<prerelease>" >&2
    exit 1
    ;;
esac

echo "Release tag OK: $TAG"
