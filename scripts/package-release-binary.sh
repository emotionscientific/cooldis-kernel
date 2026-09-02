#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${VERLET_RELEASE_OUT_DIR:-$ROOT/dist}"
SKIP_BUILD=0
SKIP_CONSOLE_BUILD="${VERLET_SKIP_CONSOLE_BUILD:-0}"
TARGET="${VERLET_RELEASE_TARGET:-}"

usage() {
  cat <<'USAGE'
package-release-binary.sh - build and package Verlet release binaries.

Usage:
  scripts/package-release-binary.sh [--out-dir DIR] [--target TRIPLE] [--skip-build] [--skip-console-build]

The package contains the public process entrypoints:
  - verlet
  - verlet-acp-agent
  - verlet-mcp-server
  - share/verlet/console static assets
  - share/man/man1/verlet.1 manual page

It writes:
  DIR/verlet-<version>-<target-triple>.tar.gz
  DIR/verlet-<version>-<target-triple>.tar.gz.sha256
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="${2:?--out-dir requires a value}"
      shift 2
      ;;
    --target)
      TARGET="${2:?--target requires a value}"
      shift 2
      ;;
    --skip-build)
      SKIP_BUILD=1
      shift
      ;;
    --skip-console-build)
      SKIP_CONSOLE_BUILD=1
      shift
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

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

sha256_file() {
  local dir="$1"
  local file="$2"

  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$dir" && sha256sum "$file" >"$file.sha256")
  elif command -v shasum >/dev/null 2>&1; then
    (cd "$dir" && shasum -a 256 "$file" >"$file.sha256")
  else
    echo "sha256sum or shasum is required" >&2
    exit 1
  fi
}

cd "$ROOT"

source scripts/release-version.sh

VERSION="$(
  read_verlet_workspace_version Cargo.toml
)"
if [[ -z "$VERSION" ]]; then
  echo "could not read verlet version from the workspace Cargo.toml" >&2
  exit 1
fi

RELEASE_VERSION="${VERLET_RELEASE_VERSION:-}"
if [[ -z "$RELEASE_VERSION" && "${GITHUB_REF_NAME:-}" == v* ]]; then
  RELEASE_VERSION="${GITHUB_REF_NAME#v}"
fi
if [[ -z "$RELEASE_VERSION" ]]; then
  RELEASE_VERSION="$VERSION"
fi
case "$RELEASE_VERSION" in
  "$VERSION"|"$VERSION"-*) ;;
  *)
    echo "release version '$RELEASE_VERSION' does not match crate version '$VERSION'" >&2
    echo "expected $VERSION or $VERSION-<prerelease>" >&2
    exit 1
    ;;
esac

HOST_TRIPLE="$(rustc -vV | awk '/^host:/ { print $2 }')"
if [[ -z "$HOST_TRIPLE" ]]; then
  echo "could not determine Rust host triple" >&2
  exit 1
fi
if [[ -z "$TARGET" ]]; then
  TARGET="$HOST_TRIPLE"
fi

TARGET_DIR="$(
  cargo metadata --locked --format-version 1 --no-deps \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
)"
if [[ -z "$TARGET_DIR" ]]; then
  echo "could not determine Cargo target directory" >&2
  exit 1
fi

BINS=(
  verlet
  verlet-acp-agent
  verlet-mcp-server
)

if [[ "$SKIP_BUILD" != "1" ]]; then
  build=(cargo build --locked --release)
  if [[ "$TARGET" != "$HOST_TRIPLE" ]]; then
    build+=(--target "$TARGET")
  fi
  for bin in "${BINS[@]}"; do
    build+=(--bin "$bin")
  done
  run "${build[@]}"
fi

if [[ "$SKIP_CONSOLE_BUILD" != "1" ]]; then
  run "$ROOT/scripts/build-console-assets.sh"
fi

CONSOLE_DIST="$ROOT/apps/console/dist"
if [[ ! -f "$CONSOLE_DIST/index.html" || ! -d "$CONSOLE_DIST/assets" ]]; then
  echo "missing console assets in $CONSOLE_DIST" >&2
  echo "run scripts/build-console-assets.sh or pass --skip-console-build only after building them" >&2
  exit 1
fi

MANUAL="$ROOT/docs/man/verlet.1"
if [[ ! -f "$MANUAL" || -L "$MANUAL" || ! -s "$MANUAL" ]]; then
  echo "missing manual page: $MANUAL" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"

if [[ "$TARGET" == "$HOST_TRIPLE" ]]; then
  RELEASE_BIN_DIR="$TARGET_DIR/release"
else
  RELEASE_BIN_DIR="$TARGET_DIR/$TARGET/release"
fi

PACKAGE="verlet-$RELEASE_VERSION-$TARGET"
STAGE="$OUT_DIR/$PACKAGE"
ARCHIVE="$PACKAGE.tar.gz"

rm -rf "$STAGE"
mkdir -p "$STAGE"

for bin in "${BINS[@]}"; do
  src="$RELEASE_BIN_DIR/$bin"
  if [[ ! -x "$src" ]]; then
    echo "missing release binary: $src" >&2
    echo "run without --skip-build or build the release binaries first" >&2
    exit 1
  fi
  cp "$src" "$STAGE/$bin"
  chmod 0755 "$STAGE/$bin"
done

mkdir -p "$STAGE/share/verlet/console"
cp -R "$CONSOLE_DIST/." "$STAGE/share/verlet/console/"
mkdir -p "$STAGE/share/man/man1"
cp "$MANUAL" "$STAGE/share/man/man1/verlet.1"

cat >"$STAGE/README.txt" <<EOF
Verlet $RELEASE_VERSION
Target: $TARGET

Binaries:
  verlet              Verlet CLI
  verlet-acp-agent    ACP stdio adapter for hosts that launch an agent process
  verlet-mcp-server   MCP stdio adapter for daemon-backed orchestration

Console:
  ./verlet console

Manual:
  man ./share/man/man1/verlet.1

Smoke:
  ./verlet --help
  ./verlet console --help
  ./verlet-acp-agent --version
  ./verlet-mcp-server --help
EOF

rm -f "$OUT_DIR/$ARCHIVE" "$OUT_DIR/$ARCHIVE.sha256"
(cd "$OUT_DIR" && tar -czf "$ARCHIVE" "$PACKAGE")
rm -rf "$STAGE"
sha256_file "$OUT_DIR" "$ARCHIVE"

printf '\nPackaged release artifacts:\n'
printf '  %s\n' "$OUT_DIR/$ARCHIVE"
printf '  %s\n' "$OUT_DIR/$ARCHIVE.sha256"
