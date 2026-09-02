#!/bin/sh
set -eu

REPO="${VERLET_REPO:-emotionscientific/verlet-kernel}"
VERSION="${VERLET_VERSION:-}"
TARGET="${VERLET_TARGET:-}"
BASE_URL="${VERLET_BASE_URL:-}"
INSTALL_ROOT="${VERLET_INSTALL_ROOT:-"$HOME/.verlet"}"
BIN_DIR="${VERLET_BIN_DIR:-"$HOME/.local/bin"}"
MAN_DIR="${VERLET_MAN_DIR:-${XDG_DATA_HOME:-"$HOME/.local/share"}/man/man1}"
ARCHIVE="${VERLET_ARCHIVE:-}"
CHECKSUM="${VERLET_CHECKSUM:-}"
FORCE=0

usage() {
  cat <<'USAGE'
install.sh - install or update the Verlet CLI/kernel runtime.

Usage:
  sh install.sh [options]
  curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/latest/download/install.sh | sh

Options:
  --version VERSION       Install a specific release version, for example 0.1.0.
  --target TARGET         Override target triple detection.
  --repo OWNER/REPO       GitHub repository. Default: emotionscientific/verlet-kernel.
  --base-url URL          Release asset base URL.
  --install-root DIR      Versioned install root. Default: ~/.verlet.
  --bin-dir DIR           Symlink directory. Default: ~/.local/bin.
  --man-dir DIR           Manual symlink directory.
                          Default: ${XDG_DATA_HOME:-$HOME/.local/share}/man/man1.
  --archive FILE          Install from a local release archive.
  --checksum FILE         Checksum file for --archive.
  --force                 Replace non-symlink files in --bin-dir and --man-dir.
  -h, --help              Show this help.

Environment overrides use the VERLET_* names matching the option names.
USAGE
}

die() {
  echo "verlet install: $*" >&2
  exit 1
}

require_replaceable_link() {
  link="$1"
  if { [ -e "$link" ] || [ -L "$link" ]; } && [ ! -L "$link" ] && [ "$FORCE" != "1" ]; then
    die "refusing to replace non-symlink $link; pass --force to replace it"
  fi
}

need_value() {
  option="$1"
  value="${2:-}"
  if [ -z "$value" ]; then
    die "$option requires a value"
  fi
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --version)
      need_value "$1" "${2:-}"
      VERSION="$2"
      shift 2
      ;;
    --target)
      need_value "$1" "${2:-}"
      TARGET="$2"
      shift 2
      ;;
    --repo)
      need_value "$1" "${2:-}"
      REPO="$2"
      shift 2
      ;;
    --base-url)
      need_value "$1" "${2:-}"
      BASE_URL="$2"
      shift 2
      ;;
    --install-root)
      need_value "$1" "${2:-}"
      INSTALL_ROOT="$2"
      shift 2
      ;;
    --bin-dir)
      need_value "$1" "${2:-}"
      BIN_DIR="$2"
      shift 2
      ;;
    --man-dir)
      need_value "$1" "${2:-}"
      MAN_DIR="$2"
      shift 2
      ;;
    --archive)
      need_value "$1" "${2:-}"
      ARCHIVE="$2"
      shift 2
      ;;
    --checksum)
      need_value "$1" "${2:-}"
      CHECKSUM="$2"
      shift 2
      ;;
    --force)
      FORCE=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1"
      ;;
  esac
done

detect_target() {
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  arch="$(uname -m | tr '[:upper:]' '[:lower:]')"
  case "$os:$arch" in
    darwin:arm64|darwin:aarch64)
      echo "aarch64-apple-darwin"
      ;;
    darwin:x86_64|darwin:amd64)
      echo "x86_64-apple-darwin"
      ;;
    linux:x86_64|linux:amd64)
      echo "x86_64-unknown-linux-gnu"
      ;;
    linux:aarch64|linux:arm64)
      echo "aarch64-unknown-linux-gnu"
      ;;
    *)
      die "unsupported platform: $(uname -s) $(uname -m); pass --target explicitly"
      ;;
  esac
}

download_file() {
  url="$1"
  out="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$out"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$out" "$url"
  else
    die "curl or wget is required to download release assets"
  fi
}

sha256_verify() {
  archive_file="$1"
  checksum_file="$2"
  archive_dir="$(cd "$(dirname "$archive_file")" && pwd)"
  checksum_abs="$(cd "$(dirname "$checksum_file")" && pwd)/$(basename "$checksum_file")"
  if command -v sha256sum >/dev/null 2>&1; then
    (cd "$archive_dir" && sha256sum -c "$checksum_abs")
  elif command -v shasum >/dev/null 2>&1; then
    (cd "$archive_dir" && shasum -a 256 -c "$checksum_abs")
  else
    die "sha256sum or shasum is required"
  fi
}

manifest_value_for_target() {
  manifest="$1"
  target="$2"
  key="$3"
  grep "\"target\": \"$target\"" "$manifest" \
    | sed -n "s/.*\"$key\": \"\\([^\"]*\\)\".*/\\1/p" \
    | head -n 1
}

if [ -z "$TARGET" ]; then
  TARGET="$(detect_target)"
fi

TMPDIR_ROOT="${TMPDIR:-/tmp}"
WORK_DIR="$(mktemp -d "$TMPDIR_ROOT/verlet-install.XXXXXX")"
trap 'rm -rf "$WORK_DIR"' EXIT HUP INT TERM

if [ -n "$ARCHIVE" ]; then
  [ -f "$ARCHIVE" ] || die "archive not found: $ARCHIVE"
  ARCHIVE_PATH="$(cd "$(dirname "$ARCHIVE")" && pwd)/$(basename "$ARCHIVE")"
  ARCHIVE_NAME="$(basename "$ARCHIVE_PATH")"
  if [ -z "$CHECKSUM" ] && [ -f "$ARCHIVE_PATH.sha256" ]; then
    CHECKSUM="$ARCHIVE_PATH.sha256"
  fi
  if [ -z "$CHECKSUM" ]; then
    die "--archive requires --checksum or a sibling .sha256 file"
  fi
  [ -f "$CHECKSUM" ] || die "checksum not found: $CHECKSUM"
  sha256_verify "$ARCHIVE_PATH" "$CHECKSUM"
else
  if [ -z "$BASE_URL" ]; then
    if [ -n "$VERSION" ]; then
      case "$VERSION" in
        v*) TAG="$VERSION" ;;
        *) TAG="v$VERSION" ;;
      esac
      BASE_URL="https://github.com/$REPO/releases/download/$TAG"
    else
      BASE_URL="https://github.com/$REPO/releases/latest/download"
    fi
  fi

  if [ -z "$VERSION" ]; then
    MANIFEST="$WORK_DIR/latest.json"
    download_file "$BASE_URL/latest.json" "$MANIFEST"
    ARCHIVE_NAME="$(manifest_value_for_target "$MANIFEST" "$TARGET" name)"
    ARCHIVE_SHA="$(manifest_value_for_target "$MANIFEST" "$TARGET" sha256)"
    ARCHIVE_URL="$(manifest_value_for_target "$MANIFEST" "$TARGET" url)"
    [ -n "$ARCHIVE_NAME" ] || die "latest.json has no artifact for target $TARGET"
    [ -n "$ARCHIVE_SHA" ] || die "latest.json has no sha256 for target $TARGET"
    [ -n "$ARCHIVE_URL" ] || ARCHIVE_URL="$BASE_URL/$ARCHIVE_NAME"
    printf '%s  %s\n' "$ARCHIVE_SHA" "$ARCHIVE_NAME" >"$WORK_DIR/$ARCHIVE_NAME.sha256"
  else
    ARCHIVE_NAME="verlet-$VERSION-$TARGET.tar.gz"
    ARCHIVE_URL="$BASE_URL/$ARCHIVE_NAME"
    download_file "$BASE_URL/$ARCHIVE_NAME.sha256" "$WORK_DIR/$ARCHIVE_NAME.sha256"
  fi

  ARCHIVE_PATH="$WORK_DIR/$ARCHIVE_NAME"
  download_file "$ARCHIVE_URL" "$ARCHIVE_PATH"
  sha256_verify "$ARCHIVE_PATH" "$WORK_DIR/$ARCHIVE_NAME.sha256"
fi

EXTRACT_DIR="$WORK_DIR/extract"
mkdir -p "$EXTRACT_DIR"
tar -xzf "$ARCHIVE_PATH" -C "$EXTRACT_DIR"

PACKAGE_COUNT="$(find "$EXTRACT_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
[ "$PACKAGE_COUNT" = "1" ] || die "archive must contain exactly one top-level directory"
PACKAGE_DIR="$(find "$EXTRACT_DIR" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
PACKAGE_NAME="$(basename "$PACKAGE_DIR")"

for bin in verlet verlet-acp-agent verlet-mcp-server; do
  [ -x "$PACKAGE_DIR/$bin" ] || die "archive is missing executable $bin"
done
manual_payload="$PACKAGE_DIR/share/man/man1/verlet.1"
[ -f "$manual_payload" ] && [ ! -L "$manual_payload" ] && [ -s "$manual_payload" ] \
  || die "archive is missing regular manual share/man/man1/verlet.1"

VERSION_DIR="$INSTALL_ROOT/versions/$PACKAGE_NAME"
mkdir -p "$INSTALL_ROOT/versions" "$BIN_DIR" "$MAN_DIR"
for bin in verlet verlet-acp-agent verlet-mcp-server; do
  require_replaceable_link "$BIN_DIR/$bin"
done
manual_link="$MAN_DIR/verlet.1"
require_replaceable_link "$manual_link"

rm -rf "$VERSION_DIR.tmp"
mv "$PACKAGE_DIR" "$VERSION_DIR.tmp"
rm -rf "$VERSION_DIR"
mv "$VERSION_DIR.tmp" "$VERSION_DIR"
rm -f "$INSTALL_ROOT/current"
ln -s "$VERSION_DIR" "$INSTALL_ROOT/current"

for bin in verlet verlet-acp-agent verlet-mcp-server; do
  link="$BIN_DIR/$bin"
  if [ -e "$link" ] || [ -L "$link" ]; then
    rm -f "$link"
  fi
  ln -s "$INSTALL_ROOT/current/$bin" "$link"
done

# Remove only the symlink shape created by the v0.3 installer. Preserve a
# user-owned file or an unrelated symlink that happens to use the old name.
LEGACY_BIN="cool""dis"
legacy_link="$BIN_DIR/$LEGACY_BIN"
if [ -L "$legacy_link" ]; then
  case "$(readlink "$legacy_link")" in
    */current/"$LEGACY_BIN") rm -f "$legacy_link" ;;
  esac
fi

if [ -e "$manual_link" ] || [ -L "$manual_link" ]; then
  rm -f "$manual_link"
fi
ln -s "$INSTALL_ROOT/current/share/man/man1/verlet.1" "$manual_link"

echo "Installed Verlet:"
echo "  $VERSION_DIR"
echo "Linked binaries:"
echo "  $BIN_DIR/verlet"
echo "  $BIN_DIR/verlet-acp-agent"
echo "  $BIN_DIR/verlet-mcp-server"
echo "Linked manual:"
echo "  $MAN_DIR/verlet.1"

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *)
    echo
    echo "Add this to your shell profile to use verlet from any directory:"
    echo "  export PATH=\"$BIN_DIR:\$PATH\""
    ;;
esac

echo
echo "Get started:"
echo "  verlet console"
echo "  verlet chat"
echo "  verlet init <name>"
echo "  man verlet"
