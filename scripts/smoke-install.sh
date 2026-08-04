#!/usr/bin/env bash
set -euo pipefail

ARCHIVE="${1:-}"

usage() {
  cat <<'USAGE'
smoke-install.sh - smoke-test the Verlet installer against a release archive.

Usage:
  scripts/smoke-install.sh path/to/verlet-<version>-<target>.tar.gz
USAGE
}

if [[ -z "$ARCHIVE" || "$ARCHIVE" == "--help" || "$ARCHIVE" == "-h" ]]; then
  usage
  exit 2
fi

if [[ ! -f "$ARCHIVE" ]]; then
  echo "archive not found: $ARCHIVE" >&2
  exit 1
fi

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_fails() {
  printf '\n==> expect failure: %s\n' "$*"
  if "$@" >/dev/null 2>&1; then
    echo "command unexpectedly succeeded: $*" >&2
    exit 1
  fi
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/verlet-install-smoke.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

INSTALL_ROOT="$TMP/home/.verlet"
BIN_DIR="$TMP/bin"
MAN_DIR="$TMP/share/man/man1"

run scripts/install.sh \
  --archive "$ARCHIVE" \
  --install-root "$INSTALL_ROOT" \
  --bin-dir "$BIN_DIR" \
  --man-dir "$MAN_DIR"

run scripts/install.sh \
  --archive "$ARCHIVE" \
  --install-root "$INSTALL_ROOT" \
  --bin-dir "$BIN_DIR" \
  --man-dir "$MAN_DIR"

touch "$INSTALL_ROOT/current/.refusal-sentinel"
rm "$MAN_DIR/verlet.1"
printf 'existing manual\n' >"$MAN_DIR/verlet.1"
run_fails scripts/install.sh \
  --archive "$ARCHIVE" \
  --install-root "$INSTALL_ROOT" \
  --bin-dir "$BIN_DIR" \
  --man-dir "$MAN_DIR"

if [[ ! -f "$INSTALL_ROOT/current/.refusal-sentinel" ]]; then
  echo "installer mutated the active version before refusing a non-symlink manual" >&2
  exit 1
fi

run scripts/install.sh \
  --archive "$ARCHIVE" \
  --install-root "$INSTALL_ROOT" \
  --bin-dir "$BIN_DIR" \
  --man-dir "$MAN_DIR" \
  --force

export PATH="$BIN_DIR:$PATH"

if [[ "$(command -v verlet)" != "$BIN_DIR/verlet" ]]; then
  echo "verlet did not resolve through smoke bin dir" >&2
  exit 1
fi

for bin in verlet verlet-acp-agent verlet-mcp-server; do
  if [[ ! -L "$BIN_DIR/$bin" ]]; then
    echo "expected installer symlink: $BIN_DIR/$bin" >&2
    exit 1
  fi
done

if [[ ! -L "$MAN_DIR/verlet.1" ]]; then
  echo "expected installer manual symlink: $MAN_DIR/verlet.1" >&2
  exit 1
fi

if [[ ! -f "$INSTALL_ROOT/current/share/man/man1/verlet.1" \
  || -L "$INSTALL_ROOT/current/share/man/man1/verlet.1" \
  || ! -s "$INSTALL_ROOT/current/share/man/man1/verlet.1" ]]; then
  echo "expected installed manual under $INSTALL_ROOT/current/share/man/man1" >&2
  exit 1
fi

if command -v man >/dev/null 2>&1; then
  printf '\n==> man %s\n' "$MAN_DIR/verlet.1"
  MANPAGER=cat PAGER=cat GROFF_NO_SGR=1 man "$MAN_DIR/verlet.1" >/dev/null
fi

if [[ ! -f "$INSTALL_ROOT/current/share/verlet/console/index.html" || ! -d "$INSTALL_ROOT/current/share/verlet/console/assets" ]]; then
  echo "expected installed console assets under $INSTALL_ROOT/current/share/verlet/console" >&2
  exit 1
fi

run verlet --version
run verlet --help
run verlet console --help
run verlet-acp-agent --version
run verlet-mcp-server --help

printf '\nVerlet installer smoke passed: %s\n' "$ARCHIVE"
