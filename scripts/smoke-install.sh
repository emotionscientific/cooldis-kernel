#!/usr/bin/env bash
set -euo pipefail

ARCHIVE="${1:-}"

usage() {
  cat <<'USAGE'
smoke-install.sh - smoke-test the Cooldis installer against a release archive.

Usage:
  scripts/smoke-install.sh path/to/cooldis-<version>-<target>.tar.gz
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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/cooldis-install-smoke.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

INSTALL_ROOT="$TMP/home/.cooldis"
BIN_DIR="$TMP/bin"

run scripts/install.sh \
  --archive "$ARCHIVE" \
  --install-root "$INSTALL_ROOT" \
  --bin-dir "$BIN_DIR"

export PATH="$BIN_DIR:$PATH"

if [[ "$(command -v cooldis)" != "$BIN_DIR/cooldis" ]]; then
  echo "cooldis did not resolve through smoke bin dir" >&2
  exit 1
fi

for bin in cooldis cooldis-acp-agent cooldis-mcp-server; do
  if [[ ! -L "$BIN_DIR/$bin" ]]; then
    echo "expected installer symlink: $BIN_DIR/$bin" >&2
    exit 1
  fi
done

run cooldis --version
run cooldis --help
run cooldis-acp-agent --version
run cooldis-mcp-server --help

printf '\nCooldis installer smoke passed: %s\n' "$ARCHIVE"
