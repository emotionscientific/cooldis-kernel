#!/usr/bin/env bash
set -euo pipefail

ARCHIVE="${1:-}"

usage() {
  cat <<'USAGE'
smoke-release-archive.sh - smoke-test a packaged Verlet release archive.

Usage:
  scripts/smoke-release-archive.sh path/to/verlet-<version>-<target>.tar.gz
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

run_quiet() {
  printf '\n==> %s\n' "$*"
  "$@" >/dev/null
}

run_fails() {
  printf '\n==> expect failure: %s\n' "$*"
  if "$@" >/dev/null 2>&1; then
    echo "command unexpectedly succeeded: $*" >&2
    exit 1
  fi
}

TMP="$(mktemp -d "${TMPDIR:-/tmp}/verlet-release-smoke.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

run tar -xzf "$ARCHIVE" -C "$TMP"

entries=("$TMP"/*)
if [[ "${#entries[@]}" -ne 1 || ! -d "${entries[0]}" ]]; then
  echo "archive must contain exactly one top-level directory" >&2
  exit 1
fi

PACKAGE_DIR="${entries[0]}"
VERLET="$PACKAGE_DIR/verlet"
ACP_AGENT="$PACKAGE_DIR/verlet-acp-agent"
MCP_SERVER="$PACKAGE_DIR/verlet-mcp-server"
CONSOLE_DIR="$PACKAGE_DIR/share/verlet/console"
MANUAL="$PACKAGE_DIR/share/man/man1/verlet.1"

for bin in "$VERLET" "$ACP_AGENT" "$MCP_SERVER"; do
  if [[ ! -x "$bin" ]]; then
    echo "missing executable binary in archive: $bin" >&2
    exit 1
  fi
done

if [[ ! -f "$CONSOLE_DIR/index.html" || ! -d "$CONSOLE_DIR/assets" ]]; then
  echo "missing console assets in archive: $CONSOLE_DIR" >&2
  exit 1
fi

if [[ ! -f "$MANUAL" || -L "$MANUAL" || ! -s "$MANUAL" ]]; then
  echo "missing regular manual page in archive: $MANUAL" >&2
  exit 1
fi

if command -v man >/dev/null 2>&1; then
  printf '\n==> man %s\n' "$MANUAL"
  MANPAGER=cat PAGER=cat GROFF_NO_SGR=1 man "$MANUAL" >/dev/null
fi

run_quiet "$VERLET" --version
run_quiet "$VERLET" --help
run_quiet "$VERLET" commands
run_quiet "$VERLET" help chat
run_quiet "$VERLET" console --help
run_quiet "$VERLET" chat --help
run_quiet "$VERLET" agent --help
run_quiet "$VERLET" agent init --help
run_quiet "$VERLET" agent plan --help
run_quiet "$VERLET" agent publish --help
run_quiet "$VERLET" agent list --help
run_quiet "$VERLET" agent show --help
run_quiet "$VERLET" agent run --help
run_quiet "$VERLET" tool --help
run_quiet "$VERLET" tool build --help
run_quiet "$VERLET" tool publish --help
run_quiet "$VERLET" tool run --help
run_quiet "$VERLET" tool manual --help
run_quiet "$VERLET" auth --help
run_quiet "$VERLET" rpc --help
run_quiet "$VERLET" debug --help
run_quiet "$VERLET" debug rpc --help

run_fails "$VERLET" hello

run_quiet "$ACP_AGENT" --version
run_quiet "$ACP_AGENT" --help
run_quiet "$MCP_SERVER" --help

AGENT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/verlet-agent-release.XXXXXX")"
trap 'rm -rf "$TMP" "$AGENT_TMP"' EXIT
AGENT_MANIFEST="$AGENT_TMP/release-verifier.verlet.agent.toml"
AGENT_REGISTRY="$AGENT_TMP/registry"
cat >"$AGENT_MANIFEST" <<'EOF'
[agent]
name = "release-verifier"
version = "0.1.0"
description = "Checks a release archive."

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"
EOF

run "$VERLET" agent plan "$AGENT_MANIFEST" --registry-root "$AGENT_REGISTRY"
run "$VERLET" agent publish "$AGENT_MANIFEST" --registry-root "$AGENT_REGISTRY"
run "$VERLET" agent list --registry-root "$AGENT_REGISTRY"
run "$VERLET" agent show agent://release-verifier@0.1.0 --registry-root "$AGENT_REGISTRY"

printf '\nVerlet release archive smoke passed: %s\n' "$ARCHIVE"
