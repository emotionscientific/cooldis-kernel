#!/usr/bin/env bash
set -euo pipefail

ARCHIVE="${1:-}"

usage() {
  cat <<'USAGE'
smoke-release-archive.sh - smoke-test a packaged Cooldis release archive.

Usage:
  scripts/smoke-release-archive.sh path/to/cooldis-<version>-<target>.tar.gz
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

TMP="$(mktemp -d "${TMPDIR:-/tmp}/cooldis-release-smoke.XXXXXX")"
trap 'rm -rf "$TMP"' EXIT

run tar -xzf "$ARCHIVE" -C "$TMP"

entries=("$TMP"/*)
if [[ "${#entries[@]}" -ne 1 || ! -d "${entries[0]}" ]]; then
  echo "archive must contain exactly one top-level directory" >&2
  exit 1
fi

PACKAGE_DIR="${entries[0]}"
COOLDIS="$PACKAGE_DIR/cooldis"
ACP_AGENT="$PACKAGE_DIR/cooldis-acp-agent"
MCP_SERVER="$PACKAGE_DIR/cooldis-mcp-server"
CONSOLE_DIR="$PACKAGE_DIR/share/cooldis/console"

for bin in "$COOLDIS" "$ACP_AGENT" "$MCP_SERVER"; do
  if [[ ! -x "$bin" ]]; then
    echo "missing executable binary in archive: $bin" >&2
    exit 1
  fi
done

if [[ ! -f "$CONSOLE_DIR/index.html" || ! -d "$CONSOLE_DIR/assets" ]]; then
  echo "missing console assets in archive: $CONSOLE_DIR" >&2
  exit 1
fi

run_quiet "$COOLDIS" --version
run_quiet "$COOLDIS" --help
run_quiet "$COOLDIS" commands
run_quiet "$COOLDIS" help chat
run_quiet "$COOLDIS" console --help
run_quiet "$COOLDIS" chat --help
run_quiet "$COOLDIS" agent --help
run_quiet "$COOLDIS" agent init --help
run_quiet "$COOLDIS" agent plan --help
run_quiet "$COOLDIS" agent publish --help
run_quiet "$COOLDIS" agent list --help
run_quiet "$COOLDIS" agent show --help
run_quiet "$COOLDIS" agent run --help
run_quiet "$COOLDIS" tool --help
run_quiet "$COOLDIS" tool build --help
run_quiet "$COOLDIS" tool publish --help
run_quiet "$COOLDIS" tool run --help
run_quiet "$COOLDIS" tool manual --help
run_quiet "$COOLDIS" auth --help
run_quiet "$COOLDIS" rpc --help
run_quiet "$COOLDIS" debug --help
run_quiet "$COOLDIS" debug rpc --help

run_fails "$COOLDIS" hello

run_quiet "$ACP_AGENT" --version
run_quiet "$ACP_AGENT" --help
run_quiet "$MCP_SERVER" --help

AGENT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/cooldis-agent-release.XXXXXX")"
trap 'rm -rf "$TMP" "$AGENT_TMP"' EXIT
AGENT_MANIFEST="$AGENT_TMP/release-verifier.cooldis.agent.toml"
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

run "$COOLDIS" agent plan "$AGENT_MANIFEST" --registry-root "$AGENT_REGISTRY"
run "$COOLDIS" agent publish "$AGENT_MANIFEST" --registry-root "$AGENT_REGISTRY"
run "$COOLDIS" agent list --registry-root "$AGENT_REGISTRY"
run "$COOLDIS" agent show agent://release-verifier@0.1.0 --registry-root "$AGENT_REGISTRY"

printf '\nCooldis release archive smoke passed: %s\n' "$ARCHIVE"
