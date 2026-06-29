#!/usr/bin/env bash
# Render and publish the researcher example agent manifest.
#
# 1. Seeds the standard operation set into the operation registry
#    (scripts/seed-ops.sh) if any of the three packages is missing.
# 2. Reads each operation's active artifact hash from the registry records.
# 3. Substitutes the hashes into researcher.cooldis.agent.toml.in.
# 4. Publishes the rendered manifest into the agent registry.
#
# Usage: examples/agents/researcher/publish.sh [op-registry-root] [agent-registry-root]
#        (defaults: .cooldis/operations and .cooldis/agents, repo-relative)
#
# Idempotent: re-running against unchanged registries republishes the same
# content; `cooldis agent publish` keeps the version history.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OP_REGISTRY_ROOT="${1:-.cooldis/operations}"
AGENT_REGISTRY_ROOT="${2:-.cooldis/agents}"
TEMPLATE="examples/agents/researcher/researcher.cooldis.agent.toml.in"

cd "$ROOT"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "researcher publish: missing template: $TEMPLATE" >&2
  exit 1
fi

needs_seed=0
for package in http-fetch file-read json-query; do
  if [[ ! -f "$OP_REGISTRY_ROOT/records/${package}.json" ]]; then
    needs_seed=1
  fi
done

if [[ "$needs_seed" == "1" ]]; then
  scripts/seed-ops.sh "$OP_REGISTRY_ROOT"
fi

active_hash() {
  local package="$1"
  local record="$OP_REGISTRY_ROOT/records/${package}.json"
  local hash

  if [[ ! -f "$record" ]]; then
    echo "researcher publish: missing operation record: $record" >&2
    return 1
  fi

  hash="$(
    sed -nE 's/.*"active_artifact_hash"[[:space:]]*:[[:space:]]*"([0-9a-fA-F]{64})".*/\1/p' "$record" |
      head -n 1
  )"
  if [[ ! "$hash" =~ ^[0-9a-fA-F]{64}$ ]]; then
    echo "researcher publish: could not read active_artifact_hash from $record" >&2
    return 1
  fi

  printf '%s' "$hash"
}

HTTP_FETCH_SHA256="$(active_hash http-fetch)"
FILE_READ_SHA256="$(active_hash file-read)"
JSON_QUERY_SHA256="$(active_hash json-query)"

for placeholder in HTTP_FETCH_SHA256 FILE_READ_SHA256 JSON_QUERY_SHA256; do
  if ! grep -Fq "{$placeholder}" "$TEMPLATE"; then
    echo "researcher publish: template missing placeholder {$placeholder}" >&2
    exit 1
  fi
done

rendered="$(mktemp "${TMPDIR:-/tmp}/researcher.cooldis.agent.XXXXXX")"
trap 'rm -f "$rendered"' EXIT

sed \
  -e "s/{HTTP_FETCH_SHA256}/$HTTP_FETCH_SHA256/g" \
  -e "s/{FILE_READ_SHA256}/$FILE_READ_SHA256/g" \
  -e "s/{JSON_QUERY_SHA256}/$JSON_QUERY_SHA256/g" \
  "$TEMPLATE" >"$rendered"

if grep -nE '\{[A-Z0-9_]+_SHA256\}' "$rendered" >&2; then
  echo "researcher publish: rendered manifest still contains unresolved SHA256 placeholder(s)" >&2
  exit 1
fi

cargo run --locked --bin cooldis -- agent publish "$rendered" \
  --registry-root "$AGENT_REGISTRY_ROOT" \
  --operations-registry-root "$OP_REGISTRY_ROOT"
