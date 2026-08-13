#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIVE_OPENAI_COMPATIBLE="${VERLET_RELEASE_LIVE_OPENAI_COMPATIBLE:-${VERLET_RELEASE_LIVE_PROVIDER:-0}}"
LIVE_PROVIDER_PROTOCOLS="${VERLET_RELEASE_LIVE_PROVIDER_PROTOCOLS:-0}"
LIVE_PRIVATE_SEARCH="${VERLET_RELEASE_LIVE_SEARCH:-0}"
LIVE_TELEGRAM="${VERLET_RELEASE_LIVE_TELEGRAM:-0}"
DOCS="${VERLET_RELEASE_DOCS:-0}"
MANIFEST="${VERLET_RELEASE_MANIFEST:-1}"
WORKBENCH="${VERLET_RELEASE_WORKBENCH:-0}"
AX_BLIND_RUN="${VERLET_RELEASE_AX_BLIND_RUN:-0}"

usage() {
  cat <<'USAGE'
release-v1-candidate.sh - run the Verlet V1 release-candidate gate.

Usage:
  scripts/release-v1-candidate.sh [--live] [--live-provider-protocols] [--docs] [--skip-manifest] [--workbench] [--ax-blind-run]

Default lane:
  - guard rails over tracked files
  - clippy correctness/suspicious/perf gate
  - scripts/verify.sh
  - app-server restart/resume/TCP health smoke
  - focused MCP server tests
  - manifest-backed thread/start e2e smoke
  - release binary package build plus archive smoke
  - packaged-binary secret import/set/list/status/delete redaction smoke
  - deterministic AX blind-test prompt bundle
  - packaged-binary folder-first init, operation publish, agent publish smoke

Optional lanes:
  --live                    run public live provider-protocol lanes except paused Telegram
  --live-provider-protocols run OpenAI Responses + Anthropic Messages wire smokes
  --live-openai-responses   alias for --live-provider-protocols
  --live-anthropic-messages alias for --live-provider-protocols
  --live-openai-compatible  legacy private provider lane; unavailable in the public checkout
  --live-provider-specific  legacy private provider lane; unavailable in the public checkout
  --live-search             legacy private search lane; unavailable in the public checkout
  --live-telegram           fail closed until the Telegram bot IO lane is unpaused
  --docs                    build workspace docs with warnings denied
  --manifest                run the manifest-backed thread/start e2e smoke (default)
  --skip-manifest           skip the manifest-backed thread/start e2e smoke
  --workbench               run the app-server workbench query-surface smoke
  --ax-blind-run            spawn the configured blind-test agent and write answers.md

Environment:
  VERLET_RELEASE_LIVE_PROVIDER=1          legacy private provider lane; unavailable here
  VERLET_RELEASE_LIVE_OPENAI_COMPATIBLE=1 legacy private provider lane; unavailable here
  VERLET_RELEASE_LIVE_PROVIDER_PROTOCOLS=1
  VERLET_RELEASE_LIVE_SEARCH=1            legacy private search lane; unavailable here
  VERLET_RELEASE_LIVE_TELEGRAM=1
  VERLET_RELEASE_DOCS=1
  VERLET_RELEASE_MANIFEST=0|1
  VERLET_RELEASE_WORKBENCH=1
  VERLET_RELEASE_AX_BLIND_RUN=1
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --live)
      LIVE_PROVIDER_PROTOCOLS=1
      shift
      ;;
    --live-provider-protocols|--live-openai-responses|--live-anthropic-messages)
      LIVE_PROVIDER_PROTOCOLS=1
      shift
      ;;
    --live-openai-compatible|--live-provider-specific)
      LIVE_OPENAI_COMPATIBLE=1
      shift
      ;;
    --live-search)
      LIVE_PRIVATE_SEARCH=1
      shift
      ;;
    --live-telegram)
      LIVE_TELEGRAM=1
      shift
      ;;
    --docs)
      DOCS=1
      shift
      ;;
    --manifest)
      MANIFEST=1
      shift
      ;;
    --skip-manifest)
      MANIFEST=0
      shift
      ;;
    --workbench)
      WORKBENCH=1
      shift
      ;;
    --ax-blind-run)
      AX_BLIND_RUN=1
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

if [[ "$LIVE_TELEGRAM" == "1" ]]; then
  echo "Telegram bot IO live lane is paused for V1: no release-gated real Telegram smoke is implemented yet." >&2
  echo "Keep --live-telegram off until the interface/channel decision lands." >&2
  exit 1
fi

if [[ "$LIVE_OPENAI_COMPATIBLE" == "1" ]]; then
  echo "The legacy OpenAI-compatible provider-specific lane is not shipped in this public checkout." >&2
  echo "Use --live-provider-protocols for public provider wire smokes." >&2
  exit 1
fi

if [[ "$LIVE_PRIVATE_SEARCH" == "1" ]]; then
  echo "The legacy remote search provider live lane is not shipped in this public checkout." >&2
  echo "Keep provider-specific live smokes in a private maintainer harness." >&2
  exit 1
fi

run() {
  printf '\n==> %s\n' "$*"
  "$@"
}

run_fails() {
  printf '\n==> expect failure: %s\n' "$*"
  if "$@"; then
    echo "command unexpectedly succeeded: $*" >&2
    exit 1
  fi
}

require_file() {
  if [[ ! -f "$1" ]]; then
    echo "required file was not created: $1" >&2
    exit 1
  fi
}

replace_in_file() {
  local path="$1"
  local old="$2"
  local new="$3"
  local tmp="${path}.tmp"
  awk -v old="$old" -v new="$new" '{ gsub(old, new); print }' "$path" >"$tmp"
  mv "$tmp" "$path"
}

assert_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if [[ "$haystack" != *"$needle"* ]]; then
    echo "$label did not contain expected text: $needle" >&2
    exit 1
  fi
}

assert_not_contains() {
  local haystack="$1"
  local needle="$2"
  local label="$3"
  if [[ "$haystack" == *"$needle"* ]]; then
    echo "$label leaked forbidden text" >&2
    exit 1
  fi
}

print_checked_output() {
  local label="$1"
  local output="$2"
  printf '%s\n%s\n' "$label" "$output"
}

cd "$ROOT"

CLIPPY_GATE=(
  -A clippy::all
  -D clippy::correctness
  -D clippy::suspicious
  -D clippy::perf
)

run "$ROOT/scripts/guard-rails.sh" tracked
run cargo clippy --workspace --all-targets --locked -- "${CLIPPY_GATE[@]}"
run "$ROOT/scripts/verify.sh"
run cargo run --locked --bin verlet-app-server-smoke
run cargo test --locked mcp_server

RELEASE_OUT="${VERLET_RELEASE_OUT_DIR:-$ROOT/dist/release-candidate}"
run "$ROOT/scripts/package-release-binary.sh" --out-dir "$RELEASE_OUT"
RELEASE_ARCHIVE="$(find "$RELEASE_OUT" -maxdepth 1 -name 'verlet-*.tar.gz' | head -n 1)"
if [[ -z "$RELEASE_ARCHIVE" ]]; then
  echo "release archive was not created under $RELEASE_OUT" >&2
  exit 1
fi
run "$ROOT/scripts/smoke-release-archive.sh" "$RELEASE_ARCHIVE"
run "$ROOT/scripts/smoke-install.sh" "$RELEASE_ARCHIVE"
run "$ROOT/scripts/write-release-manifest.sh" --out-dir "$RELEASE_OUT" --tag "vlocal"
run "$ROOT/scripts/ax-blind-test.sh" --out "$RELEASE_OUT/ax-blind-test"
if [[ "$AX_BLIND_RUN" == "1" ]]; then
  run "$ROOT/scripts/ax-blind-test.sh" --run --out "$RELEASE_OUT/ax-blind-test-run"
else
  printf '\n==> skipping live AX blind-test answer lane; pass --ax-blind-run to spawn the configured agent\n'
fi

TARGET_DIR="$(
  cargo metadata --locked --format-version 1 --no-deps \
    | sed -n 's/.*"target_directory":"\([^"]*\)".*/\1/p'
)"
if [[ -z "$TARGET_DIR" ]]; then
  echo "could not determine Cargo target directory" >&2
  exit 1
fi
RELEASE_BIN="$TARGET_DIR/release/verlet"
run "$RELEASE_BIN"
run "$RELEASE_BIN" --help
run "$RELEASE_BIN" commands
run "$RELEASE_BIN" help chat
run "$RELEASE_BIN" init --help
run "$RELEASE_BIN" console --help
run "$RELEASE_BIN" chat --help
run "$RELEASE_BIN" agent --help
run "$RELEASE_BIN" agent init --help
run "$RELEASE_BIN" agent plan --help
run "$RELEASE_BIN" agent publish --help
run "$RELEASE_BIN" agent list --help
run "$RELEASE_BIN" agent show --help
run "$RELEASE_BIN" agent run --help
run "$RELEASE_BIN" tool --help
run "$RELEASE_BIN" tool build --help
run "$RELEASE_BIN" tool publish --help
run "$RELEASE_BIN" tool run --help
run "$RELEASE_BIN" tool manual --help
run "$RELEASE_BIN" auth --help
run "$RELEASE_BIN" secret --help
run "$RELEASE_BIN" secret import --help
run "$RELEASE_BIN" secret set --help
run "$RELEASE_BIN" secret list --help
run "$RELEASE_BIN" secret status --help
run "$RELEASE_BIN" secret delete --help
run "$RELEASE_BIN" rpc --help
run "$RELEASE_BIN" debug --help
run "$RELEASE_BIN" debug rpc --help
run_fails "$RELEASE_BIN" hello

AGENT_TMP="$(mktemp -d "${TMPDIR:-/tmp}/verlet-agent-release.XXXXXX")"
trap 'rm -rf "$AGENT_TMP"' EXIT
AGENT_PROJECT="$AGENT_TMP/release-verifier"
AGENT_MANIFEST="$AGENT_PROJECT/verlet.agent.toml"
AGENT_COMPONENTS="$AGENT_PROJECT/components/operations.toml"
AGENT_REGISTRY="$AGENT_TMP/registry"
AGENT_OPERATION_REGISTRY="$AGENT_TMP/operations"
PLACEHOLDER_REF="op://example-tool@sha256:0000000000000000000000000000000000000000000000000000000000000000"
SECRET_STATE="$AGENT_TMP/secret-state"
SECRET_VALUE="fixture-secret-release-gate-should-not-print"
printf '\n==> packaged secret import/set/list/status/delete smoke\n'
SECRET_IMPORT_OUTPUT="$(
  VERLET_RELEASE_SECRET_VALUE="$SECRET_VALUE" "$RELEASE_BIN" secret import EXAMPLE_API_KEY \
    --from-env VERLET_RELEASE_SECRET_VALUE \
    --state-home "$SECRET_STATE"
)"
assert_contains "$SECRET_IMPORT_OUTPUT" "imported secret EXAMPLE_API_KEY" "secret import output"
assert_not_contains "$SECRET_IMPORT_OUTPUT" "$SECRET_VALUE" "secret import output"
print_checked_output "secret import output:" "$SECRET_IMPORT_OUTPUT"
SECRET_SET_OUTPUT="$(
  printf '%s\n' "$SECRET_VALUE" \
    | "$RELEASE_BIN" secret set SEARCH_API_KEY --value-stdin --state-home "$SECRET_STATE"
)"
assert_contains "$SECRET_SET_OUTPUT" "stored secret SEARCH_API_KEY" "secret set output"
assert_not_contains "$SECRET_SET_OUTPUT" "$SECRET_VALUE" "secret set output"
print_checked_output "secret set output:" "$SECRET_SET_OUTPUT"
SECRET_LIST_OUTPUT="$("$RELEASE_BIN" secret list --state-home "$SECRET_STATE")"
assert_contains "$SECRET_LIST_OUTPUT" "EXAMPLE_API_KEY" "secret list output"
assert_contains "$SECRET_LIST_OUTPUT" "SEARCH_API_KEY" "secret list output"
assert_not_contains "$SECRET_LIST_OUTPUT" "$SECRET_VALUE" "secret list output"
print_checked_output "secret list output:" "$SECRET_LIST_OUTPUT"
SECRET_STATUS_OUTPUT="$("$RELEASE_BIN" secret status EXAMPLE_API_KEY --state-home "$SECRET_STATE")"
assert_contains "$SECRET_STATUS_OUTPUT" '"name": "EXAMPLE_API_KEY"' "secret status output"
assert_contains "$SECRET_STATUS_OUTPUT" '"redacted": true' "secret status output"
assert_not_contains "$SECRET_STATUS_OUTPUT" "$SECRET_VALUE" "secret status output"
print_checked_output "secret status output:" "$SECRET_STATUS_OUTPUT"
SECRET_DELETE_OUTPUT="$("$RELEASE_BIN" secret delete EXAMPLE_API_KEY --state-home "$SECRET_STATE")"
assert_contains "$SECRET_DELETE_OUTPUT" "deleted secret EXAMPLE_API_KEY" "secret delete output"
assert_not_contains "$SECRET_DELETE_OUTPUT" "$SECRET_VALUE" "secret delete output"
print_checked_output "secret delete output:" "$SECRET_DELETE_OUTPUT"
run "$RELEASE_BIN" init release-verifier --out "$AGENT_PROJECT"
require_file "$AGENT_MANIFEST"
require_file "$AGENT_PROJECT/prompts/system.md"
require_file "$AGENT_COMPONENTS"
require_file "$AGENT_PROJECT/components/couplings.toml"
require_file "$AGENT_PROJECT/operations/README.md"
run "$RELEASE_BIN" agent plan "$AGENT_MANIFEST" \
  --registry-root "$AGENT_REGISTRY" \
  --operations-registry-root "$AGENT_TMP/missing-operations"
printf '\n==> %s\n' "$RELEASE_BIN tool publish --package $ROOT/tools/json-query/verlet.tool.toml --registry-root $AGENT_OPERATION_REGISTRY"
TOOL_PUBLISH_OUTPUT="$("$RELEASE_BIN" tool publish \
  --package "$ROOT/tools/json-query/verlet.tool.toml" \
  --registry-root "$AGENT_OPERATION_REGISTRY")"
printf '%s\n' "$TOOL_PUBLISH_OUTPUT"
TOOL_HASH="$(
  printf '%s\n' "$TOOL_PUBLISH_OUTPUT" \
    | awk '$1 == "artifact" && length($2) == 64 && $2 ~ /^[0-9a-f]+$/ { print $2; exit }'
)"
if [[ -z "$TOOL_HASH" ]]; then
  echo "tool publish did not print an artifact hash" >&2
  exit 1
fi
JSON_QUERY_REF="op://json-query/json_query@sha256:${TOOL_HASH}"
replace_in_file "$AGENT_MANIFEST" "$PLACEHOLDER_REF" "$JSON_QUERY_REF"
replace_in_file "$AGENT_MANIFEST" 'id = "example-tool"' 'id = "json-query"'
replace_in_file "$AGENT_MANIFEST" 'command = "example-tool"' 'command = "json-query"'
replace_in_file "$AGENT_COMPONENTS" "$PLACEHOLDER_REF" "$JSON_QUERY_REF"
replace_in_file "$AGENT_COMPONENTS" 'name = "example-tool"' 'name = "json-query"'
replace_in_file "$AGENT_COMPONENTS" 'source = "../operations/example-tool"' "source = \"$ROOT/tools/json-query\""
run "$RELEASE_BIN" agent plan "$AGENT_MANIFEST" \
  --registry-root "$AGENT_REGISTRY" \
  --operations-registry-root "$AGENT_OPERATION_REGISTRY"
run "$RELEASE_BIN" agent publish "$AGENT_MANIFEST" \
  --registry-root "$AGENT_REGISTRY" \
  --operations-registry-root "$AGENT_OPERATION_REGISTRY"
run "$RELEASE_BIN" agent list --registry-root "$AGENT_REGISTRY"
run "$RELEASE_BIN" agent show agent://release-verifier@0.1.0 --registry-root "$AGENT_REGISTRY"

if [[ "$MANIFEST" == "1" ]]; then
  run cargo run --locked --bin verlet-manifest-e2e-smoke
else
  printf '\n==> skipping manifest lane; default is enabled, use --skip-manifest only for quick local iteration\n'
fi

if [[ "$WORKBENCH" == "1" ]]; then
  run cargo run --locked --bin verlet-workbench-smoke
else
  printf '\n==> skipping workbench lane; pass --workbench to run it\n'
fi

if [[ "$DOCS" == "1" ]]; then
  RUSTDOCFLAGS="-D warnings" run cargo doc --workspace --no-deps --locked
fi

if [[ "$LIVE_PROVIDER_PROTOCOLS" == "1" ]]; then
  run cargo run --locked --bin verlet-bifrost-smoke
else
  printf '\n==> skipping OpenAI Responses + Anthropic Messages live lane; pass --live-provider-protocols to run it\n'
fi

printf '\n==> legacy provider-specific live lanes are not shipped in this public checkout\n'

printf '\n==> skipping paused Telegram bot IO live lane; pass --live-telegram only after the lane is implemented\n'

printf '\nVerlet V1 release-candidate gate passed.\n'
