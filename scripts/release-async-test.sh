#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
FAILURES=0

source "$ROOT/scripts/release-version.sh"

fail() {
  printf 'not ok - %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}

assert_contains() {
  local output=$1
  local expected=$2
  local name=$3

  if [[ "$output" != *"$expected"* ]]; then
    fail "$name"
  fi
}

assert_excludes() {
  local output=$1
  local unexpected=$2
  local name=$3

  if [[ "$output" == *"$unexpected"* ]]; then
    fail "$name"
  fi
}

VERSION="$(read_verlet_workspace_version "$ROOT/Cargo.toml")"
TAG="v$VERSION-emo-624-test"
AMD64_STEP="$ROOT/scripts/verify-linux.sh --amd64"

default_output="$($ROOT/scripts/release-async.sh "$TAG" --dry-run)"
assert_contains "$default_output" "$AMD64_STEP" \
  'default dry run includes amd64 verification'

skip_amd64_output="$($ROOT/scripts/release-async.sh "$TAG" --dry-run --skip-amd64)"
assert_excludes "$skip_amd64_output" "$AMD64_STEP" \
  '--skip-amd64 suppresses amd64 verification'

skip_local_output="$($ROOT/scripts/release-async.sh "$TAG" --dry-run --skip-local-gate)"
assert_excludes "$skip_local_output" "$AMD64_STEP" \
  '--skip-local-gate suppresses amd64 verification'

help_output="$($ROOT/scripts/release-async.sh --help)"
assert_contains "$help_output" '--skip-amd64' 'help names --skip-amd64'
assert_contains "$help_output" 'x86_64 Linux' 'help names the amd64 lane'

if ((FAILURES > 0)); then
  printf 'release-async-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'release-async-test: ok\n'
