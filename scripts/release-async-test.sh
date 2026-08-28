#!/usr/bin/env bash

set -euo pipefail

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/release-async-test.XXXXXX")"
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
ROOT="$TMP_DIR/repo"
FAKE_BIN="$TMP_DIR/bin"
DOCKER_MARKER="$TMP_DIR/docker-called"
FAILURES=0
RUN_OUTPUT=
RUN_STATUS=0

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

mkdir -p "$ROOT/scripts" "$FAKE_BIN"
cp "$SOURCE_ROOT/Cargo.toml" "$ROOT/Cargo.toml"
cp "$SOURCE_ROOT/scripts/check-release-tag.sh" \
  "$SOURCE_ROOT/scripts/release-async.sh" \
  "$SOURCE_ROOT/scripts/release-version.sh" \
  "$ROOT/scripts/"
printf 'fixture\n' >"$ROOT/tracked-marker"
printf '#!/usr/bin/env bash\n: >%q\nexit 99\n' \
  "$DOCKER_MARKER" >"$FAKE_BIN/docker"
chmod +x "$FAKE_BIN/docker"
git -C "$ROOT" init -q -b main
git -C "$ROOT" add Cargo.toml scripts tracked-marker
git -C "$ROOT" -c user.name=release-async-test \
  -c user.email=release-async-test@example.invalid \
  commit --no-gpg-sign -qm 'release async test fixture'

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

assert_status() {
  local expected=$1
  local name=$2

  if ((RUN_STATUS != expected)); then
    fail "$name (expected status $expected, got $RUN_STATUS)"
    printf '%s\n' "$RUN_OUTPUT" >&2
  fi
}

run_release() {
  if RUN_OUTPUT="$(
    PATH="$FAKE_BIN:$PATH" DOCKER_MARKER="$DOCKER_MARKER" \
      "$ROOT/scripts/release-async.sh" "$@" 2>&1
  )"; then
    RUN_STATUS=0
  else
    RUN_STATUS=$?
  fi
}

VERSION="$(read_verlet_workspace_version "$ROOT/Cargo.toml")"
TAG="v$VERSION-emo-624-test"
AMD64_STEP="$ROOT/scripts/verify-linux.sh --amd64"
QUICK_PLAN='build and smoke the host-target release archive'
FULL_PLAN="$ROOT/scripts/release-v1-candidate.sh"
SKIP_PLAN='skip the package/full-gate preflight'

assert_plan() {
  local name=$1
  local amd64=$2
  local local_plan=$3
  shift 3

  run_release "$TAG" --dry-run "$@"
  assert_status 0 "$name exits successfully"
  if [[ "$amd64" == "1" ]]; then
    assert_contains "$RUN_OUTPUT" "$AMD64_STEP" "$name includes amd64 verification"
  else
    assert_excludes "$RUN_OUTPUT" "$AMD64_STEP" "$name excludes amd64 verification"
  fi
  assert_contains "$RUN_OUTPUT" "$local_plan" "$name prints the local gate plan"
  if [[ "$local_plan" != "$QUICK_PLAN" ]]; then
    assert_excludes "$RUN_OUTPUT" "$QUICK_PLAN" "$name excludes the quick plan"
  fi
  if [[ "$local_plan" != "$FULL_PLAN" ]]; then
    assert_excludes "$RUN_OUTPUT" "$FULL_PLAN" "$name excludes the full plan"
  fi
  if [[ "$local_plan" != "$SKIP_PLAN" ]]; then
    assert_excludes "$RUN_OUTPUT" "$SKIP_PLAN" "$name excludes the skip plan"
  fi
}

assert_plan 'default dry run' 1 "$QUICK_PLAN"
assert_plan 'full-gate dry run' 1 "$FULL_PLAN" --full-gate
assert_plan 'skip-amd64 dry run' 0 "$QUICK_PLAN" --skip-amd64
assert_plan 'skip-amd64 full-gate dry run' 0 "$FULL_PLAN" \
  --skip-amd64 --full-gate
assert_plan 'skip-local-gate dry run' 0 "$SKIP_PLAN" --skip-local-gate
assert_plan 'skip-local-gate full-gate dry run' 0 "$SKIP_PLAN" \
  --skip-local-gate --full-gate
assert_plan 'both skips dry run' 0 "$SKIP_PLAN" \
  --skip-amd64 --skip-local-gate
assert_plan 'both skips full-gate dry run' 0 "$SKIP_PLAN" \
  --skip-amd64 --skip-local-gate --full-gate

printf 'dirty\n' >>"$ROOT/tracked-marker"
run_release "$TAG" --dry-run --skip-amd64
assert_status 0 'dry run accepts a dirty worktree'
assert_contains "$RUN_OUTPUT" 'would require a clean git worktree' \
  'dirty dry run reports the real-run requirement'
git -C "$ROOT" restore tracked-marker

git -C "$ROOT" checkout --detach -q
run_release "$TAG" --dry-run --skip-amd64
assert_status 1 'detached dry run rejects an implicit branch push'
assert_contains "$RUN_OUTPUT" '--no-push-main is required from a detached HEAD' \
  'detached dry run explains the required opt-out'
run_release "$TAG" --dry-run --skip-amd64 --no-push-main
assert_status 0 'detached dry run accepts --no-push-main'
assert_contains "$RUN_OUTPUT" 'without pushing a branch' \
  'detached dry run prints the tag-only push plan'

run_release --help
assert_status 0 'help exits successfully'
assert_contains "$RUN_OUTPUT" '--skip-amd64' 'help names --skip-amd64'
assert_contains "$RUN_OUTPUT" 'x86_64 Linux' 'help names the amd64 lane'

if git -C "$ROOT" rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  fail 'dry runs do not create the synthetic tag'
fi
if [[ -e "$ROOT/dist" ]]; then
  fail 'dry runs do not create dist output'
fi
if [[ -e "$DOCKER_MARKER" ]]; then
  fail 'dry runs do not invoke Docker'
fi

if ((FAILURES > 0)); then
  printf 'release-async-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'release-async-test: ok\n'
