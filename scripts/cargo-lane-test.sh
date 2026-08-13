#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
LANE_SCRIPT="$SCRIPT_DIR/cargo-lane.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cargo-lane-test.XXXXXX")" || exit 1
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
FAILURES=0

cleanup() {
  local pid
  for pid in $(jobs -pr); do
    kill -TERM "$pid" >/dev/null 2>&1 || true
  done
  wait >/dev/null 2>&1 || true
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  printf 'not ok - %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}

assert_eq() {
  local expected=$1
  local actual=$2
  local name=$3

  if [[ "$actual" != "$expected" ]]; then
    fail "$name (expected '$expected', got '$actual')"
  fi
}

assert_path_exists() {
  if [[ ! -e "$1" ]]; then
    fail "$2"
  fi
}

assert_no_path() {
  if [[ -e "$1" || -L "$1" ]]; then
    fail "$2"
  fi
}

assert_file_contains() {
  local file=$1
  local needle=$2
  local name=$3

  if [[ ! -f "$file" ]] || ! grep -Fq "$needle" "$file"; then
    fail "$name"
    if [[ -f "$file" ]]; then
      printf 'file: %s\ncontents:\n%s\n' "$file" "$(<"$file")" >&2
    else
      printf 'missing file: %s\n' "$file" >&2
    fi
  fi
}

assert_file_line() {
  local file=$1
  local expected=$2
  local name=$3

  if [[ ! -f "$file" ]] || ! grep -Fxq "$expected" "$file"; then
    fail "$name"
    if [[ -f "$file" ]]; then
      printf 'expected line: %s\nfile: %s\ncontents:\n%s\n' "$expected" "$file" "$(<"$file")" >&2
    else
      printf 'missing file: %s\n' "$file" >&2
    fi
  fi
}

assert_file_line_count() {
  local file=$1
  local expected=$2
  local needle=$3
  local name=$4
  local actual=0

  if [[ -f "$file" ]]; then
    actual=$(grep -cFx "$needle" "$file" 2>/dev/null || true)
  fi
  if ((actual != expected)); then
    fail "$name (expected $expected, got $actual)"
    if [[ -f "$file" ]]; then
      printf 'expected line: %s\nfile: %s\ncontents:\n%s\n' "$needle" "$file" "$(<"$file")" >&2
    else
      printf 'missing file: %s\n' "$file" >&2
    fi
  fi
}

wait_for_path() {
  local path=$1
  local name=$2
  local attempts=0

  while [[ ! -e "$path" && $attempts -lt 100 ]]; do
    sleep 0.05
    attempts=$((attempts + 1))
  done

  if [[ ! -e "$path" ]]; then
    fail "$name"
    return 1
  fi
}

wait_for_no_path() {
  local path=$1
  local name=$2
  local attempts=0

  while [[ ( -e "$path" || -L "$path" ) && $attempts -lt 100 ]]; do
    sleep 0.05
    attempts=$((attempts + 1))
  done

  if [[ -e "$path" || -L "$path" ]]; then
    fail "$name"
    return 1
  fi
}

wait_for_text() {
  local file=$1
  local needle=$2
  local name=$3
  local attempts=0

  while { [[ ! -f "$file" ]] || ! grep -Fq "$needle" "$file"; } && ((attempts < 100)); do
    sleep 0.05
    attempts=$((attempts + 1))
  done

  if [[ ! -f "$file" ]] || ! grep -Fq "$needle" "$file"; then
    fail "$name"
    return 1
  fi
}

wait_for_pid() {
  local pid=$1
  local expected=$2
  local name=$3
  local status
  local timeout_marker="$TMP_DIR/wait-timeout-$pid"
  local watchdog_pid

  (
    sleep 10
    : >"$timeout_marker"
    kill -TERM "$pid" 2>/dev/null || exit 0
    sleep 1
    kill -KILL "$pid" 2>/dev/null || true
  ) &
  watchdog_pid=$!

  wait "$pid" 2>/dev/null
  status=$?
  kill -TERM "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  if [[ -e "$timeout_marker" ]]; then
    fail "$name timed out"
    return
  fi
  if ((status != expected)); then
    fail "$name (expected status $expected, got $status)"
  fi
}

reset_call_environment() {
  TEST_PATH="$PATH_WITH_SCCACHE"
  TEST_MODE=record
  TEST_LABEL=record
  TEST_EXIT=0
  TEST_FORBID_PATH=
  USE_REAL_CARGO_CONTRACT=1
  USE_LANE_ROOT_OVERRIDE=1
  CALLER_LANE_SCRIPT_SET=0
  CALLER_LANE_SCRIPT=
  CALLER_ALIAS_SET=0
  CALLER_ALIAS=
  CALLER_TARGET_SET=0
  CALLER_TARGET=
  CALLER_BUILD_TARGET_SET=0
  CALLER_BUILD_TARGET=
  CALLER_INCREMENTAL_SET=0
  CALLER_INCREMENTAL=
  CALLER_LANE_INCREMENTAL_SET=0
  CALLER_LANE_INCREMENTAL=
  CALLER_DEV_DEBUG_SET=0
  CALLER_DEV_DEBUG=
  CALLER_TEST_DEBUG_SET=0
  CALLER_TEST_DEBUG=
  CALLER_JOBS_SET=0
  CALLER_JOBS=
  CALLER_WRAPPER_SET=0
  CALLER_WRAPPER=
  CALLER_BASEDIRS_SET=0
  CALLER_BASEDIRS=
  CALLER_CACHE_SIZE_SET=0
  CALLER_CACHE_SIZE=
  CALLER_CI_SET=0
  CALLER_CI=
}

prepare_call_environment() {
  local lane_root=$1
  local record=$2

  unset CARGO_TARGET_DIR CARGO_BUILD_TARGET_DIR CARGO_INCREMENTAL CARGO_PROFILE_DEV_DEBUG
  unset CARGO_PROFILE_TEST_DEBUG CARGO_BUILD_JOBS RUSTC_WRAPPER
  unset SCCACHE_BASEDIRS SCCACHE_CACHE_SIZE VERLET_REAL_CARGO VERLET_CARGO_LANE_SCRIPT
  unset VERLET_CARGO_SHIM_DIR VERLET_VERIFY_MANAGED_CARGO
  unset VERLET_CARGO_LANE_INCREMENTAL CARGO_ALIAS_ESCAPE
  unset CI

  export PATH="$TEST_PATH"
  if ((USE_LANE_ROOT_OVERRIDE)); then
    export VERLET_CARGO_LANE_ROOT="$lane_root"
  else
    unset VERLET_CARGO_LANE_ROOT
  fi
  export FAKE_CARGO_RECORD="$record"
  export FAKE_CARGO_MODE="$TEST_MODE"
  export FAKE_CARGO_LABEL="$TEST_LABEL"
  export FAKE_CARGO_EXIT="$TEST_EXIT"
  export FAKE_CARGO_STATE="$FAKE_STATE"
  export FAKE_CARGO_FORBID_PATH="$TEST_FORBID_PATH"

  if ((USE_REAL_CARGO_CONTRACT)); then
    export VERLET_REAL_CARGO="$FAKE_CARGO"
  fi
  if ((CALLER_LANE_SCRIPT_SET)); then
    export VERLET_CARGO_LANE_SCRIPT="$CALLER_LANE_SCRIPT"
  fi
  if ((CALLER_ALIAS_SET)); then
    export CARGO_ALIAS_ESCAPE="$CALLER_ALIAS"
  fi
  if ((CALLER_TARGET_SET)); then
    export CARGO_TARGET_DIR="$CALLER_TARGET"
  fi
  if ((CALLER_BUILD_TARGET_SET)); then
    export CARGO_BUILD_TARGET_DIR="$CALLER_BUILD_TARGET"
  fi
  if ((CALLER_INCREMENTAL_SET)); then
    export CARGO_INCREMENTAL="$CALLER_INCREMENTAL"
  fi
  if ((CALLER_LANE_INCREMENTAL_SET)); then
    export VERLET_CARGO_LANE_INCREMENTAL="$CALLER_LANE_INCREMENTAL"
  fi
  if ((CALLER_DEV_DEBUG_SET)); then
    export CARGO_PROFILE_DEV_DEBUG="$CALLER_DEV_DEBUG"
  fi
  if ((CALLER_TEST_DEBUG_SET)); then
    export CARGO_PROFILE_TEST_DEBUG="$CALLER_TEST_DEBUG"
  fi
  if ((CALLER_JOBS_SET)); then
    export CARGO_BUILD_JOBS="$CALLER_JOBS"
  fi
  if ((CALLER_WRAPPER_SET)); then
    export RUSTC_WRAPPER="$CALLER_WRAPPER"
  fi
  if ((CALLER_BASEDIRS_SET)); then
    export SCCACHE_BASEDIRS="$CALLER_BASEDIRS"
  fi
  if ((CALLER_CACHE_SIZE_SET)); then
    export SCCACHE_CACHE_SIZE="$CALLER_CACHE_SIZE"
  fi
  if ((CALLER_CI_SET)); then
    export CI="$CALLER_CI"
  fi
}

run_lane() {
  local cwd=$1
  local lane_root=$2
  local record=$3
  local out_file=$4
  local err_file=$5
  shift 5

  (
    cd "$cwd" || exit 1
    prepare_call_environment "$lane_root" "$record"
    "$LANE_SCRIPT" "$@"
  ) >"$out_file" 2>"$err_file"
}

STARTED_PID=
start_lane() {
  local cwd=$1
  local lane_root=$2
  local record=$3
  local out_file=$4
  local err_file=$5
  shift 5

  (
    cd "$cwd" || exit 1
    prepare_call_environment "$lane_root" "$record"
    exec "$LANE_SCRIPT" "$@"
  ) >"$out_file" 2>"$err_file" &
  STARTED_PID=$!
}

run_hook() {
  local cwd=$1
  local lane_root=$2
  local record=$3
  local out_file=$4
  local err_file=$5
  local hook=$6

  (
    cd "$cwd" || exit 1
    prepare_call_environment "$lane_root" "$record"
    "$hook"
  ) >"$out_file" 2>"$err_file"
}

start_hook() {
  local cwd=$1
  local lane_root=$2
  local record=$3
  local out_file=$4
  local err_file=$5
  local hook=$6

  (
    cd "$cwd" || exit 1
    prepare_call_environment "$lane_root" "$record"
    exec "$hook"
  ) >"$out_file" 2>"$err_file" &
  STARTED_PID=$!
}

FAKE_BIN="$TMP_DIR/fake-bin"
FAKE_BIN_NO_SCCACHE="$TMP_DIR/fake-bin-no-sccache"
FAKE_STATE="$TMP_DIR/fake-state"
FAKE_CARGO="$FAKE_BIN/cargo"
mkdir -p "$FAKE_BIN" "$FAKE_BIN_NO_SCCACHE" "$FAKE_STATE"

cat >"$FAKE_CARGO" <<'FAKE'
#!/usr/bin/env bash
set -u

active_dir=
cleanup_active() {
  if [[ -n "$active_dir" && -d "$active_dir" ]]; then
    rmdir "$active_dir" >/dev/null 2>&1 || true
  fi
}
trap cleanup_active EXIT
trap 'cleanup_active; exit 129' HUP
trap 'cleanup_active; exit 130' INT
trap 'cleanup_active; exit 143' TERM

target_dir=${CARGO_TARGET_DIR-<unset>}
if [[ "$target_dir" != '<unset>' ]]; then
  mkdir -p "$target_dir"
fi
mkdir -p "$(dirname "$FAKE_CARGO_RECORD")" "$FAKE_CARGO_STATE"
{
  printf 'target=%s\n' "$target_dir"
  printf 'build_target=%s\n' "${CARGO_BUILD_TARGET_DIR-<unset>}"
  printf 'incremental=%s\n' "${CARGO_INCREMENTAL-<unset>}"
  printf 'dev_debug=%s\n' "${CARGO_PROFILE_DEV_DEBUG-<unset>}"
  printf 'test_debug=%s\n' "${CARGO_PROFILE_TEST_DEBUG-<unset>}"
  printf 'jobs=%s\n' "${CARGO_BUILD_JOBS-<unset>}"
  printf 'rustc_wrapper=%s\n' "${RUSTC_WRAPPER-<unset>}"
  printf 'sccache_basedirs=%s\n' "${SCCACHE_BASEDIRS-<unset>}"
  printf 'sccache_cache_size=%s\n' "${SCCACHE_CACHE_SIZE-<unset>}"
  printf 'cargo_path=%s\n' "$(type -P cargo 2>/dev/null || true)"
  printf 'lane_script=%s\n' "${VERLET_CARGO_LANE_SCRIPT-<unset>}"
  printf 'shim_dir=%s\n' "${VERLET_CARGO_SHIM_DIR-<unset>}"
  printf 'real_cargo=%s\n' "${VERLET_REAL_CARGO-<unset>}"
  for arg in "$@"; do
    printf 'arg=%s\n' "$arg"
  done
} >"$FAKE_CARGO_RECORD"
printf '%s\n' "$target_dir" >>"$FAKE_CARGO_STATE/calls-$FAKE_CARGO_LABEL"

if [[ -n "${FAKE_CARGO_FORBID_PATH:-}" && -e "$FAKE_CARGO_FORBID_PATH" ]]; then
  : >"$FAKE_CARGO_STATE/forbidden-path-present-$FAKE_CARGO_LABEL"
  exit 92
fi

if [[ "${FAKE_CARGO_MODE:-record}" == hold ]]; then
  if [[ "$target_dir" == '<unset>' ]]; then
    exit 93
  fi
  lane=${target_dir##*/}
  active_dir="$FAKE_CARGO_STATE/active-$lane"
  if ! mkdir "$active_dir" 2>/dev/null; then
    : >"$FAKE_CARGO_STATE/overlap-$lane"
    exit 90
  fi

  : >"$FAKE_CARGO_STATE/started-$FAKE_CARGO_LABEL"
  while [[ ! -e "$FAKE_CARGO_STATE/release-$FAKE_CARGO_LABEL" ]]; do
    sleep 0.02
  done
fi

exit "${FAKE_CARGO_EXIT:-0}"
FAKE
chmod +x "$FAKE_CARGO"
ln -s "$FAKE_CARGO" "$FAKE_BIN_NO_SCCACHE/cargo"

cat >"$FAKE_BIN/sccache" <<'SCCACHE'
#!/usr/bin/env bash
exit 0
SCCACHE
chmod +x "$FAKE_BIN/sccache"

PATH_WITH_SCCACHE="$FAKE_BIN:/usr/bin:/bin"
PATH_WITHOUT_SCCACHE="$FAKE_BIN_NO_SCCACHE:/usr/bin:/bin"

REPO="$TMP_DIR/repo"
FEATURE_A="$TMP_DIR/feature-a"
FEATURE_B="$TMP_DIR/feature-b"
INTEGRATION_WT="$TMP_DIR/integration-wt"
mkdir -p "$REPO/.cargo" "$REPO/apps" "$REPO/crates" "$REPO/docs" "$REPO/scripts"

git -C "$REPO" init -b main >/dev/null
git -C "$REPO" config user.email cargo-lane-test@example.invalid
git -C "$REPO" config user.name 'cargo lane test'
printf 'fixture\n' >"$REPO/README.md"
cat >"$REPO/.cargo/config.toml" <<'CONFIG'
[build]
target-dir = "../.cargo-target/verlet"
CONFIG
cp "$SCRIPT_DIR/cargo-lane.sh" "$REPO/scripts/cargo-lane.sh"
cp "$SCRIPT_DIR/check-pre-commit.sh" "$REPO/scripts/check-pre-commit.sh"
cp "$SCRIPT_DIR/check-pre-push.sh" "$REPO/scripts/check-pre-push.sh"
cp "$SCRIPT_DIR/guard-rails.sh" "$REPO/scripts/guard-rails.sh"
cp "$SCRIPT_DIR/test-timeout-lint.pl" "$REPO/scripts/test-timeout-lint.pl"
cp "$SCRIPT_DIR/test-timeout-lint.sh" "$REPO/scripts/test-timeout-lint.sh"
cp "$SCRIPT_DIR/threat-model-lint.sh" "$REPO/scripts/threat-model-lint.sh"
cp "$SCRIPT_DIR/verify.sh" "$REPO/scripts/verify.sh"
cp "$SCRIPT_DIR/verlet-name-lint.sh" "$REPO/scripts/verlet-name-lint.sh"
printf '# path\treason\n' >"$REPO/scripts/verlet-name-allowlist.tsv"
# Nested verify is under test for Cargo routing. Give its real non-Cargo checks
# minimal valid inputs instead of stubbing them or copying the full source tree.
cat >"$REPO/docs/threat-model.md" <<'THREAT_MODEL'
# Threat model fixture

## TM-FIXTURE-001: Exercise the real threat-model lint

- Status: ACCEPTED
- Severity: LOW
- Threat: The cargo-lane fixture could skip the verifier prelude.
- Affected surface: Nested verify fixture.
- Mitigation: Run the real lint against this minimal valid entry.
- Deterministic guard: scripts/cargo-lane-test.sh
THREAT_MODEL
printf '%s\n' '// Test-timeout lint fixture for the apps scan root.' >"$REPO/apps/fixture.rs"
printf '%s\n' '// Test-timeout lint fixture for the crates scan root.' >"$REPO/crates/fixture.rs"
git -C "$REPO" add README.md .cargo/config.toml apps crates docs scripts
git -C "$REPO" commit -m fixture >/dev/null
git -C "$REPO" worktree add -q -b feature/alpha "$FEATURE_A"
git -C "$REPO" worktree add -q -b feature/beta "$FEATURE_B"
git -C "$REPO" worktree add -q -b integration/check "$INTEGRATION_WT"

reset_call_environment

# Lane selection includes main, integration/*, and feature branches.
SELECTION_ROOT="$TMP_DIR/lane root selection"
run_lane "$REPO" "$SELECTION_ROOT" "$TMP_DIR/main.record" "$TMP_DIR/main.out" "$TMP_DIR/main.err" check
assert_eq 0 "$?" 'main lane invocation succeeded'
assert_file_line "$TMP_DIR/main.record" "target=$SELECTION_ROOT/targets/integration" 'main selected integration lane'

run_lane "$INTEGRATION_WT" "$SELECTION_ROOT" "$TMP_DIR/integration.record" "$TMP_DIR/integration.out" "$TMP_DIR/integration.err" check
assert_eq 0 "$?" 'integration branch invocation succeeded'
assert_file_line "$TMP_DIR/integration.record" "target=$SELECTION_ROOT/targets/integration" 'integration/* selected integration lane'

run_lane "$FEATURE_A" "$SELECTION_ROOT" "$TMP_DIR/feature.record" "$TMP_DIR/feature.out" "$TMP_DIR/feature.err" check
assert_eq 0 "$?" 'feature branch invocation succeeded'
assert_file_line "$TMP_DIR/feature.record" "target=$SELECTION_ROOT/targets/feature" 'feature branch selected feature lane'

reset_call_environment
USE_LANE_ROOT_OVERRIDE=0
DEFAULT_ROOT="$REPO/.git/cargo-lanes"
run_lane "$FEATURE_A" "$TMP_DIR/ignored-override" "$TMP_DIR/default-root.record" "$TMP_DIR/default-root.out" "$TMP_DIR/default-root.err" check
assert_eq 0 "$?" 'default lane root invocation succeeded'
assert_file_line "$TMP_DIR/default-root.record" "target=$DEFAULT_ROOT/targets/feature" 'default lane root is below Git common directory'

reset_call_environment

# Hook and local verification scripts keep their existing output while every
# Cargo subprocess enters the managed lane. Clean CI continues to use Cargo
# directly so the workflow's target/cache behavior does not change.
HOOK_SERIAL_ROOT="$TMP_DIR/lanes-hook-serial"
TEST_LABEL=hook-pre-commit-a
run_hook "$FEATURE_A" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-commit-a.record" "$TMP_DIR/hook-pre-commit-a.out" "$TMP_DIR/hook-pre-commit-a.err" "$FEATURE_A/scripts/check-pre-commit.sh"
assert_eq 0 "$?" 'first worktree pre-commit hook succeeded'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-commit-a" 2 "$HOOK_SERIAL_ROOT/targets/feature" 'first pre-commit routed both Cargo calls through its feature lane'
assert_file_contains "$TMP_DIR/hook-pre-commit-a.out" '==> cargo fmt --all -- --check' 'pre-commit kept its Cargo command output shape'
assert_file_contains "$TMP_DIR/hook-pre-commit-a.out" 'Verlet pre-commit checks passed.' 'pre-commit kept its success output'

reset_call_environment
DISPATCH_SHIM_DIR="$TMP_DIR/hook-dispatch-shim"
mkdir -p "$DISPATCH_SHIM_DIR"
cat >"$DISPATCH_SHIM_DIR/cargo" <<'SHIM'
#!/usr/bin/env bash
set -euo pipefail
exec "$VERLET_CARGO_LANE_SCRIPT" "$@"
SHIM
chmod +x "$DISPATCH_SHIM_DIR/cargo"
TEST_PATH="$DISPATCH_SHIM_DIR:$PATH_WITH_SCCACHE"
TEST_LABEL=hook-pre-commit-dispatch
CALLER_LANE_SCRIPT_SET=1
CALLER_LANE_SCRIPT="$FEATURE_A/scripts/cargo-lane.sh"
run_hook "$FEATURE_A" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-commit-dispatch.record" "$TMP_DIR/hook-pre-commit-dispatch.out" "$TMP_DIR/hook-pre-commit-dispatch.err" "$FEATURE_A/scripts/check-pre-commit.sh"
assert_eq 0 "$?" 'dispatch-shim pre-commit hook succeeded without recursive lane acquisition'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-commit-dispatch" 2 "$HOOK_SERIAL_ROOT/targets/feature" 'dispatch-shim pre-commit routed both Cargo calls once'
assert_file_line "$TMP_DIR/hook-pre-commit-dispatch.record" "cargo_path=$FAKE_CARGO" 'dispatch-shim pre-commit removed the shim before entering Cargo'

reset_call_environment
TEST_LABEL=hook-pre-commit-main
run_hook "$REPO" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-commit-main.record" "$TMP_DIR/hook-pre-commit-main.out" "$TMP_DIR/hook-pre-commit-main.err" "$REPO/scripts/check-pre-commit.sh"
assert_eq 0 "$?" 'primary checkout pre-commit hook succeeded'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-commit-main" 2 "$HOOK_SERIAL_ROOT/targets/integration" 'primary checkout pre-commit selected the integration lane'

reset_call_environment
TEST_LABEL=hook-pre-commit-b
run_hook "$FEATURE_B" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-commit-b.record" "$TMP_DIR/hook-pre-commit-b.out" "$TMP_DIR/hook-pre-commit-b.err" "$FEATURE_B/scripts/check-pre-commit.sh"
assert_eq 0 "$?" 'second worktree pre-commit hook succeeded serially'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-commit-b" 2 "$HOOK_SERIAL_ROOT/targets/feature" 'second pre-commit routed both Cargo calls through its feature lane'

reset_call_environment
TEST_LABEL=hook-pre-push-a
run_hook "$FEATURE_A" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-push-a.record" "$TMP_DIR/hook-pre-push-a.out" "$TMP_DIR/hook-pre-push-a.err" "$FEATURE_A/scripts/check-pre-push.sh"
assert_eq 0 "$?" 'first worktree pre-push hook succeeded'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-push-a" 6 "$HOOK_SERIAL_ROOT/targets/feature" 'pre-push and nested verify routed every Cargo call through the feature lane'
assert_file_contains "$TMP_DIR/hook-pre-push-a.out" '==> cargo clippy --workspace --all-targets --locked -- -A clippy::all -D clippy::correctness -D clippy::suspicious -D clippy::perf' 'pre-push kept its Cargo command output shape'
assert_file_contains "$TMP_DIR/hook-pre-push-a.out" 'Verlet pre-push checks passed.' 'pre-push kept its success output'

reset_call_environment
TEST_LABEL=hook-pre-push-ci
CALLER_CI_SET=1
CALLER_CI=true
run_hook "$FEATURE_A" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-push-ci.record" "$TMP_DIR/hook-pre-push-ci.out" "$TMP_DIR/hook-pre-push-ci.err" "$FEATURE_A/scripts/check-pre-push.sh"
assert_eq 0 "$?" 'pre-push hook kept nested verify in the managed lane when CI was inherited'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-push-ci" 6 "$HOOK_SERIAL_ROOT/targets/feature" 'pre-push hook left no direct Cargo path when CI was inherited'

reset_call_environment
TEST_LABEL=hook-pre-push-b
run_hook "$FEATURE_B" "$HOOK_SERIAL_ROOT" "$TMP_DIR/hook-pre-push-b.record" "$TMP_DIR/hook-pre-push-b.out" "$TMP_DIR/hook-pre-push-b.err" "$FEATURE_B/scripts/check-pre-push.sh"
assert_eq 0 "$?" 'second worktree pre-push hook succeeded serially'
assert_file_line_count "$FAKE_STATE/calls-hook-pre-push-b" 6 "$HOOK_SERIAL_ROOT/targets/feature" 'second pre-push routed every Cargo call through its feature lane'

reset_call_environment
TEST_LABEL=verify-local
run_hook "$FEATURE_A" "$HOOK_SERIAL_ROOT" "$TMP_DIR/verify-local.record" "$TMP_DIR/verify-local.out" "$TMP_DIR/verify-local.err" "$FEATURE_A/scripts/verify.sh"
assert_eq 0 "$?" 'standalone local verify succeeded'
assert_file_line_count "$FAKE_STATE/calls-verify-local" 5 "$HOOK_SERIAL_ROOT/targets/feature" 'standalone local verify routed every Cargo call through the feature lane'

reset_call_environment
TEST_LABEL=verify-ci
CALLER_CI_SET=1
CALLER_CI=true
run_hook "$FEATURE_A" "$HOOK_SERIAL_ROOT" "$TMP_DIR/verify-ci.record" "$TMP_DIR/verify-ci.out" "$TMP_DIR/verify-ci.err" "$FEATURE_A/scripts/verify.sh"
assert_eq 0 "$?" 'clean CI verify succeeded without the local lane'
assert_file_line_count "$FAKE_STATE/calls-verify-ci" 5 '<unset>' 'clean CI verify preserved direct Cargo behavior'

# Same-owner commands reuse the target. A different feature owner rotates it.
REUSE_ROOT="$TMP_DIR/lanes-reuse"
run_lane "$FEATURE_A" "$REUSE_ROOT" "$TMP_DIR/reuse-first.record" "$TMP_DIR/reuse-first.out" "$TMP_DIR/reuse-first.err" check
assert_eq 0 "$?" 'first reuse invocation succeeded'
mkdir -p "$REUSE_ROOT/targets/feature"
: >"$REUSE_ROOT/targets/feature/reuse-sentinel"
run_lane "$FEATURE_A" "$REUSE_ROOT" "$TMP_DIR/reuse-second.record" "$TMP_DIR/reuse-second.out" "$TMP_DIR/reuse-second.err" check
assert_eq 0 "$?" 'second reuse invocation succeeded'
assert_path_exists "$REUSE_ROOT/targets/feature/reuse-sentinel" 'same owner preserved feature target'

TEST_LABEL=rotation-check
TEST_FORBID_PATH="$REUSE_ROOT/targets/feature/reuse-sentinel"
run_lane "$FEATURE_B" "$REUSE_ROOT" "$TMP_DIR/rotate.record" "$TMP_DIR/rotate.out" "$TMP_DIR/rotate.err" check
assert_eq 0 "$?" 'different owner invocation succeeded'
assert_no_path "$REUSE_ROOT/targets/feature/reuse-sentinel" 'different owner removed previous feature target'
assert_no_path "$FAKE_STATE/forbidden-path-present-rotation-check" 'different owner target was removed before Cargo entered'
expected_owner="$FEATURE_B"$'\n''feature/beta'
assert_eq "$expected_owner" "$(<"$REUSE_ROOT/feature.owner")" 'feature owner record contains worktree and branch'

reset_call_environment

# Rotation unlinks a target symlink without touching its referent.
SYMLINK_ROOT="$TMP_DIR/lanes-symlink"
SYMLINK_REFERENT="$TMP_DIR/symlink-referent"
mkdir -p "$SYMLINK_ROOT/targets" "$SYMLINK_REFERENT"
: >"$SYMLINK_REFERENT/preserved"
ln -s "$SYMLINK_REFERENT" "$SYMLINK_ROOT/targets/feature"
run_lane "$FEATURE_A" "$SYMLINK_ROOT" "$TMP_DIR/symlink.record" "$TMP_DIR/symlink.out" "$TMP_DIR/symlink.err" check
assert_eq 0 "$?" 'target symlink rotation succeeded'
assert_path_exists "$SYMLINK_REFERENT/preserved" 'target symlink rotation preserved referent'
if [[ -L "$SYMLINK_ROOT/targets/feature" ]]; then
  fail 'target symlink remained after rotation'
fi

# Managed environment values override caller values and preserve explicit bounds.
ENV_ROOT="$TMP_DIR/lanes-env"
CALLER_TARGET_SET=1
CALLER_TARGET="$TMP_DIR/caller-target"
CALLER_BUILD_TARGET_SET=1
CALLER_BUILD_TARGET="$TMP_DIR/caller-build-target"
CALLER_INCREMENTAL_SET=1
CALLER_INCREMENTAL=1
CALLER_DEV_DEBUG_SET=1
CALLER_DEV_DEBUG=2
CALLER_TEST_DEBUG_SET=1
CALLER_TEST_DEBUG=full
CALLER_WRAPPER_SET=1
CALLER_WRAPPER="$TMP_DIR/caller-wrapper"
CALLER_BASEDIRS_SET=1
CALLER_BASEDIRS="$TMP_DIR/caller-basedirs"
run_lane "$FEATURE_A" "$ENV_ROOT" "$TMP_DIR/env.record" "$TMP_DIR/env.out" "$TMP_DIR/env.err" check 'argument with spaces' --locked
assert_eq 0 "$?" 'managed environment invocation succeeded'
assert_file_line "$TMP_DIR/env.record" "target=$ENV_ROOT/targets/feature" 'caller target environment was overridden'
assert_file_line "$TMP_DIR/env.record" 'build_target=<unset>' 'Cargo config target alias was cleared'
assert_file_line "$TMP_DIR/env.record" 'incremental=0' 'incremental output disabled'
assert_file_line "$TMP_DIR/env.record" 'dev_debug=line-tables-only' 'dev debug is line tables only'
assert_file_line "$TMP_DIR/env.record" 'test_debug=line-tables-only' 'test debug is line tables only'
assert_file_line "$TMP_DIR/env.record" 'jobs=8' 'build jobs defaulted to eight'
assert_file_line "$TMP_DIR/env.record" "rustc_wrapper=$FAKE_BIN/sccache" 'sccache selected as rustc wrapper'
assert_file_line "$TMP_DIR/env.record" "sccache_basedirs=$TMP_DIR" 'sccache base is above main checkout'
assert_file_line "$TMP_DIR/env.record" 'sccache_cache_size=10G' 'sccache cache size defaulted to 10G'
assert_file_line "$TMP_DIR/env.record" 'arg=argument with spaces' 'Cargo argument boundaries were preserved'
assert_no_path "$CALLER_TARGET" 'caller target path was not created'
assert_no_path "$CALLER_BUILD_TARGET" 'caller config target alias path was not created'
assert_no_path "$FEATURE_A/target" 'checkout-local target was not created'
assert_no_path "$TMP_DIR/.cargo-target/verlet" 'old shared target was not created'

# Incremental mode uses a separate lane instance and never invokes sccache.
reset_call_environment
TEST_PATH="$PATH_WITHOUT_SCCACHE"
CALLER_LANE_INCREMENTAL_SET=1
CALLER_LANE_INCREMENTAL=1
CALLER_INCREMENTAL_SET=1
CALLER_INCREMENTAL=0
CALLER_WRAPPER_SET=1
CALLER_WRAPPER="$TMP_DIR/stale-wrapper"
CALLER_BASEDIRS_SET=1
CALLER_BASEDIRS="$TMP_DIR/stale-sccache-basedirs"
CALLER_CACHE_SIZE_SET=1
CALLER_CACHE_SIZE=99G
run_lane "$FEATURE_A" "$ENV_ROOT" "$TMP_DIR/incremental-env.record" "$TMP_DIR/incremental-env.out" "$TMP_DIR/incremental-env.err" check --locked
assert_eq 0 "$?" 'incremental mode invocation succeeded'
assert_file_line "$TMP_DIR/incremental-env.record" "target=$ENV_ROOT/targets/feature-incremental" 'incremental mode selected a separate target'
assert_file_line "$TMP_DIR/incremental-env.record" 'incremental=1' 'incremental mode enabled incremental output'
assert_file_line "$TMP_DIR/incremental-env.record" 'rustc_wrapper=<unset>' 'incremental mode cleared the compiler wrapper'
assert_file_line "$TMP_DIR/incremental-env.record" 'sccache_basedirs=<unset>' 'incremental mode cleared stale sccache base directories'
assert_file_line "$TMP_DIR/incremental-env.record" 'sccache_cache_size=<unset>' 'incremental mode cleared stale sccache cache size'
assert_file_line "$TMP_DIR/incremental-env.record" 'dev_debug=line-tables-only' 'incremental mode preserved dev debug settings'
assert_file_line "$TMP_DIR/incremental-env.record" 'test_debug=line-tables-only' 'incremental mode preserved test debug settings'
assert_file_line "$TMP_DIR/incremental-env.record" 'jobs=8' 'incremental mode preserved the build jobs default'
assert_path_exists "$ENV_ROOT/targets/feature" 'default mode target remained present'
assert_path_exists "$ENV_ROOT/targets/feature-incremental" 'incremental mode created its distinct target'
if grep -Fq 'warning: sccache not found' "$TMP_DIR/incremental-env.err"; then
  fail 'incremental mode probed for missing sccache'
fi
assert_path_exists "$ENV_ROOT/feature-incremental.owner" 'incremental mode kept a separate owner record'

reset_call_environment

# Incremental mode deactivates the dispatch Cargo shim before entering Cargo.
TEST_PATH="$TMP_DIR/fake-shim-bin:$PATH_WITH_SCCACHE"
CALLER_LANE_INCREMENTAL_SET=1
CALLER_LANE_INCREMENTAL=1
CALLER_LANE_SCRIPT_SET=1
CALLER_LANE_SCRIPT="$LANE_SCRIPT"
mkdir -p "$TMP_DIR/fake-shim-bin"
cat >"$TMP_DIR/fake-shim-bin/cargo" <<'SHIM'
#!/usr/bin/env bash
exit 91
SHIM
chmod +x "$TMP_DIR/fake-shim-bin/cargo"
run_lane "$FEATURE_A" "$ENV_ROOT" "$TMP_DIR/incremental-shim.record" "$TMP_DIR/incremental-shim.out" "$TMP_DIR/incremental-shim.err" check
assert_eq 0 "$?" 'incremental dispatch-shim invocation succeeded'
assert_file_line "$TMP_DIR/incremental-shim.record" "target=$ENV_ROOT/targets/feature-incremental" 'incremental dispatch shim preserved its lane instance'
assert_file_line "$TMP_DIR/incremental-shim.record" "cargo_path=$FAKE_CARGO" 'incremental dispatch shim was removed from PATH'
assert_file_line "$TMP_DIR/incremental-shim.record" 'lane_script=<unset>' 'incremental dispatch shim cleared its script contract'
assert_file_line "$TMP_DIR/incremental-shim.record" 'shim_dir=<unset>' 'incremental dispatch shim cleared its directory contract'
assert_file_line "$TMP_DIR/incremental-shim.record" 'real_cargo=<unset>' 'incremental dispatch shim cleared its real Cargo contract'

reset_call_environment

run_lane "$FEATURE_A" "$ENV_ROOT" "$TMP_DIR/config-env.record" "$TMP_DIR/config-env.out" "$TMP_DIR/config-env.err" \
  check --config "build.target-dir=\"$CALLER_TARGET\"" -- from-test
assert_eq 0 "$?" 'caller Cargo config invocation succeeded'
expected_config_args="arg=check
arg=--config
arg=build.target-dir=\"$CALLER_TARGET\"
arg=--
arg=from-test"
actual_config_args=$(grep '^arg=' "$TMP_DIR/config-env.record")
assert_eq "$expected_config_args" "$actual_config_args" 'caller Cargo config arguments were preserved exactly'
assert_file_line "$TMP_DIR/config-env.record" "target=$ENV_ROOT/targets/feature" 'managed target environment overrides caller Cargo config'
assert_no_path "$CALLER_TARGET" 'caller Cargo config target was not created'

reset_call_environment
CALLER_JOBS_SET=1
CALLER_JOBS=3
CALLER_CACHE_SIZE_SET=1
CALLER_CACHE_SIZE=2G
run_lane "$FEATURE_A" "$ENV_ROOT" "$TMP_DIR/preserved-env.record" "$TMP_DIR/preserved-env.out" "$TMP_DIR/preserved-env.err" check
assert_eq 0 "$?" 'explicit bound invocation succeeded'
assert_file_line "$TMP_DIR/preserved-env.record" 'jobs=3' 'explicit Cargo job count was preserved'
assert_file_line "$TMP_DIR/preserved-env.record" 'sccache_cache_size=2G' 'explicit sccache cache size was preserved'

# Direct invocation resolves Cargo from PATH, and missing sccache is non-fatal.
reset_call_environment
TEST_PATH="$PATH_WITHOUT_SCCACHE"
USE_REAL_CARGO_CONTRACT=0
CALLER_WRAPPER_SET=1
CALLER_WRAPPER="$TMP_DIR/stale-wrapper"
run_lane "$FEATURE_A" "$TMP_DIR/lanes-direct" "$TMP_DIR/direct.record" "$TMP_DIR/direct.out" "$TMP_DIR/direct.err" metadata
assert_eq 0 "$?" 'direct invocation without Cargo contract succeeded'
assert_file_line "$TMP_DIR/direct.record" 'rustc_wrapper=<unset>' 'missing sccache cleared stale rustc wrapper'
assert_file_contains "$TMP_DIR/direct.err" 'warning: sccache not found' 'missing sccache emitted a warning'
warning_count=$(grep -cF 'warning: sccache not found' "$TMP_DIR/direct.err" 2>/dev/null || true)
assert_eq 1 "$warning_count" 'missing sccache warning was emitted once'

reset_call_environment
USE_REAL_CARGO_CONTRACT=0
CALLER_LANE_SCRIPT_SET=1
CALLER_LANE_SCRIPT="$LANE_SCRIPT"
run_lane "$FEATURE_A" "$TMP_DIR/lanes-recursion" "$TMP_DIR/recursion.record" "$TMP_DIR/recursion.out" "$TMP_DIR/recursion.err" check
status=$?
if ((status == 0)); then
  fail 'incomplete Cargo shim contract unexpectedly succeeded'
fi
assert_file_contains "$TMP_DIR/recursion.err" 'shim contract is missing VERLET_REAL_CARGO' 'incomplete Cargo shim contract failed without recursion'
assert_no_path "$TMP_DIR/recursion.record" 'incomplete Cargo shim contract did not invoke Cargo'
assert_no_path "$TMP_DIR/lanes-recursion/feature.lock" 'incomplete Cargo shim contract did not acquire a lane lock'

# Both target-dir spellings fail before lock acquisition or Cargo execution.
reset_call_environment
REJECT_ROOT="$TMP_DIR/lanes-reject"
mkdir -p "$REJECT_ROOT/targets/feature"
: >"$REJECT_ROOT/targets/feature/preserved"
run_lane "$FEATURE_A" "$REJECT_ROOT" "$TMP_DIR/reject-split.record" "$TMP_DIR/reject-split.out" "$TMP_DIR/reject-split.err" check --target-dir "$TMP_DIR/rejected"
status=$?
if ((status == 0)); then
  fail 'split target-dir override unexpectedly succeeded'
fi
assert_file_contains "$TMP_DIR/reject-split.err" 'does not allow --target-dir' 'split target-dir rejection explained the error'
assert_no_path "$TMP_DIR/reject-split.record" 'split target-dir rejection did not invoke Cargo'
assert_path_exists "$REJECT_ROOT/targets/feature/preserved" 'split target-dir rejection did not rotate target'

run_lane "$FEATURE_A" "$REJECT_ROOT" "$TMP_DIR/reject-equals.record" "$TMP_DIR/reject-equals.out" "$TMP_DIR/reject-equals.err" check "--target-dir=$TMP_DIR/rejected"
status=$?
if ((status == 0)); then
  fail 'equals target-dir override unexpectedly succeeded'
fi
assert_file_contains "$TMP_DIR/reject-equals.err" 'does not allow --target-dir' 'equals target-dir rejection explained the error'
assert_no_path "$TMP_DIR/reject-equals.record" 'equals target-dir rejection did not invoke Cargo'

reset_call_environment
CALLER_ALIAS_SET=1
CALLER_ALIAS="check --target-dir $TMP_DIR/alias-target"
run_lane "$FEATURE_A" "$REJECT_ROOT" "$TMP_DIR/reject-alias.record" "$TMP_DIR/reject-alias.out" "$TMP_DIR/reject-alias.err" escape
status=$?
if ((status == 0)); then
  fail 'Cargo alias target-dir override unexpectedly succeeded'
fi
assert_file_contains "$TMP_DIR/reject-alias.err" 'does not allow CARGO_ALIAS_ESCAPE' 'Cargo alias target-dir rejection explained the error'
assert_no_path "$TMP_DIR/reject-alias.record" 'Cargo alias target-dir rejection did not invoke Cargo'
assert_no_path "$TMP_DIR/alias-target" 'Cargo alias target path was not created'

# Incremental target overrides also fail before lock acquisition or rotation.
reset_call_environment
CALLER_LANE_INCREMENTAL_SET=1
CALLER_LANE_INCREMENTAL=1
mkdir -p "$REJECT_ROOT/targets/feature-incremental"
: >"$REJECT_ROOT/targets/feature-incremental/preserved"
run_lane "$FEATURE_A" "$REJECT_ROOT" "$TMP_DIR/reject-incremental.record" "$TMP_DIR/reject-incremental.out" "$TMP_DIR/reject-incremental.err" check --target-dir "$TMP_DIR/rejected-incremental"
status=$?
if ((status == 0)); then
  fail 'incremental target-dir override unexpectedly succeeded'
fi
assert_file_contains "$TMP_DIR/reject-incremental.err" 'does not allow --target-dir' 'incremental target-dir rejection explained the error'
assert_no_path "$TMP_DIR/reject-incremental.record" 'incremental target-dir rejection did not invoke Cargo'
assert_path_exists "$REJECT_ROOT/targets/feature-incremental/preserved" 'incremental target-dir rejection did not rotate target'
assert_no_path "$REJECT_ROOT/feature-incremental.lock" 'incremental target-dir rejection did not acquire a lane lock'

# A dead holder is reclaimed before Cargo runs.
reset_call_environment
STALE_ROOT="$TMP_DIR/lanes-stale"
mkdir -p "$STALE_ROOT/feature.lock/reclaim"
printf '99999999\n' >"$STALE_ROOT/feature.lock/pid"
printf 'stale-token\n' >"$STALE_ROOT/feature.lock/token"
touch -t 200001010000 "$STALE_ROOT/feature.lock/reclaim"
run_lane "$FEATURE_A" "$STALE_ROOT" "$TMP_DIR/stale.record" "$TMP_DIR/stale.out" "$TMP_DIR/stale.err" check
assert_eq 0 "$?" 'stale holder was reclaimed'
wait_for_no_path "$STALE_ROOT/feature.lock" 'stale recovery released the lane lock'

EMPTY_ROOT="$TMP_DIR/lanes-empty-stale"
mkdir -p "$EMPTY_ROOT/feature.lock"
touch -t 200001010000 "$EMPTY_ROOT/feature.lock"
run_lane "$FEATURE_A" "$EMPTY_ROOT" "$TMP_DIR/empty-stale.record" "$TMP_DIR/empty-stale.out" "$TMP_DIR/empty-stale.err" check
assert_eq 0 "$?" 'empty stale lock was reclaimed after initialization grace'
wait_for_no_path "$EMPTY_ROOT/feature.lock" 'empty stale recovery released the lane lock'

INCREMENTAL_STALE_ROOT="$TMP_DIR/lanes-incremental-stale"
mkdir -p "$INCREMENTAL_STALE_ROOT/feature-incremental.lock/reclaim"
printf '99999999\n' >"$INCREMENTAL_STALE_ROOT/feature-incremental.lock/pid"
printf 'stale-incremental-token\n' >"$INCREMENTAL_STALE_ROOT/feature-incremental.lock/token"
touch -t 200001010000 "$INCREMENTAL_STALE_ROOT/feature-incremental.lock/reclaim"
CALLER_LANE_INCREMENTAL_SET=1
CALLER_LANE_INCREMENTAL=1
run_lane "$FEATURE_A" "$INCREMENTAL_STALE_ROOT" "$TMP_DIR/incremental-stale.record" "$TMP_DIR/incremental-stale.out" "$TMP_DIR/incremental-stale.err" check
assert_eq 0 "$?" 'stale incremental holder was reclaimed'
wait_for_no_path "$INCREMENTAL_STALE_ROOT/feature-incremental.lock" 'stale incremental recovery released the lane lock'

# Incremental owner rotation is isolated from the default owner and target.
ROTATE_INCREMENTAL_ROOT="$TMP_DIR/lanes-rotate-incremental"
reset_call_environment
run_lane "$FEATURE_A" "$ROTATE_INCREMENTAL_ROOT" "$TMP_DIR/rotate-default.record" "$TMP_DIR/rotate-default.out" "$TMP_DIR/rotate-default.err" check
assert_eq 0 "$?" 'default target for incremental rotation check succeeded'
mkdir -p "$ROTATE_INCREMENTAL_ROOT/targets/feature"
: >"$ROTATE_INCREMENTAL_ROOT/targets/feature/default-preserved"

CALLER_LANE_INCREMENTAL_SET=1
CALLER_LANE_INCREMENTAL=1
run_lane "$FEATURE_A" "$ROTATE_INCREMENTAL_ROOT" "$TMP_DIR/rotate-incremental-first.record" "$TMP_DIR/rotate-incremental-first.out" "$TMP_DIR/rotate-incremental-first.err" check
assert_eq 0 "$?" 'first incremental owner invocation succeeded'
mkdir -p "$ROTATE_INCREMENTAL_ROOT/targets/feature-incremental"
: >"$ROTATE_INCREMENTAL_ROOT/targets/feature-incremental/rotate-sentinel"
TEST_LABEL=incremental-rotation-check
TEST_FORBID_PATH="$ROTATE_INCREMENTAL_ROOT/targets/feature-incremental/rotate-sentinel"
run_lane "$FEATURE_B" "$ROTATE_INCREMENTAL_ROOT" "$TMP_DIR/rotate-incremental-second.record" "$TMP_DIR/rotate-incremental-second.out" "$TMP_DIR/rotate-incremental-second.err" check
assert_eq 0 "$?" 'different incremental owner invocation succeeded'
assert_no_path "$ROTATE_INCREMENTAL_ROOT/targets/feature-incremental/rotate-sentinel" 'different incremental owner removed the previous incremental target'
assert_no_path "$FAKE_STATE/forbidden-path-present-incremental-rotation-check" 'incremental target was rotated before Cargo entered'
assert_path_exists "$ROTATE_INCREMENTAL_ROOT/targets/feature/default-preserved" 'incremental owner rotation preserved the default target'
expected_owner="$FEATURE_B"$'\n''feature/beta'
assert_eq "$expected_owner" "$(<"$ROTATE_INCREMENTAL_ROOT/feature-incremental.owner")" 'incremental owner record contains worktree and branch'

# Same-lane commands serialize and a waiter reports why it is blocked.
SERIAL_ROOT="$TMP_DIR/lanes-serial"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=serial-one
start_lane "$FEATURE_A" "$SERIAL_ROOT" "$TMP_DIR/serial-one.record" "$TMP_DIR/serial-one.out" "$TMP_DIR/serial-one.err" check
serial_one_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-serial-one" 'first feature Cargo command started'

TEST_LABEL=serial-two
start_lane "$FEATURE_B" "$SERIAL_ROOT" "$TMP_DIR/serial-two.record" "$TMP_DIR/serial-two.out" "$TMP_DIR/serial-two.err" check
serial_two_pid=$STARTED_PID
wait_for_text "$TMP_DIR/serial-two.err" 'waiting for feature Cargo lane' 'same-lane waiter printed a diagnostic'
assert_no_path "$FAKE_STATE/started-serial-two" 'second feature Cargo command did not overlap first'
: >"$FAKE_STATE/release-serial-one"
wait_for_pid "$serial_one_pid" 0 'first serialized feature command succeeded'
wait_for_path "$FAKE_STATE/started-serial-two" 'second feature Cargo command started after release'
: >"$FAKE_STATE/release-serial-two"
wait_for_pid "$serial_two_pid" 0 'second serialized feature command succeeded'
assert_no_path "$FAKE_STATE/overlap-feature" 'fake Cargo observed no feature overlap'

# Full hook processes from different feature worktrees may interleave commands,
# but their Cargo subprocesses remain exclusive lane writers.
HOOK_PARALLEL_ROOT="$TMP_DIR/lanes-hook-parallel"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=parallel-pre-commit-a
start_hook "$FEATURE_A" "$HOOK_PARALLEL_ROOT" "$TMP_DIR/parallel-pre-commit-a.record" "$TMP_DIR/parallel-pre-commit-a.out" "$TMP_DIR/parallel-pre-commit-a.err" "$FEATURE_A/scripts/check-pre-commit.sh"
parallel_pre_commit_a_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-parallel-pre-commit-a" 'first concurrent pre-commit entered Cargo'

TEST_LABEL=parallel-pre-commit-b
start_hook "$FEATURE_B" "$HOOK_PARALLEL_ROOT" "$TMP_DIR/parallel-pre-commit-b.record" "$TMP_DIR/parallel-pre-commit-b.out" "$TMP_DIR/parallel-pre-commit-b.err" "$FEATURE_B/scripts/check-pre-commit.sh"
parallel_pre_commit_b_pid=$STARTED_PID
wait_for_text "$TMP_DIR/parallel-pre-commit-b.err" 'waiting for feature Cargo lane' 'second concurrent pre-commit waited for the feature lane'
assert_no_path "$FAKE_STATE/started-parallel-pre-commit-b" 'concurrent pre-commit Cargo calls did not overlap'
: >"$FAKE_STATE/release-parallel-pre-commit-a"
wait_for_path "$FAKE_STATE/started-parallel-pre-commit-b" 'second concurrent pre-commit entered Cargo after release'
: >"$FAKE_STATE/release-parallel-pre-commit-b"
wait_for_pid "$parallel_pre_commit_a_pid" 0 'first concurrent pre-commit succeeded'
wait_for_pid "$parallel_pre_commit_b_pid" 0 'second concurrent pre-commit succeeded'
assert_file_line_count "$FAKE_STATE/calls-parallel-pre-commit-a" 2 "$HOOK_PARALLEL_ROOT/targets/feature" 'first concurrent pre-commit kept both Cargo calls in the feature lane'
assert_file_line_count "$FAKE_STATE/calls-parallel-pre-commit-b" 2 "$HOOK_PARALLEL_ROOT/targets/feature" 'second concurrent pre-commit kept both Cargo calls in the feature lane'
assert_no_path "$FAKE_STATE/overlap-feature" 'concurrent pre-commit hooks never overlapped lane writers'

reset_call_environment
TEST_MODE=hold
TEST_LABEL=parallel-pre-push-a
start_hook "$FEATURE_A" "$HOOK_PARALLEL_ROOT" "$TMP_DIR/parallel-pre-push-a.record" "$TMP_DIR/parallel-pre-push-a.out" "$TMP_DIR/parallel-pre-push-a.err" "$FEATURE_A/scripts/check-pre-push.sh"
parallel_pre_push_a_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-parallel-pre-push-a" 'first concurrent pre-push entered Cargo'

TEST_LABEL=parallel-pre-push-b
start_hook "$FEATURE_B" "$HOOK_PARALLEL_ROOT" "$TMP_DIR/parallel-pre-push-b.record" "$TMP_DIR/parallel-pre-push-b.out" "$TMP_DIR/parallel-pre-push-b.err" "$FEATURE_B/scripts/check-pre-push.sh"
parallel_pre_push_b_pid=$STARTED_PID
wait_for_text "$TMP_DIR/parallel-pre-push-b.err" 'waiting for feature Cargo lane' 'second concurrent pre-push waited for the feature lane'
assert_no_path "$FAKE_STATE/started-parallel-pre-push-b" 'concurrent pre-push Cargo calls did not overlap'
: >"$FAKE_STATE/release-parallel-pre-push-a"
wait_for_path "$FAKE_STATE/started-parallel-pre-push-b" 'second concurrent pre-push entered Cargo after release'
: >"$FAKE_STATE/release-parallel-pre-push-b"
wait_for_pid "$parallel_pre_push_a_pid" 0 'first concurrent pre-push succeeded'
wait_for_pid "$parallel_pre_push_b_pid" 0 'second concurrent pre-push succeeded'
assert_file_line_count "$FAKE_STATE/calls-parallel-pre-push-a" 6 "$HOOK_PARALLEL_ROOT/targets/feature" 'first concurrent pre-push kept every Cargo call in the feature lane'
assert_file_line_count "$FAKE_STATE/calls-parallel-pre-push-b" 6 "$HOOK_PARALLEL_ROOT/targets/feature" 'second concurrent pre-push kept every Cargo call in the feature lane'
assert_no_path "$FAKE_STATE/overlap-feature" 'concurrent pre-push hooks never overlapped lane writers'

# Feature and integration lanes can execute at the same time.
PARALLEL_ROOT="$TMP_DIR/lanes-parallel"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=parallel-feature
start_lane "$FEATURE_A" "$PARALLEL_ROOT" "$TMP_DIR/parallel-feature.record" "$TMP_DIR/parallel-feature.out" "$TMP_DIR/parallel-feature.err" check
parallel_feature_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-parallel-feature" 'parallel feature command started'

TEST_LABEL=parallel-integration
start_lane "$REPO" "$PARALLEL_ROOT" "$TMP_DIR/parallel-integration.record" "$TMP_DIR/parallel-integration.out" "$TMP_DIR/parallel-integration.err" check
parallel_integration_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-parallel-integration" 'integration command started while feature lane was held'
if ! kill -0 "$parallel_feature_pid" 2>/dev/null; then
  fail 'feature command exited before integration lane entered Cargo'
fi
: >"$FAKE_STATE/release-parallel-feature"
: >"$FAKE_STATE/release-parallel-integration"
wait_for_pid "$parallel_feature_pid" 0 'parallel feature command succeeded'
wait_for_pid "$parallel_integration_pid" 0 'parallel integration command succeeded'
assert_no_path "$FAKE_STATE/overlap-feature" 'parallel test observed no feature overlap'
assert_no_path "$FAKE_STATE/overlap-integration" 'parallel test observed no integration overlap'

# Default and incremental instances run concurrently, while incremental peers serialize.
MODE_PARALLEL_ROOT="$TMP_DIR/lanes-mode-parallel"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=parallel-default
start_lane "$FEATURE_A" "$MODE_PARALLEL_ROOT" "$TMP_DIR/parallel-default.record" "$TMP_DIR/parallel-default.out" "$TMP_DIR/parallel-default.err" check
parallel_default_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-parallel-default" 'parallel default command started'

CALLER_LANE_INCREMENTAL_SET=1
CALLER_LANE_INCREMENTAL=1
TEST_LABEL=parallel-incremental-one
start_lane "$FEATURE_A" "$MODE_PARALLEL_ROOT" "$TMP_DIR/parallel-incremental-one.record" "$TMP_DIR/parallel-incremental-one.out" "$TMP_DIR/parallel-incremental-one.err" check
parallel_incremental_one_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-parallel-incremental-one" 'incremental command started while default instance was held'

TEST_LABEL=parallel-incremental-two
start_lane "$FEATURE_B" "$MODE_PARALLEL_ROOT" "$TMP_DIR/parallel-incremental-two.record" "$TMP_DIR/parallel-incremental-two.out" "$TMP_DIR/parallel-incremental-two.err" check
parallel_incremental_two_pid=$STARTED_PID
wait_for_text "$TMP_DIR/parallel-incremental-two.err" 'waiting for feature-incremental Cargo lane' 'incremental waiter named its lane instance'
assert_no_path "$FAKE_STATE/started-parallel-incremental-two" 'second incremental command did not overlap its peer'
if ! kill -0 "$parallel_default_pid" 2>/dev/null; then
  fail 'default command exited while incremental instance was active'
fi
: >"$FAKE_STATE/release-parallel-incremental-one"
wait_for_pid "$parallel_incremental_one_pid" 0 'first incremental command succeeded'
wait_for_path "$FAKE_STATE/started-parallel-incremental-two" 'second incremental command started after its peer released the lock'
if ! kill -0 "$parallel_default_pid" 2>/dev/null; then
  fail 'default command did not remain independent of incremental owner rotation'
fi
: >"$FAKE_STATE/release-parallel-default"
: >"$FAKE_STATE/release-parallel-incremental-two"
wait_for_pid "$parallel_default_pid" 0 'parallel default command succeeded'
wait_for_pid "$parallel_incremental_two_pid" 0 'second incremental command succeeded'
assert_no_path "$FAKE_STATE/overlap-feature" 'mode parallel test observed no default overlap'
assert_no_path "$FAKE_STATE/overlap-feature-incremental" 'mode parallel test observed no incremental overlap'
wait_for_no_path "$MODE_PARALLEL_ROOT/feature.lock" 'parallel default lock was released'
wait_for_no_path "$MODE_PARALLEL_ROOT/feature-incremental.lock" 'parallel incremental lock was released'

# Interrupting a holder terminates Cargo and releases only its lock generation.
SIGNAL_ROOT="$TMP_DIR/lanes-signal"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=signal-holder
start_lane "$FEATURE_A" "$SIGNAL_ROOT" "$TMP_DIR/signal.record" "$TMP_DIR/signal.out" "$TMP_DIR/signal.err" check
signal_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-signal-holder" 'signal test Cargo command started'
kill -TERM "$signal_pid"
wait_for_pid "$signal_pid" 143 'terminated wrapper preserved signal status'
wait_for_no_path "$SIGNAL_ROOT/feature.lock" 'terminated wrapper released feature lock'
assert_no_path "$FAKE_STATE/active-feature" 'terminated wrapper reaped fake Cargo'

# SIGINT reaches the PID-preserving Cargo holder and releases the lane.
SIGNAL_INT_ROOT="$TMP_DIR/lanes-signal-int"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=signal-int-holder
set -m
start_lane "$FEATURE_A" "$SIGNAL_INT_ROOT" "$TMP_DIR/signal-int.record" "$TMP_DIR/signal-int.out" "$TMP_DIR/signal-int.err" check
signal_int_pid=$STARTED_PID
set +m
wait_for_path "$FAKE_STATE/started-signal-int-holder" 'SIGINT test Cargo command started'
kill -INT "$signal_int_pid"
wait_for_pid "$signal_int_pid" 130 'interrupted wrapper preserved signal status'
wait_for_no_path "$SIGNAL_INT_ROOT/feature.lock" 'interrupted wrapper released feature lock'
assert_no_path "$FAKE_STATE/active-feature" 'interrupted wrapper reaped fake Cargo'

# Cargo replaces the wrapper PID, so SIGKILL cannot leave an orphan Cargo child.
KILL_ROOT="$TMP_DIR/lanes-kill"
reset_call_environment
TEST_MODE=hold
TEST_LABEL=killed-holder
start_lane "$FEATURE_A" "$KILL_ROOT" "$TMP_DIR/killed.record" "$TMP_DIR/killed.out" "$TMP_DIR/killed.err" check
killed_holder_pid=$STARTED_PID
wait_for_path "$FAKE_STATE/started-killed-holder" 'SIGKILL test Cargo command started'
kill -KILL "$killed_holder_pid"
wait_for_pid "$killed_holder_pid" 137 'killed Cargo holder reported SIGKILL status'
if kill -0 "$killed_holder_pid" 2>/dev/null; then
  fail 'killed Cargo holder remained alive'
fi
rmdir "$FAKE_STATE/active-feature" 2>/dev/null || true

reset_call_environment
start_lane "$FEATURE_A" "$KILL_ROOT" "$TMP_DIR/killed-successor.record" "$TMP_DIR/killed-successor.out" "$TMP_DIR/killed-successor.err" check
killed_successor_pid=$STARTED_PID
wait_for_pid "$killed_successor_pid" 0 'successor ran after SIGKILL cleanup'
wait_for_no_path "$KILL_ROOT/feature.lock" 'SIGKILL cleanup released feature lock'

# Cargo failures propagate exactly and still release the lane.
EXIT_ROOT="$TMP_DIR/lanes-exit"
reset_call_environment
TEST_EXIT=37
run_lane "$FEATURE_A" "$EXIT_ROOT" "$TMP_DIR/exit.record" "$TMP_DIR/exit.out" "$TMP_DIR/exit.err" check
assert_eq 37 "$?" 'Cargo exit status propagated exactly'
wait_for_no_path "$EXIT_ROOT/feature.lock" 'Cargo failure released feature lock'

if ((FAILURES > 0)); then
  printf 'cargo-lane-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'cargo-lane-test: ok\n'
