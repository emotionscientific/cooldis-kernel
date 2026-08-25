#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
VERIFY_LINUX_SCRIPT="$SCRIPT_DIR/verify-linux.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/verify-linux-test.XXXXXX")" || exit 1
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
FAKE_BIN="$TMP_DIR/bin"
DOCKER_RECORD="$TMP_DIR/docker-record"
FAKE_DOCKER_STATE="$TMP_DIR/docker-state"
HOST_LOCK_ROOT="$TMP_DIR/host-locks"
FAILURES=0
RUN_STATUS=0
MAIN_REPO="$TMP_DIR/repo"
LINKED_WORKTREE="$MAIN_REPO/.wt/feature"
COMMA_REPO="$TMP_DIR/repo,comma"
COMMA_WORKTREE="$TMP_DIR/comma-common-worktree"
SPACE_REPO="$TMP_DIR/repo with space=ok"

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

assert_file_contains() {
  local file=$1
  local needle=$2
  local name=$3

  if [[ ! -f "$file" ]] || ! grep -Fq -- "$needle" "$file"; then
    fail "$name"
    if [[ -f "$file" ]]; then
      printf 'file: %s\ncontents:\n%s\n' "$file" "$(<"$file")" >&2
    fi
  fi
}

assert_file_excludes() {
  local file=$1
  local needle=$2
  local name=$3

  if [[ -f "$file" ]] && grep -Fq -- "$needle" "$file"; then
    fail "$name"
    printf 'unexpected: %s\nfile: %s\ncontents:\n%s\n' \
      "$needle" "$file" "$(<"$file")" >&2
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

wait_for_path() {
  local path=$1
  local name=$2
  local attempts=0

  while [[ ! -e "$path" && $attempts -lt 200 ]]; do
    sleep 0.05
    attempts=$((attempts + 1))
  done
  if [[ ! -e "$path" ]]; then
    fail "$name"
    return 1
  fi
}

wait_for_text() {
  local file=$1
  local needle=$2
  local name=$3
  local attempts=0

  while { [[ ! -f "$file" ]] || ! grep -Fq -- "$needle" "$file"; } \
    && ((attempts < 200)); do
    sleep 0.05
    attempts=$((attempts + 1))
  done
  if [[ ! -f "$file" ]] || ! grep -Fq -- "$needle" "$file"; then
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
    sleep 15
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

run_wrapper() {
  local cwd=$1
  local name=$2
  shift 2

  (
    cd "$cwd" || exit 1
    PATH="$FAKE_BIN:/usr/bin:/bin" \
      FAKE_DOCKER_RECORD="$DOCKER_RECORD" \
      FAKE_DOCKER_MEMORY="${FAKE_DOCKER_MEMORY:-17179869184}" \
      FAKE_DOCKER_RUN_EXIT="${FAKE_DOCKER_RUN_EXIT:-0}" \
      FAKE_DOCKER_MODE="${FAKE_DOCKER_MODE:-record}" \
      FAKE_DOCKER_LABEL="${FAKE_DOCKER_LABEL:-record}" \
      FAKE_DOCKER_STATE="$FAKE_DOCKER_STATE" \
      VERLET_VERIFY_LINUX_LOCK_ROOT="$HOST_LOCK_ROOT" \
      "$cwd/scripts/verify-linux.sh" "$@"
  ) >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"
  RUN_STATUS=$?
}

STARTED_PID=
start_wrapper() {
  local cwd=$1
  local name=$2
  shift 2

  (
    cd "$cwd" || exit 1
    PATH="$FAKE_BIN:/usr/bin:/bin" \
      FAKE_DOCKER_RECORD="$DOCKER_RECORD" \
      FAKE_DOCKER_MEMORY="${FAKE_DOCKER_MEMORY:-17179869184}" \
      FAKE_DOCKER_RUN_EXIT="${FAKE_DOCKER_RUN_EXIT:-0}" \
      FAKE_DOCKER_MODE="${FAKE_DOCKER_MODE:-record}" \
      FAKE_DOCKER_LABEL="${FAKE_DOCKER_LABEL:-record}" \
      FAKE_DOCKER_STATE="$FAKE_DOCKER_STATE" \
      VERLET_VERIFY_LINUX_LOCK_ROOT="$HOST_LOCK_ROOT" \
      exec "$cwd/scripts/verify-linux.sh" "$@"
  ) >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err" &
  STARTED_PID=$!
}

if [[ ! -x "$VERIFY_LINUX_SCRIPT" ]]; then
  printf 'verify-linux-test: missing executable %s\n' "$VERIFY_LINUX_SCRIPT" >&2
  exit 1
fi

mkdir -p "$FAKE_BIN" "$FAKE_DOCKER_STATE" "$HOST_LOCK_ROOT" \
  "$MAIN_REPO/scripts"
cp "$VERIFY_LINUX_SCRIPT" "$MAIN_REPO/scripts/verify-linux.sh"
chmod +x "$MAIN_REPO/scripts/verify-linux.sh"

cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -u

die() {
  printf 'fake docker: %s\n' "$1" >&2
  exit 64
}

record_args() {
  local arg

  printf 'call' >>"$FAKE_DOCKER_RECORD"
  for arg in "$@"; do
    printf ' <%s>' "$arg" >>"$FAKE_DOCKER_RECORD"
  done
  printf '\n' >>"$FAKE_DOCKER_RECORD"
}

expect_arg() {
  local expected=$1
  local actual=${2-}

  [[ "$actual" == "$expected" ]] \
    || die "expected argument '$expected', got '$actual'"
}

record_args "$@"
case "${1:-}" in
  info)
    if (($# == 1)); then
      exit 0
    fi
    if (($# == 3)) \
      && [[ "$2" == "--format" && "$3" == '{{.MemTotal}}' ]]; then
      printf '%s\n' "${FAKE_DOCKER_MEMORY:-17179869184}"
      exit 0
    fi
    die 'unexpected docker info arguments'
    ;;
  run)
    shift
    expect_arg --rm "${1-}"
    shift
    expect_arg --platform "${1-}"
    shift
    platform=${1-}
    case "$platform" in
      linux/arm64) host_triple=aarch64-unknown-linux-gnu ;;
      linux/amd64) host_triple=x86_64-unknown-linux-gnu ;;
      *) die "unexpected platform '$platform'" ;;
    esac
    shift
    expect_arg --mount "${1-}"
    shift
    [[ "${1-}" == type=bind,src=*,dst=/workspace ]] \
      || die 'workspace mount does not use Docker long syntax'
    shift
    expect_arg --mount "${1-}"
    shift
    [[ "${1-}" == type=bind,src=*,dst=*,readonly ]] \
      || die 'Git mount does not use read-only Docker long syntax'
    shift
    expect_arg --mount "${1-}"
    shift
    expect_arg 'type=volume,src=verlet-verify-linux,dst=/verlet-cache' "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg CARGO_HOME=/verlet-cache/cargo "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg RUSTUP_HOME=/verlet-cache/rustup "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg RUSTUP_TOOLCHAIN=1.97.1 "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg \
      "VERLET_CARGO_LANE_ROOT=/verlet-cache/cargo-lanes/$host_triple" \
      "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg VERLET_VERIFY_MANAGED_CARGO=1 "${1-}"
    shift
    if [[ "${1-}" == --env && "${2-}" == CARGO_BUILD_JOBS=2 ]]; then
      shift 2
    fi
    expect_arg --workdir "${1-}"
    shift
    expect_arg /workspace "${1-}"
    shift
    expect_arg rust:1.97.1-bookworm "${1-}"
    shift
    expect_arg bash "${1-}"
    shift
    expect_arg -c "${1-}"
    shift
    (($# == 1)) || die 'container command must be one bash -c argument'
    bash -n <<<"$1" \
      || die 'container command is not syntactically valid Bash'
    [[ "$1" == *"trap 'exit 130' INT"* ]] \
      || die 'container command does not map Ctrl-C to status 130'
    [[ "$1" == *"trap 'exit 143' TERM"* ]] \
      || die 'container command does not map termination to status 143'
    [[ "$1" == *'exec 9>/verlet-cache/verify.lock'* ]] \
      || die 'container command does not open the volume lock'
    [[ "$1" == *'flock -n 9'* ]] \
      || die 'container command does not serialize volume users'
    [[ "$1" == *"/verlet-cache/cargo-lanes/$host_triple/*.lock"* ]] \
      || die 'container command does not settle platform-specific lane locks'
    [[ "$1" == *'bash scripts/verify.sh'* ]] \
      || die 'container command does not run the full verifier'
    [[ "$1" == *'ps -eo pid,pgid,sid,stat,args'* ]] \
      || die 'container command does not capture process snapshots'
    [[ "$1" == *'>> "$snapshot_log"'* ]] \
      || die 'process snapshots are not appended to the diagnostic log'
    [[ "$1" == *'date -u +%Y-%m-%dT%H:%M:%SZ'* ]] \
      || die 'process snapshots do not carry UTC timestamps'
    [[ "$1" == *'snapshot_limit_bytes=$((20 * 1024 * 1024))'* ]] \
      || die 'process snapshot log is not bounded to about 20 MB'
    [[ "$1" == *'mktemp "$snapshot_log.trim.XXXXXX"'* ]] \
      && [[ "$1" == *'rm -f "$snapshot_log".trim.* 2>/dev/null || true'* ]] \
      || die 'snapshot trim files are not unique and cleaned across runs'
    [[ "$1" == *"trap 'stop_snapshot_sleep; exit 0' HUP INT QUIT TERM"* ]] \
      || die 'stopping the snapshot watcher can orphan its sleep process'
    [[ "$1" == *'if ((verify_status == 137)); then'* ]] \
      || die 'container command does not diagnose a killed verifier'
    [[ "$1" == *"grep -n '^=== process snapshot ' \"\$snapshot_log\" 2>/dev/null | tail -n 3"* ]] \
      || die 'killed verifier diagnostics do not select recent snapshots'
    [[ "$1" == *'capture Docker Desktop VM dmesg before the VM restarts'* ]] \
      || die 'killed verifier diagnostics omit the dmesg reminder'
    [[ "$1" == *'print_process_snapshot_diagnostics >&2 || true'* ]] \
      || die 'snapshot diagnostics can replace the verifier status on output failure'
    [[ "$1" == *'if ((verify_status == 0)); then'* ]] \
      && [[ "$1" == *'rm -f "$snapshot_log" || true'* ]] \
      || die 'clean verification does not remove the snapshot log'
    [[ "$1" == *'exit "$verify_status"'* ]] \
      || die 'container command does not preserve the verifier status'
    if [[ "${FAKE_DOCKER_MODE:-record}" == hold ]]; then
      : >"$FAKE_DOCKER_STATE/started-${FAKE_DOCKER_LABEL:-record}"
      while [[ ! -e "$FAKE_DOCKER_STATE/release-${FAKE_DOCKER_LABEL:-record}" ]]; do
        sleep 0.02
      done
    fi
    exit "${FAKE_DOCKER_RUN_EXIT:-0}"
    ;;
esac
die "unexpected docker command '${1:-}'"
EOF
chmod +x "$FAKE_BIN/docker"

git -C "$MAIN_REPO" init -q -b main
git -C "$MAIN_REPO" config user.name 'Verify Linux Test'
git -C "$MAIN_REPO" config user.email verify-linux-test@example.invalid
git -C "$MAIN_REPO" add scripts/verify-linux.sh
git -C "$MAIN_REPO" commit --no-gpg-sign -qm 'fixture'
mkdir -p "$MAIN_REPO/.wt"
git -C "$MAIN_REPO" worktree add -q -b feature "$LINKED_WORKTREE"

run_wrapper "$MAIN_REPO" help --help
assert_eq 0 "$RUN_STATUS" '--help succeeded'
assert_file_contains "$TMP_DIR/help.out" \
  'usage: scripts/verify-linux.sh [--amd64] [--dry-run]' \
  '--help printed usage'

run_wrapper "$MAIN_REPO" unknown --not-a-flag
assert_eq 2 "$RUN_STATUS" 'unknown argument failed with usage status'
assert_file_contains "$TMP_DIR/unknown.err" \
  'error: unknown argument: --not-a-flag' \
  'unknown argument explained the failure'

run_wrapper "$MAIN_REPO" main-dry --dry-run
assert_eq 0 "$RUN_STATUS" 'main-checkout dry run succeeded'
assert_file_contains "$TMP_DIR/main-dry.out" 'docker run --rm' \
  'dry run printed the docker invocation'
assert_file_contains "$TMP_DIR/main-dry.out" '--platform linux/arm64' \
  'arm64 is the default platform'
assert_file_contains "$TMP_DIR/main-dry.out" \
  "type=bind\\,src=$MAIN_REPO\\,dst=/workspace" \
  'main checkout is mounted at the fixed workspace path'
assert_file_contains "$TMP_DIR/main-dry.out" \
  "type=bind\\,src=$MAIN_REPO/.git\\,dst=/workspace/.git\\,readonly" \
  'main checkout Git directory is over-mounted read-only'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'type=volume\,src=verlet-verify-linux\,dst=/verlet-cache' \
  'named volume is mounted at the cache root'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'VERLET_CARGO_LANE_ROOT=/verlet-cache/cargo-lanes/aarch64-unknown-linux-gnu' \
  'arm64 Cargo lanes use a platform-specific root inside the named volume'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'CARGO_HOME=/verlet-cache/cargo' \
  'Cargo home resolves inside the named volume'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'RUSTUP_HOME=/verlet-cache/rustup' \
  'Rustup home resolves inside the named volume'
assert_file_contains "$TMP_DIR/main-dry.out" 'rust:1.97.1-bookworm' \
  'the Linux image pins the workspace Rust version and Debian variant'
assert_file_contains "$TMP_DIR/main-dry.out" 'bash -c ' \
  'container bootstrap preserves the official image tool path'
assert_file_excludes "$TMP_DIR/main-dry.out" 'bash -lc ' \
  'container bootstrap does not let a login shell hide the image rustup shim'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'ln -sf /usr/local/cargo/bin/rustup /verlet-cache/cargo/bin/rustup' \
  'container bootstrap installs the rustup proxy in persistent Cargo home'
assert_file_contains "$TMP_DIR/main-dry.out" '--component clippy' \
  'container bootstrap installs every component used by the full verify suite'
assert_file_contains "$TMP_DIR/main-dry.out" 'stable-aarch64-unknown-linux-gnu' \
  'container bootstrap exposes the pinned toolchain under the stable name'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'reset with docker volume rm verlet-verify-linux' \
  'poisoned toolchain cache fails closed with the documented reset command'
assert_file_contains "$TMP_DIR/main-dry.out" 'verify_status=' \
  'container shutdown preserves the verify status while locks settle'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'ps -eo pid,pgid,sid,stat,args' \
  'container bootstrap captures process identity snapshots'
# This is a literal from the generated command.
# shellcheck disable=SC2016
assert_file_contains "$TMP_DIR/main-dry.out" \
  'snapshot_limit_bytes=$((20 * 1024 * 1024))' \
  'container bootstrap bounds the process snapshot log'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'capture Docker Desktop VM dmesg before the VM restarts' \
  'container bootstrap carries the host dmesg reminder'
assert_file_contains "$TMP_DIR/main-dry.out" "trap \\'exit 130\\' INT" \
  'container bootstrap maps Ctrl-C to status 130 while blocked'
assert_file_contains "$TMP_DIR/main-dry.out" "trap \\'exit 143\\' TERM" \
  'container bootstrap maps termination to status 143 while blocked'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'exec 9>/verlet-cache/verify.lock' \
  'container bootstrap opens the volume-wide verification lock'
assert_file_contains "$TMP_DIR/main-dry.out" 'flock -n 9' \
  'container bootstrap serializes users of shared PID-based lane locks'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'cargo-lanes/aarch64-unknown-linux-gnu/*.lock' \
  'container shutdown checks the arm64 lane root for unsettled locks'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'Cargo lane locks did not settle' \
  'unsettled lane locks turn a successful verification into a failure'
assert_file_excludes "$TMP_DIR/main-dry.out" \
  "$MAIN_REPO/.git/cargo-lanes" \
  'dry run does not target the host Cargo lane root'

HOST_LOCK_DIR="$HOST_LOCK_ROOT/verlet-verify-linux.lock"
assert_no_path "$HOST_LOCK_DIR" 'dry run did not acquire the host volume lock'

run_wrapper "$MAIN_REPO" host-lock-normal
assert_eq 0 "$RUN_STATUS" 'normal verification acquired and released the host lock'
assert_no_path "$HOST_LOCK_DIR" 'normal verification removed the host lock'

mkdir -p "$HOST_LOCK_DIR"
printf '99999999\n' >"$HOST_LOCK_DIR/pid"
printf 'dead-host-token\n' >"$HOST_LOCK_DIR/token"
run_wrapper "$MAIN_REPO" host-lock-stale
assert_eq 0 "$RUN_STATUS" 'dead host lock holder was reclaimed'
assert_no_path "$HOST_LOCK_DIR" 'stale host lock was removed after verification'

FAKE_DOCKER_MODE=hold
FAKE_DOCKER_LABEL=host-lock-holder
start_wrapper "$MAIN_REPO" host-lock-holder
host_lock_holder_pid=$STARTED_PID
wait_for_path "$FAKE_DOCKER_STATE/started-host-lock-holder" \
  'first verification entered Docker while holding the host lock'
wait_for_path "$HOST_LOCK_DIR/pid" 'first verification recorded its host lock PID'
assert_file_contains "$HOST_LOCK_DIR/pid" "$host_lock_holder_pid" \
  'host lock records the owning verification PID'

FAKE_DOCKER_LABEL=host-lock-waiter
set -m
start_wrapper "$MAIN_REPO" host-lock-waiter
host_lock_waiter_pid=$STARTED_PID
set +m
wait_for_text "$TMP_DIR/host-lock-waiter.err" \
  "waiting for Docker volume verlet-verify-linux host lock (holder pid $host_lock_holder_pid)" \
  'second verification named the live host lock holder while blocked'
assert_no_path "$FAKE_DOCKER_STATE/started-host-lock-waiter" \
  'second verification did not start a concurrent container'
kill -INT -- "-$host_lock_waiter_pid"
wait_for_pid "$host_lock_waiter_pid" 130 \
  'Ctrl-C stopped the host-lock waiter with status 130'
assert_file_contains "$HOST_LOCK_DIR/pid" "$host_lock_holder_pid" \
  'interrupted waiter preserved the live holder lock'
assert_no_path "$FAKE_DOCKER_STATE/started-host-lock-waiter" \
  'interrupted waiter never entered Docker'
: >"$FAKE_DOCKER_STATE/release-host-lock-holder"
wait_for_pid "$host_lock_holder_pid" 0 'first host-lock holder exited normally'
assert_no_path "$HOST_LOCK_DIR" 'normal holder exit released the host lock'

FAKE_DOCKER_LABEL=host-lock-signal
set -m
start_wrapper "$MAIN_REPO" host-lock-signal
host_lock_signal_pid=$STARTED_PID
set +m
wait_for_path "$FAKE_DOCKER_STATE/started-host-lock-signal" \
  'signal test entered Docker while holding the host lock'
wait_for_path "$HOST_LOCK_DIR/pid" 'signal test recorded its host lock PID'
kill -TERM -- "-$host_lock_signal_pid"
wait_for_pid "$host_lock_signal_pid" 143 \
  'signaled verification preserved termination status'
assert_no_path "$HOST_LOCK_DIR" 'signal exit released the host lock'
FAKE_DOCKER_MODE=record
FAKE_DOCKER_LABEL=record

run_wrapper "$LINKED_WORKTREE" worktree-dry --dry-run
assert_eq 0 "$RUN_STATUS" 'linked-worktree dry run succeeded'
assert_file_contains "$TMP_DIR/worktree-dry.out" \
  "type=bind\\,src=$LINKED_WORKTREE\\,dst=/workspace" \
  'linked worktree is mounted at the fixed workspace path'
assert_file_contains "$TMP_DIR/worktree-dry.out" \
  "type=bind\\,src=$MAIN_REPO/.git\\,dst=$MAIN_REPO/.git\\,readonly" \
  'linked worktree common Git directory remains reachable and read-only'
assert_file_excludes "$TMP_DIR/worktree-dry.out" \
  "$MAIN_REPO/.git/cargo-lanes" \
  'linked-worktree dry run does not target the host Cargo lane root'

run_wrapper "$MAIN_REPO" amd64-dry --amd64 --dry-run
assert_eq 0 "$RUN_STATUS" 'amd64 dry run succeeded'
assert_file_contains "$TMP_DIR/amd64-dry.out" '--platform linux/amd64' \
  '--amd64 selected the emulated architecture'
assert_file_contains "$TMP_DIR/amd64-dry.out" \
  'stable-x86_64-unknown-linux-gnu' \
  '--amd64 selects the matching pinned stable toolchain alias'
assert_file_contains "$TMP_DIR/amd64-dry.out" \
  'VERLET_CARGO_LANE_ROOT=/verlet-cache/cargo-lanes/x86_64-unknown-linux-gnu' \
  '--amd64 uses a target lane distinct from arm64'
assert_file_contains "$TMP_DIR/amd64-dry.out" \
  'cargo-lanes/x86_64-unknown-linux-gnu/*.lock' \
  '--amd64 settles only its platform-specific lane root'

FAKE_DOCKER_MEMORY=8320671744 run_wrapper "$MAIN_REPO" low-memory-dry --dry-run
assert_eq 0 "$RUN_STATUS" 'low-memory dry run succeeded'
assert_file_contains "$TMP_DIR/low-memory-dry.err" \
  'warning: Docker has less than 12 GB of memory' \
  'low-memory Docker VM emitted the resource warning'
assert_file_contains "$TMP_DIR/low-memory-dry.out" \
  'CARGO_BUILD_JOBS=2' \
  'low-memory Docker VM bounds concurrent compilation'

PATH=/usr/bin:/bin "$MAIN_REPO/scripts/verify-linux.sh" \
  >"$TMP_DIR/docker-missing.out" 2>"$TMP_DIR/docker-missing.err"
RUN_STATUS=$?
assert_eq 1 "$RUN_STATUS" 'missing Docker failed'
assert_file_contains "$TMP_DIR/docker-missing.err" \
  'error: Docker is unavailable; start Docker Desktop and try again' \
  'missing Docker points at Docker Desktop'

FAKE_DOCKER_RUN_EXIT=37 run_wrapper "$MAIN_REPO" exit-code
assert_eq 37 "$RUN_STATUS" 'docker run exit status passed through unchanged'
assert_no_path "$HOST_LOCK_DIR" 'nonzero Docker exit released the host lock'

mkdir -p "$COMMA_REPO/scripts"
cp "$VERIFY_LINUX_SCRIPT" "$COMMA_REPO/scripts/verify-linux.sh"
chmod +x "$COMMA_REPO/scripts/verify-linux.sh"
git -C "$COMMA_REPO" init -q -b main
git -C "$COMMA_REPO" config user.name 'Verify Linux Test'
git -C "$COMMA_REPO" config user.email verify-linux-test@example.invalid
git -C "$COMMA_REPO" add scripts/verify-linux.sh
git -C "$COMMA_REPO" commit --no-gpg-sign -qm 'fixture'
git -C "$COMMA_REPO" worktree add -q -b feature/comma "$COMMA_WORKTREE"

run_wrapper "$COMMA_REPO" comma-root --dry-run
assert_eq 1 "$RUN_STATUS" 'comma in worktree root failed clearly'
assert_file_contains "$TMP_DIR/comma-root.err" \
  'Git worktree root contains a comma, which Docker --mount cannot represent' \
  'comma in worktree root explained the mount limitation'

run_wrapper "$COMMA_WORKTREE" comma-common --dry-run
assert_eq 1 "$RUN_STATUS" 'comma in Git common directory failed clearly'
assert_file_contains "$TMP_DIR/comma-common.err" \
  'Git common directory contains a comma, which Docker --mount cannot represent' \
  'comma in Git common directory explained the mount limitation'

mkdir -p "$SPACE_REPO/scripts"
cp "$VERIFY_LINUX_SCRIPT" "$SPACE_REPO/scripts/verify-linux.sh"
chmod +x "$SPACE_REPO/scripts/verify-linux.sh"
git -C "$SPACE_REPO" init -q -b main
git -C "$SPACE_REPO" config user.name 'Verify Linux Test'
git -C "$SPACE_REPO" config user.email verify-linux-test@example.invalid
git -C "$SPACE_REPO" add scripts/verify-linux.sh
git -C "$SPACE_REPO" commit --no-gpg-sign -qm 'fixture'

run_wrapper "$SPACE_REPO" space-equals --dry-run
assert_eq 0 "$RUN_STATUS" 'spaces and equals in mount sources remained supported'
printf -v space_mount '%q' "type=bind,src=$SPACE_REPO,dst=/workspace"
assert_file_contains "$TMP_DIR/space-equals.out" "$space_mount" \
  'spaces and equals remained one quoted Docker mount argument'

if ((FAILURES > 0)); then
  printf 'verify-linux-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'verify-linux-test: ok\n'
