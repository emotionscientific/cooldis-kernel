#!/usr/bin/env bash

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
VERIFY_LINUX_SCRIPT="$SCRIPT_DIR/verify-linux.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/verify-linux-test.XXXXXX")" || exit 1
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
FAKE_BIN="$TMP_DIR/bin"
DOCKER_RECORD="$TMP_DIR/docker-record"
FAILURES=0
RUN_STATUS=0
MAIN_REPO="$TMP_DIR/repo"
LINKED_WORKTREE="$MAIN_REPO/.wt/feature"
COMMA_REPO="$TMP_DIR/repo,comma"
COMMA_WORKTREE="$TMP_DIR/comma-common-worktree"
SPACE_REPO="$TMP_DIR/repo with space=ok"

cleanup() {
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
      "$cwd/scripts/verify-linux.sh" "$@"
  ) >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"
  RUN_STATUS=$?
}

if [[ ! -x "$VERIFY_LINUX_SCRIPT" ]]; then
  printf 'verify-linux-test: missing executable %s\n' "$VERIFY_LINUX_SCRIPT" >&2
  exit 1
fi

mkdir -p "$FAKE_BIN" "$MAIN_REPO/scripts"
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
    expect_arg 'type=volume,src=cooldis-verify-linux,dst=/cooldis-cache' "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg CARGO_HOME=/cooldis-cache/cargo "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg RUSTUP_HOME=/cooldis-cache/rustup "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg RUSTUP_TOOLCHAIN=1.97.1 "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg \
      "COOLDIS_CARGO_LANE_ROOT=/cooldis-cache/cargo-lanes/$host_triple" \
      "${1-}"
    shift
    expect_arg --env "${1-}"
    shift
    expect_arg COOLDIS_VERIFY_MANAGED_CARGO=1 "${1-}"
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
    [[ "$1" == *"trap 'exit 130' INT"* ]] \
      || die 'container command does not map Ctrl-C to status 130'
    [[ "$1" == *"trap 'exit 143' TERM"* ]] \
      || die 'container command does not map termination to status 143'
    [[ "$1" == *'exec 9>/cooldis-cache/verify.lock'* ]] \
      || die 'container command does not open the volume lock'
    [[ "$1" == *'flock -n 9'* ]] \
      || die 'container command does not serialize volume users'
    [[ "$1" == *"/cooldis-cache/cargo-lanes/$host_triple/*.lock"* ]] \
      || die 'container command does not settle platform-specific lane locks'
    [[ "$1" == *'bash scripts/verify.sh'* ]] \
      || die 'container command does not run the full verifier'
    [[ "$1" == *'exit "$verify_status"'* ]] \
      || die 'container command does not preserve the verifier status'
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
  'type=volume\,src=cooldis-verify-linux\,dst=/cooldis-cache' \
  'named volume is mounted at the cache root'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'COOLDIS_CARGO_LANE_ROOT=/cooldis-cache/cargo-lanes/aarch64-unknown-linux-gnu' \
  'arm64 Cargo lanes use a platform-specific root inside the named volume'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'CARGO_HOME=/cooldis-cache/cargo' \
  'Cargo home resolves inside the named volume'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'RUSTUP_HOME=/cooldis-cache/rustup' \
  'Rustup home resolves inside the named volume'
assert_file_contains "$TMP_DIR/main-dry.out" 'rust:1.97.1-bookworm' \
  'the Linux image pins the workspace Rust version and Debian variant'
assert_file_contains "$TMP_DIR/main-dry.out" 'bash -c ' \
  'container bootstrap preserves the official image tool path'
assert_file_excludes "$TMP_DIR/main-dry.out" 'bash -lc ' \
  'container bootstrap does not let a login shell hide the image rustup shim'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'ln -sf /usr/local/cargo/bin/rustup /cooldis-cache/cargo/bin/rustup' \
  'container bootstrap installs the rustup proxy in persistent Cargo home'
assert_file_contains "$TMP_DIR/main-dry.out" '--component clippy' \
  'container bootstrap installs every component used by the full verify suite'
assert_file_contains "$TMP_DIR/main-dry.out" 'stable-aarch64-unknown-linux-gnu' \
  'container bootstrap exposes the pinned toolchain under the stable name'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'reset with docker volume rm cooldis-verify-linux' \
  'poisoned toolchain cache fails closed with the documented reset command'
assert_file_contains "$TMP_DIR/main-dry.out" 'verify_status=' \
  'container shutdown preserves the verify status while locks settle'
assert_file_contains "$TMP_DIR/main-dry.out" "trap \\'exit 130\\' INT" \
  'container bootstrap maps Ctrl-C to status 130 while blocked'
assert_file_contains "$TMP_DIR/main-dry.out" "trap \\'exit 143\\' TERM" \
  'container bootstrap maps termination to status 143 while blocked'
assert_file_contains "$TMP_DIR/main-dry.out" \
  'exec 9>/cooldis-cache/verify.lock' \
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
  'COOLDIS_CARGO_LANE_ROOT=/cooldis-cache/cargo-lanes/x86_64-unknown-linux-gnu' \
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
