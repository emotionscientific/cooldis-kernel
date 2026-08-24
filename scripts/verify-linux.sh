#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
IMAGE=rust:1.97.1-bookworm
RUST_TOOLCHAIN=1.97.1
PLATFORM=linux/arm64
HOST_TRIPLE=aarch64-unknown-linux-gnu
VOLUME=verlet-verify-linux
CACHE_ROOT=/verlet-cache
CONTAINER_WORKSPACE=/workspace
DRY_RUN=0
LOW_MEMORY=0

usage() {
  printf 'usage: scripts/verify-linux.sh [--amd64] [--dry-run]\n'
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit "${2:-1}"
}

print_command() {
  local arg

  for arg in "$@"; do
    printf '%q ' "$arg"
  done
  printf '\n'
}

validate_mount_source() {
  local path=$1
  local label=$2

  if [[ "$path" == *','* ]]; then
    die "$label contains a comma, which Docker --mount cannot represent: $path"
  fi
  if [[ "$path" == *$'\n'* || "$path" == *$'\r'* ]]; then
    die "$label contains a line break, which Docker --mount cannot represent"
  fi
}

while (($# > 0)); do
  case "$1" in
    --amd64)
      PLATFORM=linux/amd64
      HOST_TRIPLE=x86_64-unknown-linux-gnu
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      usage >&2
      die "unknown argument: $1" 2
      ;;
  esac
  shift
done

if ! command -v docker >/dev/null 2>&1; then
  die 'Docker is unavailable; start Docker Desktop and try again'
fi
if ! docker info >/dev/null 2>&1; then
  die 'cannot reach the Docker daemon; start Docker Desktop and try again'
fi

docker_memory=$(docker info --format '{{.MemTotal}}' 2>/dev/null || true)
if [[ "$docker_memory" =~ ^[0-9]+$ ]] \
  && ((docker_memory < 12 * 1024 * 1024 * 1024)); then
  LOW_MEMORY=1
  printf '%s\n' \
    'warning: Docker has less than 12 GB of memory; the full suite may fail.' \
    'Increase the memory limit in Docker Desktop Settings > Resources.' >&2
fi

if ! git_top=$(git -C "$ROOT" rev-parse --show-toplevel 2>/dev/null); then
  die 'verify-linux must run from inside a Git worktree'
fi
if ! git_top=$(cd "$git_top" && pwd -P); then
  die "could not resolve Git worktree root: $git_top"
fi
if [[ "$git_top" != "$ROOT" ]]; then
  die "script directory is not in the Git worktree root: $ROOT"
fi
validate_mount_source "$ROOT" 'Git worktree root'

if ! git_common=$(git -C "$ROOT" rev-parse --git-common-dir 2>/dev/null); then
  die 'could not resolve the Git common directory'
fi
if [[ "$git_common" != /* ]]; then
  git_common="$ROOT/$git_common"
fi
if ! git_common=$(cd "$git_common" && pwd -P); then
  die "could not resolve Git common directory: $git_common"
fi
validate_mount_source "$git_common" 'Git common directory'

CARGO_LANE_ROOT="$CACHE_ROOT/cargo-lanes/$HOST_TRIPLE"

if [[ -d "$ROOT/.git" ]]; then
  git_mount="type=bind,src=$git_common,dst=$CONTAINER_WORKSPACE/.git,readonly"
elif [[ -f "$ROOT/.git" ]]; then
  # Linked-worktree .git files contain an absolute host path. Preserve that
  # path in the container so Git can follow it to the read-only common gitdir.
  git_mount="type=bind,src=$git_common,dst=$git_common,readonly"
else
  die "unsupported Git metadata at $ROOT/.git"
fi

container_command="set -euo pipefail
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 131' QUIT
trap 'exit 143' TERM
git config --global --add safe.directory $CONTAINER_WORKSPACE
exec 9>$CACHE_ROOT/verify.lock
if ! flock -n 9; then
  printf 'waiting for another Linux verification to release the shared cache\n' >&2
  while ! flock -n 9; do
    sleep 0.1
  done
fi
mkdir -p $CACHE_ROOT/cargo/bin
ln -sf /usr/local/cargo/bin/rustup $CACHE_ROOT/cargo/bin/rustup
for proxy in cargo cargo-clippy cargo-fmt clippy-driver rustc rustdoc rustfmt; do
  ln -sf rustup $CACHE_ROOT/cargo/bin/\$proxy
done
export PATH=$CACHE_ROOT/cargo/bin:\$PATH
rustup toolchain install $RUST_TOOLCHAIN --profile minimal --component rustfmt --component clippy --target wasm32-unknown-unknown
pinned_toolchain=$CACHE_ROOT/rustup/toolchains/$RUST_TOOLCHAIN-$HOST_TRIPLE
stable_toolchain=$CACHE_ROOT/rustup/toolchains/stable-$HOST_TRIPLE
if [[ -L \"\$stable_toolchain\" ]]; then
  if [[ ! \"\$stable_toolchain\" -ef \"\$pinned_toolchain\" ]]; then
    printf 'error: cached stable toolchain does not point at pinned Rust $RUST_TOOLCHAIN; reset with docker volume rm $VOLUME\\n' >&2
    exit 1
  fi
elif [[ -e \"\$stable_toolchain\" ]]; then
  printf 'error: cached stable toolchain is not the pinned alias; reset with docker volume rm $VOLUME\\n' >&2
  exit 1
else
  ln -s \"\$pinned_toolchain\" \"\$stable_toolchain\"
fi
rustup target list --installed --toolchain stable | grep -Fx wasm32-unknown-unknown >/dev/null
snapshot_log=$CACHE_ROOT/verify-process-snapshots.log
snapshot_limit_bytes=\$((20 * 1024 * 1024))
cleanup_snapshot_trims() {
  rm -f \"\$snapshot_log\".trim.* 2>/dev/null || true
}
capture_process_snapshot() {
  {
    printf '\\n=== process snapshot %s ===\\n' \"\$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
    ps -eo pid,pgid,sid,stat,args
  } >> \"\$snapshot_log\" 2>/dev/null
}
bound_snapshot_log() {
  snapshot_size=\$(stat -c %s \"\$snapshot_log\" 2>/dev/null) || return 0
  if ((snapshot_size > snapshot_limit_bytes)); then
    if snapshot_tmp=\$(mktemp \"\$snapshot_log.trim.XXXXXX\" 2>/dev/null); then
      if tail -c \"\$snapshot_limit_bytes\" \"\$snapshot_log\" > \"\$snapshot_tmp\" 2>/dev/null; then
        mv \"\$snapshot_tmp\" \"\$snapshot_log\" 2>/dev/null \
          || rm -f \"\$snapshot_tmp\" 2>/dev/null \
          || true
      else
        rm -f \"\$snapshot_tmp\" 2>/dev/null || true
      fi
    fi
  fi
}
print_process_snapshot_diagnostics() {
  printf '\\nverify shell exited 137; recent in-container process snapshots:\\n'
  if [[ ! -s \"\$snapshot_log\" ]]; then
    printf '(process snapshot log is missing or empty)\\n'
  elif snapshot_tail_line=\$(grep -n '^=== process snapshot ' \"\$snapshot_log\" 2>/dev/null | tail -n 3 | head -n 1 | cut -d: -f1); then
    tail -n \"+\$snapshot_tail_line\" \"\$snapshot_log\" 2>/dev/null \
      || printf '(process snapshot log became unavailable while reading it)\\n'
  else
    tail -n 400 \"\$snapshot_log\" 2>/dev/null \
      || printf '(process snapshot log became unavailable while reading it)\\n'
  fi
  printf 'process snapshot log: %s (Docker volume $VOLUME)\\n' \"\$snapshot_log\"
  printf 'reminder: capture Docker Desktop VM dmesg before the VM restarts.\\n'
}
cleanup_snapshot_trims
: > \"\$snapshot_log\" 2>/dev/null || true
(
  snapshot_sleep_pid=
  stop_snapshot_sleep() {
    if [[ -n \"\$snapshot_sleep_pid\" ]]; then
      kill \"\$snapshot_sleep_pid\" 2>/dev/null || true
      wait \"\$snapshot_sleep_pid\" 2>/dev/null || true
    fi
  }
  trap 'stop_snapshot_sleep; exit 0' HUP INT QUIT TERM
  while :; do
    capture_process_snapshot || true
    bound_snapshot_log || true
    sleep 1 &
    snapshot_sleep_pid=\$!
    wait \"\$snapshot_sleep_pid\" || exit 0
    snapshot_sleep_pid=
  done
) &
snapshot_watcher_pid=\$!
set +e
bash scripts/verify.sh
verify_status=\$?
kill \"\$snapshot_watcher_pid\" 2>/dev/null || true
wait \"\$snapshot_watcher_pid\" 2>/dev/null || true
cleanup_snapshot_trims
capture_process_snapshot || true
bound_snapshot_log || true
set -e
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  if ! compgen -G '$CARGO_LANE_ROOT/*.lock' >/dev/null; then
    break
  fi
  sleep 0.1
done
if compgen -G '$CARGO_LANE_ROOT/*.lock' >/dev/null; then
  printf 'error: Cargo lane locks did not settle under $CARGO_LANE_ROOT\n' >&2
  if ((verify_status == 0)); then
    verify_status=1
  fi
fi
if ((verify_status == 137)); then
  print_process_snapshot_diagnostics >&2 || true
fi
if ((verify_status == 0)); then
  rm -f \"\$snapshot_log\" || true
  cleanup_snapshot_trims
fi
exit \"\$verify_status\""

docker_run=(
  docker run --rm
  --platform "$PLATFORM"
  --mount "type=bind,src=$ROOT,dst=$CONTAINER_WORKSPACE"
  --mount "$git_mount"
  --mount "type=volume,src=$VOLUME,dst=$CACHE_ROOT"
  --env "CARGO_HOME=$CACHE_ROOT/cargo"
  --env "RUSTUP_HOME=$CACHE_ROOT/rustup"
  --env "RUSTUP_TOOLCHAIN=$RUST_TOOLCHAIN"
  --env "VERLET_CARGO_LANE_ROOT=$CARGO_LANE_ROOT"
  --env VERLET_VERIFY_MANAGED_CARGO=1
)
if ((LOW_MEMORY)); then
  docker_run+=(--env CARGO_BUILD_JOBS=2)
fi
docker_run+=(
  --workdir "$CONTAINER_WORKSPACE"
  "$IMAGE"
  bash -c "$container_command"
)

if ((DRY_RUN)); then
  print_command "${docker_run[@]}"
  exit 0
fi

"${docker_run[@]}"
