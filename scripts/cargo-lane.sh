#!/usr/bin/env bash
#
# Run Cargo through one of the repository's bounded, exclusive build lanes.
# Implementation contract: workspace ticket 0071 and
# plans/bounded-cargo-build-lanes-2026-07.md.

set -euo pipefail

REAL_CARGO=
ACTIVE_CARGO_SHIM_DIR=
GIT_TOPLEVEL=
GIT_COMMON_DIR=
GIT_BRANCH=
SCCACHE_BASEDIRS_DEFAULT=
LANE=
LANE_ROOT=
TARGETS_DIR=
TARGET_DIR=
LOCK_DIR=
OWNER_FILE=
LOCK_TOKEN=
LOCK_HELD=0
LOCK_READY=0
OWNER_TEMP=
RECLAIM_OWNER_FILE=
INITIALIZATION_GRACE_SECONDS=5

usage() {
  printf 'usage: scripts/cargo-lane.sh <cargo-arguments...>\n'
}

die() {
  printf 'error: %s\n' "$1" >&2
  exit 1
}

remove_path_entry() {
  local remove=$1
  local remaining=$PATH
  local entry
  local cleaned=
  local cleaned_set=0
  local more

  while true; do
    case "$remaining" in
      *:*)
        entry=${remaining%%:*}
        remaining=${remaining#*:}
        more=1
        ;;
      *)
        entry=$remaining
        more=0
        ;;
    esac

    if [[ "$entry" != "$remove" ]]; then
      if ((cleaned_set)); then
        cleaned="$cleaned:$entry"
      else
        cleaned=$entry
        cleaned_set=1
      fi
    fi
    if ((more == 0)); then
      break
    fi
  done

  PATH=$cleaned
  export PATH
}

resolve_real_cargo() {
  local candidate=${COOLDIS_REAL_CARGO:-}
  local path_cargo
  local candidate_dir
  local candidate_name
  local script_path

  if [[ -n "${COOLDIS_CARGO_LANE_SCRIPT:-}" && -z "$candidate" ]]; then
    die 'Cargo shim contract is missing COOLDIS_REAL_CARGO'
  fi

  path_cargo=$(type -P cargo || true)

  if [[ -z "$candidate" ]]; then
    candidate=$(type -P cargo || true)
  elif [[ "$candidate" != */* ]]; then
    candidate=$(type -P "$candidate" || true)
  fi

  if [[ -z "$candidate" ]]; then
    die 'could not resolve the real Cargo executable'
  fi

  if [[ "$candidate" != /* ]]; then
    candidate_dir=$(dirname "$candidate")
    candidate_name=$(basename "$candidate")
    if ! candidate_dir=$(cd "$candidate_dir" 2>/dev/null && pwd -P); then
      die "could not resolve Cargo executable: $candidate"
    fi
    candidate="$candidate_dir/$candidate_name"
  fi

  if [[ ! -f "$candidate" || ! -x "$candidate" ]]; then
    die "Cargo executable is not executable: $candidate"
  fi

  script_path="$(
    cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P
  )/$(basename "${BASH_SOURCE[0]}")"
  if [[ "$candidate" -ef "$script_path" ]]; then
    die 'real Cargo resolved back to scripts/cargo-lane.sh'
  fi

  REAL_CARGO=$candidate

  if [[ -n "${COOLDIS_CARGO_SHIM_DIR:-}" ]]; then
    ACTIVE_CARGO_SHIM_DIR=$COOLDIS_CARGO_SHIM_DIR
  elif [[ -n "${COOLDIS_CARGO_LANE_SCRIPT:-}" \
    && -n "$path_cargo" \
    && ! "$path_cargo" -ef "$REAL_CARGO" ]]; then
    ACTIVE_CARGO_SHIM_DIR=$(cd "$(dirname "$path_cargo")" && pwd -P)
  fi
}

resolve_git_context() {
  local git_top
  local git_common
  local lane_root
  local main_checkout

  if ! git_top=$(git rev-parse --show-toplevel 2>/dev/null); then
    die 'cargo lane must run from inside a Git worktree'
  fi
  if ! GIT_TOPLEVEL=$(cd "$git_top" 2>/dev/null && pwd -P); then
    die "could not resolve Git worktree root: $git_top"
  fi

  if ! git_common=$(
    git -C "$GIT_TOPLEVEL" rev-parse --git-common-dir 2>/dev/null
  ); then
    die 'could not resolve the Git common directory'
  fi
  if [[ "$git_common" != /* ]]; then
    git_common="$GIT_TOPLEVEL/$git_common"
  fi
  if ! GIT_COMMON_DIR=$(cd "$git_common" 2>/dev/null && pwd -P); then
    die "could not resolve Git common directory: $git_common"
  fi

  if ! GIT_BRANCH=$(
    git -C "$GIT_TOPLEVEL" symbolic-ref --quiet --short HEAD
  ); then
    GIT_BRANCH=HEAD
  fi

  if [[ -n "${COOLDIS_CARGO_LANE_ROOT:-}" ]]; then
    lane_root=$COOLDIS_CARGO_LANE_ROOT
    if [[ "$lane_root" != /* ]]; then
      die 'COOLDIS_CARGO_LANE_ROOT must be an absolute path'
    fi
  else
    lane_root="$GIT_COMMON_DIR/cargo-lanes"
  fi

  mkdir -p "$lane_root/targets"
  if ! LANE_ROOT=$(cd "$lane_root" 2>/dev/null && pwd -P); then
    die "could not resolve Cargo lane root: $lane_root"
  fi
  if [[ -z "${COOLDIS_CARGO_LANE_ROOT:-}" \
    && "$LANE_ROOT" != "$GIT_COMMON_DIR/cargo-lanes" ]]; then
    die 'default Cargo lane root resolves outside the Git common directory'
  fi
  if ! TARGETS_DIR=$(cd "$LANE_ROOT/targets" 2>/dev/null && pwd -P); then
    die 'could not resolve Cargo lane target root'
  fi
  if [[ "$TARGETS_DIR" != "$LANE_ROOT/targets" ]]; then
    die 'Cargo lane target root is not the expected child of the lane root'
  fi

  main_checkout=$(dirname "$GIT_COMMON_DIR")
  if ! SCCACHE_BASEDIRS_DEFAULT=$(
    cd "$main_checkout/.." 2>/dev/null && pwd -P
  ); then
    die 'could not resolve the workspace root for sccache'
  fi
}

select_lane() {
  case "$GIT_BRANCH" in
    main | integration/*)
      LANE=integration
      ;;
    *)
      LANE=feature
      ;;
  esac

  TARGET_DIR="$TARGETS_DIR/$LANE"
  LOCK_DIR="$LANE_ROOT/$LANE.lock"
  OWNER_FILE="$LANE_ROOT/$LANE.owner"

  if [[ "$TARGET_DIR" != "$LANE_ROOT/targets/$LANE" ]]; then
    die 'Cargo lane target is not the expected lane-root child'
  fi
}

reject_target_override() {
  local arg
  local alias_name
  local alias_value

  for arg in "$@"; do
    case "$arg" in
      --target-dir | --target-dir=*)
        die 'managed Cargo lane does not allow --target-dir overrides'
        ;;
      *alias.*--target-dir*)
        die 'managed Cargo lane does not allow alias --target-dir overrides'
        ;;
    esac
  done

  while IFS= read -r alias_name; do
    case "$alias_name" in
      CARGO_ALIAS_*)
        alias_value=${!alias_name}
        if [[ "$alias_value" == *--target-dir* ]]; then
          die "managed Cargo lane does not allow $alias_name target overrides"
        fi
        ;;
    esac
  done < <(compgen -e)
}

read_lock_field() {
  local file=$1
  local value=

  if [[ -r "$file" ]]; then
    IFS= read -r value <"$file" || true
  fi
  printf '%s' "$value"
}

valid_holder_pid() {
  [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

path_older_than_initialization_grace() {
  local path=$1
  local modified
  local now

  if modified=$(stat -f '%m' "$path" 2>/dev/null); then
    :
  elif modified=$(stat -c '%Y' "$path" 2>/dev/null); then
    :
  else
    return 1
  fi
  now=$(date +%s)
  ((now - modified >= INITIALIZATION_GRACE_SECONDS))
}

release_lane_lock() {
  local recorded_token

  if ((LOCK_HELD == 0)); then
    return
  fi

  recorded_token=$(read_lock_field "$LOCK_DIR/token")
  if [[ -n "$LOCK_TOKEN" && "$recorded_token" == "$LOCK_TOKEN" ]]; then
    rm -f "$LOCK_DIR/pid" "$LOCK_DIR/token"
    rmdir "$LOCK_DIR" 2>/dev/null || true
  elif ((LOCK_READY == 0)) && [[ -d "$LOCK_DIR" && -z "$recorded_token" ]]; then
    rmdir "$LOCK_DIR" 2>/dev/null || true
  fi

  LOCK_HELD=0
  LOCK_READY=0
}

cleanup_on_exit() {
  local status=$?

  trap - EXIT HUP INT QUIT TERM
  if [[ -n "$OWNER_TEMP" ]]; then
    rm -f "$OWNER_TEMP"
  fi
  release_lane_lock
  exit "$status"
}

handle_signal() {
  trap - "$1"
  exit "$2"
}

try_reclaim_stale_lock() {
  local observed_pid=$1
  local observed_token=$2
  local current_pid
  local current_token

  if ! acquire_reclaim_claim; then
    return 1
  fi

  current_pid=$(read_lock_field "$LOCK_DIR/pid")
  current_token=$(read_lock_field "$LOCK_DIR/token")
  if [[ "$current_pid" != "$observed_pid" \
    || "$current_token" != "$observed_token" ]]; then
    release_reclaim_claim
    return 1
  fi
  if valid_holder_pid "$current_pid" && kill -0 "$current_pid" 2>/dev/null; then
    release_reclaim_claim
    return 1
  fi

  rm -f "$LOCK_DIR/pid" "$LOCK_DIR/token"
  release_reclaim_claim
  rmdir "$LOCK_DIR" 2>/dev/null || return 1
  return 0
}

recover_abandoned_reclaim_claim() {
  local owner_file
  local owner_pid

  if [[ ! -d "$LOCK_DIR/reclaim" ]]; then
    return
  fi
  if ! path_older_than_initialization_grace "$LOCK_DIR/reclaim"; then
    return
  fi

  for owner_file in "$LOCK_DIR"/reclaim/owner.*.*; do
    if [[ ! -f "$owner_file" ]]; then
      continue
    fi
    owner_pid=$(read_lock_field "$owner_file")
    if ! valid_holder_pid "$owner_pid" \
      || ! kill -0 "$owner_pid" 2>/dev/null; then
      rm -f "$owner_file"
    fi
  done
  rmdir "$LOCK_DIR/reclaim" 2>/dev/null || true
}

acquire_reclaim_claim() {
  local claimant_pid
  local owner_file

  if ! mkdir "$LOCK_DIR/reclaim" 2>/dev/null; then
    recover_abandoned_reclaim_claim
    return 1
  fi

  owner_file="$LOCK_DIR/reclaim/owner.$RANDOM.$RANDOM"
  if ! sh -c 'printf "%s\n" "$PPID" >"$1"' sh "$owner_file" 2>/dev/null; then
    rmdir "$LOCK_DIR/reclaim" 2>/dev/null || true
    return 1
  fi
  claimant_pid=$(read_lock_field "$owner_file")
  if ! valid_holder_pid "$claimant_pid" \
    || ! kill -0 "$claimant_pid" 2>/dev/null; then
    rm -f "$owner_file"
    rmdir "$LOCK_DIR/reclaim" 2>/dev/null || true
    return 1
  fi
  RECLAIM_OWNER_FILE=$owner_file
  return 0
}

release_reclaim_claim() {
  if [[ -n "$RECLAIM_OWNER_FILE" ]]; then
    rm -f "$RECLAIM_OWNER_FILE"
    RECLAIM_OWNER_FILE=
  fi
  rmdir "$LOCK_DIR/reclaim" 2>/dev/null || true
}

acquire_lane_lock() {
  local holder_pid
  local holder_token
  local missing_checks=0
  local missing_token=
  local printed_wait=0

  LOCK_TOKEN="$$.$RANDOM.$RANDOM"
  while true; do
    if mkdir "$LOCK_DIR" 2>/dev/null; then
      LOCK_HELD=1
      trap cleanup_on_exit EXIT
      trap 'handle_signal HUP 129' HUP
      trap 'handle_signal INT 130' INT
      trap 'handle_signal QUIT 131' QUIT
      trap 'handle_signal TERM 143' TERM

      if ! printf '%s\n' "$LOCK_TOKEN" >"$LOCK_DIR/token"; then
        die "could not write $LANE Cargo lane lock token"
      fi
      if ! printf '%s\n' "$$" >"$LOCK_DIR/pid"; then
        die "could not write $LANE Cargo lane holder PID"
      fi
      LOCK_READY=1
      return
    fi

    if [[ -L "$LOCK_DIR" || ( -e "$LOCK_DIR" && ! -d "$LOCK_DIR" ) ]]; then
      die "refusing unexpected $LANE Cargo lane lock path: $LOCK_DIR"
    fi
    if [[ ! -d "$LOCK_DIR" ]]; then
      if [[ ! -w "$LANE_ROOT" ]]; then
        die "Cargo lane root is not writable: $LANE_ROOT"
      fi
      sleep 0.1
      continue
    fi

    holder_pid=$(read_lock_field "$LOCK_DIR/pid")
    holder_token=$(read_lock_field "$LOCK_DIR/token")
    if ((printed_wait == 0)); then
      if valid_holder_pid "$holder_pid"; then
        printf 'cargo-lane: waiting for %s Cargo lane (holder pid %s)\n' \
          "$LANE" "$holder_pid" >&2
      else
        printf 'cargo-lane: waiting for %s Cargo lane (holder initializing)\n' \
          "$LANE" >&2
      fi
      printed_wait=1
    fi

    if valid_holder_pid "$holder_pid"; then
      missing_checks=0
      missing_token=
      if ! kill -0 "$holder_pid" 2>/dev/null; then
        try_reclaim_stale_lock "$holder_pid" "$holder_token" || true
      fi
    elif [[ -z "$holder_token" ]]; then
      missing_checks=0
      missing_token=
      if path_older_than_initialization_grace "$LOCK_DIR"; then
        try_reclaim_stale_lock "$holder_pid" "$holder_token" || true
      fi
    else
      if [[ "$holder_token" != "$missing_token" ]]; then
        missing_token=$holder_token
        missing_checks=0
      fi
      missing_checks=$((missing_checks + 1))
      if ((missing_checks * 1 >= INITIALIZATION_GRACE_SECONDS * 10)); then
        try_reclaim_stale_lock "$holder_pid" "$holder_token" || true
        missing_checks=0
      fi
    fi

    sleep 0.1
  done
}

start_lock_monitor() {
  local holder_pid=$$
  local lock_token=$LOCK_TOKEN

  (
    trap - EXIT
    trap '' HUP INT QUIT TERM
    while kill -0 "$holder_pid" 2>/dev/null; do
      sleep 0.5
    done
    try_reclaim_stale_lock "$holder_pid" "$lock_token" || true
  ) </dev/null >/dev/null 2>&1 &
}

rotate_lane_owner() {
  local current_owner=
  local expected_owner
  local resolved_target

  expected_owner="$GIT_TOPLEVEL"$'\n'"$GIT_BRANCH"
  if [[ -f "$OWNER_FILE" ]]; then
    current_owner=$(<"$OWNER_FILE")
  fi
  if [[ "$current_owner" == "$expected_owner" ]]; then
    return
  fi

  if [[ "$LANE" != feature && "$LANE" != integration ]]; then
    die "refusing to rotate unknown Cargo lane: $LANE"
  fi
  if [[ "$TARGETS_DIR" != "$LANE_ROOT/targets" \
    || "$TARGET_DIR" != "$TARGETS_DIR/$LANE" ]]; then
    die 'refusing to delete an unexpected Cargo target path'
  fi

  if [[ -L "$TARGET_DIR" ]]; then
    rm -f "$TARGET_DIR"
  elif [[ -e "$TARGET_DIR" ]]; then
    if [[ ! -d "$TARGET_DIR" ]]; then
      die "refusing to delete non-directory Cargo target: $TARGET_DIR"
    fi
    if ! resolved_target=$(cd "$TARGET_DIR" 2>/dev/null && pwd -P); then
      die "could not resolve Cargo target before rotation: $TARGET_DIR"
    fi
    if [[ "$resolved_target" != "$TARGET_DIR" ]]; then
      die 'refusing to recursively delete a Cargo target outside the lane root'
    fi
    rm -rf "$TARGET_DIR"
  fi

  OWNER_TEMP="$OWNER_FILE.tmp.$$.$RANDOM"
  printf '%s\n%s\n' "$GIT_TOPLEVEL" "$GIT_BRANCH" >"$OWNER_TEMP"
  mv "$OWNER_TEMP" "$OWNER_FILE"
  OWNER_TEMP=
}

run_cargo() {
  local sccache

  export CARGO_TARGET_DIR="$TARGET_DIR"
  unset CARGO_BUILD_TARGET_DIR
  export CARGO_INCREMENTAL=0
  export CARGO_PROFILE_DEV_DEBUG=line-tables-only
  export CARGO_PROFILE_TEST_DEBUG=line-tables-only
  if [[ -z "${CARGO_BUILD_JOBS+x}" ]]; then
    export CARGO_BUILD_JOBS=8
  fi

  export SCCACHE_BASEDIRS="$SCCACHE_BASEDIRS_DEFAULT"
  if [[ -z "${SCCACHE_CACHE_SIZE+x}" ]]; then
    export SCCACHE_CACHE_SIZE=10G
  fi
  if sccache=$(type -P sccache 2>/dev/null); then
    export RUSTC_WRAPPER="$sccache"
  else
    unset RUSTC_WRAPPER
    printf 'warning: sccache not found; continuing without compiler cache\n' >&2
  fi

  if [[ -n "$ACTIVE_CARGO_SHIM_DIR" ]]; then
    remove_path_entry "$ACTIVE_CARGO_SHIM_DIR"
  fi
  unset COOLDIS_CARGO_LANE_SCRIPT COOLDIS_CARGO_SHIM_DIR COOLDIS_REAL_CARGO

  start_lock_monitor
  exec "$REAL_CARGO" "$@"
}

main() {
  if (($# == 0)); then
    usage >&2
    exit 2
  fi

  resolve_real_cargo
  resolve_git_context
  select_lane
  reject_target_override "$@"
  acquire_lane_lock
  rotate_lane_owner
  run_cargo "$@"
}

main "$@"
