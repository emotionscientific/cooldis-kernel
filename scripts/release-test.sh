#!/usr/bin/env bash

set -u

SOURCE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/release-test.XXXXXX")"
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
ROOT="$TMP_DIR/repo"
REMOTE_REPO="$TMP_DIR/remote.git"
FAKE_BIN="$TMP_DIR/bin"
FAKE_STATE="$TMP_DIR/state"
FAKE_LOG="$TMP_DIR/commands.log"
FAKE_INSTALLER="$TMP_DIR/install.sh"
SLEEP_CALLED="$TMP_DIR/sleep-called"
FAILURES=0
RUN_OUTPUT=
RUN_STATUS=0
FAKE_PR_MODE=none
FAKE_CATALOG_STALE=0
FAKE_RELEASE_EXISTS=0
FAKE_MISSING_ASSET=0
FAKE_MAIN_ADVANCES=0

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fail() {
  printf 'not ok - %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}

assert_status() {
  local expected=$1
  local name=$2

  if ((RUN_STATUS != expected)); then
    fail "$name (expected status $expected, got $RUN_STATUS)"
    printf '%s\n' "$RUN_OUTPUT" >&2
  fi
}

assert_failure() {
  local name=$1

  if ((RUN_STATUS == 0)); then
    fail "$name (expected failure)"
    printf '%s\n' "$RUN_OUTPUT" >&2
  fi
}

assert_contains() {
  local haystack=$1
  local needle=$2
  local name=$3

  if [[ "$haystack" != *"$needle"* ]]; then
    fail "$name"
    printf 'missing: %s\noutput:\n%s\n' "$needle" "$haystack" >&2
  fi
}

assert_excludes() {
  local haystack=$1
  local needle=$2
  local name=$3

  if [[ "$haystack" == *"$needle"* ]]; then
    fail "$name"
    printf 'unexpected: %s\noutput:\n%s\n' "$needle" "$haystack" >&2
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
    printf 'unexpected: %s\nfile: %s\n' "$needle" "$file" >&2
  fi
}

assert_eq() {
  local expected=$1
  local actual=$2
  local name=$3

  if [[ "$actual" != "$expected" ]]; then
    fail "$name (expected '$expected', got '$actual')"
  fi
}

assert_not_eq() {
  local unexpected=$1
  local actual=$2
  local name=$3

  if [[ "$actual" == "$unexpected" ]]; then
    fail "$name (both were '$actual')"
  fi
}

write_fixture_helpers() {
  for helper in verify.sh verify-linux.sh release-v1-candidate.sh; do
    cat >"$ROOT/scripts/$helper" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$(basename "$0")" >>"$FAKE_LOG"
printf ' <%s>' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
EOF
  done

  cat >"$ROOT/scripts/cargo-lane.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'cargo-lane' >>"$FAKE_LOG"
printf ' <%s>' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
if [[ "${1:-}" == "update" ]]; then
  version=$(sed -n \
    '/^\[workspace\.package\]$/,/^\[/ {
      s/^version = "\([^"]*\)"/\1/p
    }' Cargo.toml | head -n 1)
  awk -v version="$version" '
    /^name = "fixture"$/ { fixture = 1; print; next }
    fixture && /^version = / { print "version = \"" version "\""; fixture = 0; next }
    { print }
  ' Cargo.lock >Cargo.lock.tmp
  mv Cargo.lock.tmp Cargo.lock
fi
EOF

  cat >"$ROOT/scripts/update-model-catalog.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
output=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --output) output=$2; shift 2 ;;
    *) shift ;;
  esac
done
printf 'update-model-catalog <%s>\n' "$output" >>"$FAKE_LOG"
if [[ "$FAKE_CATALOG_STALE" == "1" ]]; then
  printf '{"stale":true}\n' >"$output"
else
  cp crates/verlet-kernel/data/model-catalog.json "$output"
fi
EOF

  cat >"$ROOT/scripts/package-release-binary.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
out_dir=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir) out_dir=$2; shift 2 ;;
    *) shift ;;
  esac
done
mkdir -p "$out_dir"
touch "$out_dir/verlet-$VERLET_RELEASE_VERSION-aarch64-apple-darwin.tar.gz"
touch "$out_dir/verlet-$VERLET_RELEASE_VERSION-aarch64-apple-darwin.tar.gz.sha256"
printf 'package-release <%s>\n' "$out_dir" >>"$FAKE_LOG"
EOF

  for helper in smoke-release-archive.sh smoke-install.sh; do
    cat >"$ROOT/scripts/$helper" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s <%s>\n' "$(basename "$0")" "$*" >>"$FAKE_LOG"
EOF
  done

  cat >"$ROOT/scripts/write-release-manifest.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
out_dir=
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir) out_dir=$2; shift 2 ;;
    *) shift ;;
  esac
done
printf '{}\n' >"$out_dir/latest.json"
printf 'write-release-manifest <%s>\n' "$out_dir" >>"$FAKE_LOG"
EOF

  chmod +x "$ROOT/scripts/"*.sh
}

write_fake_commands() {
  cat >"$FAKE_BIN/docker" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'docker' >>"$FAKE_LOG"
printf ' <%s>' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
EOF

  cat >"$FAKE_BIN/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'cargo' >>"$FAKE_LOG"
printf ' <%s>' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
EOF

  cat >"$FAKE_BIN/sleep" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
touch "$SLEEP_CALLED"
printf 'sleep <%s>\n' "$*" >>"$FAKE_LOG"
exit 97
EOF

  cat >"$FAKE_BIN/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'curl' >>"$FAKE_LOG"
printf ' <%s>' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"
output=
while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) output=$2; shift 2 ;;
    *) shift ;;
  esac
done
[[ -n "$output" ]]
cp "$FAKE_INSTALLER" "$output"
EOF

  cat >"$FAKE_BIN/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'gh' >>"$FAKE_LOG"
printf ' <%s>' "$@" >>"$FAKE_LOG"
printf '\n' >>"$FAKE_LOG"

contains() {
  local wanted=$1
  shift
  local arg
  for arg in "$@"; do
    [[ "$arg" == "$wanted" ]] && return 0
  done
  return 1
}

has_reusable_pr_filter() {
  local arg

  for arg in "$@"; do
    if [[ "$arg" == *'select(.state == "OPEN" or .state == "MERGED")'* ]]; then
      return 0
    fi
  done
  return 1
}

version=$(sed -n \
  '/^\[workspace\.package\]$/,/^\[/ {
    s/^version = "\([^"]*\)"/\1/p
  }' Cargo.toml | head -n 1)
tag="v$version"

case "${1:-} ${2:-}" in
  'auth status')
    exit 0
    ;;
  'pr list')
    case "$FAKE_PR_MODE" in
      none) ;;
      open) printf 'OPEN|https://github.test/pr/17|\n' ;;
      closed)
        if ! has_reusable_pr_filter "$@"; then
          printf 'CLOSED|https://github.test/pr/16|\n'
        fi
        ;;
      error) exit 72 ;;
      merged)
        merge_sha=$(git rev-parse origin/main)
        printf 'MERGED|https://github.test/pr/17|%s\n' "$merge_sha"
        ;;
    esac
    ;;
  'pr create')
    touch "$FAKE_STATE/pr-created"
    printf 'https://github.test/pr/17\n'
    ;;
  'pr merge')
    branch="release/$tag"
    git push -q origin "${branch}:main"
    git rev-parse "$branch" >"$FAKE_STATE/merge-sha"
    if [[ "$FAKE_MAIN_ADVANCES" == "1" ]]; then
      remote_url=$(git remote get-url origin)
      git clone -q "$remote_url" "$FAKE_STATE/advance-repo"
      git -C "$FAKE_STATE/advance-repo" config user.name release-test
      git -C "$FAKE_STATE/advance-repo" config user.email release-test@example.invalid
      printf 'unrelated merge\n' >"$FAKE_STATE/advance-repo/unrelated"
      git -C "$FAKE_STATE/advance-repo" add unrelated
      git -C "$FAKE_STATE/advance-repo" commit --no-gpg-sign -qm 'unrelated merge'
      git -C "$FAKE_STATE/advance-repo" push -q origin main
    fi
    ;;
  'pr view')
    merge_sha=$(<"$FAKE_STATE/merge-sha")
    printf 'MERGED|%s|https://github.test/pr/17\n' "$merge_sha"
    ;;
  'release view')
    if contains url "$@"; then
      if [[ "$FAKE_RELEASE_EXISTS" != "1" && ! -f "$FAKE_STATE/release-exists" ]]; then
        exit 1
      fi
      printf 'https://github.test/releases/%s\n' "$tag"
      exit 0
    fi
    if contains assets "$@"; then
      if [[ "$FAKE_RELEASE_EXISTS" != "1" && ! -f "$FAKE_STATE/release-exists" ]]; then
        exit 1
      fi
      first=1
      for target in \
        x86_64-unknown-linux-gnu \
        aarch64-unknown-linux-gnu \
        x86_64-apple-darwin \
        aarch64-apple-darwin; do
        if [[ "$FAKE_MISSING_ASSET" != "1" || "$first" != "1" ]]; then
          printf 'verlet-%s-%s.tar.gz\n' "$version" "$target"
        fi
        printf 'verlet-%s-%s.tar.gz.sha256\n' "$version" "$target"
        first=0
      done
      printf 'install.sh\nlatest.json\n'
      exit 0
    fi
    ;;
  'release edit')
    ;;
  'run list')
    if contains emotionscientific/homebrew-tap "$@"; then
      if [[ -f "$FAKE_STATE/tap-dispatched" ]]; then
        printf '701|https://github.test/tap/runs/701\n'
      else
        printf '700|https://github.test/tap/runs/700\n'
      fi
    else
      printf '900|https://github.test/release/runs/900\n'
    fi
    ;;
  'run watch')
    if contains emotionscientific/verlet-kernel "$@"; then
      touch "$FAKE_STATE/release-exists"
    fi
    ;;
  'workflow run')
    touch "$FAKE_STATE/tap-dispatched"
    ;;
  'api -H')
    if [[ -f "$FAKE_STATE/tap-dispatched" ]]; then
      printf 'class Verlet\n  url "https://github.test/releases/download/%s/verlet.tar.gz"\n  version "%s"\nend\n' \
        "$tag" "$version"
    else
      printf 'class Verlet\n  url "https://github.test/releases/download/v0.5.1/verlet.tar.gz"\n  version "0.5.1"\nend\n'
    fi
    ;;
  *)
    printf 'fake gh: unsupported command: %s\n' "$*" >&2
    exit 64
    ;;
esac
EOF

  cat >"$FAKE_INSTALLER" <<'EOF'
#!/bin/sh
set -eu
version=
bin_dir=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --version) version=$2; shift 2 ;;
    --bin-dir) bin_dir=$2; shift 2 ;;
    --repo|--install-root|--man-dir) shift 2 ;;
    *) shift ;;
  esac
done
mkdir -p "$bin_dir"
printf '#!/bin/sh\nprintf "verlet %s\\n"\n' "$version" >"$bin_dir/verlet"
chmod +x "$bin_dir/verlet"
EOF

  chmod +x "$FAKE_BIN/docker" "$FAKE_BIN/cargo" "$FAKE_BIN/curl" \
    "$FAKE_BIN/gh" "$FAKE_BIN/sleep" "$FAKE_INSTALLER"
}

new_fixture() {
  rm -rf "$ROOT" "$REMOTE_REPO" "$FAKE_BIN" "$FAKE_STATE"
  mkdir -p "$ROOT/scripts" \
    "$ROOT/crates/verlet-kernel/data" "$FAKE_BIN" "$FAKE_STATE"
  : >"$FAKE_LOG"
  cp "$SOURCE_ROOT/scripts/release.sh" \
    "$SOURCE_ROOT/scripts/release-version.sh" \
    "$SOURCE_ROOT/scripts/check-release-tag.sh" \
    "$ROOT/scripts/"

  cat >"$ROOT/Cargo.toml" <<'EOF'
[workspace]
members = []

[workspace.package]
edition = "2024"
version = "0.5.1"
EOF
  cat >"$ROOT/Cargo.lock" <<'EOF'
version = 4

[[package]]
name = "fixture"
version = "0.5.1"
EOF
  cat >"$ROOT/CHANGELOG.md" <<'EOF'
# Changelog

## Unreleased

### Changes

- Release button fixture.

## v0.5.1 (2026-08-26)

- Previous release.
EOF
  cat >"$ROOT/README.md" <<'EOF'
# Fixture

The current release is v0.5.1. Fixture text.
EOF
  printf '{"catalog":"current"}\n' \
    >"$ROOT/crates/verlet-kernel/data/model-catalog.json"

  write_fixture_helpers
  write_fake_commands

  git -C "$ROOT" init -q -b main
  git -C "$ROOT" config user.name release-test
  git -C "$ROOT" config user.email release-test@example.invalid
  git -C "$ROOT" add .
  git -C "$ROOT" commit --no-gpg-sign -qm 'fixture release state'
  git -C "$ROOT" tag v0.5.1
  git init -q --bare -b main "$REMOTE_REPO"
  git -C "$ROOT" remote add origin "$REMOTE_REPO"
  git -C "$ROOT" push -q -u origin main --tags

  FAKE_PR_MODE=none
  FAKE_CATALOG_STALE=0
  FAKE_RELEASE_EXISTS=0
  FAKE_MISSING_ASSET=0
  FAKE_MAIN_ADVANCES=0
}

run_release() {
  if RUN_OUTPUT=$(
    cd "$ROOT" && \
      PATH="$FAKE_BIN:/usr/bin:/bin" \
      FAKE_LOG="$FAKE_LOG" \
      FAKE_STATE="$FAKE_STATE" \
      FAKE_INSTALLER="$FAKE_INSTALLER" \
      FAKE_PR_MODE="$FAKE_PR_MODE" \
      FAKE_CATALOG_STALE="$FAKE_CATALOG_STALE" \
      FAKE_RELEASE_EXISTS="$FAKE_RELEASE_EXISTS" \
      FAKE_MISSING_ASSET="$FAKE_MISSING_ASSET" \
      FAKE_MAIN_ADVANCES="$FAKE_MAIN_ADVANCES" \
      SLEEP_CALLED="$SLEEP_CALLED" \
      "$ROOT/scripts/release.sh" "$@" </dev/null 2>&1
  ); then
    RUN_STATUS=0
  else
    RUN_STATUS=$?
  fi
}

make_release_branch() {
  local date_today

  date_today=$(date -u +%Y-%m-%d)
  git -C "$ROOT" checkout -qb release/v0.6.0 main
  awk '
    /^version = "0.5.1"$/ { print "version = \"0.6.0\""; next }
    { print }
  ' "$ROOT/Cargo.toml" >"$ROOT/Cargo.toml.tmp"
  mv "$ROOT/Cargo.toml.tmp" "$ROOT/Cargo.toml"
  awk '
    /^name = "fixture"$/ { fixture = 1; print; next }
    fixture && /^version = / { print "version = \"0.6.0\""; fixture = 0; next }
    { print }
  ' "$ROOT/Cargo.lock" >"$ROOT/Cargo.lock.tmp"
  mv "$ROOT/Cargo.lock.tmp" "$ROOT/Cargo.lock"
  awk '
    { sub("The current release is v0.5.1", "The current release is v0.6.0"); print }
  ' "$ROOT/README.md" >"$ROOT/README.md.tmp"
  mv "$ROOT/README.md.tmp" "$ROOT/README.md"
  awk -v date_today="$date_today" '
    /^## Unreleased$/ && !changed {
      print "## Unreleased"
      print ""
      print "## v0.6.0 (" date_today ")"
      changed = 1
      next
    }
    { print }
  ' "$ROOT/CHANGELOG.md" >"$ROOT/CHANGELOG.md.tmp"
  mv "$ROOT/CHANGELOG.md.tmp" "$ROOT/CHANGELOG.md"
  git -C "$ROOT" add Cargo.toml Cargo.lock CHANGELOG.md README.md
  git -C "$ROOT" commit --no-gpg-sign -qm 'release: v0.6.0'
  git -C "$ROOT" push -q -u origin release/v0.6.0
}

land_fixture_main() {
  git -C "$ROOT" push -q origin release/v0.6.0:main
  git -C "$ROOT" checkout -q main
  git -C "$ROOT" merge -q --ff-only origin/main
}

new_fixture
before_head=$(git -C "$ROOT" rev-parse HEAD)
before_status=$(git -C "$ROOT" status --porcelain)
run_release v0.6.0 --dry-run
assert_status 0 'dry run succeeds'
expected_steps=$'==> [1/9] preflight\n==> [2/9] bump\n==> [3/9] review\n==> [4/9] gate\n==> [5/9] land\n==> [6/9] tag\n==> [7/9] publish\n==> [8/9] install-check\n==> [9/9] tap'
actual_steps=$(printf '%s\n' "$RUN_OUTPUT" | grep '^==> \[[1-9]/9\]')
if [[ "$actual_steps" != "$expected_steps" ]]; then
  fail 'dry run prints all steps in order'
  printf 'steps:\n%s\n' "$actual_steps" >&2
fi
assert_contains "$RUN_OUTPUT" 'gh pr view' \
  'dry run includes the PR polling command'
assert_contains "$RUN_OUTPUT" '--limit 100' \
  'dry run uses the real PR lookup limit'
assert_contains "$RUN_OUTPUT" '--json assets --jq' \
  'dry run uses the real release asset query'
if [[ "$(git -C "$ROOT" rev-parse HEAD)" != "$before_head" \
  || "$(git -C "$ROOT" status --porcelain)" != "$before_status" \
  || -e "$ROOT/dist" ]]; then
  fail 'dry run writes nothing'
fi

new_fixture
git -C "$ROOT" checkout -qb feature
run_release v0.6.0 --skip-linux --skip-catalog-check
assert_failure 'non-main preflight refuses'
assert_contains "$RUN_OUTPUT" 'check out main and rerun from preflight' \
  'non-main refusal gives next action'

new_fixture
printf 'dirty\n' >>"$ROOT/README.md"
run_release v0.6.0 --skip-linux --skip-catalog-check
assert_failure 'dirty preflight refuses'
assert_contains "$RUN_OUTPUT" 'commit or stash changes' \
  'dirty refusal gives next action'

new_fixture
updater="$TMP_DIR/updater"
git clone -q "$REMOTE_REPO" "$updater"
git -C "$updater" config user.name release-test
git -C "$updater" config user.email release-test@example.invalid
printf 'remote update\n' >>"$updater/README.md"
git -C "$updater" add README.md
git -C "$updater" commit --no-gpg-sign -qm 'remote update'
git -C "$updater" push -q origin main
run_release v0.6.0 --skip-linux --skip-catalog-check
assert_failure 'behind-remote preflight refuses'
assert_contains "$RUN_OUTPUT" 'git pull --ff-only origin main' \
  'behind-remote refusal gives next action'
rm -rf "$updater"

new_fixture
git -C "$ROOT" tag v0.6.0
git -C "$ROOT" push -q origin v0.6.0
git -C "$ROOT" tag -d v0.6.0 >/dev/null
run_release v0.6.0 --skip-linux --skip-catalog-check
assert_failure 'existing remote tag preflight refuses'
assert_contains "$RUN_OUTPUT" 'choose a new tag and rerun from preflight' \
  'existing tag refusal gives next action'

new_fixture
run_release v0.5.0 --skip-linux --skip-catalog-check
assert_failure 'non-increasing tag preflight refuses'
assert_contains "$RUN_OUTPUT" 'does not sort above latest tag v0.5.1' \
  'non-increasing tag refusal names latest tag'

new_fixture
cat >"$ROOT/CHANGELOG.md" <<'EOF'
# Changelog

## Unreleased

## v0.5.1 (2026-08-26)

- Previous release.
EOF
git -C "$ROOT" add CHANGELOG.md
git -C "$ROOT" commit --no-gpg-sign -qm 'empty unreleased fixture'
git -C "$ROOT" push -q origin main
run_release v0.6.0 --skip-linux --skip-catalog-check
assert_failure 'empty Unreleased preflight refuses'
assert_contains "$RUN_OUTPUT" 'add release notes or pass --allow-empty-changelog' \
  'empty Unreleased refusal gives next action'

new_fixture
FAKE_CATALOG_STALE=1
run_release v0.6.0 --skip-linux
assert_failure 'stale catalog preflight refuses'
assert_contains "$RUN_OUTPUT" 'review and commit the catalog separately' \
  'stale catalog refusal gives next action'

new_fixture
run_release v0.6.0 --from bump --yes --quick --skip-linux
assert_status 0 'full shimmed release succeeds'
assert_file_contains "$ROOT/Cargo.toml" 'version = "0.6.0"' \
  'bump rewrites workspace version'
assert_file_contains "$ROOT/Cargo.lock" 'version = "0.6.0"' \
  'bump refreshes lockfile'
assert_file_contains "$ROOT/README.md" 'The current release is v0.6.0.' \
  'bump rewrites current release sentence'
date_today=$(date -u +%Y-%m-%d)
expected_rollover=$(cat <<EOF
# Changelog

## Unreleased

## v0.6.0 ($date_today)

### Changes

- Release button fixture.
EOF
)
actual_rollover=$(sed -n '1,9p' "$ROOT/CHANGELOG.md")
if [[ "$actual_rollover" != "$expected_rollover" ]]; then
  fail 'changelog rollover text is exact'
  printf 'expected:\n%s\nactual:\n%s\n' "$expected_rollover" "$actual_rollover" >&2
fi
receipt="$ROOT/dist/release/v0.6.0/receipt.json"
if [[ ! -f "$receipt" ]]; then
  fail 'full release writes receipt'
else
  receipt_rows=$(grep -o '"step"' "$receipt" | wc -l | tr -d ' ')
  if [[ "$receipt_rows" != "9" ]]; then
    fail "receipt has one row per step (got $receipt_rows)"
  fi
fi

new_fixture
make_release_branch
run_release v0.6.0 --from bump --yes --quick --skip-linux
assert_status 0 'bump resume reuses a completed release branch'
assert_file_excludes "$FAKE_LOG" 'cargo-lane <update> <--workspace>' \
  'bump resume does not rewrite an existing bump commit'

new_fixture
make_release_branch
run_release v0.6.0 --from review --quick --skip-linux
assert_failure 'non-terminal review refuses without --yes'
assert_contains "$RUN_OUTPUT" 'review confirmation requires a terminal' \
  'non-terminal review explains the refusal'
assert_contains "$RUN_OUTPUT" 'pass --yes' \
  'non-terminal review names --yes'

new_fixture
make_release_branch
FAKE_PR_MODE=closed
run_release v0.6.0 --from land --yes --quick --skip-linux
assert_status 0 'land ignores a closed unmerged PR'
assert_file_contains "$FAKE_LOG" 'select(.state == "OPEN" or .state == "MERGED")' \
  'PR lookup filters to reusable states'
assert_file_contains "$FAKE_LOG" 'gh <pr> <create>' \
  'land creates a new PR after a closed unmerged PR'

new_fixture
make_release_branch
FAKE_PR_MODE=error
run_release v0.6.0 --from land --yes --quick --skip-linux
assert_failure 'land propagates a PR lookup failure'
assert_file_excludes "$FAKE_LOG" 'gh <pr> <create>' \
  'land does not create a PR after a lookup failure'

new_fixture
make_release_branch
FAKE_PR_MODE=open
run_release v0.6.0 --from land --yes --quick --skip-linux
assert_status 0 'land resume with existing PR succeeds'
assert_file_excludes "$FAKE_LOG" 'gh <pr> <create>' \
  'land resume does not create a duplicate PR'
assert_file_contains "$FAKE_LOG" 'gh <pr> <merge>' \
  'land resume reuses the existing PR'

new_fixture
make_release_branch
FAKE_MAIN_ADVANCES=1
run_release v0.6.0 --from land --yes --quick --skip-linux
assert_status 0 'land tolerates main advancing after the release merge'
merge_sha=$(<"$FAKE_STATE/merge-sha")
tag_sha=$(git -C "$ROOT" rev-parse 'v0.6.0^{commit}')
main_sha=$(git -C "$ROOT" rev-parse origin/main)
assert_eq "$merge_sha" "$tag_sha" 'tag points to the release merge commit'
assert_not_eq "$merge_sha" "$main_sha" \
  'main advance fixture leaves a newer main commit'

new_fixture
old_commit=$(git -C "$ROOT" rev-parse main)
make_release_branch
land_fixture_main
git -C "$ROOT" tag -a v0.6.0 "$old_commit" -m v0.6.0
git -C "$ROOT" push -q origin v0.6.0
FAKE_PR_MODE=merged
run_release v0.6.0 --from tag --yes --quick --skip-linux
assert_failure 'tag at another commit aborts'
assert_contains "$RUN_OUTPUT" 'expected' \
  'tag mismatch reports the expected commit'

new_fixture
make_release_branch
land_fixture_main
git -C "$ROOT" tag -a v0.6.0 -m v0.6.0
git -C "$ROOT" push -q origin v0.6.0
FAKE_RELEASE_EXISTS=1
FAKE_MISSING_ASSET=1
run_release v0.6.0 --from publish --yes --quick --skip-linux
assert_failure 'missing release archive aborts publish'
assert_contains "$RUN_OUTPUT" 'release asset set is incomplete or unexpected' \
  'asset assertion explains the failure'

if [[ -e "$SLEEP_CALLED" ]]; then
  fail 'release tests never call real polling sleep paths'
fi

if ((FAILURES > 0)); then
  printf 'release-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'release-test: ok\n'
