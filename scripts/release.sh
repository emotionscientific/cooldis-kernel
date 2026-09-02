#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
REMOTE=origin
GITHUB_REPO=emotionscientific/verlet-kernel
TAP_REPO=emotionscientific/homebrew-tap
DRY_RUN=0
YES=0
QUICK=0
SKIP_LINUX=0
SKIP_CATALOG_CHECK=0
ALLOW_EMPTY_CHANGELOG=0
FROM_STEP=preflight
TAG=
VERSION=
RELEASE_BRANCH=
PREVIOUS_TAG=
WORK_DIR=
NOTES_FILE=
PR_URL=
RELEASE_COMMIT=
RELEASE_RUN_ID=
WORKFLOW_URL=
RELEASE_URL=
TAP_RUN_URL=

STEPS=(preflight bump review gate land tag publish install-check tap)
RECEIPT_STATUS=()
RECEIPT_STARTED=()
RECEIPT_FINISHED=()
RECEIPT_URL=()

usage() {
  cat <<'USAGE'
release.sh - bump, gate, land, tag, and publish a Verlet release.

Usage:
  scripts/release.sh <vX.Y.Z[-pre]> [options]

Options:
  --yes                     Skip the publish confirmation.
  --dry-run                 Print commands without writing, pushing, or dispatching.
  --quick                   Run the host package smoke instead of the candidate gate.
  --skip-linux              Skip both Docker Linux verification lanes.
  --skip-catalog-check      Skip the model catalog freshness check.
  --allow-empty-changelog   Allow an empty Unreleased changelog section.
  --from STEP               Resume at a named step.
  --remote NAME             Git remote. Default: origin.
  -h, --help                Show this help.

Steps:
  preflight, bump, review, gate, land, tag, publish, install-check, tap
USAGE
}

die() {
  printf 'release: %s\n' "$*" >&2
  exit 1
}

print_command() {
  local arg

  printf '    '
  for arg in "$@"; do
    printf '%q ' "$arg"
  done
  printf '\n'
}

run() {
  print_command "$@"
  if [[ "$DRY_RUN" == "1" ]]; then
    return 0
  fi
  "$@"
}

check() {
  print_command "$@"
  "$@"
}

check_quiet() {
  print_command "$@"
  "$@" >/dev/null
}

plan() {
  printf '    %s\n' "$*"
}

cleanup() {
  if [[ -n "$WORK_DIR" && -d "$WORK_DIR" ]]; then
    rm -rf "$WORK_DIR"
  fi
}
trap cleanup EXIT

timestamp() {
  date -u +'%Y-%m-%dT%H:%M:%SZ'
}

step_index() {
  local wanted=$1
  local index

  for index in "${!STEPS[@]}"; do
    if [[ "${STEPS[$index]}" == "$wanted" ]]; then
      printf '%s\n' "$index"
      return 0
    fi
  done
  return 1
}

validate_tag_shape() {
  local prerelease
  local identifier
  local identifiers

  if [[ ! "$TAG" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+(\.[0-9A-Za-z-]+)*))?$ ]]; then
    die "release tag must match vX.Y.Z[-pre], for example v0.6.0-rc.1"
  fi
  prerelease=${BASH_REMATCH[5]:-}
  if [[ -z "$prerelease" ]]; then
    return
  fi
  IFS=. read -r -a identifiers <<<"$prerelease"
  for identifier in "${identifiers[@]}"; do
    if [[ "$identifier" =~ ^[0-9]+$ \
      && "$identifier" != "0" \
      && "$identifier" == 0* ]]; then
      die "numeric prerelease identifiers must not contain leading zeroes: $identifier"
    fi
  done
}

version_gt() {
  local left=${1#v}
  local right=${2#v}
  local left_core=${left%%-*}
  local right_core=${right%%-*}
  local left_pre=
  local right_pre=
  local left_parts
  local right_parts
  local left_ids
  local right_ids
  local index
  local left_id
  local right_id

  if [[ "$left" == *-* ]]; then left_pre=${left#*-}; fi
  if [[ "$right" == *-* ]]; then right_pre=${right#*-}; fi
  IFS=. read -r -a left_parts <<<"$left_core"
  IFS=. read -r -a right_parts <<<"$right_core"
  for index in 0 1 2; do
    if ((10#${left_parts[$index]} > 10#${right_parts[$index]})); then
      return 0
    fi
    if ((10#${left_parts[$index]} < 10#${right_parts[$index]})); then
      return 1
    fi
  done
  if [[ -z "$left_pre" && -n "$right_pre" ]]; then return 0; fi
  if [[ -n "$left_pre" && -z "$right_pre" ]]; then return 1; fi
  if [[ -z "$left_pre" ]]; then return 1; fi

  IFS=. read -r -a left_ids <<<"$left_pre"
  IFS=. read -r -a right_ids <<<"$right_pre"
  for ((index = 0; index < ${#left_ids[@]} || index < ${#right_ids[@]}; index++)); do
    if ((index >= ${#left_ids[@]})); then return 1; fi
    if ((index >= ${#right_ids[@]})); then return 0; fi
    left_id=${left_ids[$index]}
    right_id=${right_ids[$index]}
    if [[ "$left_id" == "$right_id" ]]; then continue; fi
    if [[ "$left_id" =~ ^[0-9]+$ && "$right_id" =~ ^[0-9]+$ ]]; then
      ((10#$left_id > 10#$right_id)) && return 0
      return 1
    fi
    if [[ "$left_id" =~ ^[0-9]+$ ]]; then return 1; fi
    if [[ "$right_id" =~ ^[0-9]+$ ]]; then return 0; fi
    [[ "$left_id" > "$right_id" ]]
    return
  done
  return 1
}

require_clean_worktree() {
  if [[ -n "$(git status --porcelain)" ]]; then
    die "working tree is not clean; commit or stash changes, then rerun from preflight"
  fi
}

current_branch() {
  git symbolic-ref --quiet --short HEAD || true
}

latest_tag() {
  local candidate

  while IFS= read -r candidate; do
    if [[ -n "$candidate" && "$candidate" != "$TAG" ]]; then
      printf '%s\n' "$candidate"
      return
    fi
  done < <(git tag --list 'v*' --sort=-version:refname)
}

unreleased_has_content() {
  awk '
    /^## Unreleased[[:space:]]*$/ { active = 1; next }
    active && /^##[[:space:]]/ { exit }
    active && /[^[:space:]]/ { found = 1 }
    END { exit found ? 0 : 1 }
  ' CHANGELOG.md
}

remote_tag_lines() {
  git ls-remote --tags "$REMOTE" \
    "refs/tags/$TAG" "refs/tags/$TAG^{}" 2>/dev/null
}

remote_tag_commit() {
  local lines=$1

  awk '
    /\^\{\}$/ { peeled = $1 }
    !first { first = $1 }
    END { if (peeled) print peeled; else print first }
  ' <<<"$lines"
}

read_ref_version() {
  local ref=$1

  git show "$ref:Cargo.toml" \
    | sed -n \
      '/^\[workspace\.package\][[:space:]]*$/,/^\[/ {
        s/^[[:space:]]*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p
      }' \
    | head -n 1
}

find_previous_tag() {
  PREVIOUS_TAG=$(latest_tag)
}

make_notes_file() {
  if [[ "$DRY_RUN" == "1" ]]; then
    return
  fi
  if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/verlet-release.XXXXXX")
  fi
  NOTES_FILE="$WORK_DIR/changelog.md"
  changelog_section >"$NOTES_FILE"
  if [[ ! -s "$NOTES_FILE" ]]; then
    die "CHANGELOG.md has no $TAG release section; repair it and rerun from bump"
  fi
}

changelog_section() {
  awk -v tag="$TAG" '
    index($0, "## " tag " (") == 1 { active = 1 }
    active && seen && /^##[[:space:]]/ { exit }
    active { print; seen = 1 }
  ' CHANGELOG.md
}

print_dry_run_changelog() {
  printf '## %s (%s)\n\n' "$TAG" "$(date -u +%Y-%m-%d)"
  awk '
    /^## Unreleased[[:space:]]*$/ { active = 1; next }
    active && /^##[[:space:]]/ { exit }
    active { print }
  ' CHANGELOG.md
}

assert_release_commit() {
  local branch
  local version
  local changed
  local expected

  require_clean_worktree
  branch=$(current_branch)
  if [[ "$branch" != "$RELEASE_BRANCH" ]]; then
    die "resume requires branch $RELEASE_BRANCH; check it out and rerun from $FROM_STEP"
  fi
  version=$(read_ref_version HEAD)
  if [[ "$version" != "$VERSION" ]]; then
    die "HEAD has workspace version $version, expected $VERSION; repair the bump and rerun from bump"
  fi
  if [[ -z "$(changelog_section)" ]]; then
    die "CHANGELOG.md has no $TAG section; repair the rollover and rerun from bump"
  fi
  if [[ "$(git log -1 --format=%s)" != "release: $TAG" ]]; then
    die "HEAD is not the release: $TAG commit; repair the release branch and rerun from bump"
  fi
  changed=$(git diff-tree --root --no-commit-id --name-only -r HEAD | sort)
  expected=$(printf '%s\n' Cargo.lock Cargo.toml CHANGELOG.md README.md | sort)
  if [[ "$changed" != "$expected" ]]; then
    printf 'release commit paths:\n%s\n' "$changed" >&2
    die "the bump commit must contain only Cargo.toml, Cargo.lock, CHANGELOG.md, and README.md; repair it and rerun from bump"
  fi
}

assert_remote_main_version() {
  local version

  version=$(read_ref_version "$REMOTE/main")
  if [[ "$version" != "$VERSION" ]]; then
    die "$REMOTE/main has workspace version $version, expected $VERSION; finish land and rerun from land"
  fi
}

assert_tag_on_main() {
  local lines
  local tag_commit

  lines=$(remote_tag_lines)
  if [[ -z "$lines" ]]; then
    die "$TAG is missing on $REMOTE; finish tag and rerun from tag"
  fi
  tag_commit=$(remote_tag_commit "$lines")
  if ! git merge-base --is-ancestor "$tag_commit" "$REMOTE/main"; then
    die "$TAG points to $tag_commit outside $REMOTE/main; repair the tag before resuming"
  fi
}

check_catalog() {
  local snapshot="$ROOT/crates/verlet-kernel/data/model-catalog.json"
  local catalog_copy
  local catalog_diff
  local diff_status

  if [[ "$SKIP_CATALOG_CHECK" == "1" ]]; then
    plan 'skip model catalog check (--skip-catalog-check)'
    return
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    plan 'scripts/update-model-catalog.sh --output <temporary-model-catalog>'
    plan 'diff -u crates/verlet-kernel/data/model-catalog.json <temporary-model-catalog>'
    return
  fi
  if [[ -z "$WORK_DIR" ]]; then
    WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/verlet-release.XXXXXX")
  fi
  catalog_copy="$WORK_DIR/model-catalog.json"
  run "$ROOT/scripts/update-model-catalog.sh" --output "$catalog_copy"
  if catalog_diff=$(diff -u "$snapshot" "$catalog_copy"); then
    return
  else
    diff_status=$?
  fi
  if [[ "$diff_status" == "1" ]]; then
    printf '%s\n' "$catalog_diff" >&2
    die "model catalog is stale; base URLs decide where provider credentials go, so review and commit the catalog separately before rerunning from preflight"
  fi
  die "could not compare the refreshed model catalog; fix the diff error and rerun from preflight"
}

preflight() {
  local branch
  local remote_lines

  branch=$(current_branch)
  if [[ "$branch" != "main" ]]; then
    die "current branch is $branch, not main; check out main and rerun from preflight"
  fi
  require_clean_worktree
  run git fetch "$REMOTE" --tags
  if [[ "$(git rev-parse HEAD)" != "$(git rev-parse "$REMOTE/main")" ]]; then
    die "HEAD does not match $REMOTE/main; update main with git pull --ff-only $REMOTE main, then rerun from preflight"
  fi
  check gh auth status
  if [[ "$SKIP_LINUX" != "1" ]]; then
    check_quiet docker info \
      || die "Docker is not reachable; start Docker or pass --skip-linux, then rerun from preflight"
  fi
  if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    die "$TAG already exists locally; choose a new tag and rerun from preflight"
  fi
  print_command git ls-remote --tags "$REMOTE" "refs/tags/$TAG" "refs/tags/$TAG^{}"
  remote_lines=$(remote_tag_lines)
  if [[ -n "$remote_lines" ]]; then
    die "$TAG already exists on $REMOTE; choose a new tag and rerun from preflight"
  fi
  find_previous_tag
  if [[ -n "$PREVIOUS_TAG" ]] && ! version_gt "$TAG" "$PREVIOUS_TAG"; then
    die "$TAG does not sort above latest tag $PREVIOUS_TAG; choose a greater tag and rerun from preflight"
  fi
  if [[ "$ALLOW_EMPTY_CHANGELOG" != "1" ]] && ! unreleased_has_content; then
    die "CHANGELOG.md Unreleased is empty; add release notes or pass --allow-empty-changelog, then rerun from preflight"
  fi
  check_catalog
}

replace_workspace_version() {
  local old=$1
  local new=$2
  local output=$3

  awk -v old="$old" -v new="$new" '
    /^\[workspace\.package\][[:space:]]*$/ { workspace = 1; print; next }
    workspace && /^\[/ { workspace = 0 }
    workspace && $0 ~ "^[[:space:]]*version[[:space:]]*=" {
      if ($0 != "version = \"" old "\"") exit 42
      print "version = \"" new "\""
      changed = 1
      next
    }
    { print }
    END { if (!changed) exit 43 }
  ' Cargo.toml >"$output"
}

replace_current_release() {
  local old=$1
  local new=$2
  local output=$3

  awk -v old="The current release is v$old." \
    -v new="The current release is v$new." '
    {
      position = index($0, old)
      if (position) {
        $0 = substr($0, 1, position - 1) new substr($0, position + length(old))
        changed++
      }
      print
    }
    END { if (changed != 1) exit 44 }
  ' README.md >"$output"
}

roll_changelog() {
  local output=$1
  local release_date

  release_date=$(date -u +%Y-%m-%d)
  awk -v tag="$TAG" -v release_date="$release_date" '
    /^## Unreleased[[:space:]]*$/ && !changed {
      print "## Unreleased"
      print ""
      print "## " tag " (" release_date ")"
      changed = 1
      next
    }
    { print }
    END { if (!changed) exit 45 }
  ' CHANGELOG.md >"$output"
}

assert_bump_paths() {
  local changed
  local expected

  changed=$(git diff --cached --name-only | sort)
  expected=$(printf '%s\n' Cargo.lock Cargo.toml CHANGELOG.md README.md | sort)
  if [[ "$changed" != "$expected" ]]; then
    printf 'staged bump paths:\n%s\n' "$changed" >&2
    die "bump must stage exactly Cargo.toml, Cargo.lock, CHANGELOG.md, and README.md"
  fi
  if [[ -n "$(git status --porcelain | awk '{ print substr($0, 4) }' | sort | comm -23 - <(printf '%s\n' Cargo.lock Cargo.toml CHANGELOG.md README.md | sort))" ]]; then
    die "bump produced changes outside the four release files; restore them and rerun from bump"
  fi
}

bump() {
  local old_version
  local temporary

  if [[ "$DRY_RUN" == "1" ]]; then
    old_version=$(read_ref_version HEAD)
    run git checkout -b "$RELEASE_BRANCH"
    plan "rewrite [workspace.package] version in Cargo.toml: $old_version -> $VERSION"
    run "$ROOT/scripts/cargo-lane.sh" update --workspace
    plan "rewrite README.md current release: v$old_version -> $TAG"
    plan "roll CHANGELOG.md Unreleased into $TAG ($(date -u +%Y-%m-%d))"
    run "$ROOT/scripts/check-release-tag.sh" "$TAG"
    run git add Cargo.toml Cargo.lock CHANGELOG.md README.md
    run git commit -m "release: $TAG"
    return
  fi

  if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
    run git checkout "$RELEASE_BRANCH"
    assert_release_commit
    return
  fi
  if git show-ref --verify --quiet "refs/remotes/$REMOTE/$RELEASE_BRANCH"; then
    run git checkout -b "$RELEASE_BRANCH" "$REMOTE/$RELEASE_BRANCH"
    assert_release_commit
    return
  fi
  if [[ "$(current_branch)" != "main" ]]; then
    die "bump must start on main; check out main and rerun from bump"
  fi
  require_clean_worktree
  old_version=$(read_ref_version HEAD)
  run git checkout -b "$RELEASE_BRANCH"

  temporary=$(mktemp "${TMPDIR:-/tmp}/verlet-cargo.XXXXXX")
  replace_workspace_version "$old_version" "$VERSION" "$temporary"
  mv "$temporary" Cargo.toml
  run "$ROOT/scripts/cargo-lane.sh" update --workspace

  temporary=$(mktemp "${TMPDIR:-/tmp}/verlet-readme.XXXXXX")
  replace_current_release "$old_version" "$VERSION" "$temporary"
  mv "$temporary" README.md

  temporary=$(mktemp "${TMPDIR:-/tmp}/verlet-changelog.XXXXXX")
  roll_changelog "$temporary"
  mv "$temporary" CHANGELOG.md

  run "$ROOT/scripts/check-release-tag.sh" "$TAG"
  run git add Cargo.toml Cargo.lock CHANGELOG.md README.md
  assert_bump_paths
  run git commit -m "release: $TAG"
  assert_release_commit
}

review() {
  if [[ "$YES" != "1" && "$DRY_RUN" != "1" && ! -t 0 ]]; then
    die "review confirmation requires a terminal; pass --yes after reviewing the changelog and commit list"
  fi
  find_previous_tag
  printf '\nChangelog section:\n\n'
  if [[ "$DRY_RUN" == "1" ]]; then
    print_dry_run_changelog
  else
    make_notes_file
    cat "$NOTES_FILE"
  fi
  printf '\nCommits since %s:\n\n' "${PREVIOUS_TAG:-repository start}"
  if [[ -n "$PREVIOUS_TAG" ]]; then
    check git log --oneline "$PREVIOUS_TAG..HEAD"
  else
    check git log --oneline HEAD
  fi
  if [[ "$YES" == "1" ]]; then
    printf '\nPublish %s? yes (--yes)\n' "$TAG"
    return
  fi
  if [[ "$DRY_RUN" == "1" ]]; then
    printf '\nPublish %s? [y/N] (dry run does not prompt)\n' "$TAG"
    return
  fi
  printf '\nPublish %s? [y/N] ' "$TAG"
  read -r answer
  case "$answer" in
    y|Y|yes|YES) ;;
    *) die "release cancelled; review the notes and rerun from review" ;;
  esac
}

gate() {
  local out_dir="$ROOT/dist/release/$TAG"
  local archive

  run "$ROOT/scripts/verify.sh"
  if [[ "$SKIP_LINUX" != "1" ]]; then
    run "$ROOT/scripts/verify-linux.sh"
    run "$ROOT/scripts/verify-linux.sh" --amd64
  else
    plan 'skip Linux verification lanes (--skip-linux)'
  fi
  if [[ "$QUICK" == "1" ]]; then
    run env VERLET_RELEASE_VERSION="$VERSION" \
      "$ROOT/scripts/package-release-binary.sh" --out-dir "$out_dir"
    if [[ "$DRY_RUN" == "1" ]]; then
      archive="$out_dir/verlet-$VERSION-<host-target>.tar.gz"
    else
      archive=$(find "$out_dir" -maxdepth 1 -name 'verlet-*.tar.gz' | head -n 1)
      if [[ -z "$archive" ]]; then
        die "quick gate did not create an archive under $out_dir; repair it and rerun from gate"
      fi
    fi
    run "$ROOT/scripts/smoke-release-archive.sh" "$archive"
    run "$ROOT/scripts/smoke-install.sh" "$archive"
    run "$ROOT/scripts/write-release-manifest.sh" --out-dir "$out_dir" --tag "$TAG"
  else
    run env VERLET_RELEASE_VERSION="$VERSION" \
      "$ROOT/scripts/release-v1-candidate.sh"
  fi
  run env RUSTDOCFLAGS='-D warnings' \
    "$ROOT/scripts/cargo-lane.sh" doc --workspace --no-deps
}

pr_record() {
  gh pr list --repo "$GITHUB_REPO" --head "$RELEASE_BRANCH" --state all \
    --limit 100 --json state,url,mergeCommit \
    --jq '[.[] | select(.state == "OPEN" or .state == "MERGED")] | .[0] | select(. != null) | [.state, .url, (.mergeCommit.oid // "")] | join("|")'
}

ensure_release_branch_for_land() {
  if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
    return
  fi
  if git show-ref --verify --quiet "refs/remotes/$REMOTE/$RELEASE_BRANCH"; then
    run git branch "$RELEASE_BRANCH" "$REMOTE/$RELEASE_BRANCH"
    return
  fi
  die "release branch $RELEASE_BRANCH is missing; rebuild it and rerun from bump"
}

land() {
  local record
  local state=
  local merge_commit=
  local release_ref=
  local started
  local elapsed

  if [[ "$DRY_RUN" == "1" ]]; then
    run git fetch "$REMOTE"
    run gh pr list --repo "$GITHUB_REPO" --head "$RELEASE_BRANCH" --state all \
      --limit 100 --json state,url,mergeCommit \
      --jq '[.[] | select(.state == "OPEN" or .state == "MERGED")] | .[0] | select(. != null) | [.state, .url, (.mergeCommit.oid // "")] | join("|")'
    run git push -u "$REMOTE" "$RELEASE_BRANCH"
    run gh pr create --repo "$GITHUB_REPO" --base main --head "$RELEASE_BRANCH" \
      --title "release: $TAG" --body '<changelog section>'
    run gh pr merge '<release-pr-url>' --repo "$GITHUB_REPO" --squash --auto
    run gh pr view '<release-pr-url>' --repo "$GITHUB_REPO" \
      --json state,mergeCommit,url \
      --jq '[.state, (.mergeCommit.oid // ""), .url] | join("|")'
    plan 'poll gh pr view every 30 seconds for up to 60 minutes'
    run git fetch "$REMOTE"
    run git merge-base --is-ancestor '<release-merge-commit>' "$REMOTE/main"
    run git diff --quiet "$RELEASE_BRANCH" '<release-merge-commit>'
    run git checkout main
    run git merge --ff-only "$REMOTE/main"
    run git branch --delete --force "$RELEASE_BRANCH"
    PR_URL="https://github.com/$GITHUB_REPO/pull/<number>"
    return
  fi

  make_notes_file
  run git fetch "$REMOTE"
  record=$(pr_record)
  if [[ -n "$record" ]]; then
    IFS='|' read -r state PR_URL merge_commit <<<"$record"
  fi

  if [[ "$state" != "MERGED" ]]; then
    ensure_release_branch_for_land
    run git push -u "$REMOTE" "$RELEASE_BRANCH"
    if [[ -z "$PR_URL" ]]; then
      PR_URL=$(gh pr create --repo "$GITHUB_REPO" --base main \
        --head "$RELEASE_BRANCH" --title "release: $TAG" \
        --body "$(<"$NOTES_FILE")")
      state=OPEN
    fi
    run gh pr merge "$PR_URL" --repo "$GITHUB_REPO" --squash --auto
    started=$(date +%s)
    while true; do
      record=$(gh pr view "$PR_URL" --repo "$GITHUB_REPO" \
        --json state,mergeCommit,url \
        --jq '[.state, (.mergeCommit.oid // ""), .url] | join("|")')
      IFS='|' read -r state merge_commit PR_URL <<<"$record"
      elapsed=$(($(date +%s) - started))
      printf '    PR state: %s, elapsed: %ss\n' "$state" "$elapsed"
      if [[ "$state" == "MERGED" ]]; then break; fi
      if ((elapsed >= 3600)); then
        die "PR did not merge within 60 minutes: $PR_URL; fix it and rerun from land"
      fi
      sleep 30
    done
  fi

  run git fetch "$REMOTE"
  if [[ -z "$merge_commit" ]]; then
    die "merged PR has no squash merge commit; inspect $PR_URL and rerun from land"
  fi
  RELEASE_COMMIT=$merge_commit
  if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
    release_ref=$RELEASE_BRANCH
  elif git show-ref --verify --quiet "refs/remotes/$REMOTE/$RELEASE_BRANCH"; then
    release_ref="$REMOTE/$RELEASE_BRANCH"
  elif [[ -n "$merge_commit" ]]; then
    release_ref=$merge_commit
  else
    die "could not find the landed release tree; inspect $PR_URL and rerun from land"
  fi
  if ! git merge-base --is-ancestor "$RELEASE_COMMIT" "$REMOTE/main"; then
    die "release merge commit $RELEASE_COMMIT is not on $REMOTE/main; inspect $PR_URL and rerun from land"
  fi
  if ! git diff --quiet "$release_ref" "$RELEASE_COMMIT"; then
    die "release merge commit does not have the release branch tree; inspect $PR_URL and rerun from land"
  fi
  if [[ "$(current_branch)" != "main" ]]; then
    run git checkout main
  fi
  run git merge --ff-only "$REMOTE/main"
  if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
    run git branch --delete --force "$RELEASE_BRANCH"
  fi
  assert_remote_main_version
}

resolve_release_commit() {
  local record
  local state
  local pr_url
  local merge_commit

  if [[ -n "$RELEASE_COMMIT" ]]; then
    return
  fi
  record=$(pr_record)
  if [[ -z "$record" ]]; then
    die "could not find the merged PR for $RELEASE_BRANCH; inspect the release PR and rerun from tag"
  fi
  IFS='|' read -r state pr_url merge_commit <<<"$record"
  if [[ "$state" != "MERGED" || -z "$merge_commit" ]]; then
    die "release PR is not merged with a squash commit: $pr_url; finish land and rerun from tag"
  fi
  PR_URL=$pr_url
  RELEASE_COMMIT=$merge_commit
  if ! git merge-base --is-ancestor "$RELEASE_COMMIT" "$REMOTE/main"; then
    die "release merge commit $RELEASE_COMMIT is not on $REMOTE/main; inspect $PR_URL before tagging"
  fi
}

tag_release() {
  local expected
  local local_commit=
  local lines
  local remote_commit=

  if [[ "$DRY_RUN" == "1" ]]; then
    run git tag -a "$TAG" '<release-merge-commit>' -F '<changelog section>'
    run git push "$REMOTE" "$TAG"
    return
  fi
  make_notes_file
  resolve_release_commit
  expected=$RELEASE_COMMIT
  if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
    local_commit=$(git rev-parse "$TAG^{commit}")
    if [[ "$local_commit" != "$expected" ]]; then
      die "$TAG points to $local_commit, expected $expected; repair the tag before rerunning from tag"
    fi
  fi
  lines=$(remote_tag_lines)
  if [[ -n "$lines" ]]; then
    remote_commit=$(remote_tag_commit "$lines")
    if [[ "$remote_commit" != "$expected" ]]; then
      die "$TAG points to $remote_commit on $REMOTE, expected $expected; repair the tag before rerunning from tag"
    fi
  fi
  if [[ -z "$local_commit" ]]; then
    run git tag -a "$TAG" "$expected" -F "$NOTES_FILE"
  fi
  if [[ -z "$remote_commit" ]]; then
    run git push "$REMOTE" "$TAG"
  else
    plan "$TAG already exists at the expected commit on $REMOTE"
  fi
  RELEASE_URL="https://github.com/$GITHUB_REPO/releases/tag/$TAG"
}

release_exists() {
  local url

  if url=$(gh release view "$TAG" --repo "$GITHUB_REPO" \
    --json url --jq .url 2>/dev/null); then
    RELEASE_URL=$url
    return 0
  fi
  return 1
}

find_release_run() {
  local started
  local elapsed
  local record

  started=$(date +%s)
  while true; do
    record=$(release_run_record)
    if [[ -n "$record" ]]; then
      IFS='|' read -r RELEASE_RUN_ID WORKFLOW_URL <<<"$record"
      return
    fi
    elapsed=$(($(date +%s) - started))
    if ((elapsed >= 300)); then
      die "Release workflow did not appear within 5 minutes; inspect Actions and rerun from publish"
    fi
    printf '    waiting for Release workflow, elapsed: %ss\n' "$elapsed"
    sleep 10
  done
}

release_run_record() {
  gh run list --repo "$GITHUB_REPO" --workflow Release \
    --event push --branch "$TAG" --limit 10 --json databaseId,url \
    --jq '.[0] | select(. != null) | [.databaseId, .url] | join("|")'
}

assert_release_assets() {
  local actual
  local expected
  local target

  expected=$(
    for target in \
      x86_64-unknown-linux-gnu \
      aarch64-unknown-linux-gnu \
      x86_64-apple-darwin \
      aarch64-apple-darwin; do
      printf 'verlet-%s-%s.tar.gz\n' "$VERSION" "$target"
      printf 'verlet-%s-%s.tar.gz.sha256\n' "$VERSION" "$target"
    done
    printf 'install.sh\nlatest.json\n'
  )
  expected=$(printf '%s\n' "$expected" | sort)
  actual=$(gh release view "$TAG" --repo "$GITHUB_REPO" \
    --json assets --jq '.assets[].name' | sort)
  if [[ "$actual" != "$expected" ]]; then
    printf 'expected release assets:\n%s\n\nactual release assets:\n%s\n' \
      "$expected" "$actual" >&2
    die "release asset set is incomplete or unexpected; repair publishing and rerun from publish"
  fi
}

publish() {
  local record=

  if [[ "$DRY_RUN" == "1" ]]; then
    run gh release view "$TAG" --repo "$GITHUB_REPO" --json url --jq .url
    run gh run list --repo "$GITHUB_REPO" --workflow Release \
      --event push --branch "$TAG" --limit 10 --json databaseId,url \
      --jq '.[0] | select(. != null) | [.databaseId, .url] | join("|")'
    run gh run watch '<release-run-id>' --repo "$GITHUB_REPO" --exit-status
    run gh release view "$TAG" --repo "$GITHUB_REPO" \
      --json assets --jq '.assets[].name'
    run gh release edit "$TAG" --repo "$GITHUB_REPO" \
      --notes-file '<changelog section>'
    WORKFLOW_URL="https://github.com/$GITHUB_REPO/actions/runs/<id>"
    RELEASE_URL="https://github.com/$GITHUB_REPO/releases/tag/$TAG"
    return
  fi
  make_notes_file
  if release_exists; then
    plan "$TAG release already exists; skip workflow watch"
    record=$(release_run_record)
    if [[ -n "$record" ]]; then
      IFS='|' read -r RELEASE_RUN_ID WORKFLOW_URL <<<"$record"
    fi
  else
    find_release_run
    run gh run watch "$RELEASE_RUN_ID" --repo "$GITHUB_REPO" --exit-status
  fi
  assert_release_assets
  run gh release edit "$TAG" --repo "$GITHUB_REPO" --notes-file "$NOTES_FILE"
  if [[ -z "$RELEASE_URL" ]]; then
    RELEASE_URL="https://github.com/$GITHUB_REPO/releases/tag/$TAG"
  fi
}

install_check() {
  local install_tmp
  local isolated_root
  local output
  local installer_url="https://github.com/$GITHUB_REPO/releases/download/$TAG/install.sh"

  if [[ "$DRY_RUN" == "1" ]]; then
    plan 'install_tmp=$(mktemp -d "${TMPDIR:-/tmp}/verlet-release-install.XXXXXX")'
    run curl -fsSL "$installer_url" -o '<install_tmp>/install.sh'
    run sh '<install_tmp>/install.sh' --version "$VERSION" \
      --repo "$GITHUB_REPO" --install-root '<install_tmp>/root/install' \
      --bin-dir '<install_tmp>/root/bin' --man-dir '<install_tmp>/root/man'
    run '<install_tmp>/root/bin/verlet' --version
    plan 'rm -rf <install_tmp>'
    return
  fi
  install_tmp=$(mktemp -d "${TMPDIR:-/tmp}/verlet-release-install.XXXXXX")
  isolated_root="$install_tmp/root"
  run curl -fsSL "$installer_url" -o "$install_tmp/install.sh"
  run sh "$install_tmp/install.sh" --version "$VERSION" \
    --repo "$GITHUB_REPO" --install-root "$isolated_root/install" \
    --bin-dir "$isolated_root/bin" --man-dir "$isolated_root/man"
  output=$("$isolated_root/bin/verlet" --version)
  if [[ "$output" != "verlet $VERSION" ]]; then
    rm -rf "$install_tmp"
    die "installed verlet reported '$output', expected 'verlet $VERSION'; repair the release and rerun from install-check"
  fi
  printf '    %s\n' "$output"
  rm -rf "$install_tmp"
}

tap_formula() {
  gh api -H 'Accept: application/vnd.github.raw+json' \
    "repos/$TAP_REPO/contents/Formula/verlet.rb"
}

formula_matches_release() {
  local formula=$1

  grep -E '^[[:space:]]*(url|version)[[:space:]]' <<<"$formula" \
    | grep -F -e "$TAG" -e "$VERSION" >/dev/null
}

latest_tap_run() {
  gh run list --repo "$TAP_REPO" --workflow update-verlet.yml \
    --event workflow_dispatch --limit 1 --json databaseId,url \
    --jq '.[0] | select(. != null) | [.databaseId, .url] | join("|")'
}

tap() {
  local formula
  local before_record
  local before_id=0
  local record
  local tap_run_id
  local started
  local elapsed

  if [[ "$DRY_RUN" == "1" ]]; then
    run gh api -H 'Accept: application/vnd.github.raw+json' \
      "repos/$TAP_REPO/contents/Formula/verlet.rb"
    run gh run list --repo "$TAP_REPO" --workflow update-verlet.yml \
      --event workflow_dispatch --limit 1 --json databaseId,url \
      --jq '.[0] | select(. != null) | [.databaseId, .url] | join("|")'
    run gh workflow run update-verlet.yml --repo "$TAP_REPO"
    run gh run list --repo "$TAP_REPO" --workflow update-verlet.yml \
      --event workflow_dispatch --limit 20 --json databaseId,url \
      --jq '[.[] | select(.databaseId != <before-id>)] | .[0] | select(. != null) | [.databaseId, .url] | join("|")'
    run gh run watch '<tap-run-id>' --repo "$TAP_REPO" --exit-status
    run gh api -H 'Accept: application/vnd.github.raw+json' \
      "repos/$TAP_REPO/contents/Formula/verlet.rb"
    TAP_RUN_URL="https://github.com/$TAP_REPO/actions/runs/<id>"
    return
  fi

  formula=$(tap_formula)
  if [[ -n "$formula" ]] && formula_matches_release "$formula"; then
    plan "tap formula already contains $TAG or $VERSION; skip dispatch"
    record=$(latest_tap_run)
    if [[ -n "$record" ]]; then
      IFS='|' read -r tap_run_id TAP_RUN_URL <<<"$record"
    fi
    return
  fi

  before_record=$(latest_tap_run)
  if [[ -n "$before_record" ]]; then
    IFS='|' read -r before_id TAP_RUN_URL <<<"$before_record"
  fi
  run gh workflow run update-verlet.yml --repo "$TAP_REPO"
  started=$(date +%s)
  while true; do
    record=$(gh run list --repo "$TAP_REPO" --workflow update-verlet.yml \
      --event workflow_dispatch --limit 20 --json databaseId,url \
      --jq "[.[] | select(.databaseId != $before_id)] | .[0] | select(. != null) | [.databaseId, .url] | join(\"|\")")
    if [[ -n "$record" ]]; then
      IFS='|' read -r tap_run_id TAP_RUN_URL <<<"$record"
      break
    fi
    elapsed=$(($(date +%s) - started))
    if ((elapsed >= 300)); then
      die "tap workflow did not appear within 5 minutes; inspect the tap repo and rerun from tap"
    fi
    printf '    waiting for tap workflow, elapsed: %ss\n' "$elapsed"
    sleep 10
  done
  run gh run watch "$tap_run_id" --repo "$TAP_REPO" --exit-status
  formula=$(tap_formula)
  if ! formula_matches_release "$formula"; then
    die "tap formula URL or version does not contain $TAG or $VERSION; inspect $TAP_RUN_URL and rerun from tap"
  fi
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

write_receipt() {
  local out_dir="$ROOT/dist/release/$TAG"
  local receipt="$out_dir/receipt.json"
  local index
  local comma=

  if [[ "$DRY_RUN" == "1" ]]; then
    plan "write $receipt"
    return
  fi
  mkdir -p "$out_dir"
  {
    printf '[\n'
    for index in "${!STEPS[@]}"; do
      printf '%s  {"step":"%s","status":"%s","started_at":"%s","finished_at":"%s","url":"%s"}' \
        "$comma" \
        "$(json_escape "${STEPS[$index]}")" \
        "$(json_escape "${RECEIPT_STATUS[$index]}")" \
        "$(json_escape "${RECEIPT_STARTED[$index]}")" \
        "$(json_escape "${RECEIPT_FINISHED[$index]}")" \
        "$(json_escape "${RECEIPT_URL[$index]}")"
      comma=$',\n'
    done
    printf '\n]\n'
  } >"$receipt"
  printf '\nReceipt: %s\n' "$receipt"
}

print_receipt() {
  local index

  printf '\n%-16s %-10s %-20s %-20s %s\n' STEP STATUS STARTED FINISHED URL
  for index in "${!STEPS[@]}"; do
    printf '%-16s %-10s %-20s %-20s %s\n' \
      "${STEPS[$index]}" \
      "${RECEIPT_STATUS[$index]}" \
      "${RECEIPT_STARTED[$index]}" \
      "${RECEIPT_FINISHED[$index]}" \
      "${RECEIPT_URL[$index]}"
  done
  printf '\nPR: %s\n' "${PR_URL:-n/a}"
  printf 'Release run: %s\n' "${WORKFLOW_URL:-n/a}"
  printf 'Release: %s\n' "${RELEASE_URL:-https://github.com/$GITHUB_REPO/releases/tag/$TAG}"
  printf 'Tap run: %s\n' "${TAP_RUN_URL:-n/a}"
}

assert_resume_state() {
  local start=$1

  if [[ "$DRY_RUN" == "1" || "$start" == "0" ]]; then
    return
  fi
  case "$start" in
    1)
      if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH" \
        || git show-ref --verify --quiet "refs/remotes/$REMOTE/$RELEASE_BRANCH"; then
        require_clean_worktree
      else
        preflight
      fi
      ;;
    2|3)
      assert_release_commit
      ;;
    4)
      require_clean_worktree
      check gh auth status
      run git fetch "$REMOTE"
      ;;
    5)
      require_clean_worktree
      check gh auth status
      run git fetch "$REMOTE"
      assert_remote_main_version
      ;;
    6|7|8)
      require_clean_worktree
      check gh auth status
      run git fetch "$REMOTE"
      assert_remote_main_version
      assert_tag_on_main
      ;;
  esac
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --yes)
      YES=1
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --quick)
      QUICK=1
      shift
      ;;
    --skip-linux)
      SKIP_LINUX=1
      shift
      ;;
    --skip-catalog-check)
      SKIP_CATALOG_CHECK=1
      shift
      ;;
    --allow-empty-changelog)
      ALLOW_EMPTY_CHANGELOG=1
      shift
      ;;
    --from)
      FROM_STEP=${2:?--from requires a value}
      shift 2
      ;;
    --remote)
      REMOTE=${2:?--remote requires a value}
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    -*)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -n "$TAG" ]]; then
        printf 'unexpected extra argument: %s\n' "$1" >&2
        usage >&2
        exit 2
      fi
      TAG=$1
      shift
      ;;
  esac
done

if [[ -z "$TAG" ]]; then
  usage >&2
  exit 2
fi
validate_tag_shape
VERSION=${TAG#v}
RELEASE_BRANCH="release/$TAG"
if ! START_INDEX=$(step_index "$FROM_STEP"); then
  die "unknown step '$FROM_STEP'; choose one of: ${STEPS[*]}"
fi
if [[ "$DRY_RUN" == "1" ]]; then
  export GIT_OPTIONAL_LOCKS=0
fi

cd "$ROOT"
source "$ROOT/scripts/release-version.sh"
assert_resume_state "$START_INDEX"

for INDEX in "${!STEPS[@]}"; do
  RECEIPT_STARTED[$INDEX]=$(timestamp)
  RECEIPT_URL[$INDEX]=
  if ((INDEX < START_INDEX)); then
    RECEIPT_STATUS[$INDEX]=skipped
    RECEIPT_FINISHED[$INDEX]=${RECEIPT_STARTED[$INDEX]}
    continue
  fi
  printf '\n==> [%s/9] %s\n' "$((INDEX + 1))" "${STEPS[$INDEX]}"
  case "${STEPS[$INDEX]}" in
    preflight) preflight ;;
    bump) bump ;;
    review) review ;;
    gate) gate ;;
    land) land; RECEIPT_URL[$INDEX]=$PR_URL ;;
    tag) tag_release; RECEIPT_URL[$INDEX]=$RELEASE_URL ;;
    publish) publish; RECEIPT_URL[$INDEX]=$WORKFLOW_URL ;;
    install-check) install_check; RECEIPT_URL[$INDEX]=$RELEASE_URL ;;
    tap) tap; RECEIPT_URL[$INDEX]=$TAP_RUN_URL ;;
  esac
  if [[ "$DRY_RUN" == "1" ]]; then
    RECEIPT_STATUS[$INDEX]=dry-run
  else
    RECEIPT_STATUS[$INDEX]=ok
  fi
  RECEIPT_FINISHED[$INDEX]=$(timestamp)
done

write_receipt
print_receipt
