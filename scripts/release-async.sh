#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE="origin"
FULL_GATE=0
SKIP_LOCAL_GATE=0
DRY_RUN=0
PUSH_MAIN=1

usage() {
  cat <<'USAGE'
release-async.sh - run a local release preflight, then trigger GitHub publishing.

Usage:
  scripts/release-async.sh <vX.Y.Z[-prerelease]> [options]

Options:
  --full-gate         Run scripts/release-v1-candidate.sh instead of the quick package smoke.
  --skip-local-gate   Skip the local package/full-gate preflight.
  --no-push-main      Push only the tag, not the current branch.
  --remote NAME       Git remote to push. Default: origin.
  --dry-run           Print the actions without creating or pushing a tag.
  -h, --help          Show this help.

Default flow:
  1. Validate the release tag against crates/verlet-kernel/Cargo.toml.
  2. Build the host-target release archive locally.
  3. Smoke the archive and local installer.
  4. Create an annotated tag at HEAD if it does not already exist.
  5. Push the current branch and tag.
  6. Print the GitHub Release workflow URL and exit without watching.

GitHub remains the publisher of record. This script is a local confidence pass
for maintainers who do not want to block active development on the remote matrix.
USAGE
}

die() {
  echo "release-async: $*" >&2
  exit 1
}

run() {
  printf '\n==> %s\n' "$*"
  if [[ "$DRY_RUN" == "1" ]]; then
    return 0
  fi
  "$@"
}

require_clean_worktree() {
  if ! git diff --quiet --; then
    die "working tree has unstaged changes; commit or stash before releasing"
  fi
  if ! git diff --cached --quiet --; then
    die "index has staged changes; commit or unstage before releasing"
  fi
}

ensure_tag_at_head() {
  local tag="$1"
  local head_sha="$2"
  local tag_sha

  if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    tag_sha="$(git rev-list -n 1 "$tag")"
    if [[ "$tag_sha" != "$head_sha" ]]; then
      die "tag $tag already points to $tag_sha, not HEAD $head_sha"
    fi
    printf '\n==> tag %s already points at HEAD\n' "$tag"
    return 0
  fi

  run git tag -a "$tag" -m "$tag"
}

print_release_run_url() {
  local tag="$1"
  local head_sha="$2"

  if ! command -v gh >/dev/null 2>&1; then
    printf '\nRelease workflow:\n'
    printf '  https://github.com/emotionscientific/cooldis-kernel/actions/workflows/release.yml\n'
    return 0
  fi

  printf '\n==> looking up Release workflow run\n'
  local url=""
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    url="$(
      gh run list \
        --workflow Release \
        --commit "$head_sha" \
        --limit 20 \
        --json event,headBranch,url,displayTitle \
        --jq ".[] | select(.event == \"push\" and (.headBranch == \"$tag\" or .displayTitle == \"$tag\")) | .url" \
        | head -n 1
    )"
    if [[ -n "$url" ]]; then
      break
    fi
    sleep 2
  done

  if [[ -n "$url" ]]; then
    printf '\nRelease workflow started:\n'
    printf '  %s\n' "$url"
  else
    printf '\nRelease workflow should start shortly:\n'
    printf '  https://github.com/emotionscientific/cooldis-kernel/actions/workflows/release.yml\n'
    printf 'Check with:\n'
    printf '  gh run list --workflow Release --commit %s --limit 5\n' "$head_sha"
  fi
}

TAG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --full-gate)
      FULL_GATE=1
      shift
      ;;
    --skip-local-gate)
      SKIP_LOCAL_GATE=1
      shift
      ;;
    --no-push-main)
      PUSH_MAIN=0
      shift
      ;;
    --remote)
      REMOTE="${2:?--remote requires a value}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    -*)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -n "$TAG" ]]; then
        echo "unexpected extra argument: $1" >&2
        usage >&2
        exit 2
      fi
      TAG="$1"
      shift
      ;;
  esac
done

if [[ -z "$TAG" ]]; then
  usage >&2
  exit 2
fi
case "$TAG" in
  v*) ;;
  *) die "release tag must start with v, for example v0.1.0-rc.4" ;;
esac

cd "$ROOT"

if [[ "$DRY_RUN" == "1" ]]; then
  printf '\n==> would require a clean git worktree\n'
else
  require_clean_worktree
fi
printf '\n==> %s\n' "$ROOT/scripts/check-release-tag.sh $TAG"
"$ROOT/scripts/check-release-tag.sh" "$TAG"

HEAD_SHA="$(git rev-parse HEAD)"
BRANCH="$(git symbolic-ref --quiet --short HEAD || true)"
if [[ "$PUSH_MAIN" == "1" && -z "$BRANCH" ]]; then
  die "--no-push-main is required from a detached HEAD"
fi

if [[ "$DRY_RUN" == "1" ]]; then
  printf '\nDry run only. Would run the local release preflight, ensure tag %s at HEAD, then push to %s.\n' "$TAG" "$REMOTE"
  exit 0
fi

if [[ "$SKIP_LOCAL_GATE" != "1" ]]; then
  if [[ "$FULL_GATE" == "1" ]]; then
    run env VERLET_RELEASE_VERSION="${TAG#v}" "$ROOT/scripts/release-v1-candidate.sh"
  else
    OUT_DIR="$ROOT/dist/release-async/$TAG"
    run rm -rf "$OUT_DIR"
    run env VERLET_RELEASE_VERSION="${TAG#v}" \
      "$ROOT/scripts/package-release-binary.sh" \
      --out-dir "$OUT_DIR"
    ARCHIVE="$(find "$OUT_DIR" -maxdepth 1 -name 'verlet-*.tar.gz' | head -n 1)"
    if [[ -z "$ARCHIVE" ]]; then
      die "release archive was not created under $OUT_DIR"
    fi
    run "$ROOT/scripts/smoke-release-archive.sh" "$ARCHIVE"
    run "$ROOT/scripts/smoke-install.sh" "$ARCHIVE"
    run "$ROOT/scripts/write-release-manifest.sh" --out-dir "$OUT_DIR" --tag "$TAG"
  fi
else
  printf '\n==> skipping local release preflight\n'
fi

ensure_tag_at_head "$TAG" "$HEAD_SHA"

if [[ "$PUSH_MAIN" == "1" ]]; then
  run git push "$REMOTE" "$BRANCH"
fi
run git push "$REMOTE" "$TAG"

print_release_run_url "$TAG" "$HEAD_SHA"

printf '\nRelease trigger complete. GitHub will build and publish asynchronously.\n'
