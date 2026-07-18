#!/usr/bin/env bash

# Pins staged product-term behavior for ordinary commits and in-progress merges:
# exact legacy output off-merge, index-only reads, every merge parent considered,
# malformed parent data failing closed, resolution-authored lines rejected, and
# the intentional override preserved.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
GUARD_SCRIPT="$SCRIPT_DIR/guard-rails.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/guard-rails-test.XXXXXX")" || exit 1
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
GIT_TEMPLATE_DIR="$TMP_DIR/git-template"
PII_TERMS_FILE="$TMP_DIR/pii-terms"
FAILURES=0
REPO=
RUN_STATUS=0

mkdir -p "$GIT_TEMPLATE_DIR"
unset GIT_COMMON_DIR GIT_CONFIG_COUNT GIT_DIR GIT_INDEX_FILE GIT_OBJECT_DIRECTORY
unset GIT_ALTERNATE_OBJECT_DIRECTORIES GIT_WORK_TREE
export GIT_CONFIG_GLOBAL=/dev/null
export GIT_CONFIG_NOSYSTEM=1
export GIT_TEMPLATE_DIR
export LC_ALL=C

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

assert_files_equal() {
  local expected=$1
  local actual=$2
  local name=$3

  if ! cmp -s "$expected" "$actual"; then
    fail "$name"
    printf 'expected:\n%sactual:\n%s' "$(<"$expected")" "$(<"$actual")" >&2
  fi
}

init_repo() {
  local name=$1

  REPO="$TMP_DIR/$name"
  mkdir -p "$REPO/scripts" "$REPO/crates/cooldis-kernel/src"
  cp "$GUARD_SCRIPT" "$REPO/scripts/guard-rails.sh"
  printf 'pub const runtime_value: &str = "base";\n' >"$REPO/crates/cooldis-kernel/src/lib.rs"
  git -C "$REPO" init -q -b current
  git -C "$REPO" config user.name 'Guard Rails Test'
  git -C "$REPO" config user.email 'guard-rails-test@example.invalid'
  git -C "$REPO" add .
  git -C "$REPO" commit --no-gpg-sign -qm 'base'
}

run_guard() {
  local name=$1
  local allow_product_terms=${2:-0}

  if COOLDIS_PII_TERMS="$PII_TERMS_FILE" \
    COOLDIS_ALLOW_PRODUCT_TERMS="$allow_product_terms" \
    "$REPO/scripts/guard-rails.sh" staged \
    >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"; then
    RUN_STATUS=0
  else
    RUN_STATUS=$?
  fi
}

: >"$PII_TERMS_FILE"
: >"$TMP_DIR/empty"
printf 'Cooldis guard rails passed (staged).\n' >"$TMP_DIR/passed"

# A plain staged product term preserves the legacy failure bytes.
init_repo non-merge-hit
printf '// telegram adapter: intentional\n' \
  >>"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
run_guard non-merge-hit
assert_eq 1 "$RUN_STATUS" 'non-merge product term failed'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/non-merge-hit.out" 'non-merge failure stdout stayed empty'
{
  printf 'Staged runtime code appears to add product-shaped terms.\n'
  printf 'Keep product logic out of Cooldis, or set COOLDIS_ALLOW_PRODUCT_TERMS=1 for an intentional exception.\n'
  printf '+// telegram adapter: intentional\n'
} >"$TMP_DIR/non-merge-hit.expected.err"
assert_files_equal "$TMP_DIR/non-merge-hit.expected.err" "$TMP_DIR/non-merge-hit.err" \
  'non-merge product-term diagnostics stayed byte-identical'

# Only the index is guarded; a clean worktree copy must not hide a staged hit.
printf 'pub const runtime_value: &str = "worktree clean";\n' \
  >"$REPO/crates/cooldis-kernel/src/lib.rs"
run_guard non-merge-index-only
assert_eq 1 "$RUN_STATUS" 'staged product term survived a differing worktree'
assert_files_equal "$TMP_DIR/non-merge-hit.expected.err" "$TMP_DIR/non-merge-index-only.err" \
  'staged product-term diagnostics came from the index'
run_guard non-merge-override 1
assert_eq 0 "$RUN_STATUS" 'non-merge override passed'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/non-merge-override.out" 'non-merge override output matched'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/non-merge-override.err" 'non-merge override stderr stayed empty'

# A plain staged clean change preserves the legacy success bytes.
init_repo non-merge-clean
printf 'pub const kernel_adapter: &str = "clean";\n' \
  >>"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
run_guard non-merge-clean
assert_eq 0 "$RUN_STATUS" 'non-merge clean change passed'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/non-merge-clean.out" 'non-merge clean output stayed byte-identical'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/non-merge-clean.err" 'non-merge clean stderr stayed empty'

# A product term copied verbatim from a merge parent is not newly authored.
init_repo merge-parent-only
git -C "$REPO" switch -qc product-parent
printf '// telegram adapter: parent\n' \
  >>"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit --no-gpg-sign -qm 'product parent'
git -C "$REPO" switch -q current
printf 'current branch\n' >"$REPO/current.txt"
git -C "$REPO" add current.txt
git -C "$REPO" commit --no-gpg-sign -qm 'diverge current'
git -C "$REPO" merge -q --no-commit product-parent >/dev/null 2>&1
run_guard merge-parent-only
assert_eq 0 "$RUN_STATUS" 'merge carrying parent-only product term passed'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/merge-parent-only.out" 'parent-only merge output matched'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/merge-parent-only.err" 'parent-only merge stderr stayed empty'

# MERGE_HEAD is line-oriented; a missing final newline must not skip a parent.
merge_head_path="$(git -C "$REPO" rev-parse --git-path MERGE_HEAD)"
printf '%s' "$(git -C "$REPO" rev-parse product-parent)" >"$REPO/$merge_head_path"
run_guard merge-parent-no-final-newline
assert_eq 0 "$RUN_STATUS" 'merge parent without final newline was still considered'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/merge-parent-no-final-newline.out" \
  'merge parent without final newline output matched'

# An octopus merge must intersect against later MERGE_HEAD parents too.
init_repo merge-octopus
git -C "$REPO" switch -qc clean-parent
printf 'clean parent\n' >"$REPO/clean-parent.txt"
git -C "$REPO" add clean-parent.txt
git -C "$REPO" commit --no-gpg-sign -qm 'clean parent'
git -C "$REPO" switch -q current
git -C "$REPO" switch -qc product-parent
printf '// telegram adapter: later parent\n' \
  >>"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit --no-gpg-sign -qm 'product parent'
git -C "$REPO" switch -q current
printf 'current branch\n' >"$REPO/current.txt"
git -C "$REPO" add current.txt
git -C "$REPO" commit --no-gpg-sign -qm 'diverge current'
git -C "$REPO" merge -q --no-commit clean-parent product-parent >/dev/null 2>&1
run_guard merge-octopus
assert_eq 0 "$RUN_STATUS" 'octopus merge considered every parent'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/merge-octopus.out" 'octopus merge output matched'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/merge-octopus.err" 'octopus merge stderr stayed empty'

# Identical normalized content in a carried file cannot hide a newly added
# occurrence in another guarded file.
init_repo merge-identical-lines
printf 'pub const second_value: &str = "base";\n' \
  >"$REPO/crates/cooldis-kernel/src/second.rs"
git -C "$REPO" add crates/cooldis-kernel/src/second.rs
git -C "$REPO" commit --no-gpg-sign -qm 'second base file'
git -C "$REPO" switch -qc product-parent
printf '// telegram adapter: collision\n' \
  >>"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit --no-gpg-sign -qm 'product parent'
git -C "$REPO" switch -q current
printf 'current branch\n' >"$REPO/current.txt"
git -C "$REPO" add current.txt
git -C "$REPO" commit --no-gpg-sign -qm 'diverge current'
git -C "$REPO" merge -q --no-commit product-parent >/dev/null 2>&1
printf '// telegram adapter: collision\n' \
  >>"$REPO/crates/cooldis-kernel/src/second.rs"
git -C "$REPO" add crates/cooldis-kernel/src/second.rs
run_guard merge-identical-lines
assert_eq 1 "$RUN_STATUS" 'identical carried content did not hide a new line'
{
  printf 'Staged runtime code appears to add product-shaped terms.\n'
  printf 'Keep product logic out of Cooldis, or set COOLDIS_ALLOW_PRODUCT_TERMS=1 for an intentional exception.\n'
  printf '+// telegram adapter: collision\n'
  printf '+// telegram adapter: collision\n'
} >"$TMP_DIR/merge-identical-lines.expected.err"
assert_files_equal "$TMP_DIR/merge-identical-lines.expected.err" \
  "$TMP_DIR/merge-identical-lines.err" \
  'identical carried content retained the merge diagnostic'

# A product term absent from both parents remains new after conflict resolution.
init_repo merge-resolution
git -C "$REPO" switch -qc product-parent
printf 'pub const runtime_value: &str = "parent";\n' >"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit --no-gpg-sign -qm 'parent edit'
git -C "$REPO" switch -q current
printf 'pub const runtime_value: &str = "current";\n' >"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit --no-gpg-sign -qm 'current edit'
if git -C "$REPO" merge -q --no-commit product-parent >/dev/null 2>&1; then
  merge_status=0
else
  merge_status=$?
fi
assert_eq 1 "$merge_status" 'merge fixture produced a conflict'
printf '// telegram adapter: resolution\n' \
  >"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
run_guard merge-resolution
assert_eq 1 "$RUN_STATUS" 'resolution-authored product term failed'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/merge-resolution.out" 'merge failure stdout stayed empty'
{
  printf 'Staged runtime code appears to add product-shaped terms.\n'
  printf 'Keep product logic out of Cooldis, or set COOLDIS_ALLOW_PRODUCT_TERMS=1 for an intentional exception.\n'
  printf '+// telegram adapter: resolution\n'
} >"$TMP_DIR/merge-resolution.expected.err"
assert_files_equal "$TMP_DIR/merge-resolution.expected.err" "$TMP_DIR/merge-resolution.err" \
  'resolution-authored product-term diagnostics matched'

# Whitespace around a MERGE_HEAD object name must not turn a diff error into
# an empty hit set and bypass a genuinely new resolution line.
merge_head_path="$(git -C "$REPO" rev-parse --git-path MERGE_HEAD)"
printf '%s   \n' "$(git -C "$REPO" rev-parse product-parent)" >"$REPO/$merge_head_path"
run_guard merge-resolution-parent-whitespace
assert_eq 1 "$RUN_STATUS" 'whitespace-suffixed merge parent did not bypass the guard'
assert_files_equal "$TMP_DIR/merge-resolution.expected.err" \
  "$TMP_DIR/merge-resolution-parent-whitespace.err" \
  'whitespace-suffixed merge parent diagnostics matched'

printf 'not-a-parent\n' >"$REPO/$merge_head_path"
run_guard merge-resolution-invalid-parent
assert_eq 1 "$RUN_STATUS" 'invalid merge parent failed closed'
printf 'error: invalid merge parent in MERGE_HEAD\n' \
  >"$TMP_DIR/merge-resolution-invalid-parent.expected.err"
assert_files_equal "$TMP_DIR/merge-resolution-invalid-parent.expected.err" \
  "$TMP_DIR/merge-resolution-invalid-parent.err" \
  'invalid merge parent diagnostic matched'
run_guard merge-resolution-override 1
assert_eq 0 "$RUN_STATUS" 'merge override passed'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/merge-resolution-override.out" 'merge override output matched'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/merge-resolution-override.err" 'merge override stderr stayed empty'

if ((FAILURES > 0)); then
  printf 'guard-rails-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'guard-rails-test: ok\n'
