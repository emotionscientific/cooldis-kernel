#!/usr/bin/env bash

# Pins staged product-term behavior for ordinary commits and in-progress merges:
# exact legacy output off-merge, parent-carried lines ignored during merges,
# resolution-authored lines rejected, and the intentional override preserved.

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
GUARD_SCRIPT="$SCRIPT_DIR/guard-rails.sh"
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/guard-rails-test.XXXXXX")" || exit 1
TMP_DIR="$(cd "$TMP_DIR" && pwd -P)"
PII_TERMS_FILE="$TMP_DIR/pii-terms"
FAILURES=0
REPO=
RUN_STATUS=0

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
  git -C "$REPO" commit -qm 'base'
}

run_guard() {
  local name=$1
  local allow_product_terms=${2:-0}

  COOLDIS_PII_TERMS="$PII_TERMS_FILE" \
    COOLDIS_ALLOW_PRODUCT_TERMS="$allow_product_terms" \
    "$REPO/scripts/guard-rails.sh" staged \
    >"$TMP_DIR/$name.out" 2>"$TMP_DIR/$name.err"
  RUN_STATUS=$?
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
git -C "$REPO" commit -qm 'product parent'
git -C "$REPO" switch -q current
printf 'current branch\n' >"$REPO/current.txt"
git -C "$REPO" add current.txt
git -C "$REPO" commit -qm 'diverge current'
git -C "$REPO" merge -q --no-commit product-parent >/dev/null 2>&1
run_guard merge-parent-only
assert_eq 0 "$RUN_STATUS" 'merge carrying parent-only product term passed'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/merge-parent-only.out" 'parent-only merge output matched'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/merge-parent-only.err" 'parent-only merge stderr stayed empty'

# A product term absent from both parents remains new after conflict resolution.
init_repo merge-resolution
git -C "$REPO" switch -qc product-parent
printf 'pub const runtime_value: &str = "parent";\n' >"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit -qm 'parent edit'
git -C "$REPO" switch -q current
printf 'pub const runtime_value: &str = "current";\n' >"$REPO/crates/cooldis-kernel/src/lib.rs"
git -C "$REPO" add crates/cooldis-kernel/src/lib.rs
git -C "$REPO" commit -qm 'current edit'
git -C "$REPO" merge -q --no-commit product-parent >/dev/null 2>&1
assert_eq 1 "$?" 'merge fixture produced a conflict'
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
run_guard merge-resolution-override 1
assert_eq 0 "$RUN_STATUS" 'merge override passed'
assert_files_equal "$TMP_DIR/passed" "$TMP_DIR/merge-resolution-override.out" 'merge override output matched'
assert_files_equal "$TMP_DIR/empty" "$TMP_DIR/merge-resolution-override.err" 'merge override stderr stayed empty'

if ((FAILURES > 0)); then
  printf 'guard-rails-test: %s failure(s)\n' "$FAILURES" >&2
  exit 1
fi

printf 'guard-rails-test: ok\n'
