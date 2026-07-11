#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHA="0123456789abcdef0123456789abcdef01234567"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

cd "$ROOT"

receipt="$(
  GITHUB_SHA="$SHA" RECEIPTS_DRY_RUN=1 \
    scripts/ci-receipts.sh scripts/testdata/verify-output.log
)"

jq -e \
  --arg sha "$SHA" \
  '.schema == 1
    and .commit == $sha
    and (.counted_at | test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$"))
    and (.loc_rust | type == "number" and . > 0)
    and (.crates | type == "number" and . > 0)
    and .tests_passed == 12
    and .source == "ci"
    and (keys == ["commit", "counted_at", "crates", "loc_rust", "schema", "source", "tests_passed"])' \
  <<<"$receipt" >/dev/null
printf 'positive fixture: passed (12 witnessed tests)\n'

if GITHUB_SHA="$SHA" RECEIPTS_DRY_RUN=1 \
  scripts/ci-receipts.sh scripts/testdata/verify-output-zero-tests.log >/dev/null 2>&1; then
  printf 'zero-tests fixture: unexpectedly succeeded\n' >&2
  exit 1
fi
printf 'zero-tests fixture: passed (failed closed)\n'

if GITHUB_SHA="$SHA" RECEIPTS_DRY_RUN=1 \
  scripts/ci-receipts.sh scripts/testdata/verify-output-zero-count.log >/dev/null 2>&1; then
  printf 'zero-count fixture: unexpectedly succeeded\n' >&2
  exit 1
fi
printf 'zero-count fixture: passed (failed closed)\n'

if GITHUB_SHA="$SHA" RECEIPTS_DRY_RUN=1 \
  scripts/ci-receipts.sh scripts/testdata/verify-output-malformed.log >/dev/null 2>&1; then
  printf 'malformed fixture: unexpectedly succeeded\n' >&2
  exit 1
fi
printf 'malformed fixture: passed (failed closed)\n'

git clone --quiet "$ROOT" "$TMP/repo"
cp scripts/ci-receipts.sh "$TMP/repo/scripts/ci-receipts.sh"
cp scripts/testdata/verify-output.log "$TMP/repo/verify-output.log"
git -C "$TMP/repo" remote set-url origin "$TMP/missing.git"
if output="$(
  cd "$TMP/repo"
  GITHUB_SHA="$SHA" scripts/ci-receipts.sh verify-output.log 2>&1
)"; then
  printf 'remote-error fixture: unexpectedly succeeded\n' >&2
  exit 1
fi
grep -F 'could not determine whether origin/receipts exists' <<<"$output" >/dev/null
[[ "$(git -C "$TMP/repo" branch --show-current)" != "receipts" ]]
printf 'remote-error fixture: passed (did not create an orphan branch)\n'
