#!/usr/bin/env bash
set -euo pipefail

# Emit receipts.json from the verify log supplied as the first argument for the
# cooldis.com receipts panel. Test counts are witnessed from that green run;
# missing, malformed, or zero counts fail closed so no false receipt is published.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERIFY_LOG="${1:-}"

fail() {
  printf 'ci-receipts: %s\n' "$*" >&2
  exit 1
}

is_numeric() {
  [[ "$1" =~ ^[0-9]+$ ]]
}

[[ -n "$VERIFY_LOG" ]] || fail "usage: $0 VERIFY_LOG"
[[ -f "$VERIFY_LOG" ]] || fail "verify log not found: $VERIFY_LOG"
[[ -n "${GITHUB_SHA:-}" ]] || fail "GITHUB_SHA is required"
[[ "$GITHUB_SHA" =~ ^[0-9a-fA-F]{40}$ ]] || fail "GITHUB_SHA must be a full 40-character commit SHA"

tests_passed=0
test_results=0
while IFS= read -r line; do
  [[ "$line" == *"test result:"* ]] || continue
  ((test_results += 1))
  if [[ "$line" =~ ^[[:space:]]*test\ result:\ ok\.\ ([0-9]+)\ passed\; ]]; then
    count="${BASH_REMATCH[1]}"
  else
    fail "malformed or non-passing test result: $line"
  fi
  is_numeric "$count" || fail "non-numeric passing test count: $count"
  tests_passed=$((tests_passed + count))
done < "$VERIFY_LOG"

is_numeric "$test_results" || fail "non-numeric test-result line count"
is_numeric "$tests_passed" || fail "non-numeric total passing test count"
((test_results > 0)) || fail "verify log contains no passing test results"
((tests_passed > 0)) || fail "verify log reports zero tests passed"

cd "$ROOT"

loc_rust=0
rust_files=0
while IFS= read -r -d '' file; do
  ((rust_files += 1))
  lines="$(wc -l < "$file")"
  lines="${lines//[[:space:]]/}"
  is_numeric "$lines" || fail "non-numeric Rust line count for $file: $lines"
  loc_rust=$((loc_rust + lines))
done < <(git ls-files -z '*.rs')

is_numeric "$rust_files" || fail "non-numeric Rust file count"
is_numeric "$loc_rust" || fail "non-numeric Rust line count"
((rust_files > 0)) || fail "repository has no tracked Rust files"
((loc_rust > 0)) || fail "repository has zero tracked Rust lines"

crates="$(cargo metadata --no-deps --format-version 1 | jq -r '.packages | length')"
is_numeric "$crates" || fail "non-numeric workspace crate count: $crates"
((crates > 0)) || fail "workspace has zero crates"

counted_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
receipt_file="$(mktemp)"
trap 'rm -f "$receipt_file"' EXIT

jq -n \
  --arg commit "$GITHUB_SHA" \
  --arg counted_at "$counted_at" \
  --argjson loc_rust "$loc_rust" \
  --argjson crates "$crates" \
  --argjson tests_passed "$tests_passed" \
  '{
    schema: 1,
    commit: $commit,
    counted_at: $counted_at,
    loc_rust: $loc_rust,
    crates: $crates,
    tests_passed: $tests_passed,
    source: "ci"
  }' > "$receipt_file"

if [[ "${RECEIPTS_DRY_RUN:-0}" == "1" ]]; then
  cat "$receipt_file"
  exit 0
fi

git config user.name "github-actions[bot]"
git config user.email "41898282+github-actions[bot]@users.noreply.github.com"
git config commit.gpgsign false

branch_existed=0
if git ls-remote --exit-code --heads origin refs/heads/receipts >/dev/null; then
  branch_existed=1
  git fetch origin refs/heads/receipts:refs/remotes/origin/receipts
  git checkout -B receipts origin/receipts
else
  branch_status=$?
  ((branch_status == 2)) || fail "could not determine whether origin/receipts exists"
  git checkout --orphan receipts
  git rm -rf .
fi

cp "$receipt_file" receipts.json
if ((branch_existed == 0)); then
  cat > README.md <<'EOF'
# Verlet CI receipts
`receipts.json` contains witnessed metrics consumed by the cooldis.com receipts panel.
It is generated after each green main-branch verification run by GitHub Actions.
EOF
fi

git add receipts.json
if ((branch_existed == 0)); then
  git add README.md
fi
git commit -m "receipt: ${GITHUB_SHA:0:7}"

if ! git push origin HEAD:refs/heads/receipts; then
  printf 'ci-receipts: push raced; rebasing and retrying once\n' >&2
  git fetch origin refs/heads/receipts:refs/remotes/origin/receipts
  if ((branch_existed == 1)); then
    git rebase -X theirs origin/receipts
  else
    git rebase -X theirs --onto origin/receipts --root
  fi
  git push origin HEAD:refs/heads/receipts
fi
