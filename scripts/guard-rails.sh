#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:-staged}"

cd "$ROOT"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

case "$MODE" in
  staged | tracked) ;;
  *)
    die "usage: scripts/guard-rails.sh [staged|tracked]"
    ;;
esac

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  die "guard rails require a git checkout"
fi

paths=()
if [[ "$MODE" == "staged" ]]; then
  while IFS= read -r -d '' path; do
    paths+=("$path")
  done < <(git diff --cached --name-only -z --diff-filter=ACMR)
else
  while IFS= read -r -d '' path; do
    paths+=("$path")
  done < <(git ls-files -z)
fi

bad_runtime_paths=()

if ((${#paths[@]} > 0)); then
  for path in "${paths[@]}"; do
    [[ -z "$path" ]] && continue

    if [[ "$path" == scratch/* || "$path" == target/* || "$path" == */target/* ]]; then
      bad_runtime_paths+=("$path")
    fi

  done
fi

if ((${#bad_runtime_paths[@]} > 0)); then
  printf 'Refusing to include generated/scratch runtime paths:\n' >&2
  printf '  %s\n' "${bad_runtime_paths[@]}" >&2
  exit 1
fi

product_term_hits() {
  local base_rev="${1:-}"

  if [[ -n "$base_rev" ]]; then
    git diff --cached "$base_rev" --unified=0 -- crates/cooldis-kernel/src crates/cooldis-guest-sdk 2>/dev/null \
      | grep -E '^\+[^+].*([^[:alnum:]_]|^)(billing|dashboard|dashboards|invite|invites|telegram|railway)([^[:alnum:]_]|$)' \
      || true
  else
    git diff --cached --unified=0 -- crates/cooldis-kernel/src crates/cooldis-guest-sdk 2>/dev/null \
      | grep -E '^\+[^+].*([^[:alnum:]_]|^)(billing|dashboard|dashboards|invite|invites|telegram|railway)([^[:alnum:]_]|$)' \
      || true
  fi
}

if [[ "$MODE" == "staged" && "${COOLDIS_ALLOW_PRODUCT_TERMS:-0}" != "1" ]]; then
  product_hits="$(product_term_hits)"

  # During a merge, only lines added relative to HEAD and every MERGE_HEAD
  # parent are new; lines carried verbatim by any parent are already landed.
  merge_head_path="$(git rev-parse --git-path MERGE_HEAD)"
  if [[ -f "$merge_head_path" && -n "$product_hits" ]]; then
    product_hit_lines=()
    while IFS= read -r hit; do
      [[ -n "$hit" ]] && product_hit_lines+=("$hit")
    done <<<"$product_hits"

    while IFS= read -r merge_parent || [[ -n "$merge_parent" ]]; do
      merge_parent="${merge_parent#"${merge_parent%%[![:space:]]*}"}"
      merge_parent="${merge_parent%"${merge_parent##*[![:space:]]}"}"
      [[ -z "$merge_parent" ]] && continue
      ((${#product_hit_lines[@]} == 0)) && break
      if ! git rev-parse --verify --quiet "${merge_parent}^{commit}" >/dev/null; then
        die "invalid merge parent in MERGE_HEAD"
      fi
      parent_hits="$(product_term_hits "$merge_parent")"
      common_product_hit_lines=()

      for hit in "${product_hit_lines[@]}"; do
        normalized_hit="${hit#+}"
        parent_has_hit=0
        while IFS= read -r parent_hit; do
          if [[ -n "$parent_hit" && "${parent_hit#+}" == "$normalized_hit" ]]; then
            parent_has_hit=1
            break
          fi
        done <<<"$parent_hits"

        if ((parent_has_hit)); then
          common_product_hit_lines+=("$hit")
        fi
      done

      if ((${#common_product_hit_lines[@]} > 0)); then
        product_hit_lines=("${common_product_hit_lines[@]}")
      else
        product_hit_lines=()
      fi
    done <"$merge_head_path"

    if ((${#product_hit_lines[@]} > 0)); then
      product_hits="$(printf '%s\n' "${product_hit_lines[@]}")"
    else
      product_hits=""
    fi
  fi

  if [[ -n "$product_hits" ]]; then
    printf 'Staged runtime code appears to add product-shaped terms.\n' >&2
    printf 'Keep product logic out of Cooldis, or set COOLDIS_ALLOW_PRODUCT_TERMS=1 for an intentional exception.\n' >&2
    printf '%s\n' "$product_hits" >&2
    exit 1
  fi
fi

# Public terminology guard: swept docs should use stable kernel contract terms
# from docs/kernel-invariants.md. Extend this list as public docs are cleaned.
swept_docs=(
  README.md
  docs/abi.md
  docs/agent-manifest-ontology.md
  docs/index.md
  docs/roadmap.md
)

if [[ "$MODE" == "staged" ]]; then
  # Only docs touched by the staged change can reintroduce banned words;
  # docs untouched by this commit are checked by tracked mode instead.
  lexicon_hits="$(
    for path in "${swept_docs[@]}"; do
      if git diff --cached --quiet -- "$path" 2>/dev/null; then
        continue
      fi
      git show ":$path" 2>/dev/null \
        | grep -n -i -E '([^[:alnum:]_]|^)(observer|capsule)([^[:alnum:]_]|$)|context strateg|lossless' \
        | sed "s|^|$path:|" \
        || true
    done
  )"
else
  lexicon_hits="$(
    for path in "${swept_docs[@]}"; do
      if git ls-files --error-unmatch "$path" >/dev/null 2>&1; then
        grep -n -H -i -E '([^[:alnum:]_]|^)(observer|capsule)([^[:alnum:]_]|$)|context strateg|lossless' \
          "$path" 2>/dev/null \
          || true
      fi
    done
  )"
fi

if [[ -n "$lexicon_hits" ]]; then
  printf 'Swept docs reintroduce deprecated terminology (see docs/kernel-invariants.md):\n' >&2
  printf '%s\n' "$lexicon_hits" >&2
  exit 1
fi

# PII guard: committed content must not carry local identity anchors. Patterns
# live outside the repo at ${COOLDIS_PII_TERMS:-$HOME/.config/cooldis/pii-terms}
# as POSIX extended regexes, one per line. Blank lines and lines beginning
# with a hash are ignored. The list is deliberately untracked so committing
# the guard mechanism does not itself commit private identity data.
default_pii_terms_file=""
if [[ -n "${HOME:-}" ]]; then
  default_pii_terms_file="$HOME/.config/cooldis/pii-terms"
fi

pii_terms_file="${COOLDIS_PII_TERMS:-$default_pii_terms_file}"
pii_patterns=()

valid_pii_pattern() {
  local pattern="$1"
  local status=0

  grep -E -i -q -e "$pattern" /dev/null >/dev/null 2>&1 || status=$?
  [[ "$status" -lt 2 ]]
}

append_pii_grep_hits() {
  local output
  local status
  local hit

  if output="$(git grep "$@" 2>/dev/null)"; then
    :
  else
    status=$?
    if ((status == 1)); then
      output=""
    else
      die "PII guard git grep failed"
    fi
  fi

  if [[ -n "$output" ]]; then
    while IFS= read -r hit; do
      [[ -n "$hit" ]] && pii_hits+=("$hit")
    done <<<"$output"
  fi
}

path_matches_pii_pattern() {
  local pattern="$1"
  local path="$2"
  local status=0

  grep -q -i -E -e "$pattern" <<<"$path" >/dev/null 2>&1 || status=$?
  if ((status > 1)); then
    die "PII guard path grep failed"
  fi

  ((status == 0))
}

if [[ -n "$pii_terms_file" && -f "$pii_terms_file" ]]; then
  pii_terms_line=0
  while IFS= read -r pattern || [[ -n "$pattern" ]]; do
    ((pii_terms_line += 1))
    [[ "$pattern" =~ ^[[:space:]]*$ ]] && continue
    [[ "$pattern" =~ ^[[:space:]]*# ]] && continue
    if ! valid_pii_pattern "$pattern"; then
      die "invalid regex in local PII terms file at line $pii_terms_line"
    fi
    pii_patterns+=("$pattern")
  done < "$pii_terms_file"
fi

pii_hits=()
pii_pathspecs=()

if ((${#pii_patterns[@]} > 0)); then
  if [[ "$MODE" == "staged" ]]; then
    if ((${#paths[@]} > 0)); then
      for path in "${paths[@]}"; do
        pii_pathspecs+=(":(literal)$path")
      done

      for pattern in "${pii_patterns[@]}"; do
        append_pii_grep_hits --cached -I -n -i -E -e "$pattern" -- "${pii_pathspecs[@]}"

        for path in "${paths[@]}"; do
          if path_matches_pii_pattern "$pattern" "$path"; then
            pii_hits+=("$path:0:$path")
          fi
        done
      done
    fi
  else
    for pattern in "${pii_patterns[@]}"; do
      append_pii_grep_hits -I -n -i -E -e "$pattern" --
      for path in "${paths[@]}"; do
        if path_matches_pii_pattern "$pattern" "$path"; then
          pii_hits+=("$path:0:$path")
        fi
      done
    done
  fi
fi

if ((${#pii_hits[@]} > 0)); then
  printf 'Committed content matches local PII patterns:\n' >&2
  printf '%s\n' "${pii_hits[@]}" | awk '!seen[$0]++' >&2
  exit 1
fi

printf 'Cooldis guard rails passed (%s).\n' "$MODE"
