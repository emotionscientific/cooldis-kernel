#!/usr/bin/env bash
# Lint docs/threat-model.md entry discipline: unique sequential TM ids per
# family, the required fields on every entry, and a known status vocabulary.
# Threat ids are append-only; a numbering gap means an entry was deleted.
set -euo pipefail

cd "$(dirname "$0")/.."
doc="docs/threat-model.md"
[[ -f "$doc" ]] || { echo "threat-model-lint: $doc not found" >&2; exit 1; }

problems=()

ids=$(grep -oE '^## TM-[A-Z]+-[0-9]{3}' "$doc" | sed 's/^## //')

dupes=$(sort <<<"$ids" | uniq -d)
[[ -z "$dupes" ]] || problems+=("duplicate threat ids: $(tr '\n' ' ' <<<"$dupes")")

for family in $(sed -E 's/^TM-([A-Z]+)-[0-9]{3}$/\1/' <<<"$ids" | sort -u); do
  expected=1
  for number in $(grep -E "^TM-$family-" <<<"$ids" | grep -oE '[0-9]{3}$'); do
    if ((10#$number != expected)); then
      printf -v want '%03d' "$expected"
      problems+=("family TM-$family is not sequential: expected TM-$family-$want, found TM-$family-$number (ids are append-only)")
      expected=$((10#$number))
    fi
    expected=$((expected + 1))
  done
done

field_report=$(awk '
  function flush() {
    if (id == "") { return }
    split("Status Severity Threat Affected_surface Mitigation Deterministic_guard", wanted, " ")
    for (i in wanted) {
      name = wanted[i]; gsub(/_/, " ", name)
      if (!(name in seen)) { print id " is missing its \"" name "\" field" }
    }
    delete seen
  }
  /^## TM-/ { flush(); id = $2 }
  /^## / && $2 !~ /^TM-/ { flush(); id = "" }
  id != "" && /^- [A-Za-z ]+:/ {
    name = $0; sub(/^- /, "", name); sub(/:.*/, "", name); seen[name] = 1
  }
  END { flush() }
' "$doc")
if [[ -n "$field_report" ]]; then
  while IFS= read -r line; do problems+=("$line"); done <<<"$field_report"
fi

bad_status=$(grep -E '^- Status:' "$doc" | grep -vE '^- Status: (OPEN|MITIGATED)$' || true)
[[ -z "$bad_status" ]] || problems+=("unknown status value(s): $(tr '\n' ' ' <<<"$bad_status")")

if ((${#problems[@]} > 0)); then
  printf 'threat-model-lint: %s\n' "${problems[@]}" >&2
  exit 1
fi
echo "threat-model lint passed ($(wc -l <<<"$ids" | tr -d ' ') entries)"
