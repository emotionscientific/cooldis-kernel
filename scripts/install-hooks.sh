#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$ROOT"

if [[ "${1:-}" == "--unset" ]]; then
  git config --unset core.hooksPath || true
  printf 'Cleared local core.hooksPath.\n'
  exit 0
fi

git config core.hooksPath .githooks
printf 'Installed Verlet git hooks via core.hooksPath=.githooks\n'
