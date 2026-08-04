#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONSOLE_DIR="$ROOT/apps/console"

if ! command -v bun >/dev/null 2>&1; then
  echo "bun is required to build Verlet console assets" >&2
  exit 1
fi

cd "$CONSOLE_DIR"
bun install --frozen-lockfile
bun run check
bun run build
