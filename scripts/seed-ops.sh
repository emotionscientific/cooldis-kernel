#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REGISTRY_ROOT="${1:-.verlet/operations}"

cd "$ROOT"

for tool in http-fetch file-read json-query; do
  cargo run --locked --bin verlet -- tool publish \
    --package "tools/${tool}" \
    --registry-root "$REGISTRY_ROOT"
done
