#!/usr/bin/env bash

read_verlet_workspace_version() {
  local cargo_toml="$1"

  sed -n \
    '/^\[workspace\.package\][[:space:]]*$/,/^\[/ {
      s/^[[:space:]]*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p
    }' \
    "$cargo_toml" \
    | head -n 1
}
