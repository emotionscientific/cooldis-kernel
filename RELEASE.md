# Release

This repository is intended to start from a clean public history. Do the
documentation review before the first public commit, then cut releases from tags.

## Tags

Kernel release tags use the runtime version:

```sh
v0.1.0
v0.1.0-rc.1
```

The tag must match `crates/verlet-kernel/Cargo.toml`. The release workflow runs
`scripts/check-release-tag.sh` before packaging tagged releases.

## Binary Targets

GitHub Releases publish one archive per target:

```text
verlet-<version>-x86_64-unknown-linux-gnu.tar.gz
verlet-<version>-aarch64-unknown-linux-gnu.tar.gz
verlet-<version>-x86_64-apple-darwin.tar.gz
verlet-<version>-aarch64-apple-darwin.tar.gz
```

Each archive contains the public process entrypoints:

```text
verlet
verlet-acp-agent
verlet-mcp-server
```

The release also includes `latest.json` and `install.sh`. The installer selects
the correct archive for the current machine, verifies the SHA-256 checksum, and
links the binaries into `~/.local/bin`.

Install the latest stable release:

```sh
curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/latest/download/install.sh | sh
```

Install a release candidate directly:

```sh
curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/download/v0.1.0-rc.N/install.sh \
  | sh -s -- --version 0.1.0-rc.N
```

After installation, the normal local entrypoint is:

```sh
verlet console
```

The foreground server entrypoint is `verlet serve`. Releases do not include a
`verlet daemon run` compatibility alias. `verlet daemon` contains only config
validation and service-file management.

## Local Packaging

Build the host target:

```sh
scripts/package-release-binary.sh --out-dir dist
```

Packaging builds the bundled Svelte console with Bun and includes
`share/verlet/console/*` in the release archive. Use
`scripts/build-console-assets.sh` to rebuild only the UI assets during local
iteration.

Build a specific target:

```sh
scripts/package-release-binary.sh \
  --out-dir dist \
  --target aarch64-apple-darwin
```

Smoke a package:

```sh
archive="$(find dist -maxdepth 1 -name 'verlet-*.tar.gz' | head -n 1)"
scripts/smoke-release-archive.sh "$archive"
scripts/smoke-install.sh "$archive"
```

The package smoke verifies the canonical CLI help surface, including
`verlet commands`, `verlet chat --help`, `verlet auth --help`,
`verlet serve --help`, `verlet tool manual --help`, and
`verlet debug rpc --help`.

## Async Publishing

Maintainers can run a local host-target package smoke before triggering the
remote release matrix:

```sh
scripts/release-async.sh v0.1.0-rc.N
```

This validates the tag, builds and smokes the local release archive, creates the
annotated tag at `HEAD`, pushes the current branch and tag, prints the GitHub
Actions release URL, and exits without watching the remote build. GitHub
Actions remains the publisher of record for the supported target matrix and the
release assets consumed by `install.sh`.

Use the full deterministic V1 gate before pushing the tag when the release needs
the broader local test lane:

```sh
scripts/release-async.sh v0.1.0-rc.N --full-gate
```

The default helper path is intentionally shorter than the full gate so local
development is not blocked on the hosted release matrix.

## Manual Publishing

After the documentation pass and first public commit:

```sh
scripts/check-release-tag.sh v0.1.0
git tag -a v0.1.0 -m "v0.1.0"
git push origin main --follow-tags
```

The `Release` workflow publishes artifacts only for tags matching `v*`.
