# Release

This repository is intended to start from a clean public history. Do the
documentation review before the first public commit, then cut releases from tags.

## Tags

Kernel release tags use the runtime version:

```sh
v0.1.0
v0.1.0-rc.1
```

The tag must match `crates/cooldis-kernel/Cargo.toml`. The release workflow runs
`scripts/check-release-tag.sh` before packaging tagged releases.

## Binary Targets

GitHub Releases publish one archive per target:

```text
cooldis-<version>-x86_64-unknown-linux-gnu.tar.gz
cooldis-<version>-aarch64-unknown-linux-gnu.tar.gz
cooldis-<version>-x86_64-apple-darwin.tar.gz
cooldis-<version>-aarch64-apple-darwin.tar.gz
```

Each archive contains the public process entrypoints:

```text
cooldis
cooldis-acp-agent
cooldis-mcp-server
```

The release also includes `latest.json` and `install.sh`. The installer selects
the correct archive for the current machine, verifies the SHA-256 checksum, and
links the binaries into `~/.local/bin`.

Install the latest stable release:

```sh
curl -fsSL https://github.com/emotionscientific/cooldis-kernel/releases/latest/download/install.sh | sh
```

Install a release candidate directly:

```sh
curl -fsSL https://github.com/emotionscientific/cooldis-kernel/releases/download/v0.1.0-rc.N/install.sh \
  | sh -s -- --version 0.1.0-rc.N
```

After installation, the normal local entrypoint is:

```sh
cooldis console
```

## Local Packaging

Build the host target:

```sh
scripts/package-release-binary.sh --out-dir dist
```

Packaging builds the bundled Svelte console with Bun and includes
`share/cooldis/console/*` in the release archive. Use
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
archive="$(find dist -maxdepth 1 -name 'cooldis-*.tar.gz' | head -n 1)"
scripts/smoke-release-archive.sh "$archive"
scripts/smoke-install.sh "$archive"
```

The package smoke verifies the canonical CLI help surface, including
`cooldis commands`, `cooldis chat --help`, `cooldis auth --help`,
`cooldis tool manual --help`, and `cooldis debug rpc --help`.

## Publishing

After the documentation pass and first public commit:

```sh
scripts/check-release-tag.sh v0.1.0
git tag -a v0.1.0 -m "v0.1.0"
git push origin main --follow-tags
```

The `Release` workflow publishes artifacts only for tags matching `v*`.
