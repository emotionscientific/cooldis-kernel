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

## Local Packaging

Build the host target:

```sh
scripts/package-release-binary.sh --out-dir dist
```

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

## Publishing

After the documentation pass and first public commit:

```sh
scripts/check-release-tag.sh v0.1.0
git tag -a v0.1.0 -m "v0.1.0"
git push origin main --follow-tags
```

The `Release` workflow publishes artifacts only for tags matching `v*`.
