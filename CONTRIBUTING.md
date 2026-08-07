# Contributing to Verlet

Verlet is an experimental agent runtime. This repository is not accepting
public contributions yet.

The code is available for inspection, local use, and evaluation, but the
maintainers are not reviewing unsolicited pull requests, feature patches, or
drive-by documentation changes at this stage. Public pull requests may be closed
without review until the project opens a contribution process.

## Current Policy

- Do not open unsolicited pull requests.
- Do not send generated patch sets or automated bot changes without prior
  maintainer approval.
- Do not include secrets, private provider credentials, customer data, or local
  machine paths in issues, fixtures, logs, or screenshots.
- Report security issues privately using the process in `SECURITY.md`.

## Issues And Feedback

If public issues are enabled, use them for concise bug reports or documentation
corrections only. Feature requests, architecture proposals, and implementation
patches are maintainer-led for now.

AI-generated issue text should still be checked by a human. Maintainers may
close low-signal generated reports, broad proposals, or requests that do not
match the current release path.

## Development Setup

For local evaluation, Verlet is a Rust workspace. From the repository root:

```sh
cargo test --workspace --all-targets --locked
```

For the repository's fuller local checks, use:

```sh
scripts/check-pre-push.sh
```

Nix users can get the pinned toolchain plus `just`, `bun`, and `sccache` with
`nix develop` (or `direnv allow`, which uses the checked-in `.envrc`). The
toolchain comes from `rust-toolchain.toml`, the same file rustup reads, so both
setups build the same thing. `nix build` produces the `verlet`,
`verlet-mcp-server`, and `verlet-acp-agent` binaries. The flake is a convenience
for local work; `.github/workflows/verify.yml` remains the gate.

## Contribution License

The repository is licensed under the Apache License, Version 2.0. A future
public contribution process may add contribution-license terms before public
patches are accepted.
