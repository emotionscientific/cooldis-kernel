# Cooldis

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/emotionscientific/cooldis-kernel)
[![Verify](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/verify.yml/badge.svg)](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/verify.yml)
[![Release](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/release.yml/badge.svg)](https://github.com/emotionscientific/cooldis-kernel/actions/workflows/release.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Status: experimental](https://img.shields.io/badge/status-experimental-yellow.svg)](README.md)

> Status: Cooldis is experimental. APIs, behavior, and release packaging may
> change before a stable public release.

## What Is Cooldis

Cooldis is an open serverless agent platform built on the Cool Declarative
Intelligence Substrate.

At the kernel level, Cooldis is a Rust runtime for declared agent workloads. An
agent declaration describes the model profile, tools, resources, secrets,
permissions, context policy, and runtime defaults before execution. Cooldis
turns that declaration into governed runtime work: capability grants, tool
visibility, provider access, operation dispatch, event records, cancellation,
resume, and audit receipts.

Cooldis is not an agent, a graph framework, a provider SDK, or a product app.
Product systems configure and call Cooldis. The kernel owns the runtime
contracts those systems depend on.

## Repository Scope

This repository contains the standalone Cooldis kernel workspace:

- Runtime primitives: tenant hosts, thread lifecycle, history, events,
  cancellation, resume, and supervisor routing.
- Agent and operation contracts: manifests, tool publication, operation ABI,
  grants, command projections, and Wasm operation support.
- Runtime surfaces: CLI, daemon, app-server RPC, MCP, ACP, provider adapters,
  virtual bash, VFS, and process handles.
- Release and verification tooling for tagged binary releases.

The product layer is outside this repository. Auth products, billing,
dashboards, invite flows, deployment orchestration, and app-specific ledgers
belong in product repos or adapters.

For the detailed repository map, see [docs/repository-map.md](docs/repository-map.md).

## Related Repositories

- [cooldis-console](https://github.com/emotionscientific/cooldis-console):
  desktop/web console and product-side client for the Cooldis app-server
  protocol.

When checked out as sibling repositories, the console usually lives at
`../cooldis-console`.

## Current Status

The V1 work is focused on runtime primitives:

- agent manifest planning, publishing, listing, showing, and local running;
- operation publication and ABI-backed invocation;
- local runtime execution through provider, virtual bash, and Wasm paths;
- daemon, RPC, MCP, ACP, and CLI projections over the same kernel contracts;
- release packaging for `cooldis`, `cooldis-acp-agent`, and
  `cooldis-mcp-server`.

Managed cloud placement, a public package registry, marketplace flows, and
stateful product harnesses are future direction. See
[docs/roadmap.md](docs/roadmap.md).

## Documentation

- [Docs index](docs/README.md): documentation map.
- [Overview](docs/index.md): product category and current status.
- [Getting started](docs/getting-started.md): local run and inspection path.
- [CLI](docs/cli.md): canonical commands and help model.
- [Runtime primitives](docs/developers/runtime-primitives.md): kernel-owned
  surfaces and boundaries.
- [ABI](docs/abi.md): operation boundary and host/guest contract.
- [Agent CLI](docs/agent-cli.md): manifest authoring and local agent commands.
- [Agent manifest ontology](docs/agent-manifest-ontology.md): manifest shape
  and deferred fields.
- [Command contracts](docs/command-contracts.md): command/manual projection
  rules.
- [Chat console](docs/chat.md): local terminal console over the app-server RPC
  boundary.
- [Provider adapters](docs/provider-adapters.md): provider runtime boundary.
- [Daemon and RPC](docs/app-server.md): app-server control plane.
- [Testing guidelines](docs/testing-guidelines.md): runtime test expectations.
- [Release guide](RELEASE.md): tags, targets, package smoke tests, and
  installer artifacts.

## Running Locally

Use the repository scripts as the source of truth:

```sh
scripts/verify.sh
cargo test --workspace --all-targets --locked
```

For the first local path, see [docs/getting-started.md](docs/getting-started.md).
For release packaging, tag checks, and async release publishing, see
[RELEASE.md](RELEASE.md).

## Contributing

Cooldis is not accepting public contributions yet. Please read
[CONTRIBUTING.md](CONTRIBUTING.md) before opening issues, and do not open
unsolicited pull requests. Report suspected vulnerabilities through the private
process in [SECURITY.md](SECURITY.md).

## License

Cooldis is licensed under the [Apache License, Version 2.0](LICENSE).
