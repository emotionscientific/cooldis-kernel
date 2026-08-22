# Verlet Docs

This directory is the public documentation surface for the Verlet kernel repo.
It is organized around the contracts a user, client, or coding agent can depend
on: manifests, operation boundaries, daemon/RPC surfaces, protocol adapters,
provider adapters, and release gates.

## Start Here

- [Agents in Version Control](https://emotionscientific.github.io/verlet-kernel/primer/agents-in-version-control.html):
  a plain primer on what an agent runtime has to get right and how Verlet
  does it, with a copyable prompt per section for asking a coding agent in
  this checkout. Also as [PDF](primer/agents-in-version-control.pdf);
  source HTML at [primer/](primer/).
- [Verlet overview](index.md): product category, current status, and next
  reading path.
- [Getting Started](getting-started.md): install, run, and inspect the local
  runtime.
- [Verlet CLI](cli.md): canonical command surface and help model.
- [Repository Map](repository-map.md): where code, tests, docs, scripts, and
  runtime contracts live.
- [Kernel Invariants](kernel-invariants.md): public vocabulary for the runtime
  rules that other docs rely on.
- [Frozen Format IDs](format-ids.md): durable identifiers that intentionally
  keep their pre-rename namespace.
- [Roadmap](roadmap.md): V1 runtime-primitives release and V2 direction.

## Core Concepts

- [Declarative Agents](concepts/declarative-agents.md)
- [Local To Managed Deployment](concepts/local-to-managed.md)
- [Permissions And Governance](concepts/permissions-and-governance.md)

## Runtime Surfaces

- [ABI: Verlet Operation Boundary](abi.md)
- [Frozen Format IDs](format-ids.md)
- [Verlet CLI](cli.md)
- [Command Contracts](command-contracts.md)
- [Verlet Daemon](daemon.md)
- [Verlet IO](io.md)
- [Chat Console](chat.md)
- [Provider Setup](provider-setup.md)
- [RPC Control Plane](app-server.md)
- [Verlet MCP Server](mcp-server.md)
- [Provider Adapter Surface](provider-adapters.md)
- [Metadata And Provider Auth Storage](provider-storage.md)

## Agent And Operation Authoring

- [Verlet Agent CLI](agent-cli.md)
- [Agent Manifest Ontology](agent-manifest-ontology.md)
- [ACP Agent](acp-agent.md)
- [ACP Thread Projection](acp-thread-projection.md)
- [Tool Publish Storage](publish-storage.md)
- [Standard Operations](standard-operations.md)
- [Rust Wasm Operation Dev Kit](wasm-operation-dev-kit.md)
- [OpenAPI To ABI Operation Adapter](openapi-adapter.md)
- [Researcher Example Agent](../examples/agents/researcher/)
- [Verlet Agent Maker Skill](../skills/verlet-agent-maker/SKILL.md)
- [Verlet Tool Maker Skill](../skills/verlet-tool-maker/SKILL.md)

## Maintainer References

- [Runtime Primitives](developers/runtime-primitives.md)
- [Protocol Surfaces](developers/protocol-surfaces.md)
- [Public API Surface](developers/public-api-surface.md)
- [Documentation System](developers/documentation-system.md)
- [Public API Coverage](public-api-coverage.md)
- [Threat Model](threat-model.md)
- [Testing Guidelines](testing-guidelines.md)
- [V1 Release Candidate Gate](v1-release-candidate.md)
- [ADR 0001: Verlet Stream Schema V1](adr/0001-stream-schema-v1.md)

Planning notes that are not ready for a public OSS tree should stay outside this
repository until they are rewritten as stable contracts or published design
records.
