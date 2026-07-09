# Public API Surface

Any surface a human, coding agent, model agent, or external client can invoke is
public enough to document.

## Surfaces

Public surfaces include:

- CLI commands;
- daemon config and service commands;
- RPC methods and app-server wire contracts;
- thread, agent, and operation manifests;
- ABI operation contracts;
- model-visible tools;
- virtual-bash commands;
- imported MCP tools once adopted into Cooldis.

## Coverage Rule

Each public surface needs:

1. a canonical contract doc;
2. a help or man-page projection;
3. a coverage entry that names current gaps.

The canonical CLI reference is [Cooldis CLI](../cli.md). The internal coverage
ledger is `docs/public-api-coverage.md`. This public docs site will graduate
stable entries from that ledger into user-facing reference pages.

For the currently supported communication surfaces, see
[Protocol Surfaces](protocol-surfaces.md).

## Projection Law

The ABI operation contract is the source of truth. CLI, MCP, HTTP, process, and
model-tool surfaces are projections.

```text
Projection may change syntax.
Projection may not change authority, required inputs, durable effects, or output
semantics.
```
