# Kernel Invariants

This page names the public runtime rules that other Verlet docs rely on. It is
not a private design canon; it is the small vocabulary an OSS reader needs to
understand what the kernel promises.

## Source Of Truth

Durable runtime truth is stored as typed records: manifests, operation records,
bindings, events, receipts, and explicit configuration. In-memory state can make
execution fast, but it does not become authority until the runtime records it.

## Faithful Surfaces

CLI commands, RPC methods, MCP tools, ACP sessions, virtual-bash commands, HTTP
routes, and model-visible tools are projections over the same contracts. A
surface can change syntax, framing, or transport. It cannot invent authority,
hide durable mutation, erase required inputs, or change output semantics.

## Explicit Authority

Agents, tools, resources, secrets, filesystem mounts, network origins, and
thread controls must be declared and bound before use. Missing authority fails
closed with a repairable error.

## Content Addressing

Published operations and agent manifests are pinned by durable identity. A
running thread should be able to explain which manifest, operation artifact, and
binding record produced each effect.

## Receipts

When the runtime performs work with durable consequences, it records a receipt
or event that names the contract, the caller, the authority used, and the
outcome. Receipts are for audit, resume, and debugging; they are not hidden log
lines.

## Provider Neutrality

Provider adapters are replaceable. Provider-specific examples in public docs
must use generic placeholders unless the provider is part of a documented public
integration.

## Public Terminology

Use stable contract terms in public docs and code: manifest, operation, binding,
grant, resource, secret, thread, event, receipt, projection, controller, and
provider adapter. Historical or private planning terms should not be required to
understand the OSS kernel.
