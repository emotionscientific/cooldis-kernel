# Permissions And Governance

Cooldis gives agents power through explicit grants.

An agent can see a tool without being allowed to use it. An agent can be
published without being trusted in every workspace. A secret can be bound to one
agent without becoming globally available.

That separation is the governance model: visibility, publication, permission,
secret access, resource access, and execution placement are different decisions.
Privileged actions produce events and receipts.

This is how Cooldis aims to provide managed-agent convenience without vendor
lock-in: the managed runtime can operate agents, while the agent definition,
tool contracts, grants, and receipts stay inspectable and portable.

## Permission Surfaces

Cooldis permissioning centers on:

- capability grants;
- secret bindings;
- resource bindings;
- placement policy;
- operation ABI contracts;
- approval hooks;
- event streams;
- receipts for durable effects;
- revoke, rollback, and quarantine paths.

## Enterprise Use

The enterprise promise is:

```text
Business teams author agents.
IT keeps authority.
```

Teams can declare agents against approved primitives. Platform and security
teams control tools, connectors, secrets, network policy, audit, approvals,
version pinning, rollback, and marketplace visibility.

The result is a managed platform that business teams can actually use without
asking platform teams to accept opaque agent behavior.
