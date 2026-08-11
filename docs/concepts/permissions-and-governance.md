# Permissions And Governance

Verlet gives agents power through explicit attachment and witnessed policy.

An operation is unavailable until it is attached. An agent can be published
without being trusted in every workspace. A secret can be allowed for one
attachment without becoming globally available.

That separation is the governance model: visibility, publication, permission,
secret access, resource access, and execution placement are different decisions.
Privileged actions produce events and receipts.

This is how Verlet aims to provide managed-agent convenience without vendor
lock-in: the managed runtime can operate agents, while the agent definition,
tool contracts, attachments, and receipts stay inspectable and portable.

## Permission Surfaces

Verlet permissioning centers on:

- operation attachment;
- package-declared capabilities;
- secret bindings;
- resource bindings;
- placement policy;
- operation ABI contracts;
- approval gates;
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
