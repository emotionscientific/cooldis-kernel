# Local To Managed Deployment

Verlet is designed for one deployment story: define locally, run locally,
publish to managed infrastructure when the agent is ready.

The product promise is:

```text
managed agent infrastructure without vendor lock-in
```

The managed platform should be the easiest production path. The open runtime and
portable agent definitions keep it from becoming the only path.

## Local Runtime

The local runtime is the Rust substrate: supervisor, threads, history, events,
operation ABI, tool publishing, provider adapters, VFS, virtual bash, daemon, and
app-server/RPC surfaces.

This layer creates trust. It is inspectable, local-first, and portable.

## Managed Infrastructure

Managed infrastructure is the production path: publish agents, bind resources and
secrets, schedule or trigger work, observe events, enforce policy, and manage
cost and tenancy.

This layer turns the local substrate into a serverless agent platform.

The managed layer should feel smooth: local definition, managed deployment,
observability, rollback, and production controls. The agent definition should
remain yours.

## Agent And Tool Packages

Packages make the platform useful immediately. A package is an installable,
versioned agent definition or capability bundle with declared requirements.

Examples:

- research agent;
- release verifier;
- documentation coverage agent;
- browser QA agent;
- filesystem inspector tool pack;
- web search Capsule.

The long-term platform supports both public and private marketplaces.

## Deployment Loop

The core loop is:

```text
define the agent
-> inspect required powers
-> run locally
-> publish to managed runtime
-> observe, revoke, or roll back
```

That loop is the product. The runtime primitives exist to make the loop honest.
