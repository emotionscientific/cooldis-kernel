# Declarative Agents And Imperative Agent Apps

Verlet is built around declarative agents.

An imperative agent is usually a program: instantiate a model, call tools,
thread state through application code, add retries, add secrets, add logs, add
policy, deploy a service, and keep expanding the app whenever the agent needs a
new power.

A declarative agent starts from a preset: describe the behavior, proposed tools
and resources, model profiles, context strategy, approvals, and placement needs.
The runtime inspects and expands that preset, then records the attachments that
actually govern the running thread.

Define the agent, not the app around it.

## What A Declaration Contains

A Verlet agent declaration can include:

- identity and instructions;
- model profiles;
- tools and capability bindings;
- resources and knowledge;
- context strategy;
- sandbox and placement requirements;
- triggers and channels;
- couplings, approval gates, and policies;
- external connections and secrets;
- events, receipts, versioning, rollback, and audit records.

This declared shape is the packageable input. The thread's recorded binding
history becomes its active toolset: the thing Verlet can inspect, run, resume,
detach, rebind, and audit.

## Why Declaration Matters

Application code hides too much. A declaration can be planned, diffed, approved,
tested, installed, attached, detached, resumed, and moved between local and
managed placement.

Without a declaration, each serious agent becomes a bespoke backend. With a
declaration, the agent itself becomes the deployable unit.

That is the core Verlet product bet:

```text
declarative agents
-> packageable agents and tools
-> governed local and cloud execution
```
