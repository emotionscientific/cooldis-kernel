# Declarative Agents And Imperative Agent Apps

Verlet is built around declarative agents.

An imperative agent is usually a program: instantiate a model, call tools,
thread state through application code, add retries, add secrets, add logs, add
policy, deploy a service, and keep expanding the app whenever the agent needs a
new power.

A declarative agent is a definition: describe the behavior, tools, resources,
secrets, model profiles, context strategy, approvals, and placement needs. The
runtime can inspect that definition before it runs and enforce it while it runs.

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

This declared shape becomes the runtime object: the thing Verlet can install,
inspect, attach, run, resume, publish, detach, and audit.

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
