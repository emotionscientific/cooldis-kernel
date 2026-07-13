---
name: cooldis-agent-maker
description: Author, validate, publish, and launch a Cooldis agent manifest — the versioned declaration of what an agent is allowed to be, written in TOML and published through the local agent registry.
---

# Cooldis Agent Maker

Use this skill to turn an intent ("an agent that fetches pages and extracts
fields") into a published, runnable agent. The deliverable is a V1 agent
manifest: a TOML declaration that composes published operations, model
profiles, resources, and policies, then lowers to an immutable registry
record with a receipt.

The manifest declares the universe; runtime policy selects within it. The
thread records what actually happened. Never put live state, secrets, or
prose behavior promises in a manifest — if it is not a declaration the
schema knows, the parse fails closed.

## Mental model

```text
intent
-> survey what exists        (cooldis tool list / cooldis agent list / cooldis man)
-> draft manifest TOML       (this skill's reference below)
-> cooldis agent plan        (offline compile: refs, hashes, counts)
-> fix until plan is clean
-> cooldis agent publish     (fail-closed resolve; immutable record + alias)
-> start a thread on it      (console picker, or cooldis agent run)
```

Every step is receipted. `plan` writes nothing; `publish` is the gate.

## Workflow

```sh
cooldis agent init my-agent            # scaffold my-agent.cooldis.agent.toml
$EDITOR my-agent.cooldis.agent.toml
cooldis agent plan my-agent.cooldis.agent.toml      # offline; lists verified/unverified refs
cooldis agent publish my-agent.cooldis.agent.toml   # verifies op refs; prints ref, hashes, alias
cooldis tool list                                  # read active operation hashes
cooldis agent list                                  # confirm the record
cooldis agent show agent://my-agent@0.1.0           # full record JSON
cooldis agent run agent://my-agent@0.1.0 --input "hello"   # ephemeral one-shot
```

All registry commands take `--registry-root <path>`; the default agent
registry is `.cooldis/agents` under the working directory. Agent `plan` and
`publish` also take `--operations-registry-root <path>`; the default
operations registry is `.cooldis/operations`. A daemon-backed console picks
up published agents in its registry roots and offers them in the thread
picker.

`plan` is offline-allowed: unresolved refs print as `unresolved-offline_ref`
and missing operation registries report `op://` rows as `[unverified-offline]`;
the command still succeeds. `publish` fails closed on ANY unresolved operation,
resource, or alias ref, and on any `op://` row that cannot be verified against
the operations registry — a published record never contains a dangling
reference.

## The manifest, section by section

Exactly these top-level sections exist; unknown sections and unknown keys
anywhere fail closed with an error naming the offender.

### `[agent]` — identity (required)

```toml
[agent]
name = "my-agent"            # required; lowercase kebab/snake record name
version = "0.1.0"            # semantic version
description = "One line of responsibility."
kind = "cooldis.agent-manifest"
schema_version = 1
# optional: namespace, display_name, labels (string map),
#           publisher { id, display_name }
```

### `[[model_profiles]]` — model policy

Ordered; the first profile is the default. A thread start passing
`model`/`modelProvider` must select exactly one declared profile — selectors
that match zero or several profiles are rejected with the declared list, so
declare distinct profiles rather than relying on start-time params.

```toml
[[model_profiles]]
id = "default"
provider_ref = "provider://local/default"   # provider:// catalog ref
model_ref = "model://local/default"         # model:// catalog ref

[model_profiles.params]             # all optional
max_tokens = 4096
temperature = 0.2
reasoning_effort = "medium"

# optional per-profile: credentials = { ref = "credential://..." },
# retry = { max_attempts = 3, backoff_ms = 500 },
# [[model_profiles.fallbacks]] provider_ref/model_ref pairs
```

Secrets never live in a manifest; `credentials.ref` is a reference resolved
inside the runtime boundary.

### `[[tools]]` — the authority surface

Three row types. Every row binds a published, content-addressed contract;
nothing mutable backs a tool row. Effect grants ride on the row that uses
them — there is no manifest-global grant pool.

**`bash_tool`** — the operation becomes a command inside virtual bash:

```toml
[[tools]]
type = "bash_tool"
id = "http_fetch"
command = "http_fetch"
operation_ref = "op://http-fetch@sha256:<hash>"
grants = ["net.http:GET:https://example.com"]
```

**`direct_tool`** — the operation becomes a structured model-visible tool:

```toml
[[tools]]
type = "direct_tool"
id = "json_query"
tool_name = "json_query"
operation_ref = "op://json-query@sha256:<hash>"
```

`op://` refs come in two forms: `op://<record>@sha256:<hash>` binds the whole
record (grants must cover every operation it declares);
`op://<record>/<operation>@sha256:<hash>` selects one operation. Read the
hashes with `cooldis tool list --registry-root <operations-root>`; it prints
the full active artifact hash from the operation registry. The raw
`active_artifact_hash` field in `records/*.json` is the same value. Never
invent hashes.

**`protocol_tool_import`** — a live MCP universe mounted through the search
surface:

```toml
[[tools]]
type = "protocol_tool_import"
id = "search"
protocol = "mcp"
server_ref = "mcp://search"        # name-only: names a configured source
include_tools = ["search"]  # optional narrowing
grants = []
# expose = ["direct_tool"] requires pin = "mcptool://<server>/<tool>@sha256:<hash>"
```

`server_ref` names placement and is never content-addressed; pins
(`mcptool://...@sha256:`) are the only passage from a live universe to a
direct row. With `expose` empty the universe is searchable in-context only.

**Thread control is declared, not ambient.** Spawning and supervising child
threads requires (a) `allow_child_agents = true` in policies and (b) rows
binding the kernel-published `cooldis-threads` operations — `thread_spawn`
(grant `threads.spawn`), `thread_submit`/`thread_cancel` (`threads.control`),
`thread_wait`/`thread_status` (`threads.read`). An agent with no such rows
has no thread powers, full stop.

```toml
[[tools]]
type = "direct_tool"
id = "cooldis-threads.thread_spawn"
tool_name = "thread_spawn"
operation_ref = "op://cooldis-threads/thread_spawn@sha256:<hash>"
grants = ["threads.spawn"]
```

A `threads.spawn`-granting row with `allow_child_agents = false` is rejected
at bind with a teaching error — fix the policy or drop the row. The
`cooldis-threads` record is kernel-published at daemon startup; read its
hash from the registry like any other operation.

### `[[resources]]` — declared read-only artifacts

```toml
[[resources]]
name = "playbook"
kind = "blob"                          # "skill" parses but is deferred
ref = "resource://artifact/sha256:..."
mount = "context"                      # only value in V1
mode = "read"                          # only value in V1
```

Declaring a resource grants nothing by itself: model visibility comes from a
context pipeline source that selects it.

### `[context]` — how model-visible context is assembled (optional)

Omit the whole section for the synthesized default. If present: exactly one
pipeline, id `"default"`, with independent sources — each an assembler ref
(`kernel://` only in V1), optional selector, and a budget share. Source ids
unique; fractional shares sum to ≤ 1; at most one `"rest"`; `pinned = true`
sources sit outside budget arithmetic. See the worked pipeline in
`docs/agent-manifest-ontology.md`.

### `[policies]` — the thread-level authority boundary

```toml
[policies]
network = "declared-origins"   # or "deny" (default)
filesystem = "vfs"             # or "none"; default "vfs"
allow_child_agents = false     # default false; see thread control above

[policies.budgets]             # optional
max_turns = 50
max_tool_calls_per_turn = 8
```

`network = "declared-origins"` means reachable origins are exactly those
declared by tool grants (`net.http:GET:<origin>`); the broker enforces at
request time, fail closed.

### `[runtime]` — thread-start defaults

```toml
[runtime]
default_cwd = "workspace"      # default
streaming = true               # default
# max_tool_rounds = 64          # default when omitted: 8
# max_tool_rounds = "unlimited" # explicit opt-in; other budgets still apply
# optional: turn_timeout_ms, cancellation_grace_ms,
#           compaction = { auto_at_text_bytes = 200000 }

[runtime.overrides]
allow = ["default_cwd"]        # deny-by-default allowlist
```

Override keys a `thread/start` caller may pass: `default_cwd`, `streaming`,
`turn_timeout_ms`, `cancellation_grace_ms`, `max_tool_rounds`,
`compaction.auto_at_text_bytes`.
Anything not allowlisted is fixed by the manifest; a start that tries to
override it is rejected.

## Reading errors

The compiler teaches; trust it over guesswork:

- unknown key/section errors name the offender — fix the spelling, do not
  add sections the schema does not know;
- profile-selection errors list the declared profiles;
- operation-registry errors on `agent publish` mean the `op://` record or hash
  is not in the operations registry — seed it or pass
  `--operations-registry-root <path>`;
- unknown-operation errors mean a two-segment ref selected an operation the
  version record does not declare — use the available operation name or bind
  the whole record;
- grant-coverage errors name the operation and the missing capability —
  add the grant to that row or switch to the two-segment `op://` form;
- reserved-section errors name a deferred V1 scope — that feature does not
  exist yet; design around it rather than emulating it in prose.

## Worked examples

- `examples/agents/researcher/` — three standard ops as bash commands,
  declared-origins network, cwd override allowlisted. The canonical small
  agent; copy its publish.sh pattern for hash substitution.
- A minimal chat agent is just `[agent]` + one `[[model_profiles]]` row —
  no tools, no resources. Useful as a smoke test of the publish loop.

When something the manifest needs does not exist yet (an operation, a pin),
stop and build that first with the `cooldis-tool-maker` skill — a manifest
referencing an unpublished contract cannot publish, by design.
