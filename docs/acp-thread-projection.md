# ACP Thread Projection

This note defines how Verlet exposes a manifest-bound thread through the Agent
Client Protocol (ACP). ACP is a compatibility surface for agent clients and
agent-hosting runtimes. It is not the Verlet control plane.

```text
ACP client / host
-> verlet-acp-agent over stdio JSON-RPC
-> Verlet app-server thread/start + turn/start
-> VerletSupervisor / RuntimeHost
-> manifest-bound Verlet thread
```

The app-server remains the canonical control plane. Manifest binding, grants,
operation registry state, provider configuration, thread durability, topology,
placement, and lifecycle records remain Verlet-owned.

See [Verlet RPC Control Plane](app-server.md) for the app-server
contract and [Command Contracts](command-contracts.md) for the existing
projection law.

## Projection Law

```text
ACP may change wire syntax.
ACP may not change authority, durable effects, thread identity, or terminal
semantics.
```

`session/new` starts or binds a Verlet thread. `session/prompt` starts a
Verlet turn. Completion waits for `RuntimeEventKind::Terminal`, not the first
assistant text delta.

ACP should stay much smaller than the app-server API. It can drive a bound
thread, but it cannot define the runtime contract.

## Method Matrix

| ACP method | V1 status | Verlet lowering |
| --- | --- | --- |
| `initialize` | support | negotiate protocol version and advertise only implemented capabilities |
| `session/new` | support | `thread/start` with configured `agentRef`, cwd/runtime overrides, and manifest bind receipts |
| `session/prompt` | support | `turn/start` on the mapped thread; stream runtime events as ACP updates |
| `session/cancel` | support | Verlet turn interrupt/cancel signal; final prompt response uses `stopReason: "cancelled"` |
| `session/set_config_option` | support | supported for session-local model and thought-level selectors |
| `session/load` | defer | only after ACP replay semantics match Verlet durable thread load/resume |
| `session/resume` | defer | only after residency and event replay behavior are specified |
| `session/close` | support | interrupt the active turn if present, then drop the in-memory ACP session handle |
| `session/list` | defer | only after ACP session info maps to Verlet durable thread history without implying false residency |
| `session/delete` | defer | only after ACP history deletion semantics map to Verlet retention policy |
| `session/request_permission` | defer | only after the Verlet permission coupling outlet is designed |
| `fs/*`, terminal, extra directory affordances | defer or reject | only expose after a grant-preserving Verlet mapping exists |
| unknown methods | reject | deterministic JSON-RPC method-not-found error |

## Session Identity

The adapter keeps an in-memory session table:

```text
ACP session id
-> Verlet thread id
-> cwd
-> agentRef / manifest hash
-> active turn state
-> negotiated ACP client capabilities
```

The Verlet thread id is the durable identity. The ACP session id is a protocol
handle for that thread. If a client needs durable reconnection, it must go
through a documented load/resume mapping once that behavior is implemented.

The app-server must persist manifest compile and bind receipts before the first
turn for ACP-created threads, just as it does for native starts.

## Prompt Turns

`session/prompt` converts ACP prompt content into Verlet `TurnInput`:

- text parts become user text;
- image or embedded context parts are accepted only when the configured provider
  and manifest surface support them;
- unsupported content fails closed with a shaped JSON-RPC error.

The adapter streams Verlet events as ACP `session/update` notifications:

| Verlet event | ACP projection |
| --- | --- |
| `RuntimeEventKind::TextDelta` | assistant text chunk |
| `RuntimeEventKind::ThinkingDelta` | thinking/status update when supported |
| `RuntimeEventKind::ToolCallStarted` | `tool_call` update |
| `RuntimeEventKind::ToolCallResult` | `tool_call_update` update |
| `RuntimeEventKind::ApprovalRequested` | deferred until the permission coupling outlet exists |
| `RuntimeEventKind::PermissionDecision` | deferred until the permission coupling outlet exists |
| `RuntimeEventKind::Usage` | `usage_update` update |
| `RuntimeEventKind::Terminal` | final `PromptResponse` stop reason |
| `ThreadEvent::Failed` | shaped JSON-RPC error or terminal failure update |

Lossy projections are allowed only as documented status/text updates. They must
not change the underlying Verlet event stream or receipts.

## Stop Reasons

The adapter maps terminal state conservatively:

| Verlet terminal state | ACP stop reason |
| --- | --- |
| completed | `end_turn` |
| cancelled | `cancelled` |
| stopped | `cancelled` when user initiated, otherwise shaped failure |
| failed | shaped JSON-RPC error unless a more precise ACP reason is known |
| token or turn limit | `max_tokens` or `max_turn_requests` when detectable |
| policy refusal | `refusal` when detectable |

If the mapping is ambiguous, preserve the Verlet terminal event and return a
clear error instead of pretending the turn completed normally.

## MCP Inputs

ACP can carry MCP server configuration in `session/new`. In Verlet V1, those
configs are compatibility inputs only. Tool authority still comes from manifest
binding, configured MCP source records, and grant checks.

V1 should choose one of these explicit behaviors:

- accept ACP `mcpServers` only when they match configured Verlet MCP sources;
- reject ACP `mcpServers` with a clear error;
- ignore them only if the response says they were not applied.

Silent tool injection is not allowed.

## Config Options

V1 exposes two ACP `configOptions`:

- `model`: a session-local selector sourced from the app-server `model/list`
  response and applied to later `turn/start` requests.
- `thought_level`: a session-local selector with `none`, `low`, `medium`, and
  `high`. `none` maps to disabled app-server thinking config. Other values map
  to turn-level provider reasoning effort when the selected provider supports
  it.

`session/set_config_option` always returns the complete refreshed option list.
Unsupported config ids and unsupported values fail closed with a shaped
JSON-RPC error. V1 does not emit legacy ACP `modes`; there is no permission mode
mapping until permission requests have a Verlet-owned coupling outlet.

ACP config options may not mutate:

- provider secrets;
- tenant/user/session identity;
- operation registry state;
- tool bindings;
- placement or sandbox policy;
- manifest runtime override allowlists.

Unsupported selector changes fail closed.

## Permission Boundary

ACP `session/request_permission` is intentionally not wired in V1. The bridge
needs a coupling outlet: a Verlet-owned place where ACP client allow, deny, and
cancel decisions can become witnessed control facts without bypassing manifest
binding, grants, runtime policy, or durable approval receipts.

Until that outlet exists, ACP hosts cannot approve Verlet operations on behalf
of a thread. Verlet tool and approval policy remains internal and fail-closed.
The design and implementation work is tracked in
[ACP: design permission coupling outlet](https://github.com/emotionscientific/cooldis/issues/153).

## Tool And Usage Updates

Verlet dynamic tool items project to ACP status updates where the ACP schema has
a matching shape:

- started tool items emit `tool_call`;
- completed or failed tool items emit `tool_call_update`;
- ACP has no distinct cancelled tool status, so cancellation must remain visible
  in the underlying Verlet event stream even if a host only sees a terminal
  prompt cancellation.

Verlet `Usage` events project to ACP `usage_update`. When Verlet does not have
an ACP-compatible context-window size, V1 sets `size` to the same token count as
`used` and preserves the raw Verlet usage payload under ACP metadata.

## Shared Protocol Adapter Boundary

ACP is the first concrete external agent-client protocol adapter. Future
protocols should lower into the same small vocabulary:

- create or bind a session;
- submit a prompt/turn;
- stream updates;
- request and resolve permission only after a Verlet coupling outlet exists;
- cancel or interrupt;
- expose config selectors;
- load or resume only when the protocol semantics match Verlet durability.

Reusable adapter helpers may own framing, session tables, stop-reason mapping,
and event projection. They must not own scheduling, manifest binding, grants, or
runtime execution. Those remain app-server and kernel responsibilities.

## Test Plan

Focused ACP tests should cover:

- `initialize` succeeds and advertises only implemented capabilities;
- invalid JSON-RPC returns a shaped parse error;
- non-JSON logs never appear on stdout;
- `session/new` returns a session id traceable to a Verlet thread id;
- manifest compile and bind receipts precede the first prompt turn;
- `session/prompt` streams deltas and waits for `RuntimeEventKind::Terminal`;
- concurrent prompt behavior is deterministic;
- `session/cancel` ends the pending prompt with `stopReason: "cancelled"`;
- unsupported methods and unsupported features fail explicitly.

Process-backed smoke tests should launch the real `verlet-acp-agent` binary over
stdio, use temp homes and socket paths, and keep logs on stderr.

Live-provider proof stays opt-in: configure a provider-neutral daemon profile
and run the ignored ACP live-provider test to prove `session/new` plus
`session/prompt` can return a real model response through the same app-server
thread path.

## Related Work

- [Verlet ACP Agent](acp-agent.md)
- [Epic: ACP-compatible Verlet thread adapter](https://github.com/emotionscientific/cooldis/issues/143)
- [ACP: define the Verlet thread projection spec](https://github.com/emotionscientific/cooldis/issues/144)
- [Protocol adapters: define the shared thread-projection boundary](https://github.com/emotionscientific/cooldis/issues/152)
- [ACP: design permission coupling outlet](https://github.com/emotionscientific/cooldis/issues/153)
- [Epic: External runtime interface via MCP, TypeScript, and Python](https://github.com/emotionscientific/cooldis/issues/93)
- [Epic: first-party MCP client support](https://github.com/emotionscientific/cooldis/issues/116)
