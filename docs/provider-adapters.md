# Provider Adapter Surface

Cooldis provider support is split into three layers:

```text
canonical history
-> ProviderRequest / ProviderResponse / ProviderStreamEvent
-> ProviderClient or ProviderWireAdapter
-> provider API or gateway
```

Canonical session history stays provider-neutral. Provider-native JSON is a wire
projection, not the runtime model.

## Capability Records

Every first-party wire adapter exposes a `ProviderCapabilityRecord`. Provider
clients may also expose one directly. The record is queryable before dispatch and
is the fail-closed contract for:

- tools;
- streaming;
- reasoning or thinking configuration;
- cache controls;
- images and attachments;
- max output/context metadata;
- tool-result constraints;
- supported ABI projections.

Current built-in families:

| Family | API | Tools | Stream | Reasoning | Cache | Images | Attachments | ABI projections |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `openai_responses` | OpenAI Responses | yes | yes | yes | no | yes | no | text, image input, LLM tool |
| `openai_chat_completions` | OpenAI Chat Completions | yes | yes | yes | no | no | no | text, LLM tool |
| `anthropic_messages` | Anthropic Messages | yes | yes | yes | yes | yes | no | text, image input, LLM tool |
| `anthropic_bedrock_messages` | Anthropic Messages on Bedrock InvokeModel/InvokeModelWithResponseStream | yes | yes | yes | yes | yes | no | text, image input, LLM tool |
| local/offline | `ProviderApi::Other(...)` | no | no | no | no | no | no | text |

Adapters or clients that need gateway-specific behavior should override the
capability record instead of smuggling quirks into canonical history.

## Fail-Closed Validation

Provider dispatch rejects unsupported surfaces before wire dispatch. Validation
currently covers:

- request API mismatch;
- streaming requests when streaming is disabled;
- tool definitions when tools are disabled;
- reasoning/thinking when reasoning is disabled;
- cache controls when provider cache controls are disabled;
- image content when images are disabled;
- `max_tokens` above a provider max output limit;
- tool-result content above configured tool-result limits.

This keeps provider differences explicit. A missing capability is an error, not
a lossy downgrade.

OpenAI Chat Completions maps effort-based thinking for `low`, `medium`, and
`high` to `reasoning_effort`. It only sends the Zhipu-style `thinking` object to
the built-in Zhipu-convention provider ids (`openai_compatible`, `zhipu`, `glm`), because
strict OpenAI-compatible endpoints can reject unknown request fields. Budget
thinking and unsupported effort values fail closed before dispatch.

## Context Compilation

Provider runtime now uses the kernel-level `AgentContextCompiler` before
provider request construction. The compiler takes explicit inputs for system
blocks, canonical session entries, compaction summaries, hook-added context,
turn/environment context, attachments, and tool definitions. It returns the
compiled model-visible context plus diagnostics for dropped entries, retained
text bytes, and truncated text bytes.

Provider-specific context policy still runs after that kernel compilation in
`CanonicalProviderRuntimeFactory` when the client exposes capabilities.
The runtime emits a `context_compiled` event after both compilation passes so
host code can observe kernel diagnostics alongside provider-specific message
drops and text truncation.

`ProviderContextPolicy` currently supports:

- `max_messages`: retain the newest N canonical messages;
- `max_text_bytes`: retain the newest text suffix across retained messages.

Truncation is UTF-8-boundary-safe and preserves non-text canonical blocks unless
their provider capability check rejects them later. The compilation result
reports dropped messages, retained text bytes, and truncated text bytes.
`max_messages` truncation keeps the issuing assistant message for any retained
tool result so truncation can never split a tool call/result pair.

## Replay-Fidelity Transform

Canonical history can carry content the target API cannot represent —
thinking from another provider, assistant images, cache controls, tool calls
whose results were never recorded, assistant messages that ended in an error.
`normalize_history_for_target` (adapters/provider_transform.rs) runs on every
turn between request construction and provider context truncation, and again
after truncation, normalizing the compiled history for the target:

- foreign thinking with visible text converts to a `<thinking>`-tagged text
  block; redacted/encrypted-only thinking drops; native thinking passes
  verbatim so builders can replay it faithfully;
- errored assistants (stop reason error/cancelled) drop with their tool
  results; dangling tool calls, unpaired tool results, and duplicate
  tool-call ids drop rather than guessing a pairing — the transform never
  creates content (no synthetic tool results);
- historical user images drop for non-image targets; the latest user message
  (when it is the final compiled message) is exempt so unsupported
  current-turn input fails closed at validation instead of being silently
  eaten; cache controls strip for non-cache targets.

Every action is counted and the counts ride on the compiled-context receipt
under `replay_transform`; a matching-provenance history is a zero-count
pass-through.

## Runtime Observability

Provider dispatch emits `model_request_started`, `model_request_completed`, and
`model_request_failed` events for normal turns and model-backed compaction. The
events carry provider/API/model coordinates, complete-vs-stream mode, purpose,
request shape, duration, stop reason, and usage where available.

Tool execution also emits structured permission and tool diagnostics. A
successful permission gate produces `permission_decision`, each tool result can
carry `duration_ms`, and `tool_log` records expose a stable log-level plus
string metadata for host dashboards or audit sinks.

## Compaction

Provider runtime supports manual Cooldis compaction through
`RuntimeHost::compact_thread`. Manual compaction runs `PreCompact` and
`PostCompact` hooks, optionally asks the configured provider client for a
summary, and appends a `SessionEntryKind::Compaction` record. Compaction records
are append-only branch entries: they clear prior model-visible messages while
preserving checkpoint, resume, and fork lineage.

Auto-compaction is opt-in through `CompactionPolicy::auto_at_text_bytes`. The
runtime checks the active compiled context before appending the next submitted
turn; if the configured text budget is exceeded, it compacts the existing
history first, then appends the user turn so the latest prompt remains visible.

## Gateway And Local Paths

Live provider smokes should exercise generic wire-compatible endpoints: OpenAI
Responses, OpenAI Chat Completions, and Anthropic Messages. They should verify
complete and text-streaming calls by checking expected marker text in the model
output, not only HTTP status or parseability.

`cooldis-bifrost-smoke` is the release-gated provider-protocol smoke for OpenAI
Responses and Anthropic Messages. Despite the historical binary name, it accepts
separate official credentials:

- OpenAI Responses: `OPENAI_API_KEY`, optionally
  `COOLDIS_OPENAI_RESPONSES_BASE_URL` and `COOLDIS_OPENAI_RESPONSES_MODEL`;
- Anthropic Messages: `ANTHROPIC_API_KEY`, optionally
  `COOLDIS_ANTHROPIC_MESSAGES_BASE_URL` and
  `COOLDIS_ANTHROPIC_MESSAGES_MODEL`.

It still accepts the older `COOLDIS_BIFROST_URL` / `COOLDIS_BIFROST_KEY` pair as
a gateway compatibility path. OpenAI Compatible/OpenAI-compatible MODEL smokes remain a
separate Chat Completions-compatible lane and do not count as OpenAI Responses
or Anthropic Messages protocol evidence.

`cooldis chat` can route its private Codex-shaped app-server through any
wire-compatible provider endpoint. These paths use the same provider adapter
boundary: gateways remain wire-compatible endpoints, and Cooldis still stores
canonical provider-neutral history rather than provider-native JSON. See
[Cooldis RPC Control Plane](app-server.md) for the local config shape and
command-line flags.

`LocalOfflineProviderClient` is the deterministic local provider shape for tests
and future local runtimes. It intentionally supports only text completion and
rejects tools, streaming, reasoning, cache controls, and images.

Gemini and further gateway-specific adapters should plug in through the same
capability record plus wire-adapter boundary when their provider-specific
differences are needed.
