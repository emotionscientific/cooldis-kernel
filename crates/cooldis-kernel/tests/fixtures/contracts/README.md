These fixtures are Cooldis host-facing contract goldens.

Update them only when the serialized contract intentionally changes. The
fixture tests print the freshly generated JSON on missing or mismatched files so
contract diffs stay reviewable.

`stream_schema_v1.json` freezes the Stream Schema V1 spine: stream record
envelopes, cursors, append acknowledgements, backend capabilities, routing
decisions, the read-plan entries used by context assembly, and the witnessed
`thread.reload.degraded` fallback payload.

`debug_thread_export_v1.json` freezes the normalized
`cooldis.debug.thread_export/1` evidence bundle shape used by
`thread/debug/export`; stream rows inside it still carry
`cooldis.stream.record/1` envelopes.

`stdlib_context_truncate_coupling.json` freezes the V1 read-plan segment map
for dropping an old prefix while retaining the raw tail.

`stdlib_context_summarize_coupling.json` freezes the V1 summary-checkpoint plus
read-plan pair emitted by the context summarization reference.

`stdlib_prompt_steer_coupling.json` freezes prompt steering as explicit
control facts: continuation requests or read-plan selection of an existing
instruction checkpoint.

`stdlib_supervisor_child_completion_coupling.json` freezes the V1 supervisor
join reference: a routed child completion fact becomes either a terminal
`loop.completed` fact or a parent `turn.continue.requested` fact.
