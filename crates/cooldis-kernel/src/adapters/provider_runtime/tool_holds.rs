//! Holds for tool-call batches (EMO-434 Part B).
//!
//! A hold is the shared-or-exclusive access one tool invocation takes on a
//! keyed runtime resource for its duration (formalism lexicon, `hold`,
//! 2026-07-13). Holds are derived by the kernel from the bound tool's
//! surface, never supplied by the model, and are witnessed on the tool-call
//! request event. Two invocations of one batch may run concurrently only
//! when no key is held exclusively by one while held at all by the other;
//! an invocation with no derivable holds takes the global key exclusively.
//!
//! The v1 key vocabulary is fixed here and only here. Extending it (or
//! accepting manifest-declared custom holds) is v2 work; do not widen it
//! inside an implementation ticket.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// How a hold occupies its key. `Shared` holds coexist on one key;
/// `Exclusive` admits nothing else on that key for the call's duration.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ToolHoldAccess {
    Shared,
    Exclusive,
}

/// The keyed runtime resource a hold occupies. The serialized shape is part
/// of the durable tool-call request payload; fields are additive-only.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum ToolHoldKey {
    /// The thread's shell/process substrate: one `BashkitExecutionHarness`
    /// and process table per thread, so all writers serialize.
    ShellSession,
    /// One parent-scoped child thread, keyed by its stable task name.
    KernelThread { task_name: String },
    /// One MCP tool universe; concurrent calls are the server's contract.
    McpServer { server: String },
    /// The whole runtime. Shared for read-only kernel ops; exclusive as the
    /// fail-safe when nothing narrower is derivable.
    Global,
}

/// One witnessed hold: a key and the access taken on it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub(super) struct ToolHold {
    pub(super) key: ToolHoldKey,
    pub(super) access: ToolHoldAccess,
}

impl ToolHold {
    fn shared(key: ToolHoldKey) -> Self {
        Self {
            key,
            access: ToolHoldAccess::Shared,
        }
    }

    fn exclusive(key: ToolHoldKey) -> Self {
        Self {
            key,
            access: ToolHoldAccess::Exclusive,
        }
    }
}

/// Derive the holds for one tool invocation from its bound name and
/// model-supplied arguments. Pure; the v1 vocabulary in full:
///
/// - `bash`, `process_exec`, `write_stdin`, `process_write`, `process_poll`,
///   `process_terminate` → exclusive [`ToolHoldKey::ShellSession`] (one
///   harness and process table per thread; polls observe mutating state and
///   stay ordered with it).
/// - `thread_spawn`/`thread_submit`/`thread_wait`/`thread_status`/
///   `thread_cancel` → exclusive [`ToolHoldKey::KernelThread`] keyed by the
///   `task_name` argument; a missing or non-string `task_name` falls back to
///   exclusive global (underivable key).
/// - `tool_call` → shared [`ToolHoldKey::McpServer`] keyed by the `universe`
///   argument; absent `universe` (router-disambiguated) falls back to
///   exclusive global.
/// - `tool_search`, `tool_describe`, `mandate_list` → shared global
///   (read-only kernel ops).
/// - Anything else → exclusive global (fail-safe).
///
/// Every non-global hold set also carries an implicit shared hold on the
/// global key, so the exclusive-global fail-safe is a true barrier: an
/// invocation with underivable holds overlaps nothing, not merely no other
/// global holder.
pub(super) fn derive_tool_holds(tool_name: &str, arguments: &Value) -> Vec<ToolHold> {
    const SHELL_SESSION_TOOLS: [&str; 6] = [
        "bash",
        "process_exec",
        "write_stdin",
        "process_write",
        "process_poll",
        "process_terminate",
    ];
    const KERNEL_THREAD_TOOLS: [&str; 5] = [
        "thread_spawn",
        "thread_submit",
        "thread_wait",
        "thread_status",
        "thread_cancel",
    ];
    const SHARED_GLOBAL_TOOLS: [&str; 3] = ["tool_search", "tool_describe", "mandate_list"];

    if SHELL_SESSION_TOOLS.contains(&tool_name) {
        return with_global_floor(vec![ToolHold::exclusive(ToolHoldKey::ShellSession)]);
    }
    if KERNEL_THREAD_TOOLS.contains(&tool_name) {
        return match arguments.get("task_name").and_then(Value::as_str) {
            Some(task_name) if !task_name.is_empty() => {
                with_global_floor(vec![ToolHold::exclusive(ToolHoldKey::KernelThread {
                    task_name: task_name.to_string(),
                })])
            }
            _ => vec![ToolHold::exclusive(ToolHoldKey::Global)],
        };
    }
    if tool_name == "tool_call" {
        return match arguments.get("universe").and_then(Value::as_str) {
            Some(server) if !server.is_empty() => {
                with_global_floor(vec![ToolHold::shared(ToolHoldKey::McpServer {
                    server: server.to_string(),
                })])
            }
            _ => vec![ToolHold::exclusive(ToolHoldKey::Global)],
        };
    }
    if SHARED_GLOBAL_TOOLS.contains(&tool_name) {
        return vec![ToolHold::shared(ToolHoldKey::Global)];
    }
    vec![ToolHold::exclusive(ToolHoldKey::Global)]
}

/// Append the implicit shared global hold to a non-global hold set.
fn with_global_floor(mut holds: Vec<ToolHold>) -> Vec<ToolHold> {
    if !holds
        .iter()
        .any(|hold| matches!(hold.key, ToolHoldKey::Global))
    {
        holds.push(ToolHold::shared(ToolHoldKey::Global));
    }
    holds
}

/// Whether two hold sets conflict: some key is held exclusively by one side
/// while held at all by the other. Conflicting calls serialize in call
/// order; non-conflicting calls may overlap.
pub(super) fn holds_conflict(a: &[ToolHold], b: &[ToolHold]) -> bool {
    a.iter().any(|left| {
        b.iter().any(|right| {
            left.key == right.key
                && (left.access == ToolHoldAccess::Exclusive
                    || right.access == ToolHoldAccess::Exclusive)
        })
    })
}

/// The batch schedule as wait edges: for call `i`, the indices of every
/// earlier call it must finish-wait on. Derived purely from the per-call
/// holds, so schedule legality is decidable from the record alone. The
/// executor owns everything this does not encode: results append in call
/// order with finish order witnessed, per-call failure isolation (a failed
/// call never cancels siblings), turn cancellation cancels all in-flight
/// calls, and one batch counts as one round.
pub(super) fn batch_wait_edges(holds: &[Vec<ToolHold>]) -> Vec<Vec<usize>> {
    holds
        .iter()
        .enumerate()
        .map(|(index, call)| {
            (0..index)
                .filter(|&earlier| holds_conflict(&holds[earlier], call))
                .collect()
        })
        .collect()
}

/// EMO-434 Part B executor entry point: the schedule for one model-emitted
/// batch, computed where the sequential `for tool_call in tool_calls` loop
/// in `append_tool_results` runs today. The implementing executor derives
/// holds per call, witnesses them on each tool-call request event, runs the
/// batch under these wait edges, appends results in call order with finish
/// order witnessed, isolates per-call failures (a failed call never cancels
/// siblings), propagates turn cancellation to every in-flight call, and
/// counts the whole batch as one router round.
pub(super) fn plan_tool_call_batch(tool_calls: &[super::ProviderToolCall]) -> Vec<Vec<usize>> {
    let holds = tool_calls
        .iter()
        .map(|call| derive_tool_holds(&call.name, &call.arguments))
        .collect::<Vec<_>>();
    batch_wait_edges(&holds)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn shell_family_takes_the_exclusive_shell_session_hold() {
        for tool in ["bash", "process_exec", "write_stdin"] {
            assert_eq!(
                derive_tool_holds(tool, &json!({})),
                vec![
                    ToolHold::exclusive(ToolHoldKey::ShellSession),
                    ToolHold::shared(ToolHoldKey::Global),
                ],
            );
        }
    }

    #[test]
    fn thread_ops_key_by_task_name_and_fall_back_to_global_exclusive() {
        assert_eq!(
            derive_tool_holds("thread_submit", &json!({"task_name": "worker-a"})),
            vec![
                ToolHold::exclusive(ToolHoldKey::KernelThread {
                    task_name: "worker-a".to_string(),
                }),
                ToolHold::shared(ToolHoldKey::Global),
            ],
        );
        assert_eq!(
            derive_tool_holds("thread_submit", &json!({})),
            vec![ToolHold::exclusive(ToolHoldKey::Global)],
        );
    }

    #[test]
    fn mcp_calls_share_per_server_and_unknown_tools_fail_safe() {
        assert_eq!(
            derive_tool_holds("tool_call", &json!({"universe": "linear"})),
            vec![
                ToolHold::shared(ToolHoldKey::McpServer {
                    server: "linear".to_string(),
                }),
                ToolHold::shared(ToolHoldKey::Global),
            ],
        );
        assert_eq!(
            derive_tool_holds("tool_call", &json!({})),
            vec![ToolHold::exclusive(ToolHoldKey::Global)],
        );
        assert_eq!(
            derive_tool_holds("never_heard_of_it", &json!({})),
            vec![ToolHold::exclusive(ToolHoldKey::Global)],
        );
    }

    #[test]
    fn the_global_exclusive_fail_safe_is_a_full_barrier() {
        let fail_safe = derive_tool_holds("unknown", &json!({}));
        for (tool, arguments) in [
            ("bash", json!({})),
            ("thread_wait", json!({"task_name": "a"})),
            ("tool_call", json!({"universe": "linear"})),
            ("tool_search", json!({})),
        ] {
            assert!(
                holds_conflict(&derive_tool_holds(tool, &arguments), &fail_safe),
                "{tool} must serialize against the fail-safe",
            );
        }
    }

    #[test]
    fn wait_edges_serialize_conflicts_in_call_order_only() {
        let bash = derive_tool_holds("bash", &json!({}));
        let read = derive_tool_holds("tool_search", &json!({}));
        let thread_a = derive_tool_holds("thread_wait", &json!({"task_name": "a"}));
        let thread_b = derive_tool_holds("thread_wait", &json!({"task_name": "b"}));

        let edges = batch_wait_edges(&[bash.clone(), bash, read.clone(), read, thread_a, thread_b]);

        assert_eq!(
            edges,
            vec![vec![], vec![0], vec![], vec![], vec![], vec![],],
        );
    }

    #[test]
    fn shared_global_readers_conflict_with_the_global_exclusive_fail_safe() {
        let reader = derive_tool_holds("tool_describe", &json!({}));
        let fail_safe = derive_tool_holds("unknown", &json!({}));
        assert!(holds_conflict(&reader, &fail_safe));
        assert!(!holds_conflict(&reader, &reader.clone()));
    }

    #[test]
    fn hold_wire_shape_is_stable() {
        let hold = ToolHold::exclusive(ToolHoldKey::KernelThread {
            task_name: "worker-a".to_string(),
        });
        assert_eq!(
            serde_json::to_value(&hold).unwrap(),
            json!({
                "key": {"kind": "kernel_thread", "task_name": "worker-a"},
                "access": "exclusive",
            }),
        );
    }
}
