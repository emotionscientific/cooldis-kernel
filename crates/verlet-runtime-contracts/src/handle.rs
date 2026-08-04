//! Handle contract types (ADR 0006): dispatch identity and the terminal
//! envelope delivered for every handle.
//!
//! A handle is a durable reference to work in flight, returned by a call in
//! place of its value. The law: a handle-returning call declares a dispatch
//! identity and is idempotent on it, and every handle reaches exactly one
//! witnessed terminal outcome carrying provenance to the originating call.
//! The envelope below is that outcome's one schema for all handle kinds; it
//! is delivered to the consumer as witnessed ingress content (kind
//! [`HANDLE_OUTCOME_CONTENT_KIND`]) through the durable ingress lane, so
//! exactly-once delivery is the ingress claim/settle protocol's, not new
//! machinery here.

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::{RuntimeTerminalState, RuntimeUsage, ThreadCoordinates, ThreadId};

/// Ingress content kind carrying a [`HandleDispatchEnvelope`]. Dispatch
/// ingress is observed and settled before a process backend starts; unlike a
/// terminal outcome it never wakes the consumer thread.
pub const HANDLE_DISPATCH_CONTENT_KIND: &str = "cooldis.handle.dispatch/1";

/// Ingress content kind carrying a [`HandleTerminalEnvelope`]. This is a
/// content kind, not a stream event kind: the envelope rides
/// `io.ingress.*` records and adds nothing to the frozen event vocabulary.
pub const HANDLE_OUTCOME_CONTENT_KIND: &str = "cooldis.handle.outcome/1";

/// Identity of a handle-returning call. Conductor calls supply it
/// explicitly; model-initiated calls get it injected by the tool router
/// from the provider tool-call id. A retried call with the same dispatch id
/// folds existing dispatch state and returns the original handle — never a
/// second execution. For thread spawns this is carried on the wire in the
/// existing `correlation_id` field.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DispatchId(String);

impl DispatchId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for DispatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandleKind {
    Thread,
    Process,
}

/// The handle value a dispatching call returns in place of its result: a
/// kind-tagged durable id. The id is the string form of the underlying
/// thread or process id; model-facing surfaces address handles by
/// `task_name` alias, never by this raw id.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HandleId {
    pub kind: HandleKind,
    pub id: String,
}

impl HandleId {
    pub fn thread(thread_id: ThreadId) -> Self {
        Self {
            kind: HandleKind::Thread,
            id: thread_id.to_string(),
        }
    }

    pub fn process(process_id: impl Into<String>) -> Self {
        Self {
            kind: HandleKind::Process,
            id: process_id.into(),
        }
    }
}

/// Durable process-dispatch fact carried as observe-only ingress.
///
/// The process id is allocated before backend startup, so this record is the
/// serialization point for idempotent dispatch. It intentionally contains no
/// terminal fact: process termination is first made durable by the separate
/// [`HandleTerminalEnvelope`] ingress witness.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HandleDispatchEnvelope {
    pub dispatch_id: DispatchId,
    pub handle: HandleId,
    pub consumer: ThreadCoordinates,
    pub command_digest: String,
}

/// The closed outcome vocabulary of a handle. Deliberately three-valued:
/// richer terminal detail (timeout, budget exhaustion, exit status) rides
/// `outcome_reason` on the envelope, and an escalation is ordinary child
/// output the parent reads — not an outcome.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HandleTerminalOutcome {
    Completed,
    Failed,
    Cancelled,
}

/// The lawful projection of the runtime's richer terminal states into the
/// closed vocabulary. Callers carry the lost detail in `outcome_reason`.
impl From<RuntimeTerminalState> for HandleTerminalOutcome {
    fn from(state: RuntimeTerminalState) -> Self {
        match state {
            RuntimeTerminalState::Completed => Self::Completed,
            RuntimeTerminalState::Cancelled => Self::Cancelled,
            RuntimeTerminalState::Stopped
            | RuntimeTerminalState::Failed
            | RuntimeTerminalState::TimedOut => Self::Failed,
        }
    }
}

/// The terminal value of any handle: exactly one of these reaches the
/// consumer per dispatch, for every handle kind and placement.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct HandleTerminalEnvelope {
    /// Identity of the originating call — the provenance leg of the law.
    pub dispatch_id: DispatchId,
    pub handle: HandleId,
    pub outcome: HandleTerminalOutcome,
    /// Detail the closed outcome vocabulary drops (timeout, budget, exit
    /// status). Never load-bearing for consumer control flow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome_reason: Option<String>,
    /// Schema-typed result value, validated against `result_schema_id`
    /// when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_schema_id: Option<String>,
    /// Content-addressed references to artifacts the work produced.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<String>,
    /// Present for thread handles; absent for processes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<RuntimeUsage>,
    /// Whether re-dispatch under a fresh dispatch identity is a sensible
    /// caller move.
    pub retryable: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_state_projection_is_total_and_three_valued() {
        assert_eq!(
            HandleTerminalOutcome::from(RuntimeTerminalState::Completed),
            HandleTerminalOutcome::Completed
        );
        assert_eq!(
            HandleTerminalOutcome::from(RuntimeTerminalState::Cancelled),
            HandleTerminalOutcome::Cancelled
        );
        for failed_like in [
            RuntimeTerminalState::Stopped,
            RuntimeTerminalState::Failed,
            RuntimeTerminalState::TimedOut,
        ] {
            assert_eq!(
                HandleTerminalOutcome::from(failed_like),
                HandleTerminalOutcome::Failed
            );
        }
    }

    #[test]
    fn envelope_wire_shape_is_pinned() {
        let envelope = HandleTerminalEnvelope {
            dispatch_id: DispatchId::new("toolu_abc123"),
            handle: HandleId::process("proc-7"),
            outcome: HandleTerminalOutcome::Failed,
            outcome_reason: Some("exit status 2".to_string()),
            result: None,
            result_schema_id: None,
            artifact_refs: vec!["blob:sha256-deadbeef".to_string()],
            usage: None,
            retryable: true,
        };
        let json = serde_json::to_value(&envelope).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "dispatch_id": "toolu_abc123",
                "handle": {"kind": "process", "id": "proc-7"},
                "outcome": "failed",
                "outcome_reason": "exit status 2",
                "artifact_refs": ["blob:sha256-deadbeef"],
                "retryable": true,
            })
        );
        let decoded: HandleTerminalEnvelope = serde_json::from_value(json).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[test]
    fn process_dispatch_envelope_wire_shape_is_pinned() {
        let raw = serde_json::json!({
            "dispatch_id": "toolu_process_420",
            "handle": {
                "kind": "process",
                "id": "018f0000-0000-7000-8000-000000000420"
            },
            "consumer": {
                "tenant_id": "tenant-a",
                "user_id": "user-a",
                "session_id": "session-a",
                "thread_id": "018f0000-0000-7000-8000-000000000419"
            },
            "command_digest": "sha256:dispatch-command"
        });
        let decoded: HandleDispatchEnvelope = serde_json::from_value(raw.clone()).unwrap();

        assert_eq!(decoded.dispatch_id, DispatchId::new("toolu_process_420"));
        assert_eq!(
            decoded.handle,
            HandleId::process("018f0000-0000-7000-8000-000000000420")
        );
        assert_eq!(decoded.consumer.tenant_id, "tenant-a");
        assert_eq!(decoded.command_digest, "sha256:dispatch-command");
        assert_eq!(serde_json::to_value(decoded).unwrap(), raw);
        assert_eq!(HANDLE_DISPATCH_CONTENT_KIND, "cooldis.handle.dispatch/1");
    }

    #[test]
    fn minimal_envelope_decodes_without_optional_fields() {
        let raw = serde_json::json!({
            "dispatch_id": "conductor-dispatch-1",
            "handle": {"kind": "thread", "id": "018f0000-0000-7000-8000-000000000000"},
            "outcome": "completed",
            "retryable": false,
        });
        let decoded: HandleTerminalEnvelope = serde_json::from_value(raw).unwrap();
        assert_eq!(decoded.outcome, HandleTerminalOutcome::Completed);
        assert!(decoded.result.is_none());
        assert!(decoded.artifact_refs.is_empty());
    }
}
