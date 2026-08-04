use crate::{
    AgentManifestBindReceipt, AgentManifestCouplingBinding, CouplingRole, EventKind, EventOrigin,
    EventRecord, EventRecordId, EventStore, EventStreamId, ThreadCoordinates, VerletError,
    VerletResult,
};
use chrono::{SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeSet;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ToolCallSubject {
    pub turn_id: String,
    pub call_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallRequestedPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub tool_name: String,
    pub arguments: JsonValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_fingerprint: Option<String>,
    /// Kernel-derived resource holds for this invocation. Empty when decoding
    /// events written before hold scheduling existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holds: Vec<JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallDecisionPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub outcome: ToolCallDecisionOutcomePayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ToolCallDecisionOutcomePayload {
    Allow,
    Rewrite { arguments: JsonValue },
    Deny { reason: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallSuspendedPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolCallCompletedPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub tool_name: String,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_fingerprint: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    /// Zero-based completion order observed by the batch executor. Event and
    /// history append order remains model call order.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_order: Option<u64>,
    /// How the call ended relative to an external interrupt. Absent means the
    /// call ran to completion without an interrupt reaching it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation: Option<ToolCallCancellation>,
}

/// Recovery honors the effect class; a recorded outcome is reused only under
/// a matching fingerprint within the same snapshot. A missing recorded
/// fingerprint retains legacy request-event and call-id reuse.
pub(crate) fn tool_invocation_fingerprint_matches(
    recorded_snapshot_id: &str,
    recorded_fingerprint: Option<&str>,
    current_snapshot_id: &str,
    current_fingerprint: Option<&str>,
) -> bool {
    recorded_snapshot_id == current_snapshot_id
        && recorded_fingerprint.is_none_or(|recorded| Some(recorded) == current_fingerprint)
}

/// Witnessed cancellation outcome for a tool call reached by an interrupt.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallCancellation {
    /// The invocation observed the cancellation token and settled within the
    /// cancellation grace.
    CancelledAcknowledged,
    /// The invocation did not settle within the cancellation grace and was
    /// abandoned; this terminal record was settled by the detached invocation
    /// itself, never by the turn loop that stopped waiting.
    CancelledExceededGrace,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ApprovalSubject {
    pub approval_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ApprovalResolvedPayload {
    pub subject: ApprovalSubject,
    pub snapshot_id: String,
    pub approved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MandateSubject {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandateSchedulePayload {
    Cron { expr: String, tz: String },
    Interval { every_ms: u64 },
    At { when: String },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandateCatchUpPolicy {
    CoalesceMissed,
    #[default]
    SkipMissed,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MandateStartedPayload {
    pub subject: MandateSubject,
    pub mandate_id: String,
    pub snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_continuations: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<MandateSchedulePayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_occurrences: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catch_up: Option<MandateCatchUpPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_template: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MandateRevokedPayload {
    pub subject: MandateSubject,
    pub mandate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandate_event_id: Option<String>,
    pub snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TurnContinuationSubject {
    pub loop_id: String,
    pub parent_turn_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnContinueRequestedPayload {
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub next_turn_input: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnContinuationAcceptedPayload {
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub mandate_id: String,
    pub next_turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TurnContinuationRejectedPayload {
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PlacementSubject {
    pub invocation_id: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PlacementDecisionPayload {
    pub subject: PlacementSubject,
    pub snapshot_id: String,
    pub placement: PlacementTarget,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlacementTarget {
    Local,
    Remote,
    Sandbox,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDecisionRequest {
    pub coordinates: ThreadCoordinates,
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub request_event_id: EventRecordId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolControllerBinding {
    pub coupling_id: String,
    pub snapshot_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingToolCallSuspension {
    pub suspended_event_id: EventRecordId,
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub request_event_id: Option<EventRecordId>,
    pub approval_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolCallDecision {
    NoDecision,
    Allow {
        consumed_fact_id: EventRecordId,
    },
    Rewrite {
        consumed_fact_id: EventRecordId,
        arguments: JsonValue,
    },
    Deny {
        consumed_fact_id: Option<EventRecordId>,
        reason: String,
        fail_closed: bool,
    },
    Wait {
        consumed_fact_id: EventRecordId,
        approval_id: Option<String>,
        reason: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TurnContinuationDecisionRequest {
    pub coordinates: ThreadCoordinates,
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub request_event_id: EventRecordId,
    pub now_ms: i64,
    pub completed_continuations: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnContinuationDecision {
    NoRequest,
    Accept {
        consumed_request_id: EventRecordId,
        mandate_id: String,
        next_turn_input: String,
    },
    Reject {
        consumed_request_id: Option<EventRecordId>,
        reason: String,
        fail_closed: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementDecisionRequest {
    pub coordinates: ThreadCoordinates,
    pub subject: PlacementSubject,
    pub snapshot_id: String,
    pub request_event_id: EventRecordId,
    pub default_target: PlacementTarget,
    pub allowed_targets: BTreeSet<PlacementTarget>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlacementDecision {
    Default {
        target: PlacementTarget,
    },
    Selected {
        consumed_fact_id: EventRecordId,
        target: PlacementTarget,
    },
    Deny {
        consumed_fact_id: Option<EventRecordId>,
        reason: String,
        fail_closed: bool,
    },
}

impl ToolCallDecision {
    pub fn consumed_fact_id(&self) -> Option<EventRecordId> {
        match self {
            Self::NoDecision => None,
            Self::Allow { consumed_fact_id }
            | Self::Rewrite {
                consumed_fact_id, ..
            }
            | Self::Wait {
                consumed_fact_id, ..
            } => Some(*consumed_fact_id),
            Self::Deny {
                consumed_fact_id, ..
            } => *consumed_fact_id,
        }
    }
}

pub async fn decide_tool_call<S: EventStore + ?Sized>(
    store: &S,
    request: ToolDecisionRequest,
) -> VerletResult<ToolCallDecision> {
    let control_events = store
        .read_events(&control_stream_id(&request.coordinates), None)
        .await
        .map_err(|err| VerletError::History(err.to_string()))?;
    let mut terminal_candidates = Vec::new();
    let mut wait_candidates = Vec::new();
    for event in control_events {
        match event.kind {
            EventKind::ToolCallDecision => {
                let payload = match serde_json::from_value::<ToolCallDecisionPayload>(
                    event.payload.clone(),
                ) {
                    Ok(payload) => payload,
                    Err(err) if provenance_reaches_request(&event, &request) => {
                        return Ok(ToolCallDecision::Deny {
                            consumed_fact_id: Some(event.id),
                            reason: format!("malformed tool.call.decision fact: {err}"),
                            fail_closed: true,
                        });
                    }
                    Err(_) => continue,
                };
                if payload.subject != request.subject || payload.snapshot_id != request.snapshot_id
                {
                    continue;
                }
                if !fresh_control_fact(&event, &request) {
                    continue;
                }
                terminal_candidates.push(tool_decision_from_payload(event.id, payload));
            }
            EventKind::ToolCallSuspended => {
                let payload =
                    match serde_json::from_value::<ToolCallSuspendedPayload>(event.payload.clone())
                    {
                        Ok(payload) => payload,
                        Err(err) if provenance_reaches_request(&event, &request) => {
                            return Ok(ToolCallDecision::Deny {
                                consumed_fact_id: Some(event.id),
                                reason: format!("malformed tool.call.suspended fact: {err}"),
                                fail_closed: true,
                            });
                        }
                        Err(_) => continue,
                    };
                if payload.subject != request.subject || payload.snapshot_id != request.snapshot_id
                {
                    continue;
                }
                if !fresh_control_fact(&event, &request) {
                    continue;
                }
                wait_candidates.push(ToolCallDecision::Wait {
                    consumed_fact_id: event.id,
                    approval_id: payload.approval_id,
                    reason: payload.reason,
                });
            }
            _ => {}
        }
    }
    match terminal_candidates.len() {
        0 => {}
        1 => return Ok(terminal_candidates.remove(0)),
        _ => {
            return Ok(ToolCallDecision::Deny {
                consumed_fact_id: None,
                reason: "conflicting terminal tool control facts".to_string(),
                fail_closed: true,
            });
        }
    }
    match wait_candidates.len() {
        0 => Ok(ToolCallDecision::NoDecision),
        1 => Ok(wait_candidates.remove(0)),
        _ => Ok(ToolCallDecision::Deny {
            consumed_fact_id: None,
            reason: "conflicting tool suspension facts".to_string(),
            fail_closed: true,
        }),
    }
}

pub async fn active_tool_controller_for_request<S: EventStore + ?Sized>(
    store: &S,
    coordinates: &ThreadCoordinates,
    tool_name: &str,
) -> VerletResult<Option<ToolControllerBinding>> {
    let Some((_, receipt)) = active_manifest_bind_receipt(store, coordinates).await? else {
        return Ok(None);
    };
    let snapshot_id = receipt.manifest_hash;
    for coupling in receipt.couplings {
        if coupling_matches_tool_request(&coupling, tool_name) {
            return Ok(Some(ToolControllerBinding {
                coupling_id: coupling.id,
                snapshot_id: snapshot_id.clone(),
            }));
        }
    }
    Ok(None)
}

pub async fn active_manifest_bind_receipt<S: EventStore + ?Sized>(
    store: &S,
    coordinates: &ThreadCoordinates,
) -> VerletResult<Option<(EventRecordId, AgentManifestBindReceipt)>> {
    let thread_events = store
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| VerletError::History(err.to_string()))?;
    let Some(event) = thread_events
        .into_iter()
        .filter(|event| event.kind == EventKind::ManifestBindCompleted)
        .max_by_key(|event| event.sequence.get())
    else {
        return Ok(None);
    };
    let receipt =
        serde_json::from_value::<AgentManifestBindReceipt>(event.payload).map_err(|err| {
            VerletError::History(format!("manifest.bind.completed payload is invalid: {err}"))
        })?;
    Ok(Some((event.id, receipt)))
}

pub async fn list_pending_tool_call_suspensions<S: EventStore + ?Sized>(
    store: &S,
    coordinates: &ThreadCoordinates,
) -> VerletResult<Vec<PendingToolCallSuspension>> {
    let control_events = store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .map_err(|err| VerletError::History(err.to_string()))?;
    let thread_events = store
        .read_events(&EventStreamId::for_thread(coordinates), None)
        .await
        .map_err(|err| VerletError::History(err.to_string()))?;
    let mut terminal_subjects = BTreeSet::<(ToolCallSubject, String)>::new();
    for event in control_events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallDecision)
    {
        let payload = serde_json::from_value::<ToolCallDecisionPayload>(event.payload.clone())
            .map_err(|err| {
                VerletError::History(format!("tool.call.decision payload is invalid: {err}"))
            })?;
        terminal_subjects.insert((payload.subject, payload.snapshot_id));
    }
    let mut completions = Vec::new();
    for event in thread_events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallCompleted)
    {
        let payload = serde_json::from_value::<ToolCallCompletedPayload>(event.payload.clone())
            .map_err(|err| {
                VerletError::History(format!("tool.call.completed payload is invalid: {err}"))
            })?;
        completions.push(payload);
    }

    let mut pending = Vec::new();
    for event in control_events
        .into_iter()
        .filter(|event| event.kind == EventKind::ToolCallSuspended)
    {
        let payload = serde_json::from_value::<ToolCallSuspendedPayload>(event.payload.clone())
            .map_err(|err| {
                VerletError::History(format!("tool.call.suspended payload is invalid: {err}"))
            })?;
        let request_event_id = event.provenance.source_event_ids.first().copied();
        let request = request_event_id
            .and_then(|request_event_id| {
                thread_events.iter().find(|event| {
                    event.id == request_event_id && event.kind == EventKind::ToolCallRequested
                })
            })
            .map(|request_event| {
                serde_json::from_value::<ToolCallRequestedPayload>(request_event.payload.clone())
                    .map_err(|err| {
                        VerletError::History(format!(
                            "tool.call.requested payload is invalid: {err}"
                        ))
                    })
            })
            .transpose()?;
        let completed = if let Some(request) = request {
            completions.iter().any(|completion| {
                completion.subject == request.subject
                    && completion.snapshot_id == request.snapshot_id
                    && completion.args_fingerprint == request.args_fingerprint
            })
        } else {
            // Legacy/manual suspension facts may not identify their request in
            // provenance. Preserve the former subject+snapshot terminal check
            // only for that unresolved case; fingerprinted requests use the
            // generation-aware path above.
            completions.iter().any(|completion| {
                completion.subject == payload.subject
                    && completion.snapshot_id == payload.snapshot_id
            })
        };
        if completed
            || terminal_subjects.contains(&(payload.subject.clone(), payload.snapshot_id.clone()))
        {
            continue;
        }
        pending.push(PendingToolCallSuspension {
            suspended_event_id: event.id,
            subject: payload.subject,
            snapshot_id: payload.snapshot_id,
            request_event_id,
            approval_id: payload.approval_id,
            reason: payload.reason,
        });
    }
    pending.sort_by_key(|pending| pending.suspended_event_id.to_string());
    Ok(pending)
}

pub async fn decide_turn_continuation<S: EventStore + ?Sized>(
    store: &S,
    request: TurnContinuationDecisionRequest,
) -> VerletResult<TurnContinuationDecision> {
    let control_events = store
        .read_events(&control_stream_id(&request.coordinates), None)
        .await
        .map_err(|err| VerletError::History(err.to_string()))?;
    let mut candidates = Vec::new();
    for event in &control_events {
        if event.kind != EventKind::TurnContinueRequested {
            continue;
        }
        let payload =
            match serde_json::from_value::<TurnContinueRequestedPayload>(event.payload.clone()) {
                Ok(payload) => payload,
                Err(err) if provenance_reaches_event(event, request.request_event_id) => {
                    return Ok(TurnContinuationDecision::Reject {
                        consumed_request_id: Some(event.id),
                        reason: format!("malformed turn.continue.requested fact: {err}"),
                        fail_closed: true,
                    });
                }
                Err(_) => continue,
            };
        if payload.subject != request.subject || payload.snapshot_id != request.snapshot_id {
            continue;
        }
        if !fresh_control_event(event, request.request_event_id) {
            continue;
        }
        candidates.push((event.id, payload));
    }

    let (consumed_request_id, continuation) = match candidates.len() {
        0 => return Ok(TurnContinuationDecision::NoRequest),
        1 => candidates.remove(0),
        _ => {
            return Ok(TurnContinuationDecision::Reject {
                consumed_request_id: None,
                reason: "conflicting turn.continue.requested facts".to_string(),
                fail_closed: true,
            });
        }
    };

    let Some(mandate) = latest_matching_mandate(&control_events, &request)? else {
        return Ok(TurnContinuationDecision::Reject {
            consumed_request_id: Some(consumed_request_id),
            reason: "continuation has no active mandate".to_string(),
            fail_closed: false,
        });
    };
    if let Some(reason) = mandate_rejection_reason(&control_events, &request, &mandate)? {
        return Ok(TurnContinuationDecision::Reject {
            consumed_request_id: Some(consumed_request_id),
            reason,
            fail_closed: false,
        });
    }
    if let Some(max) = mandate.max_continuations
        && request.completed_continuations >= max
    {
        return Ok(TurnContinuationDecision::Reject {
            consumed_request_id: Some(consumed_request_id),
            reason: "continuation mandate budget exhausted".to_string(),
            fail_closed: false,
        });
    }
    if let Some(expires_at_ms) = mandate.expires_at_ms
        && request.now_ms > expires_at_ms
    {
        let expires_at = Utc
            .timestamp_millis_opt(expires_at_ms)
            .single()
            .map(|instant| instant.to_rfc3339_opts(SecondsFormat::Millis, true));
        return Ok(TurnContinuationDecision::Reject {
            consumed_request_id: Some(consumed_request_id),
            reason: match expires_at {
                Some(expires_at) => {
                    format!("continuation mandate expired at {expires_at_ms} ({expires_at})")
                }
                None => format!("continuation mandate expired at {expires_at_ms}"),
            },
            fail_closed: false,
        });
    }

    Ok(TurnContinuationDecision::Accept {
        consumed_request_id,
        mandate_id: mandate.mandate_id,
        next_turn_input: continuation.next_turn_input,
    })
}

pub async fn decide_placement<S: EventStore + ?Sized>(
    store: &S,
    request: PlacementDecisionRequest,
) -> VerletResult<PlacementDecision> {
    let control_events = store
        .read_events(&control_stream_id(&request.coordinates), None)
        .await
        .map_err(|err| VerletError::History(err.to_string()))?;
    let mut candidates = Vec::new();
    for event in control_events {
        if event.kind != EventKind::PlacementDecision {
            continue;
        }
        let payload =
            match serde_json::from_value::<PlacementDecisionPayload>(event.payload.clone()) {
                Ok(payload) => payload,
                Err(err) if provenance_reaches_event(&event, request.request_event_id) => {
                    return Ok(PlacementDecision::Deny {
                        consumed_fact_id: Some(event.id),
                        reason: format!("malformed placement.decision fact: {err}"),
                        fail_closed: true,
                    });
                }
                Err(_) => continue,
            };
        if payload.subject != request.subject || payload.snapshot_id != request.snapshot_id {
            continue;
        }
        if !fresh_control_event(&event, request.request_event_id) {
            continue;
        }
        candidates.push((event.id, payload));
    }

    let (consumed_fact_id, payload) = match candidates.len() {
        0 => {
            return Ok(PlacementDecision::Default {
                target: request.default_target,
            });
        }
        1 => candidates.remove(0),
        _ => {
            return Ok(PlacementDecision::Deny {
                consumed_fact_id: None,
                reason: "conflicting placement.decision facts".to_string(),
                fail_closed: true,
            });
        }
    };
    if !request.allowed_targets.contains(&payload.placement) {
        return Ok(PlacementDecision::Deny {
            consumed_fact_id: Some(consumed_fact_id),
            reason: format!(
                "placement {:?} is not allowed for this invocation",
                payload.placement
            ),
            fail_closed: true,
        });
    }
    Ok(PlacementDecision::Selected {
        consumed_fact_id,
        target: payload.placement,
    })
}

pub fn control_stream_id(coordinates: &ThreadCoordinates) -> EventStreamId {
    EventStreamId::new(format!("control:{}", coordinates.thread_id))
}

fn tool_decision_from_payload(
    consumed_fact_id: EventRecordId,
    payload: ToolCallDecisionPayload,
) -> ToolCallDecision {
    match payload.outcome {
        ToolCallDecisionOutcomePayload::Allow => ToolCallDecision::Allow { consumed_fact_id },
        ToolCallDecisionOutcomePayload::Rewrite { arguments } => ToolCallDecision::Rewrite {
            consumed_fact_id,
            arguments,
        },
        ToolCallDecisionOutcomePayload::Deny { reason } => ToolCallDecision::Deny {
            consumed_fact_id: Some(consumed_fact_id),
            reason,
            fail_closed: false,
        },
    }
}

fn fresh_control_fact(event: &EventRecord, request: &ToolDecisionRequest) -> bool {
    fresh_control_event(event, request.request_event_id)
}

fn provenance_reaches_request(event: &EventRecord, request: &ToolDecisionRequest) -> bool {
    provenance_reaches_event(event, request.request_event_id)
}

fn fresh_control_event(event: &EventRecord, request_event_id: EventRecordId) -> bool {
    match event.origin {
        EventOrigin::Witnessed => true,
        EventOrigin::Discharged => provenance_reaches_event(event, request_event_id),
    }
}

fn provenance_reaches_event(event: &EventRecord, request_event_id: EventRecordId) -> bool {
    event
        .provenance
        .source_event_ids
        .contains(&request_event_id)
}

fn coupling_matches_tool_request(coupling: &AgentManifestCouplingBinding, tool_name: &str) -> bool {
    coupling.role == CouplingRole::Controller
        && coupling.trigger_kind == EventKind::ToolCallRequested.as_str()
        && coupling.sink_stream == "control"
        && coupling.sink_kinds.iter().any(|kind| {
            kind == EventKind::ToolCallDecision.as_str()
                || kind == EventKind::ToolCallSuspended.as_str()
        })
        && coupling.trigger_match.iter().all(|(key, expected)| {
            matches!(key.as_str(), "tool" | "tool_name" | "name")
                && expected.as_str() == Some(tool_name)
        })
}

fn latest_matching_mandate(
    events: &[EventRecord],
    request: &TurnContinuationDecisionRequest,
) -> VerletResult<Option<MandateStartedPayload>> {
    let mut matching = Vec::new();
    for event in events {
        if event.kind != EventKind::MandateStarted || event.origin != EventOrigin::Witnessed {
            continue;
        }
        let payload = match serde_json::from_value::<MandateStartedPayload>(event.payload.clone()) {
            Ok(payload) => payload,
            Err(err) => {
                return Err(VerletError::History(format!(
                    "mandate.started payload is invalid: {err}"
                )));
            }
        };
        if payload.subject.loop_id.as_deref() != Some(request.subject.loop_id.as_str())
            || payload.snapshot_id != request.snapshot_id
        {
            continue;
        }
        let request_thread_id = request.coordinates.thread_id.to_string();
        if payload
            .subject
            .thread_id
            .as_deref()
            .or(payload.thread_id.as_deref())
            .map(|thread_id| thread_id == request_thread_id)
            .unwrap_or(true)
        {
            matching.push((event.sequence.get(), payload));
        }
    }
    Ok(matching
        .into_iter()
        .max_by_key(|(sequence, _)| *sequence)
        .map(|(_, payload)| payload))
}

fn mandate_rejection_reason(
    events: &[EventRecord],
    request: &TurnContinuationDecisionRequest,
    mandate: &MandateStartedPayload,
) -> VerletResult<Option<String>> {
    for event in events {
        if event.kind != EventKind::MandateRevoked || event.origin != EventOrigin::Witnessed {
            continue;
        }
        let payload = match serde_json::from_value::<MandateRevokedPayload>(event.payload.clone()) {
            Ok(payload) => payload,
            Err(err) => {
                return Err(VerletError::History(format!(
                    "mandate.revoked payload is invalid: {err}"
                )));
            }
        };
        if payload.subject.loop_id.as_deref() == Some(request.subject.loop_id.as_str())
            && payload.snapshot_id == request.snapshot_id
            && payload.mandate_id == mandate.mandate_id
        {
            return Ok(Some(
                payload
                    .reason
                    .unwrap_or_else(|| "continuation mandate revoked".to_string()),
            ));
        }
    }
    Ok(None)
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AgentManifestBindReceipt, AgentManifestCouplingBinding, AgentManifestCouplingBudget,
        AgentManifestRuntimeDefaults, CouplingRole, EventKind, EventProvenance, EventStore,
        EventStreamId, InMemorySessionStore, NewEventRecord, ThreadCoordinates,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn decision_payload_admissible_is_additive_optional() {
        let tool_without: ToolCallDecisionPayload = serde_json::from_value(json!({
            "subject": {"turn_id": "turn-1", "call_id": "call-1"},
            "snapshot_id": "snapshot-a",
            "outcome": {"decision": "allow"}
        }))
        .unwrap();
        assert_eq!(tool_without.admissible, None);
        assert!(serde_json::to_value(&tool_without).unwrap()["admissible"].is_null());

        let tool_with: ToolCallDecisionPayload = serde_json::from_value(json!({
            "subject": {"turn_id": "turn-1", "call_id": "call-1"},
            "snapshot_id": "snapshot-a",
            "outcome": {"decision": "deny", "reason": "blocked"},
            "admissible": ["allow", "rewrite", "deny"]
        }))
        .unwrap();
        assert_eq!(
            tool_with.admissible,
            Some(vec![
                "allow".to_string(),
                "rewrite".to_string(),
                "deny".to_string()
            ])
        );
        assert_eq!(
            serde_json::to_value(&tool_with).unwrap()["admissible"],
            json!(["allow", "rewrite", "deny"])
        );

        let accepted: TurnContinuationAcceptedPayload = serde_json::from_value(json!({
            "subject": {"loop_id": "loop-1", "parent_turn_id": "turn-1"},
            "snapshot_id": "snapshot-a",
            "mandate_id": "mandate-1",
            "next_turn_id": "turn-2",
            "admissible": ["accepted", "rejected"]
        }))
        .unwrap();
        assert_eq!(
            accepted.admissible,
            Some(vec!["accepted".to_string(), "rejected".to_string()])
        );

        let rejected: TurnContinuationRejectedPayload = serde_json::from_value(json!({
            "subject": {"loop_id": "loop-1", "parent_turn_id": "turn-1"},
            "snapshot_id": "snapshot-a",
            "reason": "budget exhausted"
        }))
        .unwrap();
        assert_eq!(rejected.admissible, None);
    }

    #[test]
    fn tool_request_holds_and_completion_finish_order_are_decode_compatible() {
        let legacy_request: ToolCallRequestedPayload = serde_json::from_value(json!({
            "subject": {"turn_id": "turn-1", "call_id": "call-1"},
            "snapshot_id": "snapshot-a",
            "tool_name": "thread_submit",
            "arguments": {"task_name": "worker-a"}
        }))
        .unwrap();
        assert!(legacy_request.holds.is_empty());
        assert_eq!(legacy_request.args_fingerprint, None);
        assert!(tool_invocation_fingerprint_matches(
            "snapshot-a",
            None,
            "snapshot-a",
            Some("sha256:new"),
        ));
        assert!(!tool_invocation_fingerprint_matches(
            "snapshot-a",
            Some("sha256:old"),
            "snapshot-a",
            Some("sha256:new"),
        ));
        assert!(!tool_invocation_fingerprint_matches(
            "snapshot-a",
            None,
            "snapshot-b",
            None,
        ));

        #[derive(Deserialize)]
        struct LegacyToolCallRequestedPayload {
            subject: ToolCallSubject,
            snapshot_id: String,
            tool_name: String,
            arguments: JsonValue,
        }

        let new_request = serde_json::to_value(ToolCallRequestedPayload {
            subject: legacy_request.subject.clone(),
            snapshot_id: legacy_request.snapshot_id.clone(),
            tool_name: legacy_request.tool_name.clone(),
            arguments: legacy_request.arguments.clone(),
            args_fingerprint: Some(format!("sha256:{}", "a".repeat(64))),
            holds: vec![json!({
                "key": {"kind": "kernel_thread", "task_name": "worker-a"},
                "access": "exclusive"
            })],
        })
        .unwrap();
        let decoded_by_old_reader: LegacyToolCallRequestedPayload =
            serde_json::from_value(new_request).unwrap();
        assert_eq!(decoded_by_old_reader.subject, legacy_request.subject);
        assert_eq!(decoded_by_old_reader.snapshot_id, "snapshot-a");
        assert_eq!(decoded_by_old_reader.tool_name, "thread_submit");
        assert_eq!(
            decoded_by_old_reader.arguments,
            json!({"task_name": "worker-a"})
        );

        let legacy_completion: ToolCallCompletedPayload = serde_json::from_value(json!({
            "subject": {"turn_id": "turn-1", "call_id": "call-1"},
            "snapshot_id": "snapshot-a",
            "tool_name": "thread_submit",
            "success": true,
            "duration_ms": 4
        }))
        .unwrap();
        assert_eq!(legacy_completion.finish_order, None);
        assert_eq!(legacy_completion.cancellation, None);
        assert_eq!(legacy_completion.args_fingerprint, None);

        let cancelled = serde_json::to_value(ToolCallCompletedPayload {
            args_fingerprint: Some(format!("sha256:{}", "a".repeat(64))),
            cancellation: Some(ToolCallCancellation::CancelledExceededGrace),
            ..legacy_completion.clone()
        })
        .unwrap();
        assert_eq!(cancelled["cancellation"], json!("cancelled_exceeded_grace"));

        #[derive(Deserialize)]
        struct LegacyToolCallCompletedPayload {
            subject: ToolCallSubject,
            success: bool,
        }
        let decoded_by_old_reader: LegacyToolCallCompletedPayload =
            serde_json::from_value(cancelled).unwrap();
        assert_eq!(decoded_by_old_reader.subject, legacy_completion.subject);
        assert!(decoded_by_old_reader.success);

        let completed_normally = serde_json::to_value(legacy_completion).unwrap();
        assert!(completed_normally.get("cancellation").is_none());
        assert!(completed_normally.get("args_fingerprint").is_none());
        let requested_normally = serde_json::to_value(legacy_request).unwrap();
        assert!(requested_normally.get("args_fingerprint").is_none());
    }

    #[tokio::test]
    async fn tool_decision_accepts_fresh_allow_fact() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert!(matches!(decision, ToolCallDecision::Allow { .. }));
    }

    #[tokio::test]
    async fn tool_decision_rewrites_with_valid_arguments() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: ToolCallDecisionOutcomePayload::Rewrite {
                    arguments: json!({"cmd": "ls"}),
                },
                admissible: None,
            })
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert_eq!(
            decision,
            ToolCallDecision::Rewrite {
                consumed_fact_id: decision.consumed_fact_id().unwrap(),
                arguments: json!({"cmd": "ls"}),
            }
        );
    }

    #[tokio::test]
    async fn tool_decision_denies_with_reason() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: ToolCallDecisionOutcomePayload::Deny {
                    reason: "dangerous command".to_string(),
                },
                admissible: None,
            })
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            ToolCallDecision::Deny {
                reason,
                fail_closed: false,
                ..
            } if reason == "dangerous command"
        ));
    }

    #[tokio::test]
    async fn tool_suspension_waits_without_terminal_tool_result() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_suspended(ToolCallSuspendedPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                approval_id: Some("approval-1".to_string()),
                reason: Some("needs human".to_string()),
            })
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            ToolCallDecision::Wait {
                approval_id: Some(id),
                ..
            } if id == "approval-1"
        ));
    }

    #[tokio::test]
    async fn stale_snapshot_facts_are_ignored_as_no_decision() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: "old-snapshot".to_string(),
                outcome: ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert_eq!(decision, ToolCallDecision::NoDecision);
    }

    #[tokio::test]
    async fn malformed_matching_fact_fails_closed() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_raw(EventKind::ToolCallDecision, json!({"bad": true}))
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            ToolCallDecision::Deny {
                fail_closed: true,
                reason,
                ..
            } if reason.contains("malformed")
        ));
    }

    #[tokio::test]
    async fn conflicting_terminal_facts_fail_closed() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: ToolCallDecisionOutcomePayload::Deny {
                    reason: "blocked".to_string(),
                },
                admissible: None,
            })
            .await;

        let decision = decide_tool_call(&fixture.store, fixture.request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            ToolCallDecision::Deny {
                fail_closed: true,
                reason,
                ..
            } if reason.contains("conflicting")
        ));
    }

    #[tokio::test]
    async fn active_tool_controller_is_recovered_from_latest_bind_receipt() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_manifest_bind(vec![tool_controller_binding(
                "bash_gate",
                BTreeMap::from([("tool".to_string(), json!("bash"))]),
            )])
            .await;

        let binding =
            active_tool_controller_for_request(&fixture.store, &fixture.coordinates, "bash")
                .await
                .unwrap()
                .expect("controller should match bash");
        let missing =
            active_tool_controller_for_request(&fixture.store, &fixture.coordinates, "python")
                .await
                .unwrap();

        assert_eq!(binding.coupling_id, "bash_gate");
        assert_eq!(binding.snapshot_id, "snapshot-a");
        assert_eq!(missing, None);
    }

    #[tokio::test]
    async fn continuation_request_without_mandate_rejects() {
        let fixture = ToolDecisionFixture::new().await;
        fixture.append_turn_continue("try again").await;

        let decision = decide_turn_continuation(&fixture.store, fixture.continuation_request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            TurnContinuationDecision::Reject {
                reason,
                fail_closed: false,
                ..
            } if reason.contains("no active mandate")
        ));
    }

    #[tokio::test]
    async fn continuation_request_with_mandate_accepts() {
        let fixture = ToolDecisionFixture::new().await;
        fixture.append_turn_continue("try again").await;
        fixture
            .append_mandate_started(MandateStartedPayload {
                subject: MandateSubject {
                    thread_id: None,
                    loop_id: Some("loop-1".to_string()),
                },
                mandate_id: "mandate-1".to_string(),
                snapshot_id: fixture.snapshot_id.clone(),
                thread_id: Some(fixture.coordinates.thread_id.to_string()),
                max_continuations: Some(2),
                expires_at_ms: Some(10_000),
                schedule: None,
                max_occurrences: None,
                catch_up: None,
                input_template: None,
            })
            .await;

        let decision = decide_turn_continuation(&fixture.store, fixture.continuation_request())
            .await
            .unwrap();

        assert_eq!(
            decision,
            TurnContinuationDecision::Accept {
                consumed_request_id: decision_consumed_request(&decision).unwrap(),
                mandate_id: "mandate-1".to_string(),
                next_turn_input: "try again".to_string(),
            }
        );
    }

    #[tokio::test]
    async fn continuation_request_after_mandate_expiry_rejects_and_names_the_lapse() {
        let fixture = ToolDecisionFixture::new().await;
        fixture.append_turn_continue("try again").await;
        fixture
            .append_mandate_started(MandateStartedPayload {
                subject: MandateSubject {
                    thread_id: None,
                    loop_id: Some("loop-1".to_string()),
                },
                mandate_id: "mandate-1".to_string(),
                snapshot_id: fixture.snapshot_id.clone(),
                thread_id: Some(fixture.coordinates.thread_id.to_string()),
                max_continuations: None,
                expires_at_ms: Some(999),
                schedule: None,
                max_occurrences: None,
                catch_up: None,
                input_template: None,
            })
            .await;

        let decision = decide_turn_continuation(&fixture.store, fixture.continuation_request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            TurnContinuationDecision::Reject {
                consumed_request_id: Some(_),
                reason,
                fail_closed: false,
            } if reason == "continuation mandate expired at 999 (1970-01-01T00:00:00.999Z)"
        ));
    }

    #[tokio::test]
    async fn revoked_mandate_rejects_continuation() {
        let fixture = ToolDecisionFixture::new().await;
        fixture.append_turn_continue("try again").await;
        fixture
            .append_mandate_started(MandateStartedPayload {
                subject: MandateSubject {
                    thread_id: None,
                    loop_id: Some("loop-1".to_string()),
                },
                mandate_id: "mandate-1".to_string(),
                snapshot_id: fixture.snapshot_id.clone(),
                thread_id: None,
                max_continuations: None,
                expires_at_ms: None,
                schedule: None,
                max_occurrences: None,
                catch_up: None,
                input_template: None,
            })
            .await;
        fixture
            .append_mandate_revoked(MandateRevokedPayload {
                subject: MandateSubject {
                    thread_id: None,
                    loop_id: Some("loop-1".to_string()),
                },
                mandate_id: "mandate-1".to_string(),
                mandate_event_id: None,
                snapshot_id: fixture.snapshot_id.clone(),
                reason: Some("operator stopped loop".to_string()),
            })
            .await;

        let decision = decide_turn_continuation(&fixture.store, fixture.continuation_request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            TurnContinuationDecision::Reject { reason, .. }
                if reason == "operator stopped loop"
        ));
    }

    #[tokio::test]
    async fn placement_defaults_without_controller_and_rejects_invalid_target() {
        let fixture = ToolDecisionFixture::new().await;
        let defaulted = decide_placement(&fixture.store, fixture.placement_request())
            .await
            .unwrap();
        assert_eq!(
            defaulted,
            PlacementDecision::Default {
                target: PlacementTarget::Local
            }
        );

        fixture
            .append_raw(
                EventKind::PlacementDecision,
                serde_json::to_value(PlacementDecisionPayload {
                    subject: PlacementSubject {
                        invocation_id: "invoke-1".to_string(),
                    },
                    snapshot_id: fixture.snapshot_id.clone(),
                    placement: PlacementTarget::Remote,
                })
                .unwrap(),
            )
            .await;

        let decision = decide_placement(&fixture.store, fixture.placement_request())
            .await
            .unwrap();

        assert!(matches!(
            decision,
            PlacementDecision::Deny {
                fail_closed: true,
                reason,
                ..
            } if reason.contains("not allowed")
        ));
    }

    #[tokio::test]
    async fn pending_tool_suspension_is_rebuilt_from_control_stream() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_suspended(ToolCallSuspendedPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                approval_id: Some("approval-1".to_string()),
                reason: Some("needs human".to_string()),
            })
            .await;

        let pending = list_pending_tool_call_suspensions(&fixture.store, &fixture.coordinates)
            .await
            .unwrap();

        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].subject, fixture.subject);
        assert_eq!(pending[0].approval_id.as_deref(), Some("approval-1"));
        assert_eq!(pending[0].request_event_id, Some(fixture.request_event.id));
    }

    #[tokio::test]
    async fn terminal_tool_decision_closes_pending_suspension() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_suspended(ToolCallSuspendedPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                approval_id: Some("approval-1".to_string()),
                reason: None,
            })
            .await;
        fixture
            .append_decision(ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;

        let pending = list_pending_tool_call_suspensions(&fixture.store, &fixture.coordinates)
            .await
            .unwrap();

        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn legacy_suspension_without_request_provenance_closes_on_subject_completion() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_control_witnessed(
                EventKind::ToolCallSuspended,
                serde_json::to_value(ToolCallSuspendedPayload {
                    subject: fixture.subject.clone(),
                    snapshot_id: fixture.snapshot_id.clone(),
                    approval_id: Some("approval-legacy".to_string()),
                    reason: Some("legacy suspension".to_string()),
                })
                .unwrap(),
            )
            .await;
        fixture
            .store
            .append_events(
                &EventStreamId::for_thread(&fixture.coordinates),
                vec![NewEventRecord::witnessed(
                    fixture.coordinates.clone(),
                    EventKind::ToolCallCompleted,
                    serde_json::to_value(ToolCallCompletedPayload {
                        subject: fixture.subject.clone(),
                        snapshot_id: fixture.snapshot_id.clone(),
                        tool_name: "bash".to_string(),
                        success: true,
                        args_fingerprint: None,
                        duration_ms: Some(1),
                        finish_order: None,
                        cancellation: None,
                    })
                    .unwrap(),
                )],
            )
            .await
            .unwrap();

        let pending = list_pending_tool_call_suspensions(&fixture.store, &fixture.coordinates)
            .await
            .unwrap();

        assert!(pending.is_empty());
    }

    struct ToolDecisionFixture {
        store: InMemorySessionStore,
        coordinates: ThreadCoordinates,
        subject: ToolCallSubject,
        snapshot_id: String,
        request_event: crate::EventRecord,
    }

    impl ToolDecisionFixture {
        async fn new() -> Self {
            let store = InMemorySessionStore::default();
            let coordinates = ThreadCoordinates::new("tenant", "user", "session");
            let subject = ToolCallSubject {
                turn_id: "turn-1".to_string(),
                call_id: "call-1".to_string(),
            };
            let snapshot_id = "snapshot-a".to_string();
            let thread_stream = EventStreamId::for_thread(&coordinates);
            let request_event = store
                .append_events(
                    &thread_stream,
                    vec![NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::ToolCallRequested,
                        serde_json::to_value(ToolCallRequestedPayload {
                            subject: subject.clone(),
                            snapshot_id: snapshot_id.clone(),
                            tool_name: "bash".to_string(),
                            arguments: json!({"cmd": "rm -rf /"}),
                            args_fingerprint: None,
                            holds: Vec::new(),
                        })
                        .unwrap(),
                    )],
                )
                .await
                .unwrap()
                .pop()
                .unwrap();
            Self {
                store,
                coordinates,
                subject,
                snapshot_id,
                request_event,
            }
        }

        fn request(&self) -> ToolDecisionRequest {
            ToolDecisionRequest {
                coordinates: self.coordinates.clone(),
                subject: self.subject.clone(),
                snapshot_id: self.snapshot_id.clone(),
                request_event_id: self.request_event.id,
            }
        }

        fn continuation_request(&self) -> TurnContinuationDecisionRequest {
            TurnContinuationDecisionRequest {
                coordinates: self.coordinates.clone(),
                subject: TurnContinuationSubject {
                    loop_id: "loop-1".to_string(),
                    parent_turn_id: "turn-1".to_string(),
                },
                snapshot_id: self.snapshot_id.clone(),
                request_event_id: self.request_event.id,
                now_ms: 1_000,
                completed_continuations: 0,
            }
        }

        fn placement_request(&self) -> PlacementDecisionRequest {
            PlacementDecisionRequest {
                coordinates: self.coordinates.clone(),
                subject: PlacementSubject {
                    invocation_id: "invoke-1".to_string(),
                },
                snapshot_id: self.snapshot_id.clone(),
                request_event_id: self.request_event.id,
                default_target: PlacementTarget::Local,
                allowed_targets: BTreeSet::from([PlacementTarget::Local, PlacementTarget::Sandbox]),
            }
        }

        async fn append_decision(&self, payload: ToolCallDecisionPayload) {
            self.append_raw(
                EventKind::ToolCallDecision,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_suspended(&self, payload: ToolCallSuspendedPayload) {
            self.append_raw(
                EventKind::ToolCallSuspended,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_turn_continue(&self, next_turn_input: &str) {
            self.append_raw(
                EventKind::TurnContinueRequested,
                serde_json::to_value(TurnContinueRequestedPayload {
                    subject: TurnContinuationSubject {
                        loop_id: "loop-1".to_string(),
                        parent_turn_id: "turn-1".to_string(),
                    },
                    snapshot_id: self.snapshot_id.clone(),
                    next_turn_input: next_turn_input.to_string(),
                })
                .unwrap(),
            )
            .await;
        }

        async fn append_mandate_started(&self, payload: MandateStartedPayload) {
            self.append_control_witnessed(
                EventKind::MandateStarted,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_mandate_revoked(&self, payload: MandateRevokedPayload) {
            self.append_control_witnessed(
                EventKind::MandateRevoked,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_manifest_bind(&self, couplings: Vec<AgentManifestCouplingBinding>) {
            let receipt = AgentManifestBindReceipt {
                ref_uri: "agent://test/bash".to_string(),
                manifest_hash: self.snapshot_id.clone(),
                model_profile_id: "default".to_string(),
                model_profile_origin: None,
                provider_id: "test".to_string(),
                model_id: "model".to_string(),
                tool_ids: Vec::new(),
                operation_bindings: Vec::new(),
                skill_packages: Vec::new(),
                skill_discovery: None,
                static_context_segments: Vec::new(),
                tool_universes: Vec::new(),
                couplings,
                granted: Vec::new(),
                grant_bindings: Vec::new(),
                effective_runtime: AgentManifestRuntimeDefaults::default(),
                overridden_keys: Vec::new(),
                placement: None,
                placement_origin: None,
                workspace: None,
                workspace_origin: None,
            };
            self.store
                .append_events(
                    &EventStreamId::for_thread(&self.coordinates),
                    vec![NewEventRecord::discharged(
                        self.coordinates.clone(),
                        EventKind::ManifestBindCompleted,
                        serde_json::to_value(receipt).unwrap(),
                        EventProvenance {
                            source_streams: vec![EventStreamId::for_thread(&self.coordinates)],
                            source_event_ids: vec![self.request_event.id],
                            discharged_by: Some("binder:manifest".to_string()),
                            function: Some("bind/v1".to_string()),
                            ..EventProvenance::default()
                        },
                    )],
                )
                .await
                .unwrap();
        }

        async fn append_raw(&self, kind: EventKind, payload: serde_json::Value) {
            self.store
                .append_events(
                    &control_stream_id(&self.coordinates),
                    vec![NewEventRecord::discharged(
                        self.coordinates.clone(),
                        kind,
                        payload,
                        EventProvenance {
                            source_streams: vec![EventStreamId::for_thread(&self.coordinates)],
                            source_event_ids: vec![self.request_event.id],
                            discharged_by: Some("coupling:test".to_string()),
                            function: Some("op://test/run@sha256:test".to_string()),
                            ..EventProvenance::default()
                        },
                    )],
                )
                .await
                .unwrap();
        }

        async fn append_control_witnessed(&self, kind: EventKind, payload: serde_json::Value) {
            self.store
                .append_events(
                    &control_stream_id(&self.coordinates),
                    vec![NewEventRecord::witnessed(
                        self.coordinates.clone(),
                        kind,
                        payload,
                    )],
                )
                .await
                .unwrap();
        }
    }

    fn tool_controller_binding(
        id: &str,
        trigger_match: BTreeMap<String, serde_json::Value>,
    ) -> AgentManifestCouplingBinding {
        AgentManifestCouplingBinding {
            id: id.to_string(),
            role: CouplingRole::Controller,
            trigger_kind: EventKind::ToolCallRequested.to_string(),
            trigger_match,
            source_streams: vec!["thread".to_string()],
            source_kinds: vec![EventKind::ToolCallRequested.to_string()],
            sink_stream: "control".to_string(),
            sink_kinds: vec![EventKind::ToolCallDecision.to_string()],
            function_ref: "op://policy/bash-gate@sha256:abc".to_string(),
            artifact_hash: "abc".to_string(),
            operation_name: Some("bash_gate".to_string()),
            grants: Vec::new(),
            grant_expiries: Vec::new(),
            budget: AgentManifestCouplingBudget::default(),
            config_hash: "config".to_string(),
        }
    }

    fn decision_consumed_request(decision: &TurnContinuationDecision) -> Option<EventRecordId> {
        match decision {
            TurnContinuationDecision::Accept {
                consumed_request_id,
                ..
            } => Some(*consumed_request_id),
            TurnContinuationDecision::Reject {
                consumed_request_id,
                ..
            } => *consumed_request_id,
            TurnContinuationDecision::NoRequest => None,
        }
    }
}
