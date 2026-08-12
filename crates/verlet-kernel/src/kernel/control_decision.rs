use chrono::TimeZone as _;
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
pub struct ToolCallSubject {
    pub turn_id: String,
    pub call_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolCallRequestedPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub tool_name: String,
    pub arguments: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attach_event_id: Option<verlet_history::EventRecordId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args_fingerprint: Option<String>,
    /// Kernel-derived resource holds for this invocation. Empty when decoding
    /// events written before hold scheduling existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub holds: Vec<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolCallDecisionPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub outcome: ToolCallDecisionOutcomePayload,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ToolCallDecisionOutcomePayload {
    Allow,
    Rewrite { arguments: serde_json::Value },
    Deny { reason: String },
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ToolCallSuspendedPayload {
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ApprovalSubject {
    pub approval_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ApprovalResolvedPayload {
    pub subject: ApprovalSubject,
    pub snapshot_id: String,
    pub approved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MandateSubject {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub loop_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandateSchedulePayload {
    Cron { expr: String, tz: String },
    Interval { every_ms: u64 },
    At { when: String },
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MandateCatchUpPolicy {
    CoalesceMissed,
    #[default]
    SkipMissed,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct MandateRevokedPayload {
    pub subject: MandateSubject,
    pub mandate_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mandate_event_id: Option<String>,
    pub snapshot_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnContinuationSubject {
    pub loop_id: String,
    pub parent_turn_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnContinueRequestedPayload {
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub next_turn_input: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnContinuationAcceptedPayload {
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub mandate_id: String,
    pub next_turn_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct TurnContinuationRejectedPayload {
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admissible: Option<Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlacementSubject {
    pub invocation_id: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PlacementDecisionPayload {
    pub subject: PlacementSubject,
    pub snapshot_id: String,
    pub placement: PlacementTarget,
}

#[derive(
    Clone,
    Debug,
    Eq,
    Ord,
    PartialEq,
    PartialOrd,
    serde::Serialize,
    serde::Deserialize,
    strum::AsRefStr,
    strum::Display,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum PlacementTarget {
    Local,
    Remote,
    Sandbox,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDecisionRequest {
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub request_event_id: verlet_history::EventRecordId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolControllerBinding {
    pub coupling_id: String,
    pub snapshot_id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PendingToolCallSuspension {
    pub suspended_event_id: verlet_history::EventRecordId,
    pub subject: ToolCallSubject,
    pub snapshot_id: String,
    pub request_event_id: Option<verlet_history::EventRecordId>,
    pub approval_id: Option<String>,
    pub reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ToolCallDecision {
    NoDecision,
    Allow {
        consumed_fact_id: verlet_history::EventRecordId,
    },
    Rewrite {
        consumed_fact_id: verlet_history::EventRecordId,
        arguments: serde_json::Value,
    },
    Deny {
        consumed_fact_id: Option<verlet_history::EventRecordId>,
        reason: String,
        fail_closed: bool,
    },
    Wait {
        consumed_fact_id: verlet_history::EventRecordId,
        approval_id: Option<String>,
        reason: Option<String>,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TurnContinuationDecisionRequest {
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub subject: TurnContinuationSubject,
    pub snapshot_id: String,
    pub request_event_id: verlet_history::EventRecordId,
    pub now_ms: i64,
    pub completed_continuations: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TurnContinuationDecision {
    NoRequest,
    Accept {
        consumed_request_id: verlet_history::EventRecordId,
        mandate_id: String,
        next_turn_input: String,
    },
    Reject {
        consumed_request_id: Option<verlet_history::EventRecordId>,
        reason: String,
        fail_closed: bool,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementDecisionRequest {
    pub coordinates: verlet_runtime_contracts::ThreadCoordinates,
    pub subject: PlacementSubject,
    pub snapshot_id: String,
    pub request_event_id: verlet_history::EventRecordId,
    pub default_target: PlacementTarget,
    pub allowed_targets: std::collections::BTreeSet<PlacementTarget>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum PlacementDecision {
    Default {
        target: PlacementTarget,
    },
    Selected {
        consumed_fact_id: verlet_history::EventRecordId,
        target: PlacementTarget,
    },
    Deny {
        consumed_fact_id: Option<verlet_history::EventRecordId>,
        reason: String,
        fail_closed: bool,
    },
}

impl ToolCallDecision {
    pub fn consumed_fact_id(&self) -> Option<verlet_history::EventRecordId> {
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

pub async fn decide_tool_call<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    request: ToolDecisionRequest,
) -> crate::kernel::runtime_host::VerletResult<ToolCallDecision> {
    let control_events = store
        .read_events(&control_stream_id(&request.coordinates), None)
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut terminal_candidates = Vec::new();
    let mut wait_candidates = Vec::new();
    for event in control_events {
        match event.kind {
            verlet_history::EventKind::ToolCallDecision => {
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
            verlet_history::EventKind::ToolCallSuspended => {
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

pub async fn active_tool_controller_for_request<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    tool_name: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<ToolControllerBinding>> {
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

pub async fn active_manifest_bind_receipt<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<
    Option<(
        verlet_history::EventRecordId,
        crate::agent::manifest_bind::AgentManifestBindReceipt,
    )>,
> {
    let thread_events = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let Some(event) = thread_events
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .max_by_key(|event| event.sequence.get())
    else {
        return Ok(None);
    };
    let receipt = serde_json::from_value::<crate::agent::manifest_bind::AgentManifestBindReceipt>(
        event.payload,
    )
    .map_err(|err| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "manifest.bind.completed payload is invalid: {err}"
        ))
    })?;
    Ok(Some((event.id, receipt)))
}

pub async fn list_pending_tool_call_suspensions<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<Vec<PendingToolCallSuspension>> {
    let control_events = store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let thread_events = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut terminal_subjects = std::collections::BTreeSet::<(ToolCallSubject, String)>::new();
    for event in control_events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallDecision)
    {
        let payload = serde_json::from_value::<ToolCallDecisionPayload>(event.payload.clone())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "tool.call.decision payload is invalid: {err}"
                ))
            })?;
        terminal_subjects.insert((payload.subject, payload.snapshot_id));
    }
    let mut completions = Vec::new();
    for event in thread_events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallCompleted)
    {
        let payload = serde_json::from_value::<ToolCallCompletedPayload>(event.payload.clone())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "tool.call.completed payload is invalid: {err}"
                ))
            })?;
        completions.push(payload);
    }

    let mut pending = Vec::new();
    for event in control_events
        .into_iter()
        .filter(|event| event.kind == verlet_history::EventKind::ToolCallSuspended)
    {
        let payload = serde_json::from_value::<ToolCallSuspendedPayload>(event.payload.clone())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "tool.call.suspended payload is invalid: {err}"
                ))
            })?;
        let request_event_id = event.provenance.source_event_ids.first().copied();
        let request = request_event_id
            .and_then(|request_event_id| {
                thread_events.iter().find(|event| {
                    event.id == request_event_id
                        && event.kind == verlet_history::EventKind::ToolCallRequested
                })
            })
            .map(|request_event| {
                serde_json::from_value::<ToolCallRequestedPayload>(request_event.payload.clone())
                    .map_err(|err| {
                        crate::kernel::runtime_host::VerletError::History(format!(
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

pub async fn decide_turn_continuation<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    request: TurnContinuationDecisionRequest,
) -> crate::kernel::runtime_host::VerletResult<TurnContinuationDecision> {
    let control_events = store
        .read_events(&control_stream_id(&request.coordinates), None)
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut candidates = Vec::new();
    for event in &control_events {
        if event.kind != verlet_history::EventKind::TurnContinueRequested {
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
        let expires_at = chrono::Utc
            .timestamp_millis_opt(expires_at_ms)
            .single()
            .map(|instant| instant.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
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

pub async fn decide_placement<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    request: PlacementDecisionRequest,
) -> crate::kernel::runtime_host::VerletResult<PlacementDecision> {
    let control_events = store
        .read_events(&control_stream_id(&request.coordinates), None)
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut candidates = Vec::new();
    for event in control_events {
        if event.kind != verlet_history::EventKind::PlacementDecision {
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

pub fn control_stream_id(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> verlet_history::EventStreamId {
    verlet_history::EventStreamId::new(format!("control:{}", coordinates.thread_id))
}

fn tool_decision_from_payload(
    consumed_fact_id: verlet_history::EventRecordId,
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

fn fresh_control_fact(event: &verlet_history::EventRecord, request: &ToolDecisionRequest) -> bool {
    fresh_control_event(event, request.request_event_id)
}

fn provenance_reaches_request(
    event: &verlet_history::EventRecord,
    request: &ToolDecisionRequest,
) -> bool {
    provenance_reaches_event(event, request.request_event_id)
}

fn fresh_control_event(
    event: &verlet_history::EventRecord,
    request_event_id: verlet_history::EventRecordId,
) -> bool {
    match event.origin {
        verlet_history::EventOrigin::Witnessed => true,
        verlet_history::EventOrigin::Discharged => {
            provenance_reaches_event(event, request_event_id)
        }
    }
}

fn provenance_reaches_event(
    event: &verlet_history::EventRecord,
    request_event_id: verlet_history::EventRecordId,
) -> bool {
    event
        .provenance
        .source_event_ids
        .contains(&request_event_id)
}

fn coupling_matches_tool_request(
    coupling: &crate::agent::manifest_bind::AgentManifestCouplingBinding,
    tool_name: &str,
) -> bool {
    let requested_kind: &str = verlet_history::EventKind::ToolCallRequested.as_ref();
    let decision_kind: &str = verlet_history::EventKind::ToolCallDecision.as_ref();
    let suspended_kind: &str = verlet_history::EventKind::ToolCallSuspended.as_ref();
    coupling.role == crate::agent::manifest_bind::CouplingRole::Controller
        && coupling.trigger_kind == requested_kind
        && coupling.sink_stream == "control"
        && coupling
            .sink_kinds
            .iter()
            .any(|kind| kind == decision_kind || kind == suspended_kind)
        && coupling.trigger_match.iter().all(|(key, expected)| {
            matches!(key.as_str(), "tool" | "tool_name" | "name")
                && expected.as_str() == Some(tool_name)
        })
}

fn latest_matching_mandate(
    events: &[verlet_history::EventRecord],
    request: &TurnContinuationDecisionRequest,
) -> crate::kernel::runtime_host::VerletResult<Option<MandateStartedPayload>> {
    let mut matching = Vec::new();
    for event in events {
        if event.kind != verlet_history::EventKind::MandateStarted
            || event.origin != verlet_history::EventOrigin::Witnessed
        {
            continue;
        }
        let payload = match serde_json::from_value::<MandateStartedPayload>(event.payload.clone()) {
            Ok(payload) => payload,
            Err(err) => {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
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
    events: &[verlet_history::EventRecord],
    request: &TurnContinuationDecisionRequest,
    mandate: &MandateStartedPayload,
) -> crate::kernel::runtime_host::VerletResult<Option<String>> {
    for event in events {
        if event.kind != verlet_history::EventKind::MandateRevoked
            || event.origin != verlet_history::EventOrigin::Witnessed
        {
            continue;
        }
        let payload = match serde_json::from_value::<MandateRevokedPayload>(event.payload.clone()) {
            Ok(payload) => payload,
            Err(err) => {
                return Err(crate::kernel::runtime_host::VerletError::History(format!(
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
    use verlet_history::EventStore as _;

    #[test]
    fn decision_payload_admissible_is_additive_optional() {
        let tool_without: crate::kernel::control_decision::ToolCallDecisionPayload =
            serde_json::from_value(serde_json::json!({
                "subject": {"turn_id": "turn-1", "call_id": "call-1"},
                "snapshot_id": "snapshot-a",
                "outcome": {"decision": "allow"}
            }))
            .unwrap();
        assert_eq!(tool_without.admissible, None);
        assert!(serde_json::to_value(&tool_without).unwrap()["admissible"].is_null());

        let tool_with: crate::kernel::control_decision::ToolCallDecisionPayload =
            serde_json::from_value(serde_json::json!({
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
            serde_json::json!(["allow", "rewrite", "deny"])
        );

        let accepted: crate::kernel::control_decision::TurnContinuationAcceptedPayload =
            serde_json::from_value(serde_json::json!({
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

        let rejected: crate::kernel::control_decision::TurnContinuationRejectedPayload =
            serde_json::from_value(serde_json::json!({
                "subject": {"loop_id": "loop-1", "parent_turn_id": "turn-1"},
                "snapshot_id": "snapshot-a",
                "reason": "budget exhausted"
            }))
            .unwrap();
        assert_eq!(rejected.admissible, None);
    }

    #[test]
    fn tool_request_holds_and_completion_finish_order_are_decode_compatible() {
        let legacy_request: crate::kernel::control_decision::ToolCallRequestedPayload =
            serde_json::from_value(serde_json::json!({
                "subject": {"turn_id": "turn-1", "call_id": "call-1"},
                "snapshot_id": "snapshot-a",
                "tool_name": "thread_submit",
                "arguments": {"task_name": "worker-a"}
            }))
            .unwrap();
        assert!(legacy_request.holds.is_empty());
        assert_eq!(legacy_request.args_fingerprint, None);
        assert_eq!(legacy_request.attach_event_id, None);
        assert!(
            crate::kernel::control_decision::tool_invocation_fingerprint_matches(
                "snapshot-a",
                None,
                "snapshot-a",
                Some("sha256:new"),
            )
        );
        assert!(
            !crate::kernel::control_decision::tool_invocation_fingerprint_matches(
                "snapshot-a",
                Some("sha256:old"),
                "snapshot-a",
                Some("sha256:new"),
            )
        );
        assert!(
            !crate::kernel::control_decision::tool_invocation_fingerprint_matches(
                "snapshot-a",
                None,
                "snapshot-b",
                None,
            )
        );

        #[derive(serde::Deserialize)]
        struct LegacyToolCallRequestedPayload {
            subject: crate::kernel::control_decision::ToolCallSubject,
            snapshot_id: String,
            tool_name: String,
            arguments: serde_json::Value,
        }

        let new_request =
            serde_json::to_value(crate::kernel::control_decision::ToolCallRequestedPayload {
                subject: legacy_request.subject.clone(),
                snapshot_id: legacy_request.snapshot_id.clone(),
                tool_name: legacy_request.tool_name.clone(),
                arguments: legacy_request.arguments.clone(),
                attach_event_id: Some(verlet_history::EventRecordId::from_uuid(
                    uuid::Uuid::from_u128(42),
                )),
                args_fingerprint: Some(format!("sha256:{}", "a".repeat(64))),
                holds: vec![serde_json::json!({
                    "key": {"kind": "kernel_thread", "task_name": "worker-a"},
                    "access": "exclusive"
                })],
            })
            .unwrap();
        assert_eq!(
            new_request["attach_event_id"],
            serde_json::json!(uuid::Uuid::from_u128(42).to_string())
        );
        let decoded_by_old_reader: LegacyToolCallRequestedPayload =
            serde_json::from_value(new_request).unwrap();
        assert_eq!(decoded_by_old_reader.subject, legacy_request.subject);
        assert_eq!(decoded_by_old_reader.snapshot_id, "snapshot-a");
        assert_eq!(decoded_by_old_reader.tool_name, "thread_submit");
        assert_eq!(
            decoded_by_old_reader.arguments,
            serde_json::json!({"task_name": "worker-a"})
        );

        let legacy_completion: crate::kernel::control_decision::ToolCallCompletedPayload =
            serde_json::from_value(serde_json::json!({
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

        let cancelled =
            serde_json::to_value(crate::kernel::control_decision::ToolCallCompletedPayload {
                args_fingerprint: Some(format!("sha256:{}", "a".repeat(64))),
                cancellation: Some(
                    crate::kernel::control_decision::ToolCallCancellation::CancelledExceededGrace,
                ),
                ..legacy_completion.clone()
            })
            .unwrap();
        assert_eq!(
            cancelled["cancellation"],
            serde_json::json!("cancelled_exceeded_grace")
        );

        #[derive(serde::Deserialize)]
        struct LegacyToolCallCompletedPayload {
            subject: crate::kernel::control_decision::ToolCallSubject,
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
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::Allow { .. }
        ));
    }

    #[tokio::test]
    async fn tool_decision_rewrites_with_valid_arguments() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Rewrite {
                    arguments: serde_json::json!({"cmd": "ls"}),
                },
                admissible: None,
            })
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert_eq!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::Rewrite {
                consumed_fact_id: decision.consumed_fact_id().unwrap(),
                arguments: serde_json::json!({"cmd": "ls"}),
            }
        );
    }

    #[tokio::test]
    async fn tool_decision_denies_with_reason() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Deny {
                    reason: "dangerous command".to_string(),
                },
                admissible: None,
            })
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::Deny {
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
            .append_suspended(crate::kernel::control_decision::ToolCallSuspendedPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                approval_id: Some("approval-1".to_string()),
                reason: Some("needs human".to_string()),
            })
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::Wait {
                approval_id: Some(id),
                ..
            } if id == "approval-1"
        ));
    }

    #[tokio::test]
    async fn stale_snapshot_facts_are_ignored_as_no_decision() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: "old-snapshot".to_string(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert_eq!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::NoDecision
        );
    }

    #[tokio::test]
    async fn malformed_matching_fact_fails_closed() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_raw(
                verlet_history::EventKind::ToolCallDecision,
                serde_json::json!({"bad": true}),
            )
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::Deny {
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
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;
        fixture
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Deny {
                    reason: "blocked".to_string(),
                },
                admissible: None,
            })
            .await;

        let decision =
            crate::kernel::control_decision::decide_tool_call(&fixture.store, fixture.request())
                .await
                .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::ToolCallDecision::Deny {
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
                std::collections::BTreeMap::from([("tool".to_string(), serde_json::json!("bash"))]),
            )])
            .await;

        let binding = crate::kernel::control_decision::active_tool_controller_for_request(
            &fixture.store,
            &fixture.coordinates,
            "bash",
        )
        .await
        .unwrap()
        .expect("controller should match bash");
        let missing = crate::kernel::control_decision::active_tool_controller_for_request(
            &fixture.store,
            &fixture.coordinates,
            "python",
        )
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

        let decision = crate::kernel::control_decision::decide_turn_continuation(
            &fixture.store,
            fixture.continuation_request(),
        )
        .await
        .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::TurnContinuationDecision::Reject {
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
            .append_mandate_started(crate::kernel::control_decision::MandateStartedPayload {
                subject: crate::kernel::control_decision::MandateSubject {
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

        let decision = crate::kernel::control_decision::decide_turn_continuation(
            &fixture.store,
            fixture.continuation_request(),
        )
        .await
        .unwrap();

        assert_eq!(
            decision,
            crate::kernel::control_decision::TurnContinuationDecision::Accept {
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
            .append_mandate_started(crate::kernel::control_decision::MandateStartedPayload {
                subject: crate::kernel::control_decision::MandateSubject {
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

        let decision = crate::kernel::control_decision::decide_turn_continuation(
            &fixture.store,
            fixture.continuation_request(),
        )
        .await
        .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::TurnContinuationDecision::Reject {
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
            .append_mandate_started(crate::kernel::control_decision::MandateStartedPayload {
                subject: crate::kernel::control_decision::MandateSubject {
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
            .append_mandate_revoked(crate::kernel::control_decision::MandateRevokedPayload {
                subject: crate::kernel::control_decision::MandateSubject {
                    thread_id: None,
                    loop_id: Some("loop-1".to_string()),
                },
                mandate_id: "mandate-1".to_string(),
                mandate_event_id: None,
                snapshot_id: fixture.snapshot_id.clone(),
                reason: Some("operator stopped loop".to_string()),
            })
            .await;

        let decision = crate::kernel::control_decision::decide_turn_continuation(
            &fixture.store,
            fixture.continuation_request(),
        )
        .await
        .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::TurnContinuationDecision::Reject { reason, .. }
                if reason == "operator stopped loop"
        ));
    }

    #[tokio::test]
    async fn placement_defaults_without_controller_and_rejects_invalid_target() {
        let fixture = ToolDecisionFixture::new().await;
        let defaulted = crate::kernel::control_decision::decide_placement(
            &fixture.store,
            fixture.placement_request(),
        )
        .await
        .unwrap();
        assert_eq!(
            defaulted,
            crate::kernel::control_decision::PlacementDecision::Default {
                target: crate::kernel::control_decision::PlacementTarget::Local
            }
        );

        fixture
            .append_raw(
                verlet_history::EventKind::PlacementDecision,
                serde_json::to_value(crate::kernel::control_decision::PlacementDecisionPayload {
                    subject: crate::kernel::control_decision::PlacementSubject {
                        invocation_id: "invoke-1".to_string(),
                    },
                    snapshot_id: fixture.snapshot_id.clone(),
                    placement: crate::kernel::control_decision::PlacementTarget::Remote,
                })
                .unwrap(),
            )
            .await;

        let decision = crate::kernel::control_decision::decide_placement(
            &fixture.store,
            fixture.placement_request(),
        )
        .await
        .unwrap();

        assert!(matches!(
            decision,
            crate::kernel::control_decision::PlacementDecision::Deny {
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
            .append_suspended(crate::kernel::control_decision::ToolCallSuspendedPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                approval_id: Some("approval-1".to_string()),
                reason: Some("needs human".to_string()),
            })
            .await;

        let pending = crate::kernel::control_decision::list_pending_tool_call_suspensions(
            &fixture.store,
            &fixture.coordinates,
        )
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
            .append_suspended(crate::kernel::control_decision::ToolCallSuspendedPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                approval_id: Some("approval-1".to_string()),
                reason: None,
            })
            .await;
        fixture
            .append_decision(crate::kernel::control_decision::ToolCallDecisionPayload {
                subject: fixture.subject.clone(),
                snapshot_id: fixture.snapshot_id.clone(),
                outcome: crate::kernel::control_decision::ToolCallDecisionOutcomePayload::Allow,
                admissible: None,
            })
            .await;

        let pending = crate::kernel::control_decision::list_pending_tool_call_suspensions(
            &fixture.store,
            &fixture.coordinates,
        )
        .await
        .unwrap();

        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn legacy_suspension_without_request_provenance_closes_on_subject_completion() {
        let fixture = ToolDecisionFixture::new().await;
        fixture
            .append_control_witnessed(
                verlet_history::EventKind::ToolCallSuspended,
                serde_json::to_value(crate::kernel::control_decision::ToolCallSuspendedPayload {
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
                &verlet_history::EventStreamId::for_thread(&fixture.coordinates),
                vec![verlet_history::NewEventRecord::witnessed(
                    fixture.coordinates.clone(),
                    verlet_history::EventKind::ToolCallCompleted,
                    serde_json::to_value(
                        crate::kernel::control_decision::ToolCallCompletedPayload {
                            subject: fixture.subject.clone(),
                            snapshot_id: fixture.snapshot_id.clone(),
                            tool_name: "bash".to_string(),
                            success: true,
                            args_fingerprint: None,
                            duration_ms: Some(1),
                            finish_order: None,
                            cancellation: None,
                        },
                    )
                    .unwrap(),
                )],
            )
            .await
            .unwrap();

        let pending = crate::kernel::control_decision::list_pending_tool_call_suspensions(
            &fixture.store,
            &fixture.coordinates,
        )
        .await
        .unwrap();

        assert!(pending.is_empty());
    }

    struct ToolDecisionFixture {
        store: verlet_history::InMemorySessionStore,
        coordinates: verlet_runtime_contracts::ThreadCoordinates,
        subject: crate::kernel::control_decision::ToolCallSubject,
        snapshot_id: String,
        request_event: verlet_history::EventRecord,
    }

    impl ToolDecisionFixture {
        async fn new() -> Self {
            let store = verlet_history::InMemorySessionStore::default();
            let coordinates =
                verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
            let subject = crate::kernel::control_decision::ToolCallSubject {
                turn_id: "turn-1".to_string(),
                call_id: "call-1".to_string(),
            };
            let snapshot_id = "snapshot-a".to_string();
            let thread_stream = verlet_history::EventStreamId::for_thread(&coordinates);
            let request_event = store
                .append_events(
                    &thread_stream,
                    vec![verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::ToolCallRequested,
                        serde_json::to_value(
                            crate::kernel::control_decision::ToolCallRequestedPayload {
                                subject: subject.clone(),
                                snapshot_id: snapshot_id.clone(),
                                tool_name: "bash".to_string(),
                                arguments: serde_json::json!({"cmd": "rm -rf /"}),
                                attach_event_id: None,
                                args_fingerprint: None,
                                holds: Vec::new(),
                            },
                        )
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

        fn request(&self) -> crate::kernel::control_decision::ToolDecisionRequest {
            crate::kernel::control_decision::ToolDecisionRequest {
                coordinates: self.coordinates.clone(),
                subject: self.subject.clone(),
                snapshot_id: self.snapshot_id.clone(),
                request_event_id: self.request_event.id,
            }
        }

        fn continuation_request(
            &self,
        ) -> crate::kernel::control_decision::TurnContinuationDecisionRequest {
            crate::kernel::control_decision::TurnContinuationDecisionRequest {
                coordinates: self.coordinates.clone(),
                subject: crate::kernel::control_decision::TurnContinuationSubject {
                    loop_id: "loop-1".to_string(),
                    parent_turn_id: "turn-1".to_string(),
                },
                snapshot_id: self.snapshot_id.clone(),
                request_event_id: self.request_event.id,
                now_ms: 1_000,
                completed_continuations: 0,
            }
        }

        fn placement_request(&self) -> crate::kernel::control_decision::PlacementDecisionRequest {
            crate::kernel::control_decision::PlacementDecisionRequest {
                coordinates: self.coordinates.clone(),
                subject: crate::kernel::control_decision::PlacementSubject {
                    invocation_id: "invoke-1".to_string(),
                },
                snapshot_id: self.snapshot_id.clone(),
                request_event_id: self.request_event.id,
                default_target: crate::kernel::control_decision::PlacementTarget::Local,
                allowed_targets: std::collections::BTreeSet::from([
                    crate::kernel::control_decision::PlacementTarget::Local,
                    crate::kernel::control_decision::PlacementTarget::Sandbox,
                ]),
            }
        }

        async fn append_decision(
            &self,
            payload: crate::kernel::control_decision::ToolCallDecisionPayload,
        ) {
            self.append_raw(
                verlet_history::EventKind::ToolCallDecision,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_suspended(
            &self,
            payload: crate::kernel::control_decision::ToolCallSuspendedPayload,
        ) {
            self.append_raw(
                verlet_history::EventKind::ToolCallSuspended,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_turn_continue(&self, next_turn_input: &str) {
            self.append_raw(
                verlet_history::EventKind::TurnContinueRequested,
                serde_json::to_value(
                    crate::kernel::control_decision::TurnContinueRequestedPayload {
                        subject: crate::kernel::control_decision::TurnContinuationSubject {
                            loop_id: "loop-1".to_string(),
                            parent_turn_id: "turn-1".to_string(),
                        },
                        snapshot_id: self.snapshot_id.clone(),
                        next_turn_input: next_turn_input.to_string(),
                    },
                )
                .unwrap(),
            )
            .await;
        }

        async fn append_mandate_started(
            &self,
            payload: crate::kernel::control_decision::MandateStartedPayload,
        ) {
            self.append_control_witnessed(
                verlet_history::EventKind::MandateStarted,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_mandate_revoked(
            &self,
            payload: crate::kernel::control_decision::MandateRevokedPayload,
        ) {
            self.append_control_witnessed(
                verlet_history::EventKind::MandateRevoked,
                serde_json::to_value(payload).unwrap(),
            )
            .await;
        }

        async fn append_manifest_bind(
            &self,
            couplings: Vec<crate::agent::manifest_bind::AgentManifestCouplingBinding>,
        ) {
            let receipt = crate::agent::manifest_bind::AgentManifestBindReceipt {
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
                effective_runtime:
                    verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default(),
                overridden_keys: Vec::new(),
                placement: None,
                placement_origin: None,
                workspace: None,
                workspace_origin: None,
            };
            self.store
                .append_events(
                    &verlet_history::EventStreamId::for_thread(&self.coordinates),
                    vec![verlet_history::NewEventRecord::discharged(
                        self.coordinates.clone(),
                        verlet_history::EventKind::ManifestBindCompleted,
                        serde_json::to_value(receipt).unwrap(),
                        verlet_history::EventProvenance {
                            source_streams: vec![verlet_history::EventStreamId::for_thread(
                                &self.coordinates,
                            )],
                            source_event_ids: vec![self.request_event.id],
                            discharged_by: Some("binder:manifest".to_string()),
                            function: Some("bind/v1".to_string()),
                            ..verlet_history::EventProvenance::default()
                        },
                    )],
                )
                .await
                .unwrap();
        }

        async fn append_raw(&self, kind: verlet_history::EventKind, payload: serde_json::Value) {
            self.store
                .append_events(
                    &crate::kernel::control_decision::control_stream_id(&self.coordinates),
                    vec![verlet_history::NewEventRecord::discharged(
                        self.coordinates.clone(),
                        kind,
                        payload,
                        verlet_history::EventProvenance {
                            source_streams: vec![verlet_history::EventStreamId::for_thread(
                                &self.coordinates,
                            )],
                            source_event_ids: vec![self.request_event.id],
                            discharged_by: Some("coupling:test".to_string()),
                            function: Some("op://test/run@sha256:test".to_string()),
                            ..verlet_history::EventProvenance::default()
                        },
                    )],
                )
                .await
                .unwrap();
        }

        async fn append_control_witnessed(
            &self,
            kind: verlet_history::EventKind,
            payload: serde_json::Value,
        ) {
            self.store
                .append_events(
                    &crate::kernel::control_decision::control_stream_id(&self.coordinates),
                    vec![verlet_history::NewEventRecord::witnessed(
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
        trigger_match: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> crate::agent::manifest_bind::AgentManifestCouplingBinding {
        crate::agent::manifest_bind::AgentManifestCouplingBinding {
            id: id.to_string(),
            role: crate::agent::manifest_bind::CouplingRole::Controller,
            trigger_kind: verlet_history::EventKind::ToolCallRequested.to_string(),
            trigger_match,
            source_streams: vec!["thread".to_string()],
            source_kinds: vec![verlet_history::EventKind::ToolCallRequested.to_string()],
            sink_stream: "control".to_string(),
            sink_kinds: vec![verlet_history::EventKind::ToolCallDecision.to_string()],
            function_ref: "op://policy/bash-gate@sha256:abc".to_string(),
            artifact_hash: "abc".to_string(),
            operation_name: Some("bash_gate".to_string()),
            budget: verlet_agent::manifest_schema::AgentManifestCouplingBudget::default(),
            config_hash: "config".to_string(),
        }
    }

    fn decision_consumed_request(
        decision: &crate::kernel::control_decision::TurnContinuationDecision,
    ) -> Option<verlet_history::EventRecordId> {
        match decision {
            crate::kernel::control_decision::TurnContinuationDecision::Accept {
                consumed_request_id,
                ..
            } => Some(*consumed_request_id),
            crate::kernel::control_decision::TurnContinuationDecision::Reject {
                consumed_request_id,
                ..
            } => *consumed_request_id,
            crate::kernel::control_decision::TurnContinuationDecision::NoRequest => None,
        }
    }
}
