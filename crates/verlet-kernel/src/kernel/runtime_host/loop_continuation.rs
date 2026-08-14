#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LoopContinuationReceipt {
    NoRequest,
    Accepted {
        loop_id: String,
        parent_turn_id: String,
        next_turn_id: String,
        accepted_event_id: verlet_history::EventRecordId,
    },
    Rejected {
        loop_id: String,
        parent_turn_id: String,
        reason: String,
        rejected_event_id: verlet_history::EventRecordId,
    },
}

pub(crate) async fn latest_turn_continue_request(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    loop_id: &str,
    parent_turn_id: &str,
) -> crate::kernel::runtime_host::VerletResult<
    Option<(
        verlet_history::EventRecord,
        crate::kernel::control_decision::TurnContinueRequestedPayload,
    )>,
> {
    let events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut latest = None;
    for event in events {
        if event.kind != verlet_history::EventKind::TurnContinueRequested {
            continue;
        }
        let payload = match serde_json::from_value::<
            crate::kernel::control_decision::TurnContinueRequestedPayload,
        >(event.payload.clone())
        {
            Ok(payload) => payload,
            Err(_) => continue,
        };
        if payload.subject.loop_id != loop_id || payload.subject.parent_turn_id != parent_turn_id {
            continue;
        }
        let sequence = event.sequence.get();
        if latest
            .as_ref()
            .map(|(latest_sequence, _, _)| sequence > *latest_sequence)
            .unwrap_or(true)
        {
            latest = Some((sequence, event, payload));
        }
    }
    Ok(latest.map(|(_, event, payload)| (event, payload)))
}

pub(crate) async fn existing_continuation_receipt(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    subject: &crate::kernel::control_decision::TurnContinuationSubject,
    snapshot_id: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<LoopContinuationReceipt>> {
    let events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut latest = None;
    for event in events {
        let receipt = match event.kind {
            verlet_history::EventKind::TurnContinuationAccepted => {
                let payload = serde_json::from_value::<
                    crate::kernel::control_decision::TurnContinuationAcceptedPayload,
                >(event.payload.clone())
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "turn.continuation.accepted payload is invalid: {err}"
                    ))
                })?;
                if payload.subject != *subject || payload.snapshot_id != snapshot_id {
                    continue;
                }
                LoopContinuationReceipt::Accepted {
                    loop_id: payload.subject.loop_id,
                    parent_turn_id: payload.subject.parent_turn_id,
                    next_turn_id: payload.next_turn_id,
                    accepted_event_id: event.id,
                }
            }
            verlet_history::EventKind::TurnContinuationRejected => {
                let payload = serde_json::from_value::<
                    crate::kernel::control_decision::TurnContinuationRejectedPayload,
                >(event.payload.clone())
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "turn.continuation.rejected payload is invalid: {err}"
                    ))
                })?;
                if payload.subject != *subject || payload.snapshot_id != snapshot_id {
                    continue;
                }
                LoopContinuationReceipt::Rejected {
                    loop_id: payload.subject.loop_id,
                    parent_turn_id: payload.subject.parent_turn_id,
                    reason: payload.reason,
                    rejected_event_id: event.id,
                }
            }
            _ => continue,
        };
        let sequence = event.sequence.get();
        if latest
            .as_ref()
            .map(|(latest_sequence, _)| sequence > *latest_sequence)
            .unwrap_or(true)
        {
            latest = Some((sequence, receipt));
        }
    }
    Ok(latest.map(|(_, receipt)| receipt))
}

pub(crate) async fn append_continuation_accepted_event(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    subject: &crate::kernel::control_decision::TurnContinuationSubject,
    snapshot_id: &str,
    mandate_id: &str,
    next_turn_id: &str,
    consumed_request_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    append_control_discharge(
        store,
        coordinates,
        verlet_history::EventKind::TurnContinuationAccepted,
        serde_json::to_value(
            crate::kernel::control_decision::TurnContinuationAcceptedPayload {
                subject: subject.clone(),
                snapshot_id: snapshot_id.to_string(),
                mandate_id: mandate_id.to_string(),
                next_turn_id: next_turn_id.to_string(),
                admissible: None,
            },
        )
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?,
        consumed_request_id,
        "turn_continuation/v1",
    )
    .await
}

pub(crate) async fn append_continuation_rejected_event(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    subject: &crate::kernel::control_decision::TurnContinuationSubject,
    snapshot_id: &str,
    reason: &str,
    consumed_request_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    append_control_discharge(
        store,
        coordinates,
        verlet_history::EventKind::TurnContinuationRejected,
        serde_json::to_value(
            crate::kernel::control_decision::TurnContinuationRejectedPayload {
                subject: subject.clone(),
                snapshot_id: snapshot_id.to_string(),
                reason: reason.to_string(),
                admissible: None,
            },
        )
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?,
        consumed_request_id,
        "turn_continuation/v1",
    )
    .await
}

async fn append_control_discharge(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    kind: verlet_history::EventKind,
    payload: serde_json::Value,
    source_event_id: verlet_history::EventRecordId,
    function: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    store
        .append_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                kind,
                payload,
                verlet_history::EventProvenance {
                    source_streams: vec![crate::kernel::control_decision::control_stream_id(
                        coordinates,
                    )],
                    source_event_ids: vec![source_event_id],
                    discharged_by: Some("scheduler:turn-continuation".to_string()),
                    function: Some(function.to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(
                "event append returned no record".to_string(),
            )
        })
}

pub(crate) async fn append_loop_turn_submitted_event(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    next_turn_id: &str,
    accepted_event_id: verlet_history::EventRecordId,
) -> crate::kernel::runtime_host::VerletResult<verlet_history::EventRecord> {
    if let Some(existing) = turn_submitted_event(store, coordinates, next_turn_id).await? {
        return Ok(existing);
    }
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({
                    "turn_id": next_turn_id,
                    "source": "turn.continuation.accepted",
                    "accepted_event_id": accepted_event_id.to_string(),
                }),
                verlet_history::EventProvenance {
                    source_streams: vec![crate::kernel::control_decision::control_stream_id(
                        coordinates,
                    )],
                    source_event_ids: vec![accepted_event_id],
                    discharged_by: Some("scheduler:loop-continuation".to_string()),
                    function: Some("turn_submit/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        .into_iter()
        .next()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::History(
                "event append returned no record".to_string(),
            )
        })
}

pub(crate) async fn turn_submitted_event(
    store: &dyn verlet_history::RuntimeStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::EventRecord>> {
    let events = store
        .read_events(
            &verlet_history::EventStreamId::for_thread(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    Ok(events
        .into_iter()
        .filter(|event| {
            event.kind == verlet_history::EventKind::TurnSubmitted
                && event
                    .payload
                    .get("turn_id")
                    .and_then(|value| value.as_str())
                    == Some(turn_id)
        })
        .max_by_key(|event| event.sequence.get()))
}

pub(crate) async fn decide_continuation(
    store: &dyn verlet_history::RuntimeStore,
    request: crate::kernel::control_decision::TurnContinuationDecisionRequest,
) -> crate::kernel::runtime_host::VerletResult<
    crate::kernel::control_decision::TurnContinuationDecision,
> {
    crate::kernel::control_decision::decide_turn_continuation(store, request).await
}
