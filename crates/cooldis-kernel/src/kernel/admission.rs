use crate::agent::manifest_bind::canonical_json_hash;
use crate::kernel::history::{
    AdmissionDecidedPayload, AdmissionDecision, EventKind, EventProvenance, EventRecord,
    EventRecordId, EventStreamId, NewEventRecord,
};
use crate::{CooldisError, CooldisResult, RuntimeThreadHandle};
use serde_json::{Value, json};

pub(crate) const HOST_SUBMIT_SURFACE: &str = "host-submit";
pub(crate) const APP_SERVER_RPC_SURFACE: &str = "app-server-rpc";

const SURFACE_ADMISSION_FUNCTION: &str = "surface_admission/v1";
const ADMISSION_ROUTE_FUNCTION: &str = "admission_route/v1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AdmissionGateContext {
    pub(crate) route_id: String,
    pub(crate) policy_hash: String,
    pub(crate) decision: AdmissionDecision,
    pub(crate) admissible: Option<Vec<AdmissionDecision>>,
    pub(crate) source_ingress_event_ids: Vec<EventRecordId>,
    pub(crate) discharged_by: String,
    pub(crate) function: String,
}

impl AdmissionGateContext {
    pub(crate) fn route_policy(
        route_id: String,
        policy_hash: String,
        decision: AdmissionDecision,
        admissible: Vec<AdmissionDecision>,
        source_ingress_event_ids: Vec<EventRecordId>,
    ) -> Self {
        Self {
            discharged_by: format!("policy:admission_route:{route_id}"),
            function: ADMISSION_ROUTE_FUNCTION.to_string(),
            route_id,
            policy_hash,
            decision,
            admissible: Some(admissible),
            source_ingress_event_ids,
        }
    }

    pub(crate) fn surface_default(
        surface_name: &str,
        source_ingress_event_ids: Vec<EventRecordId>,
    ) -> CooldisResult<Self> {
        let route_id = format!("surface:{surface_name}");
        let policy_hash = canonical_json_hash(&surface_default_policy(&route_id))?;
        Ok(Self {
            route_id,
            policy_hash,
            decision: AdmissionDecision::Queue,
            admissible: Some(vec![AdmissionDecision::Queue]),
            source_ingress_event_ids,
            discharged_by: format!("policy:admission_surface:{surface_name}"),
            function: SURFACE_ADMISSION_FUNCTION.to_string(),
        })
    }
}

/// Appends the single `admission.decided` record for a turn-acceptance boundary.
///
/// This is the admission-as-scheduling law: callers must await this append before
/// enqueueing the turn for runtime execution, and a failed append must abort
/// scheduling so no turn runs without an admission decision on the control
/// stream.
pub(crate) async fn append_admission_decided(
    handle: &RuntimeThreadHandle,
    context: AdmissionGateContext,
) -> CooldisResult<EventRecord> {
    let kind = EventKind::AdmissionDecided;
    let coordinates = handle.context().coordinates.clone();
    let payload = AdmissionDecidedPayload {
        route_id: context.route_id.clone(),
        policy_hash: context.policy_hash.clone(),
        decision: context.decision,
        admissible: context.admissible.clone(),
        source_ingress_event_ids: context.source_ingress_event_ids.clone(),
    };
    let mut value = serde_json::to_value(payload).map_err(|err| {
        CooldisError::History(format!("admission.decided payload codec failed: {err}"))
    })?;
    if let Some(object) = value.as_object_mut() {
        object.insert("schema".to_string(), json!(kind.payload_schema_id()));
    }
    handle
        .append_control_event(NewEventRecord::discharged(
            coordinates.clone(),
            kind,
            value,
            EventProvenance {
                source_streams: vec![EventStreamId::new(format!(
                    "control:{}",
                    coordinates.thread_id
                ))],
                source_event_ids: context.source_ingress_event_ids,
                discharged_by: Some(context.discharged_by),
                function: Some(context.function),
                config_hash: Some(context.policy_hash),
                ..EventProvenance::default()
            },
        ))
        .await
}

fn surface_default_policy(route_id: &str) -> Value {
    json!({
        "schema": "cooldis.admission.surface_policy/1",
        "route_id": route_id,
        "decision": "queue",
        "admissible": ["queue"],
    })
}

#[cfg(test)]
pub(crate) fn assert_admission_precedes_turn_records<'a>(
    control_events: &'a [EventRecord],
    thread_events: &[EventRecord],
) -> &'a EventRecord {
    let admission = control_events
        .iter()
        .find(|event| event.kind.as_str() == "admission.decided")
        .expect("control stream missing admission.decided");
    let turn_events = thread_events
        .iter()
        .filter(|event| event.kind.as_str() == "session.entry.appended")
        .collect::<Vec<_>>();
    assert!(
        !turn_events.is_empty(),
        "thread stream missing executed turn session entry"
    );
    for event in turn_events {
        assert!(
            admission.created_at_ms <= event.created_at_ms,
            "admission.decided at {} must precede executed turn event {} at {}",
            admission.created_at_ms,
            event.id,
            event.created_at_ms
        );
    }
    admission
}

#[cfg(test)]
pub(crate) fn assert_admission_precedes_turn_values<'a>(
    control_events: &'a [Value],
    thread_events: &[Value],
) -> &'a Value {
    let admission = control_events
        .iter()
        .find(|event| event.get("kind").and_then(Value::as_str) == Some("admission.decided"))
        .expect("control stream missing admission.decided");
    let admission_ms = admission
        .get("atMs")
        .and_then(Value::as_i64)
        .expect("admission.decided missing atMs");
    let turn_events = thread_events
        .iter()
        .filter(|event| event.get("kind").and_then(Value::as_str) == Some("session.entry.appended"))
        .collect::<Vec<_>>();
    assert!(
        !turn_events.is_empty(),
        "thread stream missing executed turn session entry"
    );
    for event in turn_events {
        let event_ms = event
            .get("atMs")
            .and_then(Value::as_i64)
            .expect("turn event missing atMs");
        assert!(
            admission_ms <= event_ms,
            "admission.decided at {admission_ms} must precede executed turn event at {event_ms}"
        );
    }
    admission
}
