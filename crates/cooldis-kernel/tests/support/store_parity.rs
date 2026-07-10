use super::kernel_test::{
    CanonicalContent, CanonicalMessage, CanonicalStopReason, CanonicalUsage, EventKind,
    EventOrigin, EventProvenance, EventRecordId, EventSequence, EventStreamId, HistoryResult,
    NewEventRecord, ProviderApi, RuntimeStore, SessionEntryKind, StreamAckClass, StreamAppendAckV1,
    ThreadCoordinates, ThreadId,
};
use super::transcript::{NormalizedTranscript, TypedTranscript};
use serde_json::json;
use uuid::Uuid;

pub async fn session_store_parity_transcript<S: RuntimeStore + ?Sized>(
    store: &S,
) -> HistoryResult<NormalizedTranscript> {
    let coordinates = ThreadCoordinates {
        tenant_id: "parity-tenant".to_string(),
        user_id: "parity-user".to_string(),
        session_id: "parity-session".to_string(),
        thread_id: ThreadId::parse_str("00000000-0000-0000-0000-000000000048").unwrap(),
    };
    let root = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text_at("first", 1_000),
            },
        )
        .await?;
    let child = store
        .append(
            &coordinates,
            Some(root.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::Assistant {
                    content: vec![CanonicalContent::text("second")],
                    api: ProviderApi::OpenAIResponses,
                    provider: "parity-provider".to_string(),
                    model: "parity-model".to_string(),
                    usage: CanonicalUsage::default(),
                    stop_reason: CanonicalStopReason::EndTurn,
                    error_message: None,
                    timestamp_ms: 2_000,
                },
            },
        )
        .await?;
    let active_leaf = store.active_leaf(&coordinates).await?;
    let full_context = store.build_context(&coordinates).await?;
    store
        .select_branch(&coordinates, Some(root.entry_id))
        .await?;
    let root_context = store.build_context(&coordinates).await?;
    store
        .select_branch(&coordinates, Some(child.entry_id))
        .await?;

    let stream_id = EventStreamId::new("parity:events");
    let appended = store
        .append_events(
            &stream_id,
            vec![
                event_record(&coordinates, 1, EventKind::TurnSubmitted, 3_000),
                event_record(&coordinates, 2, EventKind::TurnCompleted, 4_000),
            ],
        )
        .await?;
    let read = store.read_events(&stream_id, None).await?;
    let cursor = appended[0].cursor_v1();
    let replay = store.read_events_after_cursor(&stream_id, &cursor).await?;
    let fenced = store
        .append_events_fenced(
            &stream_id,
            EventSequence::new(3),
            vec![event_record(
                &coordinates,
                3,
                EventKind::LoopCompleted,
                5_000,
            )],
        )
        .await?;
    let append_ack = StreamAppendAckV1::from_appended(
        stream_id,
        &appended,
        vec![StreamAckClass::LocalCommitted],
    )?;

    let mut transcript = TypedTranscript::new();
    transcript.push_receipt("session.append.root", &root);
    transcript.push_receipt("session.append.child", &child);
    transcript.push_receipt("session.active_leaf", &active_leaf);
    transcript.push_receipt("session.context.full", &full_context);
    transcript.push_receipt("session.context.root", &root_context);
    for event in &appended {
        transcript.push_event("events.append", event);
    }
    transcript.push_receipt("events.read", &read);
    transcript.push_receipt("events.cursor", &cursor);
    transcript.push_receipt("events.replay", &replay);
    transcript.push_receipt("events.append_ack", &append_ack);
    for event in &fenced {
        transcript.push_event("events.fenced_append", event);
    }
    Ok(transcript.normalize())
}

fn event_record(
    coordinates: &ThreadCoordinates,
    id: u128,
    kind: EventKind,
    created_at_ms: i64,
) -> NewEventRecord {
    NewEventRecord {
        id: EventRecordId::from_uuid(Uuid::from_u128(id)),
        coordinates: coordinates.clone(),
        created_at_ms,
        kind,
        origin: EventOrigin::Witnessed,
        provenance: EventProvenance::default(),
        payload: json!({"step": id}),
    }
}
