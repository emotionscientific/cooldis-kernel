pub async fn session_store_parity_transcript<S: verlet::RuntimeStore + ?Sized>(
    store: &S,
) -> verlet::HistoryResult<crate::support::transcript::NormalizedTranscript> {
    let coordinates = verlet::ThreadCoordinates {
        tenant_id: "parity-tenant".to_string(),
        user_id: "parity-user".to_string(),
        session_id: "parity-session".to_string(),
        thread_id: verlet::ThreadId::parse_str("00000000-0000-0000-0000-000000000048").unwrap(),
    };
    let root = store
        .append(
            &coordinates,
            None,
            verlet::SessionEntryKind::Message {
                message: verlet::CanonicalMessage::user_text_at("first", 1_000),
            },
        )
        .await?;
    let child = store
        .append(
            &coordinates,
            Some(root.entry_id),
            verlet::SessionEntryKind::Message {
                message: verlet::CanonicalMessage::Assistant {
                    content: vec![verlet::CanonicalContent::text("second")],
                    api: verlet::ProviderApi::OpenAIResponses,
                    provider: "parity-provider".to_string(),
                    model: "parity-model".to_string(),
                    usage: verlet::CanonicalUsage::default(),
                    stop_reason: verlet::CanonicalStopReason::EndTurn,
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

    let stream_id = verlet::EventStreamId::new("parity:events");
    let appended = store
        .append_events(
            &stream_id,
            vec![
                event_record(&coordinates, 1, verlet::EventKind::TurnSubmitted, 3_000),
                event_record(&coordinates, 2, verlet::EventKind::TurnCompleted, 4_000),
            ],
        )
        .await?;
    let read = store.read_events(&stream_id, None).await?;
    let cursor = appended[0].cursor_v1();
    let replay = store.read_events_after_cursor(&stream_id, &cursor).await?;
    let fenced = store
        .append_events_fenced(
            &stream_id,
            verlet::EventSequence::new(3),
            vec![event_record(
                &coordinates,
                3,
                verlet::EventKind::LoopCompleted,
                5_000,
            )],
        )
        .await?;
    let append_ack = verlet::StreamAppendAckV1::from_appended(
        stream_id,
        &appended,
        vec![verlet::StreamAckClass::LocalCommitted],
    )?;

    let mut transcript = crate::support::transcript::TypedTranscript::new();
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
    coordinates: &verlet::ThreadCoordinates,
    id: u128,
    kind: verlet::EventKind,
    created_at_ms: i64,
) -> verlet::NewEventRecord {
    verlet::NewEventRecord {
        id: verlet::EventRecordId::from_uuid(uuid::Uuid::from_u128(id)),
        coordinates: coordinates.clone(),
        created_at_ms,
        kind,
        origin: verlet::EventOrigin::Witnessed,
        provenance: verlet::EventProvenance::default(),
        payload: serde_json::json!({"step": id}),
    }
}
