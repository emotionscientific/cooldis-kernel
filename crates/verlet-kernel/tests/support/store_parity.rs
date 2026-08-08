pub async fn session_store_parity_transcript<S: verlet_history::RuntimeStore + ?Sized>(
    store: &S,
    stale_store: &S,
) -> verlet_history::HistoryResult<crate::support::transcript::NormalizedTranscript> {
    let coordinates = verlet_runtime_contracts::ThreadCoordinates {
        tenant_id: "parity-tenant".to_string(),
        user_id: "parity-user".to_string(),
        session_id: "parity-session".to_string(),
        thread_id: verlet_runtime_contracts::ThreadId::parse_str(
            "00000000-0000-0000-0000-000000000048",
        )
        .unwrap(),
    };
    let root = store
        .append(
            &coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text_at("first", 1_000),
            },
        )
        .await?;
    let child = store
        .append(
            &coordinates,
            Some(root.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::Assistant {
                    content: vec![verlet_history::CanonicalContent::text("second")],
                    api: verlet_history::ProviderApi::OpenAIResponses,
                    provider: "parity-provider".to_string(),
                    model: "parity-model".to_string(),
                    usage: verlet_history::CanonicalUsage::default(),
                    stop_reason: verlet_history::CanonicalStopReason::EndTurn,
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

    let stream_id = verlet_history::EventStreamId::new("parity:events");
    let appended = store
        .append_events(
            &stream_id,
            vec![
                event_record(
                    &coordinates,
                    1,
                    verlet_history::EventKind::TurnSubmitted,
                    3_000,
                ),
                event_record(
                    &coordinates,
                    2,
                    verlet_history::EventKind::TurnCompleted,
                    4_000,
                ),
            ],
        )
        .await?;
    let read = store.read_events(&stream_id, None).await?;
    let cursor = appended[0].cursor_v1();
    let replay = store.read_events_after_cursor(&stream_id, &cursor).await?;
    let fenced = store
        .append_events_fenced(
            &stream_id,
            verlet_history::EventSequence::new(3),
            vec![event_record(
                &coordinates,
                3,
                verlet_history::EventKind::LoopCompleted,
                5_000,
            )],
        )
        .await?;
    let append_ack = verlet_history::StreamAppendAckV1::from_appended(
        stream_id,
        &appended,
        vec![verlet_history::StreamAckClass::LocalCommitted],
    )?;
    let lease_stream = verlet_history::EventStreamId::new("parity:lease-epoch");
    store
        .append_events(
            &lease_stream,
            vec![event_record(
                &coordinates,
                4,
                verlet_history::EventKind::TurnSubmitted,
                6_000,
            )],
        )
        .await?;
    let lease_error = stale_store
        .append_events(
            &lease_stream,
            vec![event_record(
                &coordinates,
                5,
                verlet_history::EventKind::TurnSubmitted,
                7_000,
            )],
        )
        .await
        .unwrap_err();
    let lease_fence = match lease_error {
        verlet_history::HistoryError::StaleLeaseEpoch {
            stream_id,
            presented_epoch,
            minimum_epoch,
        } => serde_json::json!({
            "stream_id": stream_id,
            "presented_epoch": presented_epoch,
            "minimum_epoch": minimum_epoch,
            "events_after_rejection": store.read_events(&lease_stream, None).await?.len(),
        }),
        error => panic!("expected stale lease epoch, got {error}"),
    };

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
    transcript.push_receipt("events.lease_epoch_fence", &lease_fence);
    Ok(transcript.normalize())
}

fn event_record(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    id: u128,
    kind: verlet_history::EventKind,
    created_at_ms: i64,
) -> verlet_history::NewEventRecord {
    verlet_history::NewEventRecord {
        id: verlet_history::EventRecordId::from_uuid(uuid::Uuid::from_u128(id)),
        coordinates: coordinates.clone(),
        created_at_ms,
        kind,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({"step": id}),
    }
}
