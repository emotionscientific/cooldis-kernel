use super::SqliteSessionStore;
use cooldis_history::*;
use cooldis_runtime_contracts::ThreadCoordinates;
use rusqlite::params;
use std::time::{SystemTime, UNIX_EPOCH};

fn coords(tenant: &str, user: &str, session: &str) -> ThreadCoordinates {
    ThreadCoordinates::new(tenant, user, session)
}

fn message_texts(messages: &[CanonicalMessage]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| match message {
            CanonicalMessage::User { content, .. }
            | CanonicalMessage::Assistant { content, .. }
            | CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

async fn assert_fenced_append_conformance(store: &dyn EventStore) -> EventStreamId {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let record = |entry_id: &str| {
        NewEventRecord::witnessed(
            coordinates.clone(),
            EventKind::SessionEntryAppended,
            serde_json::json!({"entry_id": entry_id}),
        )
    };

    let initial = store
        .append_events_fenced(
            &stream_id,
            EventSequence::new(1),
            vec![record("entry-1"), record("entry-2")],
        )
        .await
        .unwrap();
    assert_eq!(
        initial
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    store
        .append_events(&stream_id, vec![record("competing-entry")])
        .await
        .unwrap();
    let before_conflict =
        serde_json::to_vec(&store.read_events(&stream_id, None).await.unwrap()).unwrap();

    let err = store
        .append_events_fenced(
            &stream_id,
            EventSequence::new(3),
            vec![record("losing-entry-1"), record("losing-entry-2")],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        HistoryError::AppendFenceConflict {
            stream_id: conflict_stream,
            expected_next_sequence: 3,
            actual_next_sequence: 4,
        } if conflict_stream == stream_id
    ));

    let after_conflict =
        serde_json::to_vec(&store.read_events(&stream_id, None).await.unwrap()).unwrap();
    assert_eq!(after_conflict, before_conflict);

    let duplicate_stream = EventStreamId::new("duplicate-id-stream");
    let mut duplicate = record("duplicate-event-id");
    let duplicate_event_id = initial[0].id;
    duplicate.id = duplicate_event_id;
    let duplicate_err = store
        .append_events_fenced(&duplicate_stream, EventSequence::new(1), vec![duplicate])
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate_err,
        HistoryError::DuplicateEventId(event_id) if event_id == duplicate_event_id
    ));
    assert!(
        store
            .read_events(&duplicate_stream, None)
            .await
            .unwrap()
            .is_empty()
    );
    stream_id
}

#[tokio::test]
async fn in_memory_store_honors_fenced_append_conformance() {
    assert_fenced_append_conformance(&InMemorySessionStore::new()).await;
}

#[tokio::test]
async fn sqlite_store_honors_fenced_append_conformance() {
    let path = temp_db_path("cooldis-history-fenced-append");
    let store = SqliteSessionStore::open(&path).unwrap();

    let stream_id = assert_fenced_append_conformance(&store).await;

    drop(store);
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(
        reopened.read_events(&stream_id, None).await.unwrap().len(),
        3
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_store_persists_canonical_history_across_reopen() {
    let path = temp_db_path("cooldis-history-persist");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let assistant = CanonicalMessage::assistant_with_usage(
        "openai",
        ProviderApi::OpenAIResponses,
        "gpt-test",
        vec![CanonicalContent::text("hello back")],
        CanonicalUsage {
            input_tokens: 3,
            output_tokens: 4,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 1,
        },
        CanonicalStopReason::EndTurn,
    );

    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .append(
                &coordinates,
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("hello"),
                },
            )
            .await
            .unwrap();
        store
            .append(
                &coordinates,
                None,
                SessionEntryKind::Message {
                    message: assistant.clone(),
                },
            )
            .await
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let context = reopened.build_context(&coordinates).await.unwrap();
    assert_eq!(
        message_texts(&context.messages),
        vec!["hello", "hello back"]
    );
    assert_eq!(context.entries.len(), 2);
    assert_eq!(context.messages[1], assistant);

    let raw_json = sqlite_entry_json(&path);
    assert!(raw_json.iter().any(|json| json.contains("\"provider\"")));
    assert!(raw_json.iter().any(|json| json.contains("\"openai\"")));
    assert!(
        raw_json
            .iter()
            .all(|json| !json.contains("max_output_tokens") && !json.contains("tool_choice"))
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_rebuilds_selected_branch_from_journal_after_cache_loss_or_corruption() {
    let path = temp_db_path("cooldis-history-branch-selection-rebuild");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let (selected, other, advanced) = {
        let store = SqliteSessionStore::open(&path).unwrap();
        let root = store
            .append(
                &coordinates,
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("root"),
                },
            )
            .await
            .unwrap();
        let selected = store
            .append(
                &coordinates,
                Some(root.entry_id),
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("selected"),
                },
            )
            .await
            .unwrap();
        let other = store
            .append(
                &coordinates,
                Some(root.entry_id),
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("other"),
                },
            )
            .await
            .unwrap();
        store
            .select_branch(&coordinates, Some(selected.entry_id))
            .await
            .unwrap();
        let advanced = store
            .append(
                &coordinates,
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("advanced selected branch"),
                },
            )
            .await
            .unwrap();
        (selected.entry_id, other.entry_id, advanced.entry_id)
    };

    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection.execute("DROP TABLE active_leaves", []).unwrap();
    }
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(
        reopened.active_leaf(&coordinates).await.unwrap(),
        Some(advanced)
    );
    drop(reopened);

    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE active_leaves SET entry_id = ?1 WHERE thread_id = ?2",
                params![other.to_string(), coordinates.thread_id.to_string()],
            )
            .unwrap();
    }
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(
        reopened.active_leaf(&coordinates).await.unwrap(),
        Some(advanced)
    );
    reopened.select_branch(&coordinates, None).await.unwrap();
    drop(reopened);

    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO active_leaves (thread_id, entry_id) VALUES (?1, ?2)",
                params![coordinates.thread_id.to_string(), other.to_string()],
            )
            .unwrap();
    }
    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(reopened.active_leaf(&coordinates).await.unwrap(), None);

    let events = reopened
        .read_events(&EventStreamId::for_thread(&coordinates), None)
        .await
        .unwrap();
    let selections = events
        .iter()
        .filter(|event| event.kind == EventKind::ThreadBranchSelected)
        .map(|event| {
            serde_json::from_value::<ThreadBranchSelectedPayload>(event.payload.clone()).unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selections,
        vec![
            ThreadBranchSelectedPayload {
                thread_id: coordinates.thread_id,
                selected_entry_id: Some(selected),
                prior_entry_id: Some(other),
            },
            ThreadBranchSelectedPayload {
                thread_id: coordinates.thread_id,
                selected_entry_id: None,
                prior_entry_id: Some(advanced),
            },
        ]
    );

    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_store_resumes_active_branch_and_compaction() {
    let path = temp_db_path("cooldis-history-branch");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let store = SqliteSessionStore::open(&path).unwrap();
    let root = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let left = store
        .append(
            &coordinates,
            Some(root.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("left"),
            },
        )
        .await
        .unwrap();
    let right = store
        .append(
            &coordinates,
            Some(root.entry_id),
            SessionEntryKind::Compaction {
                summary: "root summary".to_string(),
            },
        )
        .await
        .unwrap();
    drop(store);

    let reopened = SqliteSessionStore::open(&path).unwrap();
    assert_eq!(
        reopened.active_leaf(&coordinates).await.unwrap(),
        Some(right.entry_id)
    );
    let context = reopened.build_context(&coordinates).await.unwrap();
    assert_eq!(
        message_texts(&context.messages),
        vec!["Compacted conversation summary:\nroot summary"]
    );
    assert!(
        !context
            .entries
            .iter()
            .any(|entry| entry.entry_id == left.entry_id)
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_store_rejects_parent_from_wrong_coordinate_scope() {
    let store = SqliteSessionStore::in_memory().unwrap();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let root = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let mut wrong_scope = coordinates.clone();
    wrong_scope.tenant_id = "tenant_b".to_string();

    let err = store
        .append(
            &wrong_scope,
            Some(root.entry_id),
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("bad"),
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, HistoryError::ThreadScopeMismatch { .. }));
}

#[tokio::test]
async fn clone_and_select_reject_checkpoint_leaf_from_wrong_scope() {
    let store = SqliteSessionStore::in_memory().unwrap();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let leaf = store
        .append(
            &coordinates,
            None,
            SessionEntryKind::Message {
                message: CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let mut wrong_scope = coordinates.clone();
    wrong_scope.session_id = "session_2".to_string();

    let select_err = store
        .select_branch(&wrong_scope, Some(leaf.entry_id))
        .await
        .unwrap_err();
    assert!(matches!(
        select_err,
        HistoryError::ThreadScopeMismatch { .. }
    ));

    let target = coords("tenant_a", "user_1", "session_2");
    let clone_err = store
        .clone_branch(&wrong_scope, Some(leaf.entry_id), &target)
        .await
        .unwrap_err();
    assert!(matches!(
        clone_err,
        HistoryError::ThreadScopeMismatch { .. }
    ));
}

#[tokio::test]
async fn sqlite_fork_by_reference_survives_reopen_without_copying_entries() {
    let path = temp_db_path("cooldis-history-borrowed-prefix");
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let root;
    let source_leaf;
    {
        let store = SqliteSessionStore::open(&path).unwrap();
        root = store
            .append(
                &source,
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("root"),
                },
            )
            .await
            .unwrap();
        source_leaf = store
            .append(
                &source,
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("source"),
                },
            )
            .await
            .unwrap();
        store
            .fork_by_reference(
                &source,
                &target,
                ThreadBaseRef {
                    child_thread_id: target.thread_id,
                    parent_thread_id: source.thread_id,
                    parent_checkpoint_id: None,
                    parent_leaf_entry_id: Some(source_leaf.entry_id),
                    parent_stream_id: EventStreamId::for_thread(&source),
                    parent_stream_to_sequence: None,
                    parent_binding_snapshot_id: None,
                    reason: ThreadForkReason::ToolAdded,
                    created_at_ms: now_ms(),
                },
            )
            .await
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let target_context = reopened.build_context(&target).await.unwrap();
    assert_eq!(
        message_texts(&target_context.messages),
        vec!["root", "source"]
    );
    assert!(target_context.entries.iter().all(|entry| {
        entry.coordinates == source
            && (entry.entry_id == root.entry_id || entry.entry_id == source_leaf.entry_id)
    }));
    assert_eq!(
        target_context.source_cuts,
        vec![SessionContextSourceCut {
            coordinates: source.clone(),
            stream_id: EventStreamId::for_thread(&source),
            inherited: true,
            entry_ids: vec![root.entry_id, source_leaf.entry_id],
        }]
    );
    assert!(
        reopened
            .read_events(&EventStreamId::for_thread(&target), None)
            .await
            .unwrap()
            .is_empty()
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_fork_by_reference_rejects_missing_parent_cut() {
    let store = SqliteSessionStore::in_memory().unwrap();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let missing = SessionEntryId::new();

    let err = store
        .fork_by_reference(
            &source,
            &target,
            ThreadBaseRef {
                child_thread_id: target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(missing),
                parent_stream_id: EventStreamId::for_thread(&source),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: ThreadForkReason::Manual,
                created_at_ms: now_ms(),
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, HistoryError::EntryNotFound(id) if id == missing));
}

#[tokio::test]
async fn sqlite_events_reject_discharged_records_without_provenance() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let record = NewEventRecord::discharged(
        coordinates.clone(),
        EventKind::ContextCompileCompleted,
        serde_json::json!({"output_hash": "sha256:test"}),
        EventProvenance::default(),
    );
    let record_id = record.id;
    let store = SqliteSessionStore::in_memory().unwrap();

    let err = store
        .append_events(&stream_id, vec![record])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        HistoryError::DischargedWithoutProvenance(id) if id == record_id
    ));
    assert!(
        store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn sqlite_events_validate_stream_schema_before_commit() {
    let path = temp_db_path("cooldis-history-stream-schema-invalid");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let valid = NewEventRecord::witnessed(
        coordinates.clone(),
        EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": "entry-1"}),
    );
    let invalid = NewEventRecord::witnessed(
        coordinates,
        EventKind::TurnSubmitted,
        serde_json::json!("not-an-object-payload"),
    );
    let store = SqliteSessionStore::open(&path).unwrap();

    let err = store
        .append_events(&stream_id, vec![valid.clone(), invalid])
        .await
        .unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("expected object")));
    assert!(
        store
            .read_events(&stream_id, None)
            .await
            .unwrap()
            .is_empty()
    );

    let appended = store.append_events(&stream_id, vec![valid]).await.unwrap();
    assert_eq!(appended.len(), 1);
    assert_eq!(appended[0].sequence.get(), 1);

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_events_round_trip_origin_and_provenance() {
    let path = temp_db_path("cooldis-history-event-origin");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let provenance = EventProvenance {
        source_streams: vec![stream_id.clone()],
        source_range: Some(ObservationSourceRange {
            stream_id: stream_id.clone(),
            from_sequence: EventSequence::new(1),
            to_sequence: EventSequence::new(1),
        }),
        discharged_by: Some("projection:context-compiler".to_string()),
        function: Some("naive_assembly/v1".to_string()),
        ..EventProvenance::default()
    };

    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .append_events(
                &stream_id,
                vec![
                    NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::SessionEntryAppended,
                        serde_json::json!({"entry_id": "entry-1"}),
                    ),
                    NewEventRecord::discharged(
                        coordinates.clone(),
                        EventKind::ContextCompileCompleted,
                        serde_json::json!({"output_hash": "sha256:test"}),
                        provenance.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].kind, EventKind::SessionEntryAppended);
    assert_eq!(events[0].origin, EventOrigin::Witnessed);
    assert!(events[0].provenance.is_empty());
    assert_eq!(events[1].kind, EventKind::ContextCompileCompleted);
    assert_eq!(events[1].origin, EventOrigin::Discharged);
    assert_eq!(events[1].provenance, provenance);
    assert_eq!(events[1].payload["output_hash"], "sha256:test");

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_events_round_trip_stream_schema_v1_context_records() {
    let path = temp_db_path("cooldis-history-stream-schema-v1-context");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let provenance = EventProvenance {
        source_streams: vec![stream_id.clone()],
        source_ranges: vec![ObservationSourceRange {
            stream_id: stream_id.clone(),
            from_sequence: EventSequence::new(1),
            to_sequence: EventSequence::new(2),
        }],
        discharged_by: Some("projection:context-summarizer".to_string()),
        function: Some("op://cooldis/context-summarize@sha256:test".to_string()),
        ..EventProvenance::default()
    };

    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .append_events(
                &stream_id,
                vec![
                    NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::TurnSubmitted,
                        serde_json::json!({"schema": "cooldis.event.turn.submitted/1"}),
                    ),
                    NewEventRecord::discharged(
                        coordinates.clone(),
                        EventKind::ContextSummaryCompleted,
                        serde_json::json!({
                            "schema": "cooldis.event.context.summary.completed/1",
                            "role": "summary_checkpoint",
                            "text": "Earlier turns established the search plan.",
                            "covered_ranges": [{
                                "stream_id": stream_id.as_str(),
                                "from_sequence": 1,
                                "to_sequence": 2
                            }],
                            "content": {
                                "sha256": "sha256:summary"
                            }
                        }),
                        provenance.clone(),
                    ),
                    NewEventRecord::discharged(
                        coordinates.clone(),
                        EventKind::ContextReadPlanSet,
                        serde_json::json!({
                            "schema": "cooldis.event.context.read_plan.set/1",
                            "scope": "thread",
                            "name": "history.default",
                            "read_plan": {
                                "schema": "cooldis.context.read_plan/1",
                                "name": "history.default",
                                "source_stream": stream_id.as_str(),
                                "frontier": "compile_frontier",
                                "entries": [{
                                    "kind": "event_ref",
                                    "event_role": "summary_checkpoint"
                                }]
                            }
                        }),
                        EventProvenance {
                            source_streams: vec![stream_id.clone()],
                            discharged_by: Some("controller:context-budget".to_string()),
                            function: Some("context_read_plan/v1".to_string()),
                            ..EventProvenance::default()
                        },
                    ),
                ],
            )
            .await
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let envelopes = reopened
        .read_events(&stream_id, None)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.to_stream_record_v1())
        .collect::<Vec<_>>();
    assert_eq!(envelopes.len(), 3);
    assert_eq!(envelopes[0].schema, STREAM_RECORD_SCHEMA_V1);
    assert_eq!(
        envelopes[1].payload_schema,
        "cooldis.event.context.summary.completed/1"
    );
    assert_eq!(
        envelopes[1].payload["text"],
        "Earlier turns established the search plan."
    );
    assert_eq!(envelopes[1].origin, EventOrigin::Discharged);
    assert_eq!(
        envelopes[2].payload_schema,
        "cooldis.event.context.read_plan.set/1"
    );
    assert_eq!(
        envelopes[2].payload["read_plan"]["schema"],
        "cooldis.context.read_plan/1"
    );
    assert_eq!(envelopes[2].payload["read_plan"]["name"], "history.default");
    assert!(
        envelopes[1..]
            .iter()
            .all(|event| !event.provenance.is_empty())
    );
    assert_eq!(
        sqlite_event_schema_columns(&path),
        vec![
            (
                EventKind::TurnSubmitted.as_str().to_string(),
                STREAM_RECORD_SCHEMA_V1.to_string(),
                EventKind::TurnSubmitted.payload_schema_id().to_string()
            ),
            (
                EventKind::ContextSummaryCompleted.as_str().to_string(),
                STREAM_RECORD_SCHEMA_V1.to_string(),
                EventKind::ContextSummaryCompleted
                    .payload_schema_id()
                    .to_string()
            ),
            (
                EventKind::ContextReadPlanSet.as_str().to_string(),
                STREAM_RECORD_SCHEMA_V1.to_string(),
                EventKind::ContextReadPlanSet
                    .payload_schema_id()
                    .to_string()
            ),
        ]
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_stream_cursor_replays_strictly_after_verified_position_across_reopen() {
    let path = temp_db_path("cooldis-history-stream-cursor-v1");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let cursor = {
        let store = SqliteSessionStore::open(&path).unwrap();
        let appended = store
            .append_events(
                &stream_id,
                vec![
                    NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::TurnSubmitted,
                        serde_json::json!({"schema": "cooldis.event.turn.submitted/1", "turn_id": "turn-1"}),
                    ),
                    NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::ToolCallCompleted,
                        serde_json::json!({"schema": "cooldis.event.tool.call.completed/1", "call_id": "call-1"}),
                    ),
                    NewEventRecord::witnessed(
                        coordinates,
                        EventKind::TurnCompleted,
                        serde_json::json!({"schema": "cooldis.event.turn.completed/1", "turn_id": "turn-1"}),
                    ),
                ],
            )
            .await
            .unwrap();
        appended[0].cursor_v1()
    };

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let replay = reopened
        .read_events_after_cursor(&stream_id, &cursor)
        .await
        .unwrap();
    assert_eq!(
        replay
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(replay[0].kind, EventKind::ToolCallCompleted);
    assert_eq!(replay[1].kind, EventKind::TurnCompleted);

    let tampered = StreamCursorV1 {
        event_id: replay[1].id,
        ..cursor
    };
    let err = reopened
        .read_events_after_cursor(&stream_id, &tampered)
        .await
        .unwrap_err();
    assert!(matches!(err, HistoryError::StreamCursorMismatch { .. }));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_round_trips_declared_coupling_event_kinds() {
    let path = temp_db_path("cooldis-history-declared-coupling-kinds");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let provenance = EventProvenance {
        source_streams: vec![stream_id.clone()],
        discharged_by: Some("controller:test".to_string()),
        function: Some("op://policy/test@sha256:abc".to_string()),
        ..EventProvenance::default()
    };

    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .append_events(
                &stream_id,
                vec![
                    NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::TurnSubmitted,
                        serde_json::json!({"turn_id": "turn-1"}),
                    ),
                    NewEventRecord::discharged(
                        coordinates.clone(),
                        EventKind::ToolCallSuspended,
                        serde_json::json!({"call_id": "call-1"}),
                        provenance.clone(),
                    ),
                    NewEventRecord::witnessed(
                        coordinates.clone(),
                        EventKind::ApprovalResolved,
                        serde_json::json!({"approval_id": "approval-1"}),
                    ),
                    NewEventRecord::discharged(
                        coordinates.clone(),
                        EventKind::CouplingRunCompleted,
                        serde_json::json!({"coupling_id": "test"}),
                        provenance.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    assert_eq!(
        events
            .iter()
            .map(|event| event.kind.as_str())
            .collect::<Vec<_>>(),
        vec![
            "turn.submitted",
            "tool.call.suspended",
            "approval.resolved",
            "coupling.run.completed",
        ]
    );
    assert_eq!(events[0].origin, EventOrigin::Witnessed);
    assert_eq!(events[1].origin, EventOrigin::Discharged);
    assert_eq!(events[1].provenance, provenance);
    assert_eq!(events[2].origin, EventOrigin::Witnessed);
    assert!(events[2].provenance.is_empty());
    assert_eq!(events[3].origin, EventOrigin::Discharged);

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_load_fails_closed_on_unknown_kind() {
    let path = temp_db_path("cooldis-history-unknown-kind");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    {
        let store = SqliteSessionStore::open(&path).unwrap();
        drop(store);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "INSERT INTO event_records (
                        event_id,
                        stream_id,
                        sequence,
                        thread_id,
                        tenant_id,
                        user_id,
                        session_id,
                        created_at_ms,
                        kind,
                        origin,
                        provenance_json,
                        payload_json
                    ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
                params![
                    EventRecordId::new().to_string(),
                    stream_id.as_str(),
                    1_i64,
                    coordinates.thread_id.to_string(),
                    coordinates.tenant_id.as_str(),
                    coordinates.user_id.as_str(),
                    coordinates.session_id.as_str(),
                    now_ms(),
                    "unknown.event.kind",
                    "witnessed",
                    "{}",
                    "{}",
                ],
            )
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let err = reopened.read_events(&stream_id, None).await.unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("unknown event kind")));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_load_fails_closed_on_payload_schema_drift() {
    let path = temp_db_path("cooldis-history-payload-schema-drift");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .append_events(
                &stream_id,
                vec![NewEventRecord::witnessed(
                    coordinates,
                    EventKind::TurnSubmitted,
                    serde_json::json!({"schema": "cooldis.event.turn.submitted/1"}),
                )],
            )
            .await
            .unwrap();
        drop(store);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE event_records SET payload_schema = ?1",
                params!["cooldis.event.other/1"],
            )
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let err = reopened.read_events(&stream_id, None).await.unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("payload_schema")));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_load_validates_io_egress_requested_payload_after_reopen() {
    let path = temp_db_path("cooldis-history-egress-requested-replay-invalid");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    {
        let store = SqliteSessionStore::open(&path).unwrap();
        store
            .append_events(
                &stream_id,
                vec![NewEventRecord::discharged(
                    coordinates,
                    EventKind::IoEgressRequested,
                    serde_json::json!({
                        "schema": EventKind::IoEgressRequested.payload_schema_id(),
                        "egress_kind": {
                            "type": "platform_action",
                            "action": "reaction",
                            "payload": {
                                "message_id": "message-1",
                                "emoji": "👍"
                            }
                        },
                        "requested_by_tool_call_id": "call_1"
                    }),
                    EventProvenance {
                        source_streams: vec![stream_id.clone()],
                        discharged_by: Some("rpc:append_events".to_string()),
                        function: Some("io_egress_requested/v1".to_string()),
                        ..EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();
        drop(store);
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute(
                "UPDATE event_records SET payload_json = ?1",
                params![
                    serde_json::json!({
                        "schema": EventKind::IoEgressRequested.payload_schema_id(),
                        "requested_by_tool_call_id": "call_1"
                    })
                    .to_string()
                ],
            )
            .unwrap();
    }

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let err = reopened.read_events(&stream_id, None).await.unwrap_err();
    assert!(matches!(err, HistoryError::Codec(message) if message.contains("egress_kind")));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_migrates_legacy_event_records_origin_and_provenance() {
    let path = temp_db_path("cooldis-history-legacy-events");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);
    let user_entry = SessionEntry::new(
        coordinates.clone(),
        None,
        SessionEntryKind::Message {
            message: CanonicalMessage::user_text("hello"),
        },
    );
    let assistant_entry = SessionEntry::new(
        coordinates.clone(),
        Some(user_entry.entry_id),
        SessionEntryKind::Message {
            message: CanonicalMessage::assistant(
                "openai",
                ProviderApi::OpenAIResponses,
                "gpt-test",
                vec![CanonicalContent::text("hello back")],
                CanonicalStopReason::EndTurn,
            ),
        },
    );
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                    CREATE TABLE event_records (
                        event_id TEXT PRIMARY KEY NOT NULL,
                        stream_id TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        thread_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        user_id TEXT NOT NULL,
                        session_id TEXT NOT NULL,
                        created_at_ms INTEGER NOT NULL,
                        kind TEXT NOT NULL,
                        payload_json TEXT NOT NULL,
                        UNIQUE(stream_id, sequence)
                    );
                    "#,
            )
            .unwrap();
        for (sequence, kind, payload_json) in [
            (
                1_i64,
                EventKind::SessionEntryAppended,
                serde_json::to_string(&user_entry).unwrap(),
            ),
            (
                2_i64,
                EventKind::SessionEntryAppended,
                serde_json::to_string(&assistant_entry).unwrap(),
            ),
            (
                3_i64,
                EventKind::ContextCompileCompleted,
                serde_json::to_string(&serde_json::json!({
                    "output_hash": "sha256:test",
                }))
                .unwrap(),
            ),
        ] {
            connection
                .execute(
                    "INSERT INTO event_records (
                            event_id,
                            stream_id,
                            sequence,
                            thread_id,
                            tenant_id,
                            user_id,
                            session_id,
                            created_at_ms,
                            kind,
                            payload_json
                        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                    params![
                        EventRecordId::new().to_string(),
                        stream_id.as_str(),
                        sequence,
                        coordinates.thread_id.to_string(),
                        coordinates.tenant_id.as_str(),
                        coordinates.user_id.as_str(),
                        coordinates.session_id.as_str(),
                        now_ms(),
                        kind.as_str(),
                        payload_json,
                    ],
                )
                .unwrap();
        }
    }

    let migrated = SqliteSessionStore::open(&path).unwrap();
    let events = migrated.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].kind, EventKind::SessionEntryAppended);
    assert_eq!(events[0].origin, EventOrigin::Witnessed);
    assert!(events[0].provenance.is_empty());
    assert_eq!(events[1].kind, EventKind::SessionEntryAppended);
    assert_eq!(events[1].origin, EventOrigin::Discharged);
    assert_eq!(
        events[1].provenance,
        EventProvenance {
            discharged_by: Some("migration:origin-backfill@v1".to_string()),
            ..EventProvenance::default()
        }
    );
    assert_ne!(
        events[1].provenance.discharged_by.as_deref(),
        Some("propagator:agent-loop")
    );
    assert!(events[1].provenance.source_event_ids.is_empty());
    assert_eq!(events[2].kind, EventKind::ContextCompileCompleted);
    assert_eq!(events[2].origin, EventOrigin::Discharged);
    assert_eq!(
        events[2].provenance,
        EventProvenance {
            discharged_by: Some("migration:origin-backfill@v1".to_string()),
            ..EventProvenance::default()
        }
    );
    assert!(
        events
            .iter()
            .filter(|event| event.origin == EventOrigin::Discharged)
            .all(|event| !event.provenance.is_empty())
    );
    assert_eq!(
        sqlite_event_schema_columns(&path),
        vec![
            (
                EventKind::SessionEntryAppended.as_str().to_string(),
                STREAM_RECORD_SCHEMA_V1.to_string(),
                EventKind::SessionEntryAppended
                    .payload_schema_id()
                    .to_string()
            ),
            (
                EventKind::SessionEntryAppended.as_str().to_string(),
                STREAM_RECORD_SCHEMA_V1.to_string(),
                EventKind::SessionEntryAppended
                    .payload_schema_id()
                    .to_string()
            ),
            (
                EventKind::ContextCompileCompleted.as_str().to_string(),
                STREAM_RECORD_SCHEMA_V1.to_string(),
                EventKind::ContextCompileCompleted
                    .payload_schema_id()
                    .to_string()
            ),
        ]
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_and_observation_records_survive_reopen_with_provenance() {
    let path = temp_db_path("cooldis-history-events");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = EventStreamId::for_thread(&coordinates);

    let receipt_id = {
        let store = SqliteSessionStore::open(&path).unwrap();
        let entry = store
            .append(
                &coordinates,
                None,
                SessionEntryKind::Message {
                    message: CanonicalMessage::user_text("hello"),
                },
            )
            .await
            .unwrap();

        let events = store.read_events(&stream_id, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, EventKind::SessionEntryAppended);
        assert_eq!(events[0].payload["entry_id"], entry.entry_id.to_string());

        let receipt = store
            .append_observation(
                NewObservationRecord::new(
                    "compiled_context_receipt",
                    coordinates.clone(),
                    serde_json::json!({
                        "strategy": "naive_assembly",
                        "output_hash": "sha256:test",
                    }),
                )
                .with_provenance(ObservationProvenance {
                    source_streams: vec![stream_id.clone()],
                    source_event_ids: vec![events[0].id],
                    source_range: Some(ObservationSourceRange {
                        stream_id: stream_id.clone(),
                        from_sequence: events[0].sequence,
                        to_sequence: events[0].sequence,
                    }),
                    source_ranges: vec![ObservationSourceRange {
                        stream_id: stream_id.clone(),
                        from_sequence: events[0].sequence,
                        to_sequence: events[0].sequence,
                    }],
                    derivation_strategy: "naive_assembly".to_string(),
                    derivation_version: "v1".to_string(),
                }),
            )
            .await
            .unwrap();
        receipt.id
    };

    let reopened = SqliteSessionStore::open(&path).unwrap();
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].sequence.get(), 1);

    let observations = reopened
        .list_observations(&coordinates, Some("compiled_context_receipt"))
        .await
        .unwrap();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].id, receipt_id);
    assert_eq!(
        observations[0].provenance.derivation_strategy,
        "naive_assembly"
    );
    assert_eq!(
        observations[0].provenance.source_event_ids,
        vec![events[0].id]
    );
    assert_eq!(
        observations[0]
            .provenance
            .source_range
            .as_ref()
            .unwrap()
            .to_sequence
            .get(),
        1
    );

    let _ = std::fs::remove_file(path);
}

fn temp_db_path(prefix: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.sqlite3"))
}

fn sqlite_entry_json(path: &std::path::Path) -> Vec<String> {
    let connection = rusqlite::Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT entry_json FROM session_entries ORDER BY created_at_ms")
        .unwrap();
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}

fn sqlite_event_schema_columns(path: &std::path::Path) -> Vec<(String, String, String)> {
    let connection = rusqlite::Connection::open(path).unwrap();
    let mut statement = connection
        .prepare("SELECT kind, schema, payload_schema FROM event_records ORDER BY sequence")
        .unwrap();
    statement
        .query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
