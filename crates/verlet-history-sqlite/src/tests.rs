use verlet_history::EventStore as _;
use verlet_history::ObservationStore as _;
use verlet_history::SessionStore as _;
const RUSQLITE_HISTORY_STREAM_V0: &[u8] =
    include_bytes!("../tests/fixtures/rusqlite-history-stream-v0.sqlite3");

fn coords(tenant: &str, user: &str, session: &str) -> verlet_runtime_contracts::ThreadCoordinates {
    verlet_runtime_contracts::ThreadCoordinates::new(tenant, user, session)
}

fn message_texts(messages: &[verlet_history::CanonicalMessage]) -> Vec<&str> {
    messages
        .iter()
        .map(|message| match message {
            verlet_history::CanonicalMessage::User { content, .. }
            | verlet_history::CanonicalMessage::Assistant { content, .. }
            | verlet_history::CanonicalMessage::ToolResult { content, .. } => content
                .iter()
                .find_map(|content| match content {
                    verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                    _ => None,
                })
                .unwrap_or(""),
        })
        .collect()
}

async fn assert_fenced_append_conformance(
    store: &dyn verlet_history::EventStore,
) -> verlet_history::EventStreamId {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let record = |entry_id: &str| {
        verlet_history::NewEventRecord::witnessed(
            coordinates.clone(),
            verlet_history::EventKind::SessionEntryAppended,
            serde_json::json!({"entry_id": entry_id}),
        )
    };

    let initial = store
        .append_events_fenced(
            &stream_id,
            verlet_history::EventSequence::new(1),
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
            verlet_history::EventSequence::new(3),
            vec![record("losing-entry-1"), record("losing-entry-2")],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        verlet_history::HistoryError::AppendFenceConflict {
            stream_id: conflict_stream,
            expected_next_sequence: 3,
            actual_next_sequence: 4,
        } if conflict_stream == stream_id
    ));

    let after_conflict =
        serde_json::to_vec(&store.read_events(&stream_id, None).await.unwrap()).unwrap();
    assert_eq!(after_conflict, before_conflict);

    let duplicate_stream = verlet_history::EventStreamId::new("duplicate-id-stream");
    let mut duplicate = record("duplicate-event-id");
    let duplicate_event_id = initial[0].id;
    duplicate.id = duplicate_event_id;
    let duplicate_err = store
        .append_events_fenced(
            &duplicate_stream,
            verlet_history::EventSequence::new(1),
            vec![duplicate],
        )
        .await
        .unwrap_err();
    assert!(matches!(
        duplicate_err,
        verlet_history::HistoryError::DuplicateEventId(event_id) if event_id == duplicate_event_id
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
    assert_fenced_append_conformance(&verlet_history::InMemorySessionStore::new()).await;
}

#[tokio::test]
async fn sqlite_store_honors_fenced_append_conformance() {
    let path = temp_db_path("verlet-history-fenced-append");
    let store = crate::SqliteSessionStore::open(&path).await.unwrap();

    let stream_id = assert_fenced_append_conformance(&store).await;

    drop(store);
    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    assert_eq!(
        reopened.read_events(&stream_id, None).await.unwrap().len(),
        3
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_read_only_store_replays_and_rejects_writes() {
    let path = temp_db_path("verlet-history-read-only");
    let coordinates = coords("tenant-read-only", "user-read-only", "session-read-only");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let seed = verlet_history::NewEventRecord::witnessed(
        coordinates.clone(),
        verlet_history::EventKind::TurnSubmitted,
        serde_json::json!({"turn_id": "seed"}),
    );
    let denied = verlet_history::NewEventRecord::witnessed(
        coordinates,
        verlet_history::EventKind::TurnSubmitted,
        serde_json::json!({"turn_id": "denied"}),
    );

    let writable = crate::SqliteSessionStore::open(&path).await.unwrap();
    writable
        .append_events(&stream_id, vec![seed])
        .await
        .unwrap();
    drop(writable);

    let read_only = crate::SqliteSessionStore::open_read_only(&path)
        .await
        .unwrap();
    assert_eq!(
        read_only.read_events(&stream_id, None).await.unwrap().len(),
        1
    );
    assert!(
        read_only
            .append_events(&stream_id, vec![denied])
            .await
            .is_err()
    );
    drop(read_only);

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    assert_eq!(
        reopened.read_events(&stream_id, None).await.unwrap().len(),
        1
    );
    drop(reopened);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_read_only_store_rejects_a_missing_database() {
    let path = temp_db_path("verlet-history-read-only-missing");

    let error = crate::SqliteSessionStore::open_read_only(&path)
        .await
        .err()
        .expect("read-only open must not create a missing database");

    assert!(matches!(error, verlet_history::HistoryError::Storage(_)));
    assert!(!path.exists());
}

#[tokio::test]
async fn turso_replays_rusqlite_created_stream_store_decode_compat_fixture() {
    let path = temp_db_path("verlet-history-rusqlite-decode-compat");
    std::fs::write(&path, RUSQLITE_HISTORY_STREAM_V0).unwrap();

    let store = crate::SqliteSessionStore::open(&path).await.unwrap();
    let events = store
        .read_events(
            &verlet_history::EventStreamId::new("thread:018f0000-0000-7000-8000-000000000413"),
            None,
        )
        .await
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].sequence.get(), 1);
    assert_eq!(events[0].kind, verlet_history::EventKind::TurnSubmitted);
    assert_eq!(events[0].origin, verlet_history::EventOrigin::Witnessed);
    assert_eq!(events[0].payload["turn_id"], "legacy-turn");
    assert_eq!(events[1].sequence.get(), 2);
    assert_eq!(
        events[1].kind,
        verlet_history::EventKind::ContextCompileCompleted
    );
    assert_eq!(events[1].origin, verlet_history::EventOrigin::Discharged);
    assert_eq!(events[1].payload["output_hash"], "sha256:legacy-rusqlite");
    assert_eq!(
        events[1].provenance.discharged_by.as_deref(),
        Some("migration:origin-backfill@v1")
    );

    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_legacy_store_opens_apply_additive_migrations_idempotently() {
    let path = temp_db_path("verlet-history-concurrent-legacy-open");
    std::fs::write(&path, RUSQLITE_HISTORY_STREAM_V0).unwrap();
    let workers = 8;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers));
    let mut handles = Vec::new();

    for _ in 0..workers {
        let path = path.clone();
        let barrier = barrier.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            crate::SqliteSessionStore::open(path).await
        }));
    }

    for handle in handles {
        handle.await.unwrap().unwrap();
    }

    let store = crate::SqliteSessionStore::open(&path).await.unwrap();
    let events = store
        .read_events(
            &verlet_history::EventStreamId::new("thread:018f0000-0000-7000-8000-000000000413"),
            None,
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 2);

    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_store_appends_serialize_stream_sequence_without_interleaving() {
    let path = temp_db_path("verlet-history-concurrent-appends");
    let db = verlet_sqlite::Db::open(
        &path,
        verlet_sqlite::DbConfig {
            busy_timeout: std::time::Duration::ZERO,
            ..verlet_sqlite::DbConfig::default()
        },
    )
    .await
    .unwrap();
    let store = crate::SqliteSessionStore::from_db(db).await.unwrap();
    let coordinates = coords("tenant-concurrent", "user-concurrent", "session-concurrent");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    // A zero busy timeout turns this into a serialization contract rather
    // than a timing test: any second writer admitted while the first owns the
    // WAL lock fails immediately, even on a fast machine.
    let workers = 200;
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers));
    let mut handles = Vec::new();

    for worker in 0..workers {
        let store = store.clone();
        let barrier = barrier.clone();
        let stream_id = stream_id.clone();
        let coordinates = coordinates.clone();
        handles.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .append_events(
                    &stream_id,
                    vec![verlet_history::NewEventRecord::witnessed(
                        coordinates,
                        verlet_history::EventKind::TurnSubmitted,
                        serde_json::json!({"turn_id": format!("turn-{worker}")}),
                    )],
                )
                .await
        }));
    }

    for handle in handles {
        let appended = handle.await.unwrap().unwrap();
        assert_eq!(appended.len(), 1);
    }
    let events = store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), workers);
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        (1..=workers as i64).collect::<Vec<_>>()
    );

    drop(store);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn turso_pre19_reopens_pre18_database_with_committed_wal() {
    let path = temp_db_path("verlet-history-turso-pre18-wal");
    let wal_path = std::path::PathBuf::from(format!("{}-wal", path.display()));
    let decode_fixture = |encoded: &str| {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .unwrap()
    };
    std::fs::write(
        &path,
        decode_fixture(include_str!(
            "../tests/fixtures/turso-pre18-compat.sqlite3.base64"
        )),
    )
    .unwrap();
    std::fs::write(
        &wal_path,
        decode_fixture(include_str!(
            "../tests/fixtures/turso-pre18-compat.sqlite3-wal.base64"
        )),
    )
    .unwrap();
    assert!(
        std::fs::metadata(&wal_path).is_ok_and(|metadata| metadata.len() > 0),
        "pre.18 fixture must leave a committed WAL for pre.19 to recover"
    );

    let store = crate::SqliteSessionStore::open(&path).await.unwrap();
    let database = store.sqlite_database();
    let connection = database.connect().await.unwrap();
    let mut rows = connection
        .query("SELECT value FROM pre18_compat", ())
        .await
        .unwrap();
    let row = rows.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "committed-in-pre18-wal");
    assert!(rows.next().await.unwrap().is_none());
    drop(rows);
    drop(connection);
    drop(database);
    let coordinates = coords("tenant-pre18", "user-pre18", "session-pre18");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    store
        .append_events(
            &stream_id,
            vec![verlet_history::NewEventRecord::witnessed(
                coordinates,
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": "continued-by-pre19"}),
            )],
        )
        .await
        .unwrap();
    drop(store);

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    assert_eq!(
        reopened.read_events(&stream_id, None).await.unwrap().len(),
        1
    );
    let database = reopened.sqlite_database();
    let connection = database.connect().await.unwrap();
    let mut integrity = connection
        .query("PRAGMA integrity_check", ())
        .await
        .unwrap();
    assert_eq!(
        integrity
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        "ok"
    );
    drop(integrity);
    drop(connection);
    drop(database);
    drop(reopened);

    let _ = std::fs::remove_file(&wal_path);
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    let _ = std::fs::remove_file(path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_append_finishes_atomically_and_releases_the_write_lock() {
    let path = temp_db_path("verlet-history-cancelled-append");
    let store = crate::SqliteSessionStore::open(&path).await.unwrap();
    let coordinates = coords("tenant-cancel", "user-cancel", "session-cancel");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let records = (0..1_000)
        .map(|index| {
            verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                serde_json::json!({"turn_id": format!("cancelled-{index}")}),
            )
        })
        .collect::<Vec<_>>();
    let append_store = store.clone();
    let append_stream_id = stream_id.clone();
    let append =
        tokio::spawn(async move { append_store.append_events(&append_stream_id, records).await });

    let probe_db = verlet_sqlite::Db::open(
        &path,
        verlet_sqlite::DbConfig {
            busy_timeout: std::time::Duration::ZERO,
            ..verlet_sqlite::DbConfig::default()
        },
    )
    .await
    .unwrap();
    let mut observed_live_transaction = false;
    for _ in 0..10_000 {
        let mut connection = probe_db.connect().await.unwrap();
        match connection
            .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
            .await
        {
            Ok(transaction) => transaction.rollback().await.unwrap(),
            Err(_) => {
                observed_live_transaction = true;
                break;
            }
        }
        tokio::task::yield_now().await;
    }
    assert!(
        observed_live_transaction,
        "append completed before the test could observe its write transaction"
    );

    append.abort();
    assert!(append.await.unwrap_err().is_cancelled());

    // The cancellation shield finishes the aborted append's transaction in a
    // detached task; on a slow runner that can outlast a single attempt's
    // busy timeout. "database is locked" within the deadline is the shield
    // still working, not a failure — retry until the lock is released.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    let committed = loop {
        let attempt = store
            .append_events(
                &stream_id,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({"turn_id": "committed"}),
                )],
            )
            .await;
        match attempt {
            Ok(events) => break events,
            Err(error) => {
                assert!(
                    error.to_string().contains("database is locked"),
                    "unexpected append error while waiting for the shield: {error}"
                );
                assert!(
                    std::time::Instant::now() < deadline,
                    "write lock not released within the shield deadline: {error}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };
    assert_eq!(committed[0].sequence.get(), 1_001);
    let events = store.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 1_001);
    assert_eq!(
        events
            .iter()
            .map(|event| event.sequence.get())
            .collect::<Vec<_>>(),
        (1..=1_001).collect::<Vec<_>>()
    );

    drop(store);
    drop(probe_db);
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_store_persists_canonical_history_across_reopen() {
    let path = temp_db_path("verlet-history-persist");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let assistant = verlet_history::CanonicalMessage::assistant_with_usage(
        "openai",
        verlet_history::ProviderApi::OpenAIResponses,
        "gpt-test",
        vec![verlet_history::CanonicalContent::text("hello back")],
        verlet_history::CanonicalUsage {
            input_tokens: 3,
            output_tokens: 4,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 1,
        },
        verlet_history::CanonicalStopReason::EndTurn,
    );

    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        store
            .append(
                &coordinates,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("hello"),
                },
            )
            .await
            .unwrap();
        store
            .append(
                &coordinates,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: assistant.clone(),
                },
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let context = reopened.build_context(&coordinates).await.unwrap();
    assert_eq!(
        message_texts(&context.messages),
        vec!["hello", "hello back"]
    );
    assert_eq!(context.entries.len(), 2);
    assert_eq!(context.messages[1], assistant);

    let raw_json = sqlite_entry_json(&path).await;
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
    let path = temp_db_path("verlet-history-branch-selection-rebuild");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let (selected, other, advanced) = {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        let root = store
            .append(
                &coordinates,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("root"),
                },
            )
            .await
            .unwrap();
        let selected = store
            .append(
                &coordinates,
                Some(root.entry_id),
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("selected"),
                },
            )
            .await
            .unwrap();
        let other = store
            .append(
                &coordinates,
                Some(root.entry_id),
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("other"),
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
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text(
                        "advanced selected branch",
                    ),
                },
            )
            .await
            .unwrap();
        (selected.entry_id, other.entry_id, advanced.entry_id)
    };

    {
        let (_db, connection) = raw_connection(&path).await;
        connection
            .execute("DROP TABLE active_leaves", ())
            .await
            .unwrap();
    }
    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    assert_eq!(
        reopened.active_leaf(&coordinates).await.unwrap(),
        Some(advanced)
    );
    drop(reopened);

    {
        let (_db, connection) = raw_connection(&path).await;
        connection
            .execute(
                "UPDATE active_leaves SET entry_id = ?1 WHERE thread_id = ?2",
                verlet_sqlite::params![other.to_string(), coordinates.thread_id.to_string()],
            )
            .await
            .unwrap();
    }
    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    assert_eq!(
        reopened.active_leaf(&coordinates).await.unwrap(),
        Some(advanced)
    );
    reopened.select_branch(&coordinates, None).await.unwrap();
    drop(reopened);

    {
        let (_db, connection) = raw_connection(&path).await;
        connection
            .execute(
                "INSERT INTO active_leaves (thread_id, entry_id) VALUES (?1, ?2)",
                verlet_sqlite::params![coordinates.thread_id.to_string(), other.to_string()],
            )
            .await
            .unwrap();
    }
    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    assert_eq!(reopened.active_leaf(&coordinates).await.unwrap(), None);

    let events = reopened
        .read_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            None,
        )
        .await
        .unwrap();
    let selections = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ThreadBranchSelected)
        .map(|event| {
            serde_json::from_value::<verlet_history::ThreadBranchSelectedPayload>(
                event.payload.clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        selections,
        vec![
            verlet_history::ThreadBranchSelectedPayload {
                thread_id: coordinates.thread_id,
                selected_entry_id: Some(selected),
                prior_entry_id: Some(other),
            },
            verlet_history::ThreadBranchSelectedPayload {
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
    let path = temp_db_path("verlet-history-branch");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let store = crate::SqliteSessionStore::open(&path).await.unwrap();
    let root = store
        .append(
            &coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("root"),
            },
        )
        .await
        .unwrap();
    let left = store
        .append(
            &coordinates,
            Some(root.entry_id),
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("left"),
            },
        )
        .await
        .unwrap();
    let right = store
        .append(
            &coordinates,
            Some(root.entry_id),
            verlet_history::SessionEntryKind::Compaction {
                summary: "root summary".to_string(),
            },
        )
        .await
        .unwrap();
    drop(store);

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
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
    let store = crate::SqliteSessionStore::in_memory().await.unwrap();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let root = store
        .append(
            &coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("root"),
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
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("bad"),
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        verlet_history::HistoryError::ThreadScopeMismatch { .. }
    ));
}

#[tokio::test]
async fn clone_and_select_reject_checkpoint_leaf_from_wrong_scope() {
    let store = crate::SqliteSessionStore::in_memory().await.unwrap();
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let leaf = store
        .append(
            &coordinates,
            None,
            verlet_history::SessionEntryKind::Message {
                message: verlet_history::CanonicalMessage::user_text("root"),
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
        verlet_history::HistoryError::ThreadScopeMismatch { .. }
    ));

    let target = coords("tenant_a", "user_1", "session_2");
    let clone_err = store
        .clone_branch(&wrong_scope, Some(leaf.entry_id), &target)
        .await
        .unwrap_err();
    assert!(matches!(
        clone_err,
        verlet_history::HistoryError::ThreadScopeMismatch { .. }
    ));
}

#[tokio::test]
async fn sqlite_fork_by_reference_survives_reopen_without_copying_entries() {
    let path = temp_db_path("verlet-history-borrowed-prefix");
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let root;
    let source_leaf;
    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        root = store
            .append(
                &source,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("root"),
                },
            )
            .await
            .unwrap();
        source_leaf = store
            .append(
                &source,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("source"),
                },
            )
            .await
            .unwrap();
        store
            .fork_by_reference(
                &source,
                &target,
                verlet_history::ThreadBaseRef {
                    child_thread_id: target.thread_id,
                    parent_thread_id: source.thread_id,
                    parent_checkpoint_id: None,
                    parent_leaf_entry_id: Some(source_leaf.entry_id),
                    parent_stream_id: verlet_history::EventStreamId::for_thread(&source),
                    parent_stream_to_sequence: None,
                    parent_binding_snapshot_id: None,
                    reason: verlet_history::ThreadForkReason::ToolAdded,
                    created_at_ms: verlet_history::now_ms(),
                },
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
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
        vec![verlet_history::SessionContextSourceCut {
            coordinates: source.clone(),
            stream_id: verlet_history::EventStreamId::for_thread(&source),
            inherited: true,
            entry_ids: vec![root.entry_id, source_leaf.entry_id],
        }]
    );
    assert!(
        reopened
            .read_events(&verlet_history::EventStreamId::for_thread(&target), None)
            .await
            .unwrap()
            .is_empty()
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_fork_by_reference_rejects_missing_parent_cut() {
    let store = crate::SqliteSessionStore::in_memory().await.unwrap();
    let source = coords("tenant_a", "user_1", "session_1");
    let target = coords("tenant_a", "user_1", "session_1");
    let missing = verlet_history::SessionEntryId::new();

    let err = store
        .fork_by_reference(
            &source,
            &target,
            verlet_history::ThreadBaseRef {
                child_thread_id: target.thread_id,
                parent_thread_id: source.thread_id,
                parent_checkpoint_id: None,
                parent_leaf_entry_id: Some(missing),
                parent_stream_id: verlet_history::EventStreamId::for_thread(&source),
                parent_stream_to_sequence: None,
                parent_binding_snapshot_id: None,
                reason: verlet_history::ThreadForkReason::Manual,
                created_at_ms: verlet_history::now_ms(),
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, verlet_history::HistoryError::EntryNotFound(id) if id == missing));
}

#[tokio::test]
async fn sqlite_events_reject_discharged_records_without_provenance() {
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let record = verlet_history::NewEventRecord::discharged(
        coordinates.clone(),
        verlet_history::EventKind::ContextCompileCompleted,
        serde_json::json!({"output_hash": "sha256:test"}),
        verlet_history::EventProvenance::default(),
    );
    let record_id = record.id;
    let store = crate::SqliteSessionStore::in_memory().await.unwrap();

    let err = store
        .append_events(&stream_id, vec![record])
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        verlet_history::HistoryError::DischargedWithoutProvenance(id) if id == record_id
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
    let path = temp_db_path("verlet-history-stream-schema-invalid");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let valid = verlet_history::NewEventRecord::witnessed(
        coordinates.clone(),
        verlet_history::EventKind::SessionEntryAppended,
        serde_json::json!({"entry_id": "entry-1"}),
    );
    let invalid = verlet_history::NewEventRecord::witnessed(
        coordinates,
        verlet_history::EventKind::TurnSubmitted,
        serde_json::json!("not-an-object-payload"),
    );
    let store = crate::SqliteSessionStore::open(&path).await.unwrap();

    let err = store
        .append_events(&stream_id, vec![valid.clone(), invalid])
        .await
        .unwrap_err();
    assert!(
        matches!(err, verlet_history::HistoryError::Codec(message) if message.contains("expected object"))
    );
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
    let path = temp_db_path("verlet-history-event-origin");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let provenance = verlet_history::EventProvenance {
        source_streams: vec![stream_id.clone()],
        source_range: Some(verlet_history::ObservationSourceRange {
            stream_id: stream_id.clone(),
            from_sequence: verlet_history::EventSequence::new(1),
            to_sequence: verlet_history::EventSequence::new(1),
        }),
        discharged_by: Some("projection:context-compiler".to_string()),
        function: Some("naive_assembly/v1".to_string()),
        ..verlet_history::EventProvenance::default()
    };

    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        store
            .append_events(
                &stream_id,
                vec![
                    verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::SessionEntryAppended,
                        serde_json::json!({"entry_id": "entry-1"}),
                    ),
                    verlet_history::NewEventRecord::discharged(
                        coordinates.clone(),
                        verlet_history::EventKind::ContextCompileCompleted,
                        serde_json::json!({"output_hash": "sha256:test"}),
                        provenance.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events[0].kind,
        verlet_history::EventKind::SessionEntryAppended
    );
    assert_eq!(events[0].origin, verlet_history::EventOrigin::Witnessed);
    assert!(events[0].provenance.is_empty());
    assert_eq!(
        events[1].kind,
        verlet_history::EventKind::ContextCompileCompleted
    );
    assert_eq!(events[1].origin, verlet_history::EventOrigin::Discharged);
    assert_eq!(events[1].provenance, provenance);
    assert_eq!(events[1].payload["output_hash"], "sha256:test");

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_events_round_trip_stream_schema_v1_context_records() {
    let path = temp_db_path("verlet-history-stream-schema-v1-context");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let provenance = verlet_history::EventProvenance {
        source_streams: vec![stream_id.clone()],
        source_ranges: vec![verlet_history::ObservationSourceRange {
            stream_id: stream_id.clone(),
            from_sequence: verlet_history::EventSequence::new(1),
            to_sequence: verlet_history::EventSequence::new(2),
        }],
        discharged_by: Some("projection:context-summarizer".to_string()),
        function: Some("op://verlet/context-summarize@sha256:test".to_string()),
        ..verlet_history::EventProvenance::default()
    };

    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        store
            .append_events(
                &stream_id,
                vec![
                    verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::TurnSubmitted,
                        serde_json::json!({"schema": "cooldis.event.turn.submitted/1"}),
                    ),
                    verlet_history::NewEventRecord::discharged(
                        coordinates.clone(),
                        verlet_history::EventKind::ContextSummaryCompleted,
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
                    verlet_history::NewEventRecord::discharged(
                        coordinates.clone(),
                        verlet_history::EventKind::ContextReadPlanSet,
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
                        verlet_history::EventProvenance {
                            source_streams: vec![stream_id.clone()],
                            discharged_by: Some("controller:context-budget".to_string()),
                            function: Some("context_read_plan/v1".to_string()),
                            ..verlet_history::EventProvenance::default()
                        },
                    ),
                ],
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let envelopes = reopened
        .read_events(&stream_id, None)
        .await
        .unwrap()
        .into_iter()
        .map(|event| event.to_stream_record_v1())
        .collect::<Vec<_>>();
    assert_eq!(envelopes.len(), 3);
    assert_eq!(envelopes[0].schema, verlet_history::STREAM_RECORD_SCHEMA_V1);
    assert_eq!(
        envelopes[1].payload_schema,
        "cooldis.event.context.summary.completed/1"
    );
    assert_eq!(
        envelopes[1].payload["text"],
        "Earlier turns established the search plan."
    );
    assert_eq!(envelopes[1].origin, verlet_history::EventOrigin::Discharged);
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
        sqlite_event_schema_columns(&path).await,
        vec![
            (
                verlet_history::EventKind::TurnSubmitted.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1.to_string(),
                verlet_history::EventKind::TurnSubmitted.payload_schema_id()
            ),
            (
                verlet_history::EventKind::ContextSummaryCompleted.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1.to_string(),
                verlet_history::EventKind::ContextSummaryCompleted.payload_schema_id()
            ),
            (
                verlet_history::EventKind::ContextReadPlanSet.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1.to_string(),
                verlet_history::EventKind::ContextReadPlanSet.payload_schema_id()
            ),
        ]
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_stream_cursor_replays_strictly_after_verified_position_across_reopen() {
    let path = temp_db_path("verlet-history-stream-cursor-v1");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let cursor = {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        let appended = store
            .append_events(
                &stream_id,
                vec![
                    verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::TurnSubmitted,
                        serde_json::json!({"schema": "cooldis.event.turn.submitted/1", "turn_id": "turn-1"}),
                    ),
                    verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::ToolCallCompleted,
                        serde_json::json!({
                            "schema": "cooldis.event.tool.call.completed/1",
                            "subject": {"turn_id": "turn-1", "call_id": "call-1"},
                            "snapshot_id": "snapshot-1",
                            "tool_name": "bash",
                            "success": false,
                            "cancellation": "cancelled_exceeded_grace"
                        }),
                    ),
                    verlet_history::NewEventRecord::witnessed(
                        coordinates,
                        verlet_history::EventKind::TurnCompleted,
                        serde_json::json!({"schema": "cooldis.event.turn.completed/1", "turn_id": "turn-1"}),
                    ),
                ],
            )
            .await
            .unwrap();
        appended[0].cursor_v1()
    };

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
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
    assert_eq!(replay[0].kind, verlet_history::EventKind::ToolCallCompleted);
    assert_eq!(
        replay[0].payload["cancellation"],
        serde_json::json!("cancelled_exceeded_grace")
    );
    assert_eq!(replay[1].kind, verlet_history::EventKind::TurnCompleted);

    let tampered = verlet_history::StreamCursorV1 {
        event_id: replay[1].id,
        ..cursor
    };
    let err = reopened
        .read_events_after_cursor(&stream_id, &tampered)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        verlet_history::HistoryError::StreamCursorMismatch { .. }
    ));

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_round_trips_declared_coupling_event_kinds() {
    let path = temp_db_path("verlet-history-declared-coupling-kinds");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let provenance = verlet_history::EventProvenance {
        source_streams: vec![stream_id.clone()],
        discharged_by: Some("controller:test".to_string()),
        function: Some("op://policy/test@sha256:abc".to_string()),
        ..verlet_history::EventProvenance::default()
    };

    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        store
            .append_events(
                &stream_id,
                vec![
                    verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::TurnSubmitted,
                        serde_json::json!({"turn_id": "turn-1"}),
                    ),
                    verlet_history::NewEventRecord::discharged(
                        coordinates.clone(),
                        verlet_history::EventKind::ToolCallSuspended,
                        serde_json::json!({"call_id": "call-1"}),
                        provenance.clone(),
                    ),
                    verlet_history::NewEventRecord::witnessed(
                        coordinates.clone(),
                        verlet_history::EventKind::ApprovalResolved,
                        serde_json::json!({"approval_id": "approval-1"}),
                    ),
                    verlet_history::NewEventRecord::discharged(
                        coordinates.clone(),
                        verlet_history::EventKind::CouplingRunCompleted,
                        serde_json::json!({"coupling_id": "test"}),
                        provenance.clone(),
                    ),
                ],
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let events = reopened.read_events(&stream_id, None).await.unwrap();
    let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_ref()).collect();
    assert_eq!(
        kinds,
        vec![
            "turn.submitted",
            "tool.call.suspended",
            "approval.resolved",
            "coupling.run.completed",
        ]
    );
    assert_eq!(events[0].origin, verlet_history::EventOrigin::Witnessed);
    assert_eq!(events[1].origin, verlet_history::EventOrigin::Discharged);
    assert_eq!(events[1].provenance, provenance);
    assert_eq!(events[2].origin, verlet_history::EventOrigin::Witnessed);
    assert!(events[2].provenance.is_empty());
    assert_eq!(events[3].origin, verlet_history::EventOrigin::Discharged);

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_load_fails_closed_on_unknown_kind() {
    let path = temp_db_path("verlet-history-unknown-kind");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        drop(store);
        let (_db, connection) = raw_connection(&path).await;
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
                verlet_sqlite::params![
                    verlet_history::EventRecordId::new().to_string(),
                    stream_id.as_str(),
                    1_i64,
                    coordinates.thread_id.to_string(),
                    coordinates.tenant_id.as_str(),
                    coordinates.user_id.as_str(),
                    coordinates.session_id.as_str(),
                    verlet_history::now_ms(),
                    "unknown.event.kind",
                    "witnessed",
                    "{}",
                    "{}",
                ],
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let err = reopened.read_events(&stream_id, None).await.unwrap_err();
    assert!(
        matches!(err, verlet_history::HistoryError::Codec(message) if message.contains("unknown event kind"))
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_load_fails_closed_on_payload_schema_drift() {
    let path = temp_db_path("verlet-history-payload-schema-drift");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        store
            .append_events(
                &stream_id,
                vec![verlet_history::NewEventRecord::witnessed(
                    coordinates,
                    verlet_history::EventKind::TurnSubmitted,
                    serde_json::json!({"schema": "cooldis.event.turn.submitted/1"}),
                )],
            )
            .await
            .unwrap();
        drop(store);
        let (_db, connection) = raw_connection(&path).await;
        connection
            .execute(
                "UPDATE event_records SET payload_schema = ?1",
                verlet_sqlite::params!["cooldis.event.other/1"],
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let err = reopened.read_events(&stream_id, None).await.unwrap_err();
    assert!(
        matches!(err, verlet_history::HistoryError::Codec(message) if message.contains("payload_schema"))
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_load_validates_io_egress_requested_payload_after_reopen() {
    let path = temp_db_path("verlet-history-egress-requested-replay-invalid");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        store
            .append_events(
                &stream_id,
                vec![verlet_history::NewEventRecord::discharged(
                    coordinates,
                    verlet_history::EventKind::IoEgressRequested,
                    serde_json::json!({
                        "schema": verlet_history::EventKind::IoEgressRequested.payload_schema_id(),
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
                    verlet_history::EventProvenance {
                        source_streams: vec![stream_id.clone()],
                        discharged_by: Some("rpc:append_events".to_string()),
                        function: Some("io_egress_requested/v1".to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                )],
            )
            .await
            .unwrap();
        drop(store);
        let (_db, connection) = raw_connection(&path).await;
        connection
            .execute(
                "UPDATE event_records SET payload_json = ?1",
                verlet_sqlite::params![
                    serde_json::json!({
                        "schema": verlet_history::EventKind::IoEgressRequested.payload_schema_id(),
                        "requested_by_tool_call_id": "call_1"
                    })
                    .to_string()
                ],
            )
            .await
            .unwrap();
    }

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
    let err = reopened.read_events(&stream_id, None).await.unwrap_err();
    assert!(
        matches!(err, verlet_history::HistoryError::Codec(message) if message.contains("egress_kind"))
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_migrates_legacy_event_records_origin_and_provenance() {
    let path = temp_db_path("verlet-history-legacy-events");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);
    let user_entry = verlet_history::SessionEntry::new(
        coordinates.clone(),
        None,
        verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::user_text("hello"),
        },
    );
    let assistant_entry = verlet_history::SessionEntry::new(
        coordinates.clone(),
        Some(user_entry.entry_id),
        verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::assistant(
                "openai",
                verlet_history::ProviderApi::OpenAIResponses,
                "gpt-test",
                vec![verlet_history::CanonicalContent::text("hello back")],
                verlet_history::CanonicalStopReason::EndTurn,
            ),
        },
    );
    {
        let (_db, connection) = raw_connection(&path).await;
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
            .await
            .unwrap();
        for (sequence, kind, payload_json) in [
            (
                1_i64,
                verlet_history::EventKind::SessionEntryAppended,
                serde_json::to_string(&user_entry).unwrap(),
            ),
            (
                2_i64,
                verlet_history::EventKind::SessionEntryAppended,
                serde_json::to_string(&assistant_entry).unwrap(),
            ),
            (
                3_i64,
                verlet_history::EventKind::ContextCompileCompleted,
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
                    verlet_sqlite::params![
                        verlet_history::EventRecordId::new().to_string(),
                        stream_id.as_str(),
                        sequence,
                        coordinates.thread_id.to_string(),
                        coordinates.tenant_id.as_str(),
                        coordinates.user_id.as_str(),
                        coordinates.session_id.as_str(),
                        verlet_history::now_ms(),
                        kind.as_ref(),
                        payload_json,
                    ],
                )
                .await
                .unwrap();
        }
    }

    let migrated = crate::SqliteSessionStore::open(&path).await.unwrap();
    let events = migrated.read_events(&stream_id, None).await.unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(
        events[0].kind,
        verlet_history::EventKind::SessionEntryAppended
    );
    assert_eq!(events[0].origin, verlet_history::EventOrigin::Witnessed);
    assert!(events[0].provenance.is_empty());
    assert_eq!(
        events[1].kind,
        verlet_history::EventKind::SessionEntryAppended
    );
    assert_eq!(events[1].origin, verlet_history::EventOrigin::Discharged);
    assert_eq!(
        events[1].provenance,
        verlet_history::EventProvenance {
            discharged_by: Some("migration:origin-backfill@v1".to_string()),
            ..verlet_history::EventProvenance::default()
        }
    );
    assert_ne!(
        events[1].provenance.discharged_by.as_deref(),
        Some("propagator:agent-loop")
    );
    assert!(events[1].provenance.source_event_ids.is_empty());
    assert_eq!(
        events[2].kind,
        verlet_history::EventKind::ContextCompileCompleted
    );
    assert_eq!(events[2].origin, verlet_history::EventOrigin::Discharged);
    assert_eq!(
        events[2].provenance,
        verlet_history::EventProvenance {
            discharged_by: Some("migration:origin-backfill@v1".to_string()),
            ..verlet_history::EventProvenance::default()
        }
    );
    assert!(
        events
            .iter()
            .filter(|event| event.origin == verlet_history::EventOrigin::Discharged)
            .all(|event| !event.provenance.is_empty())
    );
    assert_eq!(
        sqlite_event_schema_columns(&path).await,
        vec![
            (
                verlet_history::EventKind::SessionEntryAppended.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1.to_string(),
                verlet_history::EventKind::SessionEntryAppended.payload_schema_id()
            ),
            (
                verlet_history::EventKind::SessionEntryAppended.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1.to_string(),
                verlet_history::EventKind::SessionEntryAppended.payload_schema_id()
            ),
            (
                verlet_history::EventKind::ContextCompileCompleted.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1.to_string(),
                verlet_history::EventKind::ContextCompileCompleted.payload_schema_id()
            ),
        ]
    );

    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn sqlite_event_and_observation_records_survive_reopen_with_provenance() {
    let path = temp_db_path("verlet-history-events");
    let coordinates = coords("tenant_a", "user_1", "session_1");
    let stream_id = verlet_history::EventStreamId::for_thread(&coordinates);

    let receipt_id = {
        let store = crate::SqliteSessionStore::open(&path).await.unwrap();
        let entry = store
            .append(
                &coordinates,
                None,
                verlet_history::SessionEntryKind::Message {
                    message: verlet_history::CanonicalMessage::user_text("hello"),
                },
            )
            .await
            .unwrap();

        let events = store.read_events(&stream_id, None).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].kind,
            verlet_history::EventKind::SessionEntryAppended
        );
        assert_eq!(events[0].payload["entry_id"], entry.entry_id.to_string());

        let receipt = store
            .append_observation(
                verlet_history::NewObservationRecord::new(
                    "compiled_context_receipt",
                    coordinates.clone(),
                    serde_json::json!({
                        "strategy": "naive_assembly",
                        "output_hash": "sha256:test",
                    }),
                )
                .with_provenance(verlet_history::ObservationProvenance {
                    source_streams: vec![stream_id.clone()],
                    source_event_ids: vec![events[0].id],
                    source_range: Some(verlet_history::ObservationSourceRange {
                        stream_id: stream_id.clone(),
                        from_sequence: events[0].sequence,
                        to_sequence: events[0].sequence,
                    }),
                    source_ranges: vec![verlet_history::ObservationSourceRange {
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

    let reopened = crate::SqliteSessionStore::open(&path).await.unwrap();
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
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("{prefix}-{nanos}.sqlite3"))
}

async fn raw_connection(path: &std::path::Path) -> (verlet_sqlite::Db, verlet_sqlite::Connection) {
    let db = verlet_sqlite::Db::open(path, verlet_sqlite::DbConfig::default())
        .await
        .unwrap();
    let connection = db.connect().await.unwrap();
    (db, connection)
}

async fn sqlite_entry_json(path: &std::path::Path) -> Vec<String> {
    let (_db, connection) = raw_connection(path).await;
    let mut rows = connection
        .query(
            "SELECT entry_json FROM session_entries ORDER BY created_at_ms",
            (),
        )
        .await
        .unwrap();
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        values.push(row.get::<String>(0).unwrap());
    }
    values
}

async fn sqlite_event_schema_columns(path: &std::path::Path) -> Vec<(String, String, String)> {
    let (_db, connection) = raw_connection(path).await;
    let mut rows = connection
        .query(
            "SELECT kind, schema, payload_schema FROM event_records ORDER BY sequence",
            (),
        )
        .await
        .unwrap();
    let mut values = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        values.push((
            row.get::<String>(0).unwrap(),
            row.get::<String>(1).unwrap(),
            row.get::<String>(2).unwrap(),
        ));
    }
    values
}
