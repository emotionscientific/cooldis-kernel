#[test]
fn parse_debug_journal_accepts_filters_and_rejects_conflicting_endpoints() {
    let thread_id = uuid::Uuid::now_v7().to_string();
    let options = crate::cli::debug_journal::parse_debug_journal_args(vec![
        "--thread".into(),
        thread_id.clone().into(),
        "--kind".into(),
        "session.entry.appended".into(),
        "--from-sequence".into(),
        "2".into(),
        "--to-sequence".into(),
        "8".into(),
        "--json".into(),
        "--journal".into(),
        "/tmp/session_history.turso".into(),
    ])
    .unwrap();

    assert_eq!(options.thread_id.unwrap().to_string(), thread_id);
    assert_eq!(
        options.kind,
        Some(verlet_history::EventKind::SessionEntryAppended)
    );
    assert_eq!(options.from_sequence.unwrap().get(), 2);
    assert_eq!(options.to_sequence.unwrap().get(), 8);
    assert!(options.json);
    assert_eq!(
        options.journal,
        Some(std::path::PathBuf::from("/tmp/session_history.turso"))
    );

    let conflicting = crate::cli::debug_journal::parse_debug_journal_args(vec![
        "--url".into(),
        "ws://127.0.0.1:49200/rpc".into(),
        "--journal".into(),
        "/tmp/session_history.turso".into(),
    ])
    .unwrap_err();
    assert!(conflicting.to_string().contains("not more than one"));
}

#[test]
fn parse_debug_journal_rejects_reversed_sequence_range() {
    let error = crate::cli::debug_journal::parse_debug_journal_args(vec![
        "--from-sequence".into(),
        "9".into(),
        "--to-sequence".into(),
        "3".into(),
    ])
    .unwrap_err();

    assert!(
        error
            .to_string()
            .contains("--from-sequence must not exceed --to-sequence")
    );
}

#[test]
fn parse_debug_journal_rejects_non_positive_sequences_explicitly() {
    for (flag, value) in [("--from-sequence", "0"), ("--to-sequence", "-1")] {
        let error =
            crate::cli::debug_journal::parse_debug_journal_args(vec![flag.into(), value.into()])
                .unwrap_err();

        assert!(
            error.to_string().contains("sequence must be positive"),
            "unexpected {flag} error: {error}"
        );
    }
}

#[cfg(unix)]
#[test]
fn parse_debug_journal_preserves_non_utf8_path_arguments() {
    use std::os::unix::ffi::OsStrExt as _;
    use std::os::unix::ffi::OsStringExt as _;

    let journal = std::ffi::OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]);
    let options = crate::cli::debug_journal::parse_debug_journal_args(vec![
        "--journal".into(),
        journal.clone(),
    ])
    .unwrap();
    assert_eq!(
        options.journal.unwrap().as_os_str().as_bytes(),
        journal.as_os_str().as_bytes()
    );

    let config = std::ffi::OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xfe]);
    let options = crate::cli::debug_journal::parse_debug_journal_args(vec![
        "--config".into(),
        config.clone(),
    ])
    .unwrap();
    assert_eq!(
        options.endpoint.config.unwrap().as_os_str().as_bytes(),
        config.as_os_str().as_bytes()
    );
}

#[test]
fn render_debug_journal_keeps_each_record_on_one_compact_line() {
    let thread_id = verlet_runtime_contracts::ThreadId::new();
    let record = verlet_history::EventRecord {
        id: verlet_history::EventRecordId::new(),
        stream_id: verlet_history::EventStreamId::new(format!("thread:{thread_id}")),
        sequence: verlet_history::EventSequence::new(7),
        coordinates: verlet_runtime_contracts::ThreadCoordinates {
            tenant_id: "tenant:test".to_string(),
            user_id: "user:test".to_string(),
            session_id: "session:test".to_string(),
            thread_id,
        },
        created_at_ms: 42,
        kind: verlet_history::EventKind::SessionEntryAppended,
        origin: verlet_history::EventOrigin::Witnessed,
        provenance: verlet_history::EventProvenance::default(),
        payload: serde_json::json!({ "text": "first\nsecond" }),
    };

    let rendered = crate::cli::debug_journal::render_debug_journal_records(&[record]);

    assert_eq!(rendered.lines().count(), 1);
    assert!(rendered.contains("thread:"));
    assert!(rendered.contains(":7\tsession.entry.appended\t"));
    assert!(rendered.contains(r#"{"text":"first\nsecond"}"#));
}

#[tokio::test]
async fn turn_failed_kind_filters_a_real_journal_store() {
    use verlet_history::EventStore as _;

    let root = std::env::temp_dir().join(format!(
        "verlet-debug-journal-turn-failed-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let journal = root.join("session_history.turso");
    let store = verlet_history_sqlite::SqliteSessionStore::open(&journal)
        .await
        .unwrap();
    let coordinates = verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
    store
        .append_events(
            &verlet_history::EventStreamId::for_thread(&coordinates),
            vec![verlet_history::NewEventRecord::discharged(
                coordinates,
                verlet_history::EventKind::TurnFailed,
                serde_json::to_value(verlet_history::TurnFailedPayload::new(
                    "turn-1",
                    verlet_history::TurnFailureErrorClass::ProviderHttp,
                    Some("openai".to_string()),
                    Some(503),
                    "provider HTTP status 503",
                    2,
                ))
                .unwrap(),
                verlet_history::EventProvenance {
                    discharged_by: Some("test:debug-journal".to_string()),
                    function: Some("turn_fail/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )],
        )
        .await
        .unwrap();
    drop(store);

    let options = crate::cli::debug_journal::parse_debug_journal_args(vec![
        "--kind".into(),
        "turn.failed".into(),
        "--journal".into(),
        journal.clone().into_os_string(),
    ])
    .unwrap();
    assert_eq!(options.kind, Some(verlet_history::EventKind::TurnFailed));
    let records = crate::cli::debug_journal::load_debug_journal_direct(&journal, &options)
        .await
        .unwrap();

    assert_eq!(records.len(), 1);
    assert_eq!(records[0].kind, verlet_history::EventKind::TurnFailed);
    assert_eq!(records[0].payload["turn_id"], "turn-1");
    let _ = std::fs::remove_dir_all(root);
}
