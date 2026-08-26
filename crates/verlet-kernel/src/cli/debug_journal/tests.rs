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
