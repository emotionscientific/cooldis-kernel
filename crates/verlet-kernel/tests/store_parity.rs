#[path = "support/test_mount.rs"]
mod support;

#[tokio::test]
async fn in_memory_and_sqlite_session_stores_have_observable_parity() {
    let in_memory = crate::support::store_parity::session_store_parity_transcript(
        &verlet_history::InMemorySessionStore::new(),
    )
    .await
    .unwrap();
    let repeated = crate::support::store_parity::session_store_parity_transcript(
        &verlet_history::InMemorySessionStore::new(),
    )
    .await
    .unwrap();
    let sqlite = crate::support::store_parity::session_store_parity_transcript(
        &verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(in_memory, repeated);
    assert_eq!(in_memory, sqlite);
    assert_eq!(in_memory.render(), sqlite.render());
}

#[test]
fn transcript_normalizer_preserves_requested_lineage_ids() {
    let generated_id = "00000000-0000-0000-0000-000000000001";
    let lineage_id = "00000000-0000-0000-0000-000000000002";
    let receipt = serde_json::json!({
        "event_id": generated_id,
        "source_event_ids": [generated_id, lineage_id],
        "created_at_ms": 1_234,
        "duration_ms": 25,
    });
    let mut transcript = crate::support::transcript::TypedTranscript::new();
    transcript.preserve_id(lineage_id);
    transcript.push_receipt("lineage", &receipt);

    let normalized = transcript.normalize();
    assert_eq!(normalized, transcript.normalize());
    let rendered = normalized.render();
    assert!(rendered.contains(lineage_id));
    assert!(rendered.contains("$event-1"));
    assert!(rendered.contains("$timestamp"));
    assert!(rendered.contains("$duration"));
    assert!(!rendered.contains(generated_id));
}
