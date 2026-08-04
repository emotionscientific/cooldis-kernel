mod support;

use serde_json::json;
use support::{TypedTranscript, session_store_parity_transcript};
use verlet::{InMemorySessionStore, SqliteSessionStore};

#[tokio::test]
async fn in_memory_and_sqlite_session_stores_have_observable_parity() {
    let in_memory = session_store_parity_transcript(&InMemorySessionStore::new())
        .await
        .unwrap();
    let repeated = session_store_parity_transcript(&InMemorySessionStore::new())
        .await
        .unwrap();
    let sqlite = session_store_parity_transcript(&SqliteSessionStore::in_memory().await.unwrap())
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
    let receipt = json!({
        "event_id": generated_id,
        "source_event_ids": [generated_id, lineage_id],
        "created_at_ms": 1_234,
        "duration_ms": 25,
    });
    let mut transcript = TypedTranscript::new();
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
