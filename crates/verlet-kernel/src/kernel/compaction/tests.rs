use super::*;

#[test]
fn render_compaction_summary_is_stable_and_idempotent() {
    let rendered = render_compaction_summary("old facts");
    assert_eq!(rendered, "Compacted conversation summary:\nold facts");
    assert_eq!(render_compaction_summary(&rendered), rendered);
}

#[test]
fn deterministic_summary_keeps_recent_text() {
    let messages = (0..10)
        .map(|index| CanonicalMessage::user_text(format!("message {index}")))
        .collect::<Vec<_>>();

    let summary = deterministic_compaction_summary(&messages);

    assert!(!summary.contains("message 0"));
    assert!(summary.contains("message 9"));
}
