use super::*;
use serde_json::json;
use std::path::PathBuf;

#[test]
fn slash_parser_accepts_chat_session_commands() {
    assert_eq!(
        parse_slash_command("/help").unwrap(),
        Some(SlashCommand::Help)
    );
    assert_eq!(parse_slash_command("/q").unwrap(), Some(SlashCommand::Quit));
    assert_eq!(
        parse_slash_command("/resume 019abc").unwrap(),
        Some(SlashCommand::Resume("019abc".to_string()))
    );
    assert_eq!(
        parse_slash_command("/rename customer debug").unwrap(),
        Some(SlashCommand::Rename("customer debug".to_string()))
    );
    assert_eq!(parse_slash_command("hello").unwrap(), None);
}

#[test]
fn slash_parser_repairs_unknown_or_incomplete_commands() {
    assert!(parse_slash_command("/wat").unwrap_err().contains("/help"));
    assert!(
        parse_slash_command("/resume")
            .unwrap_err()
            .contains("thread id")
    );
    assert!(parse_slash_command("/rename").unwrap_err().contains("name"));
}

#[test]
fn attach_parser_accepts_unix_and_ws_endpoints() {
    assert_eq!(
        parse_attach_target("unix:///tmp/verlet.sock").unwrap(),
        ChatAttachTarget::Unix(PathBuf::from("/tmp/verlet.sock"))
    );
    assert_eq!(
        parse_attach_target("ws://127.0.0.1:49200/rpc").unwrap(),
        ChatAttachTarget::WebSocket("ws://127.0.0.1:49200/rpc".to_string())
    );
    assert!(parse_attach_target("wss://example.com/rpc").is_err());
}

#[test]
fn composer_tracks_multiline_cursor_and_edits() {
    let mut state = test_state();
    state.insert_text("hello");
    state.insert_newline();
    state.insert_text("world");
    assert_eq!(state.input, "hello\nworld");
    assert_eq!(state.cursor_line_col(), (1, 5));

    state.move_up();
    assert_eq!(state.cursor_line_col(), (0, 5));
    state.backspace();
    assert_eq!(state.input, "hell\nworld");
    state.move_down();
    state.move_home();
    state.delete_forward();
    assert_eq!(state.input, "hell\norld");
}

#[test]
fn state_tracks_turn_lifecycle_rows() {
    let mut state = test_state();
    state.active_turn_id = Some("turn-123456".to_string());
    state.begin_assistant();
    state.append_assistant_delta("hi");
    state.append_thinking_delta("plan");
    assert!(
        state
            .history
            .iter()
            .any(|line| line.role == ChatLineRole::Assistant && line.text == "hi")
    );
    assert!(
        state
            .history
            .iter()
            .any(|line| line.role == ChatLineRole::Thinking && line.text == "plan")
    );

    state.finish_turn();
    assert_eq!(state.active_turn_id, None);
    assert_eq!(state.turn_state, "idle");
}

fn test_state() -> ChatTuiState {
    ChatTuiState::new(
        CodexTuiThread {
            id: "thread-123456".to_string(),
            raw: json!({
                "id": "thread-123456",
                "cwd": "/tmp/work",
                "name": "demo",
            }),
        },
        ChatSessionInfo {
            connection_label: "test".to_string(),
            cwd: "/tmp/work".to_string(),
            model_label: "local/echo".to_string(),
            models: vec!["local/echo (default)".to_string()],
        },
    )
}
