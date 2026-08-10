use crate::app::{App, Flow};
use crate::{Action, ChatEvent, SessionMeta, parse_slash_command};

fn meta() -> SessionMeta {
    SessionMeta {
        connection_label: "local/private".to_string(),
        cwd: "/work".to_string(),
        model_label: "provider/model".to_string(),
        thread_id: "thread-12345678".to_string(),
        thread_name: None,
        version: "0.0.0-test".to_string(),
    }
}

fn app() -> App {
    App::new(meta())
}

fn key(code: tuika::KeyCode) -> tuika::Event {
    tuika::Event::Key(tuika::Key::new(code))
}

fn type_text(app: &mut App, text: &str) {
    for ch in text.chars() {
        let _ = app.handle(&key(tuika::KeyCode::Char(ch)));
    }
}

#[test]
fn slash_parser_accepts_chat_session_commands() {
    use crate::SlashCommand;
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
fn slash_parser_rejects_unknown_or_incomplete_commands() {
    assert!(parse_slash_command("/wat").is_err());
    assert!(parse_slash_command("/resume").is_err());
    assert!(parse_slash_command("/rename").is_err());
    assert!(parse_slash_command("/").is_err());
}

#[test]
fn submitting_a_prompt_queues_the_action_and_echoes_the_user_cell() {
    let mut app = app();
    app.submit("hello there");
    assert_eq!(
        app.drain_actions(),
        vec![Action::Submit("hello there".to_string())]
    );
    assert!(
        app.cells
            .iter()
            .any(|cell| matches!(cell, crate::Cell::User(text) if text == "hello there"))
    );
}

#[test]
fn idle_gated_commands_refuse_during_an_active_turn() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    for command in ["/new", "/fork", "/compact", "/resume abc"] {
        app.submit(command);
        assert_eq!(app.drain_actions(), Vec::new(), "{command} must be gated");
    }
    app.apply(ChatEvent::TurnCompleted { error: None });
    app.submit("/new");
    assert_eq!(app.drain_actions(), vec![Action::NewThread]);
}

#[test]
fn deltas_stream_into_one_cell_and_close_on_turn_completion() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::ThinkingDelta("mm".to_string()));
    app.apply(ChatEvent::AnswerDelta("first ".to_string()));
    app.apply(ChatEvent::AnswerDelta("second".to_string()));
    let answers = app
        .cells
        .iter()
        .filter(|cell| matches!(cell, crate::Cell::Answer(_)))
        .count();
    assert_eq!(answers, 1, "deltas must append to one streaming cell");

    app.apply(ChatEvent::TurnCompleted { error: None });
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-2".to_string(),
    });
    app.apply(ChatEvent::AnswerDelta("third".to_string()));
    let answers = app
        .cells
        .iter()
        .filter(|cell| matches!(cell, crate::Cell::Answer(_)))
        .count();
    assert_eq!(answers, 2, "a new turn opens a new answer cell");
}

#[test]
fn empty_deltas_do_not_open_transcript_cells_or_output_rows() {
    let mut app = app();
    app.apply(ChatEvent::AnswerDelta(String::new()));
    app.apply(ChatEvent::ThinkingDelta(String::new()));
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "build".to_string(),
    });
    app.apply(ChatEvent::ToolOutputDelta {
        id: "call-1".to_string(),
        delta: String::new(),
    });

    assert!(
        !app.cells
            .iter()
            .any(|cell| matches!(cell, crate::Cell::Answer(_)))
    );
    assert!(
        !app.cells
            .iter()
            .any(|cell| matches!(cell, crate::Cell::Reasoning { .. }))
    );
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Exec { output, .. } if output.is_empty()
    )));
}

#[test]
fn tool_events_target_their_cell_by_id() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "cargo test".to_string(),
    });
    app.apply(ChatEvent::ToolOutputDelta {
        id: "call-1".to_string(),
        delta: "running 3 tests\nok\n".to_string(),
    });
    app.apply(ChatEvent::ToolCompleted {
        id: "call-1".to_string(),
        success: true,
        output: String::new(),
    });
    let exec = app
        .cells
        .iter()
        .find_map(|cell| match cell {
            crate::Cell::Exec { output, status, .. } => Some((output.clone(), *status)),
            _ => None,
        })
        .expect("exec cell");
    assert_eq!(exec.0, vec!["running 3 tests", "ok", ""]);
    assert_eq!(exec.1, crate::ExecStatus::Ok);
}

#[test]
fn a_tool_call_splits_the_streamed_answer() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::AnswerDelta("before".to_string()));
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "read file".to_string(),
    });
    app.apply(ChatEvent::AnswerDelta("after".to_string()));
    let kinds: Vec<&str> = app
        .cells
        .iter()
        .map(|cell| match cell {
            crate::Cell::Answer(_) => "answer",
            crate::Cell::Exec { .. } => "exec",
            _ => "other",
        })
        .filter(|kind| *kind != "other")
        .collect();
    assert_eq!(kinds, vec!["answer", "exec", "answer"]);
}

#[test]
fn escape_interrupts_only_while_a_turn_is_active() {
    let mut app = app();
    assert_eq!(app.handle(&key(tuika::KeyCode::Esc)), Flow::Continue);
    assert_eq!(app.drain_actions(), Vec::new());

    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    assert_eq!(app.handle(&key(tuika::KeyCode::Esc)), Flow::Continue);
    assert_eq!(app.drain_actions(), vec![Action::Interrupt]);
}

#[test]
fn ctrl_c_interrupts_then_quits() {
    let mut app = app();
    let ctrl_c = tuika::Event::Key(tuika::Key {
        code: tuika::KeyCode::Char('c'),
        ctrl: true,
        alt: false,
        shift: false,
    });
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    assert_eq!(app.handle(&ctrl_c), Flow::Continue);
    assert_eq!(app.drain_actions(), vec![Action::Interrupt]);
    app.apply(ChatEvent::TurnCompleted { error: None });
    assert_eq!(app.handle(&ctrl_c), Flow::Quit);
}

#[test]
fn typing_slash_opens_the_completion_popup_and_enter_runs_the_pick() {
    let mut app = app();
    type_text(&mut app, "/");
    assert!(!app.popup_items().is_empty(), "popup must open on /");
    let first = app.popup_items()[0].0.clone();
    assert_eq!(first, "/help");

    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert!(app.popup_items().is_empty(), "popup closes on confirm");
    assert!(
        app.cells.iter().any(|cell| matches!(
            cell,
            crate::Cell::Notice { title, .. } if title == "Commands"
        )),
        "/help ran"
    );
}

#[test]
fn popup_filter_narrows_with_the_typed_prefix() {
    let mut app = app();
    type_text(&mut app, "/re");
    let labels: Vec<String> = app
        .popup_items()
        .into_iter()
        .map(|(label, _)| label)
        .collect();
    assert_eq!(labels, vec!["/resume".to_string(), "/rename".to_string()]);
}

#[test]
fn enter_with_no_popup_matches_submits_the_unknown_command() {
    let mut app = app();
    type_text(&mut app, "/wat");
    assert!(app.popup_items().is_empty());

    let _ = app.handle(&key(tuika::KeyCode::Enter));

    assert!(app.popup_items().is_empty());
    assert!(app.composer.is_empty());
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { tone: crate::Tone::Error, title, .. }
            if title.contains("unknown slash command /wat")
    )));
}

/// Render the app at 80x24 and return the glyph grid.
fn render(app: &mut App, no_color: bool) -> String {
    render_at(app, no_color, 80, 24)
}

fn render_at(app: &mut App, no_color: bool, width: u16, height: u16) -> String {
    let theme = crate::chat_theme(no_color);
    let sheet = tuika::StyleSheet::from_theme(&theme);
    let probe = tuika::probe::RectProbe::new();
    let root = crate::ui::build(
        app,
        ratatui::layout::Rect::new(0, 0, width, height),
        &theme,
        &sheet,
        &probe,
    );
    let buffer = tuika::testing::render(root.as_ref(), width, height, &theme);
    tuika::testing::grid(&buffer)
}

#[test]
fn shift_enter_inserts_a_newline_and_enter_submits() {
    let mut app = app();
    type_text(&mut app, "first");
    let _ = app.handle(&tuika::Event::Key(tuika::Key {
        code: tuika::KeyCode::Enter,
        ctrl: false,
        alt: false,
        shift: true,
    }));
    type_text(&mut app, "second");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::Submit("first\nsecond".to_string())]
    );
}

#[test]
fn paste_inserts_text_without_submitting() {
    let mut app = app();
    let _ = app.handle(&tuika::Event::Paste("pasted\ntext".to_string()));
    assert_eq!(app.drain_actions(), Vec::new());
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::Submit("pasted\ntext".to_string())]
    );
}

#[test]
fn up_on_an_empty_composer_recalls_earlier_prompts() {
    let mut app = app();
    app.submit("first prompt");
    app.submit("second prompt");
    let _ = app.drain_actions();

    let _ = app.handle(&key(tuika::KeyCode::Up));
    assert_eq!(app.composer.text(), "second prompt");
    app.composer.clear();
    let _ = app.handle(&key(tuika::KeyCode::Up));
    // The cursor walked one step further back.
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::Submit("first prompt".to_string())]
    );
}

#[test]
fn ctrl_d_quits_only_on_an_empty_composer() {
    let mut app = app();
    let ctrl_d = tuika::Event::Key(tuika::Key {
        code: tuika::KeyCode::Char('d'),
        ctrl: true,
        alt: false,
        shift: false,
    });
    type_text(&mut app, "draft");
    assert_eq!(app.handle(&ctrl_d), Flow::Continue);
    app.composer.clear();
    assert_eq!(app.handle(&ctrl_d), Flow::Quit);
}

#[test]
fn tab_completes_an_argument_command_and_leaves_the_user_typing() {
    let mut app = app();
    type_text(&mut app, "/res");
    let labels: Vec<String> = app
        .popup_items()
        .into_iter()
        .map(|(label, _)| label)
        .collect();
    assert_eq!(labels, vec!["/resume".to_string()]);
    let _ = app.handle(&key(tuika::KeyCode::Tab));
    assert_eq!(app.composer.text(), "/resume ");
    assert_eq!(
        app.drain_actions(),
        Vec::new(),
        "tab must not run the command"
    );
}

#[test]
fn escape_dismisses_the_popup_without_clearing_the_composer() {
    let mut app = app();
    type_text(&mut app, "/sess");
    assert!(!app.popup_items().is_empty());
    let _ = app.handle(&key(tuika::KeyCode::Esc));
    assert!(app.popup_items().is_empty());
    assert_eq!(app.composer.text(), "/sess");
    assert_eq!(app.drain_actions(), Vec::new());
}

#[test]
fn confirming_an_argument_command_from_the_popup_completes_instead_of_running() {
    let mut app = app();
    type_text(&mut app, "/ren");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(app.composer.text(), "/rename ");
    assert_eq!(app.drain_actions(), Vec::new());
}

#[test]
fn submit_during_an_active_turn_still_queues_and_steered_updates_the_label() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.submit("also check the docs");
    assert_eq!(
        app.drain_actions(),
        vec![Action::Submit("also check the docs".to_string())]
    );
    app.apply(ChatEvent::TurnSteered);
    assert_eq!(app.turn_state, "steered");
}

#[test]
fn rpc_error_notice_does_not_finish_an_unrelated_active_turn() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::Error {
        title: "request `thread/list` was refused".to_string(),
        body: Vec::new(),
    });

    assert!(app.turn_active());
    assert!(app.turn_state.starts_with("running"));
}

#[test]
fn thread_switched_resets_turn_state_and_token_count() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::Usage { total_tokens: 500 });
    app.apply(ChatEvent::ThreadSwitched {
        thread_id: "thread-99999999".to_string(),
        name: Some("fresh".to_string()),
        cwd: None,
        reason: "started thread".to_string(),
    });
    assert!(!app.turn_active());
    assert_eq!(app.turn_state, "idle");
    assert_eq!(app.total_tokens, 0);
    assert_eq!(app.meta.thread_id, "thread-99999999");
    assert_eq!(app.meta.thread_name.as_deref(), Some("fresh"));
}

#[test]
fn a_new_turn_resets_previous_turn_usage() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::Usage { total_tokens: 500 });
    app.apply(ChatEvent::TurnCompleted { error: None });
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-2".to_string(),
    });

    assert_eq!(app.total_tokens, 0);
}

#[test]
fn usage_accumulates_across_model_requests_in_one_turn() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::Usage { total_tokens: 120 });
    app.apply(ChatEvent::Usage { total_tokens: 80 });

    assert_eq!(app.total_tokens, 200);
}

#[test]
fn status_command_reports_state_and_tokens() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::Usage { total_tokens: 1234 });
    app.submit("/status");
    let rows = app
        .cells
        .iter()
        .find_map(|cell| match cell {
            crate::Cell::Config { title, rows } if title == "Session" => Some(rows.clone()),
            _ => None,
        })
        .expect("status panel");
    assert!(
        rows.iter()
            .any(|(k, v)| k == "state" && v.starts_with("running"))
    );
    assert!(rows.iter().any(|(k, v)| k == "tokens" && v == "1234"));
    assert!(rows.iter().any(|(k, _)| k == "connection"));
}

#[test]
fn clear_empties_the_transcript_and_streams_reopen_after() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::AnswerDelta("half an answer".to_string()));
    app.submit("/clear");
    assert!(
        !app.cells
            .iter()
            .any(|cell| matches!(cell, crate::Cell::Answer(_))),
        "clear must drop the transcript"
    );
    // A delta after /clear must open a fresh cell, not write into the dropped one.
    app.apply(ChatEvent::AnswerDelta("more".to_string()));
    assert_eq!(
        app.cells
            .iter()
            .filter(|cell| matches!(cell, crate::Cell::Answer(_)))
            .count(),
        1
    );
}

#[test]
fn interrupt_command_when_idle_is_a_notice_not_an_action() {
    let mut app = app();
    app.submit("/interrupt");
    assert_eq!(app.drain_actions(), Vec::new());
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { title, .. } if title == "no active turn to interrupt"
    )));
}

#[test]
fn unknown_slash_command_reports_an_error_notice() {
    let mut app = app();
    app.submit("/frobnicate");
    assert_eq!(app.drain_actions(), Vec::new());
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { tone: crate::Tone::Error, title, .. }
            if title.contains("unknown slash command /frobnicate")
    )));
}

#[test]
fn help_lists_every_command() {
    let mut app = app();
    app.submit("/help");
    let body = app
        .cells
        .iter()
        .find_map(|cell| match cell {
            crate::Cell::Notice { title, body, .. } if title == "Commands" => Some(body.clone()),
            _ => None,
        })
        .expect("help notice");
    assert_eq!(body.len(), crate::COMMANDS.len());
    for (label, _, _) in crate::COMMANDS {
        assert!(
            body.iter().any(|line| line.starts_with(*label)),
            "missing {label}"
        );
    }
}

#[test]
fn partial_output_deltas_join_across_line_boundaries() {
    let mut app = app();
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "build".to_string(),
    });
    app.apply(ChatEvent::ToolOutputDelta {
        id: "call-1".to_string(),
        delta: "Compil".to_string(),
    });
    app.apply(ChatEvent::ToolOutputDelta {
        id: "call-1".to_string(),
        delta: "ing verlet\nFinished".to_string(),
    });
    let output = app
        .cells
        .iter()
        .find_map(|cell| match cell {
            crate::Cell::Exec { output, .. } => Some(output.clone()),
            _ => None,
        })
        .expect("exec cell");
    assert_eq!(output, vec!["Compiling verlet", "Finished"]);
}

#[test]
fn renders_exec_cells_with_elision_and_failure_marker() {
    let mut app = app();
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "cargo test".to_string(),
    });
    let long_output = (1..=10)
        .map(|i| format!("line {i}"))
        .collect::<Vec<_>>()
        .join("\n");
    app.apply(ChatEvent::ToolCompleted {
        id: "call-1".to_string(),
        success: false,
        output: long_output,
    });
    let grid = render(&mut app, false);
    assert!(grid.contains("cargo test"), "exec title:\n{grid}");
    assert!(grid.contains("(failed)"), "failure marker:\n{grid}");
    assert!(grid.contains("… +4 lines"), "elision marker:\n{grid}");
    assert!(grid.contains("line 1"), "head window:\n{grid}");
    assert!(grid.contains("line 10"), "tail window:\n{grid}");
    assert!(!grid.contains("line 5"), "elided middle leaked:\n{grid}");
}

#[test]
fn renders_thinking_sessions_and_notice_tones() {
    let mut app = app();
    app.apply(ChatEvent::ThinkingDelta("weighing options".to_string()));
    app.apply(ChatEvent::Sessions(vec![
        crate::SessionRow {
            id: "thread-12345678".to_string(),
            name: "alpha".to_string(),
            status: "idle".to_string(),
            preview: "hello".to_string(),
            current: true,
        },
        crate::SessionRow {
            id: "thread-abcdefgh".to_string(),
            name: "beta".to_string(),
            status: "running".to_string(),
            preview: String::new(),
            current: false,
        },
    ]));
    app.apply(ChatEvent::Error {
        title: "boom".to_string(),
        body: vec!["details".to_string()],
    });
    app.apply(ChatEvent::ResyncStarted);
    let grid = render(&mut app, false);
    assert!(grid.contains("Thinking"), "thinking header:\n{grid}");
    assert!(grid.contains("weighing options"), "thinking body:\n{grid}");
    assert!(
        grid.contains("* thread-1 alpha [idle] - hello"),
        "current session row:\n{grid}"
    );
    assert!(
        grid.contains("thread-a beta [running]"),
        "other session row:\n{grid}"
    );
    assert!(grid.contains("✗ boom"), "error glyph:\n{grid}");
    assert!(grid.contains("⚠ stream lagged"), "warn glyph:\n{grid}");
}

#[test]
fn renders_markdown_answers_with_code() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::AnswerDelta(
        "Fix the guard:\n\n```rust\nlet x = 1;\n```\n\nDone.".to_string(),
    ));
    app.apply(ChatEvent::TurnCompleted { error: None });
    let grid = render(&mut app, false);
    assert!(grid.contains("Fix the guard:"), "prose:\n{grid}");
    assert!(grid.contains("let x = 1;"), "code block body:\n{grid}");
    assert!(grid.contains("Done."), "trailing prose:\n{grid}");
}

fn model_rows() -> Vec<crate::ModelRow> {
    vec![
        crate::ModelRow {
            provider_id: "provider".to_string(),
            model: "model-a".to_string(),
            display_name: "Model A".to_string(),
            auth_status: "configured".to_string(),
            active: true,
        },
        crate::ModelRow {
            provider_id: "provider".to_string(),
            model: "model-b".to_string(),
            display_name: "Model B".to_string(),
            auth_status: "missing".to_string(),
            active: false,
        },
    ]
}

#[test]
fn models_event_opens_the_picker_preselecting_the_active_row() {
    let mut app = app();
    app.apply(ChatEvent::Models(model_rows()));
    let grid = render(&mut app, true);
    assert!(grid.contains("Select a model"), "picker title:\n{grid}");
    assert!(grid.contains("Model A"), "display name row:\n{grid}");
    assert!(
        grid.contains("provider/model-b"),
        "coordinates row:\n{grid}"
    );
    assert!(grid.contains("active"), "active marker:\n{grid}");
    assert!(grid.contains("needs login"), "auth marker:\n{grid}");

    // Enter on the preselected (active) row is a no-op notice, no action.
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(app.drain_actions(), Vec::new());
    let grid = render(&mut app, true);
    assert!(!grid.contains("Select a model"), "picker closed:\n{grid}");
}

/// `model_rows()` with the second row authenticated, for pinning the plain
/// selection path (a `missing` row routes into the setup wizard instead).
fn configured_model_rows() -> Vec<crate::ModelRow> {
    let mut rows = model_rows();
    rows[1].auth_status = "configured".to_string();
    rows
}

#[test]
fn picker_selection_emits_select_model_and_esc_dismisses() {
    let mut app = app();
    app.apply(ChatEvent::Models(configured_model_rows()));
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::SelectModel {
            provider_id: "provider".to_string(),
            model: "model-b".to_string(),
        }]
    );

    // Reopen, then Esc closes without selecting and without interrupting.
    app.apply(ChatEvent::Models(model_rows()));
    let _ = app.handle(&key(tuika::KeyCode::Esc));
    assert_eq!(app.drain_actions(), Vec::new());
    let grid = render(&mut app, true);
    assert!(!grid.contains("Select a model"), "picker closed:\n{grid}");
}

#[test]
fn picker_swallows_typing_and_model_selected_updates_the_footer_label() {
    let mut app = app();
    app.apply(ChatEvent::Models(model_rows()));
    type_text(&mut app, "stray keys");
    assert!(
        app.composer.is_empty(),
        "composer must stay untouched while the picker is open"
    );
    let _ = app.handle(&key(tuika::KeyCode::Esc));

    app.apply(ChatEvent::ModelSelected {
        provider_id: "provider".to_string(),
        model: "model-b".to_string(),
    });
    assert_eq!(app.meta.model_label, "provider/model-b");
    let grid = render(&mut app, true);
    assert!(
        grid.contains("model set to provider/model-b"),
        "confirmation notice:\n{grid}"
    );
}

#[test]
fn picker_is_modal_over_paste_scrolling_and_global_control_keys() {
    let mut app = app();
    type_text(&mut app, "/");
    assert!(!app.popup_items().is_empty());
    app.content_h = 100;
    app.viewport_h = 10;
    app.scroll.jump_to_bottom(app.content_h, app.viewport_h);
    let scroll_offset = app.scroll.offset();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::Models(model_rows()));

    let _ = app.handle(&tuika::Event::Paste("must not leak".to_string()));
    let _ = app.handle(&key(tuika::KeyCode::PageUp));
    for code in [tuika::KeyCode::Char('c'), tuika::KeyCode::Char('d')] {
        assert_eq!(
            app.handle(&tuika::Event::Key(tuika::Key {
                code,
                ctrl: true,
                alt: false,
                shift: false,
            })),
            Flow::Continue
        );
    }

    assert_eq!(app.composer.text(), "/");
    assert_eq!(app.scroll.offset(), scroll_offset);
    assert_eq!(app.drain_actions(), Vec::new());
    assert!(render(&mut app, true).contains("Select a model"));

    // Esc belongs to the picker even while a turn is active.
    let _ = app.handle(&key(tuika::KeyCode::Esc));
    assert_eq!(app.drain_actions(), Vec::new());
    assert!(!render(&mut app, true).contains("Select a model"));
}

#[test]
fn models_event_replaces_popup_and_picker_and_empty_result_closes_stale_picker() {
    let mut app = app();
    type_text(&mut app, "/");
    assert!(!app.popup_items().is_empty());

    let mut replacement = model_rows();
    replacement[0].active = false;
    replacement[1].active = false;
    app.apply(ChatEvent::Models(replacement.clone()));
    assert!(
        app.popup_items().is_empty(),
        "picker must win focus over popup"
    );
    assert_eq!(app.picker.as_ref().unwrap().state.selected(), Some(0));

    replacement[0].display_name = "Replacement".to_string();
    app.apply(ChatEvent::Models(replacement));
    assert_eq!(
        app.picker.as_ref().unwrap().rows[0].display_name,
        "Replacement"
    );

    app.apply(ChatEvent::Models(Vec::new()));
    assert!(app.picker.is_none());
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { tone: crate::Tone::Error, title, .. }
            if title == "no models available"
    )));
}

#[test]
fn missing_auth_selection_routes_into_the_setup_wizard() {
    let mut app = app();
    app.apply(ChatEvent::Models(model_rows()));
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    // No doomed SelectModel: the UI fetches the provider catalog to open the
    // credential step for `provider` instead.
    assert_eq!(app.drain_actions(), vec![Action::ListProviders]);
    assert!(app.picker.is_none());
}

#[test]
fn rejected_selection_keeps_model_and_active_turn_consistent() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    // Authenticated on the client's view, but the server still rejects the
    // switch (stale key, unreachable endpoint).
    app.apply(ChatEvent::Models(configured_model_rows()));
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert!(matches!(
        app.drain_actions().as_slice(),
        [Action::SelectModel { provider_id, model }]
            if provider_id == "provider" && model == "model-b"
    ));

    app.apply(ChatEvent::Error {
        title: "model/select rejected: authentication required".to_string(),
        body: Vec::new(),
    });

    assert_eq!(app.meta.model_label, "provider/model");
    assert!(app.turn_active());
    assert!(app.picker.is_none());
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { tone: crate::Tone::Error, title, .. }
            if title.contains("authentication required")
    )));
}

#[test]
fn selection_during_active_turn_uses_server_echo_and_explains_deferred_effect() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::ModelSelected {
        provider_id: "echoed-provider".to_string(),
        model: "echoed-model".to_string(),
    });

    assert_eq!(app.meta.model_label, "echoed-provider/echoed-model");
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { title, body, .. }
            if title == "model set to echoed-provider/echoed-model"
                && body == &["applies to turns after the current one".to_string()]
    )));
}

#[test]
fn long_and_wide_model_rows_fit_one_line_at_eighty_columns() {
    let mut app = app();
    app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "provider-with-a-very-long-identifier".repeat(2),
        model: "model-with-a-very-long-identifier".repeat(2),
        display_name: "模型🙂 with an extremely long display name ".repeat(3),
        auth_status: "missing".to_string(),
        active: false,
    }]));

    let grid = render_at(&mut app, true, 80, 24);
    assert_eq!(
        grid.lines()
            .filter(|line| line.contains('模') && line.contains('型'))
            .count(),
        1,
        "wide display name missing or wrapped:\n{grid}"
    );
    assert!(
        grid.contains("provider-with"),
        "provider coordinate clipped: {grid}"
    );
    assert!(grid.contains("needs login"), "auth status clipped: {grid}");
}

#[test]
fn model_picker_build_is_safe_at_zero_and_tiny_terminal_sizes() {
    for (width, height) in [(0, 0), (1, 1), (20, 4)] {
        let mut app = app();
        app.apply(ChatEvent::Models(model_rows()));
        let _ = render_at(&mut app, true, width, height);
    }
}

#[test]
fn long_transcripts_follow_the_tail() {
    let mut app = app();
    for i in 0..40 {
        app.apply(ChatEvent::Info {
            title: format!("notice number {i}"),
            body: Vec::new(),
        });
    }
    // First render measures geometry; second applies the follow offset the
    // append re-armed against it.
    let _ = render(&mut app, false);
    app.apply(ChatEvent::Info {
        title: "the last notice".to_string(),
        body: Vec::new(),
    });
    let grid = render(&mut app, false);
    assert!(
        grid.contains("the last notice"),
        "tail must stay visible:\n{grid}"
    );
    assert!(
        !grid.contains("notice number 0"),
        "head must have scrolled out:\n{grid}"
    );
}

#[test]
fn transcript_updates_preserve_manual_scrollback() {
    let mut app = app();
    app.content_h = 100;
    app.viewport_h = 10;
    app.scroll.jump_to_bottom(app.content_h, app.viewport_h);
    let _ = app
        .scroll
        .handle(&key(tuika::KeyCode::PageUp), app.content_h, app.viewport_h);
    let offset = app.scroll.offset();
    assert!(!app.scroll.is_stuck_to_bottom());

    app.apply(ChatEvent::Info {
        title: "new output".to_string(),
        body: Vec::new(),
    });

    assert_eq!(app.scroll.offset(), offset);
    assert!(!app.scroll.is_stuck_to_bottom());
}

#[test]
fn long_single_line_tool_output_wraps_instead_of_clipping_the_tail() {
    let mut app = app();
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "emit".to_string(),
    });
    app.apply(ChatEvent::ToolCompleted {
        id: "call-1".to_string(),
        success: true,
        output: format!("HEAD {} TAIL", "x".repeat(180)),
    });

    let grid = render(&mut app, false);
    assert!(
        grid.contains("TAIL"),
        "long output tail was clipped:\n{grid}"
    );
}

#[test]
fn wide_character_input_submits_losslessly() {
    let mut app = app();
    type_text(&mut app, "你🙂é");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::Submit("你🙂é".to_string())]
    );
}

#[test]
fn zero_size_terminal_build_is_safe() {
    let mut app = app();
    app.apply(ChatEvent::ToolStarted {
        id: "call-1".to_string(),
        title: "emit".to_string(),
    });
    app.apply(ChatEvent::ToolCompleted {
        id: "call-1".to_string(),
        success: true,
        output: "long output".repeat(20),
    });
    let theme = crate::chat_theme(false);
    let sheet = tuika::StyleSheet::from_theme(&theme);
    let probe = tuika::probe::RectProbe::new();
    let _ = crate::ui::build(
        &mut app,
        ratatui::layout::Rect::new(0, 0, 0, 0),
        &theme,
        &sheet,
        &probe,
    );
}

#[test]
fn footer_shows_thread_and_turn_state() {
    let mut app = app();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-abcdefgh-rest".to_string(),
    });
    let grid = render(&mut app, false);
    assert!(grid.contains("thread-1 ·"), "thread in footer:\n{grid}");
    assert!(grid.contains("running turn-abc"), "turn state:\n{grid}");
    assert!(grid.contains("Working ("), "working row:\n{grid}");
}

#[test]
fn renders_a_welcome_screen_snapshot() {
    let mut app = app();
    let theme = crate::chat_theme(false);
    let sheet = tuika::StyleSheet::from_theme(&theme);
    let probe = tuika::probe::RectProbe::new();
    let root = crate::ui::build(
        &mut app,
        ratatui::layout::Rect::new(0, 0, 80, 24),
        &theme,
        &sheet,
        &probe,
    );
    let buffer = tuika::testing::render(root.as_ref(), 80, 24, &theme);
    let grid = tuika::testing::grid(&buffer);
    assert!(grid.contains("Verlet chat"), "banner missing:\n{grid}");
    assert!(grid.contains("/help"), "tips missing:\n{grid}");
    assert!(
        grid.contains("Describe a task"),
        "composer placeholder missing:\n{grid}"
    );
    assert!(grid.contains("⏎ send"), "footer hints missing:\n{grid}");
}

#[test]
fn renders_a_turn_in_flight_snapshot() {
    let mut app = app();
    app.submit("why does the test fail?");
    let _ = app.drain_actions();
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    app.apply(ChatEvent::AnswerDelta(
        "Looking at the *snapshot*…".to_string(),
    ));
    let theme = crate::chat_theme(false);
    let sheet = tuika::StyleSheet::from_theme(&theme);
    let probe = tuika::probe::RectProbe::new();
    let root = crate::ui::build(
        &mut app,
        ratatui::layout::Rect::new(0, 0, 80, 24),
        &theme,
        &sheet,
        &probe,
    );
    let buffer = tuika::testing::render(root.as_ref(), 80, 24, &theme);
    let grid = tuika::testing::grid(&buffer);
    assert!(
        grid.contains("› why does the test fail?"),
        "user cell:\n{grid}"
    );
    assert!(grid.contains("Working ("), "working row:\n{grid}");
    assert!(grid.contains("Esc to interrupt"), "interrupt hint:\n{grid}");
    assert!(grid.contains("Looking at the snapshot…"), "answer:\n{grid}");
}

// --- setup wizard ---

fn provider_rows() -> Vec<crate::ProviderRow> {
    vec![
        crate::ProviderRow {
            provider_id: "anthropic".to_string(),
            display_name: "Anthropic".to_string(),
            auth_status: "configured".to_string(),
            label: "stored credential".to_string(),
            oauth: false,
            active: true,
        },
        crate::ProviderRow {
            provider_id: "openai".to_string(),
            display_name: "OpenAI".to_string(),
            auth_status: "missing".to_string(),
            label: "no credential".to_string(),
            oauth: false,
            active: false,
        },
        crate::ProviderRow {
            provider_id: "openai-codex".to_string(),
            display_name: "OpenAI Codex".to_string(),
            auth_status: "missing".to_string(),
            label: "no credential".to_string(),
            oauth: true,
            active: false,
        },
    ]
}

fn open_wizard(app: &mut App) {
    app.submit("/setup");
    assert_eq!(app.drain_actions(), vec![Action::ListProviders]);
    app.apply(ChatEvent::Providers(provider_rows()));
}

#[test]
fn setup_command_opens_the_provider_step_preselecting_the_active_row() {
    let mut app = app();
    open_wizard(&mut app);
    let grid = render(&mut app, true);
    assert!(grid.contains("Set up a provider"), "title:\n{grid}");
    assert!(grid.contains("Anthropic"), "provider name:\n{grid}");
    assert!(
        grid.contains("✓ stored credential"),
        "connected status:\n{grid}"
    );
    assert!(grid.contains("needs API key"), "key status:\n{grid}");
    assert!(grid.contains("needs sign-in"), "oauth status:\n{grid}");
    assert!(grid.contains("active"), "active marker:\n{grid}");
    assert!(grid.contains("c configure"), "footer hints:\n{grid}");

    // Enter on the preselected (connected) row heads to the model step.
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(app.drain_actions(), vec![Action::ListModels]);
    let grid = render(&mut app, true);
    assert!(
        !grid.contains("Set up a provider"),
        "wizard hidden:\n{grid}"
    );
}

#[test]
fn wizard_model_step_scopes_the_picker_to_the_chosen_provider() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::Models(vec![
        crate::ModelRow {
            provider_id: "anthropic".to_string(),
            model: "model-a".to_string(),
            display_name: "Model A".to_string(),
            auth_status: "configured".to_string(),
            active: true,
        },
        crate::ModelRow {
            provider_id: "openai".to_string(),
            model: "model-b".to_string(),
            display_name: "Model B".to_string(),
            auth_status: "missing".to_string(),
            active: false,
        },
    ]));
    let grid = render(&mut app, true);
    assert!(grid.contains("Select a model"), "picker title:\n{grid}");
    assert!(grid.contains("Model A"), "scoped row:\n{grid}");
    assert!(!grid.contains("Model B"), "other provider leaked:\n{grid}");
}

#[test]
fn unconnected_provider_routes_through_the_credential_step_to_key_entry() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let grid = render(&mut app, true);
    assert!(grid.contains("Connect OpenAI"), "credential title:\n{grid}");
    assert!(grid.contains("Paste API key"), "key option:\n{grid}");
    assert!(grid.contains("Clear saved key"), "clear option:\n{grid}");
    assert!(
        !grid.contains("Sign in with browser"),
        "oauth leaked:\n{grid}"
    );

    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let grid = render(&mut app, true);
    assert!(
        grid.contains("Paste the OpenAI API key"),
        "input title:\n{grid}"
    );

    type_text(&mut app, "sk-secret-123");
    let grid = render(&mut app, true);
    assert!(!grid.contains("sk-secret-123"), "key echoed:\n{grid}");
    assert!(grid.contains("•••"), "mask missing:\n{grid}");

    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::SetProviderKey {
            provider_id: "openai".to_string(),
            api_key: "sk-secret-123".to_string(),
        }]
    );
    let grid = render(&mut app, true);
    assert!(grid.contains("saving…"), "busy hint:\n{grid}");

    // Success continues into the provider's model list.
    app.apply(ChatEvent::CredentialResult {
        provider_id: "openai".to_string(),
        error: None,
    });
    assert_eq!(app.drain_actions(), vec![Action::ListModels]);
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { title, .. } if title == "openai: credential saved"
    )));
}

#[test]
fn empty_and_failed_key_submissions_surface_errors_in_place() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.handle(&key(tuika::KeyCode::Enter));

    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(app.drain_actions(), Vec::new());
    let grid = render(&mut app, true);
    assert!(grid.contains("API key is empty"), "empty error:\n{grid}");

    type_text(&mut app, "sk-bad");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::CredentialResult {
        provider_id: "openai".to_string(),
        error: Some("provider rejected the key".to_string()),
    });
    assert_eq!(app.drain_actions(), Vec::new());
    let grid = render(&mut app, true);
    assert!(
        grid.contains("provider rejected the key"),
        "failure shown in place:\n{grid}"
    );
}

#[test]
fn oauth_provider_offers_sign_in_and_shows_the_device_code() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let grid = render(&mut app, true);
    assert!(grid.contains("Connect OpenAI Codex"), "title:\n{grid}");
    assert!(
        grid.contains("Sign in with browser"),
        "browser option:\n{grid}"
    );

    // Pick the device flow (second option).
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::StartLogin {
            provider_id: "openai-codex".to_string(),
            method: crate::LoginMethod::Device,
        }]
    );
    let grid = render(&mut app, true);
    assert!(
        grid.contains("Sign in to OpenAI Codex"),
        "wait title:\n{grid}"
    );
    assert!(
        grid.contains("requesting a device code"),
        "wait body:\n{grid}"
    );

    app.apply(ChatEvent::LoginDeviceCode {
        verification_uri: "https://auth.example/device".to_string(),
        user_code: "ABCD-1234".to_string(),
    });
    let grid = render(&mut app, true);
    assert!(
        grid.contains("https://auth.example/device"),
        "verification uri:\n{grid}"
    );
    assert!(grid.contains("ABCD-1234"), "user code:\n{grid}");

    // Esc cancels the login and lands back on credentials with a notice.
    let _ = app.handle(&key(tuika::KeyCode::Esc));
    assert_eq!(app.drain_actions(), vec![Action::CancelLogin]);
    let grid = render(&mut app, true);
    assert!(
        grid.contains("Connect OpenAI Codex"),
        "back on credentials:\n{grid}"
    );
    assert!(grid.contains("sign-in canceled"), "cancel notice:\n{grid}");
}

#[test]
fn login_success_reissues_the_selection_that_started_the_wizard() {
    let mut app = app();
    // Enter on a "needs login" model row routes into the wizard.
    app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "openai-codex".to_string(),
        model: "gpt-5.6-sol".to_string(),
        display_name: "GPT 5.6 sol".to_string(),
        auth_status: "missing".to_string(),
        active: false,
    }]));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(app.drain_actions(), vec![Action::ListProviders]);

    // The catalog answer lands directly on the provider's credential step.
    app.apply(ChatEvent::Providers(provider_rows()));
    let grid = render(&mut app, true);
    assert!(
        grid.contains("Connect OpenAI Codex"),
        "credential step:\n{grid}"
    );

    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::StartLogin {
            provider_id: "openai-codex".to_string(),
            method: crate::LoginMethod::Browser,
        }]
    );
    app.apply(ChatEvent::LoginDeviceCode {
        verification_uri: "https://stale.example/device".to_string(),
        user_code: "STALE".to_string(),
    });
    assert!(matches!(
        app.setup.as_ref(),
        Some(crate::app::setup::SetupStep::LoginWait {
            method: crate::LoginMethod::Browser,
            device_code: None,
            ..
        })
    ));
    app.apply(ChatEvent::CredentialResult {
        provider_id: "openai-codex".to_string(),
        error: None,
    });
    assert_eq!(
        app.drain_actions(),
        vec![Action::SelectModel {
            provider_id: "openai-codex".to_string(),
            model: "gpt-5.6-sol".to_string(),
        }]
    );
    let grid = render(&mut app, true);
    assert!(!grid.contains("Sign in to"), "wizard closed:\n{grid}");
}

#[test]
fn wizard_swallows_composer_input_and_esc_dismisses_from_the_provider_step() {
    let mut app = app();
    open_wizard(&mut app);
    app.apply(ChatEvent::TurnStarted {
        turn_id: "turn-1".to_string(),
    });
    type_text(&mut app, "hello");
    let _ = app.handle(&tuika::Event::Paste("pasted".to_string()));
    let _ = app.handle(&tuika::Event::Key(tuika::Key {
        code: tuika::KeyCode::Char('c'),
        ctrl: true,
        alt: false,
        shift: false,
    }));
    assert!(app.composer.is_empty(), "composer must stay untouched");
    assert_eq!(app.drain_actions(), Vec::new());

    let _ = app.handle(&key(tuika::KeyCode::Esc));
    assert_eq!(app.drain_actions(), Vec::new());
    let grid = render(&mut app, true);
    assert!(
        !grid.contains("Set up a provider"),
        "wizard closed:\n{grid}"
    );
    type_text(&mut app, "hi");
    assert!(!app.composer.is_empty(), "composer usable again");
}

#[test]
fn stale_credential_results_report_out_of_band_without_reopening_the_wizard() {
    let mut app = app();
    app.apply(ChatEvent::CredentialResult {
        provider_id: "openai".to_string(),
        error: Some("boom".to_string()),
    });
    let grid = render(&mut app, true);
    assert!(
        !grid.contains("Set up a provider"),
        "wizard stayed shut:\n{grid}"
    );
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { tone: crate::Tone::Error, title, .. }
            if title == "openai: credential failed"
    )));
}

#[test]
fn setup_request_errors_release_invisible_wait_states() {
    let mut app = app();
    app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "missing-provider".to_string(),
        model: "model-a".to_string(),
        display_name: "Model A".to_string(),
        auth_status: "missing".to_string(),
        active: false,
    }]));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::Error {
        title: "provider catalog failed".to_string(),
        body: Vec::new(),
    });
    assert!(app.setup.is_none());
    assert!(app.pending_selection.is_none());

    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::Error {
        title: "model catalog failed".to_string(),
        body: Vec::new(),
    });
    assert!(app.setup.is_none());
    app.apply(ChatEvent::Models(vec![
        crate::ModelRow {
            provider_id: "anthropic".to_string(),
            model: "model-a".to_string(),
            display_name: "Model A".to_string(),
            auth_status: "configured".to_string(),
            active: true,
        },
        crate::ModelRow {
            provider_id: "openai".to_string(),
            model: "model-b".to_string(),
            display_name: "Model B".to_string(),
            auth_status: "missing".to_string(),
            active: false,
        },
    ]));
    let grid = render(&mut app, true);
    assert!(grid.contains("Model A"), "first model missing:\n{grid}");
    assert!(grid.contains("Model B"), "result stayed scoped:\n{grid}");
}

#[test]
fn setup_action_errors_return_busy_steps_to_interactive_state() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    type_text(&mut app, "sk-secret");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::Error {
        title: "saving sk-secret failed".to_string(),
        body: vec!["sk-secret was rejected".to_string()],
    });
    let grid = render(&mut app, true);
    assert!(!grid.contains("sk-secret"), "secret leaked:\n{grid}");
    assert!(!grid.contains("saving…"), "key input stayed busy:\n{grid}");
    type_text(&mut app, "sk-retry");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::SetProviderKey {
            provider_id: "openai".to_string(),
            api_key: "sk-retry".to_string(),
        }]
    );

    let mut oauth_app = App::new(meta());
    open_wizard(&mut oauth_app);
    let _ = oauth_app.handle(&key(tuika::KeyCode::Down));
    let _ = oauth_app.handle(&key(tuika::KeyCode::Down));
    let _ = oauth_app.handle(&key(tuika::KeyCode::Enter));
    let _ = oauth_app.handle(&key(tuika::KeyCode::Enter));
    let _ = oauth_app.drain_actions();
    oauth_app.apply(ChatEvent::Error {
        title: "login failed".to_string(),
        body: Vec::new(),
    });
    let grid = render(&mut oauth_app, true);
    assert!(
        grid.contains("Connect OpenAI Codex"),
        "login stayed busy:\n{grid}"
    );
    assert!(
        !grid.contains("Sign in to OpenAI Codex"),
        "login wait stayed open:\n{grid}"
    );
}

#[test]
fn credential_failure_never_renders_the_submitted_secret() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    type_text(&mut app, "sk-do-not-render");
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::CredentialResult {
        provider_id: "openai".to_string(),
        error: Some("provider echoed sk-do-not-render".to_string()),
    });
    let grid = render(&mut app, true);
    assert!(!grid.contains("sk-do-not-render"), "secret leaked:\n{grid}");
    assert!(
        grid.contains("provider echoed [redacted]"),
        "redacted error missing:\n{grid}"
    );

    let mut late_app = App::new(meta());
    open_wizard(&mut late_app);
    let _ = late_app.handle(&key(tuika::KeyCode::Down));
    let _ = late_app.handle(&key(tuika::KeyCode::Enter));
    let _ = late_app.handle(&key(tuika::KeyCode::Enter));
    type_text(&mut late_app, "sk-late-error");
    let _ = late_app.handle(&key(tuika::KeyCode::Enter));
    let _ = late_app.drain_actions();
    let _ = late_app.handle(&key(tuika::KeyCode::Esc));
    late_app.apply(ChatEvent::Error {
        title: "saving sk-late-error failed".to_string(),
        body: vec!["rejected sk-late-error".to_string()],
    });
    let grid = render(&mut late_app, true);
    assert!(
        !grid.contains("sk-late-error"),
        "late error leaked:\n{grid}"
    );
}

#[test]
fn catalog_events_do_not_clobber_open_credential_input() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    type_text(&mut app, "sk-kept");

    app.apply(ChatEvent::Providers(provider_rows()));
    app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "anthropic".to_string(),
        model: "model-a".to_string(),
        display_name: "Must Not Open".to_string(),
        auth_status: "configured".to_string(),
        active: true,
    }]));
    app.apply(ChatEvent::CredentialCleared {
        provider_id: "anthropic".to_string(),
    });
    app.apply(ChatEvent::CredentialResult {
        provider_id: "anthropic".to_string(),
        error: None,
    });

    assert_eq!(app.drain_actions(), Vec::new());
    let grid = render(&mut app, true);
    assert!(
        grid.contains("Paste the OpenAI API key"),
        "input was replaced:\n{grid}"
    );
    assert!(
        !grid.contains("Must Not Open"),
        "picker opened behind setup:\n{grid}"
    );
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::SetProviderKey {
            provider_id: "openai".to_string(),
            api_key: "sk-kept".to_string(),
        }]
    );
}

#[test]
fn manual_models_command_supersedes_the_wizard_model_scope() {
    let mut app = app();
    open_wizard(&mut app);
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();

    app.submit("/models");
    assert_eq!(app.drain_actions(), vec![Action::ListModels]);
    app.apply(ChatEvent::Models(vec![
        crate::ModelRow {
            provider_id: "anthropic".to_string(),
            model: "model-a".to_string(),
            display_name: "Model A".to_string(),
            auth_status: "configured".to_string(),
            active: true,
        },
        crate::ModelRow {
            provider_id: "openai".to_string(),
            model: "model-b".to_string(),
            display_name: "Model B".to_string(),
            auth_status: "missing".to_string(),
            active: false,
        },
    ]));
    let grid = render(&mut app, true);
    assert!(grid.contains("Model A"), "first model missing:\n{grid}");
    assert!(
        grid.contains("Model B"),
        "manual result was scoped:\n{grid}"
    );
}

#[test]
fn missing_awaited_provider_and_canceled_login_clear_pending_selection() {
    let mut app = app();
    app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "gone".to_string(),
        model: "model-a".to_string(),
        display_name: "Model A".to_string(),
        auth_status: "missing".to_string(),
        active: false,
    }]));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    let _ = app.drain_actions();
    app.apply(ChatEvent::Providers(provider_rows()));
    assert!(app.pending_selection.is_none());

    let mut cancel_app = App::new(meta());
    cancel_app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "openai-codex".to_string(),
        model: "gpt-5.6-sol".to_string(),
        display_name: "GPT 5.6 sol".to_string(),
        auth_status: "missing".to_string(),
        active: false,
    }]));
    let _ = cancel_app.handle(&key(tuika::KeyCode::Enter));
    let _ = cancel_app.drain_actions();
    cancel_app.apply(ChatEvent::Providers(provider_rows()));
    let _ = cancel_app.handle(&key(tuika::KeyCode::Enter));
    let _ = cancel_app.drain_actions();
    let _ = cancel_app.handle(&key(tuika::KeyCode::Esc));
    let _ = cancel_app.drain_actions();
    assert!(cancel_app.pending_selection.is_none());
    cancel_app.apply(ChatEvent::CredentialResult {
        provider_id: "openai-codex".to_string(),
        error: None,
    });
    assert_eq!(cancel_app.drain_actions(), Vec::new());

    let mut key_app = App::new(meta());
    key_app.apply(ChatEvent::Models(vec![crate::ModelRow {
        provider_id: "openai".to_string(),
        model: "gpt-key".to_string(),
        display_name: "GPT key".to_string(),
        auth_status: "missing".to_string(),
        active: false,
    }]));
    let _ = key_app.handle(&key(tuika::KeyCode::Enter));
    let _ = key_app.drain_actions();
    key_app.apply(ChatEvent::Providers(provider_rows()));
    let _ = key_app.handle(&key(tuika::KeyCode::Enter));
    type_text(&mut key_app, "sk-key");
    let _ = key_app.handle(&key(tuika::KeyCode::Enter));
    let _ = key_app.drain_actions();
    let _ = key_app.handle(&key(tuika::KeyCode::Esc));
    assert!(key_app.pending_selection.is_none());
}

#[test]
fn long_and_wide_provider_rows_preserve_the_active_marker() {
    let mut app = app();
    app.apply(ChatEvent::Providers(vec![crate::ProviderRow {
        provider_id: "provider".to_string(),
        display_name: "模型🙂 provider with a very long display name".repeat(2),
        auth_status: "configured".to_string(),
        label: "a very long stored credential status".repeat(2),
        oauth: false,
        active: true,
    }]));
    let grid = render_at(&mut app, true, 40, 12);
    assert!(grid.contains("active"), "active marker clipped:\n{grid}");
}

#[test]
fn clearing_a_credential_refreshes_the_open_wizard() {
    let mut app = app();
    open_wizard(&mut app);
    // `c` opens the credential step even on a connected provider.
    let _ = app.handle(&key(tuika::KeyCode::Char('c')));
    let grid = render(&mut app, true);
    assert!(grid.contains("Connect Anthropic"), "config step:\n{grid}");

    // Pick "Clear saved key" (second option for a key provider).
    let _ = app.handle(&key(tuika::KeyCode::Down));
    let _ = app.handle(&key(tuika::KeyCode::Enter));
    assert_eq!(
        app.drain_actions(),
        vec![Action::ClearCredential {
            provider_id: "anthropic".to_string(),
        }]
    );
    app.apply(ChatEvent::CredentialCleared {
        provider_id: "anthropic".to_string(),
    });
    assert_eq!(app.drain_actions(), vec![Action::ListProviders]);
    assert!(app.cells.iter().any(|cell| matches!(
        cell,
        crate::Cell::Notice { title, .. } if title == "anthropic: credential cleared"
    )));
}

#[test]
fn setup_wizard_build_is_safe_at_zero_and_tiny_terminal_sizes() {
    for (width, height) in [(0u16, 0u16), (1, 1), (20, 4)] {
        let mut app = app();
        open_wizard(&mut app);
        let _ = render_at(&mut app, true, width, height);
        let _ = app.handle(&key(tuika::KeyCode::Down));
        let _ = app.handle(&key(tuika::KeyCode::Enter));
        let _ = render_at(&mut app, true, width, height);
        let _ = app.handle(&key(tuika::KeyCode::Enter));
        let _ = render_at(&mut app, true, width, height);
    }
}
