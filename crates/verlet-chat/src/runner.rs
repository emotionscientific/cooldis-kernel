//! The async event loop: terminal in, actions out, host events folded back.
//!
//! The host owns the [`crate::app::App`] and the channels; this loop owns the
//! terminal. It multiplexes three sources — terminal input, host
//! [`crate::ChatEvent`]s, and an animation tick — and redraws after each. The
//! tick only runs while a turn is in flight (the spinner and elapsed timer are
//! the only animated parts), so an idle chat performs no timer-driven redraws
//! and otherwise wakes for input or host events only.

use futures_util::StreamExt as _;

/// Frame budget for the animation tick, and the clock `Working (12s …)`
/// counts in.
const FRAME_MS: u64 = 60;

/// A terminal failure the host should surface verbatim.
#[derive(Debug)]
pub struct RunnerError(pub String);

impl std::fmt::Display for RunnerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for RunnerError {}

impl From<std::io::Error> for RunnerError {
    fn from(err: std::io::Error) -> Self {
        RunnerError(format!("terminal failure: {err}"))
    }
}

/// Run the chat UI until the user quits or the host closes the event channel.
///
/// The host executes the [`crate::Action`]s it receives and reports outcomes
/// as [`crate::ChatEvent`]s; this function never blocks on the host — a slow
/// RPC just means the working row keeps spinning.
pub async fn run_ui(
    app: &mut crate::app::App,
    no_color: bool,
    actions: tokio::sync::mpsc::UnboundedSender<crate::Action>,
    mut events: tokio::sync::mpsc::UnboundedReceiver<crate::ChatEvent>,
) -> Result<(), RunnerError> {
    let theme = crate::theme::chat_theme(no_color);
    let sheet = tuika::style::StyleSheet::from_theme(&theme);
    let probe = tuika::probe::RectProbe::new();
    app.frame_ms = FRAME_MS;

    let _session = tuika::host::TerminalSession::enter()?;
    crossterm::execute!(std::io::stdout(), crossterm::event::EnableBracketedPaste)?;
    // Restore on every exit path; TerminalSession's own Drop handles the rest.
    let _paste_guard = PasteGuard;

    let mut terminal = ratatui::Terminal::with_options(
        ratatui::backend::CrosstermBackend::new(std::io::stdout()),
        ratatui::TerminalOptions {
            viewport: ratatui::Viewport::Fullscreen,
        },
    )?;
    let mut input = crossterm::event::EventStream::new();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(FRAME_MS));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Deliver actions queued before the loop started (an initial prompt).
    for action in app.drain_actions() {
        let _ = actions.send(action);
    }

    loop {
        terminal.draw(|f| {
            let area = f.area();
            let scene = crate::ui::build(app, area, &theme, &sheet, &probe);
            tuika::host::paint(f.buffer_mut(), area, &theme, &scene, &[]);
            // The composer's rect is only known after layout; the probe
            // reports where it landed, and the real terminal caret goes there.
            if let Some(pos) = app.cursor(probe.rect()) {
                f.set_cursor_position(pos);
            }
        })?;

        tokio::select! {
            maybe_event = input.next() => {
                match maybe_event {
                    Some(Ok(raw)) => {
                        if let Some(event) = tuika::host::translate_event(raw)
                            && app.handle(&event) == crate::app::Flow::Quit
                        {
                            break;
                        }
                        for action in app.drain_actions() {
                            let _ = actions.send(action);
                        }
                    }
                    Some(Err(err)) => {
                        return Err(RunnerError(format!("terminal event failed: {err}")));
                    }
                    None => break,
                }
            }
            host_event = events.recv() => {
                match host_event {
                    Some(event) => {
                        app.apply(event);
                        // Applying a host event can queue follow-up work
                        // (the first-run gate fetches the catalog, a saved
                        // credential re-issues a model selection).
                        for action in app.drain_actions() {
                            let _ = actions.send(action);
                        }
                    }
                    // Host loop ended (server closed or errored); its error
                    // surfaces through the host's own return value.
                    None => break,
                }
            }
            _ = tick.tick(), if app.turn_active() => {
                app.tick();
            }
        }
    }

    let _ = terminal.clear();
    Ok(())
}

struct PasteGuard;

impl Drop for PasteGuard {
    fn drop(&mut self) {
        let _ = crossterm::execute!(std::io::stdout(), crossterm::event::DisableBracketedPaste);
    }
}
