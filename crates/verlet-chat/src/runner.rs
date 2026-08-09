//! The async event loop: terminal in, actions out, host events folded back.
//!
//! The host owns the [`App`] and the channels; this loop owns the terminal.
//! It multiplexes three sources — terminal input, host [`ChatEvent`]s, and an
//! animation tick — and redraws after each. The tick only runs while a turn
//! is in flight (the spinner and elapsed timer are the only animated parts),
//! so an idle chat performs no timer-driven redraws and otherwise wakes for
//! input or host events only.

use std::io;

use futures_util::StreamExt as _;
use ratatui::backend::CrosstermBackend;
use ratatui::{Terminal, TerminalOptions, Viewport};

use tuika::probe::RectProbe;
use tuika::{StyleSheet, TerminalSession, paint, translate_event};

use crate::app::{App, Flow};
use crate::{Action, ChatEvent, theme};

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

impl From<io::Error> for RunnerError {
    fn from(err: io::Error) -> Self {
        RunnerError(format!("terminal failure: {err}"))
    }
}

/// Run the chat UI until the user quits or the host closes the event channel.
///
/// The host executes the [`Action`]s it receives and reports outcomes as
/// [`ChatEvent`]s; this function never blocks on the host — a slow RPC just
/// means the working row keeps spinning.
pub async fn run_ui(
    app: &mut App,
    no_color: bool,
    actions: tokio::sync::mpsc::UnboundedSender<Action>,
    mut events: tokio::sync::mpsc::UnboundedReceiver<ChatEvent>,
) -> Result<(), RunnerError> {
    let theme = theme::chat_theme(no_color);
    let sheet = StyleSheet::from_theme(&theme);
    let probe = RectProbe::new();
    app.frame_ms = FRAME_MS;

    let _session = TerminalSession::enter()?;
    crossterm::execute!(io::stdout(), crossterm::event::EnableBracketedPaste)?;
    // Restore on every exit path; TerminalSession's own Drop handles the rest.
    let _paste_guard = PasteGuard;

    let mut terminal = Terminal::with_options(
        CrosstermBackend::new(io::stdout()),
        TerminalOptions {
            viewport: Viewport::Fullscreen,
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
            let root = crate::ui::build(app, area, &theme, &sheet, &probe);
            paint(f.buffer_mut(), area, &theme, root.as_ref(), &[]);
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
                        if let Some(event) = translate_event(raw)
                            && app.handle(&event) == Flow::Quit
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
                    Some(event) => app.apply(event),
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
        let _ = crossterm::execute!(io::stdout(), crossterm::event::DisableBracketedPaste);
    }
}
