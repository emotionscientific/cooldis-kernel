//! Application state and input routing.
//!
//! Ported from tuika's `codex` example (`examples/codex/app.rs`) and rewired
//! for a real host: instead of driving a scripted agent, [`App::handle`]
//! emits [`crate::Action`]s for the host to execute, and the host feeds results back
//! through [`App::apply`]. Both are synchronous; every UI behavior is testable
//! by calling them in sequence.

pub(crate) mod setup;

/// Whether the event loop should keep running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Flow {
    Continue,
    Quit,
}

/// The slash commands the composer completes: `(label, blurb, args-hint)`.
/// The hint is appended (after a space) when a completion needs an argument
/// typed after it; commands without one run on confirm.
pub const COMMANDS: &[(&str, &str, bool)] = &[
    ("/help", "list the available commands", false),
    ("/status", "show connection, model, and thread", false),
    ("/sessions", "list threads on this server", false),
    ("/resume", "resume a thread by id", true),
    ("/new", "start a new thread", false),
    ("/fork", "fork the current thread", false),
    ("/rename", "name the current thread", true),
    ("/compact", "compact the current thread's context", false),
    ("/models", "pick the model for new turns", false),
    ("/setup", "connect a provider and pick a model", false),
    ("/providers", "connect a provider and pick a model", false),
    ("/interrupt", "interrupt the active turn", false),
    ("/clear", "clear the transcript", false),
    ("/quit", "exit verlet chat", false),
];

/// A parsed slash command. Parsing is kept separate from dispatch so the
/// host's tests can pin the grammar without a UI.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlashCommand {
    Help,
    Quit,
    Interrupt,
    Clear,
    Status,
    New,
    Sessions,
    Resume(String),
    Rename(String),
    Fork,
    Compact,
    Models,
    Setup,
}

/// Parse `input` as a slash command. `Ok(None)` means ordinary prompt text.
pub fn parse_slash_command(input: &str) -> Result<Option<SlashCommand>, String> {
    let trimmed = input.trim();
    if !trimmed.starts_with('/') {
        return Ok(None);
    }
    let raw = trimmed.trim_start_matches('/');
    let (command, rest) = raw
        .split_once(char::is_whitespace)
        .map(|(command, rest)| (command, rest.trim()))
        .unwrap_or((raw, ""));
    match command {
        "help" => Ok(Some(SlashCommand::Help)),
        "quit" | "q" => Ok(Some(SlashCommand::Quit)),
        "interrupt" => Ok(Some(SlashCommand::Interrupt)),
        "clear" => Ok(Some(SlashCommand::Clear)),
        "status" => Ok(Some(SlashCommand::Status)),
        "new" => Ok(Some(SlashCommand::New)),
        "sessions" => Ok(Some(SlashCommand::Sessions)),
        "resume" if rest.is_empty() => Err("/resume requires a thread id; type /help".to_string()),
        "resume" => Ok(Some(SlashCommand::Resume(rest.to_string()))),
        "rename" if rest.is_empty() => Err("/rename requires a name; type /help".to_string()),
        "rename" => Ok(Some(SlashCommand::Rename(rest.to_string()))),
        "fork" => Ok(Some(SlashCommand::Fork)),
        "compact" => Ok(Some(SlashCommand::Compact)),
        "models" => Ok(Some(SlashCommand::Models)),
        "setup" | "providers" => Ok(Some(SlashCommand::Setup)),
        "" => Err("slash command is empty; type /help".to_string()),
        other => Err(format!("unknown slash command /{other}; type /help")),
    }
}

/// The composer's trigger characters: `/` opens command completion when it
/// starts the message.
pub(crate) fn triggers() -> [tuika::components::Trigger; 1] {
    [tuika::components::Trigger::new('/').anchor(tuika::components::TriggerAnchor::BufferStart)]
}

/// The slash-completion popup state.
pub(crate) struct Popup {
    pub state: tuika::components::SelectState,
}

/// The `/models` picker: a modal list over the composer. Opened by
/// [`crate::ChatEvent::Models`], closed by Enter (emits [`crate::Action::SelectModel`])
/// or Esc. While open it consumes every key event.
pub(crate) struct ModelPicker {
    pub rows: Vec<crate::ModelRow>,
    pub state: tuika::components::SelectState,
}

/// The whole application.
pub struct App {
    pub frame: u64,
    /// Milliseconds per frame, set by the runner's tick interval; the
    /// `Working (12s …)` timer counts in frames so tests stay deterministic.
    pub frame_ms: u64,
    pub cells: Vec<crate::cells::Cell>,
    pub composer: tuika::components::TextInputState,
    pub scroll: tuika::components::ScrollState,
    pub(crate) popup: Option<Popup>,
    pub(crate) picker: Option<ModelPicker>,
    pub(crate) setup: Option<crate::app::setup::SetupStep>,
    /// A model choice waiting on credentials: re-issued as
    /// [`crate::Action::SelectModel`] once the provider's credential lands.
    pub(crate) pending_selection: Option<(String, String)>,
    /// First-run gate: no configured providers yet. The footer nags until a
    /// credential lands or a model is selected.
    pub(crate) needs_provider: bool,
    /// First-run tool offer: set with `needs_provider`, spent when the
    /// first real model selection triggers the kit offer (EMO-611).
    pub(crate) kit_offer_pending: bool,
    /// Submitted keys retained until one host reply so late generic errors
    /// can be redacted after the user leaves the input step.
    pub(crate) pending_key_redactions: Vec<(String, String)>,
    pub meta: crate::SessionMeta,
    /// Turn state label for the footer: "idle", "running", "steered", ...
    pub turn_state: String,
    turn_active: bool,
    /// Frame the current turn started on, for the working-row timer.
    turn_started: Option<u64>,
    /// Cumulative tokens reported for the current turn.
    pub total_tokens: u64,
    /// Transcript index of the streaming answer / thinking cell, if any.
    active_answer: Option<usize>,
    active_thinking: Option<usize>,
    /// Previously submitted prompts, newest last (recalled with `Up`).
    history: Vec<String>,
    history_cursor: Option<usize>,
    /// Transcript geometry from the last frame, so paging keys have
    /// dimensions to work against before the next render.
    pub content_h: usize,
    pub viewport_h: usize,
    /// Host work queued by `handle`; the runner drains it after every event.
    actions: Vec<crate::Action>,
    quit_requested: bool,
}

impl App {
    pub fn new(meta: crate::SessionMeta) -> Self {
        let banner = banner_cell(&meta);
        Self {
            frame: 0,
            frame_ms: 60,
            cells: vec![banner],
            composer: tuika::components::TextInputState::new(),
            scroll: tuika::components::ScrollState::new(),
            popup: None,
            picker: None,
            setup: None,
            pending_selection: None,
            needs_provider: false,
            kit_offer_pending: false,
            pending_key_redactions: Vec::new(),
            meta,
            turn_state: "idle".to_string(),
            turn_active: false,
            turn_started: None,
            total_tokens: 0,
            active_answer: None,
            active_thinking: None,
            history: Vec::new(),
            history_cursor: None,
            content_h: 0,
            viewport_h: 0,
            actions: Vec::new(),
            quit_requested: false,
        }
    }

    /// Advance the animation clock one frame.
    pub fn tick(&mut self) {
        self.frame = self.frame.wrapping_add(1);
    }

    /// Whether a turn is in flight (drives the working row and idle gating).
    pub fn turn_active(&self) -> bool {
        self.turn_active
    }

    /// Seconds the current turn has been running, in frame time.
    pub fn elapsed_secs(&self) -> u64 {
        let started = self.turn_started.unwrap_or(self.frame);
        (self.frame.saturating_sub(started)) * self.frame_ms / 1000
    }

    /// Host work queued since the last drain.
    pub fn drain_actions(&mut self) -> Vec<crate::Action> {
        std::mem::take(&mut self.actions)
    }

    /// Where the terminal cursor belongs, given the composer's painted rect.
    /// While a modal window owns the input surface the composer is dimmed
    /// underneath it, so no terminal caret is shown.
    pub fn cursor(&self, composer_rect: ratatui::layout::Rect) -> Option<(u16, u16)> {
        if composer_rect.width == 0 || self.setup_visible() || self.picker.is_some() {
            return None;
        }
        Some(self.composer.cursor_screen(composer_rect))
    }

    /// The rows the completion popup offers, filtered by the typed prefix.
    pub fn popup_items(&self) -> Vec<(String, String)> {
        if self.popup.is_none() {
            return Vec::new();
        }
        let token = self.composer.active_token(&triggers());
        let filter = token.map(|t| t.text).unwrap_or_default();
        COMMANDS
            .iter()
            .filter(|(label, _, _)| label.starts_with(filter.trim()))
            .map(|(label, blurb, _)| ((*label).to_string(), (*blurb).to_string()))
            .collect()
    }

    /// Route one translated event. The order here *is* the focus model: the
    /// transcript's scroll keys first, then the picker, then the composer.
    pub fn handle(&mut self, event: &tuika::event::Event) -> Flow {
        let flow = self.route(event);
        if self.quit_requested {
            return Flow::Quit;
        }
        flow
    }

    fn route(&mut self, event: &tuika::event::Event) -> Flow {
        // The setup wizard and the model picker own the whole input surface
        // while open. Keep them ahead of transcript scrolling, paste
        // handling, global controls, and slash completion so none of those
        // can leak through the modal.
        if self.setup_visible() {
            self.handle_setup(event);
            return Flow::Continue;
        }
        if self.picker.is_some() {
            self.handle_picker(event);
            return Flow::Continue;
        }
        if self.scrolling(event) {
            let _ = self.scroll.handle(event, self.content_h, self.viewport_h);
            return Flow::Continue;
        }
        if matches!(event, tuika::event::Event::Paste(_)) {
            let _ = self.composer.handle(event);
            self.sync_popup();
            return Flow::Continue;
        }
        let tuika::event::Event::Key(key) = event else {
            return Flow::Continue;
        };

        if key.ctrl && key.code == tuika::event::KeyCode::Char('c') {
            return self.interrupt_or_quit();
        }
        if key.ctrl && key.code == tuika::event::KeyCode::Char('d') && self.composer.is_empty() {
            return Flow::Quit;
        }
        if self.popup.is_some() && self.handle_popup(event, *key) {
            return Flow::Continue;
        }
        if key.plain() && key.code == tuika::event::KeyCode::Esc {
            if self.turn_active {
                self.actions.push(crate::Action::Interrupt);
            }
            return Flow::Continue;
        }
        // `Up` on an empty composer recalls the previous prompt.
        if key.plain() && key.code == tuika::event::KeyCode::Up && self.composer.is_empty() {
            self.recall();
            return Flow::Continue;
        }

        match self.composer.handle(event) {
            tuika::event::InputOutcome::Submitted => {
                let text = self.composer.text().trim().to_string();
                self.composer.clear();
                self.popup = None;
                self.history_cursor = None;
                if !text.is_empty() {
                    self.submit(&text);
                }
            }
            _ => self.sync_popup(),
        }
        Flow::Continue
    }

    /// Events that belong to the transcript rather than to any focused surface.
    fn scrolling(&self, event: &tuika::event::Event) -> bool {
        match event {
            tuika::event::Event::Mouse(m) => matches!(
                m.kind,
                tuika::event::MouseKind::ScrollUp | tuika::event::MouseKind::ScrollDown
            ),
            tuika::event::Event::Key(k) => {
                k.plain()
                    && matches!(
                        k.code,
                        tuika::event::KeyCode::PageUp | tuika::event::KeyCode::PageDown
                    )
            }
            _ => false,
        }
    }

    fn interrupt_or_quit(&mut self) -> Flow {
        if self.turn_active {
            self.actions.push(crate::Action::Interrupt);
            return Flow::Continue;
        }
        Flow::Quit
    }

    /// Returns true when the popup consumed the event.
    fn handle_popup(&mut self, event: &tuika::event::Event, key: tuika::event::Key) -> bool {
        let items = self.popup_items();
        let Some(popup) = self.popup.as_mut() else {
            return false;
        };
        // Tab completes the highlighted row in place, without running it.
        if key.plain() && key.code == tuika::event::KeyCode::Tab {
            let completion = popup
                .state
                .selected()
                .and_then(|selected| items.get(selected))
                .map(|(label, _)| label.clone());
            if let Some(label) = completion {
                self.complete(&completion_text(&label));
            }
            return true;
        }
        match popup.state.handle(event, items.len()) {
            tuika::event::InputOutcome::Submitted => {
                let label = popup
                    .state
                    .selected()
                    .and_then(|index| items.get(index))
                    .map(|(l, _)| l.clone());
                if let Some(label) = label {
                    self.popup = None;
                    if needs_argument(&label) {
                        // The command wants an argument: complete it into the
                        // composer and leave the user typing.
                        self.complete(&completion_text(&label));
                    } else {
                        self.composer.clear();
                        self.submit(&label);
                    }
                }
                true
            }
            tuika::event::InputOutcome::Cancelled => {
                self.popup = None;
                true
            }
            outcome => outcome.consumed(),
        }
    }

    /// The picker is modal: every key event lands here while it is open.
    /// Enter selects (already-active rows just close), Esc dismisses,
    /// arrows move; anything else is swallowed so the composer underneath
    /// stays untouched.
    fn handle_picker(&mut self, event: &tuika::event::Event) {
        let Some(picker) = self.picker.as_mut() else {
            return;
        };
        match picker.state.handle(event, picker.rows.len()) {
            tuika::event::InputOutcome::Submitted => {
                let row = picker
                    .state
                    .selected()
                    .and_then(|index| picker.rows.get(index))
                    .cloned();
                self.picker = None;
                let Some(row) = row else {
                    return;
                };
                if row.active {
                    self.notice(
                        crate::cells::Tone::Info,
                        format!("{}/{} is already active", row.provider_id, row.model),
                        Vec::new(),
                    );
                    return;
                }
                if row.auth_status == "missing" {
                    // Selecting the row would only earn a server rejection;
                    // route through the wizard's credential step instead and
                    // re-issue this choice once a credential lands.
                    self.setup_for_model(row.provider_id, row.model);
                    return;
                }
                self.actions.push(crate::Action::SelectModel {
                    provider_id: row.provider_id,
                    model: row.model,
                });
            }
            tuika::event::InputOutcome::Cancelled => {
                self.picker = None;
            }
            _ => {}
        }
    }

    /// Open, refilter, or close the completion popup after the composer
    /// changed. The whole rule is "is the cursor inside a `/` token?" — tuika
    /// answers that from the declared triggers.
    pub fn sync_popup(&mut self) {
        let active = self.composer.active_token(&triggers());
        match (&self.popup, active) {
            (_, None) => self.popup = None,
            (Some(popup), Some(_)) => {
                // The filter shrank the list under the caret; pull it back in.
                let mut state = popup.state;
                state.clamp(self.popup_items().len());
                self.popup = Some(Popup { state });
            }
            (None, Some(_)) => {
                self.popup = Some(Popup {
                    state: tuika::components::SelectState::new(),
                });
            }
        }
    }

    /// Replace the token under the cursor with `replacement`.
    fn complete(&mut self, replacement: &str) {
        let Some(token) = self.composer.active_token(&triggers()) else {
            return;
        };
        self.composer.replace_token(&token, replacement);
        self.sync_popup();
    }

    /// The composer's `/` token as a styled range, colored in the input.
    pub fn composer_highlights(
        &self,
        theme: &tuika::style::Theme,
    ) -> Vec<tuika::components::TextSpan> {
        self.composer
            .tokens(&triggers())
            .iter()
            .map(|token: &tuika::components::Token| {
                token.span(
                    ratatui::style::Style::default()
                        .fg(theme.accent_alt)
                        .add_modifier(ratatui::style::Modifier::BOLD),
                )
            })
            .collect()
    }

    fn recall(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next = match self.history_cursor {
            None => self.history.len() - 1,
            Some(i) => i.saturating_sub(1),
        };
        self.history_cursor = Some(next);
        let text = self.history[next].clone();
        self.composer.set_text(&text);
    }

    /// Accept a submitted line: a slash command, or a prompt for the agent.
    pub fn submit(&mut self, text: &str) {
        self.history.push(text.to_string());
        match parse_slash_command(text) {
            Ok(Some(command)) => self.slash(command),
            Ok(None) => {
                self.cells.push(crate::cells::Cell::User(text.to_string()));
                self.actions.push(crate::Action::Submit(text.to_string()));
            }
            Err(message) => self.notice(crate::cells::Tone::Error, message, Vec::new()),
        }
        self.follow();
    }

    fn slash(&mut self, command: SlashCommand) {
        match command {
            SlashCommand::Help => {
                let body = COMMANDS
                    .iter()
                    .map(|(label, blurb, needs_arg)| {
                        let hint = if *needs_arg { " <…>" } else { "" };
                        format!("{label}{hint} — {blurb}")
                    })
                    .collect();
                self.notice(crate::cells::Tone::Info, "Commands".to_string(), body);
            }
            SlashCommand::Quit => self.quit_requested = true,
            SlashCommand::Interrupt => {
                if self.turn_active {
                    self.actions.push(crate::Action::Interrupt);
                } else {
                    self.notice(
                        crate::cells::Tone::Info,
                        "no active turn to interrupt".to_string(),
                        vec![],
                    );
                }
            }
            SlashCommand::Clear => {
                self.cells.clear();
                self.active_answer = None;
                self.active_thinking = None;
                self.notice(
                    crate::cells::Tone::Info,
                    "transcript cleared".to_string(),
                    Vec::new(),
                );
            }
            SlashCommand::Status => {
                let mut rows = self.meta_rows();
                rows.push(("state".into(), self.turn_state.clone()));
                if self.total_tokens > 0 {
                    rows.push(("tokens".into(), self.total_tokens.to_string()));
                }
                self.cells.push(crate::cells::Cell::Config {
                    title: "Session".into(),
                    rows,
                });
            }
            SlashCommand::New => {
                if self.ensure_idle("/new") {
                    self.actions.push(crate::Action::NewThread);
                }
            }
            SlashCommand::Sessions => self.actions.push(crate::Action::ListSessions),
            SlashCommand::Resume(id) => {
                if self.ensure_idle("/resume") {
                    self.actions.push(crate::Action::Resume(id));
                }
            }
            SlashCommand::Rename(name) => self.actions.push(crate::Action::Rename(name)),
            SlashCommand::Fork => {
                if self.ensure_idle("/fork") {
                    self.actions.push(crate::Action::Fork);
                }
            }
            SlashCommand::Compact => {
                if self.ensure_idle("/compact") {
                    self.actions.push(crate::Action::Compact);
                }
            }
            SlashCommand::Models => {
                if matches!(
                    self.setup,
                    Some(
                        crate::app::setup::SetupStep::AwaitModels { .. }
                            | crate::app::setup::SetupStep::AwaitCatalog { .. }
                    )
                ) {
                    self.setup = None;
                    self.pending_selection = None;
                }
                self.actions.push(crate::Action::ListModels);
            }
            SlashCommand::Setup => self.open_setup_home(),
        }
    }

    fn ensure_idle(&mut self, command: &str) -> bool {
        if self.turn_active {
            self.notice(
                crate::cells::Tone::Error,
                format!("{command} is unavailable during an active turn; use /interrupt"),
                Vec::new(),
            );
            return false;
        }
        true
    }

    /// Fold one host event into the transcript and status state.
    pub fn apply(&mut self, event: crate::ChatEvent) {
        match event {
            crate::ChatEvent::AnswerDelta(delta) => {
                if delta.is_empty() {
                    return;
                }
                let index = match self.active_answer {
                    Some(index) => index,
                    None => {
                        self.cells.push(crate::cells::Cell::Answer(Box::new(
                            tuika::components::MarkdownState::new(),
                        )));
                        let index = self.cells.len() - 1;
                        self.active_answer = Some(index);
                        index
                    }
                };
                if let Some(crate::cells::Cell::Answer(state)) = self.cells.get_mut(index) {
                    state.push_str(&delta);
                }
                self.follow();
            }
            crate::ChatEvent::ThinkingDelta(delta) => {
                if delta.is_empty() {
                    return;
                }
                let index = match self.active_thinking {
                    Some(index) => index,
                    None => {
                        self.cells.push(crate::cells::Cell::Reasoning {
                            body: String::new(),
                        });
                        let index = self.cells.len() - 1;
                        self.active_thinking = Some(index);
                        index
                    }
                };
                if let Some(crate::cells::Cell::Reasoning { body }) = self.cells.get_mut(index) {
                    body.push_str(&delta);
                }
                self.follow();
            }
            crate::ChatEvent::ToolStarted { id, title } => {
                // A tool call interleaves with the streamed answer: close the
                // streaming cells so later deltas open fresh ones below.
                self.active_answer = None;
                self.active_thinking = None;
                self.cells.push(crate::cells::Cell::Exec {
                    id,
                    title,
                    output: Vec::new(),
                    status: crate::cells::ExecStatus::Running,
                });
                self.follow();
            }
            crate::ChatEvent::ToolOutputDelta { id, delta } => {
                if delta.is_empty() {
                    return;
                }
                if let Some(crate::cells::Cell::Exec { output, .. }) = self.find_exec(&id) {
                    append_output_lines(output, &delta);
                }
                self.follow();
            }
            crate::ChatEvent::ToolCompleted {
                id,
                success,
                output,
            } => {
                if let Some(crate::cells::Cell::Exec {
                    output: rows,
                    status,
                    ..
                }) = self.find_exec(&id)
                {
                    *status = if success {
                        crate::cells::ExecStatus::Ok
                    } else {
                        crate::cells::ExecStatus::Failed
                    };
                    if !output.is_empty() {
                        rows.clear();
                        append_output_lines(rows, &output);
                    }
                }
                self.follow();
            }
            crate::ChatEvent::TurnStarted { turn_id } => {
                self.turn_active = true;
                self.turn_started = Some(self.frame);
                self.turn_state = format!("running {}", crate::cells::short_id(&turn_id));
                self.total_tokens = 0;
                self.active_answer = None;
                self.active_thinking = None;
            }
            crate::ChatEvent::TurnSteered => {
                self.turn_state = "steered".to_string();
            }
            crate::ChatEvent::TurnCompleted { error } => {
                if let Some(message) = error {
                    self.notice(crate::cells::Tone::Error, message, Vec::new());
                }
                self.finish_turn();
            }
            crate::ChatEvent::Usage { total_tokens } => {
                self.total_tokens = self.total_tokens.saturating_add(total_tokens);
            }
            crate::ChatEvent::ThreadSwitched {
                thread_id,
                name,
                cwd,
                reason,
            } => {
                self.meta.thread_id = thread_id;
                self.meta.thread_name = name;
                if let Some(cwd) = cwd {
                    self.meta.cwd = cwd;
                }
                self.finish_turn();
                self.total_tokens = 0;
                self.notice(
                    crate::cells::Tone::Info,
                    format!("{reason} {}", crate::cells::short_id(&self.meta.thread_id)),
                    Vec::new(),
                );
            }
            crate::ChatEvent::ThreadRenamed { name } => {
                self.meta.thread_name = Some(name.clone());
                self.notice(
                    crate::cells::Tone::Info,
                    format!("renamed thread {name}"),
                    Vec::new(),
                );
            }
            crate::ChatEvent::Sessions(rows) => {
                self.cells.push(crate::cells::Cell::Sessions(rows));
                self.follow();
            }
            crate::ChatEvent::Models(rows) => {
                let rows = match self.setup.take() {
                    Some(crate::app::setup::SetupStep::AwaitModels { provider_id }) => {
                        let scoped: Vec<crate::ModelRow> = rows
                            .into_iter()
                            .filter(|row| row.provider_id == provider_id)
                            .collect();
                        if scoped.is_empty() {
                            self.notice(
                                crate::cells::Tone::Error,
                                format!("no models available for {provider_id}"),
                                Vec::new(),
                            );
                            return;
                        }
                        scoped
                    }
                    None => rows,
                    step => {
                        self.setup = step;
                        return;
                    }
                };
                // A model result supersedes completion and any older model
                // result. Clear first so an empty refresh cannot leave stale
                // selectable rows behind.
                self.popup = None;
                self.picker = None;
                if rows.is_empty() {
                    self.notice(
                        crate::cells::Tone::Error,
                        "no models available".to_string(),
                        Vec::new(),
                    );
                    return;
                }
                let mut state = tuika::components::SelectState::new();
                state.select(rows.iter().position(|row| row.active).or(Some(0)));
                self.picker = Some(ModelPicker { rows, state });
            }
            crate::ChatEvent::ModelSelected { provider_id, model } => {
                self.pending_selection = None;
                if provider_id != "local" {
                    self.needs_provider = false;
                    if self.kit_offer_pending {
                        // First-run: provider and model are set; offer the
                        // recommended tools once.
                        self.kit_offer_pending = false;
                        self.actions.push(crate::Action::FetchKitStatus {
                            intent: crate::KitStatusIntent::OfferIfMissing,
                        });
                    }
                }
                self.meta.model_label = format!("{provider_id}/{model}");
                let body = if self.turn_active {
                    vec!["applies to turns after the current one".to_string()]
                } else {
                    Vec::new()
                };
                self.notice(
                    crate::cells::Tone::Info,
                    format!("model set to {provider_id}/{model}"),
                    body,
                );
            }
            crate::ChatEvent::ProviderCatalog { providers } => {
                self.apply_provider_catalog(providers)
            }
            crate::ChatEvent::CustomProviderResult { provider_id, error } => {
                self.apply_custom_provider_result(provider_id, error);
            }
            crate::ChatEvent::NoConfiguredProviders => self.apply_no_configured_providers(),
            crate::ChatEvent::LoginDeviceCode {
                verification_uri,
                user_code,
            } => self.apply_device_code(verification_uri, user_code),
            crate::ChatEvent::CredentialResult { provider_id, error } => {
                self.apply_credential_result(provider_id, error);
            }
            crate::ChatEvent::CredentialCleared { provider_id } => {
                self.apply_credential_cleared(provider_id);
            }
            crate::ChatEvent::KitStatus {
                intent,
                installed,
                recommended,
            } => self.apply_kit_status(intent, installed, recommended),
            crate::ChatEvent::KitInstallResult {
                name,
                error,
                receipt,
            } => self.apply_kit_install_result(name, error, receipt),
            crate::ChatEvent::ThreadStatus(status) => {
                if !self.turn_active {
                    self.turn_state = status;
                }
            }
            crate::ChatEvent::Info { title, body } => {
                self.notice(crate::cells::Tone::Info, title, body)
            }
            crate::ChatEvent::Error { title, body } => self.apply_error(title, body),
            crate::ChatEvent::ResyncStarted => {
                self.notice(
                    crate::cells::Tone::Warn,
                    "stream lagged; transcript tail may be incomplete".to_string(),
                    vec!["the server is rebuilding the turn".to_string()],
                );
            }
        }
    }

    fn finish_turn(&mut self) {
        self.turn_active = false;
        self.turn_started = None;
        self.turn_state = "idle".to_string();
        self.active_answer = None;
        self.active_thinking = None;
    }

    /// The most recent exec cell with this tool-call id.
    fn find_exec(&mut self, id: &str) -> Option<&mut crate::cells::Cell> {
        self.cells.iter_mut().rev().find(
            |cell| matches!(cell, crate::cells::Cell::Exec { id: cell_id, .. } if cell_id == id),
        )
    }

    fn notice(&mut self, tone: crate::cells::Tone, title: String, body: Vec<String>) {
        self.cells
            .push(crate::cells::Cell::Notice { tone, title, body });
        self.follow();
    }

    fn meta_rows(&self) -> Vec<(String, String)> {
        vec![
            ("connection".into(), self.meta.connection_label.clone()),
            ("directory".into(), self.meta.cwd.clone()),
            ("model".into(), self.meta.model_label.clone()),
            (
                "thread".into(),
                format!(
                    "{} {}",
                    crate::cells::short_id(&self.meta.thread_id),
                    self.meta.thread_name.as_deref().unwrap_or("unnamed")
                ),
            ),
        ]
    }

    /// Reconcile new transcript content without overriding manual scrollback.
    fn follow(&mut self) {
        self.scroll.clamp(self.content_h, self.viewport_h);
    }

    /// Rebuild the transcript as one item per cell, laid out to `width`.
    ///
    /// Rebuilding every frame is the model working as intended: only the
    /// streaming answer holds a cache, and ratatui diffs the resulting cells.
    pub fn transcript(
        &mut self,
        width: u16,
        theme: &tuika::style::Theme,
        sheet: &tuika::style::StyleSheet,
    ) -> Vec<tuika::view::Element> {
        self.cells
            .iter_mut()
            .map(|cell| cell.view(width, theme, sheet))
            .collect()
    }
}

/// The completion text for a command label: trailing space when it takes an
/// argument so the user keeps typing, bare otherwise.
fn completion_text(label: &str) -> String {
    if needs_argument(label) {
        format!("{label} ")
    } else {
        label.to_string()
    }
}

fn needs_argument(label: &str) -> bool {
    COMMANDS
        .iter()
        .any(|(command, _, needs_arg)| *command == label && *needs_arg)
}

/// Split streamed output into rows, appending to a possibly part-filled last
/// row (deltas do not arrive line-aligned).
fn append_output_lines(rows: &mut Vec<String>, delta: &str) {
    let mut lines = delta.split('\n');
    if let Some(first) = lines.next() {
        match rows.last_mut() {
            Some(last) => last.push_str(first),
            None => rows.push(first.to_string()),
        }
    }
    for line in lines {
        rows.push(line.to_string());
    }
}

fn banner_cell(meta: &crate::SessionMeta) -> crate::cells::Cell {
    crate::cells::Cell::Banner {
        version: meta.version.clone(),
        rows: vec![
            ("connection".into(), meta.connection_label.clone()),
            ("directory".into(), meta.cwd.clone()),
            ("model".into(), meta.model_label.clone()),
            ("thread".into(), crate::cells::short_id(&meta.thread_id)),
        ],
        tips: [
            ("/help", "list the available commands"),
            ("/status", "show connection, model, and thread"),
            ("/sessions", "list threads on this server"),
            ("/models", "pick the model for new turns"),
        ]
        .iter()
        .map(|(c, b)| ((*c).to_string(), (*b).to_string()))
        .collect(),
    }
}
