use futures_util::StreamExt as _;
#[derive(Clone, Copy, Debug)]

pub(super) enum ChatInvocation {
    Chat,
}

impl ChatInvocation {
    fn print_help(self) {
        match self {
            ChatInvocation::Chat => crate::cli::console::print_chat_help(),
        }
    }

    fn client_name(self) -> &'static str {
        match self {
            ChatInvocation::Chat => "verlet-chat",
        }
    }

    fn private_connection_label(self) -> &'static str {
        match self {
            ChatInvocation::Chat => "local/private",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ChatAttachTarget {
    Unix(std::path::PathBuf),
    WebSocket(String),
}

pub(super) async fn run(
    args: Vec<std::ffi::OsString>,
    invocation: ChatInvocation,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = crate::cli::console::parse_chat_args(args)?;
    if options.help {
        invocation.print_help();
        return Ok(());
    }
    run_chat_console(options, invocation).await
}

async fn run_chat_console(
    options: crate::cli::console::ChatArgs,
    invocation: ChatInvocation,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if let Some(raw_attach) = options.attach.clone() {
        let target = parse_attach_target(&raw_attach)?;
        return run_attached_chat(options, invocation, target).await;
    }

    let launched = crate::cli::console::PrivateAppServer::start(&options).await?;
    let socket_path = launched.socket_path().to_path_buf();
    let result = async {
        #[cfg(unix)]
        {
            let client = crate::adapters::codex_tui::VerletOperatorClient::connect_unix(
                socket_path,
                chat_connect_config(invocation),
            )
            .await?;
            run_chat_client(
                client,
                options.prompt,
                invocation.private_connection_label().to_string(),
            )
            .await
        }
        #[cfg(not(unix))]
        {
            let _ = socket_path;
            Err(crate::cli::usage_error(
                "private chat app-server sockets require a Unix platform",
            ))
        }
    }
    .await;
    launched.shutdown();
    result
}

async fn run_attached_chat(
    options: crate::cli::console::ChatArgs,
    invocation: ChatInvocation,
    target: ChatAttachTarget,
) -> crate::kernel::runtime_host::VerletResult<()> {
    match target {
        ChatAttachTarget::Unix(path) => {
            #[cfg(unix)]
            {
                let label = format!("attach unix://{}", path.display());
                let client = crate::adapters::codex_tui::VerletOperatorClient::connect_unix(
                    path,
                    chat_connect_config(invocation),
                )
                .await?;
                run_chat_client(client, options.prompt, label).await
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                Err(crate::cli::usage_error(
                    "--attach unix://... requires a Unix platform",
                ))
            }
        }
        ChatAttachTarget::WebSocket(url) => {
            let label = format!("attach {url}");
            let client = crate::adapters::codex_tui::VerletOperatorClient::<tokio::net::TcpStream>::connect_websocket(
                &url,
                chat_connect_config(invocation),
            )
            .await?;
            run_chat_client(client, options.prompt, label).await
        }
    }
}

fn chat_connect_config(
    invocation: ChatInvocation,
) -> crate::adapters::codex_tui::CodexTuiConnectConfig {
    crate::adapters::codex_tui::CodexTuiConnectConfig {
        client_name: invocation.client_name().to_string(),
        ..crate::adapters::codex_tui::CodexTuiConnectConfig::default()
    }
}

async fn run_chat_client<S>(
    mut client: crate::adapters::codex_tui::VerletOperatorClient<S>,
    initial_prompt: Option<String>,
    connection_label: String,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let session = bootstrap_chat_client(&mut client, connection_label).await?;
    let thread = client.thread_start(serde_json::json!({})).await?;
    let mut state = ChatTuiState::new(thread, session);
    let run_result = run_chat_tui(&mut client, &mut state, initial_prompt).await;
    let close_result = client.close().await;
    run_result?;
    close_result
}

async fn bootstrap_chat_client<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    connection_label: String,
) -> crate::kernel::runtime_host::VerletResult<ChatSessionInfo>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    client.account_read().await?;
    let config = client.config_read(false).await?;
    let models = client.model_list().await?;
    let model_labels = model_labels(&models);
    if model_labels.is_empty() {
        return Err(crate::cli::usage_error("app-server returned no models"));
    }
    let cwd = config
        .get("config")
        .and_then(|config| config.get("cwd"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?")
        .to_string();
    let provider = config
        .get("config")
        .and_then(|config| config.get("model_provider"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("provider");
    let model = config
        .get("config")
        .and_then(|config| config.get("model"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("model");
    Ok(ChatSessionInfo {
        connection_label,
        cwd,
        model_label: format!("{provider}/{model}"),
        models: model_labels,
    })
}

pub(super) fn parse_attach_target(
    raw: &str,
) -> crate::kernel::runtime_host::VerletResult<ChatAttachTarget> {
    if let Some(path) = raw.strip_prefix("unix://") {
        if path.is_empty() {
            return Err(crate::cli::usage_error(
                "--attach unix:// requires a socket path",
            ));
        }
        return Ok(ChatAttachTarget::Unix(std::path::PathBuf::from(path)));
    }
    if raw.starts_with("ws://") {
        return Ok(ChatAttachTarget::WebSocket(raw.to_string()));
    }
    Err(crate::cli::usage_error(
        "--attach must be unix://path or ws://host:port[/rpc]",
    ))
}

#[derive(Clone, Debug)]
struct ChatSessionInfo {
    connection_label: String,
    cwd: String,
    model_label: String,
    models: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ChatLineRole {
    User,
    Assistant,
    System,
    Error,
    Thinking,
    Lifecycle,
}

impl ChatLineRole {
    fn label(self) -> &'static str {
        match self {
            ChatLineRole::User => "you",
            ChatLineRole::Assistant => "assistant",
            ChatLineRole::System => "system",
            ChatLineRole::Error => "error",
            ChatLineRole::Thinking => "thinking",
            ChatLineRole::Lifecycle => "event",
        }
    }
}

#[derive(Clone, Debug)]
struct ChatLine {
    role: ChatLineRole,
    text: String,
}

#[derive(Debug)]
struct ChatTuiState {
    thread_id: String,
    thread_name: Option<String>,
    connection_label: String,
    cwd: String,
    model_label: String,
    models: Vec<String>,
    input: String,
    cursor: usize,
    history: Vec<ChatLine>,
    active_turn_id: Option<String>,
    active_assistant_index: Option<usize>,
    active_thinking_index: Option<usize>,
    turn_state: String,
    scrollback: usize,
    no_color: bool,
}

impl ChatTuiState {
    fn new(thread: crate::adapters::codex_tui::CodexTuiThread, session: ChatSessionInfo) -> Self {
        let mut state = Self {
            thread_id: thread.id.clone(),
            thread_name: thread_name(&thread.raw),
            connection_label: session.connection_label,
            cwd: thread_cwd(&thread.raw).unwrap_or(session.cwd),
            model_label: session.model_label,
            models: session.models,
            input: String::new(),
            cursor: 0,
            history: Vec::new(),
            active_turn_id: None,
            active_assistant_index: None,
            active_thinking_index: None,
            turn_state: "idle".to_string(),
            scrollback: 0,
            no_color: verlet_runtime_contracts::env_compat::var_os("NO_COLOR").is_some(),
        };
        state.push_lifecycle(format!(
            "started thread {} ({})",
            short_id(&state.thread_id),
            state.connection_label
        ));
        state.push_system("type /help for commands");
        state
    }

    fn push(&mut self, role: ChatLineRole, text: impl Into<String>) {
        self.history.push(ChatLine {
            role,
            text: text.into(),
        });
        self.scrollback = 0;
    }

    fn push_user(&mut self, text: impl Into<String>) {
        self.push(ChatLineRole::User, text);
    }

    fn push_system(&mut self, text: impl Into<String>) {
        self.push(ChatLineRole::System, text);
    }

    fn push_error(&mut self, text: impl Into<String>) {
        self.push(ChatLineRole::Error, text);
    }

    fn push_lifecycle(&mut self, text: impl Into<String>) {
        self.push(ChatLineRole::Lifecycle, text);
    }

    fn begin_assistant(&mut self) {
        let index = self.history.len();
        self.push(ChatLineRole::Assistant, String::new());
        self.active_assistant_index = Some(index);
    }

    fn begin_thinking_if_needed(&mut self) {
        if self.active_thinking_index.is_some() {
            return;
        }
        let index = self.history.len();
        self.push(ChatLineRole::Thinking, String::new());
        self.active_thinking_index = Some(index);
    }

    fn append_assistant_delta(&mut self, delta: &str) {
        if self.active_assistant_index.is_none() {
            self.begin_assistant();
        }
        if let Some(index) = self.active_assistant_index
            && let Some(line) = self.history.get_mut(index)
        {
            line.text.push_str(delta);
        }
    }

    fn append_thinking_delta(&mut self, delta: &str) {
        self.begin_thinking_if_needed();
        if let Some(index) = self.active_thinking_index
            && let Some(line) = self.history.get_mut(index)
        {
            line.text.push_str(delta);
        }
    }

    fn finish_turn(&mut self) {
        self.active_turn_id = None;
        self.active_assistant_index = None;
        self.active_thinking_index = None;
        self.turn_state = "idle".to_string();
    }

    fn switch_thread(&mut self, thread: crate::adapters::codex_tui::CodexTuiThread, reason: &str) {
        self.thread_id = thread.id;
        self.thread_name = thread_name(&thread.raw);
        if let Some(cwd) = thread_cwd(&thread.raw) {
            self.cwd = cwd;
        }
        self.finish_turn();
        self.push_lifecycle(format!("{reason} {}", short_id(&self.thread_id)));
    }

    fn status_line(&self) -> String {
        let name = self
            .thread_name
            .as_deref()
            .filter(|name| !name.is_empty())
            .unwrap_or("unnamed");
        format!(
            "{} | cwd {} | model {} | thread {} {} | {}",
            self.connection_label,
            self.cwd,
            self.model_label,
            short_id(&self.thread_id),
            name,
            self.turn_state
        )
    }

    fn clear_input(&mut self) -> String {
        let text = self.input.trim().to_string();
        self.input.clear();
        self.cursor = 0;
        text
    }

    fn insert_text(&mut self, text: &str) {
        self.input.insert_str(self.cursor, text);
        self.cursor += text.len();
    }

    fn insert_char(&mut self, ch: char) {
        self.input.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
    }

    fn insert_newline(&mut self) {
        self.insert_char('\n');
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let previous = previous_char_boundary(&self.input, self.cursor);
        self.input.replace_range(previous..self.cursor, "");
        self.cursor = previous;
    }

    fn delete_forward(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        let next = next_char_boundary(&self.input, self.cursor);
        self.input.replace_range(self.cursor..next, "");
    }

    fn move_left(&mut self) {
        self.cursor = previous_char_boundary(&self.input, self.cursor);
    }

    fn move_right(&mut self) {
        self.cursor = next_char_boundary(&self.input, self.cursor);
    }

    fn move_home(&mut self) {
        let (start, _) = current_line_bounds(&self.input, self.cursor);
        self.cursor = start;
    }

    fn move_end(&mut self) {
        let (_, end) = current_line_bounds(&self.input, self.cursor);
        self.cursor = end;
    }

    fn move_up(&mut self) {
        let (line_start, _) = current_line_bounds(&self.input, self.cursor);
        if line_start == 0 {
            return;
        }
        let column = char_count(&self.input[line_start..self.cursor]);
        let previous_line_end = line_start - 1;
        let previous_line_start = self.input[..previous_line_end]
            .rfind('\n')
            .map(|index| index + 1)
            .unwrap_or(0);
        self.cursor =
            byte_index_for_column(&self.input, previous_line_start, previous_line_end, column);
    }

    fn move_down(&mut self) {
        let (line_start, line_end) = current_line_bounds(&self.input, self.cursor);
        if line_end >= self.input.len() {
            let _ = line_start;
            return;
        }
        let column = char_count(&self.input[line_start..self.cursor]);
        let next_line_start = line_end + 1;
        let next_line_end = self.input[next_line_start..]
            .find('\n')
            .map(|index| next_line_start + index)
            .unwrap_or(self.input.len());
        self.cursor = byte_index_for_column(&self.input, next_line_start, next_line_end, column);
    }

    fn cursor_line_col(&self) -> (u16, u16) {
        let before = &self.input[..self.cursor];
        let line = before.bytes().filter(|byte| *byte == b'\n').count();
        let column_start = before.rfind('\n').map(|index| index + 1).unwrap_or(0);
        let column = char_count(&before[column_start..]);
        (line as u16, column as u16)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SlashCommand {
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
}

pub(super) fn parse_slash_command(input: &str) -> Result<Option<SlashCommand>, String> {
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
        "" => Err("slash command is empty; type /help".to_string()),
        other => Err(format!("unknown slash command /{other}; type /help")),
    }
}

struct ChatTerminal {
    terminal: ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
}

impl ChatTerminal {
    fn enter() -> crate::kernel::runtime_host::VerletResult<Self> {
        crossterm::terminal::enable_raw_mode()
            .map_err(|err| crate::cli::usage_error(format!("failed to enable raw mode: {err}")))?;
        let mut stdout = std::io::stdout();
        if let Err(err) = crossterm::execute!(
            stdout,
            crossterm::terminal::EnterAlternateScreen,
            crossterm::event::EnableBracketedPaste
        ) {
            let _ = crossterm::terminal::disable_raw_mode();
            return Err(crate::cli::usage_error(format!(
                "failed to enter alternate screen: {err}"
            )));
        }
        let backend = ratatui::backend::CrosstermBackend::new(stdout);
        let mut terminal = ratatui::Terminal::new(backend)
            .map_err(|err| crate::cli::usage_error(format!("failed to open terminal: {err}")))?;
        terminal
            .clear()
            .map_err(|err| crate::cli::usage_error(format!("failed to clear terminal: {err}")))?;
        Ok(Self { terminal })
    }
}

impl Drop for ChatTerminal {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
        let _ = crossterm::execute!(
            self.terminal.backend_mut(),
            crossterm::event::DisableBracketedPaste,
            crossterm::terminal::LeaveAlternateScreen
        );
        let _ = self.terminal.show_cursor();
    }
}

async fn run_chat_tui<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    state: &mut ChatTuiState,
    initial_prompt: Option<String>,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut terminal = ChatTerminal::enter()?;
    let mut events = crossterm::event::EventStream::new();

    if let Some(prompt) = initial_prompt {
        submit_chat_input(client, state, prompt).await?;
    }

    loop {
        draw_chat_tui(&mut terminal.terminal, state)?;
        tokio::select! {
            maybe_event = events.next() => {
                match maybe_event {
                    Some(Ok(crossterm::event::Event::Key(key))) if key.kind == crossterm::event::KeyEventKind::Press => {
                        if handle_chat_key(client, state, key).await? {
                            break;
                        }
                    }
                    Some(Ok(crossterm::event::Event::Paste(text))) => {
                        state.insert_text(&text);
                    }
                    Some(Ok(crossterm::event::Event::Resize(_, _))) => {}
                    Some(Ok(_)) => {}
                    Some(Err(err)) => {
                        return Err(crate::cli::usage_error(format!("terminal event failed: {err}")));
                    }
                    None => break,
                }
            }
            app_event = client.next_event() => {
                handle_chat_app_event(state, app_event?).await?;
            }
        }
    }

    Ok(())
}

fn draw_chat_tui(
    terminal: &mut ratatui::Terminal<ratatui::backend::CrosstermBackend<std::io::Stdout>>,
    state: &ChatTuiState,
) -> crate::kernel::runtime_host::VerletResult<()> {
    terminal
        .draw(|frame| {
            let area = frame.area();
            let composer_lines = state.input.lines().count().max(1) as u16;
            let composer_height = composer_lines.saturating_add(2).clamp(3, 8);
            let chunks = ratatui::layout::Layout::default()
                .direction(ratatui::layout::Direction::Vertical)
                .constraints([
                    ratatui::layout::Constraint::Min(5),
                    ratatui::layout::Constraint::Length(composer_height),
                    ratatui::layout::Constraint::Length(1),
                ])
                .split(area);

            let all_history = history_lines(state);
            let history_height = chunks[0].height.saturating_sub(2) as usize;
            let scrollback = state.scrollback.min(all_history.len().saturating_sub(1));
            let end = all_history.len().saturating_sub(scrollback);
            let start = end.saturating_sub(history_height.max(1));
            let history = all_history[start..end].to_vec();
            let title = if state.no_color {
                "Verlet chat".to_string()
            } else {
                "Verlet chat".to_string()
            };
            let history_block = ratatui::widgets::Paragraph::new(history)
                .block(
                    ratatui::widgets::Block::default()
                        .title(title)
                        .borders(ratatui::widgets::Borders::ALL),
                )
                .wrap(ratatui::widgets::Wrap { trim: false });
            frame.render_widget(history_block, chunks[0]);

            let input = ratatui::widgets::Paragraph::new(state.input.as_str())
                .block(
                    ratatui::widgets::Block::default()
                        .title("message")
                        .borders(ratatui::widgets::Borders::ALL),
                )
                .wrap(ratatui::widgets::Wrap { trim: false });
            frame.render_widget(input, chunks[1]);

            let status = ratatui::text::Line::from(vec![
                ratatui::text::Span::styled("status ", muted_style(state.no_color)),
                ratatui::text::Span::raw(state.status_line()),
            ]);
            frame.render_widget(ratatui::widgets::Paragraph::new(status), chunks[2]);

            let inner_width = chunks[1].width.saturating_sub(2).max(1);
            let inner_height = chunks[1].height.saturating_sub(2).max(1);
            let (cursor_line, cursor_col) = state.cursor_line_col();
            let cursor_x = chunks[1].x + 1 + cursor_col.min(inner_width.saturating_sub(1));
            let cursor_y = chunks[1].y + 1 + cursor_line.min(inner_height.saturating_sub(1));
            frame.set_cursor_position(ratatui::layout::Position::new(cursor_x, cursor_y));
        })
        .map(|_| ())
        .map_err(|err| crate::cli::usage_error(format!("failed to draw terminal: {err}")))
}

async fn handle_chat_key<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    state: &mut ChatTuiState,
    key: crossterm::event::KeyEvent,
) -> crate::kernel::runtime_host::VerletResult<bool>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    if key
        .modifiers
        .contains(crossterm::event::KeyModifiers::CONTROL)
        && key.code == crossterm::event::KeyCode::Char('c')
    {
        if interrupt_active_turn(client, state).await? {
            return Ok(false);
        }
        return Ok(true);
    }

    match key.code {
        crossterm::event::KeyCode::Esc => {
            if interrupt_active_turn(client, state).await? {
                Ok(false)
            } else {
                Ok(true)
            }
        }
        crossterm::event::KeyCode::Enter
            if key.modifiers.intersects(
                crossterm::event::KeyModifiers::SHIFT
                    | crossterm::event::KeyModifiers::ALT
                    | crossterm::event::KeyModifiers::CONTROL,
            ) =>
        {
            state.insert_newline();
            Ok(false)
        }
        crossterm::event::KeyCode::Enter => {
            let input = state.clear_input();
            if input.is_empty() {
                return Ok(false);
            }
            submit_or_handle_slash(client, state, input).await
        }
        crossterm::event::KeyCode::Backspace => {
            state.backspace();
            Ok(false)
        }
        crossterm::event::KeyCode::Delete => {
            state.delete_forward();
            Ok(false)
        }
        crossterm::event::KeyCode::Left => {
            state.move_left();
            Ok(false)
        }
        crossterm::event::KeyCode::Right => {
            state.move_right();
            Ok(false)
        }
        crossterm::event::KeyCode::Up => {
            state.move_up();
            Ok(false)
        }
        crossterm::event::KeyCode::Down => {
            state.move_down();
            Ok(false)
        }
        crossterm::event::KeyCode::Home => {
            state.move_home();
            Ok(false)
        }
        crossterm::event::KeyCode::End => {
            state.move_end();
            Ok(false)
        }
        crossterm::event::KeyCode::PageUp => {
            state.scrollback = state.scrollback.saturating_add(8);
            Ok(false)
        }
        crossterm::event::KeyCode::PageDown => {
            state.scrollback = state.scrollback.saturating_sub(8);
            Ok(false)
        }
        crossterm::event::KeyCode::Char('j')
            if key
                .modifiers
                .contains(crossterm::event::KeyModifiers::CONTROL) =>
        {
            state.insert_newline();
            Ok(false)
        }
        crossterm::event::KeyCode::Char(ch)
            if key.modifiers.is_empty()
                || key.modifiers == crossterm::event::KeyModifiers::SHIFT =>
        {
            state.insert_char(ch);
            Ok(false)
        }
        crossterm::event::KeyCode::Tab => {
            state.insert_text("  ");
            Ok(false)
        }
        _ => Ok(false),
    }
}

async fn submit_or_handle_slash<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    state: &mut ChatTuiState,
    input: String,
) -> crate::kernel::runtime_host::VerletResult<bool>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match parse_slash_command(&input) {
        Ok(Some(command)) => handle_slash_command(client, state, command).await,
        Ok(None) => {
            submit_chat_input(client, state, input).await?;
            Ok(false)
        }
        Err(message) => {
            state.push_error(message);
            Ok(false)
        }
    }
}

async fn handle_slash_command<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    state: &mut ChatTuiState,
    command: SlashCommand,
) -> crate::kernel::runtime_host::VerletResult<bool>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    match command {
        SlashCommand::Help => {
            state.push_system(
                "/help /quit /interrupt /clear /status /new /sessions /resume <thread-id> /rename <name> /fork /compact /models",
            );
        }
        SlashCommand::Quit => return Ok(true),
        SlashCommand::Interrupt => {
            if !interrupt_active_turn(client, state).await? {
                state.push_system("no active turn to interrupt");
            }
        }
        SlashCommand::Clear => {
            state.history.clear();
            state.push_system("transcript cleared");
        }
        SlashCommand::Status => state.push_system(state.status_line()),
        SlashCommand::New => {
            if !ensure_idle(state, "/new") {
                return Ok(false);
            }
            let thread = client.thread_start(serde_json::json!({})).await?;
            state.switch_thread(thread, "started thread");
        }
        SlashCommand::Sessions => {
            let threads = client.thread_list().await?;
            push_sessions(state, &threads);
        }
        SlashCommand::Resume(thread_id) => {
            if !ensure_idle(state, "/resume") {
                return Ok(false);
            }
            let thread = client.thread_resume(&thread_id, false).await?;
            state.switch_thread(thread, "resumed thread");
        }
        SlashCommand::Rename(name) => {
            client.thread_name_set(&state.thread_id, &name).await?;
            state.thread_name = Some(name.clone());
            state.push_lifecycle(format!("renamed thread {}", name));
        }
        SlashCommand::Fork => {
            if !ensure_idle(state, "/fork") {
                return Ok(false);
            }
            let parent = state.thread_id.clone();
            let thread = client.thread_fork(&parent).await?;
            state.switch_thread(thread, &format!("forked from {}", short_id(&parent)));
        }
        SlashCommand::Compact => {
            if !ensure_idle(state, "/compact") {
                return Ok(false);
            }
            client.thread_compact_start(&state.thread_id).await?;
            state.push_lifecycle("compaction requested");
        }
        SlashCommand::Models => {
            for model in state.models.clone() {
                state.push_system(model);
            }
        }
    }
    Ok(false)
}

async fn submit_chat_input<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    state: &mut ChatTuiState,
    input: String,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    state.push_user(input.clone());
    if let Some(turn_id) = state.active_turn_id.clone() {
        client
            .turn_steer_text(&state.thread_id, &turn_id, &input)
            .await?;
        state.turn_state = format!("steered {}", short_id(&turn_id));
        return Ok(());
    }

    let turn = client.turn_start_text(&state.thread_id, &input).await?;
    state.active_turn_id = Some(turn.id.clone());
    state.turn_state = format!("running {}", short_id(&turn.id));
    state.begin_assistant();
    Ok(())
}

async fn interrupt_active_turn<S>(
    client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
    state: &mut ChatTuiState,
) -> crate::kernel::runtime_host::VerletResult<bool>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let Some(turn_id) = state.active_turn_id.clone() else {
        return Ok(false);
    };
    client.turn_interrupt(&state.thread_id, &turn_id).await?;
    state.push_lifecycle(format!("interrupted turn {}", short_id(&turn_id)));
    state.finish_turn();
    Ok(true)
}

async fn handle_chat_app_event(
    state: &mut ChatTuiState,
    event: crate::adapters::codex_tui::CodexTuiEvent,
) -> crate::kernel::runtime_host::VerletResult<()> {
    match event {
        crate::adapters::codex_tui::CodexTuiEvent::Notification(notification) => {
            handle_chat_notification(state, notification);
        }
        crate::adapters::codex_tui::CodexTuiEvent::Error(error) => {
            state.push_error(format!(
                "JSON-RPC error {}: {}",
                error.error.code, error.error.message
            ));
            state.finish_turn();
        }
        crate::adapters::codex_tui::CodexTuiEvent::Request(_)
        | crate::adapters::codex_tui::CodexTuiEvent::Response(_) => {}
    }
    Ok(())
}

fn handle_chat_notification(
    state: &mut ChatTuiState,
    notification: crate::adapters::app_server::connection::JsonRpcNotification,
) {
    let active_matches = state.active_turn_id.as_deref().is_some_and(|turn_id| {
        crate::cli::console::notification_matches_thread_turn(
            &notification,
            &state.thread_id,
            turn_id,
        )
    });
    match notification.method.as_str() {
        "item/agentMessage/delta" if active_matches => {
            if let Some(delta) = crate::cli::debug_rpc::notification_delta(&notification) {
                state.append_assistant_delta(delta);
            }
        }
        "item/agentThinking/delta" if active_matches => {
            if let Some(delta) = crate::cli::debug_rpc::notification_delta(&notification) {
                state.append_thinking_delta(delta);
            }
        }
        "turn/completed"
            if state.active_turn_id.as_deref().is_some_and(|turn_id| {
                crate::cli::console::notification_turn_id(&notification) == Some(turn_id)
            }) =>
        {
            let message = crate::cli::debug_rpc::notification_turn_error_message(&notification);
            if message != "unknown error" {
                state.push_error(message);
            }
            state.finish_turn();
        }
        "thread/status/changed"
            if crate::cli::debug_rpc::notification_thread_id(&notification)
                == Some(&state.thread_id) =>
        {
            state.turn_state = "thread status changed".to_string();
        }
        "thread/started" => {
            if let Some(thread) = notification
                .params
                .as_ref()
                .and_then(|params| params.get("thread"))
            {
                let id = thread
                    .get("id")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown");
                state.push_lifecycle(format!("server started thread {}", short_id(id)));
            }
        }
        "error" => {
            state.push_error(format!(
                "app-server error: {}",
                crate::cli::console::notification_error_message(&notification)
            ));
            state.finish_turn();
        }
        _ => {}
    }
}

fn ensure_idle(state: &mut ChatTuiState, command: &str) -> bool {
    if state.active_turn_id.is_some() {
        state.push_error(format!(
            "{command} is unavailable during an active turn; use /interrupt"
        ));
        return false;
    }
    true
}

fn push_sessions(state: &mut ChatTuiState, threads: &serde_json::Value) {
    let Some(data) = threads.get("data").and_then(serde_json::Value::as_array) else {
        state.push_error("thread/list returned an unexpected shape");
        return;
    };
    if data.is_empty() {
        state.push_system("no sessions");
        return;
    }
    for thread in data.iter().take(12) {
        let id = thread
            .get("id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let name = thread
            .get("name")
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.is_empty())
            .unwrap_or("unnamed");
        let preview = thread
            .get("preview")
            .and_then(serde_json::Value::as_str)
            .filter(|preview| !preview.is_empty())
            .unwrap_or("");
        let status = thread
            .get("status")
            .and_then(|status| status.get("type"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        let marker = if id == state.thread_id { "*" } else { " " };
        let preview = if preview.is_empty() {
            String::new()
        } else {
            format!(" - {}", one_line_preview(preview, 64))
        };
        state.push_system(format!(
            "{marker} {} {name} [{status}]{preview}",
            short_id(id)
        ));
    }
}

fn history_lines(state: &ChatTuiState) -> Vec<ratatui::text::Line<'static>> {
    let mut lines = Vec::new();
    for entry in &state.history {
        let style = role_style(entry.role, state.no_color);
        let label = entry.role.label();
        let mut text_lines = entry.text.lines();
        if let Some(first) = text_lines.next() {
            lines.push(ratatui::text::Line::from(vec![
                ratatui::text::Span::styled(
                    format!("{label:<9}"),
                    style.add_modifier(ratatui::style::Modifier::BOLD),
                ),
                ratatui::text::Span::styled(first.to_string(), style),
            ]));
            for line in text_lines {
                lines.push(ratatui::text::Line::from(vec![
                    ratatui::text::Span::raw("         "),
                    ratatui::text::Span::styled(line.to_string(), style),
                ]));
            }
        } else {
            lines.push(ratatui::text::Line::from(vec![
                ratatui::text::Span::styled(
                    format!("{label:<9}"),
                    style.add_modifier(ratatui::style::Modifier::BOLD),
                ),
            ]));
        }
    }
    lines
}

fn role_style(role: ChatLineRole, no_color: bool) -> ratatui::style::Style {
    if no_color {
        return ratatui::style::Style::default();
    }
    match role {
        ChatLineRole::User => ratatui::style::Style::default().fg(ratatui::style::Color::Cyan),
        ChatLineRole::Assistant => {
            ratatui::style::Style::default().fg(ratatui::style::Color::Green)
        }
        ChatLineRole::System => ratatui::style::Style::default().fg(ratatui::style::Color::Yellow),
        ChatLineRole::Error => ratatui::style::Style::default().fg(ratatui::style::Color::Red),
        ChatLineRole::Thinking => {
            ratatui::style::Style::default().fg(ratatui::style::Color::Magenta)
        }
        ChatLineRole::Lifecycle => {
            ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray)
        }
    }
}

fn muted_style(no_color: bool) -> ratatui::style::Style {
    if no_color {
        ratatui::style::Style::default()
    } else {
        ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray)
    }
}

fn model_labels(models: &serde_json::Value) -> Vec<String> {
    models
        .get("data")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .map(|model| {
            let provider = model
                .get("providerId")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("provider");
            let id = model
                .get("model")
                .or_else(|| model.get("id"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("model");
            let default = model
                .get("isDefault")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            let suffix = if default { " (default)" } else { "" };
            format!("{provider}/{id}{suffix}")
        })
        .collect()
}

fn thread_name(thread: &serde_json::Value) -> Option<String> {
    thread
        .get("name")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn thread_cwd(thread: &serde_json::Value) -> Option<String> {
    thread
        .get("cwd")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
}

fn short_id(id: &str) -> String {
    id.chars().take(8).collect()
}

fn one_line_preview(text: &str, max_chars: usize) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = collapsed.chars();
    let preview = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}...")
    } else {
        preview
    }
}

fn char_count(text: &str) -> usize {
    text.chars().count()
}

fn previous_char_boundary(text: &str, cursor: usize) -> usize {
    if cursor == 0 {
        return 0;
    }
    text[..cursor]
        .char_indices()
        .next_back()
        .map(|(index, _)| index)
        .unwrap_or(0)
}

fn next_char_boundary(text: &str, cursor: usize) -> usize {
    if cursor >= text.len() {
        return text.len();
    }
    text[cursor..]
        .char_indices()
        .nth(1)
        .map(|(offset, _)| cursor + offset)
        .unwrap_or(text.len())
}

fn current_line_bounds(text: &str, cursor: usize) -> (usize, usize) {
    let line_start = text[..cursor]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line_end = text[cursor..]
        .find('\n')
        .map(|index| cursor + index)
        .unwrap_or(text.len());
    (line_start, line_end)
}

fn byte_index_for_column(text: &str, start: usize, end: usize, column: usize) -> usize {
    text[start..end]
        .char_indices()
        .nth(column)
        .map(|(offset, _)| start + offset)
        .unwrap_or(end)
}

#[cfg(test)]
mod tests;
