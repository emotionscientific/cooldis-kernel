//! `verlet chat` — the host side of the terminal chat UI.
//!
//! The UI itself lives in the `verlet-chat` crate and is presentation only.
//! This module owns everything with authority: the app-server connection, the
//! turn lifecycle, and the translation between JSON-RPC notifications and the
//! UI's typed [`verlet_chat::ChatEvent`]s. The split preserves the chat
//! boundary invariant (see `docs/chat.md`): chat is a pure app-server client,
//! and the UI is a pure function of what this driver feeds it.

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

    let meta = verlet_chat::SessionMeta {
        connection_label: session.connection_label.clone(),
        cwd: thread_cwd(&thread.raw).unwrap_or_else(|| session.cwd.clone()),
        model_label: session.model_label.clone(),
        thread_id: thread.id.clone(),
        thread_name: thread_name(&thread.raw),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };
    let mut app = verlet_chat::App::new(meta);
    if let Some(prompt) = initial_prompt {
        app.submit(&prompt);
    }

    let (action_tx, action_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let no_color = verlet_runtime_contracts::env_compat::var_os("NO_COLOR").is_some();

    let mut driver = ChatDriver {
        thread_id: thread.id,
        active_turn_id: None,
        models: session.models,
    };

    let run_result = {
        let ui = verlet_chat::run_ui(&mut app, no_color, action_tx, event_rx);
        let driven = driver.drive(&mut client, action_rx, event_tx);
        tokio::pin!(ui);
        tokio::pin!(driven);
        tokio::select! {
            ui_result = &mut ui => {
                ui_result.map_err(|err| crate::cli::usage_error(err.to_string()))
            }
            // The driver only returns early on a transport failure; the UI
            // future is dropped here and its Drop restores the terminal.
            driver_result = &mut driven => driver_result,
        }
    };
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

/// The host loop's authoritative state: which thread the UI is on and whether
/// a turn is in flight. The UI mirrors this through events; it never owns it.
struct ChatDriver {
    thread_id: String,
    active_turn_id: Option<String>,
    models: Vec<String>,
}

impl ChatDriver {
    /// Run until the UI hangs up (returns `Ok`) or the transport fails
    /// (returns the error). RPC failures on individual commands are reported
    /// into the transcript and are not fatal.
    async fn drive<S>(
        &mut self,
        client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
        mut actions: tokio::sync::mpsc::UnboundedReceiver<verlet_chat::Action>,
        events: tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
    ) -> crate::kernel::runtime_host::VerletResult<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        loop {
            tokio::select! {
                action = actions.recv() => {
                    let Some(action) = action else {
                        return Ok(());
                    };
                    if let Err(err) = self.execute(client, &events, action).await {
                        let _ = events.send(verlet_chat::ChatEvent::Error {
                            title: err.to_string(),
                            body: Vec::new(),
                        });
                    }
                }
                app_event = client.next_event() => {
                    self.project(app_event?, &events);
                }
            }
        }
    }

    async fn execute<S>(
        &mut self,
        client: &mut crate::adapters::codex_tui::VerletOperatorClient<S>,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
        action: verlet_chat::Action,
    ) -> crate::kernel::runtime_host::VerletResult<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        match action {
            verlet_chat::Action::Submit(text) => {
                if let Some(turn_id) = self.active_turn_id.clone() {
                    client
                        .turn_steer_text(&self.thread_id, &turn_id, &text)
                        .await?;
                    let _ = events.send(verlet_chat::ChatEvent::TurnSteered);
                } else {
                    let turn = client.turn_start_text(&self.thread_id, &text).await?;
                    self.active_turn_id = Some(turn.id.clone());
                    let _ = events.send(verlet_chat::ChatEvent::TurnStarted { turn_id: turn.id });
                }
            }
            verlet_chat::Action::Interrupt => {
                if let Some(turn_id) = self.active_turn_id.clone() {
                    client.turn_interrupt(&self.thread_id, &turn_id).await?;
                    self.active_turn_id = None;
                    let _ = events.send(verlet_chat::ChatEvent::Info {
                        title: format!("interrupted turn {}", short_id(&turn_id)),
                        body: Vec::new(),
                    });
                    let _ = events.send(verlet_chat::ChatEvent::TurnCompleted { error: None });
                }
            }
            verlet_chat::Action::NewThread => {
                let thread = client.thread_start(serde_json::json!({})).await?;
                self.switch_thread(events, thread, "started thread");
            }
            verlet_chat::Action::ListSessions => {
                let threads = client.thread_list().await?;
                let _ = events.send(verlet_chat::ChatEvent::Sessions(session_rows(
                    &threads,
                    &self.thread_id,
                )));
            }
            verlet_chat::Action::Resume(thread_id) => {
                let thread = client.thread_resume(&thread_id, false).await?;
                self.switch_thread(events, thread, "resumed thread");
            }
            verlet_chat::Action::Rename(name) => {
                client.thread_name_set(&self.thread_id, &name).await?;
                let _ = events.send(verlet_chat::ChatEvent::ThreadRenamed { name });
            }
            verlet_chat::Action::Fork => {
                let parent = self.thread_id.clone();
                let thread = client.thread_fork(&parent).await?;
                self.switch_thread(
                    events,
                    thread,
                    &format!("forked from {}", short_id(&parent)),
                );
            }
            verlet_chat::Action::Compact => {
                client.thread_compact_start(&self.thread_id).await?;
                let _ = events.send(verlet_chat::ChatEvent::Info {
                    title: "compaction requested".to_string(),
                    body: Vec::new(),
                });
            }
            verlet_chat::Action::ListModels => {
                let _ = events.send(verlet_chat::ChatEvent::Models(self.models.clone()));
            }
        }
        Ok(())
    }

    fn switch_thread(
        &mut self,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
        thread: crate::adapters::codex_tui::CodexTuiThread,
        reason: &str,
    ) {
        self.thread_id = thread.id.clone();
        self.active_turn_id = None;
        let _ = events.send(verlet_chat::ChatEvent::ThreadSwitched {
            thread_id: thread.id,
            name: thread_name(&thread.raw),
            cwd: thread_cwd(&thread.raw),
            reason: reason.to_string(),
        });
    }

    /// Translate one client event into zero or more UI events.
    fn project(
        &mut self,
        event: crate::adapters::codex_tui::CodexTuiEvent,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
    ) {
        match event {
            crate::adapters::codex_tui::CodexTuiEvent::Notification(notification) => {
                for event in self.project_notification(&notification) {
                    let _ = events.send(event);
                }
            }
            crate::adapters::codex_tui::CodexTuiEvent::Error(error) => {
                self.active_turn_id = None;
                let _ = events.send(verlet_chat::ChatEvent::Error {
                    title: format!(
                        "JSON-RPC error {}: {}",
                        error.error.code, error.error.message
                    ),
                    body: Vec::new(),
                });
                let _ = events.send(verlet_chat::ChatEvent::TurnCompleted { error: None });
            }
            crate::adapters::codex_tui::CodexTuiEvent::Request(_)
            | crate::adapters::codex_tui::CodexTuiEvent::Response(_) => {}
        }
    }

    fn project_notification(
        &mut self,
        notification: &crate::adapters::app_server::connection::JsonRpcNotification,
    ) -> Vec<verlet_chat::ChatEvent> {
        let active_matches = self.active_turn_id.as_deref().is_some_and(|turn_id| {
            crate::cli::console::notification_matches_thread_turn(
                notification,
                &self.thread_id,
                turn_id,
            )
        });
        let this_thread =
            crate::cli::debug_rpc::notification_thread_id(notification) == Some(&self.thread_id);
        match notification.method.as_str() {
            "item/agentMessage/delta" if active_matches => {
                crate::cli::debug_rpc::notification_delta(notification)
                    .filter(|delta| !delta.is_empty())
                    .map(|delta| vec![verlet_chat::ChatEvent::AnswerDelta(delta.to_string())])
                    .unwrap_or_default()
            }
            "item/agentThinking/delta" if active_matches => {
                crate::cli::debug_rpc::notification_delta(notification)
                    .filter(|delta| !delta.is_empty())
                    .map(|delta| vec![verlet_chat::ChatEvent::ThinkingDelta(delta.to_string())])
                    .unwrap_or_default()
            }
            "item/started" if active_matches => notification
                .params
                .as_ref()
                .and_then(|params| params.get("item"))
                .and_then(tool_item_started)
                .map(|event| vec![event])
                .unwrap_or_default(),
            "item/commandExecution/outputDelta" if active_matches => {
                let item_id = notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("itemId"))
                    .and_then(serde_json::Value::as_str);
                let delta = crate::cli::debug_rpc::notification_delta(notification)
                    .filter(|delta| !delta.is_empty());
                match (item_id, delta) {
                    (Some(id), Some(delta)) => vec![verlet_chat::ChatEvent::ToolOutputDelta {
                        id: id.to_string(),
                        delta: delta.to_string(),
                    }],
                    _ => Vec::new(),
                }
            }
            "item/completed" if active_matches => notification
                .params
                .as_ref()
                .and_then(|params| params.get("item"))
                .and_then(tool_item_completed)
                .map(|event| vec![event])
                .unwrap_or_default(),
            "turn/started" if this_thread && self.active_turn_id.is_none() => {
                // A turn this client did not start (ingress, another operator):
                // adopt it so its stream renders here too.
                let turn_id = notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("turn"))
                    .and_then(|turn| turn.get("id"))
                    .and_then(serde_json::Value::as_str);
                match turn_id {
                    Some(turn_id) => {
                        self.active_turn_id = Some(turn_id.to_string());
                        vec![verlet_chat::ChatEvent::TurnStarted {
                            turn_id: turn_id.to_string(),
                        }]
                    }
                    None => Vec::new(),
                }
            }
            "turn/completed"
                if this_thread
                    && self.active_turn_id.as_deref().is_some_and(|turn_id| {
                        crate::cli::console::notification_turn_id(notification) == Some(turn_id)
                    }) =>
            {
                self.active_turn_id = None;
                let message = crate::cli::debug_rpc::notification_turn_error_message(notification);
                let error = (message != "unknown error").then_some(message);
                vec![verlet_chat::ChatEvent::TurnCompleted { error }]
            }
            "turn/usage" if active_matches => {
                let usage = notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("usage"));
                let total = ["inputTokens", "outputTokens"]
                    .iter()
                    .filter_map(|key| {
                        usage
                            .and_then(|usage| usage.get(key))
                            .and_then(serde_json::Value::as_u64)
                    })
                    .sum();
                vec![verlet_chat::ChatEvent::Usage {
                    total_tokens: total,
                }]
            }
            "thread/status/changed" if this_thread => {
                let status = notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("status"))
                    .and_then(|status| status.get("type"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("changed");
                vec![verlet_chat::ChatEvent::ThreadStatus(status.to_string())]
            }
            "thread/resync/started" if this_thread => {
                vec![verlet_chat::ChatEvent::ResyncStarted]
            }
            "thread/resynced" if this_thread => vec![verlet_chat::ChatEvent::Info {
                title: "stream resynced".to_string(),
                body: vec!["earlier output may have been elided".to_string()],
            }],
            "thread/resync/failed" if this_thread => {
                let message = notification
                    .params
                    .as_ref()
                    .and_then(|params| params.get("error"))
                    .and_then(|error| error.get("message"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown error");
                let mut events = vec![verlet_chat::ChatEvent::Error {
                    title: format!("stream resync failed: {message}"),
                    body: vec![
                        "the live subscription stopped; the transcript may be incomplete"
                            .to_string(),
                    ],
                }];
                if self.active_turn_id.take().is_some() {
                    events.push(verlet_chat::ChatEvent::TurnCompleted { error: None });
                }
                events
            }
            "error" if active_matches => {
                self.active_turn_id = None;
                vec![
                    verlet_chat::ChatEvent::Error {
                        title: format!(
                            "app-server error: {}",
                            crate::cli::console::notification_error_message(notification)
                        ),
                        body: Vec::new(),
                    },
                    verlet_chat::ChatEvent::TurnCompleted { error: None },
                ]
            }
            _ => Vec::new(),
        }
    }
}

/// An `item/started` payload for a tool call, as a UI event. Non-tool items
/// (the assistant message itself) return `None`.
fn tool_item_started(item: &serde_json::Value) -> Option<verlet_chat::ChatEvent> {
    let id = item.get("id").and_then(serde_json::Value::as_str)?;
    match item.get("type").and_then(serde_json::Value::as_str) {
        Some("dynamicToolCall") => {
            let tool = item
                .get("tool")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("tool");
            let arguments = item
                .get("arguments")
                .map(preview_json)
                .filter(|preview| !preview.is_empty());
            let title = match arguments {
                Some(preview) => format!("{tool} {preview}"),
                None => tool.to_string(),
            };
            Some(verlet_chat::ChatEvent::ToolStarted {
                id: id.to_string(),
                title,
            })
        }
        Some("commandExecution") => {
            let command = item
                .get("command")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("command");
            Some(verlet_chat::ChatEvent::ToolStarted {
                id: id.to_string(),
                title: command.to_string(),
            })
        }
        _ => None,
    }
}

/// An `item/completed` payload for a tool call, as a UI event.
fn tool_item_completed(item: &serde_json::Value) -> Option<verlet_chat::ChatEvent> {
    let id = item.get("id").and_then(serde_json::Value::as_str)?;
    match item.get("type").and_then(serde_json::Value::as_str) {
        Some("dynamicToolCall") => {
            let success = item
                .get("success")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(true);
            let output = item
                .get("contentItems")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|content| content.get("text").and_then(serde_json::Value::as_str))
                .collect::<Vec<_>>()
                .join("\n");
            Some(verlet_chat::ChatEvent::ToolCompleted {
                id: id.to_string(),
                success,
                output,
            })
        }
        Some("commandExecution") => {
            let success = item
                .get("exitCode")
                .and_then(serde_json::Value::as_i64)
                .is_none_or(|code| code == 0);
            let output = item
                .get("aggregatedOutput")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some(verlet_chat::ChatEvent::ToolCompleted {
                id: id.to_string(),
                success,
                output,
            })
        }
        _ => None,
    }
}

/// A one-line preview of a JSON value for tool-call titles.
fn preview_json(value: &serde_json::Value) -> String {
    let rendered = match value {
        serde_json::Value::Null => String::new(),
        serde_json::Value::String(text) => text.clone(),
        other => other.to_string(),
    };
    one_line_preview(&rendered, 64)
}

fn session_rows(threads: &serde_json::Value, current_id: &str) -> Vec<verlet_chat::SessionRow> {
    threads
        .get("data")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .take(12)
        .map(|thread| {
            let id = thread
                .get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            verlet_chat::SessionRow {
                id: id.to_string(),
                name: thread
                    .get("name")
                    .and_then(serde_json::Value::as_str)
                    .filter(|name| !name.is_empty())
                    .unwrap_or("unnamed")
                    .to_string(),
                status: thread
                    .get("status")
                    .and_then(|status| status.get("type"))
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unknown")
                    .to_string(),
                preview: thread
                    .get("preview")
                    .and_then(serde_json::Value::as_str)
                    .map(|preview| one_line_preview(preview, 64))
                    .unwrap_or_default(),
                current: id == current_id,
            }
        })
        .collect()
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

#[cfg(test)]
mod tests;
