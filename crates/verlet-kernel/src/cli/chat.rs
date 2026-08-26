//! `verlet chat` — the host side of the terminal chat UI.
//!
//! The UI itself lives in the `verlet-chat` crate and is presentation only.
//! This module owns everything with authority: the app-server connection, the
//! turn lifecycle, and the translation between JSON-RPC notifications and the
//! UI's typed [`verlet_chat::ChatEvent`]s. The split preserves the chat
//! boundary invariant (see `docs/chat.md`): chat is a pure app-server client,
//! and the UI is a pure function of what this driver feeds it.

#[derive(Clone, Copy, Debug)]
pub(crate) enum ChatInvocation {
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ChatAttachTarget {
    Unix(std::path::PathBuf),
    WebSocket(String),
}

pub(crate) async fn run(
    args: Vec<std::ffi::OsString>,
    invocation: ChatInvocation,
    client: Option<crate::cli::InstanceClient>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = crate::cli::console::parse_chat_args(args)?;
    if options.help {
        invocation.print_help();
        return Ok(());
    }
    run_chat_console(options, invocation, client).await
}

async fn run_chat_console(
    options: crate::cli::console::ChatArgs,
    invocation: ChatInvocation,
    client: Option<crate::cli::InstanceClient>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if let Some(raw_attach) = options.attach.clone() {
        let target = parse_attach_target(&raw_attach)?;
        return run_attached_chat(options, invocation, target).await;
    }

    let client = client.ok_or_else(|| {
        crate::cli::usage_error("chat command did not receive an instance connection")
    })?;
    run_chat_client(client, options.prompt, "project instance".to_string(), true).await
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
                let client = crate::adapters::operator_client::OperatorClient::connect_unix(
                    path,
                    chat_connect_config(invocation),
                )
                .await?;
                run_chat_client(client, options.prompt, label, false).await
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
            let client = crate::adapters::operator_client::OperatorClient::<tokio::net::TcpStream>::connect_websocket(
                &url,
                chat_connect_config(invocation),
            )
            .await?;
            run_chat_client(client, options.prompt, label, false).await
        }
    }
}

fn chat_connect_config(
    invocation: ChatInvocation,
) -> crate::adapters::operator_client::OperatorConnectConfig {
    crate::adapters::operator_client::OperatorConnectConfig {
        client_name: invocation.client_name().to_string(),
        ..crate::adapters::operator_client::OperatorConnectConfig::default()
    }
}

pub(crate) fn parse_attach_target(
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
    initial_events: Vec<verlet_chat::ChatEvent>,
}

async fn run_chat_client<S>(
    mut client: crate::adapters::operator_client::OperatorClient<S>,
    initial_prompt: Option<String>,
    connection_label: String,
    local_kits: bool,
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
    let mut app = verlet_chat::app::App::new(meta);
    if let Some(prompt) = initial_prompt {
        app.submit(&prompt);
    }

    let (action_tx, action_rx) = tokio::sync::mpsc::unbounded_channel();
    let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel();
    let no_color = std::env::var_os("NO_COLOR").is_some();

    for event in session.initial_events {
        let _ = event_tx.send(event);
    }

    let mut driver = ChatDriver::new(thread.id, local_kits)?;

    let run_result = {
        let ui = verlet_chat::runner::run_ui(&mut app, no_color, action_tx, event_rx);
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
    client: &mut crate::adapters::operator_client::OperatorClient<S>,
    connection_label: String,
) -> crate::kernel::runtime_host::VerletResult<ChatSessionInfo>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    client.account_read().await?;
    let config = client.config_read(false).await?;
    let models = client.model_list_typed().await?;
    let cwd = config
        .get("config")
        .and_then(|config| config.get("cwd"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("?")
        .to_string();
    // EMO-575 hides the offline echo launch pair from model/list, so a fresh
    // install legitimately reports no rows and no active model. That is the
    // first-run state: label the hidden launch pair and let the setup window
    // take over.
    let active = models.data.iter().find(|model| model.active);
    let model_label = active
        .map(|model| format!("{}/{}", model.provider_id, model.model))
        .unwrap_or_else(|| "local/echo".to_string());
    let auth_missing = active.is_none_or(|model| {
        model.auth_status == crate::adapters::operator_client::OperatorModelAuthStatus::Missing
    });
    let initial_events = if auth_missing {
        let auth = client.model_provider_auth_status_typed().await?;
        if auth.data.iter().any(|provider| provider.configured) {
            Vec::new()
        } else {
            vec![verlet_chat::ChatEvent::NoConfiguredProviders]
        }
    } else {
        Vec::new()
    };
    Ok(ChatSessionInfo {
        connection_label,
        cwd,
        model_label,
        initial_events,
    })
}

/// The host loop's authoritative state: which thread the UI is on and whether
/// a turn is in flight. The UI mirrors this through events; it never owns it.
struct ChatDriver {
    thread_id: String,
    active_turn_id: Option<String>,
    oauth_client: crate::openai_codex::OpenAICodexOAuthClient,
    pending_login: Option<PendingLogin>,
    next_login_id: u64,
    /// Kit installs run against the cwd-relative registry roots, which only
    /// match the daemon's roots for the local project-instance connection.
    /// False for `--attach` sessions.
    local_kits: bool,
    /// The in-flight kit install, if any. Never aborted: install is not
    /// cancellable mid-way, and the record write is atomic.
    kit_install: Option<tokio::task::JoinHandle<()>>,
}

struct PendingLogin {
    id: u64,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for PendingLogin {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum LoginTaskEvent {
    DeviceCode {
        id: u64,
        verification_uri: String,
        user_code: String,
    },
    BrowserOpenFailed {
        id: u64,
        authorization_url: String,
    },
    Finished {
        id: u64,
        provider_id: String,
        result: Result<verlet_metadata::provider_store::LlmProviderCredential, String>,
    },
}

impl ChatDriver {
    fn new(thread_id: String, local_kits: bool) -> crate::kernel::runtime_host::VerletResult<Self> {
        let oauth_client = crate::openai_codex::OpenAICodexOAuthClient::new()
            .map_err(|err| crate::cli::usage_error(err.to_string()))?;
        Ok(Self {
            thread_id,
            active_turn_id: None,
            oauth_client,
            pending_login: None,
            next_login_id: 0,
            local_kits,
            kit_install: None,
        })
    }

    /// Run until the UI hangs up (returns `Ok`) or the transport fails
    /// (returns the error). RPC failures on individual commands are reported
    /// into the transcript and are not fatal.
    async fn drive<S>(
        &mut self,
        client: &mut crate::adapters::operator_client::OperatorClient<S>,
        mut actions: tokio::sync::mpsc::UnboundedReceiver<verlet_chat::Action>,
        events: tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
    ) -> crate::kernel::runtime_host::VerletResult<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let (login_tx, mut login_rx) = tokio::sync::mpsc::unbounded_channel();
        let result = loop {
            tokio::select! {
                action = actions.recv() => {
                    let Some(action) = action else {
                        break Ok(());
                    };
                    if let Err(err) = self.execute(client, &events, &login_tx, action).await {
                        let _ = events.send(verlet_chat::ChatEvent::Error {
                            title: err.to_string(),
                            body: Vec::new(),
                        });
                    }
                }
                app_event = client.next_event() => {
                    match app_event {
                        Ok(event) => self.project(event, &events),
                        Err(err) => break Err(err),
                    }
                }
                login_event = login_rx.recv() => {
                    if let Some(login_event) = login_event {
                        self.apply_login_event(client, &events, login_event).await;
                    }
                }
            }
        };
        self.abort_pending_login();
        result
    }

    fn abort_pending_login(&mut self) {
        if let Some(login) = self.pending_login.take() {
            login.task.abort();
        }
    }

    async fn execute<S>(
        &mut self,
        client: &mut crate::adapters::operator_client::OperatorClient<S>,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
        login_tx: &tokio::sync::mpsc::UnboundedSender<LoginTaskEvent>,
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
                // Fetched fresh on every open: auth status and the active
                // flag change underneath a long-lived session.
                let models = client.model_list_typed().await?;
                let _ = events.send(verlet_chat::ChatEvent::Models(model_rows(&models)));
            }
            verlet_chat::Action::SelectModel { provider_id, model } => {
                let selected = client.model_select_typed(&provider_id, &model).await?;
                let _ = events.send(verlet_chat::ChatEvent::ModelSelected {
                    provider_id: selected.active.provider_id,
                    model: selected.active.model,
                });
            }
            verlet_chat::Action::FetchProviderCatalog => {
                let catalog = client.model_provider_catalog_typed().await?;
                let _ = events.send(verlet_chat::ChatEvent::ProviderCatalog {
                    providers: catalog_provider_rows(&catalog),
                });
            }
            verlet_chat::Action::UpsertCustomProvider { spec } => {
                let provider_id = spec.provider_id.clone();
                let error = client
                    .model_provider_upsert(&custom_provider_upsert_params(&spec))
                    .await
                    .err()
                    .map(|err| err.to_string());
                let _ = events
                    .send(verlet_chat::ChatEvent::CustomProviderResult { provider_id, error });
            }
            verlet_chat::Action::DeleteCustomProvider { provider_id } => {
                let error = client
                    .model_provider_delete(&provider_id)
                    .await
                    .err()
                    .map(|err| err.to_string());
                let _ = events
                    .send(verlet_chat::ChatEvent::CustomProviderResult { provider_id, error });
            }
            verlet_chat::Action::SetProviderKey {
                provider_id,
                api_key,
            } => {
                let error = if self.pending_login.is_some() {
                    Some("a sign-in is already in progress".to_string())
                } else {
                    client
                        .model_provider_auth_set_typed(&provider_id, &api_key)
                        .await
                        .err()
                        .map(|err| redact_secret_values(err.to_string(), [&api_key]))
                };
                let _ =
                    events.send(verlet_chat::ChatEvent::CredentialResult { provider_id, error });
            }
            verlet_chat::Action::StartLogin {
                provider_id,
                method,
            } => {
                if self.pending_login.is_some() {
                    let _ = events.send(verlet_chat::ChatEvent::CredentialResult {
                        provider_id,
                        error: Some("a sign-in is already in progress".to_string()),
                    });
                } else {
                    self.next_login_id = self.next_login_id.wrapping_add(1);
                    let id = self.next_login_id;
                    let oauth_client = self.oauth_client.clone();
                    let task_events = login_tx.clone();
                    let task_provider_id = provider_id.clone();
                    let task = tokio::spawn(async move {
                        run_login_task(id, task_provider_id, method, oauth_client, task_events)
                            .await;
                    });
                    self.pending_login = Some(PendingLogin { id, task });
                }
            }
            verlet_chat::Action::CancelLogin => self.abort_pending_login(),
            verlet_chat::Action::ClearCredential { provider_id } => {
                client
                    .model_provider_auth_delete_typed(&provider_id)
                    .await?;
                let _ = events.send(verlet_chat::ChatEvent::CredentialCleared { provider_id });
            }
            verlet_chat::Action::FetchKitStatus { intent } => {
                if !self.local_kits {
                    // The first-run offer stays silent on attached sessions;
                    // an explicit open explains why there is nothing here.
                    if intent == verlet_chat::KitStatusIntent::Open {
                        let _ = events.send(verlet_chat::ChatEvent::Error {
                            title: "kit install needs the instance host".to_string(),
                            body: vec![
                                "this session is attached to another instance".to_string(),
                                "run: verlet kit install <kit-dir> where the instance runs"
                                    .to_string(),
                            ],
                        });
                    }
                } else {
                    let task_events = events.clone();
                    tokio::task::spawn_blocking(move || {
                        let event = match kit_status_rows() {
                            Ok((installed, recommended)) => verlet_chat::ChatEvent::KitStatus {
                                intent,
                                installed,
                                recommended,
                            },
                            Err(message) => verlet_chat::ChatEvent::Error {
                                title: "could not read installed kits".to_string(),
                                body: vec![message],
                            },
                        };
                        let _ = task_events.send(event);
                    });
                }
            }
            verlet_chat::Action::InstallKit { name, source } => {
                let error = if !self.local_kits {
                    Some(
                        "kit install needs the instance host; run verlet kit install there"
                            .to_string(),
                    )
                } else if self
                    .kit_install
                    .as_ref()
                    .is_some_and(|task| !task.is_finished())
                {
                    Some("a kit install is already running".to_string())
                } else {
                    None
                };
                if let Some(error) = error {
                    let _ = events.send(verlet_chat::ChatEvent::KitInstallResult {
                        name,
                        error: Some(error),
                        receipt: Vec::new(),
                    });
                } else {
                    let task_events = events.clone();
                    self.kit_install = Some(tokio::spawn(async move {
                        run_kit_install_task(name, source, task_events).await;
                    }));
                }
            }
        }
        Ok(())
    }

    async fn apply_login_event<S>(
        &mut self,
        client: &mut crate::adapters::operator_client::OperatorClient<S>,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
        event: LoginTaskEvent,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        let current_id = self.pending_login.as_ref().map(|login| login.id);
        match event {
            LoginTaskEvent::DeviceCode {
                id,
                verification_uri,
                user_code,
            } if current_id == Some(id) => {
                let _ = events.send(verlet_chat::ChatEvent::LoginDeviceCode {
                    verification_uri,
                    user_code,
                });
            }
            LoginTaskEvent::BrowserOpenFailed {
                id,
                authorization_url,
            } if current_id == Some(id) => {
                let _ = events.send(verlet_chat::ChatEvent::Info {
                    title: "open the sign-in page in your browser".to_string(),
                    body: vec![authorization_url],
                });
            }
            LoginTaskEvent::Finished {
                id,
                provider_id,
                result,
            } if current_id == Some(id) => {
                self.pending_login = None;
                let error = match result {
                    Err(message) => Some(message),
                    Ok(verlet_metadata::provider_store::LlmProviderCredential::OAuth {
                        access,
                        refresh,
                        expires_at_ms,
                        account_id,
                        email,
                    }) => client
                        .model_provider_auth_set_oauth_typed(
                            &provider_id,
                            &access,
                            &refresh,
                            expires_at_ms,
                            account_id.as_deref(),
                            email.as_deref(),
                        )
                        .await
                        .err()
                        .map(|err| redact_secret_values(err.to_string(), [&access, &refresh])),
                    Ok(verlet_metadata::provider_store::LlmProviderCredential::ApiKey {
                        ..
                    }) => Some("OAuth sign-in returned an unsupported credential type".to_string()),
                };
                let _ =
                    events.send(verlet_chat::ChatEvent::CredentialResult { provider_id, error });
            }
            LoginTaskEvent::DeviceCode { .. }
            | LoginTaskEvent::BrowserOpenFailed { .. }
            | LoginTaskEvent::Finished { .. } => {}
        }
    }

    fn switch_thread(
        &mut self,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
        thread: crate::adapters::operator_client::OperatorThread,
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
        event: crate::adapters::operator_client::OperatorEvent,
        events: &tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
    ) {
        match event {
            crate::adapters::operator_client::OperatorEvent::Notification(notification) => {
                for event in self.project_notification(&notification) {
                    let _ = events.send(event);
                }
            }
            crate::adapters::operator_client::OperatorEvent::Error(error) => {
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
            crate::adapters::operator_client::OperatorEvent::Request(_)
            | crate::adapters::operator_client::OperatorEvent::Response(_) => {}
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

async fn run_login_task(
    id: u64,
    provider_id: String,
    method: verlet_chat::LoginMethod,
    client: crate::openai_codex::OpenAICodexOAuthClient,
    events: tokio::sync::mpsc::UnboundedSender<LoginTaskEvent>,
) {
    let result = match method {
        verlet_chat::LoginMethod::Browser => match client.begin_browser_login().await {
            Ok(login) => {
                let authorization_url = login.authorization_url().to_string();
                if crate::cli::console::open_browser_url_checked(&authorization_url)
                    .await
                    .is_err()
                {
                    let _ = events.send(LoginTaskEvent::BrowserOpenFailed {
                        id,
                        authorization_url,
                    });
                }
                client
                    .complete_browser_login(login)
                    .await
                    .map_err(|err| err.to_string())
            }
            Err(err) => Err(err.to_string()),
        },
        verlet_chat::LoginMethod::Device => match client.start_device_login().await {
            Ok(login) => {
                let _ = events.send(LoginTaskEvent::DeviceCode {
                    id,
                    verification_uri: login.verification_uri.clone(),
                    user_code: login.user_code.clone(),
                });
                client
                    .complete_device_login(login)
                    .await
                    .map_err(|err| err.to_string())
            }
            Err(err) => Err(err.to_string()),
        },
    };
    let _ = events.send(LoginTaskEvent::Finished {
        id,
        provider_id,
        result,
    });
}

/// The kits the setup window recommends: `(kit name, blurb, candidate
/// directories probed in order)`. All paths are cwd-relative, like the
/// registry roots; there is no hosted fetch lane yet (EMO-611), so a kit
/// whose directory is absent renders as manual-install guidance.
const RECOMMENDED_KITS: [(&str, &str, [&str; 2]); 1] = [(
    "pi",
    "read, write, edit, find, grep file tools",
    ["dist/pi-kit", "agent-tools/pi-kit"],
)];

fn kit_roots() -> (std::path::PathBuf, std::path::PathBuf) {
    let registry_root = crate::cli::tool::default_registry_root();
    let kits_root =
        verlet_operations::kit_package::kits_root_for_operations_registry_root(&registry_root);
    (registry_root, kits_root)
}

fn kit_status_rows() -> Result<
    (
        Vec<verlet_chat::InstalledKitRow>,
        Vec<verlet_chat::RecommendedKitRow>,
    ),
    String,
> {
    let (_, kits_root) = kit_roots();
    let records = verlet_operations::kit_package::InstalledKitStore::new(kits_root)
        .list()
        .map_err(|err| err.to_string())?;
    let installed = records
        .into_iter()
        .map(|record| verlet_chat::InstalledKitRow {
            name: record.name,
            version: record.version,
            tools: record
                .tools
                .into_iter()
                .map(|tool| tool.tool_name)
                .collect(),
        })
        .collect();
    let recommended = recommended_kit_rows(std::path::Path::new("."));
    Ok((installed, recommended))
}

fn recommended_kit_rows(project_root: &std::path::Path) -> Vec<verlet_chat::RecommendedKitRow> {
    RECOMMENDED_KITS
        .iter()
        .map(|(name, blurb, candidates)| verlet_chat::RecommendedKitRow {
            name: (*name).to_string(),
            blurb: (*blurb).to_string(),
            source: candidates
                .iter()
                .find(|candidate| {
                    project_root
                        .join(candidate)
                        .join(verlet_operations::kit_package::KIT_MANIFEST_FILE_NAME)
                        .is_file()
                })
                .map(|candidate| (*candidate).to_string()),
        })
        .collect()
}

/// The spawned kit install: the same pipeline as `verlet kit install
/// <source>` run in the project directory, reported as one
/// [`verlet_chat::ChatEvent::KitInstallResult`].
async fn run_kit_install_task(
    name: String,
    source: String,
    events: tokio::sync::mpsc::UnboundedSender<verlet_chat::ChatEvent>,
) {
    let (registry_root, kits_root) = kit_roots();
    let event = match crate::cli::kit::install_kit_from(
        std::path::Path::new(&source),
        &registry_root,
        &kits_root,
    )
    .await
    {
        Ok(outcome) => verlet_chat::ChatEvent::KitInstallResult {
            name: outcome.installed.name.clone(),
            error: None,
            receipt: outcome.receipt_lines(),
        },
        Err(err) => verlet_chat::ChatEvent::KitInstallResult {
            name,
            error: Some(err.to_string()),
            receipt: Vec::new(),
        },
    };
    let _ = events.send(event);
}

fn redact_secret_values<const N: usize>(mut message: String, secrets: [&String; N]) -> String {
    for secret in secrets {
        if !secret.is_empty() {
            message = message.replace(secret, "[redacted]");
        }
    }
    message
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

fn model_rows(
    models: &crate::adapters::operator_client::OperatorModelList,
) -> Vec<verlet_chat::ModelRow> {
    models
        .data
        .iter()
        .map(|model| verlet_chat::ModelRow {
            provider_id: model.provider_id.clone(),
            model: model.model.clone(),
            display_name: model.display_name.clone(),
            auth_status: match model.auth_status {
                crate::adapters::operator_client::OperatorModelAuthStatus::Configured => {
                    "configured".to_string()
                }
                crate::adapters::operator_client::OperatorModelAuthStatus::Env => "env".to_string(),
                crate::adapters::operator_client::OperatorModelAuthStatus::Missing => {
                    "missing".to_string()
                }
            },
            active: model.active,
        })
        .collect()
}

/// The RPC `api` value for one row, translated to the chat contract's
/// family strings (`openai_chat_completions`, ...).
fn catalog_api_family(api: &crate::adapters::operator_client::OperatorProviderApi) -> String {
    match api {
        crate::adapters::operator_client::OperatorProviderApi::Family(family) => {
            match family.as_str() {
                "open_ai_chat_completions" => "openai_chat_completions".to_string(),
                "open_ai_responses" => "openai_responses".to_string(),
                other => other.to_string(),
            }
        }
        crate::adapters::operator_client::OperatorProviderApi::Other { other } => other.clone(),
    }
}

fn catalog_provider_rows(
    catalog: &crate::adapters::operator_client::OperatorModelProviderCatalog,
) -> Vec<verlet_chat::CatalogProviderRow> {
    catalog
        .providers
        .iter()
        .map(|row| verlet_chat::CatalogProviderRow {
            provider_id: row.provider_id.clone(),
            display_name: row.display_name.clone(),
            base_url: row.base_url.clone(),
            api: catalog_api_family(&row.api),
            auth_kind: row.auth_kind.clone(),
            env_vars: row.env_vars.clone(),
            configured: row.configured,
            auth_label: row.auth_label.clone().unwrap_or_default(),
            custom: row.custom,
            active: row.active,
            model_count: row.model_count,
            default_model: row.default_model.clone(),
        })
        .collect()
}

/// `modelProvider/upsert` params for a custom provider from the setup form.
/// The API key never rides here; it follows through `modelProvider/auth/set`.
fn custom_provider_upsert_params(
    spec: &verlet_chat::CustomProviderSpec,
) -> crate::adapters::operator_client::OperatorModelProviderUpsertParams {
    let api = match spec.api.as_str() {
        "openai_responses" => "open_ai_responses",
        "anthropic_messages" => "anthropic_messages",
        _ => "open_ai_chat_completions",
    };
    let headers = spec
        .header
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                verlet_metadata::provider_store::LlmProviderConfigValue::literal(value),
            )
        })
        .collect();
    let models = spec
        .models
        .iter()
        .enumerate()
        .map(|(index, model_id)| {
            crate::adapters::operator_client::OperatorModelProviderModelUpsertRecord {
                model_id: model_id.clone(),
                display_name: None,
                metadata: if index == 0 {
                    std::collections::BTreeMap::from([("default".to_string(), "true".to_string())])
                } else {
                    std::collections::BTreeMap::new()
                },
            }
        })
        .collect();
    let auth = if spec.keyless {
        verlet_metadata::provider_store::LlmProviderAuthConfig::None
    } else {
        verlet_metadata::provider_store::LlmProviderAuthConfig::StoredOrEnvironment
    };
    crate::adapters::operator_client::OperatorModelProviderUpsertParams {
        provider: crate::adapters::operator_client::OperatorModelProviderUpsertRecord {
            provider_id: spec.provider_id.clone(),
            api: crate::adapters::operator_client::OperatorProviderApi::Family(api.to_string()),
            base_url: spec.base_url.clone(),
            display_name: Some(spec.display_name.clone()),
            auth,
            headers,
            auth_header: !spec.keyless,
            models,
            metadata: std::collections::BTreeMap::from([(
                "origin".to_string(),
                "custom".to_string(),
            )]),
        },
    }
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
