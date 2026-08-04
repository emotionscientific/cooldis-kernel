use crate::{
    JsonRpcError, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse, RequestId,
    VerletError, VerletResult,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::path::PathBuf;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::AUTHORIZATION;
use tokio_tungstenite::tungstenite::http::{HeaderValue, Request};
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, client_async_with_config};

pub const CODEX_TUI_UDS_WEBSOCKET_HANDSHAKE_URL: &str = "ws://localhost/rpc";
pub const CODEX_TUI_TEST_CLIENT_NAME: &str = "verlet-codex-tui-test";

const CODEX_TUI_MAX_WEBSOCKET_MESSAGE_SIZE: usize = 128 << 20;
const CODEX_TUI_INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);

/// Verlet-owned copy of the Codex TUI remote-client path, trimmed to a
/// headless driver for app-server tests.
#[derive(Clone)]
pub struct CodexTuiConnectConfig {
    pub client_name: String,
    pub client_version: String,
    pub experimental_api: bool,
    pub opt_out_notification_methods: Vec<String>,
    pub bearer_token: Option<String>,
}

impl Default for CodexTuiConnectConfig {
    fn default() -> Self {
        // MCP, ACP, debug RPC, and other daemon clients inherit this config.
        // Managed callers provide their boundary credential via
        // VERLET_APP_SERVER_TOKEN; an explicit field value may override it.
        let bearer_token = crate::env_compat::var("VERLET_APP_SERVER_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty());
        Self {
            client_name: CODEX_TUI_TEST_CLIENT_NAME.to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            experimental_api: true,
            opt_out_notification_methods: Vec::new(),
            bearer_token,
        }
    }
}

impl std::fmt::Debug for CodexTuiConnectConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodexTuiConnectConfig")
            .field("client_name", &self.client_name)
            .field("client_version", &self.client_version)
            .field("experimental_api", &self.experimental_api)
            .field(
                "opt_out_notification_methods",
                &self.opt_out_notification_methods,
            )
            .field(
                "bearer_token",
                &self.bearer_token.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct CodexTuiThread {
    pub id: String,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct CodexTuiTurn {
    pub id: String,
    pub raw: Value,
}

#[derive(Clone, Debug)]
pub struct CodexTuiCompletedTurn {
    pub thread_id: String,
    pub turn_id: String,
    pub assistant_text: String,
    pub notifications: Vec<JsonRpcNotification>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CodexTuiEvent {
    Notification(JsonRpcNotification),
    Request(JsonRpcRequest),
    Response(JsonRpcResponse),
    Error(JsonRpcError),
}

pub struct CodexTuiTestClient<S> {
    websocket: WebSocketStream<S>,
    next_request_id: i64,
    pending_events: VecDeque<CodexTuiEvent>,
    initialize_result: Value,
}

pub type VerletOperatorClient<S> = CodexTuiTestClient<S>;

#[cfg(unix)]
impl CodexTuiTestClient<UnixStream> {
    pub async fn connect_unix(
        socket_path: impl Into<PathBuf>,
        config: CodexTuiConnectConfig,
    ) -> VerletResult<Self> {
        let socket_path = socket_path.into();
        let endpoint = format!("unix://{}", socket_path.display());
        let mut request = CODEX_TUI_UDS_WEBSOCKET_HANDSHAKE_URL
            .into_client_request()
            .map_err(|err| tui_error(format!("invalid Verlet RPC handshake URL: {err}")))?;
        set_bearer_token(&mut request, config.bearer_token.as_deref())?;
        let stream = UnixStream::connect(&socket_path).await.map_err(|err| {
            tui_error(format!(
                "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
            ))
        })?;
        let (websocket, _) =
            client_async_with_config(request, stream, Some(codex_tui_websocket_config()))
                .await
                .map_err(|err| {
                    tui_error(format!(
                        "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
                    ))
                })?;
        Self::connect_with_websocket(websocket, endpoint, config).await
    }
}

impl CodexTuiTestClient<TcpStream> {
    pub async fn connect_websocket(url: &str, config: CodexTuiConnectConfig) -> VerletResult<Self> {
        let endpoint = url.to_string();
        let authority = websocket_tcp_authority(url)?;
        let mut request = url
            .into_client_request()
            .map_err(|err| tui_error(format!("invalid Verlet RPC URL `{endpoint}`: {err}")))?;
        set_bearer_token(&mut request, config.bearer_token.as_deref())?;
        let stream = TcpStream::connect(authority).await.map_err(|err| {
            tui_error(format!(
                "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
            ))
        })?;
        let (websocket, _) =
            client_async_with_config(request, stream, Some(codex_tui_websocket_config()))
                .await
                .map_err(|err| {
                    tui_error(format!(
                        "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
                    ))
                })?;
        Self::connect_with_websocket(websocket, endpoint, config).await
    }
}

fn set_bearer_token(request: &mut Request<()>, token: Option<&str>) -> VerletResult<()> {
    let Some(token) = token else {
        return Ok(());
    };
    let value = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|_| tui_error("Verlet app-server bearer token is not a valid HTTP header"))?;
    request.headers_mut().insert(AUTHORIZATION, value);
    Ok(())
}

impl<S> CodexTuiTestClient<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn connect_with_websocket(
        mut websocket: WebSocketStream<S>,
        endpoint: impl Into<String>,
        config: CodexTuiConnectConfig,
    ) -> VerletResult<Self> {
        let endpoint = endpoint.into();
        let (initialize_result, pending_events) =
            initialize_remote_connection(&mut websocket, &endpoint, config).await?;
        Ok(Self {
            websocket,
            next_request_id: 1,
            pending_events,
            initialize_result,
        })
    }

    pub fn initialize_result(&self) -> &Value {
        &self.initialize_result
    }

    pub async fn account_read(&mut self) -> VerletResult<Value> {
        self.request("account/read", json!({ "includeToken": false }))
            .await
    }

    pub async fn model_list(&mut self) -> VerletResult<Value> {
        self.request("model/list", json!({})).await
    }

    pub async fn config_read(&mut self, include_layers: bool) -> VerletResult<Value> {
        self.request(
            "config/read",
            json!({
                "includeLayers": include_layers,
            }),
        )
        .await
    }

    pub async fn thread_start(&mut self, params: Value) -> VerletResult<CodexTuiThread> {
        let result = self.request("thread/start", params).await?;
        thread_from_result(result, "thread/start")
    }

    pub async fn thread_resume(
        &mut self,
        thread_id: &str,
        include_turns: bool,
    ) -> VerletResult<CodexTuiThread> {
        let result = self
            .request(
                "thread/resume",
                json!({
                    "threadId": thread_id,
                    "excludeTurns": !include_turns,
                }),
            )
            .await?;
        thread_from_result(result, "thread/resume")
    }

    pub async fn thread_fork(&mut self, thread_id: &str) -> VerletResult<CodexTuiThread> {
        let result = self
            .request(
                "thread/fork",
                json!({
                    "threadId": thread_id,
                    "ephemeral": false,
                }),
            )
            .await?;
        thread_from_result(result, "thread/fork")
    }

    pub async fn thread_name_set(&mut self, thread_id: &str, name: &str) -> VerletResult<Value> {
        self.request(
            "thread/name/set",
            json!({
                "threadId": thread_id,
                "name": name,
            }),
        )
        .await
    }

    pub async fn thread_compact_start(&mut self, thread_id: &str) -> VerletResult<Value> {
        self.request(
            "thread/compact/start",
            json!({
                "threadId": thread_id,
            }),
        )
        .await
    }

    pub async fn thread_read(
        &mut self,
        thread_id: &str,
        include_turns: bool,
    ) -> VerletResult<Value> {
        self.request(
            "thread/read",
            json!({
                "threadId": thread_id,
                "includeTurns": include_turns,
            }),
        )
        .await
    }

    pub async fn thread_list(&mut self) -> VerletResult<Value> {
        self.request("thread/list", json!({})).await
    }

    pub async fn loaded_thread_list(&mut self) -> VerletResult<Value> {
        self.request("thread/loaded/list", json!({})).await
    }

    pub async fn thread_unsubscribe(&mut self, thread_id: &str) -> VerletResult<Value> {
        self.request("thread/unsubscribe", json!({ "threadId": thread_id }))
            .await
    }

    pub async fn turn_start_text(
        &mut self,
        thread_id: &str,
        text: &str,
    ) -> VerletResult<CodexTuiTurn> {
        let result = self
            .request(
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [codex_text_input(text)],
                }),
            )
            .await?;
        let turn = result
            .get("turn")
            .cloned()
            .ok_or_else(|| tui_error("turn/start response missing turn"))?;
        let id = string_field(&turn, "id", "turn/start turn")?;
        Ok(CodexTuiTurn { id, raw: turn })
    }

    pub async fn turn_steer_text(
        &mut self,
        thread_id: &str,
        expected_turn_id: &str,
        text: &str,
    ) -> VerletResult<Value> {
        self.request(
            "turn/steer",
            json!({
                "threadId": thread_id,
                "expectedTurnId": expected_turn_id,
                "input": [codex_text_input(text)],
            }),
        )
        .await
    }

    pub async fn turn_interrupt(&mut self, thread_id: &str, turn_id: &str) -> VerletResult<Value> {
        self.request(
            "turn/interrupt",
            json!({
                "threadId": thread_id,
                "turnId": turn_id,
            }),
        )
        .await
    }

    pub async fn run_prompt(
        &mut self,
        prompt: &str,
        timeout: Duration,
    ) -> VerletResult<CodexTuiCompletedTurn> {
        let thread = self.thread_start(json!({})).await?;
        let turn = self.turn_start_text(&thread.id, prompt).await?;
        self.wait_for_turn_completed(&thread.id, &turn.id, timeout)
            .await
    }

    pub async fn wait_for_turn_completed(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        timeout_duration: Duration,
    ) -> VerletResult<CodexTuiCompletedTurn> {
        let deadline = tokio::time::sleep(timeout_duration);
        tokio::pin!(deadline);
        let mut assistant_text = String::new();
        let mut notifications = Vec::new();
        loop {
            tokio::select! {
                _ = &mut deadline => {
                    return Err(tui_error(format!(
                        "timed out waiting for Verlet RPC turn `{turn_id}` to complete"
                    )));
                }
                event = self.next_event() => {
                    match event? {
                        CodexTuiEvent::Notification(notification) => {
                            if notification.method == "item/agentMessage/delta"
                                && notification.params.as_ref()
                                    .and_then(|params| params.get("threadId"))
                                    .and_then(Value::as_str) == Some(thread_id)
                                && notification.params.as_ref()
                                    .and_then(|params| params.get("turnId"))
                                    .and_then(Value::as_str) == Some(turn_id)
                                && let Some(delta) = notification.params.as_ref()
                                    .and_then(|params| params.get("delta"))
                                    .and_then(Value::as_str)
                            {
                                assistant_text.push_str(delta);
                            }

                            let completed = notification.method == "turn/completed"
                                && notification.params.as_ref()
                                    .and_then(|params| params.get("turn"))
                                    .and_then(|turn| turn.get("id"))
                                    .and_then(Value::as_str) == Some(turn_id);
                            notifications.push(notification);
                            if completed {
                                return Ok(CodexTuiCompletedTurn {
                                    thread_id: thread_id.to_string(),
                                    turn_id: turn_id.to_string(),
                                    assistant_text,
                                    notifications,
                                });
                            }
                        }
                        CodexTuiEvent::Error(error) => {
                            return Err(tui_error(format!(
                                "Verlet RPC client received JSON-RPC error {}: {}",
                                error.error.code,
                                error.error.message
                            )));
                        }
                        CodexTuiEvent::Request(_) | CodexTuiEvent::Response(_) => {}
                    }
                }
            }
        }
    }

    pub async fn request(&mut self, method: &str, params: Value) -> VerletResult<Value> {
        let id = RequestId::Integer(self.next_request_id);
        self.next_request_id += 1;
        self.request_with_id(id, method, params).await
    }

    pub async fn request_with_id(
        &mut self,
        id: RequestId,
        method: &str,
        params: Value,
    ) -> VerletResult<Value> {
        write_jsonrpc_message(
            &mut self.websocket,
            JsonRpcMessage::Request(JsonRpcRequest {
                id: id.clone(),
                method: method.to_string(),
                params: Some(params),
                trace: None,
            }),
        )
        .await?;

        loop {
            match self.read_event().await? {
                CodexTuiEvent::Response(response) if response.id == id => {
                    return Ok(response.result);
                }
                CodexTuiEvent::Error(error) if error.id == id => {
                    return Err(tui_error(format!(
                        "request `{method}` was refused: {}",
                        error.error.message
                    )));
                }
                event => self.pending_events.push_back(event),
            }
        }
    }

    pub async fn notify(&mut self, method: &str, params: Option<Value>) -> VerletResult<()> {
        write_jsonrpc_message(
            &mut self.websocket,
            JsonRpcMessage::Notification(JsonRpcNotification {
                method: method.to_string(),
                params,
            }),
        )
        .await
    }

    pub async fn next_event(&mut self) -> VerletResult<CodexTuiEvent> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(event);
        }
        self.read_event().await
    }

    async fn read_event(&mut self) -> VerletResult<CodexTuiEvent> {
        loop {
            let message = self
                .websocket
                .next()
                .await
                .ok_or_else(|| tui_error("Verlet RPC connection closed"))?
                .map_err(|err| tui_error(format!("Verlet RPC connection read failed: {err}")))?;
            match message {
                Message::Text(text) => return jsonrpc_event_from_text(&text),
                Message::Close(frame) => {
                    let reason = frame
                        .as_ref()
                        .map(|frame| frame.reason.to_string())
                        .filter(|reason| !reason.is_empty())
                        .unwrap_or_else(|| "connection closed".to_string());
                    return Err(tui_error(format!(
                        "Verlet RPC connection was closed by the endpoint: {reason}"
                    )));
                }
                Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    }

    pub async fn close(&mut self) -> VerletResult<()> {
        self.websocket
            .close(None)
            .await
            .map_err(|err| tui_error(format!("failed to close Verlet RPC connection: {err}")))
    }
}

async fn initialize_remote_connection<S>(
    websocket: &mut WebSocketStream<S>,
    endpoint: &str,
    config: CodexTuiConnectConfig,
) -> VerletResult<(Value, VecDeque<CodexTuiEvent>)>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let initialize_request_id = RequestId::String("initialize".to_string());
    write_jsonrpc_message(
        websocket,
        JsonRpcMessage::Request(JsonRpcRequest {
            id: initialize_request_id.clone(),
            method: "initialize".to_string(),
            params: Some(json!({
                "clientInfo": {
                    "name": config.client_name,
                    "title": null,
                    "version": config.client_version,
                },
                "capabilities": {
                    "experimentalApi": config.experimental_api,
                    "requestAttestation": false,
                    "optOutNotificationMethods": if config.opt_out_notification_methods.is_empty() {
                        Value::Null
                    } else {
                        json!(config.opt_out_notification_methods)
                    },
                },
            })),
            trace: None,
        }),
    )
    .await?;

    let mut pending_events = VecDeque::new();
    let initialize_result = tokio::time::timeout(CODEX_TUI_INITIALIZE_TIMEOUT, async {
        loop {
            match read_jsonrpc_event(websocket).await? {
                CodexTuiEvent::Response(response) if response.id == initialize_request_id => {
                    break Ok(response.result);
                }
                CodexTuiEvent::Error(error) if error.id == initialize_request_id => {
                    break Err(tui_error(format!(
                        "Verlet RPC endpoint `{endpoint}` refused initialization: {}",
                        error.error.message
                    )));
                }
                event => pending_events.push_back(event),
            }
        }
    })
    .await
    .map_err(|_| {
        tui_error(format!(
            "timed out waiting for initialize response from `{endpoint}`"
        ))
    })??;

    write_jsonrpc_message(
        websocket,
        JsonRpcMessage::Notification(JsonRpcNotification {
            method: "initialized".to_string(),
            params: None,
        }),
    )
    .await?;

    Ok((initialize_result, pending_events))
}

async fn write_jsonrpc_message<S>(
    websocket: &mut WebSocketStream<S>,
    message: JsonRpcMessage,
) -> VerletResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&message)
        .map_err(|err| tui_error(format!("failed to encode Verlet RPC message: {err}")))?;
    websocket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| tui_error(format!("failed to write Verlet RPC message: {err}")))
}

async fn read_jsonrpc_event<S>(websocket: &mut WebSocketStream<S>) -> VerletResult<CodexTuiEvent>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    loop {
        let message = websocket
            .next()
            .await
            .ok_or_else(|| tui_error("Verlet RPC connection closed"))?
            .map_err(|err| tui_error(format!("Verlet RPC connection read failed: {err}")))?;
        match message {
            Message::Text(text) => return jsonrpc_event_from_text(&text),
            Message::Close(frame) => {
                let reason = frame
                    .as_ref()
                    .map(|frame| frame.reason.to_string())
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| "connection closed".to_string());
                return Err(tui_error(format!(
                    "Verlet RPC connection was closed by the endpoint: {reason}"
                )));
            }
            Message::Binary(_) | Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
        }
    }
}

fn jsonrpc_event_from_text(text: &str) -> VerletResult<CodexTuiEvent> {
    match serde_json::from_str::<JsonRpcMessage>(text)
        .map_err(|err| tui_error(format!("invalid Verlet RPC message: {err}")))?
    {
        JsonRpcMessage::Notification(notification) => Ok(CodexTuiEvent::Notification(notification)),
        JsonRpcMessage::Request(request) => Ok(CodexTuiEvent::Request(request)),
        JsonRpcMessage::Response(response) => Ok(CodexTuiEvent::Response(response)),
        JsonRpcMessage::Error(error) => Ok(CodexTuiEvent::Error(error)),
    }
}

fn codex_text_input(text: &str) -> Value {
    json!({
        "type": "text",
        "text": text,
        "text_elements": [],
    })
}

fn string_field(value: &Value, field: &str, context: &str) -> VerletResult<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| tui_error(format!("{context} missing `{field}`")))
}

fn thread_from_result(result: Value, method: &str) -> VerletResult<CodexTuiThread> {
    let thread = result
        .get("thread")
        .cloned()
        .ok_or_else(|| tui_error(format!("{method} response missing thread")))?;
    let id = string_field(&thread, "id", &format!("{method} thread"))?;
    Ok(CodexTuiThread { id, raw: thread })
}

fn codex_tui_websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_frame_size(Some(CODEX_TUI_MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_message_size(Some(CODEX_TUI_MAX_WEBSOCKET_MESSAGE_SIZE))
}

fn websocket_tcp_authority(url: &str) -> VerletResult<&str> {
    let rest = url
        .strip_prefix("ws://")
        .ok_or_else(|| tui_error(format!("Verlet RPC URL must start with ws://: {url:?}")))?;
    let authority = rest
        .split_once('/')
        .map(|(authority, _)| authority)
        .unwrap_or(rest);
    if authority.is_empty() {
        return Err(tui_error("Verlet RPC URL requires host:port"));
    }
    Ok(authority)
}

fn tui_error(message: impl Into<String>) -> VerletError {
    VerletError::RpcClient(message.into())
}

#[cfg(test)]
mod tests;
