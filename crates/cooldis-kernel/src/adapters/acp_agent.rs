use crate::{
    CodexTuiCompletedTurn, CodexTuiConnectConfig, CodexTuiTestClient, CodexTuiTurn, CooldisError,
    CooldisResult, default_cooldis_daemon_socket_path,
};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, BufWriter};
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::sync::{Mutex, mpsc};

pub const ACP_PROTOCOL_VERSION: u64 = 1;
const ACP_CONFIG_MODEL: &str = "model";
const ACP_CONFIG_THOUGHT_LEVEL: &str = "thought_level";
const ACP_THOUGHT_LEVELS: &[(&str, &str, &str)] = &[
    (
        "none",
        "None",
        "Do not request provider reasoning configuration",
    ),
    (
        "low",
        "Low",
        "Request low reasoning effort when the provider supports it",
    ),
    (
        "medium",
        "Medium",
        "Request medium reasoning effort when the provider supports it",
    ),
    (
        "high",
        "High",
        "Request high reasoning effort when the provider supports it",
    ),
];

#[derive(Clone, Debug)]
pub struct CooldisAcpAgentConfig {
    pub daemon_socket: PathBuf,
    pub request_timeout: Duration,
    pub agent_ref: Option<String>,
    pub cwd: Option<PathBuf>,
}

impl Default for CooldisAcpAgentConfig {
    fn default() -> Self {
        Self {
            daemon_socket: default_cooldis_daemon_socket_path(),
            request_timeout: Duration::from_secs(120),
            agent_ref: None,
            cwd: None,
        }
    }
}

pub async fn serve_acp_stdio<R, W>(
    reader: R,
    writer: W,
    config: CooldisAcpAgentConfig,
) -> CooldisResult<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();
    let mut agent = CooldisAcpAgent::new(config, outbound_tx);
    let mut lines = BufReader::new(reader).lines();
    let mut writer = BufWriter::new(writer);

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(line) = line.map_err(acp_io_error)? else {
                    break;
                };
                if line.trim().is_empty() {
                    continue;
                }
                let responses = match serde_json::from_str::<Value>(&line) {
                    Ok(message) => agent.handle_message(message).await,
                    Err(err) => vec![error_response(
                        Value::Null,
                        -32700,
                        format!("invalid JSON-RPC message: {err}"),
                        None,
                    )],
                };
                for response in responses {
                    write_acp_response(&mut writer, &response).await?;
                }
            }
            response = outbound_rx.recv() => {
                if let Some(response) = response {
                    write_acp_response(&mut writer, &response).await?;
                }
            }
        }
    }

    Ok(())
}

async fn write_acp_response<W>(writer: &mut BufWriter<W>, response: &Value) -> CooldisResult<()>
where
    W: AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(response).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to encode ACP response: {err}"))
    })?;
    writer
        .write_all(payload.as_bytes())
        .await
        .map_err(acp_io_error)?;
    writer.write_all(b"\n").await.map_err(acp_io_error)?;
    writer.flush().await.map_err(acp_io_error)?;
    Ok(())
}

struct CooldisAcpAgent {
    config: CooldisAcpAgentConfig,
    initialize_seen: bool,
    client_info: Option<Value>,
    state: Arc<Mutex<AcpAgentState>>,
    outbound: mpsc::UnboundedSender<Value>,
    #[cfg(unix)]
    daemon_client: Option<CodexTuiTestClient<UnixStream>>,
}

impl CooldisAcpAgent {
    fn new(config: CooldisAcpAgentConfig, outbound: mpsc::UnboundedSender<Value>) -> Self {
        Self {
            config,
            initialize_seen: false,
            client_info: None,
            state: Arc::new(Mutex::new(AcpAgentState::default())),
            outbound,
            #[cfg(unix)]
            daemon_client: None,
        }
    }

    async fn handle_message(&mut self, message: Value) -> Vec<Value> {
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            return request_id(&message)
                .map(|id| {
                    vec![error_response(
                        id,
                        -32600,
                        "JSON-RPC request missing method",
                        None,
                    )]
                })
                .unwrap_or_default();
        };
        let id = request_id(&message);
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));

        if id.is_none() {
            self.handle_notification(method, params).await;
            return Vec::new();
        }

        let id = id.unwrap_or(Value::Null);
        if method != "initialize" && method != "ping" && !self.initialize_seen {
            return vec![error_response(
                id,
                -32002,
                "connection must send initialize before ACP requests",
                None,
            )];
        }

        if method == "session/prompt" {
            return match self.session_prompt(id.clone(), params).await {
                Ok(()) => Vec::new(),
                Err(err) => vec![error_response(id, err.code, err.message, err.data)],
            };
        }

        let output = match method {
            "initialize" => self.initialize(params).await.map(AcpMethodOutput::result),
            "ping" => Ok(AcpMethodOutput::result(json!({}))),
            "session/new" => self.session_new(params).await.map(AcpMethodOutput::result),
            "session/set_config_option" => self
                .session_set_config_option(params)
                .await
                .map(AcpMethodOutput::result),
            "session/cancel" => self
                .session_cancel(params)
                .await
                .map(AcpMethodOutput::result),
            "session/close" => self
                .session_close(params)
                .await
                .map(AcpMethodOutput::result),
            _ => Err(AcpError::protocol(
                -32601,
                format!("unsupported ACP method `{method}`"),
            )),
        };

        match output {
            Ok(output) => {
                let mut responses = output.notifications;
                responses.push(json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "result": output.result,
                }));
                responses
            }
            Err(err) => vec![error_response(id, err.code, err.message, err.data)],
        }
    }

    async fn handle_notification(&mut self, method: &str, params: Value) {
        if method == "session/cancel"
            && let Err(err) = self.session_cancel(params).await
        {
            eprintln!(
                "cooldis-acp-agent: session/cancel notification failed: {}",
                err.message
            );
        }
    }

    async fn initialize(&mut self, params: Value) -> Result<Value, AcpError> {
        let requested_protocol = params
            .get("protocolVersion")
            .and_then(Value::as_u64)
            .unwrap_or(ACP_PROTOCOL_VERSION);
        if requested_protocol != ACP_PROTOCOL_VERSION {
            return Err(AcpError::protocol(
                -32602,
                format!(
                    "unsupported ACP protocolVersion {requested_protocol}; supported: {ACP_PROTOCOL_VERSION}"
                ),
            ));
        }
        self.client_info = params.get("clientInfo").cloned();
        self.initialize_seen = true;
        Ok(json!({
            "protocolVersion": ACP_PROTOCOL_VERSION,
            "agentInfo": {
                "name": "cooldis-acp-agent",
                "title": "Cooldis ACP Agent",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "agentCapabilities": {
                "promptCapabilities": {
                    "audio": false,
                    "embeddedContext": false,
                    "image": false,
                },
                "sessionCapabilities": {
                    "close": {},
                },
            },
        }))
    }

    async fn session_new(&mut self, params: Value) -> Result<Value, AcpError> {
        let params: SessionNewParams = from_value(params)?;
        if let Some(mcp_servers) = params.mcp_servers.as_ref()
            && !mcp_servers.is_empty()
        {
            return Err(AcpError::protocol(
                -32602,
                "ACP session/new mcpServers are not supported yet; configure MCP sources through Cooldis manifest binding",
            ));
        }

        let thread_params = self.thread_start_params(&params);
        #[cfg(not(unix))]
        {
            let _ = thread_params;
            return Err(AcpError::internal(
                "Cooldis ACP daemon socket transport requires Unix",
            ));
        }
        #[cfg(unix)]
        {
            let client = self.client().await.map_err(AcpError::internal)?;
            let thread = client.thread_start(thread_params).await.map_err(|err| {
                AcpError::internal(format!(
                    "Cooldis thread/start failed for ACP session/new: {err}"
                ))
            })?;
            let model_list = client.model_list().await.map_err(|err| {
                AcpError::internal(format!(
                    "Cooldis model/list failed for ACP session/new: {err}"
                ))
            })?;
            let session_config = AcpSessionConfig::from_model_list(&model_list);
            let session = AcpSession {
                thread_id: thread.id.clone(),
                active_turn_id: None,
                config: session_config,
            };
            let config_options = session.config.to_acp_options();
            self.state
                .lock()
                .await
                .sessions
                .insert(thread.id.clone(), session);

            Ok(json!({
                "sessionId": thread.id,
                "configOptions": config_options,
                "cooldis": {
                    "threadId": thread.id,
                    "thread": thread.raw,
                },
            }))
        }
    }

    async fn session_prompt(&mut self, request_id: Value, params: Value) -> Result<(), AcpError> {
        let params: SessionPromptParams = from_value(params)?;
        let (thread_id, session_config) = {
            let state = self.state.lock().await;
            let session = state.sessions.get(&params.session_id).ok_or_else(|| {
                AcpError::protocol(
                    -32602,
                    format!("unknown ACP sessionId `{}`", params.session_id),
                )
            })?;
            if let Some(active_turn_id) = &session.active_turn_id {
                return Err(AcpError::protocol(
                    -32002,
                    format!(
                        "ACP session `{}` already has active turn `{active_turn_id}`",
                        params.session_id
                    ),
                ));
            }
            (session.thread_id.clone(), session.config.clone())
        };
        let prompt_text = acp_prompt_text(&params.prompt)?;

        #[cfg(not(unix))]
        {
            let _ = (thread_id, prompt_text);
            return Err(AcpError::internal(
                "Cooldis ACP daemon socket transport requires Unix",
            ));
        }
        #[cfg(unix)]
        {
            let timeout = self.config.request_timeout;
            let mut client = connect_acp_client(&self.config)
                .await
                .map_err(AcpError::internal)?;
            let turn =
                turn_start_text_with_config(&mut client, &thread_id, &prompt_text, &session_config)
                    .await
                    .map_err(|err| {
                        AcpError::internal(format!(
                            "Cooldis turn/start failed for ACP session/prompt: {err}"
                        ))
                    })?;
            let turn_id = turn.id.clone();
            {
                let mut state = self.state.lock().await;
                let session = state.sessions.get_mut(&params.session_id).ok_or_else(|| {
                    AcpError::protocol(
                        -32602,
                        format!("unknown ACP sessionId `{}`", params.session_id),
                    )
                })?;
                session.active_turn_id = Some(turn_id.clone());
            }

            let state = self.state.clone();
            let outbound = self.outbound.clone();
            let session_id = params.session_id.clone();
            tokio::spawn(async move {
                let responses = match client
                    .wait_for_turn_completed(&thread_id, &turn_id, timeout)
                    .await
                {
                    Ok(completed) => {
                        prompt_completed_responses(request_id, &session_id, completed, turn.raw)
                    }
                    Err(err) => vec![error_response(
                        request_id,
                        -32000,
                        format!("Cooldis turn wait failed for ACP session/prompt: {err}"),
                        None,
                    )],
                };
                clear_active_turn(state, &session_id, &turn_id).await;
                for response in responses {
                    let _ = outbound.send(response);
                }
            });

            Ok(())
        }
    }

    async fn session_set_config_option(&mut self, params: Value) -> Result<Value, AcpError> {
        let params: SessionSetConfigOptionParams = from_value(params)?;
        let mut state = self.state.lock().await;
        let session = state.sessions.get_mut(&params.session_id).ok_or_else(|| {
            AcpError::protocol(
                -32602,
                format!("unknown ACP sessionId `{}`", params.session_id),
            )
        })?;
        session
            .config
            .set_config_value(&params.config_id, &params.value)?;
        Ok(json!({
            "configOptions": session.config.to_acp_options(),
        }))
    }

    async fn session_cancel(&mut self, params: Value) -> Result<Value, AcpError> {
        let params: SessionCancelParams = from_value(params)?;
        let (thread_id, turn_id) = {
            let state = self.state.lock().await;
            let session = state.sessions.get(&params.session_id).ok_or_else(|| {
                AcpError::protocol(
                    -32602,
                    format!("unknown ACP sessionId `{}`", params.session_id),
                )
            })?;
            (session.thread_id.clone(), session.active_turn_id.clone())
        };
        let Some(turn_id) = turn_id else {
            return Ok(json!({}));
        };

        #[cfg(not(unix))]
        {
            let _ = (thread_id, turn_id);
            return Err(AcpError::internal(
                "Cooldis ACP daemon socket transport requires Unix",
            ));
        }
        #[cfg(unix)]
        {
            connect_acp_client(&self.config)
                .await
                .map_err(AcpError::internal)?
                .turn_interrupt(&thread_id, &turn_id)
                .await
                .map_err(|err| {
                    AcpError::internal(format!(
                        "Cooldis turn/interrupt failed for ACP session/cancel: {err}"
                    ))
                })?;
            Ok(json!({}))
        }
    }

    async fn session_close(&mut self, params: Value) -> Result<Value, AcpError> {
        let params: SessionCloseParams = from_value(params)?;
        let (thread_id, turn_id) = {
            let mut state = self.state.lock().await;
            let session = state.sessions.remove(&params.session_id).ok_or_else(|| {
                AcpError::protocol(
                    -32602,
                    format!("unknown ACP sessionId `{}`", params.session_id),
                )
            })?;
            (session.thread_id, session.active_turn_id)
        };
        let Some(turn_id) = turn_id else {
            return Ok(json!({}));
        };

        #[cfg(not(unix))]
        {
            let _ = (thread_id, turn_id);
            return Err(AcpError::internal(
                "Cooldis ACP daemon socket transport requires Unix",
            ));
        }
        #[cfg(unix)]
        {
            connect_acp_client(&self.config)
                .await
                .map_err(AcpError::internal)?
                .turn_interrupt(&thread_id, &turn_id)
                .await
                .map_err(|err| {
                    AcpError::internal(format!(
                        "Cooldis turn/interrupt failed for ACP session/close: {err}"
                    ))
                })?;
            Ok(json!({}))
        }
    }

    fn thread_start_params(&self, params: &SessionNewParams) -> Value {
        let mut request = Map::new();
        if let Some(agent_ref) = self.config.agent_ref.as_deref() {
            request.insert("agentRef".to_string(), json!(agent_ref));
        }
        let cwd = params.cwd.as_ref().or(self.config.cwd.as_ref());
        if let Some(cwd) = cwd {
            request.insert("cwd".to_string(), json!(cwd));
        }
        Value::Object(request)
    }

    #[cfg(unix)]
    async fn client(&mut self) -> Result<&mut CodexTuiTestClient<UnixStream>, String> {
        if self.daemon_client.is_none() {
            self.daemon_client = Some(connect_acp_client(&self.config).await?);
        }
        Ok(self.daemon_client.as_mut().expect("client initialized"))
    }
}

#[cfg(unix)]
async fn connect_acp_client(
    config: &CooldisAcpAgentConfig,
) -> Result<CodexTuiTestClient<UnixStream>, String> {
    let mut client = CodexTuiTestClient::connect_unix(
        config.daemon_socket.clone(),
        CodexTuiConnectConfig {
            client_name: "cooldis-acp-agent".to_string(),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await
    .map_err(|err| {
        format!(
            "failed to connect to Cooldis daemon socket {}: {err}",
            config.daemon_socket.display()
        )
    })?;
    client.account_read().await.map_err(|err| err.to_string())?;
    Ok(client)
}

async fn turn_start_text_with_config<S>(
    client: &mut CodexTuiTestClient<S>,
    thread_id: &str,
    text: &str,
    config: &AcpSessionConfig,
) -> CooldisResult<CodexTuiTurn>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut params = json!({
        "threadId": thread_id,
        "input": [{
            "type": "text",
            "text": text,
            "text_elements": [],
        }],
        "model": config.current_model,
    });
    if let Some(thinking) = config.thinking_app_server_value() {
        params["thinking"] = thinking;
    }
    let result = client.request("turn/start", params).await?;
    let turn = result
        .get("turn")
        .cloned()
        .ok_or_else(|| CooldisError::RuntimeFactory("turn/start response missing turn".into()))?;
    let id = turn
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| CooldisError::RuntimeFactory("turn/start response turn missing id".into()))?
        .to_string();
    Ok(CodexTuiTurn { id, raw: turn })
}

async fn clear_active_turn(state: Arc<Mutex<AcpAgentState>>, session_id: &str, turn_id: &str) {
    let mut state = state.lock().await;
    if let Some(session) = state.sessions.get_mut(session_id)
        && session.active_turn_id.as_deref() == Some(turn_id)
    {
        session.active_turn_id = None;
    }
}

fn prompt_completed_responses(
    request_id: Value,
    session_id: &str,
    completed: CodexTuiCompletedTurn,
    turn: Value,
) -> Vec<Value> {
    let completed_thread_id = completed.thread_id.clone();
    let completed_turn_id = completed.turn_id.clone();
    let assistant_text = completed.assistant_text.clone();
    let stop_reason = acp_stop_reason(&completed.notifications);
    let turn = completed_turn_from_notifications(&completed.notifications, &completed_turn_id)
        .unwrap_or(turn);

    let mut responses =
        acp_updates_from_notifications(session_id, &completed_turn_id, &completed.notifications);
    if !assistant_text.is_empty() {
        responses.push(json!({
            "jsonrpc": "2.0",
            "method": "session/update",
            "params": {
                "sessionId": session_id,
                "update": {
                    "sessionUpdate": "agent_message_chunk",
                    "messageId": completed_turn_id,
                    "content": {
                        "type": "text",
                        "text": assistant_text,
                    },
                },
            },
        }));
    }
    responses.push(json!({
        "jsonrpc": "2.0",
        "id": request_id,
        "result": {
            "stopReason": stop_reason,
            "cooldis": {
                "threadId": completed_thread_id,
                "turnId": completed_turn_id,
                "assistantText": assistant_text,
                "turn": turn,
            },
        },
    }));
    responses
}

fn completed_turn_from_notifications(
    notifications: &[crate::JsonRpcNotification],
    turn_id: &str,
) -> Option<Value> {
    notifications.iter().rev().find_map(|notification| {
        if notification.method != "turn/completed" {
            return None;
        }
        let turn = notification.params.as_ref()?.get("turn")?;
        if turn.get("id").and_then(Value::as_str) == Some(turn_id) {
            return Some(turn.clone());
        }
        None
    })
}

fn acp_updates_from_notifications(
    session_id: &str,
    turn_id: &str,
    notifications: &[crate::JsonRpcNotification],
) -> Vec<Value> {
    let mut updates = Vec::new();
    for notification in notifications {
        let Some(params) = notification.params.as_ref() else {
            continue;
        };
        if params.get("turnId").and_then(Value::as_str) != Some(turn_id) {
            continue;
        }
        match notification.method.as_str() {
            "item/started" => {
                if let Some(update) = acp_tool_call_started(params) {
                    updates.push(acp_session_update(session_id, update));
                }
            }
            "item/completed" => {
                if let Some(update) = acp_tool_call_completed(params) {
                    updates.push(acp_session_update(session_id, update));
                }
            }
            "turn/usage" => {
                if let Some(update) = acp_usage_update(params) {
                    updates.push(acp_session_update(session_id, update));
                }
            }
            _ => {}
        }
    }
    updates
}

fn acp_session_update(session_id: &str, update: Value) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "session/update",
        "params": {
            "sessionId": session_id,
            "update": update,
        },
    })
}

fn acp_tool_call_started(params: &Value) -> Option<Value> {
    let item = params.get("item")?;
    if item.get("type").and_then(Value::as_str) != Some("dynamicToolCall") {
        return None;
    }
    let tool_call_id = item.get("id").and_then(Value::as_str)?;
    let tool = item.get("tool").and_then(Value::as_str).unwrap_or("tool");
    let mut update = json!({
        "sessionUpdate": "tool_call",
        "toolCallId": tool_call_id,
        "title": acp_tool_title(tool),
        "kind": acp_tool_kind(tool),
        "status": acp_tool_status(item),
    });
    if let Some(input) = item.get("arguments").filter(|value| value.is_object()) {
        update["rawInput"] = input.clone();
    }
    Some(update)
}

fn acp_tool_call_completed(params: &Value) -> Option<Value> {
    let item = params.get("item")?;
    if item.get("type").and_then(Value::as_str) != Some("dynamicToolCall") {
        return None;
    }
    let tool_call_id = item.get("id").and_then(Value::as_str)?;
    let mut update = json!({
        "sessionUpdate": "tool_call_update",
        "toolCallId": tool_call_id,
        "status": acp_tool_status(item),
    });
    if let Some(content) = acp_tool_content(item) {
        update["content"] = content;
    }
    if let Some(output) = item.get("contentItems").filter(|value| value.is_array()) {
        update["rawOutput"] = json!({ "contentItems": output });
    }
    Some(update)
}

fn acp_tool_status(item: &Value) -> &'static str {
    match item.get("status").and_then(Value::as_str) {
        Some("inProgress") | Some("in_progress") => "in_progress",
        Some("completed") => "completed",
        Some("failed") => "failed",
        Some("cancelled") | Some("canceled") => "failed",
        _ => "pending",
    }
}

fn acp_tool_kind(tool: &str) -> &'static str {
    let lower = tool.to_ascii_lowercase();
    if lower.contains("bash") || lower.contains("command") || lower.contains("exec") {
        "execute"
    } else if lower.contains("read") || lower.contains("cat") {
        "read"
    } else if lower.contains("write") || lower.contains("edit") || lower.contains("patch") {
        "edit"
    } else if lower.contains("search") || lower.contains("grep") || lower.contains("find") {
        "search"
    } else if lower.contains("fetch") || lower.contains("http") {
        "fetch"
    } else {
        "other"
    }
}

fn acp_tool_title(tool: &str) -> String {
    format!("Run {tool}")
}

fn acp_tool_content(item: &Value) -> Option<Value> {
    let content_items = item.get("contentItems")?.as_array()?;
    let content = content_items
        .iter()
        .filter_map(|item| {
            let text = item.get("text").and_then(Value::as_str)?;
            Some(json!({
                "type": "content",
                "content": {
                    "type": "text",
                    "text": text,
                },
            }))
        })
        .collect::<Vec<_>>();
    (!content.is_empty()).then_some(Value::Array(content))
}

fn acp_usage_update(params: &Value) -> Option<Value> {
    let usage = params.get("usage")?;
    let input = usage_u64(usage, "inputTokens")
        .or_else(|| usage_u64(usage, "input_tokens"))
        .unwrap_or(0);
    let output = usage_u64(usage, "outputTokens")
        .or_else(|| usage_u64(usage, "output_tokens"))
        .unwrap_or(0);
    let cache_create = usage_u64(usage, "cacheCreationInputTokens")
        .or_else(|| usage_u64(usage, "cache_creation_input_tokens"))
        .unwrap_or(0);
    let cache_read = usage_u64(usage, "cacheReadInputTokens")
        .or_else(|| usage_u64(usage, "cache_read_input_tokens"))
        .unwrap_or(0);
    let used = input
        .saturating_add(output)
        .saturating_add(cache_create)
        .saturating_add(cache_read);
    Some(json!({
        "sessionUpdate": "usage_update",
        "used": used,
        "size": used,
        "_meta": {
            "cooldis": {
                "usage": usage,
            },
        },
    }))
}

fn usage_u64(usage: &Value, field: &str) -> Option<u64> {
    usage.get(field).and_then(Value::as_u64)
}

fn acp_stop_reason(notifications: &[crate::JsonRpcNotification]) -> &'static str {
    for notification in notifications.iter().rev() {
        if notification.method == "turn/completed"
            && notification
                .params
                .as_ref()
                .and_then(|params| params.get("turn"))
                .and_then(|turn| turn.get("status"))
                .and_then(Value::as_str)
                == Some("interrupted")
        {
            return "cancelled";
        }
    }
    "end_turn"
}

fn acp_prompt_text(prompt: &[Value]) -> Result<String, AcpError> {
    let mut parts = Vec::new();
    for block in prompt {
        match block.get("type").and_then(Value::as_str) {
            Some("text") => {
                let text = block.get("text").and_then(Value::as_str).ok_or_else(|| {
                    AcpError::protocol(-32602, "ACP text content block missing `text`")
                })?;
                parts.push(text.to_string());
            }
            Some("resource_link") => {
                let uri = block.get("uri").and_then(Value::as_str).ok_or_else(|| {
                    AcpError::protocol(-32602, "ACP resource_link content block missing `uri`")
                })?;
                let label = block.get("name").and_then(Value::as_str).unwrap_or(uri);
                parts.push(format!("[resource_link: {label}]\n{uri}"));
            }
            Some("resource") => {
                return Err(AcpError::protocol(
                    -32602,
                    "ACP embedded resource content requires embeddedContext capability, which cooldis-acp-agent does not advertise yet",
                ));
            }
            Some("image") => {
                return Err(AcpError::protocol(
                    -32602,
                    "ACP image content requires image capability, which cooldis-acp-agent does not advertise yet",
                ));
            }
            Some("audio") => {
                return Err(AcpError::protocol(
                    -32602,
                    "ACP audio content requires audio capability, which cooldis-acp-agent does not advertise yet",
                ));
            }
            Some(other) => {
                return Err(AcpError::protocol(
                    -32602,
                    format!("unsupported ACP prompt content block `{other}`"),
                ));
            }
            None => {
                return Err(AcpError::protocol(
                    -32602,
                    "ACP prompt content block missing `type`",
                ));
            }
        }
    }
    Ok(parts.join("\n\n"))
}

struct AcpMethodOutput {
    notifications: Vec<Value>,
    result: Value,
}

impl AcpMethodOutput {
    fn result(result: Value) -> Self {
        Self {
            notifications: Vec::new(),
            result,
        }
    }
}

#[derive(Clone, Debug)]
struct AcpModelOption {
    value: String,
    name: String,
    description: Option<String>,
}

#[derive(Clone, Debug)]
struct AcpSessionConfig {
    model_options: Vec<AcpModelOption>,
    current_model: String,
    thought_level: String,
}

impl AcpSessionConfig {
    fn from_model_list(model_list: &Value) -> Self {
        let mut default_model = None;
        let mut options = Vec::new();
        if let Some(models) = model_list.get("data").and_then(Value::as_array) {
            for model in models {
                if model
                    .get("hidden")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                {
                    continue;
                }
                let Some(value) = model
                    .get("model")
                    .or_else(|| model.get("id"))
                    .and_then(Value::as_str)
                else {
                    continue;
                };
                if model
                    .get("isDefault")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
                    && default_model.is_none()
                {
                    default_model = Some(value.to_string());
                }
                options.push(AcpModelOption {
                    value: value.to_string(),
                    name: model
                        .get("displayName")
                        .or_else(|| model.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or(value)
                        .to_string(),
                    description: model
                        .get("description")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                });
            }
        }
        let current_model = default_model
            .or_else(|| options.first().map(|option| option.value.clone()))
            .unwrap_or_else(|| "default".to_string());
        if options.is_empty() {
            options.push(AcpModelOption {
                value: current_model.clone(),
                name: current_model.clone(),
                description: Some("Configured Cooldis model".to_string()),
            });
        }
        Self {
            model_options: options,
            current_model,
            thought_level: "none".to_string(),
        }
    }

    fn to_acp_options(&self) -> Vec<Value> {
        vec![
            json!({
                "id": ACP_CONFIG_MODEL,
                "name": "Model",
                "description": "Selects the Cooldis model for later turns in this ACP session",
                "category": "model",
                "type": "select",
                "currentValue": self.current_model,
                "options": self.model_options.iter().map(|option| {
                    let mut value = json!({
                        "value": option.value,
                        "name": option.name,
                    });
                    if let Some(description) = &option.description {
                        value["description"] = json!(description);
                    }
                    value
                }).collect::<Vec<_>>(),
            }),
            json!({
                "id": ACP_CONFIG_THOUGHT_LEVEL,
                "name": "Thinking",
                "description": "Applies a Cooldis turn-level thinking hint when supported by the selected provider",
                "category": "thought_level",
                "type": "select",
                "currentValue": self.thought_level,
                "options": ACP_THOUGHT_LEVELS.iter().map(|(value, name, description)| {
                    json!({
                        "value": value,
                        "name": name,
                        "description": description,
                    })
                }).collect::<Vec<_>>(),
            }),
        ]
    }

    fn set_config_value(&mut self, config_id: &str, value: &str) -> Result<(), AcpError> {
        match config_id {
            ACP_CONFIG_MODEL => {
                if !self
                    .model_options
                    .iter()
                    .any(|option| option.value == value)
                {
                    return Err(AcpError::protocol(
                        -32602,
                        format!(
                            "unsupported ACP config option value `{value}` for `{ACP_CONFIG_MODEL}`"
                        ),
                    ));
                }
                self.current_model = value.to_string();
                Ok(())
            }
            ACP_CONFIG_THOUGHT_LEVEL => {
                if !ACP_THOUGHT_LEVELS
                    .iter()
                    .any(|(allowed, _, _)| *allowed == value)
                {
                    return Err(AcpError::protocol(
                        -32602,
                        format!(
                            "unsupported ACP config option value `{value}` for `{ACP_CONFIG_THOUGHT_LEVEL}`"
                        ),
                    ));
                }
                self.thought_level = value.to_string();
                Ok(())
            }
            other => Err(AcpError::protocol(
                -32602,
                format!("unsupported ACP config option `{other}`"),
            )),
        }
    }

    fn thinking_app_server_value(&self) -> Option<Value> {
        match self.thought_level.as_str() {
            "none" => Some(json!({ "type": "disabled" })),
            "low" | "medium" | "high" => Some(json!({
                "type": "effort",
                "effort": self.thought_level,
            })),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
struct AcpAgentState {
    sessions: BTreeMap<String, AcpSession>,
}

impl Default for AcpAgentState {
    fn default() -> Self {
        Self {
            sessions: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug)]
struct AcpSession {
    thread_id: String,
    active_turn_id: Option<String>,
    config: AcpSessionConfig,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionNewParams {
    #[serde(default)]
    cwd: Option<PathBuf>,
    #[serde(default)]
    mcp_servers: Option<Map<String, Value>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionPromptParams {
    session_id: String,
    prompt: Vec<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionSetConfigOptionParams {
    session_id: String,
    config_id: String,
    value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionCancelParams {
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionCloseParams {
    session_id: String,
}

#[derive(Debug)]
struct AcpError {
    code: i64,
    message: String,
    data: Option<Value>,
}

impl AcpError {
    fn protocol(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self::protocol(-32000, message)
    }
}

fn from_value<T>(value: Value) -> Result<T, AcpError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(value).map_err(|err| AcpError::protocol(-32602, err.to_string()))
}

fn request_id(message: &Value) -> Option<Value> {
    message.get("id").cloned()
}

fn error_response(id: Value, code: i64, message: impl Into<String>, data: Option<Value>) -> Value {
    let mut error = json!({
        "code": code,
        "message": message.into(),
    });
    if let Some(data) = data {
        error["data"] = data;
    }
    json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error,
    })
}

fn acp_io_error(error: std::io::Error) -> CooldisError {
    CooldisError::RuntimeFactory(format!("ACP stdio I/O error: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::app_server::runtime_factory_from_provider_parts;
    use crate::{
        APP_SERVER_LOCAL_MODEL,
        APP_SERVER_OPENAI_COMPATIBLE_MODEL,
        APP_SERVER_OPENAI_COMPATIBLE_PROVIDER,
        AgentLoopConfig,
        AgentLoopFactory,
        AppServerListenAddr,
        AppServerProviderConfig,
        CanonicalContent,
        CanonicalStopReason,
        CanonicalUsage,
        // lexicon-allow: capsule - existing app-server config type used by test runtime factory
        CapsuleBindingsConfig,
        CooldisAppServer,
        CooldisAppServerConfig,
        OperationRegistry,
        ProviderApi,
        ProviderClient,
        ProviderRequest,
        ProviderResponse,
        ProviderResult,
        live_smoke_support::{LiveSmokeResult, model_misbehavior, retry_model_misbehavior},
    };
    use serde_json::Value;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::sync::Notify;
    use uuid::Uuid;

    #[tokio::test]
    async fn acp_agent_initialize_advertises_cooldis_capabilities() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: PathBuf::from("/tmp/missing-cooldis-daemon.sock"),
            request_timeout: Duration::from_secs(1),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;

        let init = read_json_response(&mut lines, 1).await;
        assert_eq!(init["result"]["protocolVersion"], ACP_PROTOCOL_VERSION);
        assert_eq!(init["result"]["agentInfo"]["name"], "cooldis-acp-agent");
        assert_eq!(
            init["result"]["agentCapabilities"]["promptCapabilities"]["image"],
            false
        );
        assert!(
            init["result"]["agentCapabilities"]
                .get("sessionLoad")
                .is_none(),
            "unimplemented load/resume capability should not be advertised: {init}"
        );
        assert!(
            init["result"]["agentCapabilities"]["sessionCapabilities"]["close"].is_object(),
            "implemented close capability should be advertised: {init}"
        );
        assert!(
            init["result"]["agentCapabilities"]["sessionCapabilities"]
                .get("resume")
                .is_none(),
            "unimplemented resume capability should not be advertised: {init}"
        );
        assert!(
            init["result"]["agentCapabilities"]["sessionCapabilities"]
                .get("list")
                .is_none(),
            "unimplemented list capability should not be advertised: {init}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn acp_agent_initialize_rejects_unsupported_protocol_version() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let config = CooldisAcpAgentConfig::default();
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":2,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;

        let response = read_json_response(&mut lines, 1).await;
        assert_eq!(response["error"]["code"], -32602);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unsupported ACP protocolVersion 2"),
            "{response}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn acp_agent_initialize_accepts_missing_optional_client_capabilities() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let config = CooldisAcpAgentConfig::default();
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
        )
        .await;

        let response = read_json_response(&mut lines, 1).await;
        assert_eq!(response["result"]["protocolVersion"], ACP_PROTOCOL_VERSION);
        assert_eq!(response["result"]["agentInfo"]["name"], "cooldis-acp-agent");

        drop(write);
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn acp_agent_rejects_session_before_initialize() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let config = CooldisAcpAgentConfig::default();
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"session/new","params":{"cwd":"/tmp"}}"#,
        )
        .await;

        let response = read_json_response(&mut lines, 1).await;
        assert_eq!(response["error"]["code"], -32002);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("initialize"),
            "{response}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn acp_agent_session_new_starts_cooldis_thread() {
        let root = PathBuf::from("/tmp").join(format!("cdis-acp-{}", Uuid::now_v7().simple()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let socket = root.join("app.sock");
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app_config = isolated_app_config(listen.clone(), &root);
        let app = CooldisAppServer::new_local(app_config).await.unwrap();
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(256 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(10),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
        let _ = read_json_response(&mut lines, 1).await;

        let request = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        });
        send(&mut write, &request.to_string()).await;

        let session = read_json_response(&mut lines, 2).await;
        assert!(session.get("error").is_none(), "{session}");
        let session_id = session["result"]["sessionId"].as_str().expect("session id");
        assert!(!session_id.is_empty(), "{session}");
        assert_eq!(session["result"]["cooldis"]["threadId"], session_id);
        assert_eq!(session["result"]["cooldis"]["thread"]["id"], session_id);

        let mut inspector = crate::CodexTuiTestClient::connect_unix(
            socket.clone(),
            crate::CodexTuiConnectConfig {
                client_name: "cooldis-acp-agent-test-inspector".to_string(),
                ..crate::CodexTuiConnectConfig::default()
            },
        )
        .await
        .unwrap();
        let events = inspector
            .request(
                "thread/events/list",
                json!({
                    "threadId": session_id,
                    "kinds": ["manifest.bind.completed"],
                }),
            )
            .await
            .unwrap();
        assert!(
            events["data"].as_array().is_some_and(|events| {
                events
                    .iter()
                    .any(|event| event["kind"].as_str() == Some("manifest.bind.completed"))
            }),
            "ACP-created thread should expose manifest bind receipt: {events}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn acp_agent_session_prompt_projects_text_to_cooldis_turn() {
        let root = PathBuf::from("/tmp").join(format!("cdis-acp-{}", Uuid::now_v7().simple()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let socket = root.join("app.sock");
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app_config = isolated_app_config(listen.clone(), &root);
        let app = CooldisAppServer::new_local(app_config).await.unwrap();
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(256 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(10),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
        let _ = read_json_response(&mut lines, 1).await;

        let new_session = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        });
        send(&mut write, &new_session.to_string()).await;
        let new_session = read_json_response(&mut lines, 2).await;
        let session_id = new_session["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string();

        let prompt = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    { "type": "text", "text": "hello acp" }
                ],
            },
        });
        send(&mut write, &prompt.to_string()).await;

        let update = loop {
            let message = read_json_message(&mut lines).await;
            if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
                break message;
            }
        };
        assert_eq!(update["method"], "session/update", "{update}");
        assert_eq!(update["params"]["sessionId"], session_id);
        assert_eq!(
            update["params"]["update"]["sessionUpdate"],
            "agent_message_chunk"
        );
        assert_eq!(
            update["params"]["update"]["content"],
            json!({ "type": "text", "text": "local:hello acp" })
        );

        let response = read_json_response(&mut lines, 3).await;
        assert_eq!(response["result"]["stopReason"], "end_turn");
        assert_eq!(
            response["result"]["cooldis"]["assistantText"],
            "local:hello acp"
        );
        assert_eq!(
            response["result"]["cooldis"]["threadId"],
            new_session["result"]["sessionId"]
        );
        assert!(
            response["result"]["cooldis"]["turnId"]
                .as_str()
                .is_some_and(|turn_id| !turn_id.is_empty()),
            "{response}"
        );
        assert_eq!(
            response["result"]["cooldis"]["turn"]["status"], "completed",
            "{response}"
        );
        assert!(
            response["result"]["cooldis"]["turn"]["completedAt"].is_number(),
            "{response}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn acp_agent_session_config_options_round_trip_and_fail_closed() {
        let root = PathBuf::from("/tmp").join(format!("cdis-acp-{}", Uuid::now_v7().simple()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let socket = root.join("app.sock");
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app_config = isolated_app_config(listen.clone(), &root);
        let app = CooldisAppServer::new_local(app_config).await.unwrap();
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(256 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(10),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
        let _ = read_json_response(&mut lines, 1).await;

        let new_session = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        });
        send(&mut write, &new_session.to_string()).await;
        let new_session = read_json_response(&mut lines, 2).await;
        let session_id = new_session["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string();
        assert_config_current(&new_session["result"], "model", APP_SERVER_LOCAL_MODEL);
        assert_config_current(&new_session["result"], "thought_level", "none");
        assert_config_values_include(&new_session["result"], "thought_level", &["low", "high"]);

        let set_thinking = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/set_config_option",
            "params": {
                "sessionId": session_id,
                "configId": "thought_level",
                "value": "low",
            },
        });
        send(&mut write, &set_thinking.to_string()).await;
        let set_thinking = read_json_response(&mut lines, 3).await;
        assert!(set_thinking.get("error").is_none(), "{set_thinking}");
        assert_config_current(&set_thinking["result"], "thought_level", "low");
        assert_config_current(&set_thinking["result"], "model", APP_SERVER_LOCAL_MODEL);

        let set_model = json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/set_config_option",
            "params": {
                "sessionId": session_id,
                "configId": "model",
                "value": APP_SERVER_LOCAL_MODEL,
            },
        });
        send(&mut write, &set_model.to_string()).await;
        let set_model = read_json_response(&mut lines, 4).await;
        assert!(set_model.get("error").is_none(), "{set_model}");
        assert_config_current(&set_model["result"], "model", APP_SERVER_LOCAL_MODEL);
        assert_config_current(&set_model["result"], "thought_level", "low");

        let bad_value = json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "session/set_config_option",
            "params": {
                "sessionId": session_id,
                "configId": "thought_level",
                "value": "turbo",
            },
        });
        send(&mut write, &bad_value.to_string()).await;
        let bad_value = read_json_response(&mut lines, 5).await;
        assert_eq!(bad_value["error"]["code"], -32602, "{bad_value}");
        assert!(
            bad_value["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unsupported ACP config option value"),
            "{bad_value}"
        );

        let bad_config = json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "session/set_config_option",
            "params": {
                "sessionId": session_id,
                "configId": "provider_secret",
                "value": "nope",
            },
        });
        send(&mut write, &bad_config.to_string()).await;
        let bad_config = read_json_response(&mut lines, 6).await;
        assert_eq!(bad_config["error"]["code"], -32602, "{bad_config}");
        assert!(
            bad_config["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unsupported ACP config option"),
            "{bad_config}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn acp_agent_prompt_projects_tool_status_and_usage_updates() {
        let root = PathBuf::from("/tmp").join(format!("cdis-acp-{}", Uuid::now_v7().simple()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let socket = root.join("app.sock");
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app_config = isolated_app_config(listen.clone(), &root);
        let provider = Arc::new(ScriptedProviderClient::new(vec![
            provider_tool_call("call_1|fc_1", "missing_tool", json!({})),
            provider_text("handled missing tool", 5, 6),
        ]));
        let runtime_config =
            AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
        let runtime_factory = Arc::new(
            AgentLoopFactory::new(runtime_config, provider.clone())
                .with_operation_registry(Arc::new(OperationRegistry::new())),
        );
        let app = CooldisAppServer::with_runtime_factory(app_config, runtime_factory)
            .await
            .unwrap();
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(512 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(10),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
        let _ = read_json_response(&mut lines, 1).await;

        let new_session = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        });
        send(&mut write, &new_session.to_string()).await;
        let new_session = read_json_response(&mut lines, 2).await;
        let session_id = new_session["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string();

        let prompt = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    { "type": "text", "text": "use missing tool" }
                ],
            },
        });
        send(&mut write, &prompt.to_string()).await;

        let mut updates = Vec::new();
        let response = loop {
            let message = read_json_message(&mut lines).await;
            if message.get("id").and_then(Value::as_u64) == Some(3) {
                break message;
            }
            updates.push(message);
        };
        assert_eq!(response["result"]["stopReason"], "end_turn", "{response}");
        assert_eq!(
            response["result"]["cooldis"]["assistantText"],
            "handled missing tool"
        );
        assert_eq!(provider.request_count(), 2);
        assert!(
            updates.iter().any(|update| {
                update["params"]["update"]["sessionUpdate"] == "tool_call"
                    && update["params"]["update"]["toolCallId"] == "call_1|fc_1"
                    && update["params"]["update"]["status"] == "in_progress"
            }),
            "missing ACP tool_call update in {updates:?}"
        );
        assert!(
            updates.iter().any(|update| {
                update["params"]["update"]["sessionUpdate"] == "tool_call_update"
                    && update["params"]["update"]["toolCallId"] == "call_1|fc_1"
                    && update["params"]["update"]["status"] == "failed"
            }),
            "missing ACP failed tool_call_update in {updates:?}"
        );
        assert!(
            updates.iter().any(|update| {
                update["params"]["update"]["sessionUpdate"] == "usage_update"
                    && update["params"]["update"]["used"] == 11
                    && update["params"]["update"]["size"] == 11
            }),
            "missing ACP usage_update in {updates:?}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn acp_agent_session_cancel_interrupts_in_flight_prompt() {
        let root = PathBuf::from("/tmp").join(format!("cdis-acp-{}", Uuid::now_v7().simple()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let socket = root.join("app.sock");
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app_config = isolated_app_config(listen.clone(), &root);
        let provider = Arc::new(PendingProviderClient::default());
        let runtime_config =
            AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
        let runtime_factory = runtime_factory_from_provider_parts(
            runtime_config,
            provider.clone(),
            // lexicon-allow: capsule - existing app-server config type used by test runtime factory
            CapsuleBindingsConfig::default(),
        );
        let app = CooldisAppServer::with_runtime_factory(app_config, runtime_factory)
            .await
            .unwrap();
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(256 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(30),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
        let _ = read_json_response(&mut lines, 1).await;

        let new_session = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        });
        send(&mut write, &new_session.to_string()).await;
        let new_session = read_json_response(&mut lines, 2).await;
        let session_id = new_session["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string();

        let prompt = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    { "type": "text", "text": "wait until cancelled" }
                ],
            },
        });
        send(&mut write, &prompt.to_string()).await;
        provider.wait_for_request().await;
        send(
            &mut write,
            &json!({
                "jsonrpc": "2.0",
                "method": "session/cancel",
                "params": { "sessionId": session_id },
            })
            .to_string(),
        )
        .await;

        let response = read_json_response(&mut lines, 3).await;
        assert!(response.get("error").is_none(), "{response}");
        assert_eq!(response["result"]["stopReason"], "cancelled", "{response}");
        assert_eq!(provider.request_count(), 1);

        drop(write);
        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn acp_agent_session_close_cancels_and_removes_active_session() {
        let root = PathBuf::from("/tmp").join(format!("cdis-acp-{}", Uuid::now_v7().simple()));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let socket = root.join("app.sock");
        let listen = AppServerListenAddr::Unix(socket.clone());
        let app_config = isolated_app_config(listen.clone(), &root);
        let provider = Arc::new(PendingProviderClient::default());
        let runtime_config =
            AgentLoopConfig::new(ProviderApi::OpenAIResponses, "openai", "gpt-test");
        let runtime_factory = runtime_factory_from_provider_parts(
            runtime_config,
            provider.clone(),
            // lexicon-allow: capsule - existing app-server config type used by test runtime factory
            CapsuleBindingsConfig::default(),
        );
        let app = CooldisAppServer::with_runtime_factory(app_config, runtime_factory)
            .await
            .unwrap();
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(256 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(30),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
        let init = read_json_response(&mut lines, 1).await;
        assert!(
            init["result"]["agentCapabilities"]["sessionCapabilities"]["close"].is_object(),
            "{init}"
        );

        let new_session = json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "session/new",
            "params": {
                "cwd": workspace.display().to_string(),
            },
        });
        send(&mut write, &new_session.to_string()).await;
        let new_session = read_json_response(&mut lines, 2).await;
        let session_id = new_session["result"]["sessionId"]
            .as_str()
            .expect("session id")
            .to_string();

        let prompt = json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    { "type": "text", "text": "close me" }
                ],
            },
        });
        send(&mut write, &prompt.to_string()).await;
        provider.wait_for_request().await;

        let close = json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "session/close",
            "params": { "sessionId": session_id },
        });
        send(&mut write, &close.to_string()).await;
        let close_response = read_json_response(&mut lines, 4).await;
        assert_eq!(close_response["result"], json!({}), "{close_response}");
        let prompt_response = read_json_response(&mut lines, 3).await;
        assert_eq!(
            prompt_response["result"]["stopReason"], "cancelled",
            "{prompt_response}"
        );

        let prompt_after_close = json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "session/prompt",
            "params": {
                "sessionId": session_id,
                "prompt": [
                    { "type": "text", "text": "should fail" }
                ],
            },
        });
        send(&mut write, &prompt_after_close.to_string()).await;
        let closed_error = read_json_response(&mut lines, 5).await;
        assert_eq!(closed_error["error"]["code"], -32602);
        assert!(
            closed_error["error"]["message"]
                .as_str()
                .unwrap()
                .contains("unknown ACP sessionId"),
            "{closed_error}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    #[ignore = "live OpenAI Compatible/MODEL ACP smoke; run through scripts/with-openai_compatible-env.sh"]
    async fn acp_agent_session_prompt_uses_openai_compatible_live_provider() {
        let live_config =
            OpenAICompatibleLiveConfig::load().expect("OpenAI Compatible/MODEL live config");
        retry_model_misbehavior("acp-agent-openai-compatible-live", |attempt| {
            run_openai_compatible_acp_prompt_attempt(live_config.clone(), attempt)
        })
        .await
        .unwrap();
    }

    async fn run_openai_compatible_acp_prompt_attempt(
        live_config: OpenAICompatibleLiveConfig,
        attempt: usize,
    ) -> LiveSmokeResult<()> {
        let root = PathBuf::from("scratch/live").join(format!(
            "acp-openai-compatible-{attempt}-{}",
            Uuid::now_v7()
        ));
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace)?;
        let socket =
            PathBuf::from("/tmp").join(format!("cdis-acp-{}.sock", Uuid::now_v7().simple()));
        let listen = AppServerListenAddr::Unix(socket.clone());
        let mut app_config = isolated_app_config(listen.clone(), &root)
            .with_openai_chat_completions(
                APP_SERVER_OPENAI_COMPATIBLE_PROVIDER,
                live_config.base_url.clone(),
                live_config.api_key.clone(),
                live_config.model.clone(),
            );
        if let AppServerProviderConfig::OpenAIChatCompletions { headers, .. } =
            &mut app_config.provider
        {
            headers.push(("X-Example-Provider".to_string(), "required".to_string()));
        }
        let app = CooldisAppServer::new_local(app_config).await?;
        let serve_task = tokio::spawn(async move { app.serve(listen).await });
        wait_for_socket(&socket).await;

        let (client, server) = tokio::io::duplex(512 * 1024);
        let config = CooldisAcpAgentConfig {
            daemon_socket: socket.clone(),
            request_timeout: Duration::from_secs(180),
            agent_ref: None,
            cwd: None,
        };
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let live_result: LiveSmokeResult<()> = async {
            let (read, mut write) = tokio::io::split(client);
            let mut lines = BufReader::new(read).lines();
            send(
                &mut write,
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1,"clientCapabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
            )
            .await;
            let init = read_json_response(&mut lines, 1).await;
            if init.get("error").is_some() {
                return Err(format!("ACP initialize failed: {init}").into());
            }

            let new_session = json!({
                "jsonrpc": "2.0",
                "id": 2,
                "method": "session/new",
                "params": {
                    "cwd": workspace.display().to_string(),
                },
            });
            send(&mut write, &new_session.to_string()).await;
            let new_session = read_json_response(&mut lines, 2).await;
            if new_session.get("error").is_some() {
                return Err(format!("ACP session/new failed: {new_session}").into());
            }
            let session_id = new_session["result"]["sessionId"]
                .as_str()
                .ok_or("ACP session/new missing sessionId")?
                .to_string();
            let marker = format!("COOLDIS_ACP_LIVE_OK_{}", Uuid::now_v7().simple());
            let prompt = json!({
                "jsonrpc": "2.0",
                "id": 3,
                "method": "session/prompt",
                "params": {
                    "sessionId": session_id,
                    "prompt": [
                        {
                            "type": "text",
                            "text": format!(
                                "Reply with exactly this single token and no other text: {marker}"
                            ),
                        }
                    ],
                },
            });
            send(&mut write, &prompt.to_string()).await;

            let update = loop {
                let message = read_json_message(&mut lines).await;
                if message.get("id").and_then(Value::as_u64) == Some(3) {
                    return Err(format!(
                        "ACP prompt response arrived before text update: {message}"
                    )
                    .into());
                }
                if message["method"] != "session/update" {
                    return Err(format!(
                        "expected ACP session/update before prompt response, got {message}"
                    )
                    .into());
                }
                if message["params"]["update"]["sessionUpdate"] == "agent_message_chunk" {
                    break message;
                }
            };
            let response = read_json_response(&mut lines, 3).await;
            if response.get("error").is_some() {
                return Err(format!("ACP session/prompt failed: {response}").into());
            }
            let assistant_text = response["result"]["cooldis"]["assistantText"]
                .as_str()
                .unwrap_or_default();
            if assistant_text.trim().is_empty() {
                return Err(model_misbehavior("MODEL ACP response was empty"));
            }
            if !assistant_text.contains(&marker) {
                return Err(model_misbehavior(format!(
                    "MODEL ACP response did not contain marker {marker}: {}",
                    compact_for_assertion(assistant_text)
                )));
            }
            if update["params"]["update"]["content"]["text"]
                .as_str()
                .unwrap_or_default()
                != assistant_text
            {
                return Err(format!(
                    "ACP update text did not match final assistant text: update={update} response={response}"
                )
                .into());
            }
            println!(
                "cooldis acp openai-compatible live ok model={} thread={} marker={} response={}",
                live_config.model,
                session_id,
                marker,
                compact_for_assertion(assistant_text)
            );
            drop(write);
            Ok(())
        }
        .await;

        server_task.abort();
        let _ = server_task.await;
        serve_task.abort();
        let _ = serve_task.await;
        if live_result.is_ok() {
            let _ = std::fs::remove_dir_all(root);
        }
        let _ = std::fs::remove_file(socket);
        live_result
    }

    #[tokio::test]
    async fn acp_agent_invalid_json_returns_parse_error() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let config = CooldisAcpAgentConfig::default();
        let (server_read, server_write) = tokio::io::split(server);
        let server_task =
            tokio::spawn(async move { serve_acp_stdio(server_read, server_write, config).await });

        let (read, mut write) = tokio::io::split(client);
        let mut lines = BufReader::new(read).lines();
        send(&mut write, "not-json").await;

        let response = lines
            .next_line()
            .await
            .unwrap()
            .expect("parse error response");
        let response: Value = serde_json::from_str(&response).unwrap();
        assert_eq!(response["id"], Value::Null);
        assert_eq!(response["error"]["code"], -32700);
        assert!(
            response["error"]["message"]
                .as_str()
                .unwrap()
                .contains("invalid JSON-RPC message"),
            "{response}"
        );

        drop(write);
        server_task.abort();
        let _ = server_task.await;
    }

    async fn send<W>(writer: &mut W, message: &str)
    where
        W: tokio::io::AsyncWrite + Unpin,
    {
        writer.write_all(message.as_bytes()).await.unwrap();
        writer.write_all(b"\n").await.unwrap();
        writer.flush().await.unwrap();
    }

    async fn read_json_message<R>(lines: &mut tokio::io::Lines<BufReader<R>>) -> Value
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let deadline = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(deadline);
        tokio::select! {
            _ = &mut deadline => panic!("timed out waiting for JSON-RPC message"),
            line = lines.next_line() => {
                let line = line.unwrap().expect("server closed before message");
                serde_json::from_str(&line).unwrap()
            }
        }
    }

    async fn read_json_response<R>(lines: &mut tokio::io::Lines<BufReader<R>>, id: u64) -> Value
    where
        R: tokio::io::AsyncRead + Unpin,
    {
        let deadline = tokio::time::sleep(Duration::from_secs(30));
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                _ = &mut deadline => panic!("timed out waiting for JSON-RPC response id {id}"),
                line = lines.next_line() => {
                    let line = line.unwrap().expect("server closed before response");
                    let value: Value = serde_json::from_str(&line).unwrap();
                    if value.get("id").and_then(Value::as_u64) == Some(id) {
                        return value;
                    }
                }
            }
        }
    }

    fn assert_config_current(result: &Value, config_id: &str, expected: &str) {
        let option = config_option(result, config_id);
        assert_eq!(
            option["currentValue"], expected,
            "unexpected ACP config option current value: {option}"
        );
    }

    fn assert_config_values_include(result: &Value, config_id: &str, expected: &[&str]) {
        let option = config_option(result, config_id);
        let values = option["options"]
            .as_array()
            .expect("config option values")
            .iter()
            .filter_map(|value| value["value"].as_str())
            .collect::<Vec<_>>();
        for expected in expected {
            assert!(
                values.contains(expected),
                "missing config option value {expected:?} in {values:?}"
            );
        }
    }

    fn config_option<'a>(result: &'a Value, config_id: &str) -> &'a Value {
        result["configOptions"]
            .as_array()
            .expect("config options")
            .iter()
            .find(|option| option["id"].as_str() == Some(config_id))
            .unwrap_or_else(|| panic!("missing ACP config option {config_id}: {result}"))
    }

    async fn wait_for_socket(path: &Path) {
        for _ in 0..1_500 {
            if path.exists() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("timed out waiting for {}", path.display());
    }

    fn isolated_app_config(listen: AppServerListenAddr, root: &Path) -> CooldisAppServerConfig {
        let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
        config.runtime_home = root.join("runtime");
        config.state_home = root.join("state");
        config.agent_registry_root = root.join("agents");
        config
    }

    #[derive(Default)]
    struct PendingProviderClient {
        requests: Mutex<Vec<ProviderRequest>>,
        request_started: Notify,
    }

    impl PendingProviderClient {
        async fn wait_for_request(&self) {
            let deadline = tokio::time::sleep(Duration::from_secs(30));
            tokio::pin!(deadline);
            tokio::select! {
                _ = &mut deadline => panic!("timed out waiting for pending provider request"),
                _ = self.request_started.notified() => {}
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl ProviderClient for PendingProviderClient {
        async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
            self.requests.lock().unwrap().push(request.clone());
            self.request_started.notify_waiters();
            std::future::pending().await
        }
    }

    struct ScriptedProviderClient {
        requests: Mutex<Vec<ProviderRequest>>,
        responses: Mutex<Vec<ProviderResponse>>,
    }

    impl ScriptedProviderClient {
        fn new(responses: Vec<ProviderResponse>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses.into_iter().rev().collect()),
            }
        }

        fn request_count(&self) -> usize {
            self.requests.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl ProviderClient for ScriptedProviderClient {
        async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
            self.requests.lock().unwrap().push(request.clone());
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop()
                .expect("scripted provider response"))
        }
    }

    fn provider_text(text: &str, input_tokens: u64, output_tokens: u64) -> ProviderResponse {
        ProviderResponse {
            content: vec![CanonicalContent::text(text)],
            usage: CanonicalUsage {
                input_tokens,
                output_tokens,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: CanonicalStopReason::EndTurn,
        }
    }

    fn provider_tool_call(call_id: &str, name: &str, arguments: Value) -> ProviderResponse {
        ProviderResponse {
            content: vec![CanonicalContent::tool_call(call_id, name, arguments)],
            usage: CanonicalUsage::default(),
            stop_reason: CanonicalStopReason::ToolUse,
        }
    }

    #[derive(Clone, Debug)]
    struct OpenAICompatibleLiveConfig {
        base_url: String,
        api_key: String,
        model: String,
    }

    impl OpenAICompatibleLiveConfig {
        fn load() -> Result<Self, Box<dyn std::error::Error>> {
            let base_url = std::env::var("COOLDIS_OPENAI_COMPATIBLE_URL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "https://api.example.invalid/v1".to_string());
            let api_key = std::env::var("COOLDIS_OPENAI_COMPATIBLE_API_KEY")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .or_else(|| {
                    std::env::var("OPENAI_COMPATIBLE_API_KEY")
                        .ok()
                        .filter(|value| !value.trim().is_empty())
                })
                .ok_or("missing COOLDIS_OPENAI_COMPATIBLE_API_KEY or OPENAI_COMPATIBLE_API_KEY")?;
            let model = std::env::var("COOLDIS_OPENAI_COMPATIBLE_MODEL")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string());
            Ok(Self {
                base_url,
                api_key,
                model,
            })
        }
    }

    fn compact_for_assertion(text: &str) -> String {
        const MAX_LEN: usize = 240;
        let compact = text.split_whitespace().collect::<Vec<_>>().join(" ");
        if compact.len() > MAX_LEN {
            format!("{}...", &compact[..MAX_LEN])
        } else {
            compact
        }
    }
}
