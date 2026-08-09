use tokio::io::AsyncBufReadExt as _;
use tokio::io::AsyncWriteExt as _;

pub const MCP_PROTOCOL_VERSION: &str = "2025-06-18";

#[derive(Clone, Debug)]
pub struct VerletMcpServerConfig {
    pub daemon_socket: std::path::PathBuf,
    pub request_timeout: std::time::Duration,
}

impl Default for VerletMcpServerConfig {
    fn default() -> Self {
        Self {
            daemon_socket: crate::daemon::daemon_config::default_verlet_daemon_socket_path(),
            request_timeout: std::time::Duration::from_secs(120),
        }
    }
}

pub async fn serve_mcp_stdio<R, W>(
    reader: R,
    writer: W,
    config: VerletMcpServerConfig,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut server = VerletMcpServer::new(config);
    let mut lines = tokio::io::BufReader::new(reader).lines();
    let mut writer = tokio::io::BufWriter::new(writer);

    while let Some(line) = lines.next_line().await.map_err(mcp_io_error)? {
        if line.trim().is_empty() {
            continue;
        }
        let response = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(message) => server.handle_message(message).await,
            Err(err) => Some(error_response(
                serde_json::Value::Null,
                -32700,
                format!("invalid JSON-RPC message: {err}"),
                None,
            )),
        };
        if let Some(response) = response {
            let payload = serde_json::to_string(&response).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to encode MCP response: {err}"
                ))
            })?;
            writer
                .write_all(payload.as_bytes())
                .await
                .map_err(mcp_io_error)?;
            writer.write_all(b"\n").await.map_err(mcp_io_error)?;
            writer.flush().await.map_err(mcp_io_error)?;
        }
    }

    Ok(())
}

struct VerletMcpServer {
    config: VerletMcpServerConfig,
    initialize_seen: bool,
    initialized_seen: bool,
    client_info: Option<serde_json::Value>,
    #[cfg(unix)]
    daemon_client: Option<crate::adapters::operator_client::OperatorClient<tokio::net::UnixStream>>,
}

impl VerletMcpServer {
    fn new(config: VerletMcpServerConfig) -> Self {
        Self {
            config,
            initialize_seen: false,
            initialized_seen: false,
            client_info: None,
            daemon_client: None,
        }
    }

    async fn handle_message(&mut self, message: serde_json::Value) -> Option<serde_json::Value> {
        let Some(method) = message.get("method").and_then(serde_json::Value::as_str) else {
            return request_id(&message)
                .map(|id| error_response(id, -32600, "JSON-RPC request missing method", None));
        };
        let id = request_id(&message);
        let params = message
            .get("params")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        if id.is_none() {
            self.handle_notification(method, params).await;
            return None;
        }

        let id = id.unwrap_or(serde_json::Value::Null);
        if method != "initialize" && method != "ping" && !self.initialize_seen {
            return Some(error_response(
                id,
                -32002,
                "connection must send initialize before MCP requests",
                None,
            ));
        }

        let result = match method {
            "initialize" => self.initialize(params).await,
            "ping" => Ok(serde_json::json!({})),
            "tools/list" => Ok(serde_json::json!({ "tools": tool_definitions() })),
            "tools/call" => self.call_tool(params).await,
            _ => Err(McpError::protocol(
                -32601,
                format!("unsupported MCP method `{method}`"),
            )),
        };

        Some(match result {
            Ok(result) => serde_json::json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": result,
            }),
            Err(err) => error_response(id, err.code, err.message, err.data),
        })
    }

    async fn handle_notification(&mut self, method: &str, _params: serde_json::Value) {
        if method == "notifications/initialized" || method == "initialized" {
            self.initialized_seen = self.initialize_seen;
        }
    }

    async fn initialize(
        &mut self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, McpError> {
        let requested_protocol = params
            .get("protocolVersion")
            .and_then(serde_json::Value::as_str)
            .unwrap_or(MCP_PROTOCOL_VERSION);
        self.client_info = params.get("clientInfo").cloned();
        self.initialize_seen = true;
        Ok(serde_json::json!({
            "protocolVersion": negotiated_protocol_version(requested_protocol),
            "capabilities": {
                "tools": {
                    "listChanged": false,
                },
            },
            "serverInfo": {
                "name": "verlet-mcp-server",
                "title": "Verlet MCP Server",
                "version": env!("CARGO_PKG_VERSION"),
            },
            "instructions": "Use these tools to orchestrate supervised Verlet daemon threads and daemon-backed command execution.",
        }))
    }

    async fn call_tool(
        &mut self,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, McpError> {
        let params: CallToolParams = from_value(params)?;
        let arguments = params.arguments.unwrap_or_default();
        let canonical_name = if let Some(suffix) = params.name.strip_prefix(concat!("cool", "dis_"))
        {
            let canonical = format!("verlet_{suffix}");
            eprintln!(
                "warning: MCP tool {} is deprecated; use {} (compatibility will be removed in v0.4.0)",
                params.name, canonical
            );
            canonical
        } else {
            params.name.clone()
        };
        let result = match canonical_name.as_str() {
            "verlet_daemon_status" => self.tool_daemon_status().await,
            "verlet_thread_start" => self.tool_thread_start(arguments).await,
            "verlet_thread_list" => self.tool_thread_list().await,
            "verlet_thread_read" => self.tool_thread_read(arguments).await,
            "verlet_turn_start" => self.tool_turn_start(arguments).await,
            "verlet_turn_wait" => self.tool_turn_wait(arguments).await,
            "verlet_turn_interrupt" => self.tool_turn_interrupt(arguments).await,
            "verlet_prompt" => self.tool_prompt(arguments).await,
            "verlet_command_exec" => self.tool_command_exec(arguments).await,
            "verlet_capsule_binding_set" => {
                self.tool_capsule_binding("capsule/binding/set", arguments)
                    .await
            }
            "verlet_capsule_binding_delete" => {
                self.tool_capsule_binding("capsule/binding/delete", arguments)
                    .await
            }
            "verlet_capsule_binding_list" => {
                self.tool_capsule_binding("capsule/binding/list", arguments)
                    .await
            }
            "verlet_capsule_binding_resolve" => {
                self.tool_capsule_binding("capsule/binding/resolve", arguments)
                    .await
            }
            _ => {
                return Err(McpError::protocol(
                    -32602,
                    format!("unknown Verlet MCP tool `{}`", params.name),
                ));
            }
        };

        Ok(match result {
            Ok(value) => tool_result(value, false),
            Err(err) => tool_result(serde_json::json!({ "error": err }), true),
        })
    }

    async fn tool_daemon_status(&mut self) -> Result<serde_json::Value, String> {
        let socket = self.config.daemon_socket.display().to_string();
        let client = self.client().await.map_err(|err| err.to_string())?;
        let models = client
            .model_list()
            .await
            .map_err(|err| format!("daemon model/list failed: {err}"))?;
        Ok(serde_json::json!({
            "connected": true,
            "daemonSocket": socket,
            "models": models,
        }))
    }

    async fn tool_thread_start(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params = optional_thread_start_params(args)?;
        let thread = self
            .client()
            .await?
            .thread_start(params)
            .await
            .map_err(|err| err.to_string())?;
        Ok(serde_json::json!({
            "threadId": thread.id,
            "thread": thread.raw,
        }))
    }

    async fn tool_thread_list(&mut self) -> Result<serde_json::Value, String> {
        self.client()
            .await?
            .thread_list()
            .await
            .map_err(|err| err.to_string())
    }

    async fn tool_thread_read(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params: ThreadReadArgs = from_map(args)?;
        self.client()
            .await?
            .thread_read(&params.thread_id, params.include_turns.unwrap_or(false))
            .await
            .map_err(|err| err.to_string())
    }

    async fn tool_turn_start(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params: TurnStartArgs = from_map(args)?;
        let timeout = timeout_from_ms(params.timeout_ms, self.config.request_timeout);
        let client = self.client().await?;
        let turn = client
            .turn_start_text(&params.thread_id, &params.message)
            .await
            .map_err(|err| err.to_string())?;
        if params.wait.unwrap_or(false) {
            let completed = client
                .wait_for_turn_completed(&params.thread_id, &turn.id, timeout)
                .await
                .map_err(|err| err.to_string())?;
            return Ok(completed_turn_json(completed, Some(turn.raw)));
        }
        Ok(serde_json::json!({
            "threadId": params.thread_id,
            "turnId": turn.id,
            "turn": turn.raw,
        }))
    }

    async fn tool_turn_wait(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params: TurnWaitArgs = from_map(args)?;
        let timeout = timeout_from_ms(params.timeout_ms, self.config.request_timeout);
        let completed = self
            .client()
            .await?
            .wait_for_turn_completed(&params.thread_id, &params.turn_id, timeout)
            .await
            .map_err(|err| err.to_string())?;
        Ok(completed_turn_json(completed, None))
    }

    async fn tool_turn_interrupt(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params: TurnInterruptArgs = from_map(args)?;
        self.client()
            .await?
            .turn_interrupt(&params.thread_id, &params.turn_id)
            .await
            .map_err(|err| err.to_string())
    }

    async fn tool_prompt(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params: PromptArgs = from_map(args)?;
        let timeout = timeout_from_ms(params.timeout_ms, self.config.request_timeout);
        let thread_params = optional_thread_start_params(params.thread.unwrap_or_default())?;
        let client = self.client().await?;
        let thread = client
            .thread_start(thread_params)
            .await
            .map_err(|err| err.to_string())?;
        let turn = client
            .turn_start_text(&thread.id, &params.message)
            .await
            .map_err(|err| err.to_string())?;
        let completed = client
            .wait_for_turn_completed(&thread.id, &turn.id, timeout)
            .await
            .map_err(|err| err.to_string())?;
        Ok(serde_json::json!({
            "threadId": completed.thread_id,
            "turnId": completed.turn_id,
            "assistantText": completed.assistant_text,
            "thread": thread.raw,
            "turn": turn.raw,
            "notifications": completed.notifications,
        }))
    }

    async fn tool_command_exec(
        &mut self,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let params: CommandExecArgs = from_map(args)?;
        if params.command.is_empty() {
            return Err("command must contain at least one argv item".to_string());
        }
        let mut request = serde_json::json!({
            "command": params.command,
            "tty": false,
            "streamStdin": false,
            "streamStdoutStderr": false,
        });
        if let Some(cwd) = params.cwd {
            request["cwd"] = serde_json::json!(cwd);
        }
        if let Some(timeout_ms) = params.timeout_ms {
            request["timeoutMs"] = serde_json::json!(timeout_ms);
        }
        if let Some(output_bytes_cap) = params.output_bytes_cap {
            request["outputBytesCap"] = serde_json::json!(output_bytes_cap);
        }
        if let Some(env) = params.env {
            request["env"] = serde_json::json!(env);
        }
        self.client()
            .await?
            .request("command/exec", request)
            .await
            .map_err(|err| err.to_string())
    }

    async fn tool_capsule_binding(
        &mut self,
        method: &str,
        args: serde_json::Map<String, serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        self.client()
            .await?
            .request(method, serde_json::Value::Object(args))
            .await
            .map_err(|err| err.to_string())
    }

    #[cfg(unix)]
    async fn client(
        &mut self,
    ) -> Result<&mut crate::adapters::operator_client::OperatorClient<tokio::net::UnixStream>, String>
    {
        if self.daemon_client.is_none() {
            let mut client = crate::adapters::operator_client::OperatorClient::connect_unix(
                self.config.daemon_socket.clone(),
                crate::adapters::operator_client::OperatorConnectConfig {
                    client_name: "verlet-mcp-server".to_string(),
                    ..crate::adapters::operator_client::OperatorConnectConfig::default()
                },
            )
            .await
            .map_err(|err| {
                format!(
                    "failed to connect to Verlet daemon socket {}: {err}",
                    self.config.daemon_socket.display()
                )
            })?;
            client.account_read().await.map_err(|err| err.to_string())?;
            self.daemon_client = Some(client);
        }
        self.daemon_client
            .as_mut()
            .ok_or_else(|| "Verlet daemon client did not initialize".to_string())
    }
}

#[derive(Debug, serde::Deserialize)]
struct CallToolParams {
    name: String,
    #[serde(default)]
    arguments: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThreadReadArgs {
    thread_id: String,
    #[serde(default)]
    include_turns: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnStartArgs {
    thread_id: String,
    message: String,
    #[serde(default)]
    wait: Option<bool>,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnWaitArgs {
    thread_id: String,
    turn_id: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct TurnInterruptArgs {
    thread_id: String,
    turn_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct PromptArgs {
    message: String,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    thread: Option<serde_json::Map<String, serde_json::Value>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct CommandExecArgs {
    command: Vec<String>,
    #[serde(default)]
    cwd: Option<String>,
    #[serde(default)]
    timeout_ms: Option<u64>,
    #[serde(default)]
    output_bytes_cap: Option<usize>,
    #[serde(default)]
    env: Option<std::collections::HashMap<String, Option<String>>>,
}

#[derive(Debug)]
struct McpError {
    code: i64,
    message: String,
    data: Option<serde_json::Value>,
}

impl McpError {
    fn protocol(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

fn from_value<T>(value: serde_json::Value) -> Result<T, McpError>
where
    T: for<'de> serde::Deserialize<'de>,
{
    serde_json::from_value(value).map_err(|err| McpError::protocol(-32602, err.to_string()))
}

fn from_map<T>(map: serde_json::Map<String, serde_json::Value>) -> Result<T, String>
where
    T: for<'de> serde::Deserialize<'de>,
{
    serde_json::from_value(serde_json::Value::Object(map)).map_err(|err| err.to_string())
}

fn optional_thread_start_params(
    args: serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Value, String> {
    let allowed = [
        "cwd",
        "model",
        "modelProvider",
        "model_provider",
        "serviceTier",
        "service_tier",
        "ephemeral",
        "parentThreadId",
        "parent_thread_id",
        "topology",
        "capsuleBindings",
        "capsule_bindings",
    ];
    let mut output = serde_json::Map::new();
    for (key, value) in args {
        if !allowed.contains(&key.as_str()) {
            return Err(format!("unknown thread start argument `{key}`"));
        }
        let key = match key.as_str() {
            "model_provider" => "modelProvider",
            "service_tier" => "serviceTier",
            "parent_thread_id" => "parentThreadId",
            "capsule_bindings" => "capsuleBindings",
            other => other,
        };
        output.insert(key.to_string(), value);
    }
    Ok(serde_json::Value::Object(output))
}

fn timeout_from_ms(timeout_ms: Option<u64>, default: std::time::Duration) -> std::time::Duration {
    timeout_ms
        .map(std::time::Duration::from_millis)
        .unwrap_or(default)
}

fn completed_turn_json(
    completed: crate::adapters::operator_client::OperatorCompletedTurn,
    submitted_turn: Option<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "threadId": completed.thread_id,
        "turnId": completed.turn_id,
        "assistantText": completed.assistant_text,
        "submittedTurn": submitted_turn,
        "notifications": completed.notifications,
    })
}

fn request_id(message: &serde_json::Value) -> Option<serde_json::Value> {
    message.get("id").cloned()
}

fn negotiated_protocol_version(requested: &str) -> &str {
    match requested {
        "2025-11-25" | "2025-06-18" | "2025-03-26" | "2024-11-05" => requested,
        _ => MCP_PROTOCOL_VERSION,
    }
}

fn error_response(
    id: serde_json::Value,
    code: i64,
    message: impl Into<String>,
    data: Option<serde_json::Value>,
) -> serde_json::Value {
    let mut error = serde_json::json!({
        "code": code,
        "message": message.into(),
    });
    if let Some(data) = data {
        error["data"] = data;
    }
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": error,
    })
}

fn tool_result(value: serde_json::Value, is_error: bool) -> serde_json::Value {
    serde_json::json!({
        "content": [
            {
                "type": "text",
                "text": serde_json::to_string_pretty(&value).unwrap_or_else(|_| value.to_string()),
            }
        ],
        "structuredContent": value,
        "isError": is_error,
    })
}

fn tool_definitions() -> Vec<serde_json::Value> {
    vec![
        tool(
            "verlet_daemon_status",
            "Connect to the configured Verlet daemon socket and return basic status.",
            schema(vec![], vec![]),
            true,
        ),
        tool(
            "verlet_thread_start",
            "Start a supervised Verlet daemon thread.",
            schema(
                vec![
                    string_prop("cwd", "Optional daemon workspace directory."),
                    string_prop("model", "Optional manifest model-profile selector."),
                    string_prop(
                        "modelProvider",
                        "Optional manifest provider-profile selector.",
                    ),
                    bool_prop("ephemeral", "Whether the thread is ephemeral."),
                    string_prop(
                        "parentThreadId",
                        "Spawn the new thread from this existing Verlet thread id.",
                    ),
                    object_prop(
                        "topology",
                        "Canonical Verlet ThreadTopology object. Mutually exclusive with parentThreadId.",
                    ),
                ],
                vec![],
            ),
            false,
        ),
        tool(
            "verlet_thread_list",
            "List daemon-known Verlet threads.",
            schema(vec![], vec![]),
            true,
        ),
        tool(
            "verlet_thread_read",
            "Read a Verlet thread by id.",
            schema(
                vec![
                    string_prop("threadId", "Target Verlet thread id."),
                    bool_prop("includeTurns", "Include turn history in the response."),
                ],
                vec!["threadId"],
            ),
            true,
        ),
        tool(
            "verlet_turn_start",
            "Submit a user message to an existing Verlet thread.",
            schema(
                vec![
                    string_prop("threadId", "Target Verlet thread id."),
                    string_prop("message", "User message to submit."),
                    bool_prop("wait", "Wait for the turn to complete before returning."),
                    uint_prop("timeoutMs", "Optional wait timeout in milliseconds."),
                ],
                vec!["threadId", "message"],
            ),
            false,
        ),
        tool(
            "verlet_turn_wait",
            "Wait for a submitted Verlet turn to complete on this MCP connection.",
            schema(
                vec![
                    string_prop("threadId", "Target Verlet thread id."),
                    string_prop("turnId", "Target Verlet turn id."),
                    uint_prop("timeoutMs", "Optional wait timeout in milliseconds."),
                ],
                vec!["threadId", "turnId"],
            ),
            true,
        ),
        tool(
            "verlet_turn_interrupt",
            "Interrupt a running Verlet turn.",
            schema(
                vec![
                    string_prop("threadId", "Target Verlet thread id."),
                    string_prop("turnId", "Target Verlet turn id."),
                ],
                vec!["threadId", "turnId"],
            ),
            false,
        ),
        tool(
            "verlet_prompt",
            "Start a thread, submit one message, wait for completion, and return assistant text.",
            schema(
                vec![
                    string_prop("message", "User message to run."),
                    uint_prop("timeoutMs", "Optional wait timeout in milliseconds."),
                    object_prop(
                        "thread",
                        "Optional thread/start parameters, including profile selectors, topology, or parentThreadId.",
                    ),
                ],
                vec!["message"],
            ),
            false,
        ),
        tool(
            "verlet_command_exec",
            "Run a daemon command/exec request and return stdout, stderr, and exit code.",
            schema(
                vec![
                    array_string_prop("command", "Command argv to execute."),
                    string_prop("cwd", "Optional working directory."),
                    uint_prop("timeoutMs", "Optional timeout in milliseconds."),
                    uint_prop("outputBytesCap", "Optional stdout/stderr byte cap."),
                    object_prop("env", "Optional environment overrides."),
                ],
                vec!["command"],
            ),
            false,
        ),
        tool(
            "verlet_capsule_binding_set",
            "Bind a published capsule operation to a global, tenant, or thread scope.",
            schema(
                vec![
                    object_prop("scope", "Capsule binding scope."),
                    string_prop("operationName", "Published operation name to bind."),
                    string_prop(
                        "artifactHash",
                        "Optional published artifact hash. Defaults to the active operation record.",
                    ),
                ],
                vec!["scope", "operationName"],
            ),
            false,
        ),
        tool(
            "verlet_capsule_binding_delete",
            "Remove or tombstone a capsule operation binding for a global, tenant, or thread scope.",
            schema(
                vec![
                    object_prop("scope", "Capsule binding scope."),
                    string_prop("operationName", "Published operation name to unbind."),
                ],
                vec!["scope", "operationName"],
            ),
            false,
        ),
        tool(
            "verlet_capsule_binding_list",
            "List capsule operation bindings for a global, tenant, or thread scope.",
            schema(
                vec![object_prop("scope", "Capsule binding scope.")],
                vec!["scope"],
            ),
            true,
        ),
        tool(
            "verlet_capsule_binding_resolve",
            "Resolve capsule operation bindings for a tenant or thread.",
            schema(
                vec![
                    string_prop(
                        "tenantId",
                        "Optional tenant id. Defaults to the daemon tenant.",
                    ),
                    string_prop(
                        "threadId",
                        "Optional thread id for thread-scoped resolution.",
                    ),
                    array_string_prop(
                        "operationNames",
                        "Optional active operation names to include as a shortcut.",
                    ),
                    bool_prop(
                        "loadAllActiveWhenUnbound",
                        "Load active records when no durable binding exists.",
                    ),
                ],
                vec![],
            ),
            true,
        ),
    ]
}

fn tool(
    name: &str,
    description: &str,
    input_schema: serde_json::Value,
    read_only: bool,
) -> serde_json::Value {
    serde_json::json!({
        "name": name,
        "title": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "readOnlyHint": read_only,
            "destructiveHint": !read_only,
            "openWorldHint": true,
        },
    })
}

fn schema(
    properties: Vec<(&'static str, serde_json::Value)>,
    required: Vec<&'static str>,
) -> serde_json::Value {
    let mut props = serde_json::Map::new();
    for (key, value) in properties {
        props.insert(key.to_string(), value);
    }
    serde_json::json!({
        "type": "object",
        "properties": props,
        "required": required,
    })
}

fn string_prop(description: &'static str, text: &'static str) -> (&'static str, serde_json::Value) {
    (
        description,
        serde_json::json!({ "type": "string", "description": text }),
    )
}

fn bool_prop(description: &'static str, text: &'static str) -> (&'static str, serde_json::Value) {
    (
        description,
        serde_json::json!({ "type": "boolean", "description": text }),
    )
}

fn uint_prop(description: &'static str, text: &'static str) -> (&'static str, serde_json::Value) {
    (
        description,
        serde_json::json!({ "type": "integer", "minimum": 0, "description": text }),
    )
}

fn object_prop(description: &'static str, text: &'static str) -> (&'static str, serde_json::Value) {
    (
        description,
        serde_json::json!({ "type": "object", "description": text }),
    )
}

fn array_string_prop(
    description: &'static str,
    text: &'static str,
) -> (&'static str, serde_json::Value) {
    (
        description,
        serde_json::json!({
            "type": "array",
            "items": { "type": "string" },
            "description": text,
        }),
    )
}

fn mcp_io_error(err: std::io::Error) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!("MCP stdio I/O failed: {err}"))
}

#[cfg(test)]
mod tests;
