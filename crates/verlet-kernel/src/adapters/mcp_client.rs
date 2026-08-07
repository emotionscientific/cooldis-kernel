use crate::AgentKernelToolProvider as _;
use tokio::io::AsyncBufReadExt as _;
use tokio::io::AsyncWriteExt as _;

const DEFAULT_MCP_PROTOCOL_VERSION: &str = "2025-06-18";
const DEFAULT_REMOTE_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_REMOTE_MAX_OUTPUT_BYTES: u64 = 1_048_576;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpRemoteTransport {
    StreamableHttp,
    HttpSse,
}

impl McpRemoteTransport {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::StreamableHttp => "streamable_http",
            Self::HttpSse => "http_sse",
        }
    }

    pub fn from_str(value: &str) -> crate::VerletResult<Self> {
        match value {
            "streamable_http" | "mcp-http" | "http" => Ok(Self::StreamableHttp),
            "http_sse" | "mcp-sse" | "sse" => Ok(Self::HttpSse),
            other => Err(crate::VerletError::RuntimeFactory(format!(
                "unsupported remote MCP transport {other:?}"
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct McpRemoteServerConfig {
    pub name: String,
    pub transport: McpRemoteTransport,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_tools: Option<std::collections::BTreeSet<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_output_bytes: Option<u64>,
}

impl McpRemoteServerConfig {
    pub fn new(
        name: impl Into<String>,
        transport: McpRemoteTransport,
        url: impl Into<String>,
    ) -> crate::VerletResult<Self> {
        let name = crate::validate_record_name(&name.into())?;
        let url = url.into();
        validate_remote_mcp_url(&url)?;
        Ok(Self {
            name,
            transport,
            url,
            bearer_secret: None,
            headers: Vec::new(),
            include_tools: None,
            timeout_ms: None,
            max_output_bytes: None,
        })
    }

    pub fn with_bearer_secret(
        mut self,
        secret_name: impl Into<String>,
    ) -> crate::VerletResult<Self> {
        self.bearer_secret = Some(crate::validate_secret_name(&secret_name.into()).map_err(
            |err| {
                crate::VerletError::RuntimeFactory(format!(
                    "invalid remote MCP bearer secret: {err}"
                ))
            },
        )?);
        Ok(self)
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    pub fn with_include_tools(
        mut self,
        tools: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.include_tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_timeout_ms(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = Some(timeout_ms);
        self
    }

    pub fn with_max_output_bytes(mut self, max_output_bytes: u64) -> Self {
        self.max_output_bytes = Some(max_output_bytes);
        self
    }

    fn timeout(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.timeout_ms.unwrap_or(DEFAULT_REMOTE_TIMEOUT_MS))
    }

    fn max_output_bytes(&self) -> usize {
        self.max_output_bytes
            .unwrap_or(DEFAULT_REMOTE_MAX_OUTPUT_BYTES)
            .try_into()
            .unwrap_or(usize::MAX)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct McpRemoteSourceRecord {
    pub schema_version: u32,
    pub name: String,
    pub transport: McpRemoteTransport,
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bearer_secret: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub headers: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_tools: Option<std::collections::BTreeSet<String>>,
    pub timeout_ms: u64,
    pub max_output_bytes: u64,
    #[serde(default)]
    pub discovered_tools: Vec<crate::ToolDefinition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovered_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl McpRemoteSourceRecord {
    pub fn from_config(config: McpRemoteServerConfig, now_ms: i64) -> Self {
        Self {
            schema_version: 0,
            name: config.name,
            transport: config.transport,
            url: config.url,
            bearer_secret: config.bearer_secret,
            headers: config.headers,
            include_tools: config.include_tools,
            timeout_ms: config.timeout_ms.unwrap_or(DEFAULT_REMOTE_TIMEOUT_MS),
            max_output_bytes: config
                .max_output_bytes
                .unwrap_or(DEFAULT_REMOTE_MAX_OUTPUT_BYTES),
            discovered_tools: Vec::new(),
            discovered_at_ms: None,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
        }
    }

    pub fn to_config(&self) -> McpRemoteServerConfig {
        McpRemoteServerConfig {
            name: self.name.clone(),
            transport: self.transport.clone(),
            url: self.url.clone(),
            bearer_secret: self.bearer_secret.clone(),
            headers: self.headers.clone(),
            include_tools: self.include_tools.clone(),
            timeout_ms: Some(self.timeout_ms),
            max_output_bytes: Some(self.max_output_bytes),
        }
    }

    pub fn redacted_json(&self) -> serde_json::Value {
        serde_json::json!({
            "schema_version": self.schema_version,
            "name": self.name,
            "transport": self.transport.as_str(),
            "url": self.url,
            "auth": self.bearer_secret.as_ref().map(|name| serde_json::json!({
                "type": "bearer_secret",
                "secret": name,
                "value": {"redacted": true}
            })),
            "headers": self.headers.iter().map(|(name, _)| serde_json::json!({
                "name": name,
                "value": {"redacted": true}
            })).collect::<Vec<_>>(),
            "include_tools": self.include_tools,
            "timeout_ms": self.timeout_ms,
            "max_output_bytes": self.max_output_bytes,
            "discovered_tools": self.discovered_tools,
            "discovered_at_ms": self.discovered_at_ms,
            "created_at_ms": self.created_at_ms,
            "updated_at_ms": self.updated_at_ms,
        })
    }
}

#[derive(Clone)]
pub struct SqliteMcpSourceRegistry {
    inner: verlet_sqlite::Db,
}

impl SqliteMcpSourceRegistry {
    pub fn open(path: impl AsRef<std::path::Path>) -> crate::VerletResult<Self> {
        verlet_sqlite::block_on(Self::open_async(path))
    }

    pub async fn open_async(path: impl AsRef<std::path::Path>) -> crate::VerletResult<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent()
            && !parent.exists()
        {
            std::fs::create_dir_all(parent).map_err(|err| {
                crate::VerletError::RuntimeFactory(format!(
                    "failed to create MCP registry directory {}: {err}",
                    parent.display()
                ))
            })?;
        }
        let inner = verlet_sqlite::Db::open(path, verlet_sqlite::DbConfig::default())
            .await
            .map_err(sqlite_mcp_error)?;
        Self::from_db(inner).await
    }

    pub fn in_memory() -> crate::VerletResult<Self> {
        verlet_sqlite::block_on(Self::in_memory_async())
    }

    pub async fn in_memory_async() -> crate::VerletResult<Self> {
        let inner = verlet_sqlite::Db::in_memory(verlet_sqlite::DbConfig::default())
            .await
            .map_err(sqlite_mcp_error)?;
        Self::from_db(inner).await
    }

    async fn from_db(inner: verlet_sqlite::Db) -> crate::VerletResult<Self> {
        let registry = Self { inner };
        let connection = registry.connect().await?;
        init_mcp_source_schema(&connection).await?;
        Ok(registry)
    }

    async fn connect(&self) -> crate::VerletResult<verlet_sqlite::Connection> {
        self.inner.connect().await.map_err(sqlite_mcp_error)
    }

    pub fn upsert_source(
        &self,
        config: McpRemoteServerConfig,
    ) -> crate::VerletResult<McpRemoteSourceRecord> {
        verlet_sqlite::block_on(self.upsert_source_async(config))
    }

    pub async fn upsert_source_async(
        &self,
        config: McpRemoteServerConfig,
    ) -> crate::VerletResult<McpRemoteSourceRecord> {
        let mut connection = self.connect().await?;
        let tx = connection
            .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
            .await
            .map_err(sqlite_mcp_error)?;
        let existing = sqlite_get_mcp_source(&tx, &config.name).await?;
        let now = crate::kernel::history::now_ms();
        let mut record = McpRemoteSourceRecord::from_config(config, now);
        if let Some(existing) = existing {
            record.created_at_ms = existing.created_at_ms;
            record.discovered_tools = existing.discovered_tools;
            record.discovered_at_ms = existing.discovered_at_ms;
        }
        sqlite_put_mcp_source(&tx, &record).await?;
        tx.commit().await.map_err(sqlite_mcp_error)?;
        Ok(record)
    }

    pub fn get_source(
        &self,
        name: impl AsRef<str>,
    ) -> crate::VerletResult<Option<McpRemoteSourceRecord>> {
        verlet_sqlite::block_on(self.get_source_async(name))
    }

    pub async fn get_source_async(
        &self,
        name: impl AsRef<str>,
    ) -> crate::VerletResult<Option<McpRemoteSourceRecord>> {
        let name = crate::validate_record_name(name.as_ref())?;
        let connection = self.connect().await?;
        sqlite_get_mcp_source(&connection, &name).await
    }

    pub fn list_sources(&self) -> crate::VerletResult<Vec<McpRemoteSourceRecord>> {
        verlet_sqlite::block_on(self.list_sources_async())
    }

    pub async fn list_sources_async(&self) -> crate::VerletResult<Vec<McpRemoteSourceRecord>> {
        let connection = self.connect().await?;
        let mut rows = connection
            .query(
                "SELECT record_json FROM cooldis_mcp_source_records ORDER BY name",
                (),
            )
            .await
            .map_err(sqlite_mcp_error)?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await.map_err(sqlite_mcp_error)? {
            let json: String = row.get(0).map_err(sqlite_mcp_error)?;
            records.push(serde_json::from_str(&json).map_err(sqlite_mcp_error)?);
        }
        Ok(records)
    }

    pub fn delete_source(&self, name: impl AsRef<str>) -> crate::VerletResult<bool> {
        verlet_sqlite::block_on(self.delete_source_async(name))
    }

    pub async fn delete_source_async(&self, name: impl AsRef<str>) -> crate::VerletResult<bool> {
        let name = crate::validate_record_name(name.as_ref())?;
        let connection = self.connect().await?;
        let deleted = connection
            .execute(
                "DELETE FROM cooldis_mcp_source_records WHERE name = ?1",
                verlet_sqlite::params![name],
            )
            .await
            .map_err(sqlite_mcp_error)?;
        Ok(deleted > 0)
    }

    pub fn update_discovered_tools(
        &self,
        name: impl AsRef<str>,
        tools: Vec<crate::ToolDefinition>,
    ) -> crate::VerletResult<McpRemoteSourceRecord> {
        verlet_sqlite::block_on(self.update_discovered_tools_async(name, tools))
    }

    pub async fn update_discovered_tools_async(
        &self,
        name: impl AsRef<str>,
        tools: Vec<crate::ToolDefinition>,
    ) -> crate::VerletResult<McpRemoteSourceRecord> {
        let name = crate::validate_record_name(name.as_ref())?;
        let mut connection = self.connect().await?;
        let tx = connection
            .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
            .await
            .map_err(sqlite_mcp_error)?;
        let mut record = sqlite_get_mcp_source(&tx, &name).await?.ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!("remote MCP source {name:?} was not found"))
        })?;
        let now = crate::kernel::history::now_ms();
        record.discovered_tools = tools;
        record.discovered_at_ms = Some(now);
        record.updated_at_ms = now;
        sqlite_put_mcp_source(&tx, &record).await?;
        tx.commit().await.map_err(sqlite_mcp_error)?;
        Ok(record)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McpStdioServerConfig {
    pub name: String,
    pub command: String,
    pub args: Vec<String>,
    pub cwd: Option<std::path::PathBuf>,
    pub include_tools: Option<std::collections::BTreeSet<String>>,
}

impl McpStdioServerConfig {
    pub fn new(name: impl Into<String>, command: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            command: command.into(),
            args: Vec::new(),
            cwd: None,
            include_tools: None,
        }
    }

    pub fn with_args(mut self, args: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.args = args.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_cwd(mut self, cwd: impl Into<std::path::PathBuf>) -> Self {
        self.cwd = Some(cwd.into());
        self
    }

    pub fn with_include_tools(
        mut self,
        tools: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.include_tools = Some(tools.into_iter().map(Into::into).collect());
        self
    }
}

pub struct McpStdioToolProvider {
    client: std::sync::Arc<tokio::sync::Mutex<McpStdioClient>>,
    tools: Vec<crate::ToolDefinition>,
}

impl McpStdioToolProvider {
    pub async fn connect(config: McpStdioServerConfig) -> crate::VerletResult<Self> {
        let mut client = McpStdioClient::spawn(config.clone()).await?;
        client.initialize().await?;
        let tools = client.list_tools().await?;
        let tools = filter_tools(tools, config.include_tools.as_ref());
        Ok(Self {
            client: std::sync::Arc::new(tokio::sync::Mutex::new(client)),
            tools,
        })
    }
}

#[async_trait::async_trait]
impl crate::AgentKernelToolProvider for McpStdioToolProvider {
    async fn tool_definitions(&self) -> Vec<crate::ToolDefinition> {
        self.tools.clone()
    }

    async fn invoke_tool_call(
        &self,
        call: crate::AgentKernelToolCall,
    ) -> crate::VerletResult<Option<crate::CanonicalMessage>> {
        if !self.tools.iter().any(|tool| tool.name == call.tool_name) {
            return Ok(None);
        }
        let result = self
            .client
            .lock()
            .await
            .call_tool(&call.tool_name, call.arguments)
            .await?;
        Ok(Some(crate::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            result.content,
            result.is_error,
        )))
    }
}

pub struct McpRemoteToolProvider {
    client: std::sync::Arc<tokio::sync::Mutex<McpRemoteClient>>,
    tools: Vec<crate::ToolDefinition>,
}

impl McpRemoteToolProvider {
    pub async fn connect(
        config: McpRemoteServerConfig,
        secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
    ) -> crate::VerletResult<Self> {
        let mut client = McpRemoteClient::new(config.clone(), secret_resolver)?;
        client.initialize().await?;
        let tools = client.list_tools().await?;
        let tools = filter_tools(tools, config.include_tools.as_ref());
        Ok(Self {
            client: std::sync::Arc::new(tokio::sync::Mutex::new(client)),
            tools,
        })
    }
}

#[derive(Clone)]
pub struct McpToolUniverseDiscoverer {
    registry: SqliteMcpSourceRegistry,
    secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
}

impl McpToolUniverseDiscoverer {
    pub fn new(
        registry: SqliteMcpSourceRegistry,
        secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
    ) -> Self {
        Self {
            registry,
            secret_resolver,
        }
    }

    pub async fn caller_for(
        &self,
        server_ref: &str,
    ) -> crate::VerletResult<std::sync::Arc<McpRemoteToolProvider>> {
        let record = self.source_record(server_ref).await?;
        Ok(std::sync::Arc::new(
            McpRemoteToolProvider::connect(record.to_config(), self.secret_resolver.clone())
                .await?,
        ))
    }

    async fn source_record(&self, server_ref: &str) -> crate::VerletResult<McpRemoteSourceRecord> {
        let name = source_name_from_server_ref(server_ref)?;
        self.registry.get_source_async(&name).await?.ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!(
                "remote MCP source {name:?} was not found for server_ref {server_ref:?}"
            ))
        })
    }
}

#[async_trait::async_trait]
impl crate::ToolUniverseDiscoverer for McpToolUniverseDiscoverer {
    async fn discover(
        &self,
        server_ref: &str,
    ) -> crate::VerletResult<crate::ToolUniverseDiscovery> {
        let caller = self.caller_for(server_ref).await?;
        let mut tools = Vec::new();
        for definition in caller.tool_definitions().await {
            tools.push(crate::WitnessedToolContract::witness(&definition)?);
        }
        crate::ToolUniverseDiscovery::witness(server_ref, tools, crate::kernel::history::now_ms())
    }
}

#[async_trait::async_trait]
impl crate::AgentKernelToolProvider for McpRemoteToolProvider {
    async fn tool_definitions(&self) -> Vec<crate::ToolDefinition> {
        self.tools.clone()
    }

    async fn invoke_tool_call(
        &self,
        call: crate::AgentKernelToolCall,
    ) -> crate::VerletResult<Option<crate::CanonicalMessage>> {
        if !self.tools.iter().any(|tool| tool.name == call.tool_name) {
            return Ok(None);
        }
        let result = self
            .client
            .lock()
            .await
            .call_tool(&call.tool_name, call.arguments)
            .await?;
        Ok(Some(crate::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            result.content,
            result.is_error,
        )))
    }
}

#[async_trait::async_trait]
impl crate::ToolUniverseCaller for McpRemoteToolProvider {
    async fn call_tool(
        &self,
        tool_name: &str,
        arguments: serde_json::Value,
    ) -> crate::VerletResult<crate::ToolUniverseCallOutput> {
        if !self.tools.iter().any(|tool| tool.name == tool_name) {
            return Err(crate::VerletError::RuntimeExecution(format!(
                "remote MCP provider does not expose tool {tool_name:?}"
            )));
        }
        let result = self
            .client
            .lock()
            .await
            .call_tool(tool_name, arguments)
            .await?;
        Ok(crate::ToolUniverseCallOutput {
            content: result.content,
            is_error: result.is_error,
        })
    }
}

fn filter_tools(
    tools: Vec<crate::ToolDefinition>,
    include_tools: Option<&std::collections::BTreeSet<String>>,
) -> Vec<crate::ToolDefinition> {
    match include_tools {
        Some(include_tools) => tools
            .into_iter()
            .filter(|tool| include_tools.contains(&tool.name))
            .collect(),
        None => tools,
    }
}

fn source_name_from_server_ref(server_ref: &str) -> crate::VerletResult<String> {
    let name = server_ref.strip_prefix("mcp://").ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "server_ref {server_ref:?} must start with mcp://"
        ))
    })?;
    Ok(crate::validate_record_name(name)?)
}

struct McpToolCallResult {
    content: String,
    is_error: bool,
}

struct McpRemoteClient {
    config: McpRemoteServerConfig,
    http: reqwest::Client,
    secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
    next_id: u64,
}

impl McpRemoteClient {
    fn new(
        config: McpRemoteServerConfig,
        secret_resolver: Option<std::sync::Arc<dyn crate::SecretResolver>>,
    ) -> crate::VerletResult<Self> {
        validate_remote_mcp_url(&config.url)?;
        let http = reqwest::Client::builder()
            .timeout(config.timeout())
            .build()
            .map_err(|err| {
                crate::VerletError::RuntimeFactory(format!(
                    "failed to construct remote MCP HTTP client: {err}"
                ))
            })?;
        Ok(Self {
            config,
            http,
            secret_resolver,
            next_id: 1,
        })
    }

    async fn initialize(&mut self) -> crate::VerletResult<()> {
        self.request(
            "initialize",
            serde_json::json!({
                "protocolVersion": DEFAULT_MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "verlet-mcp-client",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )
        .await?;
        self.notify("notifications/initialized", serde_json::json!({}))
            .await
    }

    async fn list_tools(&mut self) -> crate::VerletResult<Vec<crate::ToolDefinition>> {
        let result = self.request("tools/list", serde_json::json!({})).await?;
        tools_from_mcp_result(&self.config.name, &result)
    }

    async fn call_tool(
        &mut self,
        name: &str,
        arguments: serde_json::Value,
    ) -> crate::VerletResult<McpToolCallResult> {
        let result = self
            .request(
                "tools/call",
                serde_json::json!({
                    "name": name,
                    "arguments": arguments
                }),
            )
            .await?;
        Ok(McpToolCallResult {
            content: mcp_content_text(&result),
            is_error: result
                .get("isError")
                .or_else(|| result.get("is_error"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        })
    }

    async fn notify(&mut self, method: &str, params: serde_json::Value) -> crate::VerletResult<()> {
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });
        let _ = self.post_message(message).await?;
        Ok(())
    }

    async fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> crate::VerletResult<serde_json::Value> {
        let id = self.next_id;
        self.next_id += 1;
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        let responses = self.post_message(message).await?;
        for value in responses {
            if value.get("id").and_then(serde_json::Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(crate::VerletError::RuntimeExecution(format!(
                    "remote MCP source `{}` rejected `{method}`: {error}",
                    self.config.name
                )));
            }
            return Ok(value
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }
        Err(crate::VerletError::RuntimeExecution(format!(
            "remote MCP source `{}` returned no response for request id {id}",
            self.config.name
        )))
    }

    async fn post_message(
        &self,
        message: serde_json::Value,
    ) -> crate::VerletResult<Vec<serde_json::Value>> {
        let mut request = self
            .http
            .post(&self.config.url)
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("mcp-protocol-version", DEFAULT_MCP_PROTOCOL_VERSION)
            .json(&message);
        for (name, value) in &self.config.headers {
            request = request.header(name, value);
        }
        if let Some(secret_name) = &self.config.bearer_secret {
            let resolver = self.secret_resolver.as_ref().ok_or_else(|| {
                crate::VerletError::RuntimeExecution(format!(
                    "remote MCP source `{}` requires bearer secret but no secret resolver is configured",
                    self.config.name
                ))
            })?;
            let secret = resolver
                .resolve_secret(secret_name)
                .await
                .map_err(|err| {
                    crate::VerletError::RuntimeExecution(format!(
                        "remote MCP source `{}` failed to resolve bearer secret: {err}",
                        self.config.name
                    ))
                })?
                .ok_or_else(|| {
                    crate::VerletError::RuntimeExecution(format!(
                        "remote MCP source `{}` requires a bearer secret that is not available",
                        self.config.name
                    ))
                })?;
            request = request.bearer_auth(secret.value);
        }
        let response = request.send().await.map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "remote MCP source `{}` request failed: {}",
                self.config.name,
                sanitize_http_error(err)
            ))
        })?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_string();
        let bytes = response.bytes().await.map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "remote MCP source `{}` response read failed: {}",
                self.config.name,
                sanitize_http_error(err)
            ))
        })?;
        if bytes.len() > self.config.max_output_bytes() {
            return Err(crate::VerletError::RuntimeExecution(format!(
                "remote MCP source `{}` response exceeded max_output_bytes",
                self.config.name
            )));
        }
        if !status.is_success() {
            return Err(crate::VerletError::RuntimeExecution(format!(
                "remote MCP source `{}` returned HTTP status {}",
                self.config.name, status
            )));
        }
        parse_mcp_http_response(&self.config.name, &content_type, &bytes)
    }
}

struct McpStdioClient {
    server_name: String,
    child: tokio::process::Child,
    stdin: tokio::process::ChildStdin,
    stdout: tokio::io::Lines<tokio::io::BufReader<tokio::process::ChildStdout>>,
    next_id: u64,
}

impl McpStdioClient {
    async fn spawn(config: McpStdioServerConfig) -> crate::VerletResult<Self> {
        let mut command = tokio::process::Command::new(&config.command);
        command.args(&config.args);
        if let Some(cwd) = &config.cwd {
            command.current_dir(cwd);
        }
        command
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit());
        let mut child = command.spawn().map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to launch MCP stdio server `{}`: {err}",
                config.name
            ))
        })?;
        let stdin = child.stdin.take().ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!(
                "MCP stdio server `{}` did not expose stdin",
                config.name
            ))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!(
                "MCP stdio server `{}` did not expose stdout",
                config.name
            ))
        })?;
        Ok(Self {
            server_name: config.name,
            child,
            stdin,
            stdout: tokio::io::BufReader::new(stdout).lines(),
            next_id: 1,
        })
    }

    async fn initialize(&mut self) -> crate::VerletResult<()> {
        self.request(
            "initialize",
            serde_json::json!({
                "protocolVersion": DEFAULT_MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {
                    "name": "verlet-mcp-client",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )
        .await?;
        self.notify("notifications/initialized", serde_json::json!({}))
            .await
    }

    async fn list_tools(&mut self) -> crate::VerletResult<Vec<crate::ToolDefinition>> {
        let result = self.request("tools/list", serde_json::json!({})).await?;
        tools_from_mcp_result(&self.server_name, &result)
    }

    async fn call_tool(
        &mut self,
        name: &str,
        arguments: serde_json::Value,
    ) -> crate::VerletResult<McpToolCallResult> {
        let result = self
            .request(
                "tools/call",
                serde_json::json!({
                    "name": name,
                    "arguments": arguments
                }),
            )
            .await?;
        Ok(McpToolCallResult {
            content: mcp_content_text(&result),
            is_error: result
                .get("isError")
                .or_else(|| result.get("is_error"))
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        })
    }

    async fn notify(&mut self, method: &str, params: serde_json::Value) -> crate::VerletResult<()> {
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params
        });
        self.write_message(&message).await
    }

    async fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> crate::VerletResult<serde_json::Value> {
        let id = self.next_id;
        self.next_id += 1;
        let message = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": method,
            "params": params
        });
        self.write_message(&message).await?;
        for _ in 0..64 {
            let line = self.stdout.next_line().await.map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "failed to read MCP response from `{}`: {err}",
                    self.server_name
                ))
            })?;
            let Some(line) = line else {
                return Err(crate::VerletError::RuntimeExecution(format!(
                    "MCP server `{}` closed stdout while waiting for `{method}`",
                    self.server_name
                )));
            };
            if line.trim().is_empty() {
                continue;
            }
            let value: serde_json::Value = serde_json::from_str(&line).map_err(|err| {
                crate::VerletError::RuntimeExecution(format!(
                    "MCP server `{}` emitted invalid JSON: {err}: {line}",
                    self.server_name
                ))
            })?;
            if value.get("id").and_then(serde_json::Value::as_u64) != Some(id) {
                continue;
            }
            if let Some(error) = value.get("error") {
                return Err(crate::VerletError::RuntimeExecution(format!(
                    "MCP server `{}` rejected `{method}`: {error}",
                    self.server_name
                )));
            }
            return Ok(value
                .get("result")
                .cloned()
                .unwrap_or(serde_json::Value::Null));
        }
        Err(crate::VerletError::RuntimeExecution(format!(
            "timed out waiting for MCP response id {id} from `{}`",
            self.server_name
        )))
    }

    async fn write_message(&mut self, message: &serde_json::Value) -> crate::VerletResult<()> {
        let encoded = serde_json::to_vec(message).map_err(|err| {
            crate::VerletError::RuntimeExecution(format!("failed to encode MCP request: {err}"))
        })?;
        self.stdin.write_all(&encoded).await.map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "failed to write MCP request to `{}`: {err}",
                self.server_name
            ))
        })?;
        self.stdin.write_all(b"\n").await.map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "failed to write MCP request newline to `{}`: {err}",
                self.server_name
            ))
        })?;
        self.stdin.flush().await.map_err(|err| {
            crate::VerletError::RuntimeExecution(format!(
                "failed to flush MCP request to `{}`: {err}",
                self.server_name
            ))
        })
    }
}

impl Drop for McpStdioClient {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

fn mcp_content_text(result: &serde_json::Value) -> String {
    result
        .get("content")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(
                    |item| match item.get("type").and_then(serde_json::Value::as_str) {
                        Some("text") => item
                            .get("text")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string),
                        _ => Some(item.to_string()),
                    },
                )
                .collect::<Vec<_>>()
                .join("\n")
        })
        .filter(|text| !text.is_empty())
        .or_else(|| {
            result
                .get("structuredContent")
                .map(serde_json::Value::to_string)
        })
        .or_else(|| {
            result
                .get("structured_content")
                .map(serde_json::Value::to_string)
        })
        .unwrap_or_else(|| result.to_string())
}

fn tools_from_mcp_result(
    server_name: &str,
    result: &serde_json::Value,
) -> crate::VerletResult<Vec<crate::ToolDefinition>> {
    let tools = result
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            crate::VerletError::RuntimeExecution(format!(
                "MCP server `{server_name}` returned tools/list without tools array"
            ))
        })?;
    let mut definitions = Vec::new();
    for tool in tools {
        let name = tool
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                crate::VerletError::RuntimeExecution(format!(
                    "MCP server `{server_name}` returned a tool without a name"
                ))
            })?
            .to_string();
        let description = tool
            .get("description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("Imported MCP tool.")
            .to_string();
        let input_schema = tool
            .get("inputSchema")
            .or_else(|| tool.get("input_schema"))
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"type":"object", "additionalProperties": true}));
        definitions.push(crate::ToolDefinition::new(name, description, input_schema));
    }
    Ok(definitions)
}

fn parse_mcp_http_response(
    server_name: &str,
    content_type: &str,
    bytes: &[u8],
) -> crate::VerletResult<Vec<serde_json::Value>> {
    let text = std::str::from_utf8(bytes).map_err(|err| {
        crate::VerletError::RuntimeExecution(format!(
            "remote MCP source `{server_name}` returned non-UTF8 response: {err}"
        ))
    })?;
    if content_type.contains("text/event-stream") || looks_like_sse(text) {
        return parse_mcp_sse_values(server_name, text);
    }
    let value: serde_json::Value = serde_json::from_str(text).map_err(|err| {
        crate::VerletError::RuntimeExecution(format!(
            "remote MCP source `{server_name}` returned invalid JSON: {err}: {text}"
        ))
    })?;
    Ok(vec![value])
}

fn looks_like_sse(text: &str) -> bool {
    text.lines().any(|line| {
        let trimmed = line.trim_start();
        trimmed.starts_with("event:") || trimmed.starts_with("data:")
    })
}

fn parse_mcp_sse_values(
    server_name: &str,
    text: &str,
) -> crate::VerletResult<Vec<serde_json::Value>> {
    let mut values = Vec::new();
    let mut data_lines = Vec::new();
    for line in text.lines().chain([""]) {
        let trimmed = line.trim_end();
        if trimmed.is_empty() {
            if !data_lines.is_empty() {
                let data = data_lines.join("\n");
                if data.trim() != "[DONE]" {
                    let value: serde_json::Value = serde_json::from_str(&data).map_err(|err| {
                        crate::VerletError::RuntimeExecution(format!(
                            "remote MCP source `{server_name}` returned invalid SSE JSON: {err}: {data}"
                        ))
                    })?;
                    values.push(value);
                }
                data_lines.clear();
            }
            continue;
        }
        if let Some(data) = trimmed.strip_prefix("data:") {
            data_lines.push(data.trim_start().to_string());
        }
    }
    if values.is_empty() {
        return Err(crate::VerletError::RuntimeExecution(format!(
            "remote MCP source `{server_name}` returned empty SSE response"
        )));
    }
    Ok(values)
}

fn validate_remote_mcp_url(url: &str) -> crate::VerletResult<()> {
    let url = reqwest::Url::parse(url).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!("remote MCP URL {url:?} is invalid: {err}"))
    })?;
    match url.scheme() {
        "http" | "https" => Ok(()),
        other => Err(crate::VerletError::RuntimeFactory(format!(
            "remote MCP URL scheme {other:?} is not supported"
        ))),
    }
}

fn sanitize_http_error(err: reqwest::Error) -> String {
    if err.is_timeout() {
        return "request timed out".to_string();
    }
    err.without_url().to_string()
}

async fn init_mcp_source_schema(connection: &verlet_sqlite::Connection) -> crate::VerletResult<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cooldis_mcp_source_records (
                name TEXT PRIMARY KEY NOT NULL,
                transport TEXT NOT NULL,
                url TEXT NOT NULL,
                record_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL
            );
            "#,
        )
        .await
        .map_err(sqlite_mcp_error)
}

async fn sqlite_get_mcp_source(
    connection: &verlet_sqlite::Connection,
    name: &str,
) -> crate::VerletResult<Option<McpRemoteSourceRecord>> {
    let mut rows = connection
        .query(
            "SELECT record_json FROM cooldis_mcp_source_records WHERE name = ?1",
            verlet_sqlite::params![name],
        )
        .await
        .map_err(sqlite_mcp_error)?;
    let Some(row) = rows.next().await.map_err(sqlite_mcp_error)? else {
        return Ok(None);
    };
    let json: String = row.get(0).map_err(sqlite_mcp_error)?;
    serde_json::from_str(&json)
        .map(Some)
        .map_err(sqlite_mcp_error)
}

async fn sqlite_put_mcp_source(
    connection: &verlet_sqlite::Connection,
    record: &McpRemoteSourceRecord,
) -> crate::VerletResult<()> {
    let record_json = serde_json::to_string(record).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to encode remote MCP source record: {err}"
        ))
    })?;
    connection
        .execute(
            r#"
            INSERT INTO cooldis_mcp_source_records (
                name,
                transport,
                url,
                record_json,
                updated_at_ms
            )
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(name) DO UPDATE SET
                transport = excluded.transport,
                url = excluded.url,
                record_json = excluded.record_json,
                updated_at_ms = excluded.updated_at_ms
            "#,
            verlet_sqlite::params![
                record.name.as_str(),
                record.transport.as_str(),
                record.url.as_str(),
                record_json,
                record.updated_at_ms
            ],
        )
        .await
        .map_err(sqlite_mcp_error)?;
    Ok(())
}

fn sqlite_mcp_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(format!("remote MCP registry failed: {err}"))
}

#[cfg(test)]
mod tests;
