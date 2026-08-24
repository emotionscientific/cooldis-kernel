use futures_util::SinkExt as _;
use futures_util::StreamExt as _;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

pub const OPERATOR_CLIENT_UDS_WEBSOCKET_HANDSHAKE_URL: &str = "ws://localhost/rpc";
pub const OPERATOR_CLIENT_NAME: &str = "verlet-codex-tui-test";

const OPERATOR_CLIENT_MAX_WEBSOCKET_MESSAGE_SIZE: usize = 128 << 20;
const OPERATOR_CLIENT_INITIALIZE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Verlet-owned operator client, trimmed to a headless driver for app-server
/// tests.
#[derive(Clone)]
pub struct OperatorConnectConfig {
    pub client_name: String,
    pub client_version: String,
    pub experimental_api: bool,
    pub opt_out_notification_methods: Vec<String>,
    pub bearer_token: Option<String>,
}

impl Default for OperatorConnectConfig {
    fn default() -> Self {
        // MCP, ACP, debug RPC, and other daemon clients inherit this config.
        // Managed callers provide their boundary credential via
        // VERLET_APP_SERVER_TOKEN; an explicit field value may override it.
        let bearer_token = std::env::var("VERLET_APP_SERVER_TOKEN")
            .ok()
            .filter(|token| !token.trim().is_empty());
        Self {
            client_name: OPERATOR_CLIENT_NAME.to_string(),
            client_version: env!("CARGO_PKG_VERSION").to_string(),
            experimental_api: true,
            opt_out_notification_methods: Vec::new(),
            bearer_token,
        }
    }
}

impl std::fmt::Debug for OperatorConnectConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperatorConnectConfig")
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
pub struct OperatorThread {
    pub id: String,
    pub raw: serde_json::Value,
}

#[derive(Clone, Debug)]
pub struct OperatorTurn {
    pub id: String,
    pub raw: serde_json::Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OperatorModelAuthStatus {
    Configured,
    Env,
    Missing,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModel {
    pub provider_id: String,
    pub model: String,
    pub display_name: String,
    pub auth_status: OperatorModelAuthStatus,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelList {
    pub data: Vec<OperatorModel>,
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorActiveModel {
    pub provider_id: String,
    pub model: String,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
pub struct OperatorModelSelectResult {
    pub active: OperatorActiveModel,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelProviderAuth {
    pub provider_id: String,
    #[serde(default)]
    pub display_name: String,
    pub configured: bool,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelProviderAuthList {
    #[serde(default)]
    pub auth: Option<OperatorModelProviderAuth>,
    pub data: Vec<OperatorModelProviderAuth>,
    pub next_cursor: Option<String>,
}

#[derive(serde::Deserialize)]
struct OperatorModelProviderAuthResult {
    auth: OperatorModelProviderAuth,
}

/// A provider `api` value on the RPC wire: a known family string
/// (`open_ai_chat_completions`, ...) or `{ "other": ... }`.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum OperatorProviderApi {
    Family(String),
    Other { other: String },
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorCatalogProvider {
    pub provider_id: String,
    pub display_name: String,
    pub base_url: String,
    pub api: OperatorProviderApi,
    pub auth_kind: String,
    #[serde(default)]
    pub env_vars: Vec<String>,
    #[serde(default)]
    pub doc_url: Option<String>,
    pub model_count: usize,
    #[serde(default)]
    pub default_model: Option<String>,
    pub configured: bool,
    #[serde(default)]
    pub auth_source: Option<String>,
    #[serde(default)]
    pub auth_label: Option<String>,
    pub custom: bool,
    pub active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelProviderCatalog {
    pub providers: Vec<OperatorCatalogProvider>,
}

/// `modelProvider/upsert` params, mirroring the app-server's
/// `ModelProviderUpsertParams` wire shape.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelProviderUpsertParams {
    pub provider: OperatorModelProviderUpsertRecord,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelProviderUpsertRecord {
    pub provider_id: String,
    pub api: OperatorProviderApi,
    pub base_url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    pub auth: verlet_metadata::provider_store::LlmProviderAuthConfig,
    pub headers:
        std::collections::BTreeMap<String, verlet_metadata::provider_store::LlmProviderConfigValue>,
    pub auth_header: bool,
    pub models: Vec<OperatorModelProviderModelUpsertRecord>,
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperatorModelProviderModelUpsertRecord {
    pub model_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug)]
pub struct OperatorCompletedTurn {
    pub thread_id: String,
    pub turn_id: String,
    pub assistant_text: String,
    pub notifications: Vec<crate::adapters::app_server::connection::JsonRpcNotification>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum OperatorEvent {
    Notification(crate::adapters::app_server::connection::JsonRpcNotification),
    Request(crate::adapters::app_server::connection::JsonRpcRequest),
    Response(crate::adapters::app_server::connection::JsonRpcResponse),
    Error(crate::adapters::app_server::connection::JsonRpcError),
}

pub struct OperatorClient<S> {
    websocket: tokio_tungstenite::WebSocketStream<S>,
    next_request_id: i64,
    pending_events: std::collections::VecDeque<OperatorEvent>,
    initialize_result: serde_json::Value,
}

#[cfg(unix)]
impl OperatorClient<tokio::net::UnixStream> {
    pub async fn connect_unix(
        socket_path: impl Into<std::path::PathBuf>,
        config: OperatorConnectConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let socket_path = socket_path.into();
        let endpoint = format!("unix://{}", socket_path.display());
        let mut request = OPERATOR_CLIENT_UDS_WEBSOCKET_HANDSHAKE_URL
            .into_client_request()
            .map_err(|err| tui_error(format!("invalid Verlet RPC handshake URL: {err}")))?;
        set_bearer_token(&mut request, config.bearer_token.as_deref())?;
        let stream = tokio::net::UnixStream::connect(&socket_path)
            .await
            .map_err(|err| {
                tui_error(format!(
                    "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
                ))
            })?;
        let (websocket, _) = tokio_tungstenite::client_async_with_config(
            request,
            stream,
            Some(operator_client_websocket_config()),
        )
        .await
        .map_err(|err| {
            tui_error(format!(
                "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
            ))
        })?;
        Self::connect_with_websocket(websocket, endpoint, config).await
    }
}

impl OperatorClient<tokio::net::TcpStream> {
    pub async fn connect_websocket(
        url: &str,
        config: OperatorConnectConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let endpoint = url.to_string();
        let authority = websocket_tcp_authority(url)?;
        let mut request = url
            .into_client_request()
            .map_err(|err| tui_error(format!("invalid Verlet RPC URL `{endpoint}`: {err}")))?;
        set_bearer_token(&mut request, config.bearer_token.as_deref())?;
        let stream = tokio::net::TcpStream::connect(authority)
            .await
            .map_err(|err| {
                tui_error(format!(
                    "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
                ))
            })?;
        let (websocket, _) = tokio_tungstenite::client_async_with_config(
            request,
            stream,
            Some(operator_client_websocket_config()),
        )
        .await
        .map_err(|err| {
            tui_error(format!(
                "failed to connect to the Verlet RPC endpoint `{endpoint}`: {err}"
            ))
        })?;
        Self::connect_with_websocket(websocket, endpoint, config).await
    }
}

fn set_bearer_token(
    request: &mut tokio_tungstenite::tungstenite::http::Request<()>,
    token: Option<&str>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(token) = token else {
        return Ok(());
    };
    let value =
        tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&format!("Bearer {token}"))
            .map_err(|_| tui_error("Verlet app-server bearer token is not a valid HTTP header"))?;
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        value,
    );
    Ok(())
}

impl<S> OperatorClient<S>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    pub async fn connect_with_websocket(
        mut websocket: tokio_tungstenite::WebSocketStream<S>,
        endpoint: impl Into<String>,
        config: OperatorConnectConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
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

    pub fn initialize_result(&self) -> &serde_json::Value {
        &self.initialize_result
    }

    pub async fn account_read(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("account/read", serde_json::json!({ "includeToken": false }))
            .await
    }

    pub async fn model_list(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("model/list", serde_json::json!({})).await
    }

    pub async fn model_list_typed(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelList> {
        let value = self.model_list().await?;
        serde_json::from_value(value)
            .map_err(|err| tui_error(format!("invalid model/list response: {err}")))
    }

    /// `model/select` (EMO-558): switch the app-server's active
    /// provider+model for turns started after the call.
    pub async fn model_select(
        &mut self,
        provider_id: &str,
        model: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "model/select",
            serde_json::json!({
                "providerId": provider_id,
                "model": model,
            }),
        )
        .await
    }

    pub async fn model_select_typed(
        &mut self,
        provider_id: &str,
        model: &str,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelSelectResult> {
        let value = self.model_select(provider_id, model).await?;
        serde_json::from_value(value)
            .map_err(|err| tui_error(format!("invalid model/select response: {err}")))
    }

    pub async fn model_provider_auth_status(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("modelProvider/auth/status", serde_json::json!({}))
            .await
    }

    pub async fn model_provider_auth_status_for(
        &mut self,
        provider_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "modelProvider/auth/status",
            serde_json::json!({ "providerId": provider_id }),
        )
        .await
    }

    pub async fn model_provider_auth_status_typed(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelProviderAuthList> {
        let value = self.model_provider_auth_status().await?;
        serde_json::from_value(value)
            .map_err(|err| tui_error(format!("invalid modelProvider/auth/status response: {err}")))
    }

    pub async fn model_provider_catalog(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("modelProvider/catalog", serde_json::json!({}))
            .await
    }

    pub async fn model_provider_catalog_typed(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelProviderCatalog> {
        let value = self.model_provider_catalog().await?;
        serde_json::from_value(value)
            .map_err(|err| tui_error(format!("invalid modelProvider/catalog response: {err}")))
    }

    pub async fn model_provider_upsert(
        &mut self,
        params: &OperatorModelProviderUpsertParams,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        let params = serde_json::to_value(params)
            .map_err(|err| tui_error(format!("invalid modelProvider/upsert params: {err}")))?;
        self.request("modelProvider/upsert", params).await
    }

    pub async fn model_provider_delete(
        &mut self,
        provider_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "modelProvider/delete",
            serde_json::json!({ "providerId": provider_id }),
        )
        .await
    }

    pub async fn model_provider_auth_set(
        &mut self,
        provider_id: &str,
        api_key: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "modelProvider/auth/set",
            serde_json::json!({
                "providerId": provider_id,
                "apiKey": api_key,
            }),
        )
        .await
    }

    pub async fn model_provider_auth_set_typed(
        &mut self,
        provider_id: &str,
        api_key: &str,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelProviderAuth> {
        let value = self.model_provider_auth_set(provider_id, api_key).await?;
        serde_json::from_value::<OperatorModelProviderAuthResult>(value)
            .map(|result| result.auth)
            .map_err(|err| tui_error(format!("invalid modelProvider/auth/set response: {err}")))
    }

    pub async fn model_provider_auth_set_oauth(
        &mut self,
        provider_id: &str,
        access: &str,
        refresh: &str,
        expires_at_ms: i64,
        account_id: Option<&str>,
        email: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "modelProvider/auth/setOAuth",
            serde_json::json!({
                "providerId": provider_id,
                "access": access,
                "refresh": refresh,
                "expiresAtMs": expires_at_ms,
                "accountId": account_id,
                "email": email,
            }),
        )
        .await
    }

    pub async fn model_provider_auth_set_oauth_typed(
        &mut self,
        provider_id: &str,
        access: &str,
        refresh: &str,
        expires_at_ms: i64,
        account_id: Option<&str>,
        email: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelProviderAuth> {
        let value = self
            .model_provider_auth_set_oauth(
                provider_id,
                access,
                refresh,
                expires_at_ms,
                account_id,
                email,
            )
            .await?;
        serde_json::from_value::<OperatorModelProviderAuthResult>(value)
            .map(|result| result.auth)
            .map_err(|err| {
                tui_error(format!(
                    "invalid modelProvider/auth/setOAuth response: {err}"
                ))
            })
    }

    pub async fn model_provider_auth_delete(
        &mut self,
        provider_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "modelProvider/auth/delete",
            serde_json::json!({ "providerId": provider_id }),
        )
        .await
    }

    pub async fn model_provider_auth_delete_typed(
        &mut self,
        provider_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorModelProviderAuth> {
        let value = self.model_provider_auth_delete(provider_id).await?;
        serde_json::from_value::<OperatorModelProviderAuthResult>(value)
            .map(|result| result.auth)
            .map_err(|err| tui_error(format!("invalid modelProvider/auth/delete response: {err}")))
    }

    // `secret_set` only works over the Unix
    // socket transport; the server refuses it on any other surface.

    pub async fn secret_list(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("secret/list", serde_json::json!({})).await
    }

    pub async fn secret_status(
        &mut self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("secret/status", serde_json::json!({ "name": name }))
            .await
    }

    pub async fn secret_set(
        &mut self,
        name: &str,
        value: &str,
        source: verlet_metadata::secret_store::SecretSourceKind,
        source_name: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "secret/set",
            serde_json::json!({
                "name": name,
                "value": value,
                "source": source,
                "sourceName": source_name,
            }),
        )
        .await
    }

    pub async fn secret_delete(
        &mut self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("secret/delete", serde_json::json!({ "name": name }))
            .await
    }

    /// Resolve plaintext values for named secrets (`verlet tool run`
    /// injection). Unix-socket-only; the server refuses it on any other
    /// surface. Response: `{ "values": { name: value }, "missing": [names] }`.
    pub async fn secret_resolve(
        &mut self,
        names: &[String],
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("secret/resolve", serde_json::json!({ "names": names }))
            .await
    }

    // The acting identity principal is the connection's
    // authenticated principal; there are no declared-by/minted-by inputs.
    // `identity_mint` only works over the unix socket transport because the
    // response carries the bearer secret.

    pub async fn identity_list(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("identity/list", serde_json::json!({})).await
    }

    pub async fn identity_declare(
        &mut self,
        principal_id: &str,
        kind: crate::daemon::identity::PrincipalKind,
        display: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "identity/declare",
            serde_json::json!({
                "principalId": principal_id,
                "kind": kind,
                "display": display,
            }),
        )
        .await
    }

    pub async fn identity_mint(
        &mut self,
        principal_id: &str,
        expires_at_ms: Option<i64>,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "identity/mint",
            serde_json::json!({
                "principalId": principal_id,
                "expiresAtMs": expires_at_ms,
            }),
        )
        .await
    }

    /// Revoke a credential (`credential_id`) or a principal (`principal_id`);
    /// pass exactly one.
    pub async fn identity_revoke(
        &mut self,
        credential_id: Option<&str>,
        principal_id: Option<&str>,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "identity/revoke",
            serde_json::json!({
                "credentialId": credential_id,
                "principalId": principal_id,
            }),
        )
        .await
    }

    pub async fn mcp_source_list(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("mcpSource/list", serde_json::json!({})).await
    }

    pub async fn mcp_source_read(
        &mut self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("mcpSource/read", serde_json::json!({ "name": name }))
            .await
    }

    pub async fn mcp_source_upsert(
        &mut self,
        params: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("mcpSource/upsert", params).await
    }

    pub async fn mcp_source_discover(
        &mut self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("mcpSource/discover", serde_json::json!({ "name": name }))
            .await
    }

    pub async fn mcp_source_delete(
        &mut self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("mcpSource/delete", serde_json::json!({ "name": name }))
            .await
    }

    pub async fn config_read(
        &mut self,
        include_layers: bool,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "config/read",
            serde_json::json!({
                "includeLayers": include_layers,
            }),
        )
        .await
    }

    pub async fn thread_start(
        &mut self,
        params: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorThread> {
        let result = self.request("thread/start", params).await?;
        thread_from_result(result, "thread/start")
    }

    pub async fn thread_resume(
        &mut self,
        thread_id: &str,
        include_turns: bool,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorThread> {
        let result = self
            .request(
                "thread/resume",
                serde_json::json!({
                    "threadId": thread_id,
                    "excludeTurns": !include_turns,
                }),
            )
            .await?;
        thread_from_result(result, "thread/resume")
    }

    pub async fn thread_fork(
        &mut self,
        thread_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorThread> {
        let result = self
            .request(
                "thread/fork",
                serde_json::json!({
                    "threadId": thread_id,
                    "ephemeral": false,
                }),
            )
            .await?;
        thread_from_result(result, "thread/fork")
    }

    pub async fn thread_name_set(
        &mut self,
        thread_id: &str,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "thread/name/set",
            serde_json::json!({
                "threadId": thread_id,
                "name": name,
            }),
        )
        .await
    }

    pub async fn thread_compact_start(
        &mut self,
        thread_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "thread/compact/start",
            serde_json::json!({
                "threadId": thread_id,
            }),
        )
        .await
    }

    pub async fn thread_read(
        &mut self,
        thread_id: &str,
        include_turns: bool,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "thread/read",
            serde_json::json!({
                "threadId": thread_id,
                "includeTurns": include_turns,
            }),
        )
        .await
    }

    pub async fn thread_list(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("thread/list", serde_json::json!({})).await
    }

    pub async fn loaded_thread_list(
        &mut self,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request("thread/loaded/list", serde_json::json!({}))
            .await
    }

    pub async fn thread_unsubscribe(
        &mut self,
        thread_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "thread/unsubscribe",
            serde_json::json!({ "threadId": thread_id }),
        )
        .await
    }

    pub async fn turn_start_text(
        &mut self,
        thread_id: &str,
        text: &str,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorTurn> {
        let result = self
            .request(
                "turn/start",
                serde_json::json!({
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
        Ok(OperatorTurn { id, raw: turn })
    }

    pub async fn turn_steer_text(
        &mut self,
        thread_id: &str,
        expected_turn_id: &str,
        text: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "turn/steer",
            serde_json::json!({
                "threadId": thread_id,
                "expectedTurnId": expected_turn_id,
                "input": [codex_text_input(text)],
            }),
        )
        .await
    }

    pub async fn turn_interrupt(
        &mut self,
        thread_id: &str,
        turn_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.request(
            "turn/interrupt",
            serde_json::json!({
                "threadId": thread_id,
                "turnId": turn_id,
            }),
        )
        .await
    }

    pub async fn run_prompt(
        &mut self,
        prompt: &str,
        timeout: std::time::Duration,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorCompletedTurn> {
        let thread = self.thread_start(serde_json::json!({})).await?;
        let turn = self.turn_start_text(&thread.id, prompt).await?;
        self.wait_for_turn_completed(&thread.id, &turn.id, timeout)
            .await
    }

    pub async fn wait_for_turn_completed(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        timeout_duration: std::time::Duration,
    ) -> crate::kernel::runtime_host::VerletResult<OperatorCompletedTurn> {
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
                        OperatorEvent::Notification(notification) => {
                            if notification.method == "item/agentMessage/delta"
                                && notification.params.as_ref()
                                    .and_then(|params| params.get("threadId"))
                                    .and_then(serde_json::Value::as_str) == Some(thread_id)
                                && notification.params.as_ref()
                                    .and_then(|params| params.get("turnId"))
                                    .and_then(serde_json::Value::as_str) == Some(turn_id)
                                && let Some(delta) = notification.params.as_ref()
                                    .and_then(|params| params.get("delta"))
                                    .and_then(serde_json::Value::as_str)
                            {
                                assistant_text.push_str(delta);
                            }

                            let completed = notification.method == "turn/completed"
                                && notification.params.as_ref()
                                    .and_then(|params| params.get("turn"))
                                    .and_then(|turn| turn.get("id"))
                                    .and_then(serde_json::Value::as_str) == Some(turn_id);
                            notifications.push(notification);
                            if completed {
                                return Ok(OperatorCompletedTurn {
                                    thread_id: thread_id.to_string(),
                                    turn_id: turn_id.to_string(),
                                    assistant_text,
                                    notifications,
                                });
                            }
                        }
                        OperatorEvent::Error(error) => {
                            return Err(tui_error(format!(
                                "Verlet RPC client received JSON-RPC error {}: {}",
                                error.error.code,
                                error.error.message
                            )));
                        }
                        OperatorEvent::Request(_) | OperatorEvent::Response(_) => {}
                    }
                }
            }
        }
    }

    pub async fn request(
        &mut self,
        method: &str,
        params: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        let id = crate::adapters::app_server::connection::RequestId::Integer(self.next_request_id);
        self.next_request_id += 1;
        self.request_with_id(id, method, params).await
    }

    pub async fn request_with_id(
        &mut self,
        id: crate::adapters::app_server::connection::RequestId,
        method: &str,
        params: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        write_jsonrpc_message(
            &mut self.websocket,
            crate::adapters::app_server::connection::JsonRpcMessage::Request(
                crate::adapters::app_server::connection::JsonRpcRequest {
                    id: id.clone(),
                    method: method.to_string(),
                    params: Some(params),
                    trace: None,
                },
            ),
        )
        .await?;

        loop {
            match self.read_event().await? {
                OperatorEvent::Response(response) if response.id == id => {
                    return Ok(response.result);
                }
                OperatorEvent::Error(error) if error.id == id => {
                    return Err(tui_error(format!(
                        "request `{method}` was refused: {}",
                        error.error.message
                    )));
                }
                event => self.pending_events.push_back(event),
            }
        }
    }

    pub async fn notify(
        &mut self,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        write_jsonrpc_message(
            &mut self.websocket,
            crate::adapters::app_server::connection::JsonRpcMessage::Notification(
                crate::adapters::app_server::connection::JsonRpcNotification {
                    method: method.to_string(),
                    params,
                },
            ),
        )
        .await
    }

    pub async fn next_event(&mut self) -> crate::kernel::runtime_host::VerletResult<OperatorEvent> {
        if let Some(event) = self.pending_events.pop_front() {
            return Ok(event);
        }
        self.read_event().await
    }

    async fn read_event(&mut self) -> crate::kernel::runtime_host::VerletResult<OperatorEvent> {
        loop {
            let message = self
                .websocket
                .next()
                .await
                .ok_or_else(|| tui_error("Verlet RPC connection closed"))?
                .map_err(|err| tui_error(format!("Verlet RPC connection read failed: {err}")))?;
            match message {
                tokio_tungstenite::tungstenite::Message::Text(text) => {
                    return jsonrpc_event_from_text(&text);
                }
                tokio_tungstenite::tungstenite::Message::Close(frame) => {
                    let reason = frame
                        .as_ref()
                        .map(|frame| frame.reason.to_string())
                        .filter(|reason| !reason.is_empty())
                        .unwrap_or_else(|| "connection closed".to_string());
                    return Err(tui_error(format!(
                        "Verlet RPC connection was closed by the endpoint: {reason}"
                    )));
                }
                tokio_tungstenite::tungstenite::Message::Binary(_)
                | tokio_tungstenite::tungstenite::Message::Ping(_)
                | tokio_tungstenite::tungstenite::Message::Pong(_)
                | tokio_tungstenite::tungstenite::Message::Frame(_) => {}
            }
        }
    }

    pub async fn close(&mut self) -> crate::kernel::runtime_host::VerletResult<()> {
        self.websocket
            .close(None)
            .await
            .map_err(|err| tui_error(format!("failed to close Verlet RPC connection: {err}")))
    }
}

async fn initialize_remote_connection<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    endpoint: &str,
    config: OperatorConnectConfig,
) -> crate::kernel::runtime_host::VerletResult<(
    serde_json::Value,
    std::collections::VecDeque<OperatorEvent>,
)>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let initialize_request_id =
        crate::adapters::app_server::connection::RequestId::String("initialize".to_string());
    write_jsonrpc_message(
        websocket,
        crate::adapters::app_server::connection::JsonRpcMessage::Request(crate::adapters::app_server::connection::JsonRpcRequest {
            id: initialize_request_id.clone(),
            method: "initialize".to_string(),
            params: Some(serde_json::json!({
                "clientInfo": {
                    "name": config.client_name,
                    "title": null,
                    "version": config.client_version,
                },
                "capabilities": {
                    "experimentalApi": config.experimental_api,
                    "requestAttestation": false,
                    "optOutNotificationMethods": if config.opt_out_notification_methods.is_empty() {
                        serde_json::Value::Null
                    } else {
                        serde_json::json!(config.opt_out_notification_methods)
                    },
                },
            })),
            trace: None,
        }),
    )
    .await?;

    let mut pending_events = std::collections::VecDeque::new();
    let initialize_result = tokio::time::timeout(OPERATOR_CLIENT_INITIALIZE_TIMEOUT, async {
        loop {
            match read_jsonrpc_event(websocket).await? {
                OperatorEvent::Response(response) if response.id == initialize_request_id => {
                    break Ok(response.result);
                }
                OperatorEvent::Error(error) if error.id == initialize_request_id => {
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
        crate::adapters::app_server::connection::JsonRpcMessage::Notification(
            crate::adapters::app_server::connection::JsonRpcNotification {
                method: "initialized".to_string(),
                params: None,
            },
        ),
    )
    .await?;

    Ok((initialize_result, pending_events))
}

async fn write_jsonrpc_message<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
    message: crate::adapters::app_server::connection::JsonRpcMessage,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let payload = serde_json::to_string(&message)
        .map_err(|err| tui_error(format!("failed to encode Verlet RPC message: {err}")))?;
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            payload.into(),
        ))
        .await
        .map_err(|err| tui_error(format!("failed to write Verlet RPC message: {err}")))
}

async fn read_jsonrpc_event<S>(
    websocket: &mut tokio_tungstenite::WebSocketStream<S>,
) -> crate::kernel::runtime_host::VerletResult<OperatorEvent>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        let message = websocket
            .next()
            .await
            .ok_or_else(|| tui_error("Verlet RPC connection closed"))?
            .map_err(|err| tui_error(format!("Verlet RPC connection read failed: {err}")))?;
        match message {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                return jsonrpc_event_from_text(&text);
            }
            tokio_tungstenite::tungstenite::Message::Close(frame) => {
                let reason = frame
                    .as_ref()
                    .map(|frame| frame.reason.to_string())
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| "connection closed".to_string());
                return Err(tui_error(format!(
                    "Verlet RPC connection was closed by the endpoint: {reason}"
                )));
            }
            tokio_tungstenite::tungstenite::Message::Binary(_)
            | tokio_tungstenite::tungstenite::Message::Ping(_)
            | tokio_tungstenite::tungstenite::Message::Pong(_)
            | tokio_tungstenite::tungstenite::Message::Frame(_) => {}
        }
    }
}

fn jsonrpc_event_from_text(text: &str) -> crate::kernel::runtime_host::VerletResult<OperatorEvent> {
    match serde_json::from_str::<crate::adapters::app_server::connection::JsonRpcMessage>(text)
        .map_err(|err| tui_error(format!("invalid Verlet RPC message: {err}")))?
    {
        crate::adapters::app_server::connection::JsonRpcMessage::Notification(notification) => {
            Ok(OperatorEvent::Notification(notification))
        }
        crate::adapters::app_server::connection::JsonRpcMessage::Request(request) => {
            Ok(OperatorEvent::Request(request))
        }
        crate::adapters::app_server::connection::JsonRpcMessage::Response(response) => {
            Ok(OperatorEvent::Response(response))
        }
        crate::adapters::app_server::connection::JsonRpcMessage::Error(error) => {
            Ok(OperatorEvent::Error(error))
        }
    }
}

fn codex_text_input(text: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": text,
        "text_elements": [],
    })
}

fn string_field(
    value: &serde_json::Value,
    field: &str,
    context: &str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    value
        .get(field)
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| tui_error(format!("{context} missing `{field}`")))
}

fn thread_from_result(
    result: serde_json::Value,
    method: &str,
) -> crate::kernel::runtime_host::VerletResult<OperatorThread> {
    let thread = result
        .get("thread")
        .cloned()
        .ok_or_else(|| tui_error(format!("{method} response missing thread")))?;
    let id = string_field(&thread, "id", &format!("{method} thread"))?;
    Ok(OperatorThread { id, raw: thread })
}

fn operator_client_websocket_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(OPERATOR_CLIENT_MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_message_size(Some(OPERATOR_CLIENT_MAX_WEBSOCKET_MESSAGE_SIZE))
}

fn websocket_tcp_authority(url: &str) -> crate::kernel::runtime_host::VerletResult<&str> {
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

fn tui_error(message: impl Into<String>) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RpcClient(message.into())
}

#[cfg(test)]
mod tests;
