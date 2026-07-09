use crate::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentManifestBindOverrides,
    AgentManifestBoundThread, AgentManifestModelProfileSelection, AgentManifestOperationBinding,
    AgentManifestProviderSurface, AgentManifestSkillPackageBinding, AgentRecordRef, AgentRuntime,
    AgentRuntimeFactory, AgentToolRouter, AnthropicBedrockMessagesAdapter,
    AnthropicMessagesAdapter, CanonicalContent, CanonicalMessage, CanonicalProviderRuntimeConfig,
    CanonicalProviderRuntimeFactory, CanonicalStopReason, CanonicalUsage,
    CapsuleBindingResolutionRequest, CapsuleBindingScope, CooldisError, CooldisResult,
    CooldisSupervisor, DEBUG_THREAD_EXPORT_SCHEMA_V1, EventSequence, EventStore, EventStreamId,
    KernelThreadSpawnAgentBinding, KernelThreadSpawnAgentResolver, LlmProviderAuthContext,
    LlmProviderAuthStore, LlmProviderCatalogStore, LlmProviderConfigValue, LlmProviderRecord,
    LlmProviderStoreError, LocalAgentRegistry, LocalOperationRegistry, LocalPluginCatalog,
    LocalPluginCatalogRecord, LocalSkillRegistry, MandateCatchUpPolicy, MandateSchedulePayload,
    McpRemoteServerConfig, McpRemoteToolProvider, McpRemoteTransport, McpToolUniverseDiscoverer,
    MountedToolUniverse, OPENAI_COMPATIBLE_DEFAULT_MODEL, OpenAIChatCompletionsAdapter,
    OpenAIReasoningSummary, OpenAIResponsesAdapter, OperationRegistry, OperationToolAlias,
    ProviderAbiProjection, ProviderApi, ProviderAuth, ProviderCapabilityRecord, ProviderClient,
    ProviderEndpoint, ProviderHttpClient, ProviderRequest, ProviderRequestMode, ProviderResponse,
    ProviderResult, ProviderToolResultConstraints, ProviderWireAdapter, RuntimeEventKind,
    RuntimeStore, RuntimeTerminalState, RuntimeThreadHandle, SecretResolver, SecretSourceKind,
    SecretStoreError, SessionEntry, SessionEntryKind, SessionStore, SqliteMcpSourceRegistry,
    SqliteMetadataStore, SqliteSecretStore, SqliteSessionStore, SystemBlock,
    THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA, THREAD_AGENT_SKILL_PACKAGES_METADATA,
    THREAD_BOUND_COUPLING_SET_METADATA, THREAD_OPERATION_REGISTRY_ROOT_METADATA,
    TenantRegistration, TenantRuntimeContext, ThinkingConfig, ThinkingEffort, ThreadBaseRef,
    ThreadCheckpointId, ThreadContext, ThreadEvent, ThreadForkReason, ThreadId,
    ThreadLifecycleRecord, ThreadLifecycleSink, ThreadLifecycleStatus, ThreadMetadataStore,
    ThreadStartRequest, ThreadStatus, ThreadTopology, ToolUniverseBinding, ToolUniverseCaller,
    ToolUniverseDiscoveryReceipt, ToolUniverseSearchSurface, TurnContent, TurnInput,
    TurnSubmissionMode, VirtualBashRuntimeConfig, VirtualFile, bind_published_agent_record,
    default_blob_registry_root_for_agent_registry_root, ensure_cooldis_notify_published,
    ensure_cooldis_process_published, ensure_cooldis_schedule_published,
    ensure_cooldis_threads_published, resolve_llm_provider_auth, seed_default_llm_providers,
    stream_schema_registry_v1,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use cooldis_process::{
    AsyncExecutionManager, AsyncProcessOwner, AsyncProcessSnapshot, AsyncProcessStartRequest,
    CooldisProcessId, ExecutionDeadline, HostBashLiveBackend,
};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, accept_async_with_config};
use uuid::Uuid;
mod connection;
mod default_manifest;
mod subscriptions;
#[cfg(test)]
mod tests;
mod threads;

use connection::internal_error;
pub use connection::{
    JsonRpcError, JsonRpcErrorError, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest,
    JsonRpcResponse, RequestId,
};
use default_manifest::ensure_default_manifest_published;
use subscriptions::AppServerSubscriptions;
use threads::{AppServerState, normalize_registry_roots};

pub const APP_SERVER_LOCAL_PROVIDER: &str = "local_offline";
pub const APP_SERVER_LOCAL_MODEL: &str = "echo";
pub const APP_SERVER_BIFROST_PROVIDER: &str = "openai";
pub const APP_SERVER_BIFROST_MODEL: &str = "openai/gpt-5.5";
pub const APP_SERVER_OPENAI_COMPATIBLE_PROVIDER: &str = "openai_compatible";
pub const APP_SERVER_OPENAI_COMPATIBLE_MODEL: &str = OPENAI_COMPATIBLE_DEFAULT_MODEL;
pub const APP_SERVER_ANTHROPIC_PROVIDER: &str = "anthropic";
pub const APP_SERVER_ANTHROPIC_MODEL: &str = "claude-sonnet-4-5-20250929";
pub const APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER: &str = "anthropic_bedrock";
pub const APP_SERVER_ANTHROPIC_BEDROCK_MODEL: &str =
    "global.anthropic.claude-sonnet-4-5-20250929-v1:0";

const MAX_WEBSOCKET_MESSAGE_SIZE: usize = 128 << 20;
const APP_SERVER_HEALTH_RESPONSE_BODY: &str = "{\"status\":\"ok\"}";
const APP_SERVER_USER_AGENT: &str = "cooldis-app-server/0.1";
const HTTP_UNAUTHORIZED_BODY: &str = "missing or invalid Cooldis console session token";
const MAX_HTTP_REQUEST_HEADER_BYTES: usize = 8192;
const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_COMMAND_OUTPUT_CAP_BYTES: usize = 1024 * 1024;
const DEFAULT_AGENT_REGISTRY_ROOT: &str = ".cooldis/agents";
const DEFAULT_BLOB_REGISTRY_ROOT: &str = ".cooldis/blobs";
const DEFAULT_OPERATION_REGISTRY_ROOT: &str = ".cooldis/operations";
const DEFAULT_SKILL_REGISTRY_ROOT: &str = ".cooldis/skills";
const METADATA_DB_NAME: &str = "metadata.sqlite3";
const THREAD_APP_SERVER_CWD_METADATA: &str = "cooldis.app_server.cwd";
const THREAD_APP_SERVER_MODEL_PROVIDER_METADATA: &str = "cooldis.app_server.model_provider";
const THREAD_APP_SERVER_EPHEMERAL_METADATA: &str = "cooldis.app_server.ephemeral";
const THREAD_APP_SERVER_NAME_METADATA: &str = "cooldis.app_server.name";
const THREAD_APP_SERVER_THINKING_METADATA: &str = "cooldis.app_server.thinking";
const THREAD_AGENT_REF_METADATA: &str = "cooldis.agent.ref_uri";
const THREAD_AGENT_MANIFEST_HASH_METADATA: &str = "cooldis.agent.manifest_hash";
const THREAD_AGENT_SOURCE_HASH_METADATA: &str = "cooldis.agent.source_hash";
const THREAD_AGENT_MODEL_PROFILE_ID_METADATA: &str = "cooldis.agent.model_profile_id";
const THREAD_AGENT_PROVIDER_ID_METADATA: &str = "cooldis.agent.provider_id";
const THREAD_AGENT_MODEL_ID_METADATA: &str = "cooldis.agent.model_id";
const THREAD_AGENT_SYSTEM_INSTRUCTION_METADATA: &str = "cooldis.agent.system_instruction";
const THREAD_AGENT_RUNTIME_OVERRIDES_METADATA: &str = "cooldis.agent.runtime_overrides";
const THREAD_AGENT_RUNTIME_STREAMING_METADATA: &str = "cooldis.agent.runtime.streaming";
const THREAD_AGENT_RUNTIME_COMPACTION_AUTO_AT_TEXT_BYTES_METADATA: &str =
    "cooldis.agent.runtime.compaction.auto_at_text_bytes";
const THREAD_AGENT_OPERATION_BINDINGS_METADATA: &str = "cooldis.agent.operation_bindings";
const THREAD_REBIND_FORK_REASON_METADATA: &str = "cooldis.thread.rebind_fork.reason";
// JSON-encoded Vec<ToolUniverseBinding>; the runtime factory remounts the
// search surface (and pinned rows) from this, like operation bindings.
const THREAD_AGENT_TOOL_UNIVERSES_METADATA: &str = "cooldis.agent.tool_universes";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AppServerListenAddr {
    Unix(PathBuf),
    WebSocket(SocketAddr),
}

impl AppServerListenAddr {
    pub fn parse(value: &str) -> CooldisResult<Self> {
        if let Some(path) = value.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(CooldisError::RuntimeFactory(
                    "unix app-server listen address requires a path".to_string(),
                ));
            }
            return Ok(Self::Unix(PathBuf::from(path)));
        }

        if let Some(rest) = value.strip_prefix("ws://") {
            let (authority, path) = split_websocket_listen_url(rest);
            if authority.is_empty() {
                return Err(CooldisError::RuntimeFactory(
                    "websocket app-server listen address requires host:port".to_string(),
                ));
            }
            if !matches!(path, "" | "/rpc") {
                return Err(CooldisError::RuntimeFactory(format!(
                    "unsupported app-server websocket path {path:?}; expected /rpc"
                )));
            }
            let addr = authority.parse::<SocketAddr>().map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "invalid app-server websocket listen address {authority:?}: {err}"
                ))
            })?;
            return Ok(Self::WebSocket(addr));
        }

        Err(CooldisError::RuntimeFactory(format!(
            "unsupported app-server listen address {value:?}; expected unix://PATH or ws://HOST:PORT[/rpc]"
        )))
    }

    pub fn display(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix://{}", path.display()),
            Self::WebSocket(addr) => format!("ws://{addr}/rpc"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CooldisAppServerConfig {
    pub listen: AppServerListenAddr,
    pub runtime_home: PathBuf,
    pub state_home: PathBuf,
    pub user_state_home: PathBuf,
    pub cwd: PathBuf,
    pub tenant_id: String,
    pub user_id: String,
    pub model: String,
    pub model_provider: String,
    pub provider: AppServerProviderConfig,
    pub capsule_bindings: CapsuleBindingsConfig,
    pub agent_registry_root: PathBuf,
    pub blob_registry_root: PathBuf,
    pub skill_registry_root: PathBuf,
    pub console_assets: Option<ConsoleAssetConfig>,
}

impl CooldisAppServerConfig {
    pub fn local(listen: AppServerListenAddr, cwd: impl Into<PathBuf>) -> Self {
        let root = std::env::temp_dir().join(format!("cooldis-app-server-{}", Uuid::now_v7()));
        Self {
            listen,
            runtime_home: root.join("runtime"),
            state_home: root.join("state"),
            user_state_home: root.join("user-state"),
            cwd: cwd.into(),
            tenant_id: "cooldis_app_server".to_string(),
            user_id: "local_user".to_string(),
            model: APP_SERVER_LOCAL_MODEL.to_string(),
            model_provider: APP_SERVER_LOCAL_PROVIDER.to_string(),
            provider: AppServerProviderConfig::LocalOffline,
            capsule_bindings: CapsuleBindingsConfig::default()
                .with_registry_root(DEFAULT_OPERATION_REGISTRY_ROOT),
            agent_registry_root: PathBuf::from(DEFAULT_AGENT_REGISTRY_ROOT),
            blob_registry_root: PathBuf::from(DEFAULT_BLOB_REGISTRY_ROOT),
            skill_registry_root: PathBuf::from(DEFAULT_SKILL_REGISTRY_ROOT),
            console_assets: None,
        }
    }

    pub fn with_bifrost_openai(
        mut self,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        let model = model.into();
        self.model = model.clone();
        self.model_provider = APP_SERVER_BIFROST_PROVIDER.to_string();
        self.provider = AppServerProviderConfig::BifrostOpenAIResponses {
            base_url: base_url.into(),
            api_key: api_key.into(),
            model,
            max_tokens: 4096,
            stream: true,
        };
        self
    }

    pub fn with_openai_chat_completions(
        mut self,
        provider: impl Into<String>,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        let provider = provider.into();
        let model = model.into();
        self.model = model.clone();
        self.model_provider = provider.clone();
        self.provider = AppServerProviderConfig::OpenAIChatCompletions {
            provider,
            base_url: base_url.into(),
            api_key: api_key.into(),
            model,
            max_tokens: 4096,
            stream: true,
            headers: Vec::new(),
        };
        self
    }

    pub fn with_anthropic_messages(
        mut self,
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        model: impl Into<String>,
    ) -> Self {
        let model = model.into();
        self.model = model.clone();
        self.model_provider = APP_SERVER_ANTHROPIC_PROVIDER.to_string();
        self.provider = AppServerProviderConfig::AnthropicMessages {
            base_url: base_url.into(),
            api_key: api_key.into(),
            model,
            max_tokens: 4096,
            stream: true,
        };
        self
    }

    pub fn with_anthropic_bedrock(
        mut self,
        region: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: Option<String>,
        model: impl Into<String>,
    ) -> Self {
        let model = model.into();
        self.model = model.clone();
        self.model_provider = APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER.to_string();
        self.provider = AppServerProviderConfig::AnthropicBedrock {
            region: region.into(),
            base_url: None,
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token,
            model,
            max_tokens: 4096,
            stream: true,
        };
        self
    }

    pub fn with_catalog_openai_chat_completions(
        mut self,
        provider_id: impl Into<String>,
        model: Option<String>,
    ) -> Self {
        let provider_id = provider_id.into();
        if let Some(model) = &model {
            self.model = model.clone();
        }
        self.model_provider = provider_id.clone();
        self.provider = AppServerProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens: 4096,
            stream: true,
        };
        self
    }

    pub fn with_capsule_bindings(mut self, capsule_bindings: CapsuleBindingsConfig) -> Self {
        self.capsule_bindings = capsule_bindings;
        self
    }

    pub fn with_console_assets(
        mut self,
        root: impl Into<PathBuf>,
        session_token: impl Into<String>,
    ) -> Self {
        self.console_assets = Some(ConsoleAssetConfig {
            root: root.into(),
            session_token: session_token.into(),
        });
        self
    }

    pub fn metadata_store_path(&self) -> PathBuf {
        self.state_home.join(METADATA_DB_NAME)
    }

    pub fn user_metadata_store_path(&self) -> PathBuf {
        self.user_state_home.join(METADATA_DB_NAME)
    }

    pub fn provider_metadata_store_path(&self) -> PathBuf {
        self.user_metadata_store_path()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConsoleAssetConfig {
    pub root: PathBuf,
    pub session_token: String,
}

fn operation_registry_root_for_kernel_publish(config: &CooldisAppServerConfig) -> Option<&Path> {
    let registry_root = config.capsule_bindings.registry_root.as_deref()?;
    let default_registry_root = config.cwd.join(Path::new(DEFAULT_OPERATION_REGISTRY_ROOT));
    if registry_root == default_registry_root && !registry_root.exists() {
        return None;
    }
    Some(registry_root)
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapsuleBindingsConfig {
    #[serde(
        default,
        alias = "registry_root",
        skip_serializing_if = "Option::is_none"
    )]
    pub registry_root: Option<PathBuf>,
    #[serde(
        default,
        alias = "global_operation_names",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub global_operation_names: Vec<String>,
    #[serde(
        default,
        alias = "load_all_active_when_unbound",
        skip_serializing_if = "is_false"
    )]
    pub load_all_active_when_unbound: bool,
}

impl CapsuleBindingsConfig {
    pub fn with_registry_root(mut self, registry_root: impl Into<PathBuf>) -> Self {
        self.registry_root = Some(registry_root.into());
        self
    }

    pub fn with_global_operation_name(mut self, operation_name: impl Into<String>) -> Self {
        self.global_operation_names.push(operation_name.into());
        self
    }

    pub fn with_load_all_active_when_unbound(mut self, value: bool) -> Self {
        self.load_all_active_when_unbound = value;
        self
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Clone, Debug)]
pub enum AppServerProviderConfig {
    LocalOffline,
    BifrostOpenAIResponses {
        base_url: String,
        api_key: String,
        model: String,
        max_tokens: u32,
        stream: bool,
    },
    OpenAIChatCompletions {
        provider: String,
        base_url: String,
        api_key: String,
        model: String,
        max_tokens: u32,
        stream: bool,
        headers: Vec<(String, String)>,
    },
    AnthropicMessages {
        base_url: String,
        api_key: String,
        model: String,
        max_tokens: u32,
        stream: bool,
    },
    AnthropicBedrock {
        region: String,
        base_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        model: String,
        max_tokens: u32,
        stream: bool,
    },
    CatalogOpenAIChatCompletions {
        provider_id: String,
        model: Option<String>,
        max_tokens: u32,
        stream: bool,
    },
}

#[derive(Clone)]
pub struct CooldisAppServer {
    inner: Arc<CooldisAppServerInner>,
}

struct CooldisAppServerInner {
    supervisor: CooldisSupervisor,
    tenant_id: String,
    user_id: String,
    model: String,
    model_provider: String,
    provider: AppServerProviderConfig,
    capsule_bindings: CapsuleBindingsConfig,
    agent_registry_root: PathBuf,
    blob_registry_root: PathBuf,
    skill_registry_root: PathBuf,
    console_assets: Option<ConsoleAssetConfig>,
    cwd: PathBuf,
    codex_home: PathBuf,
    metadata_store_path: PathBuf,
    user_metadata_store_path: PathBuf,
    session_store_path: PathBuf,
    metadata_store: SqliteMetadataStore,
    user_metadata_store: SqliteMetadataStore,
    process_manager: AsyncExecutionManager,
    subscriptions: Mutex<AppServerSubscriptions>,
    state: RwLock<AppServerState>,
}

#[derive(Clone, Debug)]
struct AppServerOfflineProviderClient {
    capabilities: ProviderCapabilityRecord,
}

impl AppServerOfflineProviderClient {
    fn new(provider_family: impl Into<String>, model: impl Into<String>) -> Self {
        let mut capabilities = ProviderCapabilityRecord::local_offline(provider_family, model);
        capabilities.supports_tools = true;
        capabilities.tool_result_constraints = ProviderToolResultConstraints::open_tool_results();
        capabilities
            .supported_abi_projections
            .insert(ProviderAbiProjection::LlmTool);
        Self { capabilities }
    }
}

#[async_trait::async_trait]
impl ProviderClient for AppServerOfflineProviderClient {
    fn capabilities(&self) -> Option<ProviderCapabilityRecord> {
        Some(self.capabilities.clone())
    }

    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.capabilities
            .validate_request(request, ProviderRequestMode::Complete)?;
        let last_user_text = request
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                CanonicalMessage::User { content, .. } => {
                    let text = text_from_canonical_content(content);
                    (!text.is_empty()).then_some(text)
                }
                CanonicalMessage::Assistant { .. } | CanonicalMessage::ToolResult { .. } => None,
            })
            .unwrap_or_default();
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text(format!("local:{last_user_text}"))],
            usage: CanonicalUsage {
                input_tokens: request.messages.len() as u64,
                output_tokens: last_user_text.len() as u64,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

impl CooldisAppServer {
    pub async fn new_local(mut config: CooldisAppServerConfig) -> CooldisResult<Self> {
        let metadata_store = open_and_seed_metadata_store(config.metadata_store_path())?;
        let user_metadata_store = open_and_seed_metadata_store(config.user_metadata_store_path())?;
        sync_catalog_provider_identity(&mut config, &metadata_store)?;
        normalize_registry_roots(&mut config);
        let runtime_factory =
            runtime_factory_from_config(&config, &metadata_store, &user_metadata_store).await?;
        Self::with_runtime_factory_and_metadata_stores(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
        )
        .await
    }

    pub async fn with_runtime_factory(
        config: CooldisAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
    ) -> CooldisResult<Self> {
        let metadata_store = SqliteMetadataStore::in_memory().map_err(metadata_store_error)?;
        let user_metadata_store = SqliteMetadataStore::in_memory().map_err(metadata_store_error)?;
        Self::with_runtime_factory_and_metadata_stores(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
        )
        .await
    }

    #[cfg(test)]
    async fn with_runtime_factory_and_metadata_store(
        config: CooldisAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
        metadata_store: SqliteMetadataStore,
    ) -> CooldisResult<Self> {
        let user_metadata_store = open_and_seed_metadata_store(config.user_metadata_store_path())?;
        Self::with_runtime_factory_and_metadata_stores(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
        )
        .await
    }

    async fn with_runtime_factory_and_metadata_stores(
        mut config: CooldisAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
        metadata_store: SqliteMetadataStore,
        user_metadata_store: SqliteMetadataStore,
    ) -> CooldisResult<Self> {
        normalize_registry_roots(&mut config);
        let provider_surface =
            agent_manifest_provider_surface_for_config(&config, &metadata_store)?;
        ensure_cooldis_threads_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_cooldis_schedule_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_cooldis_process_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_cooldis_notify_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_default_manifest_published(&config, provider_surface.supports_streaming)?;
        let metadata_store_path = config.metadata_store_path();
        let user_metadata_store_path = config.user_metadata_store_path();
        let supervisor = CooldisSupervisor::new();
        let tenant_context = TenantRuntimeContext::local(
            config.tenant_id.clone(),
            config.runtime_home.clone(),
            config.state_home.clone(),
        );
        let codex_home = tenant_context.codex_home();
        let session_store_path = tenant_context.session_history_path();
        supervisor
            .register_tenant(TenantRegistration {
                context: tenant_context,
                runtime_factory,
            })
            .await?;
        let app = Self {
            inner: Arc::new(CooldisAppServerInner {
                supervisor,
                tenant_id: config.tenant_id,
                user_id: config.user_id,
                model: config.model,
                model_provider: config.model_provider,
                provider: config.provider,
                capsule_bindings: config.capsule_bindings,
                agent_registry_root: config.agent_registry_root,
                blob_registry_root: config.blob_registry_root,
                skill_registry_root: config.skill_registry_root,
                console_assets: config.console_assets,
                cwd: config.cwd,
                codex_home,
                metadata_store_path,
                user_metadata_store_path,
                session_store_path,
                metadata_store,
                user_metadata_store,
                process_manager: AsyncExecutionManager::default(),
                subscriptions: Mutex::new(AppServerSubscriptions::default()),
                state: RwLock::new(AppServerState::default()),
            }),
        };
        app.inner
            .supervisor
            .set_thread_lifecycle_sink(
                &app.inner.tenant_id,
                Some(Arc::new(threads::AppServerThreadLifecycleSink::new(&app))),
            )
            .await?;
        app.load_threads_from_metadata().await?;
        Ok(app)
    }

    pub async fn serve(&self, listen: AppServerListenAddr) -> CooldisResult<()> {
        match listen {
            AppServerListenAddr::Unix(path) => self.serve_unix(path).await,
            AppServerListenAddr::WebSocket(addr) => self.serve_websocket(addr).await,
        }
    }

    pub fn supervisor(&self) -> CooldisSupervisor {
        self.inner.supervisor.clone()
    }

    pub fn tenant_id(&self) -> &str {
        &self.inner.tenant_id
    }

    pub fn user_id(&self) -> &str {
        &self.inner.user_id
    }

    pub fn model(&self) -> &str {
        &self.inner.model
    }

    pub fn model_provider(&self) -> &str {
        &self.inner.model_provider
    }

    pub fn cwd(&self) -> &Path {
        &self.inner.cwd
    }

    pub fn session_store_path(&self) -> &Path {
        &self.inner.session_store_path
    }

    #[cfg(unix)]
    async fn serve_unix(&self, path: PathBuf) -> CooldisResult<()> {
        prepare_unix_socket_path(&path)?;
        let listener = UnixListener::bind(&path).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to bind Cooldis app-server socket {}: {err}",
                path.display()
            ))
        })?;

        loop {
            let (stream, _) = listener.accept().await.map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to accept Cooldis app-server connection: {err}"
                ))
            })?;
            let app = self.clone();
            tokio::spawn(async move {
                if let Err(err) = app.handle_unix_stream(stream).await {
                    eprintln!("cooldis app-server connection failed: {err}");
                }
            });
        }
    }

    #[cfg(not(unix))]
    async fn serve_unix(&self, _path: PathBuf) -> CooldisResult<()> {
        Err(CooldisError::RuntimeFactory(
            "unix app-server sockets are only supported on Unix platforms".to_string(),
        ))
    }

    async fn serve_websocket(&self, addr: SocketAddr) -> CooldisResult<()> {
        let listener = bind_websocket_listener(addr).await?;
        self.serve_websocket_listener(listener).await
    }

    pub async fn serve_websocket_listener(&self, listener: TcpListener) -> CooldisResult<()> {
        let addr = listener.local_addr().map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to inspect Cooldis app-server websocket listener: {err}"
            ))
        })?;
        if !addr.ip().is_loopback() {
            return Err(CooldisError::RuntimeFactory(format!(
                "app-server websocket listen address {addr} is not loopback; configure websocket auth before binding non-loopback addresses"
            )));
        }

        loop {
            let (stream, peer) = listener.accept().await.map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to accept Cooldis app-server websocket connection: {err}"
                ))
            })?;
            let app = self.clone();
            tokio::spawn(async move {
                if let Err(err) = app.handle_tcp_stream(stream).await {
                    eprintln!("cooldis app-server websocket connection from {peer} failed: {err}");
                }
            });
        }
    }

    #[cfg(unix)]
    async fn handle_unix_stream(&self, stream: UnixStream) -> CooldisResult<()> {
        let websocket = accept_async_with_config(stream, Some(websocket_config()))
            .await
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to upgrade Cooldis app-server unix socket websocket: {err}"
                ))
            })?;
        self.handle_websocket(websocket).await
    }

    async fn handle_tcp_stream(&self, stream: TcpStream) -> CooldisResult<()> {
        let mut stream = stream;
        if self.handle_http_request(&mut stream).await? {
            return Ok(());
        }
        if !self.authorize_console_websocket(&mut stream).await? {
            return Ok(());
        }
        let websocket = accept_async_with_config(stream, Some(websocket_config()))
            .await
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to upgrade Cooldis app-server tcp websocket: {err}"
                ))
            })?;
        self.handle_websocket(websocket).await
    }

    async fn handle_http_request(&self, stream: &mut TcpStream) -> CooldisResult<bool> {
        let Some(request) = peek_http_request(stream).await? else {
            return Ok(false);
        };
        if request.method != "GET" && request.method != "HEAD" {
            return Ok(false);
        }

        if matches!(request.path.as_str(), "/healthz" | "/readyz") {
            consume_http_request_headers(stream).await?;
            write_http_response(
                stream,
                "200 OK",
                "application/json",
                APP_SERVER_HEALTH_RESPONSE_BODY.as_bytes(),
            )
            .await?;
            return Ok(true);
        }

        let Some(console) = &self.inner.console_assets else {
            return Ok(false);
        };
        if request.path == "/rpc" {
            return Ok(false);
        }

        consume_http_request_headers(stream).await?;
        let Some(relative_path) = console_asset_relative_path(&request.path) else {
            write_http_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found",
            )
            .await?;
            return Ok(true);
        };
        let asset_path = console.root.join(relative_path);
        let Ok(mut body) = tokio::fs::read(&asset_path).await else {
            write_http_response(
                stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found",
            )
            .await?;
            return Ok(true);
        };
        if asset_path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name == "index.html")
        {
            let html = String::from_utf8(body).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "console index.html is not valid UTF-8 at {}: {err}",
                    asset_path.display()
                ))
            })?;
            body = inject_console_config(&html, &console.session_token).into_bytes();
        }
        write_http_response(stream, "200 OK", content_type_for_path(&asset_path), &body).await?;
        Ok(true)
    }

    async fn authorize_console_websocket(&self, stream: &mut TcpStream) -> CooldisResult<bool> {
        let Some(console) = &self.inner.console_assets else {
            return Ok(true);
        };
        let Some(request) = peek_http_request(stream).await? else {
            return Ok(true);
        };
        if request.path != "/rpc" {
            return Ok(true);
        }
        if console_request_has_token(&request, &console.session_token) {
            return Ok(true);
        }
        consume_http_request_headers(stream).await?;
        write_http_response(
            stream,
            "401 Unauthorized",
            "text/plain; charset=utf-8",
            HTTP_UNAUTHORIZED_BODY.as_bytes(),
        )
        .await?;
        Ok(false)
    }
}

struct ResolvedCatalogOpenAIChatCompletionsProvider {
    runtime_config: CanonicalProviderRuntimeConfig,
    endpoint: ProviderEndpoint,
}

fn resolve_catalog_openai_chat_completions_provider<C, A>(
    provider_store: &C,
    auth_store: &A,
    auth_context: &LlmProviderAuthContext,
    provider_id: &str,
    model: Option<&str>,
    max_tokens: u32,
    stream: bool,
) -> CooldisResult<ResolvedCatalogOpenAIChatCompletionsProvider>
where
    C: LlmProviderCatalogStore,
    A: LlmProviderAuthStore,
{
    let provider = provider_store
        .get_provider(provider_id)
        .map_err(provider_store_error)?
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "catalog provider {provider_id:?} is not in the provider metadata store"
            ))
        })?;
    let model_id = selected_catalog_model_id(&provider, model)?;
    let model_record = provider
        .models
        .iter()
        .find(|candidate| candidate.model_id == model_id);
    let api = model_record
        .and_then(|model| model.api.clone())
        .unwrap_or_else(|| provider.api.clone());
    if api != ProviderApi::OpenAIChatCompletions {
        return Err(CooldisError::RuntimeFactory(format!(
            "catalog provider {provider_id:?} uses api {api:?}; only OpenAI Chat Completions catalog providers are supported here"
        )));
    }
    let base_url = model_record
        .and_then(|model| model.base_url.clone())
        .unwrap_or_else(|| provider.base_url.clone());
    let resolved_auth = resolve_llm_provider_auth(auth_store, &provider, auth_context)
        .map_err(provider_store_error)?;
    if provider.auth_header && resolved_auth.is_none() {
        return Err(CooldisError::RuntimeFactory(format!(
            "catalog provider {provider_id:?} requires an API key but none was configured"
        )));
    }
    let mut endpoint = ProviderEndpoint::openai_chat_completions(
        &base_url,
        resolved_auth
            .as_ref()
            .map(|auth| auth.api_key.clone())
            .unwrap_or_default(),
    );
    if !provider.auth_header {
        endpoint.auth = ProviderAuth::None;
    }

    let mut headers = provider.headers.clone();
    if let Some(model_record) = model_record {
        headers.extend(model_record.headers.clone());
    }
    endpoint.headers = resolve_catalog_headers(&headers, auth_context)?;

    let mut runtime_config =
        CanonicalProviderRuntimeConfig::new(api, provider.provider_id.clone(), model_id);
    runtime_config.max_tokens = max_tokens;
    runtime_config.stream = stream;
    Ok(ResolvedCatalogOpenAIChatCompletionsProvider {
        runtime_config,
        endpoint,
    })
}

fn selected_catalog_model_id(
    provider: &LlmProviderRecord,
    requested_model: Option<&str>,
) -> CooldisResult<String> {
    if let Some(model) = requested_model.filter(|model| !model.trim().is_empty()) {
        return Ok(model.to_string());
    }
    provider
        .models
        .iter()
        .find(|model| {
            model
                .metadata
                .get("default")
                .is_some_and(|value| value == "true")
        })
        .or_else(|| provider.models.first())
        .map(|model| model.model_id.clone())
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "catalog provider {:?} has no models",
                provider.provider_id
            ))
        })
}

fn resolve_catalog_headers(
    headers: &BTreeMap<String, LlmProviderConfigValue>,
    auth_context: &LlmProviderAuthContext,
) -> CooldisResult<Vec<(String, String)>> {
    headers
        .iter()
        .map(|(name, value)| {
            Ok((
                name.clone(),
                resolve_catalog_config_value(value, auth_context)?,
            ))
        })
        .collect()
}

fn resolve_catalog_config_value(
    value: &LlmProviderConfigValue,
    auth_context: &LlmProviderAuthContext,
) -> CooldisResult<String> {
    match value {
        LlmProviderConfigValue::Literal { value } => Ok(value.clone()),
        LlmProviderConfigValue::Env { name } => auth_context
            .environment
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| {
                CooldisError::RuntimeFactory(format!(
                    "catalog provider header env var {name} is not configured"
                ))
            }),
        LlmProviderConfigValue::Command { .. } => Err(CooldisError::RuntimeFactory(
            "catalog provider command-backed header resolution is not enabled".to_string(),
        )),
    }
}

fn provider_store_error(err: LlmProviderStoreError) -> CooldisError {
    CooldisError::RuntimeFactory(format!("provider metadata store failed: {err}"))
}

fn agent_manifest_provider_surface_for_config(
    config: &CooldisAppServerConfig,
    metadata_store: &SqliteMetadataStore,
) -> CooldisResult<AgentManifestProviderSurface> {
    agent_manifest_provider_surface_from_parts(
        &config.provider,
        &config.model_provider,
        &config.model,
        metadata_store,
    )
}

fn agent_manifest_provider_surface_from_parts(
    provider_config: &AppServerProviderConfig,
    model_provider: &str,
    model: &str,
    metadata_store: &SqliteMetadataStore,
) -> CooldisResult<AgentManifestProviderSurface> {
    match provider_config {
        AppServerProviderConfig::CatalogOpenAIChatCompletions { provider_id, .. } => {
            let provider = metadata_store
                .get_provider(provider_id)
                .map_err(provider_store_error)?
                .ok_or_else(|| {
                    CooldisError::RuntimeFactory(format!(
                        "catalog provider {provider_id:?} is not in the provider metadata store"
                    ))
                })?;
            Ok(AgentManifestProviderSurface::from_provider_record(
                &provider,
            ))
        }
        AppServerProviderConfig::LocalOffline => Ok(AgentManifestProviderSurface::single(
            model_provider.to_string(),
            model.to_string(),
        )
        .with_supports_streaming(false)),
        AppServerProviderConfig::BifrostOpenAIResponses { .. }
        | AppServerProviderConfig::OpenAIChatCompletions { .. }
        | AppServerProviderConfig::AnthropicMessages { .. }
        | AppServerProviderConfig::AnthropicBedrock { .. } => Ok(
            AgentManifestProviderSurface::single(model_provider.to_string(), model.to_string()),
        ),
    }
}

fn metadata_store_error(err: crate::MetadataStoreError) -> CooldisError {
    CooldisError::RuntimeFactory(err.to_string())
}

fn secret_store_error(err: SecretStoreError) -> CooldisError {
    CooldisError::RuntimeFactory(format!("secret store failed: {err}"))
}

fn metadata_store_jsonrpc_error(err: crate::MetadataStoreError) -> JsonRpcErrorError {
    internal_error(metadata_store_error(err))
}

fn text_from_canonical_content(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Text { text, .. } => Some(text.as_str()),
            CanonicalContent::Image { .. }
            | CanonicalContent::ToolCall { .. }
            | CanonicalContent::Thinking { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn thinking_text_from_canonical_content(content: &[CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            CanonicalContent::Thinking { text, .. } => Some(text.as_str()),
            CanonicalContent::Text { .. }
            | CanonicalContent::Image { .. }
            | CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn open_and_seed_metadata_store(path: impl AsRef<Path>) -> CooldisResult<SqliteMetadataStore> {
    let store = SqliteMetadataStore::open(path).map_err(metadata_store_error)?;
    seed_default_llm_providers(&store).map_err(provider_store_error)?;
    Ok(store)
}

fn sync_catalog_provider_identity(
    config: &mut CooldisAppServerConfig,
    provider_store: &SqliteMetadataStore,
) -> CooldisResult<()> {
    if let AppServerProviderConfig::CatalogOpenAIChatCompletions {
        provider_id, model, ..
    } = &config.provider
    {
        let provider = provider_store
            .get_provider(provider_id)
            .map_err(provider_store_error)?
            .ok_or_else(|| {
                CooldisError::RuntimeFactory(format!(
                    "catalog provider {provider_id:?} is not in the provider metadata store"
                ))
            })?;
        config.model_provider = provider.provider_id.clone();
        config.model = selected_catalog_model_id(&provider, model.as_deref())?;
    }
    Ok(())
}

async fn runtime_factory_from_config(
    config: &CooldisAppServerConfig,
    provider_store: &SqliteMetadataStore,
    auth_store: &SqliteMetadataStore,
) -> CooldisResult<Arc<dyn crate::AgentRuntimeFactory>> {
    match &config.provider {
        AppServerProviderConfig::LocalOffline => {
            let provider = config.model_provider.clone();
            let model = config.model.clone();
            let runtime_config = CanonicalProviderRuntimeConfig::new(
                ProviderApi::Other(provider.clone()),
                provider.clone(),
                model.clone(),
            );
            let secret_resolver = secret_resolver_from_config(config)?;
            Ok(runtime_factory_from_provider_parts_with_app_paths(
                runtime_config,
                Arc::new(AppServerOfflineProviderClient::new(provider, model)),
                config.capsule_bindings.clone(),
                secret_resolver,
                config,
            ))
        }
        AppServerProviderConfig::BifrostOpenAIResponses {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIResponsesAdapter {
                include_encrypted_reasoning: false,
                reasoning_summary: OpenAIReasoningSummary::Auto,
            });
            let client = Arc::new(
                ProviderHttpClient::new(
                    ProviderEndpoint::openai_responses(base_url, api_key.clone()),
                    adapter,
                )
                .map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "failed to build Bifrost OpenAI provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = CanonicalProviderRuntimeConfig::new(
                ProviderApi::OpenAIResponses,
                APP_SERVER_BIFROST_PROVIDER,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config)?;
            Ok(runtime_factory_from_provider_parts_with_app_paths(
                runtime_config,
                client,
                config.capsule_bindings.clone(),
                secret_resolver,
                config,
            ))
        }
        AppServerProviderConfig::OpenAIChatCompletions {
            provider,
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
            headers,
        } => {
            let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIChatCompletionsAdapter);
            let mut endpoint = ProviderEndpoint::openai_chat_completions(base_url, api_key.clone());
            endpoint.headers = headers.clone();
            let client = Arc::new(ProviderHttpClient::new(endpoint, adapter).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to build OpenAI Chat Completions provider client: {err}"
                ))
            })?);
            let mut runtime_config = CanonicalProviderRuntimeConfig::new(
                ProviderApi::OpenAIChatCompletions,
                provider.clone(),
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config)?;
            Ok(runtime_factory_from_provider_parts_with_app_paths(
                runtime_config,
                client,
                config.capsule_bindings.clone(),
                secret_resolver,
                config,
            ))
        }
        AppServerProviderConfig::AnthropicMessages {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(AnthropicMessagesAdapter);
            let client = Arc::new(
                ProviderHttpClient::new(
                    ProviderEndpoint::anthropic_messages(base_url, api_key.clone()),
                    adapter,
                )
                .map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "failed to build Anthropic Messages provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = CanonicalProviderRuntimeConfig::new(
                ProviderApi::AnthropicMessages,
                APP_SERVER_ANTHROPIC_PROVIDER,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config)?;
            Ok(runtime_factory_from_provider_parts_with_app_paths(
                runtime_config,
                client,
                config.capsule_bindings.clone(),
                secret_resolver,
                config,
            ))
        }
        AppServerProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
            max_tokens,
            stream,
        } => {
            let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(AnthropicBedrockMessagesAdapter);
            let endpoint = if let Some(base_url) = base_url {
                ProviderEndpoint::anthropic_bedrock_with_base_url(
                    base_url,
                    region,
                    model,
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone(),
                )
            } else {
                ProviderEndpoint::anthropic_bedrock(
                    region,
                    model,
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone(),
                )
            };
            let client = Arc::new(ProviderHttpClient::new(endpoint, adapter).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to build Anthropic Bedrock provider client: {err}"
                ))
            })?);
            let mut runtime_config = CanonicalProviderRuntimeConfig::new(
                ProviderApi::AnthropicMessages,
                APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config)?;
            Ok(runtime_factory_from_provider_parts_with_app_paths(
                runtime_config,
                client,
                config.capsule_bindings.clone(),
                secret_resolver,
                config,
            ))
        }
        AppServerProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens,
            stream,
        } => {
            let resolved = resolve_catalog_openai_chat_completions_provider(
                provider_store,
                auth_store,
                &LlmProviderAuthContext::from_process_env(),
                provider_id,
                model.as_deref(),
                *max_tokens,
                *stream,
            )?;
            let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIChatCompletionsAdapter);
            let client = Arc::new(ProviderHttpClient::new(resolved.endpoint, adapter).map_err(
                |err| {
                    CooldisError::RuntimeFactory(format!(
                        "failed to build catalog OpenAI Chat Completions provider client: {err}"
                    ))
                },
            )?);
            let secret_resolver = secret_resolver_from_config(config)?;
            Ok(runtime_factory_from_provider_parts_with_app_paths(
                resolved.runtime_config,
                client,
                config.capsule_bindings.clone(),
                secret_resolver,
                config,
            ))
        }
    }
}

#[cfg(test)]
pub(crate) fn runtime_factory_from_provider_parts(
    runtime_config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
    capsule_bindings: CapsuleBindingsConfig,
) -> Arc<dyn crate::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_secret_resolver(
        runtime_config,
        client,
        capsule_bindings,
        None,
    )
}

#[cfg(test)]
pub(crate) fn runtime_factory_from_provider_parts_with_secret_resolver(
    runtime_config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing app-server config field name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
) -> Arc<dyn crate::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_store_paths(
        runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config value
        capsule_bindings,
        secret_resolver,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

pub(crate) fn runtime_factory_from_provider_parts_with_app_paths(
    runtime_config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing app-server config type name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    config: &CooldisAppServerConfig,
) -> Arc<dyn crate::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_store_paths(
        runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config value
        capsule_bindings,
        secret_resolver,
        Some(config.metadata_store_path()),
        Some(config.user_metadata_store_path()),
        Some(config.state_home.join("session_history.sqlite3")),
        Some(config.agent_registry_root.clone()),
        Some(config.blob_registry_root.clone()),
        Some(config.skill_registry_root.clone()),
        Some(config.cwd.clone()),
    )
}

fn runtime_factory_from_provider_parts_with_store_paths(
    runtime_config: CanonicalProviderRuntimeConfig,
    client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing app-server config type name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    metadata_store_path: Option<PathBuf>,
    secret_store_path: Option<PathBuf>,
    session_store_path: Option<PathBuf>,
    agent_registry_root: Option<PathBuf>,
    blob_registry_root: Option<PathBuf>,
    skill_registry_root: Option<PathBuf>,
    cwd: Option<PathBuf>,
) -> Arc<dyn crate::AgentRuntimeFactory> {
    // lexicon-allow: capsule - existing app-server runtime factory name
    Arc::new(threads::CapsuleBindingRuntimeFactory {
        config: runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config field
        capsule_bindings,
        secret_resolver,
        metadata_store_path,
        secret_store_path,
        session_store_path,
        agent_registry_root,
        blob_registry_root,
        skill_registry_root,
        cwd,
    })
}

fn secret_resolver_from_config(
    config: &CooldisAppServerConfig,
) -> CooldisResult<Option<Arc<dyn SecretResolver>>> {
    let store =
        SqliteSecretStore::open(config.user_metadata_store_path()).map_err(secret_store_error)?;
    Ok(Some(Arc::new(store)))
}

fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
}

async fn bind_websocket_listener(addr: SocketAddr) -> CooldisResult<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(CooldisError::RuntimeFactory(format!(
            "app-server websocket listen address {addr} is not loopback; configure websocket auth before binding non-loopback addresses"
        )));
    }
    TcpListener::bind(addr).await.map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to bind Cooldis app-server websocket listener {addr}: {err}"
        ))
    })
}

#[derive(Debug)]
struct HttpRequestHead {
    method: String,
    path: String,
    query: Option<String>,
    headers: Vec<(String, String)>,
}

async fn peek_http_request(stream: &TcpStream) -> CooldisResult<Option<HttpRequestHead>> {
    let mut request = [0_u8; MAX_HTTP_REQUEST_HEADER_BYTES];
    let len = stream.peek(&mut request).await.map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to inspect Cooldis app-server tcp request: {err}"
        ))
    })?;
    if len == 0 {
        return Ok(None);
    }
    Ok(parse_http_request_head(&request[..len]))
}

fn parse_http_request_head(bytes: &[u8]) -> Option<HttpRequestHead> {
    let text = std::str::from_utf8(bytes).ok()?;
    let header_end = text.find("\r\n\r\n").unwrap_or(text.len());
    let mut lines = text[..header_end].split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let (path, query) = match target.split_once('?') {
        Some((path, query)) => (path.to_string(), Some(query.to_string())),
        None => (target.to_string(), None),
    };
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    Some(HttpRequestHead {
        method,
        path,
        query,
        headers,
    })
}

fn console_asset_relative_path(path: &str) -> Option<PathBuf> {
    if matches!(path, "/" | "/index.html") {
        return Some(PathBuf::from("index.html"));
    }

    let mut relative = PathBuf::new();
    let path = path.strip_prefix('/')?;
    for segment in path.split('/') {
        if segment.is_empty()
            || segment == "."
            || segment == ".."
            || segment.contains('\\')
            || segment.contains('%')
        {
            return None;
        }
        relative.push(segment);
    }
    if relative.as_os_str().is_empty() {
        None
    } else {
        Some(relative)
    }
}

fn inject_console_config(html: &str, session_token: &str) -> String {
    let token = serde_json::to_string(session_token).unwrap_or_else(|_| "\"\"".to_string());
    let script =
        format!("<script>window.__COOLDIS_CONSOLE_CONFIG__={{sessionToken:{token}}};</script>");
    if let Some(index) = html.find("</head>") {
        let mut injected = String::with_capacity(html.len() + script.len());
        injected.push_str(&html[..index]);
        injected.push_str(&script);
        injected.push_str(&html[index..]);
        injected
    } else {
        format!("{script}{html}")
    }
}

fn console_request_has_token(request: &HttpRequestHead, expected: &str) -> bool {
    request
        .query
        .as_deref()
        .is_some_and(|query| query_parameter_matches(query, "token", expected))
        || request
            .headers
            .iter()
            .filter(|(name, _)| name == "sec-websocket-protocol")
            .flat_map(|(_, value)| value.split(',').map(str::trim))
            .any(|protocol| {
                protocol == expected
                    || protocol
                        .strip_prefix("cooldis-console-token.")
                        .is_some_and(|token| token == expected)
            })
}

fn query_parameter_matches(query: &str, key: &str, expected: &str) -> bool {
    query.split('&').any(|pair| {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        name == key && value == expected
    })
}

fn content_type_for_path(path: &Path) -> &'static str {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript; charset=utf-8",
        Some("mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("webp") => "image/webp",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    }
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> CooldisResult<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to write Cooldis app-server HTTP response: {err}"
        ))
    })?;
    stream.write_all(body).await.map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to write Cooldis app-server HTTP response body: {err}"
        ))
    })?;
    Ok(())
}

async fn consume_http_request_headers(stream: &mut TcpStream) -> CooldisResult<()> {
    let mut consumed = Vec::new();
    let mut chunk = [0_u8; 512];
    loop {
        let len = stream.read(&mut chunk).await.map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to read Cooldis app-server HTTP request: {err}"
            ))
        })?;
        if len == 0 {
            return Ok(());
        }
        consumed.extend_from_slice(&chunk[..len]);
        if consumed.windows(4).any(|window| window == b"\r\n\r\n") || consumed.len() > 8192 {
            return Ok(());
        }
    }
}

fn split_websocket_listen_url(value: &str) -> (&str, &str) {
    match value.find('/') {
        Some(index) => (&value[..index], &value[index..]),
        None => (value, ""),
    }
}

#[cfg(unix)]
fn prepare_unix_socket_path(path: &Path) -> CooldisResult<()> {
    if let Some(parent) = path.parent() {
        let parent_existed = parent.exists();
        std::fs::create_dir_all(parent).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to create app-server socket directory {}: {err}",
                parent.display()
            ))
        })?;
        if !parent_existed {
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
                |err| {
                    CooldisError::RuntimeFactory(format!(
                        "failed to secure app-server socket directory {}: {err}",
                        parent.display()
                    ))
                },
            )?;
        }
    }
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to inspect existing app-server socket {}: {err}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_file() || metadata.file_type().is_dir() {
            return Err(CooldisError::RuntimeFactory(format!(
                "refusing to replace non-socket app-server path {}",
                path.display()
            )));
        }
        std::fs::remove_file(path).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to remove stale app-server socket {}: {err}",
                path.display()
            ))
        })?;
    }
    Ok(())
}
