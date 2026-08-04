use crate::ProcessHandleIngressSink;
use crate::daemon::daemon_config::synthesized_local_daemon_identity_config;
use crate::daemon::daemon_io::VerletDaemonIoBridge;
use crate::daemon::identity::{
    AuthenticationPath, AuthorityClass, BoundarySurface, IDENTITY_AUTH_REJECTION_SCHEMA_V1,
    IDENTITY_HOST_EFFECT_SCHEMA_V1, IDENTITY_SESSION_SCHEMA_V1, IdentityAuthRejectionReason,
    IdentityAuthRejectionV1, IdentityAuthority, IdentityHostEffectV1, IdentityMode,
    IdentitySessionV1, PrincipalId, PrincipalKind, ResolvedPrincipal, SqliteIdentityAuthority,
    VerletDaemonIdentityConfig, authority_class_for_method, identity_token_digest,
};
use crate::daemon::recovery_sweep::StartupRecoverySweep;
use crate::kernel::process_handle_dispatch::ProcessHandleDispatcher;
use crate::{
    AgentKernelToolCall, AgentKernelToolProvider, AgentLoopConfig, AgentLoopFactory,
    AgentManifestBindOverrides, AgentManifestBoundThread, AgentManifestModelProfileSelection,
    AgentManifestOperationBinding, AgentManifestPlacementBinding, AgentManifestProviderSurface,
    AgentManifestResolvedWorkspaceMount, AgentManifestSkillDiscovery,
    AgentManifestSkillPackageBinding, AgentManifestWorkspaceBinding, AgentRecordRef, AgentRuntime,
    AgentRuntimeFactory, AgentToolRouter, AnthropicBedrockMessagesAdapter,
    AnthropicMessagesAdapter, CanonicalContent, CanonicalMessage, CanonicalStopReason,
    CanonicalUsage, CapsuleBindingResolutionRequest, CapsuleBindingScope,
    DEBUG_THREAD_EXPORT_SCHEMA_V1, EventSequence, EventStore, EventStreamId,
    KernelThreadSpawnAgentBinding, KernelThreadSpawnAgentResolver, LlmProviderAuthContext,
    LlmProviderAuthStore, LlmProviderCatalogStore, LlmProviderConfigValue, LlmProviderRecord,
    LlmProviderStoreError, LocalAgentRegistry, LocalOperationRegistry, LocalPluginCatalog,
    LocalPluginCatalogRecord, LocalSkillRegistry, MandateCatchUpPolicy, MandateSchedulePayload,
    McpRemoteServerConfig, McpRemoteToolProvider, McpRemoteTransport, McpToolUniverseDiscoverer,
    MountedToolUniverse, OPENAI_COMPATIBLE_DEFAULT_MODEL, OpenAIChatCompletionsAdapter,
    OpenAIReasoningSummary, OpenAIResponsesAdapter, OperationRegistry, OperationToolAlias,
    PluginMount, ProviderAbiProjection, ProviderApi, ProviderAuth, ProviderCapabilityRecord,
    ProviderClient, ProviderEndpoint, ProviderHttpClient, ProviderRequest, ProviderRequestMode,
    ProviderResponse, ProviderResult, ProviderToolResultConstraints, ProviderWireAdapter,
    RuntimeEventKind, RuntimeStore, RuntimeTerminalState, RuntimeThreadHandle, SecretResolver,
    SecretSourceKind, SecretStoreError, SessionEntry, SessionEntryKind, SessionStore,
    SqliteMcpSourceRegistry, SqliteMetadataStore, SqliteSecretStore, SqliteSessionStore,
    SystemBlock, SystemDaemonClock, THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA,
    THREAD_AGENT_SKILL_DISCOVERY_METADATA, THREAD_AGENT_SKILL_PACKAGES_METADATA,
    THREAD_BOUND_COUPLING_SET_METADATA, THREAD_OPERATION_REGISTRY_ROOT_METADATA,
    TenantRegistration, TenantRuntimeContext, ThinkingConfig, ThinkingEffort, ThreadBaseRef,
    ThreadCheckpointId, ThreadContext, ThreadEvent, ThreadForkReason, ThreadId,
    ThreadLifecycleRecord, ThreadLifecycleSink, ThreadLifecycleStatus, ThreadMetadataStore,
    ThreadStartRequest, ThreadStatus, ThreadTopology, ToolUniverseBinding, ToolUniverseCaller,
    ToolUniverseDiscoveryReceipt, ToolUniverseSearchSurface, TurnContent, TurnInput,
    TurnSubmissionMode, VerletError, VerletResult, VerletSupervisor, VirtualBashRuntimeConfig,
    VirtualFile, bind_published_agent_record_with_placement,
    default_blob_registry_root_for_agent_registry_root, ensure_verlet_notify_published,
    ensure_verlet_process_published, ensure_verlet_schedule_published,
    ensure_verlet_threads_published, resolve_llm_provider_auth, seed_default_llm_providers,
    stream_schema_registry_v1,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;
use std::io::Write as _;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
#[cfg(unix)]
use tokio::net::{UnixListener, UnixStream};
use tokio::process::Command;
use tokio::sync::{Mutex, OnceCell, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, accept_hdr_async_with_config};
use uuid::Uuid;
use verlet_io_core::IngressEnvelope;
use verlet_process::{
    AsyncExecutionManager, AsyncProcessOwner, AsyncProcessSnapshot, AsyncProcessStartRequest,
    ExecutionDeadline, HostBashLiveBackend, VerletProcessId,
};
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
pub(crate) use threads::{
    active_manifest_receipt_payloads, recover_unwitnessed_workspace_metadata_as_unbound,
};

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
const APP_SERVER_USER_AGENT: &str = "verlet-app-server/0.1";
const HTTP_UNAUTHORIZED_BODY: &str = "authentication required";
const MAX_HTTP_REQUEST_HEADER_BYTES: usize = 8192;
const HTTP_REQUEST_HEADER_TIMEOUT: Duration = Duration::from_secs(10);
const CONSOLE_TOKEN_PROTOCOL_PREFIX: &str = "verlet-console-token.";
const LEGACY_CONSOLE_TOKEN_PROTOCOL_PREFIX: &str = "cooldis-console-token.";
const DEFAULT_BLOB_REGISTRY_ROOT: &str = ".verlet/blobs";
const CONSOLE_CREDENTIAL_ID_FILE: &str = "console-credential-id";
const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_COMMAND_OUTPUT_CAP_BYTES: usize = 1024 * 1024;
const DEFAULT_OPERATION_REGISTRY_ROOT: &str = ".verlet/operations";
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
const THREAD_AGENT_PLACEMENT_METADATA: &str = "cooldis.agent.placement";
pub(crate) const THREAD_AGENT_WORKSPACE_METADATA: &str = "cooldis.agent.workspace";
const THREAD_AGENT_RUNTIME_STREAMING_METADATA: &str = "cooldis.agent.runtime.streaming";
const THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA: &str =
    crate::adapters::agent_loop::THREAD_AGENT_RUNTIME_MAX_TOOL_ROUNDS_METADATA;
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
    pub fn parse(value: &str) -> VerletResult<Self> {
        if let Some(path) = value.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(VerletError::RuntimeFactory(
                    "unix app-server listen address requires a path".to_string(),
                ));
            }
            return Ok(Self::Unix(PathBuf::from(path)));
        }

        if let Some(rest) = value.strip_prefix("ws://") {
            let (authority, path) = split_websocket_listen_url(rest);
            if authority.is_empty() {
                return Err(VerletError::RuntimeFactory(
                    "websocket app-server listen address requires host:port".to_string(),
                ));
            }
            if !matches!(path, "" | "/rpc") {
                return Err(VerletError::RuntimeFactory(format!(
                    "unsupported app-server websocket path {path:?}; expected /rpc"
                )));
            }
            let addr = authority.parse::<SocketAddr>().map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "invalid app-server websocket listen address {authority:?}: {err}"
                ))
            })?;
            return Ok(Self::WebSocket(addr));
        }

        Err(VerletError::RuntimeFactory(format!(
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
pub struct VerletAppServerConfig {
    pub listen: AppServerListenAddr,
    pub runtime_home: PathBuf,
    pub state_home: PathBuf,
    pub user_state_home: PathBuf,
    pub cwd: PathBuf,
    pub tenant_id: String,
    pub user_id: String,
    pub identity_mode: IdentityMode,
    pub console_principal: Option<PrincipalId>,
    pub model: String,
    pub model_provider: String,
    pub provider: AppServerProviderConfig,
    pub capsule_bindings: CapsuleBindingsConfig,
    pub agent_registry_root: PathBuf,
    pub blob_registry_root: PathBuf,
    pub skill_registry_root: PathBuf,
    /// Deployment placement used when a bind surface does not override it.
    pub default_placement: AgentManifestPlacementBinding,
    /// Host workspace used when a requiring manifest has no bind override.
    pub default_workspace: Option<AgentManifestWorkspaceBinding>,
    /// Generation-local capability bit. The daemon flips this only after the
    /// configured sync listener has bound successfully.
    pub remote_event_store_served: Arc<AtomicBool>,
    pub console_assets: Option<ConsoleAssetConfig>,
}

impl VerletAppServerConfig {
    pub fn local(listen: AppServerListenAddr, cwd: impl Into<PathBuf>) -> Self {
        let root = std::env::temp_dir().join(format!("verlet-app-server-{}", Uuid::now_v7()));
        let identity = synthesized_local_daemon_identity_config();
        let cwd = cwd.into();
        let canonical_project_root = cwd.join(".verlet");
        let legacy_project_root = cwd.join(concat!(".", "cool", "dis"));
        let project_storage_root = if canonical_project_root.exists()
            || !legacy_project_root.exists()
        {
            PathBuf::from(".verlet")
        } else {
            eprintln!(
                "warning: {} is deprecated; existing state will continue to be used in place through v0.3.0",
                legacy_project_root.display()
            );
            PathBuf::from(concat!(".", "cool", "dis"))
        };
        let mut config = Self {
            listen,
            runtime_home: root.join("runtime"),
            state_home: root.join("state"),
            user_state_home: root.join("user-state"),
            cwd,
            tenant_id: String::new(),
            user_id: String::new(),
            identity_mode: IdentityMode::Local,
            console_principal: None,
            model: APP_SERVER_LOCAL_MODEL.to_string(),
            model_provider: APP_SERVER_LOCAL_PROVIDER.to_string(),
            provider: AppServerProviderConfig::LocalOffline,
            capsule_bindings: CapsuleBindingsConfig::default()
                .with_registry_root(project_storage_root.join("operations")),
            agent_registry_root: project_storage_root.join("agents"),
            blob_registry_root: project_storage_root.join("blobs"),
            skill_registry_root: project_storage_root.join("skills"),
            default_placement: AgentManifestPlacementBinding::default(),
            default_workspace: None,
            remote_event_store_served: Arc::new(AtomicBool::new(false)),
            console_assets: None,
        };
        config.apply_daemon_identity_config(&identity);
        config
    }

    /// Project a daemon identity config onto this app-server config. This is
    /// the single seam through which mode, tenant, and console principal reach
    /// the server; the boundary authority is initialized from these fields.
    pub fn apply_daemon_identity_config(&mut self, identity: &VerletDaemonIdentityConfig) {
        self.identity_mode = identity.mode;
        self.tenant_id = identity.tenant_id.clone().unwrap_or_default();
        self.console_principal = identity.console_principal.clone();
        self.user_id = self
            .console_principal
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default();
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

#[derive(Clone, Eq, PartialEq)]
pub struct ConsoleAssetConfig {
    pub root: PathBuf,
    pub session_token: String,
}

impl std::fmt::Debug for ConsoleAssetConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsoleAssetConfig")
            .field("root", &self.root)
            .field("session_token", &"<redacted>")
            .finish()
    }
}

fn operation_registry_root_for_kernel_publish(config: &VerletAppServerConfig) -> Option<&Path> {
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
pub struct VerletAppServer {
    inner: Arc<VerletAppServerInner>,
}

struct VerletAppServerInner {
    supervisor: VerletSupervisor,
    tenant_id: String,
    user_id: String,
    identity_mode: IdentityMode,
    console_principal: Option<PrincipalId>,
    model: String,
    model_provider: String,
    provider: AppServerProviderConfig,
    capsule_bindings: CapsuleBindingsConfig,
    agent_registry_root: PathBuf,
    blob_registry_root: PathBuf,
    skill_registry_root: PathBuf,
    default_placement: AgentManifestPlacementBinding,
    default_workspace: Option<AgentManifestWorkspaceBinding>,
    remote_event_store_served: Arc<AtomicBool>,
    console_assets: Option<ConsoleAssetConfig>,
    identity_authority: Arc<dyn IdentityAuthority>,
    identity_clock: Arc<dyn crate::DaemonClock>,
    console_credential: Option<ConsoleCredentialLease>,
    cwd: PathBuf,
    codex_home: PathBuf,
    metadata_store_path: PathBuf,
    user_metadata_store_path: PathBuf,
    session_store_path: PathBuf,
    metadata_store: SqliteMetadataStore,
    user_metadata_store: SqliteMetadataStore,
    process_manager: AsyncExecutionManager,
    process_dispatcher: OnceCell<ProcessHandleDispatcher>,
    subscriptions: Mutex<AppServerSubscriptions>,
    state: RwLock<AppServerState>,
}

struct ConsoleCredentialLease {
    credential_id: String,
    principal_id: PrincipalId,
    record_path: PathBuf,
}

struct SessionCloseWitness {
    authority: Arc<dyn IdentityAuthority>,
    clock: Arc<dyn crate::DaemonClock>,
    session_id: String,
    armed: bool,
}

impl SessionCloseWitness {
    fn new(
        authority: Arc<dyn IdentityAuthority>,
        clock: Arc<dyn crate::DaemonClock>,
        session_id: String,
    ) -> Self {
        Self {
            authority,
            clock,
            session_id,
            armed: true,
        }
    }

    async fn close(&mut self) -> VerletResult<()> {
        // `witness_session_closed` starts its cancellation-safe transaction on
        // the first poll, so disarming here prevents a cancelled await from
        // scheduling a duplicate close witness.
        self.armed = false;
        let result = self
            .authority
            .witness_session_closed(&self.session_id, self.clock.now().timestamp_millis())
            .await;
        // A completed failure has no transaction left to finish in the
        // background. Re-arm the idempotent Drop path for one best-effort retry.
        self.armed = result.is_err();
        result
    }
}

impl Drop for SessionCloseWitness {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let authority = Arc::clone(&self.authority);
        let session_id = self.session_id.clone();
        let closed_at_ms = self.clock.now().timestamp_millis();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                if let Err(error) = authority
                    .witness_session_closed(&session_id, closed_at_ms)
                    .await
                {
                    eprintln!("failed to witness aborted Verlet app-server session: {error}");
                }
            });
        }
    }
}

impl Drop for VerletAppServerInner {
    fn drop(&mut self) {
        let Some(credential) = self.console_credential.take() else {
            return;
        };
        let authority = Arc::clone(&self.identity_authority);
        let cleanup = std::thread::Builder::new()
            .name("verlet-console-credential-cleanup".to_string())
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|error| error.to_string())?;
                runtime
                    .block_on(retire_console_credential(authority, &credential))
                    .map_err(|error| error.to_string())
            });
        match cleanup {
            Ok(cleanup) => match cleanup.join() {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    eprintln!("failed to retire Verlet console credential: {error}")
                }
                Err(_) => eprintln!("Verlet console credential cleanup thread panicked"),
            },
            Err(error) => eprintln!("failed to start Verlet console credential cleanup: {error}"),
        }
    }
}

struct AppServerProcessHandleIngress {
    app: Weak<VerletAppServerInner>,
}

#[async_trait::async_trait]
impl ProcessHandleIngressSink for AppServerProcessHandleIngress {
    async fn submit_process_handle_envelope(&self, envelope: IngressEnvelope) -> VerletResult<()> {
        let inner = self.app.upgrade().ok_or_else(|| {
            VerletError::RuntimeExecution(
                "app-server stopped before process handle ingress settled".to_string(),
            )
        })?;
        VerletDaemonIoBridge::from_app_server(&VerletAppServer { inner })
            .submit_durable_handle_envelope(envelope)
            .await
            .map(|_| ())
            .map_err(|err| VerletError::RuntimeExecution(err.to_string()))
    }
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

impl VerletAppServer {
    /// Same constructor as [`Self::new`]; the name survives from before
    /// identity modes existed and remains the conventional entry point for
    /// configs built with [`VerletAppServerConfig::local`].
    pub async fn new_local(config: VerletAppServerConfig) -> VerletResult<Self> {
        Self::new(config).await
    }

    pub async fn new(mut config: VerletAppServerConfig) -> VerletResult<Self> {
        let metadata_store = open_and_seed_metadata_store(config.metadata_store_path()).await?;
        let user_metadata_store =
            open_and_seed_metadata_store(config.user_metadata_store_path()).await?;
        sync_catalog_provider_identity(&mut config, &metadata_store).await?;
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
        config: VerletAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
    ) -> VerletResult<Self> {
        let metadata_store = SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        let user_metadata_store = SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        Self::with_runtime_factory_and_metadata_stores(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn with_runtime_factory_and_session_store_decorator(
        config: VerletAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
        decorate: impl FnOnce(Arc<dyn RuntimeStore>) -> Arc<dyn RuntimeStore> + Send + 'static,
    ) -> VerletResult<Self> {
        let metadata_store = SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        let user_metadata_store = SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        Self::with_runtime_factory_and_metadata_stores_inner(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
            Some(Box::new(decorate)),
        )
        .await
    }

    #[cfg(test)]
    async fn with_runtime_factory_and_metadata_store(
        config: VerletAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
        metadata_store: SqliteMetadataStore,
    ) -> VerletResult<Self> {
        let user_metadata_store =
            open_and_seed_metadata_store(config.user_metadata_store_path()).await?;
        Self::with_runtime_factory_and_metadata_stores(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
        )
        .await
    }

    async fn with_runtime_factory_and_metadata_stores(
        config: VerletAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
        metadata_store: SqliteMetadataStore,
        user_metadata_store: SqliteMetadataStore,
    ) -> VerletResult<Self> {
        Self::with_runtime_factory_and_metadata_stores_inner(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
            None,
        )
        .await
    }

    async fn with_runtime_factory_and_metadata_stores_inner(
        mut config: VerletAppServerConfig,
        runtime_factory: Arc<dyn crate::AgentRuntimeFactory>,
        metadata_store: SqliteMetadataStore,
        user_metadata_store: SqliteMetadataStore,
        session_store_decorator: Option<
            Box<dyn FnOnce(Arc<dyn RuntimeStore>) -> Arc<dyn RuntimeStore> + Send>,
        >,
    ) -> VerletResult<Self> {
        normalize_registry_roots(&mut config);
        let provider_surface =
            agent_manifest_provider_surface_for_config(&config, &metadata_store).await?;
        ensure_verlet_threads_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_verlet_schedule_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_verlet_process_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_verlet_notify_published(operation_registry_root_for_kernel_publish(&config))?;
        ensure_default_manifest_published(&config, provider_surface.supports_streaming)?;
        let metadata_store_path = config.metadata_store_path();
        let user_metadata_store_path = config.user_metadata_store_path();
        let supervisor = VerletSupervisor::new();
        let mut tenant_context = TenantRuntimeContext::local(
            config.tenant_id.clone(),
            config.runtime_home.clone(),
            config.state_home.clone(),
        );
        let codex_home = tenant_context.codex_home();
        let session_store_path = tenant_context.session_history_path();
        let identity_store = SqliteSessionStore::open(&session_store_path)
            .await
            .map_err(|err| VerletError::History(err.to_string()))?;
        let runtime_store = Arc::new(identity_store.clone()) as Arc<dyn RuntimeStore>;
        let runtime_store = match session_store_decorator {
            Some(decorate) => decorate(runtime_store),
            None => runtime_store,
        };
        tenant_context = tenant_context.with_session_store(runtime_store);
        supervisor
            .register_tenant(TenantRegistration {
                context: tenant_context,
                runtime_factory,
            })
            .await?;
        let identity_clock: Arc<dyn crate::DaemonClock> = Arc::new(SystemDaemonClock);
        let console_credential_record_path = session_store_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(CONSOLE_CREDENTIAL_ID_FILE);
        let (identity_authority, console_credential) = initialize_boundary_identity(
            identity_store,
            Arc::clone(&identity_clock),
            config.identity_mode,
            config.console_principal.clone(),
            &config.user_id,
            config.console_assets.as_mut(),
            &console_credential_record_path,
        )
        .await?;
        let app = Self {
            inner: Arc::new(VerletAppServerInner {
                supervisor,
                tenant_id: config.tenant_id,
                user_id: config.user_id,
                identity_mode: config.identity_mode,
                console_principal: config.console_principal,
                model: config.model,
                model_provider: config.model_provider,
                provider: config.provider,
                capsule_bindings: config.capsule_bindings,
                agent_registry_root: config.agent_registry_root,
                blob_registry_root: config.blob_registry_root,
                skill_registry_root: config.skill_registry_root,
                default_placement: config.default_placement,
                default_workspace: config.default_workspace,
                remote_event_store_served: config.remote_event_store_served,
                console_assets: config.console_assets,
                identity_authority,
                identity_clock,
                console_credential,
                cwd: config.cwd,
                codex_home,
                metadata_store_path,
                user_metadata_store_path,
                session_store_path,
                metadata_store,
                user_metadata_store,
                process_manager: AsyncExecutionManager::default(),
                process_dispatcher: OnceCell::new(),
                subscriptions: Mutex::new(AppServerSubscriptions::default()),
                state: RwLock::new(AppServerState::default()),
            }),
        };
        let process_ingress: Arc<dyn ProcessHandleIngressSink> =
            Arc::new(AppServerProcessHandleIngress {
                app: Arc::downgrade(&app.inner),
            });
        let runtime_store = app
            .inner
            .supervisor
            .runtime_store(&app.inner.tenant_id)
            .await?;
        let process_dispatcher =
            ProcessHandleDispatcher::new(runtime_store, Arc::clone(&process_ingress));
        app.inner
            .process_dispatcher
            .set(process_dispatcher.clone())
            .map_err(|_| {
                VerletError::RuntimeFactory(
                    "app-server process dispatcher initialized twice".to_string(),
                )
            })?;
        app.inner
            .supervisor
            .set_process_handle_ingress(&app.inner.tenant_id, Some(process_ingress))
            .await?;
        app.inner
            .supervisor
            .set_process_handle_dispatcher(&app.inner.tenant_id, Some(process_dispatcher.clone()))
            .await?;
        app.inner
            .supervisor
            .set_thread_lifecycle_sink(
                &app.inner.tenant_id,
                Some(Arc::new(threads::AppServerThreadLifecycleSink::new(&app))),
            )
            .await?;
        app.load_threads_from_metadata().await?;
        // Construction is the earliest common boundary for daemon listeners,
        // standalone app-server serving, and in-process/local JSON-RPC users.
        // Run recovery before returning the first callable surface.
        process_dispatcher.assert_startup_registry_empty().await?;
        let recovery_store = SqliteSessionStore::open(&app.inner.session_store_path)
            .await
            .map_err(|err| VerletError::History(err.to_string()))?;
        let recovery = StartupRecoverySweep::new(
            recovery_store,
            process_dispatcher,
            &app.inner.tenant_id,
            &app.inner.user_id,
        )
        .run_once()
        .await?;
        if recovery.thread_joins > 0 || recovery.process_outcomes > 0 {
            eprintln!(
                "verlet startup recovery appended {} thread join(s) and submitted {} process outcome(s)",
                recovery.thread_joins, recovery.process_outcomes,
            );
        }
        Ok(app)
    }

    pub async fn serve(&self, listen: AppServerListenAddr) -> VerletResult<()> {
        match listen {
            AppServerListenAddr::Unix(path) => self.serve_unix(path).await,
            AppServerListenAddr::WebSocket(addr) => self.serve_websocket(addr).await,
        }
    }

    pub fn supervisor(&self) -> VerletSupervisor {
        self.inner.supervisor.clone()
    }

    pub fn tenant_id(&self) -> &str {
        &self.inner.tenant_id
    }

    pub fn user_id(&self) -> &str {
        &self.inner.user_id
    }

    /// Identity boundary settings retained from the construction config, for
    /// consumers past the connection handshake (dispatcher authorization).
    #[allow(dead_code)]
    pub(crate) fn identity_boundary_config(&self) -> (IdentityMode, Option<&PrincipalId>) {
        (
            self.inner.identity_mode,
            self.inner.console_principal.as_ref(),
        )
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

    pub(crate) fn mark_remote_event_store_served(&self) {
        self.inner
            .remote_event_store_served
            .store(true, Ordering::Release);
    }

    pub(crate) fn remote_event_store_served(&self) -> bool {
        self.inner.remote_event_store_served.load(Ordering::Acquire)
    }

    #[cfg(unix)]
    async fn serve_unix(&self, path: PathBuf) -> VerletResult<()> {
        prepare_unix_socket_path(&path)?;
        let listener = UnixListener::bind(&path).map_err(|err| {
            VerletError::RuntimeFactory(format!(
                "failed to bind Verlet app-server socket {}: {err}",
                path.display()
            ))
        })?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(|err| {
            VerletError::RuntimeFactory(format!(
                "failed to secure Verlet app-server socket {}: {err}",
                path.display()
            ))
        })?;

        loop {
            let (stream, _) = listener.accept().await.map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to accept Verlet app-server connection: {err}"
                ))
            })?;
            let peer_uid = stream
                .peer_cred()
                .map_err(|err| {
                    VerletError::RuntimeFactory(format!(
                        "failed to inspect Verlet app-server peer credentials: {err}"
                    ))
                })?
                .uid();
            let app = self.clone();
            tokio::spawn(async move {
                if let Err(err) = app.handle_unix_stream(stream, peer_uid).await {
                    eprintln!("verlet app-server connection failed: {err}");
                }
            });
        }
    }

    #[cfg(not(unix))]
    async fn serve_unix(&self, _path: PathBuf) -> VerletResult<()> {
        Err(VerletError::RuntimeFactory(
            "unix app-server sockets are only supported on Unix platforms".to_string(),
        ))
    }

    async fn serve_websocket(&self, addr: SocketAddr) -> VerletResult<()> {
        let listener = bind_websocket_listener(addr).await?;
        self.serve_websocket_listener(listener).await
    }

    pub async fn serve_websocket_listener(&self, listener: TcpListener) -> VerletResult<()> {
        let addr = listener.local_addr().map_err(|err| {
            VerletError::RuntimeFactory(format!(
                "failed to inspect Verlet app-server websocket listener: {err}"
            ))
        })?;
        if !addr.ip().is_loopback() {
            return Err(VerletError::RuntimeFactory(format!(
                "app-server websocket listen address {addr} is not loopback; configure websocket auth before binding non-loopback addresses"
            )));
        }

        loop {
            let (stream, peer) = listener.accept().await.map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to accept Verlet app-server websocket connection: {err}"
                ))
            })?;
            let app = self.clone();
            tokio::spawn(async move {
                if let Err(err) = app.handle_tcp_stream(stream).await {
                    eprintln!("verlet app-server websocket connection from {peer} failed: {err}");
                }
            });
        }
    }

    #[cfg(unix)]
    async fn handle_unix_stream(&self, mut stream: UnixStream, peer_uid: u32) -> VerletResult<()> {
        let Some(resolved_principal) = self
            .authenticate_unix_websocket(&mut stream, peer_uid)
            .await?
        else {
            return Ok(());
        };
        let websocket = accept_authenticated_websocket(stream)
            .await
            .map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to upgrade Verlet app-server unix socket websocket: {err}"
                ))
            })?;
        self.handle_websocket(websocket, resolved_principal, BoundarySurface::UnixSocket)
            .await
    }

    async fn handle_tcp_stream(&self, stream: TcpStream) -> VerletResult<()> {
        let mut stream = stream;
        if self.handle_http_request(&mut stream).await? {
            return Ok(());
        }
        let Some((resolved_principal, surface)) =
            self.authenticate_tcp_websocket(&mut stream).await?
        else {
            return Ok(());
        };
        let websocket = accept_authenticated_websocket(stream)
            .await
            .map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to upgrade Verlet app-server tcp websocket: {err}"
                ))
            })?;
        self.handle_websocket(websocket, resolved_principal, surface)
            .await
    }

    async fn handle_http_request(&self, stream: &mut TcpStream) -> VerletResult<bool> {
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
                VerletError::RuntimeFactory(format!(
                    "console index.html is not valid UTF-8 at {}: {err}",
                    asset_path.display()
                ))
            })?;
            body = inject_console_config(&html, &console.session_token).into_bytes();
        }
        write_http_response(stream, "200 OK", content_type_for_path(&asset_path), &body).await?;
        Ok(true)
    }

    async fn authenticate_tcp_websocket(
        &self,
        stream: &mut TcpStream,
    ) -> VerletResult<Option<(ResolvedPrincipal, BoundarySurface)>> {
        let request = peek_http_request(stream).await?;
        let token_and_surface = request.as_ref().and_then(request_bearer_token);
        if let Some((token, surface)) = token_and_surface
            && let Some(principal) = self.inner.identity_authority.verify_token(token).await?
        {
            return Ok(Some((principal, surface)));
        }
        let surface = token_and_surface
            .map(|(_, surface)| surface)
            .unwrap_or(BoundarySurface::Websocket);
        let reason = match token_and_surface {
            Some((token, _)) => self.token_rejection_reason(token).await?,
            None => IdentityAuthRejectionReason::CredentialUnknown,
        };
        self.reject_websocket_auth(stream, surface, reason).await?;
        Ok(None)
    }

    #[cfg(unix)]
    async fn authenticate_unix_websocket(
        &self,
        stream: &mut UnixStream,
        peer_uid: u32,
    ) -> VerletResult<Option<ResolvedPrincipal>> {
        let same_uid = peer_uid == current_effective_uid();
        if self.inner.identity_mode == IdentityMode::Local
            && same_uid
            && let Some(principal) = self
                .inner
                .identity_authority
                .resolve_peer_uid(peer_uid)
                .await?
        {
            return Ok(Some(principal));
        }
        let request = peek_unix_http_request(stream).await?;
        let token = request
            .as_ref()
            .and_then(request_bearer_token)
            .map(|(token, _)| token);
        if let Some(token) = token
            && let Some(principal) = self.inner.identity_authority.verify_token(token).await?
        {
            return Ok(Some(principal));
        }
        let reason = match token {
            Some(token) => self.token_rejection_reason(token).await?,
            None if same_uid && self.inner.identity_mode == IdentityMode::Managed => {
                IdentityAuthRejectionReason::PeerMappingDisabled { uid: peer_uid }
            }
            None => IdentityAuthRejectionReason::CredentialUnknown,
        };
        self.reject_websocket_auth(stream, BoundarySurface::UnixSocket, reason)
            .await?;
        Ok(None)
    }

    async fn token_rejection_reason(
        &self,
        token: &str,
    ) -> VerletResult<IdentityAuthRejectionReason> {
        let digest = identity_token_digest(token);
        let now_ms = self.inner.identity_clock.now().timestamp_millis();
        for principal in self.inner.identity_authority.list_principals().await? {
            for credential in self
                .inner
                .identity_authority
                .list_credentials(&principal.principal_id)
                .await?
            {
                if credential.token_digest != digest {
                    continue;
                }
                if credential.revoked_at_ms.is_some() {
                    return Ok(IdentityAuthRejectionReason::CredentialRevoked {
                        credential_id: credential.credential_id,
                    });
                }
                if principal.revoked_at_ms.is_some() {
                    return Ok(IdentityAuthRejectionReason::PrincipalRevoked {
                        principal_id: principal.principal_id,
                    });
                }
                if credential
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
                {
                    return Ok(IdentityAuthRejectionReason::CredentialExpired {
                        credential_id: credential.credential_id,
                    });
                }
            }
        }
        Ok(IdentityAuthRejectionReason::CredentialUnknown)
    }

    async fn reject_websocket_auth<S>(
        &self,
        stream: &mut S,
        surface: BoundarySurface,
        reason: IdentityAuthRejectionReason,
    ) -> VerletResult<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        self.inner
            .identity_authority
            .witness_auth_rejected(&IdentityAuthRejectionV1 {
                schema: IDENTITY_AUTH_REJECTION_SCHEMA_V1.to_string(),
                surface,
                reason,
                principal_id: None,
                rejected_at_ms: self.inner.identity_clock.now().timestamp_millis(),
            })
            .await?;
        consume_http_request_headers(stream).await?;
        write_http_response(
            stream,
            "401 Unauthorized",
            "text/plain; charset=utf-8",
            HTTP_UNAUTHORIZED_BODY.as_bytes(),
        )
        .await
    }
}

async fn initialize_boundary_identity(
    store: SqliteSessionStore,
    clock: Arc<dyn crate::DaemonClock>,
    mode: IdentityMode,
    console_principal: Option<PrincipalId>,
    default_operator_id: &str,
    console_assets: Option<&mut ConsoleAssetConfig>,
    console_credential_record_path: &Path,
) -> VerletResult<(Arc<dyn IdentityAuthority>, Option<ConsoleCredentialLease>)> {
    let authority = SqliteIdentityAuthority::new(store.clone(), Arc::clone(&clock), None).await?;
    let mut operator = authority
        .list_principals()
        .await?
        .into_iter()
        .find(|principal| {
            principal.kind == PrincipalKind::Operator && principal.revoked_at_ms.is_none()
        })
        .map(|principal| principal.principal_id);
    if mode == IdentityMode::Local && operator.is_none() {
        let principal_id = PrincipalId::new(default_operator_id);
        authority
            .bootstrap_operator(&principal_id, "Local operator")
            .await?;
        operator = Some(principal_id);
    }
    let peer_operator = (mode == IdentityMode::Local)
        .then(|| operator.clone())
        .flatten();
    let authority = SqliteIdentityAuthority::new(store, clock, peer_operator).await?;
    let mut console_credential = None;
    if let Some(console) = console_assets {
        let principal_id = console_principal
            .or_else(|| (mode == IdentityMode::Local).then_some(operator).flatten())
            .ok_or_else(|| {
                VerletError::RuntimeFactory(
                    "console authentication requires a configured active operator principal"
                        .to_string(),
                )
            })?;
        if let Some(predecessor_id) = read_console_credential_id(console_credential_record_path)? {
            ignore_missing_credential(
                authority
                    .revoke_credential(&principal_id, &predecessor_id)
                    .await,
            )?;
        }
        let (credential, token) = authority
            .mint_credential(&principal_id, &principal_id, None)
            .await?;
        // A purpose-labeled or ephemeral credential class in the identity
        // schema should replace this lifecycle file in a future ticket.
        if let Err(error) =
            persist_console_credential_id(console_credential_record_path, &credential.credential_id)
        {
            let _ = authority
                .revoke_credential(&principal_id, &credential.credential_id)
                .await;
            return Err(error);
        }
        console.session_token = token;
        console_credential = Some(ConsoleCredentialLease {
            credential_id: credential.credential_id,
            principal_id,
            record_path: console_credential_record_path.to_path_buf(),
        });
    }
    Ok((Arc::new(authority), console_credential))
}

fn read_console_credential_id(path: &Path) -> VerletResult<Option<String>> {
    let value = match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(VerletError::RuntimeFactory(format!(
                "failed to read Verlet console credential record {}: {error}",
                path.display()
            )));
        }
    };
    let credential_id = value.trim();
    if credential_id.is_empty() || credential_id.chars().any(char::is_whitespace) {
        return Err(VerletError::RuntimeFactory(format!(
            "Verlet console credential record {} is malformed",
            path.display()
        )));
    }
    Ok(Some(credential_id.to_string()))
}

fn persist_console_credential_id(path: &Path, credential_id: &str) -> VerletResult<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|error| {
        VerletError::RuntimeFactory(format!(
            "failed to prepare Verlet console credential record directory {}: {error}",
            parent.display()
        ))
    })?;
    let temporary = parent.join(format!(
        ".{CONSOLE_CREDENTIAL_ID_FILE}.{}.tmp",
        Uuid::now_v7()
    ));
    let write_result = (|| -> io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        file.write_all(credential_id.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if let Err(error) = write_result {
        let _ = std::fs::remove_file(&temporary);
        return Err(VerletError::RuntimeFactory(format!(
            "failed to persist Verlet console credential record {}: {error}",
            path.display()
        )));
    }
    Ok(())
}

fn ignore_missing_credential(result: VerletResult<()>) -> VerletResult<()> {
    match result {
        Err(error)
            if error
                .to_string()
                .contains("identity credential was not found") =>
        {
            Ok(())
        }
        other => other,
    }
}

async fn retire_console_credential(
    authority: Arc<dyn IdentityAuthority>,
    credential: &ConsoleCredentialLease,
) -> VerletResult<()> {
    ignore_missing_credential(
        authority
            .revoke_credential(&credential.principal_id, &credential.credential_id)
            .await,
    )?;
    let current = read_console_credential_id(&credential.record_path)?;
    if current.as_deref() == Some(credential.credential_id.as_str()) {
        match std::fs::remove_file(&credential.record_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(VerletError::RuntimeFactory(format!(
                    "failed to clear Verlet console credential record {}: {error}",
                    credential.record_path.display()
                )));
            }
        }
    }
    Ok(())
}

struct ResolvedCatalogOpenAIChatCompletionsProvider {
    runtime_config: AgentLoopConfig,
    endpoint: ProviderEndpoint,
}

async fn resolve_catalog_openai_chat_completions_provider<C, A>(
    provider_store: &C,
    auth_store: &A,
    auth_context: &LlmProviderAuthContext,
    provider_id: &str,
    model: Option<&str>,
    max_tokens: u32,
    stream: bool,
) -> VerletResult<ResolvedCatalogOpenAIChatCompletionsProvider>
where
    C: LlmProviderCatalogStore,
    A: LlmProviderAuthStore,
{
    let provider = provider_store
        .get_provider(provider_id)
        .await
        .map_err(provider_store_error)?
        .ok_or_else(|| {
            VerletError::RuntimeFactory(format!(
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
        return Err(VerletError::RuntimeFactory(format!(
            "catalog provider {provider_id:?} uses api {api:?}; only OpenAI Chat Completions catalog providers are supported here"
        )));
    }
    let base_url = model_record
        .and_then(|model| model.base_url.clone())
        .unwrap_or_else(|| provider.base_url.clone());
    let resolved_auth = resolve_llm_provider_auth(auth_store, &provider, auth_context)
        .await
        .map_err(provider_store_error)?;
    if provider.auth_header && resolved_auth.is_none() {
        return Err(VerletError::RuntimeFactory(format!(
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

    let mut runtime_config = AgentLoopConfig::new(api, provider.provider_id.clone(), model_id);
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
) -> VerletResult<String> {
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
            VerletError::RuntimeFactory(format!(
                "catalog provider {:?} has no models",
                provider.provider_id
            ))
        })
}

fn resolve_catalog_headers(
    headers: &BTreeMap<String, LlmProviderConfigValue>,
    auth_context: &LlmProviderAuthContext,
) -> VerletResult<Vec<(String, String)>> {
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
) -> VerletResult<String> {
    match value {
        LlmProviderConfigValue::Literal { value } => Ok(value.clone()),
        LlmProviderConfigValue::Env { name } => auth_context
            .environment
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| {
                VerletError::RuntimeFactory(format!(
                    "catalog provider header env var {name} is not configured"
                ))
            }),
        LlmProviderConfigValue::Command { .. } => Err(VerletError::RuntimeFactory(
            "catalog provider command-backed header resolution is not enabled".to_string(),
        )),
    }
}

fn provider_store_error(err: LlmProviderStoreError) -> VerletError {
    VerletError::RuntimeFactory(format!("provider metadata store failed: {err}"))
}

async fn agent_manifest_provider_surface_for_config(
    config: &VerletAppServerConfig,
    metadata_store: &SqliteMetadataStore,
) -> VerletResult<AgentManifestProviderSurface> {
    agent_manifest_provider_surface_from_parts(
        &config.provider,
        &config.model_provider,
        &config.model,
        metadata_store,
    )
    .await
}

async fn agent_manifest_provider_surface_from_parts(
    provider_config: &AppServerProviderConfig,
    model_provider: &str,
    model: &str,
    metadata_store: &SqliteMetadataStore,
) -> VerletResult<AgentManifestProviderSurface> {
    match provider_config {
        AppServerProviderConfig::CatalogOpenAIChatCompletions { provider_id, .. } => {
            let provider = metadata_store
                .get_provider(provider_id)
                .await
                .map_err(provider_store_error)?
                .ok_or_else(|| {
                    VerletError::RuntimeFactory(format!(
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

fn metadata_store_error(err: crate::MetadataStoreError) -> VerletError {
    VerletError::RuntimeFactory(err.to_string())
}

fn secret_store_error(err: SecretStoreError) -> VerletError {
    VerletError::RuntimeFactory(format!("secret store failed: {err}"))
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

async fn open_and_seed_metadata_store(path: impl AsRef<Path>) -> VerletResult<SqliteMetadataStore> {
    let store = SqliteMetadataStore::open(path)
        .await
        .map_err(metadata_store_error)?;
    seed_default_llm_providers(&store)
        .await
        .map_err(provider_store_error)?;
    Ok(store)
}

async fn sync_catalog_provider_identity(
    config: &mut VerletAppServerConfig,
    provider_store: &SqliteMetadataStore,
) -> VerletResult<()> {
    if let AppServerProviderConfig::CatalogOpenAIChatCompletions {
        provider_id, model, ..
    } = &config.provider
    {
        let provider = provider_store
            .get_provider(provider_id)
            .await
            .map_err(provider_store_error)?
            .ok_or_else(|| {
                VerletError::RuntimeFactory(format!(
                    "catalog provider {provider_id:?} is not in the provider metadata store"
                ))
            })?;
        config.model_provider = provider.provider_id.clone();
        config.model = selected_catalog_model_id(&provider, model.as_deref())?;
    }
    Ok(())
}

async fn runtime_factory_from_config(
    config: &VerletAppServerConfig,
    provider_store: &SqliteMetadataStore,
    auth_store: &SqliteMetadataStore,
) -> VerletResult<Arc<dyn crate::AgentRuntimeFactory>> {
    match &config.provider {
        AppServerProviderConfig::LocalOffline => {
            let provider = config.model_provider.clone();
            let model = config.model.clone();
            let runtime_config = AgentLoopConfig::new(
                ProviderApi::Other(provider.clone()),
                provider.clone(),
                model.clone(),
            );
            let secret_resolver = secret_resolver_from_config(config).await?;
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
                    VerletError::RuntimeFactory(format!(
                        "failed to build Bifrost OpenAI provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = AgentLoopConfig::new(
                ProviderApi::OpenAIResponses,
                APP_SERVER_BIFROST_PROVIDER,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config).await?;
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
                VerletError::RuntimeFactory(format!(
                    "failed to build OpenAI Chat Completions provider client: {err}"
                ))
            })?);
            let mut runtime_config = AgentLoopConfig::new(
                ProviderApi::OpenAIChatCompletions,
                provider.clone(),
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config).await?;
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
                    VerletError::RuntimeFactory(format!(
                        "failed to build Anthropic Messages provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = AgentLoopConfig::new(
                ProviderApi::AnthropicMessages,
                APP_SERVER_ANTHROPIC_PROVIDER,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config).await?;
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
                VerletError::RuntimeFactory(format!(
                    "failed to build Anthropic Bedrock provider client: {err}"
                ))
            })?);
            let mut runtime_config = AgentLoopConfig::new(
                ProviderApi::AnthropicMessages,
                APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            let secret_resolver = secret_resolver_from_config(config).await?;
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
            )
            .await?;
            let adapter: Arc<dyn ProviderWireAdapter> = Arc::new(OpenAIChatCompletionsAdapter);
            let client = Arc::new(ProviderHttpClient::new(resolved.endpoint, adapter).map_err(
                |err| {
                    VerletError::RuntimeFactory(format!(
                        "failed to build catalog OpenAI Chat Completions provider client: {err}"
                    ))
                },
            )?);
            let secret_resolver = secret_resolver_from_config(config).await?;
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
    runtime_config: AgentLoopConfig,
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
    runtime_config: AgentLoopConfig,
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
        AgentManifestPlacementBinding::default(),
        None,
        Arc::new(AtomicBool::new(false)),
    )
}

pub(crate) fn runtime_factory_from_provider_parts_with_app_paths(
    runtime_config: AgentLoopConfig,
    client: Arc<dyn ProviderClient>,
    // lexicon-allow: capsule - existing app-server config type name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    config: &VerletAppServerConfig,
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
        config.default_placement.clone(),
        config.default_workspace.clone(),
        Arc::clone(&config.remote_event_store_served),
    )
}

fn runtime_factory_from_provider_parts_with_store_paths(
    runtime_config: AgentLoopConfig,
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
    default_placement: AgentManifestPlacementBinding,
    default_workspace: Option<AgentManifestWorkspaceBinding>,
    remote_event_store_served: Arc<AtomicBool>,
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
        default_placement,
        default_workspace,
        remote_event_store_served,
    })
}

async fn secret_resolver_from_config(
    config: &VerletAppServerConfig,
) -> VerletResult<Option<Arc<dyn SecretResolver>>> {
    let store = SqliteSecretStore::open(config.user_metadata_store_path())
        .await
        .map_err(secret_store_error)?;
    Ok(Some(Arc::new(store)))
}

fn websocket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
}

#[allow(clippy::result_large_err)]
async fn accept_authenticated_websocket<S>(
    stream: S,
) -> Result<WebSocketStream<S>, tokio_tungstenite::tungstenite::Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let upgrade = accept_hdr_async_with_config(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if let Some(protocol) = request
                .headers()
                .get_all(SEC_WEBSOCKET_PROTOCOL)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .flat_map(|value| value.split(',').map(str::trim))
                .find(|protocol| console_protocol_token(protocol).is_some())
                .and_then(|protocol| HeaderValue::from_str(protocol).ok())
            {
                response
                    .headers_mut()
                    .insert(SEC_WEBSOCKET_PROTOCOL, protocol);
            }
            Ok(response)
        },
        Some(websocket_config()),
    );
    tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, upgrade)
        .await
        .map_err(|_| {
            tokio_tungstenite::tungstenite::Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "Verlet app-server websocket upgrade timed out",
            ))
        })?
}

async fn bind_websocket_listener(addr: SocketAddr) -> VerletResult<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(VerletError::RuntimeFactory(format!(
            "app-server websocket listen address {addr} is not loopback; configure websocket auth before binding non-loopback addresses"
        )));
    }
    TcpListener::bind(addr).await.map_err(|err| {
        VerletError::RuntimeFactory(format!(
            "failed to bind Verlet app-server websocket listener {addr}: {err}"
        ))
    })
}

struct HttpRequestHead {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
}

async fn peek_http_request(stream: &TcpStream) -> VerletResult<Option<HttpRequestHead>> {
    let mut request = [0_u8; MAX_HTTP_REQUEST_HEADER_BYTES];
    let inspected = tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, async {
        loop {
            let len = stream.peek(&mut request).await.map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to inspect Verlet app-server tcp request: {err}"
                ))
            })?;
            if len == 0 {
                return Ok(None);
            }
            if request[..len].windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                return Ok(parse_http_request_head(&request[..len]));
            }
            if len == request.len() {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    request.fill(0);
    match inspected {
        Ok(result) => result,
        Err(_) => Ok(None),
    }
}

#[cfg(unix)]
async fn peek_unix_http_request(stream: &UnixStream) -> VerletResult<Option<HttpRequestHead>> {
    let mut request = [0_u8; MAX_HTTP_REQUEST_HEADER_BYTES];
    let inspected = tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, async {
        loop {
            stream.readable().await.map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to inspect Verlet app-server unix request: {err}"
                ))
            })?;
            let len = unsafe {
                libc::recv(
                    stream.as_raw_fd(),
                    request.as_mut_ptr().cast(),
                    request.len(),
                    libc::MSG_PEEK,
                )
            };
            if len < 0 {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::WouldBlock {
                    continue;
                }
                return Err(VerletError::RuntimeFactory(format!(
                    "failed to inspect Verlet app-server unix request: {error}"
                )));
            }
            if len == 0 {
                return Ok(None);
            }
            let len = len as usize;
            if request[..len].windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                return Ok(parse_http_request_head(&request[..len]));
            }
            if len == request.len() {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await;
    request.fill(0);
    match inspected {
        Ok(result) => result,
        Err(_) => Ok(None),
    }
}

fn parse_http_request_head(bytes: &[u8]) -> Option<HttpRequestHead> {
    let text = std::str::from_utf8(bytes).ok()?;
    let header_end = text.find("\r\n\r\n")?;
    let mut lines = text[..header_end].split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let path = target
        .split_once('?')
        .map(|(path, _)| path)
        .unwrap_or(target)
        .to_string();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    Some(HttpRequestHead {
        method,
        path,
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

fn request_bearer_token(request: &HttpRequestHead) -> Option<(&str, BoundarySurface)> {
    if let Some(token) = request
        .headers
        .iter()
        .filter(|(name, _)| name == "authorization")
        .find_map(|(_, value)| authorization_bearer_token(value))
    {
        return Some((token, BoundarySurface::Websocket));
    }
    request
        .headers
        .iter()
        .filter(|(name, _)| name == "sec-websocket-protocol")
        .flat_map(|(_, value)| value.split(',').map(str::trim))
        .find_map(|protocol| {
            console_protocol_token(protocol).map(|token| (token, BoundarySurface::Console))
        })
}

fn authorization_bearer_token(value: &str) -> Option<&str> {
    let mut parts = value.split_ascii_whitespace();
    let scheme = parts.next()?;
    let token = parts.next()?;
    (scheme.eq_ignore_ascii_case("bearer") && parts.next().is_none()).then_some(token)
}

fn console_protocol_token(protocol: &str) -> Option<&str> {
    protocol
        .strip_prefix(CONSOLE_TOKEN_PROTOCOL_PREFIX)
        .or_else(|| protocol.strip_prefix(LEGACY_CONSOLE_TOKEN_PROTOCOL_PREFIX))
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_whitespace))
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

async fn write_http_response<S>(
    stream: &mut S,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> VerletResult<()>
where
    S: AsyncWrite + Unpin,
{
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.map_err(|err| {
        VerletError::RuntimeFactory(format!(
            "failed to write Verlet app-server HTTP response: {err}"
        ))
    })?;
    stream.write_all(body).await.map_err(|err| {
        VerletError::RuntimeFactory(format!(
            "failed to write Verlet app-server HTTP response body: {err}"
        ))
    })?;
    Ok(())
}

async fn consume_http_request_headers<S>(stream: &mut S) -> VerletResult<()>
where
    S: AsyncRead + Unpin,
{
    let mut chunk = [0_u8; 512];
    let consumed = tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, async {
        let mut matched = 0;
        let mut total = 0;
        loop {
            let len = stream.read(&mut chunk).await.map_err(|err| {
                VerletError::RuntimeFactory(format!(
                    "failed to read Verlet app-server HTTP request: {err}"
                ))
            })?;
            if len == 0 {
                return Ok(());
            }
            total += len;
            let mut complete = false;
            for byte in &chunk[..len] {
                matched = match (matched, *byte) {
                    (0, b'\r') => 1,
                    (1, b'\n') => 2,
                    (2, b'\r') => 3,
                    (3, b'\n') => {
                        complete = true;
                        4
                    }
                    (_, b'\r') => 1,
                    _ => 0,
                };
                if complete {
                    break;
                }
            }
            chunk[..len].fill(0);
            if complete || total >= MAX_HTTP_REQUEST_HEADER_BYTES {
                return Ok(());
            }
        }
    })
    .await;
    chunk.fill(0);
    match consumed {
        Ok(result) => result,
        Err(_) => Ok(()),
    }
}

fn credential_ref(auth: &AuthenticationPath) -> String {
    match auth {
        AuthenticationPath::Credential { credential_id } => credential_id.clone(),
        AuthenticationPath::PeerUid { uid } => format!("peer_uid:{uid}"),
    }
}

#[cfg(unix)]
fn current_effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn current_effective_uid() -> u32 {
    0
}

fn split_websocket_listen_url(value: &str) -> (&str, &str) {
    match value.find('/') {
        Some(index) => (&value[..index], &value[index..]),
        None => (value, ""),
    }
}

#[cfg(unix)]
fn prepare_unix_socket_path(path: &Path) -> VerletResult<()> {
    if let Some(parent) = path.parent() {
        let parent_existed = parent.exists();
        std::fs::create_dir_all(parent).map_err(|err| {
            VerletError::RuntimeFactory(format!(
                "failed to create app-server socket directory {}: {err}",
                parent.display()
            ))
        })?;
        if !parent_existed {
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
                |err| {
                    VerletError::RuntimeFactory(format!(
                        "failed to secure app-server socket directory {}: {err}",
                        parent.display()
                    ))
                },
            )?;
        }
    }
    if path.exists() {
        let metadata = std::fs::symlink_metadata(path).map_err(|err| {
            VerletError::RuntimeFactory(format!(
                "failed to inspect existing app-server socket {}: {err}",
                path.display()
            ))
        })?;
        if metadata.file_type().is_file() || metadata.file_type().is_dir() {
            return Err(VerletError::RuntimeFactory(format!(
                "refusing to replace non-socket app-server path {}",
                path.display()
            )));
        }
        std::fs::remove_file(path).map_err(|err| {
            VerletError::RuntimeFactory(format!(
                "failed to remove stale app-server socket {}: {err}",
                path.display()
            ))
        })?;
    }
    Ok(())
}
