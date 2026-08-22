use crate::daemon::identity::IdentityAuthority as _;
use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
#[cfg(unix)]
use std::os::unix::io::AsRawFd as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use verlet_metadata::provider_store::LlmProviderCatalogStore as _;
pub mod connection;
mod default_manifest;
pub mod instance;
pub mod lifecycle;
pub(crate) mod live_models;
pub(crate) mod model_catalog;
mod orchestrator_boundary;
mod subscriptions;
#[cfg(test)]
mod tests;
pub mod threads;

pub const APP_SERVER_LOCAL_PROVIDER: &str = "local_offline";
pub const APP_SERVER_LOCAL_MODEL: &str = "echo";
pub const APP_SERVER_BIFROST_PROVIDER: &str = "openai";
pub const APP_SERVER_BIFROST_MODEL: &str = "openai/gpt-5.5";
pub const APP_SERVER_OPENAI_COMPATIBLE_PROVIDER: &str = "openai_compatible";
pub const APP_SERVER_OPENAI_COMPATIBLE_MODEL: &str =
    verlet_metadata::provider_store::OPENAI_COMPATIBLE_DEFAULT_MODEL;
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
const HTTP_REQUEST_HEADER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const CONSOLE_TOKEN_PROTOCOL_PREFIX: &str = "verlet-console-token.";
const DEFAULT_BLOB_REGISTRY_ROOT: &str = ".verlet/blobs";
const CONSOLE_CREDENTIAL_ID_FILE: &str = "console-credential-id";
const DEFAULT_COMMAND_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_COMMAND_OUTPUT_CAP_BYTES: usize = 1024 * 1024;
const DEFAULT_OPERATION_REGISTRY_ROOT: &str = ".verlet/operations";
const METADATA_DB_NAME: &str = "metadata.sqlite3";
pub const ENDPOINT_RECORD_NAME: &str = "endpoint.json";
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
    Unix(std::path::PathBuf),
    WebSocket(std::net::SocketAddr),
}

impl AppServerListenAddr {
    pub fn parse(value: &str) -> crate::kernel::runtime_host::VerletResult<Self> {
        if let Some(path) = value.strip_prefix("unix://") {
            if path.is_empty() {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "unix app-server listen address requires a path".to_string(),
                ));
            }
            return Ok(Self::Unix(std::path::PathBuf::from(path)));
        }

        if let Some(rest) = value.strip_prefix("ws://") {
            let (authority, path) = split_websocket_listen_url(rest);
            if authority.is_empty() {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    "websocket app-server listen address requires host:port".to_string(),
                ));
            }
            if !matches!(path, "" | "/rpc") {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!("unsupported app-server websocket path {path:?}; expected /rpc"),
                ));
            }
            let addr = authority.parse::<std::net::SocketAddr>().map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "invalid app-server websocket listen address {authority:?}: {err}"
                ))
            })?;
            return Ok(Self::WebSocket(addr));
        }

        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "unsupported app-server listen address {value:?}; expected unix://PATH or ws://HOST:PORT[/rpc]"
            ),
        ))
    }

    pub fn display(&self) -> String {
        match self {
            Self::Unix(path) => format!("unix://{}", path.display()),
            Self::WebSocket(addr) => format!("ws://{addr}/rpc"),
        }
    }
}

#[derive(Debug)]
pub struct VerletAppServerConfig {
    pub listen: AppServerListenAddr,
    pub runtime_home: std::path::PathBuf,
    pub state_home: std::path::PathBuf,
    pub user_state_home: std::path::PathBuf,
    pub cwd: std::path::PathBuf,
    pub tenant_id: String,
    pub user_id: String,
    pub identity_mode: crate::daemon::identity::IdentityMode,
    pub console_principal: Option<crate::daemon::identity::PrincipalId>,
    pub model: String,
    pub model_provider: String,
    /// True only when a user-supplied model (CLI flag, config file, or host
    /// request) was applied; the launch defaults leave it false so
    /// `model/list` can hide the offline echo pair (EMO-575).
    pub model_explicit: bool,
    pub provider: AppServerProviderConfig,
    pub capsule_bindings: CapsuleBindingsConfig,
    pub agent_registry_root: std::path::PathBuf,
    pub blob_registry_root: std::path::PathBuf,
    pub skill_registry_root: std::path::PathBuf,
    /// Placement-lease epoch presented by every journal store handle opened
    /// by this daemon/app-server instance.
    pub lease_epoch: u64,
    /// Deployment placement used when a bind surface does not override it.
    pub default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding,
    /// Host workspace used when a requiring manifest has no bind override.
    pub default_workspace: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    /// Generation-local capability bit. The daemon flips this only after the
    /// configured sync listener has bound successfully.
    pub remote_event_store_served: std::sync::Arc<std::sync::atomic::AtomicBool>,
    pub console_assets: Option<ConsoleAssetConfig>,
    root_reservation: Option<instance::InstanceRootReservation>,
    /// Per-instance replacements for process-state reads at depth
    /// (EMO-552): provider auth, hook shell, process-id source.
    pub instance_environment: instance::InstanceEnvironment,
}

impl VerletAppServerConfig {
    pub fn local(listen: AppServerListenAddr, cwd: impl Into<std::path::PathBuf>) -> Self {
        let root = std::env::temp_dir().join(format!("verlet-app-server-{}", uuid::Uuid::now_v7()));
        let identity = crate::daemon::daemon_config::synthesized_local_daemon_identity_config();
        let cwd = cwd.into();
        let project_storage_root = std::path::PathBuf::from(".verlet");
        let mut config = Self {
            listen,
            runtime_home: root.join("runtime"),
            state_home: root.join("state"),
            user_state_home: root.join("user-state"),
            cwd,
            tenant_id: String::new(),
            user_id: String::new(),
            identity_mode: crate::daemon::identity::IdentityMode::Local,
            console_principal: None,
            model: APP_SERVER_LOCAL_MODEL.to_string(),
            model_provider: APP_SERVER_LOCAL_PROVIDER.to_string(),
            model_explicit: false,
            provider: AppServerProviderConfig::LocalOffline,
            // lexicon-allow: capsule - existing app-server operation binding field.
            capsule_bindings: CapsuleBindingsConfig::default()
                .with_registry_root(project_storage_root.join("operations")),
            agent_registry_root: project_storage_root.join("agents"),
            blob_registry_root: project_storage_root.join("blobs"),
            skill_registry_root: project_storage_root.join("skills"),
            lease_epoch: 0,
            default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding::default(
            ),
            default_workspace: None,
            remote_event_store_served: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            console_assets: None,
            root_reservation: None,
            instance_environment: instance::InstanceEnvironment::standalone(),
        };
        config.apply_daemon_identity_config(&identity);
        config
    }

    /// Hosted-instance config (EMO-552): explicit absolute roots + fully
    /// injected environment; no listener (`listen` is set but the host
    /// never binds it — the host either hands over a selected TCP stream or
    /// uses `dispatch_authenticated_json_rpc` for an already-authenticated
    /// in-process request). Construction canonicalizes the roots and reserves
    /// them process-wide before any store opens;
    /// overlapping a live instance's roots is a loud error. None of the
    /// cwd/XDG defaulting of [`VerletAppServerConfig::local`] applies.
    pub fn hosted(
        roots: instance::InstanceRoots,
        environment: instance::InstanceEnvironment,
        cwd: impl Into<std::path::PathBuf>,
        identity: &crate::daemon::identity::VerletDaemonIdentityConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let cwd = cwd.into();
        environment.validate_hosted()?;
        if !cwd.is_absolute() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!("hosted instance cwd must be absolute: {}", cwd.display()),
            ));
        }
        let reservation = instance::reserve_instance_roots(&roots)?;
        let canonical_roots = reservation.canonical_roots();
        let runtime_home = canonical_roots[0].clone();
        let state_home = canonical_roots[1].clone();
        let user_state_home = canonical_roots[2].clone();
        let agent_registry_root = canonical_roots[3].clone();
        let blob_registry_root = canonical_roots[4].clone();
        let skill_registry_root = canonical_roots[5].clone();
        let mut config = Self {
            listen: AppServerListenAddr::Unix(instance::instance_unix_socket_path(&state_home)?),
            runtime_home: runtime_home.clone(),
            state_home,
            user_state_home,
            cwd,
            tenant_id: String::new(),
            user_id: String::new(),
            identity_mode: crate::daemon::identity::IdentityMode::Local,
            console_principal: None,
            model: APP_SERVER_LOCAL_MODEL.to_string(),
            model_provider: APP_SERVER_LOCAL_PROVIDER.to_string(),
            model_explicit: false,
            provider: AppServerProviderConfig::LocalOffline,
            capsule_bindings: CapsuleBindingsConfig::default()
                .with_registry_root(runtime_home.join("operations")),
            agent_registry_root,
            blob_registry_root,
            skill_registry_root,
            lease_epoch: 0,
            default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding::default(
            ),
            default_workspace: None,
            remote_event_store_served: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            console_assets: None,
            root_reservation: Some(reservation),
            instance_environment: environment,
        };
        config.apply_daemon_identity_config(identity);
        Ok(config)
    }

    /// Project a daemon identity config onto this app-server config. This is
    /// the single seam through which mode, tenant, and console principal reach
    /// the server; the boundary authority is initialized from these fields.
    pub fn apply_daemon_identity_config(
        &mut self,
        identity: &crate::daemon::identity::VerletDaemonIdentityConfig,
    ) {
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
        self.model_explicit = true;
        self.provider = AppServerProviderConfig::BifrostOpenAIResponses {
            base_url: base_url.into(),
            api_key: api_key.into(),
            model,
            max_tokens: 4096,
            stream: true,
        };
        self
    }

    pub fn with_openai_codex(mut self, model: impl Into<String>) -> Self {
        let model = model.into();
        self.model = model.clone();
        self.model_provider = verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID.to_string();
        self.model_explicit = true;
        self.provider = AppServerProviderConfig::OpenAICodex {
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
        self.model_explicit = true;
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
        self.model_explicit = true;
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
        self.model_explicit = true;
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
            self.model_explicit = true;
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
        root: impl Into<std::path::PathBuf>,
        session_token: impl Into<String>,
    ) -> Self {
        self.console_assets = Some(ConsoleAssetConfig {
            root: root.into(),
            session_token: session_token.into(),
        });
        self
    }

    pub fn metadata_store_path(&self) -> std::path::PathBuf {
        self.state_home.join(METADATA_DB_NAME)
    }

    pub fn user_metadata_store_path(&self) -> std::path::PathBuf {
        self.user_state_home.join(METADATA_DB_NAME)
    }

    pub fn provider_metadata_store_path(&self) -> std::path::PathBuf {
        self.user_metadata_store_path()
    }

    fn validate_endpoint_availability(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        for root in self.endpoint_record_roots()? {
            instance::refuse_live_instance(&root)?;
        }
        Ok(())
    }

    fn endpoint_record_roots(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<std::path::PathBuf>> {
        let mut roots = Vec::with_capacity(2);
        for root in [&self.state_home, &self.user_state_home] {
            let absolute = instance::absolute_path(root)?;
            let root = std::fs::canonicalize(&absolute).unwrap_or(absolute);
            if !roots.iter().any(|existing| existing == &root) {
                roots.push(root);
            }
        }
        Ok(roots)
    }

    fn instance_endpoint(
        &self,
        state_root: &std::path::Path,
    ) -> crate::kernel::runtime_host::VerletResult<instance::InstanceEndpoint> {
        let unix_socket = match &self.listen {
            AppServerListenAddr::Unix(path) => {
                let path = instance::absolute_path(path)?;
                if instance::unix_socket_path_fits(&path) {
                    path
                } else {
                    instance::instance_unix_socket_path(state_root)?
                }
            }
            AppServerListenAddr::WebSocket(_) => instance::instance_unix_socket_path(state_root)?,
        };
        let ws_url = match &self.listen {
            AppServerListenAddr::WebSocket(addr) if addr.port() != 0 => {
                Some(AppServerListenAddr::WebSocket(*addr).display())
            }
            AppServerListenAddr::Unix(_) | AppServerListenAddr::WebSocket(_) => None,
        };
        Ok(instance::InstanceEndpoint {
            pid: std::process::id(),
            unix_socket,
            ws_url,
            started_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            instance_id: uuid::Uuid::now_v7().to_string(),
        })
    }

    fn validate_root_reservation(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        let Some(reservation) = &self.root_reservation else {
            return Ok(());
        };
        self.instance_environment.validate_hosted()?;
        if !self.cwd.is_absolute() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "hosted instance cwd must be absolute: {}",
                    self.cwd.display()
                ),
            ));
        }
        let configured_roots = [
            ("runtime_home", &self.runtime_home),
            ("state_home", &self.state_home),
            ("user_state_home", &self.user_state_home),
            ("agent_registry_root", &self.agent_registry_root),
            ("blob_registry_root", &self.blob_registry_root),
            ("skill_registry_root", &self.skill_registry_root),
        ];
        for ((name, configured), reserved) in configured_roots
            .into_iter()
            .zip(reservation.canonical_roots())
        {
            if configured != reserved {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "hosted instance root {name} changed after reservation: configured {}, reserved {}",
                        configured.display(),
                        reserved.display()
                    ),
                ));
            }
        }
        let expected_operation_registry_root = self.runtime_home.join("operations");
        if self.capsule_bindings.registry_root.as_deref()
            != Some(expected_operation_registry_root.as_path())
        {
            let configured = self
                .capsule_bindings
                .registry_root
                .as_deref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<none>".to_string());
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "hosted instance operation registry root changed after reservation: configured {configured}, expected {}",
                    expected_operation_registry_root.display()
                ),
            ));
        }
        Ok(())
    }

    pub(crate) fn is_hosted(&self) -> bool {
        self.root_reservation.is_some()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ConsoleAssetConfig {
    pub root: std::path::PathBuf,
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

fn operation_registry_root_for_kernel_publish(
    config: &VerletAppServerConfig,
) -> Option<&std::path::Path> {
    let registry_root = config.capsule_bindings.registry_root.as_deref()?;
    let default_registry_root = config
        .cwd
        .join(std::path::Path::new(DEFAULT_OPERATION_REGISTRY_ROOT));
    if registry_root == default_registry_root && !registry_root.exists() {
        return None;
    }
    Some(registry_root)
}

#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapsuleBindingsConfig {
    #[serde(
        default,
        alias = "registry_root",
        skip_serializing_if = "Option::is_none"
    )]
    pub registry_root: Option<std::path::PathBuf>,
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
    pub fn with_registry_root(mut self, registry_root: impl Into<std::path::PathBuf>) -> Self {
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

#[derive(Clone)]
pub enum AppServerProviderConfig {
    LocalOffline,
    OpenAICodex {
        model: String,
        max_tokens: u32,
        stream: bool,
    },
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

impl std::fmt::Debug for AppServerProviderConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LocalOffline => f.write_str("LocalOffline"),
            Self::OpenAICodex {
                model,
                max_tokens,
                stream,
            } => f
                .debug_struct("OpenAICodex")
                .field("model", model)
                .field("max_tokens", max_tokens)
                .field("stream", stream)
                .finish(),
            Self::BifrostOpenAIResponses {
                base_url,
                model,
                max_tokens,
                stream,
                ..
            } => f
                .debug_struct("BifrostOpenAIResponses")
                .field("base_url", base_url)
                .field("api_key", &"<redacted>")
                .field("model", model)
                .field("max_tokens", max_tokens)
                .field("stream", stream)
                .finish(),
            Self::OpenAIChatCompletions {
                provider,
                base_url,
                model,
                max_tokens,
                stream,
                headers,
                ..
            } => f
                .debug_struct("OpenAIChatCompletions")
                .field("provider", provider)
                .field("base_url", base_url)
                .field("api_key", &"<redacted>")
                .field("model", model)
                .field("max_tokens", max_tokens)
                .field("stream", stream)
                .field(
                    "headers",
                    &format_args!("<redacted: {} entries>", headers.len()),
                )
                .finish(),
            Self::AnthropicMessages {
                base_url,
                model,
                max_tokens,
                stream,
                ..
            } => f
                .debug_struct("AnthropicMessages")
                .field("base_url", base_url)
                .field("api_key", &"<redacted>")
                .field("model", model)
                .field("max_tokens", max_tokens)
                .field("stream", stream)
                .finish(),
            Self::AnthropicBedrock {
                region,
                base_url,
                model,
                max_tokens,
                stream,
                session_token,
                ..
            } => f
                .debug_struct("AnthropicBedrock")
                .field("region", region)
                .field("base_url", base_url)
                .field("access_key_id", &"<redacted>")
                .field("secret_access_key", &"<redacted>")
                .field(
                    "session_token",
                    &session_token.as_ref().map(|_| "<redacted>"),
                )
                .field("model", model)
                .field("max_tokens", max_tokens)
                .field("stream", stream)
                .finish(),
            Self::CatalogOpenAIChatCompletions {
                provider_id,
                model,
                max_tokens,
                stream,
            } => f
                .debug_struct("CatalogOpenAIChatCompletions")
                .field("provider_id", provider_id)
                .field("model", model)
                .field("max_tokens", max_tokens)
                .field("stream", stream)
                .finish(),
        }
    }
}

#[derive(Clone)]
pub struct VerletAppServer {
    inner: std::sync::Arc<VerletAppServerInner>,
}

/// Runtime-active provider+model selection (EMO-558).
///
/// Initialized from `VerletAppServerConfig` at construction and swapped by
/// the `model/select` RPC. Never persisted: a restart returns to the
/// configured defaults. Turn starts read this instead of the construction
/// fields; a turn already in flight keeps the selection it started with.
#[derive(Clone)]
pub(crate) struct ActiveModelSelection {
    pub(crate) model: String,
    pub(crate) model_provider: String,
    pub(crate) provider: AppServerProviderConfig,
}

pub(crate) struct AppServerTurnEndpointRouter {
    state: std::sync::RwLock<AppServerTurnEndpointState>,
}

struct AppServerTurnEndpointState {
    current: Option<crate::adapters::agent_loop::ResolvedTurnEndpoint>,
    cache: std::collections::BTreeMap<
        String,
        std::collections::BTreeMap<String, crate::adapters::agent_loop::ResolvedTurnEndpoint>,
    >,
}

impl AppServerTurnEndpointRouter {
    fn new(initial: Option<crate::adapters::agent_loop::ResolvedTurnEndpoint>) -> Self {
        let mut cache = std::collections::BTreeMap::new();
        if let Some(endpoint) = &initial {
            cache
                .entry(endpoint.config.provider.clone())
                .or_insert_with(std::collections::BTreeMap::new)
                .insert(endpoint.config.model.clone(), endpoint.clone());
        }
        Self {
            state: std::sync::RwLock::new(AppServerTurnEndpointState {
                current: initial,
                cache,
            }),
        }
    }

    async fn cached(
        &self,
        provider_id: &str,
        model: &str,
    ) -> Option<crate::adapters::agent_loop::ResolvedTurnEndpoint> {
        self.read_state()
            .cache
            .get(provider_id)
            .and_then(|models| models.get(model))
            .cloned()
    }

    #[cfg(test)]
    fn preload_for_test(&self, endpoint: crate::adapters::agent_loop::ResolvedTurnEndpoint) {
        self.write_state()
            .cache
            .entry(endpoint.config.provider.clone())
            .or_insert_with(std::collections::BTreeMap::new)
            .insert(endpoint.config.model.clone(), endpoint);
    }

    async fn invalidate(&self, provider_id: &str) {
        self.write_state().cache.remove(provider_id);
    }

    async fn invalidate_model(&self, provider_id: &str, model: &str) {
        let mut state = self.write_state();
        let remove_provider = state.cache.get_mut(provider_id).is_some_and(|models| {
            models.remove(model);
            models.is_empty()
        });
        if remove_provider {
            state.cache.remove(provider_id);
        }
    }

    fn activate(&self, endpoint: crate::adapters::agent_loop::ResolvedTurnEndpoint) {
        let mut state = self.write_state();
        state
            .cache
            .entry(endpoint.config.provider.clone())
            .or_insert_with(std::collections::BTreeMap::new)
            .insert(endpoint.config.model.clone(), endpoint.clone());
        state.current = Some(endpoint);
    }

    fn read_state(&self) -> std::sync::RwLockReadGuard<'_, AppServerTurnEndpointState> {
        match self.state.read() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn write_state(&self) -> std::sync::RwLockWriteGuard<'_, AppServerTurnEndpointState> {
        match self.state.write() {
            Ok(state) => state,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

impl crate::adapters::agent_loop::TurnEndpointRouter for AppServerTurnEndpointRouter {
    fn resolve(&self) -> Option<crate::adapters::agent_loop::ResolvedTurnEndpoint> {
        self.read_state().current.clone()
    }
}

struct VerletAppServerInner {
    supervisor: crate::kernel::supervisor::VerletSupervisor,
    tasks: std::sync::Arc<crate::adapters::app_server::lifecycle::InstanceTaskSet>,
    client_connections: std::sync::Arc<ClientConnectionTracker>,
    shutdown: tokio::sync::Mutex<bool>,
    dispatch_gate: tokio::sync::RwLock<()>,
    /// Process-wide claim on this instance's roots (EMO-552). `None` for
    /// standalone-daemon construction; hosted construction holds it so the
    /// claim releases when the instance is dropped after shutdown.
    #[allow(dead_code)]
    root_reservation: Option<instance::InstanceRootReservation>,
    /// Injected environment (EMO-552): provider auth, hook shell,
    /// process-id source. The deep process-state reads resolve through
    /// this instead of `std::env`/globals.
    #[allow(dead_code)]
    instance_environment: instance::InstanceEnvironment,
    configured_listen: AppServerListenAddr,
    endpoint: instance::InstanceEndpoint,
    endpoint_record_roots: Vec<std::path::PathBuf>,
    tenant_id: String,
    user_id: String,
    identity_mode: crate::daemon::identity::IdentityMode,
    console_principal: Option<crate::daemon::identity::PrincipalId>,
    model: String,
    model_provider: String,
    /// See [`VerletAppServerConfig::model_explicit`]: gates the `model/list`
    /// launch row for pairs that exist nowhere in the store or catalog.
    model_explicit: bool,
    provider: AppServerProviderConfig,
    /// EMO-558: the live selection. The `model`/`model_provider`/`provider`
    /// fields above stay as the launch defaults; every read that must follow
    /// `model/select` (turn starts, `model/list` active flag, banner state)
    /// goes through this lock instead. Migrating those reads is EMO-558
    active_model: tokio::sync::RwLock<ActiveModelSelection>,
    turn_endpoint_router: std::sync::Arc<AppServerTurnEndpointRouter>,
    /// Serializes selection with provider/auth mutations so validation,
    /// endpoint construction, cache invalidation, and activation form one
    /// linearizable control-plane transition.
    model_mutation: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// False for injected runtime factories that do not carry the endpoint
    /// router. Such constructions must not report a model selection that their
    /// provider loop cannot honor.
    model_selection_enabled: bool,
    capsule_bindings: CapsuleBindingsConfig,
    agent_registry_root: std::path::PathBuf,
    blob_registry_root: std::path::PathBuf,
    skill_registry_root: std::path::PathBuf,
    default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding,
    default_workspace: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    remote_event_store_served: std::sync::Arc<std::sync::atomic::AtomicBool>,
    console_assets: Option<ConsoleAssetConfig>,
    identity_authority: std::sync::Arc<dyn crate::daemon::identity::IdentityAuthority>,
    identity_clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    console_credential: tokio::sync::Mutex<Option<ConsoleCredentialLease>>,
    cwd: std::path::PathBuf,
    codex_home: std::path::PathBuf,
    metadata_store_path: std::path::PathBuf,
    user_metadata_store_path: std::path::PathBuf,
    session_store_path: std::path::PathBuf,
    lease_epoch: u64,
    metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
    user_metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
    model_catalog: model_catalog::MergedModelCatalog,
    live_models: std::sync::Arc<live_models::LiveModelCache>,
    process_manager: verlet_process::live::AsyncExecutionManager,
    process_dispatcher:
        tokio::sync::OnceCell<crate::kernel::process_handle_dispatch::ProcessHandleDispatcher>,
    subscriptions:
        tokio::sync::Mutex<crate::adapters::app_server::subscriptions::AppServerSubscriptions>,
    state: tokio::sync::RwLock<crate::adapters::app_server::threads::AppServerState>,
}

struct ClientConnectionTracker {
    active: std::sync::atomic::AtomicUsize,
    changes: tokio::sync::watch::Sender<u64>,
}

impl ClientConnectionTracker {
    fn new() -> Self {
        let (changes, _) = tokio::sync::watch::channel(0);
        Self {
            active: std::sync::atomic::AtomicUsize::new(0),
            changes,
        }
    }

    fn enter(self: &std::sync::Arc<Self>) -> ClientConnectionGuard {
        self.active
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        self.changed();
        ClientConnectionGuard {
            tracker: std::sync::Arc::clone(self),
        }
    }

    #[cfg(unix)]
    async fn accept_unix(
        self: &std::sync::Arc<Self>,
        listener: &tokio::net::UnixListener,
    ) -> std::io::Result<(
        tokio::net::UnixStream,
        tokio::net::unix::SocketAddr,
        ClientConnectionGuard,
    )> {
        let (stream, peer) = listener.accept().await?;
        let connection = self.enter();
        Ok((stream, peer, connection))
    }

    async fn accept_tcp(
        self: &std::sync::Arc<Self>,
        listener: &tokio::net::TcpListener,
    ) -> std::io::Result<(
        tokio::net::TcpStream,
        std::net::SocketAddr,
        ClientConnectionGuard,
    )> {
        let (stream, peer) = listener.accept().await?;
        let connection = self.enter();
        Ok((stream, peer, connection))
    }

    async fn wait_for_idle_timeout(&self, idle_timeout: std::time::Duration) {
        let mut changes = self.changes.subscribe();
        loop {
            if self.active.load(std::sync::atomic::Ordering::Acquire) == 0 {
                let timeout = tokio::time::sleep(idle_timeout);
                tokio::pin!(timeout);
                tokio::select! {
                    () = &mut timeout => {
                        let still_idle = self.active.load(std::sync::atomic::Ordering::Acquire) == 0;
                        let unchanged = !changes.has_changed().unwrap_or(false);
                        if still_idle && unchanged {
                            return;
                        }
                    }
                    changed = changes.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            } else if changes.changed().await.is_err() {
                return;
            }
        }
    }

    fn changed(&self) {
        self.changes.send_modify(|version| *version += 1);
    }
}

struct ClientConnectionGuard {
    tracker: std::sync::Arc<ClientConnectionTracker>,
}

impl Drop for ClientConnectionGuard {
    fn drop(&mut self) {
        let decremented = self.tracker.active.fetch_update(
            std::sync::atomic::Ordering::AcqRel,
            std::sync::atomic::Ordering::Acquire,
            |active| active.checked_sub(1),
        );
        debug_assert!(decremented.is_ok(), "client connection count underflow");
        if decremented.is_ok() {
            self.tracker.changed();
        }
    }
}

impl VerletAppServerInner {
    fn agent_registry(&self) -> crate::agent::manifest::LocalAgentRegistry {
        crate::agent::manifest::LocalAgentRegistry::new(self.agent_registry_root.clone())
            .with_blob_registry_root(self.blob_registry_root.clone())
    }
}

struct ConsoleCredentialLease {
    credential_id: String,
    principal_id: crate::daemon::identity::PrincipalId,
    record_path: std::path::PathBuf,
}

struct SessionCloseWitness {
    authority: std::sync::Arc<dyn crate::daemon::identity::IdentityAuthority>,
    clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    tasks: std::sync::Arc<crate::adapters::app_server::lifecycle::InstanceTaskSet>,
    session_id: String,
    armed: bool,
}

impl SessionCloseWitness {
    fn new(
        authority: std::sync::Arc<dyn crate::daemon::identity::IdentityAuthority>,
        clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
        tasks: std::sync::Arc<crate::adapters::app_server::lifecycle::InstanceTaskSet>,
        session_id: String,
    ) -> Self {
        Self {
            authority,
            clock,
            tasks,
            session_id,
            armed: true,
        }
    }

    async fn close(&mut self) -> crate::kernel::runtime_host::VerletResult<()> {
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
        let authority = std::sync::Arc::clone(&self.authority);
        let session_id = self.session_id.clone();
        let closed_at_ms = self.clock.now().timestamp_millis();
        if !self.tasks.spawn_from_drop(async move {
            if let Err(error) = authority
                .witness_session_closed(&session_id, closed_at_ms)
                .await
            {
                eprintln!("failed to witness aborted Verlet app-server session: {error}");
            }
        }) {
            eprintln!(
                "could not schedule aborted Verlet app-server session cleanup after instance shutdown"
            );
        }
    }
}

impl Drop for VerletAppServerInner {
    fn drop(&mut self) {
        instance::unregister_instance_endpoint(&self.endpoint.instance_id);
        let Some(credential) = self.console_credential.get_mut().take() else {
            return;
        };
        eprintln!(
            "Verlet app-server dropped before shutdown completed; console credential {} cleanup may be incomplete",
            credential.credential_id
        );
    }
}

struct AppServerProcessHandleIngress {
    app: std::sync::Weak<VerletAppServerInner>,
}

#[async_trait::async_trait]
impl crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink
    for AppServerProcessHandleIngress
{
    async fn submit_process_handle_envelope(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let inner = self.app.upgrade().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(
                "app-server stopped before process handle ingress settled".to_string(),
            )
        })?;
        crate::daemon::daemon_io::VerletDaemonIoBridge::from_app_server(&VerletAppServer { inner })
            .submit_durable_handle_envelope(envelope)
            .await
            .map(|_| ())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(err.to_string())
            })
    }
}

#[derive(Clone, Debug)]
struct AppServerOfflineProviderClient {
    capabilities: verlet_provider::ProviderCapabilityRecord,
}

impl AppServerOfflineProviderClient {
    fn new(provider_family: impl Into<String>, model: impl Into<String>) -> Self {
        let mut capabilities =
            verlet_provider::ProviderCapabilityRecord::local_offline(provider_family, model);
        capabilities.supports_tools = true;
        capabilities.tool_result_constraints =
            verlet_provider::ProviderToolResultConstraints::open_tool_results();
        capabilities
            .supported_abi_projections
            .insert(verlet_provider::ProviderAbiProjection::LlmTool);
        Self { capabilities }
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for AppServerOfflineProviderClient {
    fn capabilities(&self) -> Option<verlet_provider::ProviderCapabilityRecord> {
        Some(self.capabilities.clone())
    }

    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        self.capabilities
            .validate_request(request, verlet_provider::ProviderRequestMode::Complete)?;
        let last_user_text = request
            .messages
            .iter()
            .rev()
            .find_map(|message| match message {
                verlet_history::CanonicalMessage::User { content, .. } => {
                    let text = text_from_canonical_content(content);
                    (!text.is_empty()).then_some(text)
                }
                verlet_history::CanonicalMessage::Assistant { .. }
                | verlet_history::CanonicalMessage::ToolResult { .. } => None,
            })
            .unwrap_or_default();
        Ok(verlet_provider::ProviderResponse {
            content: vec![verlet_history::CanonicalContent::text(format!(
                "local:{last_user_text}"
            ))],
            usage: verlet_history::CanonicalUsage {
                input_tokens: request.messages.len() as u64,
                output_tokens: last_user_text.len() as u64,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: verlet_history::CanonicalStopReason::EndTurn,
        })
    }
}

impl VerletAppServer {
    /// Same constructor as [`Self::new`]; the name survives from before
    /// identity modes existed and remains the conventional entry point for
    /// configs built with [`VerletAppServerConfig::local`].
    pub async fn new_local(
        config: VerletAppServerConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        Self::new(config).await
    }

    pub async fn new(
        mut config: VerletAppServerConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        config.validate_root_reservation()?;
        config.validate_endpoint_availability()?;
        let metadata_store = open_and_seed_metadata_store(config.metadata_store_path()).await?;
        let user_metadata_store =
            open_and_seed_metadata_store(config.user_metadata_store_path()).await?;
        reconcile_catalog_provider_records(&config, &metadata_store).await?;
        sync_catalog_provider_identity(&mut config, &metadata_store).await?;
        crate::adapters::app_server::threads::normalize_registry_roots(&mut config);
        let initial_endpoint = resolved_turn_endpoint_from_provider_config(
            &config.provider,
            &config.model_provider,
            &config.model,
            &metadata_store,
            &user_metadata_store,
            &config.instance_environment.provider_auth.resolve(),
        )
        .await?;
        let turn_endpoint_router = std::sync::Arc::new(AppServerTurnEndpointRouter::new(Some(
            initial_endpoint.clone(),
        )));
        let runtime_factory = runtime_factory_from_config(
            &config,
            initial_endpoint,
            std::sync::Arc::clone(&turn_endpoint_router),
        )
        .await?;
        Self::with_runtime_factory_and_metadata_stores_and_router(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
            turn_endpoint_router,
        )
        .await
    }

    pub async fn with_runtime_factory(
        config: VerletAppServerConfig,
        runtime_factory: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
        >,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        config.validate_root_reservation()?;
        config.validate_endpoint_availability()?;
        let metadata_store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        let user_metadata_store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
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
        runtime_factory: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
        >,
        decorate: impl FnOnce(
            std::sync::Arc<dyn verlet_history::RuntimeStore>,
        ) -> std::sync::Arc<dyn verlet_history::RuntimeStore>
        + Send
        + 'static,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let metadata_store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        let user_metadata_store = verlet_metadata::provider_store::SqliteMetadataStore::in_memory()
            .await
            .map_err(metadata_store_error)?;
        Self::with_runtime_factory_and_metadata_stores_inner(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
            Some(Box::new(decorate)),
            std::sync::Arc::new(AppServerTurnEndpointRouter::new(None)),
        )
        .await
    }

    #[cfg(test)]
    async fn with_runtime_factory_and_metadata_store(
        config: VerletAppServerConfig,
        runtime_factory: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
        >,
        metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
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
        runtime_factory: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
        >,
        metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
        user_metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let turn_endpoint_router = std::sync::Arc::new(AppServerTurnEndpointRouter::new(None));
        Self::with_runtime_factory_and_metadata_stores_and_router(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
            turn_endpoint_router,
        )
        .await
    }

    async fn with_runtime_factory_and_metadata_stores_and_router(
        config: VerletAppServerConfig,
        runtime_factory: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
        >,
        metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
        user_metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
        turn_endpoint_router: std::sync::Arc<AppServerTurnEndpointRouter>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        Self::with_runtime_factory_and_metadata_stores_inner(
            config,
            runtime_factory,
            metadata_store,
            user_metadata_store,
            None,
            turn_endpoint_router,
        )
        .await
    }

    async fn with_runtime_factory_and_metadata_stores_inner(
        mut config: VerletAppServerConfig,
        runtime_factory: std::sync::Arc<
            dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
        >,
        metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
        user_metadata_store: verlet_metadata::provider_store::SqliteMetadataStore,
        session_store_decorator: Option<
            Box<
                dyn FnOnce(
                        std::sync::Arc<dyn verlet_history::RuntimeStore>,
                    ) -> std::sync::Arc<dyn verlet_history::RuntimeStore>
                    + Send,
            >,
        >,
        turn_endpoint_router: std::sync::Arc<AppServerTurnEndpointRouter>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        config.validate_root_reservation()?;
        config.validate_endpoint_availability()?;
        crate::adapters::app_server::threads::normalize_registry_roots(&mut config);
        let provider_surface =
            agent_manifest_provider_surface_for_config(&config, &metadata_store).await?;
        crate::operations::kernel_packages::ensure_verlet_threads_published(
            operation_registry_root_for_kernel_publish(&config),
        )?;
        crate::operations::kernel_packages::ensure_verlet_schedule_published(
            operation_registry_root_for_kernel_publish(&config),
        )?;
        crate::operations::kernel_packages::ensure_verlet_process_published(
            operation_registry_root_for_kernel_publish(&config),
        )?;
        crate::operations::kernel_packages::ensure_verlet_notify_published(
            operation_registry_root_for_kernel_publish(&config),
        )?;
        crate::adapters::app_server::default_manifest::ensure_default_manifest_published(
            &config,
            provider_surface.supports_streaming,
        )?;
        let metadata_store_path = config.metadata_store_path();
        let user_metadata_store_path = config.user_metadata_store_path();
        let endpoint_record_roots = config.endpoint_record_roots()?;
        let endpoint = config.instance_endpoint(&endpoint_record_roots[0])?;
        let model_catalog = model_catalog::MergedModelCatalog::new(&config.user_state_home);
        #[cfg(not(test))]
        let model_catalog_state_home = config.user_state_home.clone();
        let supervisor = crate::kernel::supervisor::VerletSupervisor::new();
        let mut tenant_context = crate::kernel::supervisor::TenantRuntimeContext::local(
            config.tenant_id.clone(),
            config.runtime_home.clone(),
            config.state_home.clone(),
        );
        let codex_home = tenant_context.codex_home();
        let session_store_path = tenant_context.session_history_path();
        let identity_store = verlet_history_sqlite::SqliteSessionStore::open(&session_store_path)
            .await
            .map_err(history_store_error)?
            .with_lease_epoch(config.lease_epoch);
        let runtime_store = std::sync::Arc::new(identity_store.clone())
            as std::sync::Arc<dyn verlet_history::RuntimeStore>;
        let runtime_store = match session_store_decorator {
            Some(decorate) => decorate(runtime_store),
            None => runtime_store,
        };
        tenant_context = tenant_context.with_session_store(runtime_store);
        supervisor
            .register_tenant(crate::kernel::supervisor::TenantRegistration {
                context: tenant_context,
                runtime_factory,
            })
            .await?;
        let identity_clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock> =
            std::sync::Arc::new(crate::daemon::clock_route::SystemDaemonClock);
        let console_credential_record_path = session_store_path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(CONSOLE_CREDENTIAL_ID_FILE);
        let (identity_authority, console_credential) = initialize_boundary_identity(
            identity_store,
            std::sync::Arc::clone(&identity_clock),
            config.identity_mode,
            config.console_principal.clone(),
            &config.user_id,
            config.console_assets.as_mut(),
            &console_credential_record_path,
        )
        .await?;
        let process_manager =
            verlet_process::live::AsyncExecutionManager::new_with_process_id_source(
                verlet_process::live::AsyncExecutionManagerConfig::default(),
                std::sync::Arc::clone(&config.instance_environment.process_ids),
            );
        let app = Self {
            inner: std::sync::Arc::new(VerletAppServerInner {
                supervisor,
                tasks: std::sync::Arc::new(
                    crate::adapters::app_server::lifecycle::InstanceTaskSet::new(),
                ),
                client_connections: std::sync::Arc::new(ClientConnectionTracker::new()),
                shutdown: tokio::sync::Mutex::new(false),
                dispatch_gate: tokio::sync::RwLock::new(()),
                root_reservation: config.root_reservation,
                instance_environment: config.instance_environment,
                configured_listen: config.listen,
                endpoint,
                endpoint_record_roots,
                tenant_id: config.tenant_id,
                user_id: config.user_id,
                identity_mode: config.identity_mode,
                console_principal: config.console_principal,
                model: config.model.clone(),
                model_provider: config.model_provider.clone(),
                model_explicit: config.model_explicit,
                provider: config.provider.clone(),
                active_model: tokio::sync::RwLock::new(ActiveModelSelection {
                    model: config.model,
                    model_provider: config.model_provider,
                    provider: config.provider,
                }),
                model_selection_enabled: crate::adapters::agent_loop::TurnEndpointRouter::resolve(
                    turn_endpoint_router.as_ref(),
                )
                .is_some(),
                turn_endpoint_router,
                model_mutation: std::sync::Arc::new(tokio::sync::Mutex::new(())),
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
                console_credential: tokio::sync::Mutex::new(console_credential),
                cwd: config.cwd,
                codex_home,
                metadata_store_path,
                user_metadata_store_path,
                session_store_path,
                lease_epoch: config.lease_epoch,
                metadata_store,
                user_metadata_store,
                model_catalog,
                live_models: std::sync::Arc::new(live_models::LiveModelCache::new()),
                process_manager,
                process_dispatcher: tokio::sync::OnceCell::new(),
                subscriptions: tokio::sync::Mutex::new(
                    crate::adapters::app_server::subscriptions::AppServerSubscriptions::default(),
                ),
                state: tokio::sync::RwLock::new(
                    crate::adapters::app_server::threads::AppServerState::default(),
                ),
            }),
        };
        let initialization = async {
            let process_ingress: std::sync::Arc<
                dyn crate::kernel::runtime_host::runtime_api::ProcessHandleIngressSink,
            > = std::sync::Arc::new(AppServerProcessHandleIngress {
                app: std::sync::Arc::downgrade(&app.inner),
            });
            let runtime_store = app
                .inner
                .supervisor
                .runtime_store(&app.inner.tenant_id)
                .await?;
            let process_tasks = std::sync::Arc::downgrade(&app.inner.tasks);
            let process_dispatcher = crate::kernel::process_handle_dispatch::ProcessHandleDispatcher::new_with_task_owner(
                runtime_store,
                std::sync::Arc::clone(&process_ingress),
                app.inner.tasks.cancellation(),
                std::sync::Arc::new(move |task| {
                    process_tasks
                        .upgrade()
                        .is_some_and(|tasks| tasks.spawn(task))
                }),
            );
            app.inner
                .process_dispatcher
                .set(process_dispatcher.clone())
                .map_err(|_| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(
                        "app-server process dispatcher initialized twice".to_string(),
                    )
                })?;
            app.inner
                .supervisor
                .set_process_handle_ingress(&app.inner.tenant_id, Some(process_ingress))
                .await?;
            app.inner
                .supervisor
                .set_process_handle_dispatcher(
                    &app.inner.tenant_id,
                    Some(process_dispatcher.clone()),
                )
                .await?;
            app.inner
                .supervisor
                .set_thread_lifecycle_sink(
                    &app.inner.tenant_id,
                    Some(std::sync::Arc::new(
                        threads::AppServerThreadLifecycleSink::new(&app),
                    )),
                )
                .await?;
            app.load_threads_from_metadata().await?;
            // Construction is the earliest common boundary for daemon listeners,
            // standalone app-server serving, and in-process/local JSON-RPC users.
            // Run recovery before returning the first callable surface.
            process_dispatcher.assert_startup_registry_empty().await?;
            let recovery_store =
                verlet_history_sqlite::SqliteSessionStore::open(&app.inner.session_store_path)
                    .await
                    .map_err(history_store_error)?
                    .with_lease_epoch(app.inner.lease_epoch);
            crate::daemon::recovery_sweep::StartupRecoverySweep::new(
                recovery_store,
                process_dispatcher,
                &app.inner.tenant_id,
                &app.inner.user_id,
            )
            .run_once()
            .await
        }
        .await;
        let recovery = match initialization {
            Ok(recovery) => recovery,
            Err(error) => {
                if let Err(shutdown_error) = app.shutdown().await {
                    eprintln!(
                        "failed to shut down partially initialized Verlet app-server after {error}: {shutdown_error}"
                    );
                }
                return Err(error);
            }
        };
        if recovery.thread_joins > 0 || recovery.process_outcomes > 0 {
            eprintln!(
                "verlet startup recovery appended {} thread join(s) and submitted {} process outcome(s)",
                recovery.thread_joins, recovery.process_outcomes,
            );
        }
        instance::register_instance_endpoint(&app.inner.endpoint.instance_id);
        if let Err(error) = app.start_instance_unix_listener().await {
            if let Err(shutdown_error) = app.shutdown().await {
                eprintln!(
                    "failed to shut down Verlet app-server after endpoint listener error {error}: {shutdown_error}"
                );
            }
            return Err(error);
        }
        for state_root in &app.inner.endpoint_record_roots {
            if let Err(error) = instance::write_instance_endpoint(state_root, &app.inner.endpoint) {
                if let Err(shutdown_error) = app.shutdown().await {
                    eprintln!(
                        "failed to shut down Verlet app-server after endpoint record error {error}: {shutdown_error}"
                    );
                }
                return Err(error);
            }
        }
        // Catalog refresh is instance-owned and starts only after every
        // fallible construction and recovery step. It never participates in
        // constructor success or the first chat/RPC path.
        #[cfg(not(test))]
        model_catalog::spawn_runtime_refresh(&app.inner.tasks, model_catalog_state_home);
        Ok(app)
    }

    pub async fn serve(
        &self,
        listen: AppServerListenAddr,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        match listen {
            AppServerListenAddr::Unix(path) => self.serve_unix(path).await,
            AppServerListenAddr::WebSocket(addr) => self.serve_websocket(addr).await,
        }
    }

    #[cfg(unix)]
    async fn start_instance_unix_listener(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        let path = self.inner.endpoint.unix_socket.clone();
        prepare_unix_socket_path(&path)?;
        let listener = tokio::net::UnixListener::bind(&path).map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to bind Verlet endpoint socket {}: {error}",
                path.display()
            ))
        })?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(
            |error| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to secure Verlet endpoint socket {}: {error}",
                    path.display()
                ))
            },
        )?;

        let inner = std::sync::Arc::downgrade(&self.inner);
        let tasks = std::sync::Arc::downgrade(&self.inner.tasks);
        let client_connections = std::sync::Arc::clone(&self.inner.client_connections);
        let cancellation = self.inner.tasks.cancellation();
        if !self.inner.tasks.spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = cancellation.cancelled() => return,
                    accepted = client_connections.accept_unix(&listener) => accepted,
                };
                let (stream, _, connection) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        eprintln!("Verlet endpoint socket accept failed: {error}");
                        continue;
                    }
                };
                let peer_uid = match stream.peer_cred() {
                    Ok(credentials) => credentials.uid(),
                    Err(error) => {
                        eprintln!("failed to inspect Verlet endpoint peer credentials: {error}");
                        continue;
                    }
                };
                let Some(inner) = inner.upgrade() else {
                    return;
                };
                let Some(tasks) = tasks.upgrade() else {
                    return;
                };
                let app = VerletAppServer { inner };
                if !tasks.spawn(async move {
                    let _connection = connection;
                    if let Err(error) = app.handle_unix_stream(stream, peer_uid).await {
                        eprintln!("Verlet endpoint connection failed: {error}");
                    }
                }) {
                    return;
                }
            }
        }) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "Verlet app-server shut down before its endpoint socket started".to_string(),
            ));
        }
        Ok(())
    }

    #[cfg(not(unix))]
    async fn start_instance_unix_listener(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        Ok(())
    }

    /// Transport-independent RPC dispatch (EMO-551): serve one JSON-RPC
    /// request for an already-authenticated principal, without a socket,
    /// session witness derivation from process identity, or listener. This
    /// is the in-process seam an embedding host may route into (EMO-553): the
    /// caller supplies an already-authenticated principal, resolves exactly
    /// one instance, and calls this per request. Socket routing remains
    /// selection rather than authentication: the selected instance verifies
    /// that connection itself. Session open/close witnessing happens here
    /// against the supplied principal — never from process UID (that
    /// derivation stays in `local_json_rpc_request`, which remains the
    /// standalone local-operator path).
    pub async fn dispatch_authenticated_json_rpc(
        &self,
        principal: crate::daemon::identity::ResolvedPrincipal,
        method: &str,
        params: serde_json::Value,
    ) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
        self.authenticated_json_rpc_request(
            principal,
            crate::daemon::identity::BoundarySurface::Host,
            "in-process-host",
            method,
            params,
        )
        .await
    }

    /// Serve one host-routed TCP connection (EMO-553): the host facade
    /// selected this instance from its credential route table and hands
    /// over the stream with the HTTP request still un-consumed (routing
    /// peeks, it does not read). This instance's own identity authority
    /// verifies the token — routing is selection, not authentication — and
    /// a rejected token is refused here exactly like on the standalone
    /// path, with the rejection witnessed by this instance. Sessions and
    /// rejections are witnessed on
    /// [`crate::daemon::identity::BoundarySurface::Host`], never the
    /// `Websocket`/`Console` surface the bearer header shape would
    /// suggest. Unlike the standalone accept path, the caller owns the
    /// task this runs on (the host task set); dispatch inside still holds
    /// this instance's dispatch gate, so instance shutdown ends the
    /// connection's requests.
    pub(crate) async fn serve_host_routed_tcp_stream(
        &self,
        stream: tokio::net::TcpStream,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let _connection = self.inner.client_connections.enter();
        let mut stream = stream;
        let authentication = async {
            let Some((resolved_principal, _)) = self
                .authenticate_tcp_websocket_on_surface(
                    &mut stream,
                    Some(crate::daemon::identity::BoundarySurface::Host),
                )
                .await?
            else {
                return Ok(None);
            };
            let websocket = accept_authenticated_websocket(stream)
                .await
                .map_err(|error| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to upgrade Verlet host-routed websocket: {error}"
                    ))
                })?;
            Ok::<_, crate::kernel::runtime_host::VerletError>(Some((websocket, resolved_principal)))
        };
        let cancellation = self.inner.tasks.cancellation();
        let authenticated = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            authenticated = authentication => authenticated?,
        };
        let Some((websocket, resolved_principal)) = authenticated else {
            return Ok(());
        };
        self.handle_websocket(
            websocket,
            resolved_principal,
            crate::daemon::identity::BoundarySurface::Host,
        )
        .await
    }

    /// Instance-owned async shutdown (EMO-551). Cancels and awaits every
    /// background task this instance spawned (subscription watchers,
    /// connection tasks, websocket writers, process-settlement monitors, and
    /// persistence one-shots),
    /// retires the console credential (moving that work out of
    /// `VerletAppServerInner::drop`, which stops constructing a runtime), and
    /// shuts down + unregisters the supervisor tenant so the id can be
    /// reused. Idempotent; after it resolves, dropping the instance is inert
    /// and a co-resident instance is unaffected.
    pub async fn shutdown(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        let mut shutdown = self.inner.shutdown.lock().await;
        if *shutdown {
            return Ok(());
        }

        for state_root in &self.inner.endpoint_record_roots {
            instance::remove_owned_instance_endpoint(state_root, &self.inner.endpoint.instance_id)?;
        }
        self.inner.tasks.cancel();
        let _dispatch = self.inner.dispatch_gate.write().await;
        self.inner.tasks.shutdown().await;
        let mut console_credential = self.inner.console_credential.lock().await;
        if let Some(credential) = console_credential.as_ref() {
            retire_console_credential(
                std::sync::Arc::clone(&self.inner.identity_authority),
                credential,
            )
            .await?;
        }
        console_credential.take();
        drop(console_credential);
        self.inner
            .supervisor
            .shutdown_and_unregister_tenant(&self.inner.tenant_id)
            .await?;
        instance::unregister_instance_endpoint(&self.inner.endpoint.instance_id);
        *shutdown = true;
        Ok(())
    }

    pub async fn wait_for_idle_timeout(&self, idle_timeout: std::time::Duration) {
        self.inner
            .client_connections
            .wait_for_idle_timeout(idle_timeout)
            .await;
    }

    #[cfg(test)]
    pub(crate) fn active_client_connections_for_test(&self) -> usize {
        self.inner
            .client_connections
            .active
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn supervisor(&self) -> crate::kernel::supervisor::VerletSupervisor {
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
    pub(crate) fn identity_boundary_config(
        &self,
    ) -> (
        crate::daemon::identity::IdentityMode,
        Option<&crate::daemon::identity::PrincipalId>,
    ) {
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

    pub fn cwd(&self) -> &std::path::Path {
        &self.inner.cwd
    }

    pub fn session_store_path(&self) -> &std::path::Path {
        &self.inner.session_store_path
    }

    pub(crate) fn lease_epoch(&self) -> u64 {
        self.inner.lease_epoch
    }

    pub(crate) fn mark_remote_event_store_served(&self) {
        self.inner
            .remote_event_store_served
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) fn remote_event_store_served(&self) -> bool {
        self.inner
            .remote_event_store_served
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(unix)]
    async fn serve_unix(
        &self,
        path: std::path::PathBuf,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let path = instance::absolute_path(&path)?;
        let configured_path = match &self.inner.configured_listen {
            AppServerListenAddr::Unix(configured_path) => {
                Some(instance::absolute_path(configured_path)?)
            }
            AppServerListenAddr::WebSocket(_) => None,
        };
        if path == self.inner.endpoint.unix_socket || configured_path.as_ref() == Some(&path) {
            self.inner.tasks.cancellation().cancelled().await;
            return Ok(());
        }
        prepare_unix_socket_path(&path)?;
        let listener = tokio::net::UnixListener::bind(&path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to bind Verlet app-server socket {}: {err}",
                path.display()
            ))
        })?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to secure Verlet app-server socket {}: {err}",
                path.display()
            ))
        })?;

        let cancellation = self.inner.tasks.cancellation();
        loop {
            let accepted = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                accepted = self.inner.client_connections.accept_unix(&listener) => accepted,
            };
            let (stream, _, connection) = accepted.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to accept Verlet app-server connection: {err}"
                ))
            })?;
            let peer_uid = stream
                .peer_cred()
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to inspect Verlet app-server peer credentials: {err}"
                    ))
                })?
                .uid();
            let app = self.clone();
            self.inner.tasks.spawn(async move {
                let _connection = connection;
                if let Err(err) = app.handle_unix_stream(stream, peer_uid).await {
                    eprintln!("verlet app-server connection failed: {err}");
                }
            });
        }
    }

    #[cfg(not(unix))]
    async fn serve_unix(
        &self,
        _path: std::path::PathBuf,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "unix app-server sockets are only supported on Unix platforms".to_string(),
        ))
    }

    async fn serve_websocket(
        &self,
        addr: std::net::SocketAddr,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let listener = bind_websocket_listener(addr).await?;
        self.serve_websocket_listener(listener).await
    }

    pub async fn serve_websocket_listener(
        &self,
        listener: tokio::net::TcpListener,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let addr = listener.local_addr().map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to inspect Verlet app-server websocket listener: {err}"
            ))
        })?;
        if !addr.ip().is_loopback() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "app-server websocket listen address {addr} is not loopback; configure websocket auth before binding non-loopback addresses"
                ),
            ));
        }

        let cancellation = self.inner.tasks.cancellation();
        loop {
            let accepted = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                accepted = self.inner.client_connections.accept_tcp(&listener) => accepted,
            };
            let (stream, peer, connection) = accepted.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to accept Verlet app-server websocket connection: {err}"
                ))
            })?;
            let app = self.clone();
            self.inner.tasks.spawn(async move {
                let _connection = connection;
                if let Err(err) = app.handle_tcp_stream(stream).await {
                    eprintln!("verlet app-server websocket connection from {peer} failed: {err}");
                }
            });
        }
    }

    #[cfg(unix)]
    async fn handle_unix_stream(
        &self,
        mut stream: tokio::net::UnixStream,
        peer_uid: u32,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let authentication = async {
            let Some(resolved_principal) = self
                .authenticate_unix_websocket(&mut stream, peer_uid)
                .await?
            else {
                return Ok(None);
            };
            let websocket = accept_authenticated_websocket(stream)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to upgrade Verlet app-server unix socket websocket: {err}"
                    ))
                })?;
            Ok::<_, crate::kernel::runtime_host::VerletError>(Some((websocket, resolved_principal)))
        };
        let cancellation = self.inner.tasks.cancellation();
        let authenticated = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            authenticated = authentication => authenticated?,
        };
        let Some((websocket, resolved_principal)) = authenticated else {
            return Ok(());
        };
        self.handle_websocket(
            websocket,
            resolved_principal,
            crate::daemon::identity::BoundarySurface::UnixSocket,
        )
        .await
    }

    async fn handle_tcp_stream(
        &self,
        stream: tokio::net::TcpStream,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let mut stream = stream;
        let authentication = async {
            if self.handle_http_request(&mut stream).await? {
                return Ok(None);
            }
            let Some((resolved_principal, surface)) =
                self.authenticate_tcp_websocket(&mut stream).await?
            else {
                return Ok(None);
            };
            let websocket = accept_authenticated_websocket(stream)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to upgrade Verlet app-server tcp websocket: {err}"
                    ))
                })?;
            Ok::<_, crate::kernel::runtime_host::VerletError>(Some((
                websocket,
                resolved_principal,
                surface,
            )))
        };
        let cancellation = self.inner.tasks.cancellation();
        let authenticated = tokio::select! {
            _ = cancellation.cancelled() => return Ok(()),
            authenticated = authentication => authenticated?,
        };
        let Some((websocket, resolved_principal, surface)) = authenticated else {
            return Ok(());
        };
        self.handle_websocket(websocket, resolved_principal, surface)
            .await
    }

    async fn handle_http_request(
        &self,
        stream: &mut tokio::net::TcpStream,
    ) -> crate::kernel::runtime_host::VerletResult<bool> {
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
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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
        stream: &mut tokio::net::TcpStream,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<(
            crate::daemon::identity::ResolvedPrincipal,
            crate::daemon::identity::BoundarySurface,
        )>,
    > {
        self.authenticate_tcp_websocket_on_surface(stream, None)
            .await
    }

    async fn authenticate_tcp_websocket_on_surface(
        &self,
        stream: &mut tokio::net::TcpStream,
        forced_surface: Option<crate::daemon::identity::BoundarySurface>,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<(
            crate::daemon::identity::ResolvedPrincipal,
            crate::daemon::identity::BoundarySurface,
        )>,
    > {
        let request = peek_http_request(stream).await?;
        let token_and_surface = request.as_ref().and_then(request_bearer_token);
        if let Some((token, derived_surface)) = token_and_surface
            && let Some(principal) = self.inner.identity_authority.verify_token(token).await?
        {
            let surface = forced_surface.unwrap_or(derived_surface);
            return Ok(Some((principal, surface)));
        }
        let surface = forced_surface.unwrap_or_else(|| {
            token_and_surface
                .map(|(_, surface)| surface)
                .unwrap_or(crate::daemon::identity::BoundarySurface::Websocket)
        });
        let reason = match token_and_surface {
            Some((token, _)) => self.token_rejection_reason(token).await?,
            None => crate::daemon::identity::IdentityAuthRejectionReason::CredentialUnknown,
        };
        self.reject_websocket_auth(stream, surface, reason).await?;
        Ok(None)
    }

    #[cfg(unix)]
    async fn authenticate_unix_websocket(
        &self,
        stream: &mut tokio::net::UnixStream,
        peer_uid: u32,
    ) -> crate::kernel::runtime_host::VerletResult<Option<crate::daemon::identity::ResolvedPrincipal>>
    {
        let same_uid = peer_uid == current_effective_uid();
        if self.inner.identity_mode == crate::daemon::identity::IdentityMode::Local
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
            None if same_uid
                && self.inner.identity_mode == crate::daemon::identity::IdentityMode::Managed =>
            {
                crate::daemon::identity::IdentityAuthRejectionReason::PeerMappingDisabled {
                    uid: peer_uid,
                }
            }
            None => crate::daemon::identity::IdentityAuthRejectionReason::CredentialUnknown,
        };
        self.reject_websocket_auth(
            stream,
            crate::daemon::identity::BoundarySurface::UnixSocket,
            reason,
        )
        .await?;
        Ok(None)
    }

    async fn token_rejection_reason(
        &self,
        token: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::daemon::identity::IdentityAuthRejectionReason,
    > {
        let digest = crate::daemon::identity::identity_token_digest(token);
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
                    return Ok(
                        crate::daemon::identity::IdentityAuthRejectionReason::CredentialRevoked {
                            credential_id: credential.credential_id,
                        },
                    );
                }
                if principal.revoked_at_ms.is_some() {
                    return Ok(
                        crate::daemon::identity::IdentityAuthRejectionReason::PrincipalRevoked {
                            principal_id: principal.principal_id,
                        },
                    );
                }
                if credential
                    .expires_at_ms
                    .is_some_and(|expires_at_ms| expires_at_ms <= now_ms)
                {
                    return Ok(
                        crate::daemon::identity::IdentityAuthRejectionReason::CredentialExpired {
                            credential_id: credential.credential_id,
                        },
                    );
                }
            }
        }
        Ok(crate::daemon::identity::IdentityAuthRejectionReason::CredentialUnknown)
    }

    async fn reject_websocket_auth<S>(
        &self,
        stream: &mut S,
        surface: crate::daemon::identity::BoundarySurface,
        reason: crate::daemon::identity::IdentityAuthRejectionReason,
    ) -> crate::kernel::runtime_host::VerletResult<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.inner
            .identity_authority
            .witness_auth_rejected(&crate::daemon::identity::IdentityAuthRejectionV1 {
                schema: crate::daemon::identity::IDENTITY_AUTH_REJECTION_SCHEMA_V1.to_string(),
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

pub(crate) async fn refuse_host_tcp_stream(
    mut stream: tokio::net::TcpStream,
) -> crate::kernel::runtime_host::VerletResult<()> {
    consume_http_request_headers(&mut stream).await?;
    write_http_response(
        &mut stream,
        "401 Unauthorized",
        "text/plain; charset=utf-8",
        HTTP_UNAUTHORIZED_BODY.as_bytes(),
    )
    .await
}

async fn initialize_boundary_identity(
    store: verlet_history_sqlite::SqliteSessionStore,
    clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    mode: crate::daemon::identity::IdentityMode,
    console_principal: Option<crate::daemon::identity::PrincipalId>,
    default_operator_id: &str,
    console_assets: Option<&mut ConsoleAssetConfig>,
    console_credential_record_path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<(
    std::sync::Arc<dyn crate::daemon::identity::IdentityAuthority>,
    Option<ConsoleCredentialLease>,
)> {
    let authority = crate::daemon::identity::SqliteIdentityAuthority::new(
        store.clone(),
        std::sync::Arc::clone(&clock),
        None,
    )
    .await?;
    let mut operator = authority
        .list_principals()
        .await?
        .into_iter()
        .find(|principal| {
            principal.kind == crate::daemon::identity::PrincipalKind::Operator
                && principal.revoked_at_ms.is_none()
        })
        .map(|principal| principal.principal_id);
    if mode == crate::daemon::identity::IdentityMode::Local && operator.is_none() {
        let principal_id = crate::daemon::identity::PrincipalId::new(default_operator_id);
        authority
            .bootstrap_operator(&principal_id, "Local operator")
            .await?;
        operator = Some(principal_id);
    }
    let peer_operator = (mode == crate::daemon::identity::IdentityMode::Local)
        .then(|| operator.clone())
        .flatten();
    let authority =
        crate::daemon::identity::SqliteIdentityAuthority::new(store, clock, peer_operator).await?;
    let mut console_credential = None;
    if let Some(console) = console_assets {
        let principal_id = console_principal
            .or_else(|| {
                (mode == crate::daemon::identity::IdentityMode::Local)
                    .then_some(operator)
                    .flatten()
            })
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(
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
    Ok((std::sync::Arc::new(authority), console_credential))
}

fn read_console_credential_id(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<Option<String>> {
    let value = match std::fs::read_to_string(path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "failed to read Verlet console credential record {}: {error}",
                    path.display()
                ),
            ));
        }
    };
    let credential_id = value.trim();
    if credential_id.is_empty() || credential_id.chars().any(char::is_whitespace) {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "Verlet console credential record {} is malformed",
                path.display()
            ),
        ));
    }
    Ok(Some(credential_id.to_string()))
}

fn persist_console_credential_id(
    path: &std::path::Path,
    credential_id: &str,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    std::fs::create_dir_all(parent).map_err(|error| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to prepare Verlet console credential record directory {}: {error}",
            parent.display()
        ))
    })?;
    let temporary = parent.join(format!(
        ".{CONSOLE_CREDENTIAL_ID_FILE}.{}.tmp",
        uuid::Uuid::now_v7()
    ));
    let write_result = (|| -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
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
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "failed to persist Verlet console credential record {}: {error}",
                path.display()
            ),
        ));
    }
    Ok(())
}

fn ignore_missing_credential(
    result: crate::kernel::runtime_host::VerletResult<()>,
) -> crate::kernel::runtime_host::VerletResult<()> {
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
    authority: std::sync::Arc<dyn crate::daemon::identity::IdentityAuthority>,
    credential: &ConsoleCredentialLease,
) -> crate::kernel::runtime_host::VerletResult<()> {
    ignore_missing_credential(
        authority
            .revoke_credential(&credential.principal_id, &credential.credential_id)
            .await,
    )?;
    let current = read_console_credential_id(&credential.record_path)?;
    if current.as_deref() == Some(credential.credential_id.as_str()) {
        match std::fs::remove_file(&credential.record_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "failed to clear Verlet console credential record {}: {error}",
                        credential.record_path.display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

async fn resolve_catalog_provider(
    provider_store: &verlet_metadata::provider_store::SqliteMetadataStore,
    auth_store: &verlet_metadata::provider_store::SqliteMetadataStore,
    auth_context: &verlet_metadata::provider_store::LlmProviderAuthContext,
    provider_id: &str,
    model: Option<&str>,
    max_tokens: u32,
    stream: bool,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::agent_loop::ResolvedTurnEndpoint> {
    let provider = provider_store
        .get_provider(provider_id)
        .await
        .map_err(provider_store_error)?
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "catalog provider {provider_id:?} is not in the provider metadata store; add it with modelProvider/upsert"
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
    let base_url = model_record
        .and_then(|model| model.base_url.clone())
        .unwrap_or_else(|| provider.base_url.clone());

    let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        if provider_id == verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID {
            verlet_history::ProviderApi::OpenAIResponses
        } else {
            api.clone()
        },
        provider.provider_id.clone(),
        model_id,
    );
    runtime_config.max_tokens = max_tokens;
    runtime_config.stream = stream;

    if provider_id == verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID {
        let credential = verlet_metadata::provider_store::LlmProviderAuthStore::get_credential(
            auth_store,
            provider_id,
        )
        .await
        .map_err(provider_store_error)?;
        if !matches!(
            credential,
            Some(verlet_metadata::provider_store::LlmProviderCredential::OAuth { .. })
        ) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "catalog provider {provider_id:?} requires a stored OAuth login; run `verlet auth login openai-codex`"
                ),
            ));
        }
        validate_catalog_codex_responses_url(provider_id, &base_url)?;
        let client = std::sync::Arc::new(
            crate::openai_codex::OpenAICodexProviderClient::with_responses_url(
                auth_store.clone(),
                base_url,
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to build catalog provider {provider_id:?} OpenAI Codex client: {err}; update its endpoint with modelProvider/upsert"
                ))
            })?,
        );
        return Ok(crate::adapters::agent_loop::ResolvedTurnEndpoint {
            config: runtime_config,
            client,
        });
    }

    let resolved_auth = verlet_metadata::provider_store::resolve_llm_provider_auth(
        auth_store,
        &provider,
        auth_context,
    )
    .await
    .map_err(provider_store_error)?;
    // Keyless providers (`auth: none`, EMO-577) route without a credential;
    // a stored/runtime/env credential, when present, still wins above.
    let keyless = matches!(
        provider.auth,
        verlet_metadata::provider_store::LlmProviderAuthConfig::None
    );
    if provider.auth_header && resolved_auth.is_none() && !keyless {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "catalog provider {provider_id:?} requires an API key but none was configured; set its environment credential or run `verlet auth set {provider_id} --api-key-stdin`"
            ),
        ));
    }
    let api_key = resolved_auth
        .as_ref()
        .map(|auth| auth.api_key.clone())
        .unwrap_or_default();
    let (mut endpoint, adapter): (
        verlet_provider::ProviderEndpoint,
        std::sync::Arc<dyn verlet_provider::ProviderWireAdapter>,
    ) = match api {
        verlet_history::ProviderApi::OpenAIChatCompletions => (
            verlet_provider::ProviderEndpoint::openai_chat_completions(&base_url, api_key),
            std::sync::Arc::new(verlet_provider::OpenAIChatCompletionsAdapter),
        ),
        verlet_history::ProviderApi::OpenAIResponses => (
            verlet_provider::ProviderEndpoint::openai_responses(&base_url, api_key),
            std::sync::Arc::new(verlet_provider::OpenAIResponsesAdapter {
                include_encrypted_reasoning: false,
                reasoning_summary: verlet_provider::OpenAIReasoningSummary::Auto,
            }),
        ),
        verlet_history::ProviderApi::AnthropicMessages => (
            verlet_provider::ProviderEndpoint::anthropic_messages(&base_url, api_key),
            std::sync::Arc::new(verlet_provider::AnthropicMessagesAdapter),
        ),
        verlet_history::ProviderApi::Other(_) => {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "catalog provider {provider_id:?} uses unsupported api {api:?}; update it with modelProvider/upsert to use OpenAI Chat Completions, OpenAI Responses, or Anthropic Messages"
                ),
            ));
        }
    };
    if !provider.auth_header || (keyless && resolved_auth.is_none()) {
        endpoint.auth = verlet_provider::ProviderAuth::None;
    }

    let headers = merged_catalog_headers(&provider, model_record);
    for (name, value) in resolve_catalog_headers(provider_id, &headers, auth_context)? {
        set_endpoint_header(&mut endpoint.headers, name, value);
    }

    let client = std::sync::Arc::new(
        verlet_provider::ProviderHttpClient::new(endpoint, adapter).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to build catalog provider {provider_id:?} {api:?} client: {err}; update its endpoint with modelProvider/upsert"
            ))
        })?,
    );
    Ok(crate::adapters::agent_loop::ResolvedTurnEndpoint {
        config: runtime_config,
        client,
    })
}

fn selected_catalog_model_id(
    provider: &verlet_metadata::provider_store::LlmProviderRecord,
    requested_model: Option<&str>,
) -> crate::kernel::runtime_host::VerletResult<String> {
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
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "catalog provider {:?} has no models; add one with modelProvider/upsert",
                provider.provider_id
            ))
        })
}

fn validate_catalog_codex_responses_url(
    provider_id: &str,
    base_url: &str,
) -> crate::kernel::runtime_host::VerletResult<()> {
    const CODEX_RESPONSES_PATH: &str = "/backend-api/codex/responses";
    let parsed = reqwest::Url::parse(base_url).ok();
    let valid = parsed.as_ref().is_some_and(|url| {
        let host = url.host_str().unwrap_or_default();
        let is_chatgpt = url.scheme() == "https"
            && host.eq_ignore_ascii_case("chatgpt.com")
            && url.port().is_none();
        let is_loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|address| address.is_loopback());
        (is_chatgpt || (matches!(url.scheme(), "http" | "https") && is_loopback))
            && url.username().is_empty()
            && url.password().is_none()
            && url.path() == CODEX_RESPONSES_PATH
            && url.query().is_none()
            && url.fragment().is_none()
    });
    if valid {
        return Ok(());
    }
    Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
        format!(
            "catalog provider {provider_id:?} has an invalid Codex base URL {base_url:?}; set provider or model baseUrl to https://chatgpt.com{CODEX_RESPONSES_PATH} (or a loopback test endpoint) with modelProvider/upsert"
        ),
    ))
}

pub(crate) fn merged_catalog_headers(
    provider: &verlet_metadata::provider_store::LlmProviderRecord,
    model_record: Option<&verlet_metadata::provider_store::LlmProviderModelRecord>,
) -> std::collections::BTreeMap<String, verlet_metadata::provider_store::LlmProviderConfigValue> {
    let mut headers = std::collections::BTreeMap::new();
    for (name, value) in &provider.headers {
        insert_catalog_header(&mut headers, name.clone(), value.clone());
    }
    if let Some(model_record) = model_record {
        for (name, value) in &model_record.headers {
            insert_catalog_header(&mut headers, name.clone(), value.clone());
        }
    }
    headers
}

fn insert_catalog_header(
    headers: &mut std::collections::BTreeMap<
        String,
        verlet_metadata::provider_store::LlmProviderConfigValue,
    >,
    name: String,
    value: verlet_metadata::provider_store::LlmProviderConfigValue,
) {
    if let Some(existing) = headers
        .keys()
        .find(|existing| existing.eq_ignore_ascii_case(&name))
        .cloned()
    {
        headers.remove(&existing);
    }
    headers.insert(name, value);
}

fn set_endpoint_header(headers: &mut Vec<(String, String)>, name: String, value: String) {
    if let Some(existing) = headers
        .iter_mut()
        .find(|(existing, _)| existing.eq_ignore_ascii_case(&name))
    {
        *existing = (name, value);
    } else {
        headers.push((name, value));
    }
}

fn resolve_catalog_headers(
    provider_id: &str,
    headers: &std::collections::BTreeMap<
        String,
        verlet_metadata::provider_store::LlmProviderConfigValue,
    >,
    auth_context: &verlet_metadata::provider_store::LlmProviderAuthContext,
) -> crate::kernel::runtime_host::VerletResult<Vec<(String, String)>> {
    headers
        .iter()
        .map(|(name, value)| {
            Ok((
                name.clone(),
                resolve_catalog_config_value(provider_id, name, value, auth_context)?,
            ))
        })
        .collect()
}

fn resolve_catalog_config_value(
    provider_id: &str,
    header_name: &str,
    value: &verlet_metadata::provider_store::LlmProviderConfigValue,
    auth_context: &verlet_metadata::provider_store::LlmProviderAuthContext,
) -> crate::kernel::runtime_host::VerletResult<String> {
    match value {
        verlet_metadata::provider_store::LlmProviderConfigValue::Literal { value } => {
            Ok(value.clone())
        }
        verlet_metadata::provider_store::LlmProviderConfigValue::Env { name } => auth_context
            .environment
            .get(name)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "catalog provider {provider_id:?} header {header_name:?} requires environment variable {name}; set {name} or update/remove the header with modelProvider/upsert"
                ))
            }),
        verlet_metadata::provider_store::LlmProviderConfigValue::Command { .. } => {
            Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "catalog provider {provider_id:?} header {header_name:?} uses unsupported command-backed resolution; update or remove the header with modelProvider/upsert"
            )))
        }
    }
}

fn provider_store_error(
    err: verlet_metadata::provider_store::LlmProviderStoreError,
) -> crate::kernel::runtime_host::VerletError {
    let message = err.to_string();
    if instance::turso_cross_process_lock_error(&message) {
        return instance::cross_process_database_guidance();
    }
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
        "provider metadata store failed: {message}"
    ))
}

async fn agent_manifest_provider_surface_for_config(
    config: &VerletAppServerConfig,
    metadata_store: &verlet_metadata::provider_store::SqliteMetadataStore,
) -> crate::kernel::runtime_host::VerletResult<
    crate::agent::manifest_bind::AgentManifestProviderSurface,
> {
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
    metadata_store: &verlet_metadata::provider_store::SqliteMetadataStore,
) -> crate::kernel::runtime_host::VerletResult<
    crate::agent::manifest_bind::AgentManifestProviderSurface,
> {
    match provider_config {
        AppServerProviderConfig::CatalogOpenAIChatCompletions { provider_id, .. } => {
            let provider = metadata_store
                .get_provider(provider_id)
                .await
                .map_err(provider_store_error)?
                .ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "catalog provider {provider_id:?} is not in the provider metadata store"
                    ))
                })?;
            Ok(
                crate::agent::manifest_bind::AgentManifestProviderSurface::from_provider_record(
                    &provider,
                ),
            )
        }
        AppServerProviderConfig::LocalOffline => Ok(
            crate::agent::manifest_bind::AgentManifestProviderSurface::single(
                model_provider.to_string(),
                model.to_string(),
            )
            .with_supports_streaming(false),
        ),
        AppServerProviderConfig::OpenAICodex { .. }
        | AppServerProviderConfig::BifrostOpenAIResponses { .. }
        | AppServerProviderConfig::OpenAIChatCompletions { .. }
        | AppServerProviderConfig::AnthropicMessages { .. }
        | AppServerProviderConfig::AnthropicBedrock { .. } => Ok(
            crate::agent::manifest_bind::AgentManifestProviderSurface::single(
                model_provider.to_string(),
                model.to_string(),
            ),
        ),
    }
}

fn metadata_store_error(
    err: verlet_metadata::provider_store::MetadataStoreError,
) -> crate::kernel::runtime_host::VerletError {
    let message = err.to_string();
    if instance::turso_cross_process_lock_error(&message) {
        return instance::cross_process_database_guidance();
    }
    crate::kernel::runtime_host::VerletError::RuntimeFactory(message)
}

fn secret_store_error(
    err: verlet_metadata::secret_store::SecretStoreError,
) -> crate::kernel::runtime_host::VerletError {
    let message = err.to_string();
    if instance::turso_cross_process_lock_error(&message) {
        return instance::cross_process_database_guidance();
    }
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
        "secret store failed: {message}"
    ))
}

fn history_store_error(
    err: verlet_history::HistoryError,
) -> crate::kernel::runtime_host::VerletError {
    let message = err.to_string();
    if instance::turso_cross_process_lock_error(&message) {
        return instance::cross_process_database_guidance();
    }
    crate::kernel::runtime_host::VerletError::History(message)
}

fn metadata_store_jsonrpc_error(
    err: verlet_metadata::provider_store::MetadataStoreError,
) -> crate::adapters::app_server::connection::JsonRpcErrorError {
    crate::adapters::app_server::connection::internal_error(metadata_store_error(err))
}

fn text_from_canonical_content(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. }
            | verlet_history::CanonicalContent::Thinking { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn thinking_text_from_canonical_content(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Thinking { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Text { .. }
            | verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

async fn open_and_seed_metadata_store(
    path: impl AsRef<std::path::Path>,
) -> crate::kernel::runtime_host::VerletResult<verlet_metadata::provider_store::SqliteMetadataStore>
{
    let store = verlet_metadata::provider_store::SqliteMetadataStore::open(path)
        .await
        .map_err(metadata_store_error)?;
    verlet_metadata::provider_store::seed_default_llm_providers(&store)
        .await
        .map_err(provider_store_error)?;
    Ok(store)
}

async fn sync_catalog_provider_identity(
    config: &mut VerletAppServerConfig,
    provider_store: &verlet_metadata::provider_store::SqliteMetadataStore,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if let AppServerProviderConfig::CatalogOpenAIChatCompletions {
        provider_id, model, ..
    } = &config.provider
    {
        let provider = provider_store
            .get_provider(provider_id)
            .await
            .map_err(provider_store_error)?
            .ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "catalog provider {provider_id:?} is not in the provider metadata store"
                ))
            })?;
        config.model_provider = provider.provider_id.clone();
        config.model = selected_catalog_model_id(&provider, model.as_deref())?;
    }
    Ok(())
}

/// Records templated by older catalog refreshes may persist endpoint fields
/// that came from the network. Re-pin reviewed providers before any runtime
/// endpoint is built, and retire refresh-only provider records while leaving
/// their separately stored credentials recoverable for an explicit custom
/// provider configuration.
async fn reconcile_catalog_provider_records(
    config: &VerletAppServerConfig,
    provider_store: &verlet_metadata::provider_store::SqliteMetadataStore,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let reviewed = model_catalog::MergedModelCatalog::new(&config.user_state_home)
        .providers()
        .into_iter()
        .map(|provider| (provider.provider_id.clone(), provider))
        .collect::<std::collections::BTreeMap<_, _>>();
    for mut record in provider_store
        .list_providers()
        .await
        .map_err(provider_store_error)?
        .into_iter()
        .filter(|record| record.metadata.get("origin").map(String::as_str) == Some("catalog"))
    {
        let Some(provider) = reviewed.get(&record.provider_id) else {
            provider_store
                .delete_provider(&record.provider_id)
                .await
                .map_err(provider_store_error)?;
            continue;
        };
        let Some(api) = model_catalog::catalog_api_to_provider_api(&provider.api) else {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "reviewed catalog provider {:?} has unsupported API family {:?}",
                    provider.provider_id, provider.api
                ),
            ));
        };
        if record.base_url == provider.base_url && record.api == api {
            continue;
        }
        record.base_url.clone_from(&provider.base_url);
        record.api = api;
        provider_store
            .upsert_provider(record)
            .await
            .map_err(provider_store_error)?;
    }
    Ok(())
}

async fn runtime_factory_from_config(
    config: &VerletAppServerConfig,
    endpoint: crate::adapters::agent_loop::ResolvedTurnEndpoint,
    turn_endpoint_router: std::sync::Arc<AppServerTurnEndpointRouter>,
) -> crate::kernel::runtime_host::VerletResult<
    std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory>,
> {
    let secret_resolver = secret_resolver_from_config(config).await?;
    Ok(
        runtime_factory_from_provider_parts_with_app_paths_and_router(
            endpoint.config,
            endpoint.client,
            config.capsule_bindings.clone(),
            secret_resolver,
            config,
            Some(turn_endpoint_router),
        ),
    )
}

pub(crate) async fn resolved_turn_endpoint_from_provider_config(
    provider_config: &AppServerProviderConfig,
    model_provider: &str,
    selected_model: &str,
    provider_store: &verlet_metadata::provider_store::SqliteMetadataStore,
    auth_store: &verlet_metadata::provider_store::SqliteMetadataStore,
    auth_context: &verlet_metadata::provider_store::LlmProviderAuthContext,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::agent_loop::ResolvedTurnEndpoint> {
    let (runtime_config, client): (
        crate::adapters::agent_loop::AgentLoopConfig,
        std::sync::Arc<dyn verlet_provider::ProviderClient>,
    ) = match provider_config {
        AppServerProviderConfig::LocalOffline => {
            let runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::Other(model_provider.to_string()),
                model_provider,
                selected_model,
            );
            (
                runtime_config,
                std::sync::Arc::new(AppServerOfflineProviderClient::new(
                    model_provider,
                    selected_model,
                )),
            )
        }
        AppServerProviderConfig::OpenAICodex {
            model,
            max_tokens,
            stream,
        } => {
            let client = std::sync::Arc::new(
                crate::openai_codex::OpenAICodexProviderClient::new(auth_store.clone()).map_err(
                    |err| {
                        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                            "failed to build OpenAI Codex provider client: {err}"
                        ))
                    },
                )?,
            );
            let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID,
                model.clone(),
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            (runtime_config, client)
        }
        AppServerProviderConfig::BifrostOpenAIResponses {
            base_url,
            api_key,
            max_tokens,
            stream,
            ..
        } => {
            let adapter: std::sync::Arc<dyn verlet_provider::ProviderWireAdapter> =
                std::sync::Arc::new(verlet_provider::OpenAIResponsesAdapter {
                    include_encrypted_reasoning: false,
                    reasoning_summary: verlet_provider::OpenAIReasoningSummary::Auto,
                });
            let client = std::sync::Arc::new(
                verlet_provider::ProviderHttpClient::new(
                    verlet_provider::ProviderEndpoint::openai_responses(base_url, api_key.clone()),
                    adapter,
                )
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to build Bifrost OpenAI provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIResponses,
                model_provider,
                selected_model,
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            (runtime_config, client)
        }
        AppServerProviderConfig::OpenAIChatCompletions {
            base_url,
            api_key,
            max_tokens,
            stream,
            headers,
            ..
        } => {
            let adapter: std::sync::Arc<dyn verlet_provider::ProviderWireAdapter> =
                std::sync::Arc::new(verlet_provider::OpenAIChatCompletionsAdapter);
            let mut endpoint = verlet_provider::ProviderEndpoint::openai_chat_completions(
                base_url,
                api_key.clone(),
            );
            endpoint.headers = headers.clone();
            let client = std::sync::Arc::new(
                verlet_provider::ProviderHttpClient::new(endpoint, adapter).map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to build OpenAI Chat Completions provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::OpenAIChatCompletions,
                model_provider,
                selected_model,
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            (runtime_config, client)
        }
        AppServerProviderConfig::AnthropicMessages {
            base_url,
            api_key,
            max_tokens,
            stream,
            ..
        } => {
            let adapter: std::sync::Arc<dyn verlet_provider::ProviderWireAdapter> =
                std::sync::Arc::new(verlet_provider::AnthropicMessagesAdapter);
            let client = std::sync::Arc::new(
                verlet_provider::ProviderHttpClient::new(
                    verlet_provider::ProviderEndpoint::anthropic_messages(
                        base_url,
                        api_key.clone(),
                    ),
                    adapter,
                )
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to build Anthropic Messages provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::AnthropicMessages,
                model_provider,
                selected_model,
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            (runtime_config, client)
        }
        AppServerProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            max_tokens,
            stream,
            ..
        } => {
            let adapter: std::sync::Arc<dyn verlet_provider::ProviderWireAdapter> =
                std::sync::Arc::new(verlet_provider::AnthropicBedrockMessagesAdapter);
            let endpoint = if let Some(base_url) = base_url {
                verlet_provider::ProviderEndpoint::anthropic_bedrock_with_base_url(
                    base_url,
                    region,
                    selected_model,
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone(),
                )
            } else {
                verlet_provider::ProviderEndpoint::anthropic_bedrock(
                    region,
                    selected_model,
                    access_key_id.clone(),
                    secret_access_key.clone(),
                    session_token.clone(),
                )
            };
            let client = std::sync::Arc::new(
                verlet_provider::ProviderHttpClient::new(endpoint, adapter).map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to build Anthropic Bedrock provider client: {err}"
                    ))
                })?,
            );
            let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
                verlet_history::ProviderApi::AnthropicMessages,
                model_provider,
                selected_model,
            );
            runtime_config.max_tokens = *max_tokens;
            runtime_config.stream = *stream;
            (runtime_config, client)
        }
        AppServerProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            max_tokens,
            stream,
            ..
        } => {
            return resolve_catalog_provider(
                provider_store,
                auth_store,
                auth_context,
                provider_id,
                Some(selected_model),
                *max_tokens,
                *stream,
            )
            .await;
        }
    };
    Ok(crate::adapters::agent_loop::ResolvedTurnEndpoint {
        config: runtime_config,
        client,
    })
}

#[cfg(test)]
pub(crate) fn runtime_factory_from_provider_parts(
    runtime_config: crate::adapters::agent_loop::AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    // lexicon-allow: capsule - existing app-server config API names
    capsule_bindings: CapsuleBindingsConfig,
) -> std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_secret_resolver(
        runtime_config,
        client,
        capsule_bindings,
        None,
    )
}

#[cfg(test)]
pub(crate) fn runtime_factory_from_provider_parts_with_secret_resolver(
    runtime_config: crate::adapters::agent_loop::AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    // lexicon-allow: capsule - existing app-server config field name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<std::sync::Arc<dyn verlet_metadata::secret_store::SecretResolver>>,
) -> std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_store_paths(
        runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config value
        capsule_bindings,
        secret_resolver,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        None,
        crate::agent::manifest_bind::AgentManifestPlacementBinding::default(),
        None,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        None,
        None,
    )
}

#[cfg(test)]
pub(crate) fn runtime_factory_from_provider_parts_with_turn_endpoint_router(
    runtime_config: crate::adapters::agent_loop::AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    capsule_bindings: CapsuleBindingsConfig,
    turn_endpoint_router: std::sync::Arc<AppServerTurnEndpointRouter>,
) -> std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_store_paths(
        runtime_config,
        client,
        capsule_bindings,
        None,
        None,
        None,
        None,
        0,
        None,
        None,
        None,
        None,
        crate::agent::manifest_bind::AgentManifestPlacementBinding::default(),
        None,
        std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        None,
        Some(turn_endpoint_router),
    )
}

#[cfg(test)]
pub(crate) fn runtime_factory_from_provider_parts_with_app_paths(
    runtime_config: crate::adapters::agent_loop::AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<std::sync::Arc<dyn verlet_metadata::secret_store::SecretResolver>>,
    config: &VerletAppServerConfig,
) -> std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_app_paths_and_router(
        runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config argument
        capsule_bindings,
        secret_resolver,
        config,
        None,
    )
}

fn runtime_factory_from_provider_parts_with_app_paths_and_router(
    runtime_config: crate::adapters::agent_loop::AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    // lexicon-allow: capsule - existing app-server config type name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<std::sync::Arc<dyn verlet_metadata::secret_store::SecretResolver>>,
    config: &VerletAppServerConfig,
    turn_endpoint_router: Option<std::sync::Arc<AppServerTurnEndpointRouter>>,
) -> std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory> {
    runtime_factory_from_provider_parts_with_store_paths(
        runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config value
        capsule_bindings,
        secret_resolver,
        Some(config.metadata_store_path()),
        Some(config.user_metadata_store_path()),
        Some(config.state_home.join("session_history.sqlite3")),
        config.lease_epoch,
        Some(config.agent_registry_root.clone()),
        Some(config.blob_registry_root.clone()),
        Some(config.skill_registry_root.clone()),
        Some(config.cwd.clone()),
        config.default_placement.clone(),
        config.default_workspace.clone(),
        std::sync::Arc::clone(&config.remote_event_store_served),
        config.instance_environment.hook_shell.clone(),
        turn_endpoint_router,
    )
}

fn runtime_factory_from_provider_parts_with_store_paths(
    runtime_config: crate::adapters::agent_loop::AgentLoopConfig,
    client: std::sync::Arc<dyn verlet_provider::ProviderClient>,
    // lexicon-allow: capsule - existing app-server config type name
    capsule_bindings: CapsuleBindingsConfig,
    secret_resolver: Option<std::sync::Arc<dyn verlet_metadata::secret_store::SecretResolver>>,
    metadata_store_path: Option<std::path::PathBuf>,
    secret_store_path: Option<std::path::PathBuf>,
    session_store_path: Option<std::path::PathBuf>,
    lease_epoch: u64,
    agent_registry_root: Option<std::path::PathBuf>,
    blob_registry_root: Option<std::path::PathBuf>,
    skill_registry_root: Option<std::path::PathBuf>,
    cwd: Option<std::path::PathBuf>,
    default_placement: crate::agent::manifest_bind::AgentManifestPlacementBinding,
    default_workspace: Option<crate::agent::manifest_bind::AgentManifestWorkspaceBinding>,
    remote_event_store_served: std::sync::Arc<std::sync::atomic::AtomicBool>,
    hook_shell: Option<String>,
    turn_endpoint_router: Option<std::sync::Arc<AppServerTurnEndpointRouter>>,
) -> std::sync::Arc<dyn crate::kernel::runtime_host::runtime_api::AgentRuntimeFactory> {
    // lexicon-allow: capsule - existing app-server runtime factory name
    std::sync::Arc::new(threads::CapsuleBindingRuntimeFactory {
        config: runtime_config,
        client,
        // lexicon-allow: capsule - existing app-server config field
        capsule_bindings,
        secret_resolver,
        metadata_store_path,
        secret_store_path,
        session_store_path,
        lease_epoch,
        agent_registry_root,
        blob_registry_root,
        skill_registry_root,
        cwd,
        hook_shell,
        default_placement,
        default_workspace,
        remote_event_store_served,
        turn_endpoint_router: turn_endpoint_router.map(|router| {
            router as std::sync::Arc<dyn crate::adapters::agent_loop::TurnEndpointRouter>
        }),
    })
}

async fn secret_resolver_from_config(
    config: &VerletAppServerConfig,
) -> crate::kernel::runtime_host::VerletResult<
    Option<std::sync::Arc<dyn verlet_metadata::secret_store::SecretResolver>>,
> {
    let store =
        verlet_metadata::secret_store::SqliteSecretStore::open(config.user_metadata_store_path())
            .await
            .map_err(secret_store_error)?;
    Ok(Some(std::sync::Arc::new(store)))
}

fn websocket_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_frame_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
        .max_message_size(Some(MAX_WEBSOCKET_MESSAGE_SIZE))
}

#[allow(clippy::result_large_err)]
async fn accept_authenticated_websocket<S>(
    stream: S,
) -> Result<tokio_tungstenite::WebSocketStream<S>, tokio_tungstenite::tungstenite::Error>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let upgrade = tokio_tungstenite::accept_hdr_async_with_config(
        stream,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
         mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if let Some(protocol) = request
                .headers()
                .get_all(tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL)
                .iter()
                .filter_map(|value| value.to_str().ok())
                .flat_map(|value| value.split(',').map(str::trim))
                .find(|protocol| console_protocol_token(protocol).is_some())
                .and_then(|protocol| {
                    tokio_tungstenite::tungstenite::http::HeaderValue::from_str(protocol).ok()
                })
            {
                response.headers_mut().insert(
                    tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL,
                    protocol,
                );
            }
            Ok(response)
        },
        Some(websocket_config()),
    );
    tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, upgrade)
        .await
        .map_err(|_| {
            tokio_tungstenite::tungstenite::Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "Verlet app-server websocket upgrade timed out",
            ))
        })?
}

async fn bind_websocket_listener(
    addr: std::net::SocketAddr,
) -> crate::kernel::runtime_host::VerletResult<tokio::net::TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "app-server websocket listen address {addr} is not loopback; configure websocket auth before binding non-loopback addresses"
            ),
        ));
    }
    tokio::net::TcpListener::bind(addr).await.map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to bind Verlet app-server websocket listener {addr}: {err}"
        ))
    })
}

pub(crate) struct HttpRequestHead {
    method: String,
    path: String,
    has_query: bool,
    headers: Vec<(String, String)>,
}

impl HttpRequestHead {
    pub(crate) fn method(&self) -> &str {
        &self.method
    }

    pub(crate) fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn has_query(&self) -> bool {
        self.has_query
    }
}

pub(crate) async fn peek_http_request(
    stream: &tokio::net::TcpStream,
) -> crate::kernel::runtime_host::VerletResult<Option<HttpRequestHead>> {
    let mut request = [0_u8; MAX_HTTP_REQUEST_HEADER_BYTES];
    let inspected = tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, async {
        loop {
            let len = stream.peek(&mut request).await.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
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
async fn peek_unix_http_request(
    stream: &tokio::net::UnixStream,
) -> crate::kernel::runtime_host::VerletResult<Option<HttpRequestHead>> {
    let mut request = [0_u8; MAX_HTTP_REQUEST_HEADER_BYTES];
    let inspected = tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, async {
        loop {
            stream.readable().await.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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
                let error = std::io::Error::last_os_error();
                if error.kind() == std::io::ErrorKind::WouldBlock {
                    continue;
                }
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!("failed to inspect Verlet app-server unix request: {error}"),
                ));
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
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
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
    let (path, has_query) = target
        .split_once('?')
        .map(|(path, _)| (path, true))
        .unwrap_or((target, false));
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect();
    Some(HttpRequestHead {
        method,
        path: path.to_string(),
        has_query,
        headers,
    })
}

fn console_asset_relative_path(path: &str) -> Option<std::path::PathBuf> {
    if matches!(path, "/" | "/index.html") {
        return Some(std::path::PathBuf::from("index.html"));
    }

    let mut relative = std::path::PathBuf::new();
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

pub(crate) fn request_bearer_token(
    request: &HttpRequestHead,
) -> Option<(&str, crate::daemon::identity::BoundarySurface)> {
    if let Some(token) = request
        .headers
        .iter()
        .filter(|(name, _)| name == "authorization")
        .find_map(|(_, value)| authorization_bearer_token(value))
    {
        return Some((token, crate::daemon::identity::BoundarySurface::Websocket));
    }
    request
        .headers
        .iter()
        .filter(|(name, _)| name == "sec-websocket-protocol")
        .flat_map(|(_, value)| value.split(',').map(str::trim))
        .find_map(|protocol| {
            console_protocol_token(protocol)
                .map(|token| (token, crate::daemon::identity::BoundarySurface::Console))
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
        .filter(|token| !token.is_empty() && !token.chars().any(char::is_whitespace))
}

fn content_type_for_path(path: &std::path::Path) -> &'static str {
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
) -> crate::kernel::runtime_host::VerletResult<()>
where
    S: tokio::io::AsyncWrite + Unpin,
{
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to write Verlet app-server HTTP response: {err}"
        ))
    })?;
    stream.write_all(body).await.map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to write Verlet app-server HTTP response body: {err}"
        ))
    })?;
    Ok(())
}

pub(crate) async fn consume_http_request_headers<S>(
    stream: &mut S,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut chunk = [0_u8; 512];
    let consumed = tokio::time::timeout(HTTP_REQUEST_HEADER_TIMEOUT, async {
        let mut matched = 0;
        let mut total = 0;
        loop {
            let len = stream.read(&mut chunk).await.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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

fn credential_ref(auth: &crate::daemon::identity::AuthenticationPath) -> String {
    match auth {
        crate::daemon::identity::AuthenticationPath::Credential { credential_id } => {
            credential_id.clone()
        }
        crate::daemon::identity::AuthenticationPath::PeerUid { uid } => format!("peer_uid:{uid}"),
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
fn prepare_unix_socket_path(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if let Some(parent) = path.parent() {
        let parent_existed = parent.exists();
        std::fs::create_dir_all(parent).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to create app-server socket directory {}: {err}",
                parent.display()
            ))
        })?;
        if !parent_existed {
            std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700)).map_err(
                |err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "failed to secure app-server socket directory {}: {err}",
                        parent.display()
                    ))
                },
            )?;
        }
    }
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "failed to inspect existing app-server socket {}: {error}",
                    path.display()
                ),
            ));
        }
    };
    if let Some(metadata) = metadata {
        use std::os::unix::fs::FileTypeExt as _;
        if !metadata.file_type().is_socket() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "refusing to replace non-socket app-server path {}",
                    path.display()
                ),
            ));
        }
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(stream) => {
                drop(stream);
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "refusing to replace live app-server socket {}",
                        path.display()
                    ),
                ));
            }
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::NotFound
                ) => {}
            Err(error) => {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "failed to verify existing app-server socket {} is stale: {error}",
                        path.display()
                    ),
                ));
            }
        }
        std::fs::remove_file(path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to remove stale app-server socket {}: {err}",
                path.display()
            ))
        })?;
    }
    Ok(())
}
