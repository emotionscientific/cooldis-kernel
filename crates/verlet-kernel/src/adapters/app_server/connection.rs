use crate::AgentKernelToolProvider as _;
use crate::EventStore as _;
use crate::LlmProviderAuthStore as _;
use crate::LlmProviderCatalogStore as _;
use crate::ThreadMetadataStore as _;
use base64::Engine as _;
use futures_util::SinkExt as _;
use futures_util::StreamExt as _;

#[derive(Clone)]
pub(super) struct ConnectionState {
    pub(super) app: crate::adapters::app_server::VerletAppServer,
    pub(super) resolved_principal: crate::daemon::identity::ResolvedPrincipal,
    pub(super) witnessed_session_id: String,
    pub(super) boundary_surface: crate::daemon::identity::BoundarySurface,
    pub(super) outbound: tokio::sync::mpsc::UnboundedSender<JsonRpcMessage>,
    pub(super) handshake: std::sync::Arc<tokio::sync::Mutex<HandshakeState>>,
    pub(super) opt_out_notifications:
        std::sync::Arc<tokio::sync::RwLock<std::collections::HashSet<String>>>,
    pub(super) subscriptions:
        std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, u64>>>,
    pub(super) fs_watches:
        std::sync::Arc<tokio::sync::Mutex<std::collections::HashMap<String, std::path::PathBuf>>>,
}

#[derive(Default)]
pub(super) struct HandshakeState {
    pub(super) initialize_seen: bool,
    pub(super) initialized_seen: bool,
    pub(super) client_name: Option<String>,
    pub(super) client_version: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Integer(i64),
}

impl std::fmt::Display for RequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(value) => f.write_str(value),
            Self::Integer(value) => write!(f, "{value}"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum JsonRpcMessage {
    Request(JsonRpcRequest),
    Notification(JsonRpcNotification),
    Response(JsonRpcResponse),
    Error(JsonRpcError),
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcRequest {
    pub id: RequestId,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcNotification {
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcResponse {
    pub id: RequestId,
    pub result: serde_json::Value,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcError {
    pub error: JsonRpcErrorError,
    pub id: RequestId,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct JsonRpcErrorError {
    pub code: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<serde_json::Value>,
    pub message: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InitializeParams {
    pub(super) client_info: ClientInfo,
    #[serde(default)]
    pub(super) capabilities: Option<InitializeCapabilities>,
}

#[derive(Debug, serde::Deserialize)]
pub(super) struct ClientInfo {
    pub(super) name: String,
    #[serde(default)]
    pub(super) title: Option<String>,
    pub(super) version: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct InitializeCapabilities {
    #[serde(default)]
    pub(super) experimental_api: bool,
    #[serde(default)]
    pub(super) request_attestation: bool,
    #[serde(default)]
    pub(super) opt_out_notification_methods: Option<Vec<String>>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadStartParams {
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) model_provider: Option<String>,
    #[serde(default)]
    pub(super) service_tier: Option<String>,
    #[serde(default)]
    pub(super) cwd: Option<String>,
    #[serde(default)]
    pub(super) ephemeral: Option<bool>,
    #[serde(default)]
    pub(super) parent_thread_id: Option<String>,
    #[serde(default)]
    pub(super) topology: Option<crate::ThreadTopology>,
    #[serde(default)]
    pub(super) capsule_bindings: Option<ThreadCapsuleBindingsParams>,
    #[serde(default)]
    pub(super) agent_ref: Option<String>,
    #[serde(default)]
    pub(super) runtime_overrides: Option<crate::AgentManifestBindOverrides>,
    #[serde(default)]
    pub(super) placement: Option<crate::AgentManifestPlacementBinding>,
    #[serde(default)]
    pub(super) workspace: Option<crate::AgentManifestWorkspaceBinding>,
    #[serde(
        default,
        deserialize_with = "crate::adapters::app_server::threads::deserialize_optional_thinking"
    )]
    pub(super) thinking: Option<crate::ThinkingConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadSpawnParams {
    pub(super) thread_id: String,
    pub(super) task_name: String,
    pub(super) message: String,
    #[serde(default)]
    pub(super) agent_ref: Option<String>,
    #[serde(default)]
    pub(super) placement: Option<crate::AgentManifestPlacementBinding>,
    #[serde(default)]
    pub(super) workspace: Option<crate::AgentManifestWorkspaceBinding>,
    #[serde(default)]
    pub(super) dispatch_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadSubmitParams {
    pub(super) thread_id: String,
    pub(super) message: String,
    #[serde(default)]
    pub(super) dispatch_id: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadCapsuleBindingsParams {
    #[serde(default)]
    pub(super) operation_names: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CapsuleBindingSetParams {
    pub(super) scope: crate::CapsuleBindingScope,
    pub(super) operation_name: String,
    #[serde(default)]
    pub(super) artifact_hash: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CapsuleBindingOperationParams {
    pub(super) scope: crate::CapsuleBindingScope,
    pub(super) operation_name: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CapsuleBindingListParams {
    pub(super) scope: crate::CapsuleBindingScope,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CapsuleBindingResolveParams {
    #[serde(default)]
    pub(super) tenant_id: Option<String>,
    #[serde(default)]
    pub(super) thread_id: Option<String>,
    #[serde(default)]
    pub(super) operation_names: Vec<String>,
    #[serde(default)]
    pub(super) load_all_active_when_unbound: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AgentReadParams {
    #[serde(rename = "ref")]
    pub(super) ref_uri: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct AgentDraftParams {
    #[serde(default)]
    pub(super) source: Option<String>,
    #[serde(default)]
    pub(super) manifest: Option<serde_json::Value>,
    #[serde(default)]
    pub(super) base_ref: Option<String>,
    #[serde(default)]
    pub(super) base_manifest_hash: Option<String>,
    #[serde(default)]
    pub(super) expected_latest_version: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderReadParams {
    pub(super) provider_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderUpsertParams {
    pub(super) provider: ModelProviderUpsertRecord,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderDeleteParams {
    pub(super) provider_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderUpsertRecord {
    pub(super) provider_id: String,
    pub(super) api: serde_json::Value,
    pub(super) base_url: String,
    #[serde(default)]
    pub(super) display_name: Option<String>,
    #[serde(default)]
    pub(super) auth: crate::LlmProviderAuthConfig,
    #[serde(default)]
    pub(super) headers: std::collections::BTreeMap<String, crate::LlmProviderConfigValue>,
    #[serde(default)]
    pub(super) auth_header: bool,
    #[serde(default)]
    pub(super) models: Vec<ModelProviderModelUpsertRecord>,
    #[serde(default)]
    pub(super) metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderModelUpsertRecord {
    pub(super) model_id: String,
    #[serde(default)]
    pub(super) display_name: Option<String>,
    #[serde(default)]
    pub(super) api: Option<serde_json::Value>,
    #[serde(default)]
    pub(super) base_url: Option<String>,
    #[serde(default)]
    pub(super) context_window_tokens: Option<u64>,
    #[serde(default)]
    pub(super) max_output_tokens: Option<u32>,
    #[serde(default)]
    pub(super) input_modalities: Vec<crate::LlmProviderInputModality>,
    #[serde(default)]
    pub(super) headers: std::collections::BTreeMap<String, crate::LlmProviderConfigValue>,
    #[serde(default)]
    pub(super) metadata: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderAuthStatusParams {
    #[serde(default)]
    pub(super) provider_id: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderAuthSetParams {
    pub(super) provider_id: String,
    pub(super) api_key: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelProviderAuthDeleteParams {
    pub(super) provider_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MandateStartParams {
    pub(super) thread_id: String,
    pub(super) schedule: crate::MandateSchedulePayload,
    #[serde(default)]
    pub(super) max_occurrences: Option<u32>,
    #[serde(default)]
    pub(super) catch_up: Option<crate::MandateCatchUpPolicy>,
    #[serde(default)]
    pub(super) input_template: Option<String>,
    #[serde(default)]
    pub(super) expires_at: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MandateRevokeParams {
    pub(super) thread_id: String,
    pub(super) mandate_event_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct MandateListParams {
    pub(super) thread_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct McpSourceReadParams {
    pub(super) name: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct McpSourceUpsertParams {
    pub(super) name: String,
    #[serde(default)]
    pub(super) transport: Option<String>,
    #[serde(default)]
    pub(super) kind: Option<String>,
    pub(super) url: String,
    #[serde(default)]
    pub(super) bearer_secret: Option<String>,
    #[serde(default)]
    pub(super) bearer_token: Option<String>,
    #[serde(default)]
    pub(super) headers: Vec<McpSourceHeaderParam>,
    #[serde(default)]
    pub(super) include_tools: Vec<String>,
    #[serde(default)]
    pub(super) timeout_ms: Option<u64>,
    #[serde(default)]
    pub(super) max_output_bytes: Option<u64>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct McpSourceHeaderParam {
    pub(super) name: String,
    pub(super) value: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct McpSourceTestToolParams {
    pub(super) name: String,
    pub(super) tool: String,
    #[serde(default)]
    pub(super) arguments: serde_json::Value,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct McpSourceManifestPatchParams {
    pub(super) name: String,
    #[serde(default)]
    pub(super) import_id: Option<String>,
    #[serde(default)]
    pub(super) agent_ref: Option<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadForkParams {
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) checkpoint_id: Option<String>,
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) model_provider: Option<String>,
    #[serde(default)]
    pub(super) service_tier: Option<String>,
    #[serde(default)]
    pub(super) cwd: Option<String>,
    #[serde(default)]
    pub(super) ephemeral: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadRebindForkParams {
    pub(super) thread_id: String,
    pub(super) agent_ref: String,
    #[serde(default)]
    pub(super) checkpoint_id: Option<String>,
    #[serde(default)]
    pub(super) model_profile_id: Option<String>,
    #[serde(default)]
    pub(super) runtime_overrides: Option<crate::AgentManifestBindOverrides>,
    #[serde(default)]
    pub(super) placement: Option<crate::AgentManifestPlacementBinding>,
    #[serde(default)]
    pub(super) workspace: Option<crate::AgentManifestWorkspaceBinding>,
    #[serde(default)]
    pub(super) reason: crate::ThreadForkReason,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadResumeParams {
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(default)]
    pub(super) model_provider: Option<String>,
    #[serde(default)]
    pub(super) service_tier: Option<String>,
    #[serde(default)]
    pub(super) cwd: Option<String>,
    #[serde(default)]
    pub(super) exclude_turns: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TurnStartParams {
    pub(super) thread_id: String,
    pub(super) input: Vec<serde_json::Value>,
    #[serde(default)]
    pub(super) cwd: Option<String>,
    #[serde(default)]
    pub(super) model: Option<String>,
    #[serde(
        default,
        deserialize_with = "crate::adapters::app_server::threads::deserialize_optional_thinking"
    )]
    pub(super) thinking: Option<crate::ThinkingConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TurnSteerParams {
    pub(super) thread_id: String,
    pub(super) input: Vec<serde_json::Value>,
    pub(super) expected_turn_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct TurnInterruptParams {
    pub(super) thread_id: String,
    pub(super) turn_id: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadReadParams {
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) include_turns: Option<bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadEventsListParams {
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) stream: Option<String>,
    #[serde(default)]
    pub(super) cursor: Option<String>,
    #[serde(default)]
    pub(super) stream_cursor: Option<crate::StreamCursorV1>,
    #[serde(default)]
    pub(super) limit: Option<usize>,
    #[serde(default)]
    pub(super) kinds: Vec<String>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadControlListParams {
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) limit: Option<usize>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadDebugExportParams {
    pub(super) thread_id: String,
    #[serde(default)]
    pub(super) streams: Vec<String>,
    #[serde(default)]
    pub(super) include_thread: Option<bool>,
    #[serde(default)]
    pub(super) max_events_per_stream: Option<usize>,
    #[serde(default)]
    pub(super) redact: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadUnsubscribeParams {
    pub(super) thread_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadSetNameParams {
    pub(super) thread_id: String,
    pub(super) name: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadMetadataUpdateParams {
    pub(super) thread_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadCompactStartParams {
    pub(super) thread_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ThreadShellCommandParams {
    pub(super) thread_id: String,
    pub(super) command: String,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConfigReadParams {
    #[serde(default)]
    pub(super) include_layers: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsReadFileParams {
    pub(super) path: std::path::PathBuf,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsWriteFileParams {
    pub(super) path: std::path::PathBuf,
    pub(super) data_base64: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsCreateDirectoryParams {
    pub(super) path: std::path::PathBuf,
    #[serde(default)]
    pub(super) recursive: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsGetMetadataParams {
    pub(super) path: std::path::PathBuf,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsReadDirectoryParams {
    pub(super) path: std::path::PathBuf,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsRemoveParams {
    pub(super) path: std::path::PathBuf,
    #[serde(default)]
    pub(super) recursive: Option<bool>,
    #[serde(default)]
    pub(super) force: Option<bool>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsCopyParams {
    pub(super) source_path: std::path::PathBuf,
    pub(super) destination_path: std::path::PathBuf,
    #[serde(default)]
    pub(super) recursive: bool,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsWatchParams {
    pub(super) watch_id: String,
    pub(super) path: std::path::PathBuf,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct FsUnwatchParams {
    pub(super) watch_id: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CommandExecParams {
    #[serde(default)]
    pub(super) command: Vec<String>,
    #[serde(default)]
    pub(super) process_id: Option<String>,
    #[serde(default)]
    pub(super) tty: bool,
    #[serde(default)]
    pub(super) stream_stdin: bool,
    #[serde(default)]
    pub(super) stream_stdout_stderr: bool,
    #[serde(default)]
    pub(super) output_bytes_cap: Option<usize>,
    #[serde(default)]
    pub(super) disable_output_cap: bool,
    #[serde(default)]
    pub(super) disable_timeout: bool,
    #[serde(default)]
    pub(super) timeout_ms: Option<u64>,
    #[serde(default)]
    pub(super) yield_time_ms: Option<u64>,
    #[serde(default)]
    pub(super) cwd: Option<String>,
    #[serde(default)]
    pub(super) env: Option<std::collections::HashMap<String, Option<String>>>,
    #[serde(default)]
    pub(super) dispatch_id: Option<String>,
    /// Consumer of a handle-returning streaming execution. Buffered
    /// command/exec does not create a handle and does not require this field.
    #[serde(default)]
    pub(super) thread_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ApprovalResolveDecision {
    Approved,
    Denied,
}

impl ApprovalResolveDecision {
    fn approved(self) -> bool {
        matches!(self, Self::Approved)
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Denied => "denied",
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ApprovalResolveParams {
    pub(super) thread_id: String,
    pub(super) approval_id: String,
    pub(super) decision: ApprovalResolveDecision,
    #[serde(default)]
    pub(super) reason: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CommandExecProcessParams {
    pub(super) process_id: String,
    #[serde(default)]
    pub(super) yield_time_ms: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CommandExecWriteParams {
    pub(super) process_id: String,
    pub(super) delta_base64: String,
    #[serde(default)]
    pub(super) yield_time_ms: Option<u64>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CommandExecTerminateParams {
    pub(super) process_id: String,
    #[serde(default)]
    pub(super) reason: Option<String>,
    #[serde(default)]
    pub(super) yield_time_ms: Option<u64>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExperimentalFeatureEnablementSetParams {
    #[serde(default)]
    pub(super) enablement: std::collections::BTreeMap<String, bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetAuthStatusParams {
    #[serde(default)]
    pub(super) include_token: Option<bool>,
    #[serde(default)]
    pub(super) refresh_token: Option<bool>,
}

#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct GetConversationSummaryParams {
    #[serde(default)]
    pub(super) conversation_id: Option<String>,
    #[serde(default)]
    pub(super) rollout_path: Option<String>,
}

const METHOD_NOT_AUTHORIZED_CODE: i64 = -32003;

pub(super) const DISPATCH_METHOD_AUTHORITY_CLASSES: &[(
    &str,
    crate::daemon::identity::AuthorityClass,
)] = &[
    // Compatibility reads and local registry projections.
    (
        "account/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "account/rateLimits/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "app/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "capsule/binding/set",
        crate::daemon::identity::AuthorityClass::Host,
    ), // lexicon-allow: capsule - existing RPC method.
    (
        "capsule/binding/delete",
        crate::daemon::identity::AuthorityClass::Host,
    ), // lexicon-allow: capsule - existing RPC method.
    (
        "capsule/binding/list",
        crate::daemon::identity::AuthorityClass::Host,
    ), // lexicon-allow: capsule - existing RPC method.
    (
        "capsule/binding/resolve",
        crate::daemon::identity::AuthorityClass::Host,
    ), // lexicon-allow: capsule - existing RPC method.
    (
        "agent/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "agent/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "agent/plan",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "agent/publish",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "operation/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "command/exec",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "command/exec/write",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "command/exec/terminate",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "command/exec/resize",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "model/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "modelProvider/capabilities/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "modelProvider/list",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "modelProvider/read",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "modelProvider/upsert",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "modelProvider/delete",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "modelProvider/auth/status",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "modelProvider/auth/set",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "modelProvider/auth/delete",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "experimentalFeature/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    // This is currently an echo-only compatibility stub. Reclassify it before
    // wiring the method to daemon configuration or other host state.
    (
        "experimentalFeature/enablement/set",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "getAuthStatus",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "getConversationSummary",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "ingress/submit",
        crate::daemon::identity::AuthorityClass::Ingress,
    ),
    (
        "stream/append",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "stream/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    // These construct or reconstruct runtime bindings and may resolve secrets.
    (
        "thread/start",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "thread/spawn",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "thread/submit",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    // A clone fork replays the source thread's witnessed standing grant. It
    // cannot select a new manifest, placement, manifest workspace binding, or
    // tool policy; its cwd/model fields are presentation/runtime controls.
    (
        "thread/fork",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    // A rebind fork can select all of those surfaces, so it remains Host.
    (
        "thread/rebindFork",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "thread/resume",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "thread/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/events/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/couplings/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/approvals/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/waiting/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "approval/resolve",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mandate/start",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "mandate/revoke",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "mandate/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/debug/export",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "thread/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/loaded/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/unsubscribe",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/name/set",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/metadata/update",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    // Compaction runs inside an existing bound thread and obtains no new grant.
    (
        "thread/compact/start",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "thread/shellCommand",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    // Ingress may lazily reconstruct the thread from its witnessed standing
    // grant. Ingress-only principals are separately barred from turn controls.
    (
        "turn/start",
        crate::daemon::identity::AuthorityClass::Ingress,
    ),
    // Steering only adds input to an already-running, already-bound turn.
    (
        "turn/steer",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "turn/interrupt",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "skills/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "plugin/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "hooks/list",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "mcpServerStatus/list",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/list",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/read",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/upsert",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/discover",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/delete",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/testTool",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "mcpSource/manifestPatch",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    ("fs/readFile", crate::daemon::identity::AuthorityClass::Host),
    (
        "fs/writeFile",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "fs/createDirectory",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "fs/getMetadata",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    (
        "fs/readDirectory",
        crate::daemon::identity::AuthorityClass::Host,
    ),
    ("fs/remove", crate::daemon::identity::AuthorityClass::Host),
    ("fs/copy", crate::daemon::identity::AuthorityClass::Host),
    ("fs/watch", crate::daemon::identity::AuthorityClass::Host),
    ("fs/unwatch", crate::daemon::identity::AuthorityClass::Host),
    (
        "config/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
    (
        "configRequirements/read",
        crate::daemon::identity::AuthorityClass::Interactive,
    ),
];

pub(super) const HOST_EFFECT_METHODS: &[&str] = &[
    // Local registry mutations.
    "capsule/binding/set",    // lexicon-allow: capsule - existing RPC method.
    "capsule/binding/delete", // lexicon-allow: capsule - existing RPC method.
    "agent/publish",
    // Processes and process-handle control.
    "command/exec",
    "command/exec/write",
    "command/exec/terminate",
    "command/exec/resize",
    // Provider configuration and secret status/mutations.
    "modelProvider/list",
    "modelProvider/read",
    "modelProvider/upsert",
    "modelProvider/delete",
    "modelProvider/auth/status",
    "modelProvider/auth/set",
    "modelProvider/auth/delete",
    // Runtime construction, reconstruction, and standing-grant changes.
    "thread/start",
    "thread/spawn",
    "thread/rebindFork",
    "thread/resume",
    // Client-owned durable store mutation.
    "stream/append",
    // Effects that may release or directly invoke host tools.
    "approval/resolve",
    "thread/shellCommand",
    // MCP secret/network effects. Plain list/read and manifestPatch are
    // redacted registry projections and do not resolve secret material.
    "mcpSource/upsert",
    "mcpSource/discover",
    "mcpSource/delete",
    "mcpSource/testTool",
    // Filesystem mutations; read/watch methods remain Host but are not effects.
    "fs/writeFile",
    "fs/createDirectory",
    "fs/remove",
    "fs/copy",
];

impl crate::adapters::app_server::VerletAppServer {
    pub async fn local_json_rpc_request(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> crate::VerletResult<serde_json::Value> {
        let resolved_principal = self
            .inner
            .identity_authority
            .resolve_peer_uid(crate::adapters::app_server::current_effective_uid())
            .await?
            .ok_or_else(|| {
                crate::VerletError::RuntimeFactory(
                    "local JSON-RPC requires the local-mode operator principal".to_string(),
                )
            })?;
        let session_id = format!("session_{}", uuid::Uuid::now_v7());
        let surface = crate::daemon::identity::BoundarySurface::UnixSocket;
        self.inner
            .identity_authority
            .witness_session_opened(&crate::daemon::identity::IdentitySessionV1 {
                schema: crate::daemon::identity::IDENTITY_SESSION_SCHEMA_V1.to_string(),
                session_id: session_id.clone(),
                principal_id: resolved_principal.principal_id.clone(),
                kind: resolved_principal.kind,
                surface,
                credential_ref: crate::adapters::app_server::credential_ref(
                    &resolved_principal.auth,
                ),
                opened_at_ms: self.inner.identity_clock.now().timestamp_millis(),
                closed_at_ms: None,
            })
            .await?;
        let mut close_witness = crate::adapters::app_server::SessionCloseWitness::new(
            std::sync::Arc::clone(&self.inner.identity_authority),
            std::sync::Arc::clone(&self.inner.identity_clock),
            session_id.clone(),
        );
        let (outbound, _rx) = tokio::sync::mpsc::unbounded_channel::<JsonRpcMessage>();
        let connection = ConnectionState {
            app: self.clone(),
            resolved_principal,
            witnessed_session_id: session_id,
            boundary_surface: surface,
            outbound,
            handshake: std::sync::Arc::new(tokio::sync::Mutex::new(HandshakeState::default())),
            opt_out_notifications: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashSet::new(),
            )),
            subscriptions: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            fs_watches: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        };
        connection
            .handle_initialize(Some(serde_json::json!({
                "clientInfo": {
                    "name": "local-json-rpc",
                    "title": null,
                    "version": env!("CARGO_PKG_VERSION"),
                },
                "capabilities": null,
            })))
            .await
            .map_err(jsonrpc_error_to_runtime_factory)?;
        let dispatch_result = self
            .dispatch_request(&connection, method, Some(params))
            .await
            .map_err(jsonrpc_error_to_runtime_factory);
        let close_result = close_witness.close().await;
        match (dispatch_result, close_result) {
            (Err(error), Err(close_error)) => {
                eprintln!(
                    "failed to witness closing a failed local JSON-RPC session: {close_error}"
                );
                Err(error)
            }
            (Err(error), Ok(())) => Err(error),
            (Ok(_), Err(error)) => Err(error),
            (Ok(value), Ok(())) => Ok(value),
        }
    }

    pub(super) async fn handle_websocket<S>(
        &self,
        websocket: tokio_tungstenite::WebSocketStream<S>,
        resolved_principal: crate::daemon::identity::ResolvedPrincipal,
        surface: crate::daemon::identity::BoundarySurface,
    ) -> crate::VerletResult<()>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let session_id = format!("session_{}", uuid::Uuid::now_v7());
        self.inner
            .identity_authority
            .witness_session_opened(&crate::daemon::identity::IdentitySessionV1 {
                schema: crate::daemon::identity::IDENTITY_SESSION_SCHEMA_V1.to_string(),
                session_id: session_id.clone(),
                principal_id: resolved_principal.principal_id.clone(),
                kind: resolved_principal.kind,
                surface,
                credential_ref: crate::adapters::app_server::credential_ref(
                    &resolved_principal.auth,
                ),
                opened_at_ms: self.inner.identity_clock.now().timestamp_millis(),
                closed_at_ms: None,
            })
            .await?;
        let mut close_witness = crate::adapters::app_server::SessionCloseWitness::new(
            std::sync::Arc::clone(&self.inner.identity_authority),
            std::sync::Arc::clone(&self.inner.identity_clock),
            session_id.clone(),
        );
        let (mut sink, mut stream) = websocket.split();
        let (outbound, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel::<JsonRpcMessage>();
        let writer = tokio::spawn(async move {
            while let Some(message) = outbound_rx.recv().await {
                let payload = match serde_json::to_string(&message) {
                    Ok(payload) => payload,
                    Err(err) => {
                        eprintln!("failed to encode Verlet app-server JSON-RPC message: {err}");
                        continue;
                    }
                };
                if let Err(err) = sink
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        payload.into(),
                    ))
                    .await
                {
                    eprintln!("failed to write Verlet app-server websocket message: {err}");
                    break;
                }
            }
        });

        let connection = ConnectionState {
            app: self.clone(),
            resolved_principal,
            witnessed_session_id: session_id,
            boundary_surface: surface,
            outbound,
            handshake: std::sync::Arc::new(tokio::sync::Mutex::new(HandshakeState::default())),
            opt_out_notifications: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashSet::new(),
            )),
            subscriptions: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            fs_watches: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
        };

        let mut read_result = Ok(());
        while let Some(message) = stream.next().await {
            match message {
                Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                    if let Err(err) = handle_inbound_text(&connection, &text).await {
                        eprintln!("Verlet app-server JSON-RPC handling failed: {err}");
                    }
                }
                Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => break,
                Ok(tokio_tungstenite::tungstenite::Message::Binary(_))
                | Ok(tokio_tungstenite::tungstenite::Message::Ping(_))
                | Ok(tokio_tungstenite::tungstenite::Message::Pong(_))
                | Ok(tokio_tungstenite::tungstenite::Message::Frame(_)) => {}
                Err(err) => {
                    read_result = Err(crate::VerletError::RuntimeFactory(format!(
                        "Verlet app-server websocket read failed: {err}"
                    )));
                    break;
                }
            }
        }

        connection.abort_subscriptions().await;
        writer.abort();
        let close_result = close_witness.close().await;
        finish_websocket_session(read_result, close_result)
    }

    pub(super) async fn handle_request(
        &self,
        connection: &ConnectionState,
        request: JsonRpcRequest,
    ) {
        let result = if request.method == "initialize" {
            connection.handle_initialize(request.params.clone()).await
        } else if !connection.initialize_seen().await {
            Err(jsonrpc_error(
                -32002,
                "connection must send initialize before app-server requests",
            ))
        } else {
            self.dispatch_request(connection, &request.method, request.params.clone())
                .await
        };

        let message = match result {
            Ok(result) => JsonRpcMessage::Response(JsonRpcResponse {
                id: request.id,
                result,
            }),
            Err(error) => JsonRpcMessage::Error(JsonRpcError {
                id: request.id,
                error,
            }),
        };
        let _ = connection.outbound.send(message);
    }

    pub(super) async fn dispatch_request(
        &self,
        connection: &ConnectionState,
        method: &str,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let authority_class = crate::daemon::identity::authority_class_for_method(method);
        debug_assert_eq!(
            DISPATCH_METHOD_AUTHORITY_CLASSES
                .iter()
                .find_map(|(listed_method, class)| (*listed_method == method).then_some(*class)),
            DISPATCH_METHOD_AUTHORITY_CLASSES
                .iter()
                .any(|(listed_method, _)| *listed_method == method)
                .then_some(authority_class),
            "dispatcher authority-class table drifted for {method}",
        );
        if !connection.resolved_principal.kind.permits(authority_class) {
            self.inner
                .identity_authority
                .witness_auth_rejected(&crate::daemon::identity::IdentityAuthRejectionV1 {
                    schema: crate::daemon::identity::IDENTITY_AUTH_REJECTION_SCHEMA_V1.to_string(),
                    surface: connection.boundary_surface,
                    reason:
                        crate::daemon::identity::IdentityAuthRejectionReason::MethodNotAuthorized {
                            method: method.to_string(),
                            class: authority_class,
                        },
                    principal_id: Some(connection.resolved_principal.principal_id.clone()),
                    rejected_at_ms: self.inner.identity_clock.now().timestamp_millis(),
                })
                .await
                .map_err(internal_error)?;
            return Err(jsonrpc_error(
                METHOD_NOT_AUTHORIZED_CODE,
                "request is not authorized for this principal",
            ));
        }
        let ingress_only = connection
            .resolved_principal
            .kind
            .permits(crate::daemon::identity::AuthorityClass::Ingress)
            && !connection
                .resolved_principal
                .kind
                .permits(crate::daemon::identity::AuthorityClass::Interactive)
            && !connection
                .resolved_principal
                .kind
                .permits(crate::daemon::identity::AuthorityClass::Host);
        if method == "turn/start"
            && ingress_only
            && params
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .is_some_and(|params| {
                    ["cwd", "model", "thinking"]
                        .iter()
                        .any(|name| params.contains_key(*name))
                })
        {
            return Err(jsonrpc_error(
                -32602,
                "turn/start parameter overrides are not available to this principal",
            ));
        }
        if HOST_EFFECT_METHODS.contains(&method) {
            self.inner
                .identity_authority
                .witness_host_effect(&crate::daemon::identity::IdentityHostEffectV1 {
                    schema: crate::daemon::identity::IDENTITY_HOST_EFFECT_SCHEMA_V1.to_string(),
                    session_id: connection.witnessed_session_id.clone(),
                    principal_id: connection.resolved_principal.principal_id.clone(),
                    method: method.to_string(),
                    witnessed_at_ms: self.inner.identity_clock.now().timestamp_millis(),
                })
                .await
                .map_err(internal_error)?;
        }
        match method {
            "account/read" => Ok(serde_json::json!({
                "account": null,
                "requiresOpenaiAuth": false,
            })),
            "account/rateLimits/read" => Ok(serde_json::json!({
                "rateLimits": empty_rate_limits(),
                "rateLimitsByLimitId": null,
            })),
            "app/list" => Ok(serde_json::json!({ "data": [], "nextCursor": null })),
            "capsule/binding/set" => {
                let params: CapsuleBindingSetParams = parse_params(params)?;
                self.capsule_binding_set(params)
            }
            "capsule/binding/delete" => {
                let params: CapsuleBindingOperationParams = parse_params(params)?;
                self.capsule_binding_delete(params)
            }
            "capsule/binding/list" => {
                let params: CapsuleBindingListParams = parse_params(params)?;
                self.capsule_binding_list(params)
            }
            "capsule/binding/resolve" => {
                let params: CapsuleBindingResolveParams = parse_params(params)?;
                self.capsule_binding_resolve(params)
            }
            "agent/list" => self.agent_list(),
            "agent/read" => {
                let params: AgentReadParams = parse_params(params)?;
                self.agent_read(params)
            }
            "agent/plan" => {
                let params: AgentDraftParams = parse_params(params)?;
                self.agent_plan(params)
            }
            "agent/publish" => {
                let params: AgentDraftParams = parse_params(params)?;
                self.agent_publish(params)
            }
            "operation/list" => self.operation_list(),
            "command/exec" => {
                let params: CommandExecParams = parse_params(params)?;
                self.command_exec(params).await
            }
            "command/exec/write" => {
                let params: CommandExecWriteParams = parse_params(params)?;
                self.command_exec_write(params).await
            }
            "command/exec/terminate" => {
                let params: CommandExecTerminateParams = parse_params(params)?;
                self.command_exec_terminate(params).await
            }
            "command/exec/resize" => {
                let params: CommandExecProcessParams = parse_params(params)?;
                self.command_exec_resize(params)
            }
            "model/list" => Ok(serde_json::json!({
                "data": self.model_list_json().await?,
                "nextCursor": null,
            })),
            "modelProvider/capabilities/read" => Ok(self.model_provider_capabilities_json().await),
            "modelProvider/list" => self.model_provider_list().await,
            "modelProvider/read" => {
                let params: ModelProviderReadParams = parse_params(params)?;
                self.model_provider_read(params).await
            }
            "modelProvider/upsert" => {
                let params: ModelProviderUpsertParams = parse_params(params)?;
                self.model_provider_upsert(params).await
            }
            "modelProvider/delete" => {
                let params: ModelProviderDeleteParams = parse_params(params)?;
                self.model_provider_delete(params).await
            }
            "modelProvider/auth/status" => {
                let params: ModelProviderAuthStatusParams = parse_params(params)?;
                self.model_provider_auth_status(params).await
            }
            "modelProvider/auth/set" => {
                let params: ModelProviderAuthSetParams = parse_params(params)?;
                self.model_provider_auth_set(params).await
            }
            "modelProvider/auth/delete" => {
                let params: ModelProviderAuthDeleteParams = parse_params(params)?;
                self.model_provider_auth_delete(params).await
            }
            "experimentalFeature/list" => Ok(serde_json::json!({ "data": [], "nextCursor": null })),
            "experimentalFeature/enablement/set" => {
                let params: ExperimentalFeatureEnablementSetParams = parse_params(params)?;
                Ok(serde_json::json!({ "enablement": params.enablement }))
            }
            "getAuthStatus" => {
                let params: GetAuthStatusParams = parse_params(params)?;
                let _ = (params.include_token, params.refresh_token);
                Ok(serde_json::json!({
                    "authMethod": null,
                    "authToken": null,
                    "requiresOpenaiAuth": false,
                    "principalId": connection.resolved_principal.principal_id,
                    "kind": connection.resolved_principal.kind,
                }))
            }
            "getConversationSummary" => {
                let params: GetConversationSummaryParams = parse_params(params)?;
                self.get_conversation_summary(params).await
            }
            "ingress/submit" => {
                let params: crate::adapters::app_server::orchestrator_boundary::IngressSubmitParams = parse_params(params)?;
                self.ingress_submit(connection, params).await
            }
            "stream/append" => {
                let params: crate::adapters::app_server::orchestrator_boundary::StreamAppendParams =
                    parse_params(params)?;
                self.stream_append(connection, params).await
            }
            "stream/read" => {
                let params: crate::adapters::app_server::orchestrator_boundary::StreamReadParams =
                    parse_params(params)?;
                self.stream_read(connection, params).await
            }
            "thread/start" => {
                let params: ThreadStartParams = parse_params(params)?;
                self.thread_start(connection, params).await
            }
            "thread/spawn" => {
                let params: ThreadSpawnParams = parse_params(params)?;
                self.thread_spawn(connection, params).await
            }
            "thread/submit" => {
                let params: ThreadSubmitParams = parse_params(params)?;
                self.thread_submit(params).await
            }
            "thread/fork" => {
                let params: ThreadForkParams = parse_params(params)?;
                self.thread_fork(connection, params).await
            }
            "thread/rebindFork" => {
                let params: ThreadRebindForkParams = parse_params(params)?;
                self.thread_rebind_fork(connection, params).await
            }
            "thread/resume" => {
                let params: ThreadResumeParams = parse_params(params)?;
                self.thread_resume(connection, params).await
            }
            "thread/read" => {
                let params: ThreadReadParams = parse_params(params)?;
                let thread = self
                    .thread_json_by_id(&params.thread_id, params.include_turns.unwrap_or(true))
                    .await?;
                Ok(serde_json::json!({ "thread": thread }))
            }
            "thread/events/list" => {
                let params: ThreadEventsListParams = parse_params(params)?;
                self.thread_events_list(params).await
            }
            "thread/couplings/list" => {
                let params: ThreadControlListParams = parse_params(params)?;
                self.thread_couplings_list(params).await
            }
            "thread/approvals/list" => {
                let params: ThreadControlListParams = parse_params(params)?;
                self.thread_approvals_list(params).await
            }
            "thread/waiting/list" => {
                let params: ThreadControlListParams = parse_params(params)?;
                self.thread_waiting_list(params).await
            }
            "approval/resolve" => {
                let params: ApprovalResolveParams = parse_params(params)?;
                self.approval_resolve(params).await
            }
            "mandate/start" => {
                let params: MandateStartParams = parse_params(params)?;
                self.mandate_start(params).await
            }
            "mandate/revoke" => {
                let params: MandateRevokeParams = parse_params(params)?;
                self.mandate_revoke(params).await
            }
            "mandate/list" => {
                let params: MandateListParams = parse_params(params)?;
                self.mandate_list(params).await
            }
            "thread/debug/export" => {
                let params: ThreadDebugExportParams = parse_params(params)?;
                self.thread_debug_export(params).await
            }
            "thread/list" => {
                let state = self.inner.state.read().await;
                let mut threads = state
                    .threads
                    .values()
                    .map(|thread| crate::adapters::app_server::threads::thread_json(thread, false))
                    .collect::<Vec<_>>();
                threads.sort_by(|left, right| {
                    right
                        .get("updatedAt")
                        .and_then(serde_json::Value::as_u64)
                        .cmp(&left.get("updatedAt").and_then(serde_json::Value::as_u64))
                });
                Ok(serde_json::json!({
                    "data": threads,
                    "nextCursor": null,
                    "backwardsCursor": null,
                }))
            }
            "thread/loaded/list" => {
                let state = self.inner.state.read().await;
                let mut ids = state.threads.keys().cloned().collect::<Vec<_>>();
                ids.sort();
                Ok(serde_json::json!({ "data": ids, "nextCursor": null }))
            }
            "thread/unsubscribe" => {
                let params: ThreadUnsubscribeParams = parse_params(params)?;
                connection.unsubscribe(&params.thread_id).await;
                Ok(serde_json::json!({}))
            }
            "thread/name/set" => {
                let params: ThreadSetNameParams = parse_params(params)?;
                let handle = self.handle_for_thread(&params.thread_id).await?;
                let lifecycle_metadata = {
                    let mut state = self.inner.state.write().await;
                    let thread = state
                        .threads
                        .get_mut(&params.thread_id)
                        .ok_or_else(|| thread_not_found(&params.thread_id))?;
                    thread.name = Some(params.name.clone());
                    thread.updated_at_ms = now_ms();
                    let mut metadata =
                        crate::adapters::app_server::threads::app_server_thread_metadata_with_name(
                            &thread.cwd,
                            &thread.model_provider,
                            thread.ephemeral,
                            thread.name.as_deref(),
                        );
                    crate::adapters::app_server::threads::insert_app_server_thinking_metadata(
                        &mut metadata,
                        thread.thinking.as_ref(),
                    )?;
                    metadata
                };
                self.persist_thread_lifecycle_with_metadata(&handle, lifecycle_metadata)
                    .await?;
                Ok(serde_json::json!({}))
            }
            "thread/metadata/update" => {
                let params: ThreadMetadataUpdateParams = parse_params(params)?;
                let thread = self.thread_json_by_id(&params.thread_id, false).await?;
                Ok(serde_json::json!({ "thread": thread }))
            }
            "thread/compact/start" => {
                let params: ThreadCompactStartParams = parse_params(params)?;
                self.thread_compact_start(params).await
            }
            "thread/shellCommand" => {
                let params: ThreadShellCommandParams = parse_params(params)?;
                self.thread_shell_command(connection, params).await
            }
            "turn/start" => {
                let params: TurnStartParams = parse_params(params)?;
                self.turn_start(connection, params).await
            }
            "turn/steer" => {
                let params: TurnSteerParams = parse_params(params)?;
                self.turn_steer(params).await
            }
            "turn/interrupt" => {
                let params: TurnInterruptParams = parse_params(params)?;
                self.turn_interrupt(params).await
            }
            "skills/list" => Ok(serde_json::json!({ "data": [] })),
            "plugin/list" => Ok(serde_json::json!({
                "marketplaces": [],
                "marketplaceLoadErrors": [],
                "featuredPluginIds": [],
            })),
            "hooks/list" => Ok(serde_json::json!({ "data": [], "witnessing": true })),
            "mcpServerStatus/list" => self.mcp_server_status_list().await,
            "mcpSource/list" => self.mcp_source_list().await,
            "mcpSource/read" => {
                let params: McpSourceReadParams = parse_params(params)?;
                self.mcp_source_read(params).await
            }
            "mcpSource/upsert" => {
                let params: McpSourceUpsertParams = parse_params(params)?;
                self.mcp_source_upsert(params).await
            }
            "mcpSource/discover" => {
                let params: McpSourceReadParams = parse_params(params)?;
                self.mcp_source_discover(params).await
            }
            "mcpSource/delete" => {
                let params: McpSourceReadParams = parse_params(params)?;
                self.mcp_source_delete(params).await
            }
            "mcpSource/testTool" => {
                let params: McpSourceTestToolParams = parse_params(params)?;
                self.mcp_source_test_tool(params).await
            }
            "mcpSource/manifestPatch" => {
                let params: McpSourceManifestPatchParams = parse_params(params)?;
                self.mcp_source_manifest_patch(params).await
            }
            "fs/readFile" => {
                let params: FsReadFileParams = parse_params(params)?;
                self.fs_read_file(params).await
            }
            "fs/writeFile" => {
                let params: FsWriteFileParams = parse_params(params)?;
                self.fs_write_file(params).await
            }
            "fs/createDirectory" => {
                let params: FsCreateDirectoryParams = parse_params(params)?;
                self.fs_create_directory(params).await
            }
            "fs/getMetadata" => {
                let params: FsGetMetadataParams = parse_params(params)?;
                self.fs_get_metadata(params).await
            }
            "fs/readDirectory" => {
                let params: FsReadDirectoryParams = parse_params(params)?;
                self.fs_read_directory(params).await
            }
            "fs/remove" => {
                let params: FsRemoveParams = parse_params(params)?;
                self.fs_remove(params).await
            }
            "fs/copy" => {
                let params: FsCopyParams = parse_params(params)?;
                self.fs_copy(params).await
            }
            "fs/watch" => {
                let params: FsWatchParams = parse_params(params)?;
                connection.fs_watch(params).await
            }
            "fs/unwatch" => {
                let params: FsUnwatchParams = parse_params(params)?;
                connection.fs_unwatch(params).await
            }
            "config/read" => {
                let params: ConfigReadParams = parse_params(params)?;
                Ok(serde_json::json!({
                    "config": self.config_json(),
                    "origins": {},
                    "layers": if params.include_layers { serde_json::json!([]) } else { serde_json::Value::Null },
                }))
            }
            "configRequirements/read" => Ok(serde_json::json!({ "requirements": null })),
            _ => Err(jsonrpc_error(
                -32601,
                format!("unsupported method `{method}`"),
            )),
        }
    }

    pub(super) async fn mcp_server_status_list(
        &self,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        self.mcp_source_list().await
    }

    pub(super) async fn mcp_source_list(&self) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::SqliteMcpSourceRegistry::open_async(&self.inner.metadata_store_path)
            .await
            .map_err(internal_error)?;
        let data = registry
            .list_sources_async()
            .await
            .map_err(internal_error)?
            .into_iter()
            .map(|record| record.redacted_json())
            .collect::<Vec<_>>();
        Ok(serde_json::json!({ "data": data, "nextCursor": null }))
    }

    pub(super) async fn mcp_source_read(
        &self,
        params: McpSourceReadParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = self.mcp_source_registry().await?;
        let record = registry
            .get_source_async(&params.name)
            .await
            .map_err(mcp_source_param_error)?
            .ok_or_else(|| mcp_source_not_found(&params.name))?;
        Ok(serde_json::json!({ "source": record.redacted_json() }))
    }

    pub(super) async fn mcp_source_upsert(
        &self,
        params: McpSourceUpsertParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let transport = params
            .transport
            .or(params.kind)
            .ok_or_else(|| jsonrpc_error(-32602, "mcpSource/upsert requires transport"))?;
        let transport =
            crate::McpRemoteTransport::from_str(&transport).map_err(mcp_source_param_error)?;
        let mut config = crate::McpRemoteServerConfig::new(params.name, transport, params.url)
            .map_err(mcp_source_param_error)?;

        if let Some(token) = params.bearer_token {
            let secret_name = params
                .bearer_secret
                .unwrap_or_else(|| format!("mcp.{}.bearer", config.name));
            self.mcp_secret_store()
                .await?
                .set_secret(
                    &secret_name,
                    token,
                    crate::SecretSourceKind::Local,
                    Some(format!("mcp:{}", config.name)),
                )
                .await
                .map_err(mcp_source_param_error)?;
            config = config
                .with_bearer_secret(secret_name)
                .map_err(mcp_source_param_error)?;
        } else if let Some(secret_name) = params.bearer_secret {
            config = config
                .with_bearer_secret(secret_name)
                .map_err(mcp_source_param_error)?;
        }

        for header in params.headers {
            config = config.with_header(header.name, header.value);
        }
        if !params.include_tools.is_empty() {
            config = config.with_include_tools(params.include_tools);
        }
        if let Some(timeout_ms) = params.timeout_ms {
            config = config.with_timeout_ms(timeout_ms);
        }
        if let Some(max_output_bytes) = params.max_output_bytes {
            config = config.with_max_output_bytes(max_output_bytes);
        }

        let record = self
            .mcp_source_registry()
            .await?
            .upsert_source_async(config)
            .await
            .map_err(internal_error)?;
        Ok(serde_json::json!({ "source": record.redacted_json() }))
    }

    pub(super) async fn mcp_source_discover(
        &self,
        params: McpSourceReadParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = self.mcp_source_registry().await?;
        let record = registry
            .get_source_async(&params.name)
            .await
            .map_err(mcp_source_param_error)?
            .ok_or_else(|| mcp_source_not_found(&params.name))?;
        let provider = crate::McpRemoteToolProvider::connect(
            record.to_config(),
            Some(std::sync::Arc::new(self.mcp_secret_store().await?)),
        )
        .await
        .map_err(internal_error)?;
        let tools = provider.tool_definitions().await;
        let record = registry
            .update_discovered_tools_async(&params.name, tools)
            .await
            .map_err(internal_error)?;
        Ok(serde_json::json!({ "source": record.redacted_json() }))
    }

    pub(super) async fn mcp_source_delete(
        &self,
        params: McpSourceReadParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let deleted = self
            .mcp_source_registry()
            .await?
            .delete_source_async(&params.name)
            .await
            .map_err(mcp_source_param_error)?;
        Ok(serde_json::json!({ "deleted": deleted }))
    }

    pub(super) async fn mcp_source_test_tool(
        &self,
        params: McpSourceTestToolParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = self.mcp_source_registry().await?;
        let record = registry
            .get_source_async(&params.name)
            .await
            .map_err(mcp_source_param_error)?
            .ok_or_else(|| mcp_source_not_found(&params.name))?;
        let provider = crate::McpRemoteToolProvider::connect(
            record.to_config(),
            Some(std::sync::Arc::new(self.mcp_secret_store().await?)),
        )
        .await
        .map_err(internal_error)?;
        let result = provider
            .invoke_tool_call(crate::AgentKernelToolCall {
                call_id: "mcpSource/testTool".to_string(),
                tool_name: params.tool.clone(),
                arguments: params.arguments,
                turn_context: None,
            })
            .await
            .map_err(internal_error)?
            .ok_or_else(|| {
                jsonrpc_error(
                    -32602,
                    format!(
                        "MCP source {:?} does not expose tool {:?}",
                        params.name, params.tool
                    ),
                )
            })?;
        match result {
            crate::CanonicalMessage::ToolResult {
                tool_name,
                content,
                is_error,
                ..
            } => Ok(serde_json::json!({
                "toolName": tool_name,
                "content": content,
                "contentText": crate::adapters::app_server::text_from_canonical_content(&content),
                "isError": is_error,
            })),
            _ => Err(internal_error(crate::VerletError::RuntimeFactory(
                "MCP source test returned a non-tool result".to_string(),
            ))),
        }
    }

    pub(super) async fn mcp_source_manifest_patch(
        &self,
        params: McpSourceManifestPatchParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = self.mcp_source_registry().await?;
        let record = registry
            .get_source_async(&params.name)
            .await
            .map_err(mcp_source_param_error)?
            .ok_or_else(|| mcp_source_not_found(&params.name))?;
        let import_id = match params.import_id {
            Some(import_id) => crate::validate_record_name(&import_id)
                .map_err(|err| jsonrpc_error(-32602, format!("invalid importId: {err}")))?,
            None => record.name.clone(),
        };
        let server_ref = format!("mcp://{}", record.name);
        let tool = serde_json::json!({
            "type": "protocol_tool_import",
            "id": import_id,
            "protocol": "mcp",
            "server_ref": server_ref,
        });
        let toml = format!(
            "[[tools]]\ntype = \"protocol_tool_import\"\nid = \"{}\"\nprotocol = \"mcp\"\nserver_ref = \"{}\"\n",
            import_id, server_ref
        );
        let diagnostics = match params.agent_ref {
            Some(agent_ref) => {
                self.mcp_source_manifest_patch_diagnostics(&agent_ref, &import_id, &server_ref)?
            }
            None => Vec::new(),
        };
        Ok(serde_json::json!({
            "source": record.redacted_json(),
            "serverRef": server_ref,
            "toml": toml,
            "tool": tool,
            "diagnostics": diagnostics,
        }))
    }

    fn mcp_source_manifest_patch_diagnostics(
        &self,
        agent_ref: &str,
        import_id: &str,
        server_ref: &str,
    ) -> Result<Vec<serde_json::Value>, JsonRpcErrorError> {
        let registry = crate::LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        crate::AgentRecordRef::parse(agent_ref)
            .map_err(|err| malformed_agent_ref(agent_ref, err))?;
        let (record, _) = registry
            .load_ref_with_alias_receipt(agent_ref)
            .map_err(|err| unknown_agent_ref(agent_ref, err))?;
        let mut diagnostics = Vec::new();
        for tool in record
            .resolved_manifest
            .get("tools")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            if tool.get("id").and_then(serde_json::Value::as_str) == Some(import_id) {
                diagnostics.push(serde_json::json!({
                    "code": "duplicate_tool_id",
                    "message": format!(
                        "agent {agent_ref:?} already has a tool import id {import_id:?}"
                    ),
                    "toolId": import_id,
                }));
            }
            if tool.get("type").and_then(serde_json::Value::as_str) == Some("protocol_tool_import")
                && tool.get("server_ref").and_then(serde_json::Value::as_str) == Some(server_ref)
            {
                diagnostics.push(serde_json::json!({
                    "code": "source_already_imported",
                    "message": format!(
                        "agent {agent_ref:?} already imports source {server_ref:?}"
                    ),
                    "serverRef": server_ref,
                    "toolId": tool.get("id").and_then(serde_json::Value::as_str),
                }));
            }
        }
        Ok(diagnostics)
    }

    async fn mcp_source_registry(
        &self,
    ) -> Result<crate::SqliteMcpSourceRegistry, JsonRpcErrorError> {
        crate::SqliteMcpSourceRegistry::open_async(&self.inner.metadata_store_path)
            .await
            .map_err(internal_error)
    }

    async fn mcp_secret_store(&self) -> Result<crate::SqliteSecretStore, JsonRpcErrorError> {
        crate::SqliteSecretStore::open(&self.inner.user_metadata_store_path)
            .await
            .map_err(|err| internal_error(crate::adapters::app_server::secret_store_error(err)))
    }

    pub(super) fn agent_list(&self) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        let data = registry
            .list_records()
            .map_err(internal_error)?
            .iter()
            .map(|record| agent_list_entry(&registry, record))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(serde_json::json!({ "data": data, "cursor": null }))
    }

    pub(super) fn agent_read(
        &self,
        params: AgentReadParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        crate::AgentRecordRef::parse(&params.ref_uri)
            .map_err(|err| malformed_agent_ref(&params.ref_uri, err))?;
        let (record, alias_receipt) = registry
            .load_ref_with_alias_receipt(&params.ref_uri)
            .map_err(|err| unknown_agent_ref(&params.ref_uri, err))?;
        let mut value = serde_json::to_value(record).map_err(json_codec_error)?;
        if let Some(alias_receipt) = alias_receipt {
            value["aliasResolutionReceipt"] =
                serde_json::to_value(alias_receipt).map_err(json_codec_error)?;
        }
        Ok(value)
    }

    pub(super) fn agent_plan(
        &self,
        params: AgentDraftParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        let (mut plan, source) = agent_publish_plan_from_draft(&params)?;
        verify_agent_plan_refs(&mut plan, self.agent_publish_operation_registry_root())?;
        let suggested_next_version = suggested_agent_version(&registry, &plan.name, &plan.version)
            .map_err(internal_error)?;
        Ok(serde_json::json!({
            "plan": plan.clone(),
            "manifest": plan.resolved_manifest,
            "source": source,
            "diagnostics": agent_plan_diagnostics(&plan),
            "suggestedNextVersion": suggested_next_version,
            "base": agent_draft_base_json(&registry, &params)?,
        }))
    }

    pub(super) fn agent_publish(
        &self,
        params: AgentDraftParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::LocalAgentRegistry::new(self.inner.agent_registry_root.clone());
        let base = validate_agent_publish_base(&registry, &params)?;
        let (plan, source) = agent_publish_plan_from_draft(&params)?;
        if plan.name != base.name || plan.namespace != base.namespace {
            return Err(jsonrpc_error(
                -32602,
                "agent/publish draft identity must match the base agent",
            ));
        }
        let operation_registry_root = self.agent_publish_operation_registry_root();
        let record = registry
            .publish_plan_with_operation_registry(plan, operation_registry_root)
            .map_err(internal_error)?;
        let (_latest_record, latest_receipt) = registry
            .resolve_alias(&record.name, "latest")
            .map_err(internal_error)?;
        Ok(serde_json::json!({
            "record": record.clone(),
            "manifest": record.resolved_manifest,
            "source": source,
            "latestAlias": latest_receipt,
        }))
    }

    fn agent_publish_operation_registry_root(&self) -> std::path::PathBuf {
        self.inner
            .capsule_bindings
            .registry_root
            .clone()
            .unwrap_or_else(crate::default_operations_registry_root)
    }

    pub(super) async fn model_provider_list(&self) -> Result<serde_json::Value, JsonRpcErrorError> {
        let mut providers = self
            .inner
            .metadata_store
            .list_providers()
            .await
            .map_err(|err| {
                internal_error(crate::adapters::app_server::provider_store_error(err))
            })?;
        providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
        let mut data = Vec::with_capacity(providers.len());
        for provider in &providers {
            data.push(self.model_provider_json(provider).await?);
        }
        Ok(serde_json::json!({ "data": data, "nextCursor": null }))
    }

    pub(super) async fn model_provider_read(
        &self,
        params: ModelProviderReadParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let provider = self.model_provider_record(&params.provider_id).await?;
        Ok(serde_json::json!({ "provider": self.model_provider_json(&provider).await? }))
    }

    pub(super) async fn model_provider_upsert(
        &self,
        params: ModelProviderUpsertParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let provider = model_provider_record_from_rpc(params.provider)?;
        self.inner
            .metadata_store
            .upsert_provider(provider.clone())
            .await
            .map_err(|err| {
                internal_error(crate::adapters::app_server::provider_store_error(err))
            })?;
        let provider = self.model_provider_record(&provider.provider_id).await?;
        Ok(serde_json::json!({ "provider": self.model_provider_json(&provider).await? }))
    }

    pub(super) async fn model_provider_delete(
        &self,
        params: ModelProviderDeleteParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let provider_id = params.provider_id;
        self.model_provider_record(&provider_id).await?;
        let metadata_store = self.inner.metadata_store.clone();
        let user_metadata_store = self.inner.user_metadata_store.clone();
        let delete_provider_id = provider_id.clone();
        tokio::spawn(async move {
            metadata_store
                .delete_provider(&delete_provider_id)
                .await
                .map_err(|err| {
                    internal_error(crate::adapters::app_server::provider_store_error(err))
                })?;
            user_metadata_store
                .delete_credential(&delete_provider_id)
                .await
                .map_err(|err| {
                    internal_error(crate::adapters::app_server::provider_store_error(err))
                })?;
            metadata_store
                .delete_credential(&delete_provider_id)
                .await
                .map_err(|err| {
                    internal_error(crate::adapters::app_server::provider_store_error(err))
                })?;
            Ok::<(), JsonRpcErrorError>(())
        })
        .await
        .map_err(|error| {
            internal_error(crate::VerletError::RuntimeFactory(format!(
                "model provider deletion task failed: {error}"
            )))
        })??;
        Ok(serde_json::json!({ "deleted": true, "providerId": provider_id }))
    }

    pub(super) async fn model_provider_auth_status(
        &self,
        params: ModelProviderAuthStatusParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let providers = if let Some(provider_id) = params.provider_id.as_deref() {
            vec![self.model_provider_record(provider_id).await?]
        } else {
            let mut providers =
                self.inner
                    .metadata_store
                    .list_providers()
                    .await
                    .map_err(|err| {
                        internal_error(crate::adapters::app_server::provider_store_error(err))
                    })?;
            providers.sort_by(|left, right| left.provider_id.cmp(&right.provider_id));
            providers
        };
        let mut data = Vec::with_capacity(providers.len());
        for provider in &providers {
            data.push(self.model_provider_auth_json(provider).await?);
        }
        let auth = if params.provider_id.is_some() {
            data.first().cloned().unwrap_or(serde_json::Value::Null)
        } else {
            serde_json::Value::Null
        };
        Ok(serde_json::json!({ "auth": auth, "data": data, "nextCursor": null }))
    }

    pub(super) async fn model_provider_auth_set(
        &self,
        params: ModelProviderAuthSetParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let api_key = params.api_key.trim();
        if api_key.is_empty() {
            return Err(jsonrpc_error(
                -32602,
                "modelProvider/auth/set requires a non-empty apiKey",
            ));
        }
        let provider = self.model_provider_record(&params.provider_id).await?;
        self.inner
            .user_metadata_store
            .set_credential(
                &provider.provider_id,
                crate::LlmProviderCredential::ApiKey {
                    key: api_key.to_string(),
                },
            )
            .await
            .map_err(|err| {
                internal_error(crate::adapters::app_server::provider_store_error(err))
            })?;
        Ok(serde_json::json!({ "auth": self.model_provider_auth_json(&provider).await? }))
    }

    pub(super) async fn model_provider_auth_delete(
        &self,
        params: ModelProviderAuthDeleteParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let provider = self.model_provider_record(&params.provider_id).await?;
        self.inner
            .user_metadata_store
            .delete_credential(&provider.provider_id)
            .await
            .map_err(|err| {
                internal_error(crate::adapters::app_server::provider_store_error(err))
            })?;
        Ok(serde_json::json!({ "auth": self.model_provider_auth_json(&provider).await? }))
    }

    async fn model_provider_record(
        &self,
        provider_id: &str,
    ) -> Result<crate::LlmProviderRecord, JsonRpcErrorError> {
        self.inner
            .metadata_store
            .get_provider(provider_id)
            .await
            .map_err(|err| internal_error(crate::adapters::app_server::provider_store_error(err)))?
            .ok_or_else(|| {
                jsonrpc_error(
                    -32602,
                    format!("model provider {provider_id:?} was not found"),
                )
            })
    }

    async fn model_provider_auth_json(
        &self,
        provider: &crate::LlmProviderRecord,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let status = crate::llm_provider_auth_status(
            &self.inner.user_metadata_store,
            provider,
            &crate::LlmProviderAuthContext::from_process_env(),
        )
        .await
        .map_err(|err| internal_error(crate::adapters::app_server::provider_store_error(err)))?;
        Ok(serde_json::json!({
            "providerId": provider.provider_id,
            "displayName": provider.display_name,
            "configured": status.configured,
            "source": status.source,
            "label": status.label,
            "authHeader": provider.auth_header,
        }))
    }

    async fn model_provider_json(
        &self,
        provider: &crate::LlmProviderRecord,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let status = crate::llm_provider_auth_status(
            &self.inner.user_metadata_store,
            provider,
            &crate::LlmProviderAuthContext::from_process_env(),
        )
        .await
        .map_err(|err| internal_error(crate::adapters::app_server::provider_store_error(err)))?;
        Ok(serde_json::json!({
            "providerId": provider.provider_id,
            "api": provider_api_rpc_json(&provider.api),
            "baseUrl": provider.base_url,
            "displayName": provider.display_name,
            "auth": redacted_model_provider_auth_config(&provider.auth),
            "authHeader": provider.auth_header,
            "headers": redacted_model_provider_config_values(&provider.headers),
            "models": provider.models.iter().map(|model| {
                model_provider_model_json(provider, model, model.model_id == self.inner.model)
            }).collect::<Vec<_>>(),
            "metadata": provider.metadata,
            "createdAtMs": provider.created_at_ms,
            "updatedAtMs": provider.updated_at_ms,
            "configuredAuth": {
                "configured": status.configured,
                "source": status.source,
                "label": status.label,
            },
            "isActiveProvider": provider.provider_id == self.inner.model_provider,
        }))
    }

    pub(super) fn operation_list(&self) -> Result<serde_json::Value, JsonRpcErrorError> {
        let Some(registry_root) = self.inner.capsule_bindings.registry_root.as_deref() else {
            return Ok(serde_json::json!({ "data": [], "cursor": null }));
        };
        let registry = crate::LocalOperationRegistry::new(registry_root);
        let data = registry
            .list_records()
            .map_err(|err| internal_error(err.into()))?
            .iter()
            .map(operation_list_entry)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(serde_json::json!({ "data": data, "cursor": null }))
    }

    async fn lifecycle_for_thread_query(
        &self,
        thread_id: &str,
    ) -> Result<crate::ThreadLifecycleRecord, JsonRpcErrorError> {
        let thread_id =
            crate::ThreadId::parse_str(thread_id).map_err(|_| thread_not_found(thread_id))?;
        let lifecycle = self
            .inner
            .metadata_store
            .get_thread_lifecycle(thread_id)
            .await
            .map_err(crate::adapters::app_server::metadata_store_jsonrpc_error)?
            .ok_or_else(|| thread_not_found(&thread_id.to_string()))?;
        if lifecycle.coordinates.tenant_id != self.inner.tenant_id
            || lifecycle.coordinates.user_id != self.inner.user_id
        {
            return Err(thread_not_found(&thread_id.to_string()));
        }
        Ok(lifecycle)
    }

    pub(super) async fn thread_events_list(
        &self,
        params: ThreadEventsListParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;

        if params.cursor.is_some() && params.stream_cursor.is_some() {
            return Err(jsonrpc_error(
                -32602,
                "thread/events/list accepts either cursor or streamCursor, not both",
            ));
        }
        let stream_selector = params.stream.as_deref().unwrap_or("thread");
        let stream_id = thread_events_stream_id(&lifecycle.coordinates, stream_selector)?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let mut events = if let Some(stream_cursor) = params.stream_cursor.as_ref() {
            store
                .read_events_after_cursor(&stream_id, stream_cursor)
                .await
                .map_err(thread_events_cursor_history_error)?
        } else {
            let from_sequence = params
                .cursor
                .as_deref()
                .map(decode_thread_events_cursor)
                .transpose()?;
            store
                .read_events(&stream_id, from_sequence)
                .await
                .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?
        };
        if !params.kinds.is_empty() {
            let kinds = params
                .kinds
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            events.retain(|event| kinds.contains(event.kind.as_str()));
        }

        let limit = params.limit.unwrap_or(100).clamp(1, 500);
        let mut page = events.into_iter().take(limit + 1).collect::<Vec<_>>();
        let (cursor, stream_cursor) = if page.len() > limit {
            page.pop();
            let cursor = page
                .last()
                .map(|event| encode_thread_events_cursor(event.sequence.get() + 1))
                .transpose()?;
            let stream_cursor = page.last().map(crate::EventRecord::cursor_v1);
            (cursor, stream_cursor)
        } else {
            (None, None)
        };
        let data = page
            .iter()
            .map(thread_event_record_json)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(serde_json::json!({ "data": data, "cursor": cursor, "streamCursor": stream_cursor }))
    }

    pub(super) async fn thread_couplings_list(
        &self,
        params: ThreadControlListParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let Some((bind_event_id, receipt)) =
            crate::active_manifest_bind_receipt(&store, &lifecycle.coordinates)
                .await
                .map_err(internal_error)?
        else {
            return Ok(serde_json::json!({
                "data": [],
                "nextCursor": null,
                "agentRef": null,
                "manifestHash": null,
                "bindEventId": null,
            }));
        };
        let limit = params.limit.unwrap_or(100).clamp(1, 500);
        let data = receipt
            .couplings
            .iter()
            .take(limit)
            .map(coupling_binding_json)
            .collect::<Vec<_>>();
        Ok(serde_json::json!({
            "data": data,
            "nextCursor": null,
            "agentRef": receipt.ref_uri,
            "manifestHash": receipt.manifest_hash,
            "bindEventId": bind_event_id.to_string(),
        }))
    }

    pub(super) async fn thread_approvals_list(
        &self,
        params: ThreadControlListParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let mut pending = crate::list_pending_tool_call_suspensions(&store, &lifecycle.coordinates)
            .await
            .map_err(internal_error)?;
        pending.retain(|suspension| suspension.approval_id.is_some());
        let limit = params.limit.unwrap_or(100).clamp(1, 500);
        pending.truncate(limit);
        let data = pending
            .iter()
            .map(pending_tool_approval_json)
            .collect::<Vec<_>>();
        Ok(serde_json::json!({ "data": data, "nextCursor": null }))
    }

    pub(super) async fn thread_waiting_list(
        &self,
        params: ThreadControlListParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let control_stream =
            crate::EventStreamId::new(format!("control:{}", lifecycle.coordinates.thread_id));
        let control_events = store
            .read_events(&control_stream, None)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let thread_events = store
            .read_events(
                &crate::EventStreamId::for_thread(&lifecycle.coordinates),
                None,
            )
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let mut closed_turns = std::collections::BTreeSet::new();
        for event in control_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnResumed)
            .chain(
                thread_events
                    .iter()
                    .filter(|event| event.kind == crate::EventKind::TurnCompleted),
            )
        {
            if let Some(turn_id) = event
                .payload
                .get("turn_id")
                .and_then(serde_json::Value::as_str)
            {
                closed_turns.insert(turn_id.to_string());
            }
        }

        let mut data = Vec::new();
        let mut active_tool_wait_subjects = std::collections::BTreeSet::new();
        for event in control_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnWaiting)
        {
            let turn_id = event
                .payload
                .get("turn_id")
                .and_then(serde_json::Value::as_str);
            if turn_id.is_some_and(|turn_id| closed_turns.contains(turn_id)) {
                continue;
            }
            if let (Some(turn_id), Some(call_id)) = (
                turn_id,
                event
                    .payload
                    .get("subject")
                    .and_then(|subject| subject.get("call_id"))
                    .and_then(serde_json::Value::as_str),
            ) {
                active_tool_wait_subjects.insert((turn_id.to_string(), call_id.to_string()));
            }
            data.push(turn_waiting_json(event));
        }

        let pending = crate::list_pending_tool_call_suspensions(&store, &lifecycle.coordinates)
            .await
            .map_err(internal_error)?;
        for suspension in pending {
            let subject = (
                suspension.subject.turn_id.clone(),
                suspension.subject.call_id.clone(),
            );
            if active_tool_wait_subjects.contains(&subject) {
                continue;
            }
            data.push(pending_tool_waiting_json(&suspension));
        }

        data.sort_by(|left, right| {
            left.get("eventId")
                .and_then(serde_json::Value::as_str)
                .cmp(&right.get("eventId").and_then(serde_json::Value::as_str))
        });
        let limit = params.limit.unwrap_or(100).clamp(1, 500);
        data.truncate(limit);
        Ok(serde_json::json!({ "data": data, "nextCursor": null }))
    }

    pub(super) async fn approval_resolve(
        &self,
        params: ApprovalResolveParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let control_stream =
            crate::EventStreamId::new(format!("control:{}", lifecycle.coordinates.thread_id));
        let control_events = store
            .read_events(&control_stream, None)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        if let Some((existing, payload)) =
            existing_approval_resolution(&control_events, &params.approval_id)?
        {
            if payload.approved == params.decision.approved() {
                return Ok(approval_resolution_json(
                    "already_resolved",
                    params.decision,
                    existing,
                    &payload,
                ));
            }
            return Err(jsonrpc_error(
                -32602,
                format!(
                    "approval {} already resolved with decision {}",
                    params.approval_id,
                    approval_decision_from_bool(payload.approved)
                ),
            ));
        }

        let suspension = crate::list_pending_tool_call_suspensions(&store, &lifecycle.coordinates)
            .await
            .map_err(internal_error)?
            .into_iter()
            .find(|suspension| suspension.approval_id.as_deref() == Some(&params.approval_id))
            .ok_or_else(|| {
                jsonrpc_error(
                    -32602,
                    format!("approval {} is not open", params.approval_id),
                )
            })?;
        let payload = crate::ApprovalResolvedPayload {
            subject: crate::ApprovalSubject {
                approval_id: params.approval_id,
            },
            snapshot_id: suspension.snapshot_id,
            approved: params.decision.approved(),
            reason: params.reason,
        };
        let value = serde_json::to_value(&payload).map_err(json_codec_error)?;
        let mut appended = store
            .append_events(
                &control_stream,
                vec![crate::NewEventRecord::witnessed(
                    lifecycle.coordinates,
                    crate::EventKind::ApprovalResolved,
                    value,
                )],
            )
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let record = appended.pop().ok_or_else(|| {
            internal_error(crate::VerletError::History(
                "approval/resolve appended no event".to_string(),
            ))
        })?;
        Ok(approval_resolution_json(
            "resolved",
            params.decision,
            &record,
            &payload,
        ))
    }

    pub(super) async fn mandate_start(
        &self,
        params: MandateStartParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let receipt = crate::start_mandate(
            &store,
            &lifecycle.coordinates,
            crate::MandateStartRequest {
                schedule: params.schedule,
                max_occurrences: params.max_occurrences,
                catch_up: params.catch_up,
                input_template: params.input_template,
                snapshot_id: lifecycle
                    .metadata
                    .get(crate::adapters::app_server::THREAD_AGENT_MANIFEST_HASH_METADATA)
                    .cloned(),
                expires_at: params.expires_at,
            },
            chrono::Utc::now(),
        )
        .await
        .map_err(mandate_jsonrpc_error)?;
        Ok(serde_json::json!({
            "mandateEventId": receipt.event.id.to_string(),
            "streamId": receipt.event.stream_id.as_str(),
            "sequence": receipt.event.sequence.get(),
        }))
    }

    pub(super) async fn mandate_revoke(
        &self,
        params: MandateRevokeParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let mandate_event_id = crate::parse_mandate_event_id(&params.mandate_event_id)
            .map_err(mandate_jsonrpc_error)?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let receipt = crate::revoke_mandate(&store, &lifecycle.coordinates, mandate_event_id)
            .await
            .map_err(mandate_jsonrpc_error)?;
        Ok(serde_json::json!({
            "status": receipt.status.as_str(),
            "mandateEventId": mandate_event_id.to_string(),
            "revokedEventId": receipt.revoke_event.id.to_string(),
            "streamId": receipt.revoke_event.stream_id.as_str(),
            "sequence": receipt.revoke_event.sequence.get(),
        }))
    }

    pub(super) async fn mandate_list(
        &self,
        params: MandateListParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let data = crate::list_active_mandates(&store, &lifecycle.coordinates)
            .await
            .map_err(mandate_jsonrpc_error)?
            .iter()
            .map(active_mandate_json)
            .collect::<Vec<_>>();
        Ok(serde_json::json!({ "data": data, "nextCursor": null }))
    }

    pub(super) async fn thread_debug_export(
        &self,
        params: ThreadDebugExportParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;

        let mut selectors = if params.streams.is_empty() {
            vec!["thread".to_string(), "control".to_string()]
        } else {
            params.streams
        };
        selectors.sort();
        selectors.dedup();
        if selectors.len() > 16 {
            return Err(jsonrpc_error(
                -32602,
                "thread/debug/export supports at most 16 streams per bundle",
            ));
        }
        let max_events = params
            .max_events_per_stream
            .unwrap_or(5_000)
            .clamp(1, 10_000);
        let redact = params.redact.unwrap_or(true);
        let store = crate::SqliteSessionStore::open(&self.inner.session_store_path)
            .await
            .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
        let mut streams = Vec::new();
        let mut receipts = Vec::new();
        let mut redacted_keys = std::collections::BTreeSet::new();
        for selector in selectors {
            let stream_id = thread_events_stream_id(&lifecycle.coordinates, &selector)?;
            let mut events = store
                .read_events(&stream_id, None)
                .await
                .map_err(|err| internal_error(crate::VerletError::History(err.to_string())))?;
            let tail_sequence = events.last().map(|event| event.sequence.get());
            let tail_stream_cursor = events.last().map(crate::EventRecord::cursor_v1);
            let truncated = events.len() > max_events;
            if truncated {
                events.truncate(max_events);
            }
            receipts.extend(
                events
                    .iter()
                    .filter(|event| event.origin.as_str() == "discharged")
                    .map(debug_export_receipt_json),
            );
            let last_exported_sequence = events.last().map(|event| event.sequence.get());
            let last_exported_stream_cursor = events.last().map(crate::EventRecord::cursor_v1);
            let cursor = if truncated {
                events
                    .last()
                    .map(|event| encode_thread_events_cursor(event.sequence.get() + 1))
                    .transpose()?
            } else {
                None
            };
            let stream_cursor = if truncated {
                last_exported_stream_cursor.clone()
            } else {
                None
            };
            let mut data = events
                .iter()
                .map(thread_event_record_json)
                .collect::<Result<Vec<_>, _>>()?;
            if redact {
                for event in &mut data {
                    redact_debug_export_value_with_evidence(event, &mut redacted_keys);
                }
            }
            streams.push(serde_json::json!({
                "selector": selector,
                "streamId": stream_id.as_str(),
                "backend": {
                    "kind": "sqlite",
                    "sessionStorePath": self.inner.session_store_path.display().to_string(),
                },
                "ackClasses": debug_export_ack_classes(),
                "range": {
                    "fromSequence": 1,
                    "fromCursor": encode_thread_events_cursor(1)?,
                    "lastExportedSequence": last_exported_sequence,
                    "lastExportedStreamCursor": last_exported_stream_cursor,
                    "toCursor": last_exported_sequence
                        .map(|sequence| encode_thread_events_cursor(sequence + 1))
                        .transpose()?,
                    "tailSequence": tail_sequence,
                    "tailStreamCursor": tail_stream_cursor,
                    "tailCursor": encode_thread_events_cursor(
                        tail_sequence.map(|sequence| sequence + 1).unwrap_or(1),
                    )?,
                },
                "data": data,
                "eventCount": data.len(),
                "truncated": truncated,
                "cursor": cursor,
                "streamCursor": stream_cursor,
            }));
        }

        let mut thread = if params.include_thread.unwrap_or(true) {
            self.thread_json_by_id(&params.thread_id, true).await?
        } else {
            serde_json::Value::Null
        };
        if redact {
            redact_debug_export_value_with_evidence(&mut thread, &mut redacted_keys);
        }
        let bundle = serde_json::json!({
            "schema": "cooldis.debug.thread_export/1",
            "threadId": params.thread_id,
            "generatedAtMs": now_ms(),
            "backend": {
                "kind": "sqlite",
                "sessionStorePath": self.inner.session_store_path.display().to_string(),
                "ackClasses": debug_export_ack_classes(),
            },
            "ackClasses": debug_export_ack_classes(),
            "redaction": {
                "enabled": redact,
                "mode": if redact { "secret-shaped-json-keys" } else { "none" },
                "replacement": if redact { "[REDACTED]" } else { "" },
                "redactedKeys": redacted_keys.into_iter().collect::<Vec<_>>(),
            },
            "thread": thread,
            "streams": streams,
            "receipts": receipts,
        });
        crate::stream_schema_registry_v1()
            .validate(crate::DEBUG_THREAD_EXPORT_SCHEMA_V1, &bundle)
            .map_err(|err| {
                internal_error(crate::VerletError::RuntimeFactory(format!(
                    "debug export schema validation failed: {err}"
                )))
            })?;
        Ok(bundle)
    }

    pub(super) async fn bind_thread_start_agent(
        &self,
        agent_ref: &str,
        params: &ThreadStartParams,
    ) -> Result<crate::AgentManifestBoundThread, JsonRpcErrorError> {
        let overrides = params.runtime_overrides.clone().unwrap_or_default();
        let model_selection = crate::AgentManifestModelProfileSelection::from_provider_model(
            params.model_provider.clone(),
            params.model.clone(),
        );
        let bound = self
            .bind_app_server_agent_ref(
                agent_ref,
                &model_selection,
                &overrides,
                params.placement.as_ref(),
                params.workspace.as_ref(),
            )
            .await
            .map_err(thread_start_bind_error)?;
        crate::adapters::app_server::threads::require_local_binding_surface("thread/start", &bound)
            .map_err(thread_start_bind_error)?;
        Ok(bound)
    }

    pub(super) async fn bind_rebind_fork_agent(
        &self,
        agent_ref: &str,
        model_profile_id: Option<&str>,
        overrides: Option<&crate::AgentManifestBindOverrides>,
        placement: Option<&crate::AgentManifestPlacementBinding>,
        workspace: Option<&crate::AgentManifestWorkspaceBinding>,
    ) -> Result<crate::AgentManifestBoundThread, JsonRpcErrorError> {
        let model_selection = model_profile_id
            .map(crate::AgentManifestModelProfileSelection::profile_id)
            .unwrap_or_default();
        let overrides = overrides.cloned().unwrap_or_default();
        let bound = self
            .bind_app_server_agent_ref(
                agent_ref,
                &model_selection,
                &overrides,
                placement,
                workspace,
            )
            .await
            .map_err(thread_start_bind_error)?;
        crate::adapters::app_server::threads::require_local_binding_surface(
            "thread/rebindFork",
            &bound,
        )
        .map_err(thread_start_bind_error)?;
        Ok(bound)
    }

    pub(super) async fn agent_manifest_provider_surface(
        &self,
    ) -> crate::VerletResult<crate::AgentManifestProviderSurface> {
        crate::adapters::app_server::agent_manifest_provider_surface_from_parts(
            &self.inner.provider,
            &self.inner.model_provider,
            &self.inner.model,
            &self.inner.metadata_store,
        )
        .await
    }

    pub(super) async fn configured_mcp_server_refs(
        &self,
    ) -> crate::VerletResult<std::collections::BTreeSet<String>> {
        let registry = crate::SqliteMcpSourceRegistry::open_async(&self.inner.metadata_store_path)
            .await
            .map_err(|err| crate::VerletError::RuntimeFactory(err.to_string()))?;
        Ok(registry
            .list_sources_async()
            .await
            .map_err(|err| crate::VerletError::RuntimeFactory(err.to_string()))?
            .into_iter()
            .map(|source| format!("mcp://{}", source.name))
            .collect())
    }

    pub(super) async fn tool_universe_discoverer(
        &self,
    ) -> crate::VerletResult<crate::McpToolUniverseDiscoverer> {
        let registry = crate::SqliteMcpSourceRegistry::open_async(&self.inner.metadata_store_path)
            .await
            .map_err(|err| crate::VerletError::RuntimeFactory(err.to_string()))?;
        let secret_store = crate::SqliteSecretStore::open(&self.inner.user_metadata_store_path)
            .await
            .map_err(crate::adapters::app_server::secret_store_error)?;
        Ok(crate::McpToolUniverseDiscoverer::new(
            registry,
            Some(std::sync::Arc::new(secret_store)),
        ))
    }

    pub(super) async fn thread_start(
        &self,
        connection: &ConnectionState,
        mut params: ThreadStartParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        lower_thread_start_cwd_override(&mut params, &self.inner.cwd)?;
        let default_agent_ref = thread_start_default_agent_ref(&params);
        if params
            .capsule_bindings
            .as_ref()
            .is_some_and(|bindings| !bindings.operation_names.is_empty())
        {
            return Err(jsonrpc_error(
                -32602,
                "thread/start capsuleBindings.operationNames is closed: operations are declared in an agent manifest and published before thread/start",
            ));
        }
        let manifest_agent_ref = params
            .agent_ref
            .as_deref()
            .or(default_agent_ref)
            .ok_or_else(|| {
                internal_error(crate::VerletError::RuntimeFactory(
                    "thread/start could not resolve an explicit or default manifest ref"
                        .to_string(),
                ))
            })?;
        let bound_agent = self
            .bind_thread_start_agent(manifest_agent_ref, &params)
            .await?;
        let cwd = crate::adapters::app_server::threads::resolve_cwd(
            &self.inner.cwd,
            Some(
                bound_agent
                    .bind_receipt
                    .effective_runtime
                    .default_cwd
                    .as_str(),
            ),
        );
        let model = bound_agent.bind_receipt.model_id.clone();
        let model_provider = bound_agent.bind_receipt.provider_id.clone();
        let requested_topology =
            crate::adapters::app_server::threads::thread_start_topology(&params)?;
        let session_id = self.thread_start_session_id(&requested_topology).await?;
        let ephemeral = params.ephemeral.unwrap_or(false);
        let mut metadata = crate::adapters::app_server::threads::thread_start_metadata(
            &params,
            &cwd,
            &model_provider,
            ephemeral,
        )?;
        crate::adapters::app_server::threads::append_bound_agent_metadata(
            &mut metadata,
            &bound_agent,
            params.runtime_overrides.as_ref(),
            self.inner.capsule_bindings.registry_root.as_deref(),
        )?;
        let handle = self
            .inner
            .supervisor
            .start_thread(crate::ThreadStartRequest {
                tenant_id: self.inner.tenant_id.clone(),
                user_id: self.inner.user_id.clone(),
                session_id: session_id.clone(),
                topology: requested_topology,
                metadata,
            })
            .await
            .map_err(internal_error)?;
        self.witness_bound_agent_and_persist_lifecycle(handle.clone(), bound_agent)
            .await?;
        crate::adapters::app_server::subscriptions::wait_for_initial_thread_status(&handle).await;
        let context = handle.context();
        let coordinates = context.coordinates.clone();
        let parent_thread_id = context.parent_thread_id.map(|id| id.to_string());
        let topology = context.topology.clone();
        let thread_id = coordinates.thread_id.to_string();
        let now = now_ms();
        let thread_state = crate::adapters::app_server::threads::AppServerThreadState {
            thread_id: thread_id.clone(),
            session_id,
            parent_thread_id,
            topology,
            cwd: cwd.clone(),
            model_provider: model_provider.clone(),
            created_at_ms: now,
            updated_at_ms: now,
            status: handle.status(),
            preview: String::new(),
            ephemeral,
            name: None,
            thinking: params.thinking.clone(),
            turns: std::collections::BTreeMap::new(),
            active_turn_id: None,
        };

        {
            let mut state = self.inner.state.write().await;
            state.threads.insert(thread_id.clone(), thread_state);
        }

        connection.subscribe_thread(handle).await;
        let thread = self.thread_json_by_id(&thread_id, false).await?;
        connection
            .notify(
                "thread/started",
                serde_json::json!({ "thread": thread.clone() }),
            )
            .await;

        Ok(serde_json::json!({
            "thread": thread,
            "model": model,
            "modelProvider": model_provider,
            "serviceTier": params.service_tier,
            "cwd": cwd_string(&cwd),
            "instructionSources": [],
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandbox": { "type": "dangerFullAccess" },
            "reasoningEffort": null,
        }))
    }

    pub(super) async fn thread_start_session_id(
        &self,
        topology: &crate::ThreadTopology,
    ) -> Result<String, JsonRpcErrorError> {
        let mut session_id = None;
        for related_thread_id in topology.related_thread_ids() {
            let handle = self
                .inner
                .supervisor
                .get_thread(&self.inner.tenant_id, related_thread_id)
                .await
                .map_err(|_| thread_not_found(&related_thread_id.to_string()))?;
            let coordinates = &handle.context().coordinates;
            if coordinates.user_id != self.inner.user_id {
                return Err(jsonrpc_error(
                    -32602,
                    format!(
                        "related thread {related_thread_id} belongs to user {}, not {}",
                        coordinates.user_id, self.inner.user_id
                    ),
                ));
            }
            match &session_id {
                Some(existing) if existing != &coordinates.session_id => {
                    return Err(jsonrpc_error(
                        -32602,
                        "thread/start topology references multiple sessions",
                    ));
                }
                Some(_) => {}
                None => session_id = Some(coordinates.session_id.clone()),
            }
        }
        Ok(session_id.unwrap_or_else(|| format!("app-server-session-{}", uuid::Uuid::now_v7())))
    }

    pub(super) async fn thread_resume(
        &self,
        connection: &ConnectionState,
        params: ThreadResumeParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let parsed = crate::ThreadId::parse_str(&params.thread_id)
            .map_err(|_| thread_not_found(&params.thread_id))?;
        let handle = match self
            .inner
            .supervisor
            .get_thread(&self.inner.tenant_id, parsed)
            .await
        {
            Ok(handle) => handle,
            Err(_) => {
                self.load_thread_from_metadata(&params.thread_id, parsed)
                    .await?
            }
        };

        let cwd_override = params.cwd.as_deref().map(|cwd| {
            crate::adapters::app_server::threads::resolve_cwd(&self.inner.cwd, Some(cwd))
        });
        let model = params
            .model
            .clone()
            .unwrap_or_else(|| self.inner.model.clone());
        let model_provider = {
            let state = self.inner.state.read().await;
            params.model_provider.clone().unwrap_or_else(|| {
                state
                    .threads
                    .get(&params.thread_id)
                    .map(|thread| thread.model_provider.clone())
                    .unwrap_or_else(|| self.inner.model_provider.clone())
            })
        };

        let lifecycle_metadata = {
            let mut state = self.inner.state.write().await;
            let thread = state
                .threads
                .get_mut(&params.thread_id)
                .ok_or_else(|| thread_not_found(&params.thread_id))?;
            if let Some(cwd) = cwd_override.clone() {
                thread.cwd = cwd;
            }
            thread.model_provider = model_provider.clone();
            thread.status = handle.status();
            thread.updated_at_ms = now_ms();
            let mut metadata = crate::adapters::app_server::threads::app_server_thread_metadata(
                &thread.cwd,
                &thread.model_provider,
                thread.ephemeral,
            );
            crate::adapters::app_server::threads::insert_app_server_thinking_metadata(
                &mut metadata,
                thread.thinking.as_ref(),
            )?;
            metadata
        };
        self.persist_thread_lifecycle_with_metadata(&handle, lifecycle_metadata)
            .await?;

        connection.subscribe_thread(handle).await;
        let thread = self
            .thread_json_by_id(&params.thread_id, !params.exclude_turns)
            .await?;
        let cwd = thread
            .get("cwd")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| cwd_string(&self.inner.cwd));
        Ok(serde_json::json!({
            "thread": thread,
            "model": model,
            "modelProvider": model_provider,
            "serviceTier": params.service_tier,
            "cwd": cwd,
            "instructionSources": [],
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandbox": { "type": "dangerFullAccess" },
            "reasoningEffort": null,
        }))
    }

    pub(super) async fn thread_fork(
        &self,
        connection: &ConnectionState,
        params: ThreadForkParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let source_handle = self.handle_for_thread(&params.thread_id).await?;
        let coordinates = source_handle.context().coordinates.clone();
        let source = {
            let state = self.inner.state.read().await;
            state
                .threads
                .get(&params.thread_id)
                .ok_or_else(|| thread_not_found(&params.thread_id))?
                .clone()
        };
        let cwd = params
            .cwd
            .as_deref()
            .map(|cwd| {
                crate::adapters::app_server::threads::resolve_cwd(&self.inner.cwd, Some(cwd))
            })
            .unwrap_or_else(|| source.cwd.clone());
        let model = params
            .model
            .clone()
            .unwrap_or_else(|| self.inner.model.clone());
        let model_provider = params
            .model_provider
            .clone()
            .unwrap_or_else(|| source.model_provider.clone());
        let mut checkpoint_metadata =
            crate::adapters::app_server::threads::app_server_thread_metadata(
                &cwd,
                &model_provider,
                params.ephemeral,
            );
        crate::adapters::app_server::threads::insert_app_server_thinking_metadata(
            &mut checkpoint_metadata,
            source.thinking.as_ref(),
        )?;
        let workspace = source_handle
            .context()
            .metadata
            .get(crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA)
            .map(|raw| {
                serde_json::from_str::<crate::AgentManifestResolvedWorkspaceMount>(raw).map_err(
                    |err| {
                        internal_error(crate::VerletError::RuntimeFactory(format!(
                            "source thread workspace binding is invalid: {err}"
                        )))
                    },
                )
            })
            .transpose()?;
        let workspace_metadata = workspace
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(|err| {
                internal_error(crate::VerletError::RuntimeFactory(format!(
                    "source thread workspace binding could not be encoded: {err}"
                )))
            })?;
        let skill_packages = crate::adapters::app_server::threads::thread_manifest_skill_packages(
            source_handle.context(),
        )
        .map_err(internal_error)?;
        let skill_discovery =
            crate::adapters::app_server::threads::thread_manifest_skill_discovery(
                source_handle.context(),
            )
            .map_err(internal_error)?;
        let skill_context_segments =
            crate::adapters::app_server::threads::thread_manifest_skill_context_segments(
                source_handle.context(),
            )
            .map_err(internal_error)?;
        let skill_packages_metadata = source_handle
            .context()
            .metadata
            .get(crate::THREAD_AGENT_SKILL_PACKAGES_METADATA)
            .cloned();
        let skill_discovery_metadata = source_handle
            .context()
            .metadata
            .get(crate::THREAD_AGENT_SKILL_DISCOVERY_METADATA)
            .cloned();
        let skill_context_metadata = source_handle
            .context()
            .metadata
            .get(crate::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA)
            .cloned();
        let inherited_manifest_receipts = if workspace.is_some()
            || !skill_packages.is_empty()
            || skill_discovery.is_some()
            || !skill_context_segments.is_empty()
        {
            let missing_witness = if workspace.is_some() {
                "source thread workspace binding has no durable manifest bind witness"
            } else {
                "source thread skill package binding has no durable manifest bind witness"
            };
            let (compile_payload, bind_payload) =
                crate::adapters::app_server::threads::active_manifest_receipt_payloads(
                    &source_handle,
                )
                .await
                .map_err(internal_error)?
                .ok_or_else(|| {
                    internal_error(crate::VerletError::RuntimeFactory(
                        missing_witness.to_string(),
                    ))
                })?;
            let witnessed_workspace = bind_payload
                .get("workspace")
                .cloned()
                .map(serde_json::from_value::<crate::AgentManifestResolvedWorkspaceMount>)
                .transpose()
                .map_err(|err| {
                    internal_error(crate::VerletError::RuntimeFactory(format!(
                        "source thread manifest workspace witness is invalid: {err}"
                    )))
                })?;
            if witnessed_workspace.as_ref() != workspace.as_ref() {
                return Err(internal_error(crate::VerletError::RuntimeFactory(
                    "source thread workspace metadata disagrees with its durable manifest bind witness"
                        .to_string(),
                )));
            }
            let witnessed_skill_packages = bind_payload
                .get("skill_packages")
                .cloned()
                .map(serde_json::from_value::<Vec<crate::AgentManifestSkillPackageBinding>>)
                .transpose()
                .map_err(|err| {
                    internal_error(crate::VerletError::RuntimeFactory(format!(
                        "source thread manifest skill package witness is invalid: {err}"
                    )))
                })?
                .unwrap_or_default();
            if !crate::agent::manifest_bind::skill_package_bindings_match(
                &witnessed_skill_packages,
                &skill_packages,
            ) {
                return Err(internal_error(crate::VerletError::RuntimeFactory(
                    "source thread skill package metadata disagrees with its durable manifest bind witness"
                        .to_string(),
                )));
            }
            let witnessed_skill_discovery = bind_payload
                .get("skill_discovery")
                .cloned()
                .map(serde_json::from_value::<crate::AgentManifestSkillDiscovery>)
                .transpose()
                .map_err(|err| {
                    internal_error(crate::VerletError::RuntimeFactory(format!(
                        "source thread manifest skill discovery witness is invalid: {err}"
                    )))
                })?;
            let source_agent_ref = source_handle
                .context()
                .metadata
                .get(crate::adapters::app_server::THREAD_AGENT_REF_METADATA)
                .ok_or_else(|| {
                    internal_error(crate::VerletError::RuntimeFactory(
                        "source thread with manifest witnesses is missing its agent ref metadata"
                            .to_string(),
                    ))
                })?;
            let source_manifest_hash = source_handle
                .context()
                .metadata
                .get(crate::adapters::app_server::THREAD_AGENT_MANIFEST_HASH_METADATA)
                .ok_or_else(|| {
                    internal_error(crate::VerletError::RuntimeFactory(
                        "source thread with manifest witnesses is missing its manifest hash metadata"
                            .to_string(),
                    ))
                })?;
            if bind_payload
                .get("ref_uri")
                .and_then(serde_json::Value::as_str)
                != Some(source_agent_ref.as_str())
                || bind_payload
                    .get("manifest_hash")
                    .and_then(serde_json::Value::as_str)
                    != Some(source_manifest_hash.as_str())
            {
                return Err(internal_error(crate::VerletError::RuntimeFactory(
                    "source thread manifest identity metadata disagrees with its durable bind witness"
                        .to_string(),
                )));
            }
            let source_record =
                crate::LocalAgentRegistry::new(self.inner.agent_registry_root.clone())
                    .load_ref(source_agent_ref)
                    .map_err(internal_error)?;
            if source_record.manifest_hash != *source_manifest_hash {
                return Err(internal_error(crate::VerletError::RuntimeFactory(format!(
                    "source thread stored manifest hash {} but {:?} loaded {}",
                    source_manifest_hash, source_agent_ref, source_record.manifest_hash
                ))));
            }
            let (source_manifest, _) = crate::compile_published_agent_record(&source_record, None)
                .map_err(internal_error)?;
            crate::agent::manifest_bind::validate_skill_discovery_witness_for_manifest(
                &source_manifest,
                witnessed_skill_discovery.as_ref(),
            )
            .map_err(internal_error)?;
            if witnessed_skill_discovery.as_ref() != skill_discovery.as_ref() {
                return Err(internal_error(crate::VerletError::RuntimeFactory(
                    "source thread skill discovery metadata disagrees with its durable manifest bind witness"
                        .to_string(),
                )));
            }
            let witnessed_skill_context_segments =
                crate::agent::manifest_bind::skill_context_segments_for_witnesses(
                    &witnessed_skill_packages,
                    Some(self.inner.skill_registry_root.as_path()),
                    witnessed_skill_discovery.as_ref(),
                )
                .map_err(internal_error)?;
            if witnessed_skill_context_segments != skill_context_segments {
                return Err(internal_error(crate::VerletError::RuntimeFactory(
                    "source thread skill context metadata disagrees with its durable skill witnesses"
                        .to_string(),
                )));
            }
            Some((compile_payload, bind_payload))
        } else {
            None
        };
        if let Some(raw) = &workspace_metadata {
            checkpoint_metadata.insert(
                crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA.to_string(),
                raw.clone(),
            );
        }
        if let Some(raw) = &skill_packages_metadata {
            checkpoint_metadata.insert(
                crate::THREAD_AGENT_SKILL_PACKAGES_METADATA.to_string(),
                raw.clone(),
            );
        }
        if let Some(raw) = &skill_discovery_metadata {
            checkpoint_metadata.insert(
                crate::THREAD_AGENT_SKILL_DISCOVERY_METADATA.to_string(),
                raw.clone(),
            );
        }
        if let Some(raw) = &skill_context_metadata {
            checkpoint_metadata.insert(
                crate::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA.to_string(),
                raw.clone(),
            );
        }
        let mut checkpoint = match params.checkpoint_id.as_deref() {
            Some(checkpoint_id) => {
                let checkpoint_id = crate::ThreadCheckpointId::parse_str(checkpoint_id)
                    .map_err(|err| jsonrpc_error(-32602, format!("invalid checkpointId: {err}")))?;
                self.inner
                    .supervisor
                    .checkpoint_at(&coordinates, checkpoint_id)
                    .await
                    .map_err(|err| {
                        jsonrpc_error(
                            -32000,
                            format!(
                                "checkpoint {checkpoint_id} is not available for thread {}: {err}",
                                params.thread_id
                            ),
                        )
                    })?
            }
            None => self
                .inner
                .supervisor
                .create_checkpoint_at(
                    &coordinates,
                    None,
                    Some("app-server-fork".to_string()),
                    checkpoint_metadata,
                )
                .await
                .map_err(internal_error)?,
        };
        if let Some(raw) = workspace_metadata {
            checkpoint.metadata.insert(
                crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA.to_string(),
                raw,
            );
        }
        if let Some(raw) = skill_packages_metadata {
            checkpoint
                .metadata
                .insert(crate::THREAD_AGENT_SKILL_PACKAGES_METADATA.to_string(), raw);
        }
        if let Some(raw) = skill_discovery_metadata {
            checkpoint.metadata.insert(
                crate::THREAD_AGENT_SKILL_DISCOVERY_METADATA.to_string(),
                raw,
            );
        }
        if let Some(raw) = skill_context_metadata {
            checkpoint.metadata.insert(
                crate::THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA.to_string(),
                raw,
            );
        }
        let source_cut = thread_source_cut_json(&coordinates, &checkpoint, None);
        let handle = self
            .inner
            .supervisor
            .fork_thread_from_checkpoint_at(checkpoint.clone())
            .await
            .map_err(internal_error)?;
        if let Some((compile_payload, bind_payload)) = inherited_manifest_receipts {
            self.witness_manifest_payloads_and_persist_lifecycle(
                handle.clone(),
                compile_payload,
                bind_payload,
            )
            .await?;
        } else {
            self.persist_thread_lifecycle(&handle).await?;
        }
        crate::adapters::app_server::subscriptions::wait_for_initial_thread_status(&handle).await;

        let fork_context = handle.context();
        let fork_coordinates = fork_context.coordinates.clone();
        let parent_thread_id = fork_context.parent_thread_id.map(|id| id.to_string());
        let topology = fork_context.topology.clone();
        let thread_id = fork_coordinates.thread_id.to_string();
        let now = now_ms();
        let thread_state = crate::adapters::app_server::threads::AppServerThreadState {
            thread_id: thread_id.clone(),
            session_id: fork_coordinates.session_id,
            parent_thread_id,
            topology,
            cwd: cwd.clone(),
            model_provider: model_provider.clone(),
            created_at_ms: now,
            updated_at_ms: now,
            status: handle.status(),
            preview: source.preview,
            ephemeral: params.ephemeral,
            name: None,
            thinking: source.thinking,
            turns: source.turns,
            active_turn_id: None,
        };

        {
            let mut state = self.inner.state.write().await;
            state.threads.insert(thread_id.clone(), thread_state);
        }

        connection.subscribe_thread(handle).await;
        let thread = self.thread_json_by_id(&thread_id, true).await?;
        connection
            .notify(
                "thread/started",
                serde_json::json!({ "thread": thread.clone() }),
            )
            .await;

        Ok(serde_json::json!({
            "thread": thread,
            "model": model,
            "modelProvider": model_provider,
            "serviceTier": params.service_tier,
            "cwd": cwd_string(&cwd),
            "fork": {
                "mode": "clone",
                "parentThreadId": coordinates.thread_id.to_string(),
                "checkpointId": checkpoint.id.to_string(),
                "sourceCut": source_cut,
            },
            "instructionSources": [],
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandbox": { "type": "dangerFullAccess" },
            "reasoningEffort": null,
        }))
    }

    pub(super) async fn thread_rebind_fork(
        &self,
        connection: &ConnectionState,
        params: ThreadRebindForkParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let source_handle = self.handle_for_thread(&params.thread_id).await?;
        let coordinates = source_handle.context().coordinates.clone();
        let source_context = source_handle.context().clone();
        let source = {
            let state = self.inner.state.read().await;
            state
                .threads
                .get(&params.thread_id)
                .ok_or_else(|| thread_not_found(&params.thread_id))?
                .clone()
        };
        if source.active_turn_id.is_some()
            || source.turns.values().any(|turn| {
                matches!(
                    turn.status,
                    crate::adapters::app_server::threads::AppServerTurnStatus::InProgress
                )
            })
            || source_handle.status() != crate::ThreadStatus::Idle
        {
            return Err(jsonrpc_error(
                -32000,
                "thread/rebindFork requires the source thread to be idle",
            ));
        }

        let bound_agent = self
            .bind_rebind_fork_agent(
                &params.agent_ref,
                params.model_profile_id.as_deref(),
                params.runtime_overrides.as_ref(),
                params.placement.as_ref(),
                params.workspace.as_ref(),
            )
            .await?;
        let cwd = crate::adapters::app_server::threads::resolve_cwd(
            &self.inner.cwd,
            Some(
                bound_agent
                    .bind_receipt
                    .effective_runtime
                    .default_cwd
                    .as_str(),
            ),
        );
        let model = bound_agent.bind_receipt.model_id.clone();
        let model_provider = bound_agent.bind_receipt.provider_id.clone();
        let reason = thread_fork_reason_string(&params.reason)?;
        let mut child_metadata = crate::adapters::app_server::threads::app_server_thread_metadata(
            &cwd,
            &model_provider,
            source.ephemeral,
        );
        crate::adapters::app_server::threads::insert_app_server_thinking_metadata(
            &mut child_metadata,
            source.thinking.as_ref(),
        )?;
        crate::adapters::app_server::threads::append_bound_agent_metadata(
            &mut child_metadata,
            &bound_agent,
            params.runtime_overrides.as_ref(),
            self.inner.capsule_bindings.registry_root.as_deref(),
        )?;
        child_metadata.insert(
            crate::adapters::app_server::THREAD_REBIND_FORK_REASON_METADATA.to_string(),
            reason.clone(),
        );
        let checkpoint = match params.checkpoint_id.as_deref() {
            Some(checkpoint_id) => {
                let checkpoint_id = crate::ThreadCheckpointId::parse_str(checkpoint_id)
                    .map_err(|err| jsonrpc_error(-32602, format!("invalid checkpointId: {err}")))?;
                self.inner
                    .supervisor
                    .checkpoint_at(&coordinates, checkpoint_id)
                    .await
                    .map_err(internal_error)?
            }
            None => self
                .inner
                .supervisor
                .create_checkpoint_at(
                    &coordinates,
                    None,
                    Some("app-server-rebind-fork".to_string()),
                    child_metadata.clone(),
                )
                .await
                .map_err(internal_error)?,
        };
        child_metadata.insert(
            "forked_from_thread_id".to_string(),
            coordinates.thread_id.to_string(),
        );
        child_metadata.insert(
            "forked_from_checkpoint_id".to_string(),
            checkpoint.id.to_string(),
        );
        let handle = self
            .inner
            .supervisor
            .start_thread(crate::ThreadStartRequest {
                tenant_id: self.inner.tenant_id.clone(),
                user_id: self.inner.user_id.clone(),
                session_id: coordinates.session_id.clone(),
                topology: crate::ThreadTopology::branch_from(
                    coordinates.thread_id,
                    Some(checkpoint.id),
                ),
                metadata: child_metadata,
            })
            .await
            .map_err(internal_error)?;
        self.witness_bound_agent_and_persist_lifecycle(handle.clone(), bound_agent.clone())
            .await?;
        crate::adapters::app_server::subscriptions::wait_for_initial_thread_status(&handle).await;

        let child_coordinates = handle.context().coordinates.clone();
        let parent_stream_id = crate::EventStreamId::for_thread(&coordinates);
        let parent_binding_snapshot_id = source_context
            .metadata
            .get(crate::adapters::app_server::THREAD_AGENT_MANIFEST_HASH_METADATA)
            .cloned();
        let base = crate::ThreadBaseRef {
            child_thread_id: child_coordinates.thread_id,
            parent_thread_id: coordinates.thread_id,
            parent_checkpoint_id: Some(checkpoint.id),
            parent_leaf_entry_id: checkpoint.active_entry_id,
            parent_stream_id: parent_stream_id.clone(),
            parent_stream_to_sequence: None,
            parent_binding_snapshot_id,
            reason: params.reason.clone(),
            created_at_ms: now_ms() as i64,
        };
        if let Err(err) = self
            .inner
            .supervisor
            .fork_history_by_reference_at(&coordinates, &child_coordinates, base.clone())
            .await
        {
            let _ = self
                .inner
                .supervisor
                .shutdown_thread_at(&child_coordinates)
                .await;
            return Err(internal_error(err));
        }

        let source_cut =
            thread_source_cut_json(&coordinates, &checkpoint, base.parent_stream_to_sequence);
        let fork_payload = serde_json::json!({
            "parentThreadId": coordinates.thread_id.to_string(),
            "checkpointId": checkpoint.id.to_string(),
            "agentRef": bound_agent.bind_receipt.ref_uri,
            "manifestHash": bound_agent.bind_receipt.manifest_hash,
            "reason": reason,
            "sourceCut": source_cut.clone(),
        });
        if let Err(err) = handle
            .append_runtime_session_entry("thread_rebind_fork", fork_payload.clone())
            .await
        {
            let _ = self
                .inner
                .supervisor
                .shutdown_thread_at(&child_coordinates)
                .await;
            return Err(internal_error(err));
        }
        self.persist_thread_lifecycle(&handle).await?;

        let record = handle.lifecycle_record().await;
        let thread_state = self
            .thread_state_from_lifecycle(&record, handle.status())
            .await
            .map_err(internal_error)?;
        let thread_id = child_coordinates.thread_id.to_string();
        {
            let mut state = self.inner.state.write().await;
            state.threads.insert(thread_id.clone(), thread_state);
        }

        connection.subscribe_thread(handle).await;
        let thread = self.thread_json_by_id(&thread_id, true).await?;
        connection
            .notify(
                "thread/started",
                serde_json::json!({ "thread": thread.clone() }),
            )
            .await;

        Ok(serde_json::json!({
            "thread": thread,
            "model": model,
            "modelProvider": model_provider,
            "cwd": cwd_string(&cwd),
            "fork": {
                "mode": "reference",
                "parentThreadId": coordinates.thread_id.to_string(),
                "checkpointId": checkpoint.id.to_string(),
                "agentRef": fork_payload["agentRef"].clone(),
                "manifestHash": fork_payload["manifestHash"].clone(),
                "sourceCut": source_cut,
            },
            "instructionSources": [],
            "approvalPolicy": "never",
            "approvalsReviewer": "user",
            "sandbox": { "type": "dangerFullAccess" },
            "reasoningEffort": null,
        }))
    }

    pub(super) async fn thread_compact_start(
        &self,
        params: ThreadCompactStartParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let coordinates = self.coordinates_for_thread(&params.thread_id).await?;
        let turn_id = format!("compact-{}", uuid::Uuid::now_v7());
        self.inner
            .supervisor
            .compact_thread_at(&coordinates, turn_id, None)
            .await
            .map_err(internal_error)?;
        Ok(serde_json::json!({}))
    }

    pub(super) async fn thread_shell_command(
        &self,
        connection: &ConnectionState,
        params: ThreadShellCommandParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let command = params.command.trim().to_string();
        if command.is_empty() {
            return Err(jsonrpc_error(-32602, "command must not be empty"));
        }
        self.coordinates_for_thread(&params.thread_id).await?;
        let item_id = format!("shell-command-{}", uuid::Uuid::now_v7());
        let (turn_id, turn_started, started_item) = {
            let mut state = self.inner.state.write().await;
            let thread = state
                .threads
                .get_mut(&params.thread_id)
                .ok_or_else(|| thread_not_found(&params.thread_id))?;
            let (turn_id, turn_started) = if let Some(turn_id) = thread.active_turn_id.clone() {
                (turn_id, None)
            } else {
                let turn_id = format!("shell-turn-{}", uuid::Uuid::now_v7());
                let input = vec![serde_json::json!({
                    "type": "text",
                    "text": format!("!{command}"),
                    "text_elements": [],
                })];
                let turn = crate::adapters::app_server::threads::AppServerTurnState::new(
                    turn_id.clone(),
                    input,
                );
                let turn_json = crate::adapters::app_server::threads::turn_json(&turn);
                thread.active_turn_id = Some(turn_id.clone());
                thread.turns.insert(turn_id.clone(), turn);
                (turn_id, Some(turn_json))
            };
            thread.updated_at_ms = now_ms();
            let item = crate::adapters::app_server::threads::command_execution_item(
                &item_id,
                &command,
                &thread.cwd,
                "inProgress",
                None,
                None,
                None,
            );
            if let Some(turn) = thread.turns.get_mut(&turn_id) {
                turn.items.push(item.clone());
            }
            (turn_id, turn_started, item)
        };

        if let Some(turn) = turn_started {
            connection
                .notify(
                    "turn/started",
                    serde_json::json!({ "threadId": params.thread_id, "turn": turn }),
                )
                .await;
        }
        connection
            .notify(
                "item/started",
                serde_json::json!({
                    "item": started_item,
                    "threadId": params.thread_id,
                    "turnId": turn_id,
                    "startedAtMs": now_ms(),
                }),
            )
            .await;

        let connection = connection.clone();
        let cwd = {
            let state = self.inner.state.read().await;
            state
                .threads
                .get(&params.thread_id)
                .map(|thread| thread.cwd.clone())
                .unwrap_or_else(|| self.inner.cwd.clone())
        };
        tokio::spawn(async move {
            crate::adapters::app_server::subscriptions::complete_shell_command(
                connection,
                params.thread_id,
                turn_id,
                item_id,
                command,
                cwd,
            )
            .await;
        });

        Ok(serde_json::json!({}))
    }

    async fn record_rpc_ingress_received(
        &self,
        connection: &ConnectionState,
        handle: &crate::RuntimeThreadHandle,
        method: &str,
        input: &[serde_json::Value],
        surface: &str,
    ) -> Result<crate::EventRecord, JsonRpcErrorError> {
        let coordinates = handle.context().coordinates.clone();
        let principal = verlet_io_core::IoPrincipal::new(
            self.inner.tenant_id.clone(),
            connection.resolved_principal.principal_id.to_string(),
            format!("caller:{}", connection.witnessed_session_id),
        );
        let envelope_digest =
            crate::agent::manifest_bind::canonical_json_hash(&serde_json::json!({
                "method": method,
                "input": input,
                "principal": principal,
            }))
            .map_err(internal_error)?;
        let payload = crate::IoIngressReceivedPayload {
            route_id: Some(format!("surface:{surface}")),
            dedupe_key: None,
            external_conversation_id: None,
            external_actor_id: None,
            external_message_id: None,
            content: None,
            envelope_digest,
        };
        handle
            .append_control_event(rpc_ingress_received_record(
                coordinates,
                payload,
                &principal,
                None,
            )?)
            .await
            .map_err(internal_error)
    }

    pub(super) async fn thread_spawn(
        &self,
        connection: &ConnectionState,
        params: ThreadSpawnParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        if params.placement.is_some() && params.agent_ref.is_none() {
            return Err(jsonrpc_error(
                -32602,
                "placement requires agentRef on thread/spawn",
            ));
        }
        if params.workspace.is_some() && params.agent_ref.is_none() {
            return Err(jsonrpc_error(
                -32602,
                "workspace requires agentRef on thread/spawn",
            ));
        }
        let parent = self.handle_for_thread(&params.thread_id).await?;
        let dispatch_id = verlet_runtime_contracts::DispatchId::new(
            params
                .dispatch_id
                .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
        );
        let resolver = if params.agent_ref.is_some() {
            Some(std::sync::Arc::new(
                self.app_server_thread_spawn_agent_resolver(
                    params.placement.clone(),
                    params.workspace.clone(),
                )
                .await
                .map_err(internal_error)?,
            )
                as std::sync::Arc<dyn crate::KernelThreadSpawnAgentResolver>)
        } else {
            None
        };
        let receipt = self
            .inner
            .supervisor
            .kernel_control(&self.inner.tenant_id)
            .await
            .map_err(internal_error)?
            .dispatch_thread_spawn(
                parent.context(),
                dispatch_id,
                params.task_name,
                params.message,
                params.agent_ref,
                resolver,
            )
            .await
            .map_err(internal_error)?;
        let child_id = receipt.thread_id.to_string();
        let thread = match self.handle_for_thread(&child_id).await {
            Ok(child) => {
                connection.subscribe_thread(child).await;
                self.thread_json_by_id(&child_id, false).await?
            }
            Err(_) => {
                self.register_remote_thread_projection(&parent, &receipt)
                    .await?
            }
        };
        Ok(serde_json::json!({
            "thread": thread,
            "threadId": child_id,
            "parentThreadId": receipt.parent_thread_id,
            "status": receipt.status,
            "taskName": receipt.task_name,
            "submittedTurnId": receipt.submitted_turn_id,
            "handle": receipt.handle,
            "dispatchId": receipt.dispatch_id,
        }))
    }

    pub(super) async fn thread_submit(
        &self,
        params: ThreadSubmitParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let target_thread_id = crate::ThreadId::parse_str(&params.thread_id)
            .map_err(|_| thread_not_found(&params.thread_id))?;
        let caller = match self.handle_for_thread(&params.thread_id).await {
            Ok(target) => target,
            Err(_) => {
                let lifecycle = self.lifecycle_for_thread_query(&params.thread_id).await?;
                let parent_thread_id = lifecycle
                    .parent_thread_id
                    .ok_or_else(|| thread_not_found(&params.thread_id))?;
                self.handle_for_thread(&parent_thread_id.to_string())
                    .await?
            }
        };
        let dispatch_id = verlet_runtime_contracts::DispatchId::new(
            params
                .dispatch_id
                .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
        );
        let receipt = self
            .inner
            .supervisor
            .kernel_control(&self.inner.tenant_id)
            .await
            .map_err(internal_error)?
            .submit_to_thread_with_dispatch(
                caller.context(),
                target_thread_id,
                dispatch_id,
                crate::TurnInput::text(params.message),
            )
            .await
            .map_err(internal_error)?;
        Ok(serde_json::json!({
            "threadId": receipt.target_thread_id,
            "status": receipt.status,
            "turnId": receipt.turn_id,
            "interactionId": receipt.interaction_id,
            "dispatchId": receipt.dispatch_id,
        }))
    }

    pub(super) async fn turn_start(
        &self,
        connection: &ConnectionState,
        params: TurnStartParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let handle = self.handle_for_thread(&params.thread_id).await?;
        let coordinates = handle.context().coordinates.clone();
        connection.subscribe_thread(handle.clone()).await;
        let turn_id = format!("turn-{}", uuid::Uuid::now_v7());
        let input = crate::adapters::app_server::threads::turn_input_from_values(&params.input)
            .with_provider(self.inner.model_provider.clone())
            .with_model(params.model.unwrap_or_else(|| self.inner.model.clone()));
        let cwd = params.cwd.as_deref().map(|cwd| {
            crate::adapters::app_server::threads::resolve_cwd(&self.inner.cwd, Some(cwd))
        });
        let input = if let Some(cwd) = cwd {
            input.with_cwd(cwd)
        } else {
            input
        };
        let input = if let Some(thinking) = params.thinking.clone() {
            input.with_thinking(thinking)
        } else {
            input
        };
        let surface = connection.admission_surface().await;
        let ingress_event = self
            .record_rpc_ingress_received(connection, &handle, "turn/start", &params.input, surface)
            .await?;
        let admission = crate::kernel::admission::AdmissionGateContext::surface_default(
            surface,
            vec![ingress_event.id],
        )
        .map_err(internal_error)?;
        crate::kernel::admission::append_admission_decided(&handle, admission)
            .await
            .map_err(internal_error)?;
        let turn = {
            let mut state = self.inner.state.write().await;
            let thread = state
                .threads
                .get_mut(&params.thread_id)
                .ok_or_else(|| thread_not_found(&params.thread_id))?;
            let turn = crate::adapters::app_server::threads::AppServerTurnState::new(
                turn_id.clone(),
                params.input.clone(),
            );
            if thread.preview.is_empty() {
                thread.preview =
                    crate::adapters::app_server::threads::user_input_preview(&params.input);
            }
            thread.updated_at_ms = now_ms();
            thread.active_turn_id = Some(turn_id.clone());
            let turn_json = crate::adapters::app_server::threads::turn_json(&turn);
            thread.turns.insert(turn_id.clone(), turn);
            turn_json
        };

        self.inner
            .supervisor
            .submit_admitted_turn_to(
                &coordinates,
                turn_id.clone(),
                input,
                crate::TurnSubmissionMode::Queue,
                None,
            )
            .await
            .map_err(internal_error)?;
        connection
            .notify(
                "turn/started",
                serde_json::json!({ "threadId": params.thread_id, "turn": turn.clone() }),
            )
            .await;
        Ok(serde_json::json!({ "turn": turn }))
    }

    pub(super) async fn turn_steer(
        &self,
        params: TurnSteerParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let coordinates = self.coordinates_for_thread(&params.thread_id).await?;
        {
            let state = self.inner.state.read().await;
            let thread = state
                .threads
                .get(&params.thread_id)
                .ok_or_else(|| thread_not_found(&params.thread_id))?;
            if thread.active_turn_id.as_deref() != Some(params.expected_turn_id.as_str()) {
                return Err(jsonrpc_error(
                    -32602,
                    format!(
                        "expected active turn `{}` for thread `{}`, but active turn is `{}`",
                        params.expected_turn_id,
                        params.thread_id,
                        thread.active_turn_id.as_deref().unwrap_or("<none>")
                    ),
                ));
            }
        }
        self.inner
            .supervisor
            .submit_turn_to_with_mode(
                &coordinates,
                params.expected_turn_id.clone(),
                crate::adapters::app_server::threads::turn_input_from_values(&params.input),
                crate::TurnSubmissionMode::Steer,
            )
            .await
            .map_err(internal_error)?;
        Ok(serde_json::json!({ "turnId": params.expected_turn_id }))
    }

    pub(super) async fn turn_interrupt(
        &self,
        params: TurnInterruptParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let coordinates = self.coordinates_for_thread(&params.thread_id).await?;
        {
            let mut state = self.inner.state.write().await;
            if let Some(thread) = state.threads.get_mut(&params.thread_id)
                && let Some(turn) = thread.turns.get_mut(&params.turn_id)
            {
                turn.status =
                    crate::adapters::app_server::threads::AppServerTurnStatus::Interrupted;
                thread.updated_at_ms = now_ms();
            }
        }
        self.inner
            .supervisor
            .cancel_at(&coordinates, format!("interrupted turn {}", params.turn_id))
            .await
            .map_err(internal_error)?;
        Ok(serde_json::json!({}))
    }

    pub(super) fn capsule_registry_root(&self) -> Result<&std::path::Path, JsonRpcErrorError> {
        self.inner
            .capsule_bindings
            .registry_root
            .as_deref()
            .ok_or_else(|| jsonrpc_error(-32602, "capsule bindings require a registry root"))
    }

    pub(super) fn capsule_binding_set(
        &self,
        params: CapsuleBindingSetParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::LocalOperationRegistry::new(self.capsule_registry_root()?);
        let artifact_hash = match params.artifact_hash {
            Some(artifact_hash) => artifact_hash,
            None => {
                registry
                    .load_record(&params.operation_name)
                    .map_err(|err| internal_error(err.into()))?
                    .active_artifact_hash
            }
        };
        let binding = registry
            .bind_capsule_operation(params.scope, &params.operation_name, &artifact_hash)
            .map_err(|err| internal_error(err.into()))?;
        Ok(serde_json::json!({ "binding": binding }))
    }

    pub(super) fn capsule_binding_delete(
        &self,
        params: CapsuleBindingOperationParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let registry = crate::LocalOperationRegistry::new(self.capsule_registry_root()?);
        let binding = registry
            .unbind_capsule_operation(params.scope, &params.operation_name)
            .map_err(|err| internal_error(err.into()))?;
        Ok(serde_json::json!({ "binding": binding }))
    }

    pub(super) fn capsule_binding_list(
        &self,
        params: CapsuleBindingListParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        // lexicon-allow: capsule - existing app-server capsule-binding surface; line shifted by repo-wide path qualification
        let registry = crate::LocalOperationRegistry::new(self.capsule_registry_root()?);
        let bindings = registry
            // lexicon-allow: capsule - existing app-server capsule-binding surface; line shifted by repo-wide path qualification
            .list_capsule_bindings(params.scope)
            .map_err(|err| internal_error(err.into()))?;
        Ok(serde_json::json!({ "data": bindings, "nextCursor": null }))
    }

    // lexicon-allow: capsule - existing app-server capsule-binding surface; line shifted by repo-wide path qualification
    pub(super) fn capsule_binding_resolve(
        &self,
        // lexicon-allow: capsule - existing app-server capsule-binding surface; line shifted by repo-wide path qualification
        params: CapsuleBindingResolveParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        // lexicon-allow: capsule - preserves existing app-server operation binding API.
        let registry = crate::LocalOperationRegistry::new(self.capsule_registry_root()?);
        let tenant_id = params
            .tenant_id
            .unwrap_or_else(|| self.inner.tenant_id.clone());
        let request = if let Some(thread_id) = params.thread_id {
            // lexicon-allow: capsule - preserves existing app-server operation binding API.
            crate::CapsuleBindingResolutionRequest::for_thread(tenant_id, thread_id)
        } else {
            // lexicon-allow: capsule - preserves existing app-server operation binding API.
            crate::CapsuleBindingResolutionRequest::for_tenant(tenant_id)
        }
        .with_active_operation_names(params.operation_names)
        .load_all_active_when_unbound(params.load_all_active_when_unbound.unwrap_or(false));
        let snapshot = registry
            // lexicon-allow: capsule - preserves existing app-server operation binding API.
            .resolve_capsule_binding_snapshot(request)
            .map_err(|err| internal_error(err.into()))?;
        Ok(serde_json::json!({ "snapshot": snapshot }))
    }

    pub(super) async fn command_exec(
        &self,
        params: CommandExecParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        if let Some(process_id) = params.process_id.as_deref() {
            let process_id = parse_command_process_id(process_id)?;
            self.require_command_process_handle(process_id).await?;
            let outcome = self
                .inner
                .process_manager
                .poll(
                    process_id,
                    command_yield_time(params.yield_time_ms),
                    command_visible_output_cap(&params),
                )
                .await
                .map_err(command_exec_process_error)?;
            return Ok(command_process_snapshot_json(&outcome.snapshot));
        }
        if params.command.is_empty() {
            return Err(jsonrpc_error(
                -32602,
                "command/exec requires a non-empty command argv",
            ));
        }
        if params.tty {
            return Err(jsonrpc_error(
                -32602,
                "command/exec tty sessions are not implemented yet",
            ));
        }
        if params.output_bytes_cap.is_some() && params.disable_output_cap {
            return Err(jsonrpc_error(
                -32602,
                "command/exec cannot set both outputBytesCap and disableOutputCap",
            ));
        }
        if params.timeout_ms.is_some() && params.disable_timeout {
            return Err(jsonrpc_error(
                -32602,
                "command/exec cannot set both timeoutMs and disableTimeout",
            ));
        }
        if params.stream_stdin || params.stream_stdout_stderr {
            return self.command_exec_streaming_start(params).await;
        }

        let cwd = params
            .cwd
            .as_deref()
            .map(|cwd| {
                crate::adapters::app_server::threads::resolve_cwd(&self.inner.cwd, Some(cwd))
            })
            .unwrap_or_else(|| self.inner.cwd.clone());
        let mut command = tokio::process::Command::new(&params.command[0]);
        command.args(&params.command[1..]).current_dir(cwd);
        command.kill_on_drop(true);
        if let Some(env) = params.env {
            for (key, value) in env {
                if let Some(value) = value {
                    command.env(key, value);
                } else {
                    command.env_remove(key);
                }
            }
        }

        let output = if params.disable_timeout {
            command.output().await.map_err(command_exec_io_error)?
        } else {
            let timeout_ms = params
                .timeout_ms
                .unwrap_or(crate::adapters::app_server::DEFAULT_COMMAND_TIMEOUT_MS);
            tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                command.output(),
            )
            .await
            .map_err(|_| {
                jsonrpc_error(
                    -32000,
                    format!("command/exec timed out after {timeout_ms}ms"),
                )
            })?
            .map_err(command_exec_io_error)?
        };
        let output_cap = if params.disable_output_cap {
            None
        } else {
            Some(
                params
                    .output_bytes_cap
                    .unwrap_or(crate::adapters::app_server::DEFAULT_COMMAND_OUTPUT_CAP_BYTES),
            )
        };
        Ok(serde_json::json!({
            "exitCode": output.status.code().unwrap_or(-1),
            "stdout": String::from_utf8_lossy(&cap_output(output.stdout, output_cap)).into_owned(),
            "stderr": String::from_utf8_lossy(&cap_output(output.stderr, output_cap)).into_owned(),
        }))
    }

    async fn command_exec_streaming_start(
        &self,
        params: CommandExecParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let thread_id = params.thread_id.as_deref().ok_or_else(|| {
            jsonrpc_error(
                -32602,
                "streaming command/exec requires threadId for process handle settlement",
            )
        })?;
        let consumer = self.coordinates_for_thread(thread_id).await?;
        let dispatch_id = verlet_runtime_contracts::DispatchId::new(
            params
                .dispatch_id
                .clone()
                .unwrap_or_else(|| uuid::Uuid::now_v7().to_string()),
        );
        let command_bytes = serde_json::to_vec(&params.command).map_err(json_codec_error)?;
        let cwd = params
            .cwd
            .as_deref()
            .map(|cwd| {
                crate::adapters::app_server::threads::resolve_cwd(&self.inner.cwd, Some(cwd))
            })
            .unwrap_or_else(|| self.inner.cwd.clone());
        let env = params
            .env
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect::<std::collections::BTreeMap<_, _>>();
        let timeout = if params.disable_timeout {
            std::time::Duration::from_secs(24 * 60 * 60)
        } else {
            std::time::Duration::from_millis(
                params
                    .timeout_ms
                    .unwrap_or(crate::adapters::app_server::DEFAULT_COMMAND_TIMEOUT_MS),
            )
        };
        let request =
            verlet_process::AsyncProcessStartRequest::host_command(params.command.clone(), cwd)
                .with_owner(verlet_process::AsyncProcessOwner::app_server_command())
                .with_env(env)
                .pipe_stdin(params.stream_stdin)
                .with_deadline(verlet_process::ExecutionDeadline::from_now(timeout))
                .with_yield_time(command_yield_time(params.yield_time_ms))
                .with_output_cap_bytes(command_process_output_cap(&params));
        let dispatcher = self.inner.process_dispatcher.get().ok_or_else(|| {
            internal_error(crate::VerletError::RuntimeExecution(
                "app-server process dispatcher is not initialized".to_string(),
            ))
        })?;
        let outcome = dispatcher
            .dispatch_start(
                &consumer,
                dispatch_id.clone(),
                crate::kernel::process_handle_dispatch::command_digest(&command_bytes),
                self.inner.process_manager.clone(),
                std::sync::Arc::new(verlet_process::HostBashLiveBackend),
                request,
            )
            .await
            .map_err(|err| jsonrpc_error(-32000, format!("command/exec failed: {err}")))?;
        let mut value = command_process_snapshot_json(&outcome.snapshot);
        value["dispatchId"] = serde_json::json!(dispatch_id.to_string());
        Ok(value)
    }

    pub(super) async fn command_exec_write(
        &self,
        params: CommandExecWriteParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let process_id = parse_command_process_id(&params.process_id)?;
        self.require_command_process_handle(process_id).await?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(params.delta_base64)
            .map_err(|err| {
                jsonrpc_error(
                    -32602,
                    format!("command/exec/write requires valid base64 deltaBase64: {err}"),
                )
            })?;
        let outcome = self
            .inner
            .process_manager
            .write(
                process_id,
                bytes,
                command_yield_time(params.yield_time_ms),
                crate::adapters::app_server::DEFAULT_COMMAND_OUTPUT_CAP_BYTES,
            )
            .await
            .map_err(command_exec_process_error)?;
        Ok(command_process_snapshot_json(&outcome.snapshot))
    }

    pub(super) async fn command_exec_terminate(
        &self,
        params: CommandExecTerminateParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let process_id = parse_command_process_id(&params.process_id)?;
        self.require_command_process_handle(process_id).await?;
        let outcome = self
            .inner
            .process_manager
            .terminate(
                process_id,
                params
                    .reason
                    .unwrap_or_else(|| "command/exec terminate requested".to_string()),
                command_yield_time(params.yield_time_ms),
                crate::adapters::app_server::DEFAULT_COMMAND_OUTPUT_CAP_BYTES,
            )
            .await
            .map_err(command_exec_process_error)?;
        Ok(command_process_snapshot_json(&outcome.snapshot))
    }

    pub(super) fn command_exec_resize(
        &self,
        params: CommandExecProcessParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let _ = params.yield_time_ms;
        Err(jsonrpc_error(
            -32602,
            format!(
                "command/exec resize is not supported for process `{}` until PTY backends exist",
                params.process_id
            ),
        ))
    }

    async fn require_command_process_handle(
        &self,
        process_id: verlet_process::VerletProcessId,
    ) -> Result<(), JsonRpcErrorError> {
        self.inner
            .process_dispatcher
            .get()
            .ok_or_else(|| {
                internal_error(crate::VerletError::RuntimeExecution(
                    "app-server process dispatcher is not initialized".to_string(),
                ))
            })?
            .require_live_handle(process_id, None)
            .await
            .map(|_| ())
            .map_err(internal_error)
    }

    pub(super) async fn get_conversation_summary(
        &self,
        params: GetConversationSummaryParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let thread_id = params.conversation_id.ok_or_else(|| {
            jsonrpc_error(
                -32602,
                "getConversationSummary by rolloutPath is not implemented in Verlet app-server",
            )
        })?;
        let state = self.inner.state.read().await;
        let thread = state
            .threads
            .get(&thread_id)
            .ok_or_else(|| thread_not_found(&thread_id))?;
        let _rollout_path = params.rollout_path;
        Ok(serde_json::json!({
            "summary": {
                "conversationId": thread.thread_id,
                "path": "",
                "preview": thread.preview,
                "timestamp": null,
                "updatedAt": null,
                "modelProvider": thread.model_provider,
                "cwd": cwd_string(&thread.cwd),
                "cliVersion": env!("CARGO_PKG_VERSION"),
                "source": "unknown",
                "gitInfo": null,
            },
        }))
    }

    pub(super) async fn fs_read_file(
        &self,
        params: FsReadFileParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        let bytes = tokio::fs::read(path).await.map_err(fs_error)?;
        Ok(
            serde_json::json!({ "dataBase64": base64::engine::general_purpose::STANDARD.encode(bytes) }),
        )
    }

    pub(super) async fn fs_write_file(
        &self,
        params: FsWriteFileParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(params.data_base64)
            .map_err(|err| {
                jsonrpc_error(
                    -32602,
                    format!("fs/writeFile requires valid base64 dataBase64: {err}"),
                )
            })?;
        tokio::fs::write(path, bytes).await.map_err(fs_error)?;
        Ok(serde_json::json!({}))
    }

    pub(super) async fn fs_create_directory(
        &self,
        params: FsCreateDirectoryParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        if params.recursive.unwrap_or(true) {
            tokio::fs::create_dir_all(path).await
        } else {
            tokio::fs::create_dir(path).await
        }
        .map_err(fs_error)?;
        Ok(serde_json::json!({}))
    }

    pub(super) async fn fs_get_metadata(
        &self,
        params: FsGetMetadataParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        let symlink_metadata = tokio::fs::symlink_metadata(&path).await.map_err(fs_error)?;
        let metadata = tokio::fs::metadata(&path).await.map_err(fs_error)?;
        Ok(serde_json::json!({
            "isDirectory": metadata.is_dir(),
            "isFile": metadata.is_file(),
            "isSymlink": symlink_metadata.file_type().is_symlink(),
            "createdAtMs": metadata_time_ms(metadata.created().ok()),
            "modifiedAtMs": metadata_time_ms(metadata.modified().ok()),
        }))
    }

    pub(super) async fn fs_read_directory(
        &self,
        params: FsReadDirectoryParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        let mut entries = tokio::fs::read_dir(path).await.map_err(fs_error)?;
        let mut values = Vec::new();
        while let Some(entry) = entries.next_entry().await.map_err(fs_error)? {
            let metadata = entry.metadata().await.map_err(fs_error)?;
            values.push(serde_json::json!({
                "fileName": entry.file_name().to_string_lossy(),
                "isDirectory": metadata.is_dir(),
                "isFile": metadata.is_file(),
            }));
        }
        values.sort_by(|left, right| {
            left.get("fileName")
                .and_then(serde_json::Value::as_str)
                .cmp(&right.get("fileName").and_then(serde_json::Value::as_str))
        });
        Ok(serde_json::json!({ "entries": values }))
    }

    pub(super) async fn fs_remove(
        &self,
        params: FsRemoveParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        if params.force.unwrap_or(true) && tokio::fs::metadata(&path).await.is_err() {
            return Ok(serde_json::json!({}));
        }
        let metadata = tokio::fs::metadata(&path).await.map_err(fs_error)?;
        if metadata.is_dir() {
            if params.recursive.unwrap_or(true) {
                tokio::fs::remove_dir_all(path).await
            } else {
                tokio::fs::remove_dir(path).await
            }
        } else {
            tokio::fs::remove_file(path).await
        }
        .map_err(fs_error)?;
        Ok(serde_json::json!({}))
    }

    pub(super) async fn fs_copy(
        &self,
        params: FsCopyParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let source = absolute_path(params.source_path)?;
        let destination = absolute_path(params.destination_path)?;
        tokio::task::spawn_blocking(move || copy_path(&source, &destination, params.recursive))
            .await
            .map_err(|err| jsonrpc_error(-32000, format!("fs/copy task failed: {err}")))?
            .map_err(fs_error)?;
        Ok(serde_json::json!({}))
    }

    pub(super) async fn coordinates_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<crate::ThreadCoordinates, JsonRpcErrorError> {
        Ok(self
            .handle_for_thread(thread_id)
            .await?
            .context()
            .coordinates
            .clone())
    }

    /// Return a resident runtime handle, loading persisted lifecycle state when needed.
    pub(super) async fn handle_for_thread(
        &self,
        thread_id: &str,
    ) -> Result<crate::RuntimeThreadHandle, JsonRpcErrorError> {
        let parsed =
            crate::ThreadId::parse_str(thread_id).map_err(|_| thread_not_found(thread_id))?;
        match self
            .inner
            .supervisor
            .get_thread(&self.inner.tenant_id, parsed)
            .await
        {
            Ok(handle) => Ok(handle),
            Err(_) => self.load_thread_from_metadata(thread_id, parsed).await,
        }
    }

    pub(super) async fn model_list_json(
        &self,
    ) -> Result<Vec<serde_json::Value>, JsonRpcErrorError> {
        match &self.inner.provider {
            crate::adapters::app_server::AppServerProviderConfig::CatalogOpenAIChatCompletions { provider_id, .. } => {
                let provider = self
                    .inner
                    .metadata_store
                    .get_provider(provider_id)
                    .await
                    .map_err(|err| internal_error(crate::adapters::app_server::provider_store_error(err)))?
                    .ok_or_else(|| {
                        internal_error(crate::VerletError::RuntimeFactory(format!(
                            "catalog provider {provider_id:?} is not in the provider metadata store"
                        )))
                    })?;
                let mut default_seen = false;
                let mut models = provider
                    .models
                    .iter()
                    .map(|model| {
                        let is_default = !default_seen && model.model_id == self.inner.model;
                        if is_default {
                            default_seen = true;
                        }
                        catalog_model_json(&provider, model, is_default)
                    })
                    .collect::<Vec<_>>();
                if !default_seen {
                    models.push(configured_model_json(
                        &self.inner.model_provider,
                        &self.inner.model,
                        catalog_provider_display_name(&provider),
                        "Configured catalog provider model",
                    ));
                }
                Ok(models)
            }
            crate::adapters::app_server::AppServerProviderConfig::LocalOffline => Ok(vec![configured_model_json(
                &self.inner.model_provider,
                &self.inner.model,
                "Verlet Local Offline".to_string(),
                "Deterministic local Verlet model",
            )]),
            crate::adapters::app_server::AppServerProviderConfig::BifrostOpenAIResponses { .. }
            | crate::adapters::app_server::AppServerProviderConfig::OpenAIChatCompletions { .. }
            | crate::adapters::app_server::AppServerProviderConfig::AnthropicMessages { .. }
            | crate::adapters::app_server::AppServerProviderConfig::AnthropicBedrock { .. } => Ok(vec![configured_model_json(
                &self.inner.model_provider,
                &self.inner.model,
                format!("{} {}", self.inner.model_provider, self.inner.model),
                "Configured Verlet app-server model",
            )]),
        }
    }

    pub(super) async fn model_provider_capabilities_json(&self) -> serde_json::Value {
        let supports_streaming =
            crate::adapters::app_server::agent_manifest_provider_surface_from_parts(
                &self.inner.provider,
                &self.inner.model_provider,
                &self.inner.model,
                &self.inner.metadata_store,
            )
            .await
            .map(|surface| surface.supports_streaming)
            .unwrap_or(false);
        serde_json::json!({
            "namespaceTools": true,
            "imageGeneration": false,
            "webSearch": false,
            "supportsStreaming": supports_streaming,
        })
    }

    pub(super) fn config_json(&self) -> serde_json::Value {
        serde_json::json!({
            "cwd": cwd_string(&self.inner.cwd),
            "model": self.inner.model,
            "review_model": null,
            "model_context_window": null,
            "model_auto_compact_token_limit": null,
            "model_provider": self.inner.model_provider,
            "approval_policy": "never",
            "approvals_reviewer": "user",
            "sandbox_mode": "danger-full-access",
            "sandbox_workspace_write": null,
            "forced_chatgpt_workspace_id": null,
            "forced_login_method": null,
            "web_search": null,
            "tools": null,
            "profile": null,
            "profiles": {},
            "instructions": null,
            "developer_instructions": null,
            "compact_prompt": null,
            "model_reasoning_effort": "none",
            "model_reasoning_summary": null,
            "model_verbosity": null,
            "service_tier": null,
            "analytics": null,
            "desktop": null,
        })
    }
}

pub(super) fn finish_websocket_session(
    read_result: crate::VerletResult<()>,
    close_result: crate::VerletResult<()>,
) -> crate::VerletResult<()> {
    match (read_result, close_result) {
        (Err(read_error), Err(close_error)) => {
            eprintln!(
                "failed to witness closing a failed Verlet app-server session: {close_error}"
            );
            Err(read_error)
        }
        (Err(read_error), Ok(())) => Err(read_error),
        (Ok(()), Err(close_error)) => Err(close_error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

impl ConnectionState {
    pub(super) async fn handle_initialize(
        &self,
        params: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let params: InitializeParams = parse_params(params)?;
        let mut opt_out = std::collections::HashSet::new();
        let _experimental_api = params
            .capabilities
            .as_ref()
            .map(|capabilities| capabilities.experimental_api)
            .unwrap_or(false);
        let _request_attestation = params
            .capabilities
            .as_ref()
            .map(|capabilities| capabilities.request_attestation)
            .unwrap_or(false);
        if let Some(methods) = params
            .capabilities
            .and_then(|capabilities| capabilities.opt_out_notification_methods)
        {
            opt_out.extend(methods);
        }
        {
            let mut handshake = self.handshake.lock().await;
            handshake.initialize_seen = true;
            handshake.client_name = Some(params.client_info.name);
            handshake.client_version = Some(params.client_info.version);
            let _client_title = params.client_info.title;
        }
        *self.opt_out_notifications.write().await = opt_out;
        Ok(serde_json::json!({
            "userAgent": crate::adapters::app_server::APP_SERVER_USER_AGENT,
            "codexHome": cwd_string(&self.app.inner.codex_home),
            "platformFamily": std::env::consts::FAMILY,
            "platformOs": std::env::consts::OS,
        }))
    }

    pub(super) async fn initialize_seen(&self) -> bool {
        self.handshake.lock().await.initialize_seen
    }

    pub(super) async fn admission_surface(&self) -> &'static str {
        let handshake = self.handshake.lock().await;
        crate::kernel::admission::app_server_surface(handshake.client_name.as_deref())
    }

    pub(super) async fn mark_initialized(&self) {
        self.handshake.lock().await.initialized_seen = true;
    }

    pub(super) async fn notify(&self, method: &str, params: serde_json::Value) {
        if self.opt_out_notifications.read().await.contains(method) {
            return;
        }
        let _ = self
            .outbound
            .send(JsonRpcMessage::Notification(JsonRpcNotification {
                method: method.to_string(),
                params: Some(params),
            }));
    }

    pub(super) async fn subscribe_thread(&self, handle: crate::RuntimeThreadHandle) {
        let thread_id = handle.context().coordinates.thread_id.to_string();
        self.unsubscribe(&thread_id).await;
        let subscriber = crate::adapters::app_server::subscriptions::AppServerSubscriber {
            outbound: self.outbound.clone(),
            opt_out_notifications: self.opt_out_notifications.clone(),
        };
        let subscriber_id = self
            .app
            .subscribe_thread_connection(handle, subscriber)
            .await;
        self.subscriptions
            .lock()
            .await
            .insert(thread_id, subscriber_id);
    }

    pub(super) async fn unsubscribe(&self, thread_id: &str) {
        if let Some(subscriber_id) = self.subscriptions.lock().await.remove(thread_id) {
            self.app
                .unsubscribe_thread_connection(thread_id, subscriber_id)
                .await;
        }
    }

    pub(super) async fn abort_subscriptions(&self) {
        let subscriptions = std::mem::take(&mut *self.subscriptions.lock().await);
        for (thread_id, subscriber_id) in subscriptions {
            self.app
                .unsubscribe_thread_connection(&thread_id, subscriber_id)
                .await;
        }
    }

    pub(super) async fn fs_watch(
        &self,
        params: FsWatchParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        let path = absolute_path(params.path)?;
        let canonical = tokio::fs::canonicalize(&path)
            .await
            .unwrap_or_else(|_| path.clone());
        self.fs_watches
            .lock()
            .await
            .insert(params.watch_id, canonical.clone());
        Ok(serde_json::json!({ "path": cwd_string(&canonical) }))
    }

    pub(super) async fn fs_unwatch(
        &self,
        params: FsUnwatchParams,
    ) -> Result<serde_json::Value, JsonRpcErrorError> {
        self.fs_watches.lock().await.remove(&params.watch_id);
        Ok(serde_json::json!({}))
    }
}

pub(super) async fn handle_inbound_text(
    connection: &ConnectionState,
    text: &str,
) -> crate::VerletResult<()> {
    let message = serde_json::from_str::<JsonRpcMessage>(text).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "invalid Verlet app-server JSON-RPC message: {err}"
        ))
    })?;
    match message {
        JsonRpcMessage::Request(request) => {
            connection.app.handle_request(connection, request).await;
        }
        JsonRpcMessage::Notification(notification) => {
            if notification.method == "initialized" {
                connection.mark_initialized().await;
            }
        }
        JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_) => {}
    }
    Ok(())
}

pub(super) fn empty_rate_limits() -> serde_json::Value {
    serde_json::json!({
        "limitId": null,
        "limitName": null,
        "primary": null,
        "secondary": null,
        "credits": {
            "hasCredits": true,
            "unlimited": true,
            "balance": null,
        },
        "planType": null,
        "rateLimitReachedType": null,
    })
}

pub(super) fn turn_error(message: String, code: Option<String>) -> serde_json::Value {
    let _ = code;
    serde_json::json!({
        "message": message,
        "codexErrorInfo": "other",
        "additionalDetails": null,
    })
}

pub(super) fn parse_params<T>(params: Option<serde_json::Value>) -> Result<T, JsonRpcErrorError>
where
    T: serde::de::DeserializeOwned,
{
    let value = match params {
        Some(serde_json::Value::Null) | None => serde_json::json!({}),
        Some(value) => value,
    };
    serde_json::from_value(value)
        .map_err(|err| jsonrpc_error(-32602, format!("invalid params: {err}")))
}

pub(super) fn jsonrpc_error(code: i64, message: impl Into<String>) -> JsonRpcErrorError {
    JsonRpcErrorError {
        code,
        data: None,
        message: message.into(),
    }
}

pub(super) fn internal_error(err: crate::VerletError) -> JsonRpcErrorError {
    jsonrpc_error(-32000, err.to_string())
}

pub(super) fn json_codec_error(err: serde_json::Error) -> JsonRpcErrorError {
    jsonrpc_error(-32000, format!("JSON codec failed: {err}"))
}

pub(super) fn jsonrpc_error_to_runtime_factory(err: JsonRpcErrorError) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(
        err.message
            .strip_prefix("runtime factory failed: ")
            .unwrap_or(&err.message)
            .to_string(),
    )
}

pub(super) fn fs_error(err: std::io::Error) -> JsonRpcErrorError {
    if err.kind() == std::io::ErrorKind::InvalidInput {
        jsonrpc_error(-32602, err.to_string())
    } else {
        jsonrpc_error(-32000, err.to_string())
    }
}

pub(super) fn command_exec_io_error(err: std::io::Error) -> JsonRpcErrorError {
    jsonrpc_error(-32000, format!("command/exec failed: {err}"))
}

pub(super) fn command_exec_process_error(
    err: verlet_process::VerletProcessError,
) -> JsonRpcErrorError {
    jsonrpc_error(-32000, format!("command/exec failed: {err}"))
}

pub(super) fn parse_command_process_id(
    process_id: &str,
) -> Result<verlet_process::VerletProcessId, JsonRpcErrorError> {
    process_id.parse().map_err(|err| {
        jsonrpc_error(
            -32602,
            format!("command/exec processId must be a Verlet process id: {err}"),
        )
    })
}

pub(super) fn command_yield_time(yield_time_ms: Option<u64>) -> std::time::Duration {
    std::time::Duration::from_millis(yield_time_ms.unwrap_or(10_000).min(30_000))
}

pub(super) fn command_process_output_cap(params: &CommandExecParams) -> usize {
    if params.disable_output_cap {
        usize::MAX
    } else {
        params
            .output_bytes_cap
            .unwrap_or(crate::adapters::app_server::DEFAULT_COMMAND_OUTPUT_CAP_BYTES)
    }
}

pub(super) fn command_visible_output_cap(params: &CommandExecParams) -> usize {
    command_process_output_cap(params)
}

pub(super) fn command_process_snapshot_json(
    snapshot: &verlet_process::AsyncProcessSnapshot,
) -> serde_json::Value {
    serde_json::json!({
        "processId": snapshot.process_id.map(|id| id.to_string()),
        "status": snapshot.status.as_str(),
        "exitCode": snapshot.exit_code,
        "stdout": String::from_utf8_lossy(&snapshot.stdout).into_owned(),
        "stderr": String::from_utf8_lossy(&snapshot.stderr).into_owned(),
        "truncated": snapshot.stdout_truncated || snapshot.stderr_truncated,
        "stdoutTruncated": snapshot.stdout_truncated,
        "stderrTruncated": snapshot.stderr_truncated,
        "backend": snapshot.backend,
        "label": snapshot.label,
        "eventCount": snapshot.events.len(),
    })
}

pub(super) fn mcp_source_param_error(err: impl std::fmt::Display) -> JsonRpcErrorError {
    jsonrpc_error(-32602, err.to_string())
}

pub(super) fn mcp_source_not_found(name: &str) -> JsonRpcErrorError {
    jsonrpc_error(-32602, format!("MCP source {name:?} was not found"))
}

pub(super) fn thread_not_found(thread_id: &str) -> JsonRpcErrorError {
    jsonrpc_error(-32001, format!("thread not found: {thread_id}"))
}

pub(super) fn unknown_agent_ref(agent_ref: &str, err: crate::VerletError) -> JsonRpcErrorError {
    jsonrpc_error(-32602, format!("unknown agent ref {agent_ref:?}: {err}"))
}

pub(super) fn malformed_agent_ref(agent_ref: &str, err: crate::VerletError) -> JsonRpcErrorError {
    jsonrpc_error(-32602, format!("malformed agent ref {agent_ref:?}: {err}"))
}

pub(super) fn thread_start_bind_error(err: crate::VerletError) -> JsonRpcErrorError {
    match &err {
        crate::VerletError::RuntimeFactory(message)
            if message.starts_with("thread/start ") || message.starts_with("runtime override ") =>
        {
            jsonrpc_error(-32602, err.to_string())
        }
        _ => internal_error(err),
    }
}

pub(super) fn lower_thread_start_cwd_override(
    params: &mut ThreadStartParams,
    base_cwd: &std::path::Path,
) -> Result<(), JsonRpcErrorError> {
    let Some(cwd) = params.cwd.take() else {
        return Ok(());
    };
    if cwd.trim().is_empty() {
        return Ok(());
    }
    let lowered_cwd = cwd_string(&crate::adapters::app_server::threads::resolve_cwd(
        base_cwd,
        Some(&cwd),
    ));
    let overrides = params
        .runtime_overrides
        .get_or_insert_with(Default::default);
    if let Some(default_cwd) = &overrides.default_cwd {
        let effective_default_cwd = cwd_string(&crate::adapters::app_server::threads::resolve_cwd(
            base_cwd,
            Some(default_cwd),
        ));
        if effective_default_cwd != lowered_cwd {
            return Err(jsonrpc_error(
                -32602,
                "thread/start accepts either cwd or runtimeOverrides.defaultCwd, not both",
            ));
        }
        overrides.default_cwd = Some(lowered_cwd);
        return Ok(());
    }
    overrides.default_cwd = Some(lowered_cwd);
    Ok(())
}

pub(super) fn thread_start_default_agent_ref(params: &ThreadStartParams) -> Option<&'static str> {
    if params.agent_ref.is_none() {
        Some(crate::adapters::app_server::default_manifest::DEFAULT_AGENT_REF)
    } else {
        None
    }
}

pub(super) fn malformed_thread_events_cursor() -> JsonRpcErrorError {
    jsonrpc_error(-32602, "malformed thread/events/list cursor")
}

pub(super) fn thread_events_cursor_history_error(err: crate::HistoryError) -> JsonRpcErrorError {
    match err {
        crate::HistoryError::StreamCursorStreamMismatch { .. }
        | crate::HistoryError::StreamCursorMismatch { .. } => jsonrpc_error(
            -32602,
            format!("malformed thread/events/list cursor: {err}"),
        ),
        crate::HistoryError::Codec(message) if message.contains("stream cursor") => jsonrpc_error(
            -32602,
            format!("malformed thread/events/list cursor: {message}"),
        ),
        other => internal_error(crate::VerletError::History(other.to_string())),
    }
}

pub(super) fn malformed_thread_events_stream(stream: &str) -> JsonRpcErrorError {
    jsonrpc_error(
        -32602,
        format!("thread/events/list stream {stream:?} must be thread, control, or derived:<name>"),
    )
}

pub(super) fn thread_events_stream_id(
    coordinates: &crate::ThreadCoordinates,
    stream: &str,
) -> Result<crate::EventStreamId, JsonRpcErrorError> {
    if stream == "thread" {
        return Ok(crate::EventStreamId::for_thread(coordinates));
    }
    if stream == "control" {
        return Ok(crate::EventStreamId::new(format!(
            "control:{}",
            coordinates.thread_id
        )));
    }
    let Some(name) = stream.strip_prefix("derived:") else {
        return Err(malformed_thread_events_stream(stream));
    };
    crate::validate_record_name(name).map_err(|_| malformed_thread_events_stream(stream))?;
    Ok(crate::EventStreamId::new(format!(
        "{stream}:{}",
        coordinates.thread_id
    )))
}

pub(super) fn rpc_ingress_received_record(
    coordinates: crate::ThreadCoordinates,
    payload: crate::IoIngressReceivedPayload,
    principal: &verlet_io_core::IoPrincipal,
    envelope_metadata: Option<&std::collections::BTreeMap<String, String>>,
) -> Result<crate::NewEventRecord, JsonRpcErrorError> {
    let mut value = serde_json::to_value(payload).map_err(json_codec_error)?;
    let object = value.as_object_mut().ok_or_else(|| {
        internal_error(crate::VerletError::History(
            "io.ingress.received payload did not encode as an object".to_string(),
        ))
    })?;
    object.insert(
        "schema".to_string(),
        serde_json::json!(crate::EventKind::IoIngressReceived.payload_schema_id()),
    );
    object.insert(
        "principal".to_string(),
        serde_json::to_value(principal).map_err(json_codec_error)?,
    );
    if let Some(metadata) = envelope_metadata {
        object.insert(
            "envelope_metadata".to_string(),
            serde_json::to_value(metadata).map_err(json_codec_error)?,
        );
    }
    Ok(crate::NewEventRecord::witnessed(
        coordinates,
        crate::EventKind::IoIngressReceived,
        value,
    ))
}

pub(super) fn absolute_path(
    path: std::path::PathBuf,
) -> Result<std::path::PathBuf, JsonRpcErrorError> {
    if path.is_absolute() {
        Ok(path)
    } else {
        Err(jsonrpc_error(
            -32602,
            format!("path must be absolute: {}", path.display()),
        ))
    }
}

pub(super) fn cwd_string(path: &std::path::Path) -> String {
    path.to_string_lossy().into_owned()
}

fn thread_source_cut_json(
    coordinates: &crate::ThreadCoordinates,
    checkpoint: &crate::ThreadCheckpoint,
    stream_to_sequence: Option<crate::EventSequence>,
) -> serde_json::Value {
    let stream_id = crate::EventStreamId::for_thread(coordinates);
    serde_json::json!({
        "threadId": coordinates.thread_id.to_string(),
        "checkpointId": checkpoint.id.to_string(),
        "leafEntryId": checkpoint.active_entry_id.as_ref().map(ToString::to_string),
        "streamId": stream_id.as_str(),
        "streamToSequence": stream_to_sequence.map(|sequence| sequence.get()),
    })
}

pub(super) fn thread_fork_reason_string(
    reason: &crate::ThreadForkReason,
) -> Result<String, JsonRpcErrorError> {
    let value = serde_json::to_value(reason).map_err(|err| {
        internal_error(crate::VerletError::RuntimeFactory(format!(
            "failed to encode thread fork reason: {err}"
        )))
    })?;
    value.as_str().map(str::to_string).ok_or_else(|| {
        internal_error(crate::VerletError::RuntimeFactory(
            "thread fork reason did not encode as a string".to_string(),
        ))
    })
}

fn agent_publish_plan_from_draft(
    params: &AgentDraftParams,
) -> Result<(crate::AgentPublishPlan, String), JsonRpcErrorError> {
    match (params.source.as_deref(), params.manifest.as_ref()) {
        (Some(_), Some(_)) => Err(jsonrpc_error(
            -32602,
            "agent draft accepts either source or manifest, not both",
        )),
        (Some(source), None) => crate::AgentPublishPlan::from_source(source)
            .map(|plan| (plan, source.to_string()))
            .map_err(agent_draft_error),
        (None, Some(manifest)) => {
            let manifest = serde_json::from_value::<crate::AgentManifestSchema>(manifest.clone())
                .map_err(|err| {
                jsonrpc_error(-32602, format!("invalid agent manifest JSON: {err}"))
            })?;
            manifest.validate().map_err(|err| {
                agent_draft_error(crate::VerletError::RuntimeFactory(err.to_string()))
            })?;
            let source = crate::agent::manifest::agent_manifest_source_from_schema(&manifest)
                .map_err(agent_draft_error)?;
            crate::AgentPublishPlan::from_source(&source)
                .map(|plan| (plan, source))
                .map_err(agent_draft_error)
        }
        (None, None) => Err(jsonrpc_error(
            -32602,
            "agent draft requires source or manifest",
        )),
    }
}

fn verify_agent_plan_refs(
    plan: &mut crate::AgentPublishPlan,
    operation_registry_root: std::path::PathBuf,
) -> Result<(), JsonRpcErrorError> {
    if operation_registry_root.exists() {
        plan.verify_operation_refs(&operation_registry_root)
            .map_err(agent_draft_error)
    } else {
        plan.mark_operation_refs_unverified_offline();
        Ok(())
    }
}

fn agent_draft_error(err: crate::VerletError) -> JsonRpcErrorError {
    jsonrpc_error(-32602, format!("invalid agent manifest: {err}"))
}

fn agent_plan_diagnostics(plan: &crate::AgentPublishPlan) -> Vec<serde_json::Value> {
    let mut diagnostics = Vec::new();
    for resolved_ref in &plan.resolved_refs {
        if resolved_ref.status == crate::AgentManifestRefStatus::UnresolvedOffline {
            diagnostics.push(serde_json::json!({
                "code": "unresolved_ref",
                "severity": "warning",
                "message": format!(
                    "artifact ref {:?} is unresolved offline",
                    resolved_ref.declared
                ),
                "ref": resolved_ref.declared,
            }));
        }
    }
    for verification in &plan.ref_verifications {
        if verification.status == crate::AgentManifestRefVerificationStatus::UnverifiedOffline {
            diagnostics.push(serde_json::json!({
                "code": "unverified_operation_ref",
                "severity": "warning",
                "message": format!(
                    "operation ref {:?} was not verified because no operation registry is available",
                    verification.declared
                ),
                "ref": verification.declared,
            }));
        }
    }
    diagnostics
}

fn agent_draft_base_json(
    registry: &crate::LocalAgentRegistry,
    params: &AgentDraftParams,
) -> Result<serde_json::Value, JsonRpcErrorError> {
    let Some(base_ref) = params.base_ref.as_deref() else {
        return Ok(serde_json::Value::Null);
    };
    crate::AgentRecordRef::parse(base_ref).map_err(|err| malformed_agent_ref(base_ref, err))?;
    let (base, alias) = registry
        .load_ref_with_alias_receipt(base_ref)
        .map_err(|err| unknown_agent_ref(base_ref, err))?;
    let latest = registry
        .resolve_alias(&base.name, "latest")
        .map(|(_record, receipt)| receipt)
        .ok();
    Ok(serde_json::json!({
        "ref": base_ref,
        "name": base.name,
        "namespace": base.namespace,
        "version": base.version,
        "manifestHash": base.manifest_hash,
        "latestVersion": latest.as_ref().map(|receipt| receipt.version.as_str()),
        "latestManifestHash": latest.as_ref().map(|receipt| receipt.manifest_hash.as_str()),
        "aliasResolutionReceipt": alias,
    }))
}

fn validate_agent_publish_base(
    registry: &crate::LocalAgentRegistry,
    params: &AgentDraftParams,
) -> Result<crate::PublishedAgentRecord, JsonRpcErrorError> {
    let base_ref = params.base_ref.as_deref().ok_or_else(|| {
        jsonrpc_error(
            -32602,
            "agent/publish requires baseRef for stale draft protection",
        )
    })?;
    let base_manifest_hash = params.base_manifest_hash.as_deref().ok_or_else(|| {
        jsonrpc_error(
            -32602,
            "agent/publish requires baseManifestHash for stale draft protection",
        )
    })?;
    let expected_latest_version = params.expected_latest_version.as_deref().ok_or_else(|| {
        jsonrpc_error(
            -32602,
            "agent/publish requires expectedLatestVersion for stale draft protection",
        )
    })?;
    crate::AgentRecordRef::parse(base_ref).map_err(|err| malformed_agent_ref(base_ref, err))?;
    let (base, _alias) = registry
        .load_ref_with_alias_receipt(base_ref)
        .map_err(|err| unknown_agent_ref(base_ref, err))?;
    if base.manifest_hash != base_manifest_hash {
        return Err(stale_agent_manifest_draft(format!(
            "base ref {base_ref:?} resolved to manifest hash {}, expected {}",
            base.manifest_hash, base_manifest_hash
        )));
    }
    let current = registry.load_record(&base.name).map_err(internal_error)?;
    if current.manifest_hash != base_manifest_hash {
        return Err(stale_agent_manifest_draft(format!(
            "agent {:?} current manifest hash is {}, expected {}",
            base.name, current.manifest_hash, base_manifest_hash
        )));
    }
    let (_latest_record, latest) = registry
        .resolve_alias(&base.name, "latest")
        .map_err(internal_error)?;
    if latest.version != expected_latest_version || latest.manifest_hash != base_manifest_hash {
        return Err(stale_agent_manifest_draft(format!(
            "agent {:?} latest is {} ({}) but draft expected {} ({})",
            base.name,
            latest.version,
            latest.manifest_hash,
            expected_latest_version,
            base_manifest_hash
        )));
    }
    Ok(base)
}

fn stale_agent_manifest_draft(detail: String) -> JsonRpcErrorError {
    jsonrpc_error(
        -32000,
        format!("stale agent manifest draft: {detail}; refresh the agent before publishing"),
    )
}

fn suggested_agent_version(
    registry: &crate::LocalAgentRegistry,
    name: &str,
    version: &str,
) -> crate::VerletResult<String> {
    let mut candidate = version.to_string();
    for _ in 0..1000 {
        if agent_version_slot_available(registry, name, &candidate)? {
            return Ok(candidate);
        }
        candidate = bump_agent_version(&candidate);
    }
    Err(crate::VerletError::RuntimeFactory(format!(
        "could not find an available version for agent {name:?} after 1000 attempts"
    )))
}

fn agent_version_slot_available(
    registry: &crate::LocalAgentRegistry,
    name: &str,
    version: &str,
) -> crate::VerletResult<bool> {
    if version == "latest" {
        return Ok(false);
    }
    if registry.version_record_path(name, version)?.exists() {
        return Ok(false);
    }
    if let Ok(alias_path) = registry.alias_record_path(name, version)
        && alias_path.exists()
    {
        return Ok(false);
    }
    Ok(true)
}

fn bump_agent_version(version: &str) -> String {
    let parts = version.split('.').collect::<Vec<_>>();
    if parts.len() == 3
        && let (Ok(major), Ok(minor), Ok(patch)) = (
            parts[0].parse::<u64>(),
            parts[1].parse::<u64>(),
            parts[2].parse::<u64>(),
        )
    {
        return format!("{major}.{minor}.{}", patch + 1);
    }
    if let Some((prefix, suffix)) = version.rsplit_once('.')
        && let Ok(number) = suffix.parse::<u64>()
    {
        return format!("{prefix}.{}", number + 1);
    }
    format!("{version}.1")
}

pub(super) fn agent_list_entry(
    registry: &crate::LocalAgentRegistry,
    record: &crate::PublishedAgentRecord,
) -> Result<serde_json::Value, JsonRpcErrorError> {
    let identity = record.resolved_manifest.get("identity");
    let default_model_profile = record
        .resolved_manifest
        .get("model_profiles")
        .and_then(serde_json::Value::as_array)
        .and_then(|profiles| profiles.first())
        .map(|profile| {
            serde_json::json!({
                "id": profile.get("id").and_then(serde_json::Value::as_str).unwrap_or_default(),
                "providerRef": profile
                    .get("provider_ref")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
                "modelRef": profile
                    .get("model_ref")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default(),
            })
        })
        .unwrap_or(serde_json::Value::Null);
    let tool_ids = record
        .tool_refs
        .iter()
        .map(|tool| tool.name.clone())
        .collect::<Vec<_>>();
    let mut entry = serde_json::json!({
        "name": record.name,
        "version": record.version,
        "refUri": record.ref_uri,
        "manifestHash": record.manifest_hash,
        "defaultModelProfile": default_model_profile,
        "toolIds": tool_ids,
        "aliases": agent_aliases(registry, &record.name)?,
        "publishedAtMs": record.published_at_ms,
    });
    if let Some(title) = identity
        .and_then(|identity| identity.get("display_name"))
        .and_then(serde_json::Value::as_str)
    {
        entry["title"] = serde_json::json!(title);
    }
    if let Some(summary) = identity
        .and_then(|identity| identity.get("description"))
        .and_then(serde_json::Value::as_str)
        .or(record.description.as_deref())
    {
        entry["summary"] = serde_json::json!(summary);
    }
    Ok(entry)
}

pub(super) fn agent_aliases(
    registry: &crate::LocalAgentRegistry,
    name: &str,
) -> Result<Vec<serde_json::Value>, JsonRpcErrorError> {
    let aliases_dir = registry.root().join("aliases").join(name);
    if !aliases_dir.exists() {
        return Ok(Vec::new());
    }
    let mut aliases = Vec::new();
    for entry in std::fs::read_dir(&aliases_dir).map_err(|err| {
        internal_error(crate::VerletError::RuntimeFactory(format!(
            "failed to read agent aliases directory {}: {err}",
            aliases_dir.display()
        )))
    })? {
        let entry = entry.map_err(|err| {
            internal_error(crate::VerletError::RuntimeFactory(format!(
                "failed to read agent alias entry in {}: {err}",
                aliases_dir.display()
            )))
        })?;
        if entry.path().extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let path = entry.path();
        let Some(alias) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let (_record, receipt) = registry
            .resolve_alias(name, alias)
            .map_err(internal_error)?;
        aliases.push(serde_json::json!({
            "alias": receipt.alias,
            "version": receipt.version,
        }));
    }
    aliases.sort_by(|left, right| {
        left.get("alias")
            .and_then(serde_json::Value::as_str)
            .cmp(&right.get("alias").and_then(serde_json::Value::as_str))
    });
    Ok(aliases)
}

pub(super) fn operation_list_entry(
    record: &crate::PublishedOperationRecord,
) -> Result<serde_json::Value, JsonRpcErrorError> {
    Ok(serde_json::json!({
        "name": record.name,
        "activeArtifactHash": record.active_artifact_hash,
        "summary": operation_summary(record),
        "manifest": record.manifest,
        "projections": record.projections,
        "interface": record.interface,
        "capabilityGrants": record.capability_grants,
        "metadata": record.metadata,
        "source": record.source,
        "build": record.build,
    }))
}

pub(super) fn operation_summary(record: &crate::PublishedOperationRecord) -> Option<String> {
    record.interface.as_ref().and_then(|interface| {
        interface.identity.description.clone().or_else(|| {
            interface.operations.iter().find_map(|operation| {
                operation.description.clone().or_else(|| {
                    operation
                        .manual
                        .as_ref()
                        .map(|manual| manual.summary.clone())
                })
            })
        })
    })
}

pub(super) fn configured_model_json(
    provider_id: &str,
    model_id: &str,
    display_name: String,
    description: &str,
) -> serde_json::Value {
    serde_json::json!({
        "id": model_id,
        "model": model_id,
        "providerId": provider_id,
        "providerRef": format!("provider://{provider_id}"),
        "modelRef": format!("model://{provider_id}/{model_id}"),
        "upgrade": null,
        "upgradeInfo": null,
        "availabilityNux": null,
        "displayName": display_name,
        "description": description,
        "hidden": false,
        "supportedReasoningEfforts": [
            { "reasoningEffort": "none", "description": "No reasoning" }
        ],
        "defaultReasoningEffort": "none",
        "inputModalities": ["text"],
        "supportsPersonality": false,
        "additionalSpeedTiers": [],
        "serviceTiers": [],
        "isDefault": true,
    })
}

pub(super) fn catalog_model_json(
    provider: &crate::LlmProviderRecord,
    model: &crate::LlmProviderModelRecord,
    is_default: bool,
) -> serde_json::Value {
    let display_name = model
        .display_name
        .clone()
        .unwrap_or_else(|| model.model_id.clone());
    serde_json::json!({
        "id": model.model_id,
        "model": model.model_id,
        "providerId": provider.provider_id,
        "providerRef": format!("provider://{}", provider.provider_id),
        "modelRef": format!("model://{}/{}", provider.provider_id, model.model_id),
        "api": model.api.as_ref().unwrap_or(&provider.api),
        "baseUrl": model.base_url.as_ref().unwrap_or(&provider.base_url),
        "upgrade": null,
        "upgradeInfo": null,
        "availabilityNux": null,
        "displayName": display_name,
        "description": catalog_provider_display_name(provider),
        "hidden": false,
        "supportedReasoningEfforts": [
            { "reasoningEffort": "none", "description": "No reasoning" }
        ],
        "defaultReasoningEffort": "none",
        "inputModalities": model.input_modalities,
        "supportsPersonality": false,
        "additionalSpeedTiers": [],
        "serviceTiers": [],
        "contextWindowTokens": model.context_window_tokens,
        "maxOutputTokens": model.max_output_tokens,
        "metadata": model.metadata,
        "isDefault": is_default,
    })
}

pub(super) fn catalog_provider_display_name(provider: &crate::LlmProviderRecord) -> String {
    provider
        .display_name
        .clone()
        .unwrap_or_else(|| provider.provider_id.clone())
}

pub(super) fn model_provider_record_from_rpc(
    provider: ModelProviderUpsertRecord,
) -> Result<crate::LlmProviderRecord, JsonRpcErrorError> {
    validate_model_provider_auth_config(&provider.auth)?;
    validate_model_provider_config_values(&provider.headers)?;
    let api = provider_api_from_rpc_value(provider.api)?;
    let mut record = crate::LlmProviderRecord::new(provider.provider_id, api, provider.base_url)
        .with_auth(provider.auth)
        .with_auth_header(provider.auth_header);
    record.display_name = provider.display_name;
    record.headers = provider.headers;
    record.metadata = provider.metadata;
    record.models = provider
        .models
        .into_iter()
        .map(model_provider_model_record_from_rpc)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(record)
}

fn model_provider_model_record_from_rpc(
    model: ModelProviderModelUpsertRecord,
) -> Result<crate::LlmProviderModelRecord, JsonRpcErrorError> {
    validate_model_provider_config_values(&model.headers)?;
    Ok(crate::LlmProviderModelRecord {
        model_id: model.model_id,
        display_name: model.display_name,
        api: model.api.map(provider_api_from_rpc_value).transpose()?,
        base_url: model.base_url,
        context_window_tokens: model.context_window_tokens,
        max_output_tokens: model.max_output_tokens,
        input_modalities: model.input_modalities,
        headers: model.headers,
        metadata: model.metadata,
    })
}

fn validate_model_provider_auth_config(
    auth: &crate::LlmProviderAuthConfig,
) -> Result<(), JsonRpcErrorError> {
    match auth {
        crate::LlmProviderAuthConfig::InlineApiKey { .. } => Err(jsonrpc_error(
            -32602,
            "modelProvider/upsert rejects inline API keys; use modelProvider/auth/set",
        )),
        crate::LlmProviderAuthConfig::Command { .. } => Err(jsonrpc_error(
            -32602,
            "modelProvider/upsert does not support command-backed auth in v1",
        )),
        crate::LlmProviderAuthConfig::StoredOrEnvironment
        | crate::LlmProviderAuthConfig::None
        | crate::LlmProviderAuthConfig::Env { .. } => Ok(()),
    }
}

fn validate_model_provider_config_values(
    values: &std::collections::BTreeMap<String, crate::LlmProviderConfigValue>,
) -> Result<(), JsonRpcErrorError> {
    if let Some((name, _)) = values
        .iter()
        .find(|(_, value)| matches!(value, crate::LlmProviderConfigValue::Command { .. }))
    {
        return Err(jsonrpc_error(
            -32602,
            format!("modelProvider/upsert does not support command-backed header {name:?} in v1"),
        ));
    }
    Ok(())
}

fn model_provider_model_json(
    provider: &crate::LlmProviderRecord,
    model: &crate::LlmProviderModelRecord,
    is_default: bool,
) -> serde_json::Value {
    serde_json::json!({
        "modelId": model.model_id,
        "displayName": model.display_name,
        "api": provider_api_rpc_json(model.api.as_ref().unwrap_or(&provider.api)),
        "baseUrl": model.base_url.as_ref().unwrap_or(&provider.base_url),
        "contextWindowTokens": model.context_window_tokens,
        "maxOutputTokens": model.max_output_tokens,
        "inputModalities": model.input_modalities,
        "headers": redacted_model_provider_config_values(&model.headers),
        "metadata": model.metadata,
        "isDefault": is_default,
    })
}

fn redacted_model_provider_auth_config(auth: &crate::LlmProviderAuthConfig) -> serde_json::Value {
    match auth {
        crate::LlmProviderAuthConfig::StoredOrEnvironment => {
            serde_json::json!({ "type": "stored_or_environment" })
        }
        crate::LlmProviderAuthConfig::None => serde_json::json!({ "type": "none" }),
        crate::LlmProviderAuthConfig::Env { name } => {
            serde_json::json!({ "type": "env", "name": name })
        }
        crate::LlmProviderAuthConfig::InlineApiKey { .. } => {
            serde_json::json!({ "type": "inline_api_key", "key": { "redacted": true } })
        }
        crate::LlmProviderAuthConfig::Command { .. } => {
            serde_json::json!({ "type": "command", "command": { "redacted": true } })
        }
    }
}

fn redacted_model_provider_config_values(
    values: &std::collections::BTreeMap<String, crate::LlmProviderConfigValue>,
) -> Vec<serde_json::Value> {
    values
        .iter()
        .map(|(name, value)| {
            serde_json::json!({
                "name": name,
                "value": redacted_model_provider_config_value(value),
            })
        })
        .collect()
}

fn redacted_model_provider_config_value(
    value: &crate::LlmProviderConfigValue,
) -> serde_json::Value {
    match value {
        crate::LlmProviderConfigValue::Literal { .. } => {
            serde_json::json!({ "type": "literal", "value": { "redacted": true } })
        }
        crate::LlmProviderConfigValue::Env { name } => {
            serde_json::json!({ "type": "env", "name": name })
        }
        crate::LlmProviderConfigValue::Command { .. } => {
            serde_json::json!({ "type": "command", "command": { "redacted": true } })
        }
    }
}

fn provider_api_from_rpc_value(
    value: serde_json::Value,
) -> Result<crate::ProviderApi, JsonRpcErrorError> {
    match value {
        serde_json::Value::String(api) => provider_api_from_rpc_str(&api),
        serde_json::Value::Object(mut object) => {
            let Some(other) = object
                .remove("other")
                .and_then(|value| value.as_str().map(str::to_string))
            else {
                return Err(jsonrpc_error(
                    -32602,
                    "modelProvider/upsert api object must be {\"other\":\"...\"}",
                ));
            };
            Ok(crate::ProviderApi::Other(other))
        }
        _ => Err(jsonrpc_error(
            -32602,
            "modelProvider/upsert api must be a string or {\"other\":\"...\"}",
        )),
    }
}

fn provider_api_from_rpc_str(api: &str) -> Result<crate::ProviderApi, JsonRpcErrorError> {
    match api {
        "open_ai_responses" | "open_a_i_responses" => Ok(crate::ProviderApi::OpenAIResponses),
        "open_ai_chat_completions" | "open_a_i_chat_completions" => {
            Ok(crate::ProviderApi::OpenAIChatCompletions)
        }
        "anthropic_messages" => Ok(crate::ProviderApi::AnthropicMessages),
        other => Err(jsonrpc_error(
            -32602,
            format!(
                "unknown modelProvider api {other:?}; expected open_ai_responses, open_ai_chat_completions, anthropic_messages, or {{\"other\":\"...\"}}"
            ),
        )),
    }
}

fn provider_api_rpc_json(api: &crate::ProviderApi) -> serde_json::Value {
    match api {
        crate::ProviderApi::OpenAIResponses => serde_json::json!("open_ai_responses"),
        crate::ProviderApi::OpenAIChatCompletions => serde_json::json!("open_ai_chat_completions"),
        crate::ProviderApi::AnthropicMessages => serde_json::json!("anthropic_messages"),
        crate::ProviderApi::Other(other) => serde_json::json!({ "other": other }),
    }
}

pub(super) fn thread_event_record_json(
    record: &crate::EventRecord,
) -> Result<serde_json::Value, JsonRpcErrorError> {
    let envelope = record.to_stream_record_v1();
    let mut value = serde_json::to_value(envelope).map_err(json_codec_error)?;
    let object = value.as_object_mut().ok_or_else(|| {
        internal_error(crate::VerletError::RuntimeFactory(
            "stream record envelope did not encode as an object".to_string(),
        ))
    })?;
    object.insert(
        "eventId".to_string(),
        serde_json::json!(record.id.to_string()),
    );
    object.insert("atMs".to_string(), serde_json::json!(record.created_at_ms));
    Ok(value)
}

pub(super) fn coupling_binding_json(
    binding: &crate::AgentManifestCouplingBinding,
) -> serde_json::Value {
    serde_json::json!({
        "id": binding.id.clone(),
        "role": coupling_role_json(binding.role),
        "triggerKind": binding.trigger_kind.clone(),
        "triggerMatch": binding.trigger_match.clone(),
        "sourceStreams": binding.source_streams.clone(),
        "sourceKinds": binding.source_kinds.clone(),
        "sinkStream": binding.sink_stream.clone(),
        "sinkKinds": binding.sink_kinds.clone(),
        "functionRef": binding.function_ref.clone(),
        "artifactHash": binding.artifact_hash.clone(),
        "operationName": binding.operation_name.clone(),
        "grants": binding.grants.clone(),
        "budget": {
            "maxMs": binding.budget.max_ms,
            "maxDischargeEvents": binding.budget.max_discharge_events,
        },
        "configHash": binding.config_hash.clone(),
    })
}

pub(super) fn coupling_role_json(role: crate::CouplingRole) -> &'static str {
    match role {
        crate::CouplingRole::Projection => "projection",
        crate::CouplingRole::Controller => "controller",
    }
}

pub(super) fn existing_approval_resolution<'a>(
    events: &'a [crate::EventRecord],
    approval_id: &str,
) -> Result<Option<(&'a crate::EventRecord, crate::ApprovalResolvedPayload)>, JsonRpcErrorError> {
    for event in events
        .iter()
        .filter(|event| event.kind == crate::EventKind::ApprovalResolved)
    {
        let payload =
            serde_json::from_value::<crate::ApprovalResolvedPayload>(event.payload.clone())
                .map_err(|err| {
                    internal_error(crate::VerletError::History(format!(
                        "approval.resolved payload is invalid: {err}"
                    )))
                })?;
        if payload.subject.approval_id == approval_id {
            return Ok(Some((event, payload)));
        }
    }
    Ok(None)
}

pub(super) fn approval_decision_from_bool(approved: bool) -> &'static str {
    if approved { "approved" } else { "denied" }
}

pub(super) fn approval_resolution_json(
    status: &str,
    decision: ApprovalResolveDecision,
    record: &crate::EventRecord,
    payload: &crate::ApprovalResolvedPayload,
) -> serde_json::Value {
    serde_json::json!({
        "status": status,
        "approvalId": payload.subject.approval_id.clone(),
        "decision": decision.as_str(),
        "approved": payload.approved,
        "reason": payload.reason.clone(),
        "snapshotId": payload.snapshot_id.clone(),
        "eventId": record.id.to_string(),
        "streamId": record.stream_id.as_str(),
        "sequence": record.sequence.get(),
        "createdAtMs": record.created_at_ms,
    })
}

pub(super) fn active_mandate_json(mandate: &crate::ActiveMandate) -> serde_json::Value {
    serde_json::json!({
        "mandateEventId": mandate.event.id.to_string(),
        "mandateId": mandate.payload.mandate_id.clone(),
        "threadId": mandate
            .payload
            .subject
            .thread_id
            .as_deref()
            .or(mandate.payload.thread_id.as_deref()),
        "schedule": mandate.payload.schedule.clone(),
        "maxOccurrences": mandate.payload.max_occurrences,
        "catchUp": mandate.payload.catch_up,
        "inputTemplate": mandate.payload.input_template.clone(),
        "createdAtMs": mandate.event.created_at_ms,
        "streamId": mandate.event.stream_id.as_str(),
        "sequence": mandate.event.sequence.get(),
    })
}

pub(super) fn mandate_jsonrpc_error(err: crate::VerletError) -> JsonRpcErrorError {
    match err {
        crate::VerletError::RuntimeExecution(_) => jsonrpc_error(-32602, err.to_string()),
        _ => internal_error(err),
    }
}

pub(super) fn pending_tool_approval_json(
    suspension: &crate::PendingToolCallSuspension,
) -> serde_json::Value {
    serde_json::json!({
        "approvalId": suspension.approval_id.clone(),
        "status": "pending",
        "kind": crate::EventKind::ToolCallSuspended.as_str(),
        "eventId": suspension.suspended_event_id.to_string(),
        "suspendedEventId": suspension.suspended_event_id.to_string(),
        "requestEventId": suspension.request_event_id.map(|id| id.to_string()),
        "turnId": suspension.subject.turn_id.clone(),
        "callId": suspension.subject.call_id.clone(),
        "snapshotId": suspension.snapshot_id.clone(),
        "reason": suspension.reason.clone(),
    })
}

pub(super) fn pending_tool_waiting_json(
    suspension: &crate::PendingToolCallSuspension,
) -> serde_json::Value {
    serde_json::json!({
        "kind": crate::EventKind::ToolCallSuspended.as_str(),
        "eventId": suspension.suspended_event_id.to_string(),
        "suspendedEventId": suspension.suspended_event_id.to_string(),
        "requestEventId": suspension.request_event_id.map(|id| id.to_string()),
        "turnId": suspension.subject.turn_id.clone(),
        "callId": suspension.subject.call_id.clone(),
        "snapshotId": suspension.snapshot_id.clone(),
        "approvalId": suspension.approval_id.clone(),
        "reason": suspension.reason.clone(),
        "continuation": "tool.call",
    })
}

pub(super) fn turn_waiting_json(record: &crate::EventRecord) -> serde_json::Value {
    let subject = record.payload.get("subject");
    let call_id = subject
        .and_then(|subject| subject.get("call_id"))
        .and_then(serde_json::Value::as_str);
    let source_event_ids = record
        .provenance
        .source_event_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    serde_json::json!({
        "kind": crate::EventKind::TurnWaiting.as_str(),
        "eventId": record.id.to_string(),
        "streamId": record.stream_id.as_str(),
        "sequence": record.sequence.get(),
        "createdAtMs": record.created_at_ms,
        "turnId": record.payload.get("turn_id").and_then(serde_json::Value::as_str),
        "callId": call_id,
        "snapshotId": record.payload.get("snapshot_id").and_then(serde_json::Value::as_str),
        "approvalId": record.payload.get("approval_id").and_then(serde_json::Value::as_str),
        "waitingOnEventId": record.payload.get("waiting_on_event_id").and_then(serde_json::Value::as_str),
        "continuation": record.payload.get("continuation").and_then(serde_json::Value::as_str),
        "reason": record.payload.get("reason").and_then(serde_json::Value::as_str),
        "payload": record.payload.clone(),
        "sourceEventIds": source_event_ids,
    })
}

pub(super) fn debug_export_ack_classes() -> Vec<&'static str> {
    vec!["local_committed", "query_projected"]
}

pub(super) fn debug_export_receipt_json(record: &crate::EventRecord) -> serde_json::Value {
    serde_json::json!({
        "eventId": record.id.to_string(),
        "streamId": record.stream_id.as_str(),
        "sequence": record.sequence.get(),
        "kind": record.kind.as_str(),
        "origin": record.origin.as_str(),
        "payloadSchema": record.kind.payload_schema_id(),
        "createdAtMs": record.created_at_ms,
    })
}

pub(super) fn redact_debug_export_value_with_evidence(
    value: &mut serde_json::Value,
    redacted_keys: &mut std::collections::BTreeSet<String>,
) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, child) in object {
                if debug_export_redacts_key(key) {
                    redacted_keys.insert(key.clone());
                    *child = serde_json::Value::String("[REDACTED]".to_string());
                } else {
                    redact_debug_export_value_with_evidence(child, redacted_keys);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                redact_debug_export_value_with_evidence(child, redacted_keys);
            }
        }
        _ => {}
    }
}

pub(super) fn debug_export_redacts_key(key: &str) -> bool {
    let normalized = key
        .chars()
        .filter(|ch| *ch != '_' && *ch != '-')
        .flat_map(char::to_lowercase)
        .collect::<String>();
    normalized.contains("apikey")
        || normalized.contains("token")
        || normalized.contains("secret")
        || normalized == "authorization"
        || normalized.contains("password")
        || normalized.contains("bearer")
}

pub(super) fn encode_thread_events_cursor(sequence: i64) -> Result<String, JsonRpcErrorError> {
    if sequence < 1 {
        return Err(malformed_thread_events_cursor());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(format!("v1:{sequence}")))
}

pub(super) fn decode_thread_events_cursor(
    cursor: &str,
) -> Result<crate::EventSequence, JsonRpcErrorError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(cursor.as_bytes())
        .map_err(|_| malformed_thread_events_cursor())?;
    let decoded = String::from_utf8(bytes).map_err(|_| malformed_thread_events_cursor())?;
    let Some(sequence) = decoded.strip_prefix("v1:") else {
        return Err(malformed_thread_events_cursor());
    };
    let sequence = sequence
        .parse::<i64>()
        .map_err(|_| malformed_thread_events_cursor())?;
    if sequence < 1 {
        return Err(malformed_thread_events_cursor());
    }
    Ok(crate::EventSequence::new(sequence))
}

pub(super) fn metadata_time_ms(time: Option<std::time::SystemTime>) -> u64 {
    time.and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

pub(super) fn cap_output(mut output: Vec<u8>, cap: Option<usize>) -> Vec<u8> {
    if let Some(cap) = cap {
        output.truncate(cap);
    }
    output
}

pub(super) fn copy_path(
    source: &std::path::Path,
    destination: &std::path::Path,
    recursive: bool,
) -> std::io::Result<()> {
    let metadata = std::fs::metadata(source)?;
    if metadata.is_dir() {
        if !recursive {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "fs/copy requires recursive=true for directories",
            ));
        }
        std::fs::create_dir_all(destination)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_path(
                &entry.path(),
                &destination.join(entry.file_name()),
                recursive,
            )?;
        }
        return Ok(());
    }
    std::fs::copy(source, destination)?;
    Ok(())
}

pub(super) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}
