//! Verlet is a small multi-tenant host boundary for agent runtime loops.
//!
//! The crate intentionally starts above provider, shell, sandbox, and product
//! concerns. A concrete runtime implementation can be a provider loop, a test
//! runtime, or a later virtual-shell/procedure backend, but the host owns
//! tenancy, lifecycle, cancellation, and event routing.

#[cfg(test)]
mod test_abort_tripwire;

#[cfg(test)]
extern crate self as verlet;

#[cfg(test)]
#[allow(dead_code)]
#[path = "../tests/support/lib_mount.rs"]
mod support;

#[cfg(test)]
pub(crate) mod test_support {
    #[allow(unused_imports)]
    pub(crate) use crate::support::event_trace::{
        EventTrace, assert_event_order, collect_until_cancelled, collect_until_compaction,
        collect_until_failed, collect_until_output, find_event_index, text_from_content,
        text_from_message,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::fault::{
        AppliedFaultPlan, FaultingIngressQueue, FaultingProviderClient, FaultingRuntimeStore,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::fault_plan::{
        CRASH_CUT_REGISTRY, CUTS_V1, CrashCutHost, CrashCutRegistration, CrashCutSeam,
        FAULT_VOCABULARY_VERSION, FaultComponent, FaultDirective, FaultPlan, FaultTiming,
        Intensity, PROVIDER_OPERATIONS_V1, PlannedAction, QUEUE_OPERATIONS_V1, STORE_OPERATIONS_V1,
        SplitMix64, crash_cut, run_crash_cut,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::invariant_claims::Inv6ClaimsSettle;
    #[allow(unused_imports)]
    pub(crate) use crate::support::invariant_forks::{
        INV7_ONE_CHILD_PER_FORK_CLAIM, INV8_RESERVED_BEFORE_CREATED, OneChildPerForkClaimInvariant,
        ReservedBeforeCreatedInvariant, fork_invariants_v1,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::invariants::{
        BoundedQueueInvariant, INV1_REPLAY_EQUIVALENCE, INV2_UNIQUE_ACTIVE_TOPOLOGY,
        INV3_BOUNDED_QUEUE, INV4_NO_DUPLICATE_PROJECTED_OUTPUT, INV5_TERMINAL_CONSISTENCY,
        NoDuplicateProjectedOutputInvariant, ReplayEquivalenceInvariant,
        TerminalConsistencyInvariant, UniqueActiveTopologyInvariant, invariant_set_v1,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::scenario::{
        CorpusEntry, InvariantViolation, Scenario, ScenarioBounds, ScenarioFailure,
        ScenarioInvariant, ScenarioOp, ScenarioWorld, StreamIoCrashReceipt, run_scenario,
        run_stream_io_crash_scenario,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::scripted_provider::{
        ScriptedProviderClient, ScriptedProviderStep, provider_factory, response_text,
        response_tool_call, response_tool_call_with_id, streaming_provider_factory,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::simulated_io::{
        CrashSurvival, IO_LOCK, IO_OPEN, IO_READ, IO_REMOVE, IO_SYNC, IO_TRUNCATE, IO_UNLOCK,
        IO_WRITE, IoFaultAction, IoFaultPlan, IoFaultRule, IoTranscriptEntry, SimulatedIo,
    };
    #[allow(unused_imports)]
    pub(crate) use crate::support::store_parity::session_store_parity_transcript;
    #[allow(unused_imports)]
    pub(crate) use crate::support::transcript::{
        NormalizedTranscript, NormalizedTranscriptItem, TypedTranscript,
    };
}

pub mod adapters {
    pub mod acp_agent;
    pub mod agent_loop;
    pub mod app_server;
    pub mod codex_tui;
    pub mod mcp_client;
    pub mod mcp_server;
    pub mod provider;
    pub mod provider_transform;
}

pub mod agent {
    pub mod agent_process;
    pub mod agent_tool_router;
    pub mod contracts;
    pub mod coupling_templates;
    pub mod hooks;
    pub mod manifest;
    pub mod manifest_bind;
    pub mod manifest_schema;
    pub mod tool_interceptor;
    pub mod tool_universe;
}

pub mod capabilities {
    pub mod abi;
    pub mod bridge;
    pub mod execution;
    pub mod process;
    pub mod vfs;
    pub mod wasm_runner;
}

#[doc(hidden)]
pub mod cli;

pub mod daemon {
    pub mod clock_route;
    pub mod daemon_config;
    pub mod daemon_io;
    pub(crate) mod handle_ingress;
    pub mod identity;
    pub(crate) mod recovery_sweep;
    pub mod remote_store;
}

#[doc(hidden)]
pub use verlet_runtime_contracts::env_compat;

pub mod kernel {
    pub(crate) mod admission;
    pub mod compaction;
    pub mod context_compiler;
    pub mod control_decision;
    pub mod coupling_executor_registry;
    pub mod coupling_scheduler;
    pub mod history;
    pub mod mandate_lifecycle;
    pub mod process_handle_dispatch;
    pub mod provider_store;
    pub mod runtime_host;
    pub mod secret_store;
    pub mod stdlib_couplings;
    pub mod supervisor;
    pub mod thread_spawn_projector;
    pub mod wasm_couplings;
}

#[doc(hidden)]
pub mod live_smoke_support;

pub mod operations {
    pub mod kernel_packages;
    pub mod openapi_import;
    pub mod operation_builder;
    pub mod operation_registry;
    pub mod operation_store;
    pub mod plugins;
    pub mod skill_import;
    pub mod skill_package;
    pub mod tool_package;
}

pub use adapters::acp_agent::{ACP_PROTOCOL_VERSION, VerletAcpAgentConfig, serve_acp_stdio};
pub use adapters::agent_loop::{AgentLoopConfig, AgentLoopFactory, ModelRequestRetryPolicy};
pub use adapters::app_server::{
    APP_SERVER_ANTHROPIC_BEDROCK_MODEL, APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER,
    APP_SERVER_ANTHROPIC_MODEL, APP_SERVER_ANTHROPIC_PROVIDER, APP_SERVER_BIFROST_MODEL,
    APP_SERVER_BIFROST_PROVIDER, APP_SERVER_LOCAL_MODEL, APP_SERVER_LOCAL_PROVIDER,
    APP_SERVER_OPENAI_COMPATIBLE_MODEL, APP_SERVER_OPENAI_COMPATIBLE_PROVIDER, AppServerListenAddr,
    AppServerProviderConfig, CapsuleBindingsConfig, ConsoleAssetConfig, JsonRpcError,
    JsonRpcErrorError, JsonRpcMessage, JsonRpcNotification, JsonRpcRequest, JsonRpcResponse,
    RequestId, VerletAppServer, VerletAppServerConfig,
};
pub use adapters::codex_tui::{
    CODEX_TUI_TEST_CLIENT_NAME, CODEX_TUI_UDS_WEBSOCKET_HANDSHAKE_URL, CodexTuiCompletedTurn,
    CodexTuiConnectConfig, CodexTuiEvent, CodexTuiTestClient, CodexTuiThread, CodexTuiTurn,
    VerletOperatorClient,
};
pub use adapters::mcp_client::{
    McpRemoteServerConfig, McpRemoteSourceRecord, McpRemoteToolProvider, McpRemoteTransport,
    McpStdioServerConfig, McpStdioToolProvider, McpToolUniverseDiscoverer, SqliteMcpSourceRegistry,
};
pub use adapters::mcp_server::{MCP_PROTOCOL_VERSION, VerletMcpServerConfig, serve_mcp_stdio};
pub use adapters::provider::{
    AnthropicBedrockMessagesAdapter, AnthropicMessagesAdapter, LocalOfflineProviderClient,
    OpenAIChatCompletionsAdapter, OpenAIReasoningSummary, OpenAIResponsesAdapter,
    ProviderAbiProjection, ProviderAuth, ProviderCapabilityRecord, ProviderClient,
    ProviderContextCompilation, ProviderContextPolicy, ProviderEndpoint, ProviderError,
    ProviderHttpClient, ProviderRequest, ProviderRequestMode, ProviderResponse, ProviderResult,
    ProviderStreamEvent, ProviderToolResultConstraints, ProviderWireAdapter, SystemBlock,
    ThinkingConfig, ThinkingEffort, ToolDefinition, compile_provider_context,
    compile_provider_request_context,
};
pub use adapters::provider_transform::{
    ReplayTransform, ReplayTransformCounts, normalize_history_for_target,
};
pub use agent::agent_process::{
    KernelNotifyOperationProvider, KernelProcessOperationProvider, KernelScheduleOperationProvider,
    KernelThreadOperationProvider, KernelThreadSpawnAgentBinding, KernelThreadSpawnAgentResolver,
};
pub use agent::agent_tool_router::{
    AgentKernelPendingToolCall, AgentKernelToolCall, AgentKernelToolOutcome,
    AgentKernelToolProvider, AgentToolRouter, DEFAULT_TOOL_CANCELLATION_GRACE, OperationToolAlias,
    RoutedAgentToolCall, ToolInvocationCancellation,
};
pub use agent::contracts::{
    AGENT_CONTRACT_KIND, AGENT_CONTRACT_SOURCE_FORMAT, AGENT_CONTRACT_VERSION,
    AGENT_THREAD_DECLARATION_KIND, AGENT_THREAD_HANDLE_KIND, AgentCapabilityRequirement,
    AgentContractCompiler, AgentContractField, AgentContractReference, AgentContractSource,
    AgentContractSourceFormat, AgentContractValueKind, AgentDelegateRequirement,
    AgentEffectRequirement, AgentInitialTurn, AgentThreadDeclaration, AgentThreadHandle,
    AgentThreadReceiptSet, AgentThreadTopologyDeclaration, CompiledAgentContract,
    CompiledThreadContract, DEFAULT_THREAD_PROPAGATOR_KIND, LEGACY_AGENT_CONTRACT_KIND,
    LEGACY_AGENT_CONTRACT_SOURCE_FORMAT, LEGACY_AGENT_THREAD_DECLARATION_KIND,
    LEGACY_AGENT_THREAD_HANDLE_KIND, THREAD_CONTRACT_KIND, THREAD_CONTRACT_SOURCE_FORMAT,
    THREAD_CONTRACT_VERSION, THREAD_DECLARATION_KIND, THREAD_HANDLE_KIND,
    ThreadCapabilityRequirement, ThreadContractCompiler, ThreadContractField,
    ThreadContractReference, ThreadContractSource, ThreadContractSourceFormat,
    ThreadContractValueKind, ThreadDeclaration, ThreadDelegateRequirement, ThreadEffectRequirement,
    ThreadHandle, ThreadInitialTurn, ThreadPropagatorSelection, ThreadReceiptSet,
    ThreadTopologyDeclaration,
};
pub use agent::coupling_templates::{
    COUPLING_TEMPLATE_CATALOG_SCHEMA_V1, CouplingTemplateCatalogV1, CouplingTemplateMaturity,
    CouplingTemplateStreamPattern, CouplingTemplateV1, coupling_template_catalog_v1,
    coupling_template_ids_v1,
};
pub use agent::hooks::{
    CommandHookHandler, HookEventName, HookHandler, HookHandlerOutput, HookHandlerSpec,
    HookMutationWitness, HookPipeline, HookRequest, HookRunRecord, HookRunStatus, HookValueDigest,
    PostCompactHookOutcome, PostCompactHookRequest, PostToolUseHookOutcome, PostToolUseHookRequest,
    PreCompactHookOutcome, PreCompactHookRequest, PreToolUseHookOutcome, PreToolUseHookRequest,
    SessionStartHookOutcome, SessionStartHookRequest, StopHookOutcome, StopHookRequest,
    UserPromptSubmitHookOutcome, UserPromptSubmitHookRequest,
};
pub use agent::manifest::{
    AgentAliasRecord, AgentAliasResolutionReceipt, AgentManifestDiffChange, AgentManifestDiffKind,
    AgentManifestRefVerification, AgentManifestRefVerificationStatus, AgentPublishPlan,
    AgentRecordRef, AgentToolRef, AgentVersionSummary, LocalAgentRegistry, PublishedAgentRecord,
    agent_ref_uri, default_blob_registry_root, default_blob_registry_root_for_agent_registry_root,
    default_operations_registry_root, diff_canonical_json,
};
pub use agent::manifest_bind::{
    AgentManifestBindOverrides, AgentManifestBindReceipt, AgentManifestBindingOrigin,
    AgentManifestBoundThread, AgentManifestCompileReceipt, AgentManifestCouplingBinding,
    AgentManifestDirectToolBinding, AgentManifestDiscoveredSkill, AgentManifestGrantBindingReceipt,
    AgentManifestModelProfileOrigin, AgentManifestModelProfileSelection,
    AgentManifestOperationBinding, AgentManifestPlacementBinding, AgentManifestProviderSurface,
    AgentManifestResolvedWorkspaceMount, AgentManifestSkillDiscovery,
    AgentManifestSkillPackageBinding, AgentManifestStaticContextSegment,
    AgentManifestWorkspaceBinding, BoundCoupling, BoundCouplingFunction, BoundCouplingSelector,
    BoundCouplingSet, BoundCouplingSink, CouplingRole, MANIFEST_BINDER_DISCHARGED_BY,
    MANIFEST_BINDER_FUNCTION, MANIFEST_COMPILER_DISCHARGED_BY, MANIFEST_COMPILER_FUNCTION,
    THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA, THREAD_AGENT_SKILL_DISCOVERY_METADATA,
    THREAD_AGENT_SKILL_PACKAGES_METADATA, THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA,
    apply_runtime_overrides, bind_published_agent_record, bind_published_agent_record_at,
    bind_published_agent_record_with_placement, bind_published_agent_record_with_placement_at,
    compile_published_agent_record, resolve_manifest_placement, resolve_manifest_workspace,
};
pub use agent::manifest_schema::{
    AgentManifestBashTool, AgentManifestBudgetRest, AgentManifestBudgetShare,
    AgentManifestCompactionDefaults, AgentManifestContextPipeline, AgentManifestContextSelector,
    AgentManifestContextSource, AgentManifestCoupling, AgentManifestCouplingBudget,
    AgentManifestCouplingQuota, AgentManifestCouplingSelector, AgentManifestCouplingSink,
    AgentManifestCouplingSource, AgentManifestCouplingTrigger, AgentManifestCredentialRef,
    AgentManifestDirectTool, AgentManifestFilesystemPolicy, AgentManifestGrant,
    AgentManifestGrantExpiry, AgentManifestIdentity, AgentManifestMaxToolRounds,
    AgentManifestModelFallback, AgentManifestModelParams, AgentManifestModelProfile,
    AgentManifestModelRetryPolicy, AgentManifestNetworkPolicy, AgentManifestPolicies,
    AgentManifestPolicyBudgets, AgentManifestProtocolToolImport, AgentManifestPublisher,
    AgentManifestRefStatus, AgentManifestResolvedRef, AgentManifestResource,
    AgentManifestResourceKind, AgentManifestResourceMode, AgentManifestResourceMount,
    AgentManifestRuntimeDefaults, AgentManifestRuntimeOverrideKey,
    AgentManifestRuntimeOverridePolicy, AgentManifestSchema, AgentManifestSkills,
    AgentManifestTool, AgentManifestToolProtocol, AgentManifestToolSurface,
    AgentManifestWorkspaceMode, AgentManifestWorkspaceRequirement, EffectClass,
    default_context_pipeline,
};
pub use agent::tool_interceptor::{
    AllowAllToolPermissionGate, ToolExecutionInterceptor, ToolExecutionOutcome,
    ToolExecutionRequest, ToolPermissionDecision, ToolPermissionGate, ToolPermissionRequest,
};
pub use agent::tool_universe::{
    MountedToolUniverse, PinnedToolRef, TOOL_CALL_TOOL, TOOL_DESCRIBE_TOOL, TOOL_SEARCH_TOOL,
    TOOL_UNIVERSE_SURFACE_DISCHARGED_BY, ToolUniverseBindReceipt, ToolUniverseBinding,
    ToolUniverseCallOutput, ToolUniverseCallReceipt, ToolUniverseCaller, ToolUniverseDiscoverer,
    ToolUniverseDiscovery, ToolUniverseDiscoveryReceipt, ToolUniverseSearchSurface,
    ToolUniverseToolReceipt, WitnessedToolContract, schema_hash_of, validate_tool_arguments,
};
pub use capabilities::abi::{
    AbiCapabilityGrant, AbiEffectBinding, AbiEffectClaim, AbiEffectKind, AbiEffectPort,
    AbiEffectReceipt, AbiEffectReceiptKind, AbiEventBinding, AbiEventPort, AbiEventValue,
    AbiOperationContract, AbiPortValue, AbiSinkBinding, AbiSinkPort, AbiSourceBinding,
    AbiSourcePort, AbiVfsWriteMode, AttachmentBinding, AttachmentIdentity, ExecutionPrincipal,
    InvocationContext, Principal, PrincipalKind, WasmOperationDefinition, WasmOperationEventKind,
    WasmOperationManifest, WasmOperationMode, WasmOperationValueKind,
};
pub use capabilities::bridge::{
    BROWSER_NAMESPACE, BridgeBackendKind, BridgeCapabilities, BridgeScope, BridgeSession,
    BridgeSessionId, COMPUTER_NAMESPACE, CapabilityBridge, CapabilityDescriptor, CapabilityGrant,
    FS_NAMESPACE, FileDeltaKind, OpenBridgeSessionRequest, OperationEvent, OperationEventStream,
    OperationExitStatus, OperationId, OperationLogLevel, OperationRequest, PROCEDURE_NAMESPACE,
    REDUCER_NAMESPACE, RejectingCapabilityBridge, UNIX_EXEC_OPERATION, UNIX_NAMESPACE,
    UnixExecPayload, UnixExecutionMode,
};
pub use capabilities::execution::{
    BASH_TOOL, BashExecutionPolicy, BashToolProvider, BashToolResultPayload,
    BashkitExecutionHarness, BashkitLiveBackend, CommandRoute, CommandRoutingPolicy,
    ExecutionDeadline, ExternalCommandExecutor, ExternalCommandInvocation, ExternalCommandRequest,
    ExternalCommandResult, ExternalExecutorKind, ExternalFileWrite, HostBashExecutor,
    HostBashExecutorConfig, PROCESS_EXEC_TOOL, RejectingExternalCommandExecutor,
    SPILL_RETENTION_MAX_BYTES, ToolOutputSpill, ToolOutputSpillReceipt, VirtualBashRuntimeConfig,
    VirtualBashRuntimeFactory, VirtualCommandOutput, VirtualFile, VirtualMount,
    VirtualMountBackend, VirtualMountMode, WRITE_STDIN_TOOL,
};
pub use capabilities::process::{
    AsyncExecutionManager, AsyncExecutionManagerConfig, AsyncProcessOutcome, AsyncProcessOwner,
    AsyncProcessSnapshot, AsyncProcessStartRequest, HostBashLiveBackend, LiveProcessBackend,
    LiveProcessInvocation, LiveProcessSpawn, LiveProcessStartRequest, ProcessSnapshotStatus,
    VerletProcessArtifact, VerletProcessBackend, VerletProcessEvent, VerletProcessEventKind,
    VerletProcessExitStatus, VerletProcessFileDelta, VerletProcessHandle, VerletProcessId,
    VerletProcessOutput, VerletProcessTerminalState, WasmOperationOutput,
};
pub use capabilities::vfs::{
    HostFileSystem, HostFileSystemMode, ManagedObjectStoreFs, ObjectStoreCachePolicy,
    ObjectStoreMountBackend, ObjectStoreMountConfig, R2ObjectStoreConfig, ReadOnlyFileSystem,
    S3ObjectStoreConfig, VerletVfs, VerletVfsBackend, VfsMutation, VfsMutationKind,
};
pub use capabilities::wasm_runner::{
    WasmHttpRequest, WasmHttpResponse, WasmRuntimeArtifact, WasmRuntimeConfig, WasmRuntimeFactory,
};
pub use daemon::clock_route::{
    CLOCK_TICK_ROUTE_KIND, DaemonClock, SystemDaemonClock, TIMER_FIRED_ENVELOPE_KIND,
    VerletDaemonClockRoute,
};
pub use daemon::daemon_config::{
    LoadedVerletDaemonConfig, VerletCoalesceBurstsConfig, VerletDaemonAppServerConfig,
    VerletDaemonConfig, VerletDaemonOperationsConfig, VerletDaemonRegistriesConfig,
    VerletDaemonServiceSpec, VerletDaemonServiceTarget, VerletEgressProjectionRuleConfig,
    VerletEgressRetryConfig, VerletIngressConfig, VerletIoConfig, VerletIoRouteConfig,
    VerletProjectDiscovery, VerletProviderConfig, VerletQueueConfig, VerletRuntimeConfig,
    VerletTelegramRouteConfig, VerletTypingSimulationConfig, default_verlet_daemon_socket_path,
    discover_verlet_daemon_config_path, discover_verlet_project, install_verlet_daemon_service,
    load_verlet_daemon_config, load_verlet_daemon_config_layers, render_verlet_daemon_service,
    uninstall_verlet_daemon_service, verlet_daemon_service_file_name,
    verlet_daemon_service_install_path, verlet_daemon_service_install_path_for_home,
};
pub use daemon::daemon_io::{
    DirectRuntimeIngressSink, RouteIngressSink, TelegramWebhookServer, TelegramWebhookServerConfig,
    VerletDaemonIoBridge, VerletDaemonQueueWorker,
};
pub use kernel::compaction::{
    COMPACTION_SUMMARY_PREFIX, CompactionPolicy, CompactionTrigger, compaction_summary_message,
    deterministic_compaction_summary, render_compaction_summary,
};
pub use kernel::context_compiler::{
    AgentContextAttachment, AgentContextCompilationDiagnostics, AgentContextCompileInput,
    AgentContextCompilePolicy, AgentContextCompiler, AgentContextDroppedEntry, AgentContextSource,
    CompiledAgentContext,
};
pub use kernel::control_decision::{
    ApprovalResolvedPayload, ApprovalSubject, MandateCatchUpPolicy, MandateRevokedPayload,
    MandateSchedulePayload, MandateStartedPayload, MandateSubject, PendingToolCallSuspension,
    PlacementDecision, PlacementDecisionPayload, PlacementDecisionRequest, PlacementSubject,
    PlacementTarget, ToolCallCancellation, ToolCallCompletedPayload, ToolCallDecision,
    ToolCallDecisionOutcomePayload, ToolCallDecisionPayload, ToolCallRequestedPayload,
    ToolCallSubject, ToolCallSuspendedPayload, ToolControllerBinding, ToolDecisionRequest,
    TurnContinuationAcceptedPayload, TurnContinuationDecision, TurnContinuationDecisionRequest,
    TurnContinuationRejectedPayload, TurnContinuationSubject, TurnContinueRequestedPayload,
    active_manifest_bind_receipt, active_tool_controller_for_request, control_stream_id,
    decide_placement, decide_tool_call, decide_turn_continuation,
    list_pending_tool_call_suspensions,
};
pub use kernel::coupling_scheduler::{
    CouplingActivation, CouplingBudgetSpent, CouplingDischarge, CouplingExecutionResult,
    CouplingExecutor, CouplingInvocation, CouplingRunReceipt, CouplingRunStatus, CouplingScheduler,
    CouplingSchedulerConfig, CouplingSchedulerCycleReceipt, CouplingSourceCut,
    CouplingSourceCutEntry,
};
pub use kernel::history::{
    AdmissionDecidedPayload, AdmissionDecision, CONTEXT_READ_PLAN_SCHEMA_V1, CacheControl,
    CacheTtl, CanonicalContent, CanonicalMessage, CanonicalStopReason, CanonicalUsage,
    ClientRecordAppendedPayload, DEBUG_THREAD_EXPORT_SCHEMA_V1, EVENT_KIND_SCHEMA_VERSION,
    EventKind, EventOrigin, EventProvenance, EventRecord, EventRecordId, EventSequence, EventStore,
    EventStreamId, GrantPetitionedPayload, HistoryError, HistoryResult, InMemorySessionStore,
    IngressOutcomeIntent, IngressSettledBy, IoEgressDeliveredPayload, IoEgressFailedPayload,
    IoEgressRequestedPayload, IoIngressClaimedPayload, IoIngressReceivedPayload,
    IoIngressSettledPayload, NewEventRecord, NewObservationRecord, ObservationId,
    ObservationProvenance, ObservationRecord, ObservationSourceRange, ObservationStore,
    PolicyBoundPayload, PolicyKind, ProviderApi, RuntimeStore, STREAM_APPEND_ACK_SCHEMA_V1,
    STREAM_BACKEND_CAPABILITIES_SCHEMA_V1, STREAM_CURSOR_SCHEMA_V1, STREAM_RECORD_SCHEMA_V1,
    STREAM_ROUTING_DECISION_SCHEMA_V1, SessionContext, SessionContextSourceCut, SessionEntry,
    SessionEntryId, SessionEntryKind, SessionStore, SqliteSessionStore, StreamAckClass,
    StreamAppendAckV1, StreamBackendCapabilitiesV1, StreamBackendKindV1, StreamCursorV1,
    StreamRecordEnvelopeV1, StreamRouteProfile, StreamRoutingDecisionV1, StreamRoutingKeysV1,
    StreamStorageScopeV1, ThinkingMetadata, ThinkingProvider, ThreadBaseRef, ThreadForkReason,
    ThreadJoinedPayload, ThreadReloadDegradedPayload, ThreadSpawnRequestedPayload,
    ThreadSpawnedForkPayload, ThreadSpawnedForkSourceCutPayload, ThreadSpawnedPayload,
    ThreadTerminalState, TimerFiredPayload, stream_schema_registry_v1,
    validate_context_payload_schema_v1,
};
pub use kernel::mandate_lifecycle::{
    ActiveMandate, MIN_MANDATE_INTERVAL_MS, MandateRevokeReceipt, MandateRevokeStatus,
    MandateStartReceipt, MandateStartRequest, list_active_mandates, parse_mandate_event_id,
    revoke_mandate, start_mandate, validate_mandate_start_request,
};
pub use kernel::provider_store::{
    LlmProviderAuthConfig, LlmProviderAuthContext, LlmProviderAuthSourceKind,
    LlmProviderAuthStatus, LlmProviderAuthStore, LlmProviderCatalogStore, LlmProviderConfigValue,
    LlmProviderCredential, LlmProviderInputModality, LlmProviderModelRecord, LlmProviderRecord,
    LlmProviderResolvedAuth, LlmProviderStoreError, LlmProviderStoreResult, MetadataStoreError,
    MetadataStoreResult, OPENAI_COMPATIBLE_BASE_URL, OPENAI_COMPATIBLE_DEFAULT_MODEL,
    OPENAI_COMPATIBLE_EXAMPLE_HEADER, OPENAI_COMPATIBLE_PROVIDER_ID, SqliteLlmProviderStore,
    SqliteMetadataStore, ThreadMetadataStore, default_openai_compatible_llm_provider_record,
    llm_provider_auth_status, resolve_llm_provider_auth, seed_default_llm_providers,
    seed_openai_compatible_llm_provider,
};
pub use kernel::runtime_host::{
    AgentProcessCheckpointReceipt, AgentProcessChildRef, AgentProcessChildrenReceipt,
    AgentProcessLifecycleReceipt, AgentProcessSpawnReceipt, AgentProcessStatusReceipt,
    AgentProcessSubmitReceipt, AgentProcessWaitReceipt, AgentRuntime, AgentRuntimeFactory,
    ProcessHandleIngressSink, RuntimeApprovalDecision, RuntimeEvent, RuntimeEventId,
    RuntimeEventKind, RuntimeExecutionPolicy, RuntimeHost, RuntimeHostLifecycleSnapshot,
    RuntimeHostSnapshot, RuntimeKernelControl, RuntimeModelRequestErrorClass,
    RuntimeModelRequestMode, RuntimeModelRequestPurpose, RuntimePermissionDecision,
    RuntimeServices, RuntimeTerminalState, RuntimeThreadHandle, RuntimeToolLogLevel, RuntimeUsage,
    THREAD_AGENT_MANIFEST_HASH_METADATA, THREAD_BOUND_COUPLING_SET_METADATA,
    THREAD_OPERATION_REGISTRY_ROOT_METADATA, THREAD_SPAWN_GRANTED_METADATA,
    THREAD_SPAWN_INPUTS_HASH_METADATA, ThreadCheckpoint, ThreadCheckpointId,
    ThreadCheckpointLineage, ThreadCommand, ThreadContext, ThreadCoordinates, ThreadEvent,
    ThreadId, ThreadInitiationSource, ThreadInteractionKind, ThreadLifecycleRecord,
    ThreadLifecycleSink, ThreadLifecycleStatus, ThreadLineage, ThreadScope, ThreadSignal,
    ThreadSignalId, ThreadSignalKind, ThreadSpawnAttribution, ThreadSpawnWitness, ThreadStatus,
    ThreadTopology, TurnBudget, TurnContent, TurnContext, TurnContextSnapshot, TurnInput,
    TurnSubmissionMode, VerletError, VerletResult, emit_runtime_event,
};
pub use kernel::secret_store::{
    ManifestSecretResolution, RedactedSecretValue, ResolvedSecret, SecretResolver,
    SecretSourceKind, SecretStatus, SecretStoreError, SecretStoreResult, SqliteSecretStore,
    required_secret_names, resolve_manifest_secret_resolution, resolve_manifest_secrets,
    validate_secret_name,
};
pub use kernel::stdlib_couplings::{
    STD_CONTEXT_SPILL_TEMPLATE_ID, STD_CONTEXT_SUMMARIZE_TEMPLATE_ID,
    STD_CONTEXT_TRUNCATE_TEMPLATE_ID, STD_FAILURE_DEADLETTER_TEMPLATE_ID,
    STD_MEMORY_EXTRACT_TEMPLATE_ID, STD_MEMORY_RECALL_TEMPLATE_ID,
    STD_PERMISSION_APPROVAL_GATE_TEMPLATE_ID, STD_PERMISSION_TOOL_GATE_TEMPLATE_ID,
    STD_PROMPT_DYNAMIC_INSTRUCTIONS_TEMPLATE_ID, STD_PROMPT_STEER_TEMPLATE_ID,
    STD_QUEUE_COMPLETION_CALLBACK_TEMPLATE_ID, STD_QUEUE_TASK_TEMPLATE_ID,
    STD_RETRY_WITH_BUDGET_TEMPLATE_ID, STD_SCHEDULE_CRON_TEMPLATE_ID,
    STD_SUPERVISOR_CHILD_COMPLETION_TEMPLATE_ID, STD_SUPERVISOR_SPAWN_TEMPLATE_ID,
    StdlibCouplingExecutor,
};
pub use kernel::supervisor::{
    SessionSnapshot, SupervisorLifecycleSnapshot, SupervisorSnapshot, TenantLifecycleSnapshot,
    TenantRegistration, TenantRuntimeConfig, TenantRuntimeContext, TenantRuntimeContextDescriptor,
    TenantSnapshot, ThreadStartRequest, VerletSupervisor,
};
pub use kernel::thread_spawn_projector::{
    ThreadSpawnDispatchFold, ThreadSpawnDispatchReceipt, ThreadSpawnProjected,
    ThreadSpawnProjectionFailure, ThreadSpawnProjectionReceipt, ThreadSpawnProjector,
    ThreadTaskNameResolutionReceipt, fold_thread_spawn_dispatch, fold_thread_task_name_resolution,
};
pub use kernel::wasm_couplings::WasmCouplingExecutor;
pub use operations::kernel_packages::{
    CHANNEL_EMIT_CAPABILITY, CHANNEL_EMIT_OPERATION, KERNEL_RUNTIME_KIND, MANDATE_LIST_OPERATION,
    MANDATE_REVOKE_OPERATION, MANDATE_START_OPERATION, NOTIFY_PREVIEW_CAPABILITY,
    NOTIFY_PREVIEW_OPERATION, OPERATION_METADATA_RUNTIME_KIND, PROCESS_CONTROL_CAPABILITY,
    PROCESS_EXEC_OPERATION, PROCESS_POLL_OPERATION, PROCESS_READ_CAPABILITY,
    PROCESS_SPAWN_CAPABILITY, PROCESS_TERMINATE_OPERATION, PROCESS_WRITE_CAPABILITY,
    PROCESS_WRITE_OPERATION, SCHEDULE_MANAGE_CAPABILITY, SCHEDULE_READ_CAPABILITY,
    THREAD_CANCEL_OPERATION, THREAD_SPAWN_OPERATION, THREAD_STATUS_OPERATION,
    THREAD_SUBMIT_OPERATION, THREAD_WAIT_OPERATION, THREADS_CONTROL_CAPABILITY,
    THREADS_READ_CAPABILITY, THREADS_SPAWN_CAPABILITY, VERLET_NOTIFY_PACKAGE,
    VERLET_PROCESS_PACKAGE, VERLET_SCHEDULE_PACKAGE, VERLET_THREADS_PACKAGE,
    ensure_verlet_notify_published, ensure_verlet_process_published,
    ensure_verlet_schedule_published, ensure_verlet_threads_published,
    verlet_notify_kernel_package, verlet_process_kernel_package, verlet_schedule_kernel_package,
    verlet_threads_kernel_package,
};
pub use operations::openapi_import::render_openapi_import_artifact;
pub use operations::operation_builder::{
    RustWasmBuildOptions, RustWasmBuildOutput, build_rust_wasm_module,
};
pub use operations::operation_registry::{
    KernelOperationDispatcher, KernelOperationRegistration, OperationCliProjection,
    OperationHttpProjection, OperationLlmToolProjection, OperationMcpProjection,
    OperationProcessProjection, OperationProjection, OperationProjectionSet, OperationRegistration,
    OperationRegistry, RegisteredOperation,
};
pub use operations::operation_store::{
    CapsuleBindingRecord, CapsuleBindingResolutionRequest, CapsuleBindingScope,
    CapsuleBindingSnapshot, CapsuleBindingTarget, LocalOperationRegistry, OperationBlobStore,
    PublishInterfaceOperationRequest, PublishOperationRequest, PublishedOperationBuild,
    PublishedOperationRecord, PublishedOperationSource, validate_record_name, wasm_sha256,
};
pub use operations::plugins::{
    LocalPluginCatalog, LocalPluginCatalogConfig, LocalPluginCatalogRecord, PluginMount,
};
pub use operations::skill_import::{PublishedSkillImport, SkillImportAsset, SkillImportPlan};
pub use operations::skill_package::{
    DeclaredSkillPackageRef, LocalSkillRegistry, PublishSkillPackageRequest,
    PublishedSkillPackageRecord, SkillPackage, SkillPackageEntry, SkillPackageRef,
};
pub use operations::tool_package::{
    TOOL_BUILD_RECEIPT_KIND, TOOL_BUILD_RECEIPT_SCHEMA_VERSION, TOOL_PACKAGE_KIND,
    TOOL_PACKAGE_SCHEMA_VERSION, ToolBuildReceipt, ToolCommandContract, ToolFixtureContract,
    ToolFixtureDeclaration, ToolFixtureRun, ToolInterfaceContract, ToolManualExample,
    ToolManualExitStatus, ToolMcpContract, ToolOperationBuild, ToolOperationDeclaration,
    ToolOperationInterface, ToolOperationManual, ToolPackageIdentity, ToolPackageManifest,
    ToolPackageSource, ToolRuntimeContract,
};
pub use verlet_operations::{
    IMPORT_BUILD_RECEIPT_KIND, IMPORT_BUILD_RECEIPT_SCHEMA_VERSION, IMPORT_PACKAGE_FILE_NAME,
    ImportAuthDeclaration, ImportBuildReceipt, ImportOperationBuild, ImportOperationDeclaration,
    ImportPackageIdentity, ImportPackageManifest, ImportPackageSource, ImportSpecDeclaration,
    ImportedOperationPlan, LocalBlobRegistry, OpenApiImportError, OperationImportPlan,
    OperationParameterLocation, OperationParameterPlan, OperationRequestBodyPlan,
    OperationSecretHeaderPlan, PublishedBlobRecord, blob_hash_from_ref, blob_ref_uri,
};
