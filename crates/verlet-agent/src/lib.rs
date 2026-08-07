pub mod contracts;
pub mod manifest_schema;
pub mod tool_ref;

pub use contracts::{
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
    ThreadTopologyDeclaration, sha256_hex,
};
pub use manifest_schema::{
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
    KERNEL_ASSEMBLER_ANCHORED_WINDOW, KERNEL_ASSEMBLER_RECORD_SELECT, KERNEL_ASSEMBLER_STATIC,
    RESERVED_MANIFEST_SECTIONS, RESERVED_RESOURCE_KINDS, default_context_pipeline,
    validate_namespace, validate_version,
};
pub use tool_ref::PinnedToolRef;

pub type VerletResult<T> = Result<T, VerletAgentError>;

#[derive(Debug, thiserror::Error)]
pub enum VerletAgentError {
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error(transparent)]
    Operations(#[from] verlet_operations::VerletOperationsError),
}
