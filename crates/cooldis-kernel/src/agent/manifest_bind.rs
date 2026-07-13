//! Bind layer: from a published agent manifest record to an effective,
//! receipted thread configuration.
//!
//! Compile and bind are separate receipted acts. Compile re-parses the
//! published record's resolved manifest into the typed schema and rides a
//! discharged `manifest.compile.completed` event; bind enforces the schema
//! against the live runtime surface (provider records, operation registry,
//! grants, runtime defaults plus caller overrides) and rides a discharged
//! `manifest.bind.completed` event. Both fail closed: a thread either starts
//! with a fully resolved, receipted configuration or it does not start.

use crate::agent::manifest::{AgentAliasResolutionReceipt, PublishedAgentRecord};
use crate::agent::manifest_schema::{
    AgentManifestBudgetRest, AgentManifestBudgetShare, AgentManifestContextPipeline,
    AgentManifestCoupling, AgentManifestCouplingBudget, AgentManifestCouplingQuota,
    AgentManifestCouplingSelector, AgentManifestCouplingSink, AgentManifestModelProfile,
    AgentManifestProtocolToolImport, AgentManifestResource, AgentManifestResourceKind,
    AgentManifestRuntimeDefaults, AgentManifestRuntimeOverrideKey, AgentManifestSchema,
    AgentManifestTool, AgentManifestToolSurface, KERNEL_ASSEMBLER_STATIC,
};
use crate::agent::tool_universe::{
    PinnedToolRef, ToolUniverseBindReceipt, ToolUniverseBinding, ToolUniverseDiscoverer,
};
use crate::kernel::control_decision::PlacementTarget;
use crate::kernel::coupling_executor_registry::{
    RegisteredCouplingExecutorKind, registered_coupling_executor_for_id,
};
use crate::{
    COOLDIS_THREADS_PACKAGE, CooldisError, CooldisResult, EventKind, LlmProviderRecord,
    LocalBlobRegistry, LocalOperationRegistry, LocalSkillRegistry, ProviderCapabilityRecord,
    PublishedOperationSource, SkillPackageRef, THREADS_SPAWN_CAPABILITY,
};
use cooldis_abi::{
    COUPLING_DISCHARGE_ABI, COUPLING_INVOCATION_ABI, WasmOperationDefinition,
    WasmOperationValueKind,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// `discharged_by` coupling names and `function` versions for the two
/// manifest receipts, mirroring `projection:context-compiler` /
/// `naive_assembly/v1` on context compile receipts.
pub const MANIFEST_COMPILER_DISCHARGED_BY: &str = "projection:manifest-compiler";
pub const MANIFEST_COMPILER_FUNCTION: &str = "manifest_schema/v1";
pub const MANIFEST_BINDER_DISCHARGED_BY: &str = "binder:manifest";
pub const MANIFEST_BINDER_FUNCTION: &str = "bind/v1";
pub const THREAD_AGENT_SKILL_PACKAGES_METADATA: &str = "cooldis.agent.skill_packages";
pub const THREAD_AGENT_SKILL_CONTEXT_SEGMENTS_METADATA: &str =
    "cooldis.agent.skill_context_segments";
pub const THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA: &str =
    "cooldis.agent.static_context_segments";

/// Caller-supplied runtime overrides at `thread/start`. Every populated
/// field is checked against the manifest's override allowlist
/// (`AgentManifestRuntimeOverridePolicy`); a non-allowlisted override fails
/// the bind, it is never silently ignored.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestBindOverrides {
    #[serde(default, alias = "defaultCwd")]
    pub default_cwd: Option<String>,
    #[serde(default, alias = "streaming")]
    pub streaming: Option<bool>,
    #[serde(default, alias = "turnTimeoutMs")]
    pub turn_timeout_ms: Option<u64>,
    #[serde(default, alias = "cancellationGraceMs")]
    pub cancellation_grace_ms: Option<u64>,
    #[serde(
        default,
        alias = "compactionAutoAtTextBytes",
        alias = "compaction.auto_at_text_bytes"
    )]
    pub compaction_auto_at_text_bytes: Option<u64>,
}

impl AgentManifestBindOverrides {
    pub fn is_empty(&self) -> bool {
        self.default_cwd.is_none()
            && self.streaming.is_none()
            && self.turn_timeout_ms.is_none()
            && self.cancellation_grace_ms.is_none()
            && self.compaction_auto_at_text_bytes.is_none()
    }
}

/// Provider/model ids the app-server can truthfully bind for a manifest run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentManifestProviderSurface {
    pub provider_id: String,
    pub model_ids: BTreeSet<String>,
    pub supports_streaming: bool,
}

impl AgentManifestProviderSurface {
    /// A provider surface with one configured model id.
    pub fn single(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            provider_id: provider_id.into(),
            model_ids: BTreeSet::from([model_id.into()]),
            supports_streaming: true,
        }
    }

    pub fn with_supports_streaming(mut self, supports_streaming: bool) -> Self {
        self.supports_streaming = supports_streaming;
        self
    }

    /// A catalog-backed provider surface from the stored provider record.
    pub fn from_provider_record(record: &LlmProviderRecord) -> Self {
        Self {
            provider_id: record.provider_id.clone(),
            model_ids: record
                .models
                .iter()
                .map(|model| model.model_id.clone())
                .collect(),
            supports_streaming: ProviderCapabilityRecord::for_api(record.api.clone())
                .supports_streaming,
        }
    }
}

/// Optional thread-start selector for choosing among profiles the manifest
/// already declares. It never creates a new provider/model universe.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AgentManifestModelProfileSelection {
    pub profile_id: Option<String>,
    pub provider_id: Option<String>,
    pub model_id: Option<String>,
}

impl AgentManifestModelProfileSelection {
    pub fn from_provider_model(provider_id: Option<String>, model_id: Option<String>) -> Self {
        Self {
            profile_id: None,
            provider_id,
            model_id,
        }
    }

    pub fn profile_id(profile_id: impl Into<String>) -> Self {
        Self {
            profile_id: Some(profile_id.into()),
            provider_id: None,
            model_id: None,
        }
    }

    fn is_empty(&self) -> bool {
        self.profile_id.is_none() && self.provider_id.is_none() && self.model_id.is_none()
    }
}

/// Result of compiling and binding a published manifest for one thread.
#[derive(Clone, Debug, PartialEq)]
pub struct AgentManifestBoundThread {
    pub manifest: AgentManifestSchema,
    pub compile_receipt: AgentManifestCompileReceipt,
    pub bind_receipt: AgentManifestBindReceipt,
    pub coupling_set: BoundCouplingSet,
    pub couplings: Vec<BoundCoupling>,
    pub operation_names: Vec<String>,
    pub operation_bindings: Vec<AgentManifestOperationBinding>,
    pub skill_packages: Vec<AgentManifestSkillPackageBinding>,
    pub static_context_segments: Vec<AgentManifestStaticContextSegment>,
    pub skill_context_segments: Vec<AgentManifestStaticContextSegment>,
    /// Witnessed universe bindings for the thread's protocol tool imports;
    /// the runtime factory mounts these as the search surface (plus direct
    /// rows for pins).
    pub tool_universes: Vec<ToolUniverseBinding>,
}

/// Re-parse the immutable published record into the typed manifest schema
/// and build the compile receipt for the thread stream.
pub fn compile_published_agent_record(
    record: &PublishedAgentRecord,
    alias: Option<AgentAliasResolutionReceipt>,
) -> CooldisResult<(AgentManifestSchema, AgentManifestCompileReceipt)> {
    record.validate()?;
    let manifest: AgentManifestSchema = serde_json::from_value(record.resolved_manifest.clone())
        .map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to decode resolved agent manifest {}: {err}",
                record.ref_uri
            ))
        })?;
    manifest.validate()?;
    let receipt = AgentManifestCompileReceipt {
        ref_uri: record.ref_uri.clone(),
        manifest_hash: record.manifest_hash.clone(),
        source_hash: record.source_hash.clone(),
        alias,
    };
    Ok((manifest, receipt))
}

/// Compile and bind a published manifest against the live app-server
/// provider, operation, and MCP source surfaces.
pub async fn bind_published_agent_record(
    record: &PublishedAgentRecord,
    alias: Option<AgentAliasResolutionReceipt>,
    provider_surface: &AgentManifestProviderSurface,
    operation_registry_root: Option<&Path>,
    blob_registry_root: Option<&Path>,
    skill_registry_root: Option<&Path>,
    configured_mcp_server_refs: &BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn ToolUniverseDiscoverer>,
    model_selection: &AgentManifestModelProfileSelection,
    overrides: &AgentManifestBindOverrides,
) -> CooldisResult<AgentManifestBoundThread> {
    let (manifest, compile_receipt) = compile_published_agent_record(record, alias)?;
    let selected = select_manifest_model_profile(&manifest, model_selection)?;
    let profile = selected.profile;
    let provider_id = selected.provider_id;
    if provider_id != provider_surface.provider_id {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest provider_ref {:?} is not configured; available provider is {:?}",
            profile.provider_ref, provider_surface.provider_id
        )));
    }
    let model_id = selected.model_id;
    if !provider_surface.model_ids.contains(&model_id) {
        let available = provider_surface
            .model_ids
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest model_ref {:?} is not configured for provider {:?}; available models: {}",
            profile.model_ref, provider_id, available
        )));
    }

    let (effective_runtime, overridden_keys) =
        apply_runtime_overrides(&manifest.runtime, overrides)?;
    if effective_runtime.streaming && !provider_surface.supports_streaming {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest runtime.streaming requires provider {:?} to support streaming",
            provider_surface.provider_id
        )));
    }
    let bound_tools = bind_tools(
        &manifest.tools,
        operation_registry_root,
        configured_mcp_server_refs,
        tool_universe_discoverer,
    )
    .await?;
    let static_context_segments = bind_static_context_sources(&manifest, blob_registry_root)?;
    let bound_skills = bind_skill_resources(&manifest.resources, skill_registry_root)?;
    let couplings = bind_couplings(&manifest.couplings, operation_registry_root)?;
    enforce_child_agent_policy(&manifest, &bound_tools.operation_bindings, &couplings)?;
    let operation_names = bound_tools
        .operation_bindings
        .iter()
        .map(|binding| binding.name.clone())
        .collect::<Vec<_>>();
    let coupling_set = BoundCouplingSet {
        snapshot_id: record.manifest_hash.clone(),
        couplings: couplings.clone(),
    };
    let coupling_bindings = coupling_set
        .couplings
        .iter()
        .map(AgentManifestCouplingBinding::from_bound)
        .collect::<Vec<_>>();
    let bind_receipt = AgentManifestBindReceipt {
        ref_uri: record.ref_uri.clone(),
        manifest_hash: record.manifest_hash.clone(),
        model_profile_id: profile.id.clone(),
        provider_id,
        model_id,
        tool_ids: bound_tools.tool_ids,
        operation_bindings: bound_tools.operation_bindings.clone(),
        skill_packages: bound_skills.package_bindings.clone(),
        static_context_segments: static_context_segments.clone(),
        tool_universes: bound_tools
            .tool_universes
            .iter()
            .map(ToolUniverseBindReceipt::from_binding)
            .collect(),
        couplings: coupling_bindings,
        granted: bound_tools.granted,
        effective_runtime,
        overridden_keys,
        // Placement resolution lands with the ADR 0006 implementation
        // ticket; until then every bind is local and the field stays absent.
        placement: None,
    };
    Ok(AgentManifestBoundThread {
        manifest,
        compile_receipt,
        bind_receipt,
        coupling_set,
        couplings,
        operation_names,
        operation_bindings: bound_tools.operation_bindings,
        skill_packages: bound_skills.package_bindings,
        static_context_segments,
        skill_context_segments: bound_skills.context_segments,
        tool_universes: bound_tools.tool_universes,
    })
}

#[derive(Clone, Debug)]
struct OperationRef {
    name: String,
    operation: Option<String>,
    artifact_hash: Option<String>,
}

struct BoundTools {
    tool_ids: Vec<String>,
    granted: Vec<String>,
    operation_bindings: Vec<AgentManifestOperationBinding>,
    tool_universes: Vec<ToolUniverseBinding>,
}

struct BoundSkills {
    package_bindings: Vec<AgentManifestSkillPackageBinding>,
    context_segments: Vec<AgentManifestStaticContextSegment>,
}

fn bind_static_context_sources(
    manifest: &AgentManifestSchema,
    blob_registry_root: Option<&Path>,
) -> CooldisResult<Vec<AgentManifestStaticContextSegment>> {
    let pipeline = manifest.effective_context_pipeline();
    let mut segments = Vec::new();
    for source in &pipeline.sources {
        if source.assembler != KERNEL_ASSEMBLER_STATIC {
            continue;
        }
        let Some(input) = source
            .input
            .as_deref()
            .filter(|input| !input.trim().is_empty())
        else {
            continue;
        };
        let Some(resource) = static_source_resource(input, &manifest.resources)? else {
            continue;
        };
        if resource.kind != AgentManifestResourceKind::Blob {
            continue;
        }
        let registry_root = blob_registry_root.ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "blob resource {:?} ref {:?} requires an app-server blob registry root",
                resource.name, resource.reference
            ))
        })?;
        let registry = LocalBlobRegistry::new(registry_root);
        let (record, content) = registry
            .load_text_ref(&resource.reference)
            .map_err(|err| missing_blob_resource_error(resource, err))?;
        segments.push(AgentManifestStaticContextSegment {
            id: source.id.clone(),
            assembler: source.assembler.clone(),
            input: input.to_string(),
            pinned: source.pinned,
            budget_share: static_source_budget_share(&pipeline, source.id.as_str()),
            ref_uri: record.ref_uri,
            content_sha256: record.content_sha256,
            content,
        });
    }
    Ok(segments)
}

fn static_source_resource<'a>(
    input: &str,
    resources: &'a [AgentManifestResource],
) -> CooldisResult<Option<&'a AgentManifestResource>> {
    if input.starts_with("resource://") || input.starts_with("skill://") {
        return resources
            .iter()
            .find(|resource| resource.reference == input)
            .map(Some)
            .ok_or_else(|| {
                CooldisError::RuntimeFactory(format!(
                    "static context source input {input:?} does not match a declared resource ref"
                ))
            });
    }
    resources
        .iter()
        .find(|resource| resource.name == input)
        .map(Some)
        .ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "static context source input {input:?} does not name a declared resource"
            ))
        })
}

fn missing_blob_resource_error(
    resource: &AgentManifestResource,
    err: impl std::fmt::Display,
) -> CooldisError {
    CooldisError::RuntimeFactory(format!(
        "blob resource {:?} ref {:?} was not found in the local blob registry: {err}; run `cooldis blob publish <file>` and use the returned resource://artifact/sha256:<hash> ref",
        resource.name, resource.reference
    ))
}

fn static_source_budget_share(
    pipeline: &AgentManifestContextPipeline,
    source_id: &str,
) -> Option<f64> {
    pipeline
        .sources
        .iter()
        .find(|source| source.id == source_id)
        .and_then(|source| match source.budget_share {
            Some(AgentManifestBudgetShare::Fraction(value)) => Some(value),
            Some(AgentManifestBudgetShare::Rest(AgentManifestBudgetRest::Rest)) | None => None,
        })
}

/// Role inferred from the coupling's resolved sink relation. Manifest
/// authors cannot choose this directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CouplingRole {
    Projection,
    Controller,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct BoundCouplingSet {
    pub snapshot_id: String,
    pub couplings: Vec<BoundCoupling>,
}

impl BoundCouplingSet {
    pub fn new(snapshot_id: impl Into<String>, couplings: Vec<BoundCoupling>) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            couplings,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundCoupling {
    pub id: String,
    pub role: CouplingRole,
    pub trigger_kind: EventKind,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trigger_match: BTreeMap<String, JsonValue>,
    pub trigger_quota: AgentManifestCouplingQuota,
    pub source_selectors: Vec<BoundCouplingSelector>,
    pub sink: BoundCouplingSink,
    pub function_ref: String,
    pub function: BoundCouplingFunction,
    pub grants: Vec<String>,
    pub budget: AgentManifestCouplingBudget,
    pub config: JsonValue,
    pub config_hash: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundCouplingSelector {
    pub stream: String,
    pub kinds: Vec<EventKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundCouplingSink {
    pub stream: String,
    pub kinds: Vec<EventKind>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BoundCouplingFunction {
    pub name: String,
    pub artifact_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct OperationBindingAccumulator {
    grants: BTreeSet<String>,
    operations: BTreeSet<String>,
    direct_tools: BTreeSet<AgentManifestDirectToolBinding>,
    whole_record: bool,
}

impl OperationBindingAccumulator {
    fn merge(
        &mut self,
        grants: BTreeSet<String>,
        operation: Option<String>,
        direct_tool: Option<AgentManifestDirectToolBinding>,
    ) {
        self.grants.extend(grants);
        if let Some(direct_tool) = direct_tool {
            self.direct_tools.insert(direct_tool);
        }
        match operation {
            Some(operation) if !self.whole_record => {
                self.operations.insert(operation);
            }
            Some(_) => {}
            None => {
                self.whole_record = true;
                self.operations.clear();
            }
        }
    }

    fn operation_names(&self) -> Vec<String> {
        if self.whole_record {
            Vec::new()
        } else {
            self.operations.iter().cloned().collect()
        }
    }
}

type OperationBindingMap = BTreeMap<(String, String), OperationBindingAccumulator>;

fn bind_skill_resources(
    resources: &[AgentManifestResource],
    skill_registry_root: Option<&Path>,
) -> CooldisResult<BoundSkills> {
    let skill_resources = resources
        .iter()
        .filter(|resource| resource.kind == AgentManifestResourceKind::Skill)
        .collect::<Vec<_>>();
    if skill_resources.is_empty() {
        return Ok(BoundSkills {
            package_bindings: Vec::new(),
            context_segments: Vec::new(),
        });
    }
    let registry_root = skill_registry_root.ok_or_else(|| {
        CooldisError::RuntimeFactory(
            "skill resources require an app-server skill registry root".to_string(),
        )
    })?;
    let registry = LocalSkillRegistry::new(registry_root);
    let mut package_bindings = Vec::new();
    let mut context_segments = Vec::new();
    let mut mounted_skill_names = BTreeSet::new();
    for resource in skill_resources {
        let parsed = SkillPackageRef::parse(&resource.reference).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "skill resource {:?} ref {:?} is invalid: {err}",
                resource.name, resource.reference
            ))
        })?;
        let record = registry
            .load_version_record(&parsed.name, &parsed.artifact_hash)
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "skill resource {:?} ref {:?} was not found in the local skill registry: {err}; publish the skill package or replace the ref with a hash from the registry",
                    resource.name, resource.reference
                ))
            })?;
        for skill in &record.package.skills {
            if !mounted_skill_names.insert(skill.name.clone()) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "skill resource {:?} package {:?} would mount duplicate /skills/{}.md; skill names must be unique across bound packages",
                    resource.name, record.name, skill.name
                )));
            }
        }
        let index = record.package.render_index();
        let index_sha256 = sha256_prefixed(index.as_bytes());
        let ref_uri = record.ref_uri();
        context_segments.push(AgentManifestStaticContextSegment {
            id: format!("skill-index:{}", resource.name),
            assembler: KERNEL_ASSEMBLER_STATIC.to_string(),
            input: resource.name.clone(),
            pinned: true,
            budget_share: None,
            ref_uri: ref_uri.clone(),
            content_sha256: index_sha256.clone(),
            content: index,
        });
        package_bindings.push(AgentManifestSkillPackageBinding {
            resource_name: resource.name.clone(),
            package_name: record.name.clone(),
            ref_uri,
            artifact_hash: record.active_artifact_hash.clone(),
            package_digest: format!("sha256:{}", parsed.artifact_hash),
            skill_count: record.package.skills.len(),
            index_sha256,
        });
    }
    Ok(BoundSkills {
        package_bindings,
        context_segments,
    })
}

fn bind_couplings(
    couplings: &[AgentManifestCoupling],
    operation_registry_root: Option<&Path>,
) -> CooldisResult<Vec<BoundCoupling>> {
    couplings
        .iter()
        .map(|coupling| bind_coupling(coupling, operation_registry_root))
        .collect()
}

fn bind_coupling(
    coupling: &AgentManifestCoupling,
    operation_registry_root: Option<&Path>,
) -> CooldisResult<BoundCoupling> {
    let executor_kind = registered_coupling_executor_for_id(&coupling.id).ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "no registered executor for coupling id {:?}",
            coupling.id
        ))
    })?;
    if executor_kind == RegisteredCouplingExecutorKind::Wasm
        && !coupling.function_ref.starts_with("op://")
    {
        return Err(CooldisError::RuntimeFactory(format!(
            "custom coupling {:?} function_ref {:?} must be an op:// Wasm operation ref",
            coupling.id, coupling.function_ref
        )));
    }
    let registry_root = operation_registry_root.ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "coupling {:?} function_ref {:?} requires an app-server operation registry root",
            coupling.id, coupling.function_ref
        ))
    })?;
    let trigger_kind =
        parse_coupling_event_kind(&coupling.id, "trigger kind", &coupling.trigger.kind)?;
    let source_selectors = coupling
        .source
        .selectors
        .iter()
        .map(|selector| bind_coupling_source_selector(&coupling.id, selector))
        .collect::<CooldisResult<Vec<_>>>()?;
    let source_streams = source_selectors
        .iter()
        .map(|selector| selector.stream.clone())
        .collect::<BTreeSet<_>>();
    let sink = bind_coupling_sink(&coupling.id, &coupling.sink)?;
    if source_streams.contains(&sink.stream) {
        return Err(CooldisError::RuntimeFactory(format!(
            "coupling {:?} sink must not equal selected source stream {:?}",
            coupling.id, sink.stream
        )));
    }
    let role = if sink.stream == "control" {
        CouplingRole::Controller
    } else {
        CouplingRole::Projection
    };
    let verification = verify_operation_ref_for_subject(
        "coupling",
        &coupling.id,
        &coupling.function_ref,
        &coupling.grants,
        registry_root,
    )?;
    let operation_name = match executor_kind {
        RegisteredCouplingExecutorKind::Stdlib => verification.operation.clone(),
        RegisteredCouplingExecutorKind::Wasm => {
            wasm_coupling_operation_name(&coupling.id, &coupling.function_ref, &verification)?
        }
    };
    let config_hash = coupling_config_hash(&coupling.config)?;
    Ok(BoundCoupling {
        id: coupling.id.clone(),
        role,
        trigger_kind,
        trigger_match: coupling.trigger.match_fields.clone(),
        trigger_quota: coupling.trigger.quota.clone(),
        source_selectors,
        sink,
        function_ref: coupling.function_ref.clone(),
        function: BoundCouplingFunction {
            name: verification.name,
            artifact_hash: verification.artifact_hash,
            operation_name,
        },
        grants: verification.grants.into_iter().collect(),
        budget: coupling.budget.clone(),
        config: coupling.config.clone(),
        config_hash,
    })
}

fn wasm_coupling_operation_name(
    coupling_id: &str,
    function_ref: &str,
    verification: &VerifiedOperationRef,
) -> CooldisResult<Option<String>> {
    if !matches!(
        verification.record.source,
        PublishedOperationSource::Wasm { .. }
    ) {
        return Err(CooldisError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} must resolve to a Wasm operation record"
        )));
    }
    let operation = selected_wasm_coupling_operation(coupling_id, function_ref, verification)?;
    if operation.input != WasmOperationValueKind::Json {
        return Err(CooldisError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} operation {:?} must declare json input for {COUPLING_INVOCATION_ABI}",
            operation.name
        )));
    }
    if operation.output != WasmOperationValueKind::Json {
        return Err(CooldisError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} operation {:?} must declare json output for {COUPLING_DISCHARGE_ABI}",
            operation.name
        )));
    }
    if !operation.required_capabilities.is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} operation {:?} declares effect capabilities; couplings are pure compute and must use config, selected events, and stream grants only",
            operation.name
        )));
    }
    Ok(Some(operation.name.clone()))
}

fn selected_wasm_coupling_operation<'a>(
    coupling_id: &str,
    function_ref: &str,
    verification: &'a VerifiedOperationRef,
) -> CooldisResult<&'a WasmOperationDefinition> {
    if let Some(operation_name) = verification.operation.as_deref() {
        return verification
            .record
            .manifest
            .operation(operation_name)
            .ok_or_else(|| {
                unknown_operation_ref_error(
                    "coupling",
                    coupling_id,
                    function_ref,
                    &verification.record.name,
                    &verification.record,
                )
            });
    }
    if verification.record.manifest.operations.len() == 1 {
        return Ok(&verification.record.manifest.operations[0]);
    }
    Err(CooldisError::RuntimeFactory(format!(
        "custom coupling {coupling_id:?} function_ref {function_ref:?} must select one operation with op://<record>/<operation>@sha256:<hash>"
    )))
}

fn bind_coupling_source_selector(
    coupling_id: &str,
    selector: &AgentManifestCouplingSelector,
) -> CooldisResult<BoundCouplingSelector> {
    let kinds = selector
        .kind
        .iter()
        .map(|kind| parse_coupling_event_kind(coupling_id, "source kind", kind))
        .collect::<CooldisResult<Vec<_>>>()?;
    Ok(BoundCouplingSelector {
        stream: selector.stream.clone(),
        kinds,
        scope: selector.scope.clone(),
        since: selector.since.clone(),
    })
}

fn bind_coupling_sink(
    coupling_id: &str,
    sink: &AgentManifestCouplingSink,
) -> CooldisResult<BoundCouplingSink> {
    let kinds = sink
        .kind
        .iter()
        .map(|kind| parse_coupling_event_kind(coupling_id, "sink kind", kind))
        .collect::<CooldisResult<Vec<_>>>()?;
    Ok(BoundCouplingSink {
        stream: sink.stream.clone(),
        kinds,
    })
}

fn parse_coupling_event_kind(
    coupling_id: &str,
    label: &str,
    value: &str,
) -> CooldisResult<EventKind> {
    value.parse::<EventKind>().map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "coupling {coupling_id:?} {label} {value:?} is not in the kernel event-kind vocabulary: {err}"
        ))
    })
}

pub(crate) fn coupling_config_hash(value: &JsonValue) -> CooldisResult<String> {
    canonical_json_hash(value)
}

pub(crate) fn coupling_set_content_hash(coupling_set: &BoundCouplingSet) -> CooldisResult<String> {
    let mut couplings = coupling_set.couplings.iter().collect::<Vec<_>>();
    couplings.sort_by(|left, right| left.id.cmp(&right.id));
    let value = JsonValue::Array(
        couplings
            .into_iter()
            .map(|coupling| {
                serde_json::json!({
                    "id": coupling.id,
                    "function_ref": coupling.function_ref,
                    "config": coupling.config,
                })
            })
            .collect(),
    );
    canonical_json_hash(&value)
}

pub(crate) fn canonical_json_hash(value: &JsonValue) -> CooldisResult<String> {
    let mut canonical = Vec::new();
    write_canonical_json(value, &mut canonical)?;
    let digest = Sha256::digest(&canonical);
    Ok(format!("sha256:{digest:x}"))
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn write_canonical_json(value: &JsonValue, output: &mut Vec<u8>) -> CooldisResult<()> {
    match value {
        JsonValue::Null => output.extend_from_slice(b"null"),
        JsonValue::Bool(true) => output.extend_from_slice(b"true"),
        JsonValue::Bool(false) => output.extend_from_slice(b"false"),
        JsonValue::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        JsonValue::String(string) => serde_json::to_writer(output, string).map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to canonicalize coupling config: {err}"))
        })?,
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(item, output)?;
            }
            output.push(b']');
        }
        JsonValue::Object(object) => {
            output.push(b'{');
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|err| {
                    CooldisError::RuntimeFactory(format!(
                        "failed to canonicalize coupling config: {err}"
                    ))
                })?;
                output.push(b':');
                write_canonical_json(item, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

async fn bind_tools(
    tools: &[AgentManifestTool],
    operation_registry_root: Option<&Path>,
    configured_mcp_server_refs: &BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn ToolUniverseDiscoverer>,
) -> CooldisResult<BoundTools> {
    let mut tool_ids = Vec::new();
    let mut granted = BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();
    let mut direct_tool_names = BTreeSet::new();
    let mut tool_universes = Vec::new();
    for tool in tools {
        match tool {
            AgentManifestTool::Bash(tool) => {
                bind_operation_ref(
                    &tool.id,
                    &tool.operation_ref,
                    &tool.grants,
                    None,
                    operation_registry_root,
                    &mut granted,
                    &mut operation_bindings,
                )
                .await?;
                tool_ids.push(tool.id.clone());
            }
            AgentManifestTool::Direct(tool) => {
                if !direct_tool_names.insert(tool.tool_name.clone()) {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "duplicate direct tool_name surface {:?}",
                        tool.tool_name
                    )));
                }
                bind_operation_ref(
                    &tool.id,
                    &tool.operation_ref,
                    &tool.grants,
                    Some(&tool.tool_name),
                    operation_registry_root,
                    &mut granted,
                    &mut operation_bindings,
                )
                .await?;
                tool_ids.push(tool.id.clone());
            }
            AgentManifestTool::ProtocolImport(tool) => {
                if !configured_mcp_server_refs.contains(&tool.server_ref) {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "protocol tool {:?} server_ref {:?} is not configured",
                        tool.id, tool.server_ref
                    )));
                }
                let binding = bind_protocol_tool_import(tool, tool_universe_discoverer).await?;
                if let Some(pin) = &binding.pin
                    && !direct_tool_names.insert(pin.tool_name.clone())
                {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "duplicate direct tool_name surface {:?}",
                        pin.tool_name
                    )));
                }
                tool_universes.push(binding);
                tool_ids.push(tool.id.clone());
            }
        }
    }
    Ok(BoundTools {
        tool_ids,
        granted: granted.into_iter().collect(),
        operation_bindings: operation_bindings_from_map(operation_bindings),
        tool_universes,
    })
}

fn enforce_child_agent_policy(
    manifest: &AgentManifestSchema,
    operation_bindings: &[AgentManifestOperationBinding],
    couplings: &[BoundCoupling],
) -> CooldisResult<()> {
    if manifest.policies.allow_child_agents {
        return Ok(());
    }
    let tool_declares_thread_spawn = operation_bindings.iter().any(|binding| {
        binding.name == COOLDIS_THREADS_PACKAGE
            && binding
                .grants
                .iter()
                .any(|grant| grant == THREADS_SPAWN_CAPABILITY)
    });
    let coupling_declares_thread_spawn = couplings.iter().any(|coupling| {
        coupling.id == crate::STD_SUPERVISOR_SPAWN_TEMPLATE_ID
            && coupling
                .grants
                .iter()
                .any(|grant| grant == THREADS_SPAWN_CAPABILITY)
    });
    let declares_thread_spawn = tool_declares_thread_spawn || coupling_declares_thread_spawn;
    if declares_thread_spawn {
        return Err(CooldisError::RuntimeFactory(
            "agent manifest policies.allow_child_agents = false but a child-thread operation or supervisor coupling grants threads.spawn; remove thread_spawn/std::supervisor.spawn or set allow_child_agents = true".to_string(),
        ));
    }
    Ok(())
}

struct SelectedManifestModelProfile<'a> {
    profile: &'a AgentManifestModelProfile,
    provider_id: String,
    model_id: String,
}

fn select_manifest_model_profile<'a>(
    manifest: &'a AgentManifestSchema,
    selection: &AgentManifestModelProfileSelection,
) -> CooldisResult<SelectedManifestModelProfile<'a>> {
    if manifest.model_profiles.is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "agent manifest requires at least one model profile".to_string(),
        ));
    }

    let mut matches = Vec::new();
    for profile in &manifest.model_profiles {
        let provider_id = provider_id_from_ref(&profile.provider_ref)?;
        let model_id = model_id_from_ref(&profile.model_ref, &provider_id)?;
        let profile_matches = selection
            .profile_id
            .as_ref()
            .is_none_or(|requested| requested == &profile.id)
            && selection
                .provider_id
                .as_ref()
                .is_none_or(|requested| requested == &provider_id)
            && selection
                .model_id
                .as_ref()
                .is_none_or(|requested| requested == &model_id);
        if selection.is_empty() || profile_matches {
            matches.push(SelectedManifestModelProfile {
                profile,
                provider_id,
                model_id,
            });
            if selection.is_empty() {
                break;
            }
        }
    }

    match matches.len() {
        1 => Ok(matches.remove(0)),
        0 => Err(model_profile_selection_error(
            "no declared model profile matches",
            manifest,
            selection,
        )),
        _ => Err(model_profile_selection_error(
            "ambiguous declared model profile selection",
            manifest,
            selection,
        )),
    }
}

fn model_profile_selection_error(
    reason: &str,
    manifest: &AgentManifestSchema,
    selection: &AgentManifestModelProfileSelection,
) -> CooldisError {
    let requested = model_profile_selection_summary(selection);
    let declared = declared_model_profiles_summary(manifest).unwrap_or_else(|err| err.to_string());
    CooldisError::RuntimeFactory(format!(
        "thread/start {reason} for {requested}; declared model profiles: {declared}"
    ))
}

fn model_profile_selection_summary(selection: &AgentManifestModelProfileSelection) -> String {
    let mut parts = Vec::new();
    if let Some(profile_id) = &selection.profile_id {
        parts.push(format!("profile={profile_id}"));
    }
    if let Some(provider_id) = &selection.provider_id {
        parts.push(format!("provider={provider_id}"));
    }
    if let Some(model_id) = &selection.model_id {
        parts.push(format!("model={model_id}"));
    }
    if parts.is_empty() {
        "default profile".to_string()
    } else {
        parts.join(", ")
    }
}

fn declared_model_profiles_summary(manifest: &AgentManifestSchema) -> CooldisResult<String> {
    manifest
        .model_profiles
        .iter()
        .map(|profile| {
            let provider_id = provider_id_from_ref(&profile.provider_ref)?;
            let model_id = model_id_from_ref(&profile.model_ref, &provider_id)?;
            Ok(format!(
                "id={} provider={} model={}",
                profile.id, provider_id, model_id
            ))
        })
        .collect::<CooldisResult<Vec<_>>>()
        .map(|profiles| profiles.join("; "))
}

fn operation_bindings_from_map(
    operation_bindings: OperationBindingMap,
) -> Vec<AgentManifestOperationBinding> {
    operation_bindings
        .into_iter()
        .map(|((name, artifact_hash), binding)| {
            let operations = binding.operation_names();
            AgentManifestOperationBinding {
                name,
                artifact_hash,
                grants: binding.grants.into_iter().collect(),
                operations,
                direct_tools: binding.direct_tools.into_iter().collect(),
            }
        })
        .collect()
}

/// Bind one protocol tool import: witness the universe's discovery, apply
/// the manifest-level filter, and resolve the pin fail-closed.
///
/// A configured universe with no discovery path is an error, not a silent
/// skip. A pin resolves only when the filtered witnessed contract matches
/// its content address exactly; missing or mismatched contracts are drift.
async fn bind_protocol_tool_import(
    tool: &AgentManifestProtocolToolImport,
    discoverer: Option<&dyn ToolUniverseDiscoverer>,
) -> CooldisResult<ToolUniverseBinding> {
    let discoverer = discoverer.ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "protocol tool import {:?} requires a tool universe discoverer; fail closed",
            tool.id
        ))
    })?;
    let discovery = discoverer.discover(&tool.server_ref).await?;
    if discovery.server_ref != tool.server_ref {
        return Err(CooldisError::RuntimeFactory(format!(
            "protocol tool import {:?} discovery returned server_ref {:?}, expected {:?}; fail closed",
            tool.id, discovery.server_ref, tool.server_ref
        )));
    }
    let include_tools = tool
        .include_tools
        .as_ref()
        .map(|tools| tools.iter().cloned().collect::<BTreeSet<_>>());
    let discovery = match &include_tools {
        Some(include_tools) => discovery.filtered(include_tools)?,
        None => discovery,
    };
    let pin = tool
        .pin
        .as_ref()
        .map(|reference| PinnedToolRef::parse(reference))
        .transpose()?;
    let exposes_direct = tool.expose.contains(&AgentManifestToolSurface::DirectTool);
    match (&pin, exposes_direct) {
        (None, true) => {
            return Err(CooldisError::RuntimeFactory(format!(
                "protocol tool import {:?} declares expose = [\"direct_tool\"] without a pin; fail closed",
                tool.id
            )));
        }
        (Some(_), false) => {
            return Err(CooldisError::RuntimeFactory(format!(
                "protocol tool import {:?} declares a pin without expose = [\"direct_tool\"]; fail closed",
                tool.id
            )));
        }
        _ => {}
    }
    if let Some(pin) = &pin {
        let witnessed = discovery.contract(&pin.tool_name);
        let witnessed_hash = witnessed
            .map(|contract| contract.schema_hash.as_str())
            .unwrap_or("<missing>");
        if !witnessed.is_some_and(|contract| contract.matches_pin(pin)) {
            return Err(CooldisError::RuntimeFactory(format!(
                "protocol tool import {:?} pin drift for {:?}: expected schema hash {}, witnessed {}; fail closed",
                tool.id, pin.tool_name, pin.schema_hash, witnessed_hash
            )));
        }
    }
    let binding = ToolUniverseBinding {
        import_id: tool.id.clone(),
        server_ref: tool.server_ref.clone(),
        include_tools,
        pin,
        discovery,
    };
    binding.validate()?;
    Ok(binding)
}

async fn bind_operation_ref(
    tool_id: &str,
    operation_ref: &str,
    grants: &[String],
    direct_tool_name: Option<&str>,
    operation_registry_root: Option<&Path>,
    granted: &mut BTreeSet<String>,
    operation_bindings: &mut OperationBindingMap,
) -> CooldisResult<()> {
    let registry_root = operation_registry_root.ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "tool {tool_id:?} operation_ref {operation_ref:?} requires an app-server operation registry root"
        ))
    })?;
    let verification = verify_operation_ref(tool_id, operation_ref, grants, registry_root)?;
    let direct_tool_binding = direct_tool_name
        .map(|tool_name| {
            let operation = direct_tool_operation_name(
                tool_id,
                operation_ref,
                verification.operation.as_deref(),
                &verification.record,
            )?;
            Ok::<AgentManifestDirectToolBinding, CooldisError>(AgentManifestDirectToolBinding {
                tool_name: tool_name.to_string(),
                operation,
            })
        })
        .transpose()?;
    granted.extend(verification.grants.iter().cloned());
    operation_bindings
        .entry((verification.name, verification.artifact_hash))
        .or_default()
        .merge(
            verification.grants,
            verification.operation,
            direct_tool_binding,
        );
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedOperationRef {
    pub(crate) name: String,
    pub(crate) artifact_hash: String,
    pub(crate) operation: Option<String>,
    pub(crate) grants: BTreeSet<String>,
    pub(crate) record: crate::PublishedOperationRecord,
}

pub(crate) fn verify_operation_ref(
    tool_id: &str,
    operation_ref: &str,
    grants: &[String],
    operation_registry_root: &Path,
) -> CooldisResult<VerifiedOperationRef> {
    verify_operation_ref_for_subject(
        "tool",
        tool_id,
        operation_ref,
        grants,
        operation_registry_root,
    )
}

fn verify_operation_ref_for_subject(
    subject_kind: &str,
    subject_id: &str,
    operation_ref: &str,
    grants: &[String],
    operation_registry_root: &Path,
) -> CooldisResult<VerifiedOperationRef> {
    let parsed = parse_operation_ref(operation_ref)?;
    let artifact_hash = parsed.artifact_hash.clone().ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} must be content-addressed with @sha256:<hash>; for agent publish, pass --resolve-ops to pin op:// authoring refs from the operations registry"
        ))
    })?;
    let registry = LocalOperationRegistry::new(operation_registry_root);
    registry.load_record(&parsed.name).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} was not found in the local operation registry: {err}; seed the operation registry or fix the op:// record name"
        ))
    })?;
    let record = registry
        .load_version_record(&parsed.name, &artifact_hash)
        .map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} names artifact hash sha256:{artifact_hash} that is not a published version in the local operation registry: {err}; republish the operation or replace the ref with a hash from the registry"
            ))
        })?;
    let granted_set = grants.iter().cloned().collect::<BTreeSet<_>>();
    let operations = if let Some(operation_name) = parsed.operation.as_deref() {
        let operation = record.manifest.operation(operation_name).ok_or_else(|| {
            unknown_operation_ref_error(
                subject_kind,
                subject_id,
                operation_ref,
                &parsed.name,
                &record,
            )
        })?;
        vec![operation]
    } else {
        record.manifest.operations.iter().collect::<Vec<_>>()
    };
    let missing = operations
        .into_iter()
        .flat_map(|operation| {
            operation
                .required_capabilities
                .iter()
                .filter(|capability| !granted_set.contains(capability.as_str()))
                .map(|capability| format!("{}:{capability}", operation.name))
        })
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} requires grants not declared on the {subject_kind} binding: {}",
            missing.join(", ")
        )));
    }
    Ok(VerifiedOperationRef {
        name: parsed.name,
        artifact_hash,
        operation: parsed.operation,
        grants: granted_set,
        record,
    })
}

fn direct_tool_operation_name(
    tool_id: &str,
    operation_ref: &str,
    operation_name: Option<&str>,
    record: &crate::PublishedOperationRecord,
) -> CooldisResult<String> {
    if let Some(operation) = operation_name {
        if record.manifest.operation(operation).is_none() {
            return Err(unknown_operation_ref_error(
                "tool",
                tool_id,
                operation_ref,
                &record.name,
                record,
            ));
        }
        return Ok(operation.to_string());
    }
    if record.manifest.operations.len() == 1 {
        return Ok(record.manifest.operations[0].name.clone());
    }
    Err(CooldisError::RuntimeFactory(format!(
        "direct tool {tool_id:?} operation_ref {operation_ref:?} must select one operation with op://<record>/<operation>@sha256:<hash>"
    )))
}

fn unknown_operation_ref_error(
    subject_kind: &str,
    subject_id: &str,
    operation_ref: &str,
    record_name: &str,
    record: &crate::PublishedOperationRecord,
) -> CooldisError {
    let available = record
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    CooldisError::RuntimeFactory(format!(
        "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} selects an operation that is not in record {record_name:?}; use op://<record>@sha256:<hash> for the whole record or op://<record>/<operation>@sha256:<hash> for one operation; available operations: {}",
        if available.is_empty() {
            "<none>"
        } else {
            &available
        }
    ))
}

fn provider_id_from_ref(provider_ref: &str) -> CooldisResult<String> {
    let id = provider_ref.strip_prefix("provider://").ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "provider_ref {provider_ref:?} must start with provider://"
        ))
    })?;
    if id.is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "provider_ref must include a provider id".to_string(),
        ));
    }
    Ok(id.to_string())
}

fn model_id_from_ref(model_ref: &str, provider_id: &str) -> CooldisResult<String> {
    let id = model_ref.strip_prefix("model://").ok_or_else(|| {
        CooldisError::RuntimeFactory(format!("model_ref {model_ref:?} must start with model://"))
    })?;
    if id.is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "model_ref must include a model id".to_string(),
        ));
    }
    if let Some((provider, model)) = id.split_once('/') {
        if provider != provider_id {
            return Err(CooldisError::RuntimeFactory(format!(
                "model_ref {model_ref:?} names provider {provider:?}, expected {provider_id:?}"
            )));
        }
        if model.is_empty() {
            return Err(CooldisError::RuntimeFactory(format!(
                "model_ref {model_ref:?} must include a model id"
            )));
        }
        return Ok(model.to_string());
    }
    Ok(id.to_string())
}

fn parse_operation_ref(operation_ref: &str) -> CooldisResult<OperationRef> {
    let body = operation_ref.strip_prefix("op://").ok_or_else(|| {
        CooldisError::RuntimeFactory(format!(
            "operation_ref {operation_ref:?} must start with op://"
        ))
    })?;
    let (name, artifact_hash) = match body.split_once("@sha256:") {
        Some((name, hash)) => {
            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "operation_ref {operation_ref:?} has an invalid sha256 artifact hash"
                )));
            }
            (name, Some(hash.to_string()))
        }
        None => (body, None),
    };
    let (name, operation) = parse_operation_ref_body(operation_ref, name)?;
    Ok(OperationRef {
        name,
        operation,
        artifact_hash,
    })
}

fn parse_operation_ref_body(
    operation_ref: &str,
    body: &str,
) -> CooldisResult<(String, Option<String>)> {
    let grammar = "op://<record>@sha256:<hash> or op://<record>/<operation>@sha256:<hash>";
    let segments = body.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        [record] if !record.is_empty() => Ok(((*record).to_string(), None)),
        [record, operation] if !record.is_empty() && !operation.is_empty() => {
            Ok(((*record).to_string(), Some((*operation).to_string())))
        }
        _ => Err(CooldisError::RuntimeFactory(format!(
            "operation_ref {operation_ref:?} must match {grammar}"
        ))),
    }
}

fn override_key_name(key: AgentManifestRuntimeOverrideKey) -> &'static str {
    match key {
        AgentManifestRuntimeOverrideKey::DefaultCwd => "default_cwd",
        AgentManifestRuntimeOverrideKey::Streaming => "streaming",
        AgentManifestRuntimeOverrideKey::TurnTimeoutMs => "turn_timeout_ms",
        AgentManifestRuntimeOverrideKey::CancellationGraceMs => "cancellation_grace_ms",
        AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes => {
            "compaction.auto_at_text_bytes"
        }
    }
}

fn require_override_key(
    allowlist: &[AgentManifestRuntimeOverrideKey],
    key: AgentManifestRuntimeOverrideKey,
) -> CooldisResult<&'static str> {
    let name = override_key_name(key);
    if allowlist.contains(&key) {
        Ok(name)
    } else {
        Err(CooldisError::RuntimeFactory(format!(
            "runtime override {name:?} is not allowlisted by the agent manifest"
        )))
    }
}

fn validate_optional_positive_u64(label: &str, value: Option<u64>) -> CooldisResult<()> {
    if value == Some(0) {
        return Err(CooldisError::RuntimeFactory(format!(
            "runtime override {label:?} must be > 0"
        )));
    }
    Ok(())
}

/// Payload of the discharged `manifest.compile.completed` event: which
/// immutable manifest this thread compiled, and through which alias it was
/// reached, if any.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentManifestCompileReceipt {
    pub ref_uri: String,
    pub manifest_hash: String,
    pub source_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<AgentAliasResolutionReceipt>,
}

/// Payload of the discharged `manifest.bind.completed` event: what the
/// thread can actually do. An audit answers "what could this agent do for
/// this run" from this receipt alone.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentManifestBindReceipt {
    pub ref_uri: String,
    pub manifest_hash: String,
    /// The selected declared profile for this bind.
    pub model_profile_id: String,
    pub provider_id: String,
    pub model_id: String,
    pub tool_ids: Vec<String>,
    /// Exact operation artifacts mounted for this manifest-backed thread.
    #[serde(default)]
    pub operation_bindings: Vec<AgentManifestOperationBinding>,
    /// Exact skill packages mounted for this manifest-backed thread.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skill_packages: Vec<AgentManifestSkillPackageBinding>,
    /// Exact static context sources mounted as provider system blocks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_context_segments: Vec<AgentManifestStaticContextSegment>,
    /// Witnessed tool universes mounted on the search surface: server ref,
    /// discovery hash, in-scope contracts, and pinned rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_universes: Vec<ToolUniverseBindReceipt>,
    /// Resolved coupling functions that can observe or alter this thread's
    /// future behavior.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub couplings: Vec<AgentManifestCouplingBinding>,
    /// The union of effect grants on the bound tool bindings.
    pub granted: Vec<String>,
    /// Runtime defaults after allowlisted overrides were applied.
    pub effective_runtime: AgentManifestRuntimeDefaults,
    /// Which override keys the caller actually exercised.
    pub overridden_keys: Vec<String>,
    /// Where this thread's runtime executes, fixed at bind time (ADR 0006).
    /// Absent means local. Optional with a serde default so receipts
    /// witnessed before the field existed keep decoding and folding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<AgentManifestPlacementBinding>,
}

/// The placement resolved for a manifest-backed thread at bind time.
///
/// Placement attaches at the binding — or the conductor boundary call that
/// creates one — never inline in a model-visible tool call. The manifest
/// itself carries no placement (a manifest is portable by construction);
/// daemon config supplies deployment defaults and operator surfaces may
/// override at bind time. ADR 0006 requires the resolved binding target to be
/// witnessed with the existing `placement.decision` event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentManifestPlacementBinding {
    pub target: PlacementTarget,
    /// Which registered executor serves a non-local target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_ref: Option<String>,
    /// Executor-specific configuration, opaque to the bind layer.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub config: BTreeMap<String, JsonValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestSkillPackageBinding {
    pub resource_name: String,
    pub package_name: String,
    pub ref_uri: String,
    pub artifact_hash: String,
    pub package_digest: String,
    pub skill_count: usize,
    pub index_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestStaticContextSegment {
    pub id: String,
    pub assembler: String,
    pub input: String,
    pub pinned: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget_share: Option<f64>,
    pub ref_uri: String,
    pub content_sha256: String,
    pub content: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingBinding {
    pub id: String,
    pub role: CouplingRole,
    pub trigger_kind: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub trigger_match: BTreeMap<String, JsonValue>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_streams: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_kinds: Vec<String>,
    pub sink_stream: String,
    pub sink_kinds: Vec<String>,
    pub function_ref: String,
    pub artifact_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<String>,
    pub budget: AgentManifestCouplingBudget,
    pub config_hash: String,
}

impl AgentManifestCouplingBinding {
    fn from_bound(coupling: &BoundCoupling) -> Self {
        let source_streams = coupling
            .source_selectors
            .iter()
            .map(|selector| selector.stream.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let source_kinds = coupling
            .source_selectors
            .iter()
            .flat_map(|selector| selector.kinds.iter().map(|kind| kind.to_string()))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        Self {
            id: coupling.id.clone(),
            role: coupling.role,
            trigger_kind: coupling.trigger_kind.to_string(),
            trigger_match: coupling.trigger_match.clone(),
            source_streams,
            source_kinds,
            sink_stream: coupling.sink.stream.clone(),
            sink_kinds: coupling
                .sink
                .kinds
                .iter()
                .map(|kind| kind.to_string())
                .collect(),
            function_ref: coupling.function_ref.clone(),
            artifact_hash: coupling.function.artifact_hash.clone(),
            operation_name: coupling.function.operation_name.clone(),
            grants: coupling.grants.clone(),
            budget: coupling.budget.clone(),
            config_hash: coupling.config_hash.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestOperationBinding {
    pub name: String,
    pub artifact_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<String>,
    /// Empty means the binding exposes the whole record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<String>,
    /// Direct model/tool-router aliases declared by manifest `direct_tool` rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub direct_tools: Vec<AgentManifestDirectToolBinding>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestDirectToolBinding {
    pub tool_name: String,
    pub operation: String,
}

/// Apply caller overrides onto the manifest's runtime defaults, enforcing
/// the deny-by-default allowlist. Returns the effective defaults plus the
/// list of keys actually overridden, for the bind receipt.
pub fn apply_runtime_overrides(
    defaults: &AgentManifestRuntimeDefaults,
    overrides: &AgentManifestBindOverrides,
) -> CooldisResult<(AgentManifestRuntimeDefaults, Vec<String>)> {
    validate_optional_positive_u64("turn_timeout_ms", overrides.turn_timeout_ms)?;
    validate_optional_positive_u64("cancellation_grace_ms", overrides.cancellation_grace_ms)?;
    validate_optional_positive_u64(
        "compaction.auto_at_text_bytes",
        overrides.compaction_auto_at_text_bytes,
    )?;

    let allowlist = defaults.overrides.allow.clone();
    let mut effective = defaults.clone();
    let mut overridden_keys = Vec::new();
    if let Some(value) = &overrides.default_cwd {
        let key = require_override_key(&allowlist, AgentManifestRuntimeOverrideKey::DefaultCwd)?;
        effective.default_cwd = value.clone();
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.streaming {
        let key = require_override_key(&allowlist, AgentManifestRuntimeOverrideKey::Streaming)?;
        effective.streaming = value;
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.turn_timeout_ms {
        let key = require_override_key(&allowlist, AgentManifestRuntimeOverrideKey::TurnTimeoutMs)?;
        effective.turn_timeout_ms = Some(value);
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.cancellation_grace_ms {
        let key = require_override_key(
            &allowlist,
            AgentManifestRuntimeOverrideKey::CancellationGraceMs,
        )?;
        effective.cancellation_grace_ms = Some(value);
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.compaction_auto_at_text_bytes {
        let key = require_override_key(
            &allowlist,
            AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes,
        )?;
        effective.compaction.auto_at_text_bytes = Some(value);
        overridden_keys.push(key.to_string());
    }
    Ok((effective, overridden_keys))
}

#[cfg(test)]
mod tests;
