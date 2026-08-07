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

use sha2::Digest as _;
use std::io::Read as _;
#[cfg(unix)]
use std::os::fd::AsRawFd as _;
#[cfg(unix)]
use std::os::fd::FromRawFd as _;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt as _;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt as _;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;
#[cfg(windows)]
use std::os::windows::fs::MetadataExt as _;

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
pub const THREAD_AGENT_SKILL_DISCOVERY_METADATA: &str = "cooldis.agent.skill_discovery";
pub const THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA: &str =
    "cooldis.agent.static_context_segments";

/// Caller-supplied runtime overrides at `thread/start`. Every populated
/// field is checked against the manifest's override allowlist
/// (`AgentManifestRuntimeOverridePolicy`); a non-allowlisted override fails
/// the bind, it is never silently ignored.
#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
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
        alias = "maxToolRounds",
        skip_serializing_if = "Option::is_none"
    )]
    pub max_tool_rounds: Option<crate::agent::manifest_schema::AgentManifestMaxToolRounds>,
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
            && self.max_tool_rounds.is_none()
            && self.compaction_auto_at_text_bytes.is_none()
    }
}

/// Provider/model ids the app-server can truthfully bind for a manifest run.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AgentManifestProviderSurface {
    pub provider_id: String,
    pub model_ids: std::collections::BTreeSet<String>,
    pub supports_streaming: bool,
}

impl AgentManifestProviderSurface {
    /// A provider surface with one configured model id.
    pub fn single(provider_id: impl Into<String>, model_id: impl Into<String>) -> Self {
        Self {
            provider_id: provider_id.into(),
            model_ids: std::collections::BTreeSet::from([model_id.into()]),
            supports_streaming: true,
        }
    }

    pub fn with_supports_streaming(mut self, supports_streaming: bool) -> Self {
        self.supports_streaming = supports_streaming;
        self
    }

    /// A catalog-backed provider surface from the stored provider record.
    pub fn from_provider_record(record: &crate::LlmProviderRecord) -> Self {
        Self {
            provider_id: record.provider_id.clone(),
            model_ids: record
                .models
                .iter()
                .map(|model| model.model_id.clone())
                .collect(),
            supports_streaming: crate::ProviderCapabilityRecord::for_api(record.api.clone())
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
    pub manifest: crate::agent::manifest_schema::AgentManifestSchema,
    pub compile_receipt: AgentManifestCompileReceipt,
    pub bind_receipt: AgentManifestBindReceipt,
    pub coupling_set: BoundCouplingSet,
    pub couplings: Vec<BoundCoupling>,
    pub operation_names: Vec<String>,
    pub operation_bindings: Vec<AgentManifestOperationBinding>,
    pub skill_packages: Vec<AgentManifestSkillPackageBinding>,
    pub skill_discovery: Option<AgentManifestSkillDiscovery>,
    pub static_context_segments: Vec<AgentManifestStaticContextSegment>,
    pub skill_context_segments: Vec<AgentManifestStaticContextSegment>,
    /// Witnessed universe bindings for the thread's protocol tool imports;
    /// the runtime factory mounts these as the search surface (plus direct
    /// rows for pins).
    pub tool_universes: Vec<crate::agent::tool_universe::ToolUniverseBinding>,
}

/// Re-parse the immutable published record into the typed manifest schema
/// and build the compile receipt for the thread stream.
pub fn compile_published_agent_record(
    record: &crate::agent::manifest::PublishedAgentRecord,
    alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
) -> crate::VerletResult<(
    crate::agent::manifest_schema::AgentManifestSchema,
    AgentManifestCompileReceipt,
)> {
    record.validate()?;
    let manifest: crate::agent::manifest_schema::AgentManifestSchema =
        serde_json::from_value(record.resolved_manifest.clone()).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
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
    record: &crate::agent::manifest::PublishedAgentRecord,
    alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
    provider_surface: &AgentManifestProviderSurface,
    operation_registry_root: Option<&std::path::Path>,
    blob_registry_root: Option<&std::path::Path>,
    skill_registry_root: Option<&std::path::Path>,
    configured_mcp_server_refs: &std::collections::BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
    model_selection: &AgentManifestModelProfileSelection,
    overrides: &AgentManifestBindOverrides,
) -> crate::VerletResult<AgentManifestBoundThread> {
    bind_published_agent_record_at(
        record,
        alias,
        provider_surface,
        operation_registry_root,
        blob_registry_root,
        skill_registry_root,
        configured_mcp_server_refs,
        tool_universe_discoverer,
        model_selection,
        overrides,
        crate::kernel::history::now_ms(),
    )
    .await
}

/// Compile and bind with caller-supplied time for deterministic authority
/// checks. Production callers normally use [`bind_published_agent_record`].
pub async fn bind_published_agent_record_at(
    record: &crate::agent::manifest::PublishedAgentRecord,
    alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
    provider_surface: &AgentManifestProviderSurface,
    operation_registry_root: Option<&std::path::Path>,
    blob_registry_root: Option<&std::path::Path>,
    skill_registry_root: Option<&std::path::Path>,
    configured_mcp_server_refs: &std::collections::BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
    model_selection: &AgentManifestModelProfileSelection,
    overrides: &AgentManifestBindOverrides,
    now_ms: i64,
) -> crate::VerletResult<AgentManifestBoundThread> {
    bind_published_agent_record_with_placement_at(
        record,
        alias,
        provider_surface,
        operation_registry_root,
        blob_registry_root,
        skill_registry_root,
        configured_mcp_server_refs,
        tool_universe_discoverer,
        model_selection,
        overrides,
        None,
        None,
        None,
        None,
        false,
        now_ms,
    )
    .await
}

/// Compile and bind a published manifest while resolving deployment placement.
///
/// `placement_override` is an operator-surface bind override and takes
/// precedence over `default_placement`. Placement is intentionally separate
/// from manifest runtime overrides: manifests are portable and cannot allow,
/// deny, or select their deployment target.
pub async fn bind_published_agent_record_with_placement(
    record: &crate::agent::manifest::PublishedAgentRecord,
    alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
    provider_surface: &AgentManifestProviderSurface,
    operation_registry_root: Option<&std::path::Path>,
    blob_registry_root: Option<&std::path::Path>,
    skill_registry_root: Option<&std::path::Path>,
    configured_mcp_server_refs: &std::collections::BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
    model_selection: &AgentManifestModelProfileSelection,
    overrides: &AgentManifestBindOverrides,
    default_placement: Option<&AgentManifestPlacementBinding>,
    placement_override: Option<&AgentManifestPlacementBinding>,
    default_workspace: Option<&AgentManifestWorkspaceBinding>,
    workspace_override: Option<&AgentManifestWorkspaceBinding>,
    remote_event_store_served: bool,
) -> crate::VerletResult<AgentManifestBoundThread> {
    bind_published_agent_record_with_placement_at(
        record,
        alias,
        provider_surface,
        operation_registry_root,
        blob_registry_root,
        skill_registry_root,
        configured_mcp_server_refs,
        tool_universe_discoverer,
        model_selection,
        overrides,
        default_placement,
        placement_override,
        default_workspace,
        workspace_override,
        remote_event_store_served,
        crate::kernel::history::now_ms(),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn bind_published_agent_record_with_placement_at(
    record: &crate::agent::manifest::PublishedAgentRecord,
    alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
    provider_surface: &AgentManifestProviderSurface,
    operation_registry_root: Option<&std::path::Path>,
    blob_registry_root: Option<&std::path::Path>,
    skill_registry_root: Option<&std::path::Path>,
    configured_mcp_server_refs: &std::collections::BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
    model_selection: &AgentManifestModelProfileSelection,
    overrides: &AgentManifestBindOverrides,
    default_placement: Option<&AgentManifestPlacementBinding>,
    placement_override: Option<&AgentManifestPlacementBinding>,
    default_workspace: Option<&AgentManifestWorkspaceBinding>,
    workspace_override: Option<&AgentManifestWorkspaceBinding>,
    remote_event_store_served: bool,
    now_ms: i64,
) -> crate::VerletResult<AgentManifestBoundThread> {
    bind_published_agent_record_with_placement_and_skill_witness(
        record,
        alias,
        provider_surface,
        operation_registry_root,
        blob_registry_root,
        skill_registry_root,
        configured_mcp_server_refs,
        tool_universe_discoverer,
        model_selection,
        overrides,
        default_placement,
        placement_override,
        default_workspace,
        workspace_override,
        remote_event_store_served,
        None,
        None,
        false,
        now_ms,
    )
    .await
}

pub(crate) async fn bind_published_agent_record_with_placement_and_skill_witness(
    record: &crate::agent::manifest::PublishedAgentRecord,
    alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
    provider_surface: &AgentManifestProviderSurface,
    operation_registry_root: Option<&std::path::Path>,
    blob_registry_root: Option<&std::path::Path>,
    skill_registry_root: Option<&std::path::Path>,
    configured_mcp_server_refs: &std::collections::BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
    model_selection: &AgentManifestModelProfileSelection,
    overrides: &AgentManifestBindOverrides,
    default_placement: Option<&AgentManifestPlacementBinding>,
    placement_override: Option<&AgentManifestPlacementBinding>,
    default_workspace: Option<&AgentManifestWorkspaceBinding>,
    workspace_override: Option<&AgentManifestWorkspaceBinding>,
    remote_event_store_served: bool,
    skill_package_witness: Option<&[AgentManifestSkillPackageBinding]>,
    skill_discovery_witness: Option<&AgentManifestSkillDiscovery>,
    rehydrating_from_witness: bool,
    now_ms: i64,
) -> crate::VerletResult<AgentManifestBoundThread> {
    let (manifest, compile_receipt) = compile_published_agent_record(record, alias)?;
    let placement = resolve_manifest_placement_with_origin(
        default_placement,
        placement_override,
        remote_event_store_served,
    )?;
    let workspace = resolve_manifest_workspace_with_origin(
        manifest.workspace.as_ref(),
        default_workspace,
        workspace_override,
    )?;
    if placement.binding.target != crate::kernel::control_decision::PlacementTarget::Local
        && workspace.is_some()
    {
        return Err(crate::VerletError::RuntimeFactory(
            "workspace bindings currently require local placement; remote and sandbox workspace transfer belongs to the sandbox executor boundary"
                .to_string(),
        ));
    }
    let selected = select_manifest_model_profile(&manifest, model_selection)?;
    let profile = selected.profile;
    let provider_id = selected.provider_id;
    if provider_id != provider_surface.provider_id {
        return Err(crate::VerletError::RuntimeFactory(format!(
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
        return Err(crate::VerletError::RuntimeFactory(format!(
            "agent manifest model_ref {:?} is not configured for provider {:?}; available models: {}",
            profile.model_ref, provider_id, available
        )));
    }

    let (effective_runtime, overridden_keys) =
        apply_runtime_overrides(&manifest.runtime, overrides)?;
    if effective_runtime.streaming && !provider_surface.supports_streaming {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "agent manifest runtime.streaming requires provider {:?} to support streaming",
            provider_surface.provider_id
        )));
    }
    let bound_tools = bind_tools(
        &manifest.tools,
        operation_registry_root,
        configured_mcp_server_refs,
        tool_universe_discoverer,
        now_ms,
    )
    .await?;
    let static_context_segments = bind_static_context_sources(&manifest, blob_registry_root)?;
    let bound_skills = match skill_package_witness {
        Some(witness) => {
            bind_skill_resources_from_witness(&manifest.resources, skill_registry_root, witness)?
        }
        None => bind_skill_resources(&manifest.resources, skill_registry_root)?,
    };
    let (skill_discovery, discovery_context_segment) = bind_workspace_skill_discovery(
        &manifest,
        workspace.as_ref().map(|workspace| &workspace.mount),
        skill_discovery_witness,
        rehydrating_from_witness,
        &bound_skills.skill_names,
    )?;
    let mut skill_context_segments = bound_skills.context_segments;
    if let Some(segment) = discovery_context_segment {
        skill_context_segments.push(segment);
    }
    let bound_couplings = bind_couplings(&manifest.couplings, operation_registry_root, now_ms)?;
    let couplings = bound_couplings.couplings;
    enforce_child_agent_policy(&manifest, &bound_tools.operation_bindings, &couplings)?;
    let operation_names = bound_tools
        .operation_bindings
        .iter()
        .map(|binding| binding.name.clone())
        .collect::<Vec<_>>();
    let coupling_set = BoundCouplingSet {
        snapshot_id: record.manifest_hash.clone(),
        couplings: couplings.clone(),
        grant_expiries: bound_couplings.grant_expiries,
    };
    let coupling_bindings = coupling_set
        .couplings
        .iter()
        .map(|coupling| {
            AgentManifestCouplingBinding::from_bound(
                coupling,
                coupling_set
                    .grant_expiries
                    .get(&coupling.id)
                    .cloned()
                    .unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    let bind_receipt = AgentManifestBindReceipt {
        ref_uri: record.ref_uri.clone(),
        manifest_hash: record.manifest_hash.clone(),
        model_profile_id: profile.id.clone(),
        model_profile_origin: Some(selected.origin),
        provider_id,
        model_id,
        tool_ids: bound_tools.tool_ids,
        operation_bindings: bound_tools.operation_bindings.clone(),
        skill_packages: bound_skills.package_bindings.clone(),
        skill_discovery: skill_discovery.clone(),
        static_context_segments: static_context_segments.clone(),
        tool_universes: bound_tools
            .tool_universes
            .iter()
            .map(crate::agent::tool_universe::ToolUniverseBindReceipt::from_binding)
            .collect(),
        couplings: coupling_bindings,
        granted: bound_tools.granted,
        grant_bindings: bound_tools
            .grant_bindings
            .into_iter()
            .chain(bound_couplings.grant_bindings)
            .collect(),
        effective_runtime,
        overridden_keys,
        placement: Some(placement.binding),
        placement_origin: Some(placement.origin),
        workspace_origin: workspace.as_ref().map(|workspace| workspace.origin),
        workspace: workspace.map(|workspace| workspace.mount),
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
        skill_discovery,
        static_context_segments,
        skill_context_segments,
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
    grant_bindings: Vec<AgentManifestGrantBindingReceipt>,
    operation_bindings: Vec<AgentManifestOperationBinding>,
    tool_universes: Vec<crate::agent::tool_universe::ToolUniverseBinding>,
}

struct BoundSkills {
    package_bindings: Vec<AgentManifestSkillPackageBinding>,
    context_segments: Vec<AgentManifestStaticContextSegment>,
    skill_names: std::collections::BTreeSet<String>,
}

fn bind_static_context_sources(
    manifest: &crate::agent::manifest_schema::AgentManifestSchema,
    blob_registry_root: Option<&std::path::Path>,
) -> crate::VerletResult<Vec<AgentManifestStaticContextSegment>> {
    let pipeline = manifest.effective_context_pipeline();
    let mut segments = Vec::new();
    for source in &pipeline.sources {
        if source.assembler != crate::agent::manifest_schema::KERNEL_ASSEMBLER_STATIC {
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
        if resource.kind != crate::agent::manifest_schema::AgentManifestResourceKind::Blob {
            continue;
        }
        let registry_root = blob_registry_root.ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!(
                "blob resource {:?} ref {:?} requires an app-server blob registry root",
                resource.name, resource.reference
            ))
        })?;
        let registry = crate::LocalBlobRegistry::new(registry_root);
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
    resources: &'a [crate::agent::manifest_schema::AgentManifestResource],
) -> crate::VerletResult<Option<&'a crate::agent::manifest_schema::AgentManifestResource>> {
    if input.starts_with("resource://") || input.starts_with("skill://") {
        return resources
            .iter()
            .find(|resource| resource.reference == input)
            .map(Some)
            .ok_or_else(|| {
                crate::VerletError::RuntimeFactory(format!(
                    "static context source input {input:?} does not match a declared resource ref"
                ))
            });
    }
    resources
        .iter()
        .find(|resource| resource.name == input)
        .map(Some)
        .ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!(
                "static context source input {input:?} does not name a declared resource"
            ))
        })
}

fn missing_blob_resource_error(
    resource: &crate::agent::manifest_schema::AgentManifestResource,
    err: impl std::fmt::Display,
) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(format!(
        "blob resource {:?} ref {:?} was not found in the local blob registry: {err}; run `verlet blob publish <file>` and use the returned resource://artifact/sha256:<hash> ref",
        resource.name, resource.reference
    ))
}

fn static_source_budget_share(
    pipeline: &crate::agent::manifest_schema::AgentManifestContextPipeline,
    source_id: &str,
) -> Option<f64> {
    pipeline
        .sources
        .iter()
        .find(|source| source.id == source_id)
        .and_then(|source| match source.budget_share {
            Some(crate::agent::manifest_schema::AgentManifestBudgetShare::Fraction(value)) => {
                Some(value)
            }
            Some(crate::agent::manifest_schema::AgentManifestBudgetShare::Rest(
                crate::agent::manifest_schema::AgentManifestBudgetRest::Rest,
            ))
            | None => None,
        })
}

/// Role inferred from the coupling's resolved sink relation. Manifest
/// authors cannot choose this directly.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CouplingRole {
    Projection,
    Controller,
}

#[derive(Clone, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundCouplingSet {
    pub snapshot_id: String,
    pub couplings: Vec<BoundCoupling>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub grant_expiries: std::collections::BTreeMap<
        String,
        Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
    >,
}

impl BoundCouplingSet {
    pub fn new(snapshot_id: impl Into<String>, couplings: Vec<BoundCoupling>) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            couplings,
            grant_expiries: std::collections::BTreeMap::new(),
        }
    }

    pub fn new_with_grant_expiries(
        snapshot_id: impl Into<String>,
        couplings: Vec<BoundCoupling>,
        grant_expiries: std::collections::BTreeMap<
            String,
            Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
        >,
    ) -> Self {
        Self {
            snapshot_id: snapshot_id.into(),
            couplings,
            grant_expiries,
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundCoupling {
    pub id: String,
    pub role: CouplingRole,
    pub trigger_kind: crate::EventKind,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub trigger_match: std::collections::BTreeMap<String, serde_json::Value>,
    pub trigger_quota: crate::agent::manifest_schema::AgentManifestCouplingQuota,
    pub source_selectors: Vec<BoundCouplingSelector>,
    pub sink: BoundCouplingSink,
    pub function_ref: String,
    pub function: BoundCouplingFunction,
    pub grants: Vec<String>,
    pub budget: crate::agent::manifest_schema::AgentManifestCouplingBudget,
    pub config: serde_json::Value,
    pub config_hash: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundCouplingSelector {
    pub stream: String,
    pub kinds: Vec<crate::EventKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundCouplingSink {
    pub stream: String,
    pub kinds: Vec<crate::EventKind>,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct BoundCouplingFunction {
    pub name: String,
    pub artifact_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_name: Option<String>,
}

#[derive(Clone, Debug, Default)]
struct OperationBindingAccumulator {
    grants: std::collections::BTreeSet<String>,
    grant_expiries:
        std::collections::BTreeSet<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
    operations: std::collections::BTreeSet<String>,
    direct_tools: std::collections::BTreeSet<AgentManifestDirectToolBinding>,
    effect_class: Option<crate::agent::manifest_schema::EffectClass>,
    whole_record: bool,
}

impl OperationBindingAccumulator {
    #[cfg(test)]
    fn merge(
        &mut self,
        grants: std::collections::BTreeSet<String>,
        operation: Option<String>,
        direct_tool: Option<AgentManifestDirectToolBinding>,
    ) {
        self.merge_with_expiries(
            grants,
            std::collections::BTreeSet::new(),
            operation,
            direct_tool,
            crate::agent::manifest_schema::EffectClass::AtMostOnce,
        );
    }

    fn merge_with_expiries(
        &mut self,
        grants: std::collections::BTreeSet<String>,
        grant_expiries: std::collections::BTreeSet<
            crate::agent::manifest_schema::AgentManifestGrantExpiry,
        >,
        operation: Option<String>,
        direct_tool: Option<AgentManifestDirectToolBinding>,
        effect_class: crate::agent::manifest_schema::EffectClass,
    ) {
        self.grants.extend(grants);
        self.grant_expiries.extend(grant_expiries);
        if let Some(direct_tool) = direct_tool {
            self.direct_tools.insert(direct_tool);
        }
        self.effect_class = Some(
            self.effect_class
                .map(|bound| bound.max(effect_class))
                .unwrap_or(effect_class),
        );
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

type OperationBindingMap =
    std::collections::BTreeMap<(String, String), OperationBindingAccumulator>;

fn bind_skill_resources(
    resources: &[crate::agent::manifest_schema::AgentManifestResource],
    skill_registry_root: Option<&std::path::Path>,
) -> crate::VerletResult<BoundSkills> {
    let skill_resources = resources
        .iter()
        .filter(|resource| {
            resource.kind == crate::agent::manifest_schema::AgentManifestResourceKind::Skill
        })
        .collect::<Vec<_>>();
    if skill_resources.is_empty() {
        return Ok(BoundSkills {
            package_bindings: Vec::new(),
            context_segments: Vec::new(),
            skill_names: std::collections::BTreeSet::new(),
        });
    }
    let registry_root = skill_registry_root.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(
            "skill resources require an app-server skill registry root".to_string(),
        )
    })?;
    let registry = crate::LocalSkillRegistry::new(registry_root);
    let mut package_bindings = Vec::new();
    let mut context_segments = Vec::new();
    let mut mounted_skill_names = std::collections::BTreeSet::new();
    for resource in skill_resources {
        let parsed = crate::DeclaredSkillPackageRef::parse(&resource.reference).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "skill resource {:?} ref {:?} is invalid: {err}",
                resource.name, resource.reference
            ))
        })?;
        let record = match &parsed {
            crate::DeclaredSkillPackageRef::Floating { name } => registry.load_record(name).map_err(|err| {
                crate::VerletError::RuntimeFactory(format!(
                    "skill resource {:?} floating ref {:?} was not found in the local skill registry: {err}; publish it first with `verlet skill publish <dir>`",
                    resource.name, resource.reference
                ))
            }),
            crate::DeclaredSkillPackageRef::Pinned(reference) => registry
                .load_version_record(&reference.name, &reference.artifact_hash)
                .map_err(|err| {
                    crate::VerletError::RuntimeFactory(format!(
                        "skill resource {:?} ref {:?} was not found in the local skill registry: {err}; publish the skill package or replace the ref with a hash from the registry",
                        resource.name, resource.reference
                    ))
                }),
        }
        ?;
        append_bound_skill(
            &resource.name,
            &record,
            &mut mounted_skill_names,
            &mut package_bindings,
            &mut context_segments,
        )?;
    }
    Ok(BoundSkills {
        package_bindings,
        context_segments,
        skill_names: mounted_skill_names,
    })
}

fn bind_skill_resources_from_witness(
    resources: &[crate::agent::manifest_schema::AgentManifestResource],
    skill_registry_root: Option<&std::path::Path>,
    witness: &[AgentManifestSkillPackageBinding],
) -> crate::VerletResult<BoundSkills> {
    let skill_resources = resources
        .iter()
        .filter(|resource| {
            resource.kind == crate::agent::manifest_schema::AgentManifestResourceKind::Skill
        })
        .collect::<Vec<_>>();
    if skill_resources.len() != witness.len() {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "stored skill package witness has {} bindings for {} manifest skill resources",
            witness.len(),
            skill_resources.len()
        )));
    }
    if skill_resources.is_empty() {
        return Ok(BoundSkills {
            package_bindings: Vec::new(),
            context_segments: Vec::new(),
            skill_names: std::collections::BTreeSet::new(),
        });
    }
    let mut bindings_by_resource = std::collections::BTreeMap::new();
    for binding in witness {
        if bindings_by_resource
            .insert(binding.resource_name.as_str(), binding)
            .is_some()
        {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill package witness repeats resource {:?}",
                binding.resource_name
            )));
        }
    }
    let mut package_bindings = Vec::new();
    for resource in skill_resources {
        let binding = bindings_by_resource
            .remove(resource.name.as_str())
            .ok_or_else(|| {
                crate::VerletError::RuntimeFactory(format!(
                    "stored skill package witness has no binding for manifest resource {:?}",
                    resource.name
                ))
            })?;
        match crate::DeclaredSkillPackageRef::parse(&resource.reference)? {
            crate::DeclaredSkillPackageRef::Floating { name } if name == binding.package_name => {}
            crate::DeclaredSkillPackageRef::Pinned(reference)
                if reference.name == binding.package_name
                    && reference.artifact_hash == binding.artifact_hash => {}
            _ => {
                return Err(crate::VerletError::RuntimeFactory(format!(
                    "stored skill package binding for resource {:?} does not match manifest ref {:?}",
                    resource.name, resource.reference
                )));
            }
        }
        package_bindings.push(binding.clone());
    }
    let (context_segments, skill_names) =
        skill_context_segments_and_names_for_bindings(&package_bindings, skill_registry_root)?;
    Ok(BoundSkills {
        package_bindings,
        context_segments,
        skill_names,
    })
}

fn append_bound_skill(
    resource_name: &str,
    record: &crate::PublishedSkillPackageRecord,
    mounted_skill_names: &mut std::collections::BTreeSet<String>,
    package_bindings: &mut Vec<AgentManifestSkillPackageBinding>,
    context_segments: &mut Vec<AgentManifestStaticContextSegment>,
) -> crate::VerletResult<()> {
    for skill in &record.package.skills {
        if !mounted_skill_names.insert(skill.name.clone()) {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "skill resource {resource_name:?} package {:?} would mount duplicate /skills/{}.md; skill names must be unique across bound packages",
                record.name, skill.name
            )));
        }
    }
    let index = record.package.render_index();
    let index_sha256 = sha256_prefixed(index.as_bytes());
    let ref_uri = record.ref_uri();
    context_segments.push(AgentManifestStaticContextSegment {
        id: format!("skill-index:{resource_name}"),
        assembler: crate::agent::manifest_schema::KERNEL_ASSEMBLER_STATIC.to_string(),
        input: resource_name.to_string(),
        pinned: true,
        budget_share: None,
        ref_uri: ref_uri.clone(),
        content_sha256: index_sha256.clone(),
        content: index,
    });
    package_bindings.push(AgentManifestSkillPackageBinding {
        resource_name: resource_name.to_string(),
        package_name: record.name.clone(),
        ref_uri,
        artifact_hash: record.active_artifact_hash.clone(),
        package_digest: format!("sha256:{}", record.active_artifact_hash),
        skill_count: record.package.skills.len(),
        index_sha256,
    });
    Ok(())
}

pub(crate) fn skill_context_segments_for_witnesses(
    bindings: &[AgentManifestSkillPackageBinding],
    skill_registry_root: Option<&std::path::Path>,
    discovery: Option<&AgentManifestSkillDiscovery>,
) -> crate::VerletResult<Vec<AgentManifestStaticContextSegment>> {
    let (mut segments, skill_names) =
        skill_context_segments_and_names_for_bindings(bindings, skill_registry_root)?;
    if let Some(discovery) = discovery {
        let normalized_path = normalize_workspace_relative_path(&discovery.path);
        if discovery.path != normalized_path {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill discovery witness path {:?} is not canonical",
                discovery.path
            )));
        }
        validate_skill_discovery_witness(discovery, &normalized_path, &skill_names)?;
        segments.push(skill_discovery_context_segment(discovery));
    }
    Ok(segments)
}

fn skill_context_segments_and_names_for_bindings(
    bindings: &[AgentManifestSkillPackageBinding],
    skill_registry_root: Option<&std::path::Path>,
) -> crate::VerletResult<(
    Vec<AgentManifestStaticContextSegment>,
    std::collections::BTreeSet<String>,
)> {
    if bindings.is_empty() {
        return Ok((Vec::new(), std::collections::BTreeSet::new()));
    }
    let registry_root = skill_registry_root.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(
            "skill package bindings require an app-server skill registry root".to_string(),
        )
    })?;
    let registry = crate::LocalSkillRegistry::new(registry_root);
    let mut actual_bindings = Vec::new();
    let mut context_segments = Vec::new();
    let mut mounted_skill_names = std::collections::BTreeSet::new();
    for binding in bindings {
        let record = registry
            .load_version_record(&binding.package_name, &binding.artifact_hash)
            .map_err(|err| {
                crate::VerletError::RuntimeFactory(format!(
                    "stored skill package binding {:?}@sha256:{} was not found: {err}",
                    binding.package_name, binding.artifact_hash
                ))
            })?;
        append_bound_skill(
            &binding.resource_name,
            &record,
            &mut mounted_skill_names,
            &mut actual_bindings,
            &mut context_segments,
        )?;
        if actual_bindings.last() != Some(binding) {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill package binding for resource {:?} disagrees with immutable registry content",
                binding.resource_name
            )));
        }
    }
    Ok((context_segments, mounted_skill_names))
}

pub(crate) fn skill_package_bindings_match(
    left: &[AgentManifestSkillPackageBinding],
    right: &[AgentManifestSkillPackageBinding],
) -> bool {
    fn key(
        binding: &AgentManifestSkillPackageBinding,
    ) -> (&str, &str, &str, &str, &str, usize, &str) {
        (
            &binding.resource_name,
            &binding.package_name,
            &binding.ref_uri,
            &binding.artifact_hash,
            &binding.package_digest,
            binding.skill_count,
            &binding.index_sha256,
        )
    }

    if left.len() != right.len() {
        return false;
    }
    let mut left = left.iter().collect::<Vec<_>>();
    let mut right = right.iter().collect::<Vec<_>>();
    left.sort_unstable_by_key(|binding| key(binding));
    right.sort_unstable_by_key(|binding| key(binding));
    left == right
}

fn bind_workspace_skill_discovery(
    manifest: &crate::agent::manifest_schema::AgentManifestSchema,
    workspace: Option<&AgentManifestResolvedWorkspaceMount>,
    witness: Option<&AgentManifestSkillDiscovery>,
    rehydrating: bool,
    registry_skill_names: &std::collections::BTreeSet<String>,
) -> crate::VerletResult<(
    Option<AgentManifestSkillDiscovery>,
    Option<AgentManifestStaticContextSegment>,
)> {
    if !manifest.skills.discover {
        if witness.is_some() {
            return Err(crate::VerletError::RuntimeFactory(
                "stored skill discovery witness exists, but the manifest disables workspace skill discovery"
                    .to_string(),
            ));
        }
        return Ok((None, None));
    }

    let workspace = workspace.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(
            "agent manifest skill discovery requires a resolved workspace binding".to_string(),
        )
    })?;
    let resolved_path = normalize_workspace_relative_path(&manifest.skills.path);
    let discovery = match witness {
        Some(witness) => {
            validate_skill_discovery_witness(witness, &resolved_path, registry_skill_names)?;
            witness.clone()
        }
        None if rehydrating => {
            return Err(crate::VerletError::RuntimeFactory(
                "manifest enables workspace skill discovery, but the durable bind receipt has no skill discovery witness"
                    .to_string(),
            ));
        }
        None => discover_workspace_skills(workspace, &resolved_path, registry_skill_names)?,
    };
    let segment = skill_discovery_context_segment(&discovery);
    Ok((Some(discovery), Some(segment)))
}

fn discover_workspace_skills(
    workspace: &AgentManifestResolvedWorkspaceMount,
    resolved_path: &str,
    registry_skill_names: &std::collections::BTreeSet<String>,
) -> crate::VerletResult<AgentManifestSkillDiscovery> {
    let discovery_root = workspace.host_path.join(resolved_path);
    let canonical_discovery_root = match std::fs::canonicalize(&discovery_root) {
        Ok(path) => path,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            return Ok(AgentManifestSkillDiscovery {
                path: resolved_path.to_string(),
                skills: Vec::new(),
            });
        }
        Err(err) => {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "failed to resolve workspace skill discovery directory {}: {err}",
                discovery_root.display()
            )));
        }
    };
    if !canonical_discovery_root.starts_with(&workspace.host_path) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "workspace skill discovery path {resolved_path:?} resolves outside the witnessed workspace"
        )));
    }
    let skill_dirs =
        open_workspace_skill_directories(&workspace.host_path, &canonical_discovery_root)?;

    let mut skills = Vec::new();
    let mut names = registry_skill_names.clone();
    for skill_dir in skill_dirs {
        let skill_file = skill_dir.path.join("SKILL.md");
        let canonical_skill_file = match std::fs::canonicalize(&skill_file) {
            Ok(path) => path,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(crate::VerletError::RuntimeFactory(format!(
                    "failed to resolve discovered workspace skill file {}: {err}",
                    skill_file.display()
                )));
            }
        };
        if !canonical_skill_file.starts_with(&workspace.host_path) {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "discovered workspace skill file {} resolves outside the witnessed workspace",
                skill_file.display()
            )));
        }
        let Some(file) = open_workspace_skill_file(&skill_dir, &skill_file).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to open discovered workspace skill file {}: {err}",
                skill_file.display()
            ))
        })?
        else {
            continue;
        };
        let body = read_opened_workspace_skill_file(&workspace.host_path, &skill_file, file)?;
        let entry = crate::SkillPackageEntry::from_skill_body(&skill_dir.path, body)?;
        if !names.insert(entry.name.clone()) {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "workspace skill discovery found duplicate skill name {:?} across discovered entries or registry-bound skill packages",
                entry.name
            )));
        }
        let directory_name = skill_dir
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                crate::VerletError::RuntimeFactory(format!(
                    "workspace skill directory {} has no unicode name",
                    skill_dir.path.display()
                ))
            })?;
        skills.push(AgentManifestDiscoveredSkill {
            name: entry.name,
            path: discovered_skill_path(resolved_path, directory_name),
            content_sha256: sha256_prefixed(entry.body.as_bytes()),
            description: entry.description,
        });
    }
    skills.sort_by(|left, right| left.name.cmp(&right.name));
    let discovery = AgentManifestSkillDiscovery {
        path: resolved_path.to_string(),
        skills,
    };
    validate_skill_discovery_witness(&discovery, resolved_path, registry_skill_names)?;
    Ok(discovery)
}

struct WorkspaceSkillDirectory {
    path: std::path::PathBuf,
    #[cfg(unix)]
    file: std::fs::File,
}

#[cfg(unix)]
fn open_workspace_skill_directories(
    workspace_host_path: &std::path::Path,
    discovery_root: &std::path::Path,
) -> crate::VerletResult<Vec<WorkspaceSkillDirectory>> {
    let mut options = std::fs::File::options();
    options
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW);
    let directory = options.open(discovery_root).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to open workspace skill discovery directory {}: {err}",
            discovery_root.display()
        ))
    })?;
    validate_opened_workspace_directory(workspace_host_path, discovery_root, &directory)?;
    let names = read_opened_directory_names(&directory).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to read opened workspace skill discovery directory {}: {err}",
            discovery_root.display()
        ))
    })?;
    let mut skill_dirs = Vec::new();
    for name in names {
        if let Some(file) = open_directory_at(&directory, &name).map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to inspect workspace skill discovery entry {}: {err}",
                discovery_root.join(&name).display()
            ))
        })? {
            skill_dirs.push(WorkspaceSkillDirectory {
                path: discovery_root.join(name),
                file,
            });
        }
    }
    Ok(skill_dirs)
}

#[cfg(not(unix))]
fn open_workspace_skill_directories(
    _workspace_host_path: &std::path::Path,
    discovery_root: &std::path::Path,
) -> crate::VerletResult<Vec<WorkspaceSkillDirectory>> {
    let entries = std::fs::read_dir(discovery_root).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to read workspace skill discovery directory {}: {err}",
            discovery_root.display()
        ))
    })?;
    let mut skill_dirs = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to read an entry in workspace skill discovery directory {}: {err}",
                discovery_root.display()
            ))
        })?;
        let file_type = entry.file_type().map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "failed to inspect workspace skill discovery entry {}: {err}",
                entry.path().display()
            ))
        })?;
        if file_type.is_dir() {
            skill_dirs.push(WorkspaceSkillDirectory { path: entry.path() });
        }
    }
    skill_dirs.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(skill_dirs)
}

#[cfg(unix)]
fn validate_opened_workspace_directory(
    workspace_host_path: &std::path::Path,
    discovery_root: &std::path::Path,
    directory: &std::fs::File,
) -> crate::VerletResult<()> {
    let resolved = std::fs::canonicalize(discovery_root).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to re-resolve opened workspace skill discovery directory {}: {err}",
            discovery_root.display()
        ))
    })?;
    if !resolved.starts_with(workspace_host_path) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "opened workspace skill discovery directory {} resolves outside the witnessed workspace",
            discovery_root.display()
        )));
    }
    let opened_metadata = directory.metadata().map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to inspect opened workspace skill discovery directory {}: {err}",
            discovery_root.display()
        ))
    })?;
    let resolved_metadata = std::fs::metadata(&resolved).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to inspect resolved workspace skill discovery directory {}: {err}",
            resolved.display()
        ))
    })?;
    if !opened_metadata.is_dir() || !same_file_identity(&opened_metadata, &resolved_metadata) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "workspace skill discovery directory {} changed while it was opened",
            discovery_root.display()
        )));
    }
    Ok(())
}

#[cfg(unix)]
fn open_directory_at(
    parent: &std::fs::File,
    name: &std::ffi::OsStr,
) -> std::io::Result<Option<std::fs::File>> {
    let name = std::ffi::CString::new(name.as_bytes()).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "directory entry contains a nul byte",
        )
    })?;
    let descriptor = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW,
        )
    };
    if descriptor >= 0 {
        return Ok(Some(unsafe { std::fs::File::from_raw_fd(descriptor) }));
    }
    let err = std::io::Error::last_os_error();
    if matches!(
        err.raw_os_error(),
        Some(libc::ENOENT) | Some(libc::ENOTDIR) | Some(libc::ELOOP)
    ) {
        Ok(None)
    } else {
        Err(err)
    }
}

#[cfg(unix)]
struct DirectoryStream(*mut libc::DIR);

#[cfg(unix)]
impl Drop for DirectoryStream {
    fn drop(&mut self) {
        unsafe {
            libc::closedir(self.0);
        }
    }
}

#[cfg(unix)]
fn read_opened_directory_names(
    directory: &std::fs::File,
) -> std::io::Result<Vec<std::ffi::OsString>> {
    let descriptor = unsafe { libc::dup(directory.as_raw_fd()) };
    if descriptor < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let stream = unsafe { libc::fdopendir(descriptor) };
    if stream.is_null() {
        let err = std::io::Error::last_os_error();
        unsafe {
            libc::close(descriptor);
        }
        return Err(err);
    }
    let stream = DirectoryStream(stream);
    let mut names = Vec::new();
    loop {
        clear_directory_errno();
        let entry = unsafe { libc::readdir(stream.0) };
        if entry.is_null() {
            if let Some(err) = directory_errno() {
                return Err(err);
            }
            break;
        }
        let name = unsafe { std::ffi::CStr::from_ptr((*entry).d_name.as_ptr()) }.to_bytes();
        if name != b"." && name != b".." {
            names.push(std::ffi::OsString::from_vec(name.to_vec()));
        }
    }
    names.sort();
    Ok(names)
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn clear_directory_errno() {
    unsafe {
        *libc::__errno_location() = 0;
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn directory_errno() -> Option<std::io::Error> {
    let errno = unsafe { *libc::__errno_location() };
    (errno != 0).then(|| std::io::Error::from_raw_os_error(errno))
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn clear_directory_errno() {
    unsafe {
        *libc::__error() = 0;
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "freebsd",
    target_os = "dragonfly",
    target_os = "netbsd",
    target_os = "openbsd"
))]
fn directory_errno() -> Option<std::io::Error> {
    let errno = unsafe { *libc::__error() };
    (errno != 0).then(|| std::io::Error::from_raw_os_error(errno))
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd"
    ))
))]
fn clear_directory_errno() {}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "android",
        target_os = "macos",
        target_os = "ios",
        target_os = "freebsd",
        target_os = "dragonfly",
        target_os = "netbsd",
        target_os = "openbsd"
    ))
))]
fn directory_errno() -> Option<std::io::Error> {
    None
}

#[cfg(unix)]
fn open_workspace_skill_file(
    skill_dir: &WorkspaceSkillDirectory,
    _path: &std::path::Path,
) -> std::io::Result<Option<std::fs::File>> {
    let descriptor = unsafe {
        libc::openat(
            skill_dir.file.as_raw_fd(),
            c"SKILL.md".as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if descriptor >= 0 {
        return Ok(Some(unsafe { std::fs::File::from_raw_fd(descriptor) }));
    }
    let err = std::io::Error::last_os_error();
    if err.kind() == std::io::ErrorKind::NotFound {
        Ok(None)
    } else {
        Err(err)
    }
}

#[cfg(not(unix))]
fn open_workspace_skill_file(
    _skill_dir: &WorkspaceSkillDirectory,
    path: &std::path::Path,
) -> std::io::Result<Option<std::fs::File>> {
    let mut options = std::fs::File::options();
    options.read(true);
    match options.open(path) {
        Ok(file) => Ok(Some(file)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err),
    }
}

fn read_opened_workspace_skill_file(
    workspace_host_path: &std::path::Path,
    skill_file: &std::path::Path,
    mut file: std::fs::File,
) -> crate::VerletResult<String> {
    let canonical_skill_file = std::fs::canonicalize(skill_file).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to re-resolve opened workspace skill file {}: {err}",
            skill_file.display()
        ))
    })?;
    if !canonical_skill_file.starts_with(workspace_host_path) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "opened workspace skill file {} resolves outside the witnessed workspace",
            skill_file.display()
        )));
    }
    let opened_metadata = file.metadata().map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to inspect opened workspace skill file {}: {err}",
            skill_file.display()
        ))
    })?;
    if !opened_metadata.is_file() {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "discovered workspace skill path {} is not a regular file",
            skill_file.display()
        )));
    }
    let resolved_metadata = std::fs::metadata(&canonical_skill_file).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to inspect resolved workspace skill file {}: {err}",
            canonical_skill_file.display()
        ))
    })?;
    if !same_file_identity(&opened_metadata, &resolved_metadata) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "workspace skill file {} changed while it was opened",
            skill_file.display()
        )));
    }
    let mut body = String::new();
    file.read_to_string(&mut body).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "failed to read opened workspace skill file {}: {err}",
            skill_file.display()
        ))
    })?;
    Ok(body)
}

#[cfg(unix)]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(windows)]
fn same_file_identity(left: &std::fs::Metadata, right: &std::fs::Metadata) -> bool {
    left.volume_serial_number() == right.volume_serial_number()
        && left.file_index() == right.file_index()
}

#[cfg(not(any(unix, windows)))]
fn same_file_identity(_left: &std::fs::Metadata, _right: &std::fs::Metadata) -> bool {
    false
}

fn discovered_skill_path(resolved_path: &str, directory_name: &str) -> String {
    if resolved_path == "." {
        format!("{directory_name}/SKILL.md")
    } else {
        format!("{resolved_path}/{directory_name}/SKILL.md")
    }
}

fn normalize_workspace_relative_path(path: &str) -> String {
    let parts = std::path::Path::new(path)
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part.to_string_lossy().into_owned()),
            std::path::Component::CurDir => None,
            _ => None,
        })
        .collect::<Vec<_>>();
    if parts.is_empty() {
        ".".to_string()
    } else {
        parts.join("/")
    }
}

fn validate_skill_discovery_witness(
    witness: &AgentManifestSkillDiscovery,
    resolved_path: &str,
    registry_skill_names: &std::collections::BTreeSet<String>,
) -> crate::VerletResult<()> {
    if witness.path != resolved_path {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "stored skill discovery witness path {:?} does not match manifest path {:?}",
            witness.path, resolved_path
        )));
    }
    if witness.path.chars().any(char::is_control) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "stored skill discovery witness path {:?} is unsafe",
            witness.path
        )));
    }
    let mut names = registry_skill_names.clone();
    let path_prefix = if resolved_path == "." {
        String::new()
    } else {
        format!("{resolved_path}/")
    };
    let mut paths = std::collections::BTreeSet::new();
    for skill in &witness.skills {
        if skill.name.trim().is_empty()
            || skill.name.contains('/')
            || skill.name.contains('\0')
            || skill.name == "."
            || skill.name == ".."
            || skill.name.chars().any(char::is_control)
        {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill discovery witness contains unsafe skill name {:?}",
                skill.name
            )));
        }
        if !names.insert(skill.name.clone()) {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill discovery witness contains duplicate skill name {:?} across discovered entries or registry-bound skill packages",
                skill.name
            )));
        }
        if skill.description.trim().is_empty() || skill.description.chars().any(char::is_control) {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill discovery witness entry {:?} has an empty or unsafe description",
                skill.name
            )));
        }
        let hash = skill.content_sha256.strip_prefix("sha256:").unwrap_or("");
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill discovery witness entry {:?} has non-canonical content sha256 {:?}",
                skill.name, skill.content_sha256
            )));
        }
        let skill_path = std::path::Path::new(&skill.path);
        let unsafe_path = skill_path.is_absolute()
            || skill_path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            });
        let relative_path = if resolved_path == "." {
            Some(skill.path.as_str())
        } else {
            skill.path.strip_prefix(&path_prefix)
        };
        let direct_child_path = relative_path.and_then(|relative_path| {
            let mut components = std::path::Path::new(relative_path).components();
            let directory = match components.next() {
                Some(std::path::Component::Normal(directory)) => directory.to_str(),
                _ => None,
            }?;
            let filename = match components.next() {
                Some(std::path::Component::Normal(filename)) => filename.to_str(),
                _ => None,
            }?;
            if components.next().is_none()
                && filename == "SKILL.md"
                && !directory.chars().any(char::is_control)
                && skill.path == discovered_skill_path(resolved_path, directory)
            {
                Some(())
            } else {
                None
            }
        });
        if unsafe_path || !paths.insert(skill.path.as_str()) || direct_child_path.is_none() {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "stored skill discovery witness entry {:?} path {:?} is not a canonical direct child of discovery path {:?}",
                skill.name, skill.path, resolved_path
            )));
        }
    }
    if witness
        .skills
        .windows(2)
        .any(|pair| pair[0].name > pair[1].name)
    {
        return Err(crate::VerletError::RuntimeFactory(
            "stored skill discovery witness entries are not sorted by skill name".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_skill_discovery_witness_for_manifest(
    manifest: &crate::agent::manifest_schema::AgentManifestSchema,
    witness: Option<&AgentManifestSkillDiscovery>,
) -> crate::VerletResult<()> {
    if !manifest.skills.discover {
        if witness.is_some() {
            return Err(crate::VerletError::RuntimeFactory(
                "stored skill discovery witness exists, but the manifest disables workspace skill discovery"
                    .to_string(),
            ));
        }
        return Ok(());
    }
    let witness = witness.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(
            "manifest enables workspace skill discovery, but the durable bind receipt has no skill discovery witness"
                .to_string(),
        )
    })?;
    let resolved_path = normalize_workspace_relative_path(&manifest.skills.path);
    validate_skill_discovery_witness(witness, &resolved_path, &std::collections::BTreeSet::new())
}

fn skill_discovery_context_segment(
    discovery: &AgentManifestSkillDiscovery,
) -> AgentManifestStaticContextSegment {
    let mut content = String::new();
    for skill in &discovery.skills {
        content.push_str(&skill.name);
        content.push_str(" — ");
        content.push_str(&skill.description);
        content.push_str(" — ");
        content.push_str(&skill.path);
        content.push('\n');
    }
    let ref_uri = if discovery.path == "." {
        "workspace:///".to_string()
    } else {
        format!("workspace:///{}", discovery.path.trim_start_matches('/'))
    };
    AgentManifestStaticContextSegment {
        id: "skill-discovery-index".to_string(),
        assembler: crate::agent::manifest_schema::KERNEL_ASSEMBLER_STATIC.to_string(),
        input: discovery.path.clone(),
        pinned: true,
        budget_share: None,
        ref_uri,
        content_sha256: sha256_prefixed(content.as_bytes()),
        content,
    }
}

struct BoundCouplings {
    couplings: Vec<BoundCoupling>,
    grant_expiries: std::collections::BTreeMap<
        String,
        Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
    >,
    grant_bindings: Vec<AgentManifestGrantBindingReceipt>,
}

fn bind_couplings(
    couplings: &[crate::agent::manifest_schema::AgentManifestCoupling],
    operation_registry_root: Option<&std::path::Path>,
    now_ms: i64,
) -> crate::VerletResult<BoundCouplings> {
    let mut bound = Vec::new();
    let mut expiries = std::collections::BTreeMap::new();
    let mut grant_bindings = Vec::new();
    for coupling in couplings {
        let mut receipts =
            grant_binding_receipts("coupling", &coupling.id, &coupling.grants, now_ms)?;
        if receipts.iter().any(|receipt| receipt.lapsed_at_bind) {
            receipts
                .iter_mut()
                .for_each(|receipt| receipt.surface_excluded = true);
            grant_bindings.extend(receipts);
            continue;
        }
        let coupling_expiries = grant_expiries(&coupling.grants);
        if !coupling_expiries.is_empty() {
            expiries.insert(coupling.id.clone(), coupling_expiries);
        }
        bound.push(bind_coupling(coupling, operation_registry_root)?);
        grant_bindings.extend(receipts);
    }
    Ok(BoundCouplings {
        couplings: bound,
        grant_expiries: expiries,
        grant_bindings,
    })
}

fn bind_coupling(
    coupling: &crate::agent::manifest_schema::AgentManifestCoupling,
    operation_registry_root: Option<&std::path::Path>,
) -> crate::VerletResult<BoundCoupling> {
    let executor_kind =
        crate::kernel::coupling_executor_registry::registered_coupling_executor_for_id(
            &coupling.id,
        )
        .ok_or_else(|| {
            crate::VerletError::RuntimeFactory(format!(
                "no registered executor for coupling id {:?}",
                coupling.id
            ))
        })?;
    if executor_kind
        == crate::kernel::coupling_executor_registry::RegisteredCouplingExecutorKind::Wasm
        && !coupling.function_ref.starts_with("op://")
    {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "custom coupling {:?} function_ref {:?} must be an op:// Wasm operation ref",
            coupling.id, coupling.function_ref
        )));
    }
    let registry_root = operation_registry_root.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
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
        .collect::<crate::VerletResult<Vec<_>>>()?;
    let source_streams = source_selectors
        .iter()
        .map(|selector| selector.stream.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let sink = bind_coupling_sink(&coupling.id, &coupling.sink)?;
    if source_streams.contains(&sink.stream) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "coupling {:?} sink must not equal selected source stream {:?}",
            coupling.id, sink.stream
        )));
    }
    let role = if sink.stream == "control" {
        CouplingRole::Controller
    } else {
        CouplingRole::Projection
    };
    let grants = grant_capabilities(&coupling.grants);
    let verification = verify_operation_ref_for_subject(
        "coupling",
        &coupling.id,
        &coupling.function_ref,
        &grants,
        registry_root,
    )?;
    let operation_name = match executor_kind {
        crate::kernel::coupling_executor_registry::RegisteredCouplingExecutorKind::Stdlib => {
            verification.operation.clone()
        }
        crate::kernel::coupling_executor_registry::RegisteredCouplingExecutorKind::Wasm => {
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
) -> crate::VerletResult<Option<String>> {
    if !matches!(
        verification.record.source,
        crate::PublishedOperationSource::Wasm { .. }
    ) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} must resolve to a Wasm operation record"
        )));
    }
    let operation = selected_wasm_coupling_operation(coupling_id, function_ref, verification)?;
    if operation.input != verlet_abi::WasmOperationValueKind::Json {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} operation {:?} must declare json input for {COUPLING_INVOCATION_ABI}",
            operation.name,
            COUPLING_INVOCATION_ABI = verlet_abi::COUPLING_INVOCATION_ABI
        )));
    }
    if operation.output != verlet_abi::WasmOperationValueKind::Json {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "custom coupling {coupling_id:?} function_ref {function_ref:?} operation {:?} must declare json output for {COUPLING_DISCHARGE_ABI}",
            operation.name,
            COUPLING_DISCHARGE_ABI = verlet_abi::COUPLING_DISCHARGE_ABI
        )));
    }
    if !operation.required_capabilities.is_empty() {
        return Err(crate::VerletError::RuntimeFactory(format!(
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
) -> crate::VerletResult<&'a verlet_abi::WasmOperationDefinition> {
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
    Err(crate::VerletError::RuntimeFactory(format!(
        "custom coupling {coupling_id:?} function_ref {function_ref:?} must select one operation with op://<record>/<operation>@sha256:<hash>"
    )))
}

fn bind_coupling_source_selector(
    coupling_id: &str,
    selector: &crate::agent::manifest_schema::AgentManifestCouplingSelector,
) -> crate::VerletResult<BoundCouplingSelector> {
    let kinds = selector
        .kind
        .iter()
        .map(|kind| parse_coupling_event_kind(coupling_id, "source kind", kind))
        .collect::<crate::VerletResult<Vec<_>>>()?;
    Ok(BoundCouplingSelector {
        stream: selector.stream.clone(),
        kinds,
        scope: selector.scope.clone(),
        since: selector.since.clone(),
    })
}

fn bind_coupling_sink(
    coupling_id: &str,
    sink: &crate::agent::manifest_schema::AgentManifestCouplingSink,
) -> crate::VerletResult<BoundCouplingSink> {
    let kinds = sink
        .kind
        .iter()
        .map(|kind| parse_coupling_event_kind(coupling_id, "sink kind", kind))
        .collect::<crate::VerletResult<Vec<_>>>()?;
    Ok(BoundCouplingSink {
        stream: sink.stream.clone(),
        kinds,
    })
}

fn parse_coupling_event_kind(
    coupling_id: &str,
    label: &str,
    value: &str,
) -> crate::VerletResult<crate::EventKind> {
    value.parse::<crate::EventKind>().map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "coupling {coupling_id:?} {label} {value:?} is not in the kernel event-kind vocabulary: {err}"
        ))
    })
}

pub(crate) fn coupling_config_hash(value: &serde_json::Value) -> crate::VerletResult<String> {
    canonical_json_hash(value)
}

pub(crate) fn coupling_set_content_hash(
    coupling_set: &BoundCouplingSet,
) -> crate::VerletResult<String> {
    let mut couplings = coupling_set.couplings.iter().collect::<Vec<_>>();
    couplings.sort_by(|left, right| left.id.cmp(&right.id));
    let value = serde_json::Value::Array(
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

pub(crate) fn canonical_json_hash(value: &serde_json::Value) -> crate::VerletResult<String> {
    let mut canonical = Vec::new();
    write_canonical_json(value, &mut canonical)?;
    let digest = sha2::Sha256::digest(&canonical);
    Ok(format!("sha256:{digest:x}"))
}

fn sha256_prefixed(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

fn write_canonical_json(
    value: &serde_json::Value,
    output: &mut Vec<u8>,
) -> crate::VerletResult<()> {
    match value {
        serde_json::Value::Null => output.extend_from_slice(b"null"),
        serde_json::Value::Bool(true) => output.extend_from_slice(b"true"),
        serde_json::Value::Bool(false) => output.extend_from_slice(b"false"),
        serde_json::Value::Number(number) => {
            output.extend_from_slice(number.to_string().as_bytes())
        }
        serde_json::Value::String(string) => {
            serde_json::to_writer(output, string).map_err(|err| {
                crate::VerletError::RuntimeFactory(format!(
                    "failed to canonicalize coupling config: {err}"
                ))
            })?
        }
        serde_json::Value::Array(values) => {
            output.push(b'[');
            for (index, item) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_json(item, output)?;
            }
            output.push(b']');
        }
        serde_json::Value::Object(object) => {
            output.push(b'{');
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, item)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                serde_json::to_writer(&mut *output, key).map_err(|err| {
                    crate::VerletError::RuntimeFactory(format!(
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

fn grant_capabilities(grants: &[crate::agent::manifest_schema::AgentManifestGrant]) -> Vec<String> {
    grants
        .iter()
        .map(|grant| grant.capability().to_string())
        .collect()
}

fn grant_expiries(
    grants: &[crate::agent::manifest_schema::AgentManifestGrant],
) -> Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry> {
    grants
        .iter()
        .filter_map(|grant| grant.expiry().cloned())
        .collect()
}

fn grant_binding_receipts(
    subject_kind: &str,
    subject_id: &str,
    grants: &[crate::agent::manifest_schema::AgentManifestGrant],
    now_ms: i64,
) -> crate::VerletResult<Vec<AgentManifestGrantBindingReceipt>> {
    grants
        .iter()
        .map(|grant| {
            let expires_at = grant.expiry().map(|expiry| expiry.expires_at.clone());
            let lapsed_at_bind = match grant.expiry() {
                Some(expiry) => now_ms > grant_expiry_timestamp_ms(expiry)?,
                None => false,
            };
            Ok(AgentManifestGrantBindingReceipt {
                subject_kind: subject_kind.to_string(),
                subject_id: subject_id.to_string(),
                capability: grant.capability().to_string(),
                expires_at,
                lapsed_at_bind,
                surface_excluded: false,
            })
        })
        .collect()
}

pub(crate) fn grant_expiry_timestamp_ms(
    expiry: &crate::agent::manifest_schema::AgentManifestGrantExpiry,
) -> crate::VerletResult<i64> {
    chrono::DateTime::parse_from_rfc3339(&expiry.expires_at)
        .map(|instant| instant.timestamp_millis())
        .map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "grant {:?} has invalid RFC3339 expiry {:?}: {err}",
                expiry.capability, expiry.expires_at
            ))
        })
}

/// Enforce manifest authority at the consumption point. A running turn keeps
/// its bound form snapshot, but authority is live: once `now_ms` passes a
/// grant expiry, the next tool or coupling invocation fails closed.
pub(crate) fn ensure_grant_expiries_live(
    expiries: &[crate::agent::manifest_schema::AgentManifestGrantExpiry],
    now_ms: i64,
) -> crate::VerletResult<()> {
    let mut lapsed = Vec::new();
    for expiry in expiries {
        if now_ms > grant_expiry_timestamp_ms(expiry)? {
            lapsed.push(format!(
                "{} (expired at {})",
                expiry.capability, expiry.expires_at
            ));
        }
    }
    if lapsed.is_empty() {
        Ok(())
    } else {
        Err(crate::VerletError::RuntimeExecution(format!(
            "missing capability grants: {}",
            lapsed.join(", ")
        )))
    }
}

async fn bind_tools(
    tools: &[crate::agent::manifest_schema::AgentManifestTool],
    operation_registry_root: Option<&std::path::Path>,
    configured_mcp_server_refs: &std::collections::BTreeSet<String>,
    tool_universe_discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
    now_ms: i64,
) -> crate::VerletResult<BoundTools> {
    let mut tool_ids = Vec::new();
    let mut granted = std::collections::BTreeSet::new();
    let mut operation_bindings = OperationBindingMap::new();
    let mut direct_tool_names = std::collections::BTreeSet::new();
    let mut tool_universes = Vec::new();
    let mut grant_bindings = Vec::new();
    for tool in tools {
        match tool {
            crate::agent::manifest_schema::AgentManifestTool::Bash(tool) => {
                let mut receipts = grant_binding_receipts("tool", &tool.id, &tool.grants, now_ms)?;
                if receipts.iter().any(|receipt| receipt.lapsed_at_bind) {
                    receipts
                        .iter_mut()
                        .for_each(|receipt| receipt.surface_excluded = true);
                    grant_bindings.extend(receipts);
                    continue;
                }
                let grants = grant_capabilities(&tool.grants);
                let grant_expiries = grant_expiries(&tool.grants);
                bind_operation_ref_with_expiries(
                    &tool.id,
                    &tool.operation_ref,
                    &grants,
                    &grant_expiries,
                    tool.effect_class,
                    None,
                    operation_registry_root,
                    &mut granted,
                    &mut operation_bindings,
                )
                .await?;
                tool_ids.push(tool.id.clone());
                grant_bindings.extend(receipts);
            }
            crate::agent::manifest_schema::AgentManifestTool::Direct(tool) => {
                let mut receipts = grant_binding_receipts("tool", &tool.id, &tool.grants, now_ms)?;
                if receipts.iter().any(|receipt| receipt.lapsed_at_bind) {
                    receipts
                        .iter_mut()
                        .for_each(|receipt| receipt.surface_excluded = true);
                    grant_bindings.extend(receipts);
                    continue;
                }
                if !direct_tool_names.insert(tool.tool_name.clone()) {
                    return Err(crate::VerletError::RuntimeFactory(format!(
                        "duplicate direct tool_name surface {:?}",
                        tool.tool_name
                    )));
                }
                let grants = grant_capabilities(&tool.grants);
                let grant_expiries = grant_expiries(&tool.grants);
                bind_operation_ref_with_expiries(
                    &tool.id,
                    &tool.operation_ref,
                    &grants,
                    &grant_expiries,
                    tool.effect_class,
                    Some(&tool.tool_name),
                    operation_registry_root,
                    &mut granted,
                    &mut operation_bindings,
                )
                .await?;
                tool_ids.push(tool.id.clone());
                grant_bindings.extend(receipts);
            }
            crate::agent::manifest_schema::AgentManifestTool::ProtocolImport(tool) => {
                let mut receipts = grant_binding_receipts("tool", &tool.id, &tool.grants, now_ms)?;
                if receipts.iter().any(|receipt| receipt.lapsed_at_bind) {
                    receipts
                        .iter_mut()
                        .for_each(|receipt| receipt.surface_excluded = true);
                    grant_bindings.extend(receipts);
                    continue;
                }
                if !configured_mcp_server_refs.contains(&tool.server_ref) {
                    return Err(crate::VerletError::RuntimeFactory(format!(
                        "protocol tool {:?} server_ref {:?} is not configured",
                        tool.id, tool.server_ref
                    )));
                }
                let binding = bind_protocol_tool_import(tool, tool_universe_discoverer).await?;
                if let Some(pin) = &binding.pin
                    && !direct_tool_names.insert(pin.tool_name.clone())
                {
                    return Err(crate::VerletError::RuntimeFactory(format!(
                        "duplicate direct tool_name surface {:?}",
                        pin.tool_name
                    )));
                }
                granted.extend(
                    tool.grants
                        .iter()
                        .map(|grant| grant.capability().to_string()),
                );
                tool_universes.push(binding);
                tool_ids.push(tool.id.clone());
                grant_bindings.extend(receipts);
            }
        }
    }
    Ok(BoundTools {
        tool_ids,
        granted: granted.into_iter().collect(),
        grant_bindings,
        operation_bindings: operation_bindings_from_map(operation_bindings),
        tool_universes,
    })
}

fn enforce_child_agent_policy(
    manifest: &crate::agent::manifest_schema::AgentManifestSchema,
    operation_bindings: &[AgentManifestOperationBinding],
    couplings: &[BoundCoupling],
) -> crate::VerletResult<()> {
    if manifest.policies.allow_child_agents {
        return Ok(());
    }
    let tool_declares_thread_spawn = operation_bindings.iter().any(|binding| {
        binding.name == crate::VERLET_THREADS_PACKAGE
            && binding
                .grants
                .iter()
                .any(|grant| grant == crate::THREADS_SPAWN_CAPABILITY)
    });
    let coupling_declares_thread_spawn = couplings.iter().any(|coupling| {
        coupling.id == crate::STD_SUPERVISOR_SPAWN_TEMPLATE_ID
            && coupling
                .grants
                .iter()
                .any(|grant| grant == crate::THREADS_SPAWN_CAPABILITY)
    });
    let declares_thread_spawn = tool_declares_thread_spawn || coupling_declares_thread_spawn;
    if declares_thread_spawn {
        return Err(crate::VerletError::RuntimeFactory(
            "agent manifest policies.allow_child_agents = false but a child-thread operation or supervisor coupling grants threads.spawn; remove thread_spawn/std::supervisor.spawn or set allow_child_agents = true".to_string(),
        ));
    }
    Ok(())
}

struct SelectedManifestModelProfile<'a> {
    profile: &'a crate::agent::manifest_schema::AgentManifestModelProfile,
    provider_id: String,
    model_id: String,
    origin: AgentManifestModelProfileOrigin,
}

fn select_manifest_model_profile<'a>(
    manifest: &'a crate::agent::manifest_schema::AgentManifestSchema,
    selection: &AgentManifestModelProfileSelection,
) -> crate::VerletResult<SelectedManifestModelProfile<'a>> {
    if manifest.model_profiles.is_empty() {
        return Err(crate::VerletError::RuntimeFactory(
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
                origin: if selection.is_empty() {
                    AgentManifestModelProfileOrigin::ManifestDefault
                } else {
                    AgentManifestModelProfileOrigin::SelectedAtStart
                },
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
    manifest: &crate::agent::manifest_schema::AgentManifestSchema,
    selection: &AgentManifestModelProfileSelection,
) -> crate::VerletError {
    let requested = model_profile_selection_summary(selection);
    let declared = declared_model_profiles_summary(manifest).unwrap_or_else(|err| err.to_string());
    crate::VerletError::RuntimeFactory(format!(
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

fn declared_model_profiles_summary(
    manifest: &crate::agent::manifest_schema::AgentManifestSchema,
) -> crate::VerletResult<String> {
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
        .collect::<crate::VerletResult<Vec<_>>>()
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
                effect_class: binding.effect_class.unwrap_or_default(),
                grants: binding.grants.into_iter().collect(),
                grant_expiries: binding.grant_expiries.into_iter().collect(),
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
    tool: &crate::agent::manifest_schema::AgentManifestProtocolToolImport,
    discoverer: Option<&dyn crate::agent::tool_universe::ToolUniverseDiscoverer>,
) -> crate::VerletResult<crate::agent::tool_universe::ToolUniverseBinding> {
    let discoverer = discoverer.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "protocol tool import {:?} requires a tool universe discoverer; fail closed",
            tool.id
        ))
    })?;
    let discovery = discoverer.discover(&tool.server_ref).await?;
    if discovery.server_ref != tool.server_ref {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "protocol tool import {:?} discovery returned server_ref {:?}, expected {:?}; fail closed",
            tool.id, discovery.server_ref, tool.server_ref
        )));
    }
    let include_tools = tool.include_tools.as_ref().map(|tools| {
        tools
            .iter()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    });
    let discovery = match &include_tools {
        Some(include_tools) => discovery.filtered(include_tools)?,
        None => discovery,
    };
    let pin = tool
        .pin
        .as_ref()
        .map(|reference| crate::agent::tool_universe::PinnedToolRef::parse(reference))
        .transpose()?;
    let exposes_direct = tool
        .expose
        .contains(&crate::agent::manifest_schema::AgentManifestToolSurface::DirectTool);
    match (&pin, exposes_direct) {
        (None, true) => {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "protocol tool import {:?} declares expose = [\"direct_tool\"] without a pin; fail closed",
                tool.id
            )));
        }
        (Some(_), false) => {
            return Err(crate::VerletError::RuntimeFactory(format!(
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
            return Err(crate::VerletError::RuntimeFactory(format!(
                "protocol tool import {:?} pin drift for {:?}: expected schema hash {}, witnessed {}; fail closed",
                tool.id, pin.tool_name, pin.schema_hash, witnessed_hash
            )));
        }
    }
    let binding = crate::agent::tool_universe::ToolUniverseBinding {
        import_id: tool.id.clone(),
        server_ref: tool.server_ref.clone(),
        effect_class: tool.effect_class,
        include_tools,
        pin,
        grant_expiries: grant_expiries(&tool.grants),
        discovery,
    };
    binding.validate()?;
    Ok(binding)
}

#[cfg(test)]
async fn bind_operation_ref(
    tool_id: &str,
    operation_ref: &str,
    grants: &[String],
    direct_tool_name: Option<&str>,
    operation_registry_root: Option<&std::path::Path>,
    granted: &mut std::collections::BTreeSet<String>,
    operation_bindings: &mut OperationBindingMap,
) -> crate::VerletResult<()> {
    bind_operation_ref_with_expiries(
        tool_id,
        operation_ref,
        grants,
        &[],
        crate::agent::manifest_schema::EffectClass::AtMostOnce,
        direct_tool_name,
        operation_registry_root,
        granted,
        operation_bindings,
    )
    .await
}

async fn bind_operation_ref_with_expiries(
    tool_id: &str,
    operation_ref: &str,
    grants: &[String],
    grant_expiries: &[crate::agent::manifest_schema::AgentManifestGrantExpiry],
    effect_class: crate::agent::manifest_schema::EffectClass,
    direct_tool_name: Option<&str>,
    operation_registry_root: Option<&std::path::Path>,
    granted: &mut std::collections::BTreeSet<String>,
    operation_bindings: &mut OperationBindingMap,
) -> crate::VerletResult<()> {
    let registry_root = operation_registry_root.ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
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
            Ok::<AgentManifestDirectToolBinding, crate::VerletError>(
                AgentManifestDirectToolBinding {
                    tool_name: tool_name.to_string(),
                    operation,
                    effect_class,
                    grant_expiries: grant_expiries.to_vec(),
                },
            )
        })
        .transpose()?;
    granted.extend(verification.grants.iter().cloned());
    operation_bindings
        .entry((verification.name, verification.artifact_hash))
        .or_default()
        .merge_with_expiries(
            verification.grants,
            grant_expiries.iter().cloned().collect(),
            verification.operation,
            direct_tool_binding,
            effect_class,
        );
    Ok(())
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedOperationRef {
    pub(crate) name: String,
    pub(crate) artifact_hash: String,
    pub(crate) operation: Option<String>,
    pub(crate) grants: std::collections::BTreeSet<String>,
    pub(crate) record: crate::PublishedOperationRecord,
}

pub(crate) fn verify_operation_ref(
    tool_id: &str,
    operation_ref: &str,
    grants: &[String],
    operation_registry_root: &std::path::Path,
) -> crate::VerletResult<VerifiedOperationRef> {
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
    operation_registry_root: &std::path::Path,
) -> crate::VerletResult<VerifiedOperationRef> {
    let parsed = parse_operation_ref(operation_ref)?;
    let artifact_hash = parsed.artifact_hash.clone().ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} must be content-addressed with @sha256:<hash>; for agent publish, pass --resolve-ops to pin op:// authoring refs from the operations registry"
        ))
    })?;
    let registry = crate::LocalOperationRegistry::new(operation_registry_root);
    registry.load_record(&parsed.name).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} was not found in the local operation registry: {err}; seed the operation registry or fix the op:// record name"
        ))
    })?;
    let record = registry
        .load_version_record(&parsed.name, &artifact_hash)
        .map_err(|err| {
            crate::VerletError::RuntimeFactory(format!(
                "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} names artifact hash sha256:{artifact_hash} that is not a published version in the local operation registry: {err}; republish the operation or replace the ref with a hash from the registry"
            ))
        })?;
    let granted_set = grants
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
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
        return Err(crate::VerletError::RuntimeFactory(format!(
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
) -> crate::VerletResult<String> {
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
    Err(crate::VerletError::RuntimeFactory(format!(
        "direct tool {tool_id:?} operation_ref {operation_ref:?} must select one operation with op://<record>/<operation>@sha256:<hash>"
    )))
}

fn unknown_operation_ref_error(
    subject_kind: &str,
    subject_id: &str,
    operation_ref: &str,
    record_name: &str,
    record: &crate::PublishedOperationRecord,
) -> crate::VerletError {
    let available = record
        .manifest
        .operations
        .iter()
        .map(|operation| operation.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    crate::VerletError::RuntimeFactory(format!(
        "{subject_kind} {subject_id:?} operation_ref {operation_ref:?} selects an operation that is not in record {record_name:?}; use op://<record>@sha256:<hash> for the whole record or op://<record>/<operation>@sha256:<hash> for one operation; available operations: {}",
        if available.is_empty() {
            "<none>"
        } else {
            &available
        }
    ))
}

fn provider_id_from_ref(provider_ref: &str) -> crate::VerletResult<String> {
    let id = provider_ref.strip_prefix("provider://").ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "provider_ref {provider_ref:?} must start with provider://"
        ))
    })?;
    if id.is_empty() {
        return Err(crate::VerletError::RuntimeFactory(
            "provider_ref must include a provider id".to_string(),
        ));
    }
    Ok(id.to_string())
}

fn model_id_from_ref(model_ref: &str, provider_id: &str) -> crate::VerletResult<String> {
    let id = model_ref.strip_prefix("model://").ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "model_ref {model_ref:?} must start with model://"
        ))
    })?;
    if id.is_empty() {
        return Err(crate::VerletError::RuntimeFactory(
            "model_ref must include a model id".to_string(),
        ));
    }
    if let Some((provider, model)) = id.split_once('/') {
        if provider != provider_id {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "model_ref {model_ref:?} names provider {provider:?}, expected {provider_id:?}"
            )));
        }
        if model.is_empty() {
            return Err(crate::VerletError::RuntimeFactory(format!(
                "model_ref {model_ref:?} must include a model id"
            )));
        }
        return Ok(model.to_string());
    }
    Ok(id.to_string())
}

fn parse_operation_ref(operation_ref: &str) -> crate::VerletResult<OperationRef> {
    let body = operation_ref.strip_prefix("op://").ok_or_else(|| {
        crate::VerletError::RuntimeFactory(format!(
            "operation_ref {operation_ref:?} must start with op://"
        ))
    })?;
    let (name, artifact_hash) = match body.split_once("@sha256:") {
        Some((name, hash)) => {
            if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(crate::VerletError::RuntimeFactory(format!(
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
) -> crate::VerletResult<(String, Option<String>)> {
    let grammar = "op://<record>@sha256:<hash> or op://<record>/<operation>@sha256:<hash>";
    let segments = body.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        [record] if !record.is_empty() => Ok(((*record).to_string(), None)),
        [record, operation] if !record.is_empty() && !operation.is_empty() => {
            Ok(((*record).to_string(), Some((*operation).to_string())))
        }
        _ => Err(crate::VerletError::RuntimeFactory(format!(
            "operation_ref {operation_ref:?} must match {grammar}"
        ))),
    }
}

fn override_key_name(
    key: crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey,
) -> &'static str {
    match key {
        crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::DefaultCwd => "default_cwd",
        crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::Streaming => "streaming",
        crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::TurnTimeoutMs => "turn_timeout_ms",
        crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::CancellationGraceMs => "cancellation_grace_ms",
        crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::MaxToolRounds => "max_tool_rounds",
        crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes => {
            "compaction.auto_at_text_bytes"
        }
    }
}

fn require_override_key(
    allowlist: &[crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey],
    key: crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey,
) -> crate::VerletResult<&'static str> {
    let name = override_key_name(key);
    if allowlist.contains(&key) {
        Ok(name)
    } else {
        Err(crate::VerletError::RuntimeFactory(format!(
            "runtime override {name:?} is not allowlisted by the agent manifest"
        )))
    }
}

fn validate_optional_positive_u64(label: &str, value: Option<u64>) -> crate::VerletResult<()> {
    if value == Some(0) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "runtime override {label:?} must be > 0"
        )));
    }
    Ok(())
}

fn validate_tool_round_budget(
    label: &str,
    value: Option<crate::agent::manifest_schema::AgentManifestMaxToolRounds>,
) -> crate::VerletResult<()> {
    if value == Some(crate::agent::manifest_schema::AgentManifestMaxToolRounds::Limited(0)) {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "runtime override {label:?} must be > 0 or \"unlimited\""
        )));
    }
    Ok(())
}

/// Payload of the discharged `manifest.compile.completed` event: which
/// immutable manifest this thread compiled, and through which alias it was
/// reached, if any.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifestCompileReceipt {
    pub ref_uri: String,
    pub manifest_hash: String,
    pub source_hash: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alias: Option<crate::agent::manifest::AgentAliasResolutionReceipt>,
}

/// Payload of the discharged `manifest.bind.completed` event: what the
/// thread can actually do. An audit answers "what could this agent do for
/// this run" from this receipt alone.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifestBindReceipt {
    pub ref_uri: String,
    pub manifest_hash: String,
    /// The selected declared profile for this bind.
    pub model_profile_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile_origin: Option<AgentManifestModelProfileOrigin>,
    pub provider_id: String,
    pub model_id: String,
    pub tool_ids: Vec<String>,
    /// Exact operation artifacts mounted for this manifest-backed thread.
    #[serde(default)]
    pub operation_bindings: Vec<AgentManifestOperationBinding>,
    /// Exact skill packages mounted for this manifest-backed thread.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skill_packages: Vec<AgentManifestSkillPackageBinding>,
    /// Workspace skill entries traversed exactly once through the resolved
    /// workspace binding. `None` means discovery was disabled or this is a
    /// legacy receipt from before discovery witnesses existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub skill_discovery: Option<AgentManifestSkillDiscovery>,
    /// Exact static context sources mounted as provider system blocks.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub static_context_segments: Vec<AgentManifestStaticContextSegment>,
    /// Witnessed tool universes mounted on the search surface: server ref,
    /// discovery hash, in-scope contracts, and pinned rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_universes: Vec<crate::agent::tool_universe::ToolUniverseBindReceipt>,
    /// Resolved coupling functions that can observe or alter this thread's
    /// future behavior.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub couplings: Vec<AgentManifestCouplingBinding>,
    /// The union of effect grants on the bound tool bindings.
    pub granted: Vec<String>,
    /// Per-row expiry witness for manifest tool and coupling grants. Expired
    /// rows remain here even though their runtime surface was excluded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_bindings: Vec<AgentManifestGrantBindingReceipt>,
    /// Runtime defaults after allowlisted overrides were applied.
    pub effective_runtime: crate::agent::manifest_schema::AgentManifestRuntimeDefaults,
    /// Which override keys the caller actually exercised.
    pub overridden_keys: Vec<String>,
    /// Where this thread's runtime executes, fixed at bind time (ADR 0006).
    /// Absent means local. Optional with a serde default so receipts
    /// witnessed before the field existed keep decoding and folding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement: Option<AgentManifestPlacementBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub placement_origin: Option<AgentManifestBindingOrigin>,
    /// Effective host workspace mount fixed for this bind. Optional so bind
    /// receipts written before workspace binding existed keep decoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<AgentManifestResolvedWorkspaceMount>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_origin: Option<AgentManifestBindingOrigin>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentManifestModelProfileOrigin {
    ManifestDefault,
    SelectedAtStart,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentManifestBindingOrigin {
    DaemonDefault,
    BindOverride,
    Manifest,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestGrantBindingReceipt {
    pub subject_kind: String,
    pub subject_id: String,
    pub capability: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<String>,
    #[serde(default, skip_serializing_if = "is_false")]
    pub lapsed_at_bind: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub surface_excluded: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// The placement resolved for a manifest-backed thread at bind time.
///
/// Placement attaches at the binding — or the conductor boundary call that
/// creates one — never inline in a model-visible tool call. The manifest
/// itself carries no placement (a manifest is portable by construction);
/// daemon config supplies deployment defaults and operator surfaces may
/// override at bind time. ADR 0006 requires the resolved binding target to be
/// witnessed with the existing `placement.decision` event.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifestPlacementBinding {
    pub target: crate::kernel::control_decision::PlacementTarget,
    /// Which registered executor serves a non-local target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor_ref: Option<String>,
    /// Executor-specific configuration, opaque to the bind layer.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub config: std::collections::BTreeMap<String, serde_json::Value>,
}

impl Default for AgentManifestPlacementBinding {
    fn default() -> Self {
        Self {
            target: crate::kernel::control_decision::PlacementTarget::Local,
            executor_ref: None,
            config: std::collections::BTreeMap::new(),
        }
    }
}

/// Resolve daemon placement defaults and an optional operator bind override.
///
/// Remote placement requires a sync endpoint that has completed its bind and
/// is being served by this daemon generation. Merely parsing `[daemon.sync]`
/// is not authority to open the gate. Sandbox remains fail-closed until its
/// executor lands.
pub fn resolve_manifest_placement(
    default_placement: Option<&AgentManifestPlacementBinding>,
    placement_override: Option<&AgentManifestPlacementBinding>,
    remote_event_store_served: bool,
) -> crate::VerletResult<AgentManifestPlacementBinding> {
    Ok(resolve_manifest_placement_with_origin(
        default_placement,
        placement_override,
        remote_event_store_served,
    )?
    .binding)
}

struct ResolvedManifestPlacement {
    binding: AgentManifestPlacementBinding,
    origin: AgentManifestBindingOrigin,
}

fn resolve_manifest_placement_with_origin(
    default_placement: Option<&AgentManifestPlacementBinding>,
    placement_override: Option<&AgentManifestPlacementBinding>,
    remote_event_store_served: bool,
) -> crate::VerletResult<ResolvedManifestPlacement> {
    let (resolved, origin) = match placement_override {
        Some(placement) => (placement.clone(), AgentManifestBindingOrigin::BindOverride),
        None => (
            default_placement.cloned().unwrap_or_default(),
            AgentManifestBindingOrigin::DaemonDefault,
        ),
    };
    if resolved.target == crate::kernel::control_decision::PlacementTarget::Remote
        && remote_event_store_served
    {
        return Ok(ResolvedManifestPlacement {
            binding: resolved,
            origin,
        });
    }
    if resolved.target != crate::kernel::control_decision::PlacementTarget::Local {
        let target = match resolved.target {
            crate::kernel::control_decision::PlacementTarget::Local => "local",
            crate::kernel::control_decision::PlacementTarget::Remote => "remote",
            crate::kernel::control_decision::PlacementTarget::Sandbox => "sandbox",
        };
        return Err(crate::VerletError::RuntimeFactory(format!(
            "placement target {target} requires the remote EventStore backend capability, which is not available"
        )));
    }
    Ok(ResolvedManifestPlacement {
        binding: resolved,
        origin,
    })
}

/// Machine-local workspace authority supplied by daemon config or an
/// operator bind-time override.
///
/// This type is never part of the content-addressed manifest and is never a
/// model-facing tool argument. `host_path` may be relative on daemon config
/// input; bind resolution canonicalizes it before writing the receipt.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestWorkspaceBinding {
    #[serde(alias = "hostPath")]
    pub host_path: std::path::PathBuf,
    pub mode: crate::agent::manifest_schema::AgentManifestWorkspaceMode,
}

/// Effective workspace mount witnessed by `manifest.bind.completed` and
/// persisted in thread lifecycle metadata for restart and fork recovery.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AgentManifestResolvedWorkspaceMount {
    pub guest_path: std::path::PathBuf,
    pub host_path: std::path::PathBuf,
    pub mode: crate::agent::manifest_schema::AgentManifestWorkspaceMode,
}

impl AgentManifestResolvedWorkspaceMount {
    pub fn binding(&self) -> AgentManifestWorkspaceBinding {
        AgentManifestWorkspaceBinding {
            host_path: self.host_path.clone(),
            mode: self.mode,
        }
    }
}

/// Resolve an abstract manifest workspace requirement against operator
/// bindings. An override wins over the daemon default. Both sides are
/// fail-closed: required-without-binding and binding-without-declaration are
/// errors, and the supplied mode must satisfy the declared floor.
pub fn resolve_manifest_workspace(
    requirement: Option<&crate::agent::manifest_schema::AgentManifestWorkspaceRequirement>,
    default_workspace: Option<&AgentManifestWorkspaceBinding>,
    workspace_override: Option<&AgentManifestWorkspaceBinding>,
) -> crate::VerletResult<Option<AgentManifestResolvedWorkspaceMount>> {
    Ok(
        resolve_manifest_workspace_with_origin(requirement, default_workspace, workspace_override)?
            .map(|workspace| workspace.mount),
    )
}

struct ResolvedManifestWorkspace {
    mount: AgentManifestResolvedWorkspaceMount,
    origin: AgentManifestBindingOrigin,
}

fn resolve_manifest_workspace_with_origin(
    requirement: Option<&crate::agent::manifest_schema::AgentManifestWorkspaceRequirement>,
    default_workspace: Option<&AgentManifestWorkspaceBinding>,
    workspace_override: Option<&AgentManifestWorkspaceBinding>,
) -> crate::VerletResult<Option<ResolvedManifestWorkspace>> {
    let (binding, origin) = match workspace_override {
        Some(binding) => (Some(binding), AgentManifestBindingOrigin::BindOverride),
        None => (default_workspace, AgentManifestBindingOrigin::DaemonDefault),
    };
    let (requirement, binding) = match (requirement, binding) {
        (None, None) => return Ok(None),
        (Some(_), None) => {
            return Err(crate::VerletError::RuntimeFactory(
                "agent manifest requires a workspace binding, but neither the bind override nor daemon default supplied one"
                    .to_string(),
            ));
        }
        (None, Some(_)) => {
            return Err(crate::VerletError::RuntimeFactory(
                "workspace binding was supplied, but the agent manifest did not declare a workspace requirement"
                    .to_string(),
            ));
        }
        (Some(requirement), Some(binding)) => (requirement, binding),
    };

    if binding.mode < requirement.min_mode {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "workspace binding mode {} does not satisfy manifest minimum mode {}",
            workspace_mode_name(binding.mode),
            workspace_mode_name(requirement.min_mode)
        )));
    }
    let host_path = std::fs::canonicalize(&binding.host_path).map_err(|err| {
        crate::VerletError::RuntimeFactory(format!(
            "workspace host path {} could not be resolved: {err}",
            binding.host_path.display()
        ))
    })?;
    if !host_path.is_dir() {
        return Err(crate::VerletError::RuntimeFactory(format!(
            "workspace host path {} is not a directory",
            host_path.display()
        )));
    }
    Ok(Some(ResolvedManifestWorkspace {
        mount: AgentManifestResolvedWorkspaceMount {
            guest_path: std::path::PathBuf::from(&requirement.guest_path),
            host_path,
            mode: binding.mode,
        },
        origin,
    }))
}

fn workspace_mode_name(
    mode: crate::agent::manifest_schema::AgentManifestWorkspaceMode,
) -> &'static str {
    match mode {
        crate::agent::manifest_schema::AgentManifestWorkspaceMode::ReadOnly => "ro",
        crate::agent::manifest_schema::AgentManifestWorkspaceMode::ReadWrite => "rw",
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestSkillDiscovery {
    /// Normalized path relative to the witnessed workspace root.
    pub path: String,
    pub skills: Vec<AgentManifestDiscoveredSkill>,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestDiscoveredSkill {
    pub name: String,
    /// Workspace-relative path to the live skill body.
    pub path: String,
    pub content_sha256: String,
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingBinding {
    pub id: String,
    pub role: CouplingRole,
    pub trigger_kind: String,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub trigger_match: std::collections::BTreeMap<String, serde_json::Value>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_expiries: Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
    pub budget: crate::agent::manifest_schema::AgentManifestCouplingBudget,
    pub config_hash: String,
}

impl AgentManifestCouplingBinding {
    fn from_bound(
        coupling: &BoundCoupling,
        grant_expiries: Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
    ) -> Self {
        let source_streams = coupling
            .source_selectors
            .iter()
            .map(|selector| selector.stream.clone())
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let source_kinds = coupling
            .source_selectors
            .iter()
            .flat_map(|selector| selector.kinds.iter().map(|kind| kind.to_string()))
            .collect::<std::collections::BTreeSet<_>>()
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
            grant_expiries,
            budget: coupling.budget.clone(),
            config_hash: coupling.config_hash.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestOperationBinding {
    pub name: String,
    pub artifact_hash: String,
    #[serde(
        default,
        skip_serializing_if = "crate::agent::manifest_schema::EffectClass::is_at_most_once"
    )]
    pub effect_class: crate::agent::manifest_schema::EffectClass,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grants: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_expiries: Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
    /// Empty means the binding exposes the whole record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<String>,
    /// Direct model/tool-router aliases declared by manifest `direct_tool` rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub direct_tools: Vec<AgentManifestDirectToolBinding>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestDirectToolBinding {
    pub tool_name: String,
    pub operation: String,
    #[serde(
        default,
        skip_serializing_if = "crate::agent::manifest_schema::EffectClass::is_at_most_once"
    )]
    pub effect_class: crate::agent::manifest_schema::EffectClass,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub grant_expiries: Vec<crate::agent::manifest_schema::AgentManifestGrantExpiry>,
}

/// Apply caller overrides onto the manifest's runtime defaults, enforcing
/// the deny-by-default allowlist. Returns the effective defaults plus the
/// list of keys actually overridden, for the bind receipt.
pub fn apply_runtime_overrides(
    defaults: &crate::agent::manifest_schema::AgentManifestRuntimeDefaults,
    overrides: &AgentManifestBindOverrides,
) -> crate::VerletResult<(
    crate::agent::manifest_schema::AgentManifestRuntimeDefaults,
    Vec<String>,
)> {
    validate_optional_positive_u64("turn_timeout_ms", overrides.turn_timeout_ms)?;
    validate_optional_positive_u64("cancellation_grace_ms", overrides.cancellation_grace_ms)?;
    validate_tool_round_budget("max_tool_rounds", overrides.max_tool_rounds)?;
    validate_optional_positive_u64(
        "compaction.auto_at_text_bytes",
        overrides.compaction_auto_at_text_bytes,
    )?;

    let allowlist = defaults.overrides.allow.clone();
    let mut effective = defaults.clone();
    let mut overridden_keys = Vec::new();
    if let Some(value) = &overrides.default_cwd {
        let key = require_override_key(
            &allowlist,
            crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::DefaultCwd,
        )?;
        effective.default_cwd = value.clone();
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.streaming {
        let key = require_override_key(
            &allowlist,
            crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::Streaming,
        )?;
        effective.streaming = value;
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.turn_timeout_ms {
        let key = require_override_key(
            &allowlist,
            crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::TurnTimeoutMs,
        )?;
        effective.turn_timeout_ms = Some(value);
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.cancellation_grace_ms {
        let key = require_override_key(
            &allowlist,
            crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::CancellationGraceMs,
        )?;
        effective.cancellation_grace_ms = Some(value);
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.max_tool_rounds {
        let key = require_override_key(
            &allowlist,
            crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::MaxToolRounds,
        )?;
        effective.max_tool_rounds = Some(value);
        overridden_keys.push(key.to_string());
    }
    if let Some(value) = overrides.compaction_auto_at_text_bytes {
        let key = require_override_key(
            &allowlist,
            crate::agent::manifest_schema::AgentManifestRuntimeOverrideKey::CompactionAutoAtTextBytes,
        )?;
        effective.compaction.auto_at_text_bytes = Some(value);
        overridden_keys.push(key.to_string());
    }
    Ok((effective, overridden_keys))
}

#[cfg(test)]
mod tests;
