//! Typed V1 AgentManifest schema.
//!
//! Shape and vocabulary are decided in `docs/agent-manifest-ontology.md` and
//! bounded by the V1 slice in `docs/v1-release-candidate.md`. Everything here
//! is fail-closed: unknown
//! keys, unknown enum values, and reserved deferred sections are compile
//! errors, never silent passes.
//!
//! This module owns the declared shape only. Registry records, publish
//! plumbing, and alias resolution live in `agent::manifest`; the compiled
//! plan produced from this schema is recorded there as well.

use crate::tool_ref::PinnedToolRef;
use crate::{CooldisAgentError as CooldisError, CooldisResult};
use cooldis_operations::{DeclaredSkillPackageRef, validate_record_name};
use serde::de::{self, DeserializeOwned, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, Path, PathBuf};

/// Top-level sections reserved by the ontology but deferred from the V1
/// schema. Compile rejects each by name so the error states the deferral
/// instead of reporting an unknown key.
pub const RESERVED_MANIFEST_SECTIONS: &[&str] =
    &["views", "hooks", "topology", "io", "persistence"];

/// Resource kinds reserved for later versions (audit section 5). Compile
/// rejects them with an error naming the deferral.
pub const RESERVED_RESOURCE_KINDS: &[&str] = &["dataset", "index"];

/// Kernel built-in assembler refs accepted by V1 context sources (audit
/// section 7). Any other assembler ref fails closed.
pub const KERNEL_ASSEMBLER_STATIC: &str = "kernel://assembler/static";
pub const KERNEL_ASSEMBLER_RECORD_SELECT: &str = "kernel://assembler/record-select";
pub const KERNEL_ASSEMBLER_ANCHORED_WINDOW: &str = "kernel://assembler/anchored-window";

/// The fully parsed and validated V1 manifest. Sections that may be omitted
/// from the source document carry their decided defaults here; consumers
/// never re-derive defaults.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestSchema {
    pub identity: AgentManifestIdentity,
    /// Ordered list; the first profile is the default (audit section 2).
    pub model_profiles: Vec<AgentManifestModelProfile>,
    pub tools: Vec<AgentManifestTool>,
    pub resources: Vec<AgentManifestResource>,
    /// Abstract host-workspace requirement. The portable manifest names only
    /// the guest path and minimum access mode; bind-time operator surfaces
    /// supply the machine-local host directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<AgentManifestWorkspaceRequirement>,
    /// Optional workspace skill discovery. The schema default is off so
    /// manifests must opt in explicitly before bind traverses the workspace.
    #[serde(default, skip_serializing_if = "AgentManifestSkills::is_default")]
    pub skills: AgentManifestSkills,
    /// `None` means the source omitted `[context]`; use
    /// [`AgentManifestSchema::effective_context_pipeline`] to get the
    /// synthesized default instead of reading this directly.
    pub context: Option<AgentManifestContextPipeline>,
    #[serde(default)]
    pub couplings: Vec<AgentManifestCoupling>,
    pub policies: AgentManifestPolicies,
    pub runtime: AgentManifestRuntimeDefaults,
}

impl AgentManifestSchema {
    /// Parse a manifest TOML document into the typed V1 schema and validate
    /// it. Reserved sections are rejected with errors naming the deferral;
    /// unknown keys anywhere fail closed.
    pub fn from_toml_value(value: &toml::Value) -> CooldisResult<Self> {
        let manifest = Self::from_toml_value_unvalidated(value)?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Parse a manifest TOML document into the typed V1 schema before
    /// cross-field validation. This is only for kernel lowerers that fill in
    /// source-derived fields before running [`AgentManifestSchema::validate`].
    #[doc(hidden)]
    pub fn from_toml_value_unvalidated(value: &toml::Value) -> CooldisResult<Self> {
        let table = value.as_table().ok_or_else(|| {
            CooldisError::RuntimeFactory("agent manifest must be a TOML table".to_string())
        })?;
        for key in table.keys() {
            if RESERVED_MANIFEST_SECTIONS.contains(&key.as_str()) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "agent manifest section {key:?} is reserved for a deferred V1 scope"
                )));
            }
            if !matches!(
                key.as_str(),
                "agent"
                    | "model_profiles"
                    | "tools"
                    | "resources"
                    | "workspace"
                    | "skills"
                    | "context"
                    | "couplings"
                    | "policies"
                    | "runtime"
            ) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "unknown top-level agent manifest section {key:?}"
                )));
            }
        }

        reject_reserved_resource_kinds(value)?;
        validate_raw_grant_shapes(value)?;

        let identity = required_section(value, "agent")?;
        let model_profiles = optional_section(value, "model_profiles")?.unwrap_or_default();
        let tools = optional_section(value, "tools")?.unwrap_or_default();
        let resources = optional_section(value, "resources")?.unwrap_or_default();
        let workspace = optional_section(value, "workspace")?;
        let skills = optional_section(value, "skills")?.unwrap_or_default();
        let couplings = optional_section(value, "couplings")?.unwrap_or_default();
        let context = match value.get("context") {
            Some(section) => {
                let context: AgentManifestContextToml = decode_section(section.clone(), "context")?;
                if context.pipelines.len() != 1 {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "agent manifest context requires exactly one pipeline named \"default\", got {}",
                        context.pipelines.len()
                    )));
                }
                context.pipelines.into_iter().next()
            }
            None => None,
        };
        let policies = optional_section(value, "policies")?.unwrap_or_default();
        let runtime = optional_section(value, "runtime")?.unwrap_or_default();
        let manifest = Self {
            identity,
            model_profiles,
            tools,
            resources,
            workspace,
            skills,
            context,
            couplings,
            policies,
            runtime,
        };
        Ok(manifest)
    }

    /// Cross-field validation: unique ids, budget-share arithmetic, grant
    /// shapes, ref shapes, override allowlist keys. Field-level shape errors
    /// are already rejected at parse time.
    pub fn validate(&self) -> CooldisResult<()> {
        validate_record_name(&self.identity.name)?;
        if let Some(namespace) = &self.identity.namespace {
            validate_namespace(namespace)?;
        }
        if let Some(version) = &self.identity.version {
            validate_version(version)?;
        }
        if let Some(kind) = &self.identity.kind
            && kind != "cooldis.agent-manifest"
        {
            return Err(CooldisError::RuntimeFactory(format!(
                "agent manifest kind must be \"cooldis.agent-manifest\", got {kind:?}"
            )));
        }
        if let Some(schema_version) = self.identity.schema_version
            && schema_version != 1
        {
            return Err(CooldisError::RuntimeFactory(format!(
                "agent manifest schema_version {schema_version} is not supported"
            )));
        }

        if self.model_profiles.is_empty() {
            return Err(CooldisError::RuntimeFactory(
                "agent manifest requires at least one model profile".to_string(),
            ));
        }
        let mut model_ids = BTreeSet::new();
        for profile in &self.model_profiles {
            validate_record_name(&profile.id)?;
            if !model_ids.insert(profile.id.clone()) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "duplicate model profile id {:?}",
                    profile.id
                )));
            }
            validate_ref_scheme(
                "model profile provider_ref",
                &profile.provider_ref,
                "provider://",
            )?;
            validate_ref_scheme("model profile model_ref", &profile.model_ref, "model://")?;
            if let Some(credentials) = &profile.credentials {
                validate_ref_scheme(
                    "model profile credential ref",
                    &credentials.reference,
                    "credential://",
                )?;
            }
            if let Some(retry) = &profile.retry {
                if retry.max_attempts == 0 {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "model profile {:?} retry.max_attempts must be > 0",
                        profile.id
                    )));
                }
                if retry.backoff_ms == Some(0) {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "model profile {:?} retry.backoff_ms must be > 0",
                        profile.id
                    )));
                }
            }
            if profile.params.max_tokens == Some(0) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "model profile {:?} params.max_tokens must be > 0",
                    profile.id
                )));
            }
            for fallback in &profile.fallbacks {
                validate_ref_scheme(
                    "model fallback provider_ref",
                    &fallback.provider_ref,
                    "provider://",
                )?;
                validate_ref_scheme("model fallback model_ref", &fallback.model_ref, "model://")?;
            }
        }

        let mut tool_ids = BTreeSet::new();
        let mut commands = BTreeSet::new();
        let mut tool_names = BTreeSet::new();
        for tool in &self.tools {
            let (id, reference, grants) = match tool {
                AgentManifestTool::Bash(tool) => {
                    validate_record_name(&tool.id)?;
                    validate_surface("bash command", &tool.command)?;
                    if !commands.insert(tool.command.clone()) {
                        return Err(CooldisError::RuntimeFactory(format!(
                            "duplicate bash command surface {:?}",
                            tool.command
                        )));
                    }
                    validate_ref_scheme("bash tool operation_ref", &tool.operation_ref, "op://")?;
                    (&tool.id, &tool.operation_ref, &tool.grants)
                }
                AgentManifestTool::Direct(tool) => {
                    validate_record_name(&tool.id)?;
                    validate_surface("direct tool_name", &tool.tool_name)?;
                    if !tool_names.insert(tool.tool_name.clone()) {
                        return Err(CooldisError::RuntimeFactory(format!(
                            "duplicate direct tool_name surface {:?}",
                            tool.tool_name
                        )));
                    }
                    validate_ref_scheme("direct tool operation_ref", &tool.operation_ref, "op://")?;
                    (&tool.id, &tool.operation_ref, &tool.grants)
                }
                AgentManifestTool::ProtocolImport(tool) => {
                    validate_record_name(&tool.id)?;
                    validate_ref_scheme("protocol tool server_ref", &tool.server_ref, "mcp://")?;
                    if tool
                        .server_ref
                        .strip_prefix("mcp://")
                        .is_some_and(|body| body.contains('@'))
                    {
                        return Err(CooldisError::RuntimeFactory(format!(
                            "protocol tool server_ref {:?} is content-addressed, but protocol \
                             source refs name placement by configured source; the only legal \
                             form is mcp://<source-name>. Content-addressing belongs to per-tool \
                             contract pins (mcptool://...@sha256:<hash>), not the source record",
                            tool.server_ref
                        )));
                    }
                    if tool.expose.contains(&AgentManifestToolSurface::BashTool) {
                        return Err(CooldisError::RuntimeFactory(format!(
                            "protocol tool import {:?} expose surface \"bash_tool\" is deferred; \
                             live universes mount as the search surface",
                            tool.id
                        )));
                    }
                    // The lexicon's tool-row law, enforced mechanically:
                    // nothing mutable backs a tool row, so a direct row
                    // requires an accepted, content-addressed contract.
                    if tool.expose.contains(&AgentManifestToolSurface::DirectTool)
                        && tool.pin.is_none()
                    {
                        return Err(CooldisError::RuntimeFactory(format!(
                            "protocol tool import {:?} declares expose = [\"direct_tool\"] \
                             without a pin; a live universe contract cannot back a tool row — \
                             pin the witnessed contract (mcptool://<server>/<tool>@sha256:<hash>) \
                             or drop the expose and use the search surface",
                            tool.id
                        )));
                    }
                    if tool.pin.is_some()
                        && !tool.expose.contains(&AgentManifestToolSurface::DirectTool)
                    {
                        return Err(CooldisError::RuntimeFactory(format!(
                            "protocol tool import {:?} declares a pin without expose = [\"direct_tool\"]",
                            tool.id
                        )));
                    }
                    if let Some(pin) = &tool.pin {
                        let parsed = PinnedToolRef::parse(pin)?;
                        if !tool_names.insert(parsed.tool_name.clone()) {
                            return Err(CooldisError::RuntimeFactory(format!(
                                "duplicate direct tool_name surface {:?}",
                                parsed.tool_name
                            )));
                        }
                        if parsed.server_ref() != tool.server_ref {
                            return Err(CooldisError::RuntimeFactory(format!(
                                "protocol tool import {:?} pin names server {:?} but the import \
                                 declares server_ref {:?}",
                                tool.id,
                                parsed.server_ref(),
                                tool.server_ref
                            )));
                        }
                    }
                    if let Some(include_tools) = &tool.include_tools {
                        if include_tools.is_empty()
                            || include_tools.iter().any(|name| name.trim().is_empty())
                        {
                            return Err(CooldisError::RuntimeFactory(format!(
                                "protocol tool import {:?} include_tools must be non-empty tool names",
                                tool.id
                            )));
                        }
                    }
                    (&tool.id, &tool.server_ref, &tool.grants)
                }
            };
            let _ = reference;
            if !tool_ids.insert(id.clone()) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "duplicate tool id {id:?}"
                )));
            }
            validate_grants(id, grants)?;
        }

        let mut resource_names = BTreeSet::new();
        for resource in &self.resources {
            validate_record_name(&resource.name)?;
            if !resource_names.insert(resource.name.clone()) {
                return Err(CooldisError::RuntimeFactory(format!(
                    "duplicate resource name {:?}",
                    resource.name
                )));
            }
            match resource.kind {
                AgentManifestResourceKind::Blob => {
                    validate_ref_scheme("blob resource ref", &resource.reference, "resource://")?;
                }
                AgentManifestResourceKind::Skill => {
                    validate_skill_resource_ref(&resource.reference)?;
                }
            }
        }

        if let Some(workspace) = &self.workspace {
            validate_workspace_requirement(workspace)?;
        }
        validate_skill_discovery(&self.skills, self.workspace.as_ref())?;

        if let Some(context) = &self.context {
            validate_context_pipeline(context)?;
        }
        validate_couplings(&self.couplings)?;

        if self.policies.budgets.max_turns == Some(0) {
            return Err(CooldisError::RuntimeFactory(
                "policies.budgets.max_turns must be > 0".to_string(),
            ));
        }
        if self.policies.budgets.max_tool_calls_per_turn == Some(0) {
            return Err(CooldisError::RuntimeFactory(
                "policies.budgets.max_tool_calls_per_turn must be > 0".to_string(),
            ));
        }
        if self.runtime.turn_timeout_ms == Some(0) {
            return Err(CooldisError::RuntimeFactory(
                "runtime.turn_timeout_ms must be > 0".to_string(),
            ));
        }
        if self.runtime.cancellation_grace_ms == Some(0) {
            return Err(CooldisError::RuntimeFactory(
                "runtime.cancellation_grace_ms must be > 0".to_string(),
            ));
        }
        if self.runtime.max_tool_rounds == Some(AgentManifestMaxToolRounds::Limited(0)) {
            return Err(CooldisError::RuntimeFactory(
                "runtime.max_tool_rounds must be > 0 or \"unlimited\"".to_string(),
            ));
        }
        if self.runtime.compaction.auto_at_text_bytes == Some(0) {
            return Err(CooldisError::RuntimeFactory(
                "runtime.compaction.auto_at_text_bytes must be > 0".to_string(),
            ));
        }
        Ok(())
    }

    /// The effective context pipeline: the declared one, or the kernel
    /// default (identity + history) when `[context]` is absent.
    pub fn effective_context_pipeline(&self) -> AgentManifestContextPipeline {
        self.context
            .clone()
            .unwrap_or_else(default_context_pipeline)
    }
}

/// The kernel default pipeline synthesized when a manifest omits
/// `[context]`: a pinned static identity source plus an anchored-window
/// history source taking the rest of the budget (audit section 7).
pub fn default_context_pipeline() -> AgentManifestContextPipeline {
    AgentManifestContextPipeline {
        id: "default".to_string(),
        sources: vec![
            AgentManifestContextSource {
                id: "identity".to_string(),
                assembler: KERNEL_ASSEMBLER_STATIC.to_string(),
                input: None,
                select: None,
                budget_share: None,
                pinned: true,
            },
            AgentManifestContextSource {
                id: "history".to_string(),
                assembler: KERNEL_ASSEMBLER_ANCHORED_WINDOW.to_string(),
                input: None,
                select: Some(AgentManifestContextSelector {
                    stream: Some("thread".to_string()),
                    since: Some("anchor|start".to_string()),
                    ..AgentManifestContextSelector::default()
                }),
                budget_share: Some(AgentManifestBudgetShare::Rest(
                    AgentManifestBudgetRest::Rest,
                )),
                pinned: false,
            },
        ],
    }
}

/// Durable name and version envelope (audit section 1). `kind` is the object
/// discriminator `cooldis.agent-manifest`, never the agent's role; roles live
/// in `labels`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestIdentity {
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub schema_version: Option<u32>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default)]
    pub publisher: Option<AgentManifestPublisher>,
}

/// Opaque publisher metadata (audit section 1). Cooldis V0 does not
/// authenticate publishers; product auth projects into this later.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestPublisher {
    pub id: String,
    #[serde(default)]
    pub display_name: Option<String>,
}

/// One named model/provider profile (audit section 2). The manifest stores
/// credential references and policy, never secret material.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestModelProfile {
    pub id: String,
    /// `provider://...` catalog foreign key. Shape-validated at publish;
    /// enforced against the live provider store at bind.
    pub provider_ref: String,
    /// `model://...` catalog foreign key, same enforcement split.
    pub model_ref: String,
    #[serde(default)]
    pub params: AgentManifestModelParams,
    #[serde(default)]
    pub credentials: Option<AgentManifestCredentialRef>,
    #[serde(default)]
    pub retry: Option<AgentManifestModelRetryPolicy>,
    #[serde(default)]
    pub fallbacks: Vec<AgentManifestModelFallback>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestModelParams {
    #[serde(default)]
    pub max_tokens: Option<u32>,
    #[serde(default)]
    pub temperature: Option<f64>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
}

/// A `credential://...` reference resolved inside the runtime boundary.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCredentialRef {
    #[serde(rename = "ref")]
    pub reference: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestModelRetryPolicy {
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestModelFallback {
    pub provider_ref: String,
    pub model_ref: String,
}

/// The three V1 tool declaration types (audit section 4). Wasm/V8/MCP are
/// implementation or import substrates behind these, never manifest types.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum AgentManifestTool {
    #[serde(rename = "bash_tool")]
    Bash(AgentManifestBashTool),
    #[serde(rename = "direct_tool")]
    Direct(AgentManifestDirectTool),
    #[serde(rename = "protocol_tool_import")]
    ProtocolImport(AgentManifestProtocolToolImport),
}

/// A command exposed inside virtual bash, backed by a published operation.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestBashTool {
    pub id: String,
    pub command: String,
    /// `op://...` operation artifact reference.
    pub operation_ref: String,
    /// Effect grants ride on the binding that uses them (audit section 10);
    /// there is no manifest-global grant pool.
    #[serde(default)]
    pub grants: Vec<AgentManifestGrant>,
}

/// A structured model/tool-router call exposed outside bash.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestDirectTool {
    pub id: String,
    pub tool_name: String,
    /// `op://...` operation artifact reference.
    pub operation_ref: String,
    #[serde(default)]
    pub grants: Vec<AgentManifestGrant>,
}

/// A protocol-shaped tool universe mounted through the search surface. The
/// per-tool contracts are discovered from the server and witnessed at bind;
/// the manifest declares the universe, its filters, and (for direct rows)
/// the pinned contract. The lexicon law governs: nothing mutable backs a
/// tool row, so `expose = ["direct_tool"]` without a `pin` fails compile.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestProtocolToolImport {
    pub id: String,
    pub protocol: AgentManifestToolProtocol,
    /// Name-only `mcp://<source-name>` server configuration reference. This
    /// names placement; content-addressing belongs to per-tool contract pins
    /// (`mcptool://...@sha256:<hash>`), not the source record.
    pub server_ref: String,
    /// Which surfaces imported tools may be exposed on. Empty means the
    /// search surface only (the default for live universes).
    #[serde(default)]
    pub expose: Vec<AgentManifestToolSurface>,
    /// `mcptool://<server>/<tool>@sha256:<hash>` — the accepted, content-
    /// addressed contract backing a direct row. Required when `expose`
    /// names `direct_tool`; one pin per import (one pin = one row).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin: Option<String>,
    /// Manifest-level restriction of the universe to the named tools,
    /// intersected with the source record's own filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_tools: Option<Vec<String>>,
    #[serde(default)]
    pub grants: Vec<AgentManifestGrant>,
}

/// One effect grant on a manifest tool or coupling row. The untagged string
/// variant preserves the exact V1 wire shape and content hash for manifests
/// that do not opt into expiry.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AgentManifestGrant {
    Capability(String),
    Expiring(AgentManifestGrantExpiry),
}

impl AgentManifestGrant {
    pub fn capability(&self) -> &str {
        match self {
            Self::Capability(capability) => capability,
            Self::Expiring(grant) => &grant.capability,
        }
    }

    pub fn expiry(&self) -> Option<&AgentManifestGrantExpiry> {
        match self {
            Self::Capability(_) => None,
            Self::Expiring(grant) => Some(grant),
        }
    }
}

impl From<String> for AgentManifestGrant {
    fn from(capability: String) -> Self {
        Self::Capability(capability)
    }
}

impl From<&str> for AgentManifestGrant {
    fn from(capability: &str) -> Self {
        Self::Capability(capability.to_string())
    }
}

/// Absolute UTC expiry attached to one manifest capability grant.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestGrantExpiry {
    pub capability: String,
    pub expires_at: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestToolProtocol {
    #[serde(rename = "mcp")]
    Mcp,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestToolSurface {
    #[serde(rename = "direct_tool")]
    DirectTool,
    #[serde(rename = "bash_tool")]
    BashTool,
}

/// A declared read-only artifact (audit section 5). Declaring a resource
/// grants nothing by itself: visibility comes from a pipeline source.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestResource {
    pub name: String,
    pub kind: AgentManifestResourceKind,
    /// `resource://...` or `skill://...`, content-addressed or resolving to
    /// content-addressed at publish.
    #[serde(rename = "ref")]
    pub reference: String,
    #[serde(default)]
    pub mount: AgentManifestResourceMount,
    #[serde(default)]
    pub mode: AgentManifestResourceMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestResourceKind {
    #[serde(rename = "blob")]
    Blob,
    /// Published markdown skill package mounted into context as an index and
    /// into VFS as read-only bodies.
    #[serde(rename = "skill")]
    Skill,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestResourceMount {
    #[default]
    #[serde(rename = "context")]
    Context,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestResourceMode {
    #[default]
    #[serde(rename = "read")]
    Read,
}

/// Portable declaration that this agent requires a host-backed workspace.
///
/// The requirement deliberately contains no host path. `guest_path` is the
/// absolute path visible inside virtual bash and mounted operations;
/// `min_mode` is the least authority an operator binding must provide.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestWorkspaceRequirement {
    pub guest_path: String,
    #[serde(default)]
    pub min_mode: AgentManifestWorkspaceMode,
}

/// Layer-2 declaration for conventional workspace skill discovery.
/// Discovery is intentionally opt-in and never creates a separate mount.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestSkills {
    #[serde(default)]
    pub discover: bool,
    #[serde(default = "default_skill_discovery_path")]
    pub path: String,
}

impl AgentManifestSkills {
    fn is_default(&self) -> bool {
        self == &Self::default()
    }
}

impl Default for AgentManifestSkills {
    fn default() -> Self {
        Self {
            discover: false,
            path: default_skill_discovery_path(),
        }
    }
}

fn default_skill_discovery_path() -> String {
    ".agents/skills".to_string()
}

/// Access modes shared by workspace requirements, operator bindings, and
/// resolved mount receipts. Read-write satisfies a read-only mode floor.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub enum AgentManifestWorkspaceMode {
    #[default]
    #[serde(rename = "ro")]
    ReadOnly,
    #[serde(rename = "rw")]
    ReadWrite,
}

/// How model-visible context is assembled (audit section 7). Assemblers
/// propose; the kernel performs the deterministic merge, the final budget
/// fit, and the receipt. V1 accepts exactly one pipeline with id "default"
/// and only `kernel://` assembler refs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestContextPipeline {
    pub id: String,
    pub sources: Vec<AgentManifestContextSource>,
}

/// One independent source: an assembler ref paired with a selector and a
/// budget share. Validation: source ids unique, at most one `rest` share,
/// fractional shares sum to <= 1, pinned sources excluded from budget
/// arithmetic.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestContextSource {
    pub id: String,
    pub assembler: String,
    /// Static-assembler input, a declared resource name or resource ref.
    #[serde(default)]
    pub input: Option<String>,
    #[serde(default)]
    pub select: Option<AgentManifestContextSelector>,
    #[serde(default)]
    pub budget_share: Option<AgentManifestBudgetShare>,
    #[serde(default)]
    pub pinned: bool,
}

/// Selector shape shared with future couplings (stream, kind, scope, since).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestContextSelector {
    #[serde(default)]
    pub kind: Vec<String>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
}

/// A declared projection or controller coupling. The role is inferred by the
/// binder from resolved source/sink stream relation; authors never declare it.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCoupling {
    pub id: String,
    pub function_ref: String,
    #[serde(default)]
    pub grants: Vec<AgentManifestGrant>,
    pub trigger: AgentManifestCouplingTrigger,
    pub source: AgentManifestCouplingSource,
    pub sink: AgentManifestCouplingSink,
    #[serde(default)]
    pub budget: AgentManifestCouplingBudget,
    #[serde(default)]
    pub config: JsonValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingTrigger {
    pub kind: String,
    #[serde(default, rename = "match")]
    pub match_fields: BTreeMap<String, JsonValue>,
    #[serde(default)]
    pub quota: AgentManifestCouplingQuota,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingQuota {
    /// Maximum non-skipped coupling runs admitted during one scheduler cycle.
    #[serde(default)]
    pub per_turn: Option<u32>,
    /// Maximum non-skipped coupling runs admitted over the thread lifetime.
    /// The kernel derives this count from the thread journal instead of a
    /// separate persisted quota store.
    #[serde(default)]
    pub per_thread: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingSource {
    pub selectors: Vec<AgentManifestCouplingSelector>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingSelector {
    pub stream: String,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    pub kind: Vec<String>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingSink {
    pub stream: String,
    #[serde(default, deserialize_with = "deserialize_string_list")]
    pub kind: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCouplingBudget {
    #[serde(default)]
    pub max_ms: Option<u64>,
    #[serde(default)]
    pub max_discharge_events: Option<u32>,
}

/// A fractional share of the context budget, or the single `"rest"` source
/// that takes whatever remains.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AgentManifestBudgetShare {
    Fraction(f64),
    Rest(AgentManifestBudgetRest),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestBudgetRest {
    #[serde(rename = "rest")]
    Rest,
}

/// Thread-level authority boundary (audit section 10). The manifest declares
/// requirements, the operator grants them, the runtime enforces fail-closed.
/// Effect grants live on tool bindings, not here.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestPolicies {
    #[serde(default)]
    pub network: AgentManifestNetworkPolicy,
    #[serde(default)]
    pub filesystem: AgentManifestFilesystemPolicy,
    #[serde(default)]
    pub allow_child_agents: bool,
    #[serde(default)]
    pub budgets: AgentManifestPolicyBudgets,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestNetworkPolicy {
    #[default]
    #[serde(rename = "deny")]
    Deny,
    /// Network reachable only through origins declared by tool grants.
    #[serde(rename = "declared-origins")]
    DeclaredOrigins,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestFilesystemPolicy {
    #[default]
    #[serde(rename = "vfs")]
    Vfs,
    #[serde(rename = "none")]
    None,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestPolicyBudgets {
    #[serde(default)]
    pub max_turns: Option<u32>,
    #[serde(default)]
    pub max_tool_calls_per_turn: Option<u32>,
}

/// Defaults applied when a thread starts from the manifest (audit section
/// 15). V1 ships exactly these keys; unknown `[runtime]` keys fail closed.
/// Anything not in the override allowlist is fixed by the manifest.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestRuntimeDefaults {
    #[serde(default = "default_runtime_cwd")]
    pub default_cwd: String,
    #[serde(default = "default_runtime_streaming")]
    pub streaming: bool,
    #[serde(default)]
    pub turn_timeout_ms: Option<u64>,
    #[serde(default)]
    pub cancellation_grace_ms: Option<u64>,
    /// Maximum model/tool batches in one turn. `None` means the manifest
    /// omitted the field and the kernel default applies; `Unlimited` is an
    /// explicit opt-in that still leaves the other turn budgets in force.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tool_rounds: Option<AgentManifestMaxToolRounds>,
    #[serde(default)]
    pub compaction: AgentManifestCompactionDefaults,
    #[serde(default)]
    pub overrides: AgentManifestRuntimeOverridePolicy,
}

impl Default for AgentManifestRuntimeDefaults {
    fn default() -> Self {
        Self {
            default_cwd: default_runtime_cwd(),
            streaming: default_runtime_streaming(),
            turn_timeout_ms: None,
            cancellation_grace_ms: None,
            max_tool_rounds: None,
            compaction: AgentManifestCompactionDefaults::default(),
            overrides: AgentManifestRuntimeOverridePolicy::default(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AgentManifestMaxToolRounds {
    Limited(usize),
    Unlimited,
}

impl Serialize for AgentManifestMaxToolRounds {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Limited(rounds) => serializer.serialize_u64(*rounds as u64),
            Self::Unlimited => serializer.serialize_str("unlimited"),
        }
    }
}

impl<'de> Deserialize<'de> for AgentManifestMaxToolRounds {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MaxToolRoundsVisitor;

        impl de::Visitor<'_> for MaxToolRoundsVisitor {
            type Value = AgentManifestMaxToolRounds;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a non-negative integer or the string \"unlimited\"")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                usize::try_from(value)
                    .map(AgentManifestMaxToolRounds::Limited)
                    .map_err(|_| E::custom("max_tool_rounds does not fit this platform"))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                u64::try_from(value)
                    .map_err(|_| E::custom("max_tool_rounds cannot be negative"))
                    .and_then(|value| self.visit_u64(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value == "unlimited" {
                    Ok(AgentManifestMaxToolRounds::Unlimited)
                } else {
                    Err(E::unknown_variant(value, &["unlimited"]))
                }
            }
        }

        deserializer.deserialize_any(MaxToolRoundsVisitor)
    }
}

fn default_runtime_cwd() -> String {
    "workspace".to_string()
}

fn default_runtime_streaming() -> bool {
    true
}

/// Compaction is a built-in coupling in V1, configured here rather than
/// declared under the deferred `[[couplings]]` section. `None` means the
/// kernel default threshold.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestCompactionDefaults {
    #[serde(default)]
    pub auto_at_text_bytes: Option<u64>,
}

/// Deny-by-default override allowlist for `thread/start` callers.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentManifestRuntimeOverridePolicy {
    #[serde(default)]
    pub allow: Vec<AgentManifestRuntimeOverrideKey>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestRuntimeOverrideKey {
    #[serde(rename = "default_cwd")]
    DefaultCwd,
    #[serde(rename = "streaming")]
    Streaming,
    #[serde(rename = "turn_timeout_ms")]
    TurnTimeoutMs,
    #[serde(rename = "cancellation_grace_ms")]
    CancellationGraceMs,
    #[serde(rename = "max_tool_rounds")]
    MaxToolRounds,
    #[serde(rename = "compaction.auto_at_text_bytes")]
    CompactionAutoAtTextBytes,
}

/// One declared-ref -> resolved-ref entry in a compiled manifest plan.
/// `cooldis agent plan` is offline-allowed and may record unresolved refs
/// explicitly; `cooldis agent publish` fails closed on any unresolved
/// operation, resource, or alias ref.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentManifestResolvedRef {
    pub declared: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_hash: Option<String>,
    pub status: AgentManifestRefStatus,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentManifestRefStatus {
    #[serde(rename = "resolved")]
    Resolved,
    /// Recorded only by offline `agent plan`; never present in a published
    /// record.
    #[serde(rename = "unresolved-offline")]
    UnresolvedOffline,
}

impl AgentManifestResolvedRef {
    pub fn validate(&self) -> CooldisResult<()> {
        validate_compile_time_artifact_ref(&self.declared)?;
        match self.status {
            AgentManifestRefStatus::Resolved => {
                let Some(content_hash) = &self.content_hash else {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "resolved artifact ref {:?} is missing content_hash",
                        self.declared
                    )));
                };
                validate_hash_label("content_hash", content_hash)?;
                let resolved = self.resolved.as_deref().unwrap_or(&self.declared);
                if resolved != self.declared {
                    validate_compile_time_artifact_ref(resolved)?;
                }
                let expected_hash = content_hash_from_ref(resolved)
                    .or_else(|| content_hash_from_ref(&self.declared))
                    .ok_or_else(|| {
                        CooldisError::RuntimeFactory(format!(
                            "resolved artifact ref {:?} must resolve to a content-addressed ref",
                            self.declared
                        ))
                    })?;
                if content_hash != &expected_hash {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "resolved artifact ref {:?} records content_hash {}, expected {}",
                        self.declared, content_hash, expected_hash
                    )));
                }
            }
            AgentManifestRefStatus::UnresolvedOffline => {
                if self.resolved.is_some() || self.content_hash.is_some() {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "unresolved artifact ref {:?} must not include resolved content",
                        self.declared
                    )));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentManifestContextToml {
    #[serde(default)]
    pipelines: Vec<AgentManifestContextPipeline>,
}

fn required_section<T: DeserializeOwned>(value: &toml::Value, name: &str) -> CooldisResult<T> {
    let section = value
        .get(name)
        .ok_or_else(|| CooldisError::RuntimeFactory(format!("agent manifest requires [{name}]")))?;
    decode_section(section.clone(), name)
}

fn optional_section<T: DeserializeOwned>(
    value: &toml::Value,
    name: &str,
) -> CooldisResult<Option<T>> {
    value
        .get(name)
        .map(|section| decode_section(section.clone(), name))
        .transpose()
}

fn decode_section<T: DeserializeOwned>(value: toml::Value, name: &str) -> CooldisResult<T> {
    value.try_into().map_err(|err| {
        CooldisError::RuntimeFactory(format!("invalid agent manifest [{name}] section: {err}"))
    })
}

fn reject_reserved_resource_kinds(value: &toml::Value) -> CooldisResult<()> {
    let Some(resources) = value.get("resources").and_then(toml::Value::as_array) else {
        return Ok(());
    };
    for resource in resources {
        if let Some(kind) = resource.get("kind").and_then(toml::Value::as_str)
            && RESERVED_RESOURCE_KINDS.contains(&kind)
        {
            return Err(CooldisError::RuntimeFactory(format!(
                "resource kind {kind:?} is reserved for a deferred V1 resource scope"
            )));
        }
    }
    Ok(())
}

fn validate_raw_grant_shapes(value: &toml::Value) -> CooldisResult<()> {
    for (section, subject_kind) in [("tools", "tool"), ("couplings", "coupling")] {
        let Some(subjects) = value.get(section).and_then(toml::Value::as_array) else {
            continue;
        };
        for (subject_index, subject) in subjects.iter().enumerate() {
            let subject_id = subject
                .get("id")
                .and_then(toml::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| format!("<row {subject_index}>"));
            let Some(grants) = subject.get("grants").and_then(toml::Value::as_array) else {
                continue;
            };
            for (grant_index, grant) in grants.iter().enumerate() {
                if grant.is_str() {
                    continue;
                }
                let Some(grant) = grant.as_table() else {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "{subject_kind} {subject_id:?} grant {grant_index} must be a capability string or an expiry object, got {}",
                        toml_value_kind(grant)
                    )));
                };
                if let Some(field) = grant
                    .keys()
                    .find(|field| !matches!(field.as_str(), "capability" | "expires_at"))
                {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "{subject_kind} {subject_id:?} grant {grant_index} object has unknown field {field:?}"
                    )));
                }
                for field in ["capability", "expires_at"] {
                    match grant.get(field) {
                        Some(toml::Value::String(_)) => {}
                        Some(value) => {
                            let expected = if field == "expires_at" {
                                "a quoted RFC3339 UTC string"
                            } else {
                                "a string"
                            };
                            return Err(CooldisError::RuntimeFactory(format!(
                                "{subject_kind} {subject_id:?} grant {grant_index} {field} must be {expected}, got {}",
                                toml_value_kind(value)
                            )));
                        }
                        None => {
                            return Err(CooldisError::RuntimeFactory(format!(
                                "{subject_kind} {subject_id:?} grant {grant_index} object requires {field:?}"
                            )));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn toml_value_kind(value: &toml::Value) -> &'static str {
    match value {
        toml::Value::String(_) => "string",
        toml::Value::Integer(_) => "integer",
        toml::Value::Float(_) => "float",
        toml::Value::Boolean(_) => "boolean",
        toml::Value::Datetime(_) => "datetime",
        toml::Value::Array(_) => "array",
        toml::Value::Table(_) => "table",
    }
}

fn validate_ref_scheme(label: &str, value: &str, scheme: &str) -> CooldisResult<()> {
    let rest = value.strip_prefix(scheme).ok_or_else(|| {
        CooldisError::RuntimeFactory(format!("{label} {value:?} must start with {scheme}"))
    })?;
    if rest.is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "{label} {value:?} must include a reference body"
        )));
    }
    Ok(())
}

fn validate_skill_resource_ref(value: &str) -> CooldisResult<()> {
    DeclaredSkillPackageRef::parse(value)
        .map(|_| ())
        .map_err(|err| CooldisError::RuntimeFactory(err.to_string()))
}

fn validate_artifact_ref(value: &str) -> CooldisResult<()> {
    if value.starts_with("skill://") {
        return DeclaredSkillPackageRef::parse(value)
            .map(|_| ())
            .map_err(|err| CooldisError::RuntimeFactory(err.to_string()));
    }
    if (value.starts_with("op://") && value.len() > "op://".len())
        || (value.starts_with("mcp://") && value.len() > "mcp://".len())
        || (value.starts_with("resource://") && value.len() > "resource://".len())
    {
        Ok(())
    } else {
        Err(CooldisError::RuntimeFactory(format!(
            "agent artifact ref {value:?} must start with op://, mcp://, resource://, or skill://"
        )))
    }
}

fn validate_compile_time_artifact_ref(value: &str) -> CooldisResult<()> {
    if value.starts_with("skill://") {
        return match DeclaredSkillPackageRef::parse(value)? {
            DeclaredSkillPackageRef::Floating { .. } => Err(CooldisError::RuntimeFactory(format!(
                "floating skill ref {value:?} resolves only at bind time and must not appear in compile-time resolved_refs"
            ))),
            DeclaredSkillPackageRef::Pinned(_) => Ok(()),
        };
    }
    validate_artifact_ref(value)
}

fn content_hash_from_ref(reference: &str) -> Option<String> {
    if reference.starts_with("skill://") {
        return match DeclaredSkillPackageRef::parse(reference).ok()? {
            DeclaredSkillPackageRef::Floating { .. } => None,
            DeclaredSkillPackageRef::Pinned(reference) => {
                Some(format!("sha256:{}", reference.artifact_hash))
            }
        };
    }
    if let Some(hash) = reference.strip_prefix("resource://artifact/sha256:")
        && hash.len() == 64
    {
        let content_hash = format!("sha256:{hash}");
        validate_hash_label("content_hash", &content_hash).ok()?;
        return Some(content_hash);
    }
    let (_prefix, hash) = reference.rsplit_once("@sha256:")?;
    if hash.len() != 64 {
        return None;
    }
    let content_hash = format!("sha256:{hash}");
    validate_hash_label("content_hash", &content_hash).ok()?;
    Some(content_hash)
}

fn validate_hash_label(label: &str, value: &str) -> CooldisResult<()> {
    let Some(hash) = value.strip_prefix("sha256:") else {
        return Err(CooldisError::RuntimeFactory(format!(
            "{label} must start with sha256:"
        )));
    };
    validate_hex_hash(hash).map_err(|err| {
        CooldisError::RuntimeFactory(format!("{label} must be sha256:<64 lowercase hex>: {err}"))
    })
}

fn validate_hex_hash(value: &str) -> Result<(), &'static str> {
    if value.len() != 64 {
        return Err("wrong length");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("invalid character");
    }
    Ok(())
}

fn validate_surface(label: &str, value: &str) -> CooldisResult<()> {
    if value.trim().is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "{label} cannot be empty"
        )));
    }
    Ok(())
}

fn validate_grants(id: &str, grants: &[AgentManifestGrant]) -> CooldisResult<()> {
    for grant in grants {
        if grant.capability().trim().is_empty() {
            return Err(CooldisError::RuntimeFactory(format!(
                "tool {id:?} has an empty grant"
            )));
        }
        if let Some(expiry) = grant.expiry() {
            validate_grant_expiry("tool", id, expiry)?;
        }
    }
    Ok(())
}

fn validate_couplings(couplings: &[AgentManifestCoupling]) -> CooldisResult<()> {
    let mut ids = BTreeSet::new();
    for coupling in couplings {
        validate_coupling_id(&coupling.id)?;
        if !ids.insert(coupling.id.clone()) {
            return Err(CooldisError::RuntimeFactory(format!(
                "duplicate coupling id {:?}",
                coupling.id
            )));
        }
        validate_ref_scheme("coupling function_ref", &coupling.function_ref, "op://")?;
        validate_coupling_grants(&coupling.id, &coupling.grants)?;
        validate_coupling_event_kind("coupling trigger kind", &coupling.trigger.kind)?;
        validate_positive_optional_u32(
            "coupling trigger quota.per_turn",
            coupling.trigger.quota.per_turn,
        )?;
        validate_positive_optional_u32(
            "coupling trigger quota.per_thread",
            coupling.trigger.quota.per_thread,
        )?;
        if coupling.source.selectors.is_empty() {
            return Err(CooldisError::RuntimeFactory(format!(
                "coupling {:?} source requires at least one selector",
                coupling.id
            )));
        }
        let mut source_streams = BTreeSet::new();
        for selector in &coupling.source.selectors {
            validate_coupling_stream("coupling source stream", &selector.stream)?;
            source_streams.insert(selector.stream.clone());
            if selector.kind.is_empty() {
                return Err(CooldisError::RuntimeFactory(format!(
                    "coupling {:?} source selector for stream {:?} requires at least one kind",
                    coupling.id, selector.stream
                )));
            }
            for kind in &selector.kind {
                validate_coupling_event_kind("coupling source kind", kind)?;
            }
        }
        validate_coupling_stream("coupling sink stream", &coupling.sink.stream)?;
        if source_streams.contains(&coupling.sink.stream) {
            return Err(CooldisError::RuntimeFactory(format!(
                "coupling {:?} sink must not equal selected source stream {:?}",
                coupling.id, coupling.sink.stream
            )));
        }
        if coupling.sink.kind.is_empty() {
            return Err(CooldisError::RuntimeFactory(format!(
                "coupling {:?} sink requires at least one kind",
                coupling.id
            )));
        }
        for kind in &coupling.sink.kind {
            validate_coupling_event_kind("coupling sink kind", kind)?;
        }
        validate_positive_optional_u64("coupling budget.max_ms", coupling.budget.max_ms)?;
        validate_positive_optional_u32(
            "coupling budget.max_discharge_events",
            coupling.budget.max_discharge_events,
        )?;
    }
    Ok(())
}

fn validate_coupling_id(id: &str) -> CooldisResult<()> {
    if id.is_empty()
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        return Err(CooldisError::RuntimeFactory(format!(
            "coupling id {id:?} must use ASCII letters, numbers, '.', '_', '-', or ':'"
        )));
    }
    Ok(())
}

fn validate_coupling_grants(id: &str, grants: &[AgentManifestGrant]) -> CooldisResult<()> {
    for grant in grants {
        if grant.capability().trim().is_empty() {
            return Err(CooldisError::RuntimeFactory(format!(
                "coupling {id:?} has an empty grant"
            )));
        }
        if let Some(expiry) = grant.expiry() {
            validate_grant_expiry("coupling", id, expiry)?;
        }
    }
    Ok(())
}

fn validate_grant_expiry(
    subject_kind: &str,
    subject_id: &str,
    expiry: &AgentManifestGrantExpiry,
) -> CooldisResult<()> {
    let parsed = expiry
        .expires_at
        .parse::<toml::value::Datetime>()
        .map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "{subject_kind} {subject_id:?} grant {:?} expires_at must be an RFC3339 UTC instant: {err}",
                expiry.capability
            ))
        })?;
    let is_utc = matches!(
        parsed.offset,
        Some(toml::value::Offset::Z | toml::value::Offset::Custom { minutes: 0 })
    );
    if parsed.date.is_none() || parsed.time.is_none() || !is_utc {
        return Err(CooldisError::RuntimeFactory(format!(
            "{subject_kind} {subject_id:?} grant {:?} expires_at must be an RFC3339 UTC instant",
            expiry.capability
        )));
    }
    Ok(())
}

fn validate_coupling_stream(label: &str, stream: &str) -> CooldisResult<()> {
    if matches!(stream, "thread" | "control") {
        return Ok(());
    }
    if let Some(name) = stream.strip_prefix("derived:") {
        validate_record_name(name).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "{label} {stream:?} has invalid derived name: {err}"
            ))
        })?;
        return Ok(());
    }
    Err(CooldisError::RuntimeFactory(format!(
        "{label} {stream:?} must be thread, control, or derived:<name>"
    )))
}

fn validate_coupling_event_kind(label: &str, kind: &str) -> CooldisResult<()> {
    if kind.trim().is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "{label} cannot be empty"
        )));
    }
    Ok(())
}

fn validate_positive_optional_u32(label: &str, value: Option<u32>) -> CooldisResult<()> {
    if value == Some(0) {
        return Err(CooldisError::RuntimeFactory(format!("{label} must be > 0")));
    }
    Ok(())
}

fn validate_positive_optional_u64(label: &str, value: Option<u64>) -> CooldisResult<()> {
    if value == Some(0) {
        return Err(CooldisError::RuntimeFactory(format!("{label} must be > 0")));
    }
    Ok(())
}

fn deserialize_string_list<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum StringList {
        One(String),
        Many(Vec<String>),
    }

    match StringList::deserialize(deserializer)? {
        StringList::One(value) => Ok(vec![value]),
        StringList::Many(values) => {
            if values.iter().any(|value| value.trim().is_empty()) {
                return Err(de::Error::custom("kind list entries cannot be empty"));
            }
            Ok(values)
        }
    }
}

fn validate_workspace_requirement(
    workspace: &AgentManifestWorkspaceRequirement,
) -> CooldisResult<()> {
    let path = Path::new(&workspace.guest_path);
    if !path.is_absolute() {
        return Err(CooldisError::RuntimeFactory(format!(
            "workspace guest_path {:?} must be absolute",
            workspace.guest_path
        )));
    }
    if path == Path::new("/") {
        return Err(CooldisError::RuntimeFactory(
            "workspace guest_path must not be /".to_string(),
        ));
    }
    if path.starts_with(Path::new("/skills")) {
        return Err(CooldisError::RuntimeFactory(
            "workspace guest_path /skills and its descendants are reserved for skill resources"
                .to_string(),
        ));
    }
    if path.starts_with(Path::new("/spill")) {
        return Err(CooldisError::RuntimeFactory(
            "workspace guest_path /spill and its descendants are reserved for tool output spill"
                .to_string(),
        ));
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || path.components().collect::<PathBuf>() != path
    {
        return Err(CooldisError::RuntimeFactory(format!(
            "workspace guest_path {:?} must be normalized",
            workspace.guest_path
        )));
    }
    Ok(())
}

fn validate_skill_discovery(
    skills: &AgentManifestSkills,
    workspace: Option<&AgentManifestWorkspaceRequirement>,
) -> CooldisResult<()> {
    let path = Path::new(&skills.path);
    if skills.path.is_empty() || path.is_absolute() {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest skills.path {:?} must be a non-empty workspace-relative path",
            skills.path
        )));
    }
    if skills.path.chars().any(char::is_control) {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest skills.path {:?} must be a workspace-relative path without control characters",
            skills.path
        )));
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::RootDir | Component::Prefix(_)
        )
    }) {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest skills.path {:?} must not contain `..` and must remain workspace-relative",
            skills.path
        )));
    }
    if skills.discover && workspace.is_none() {
        return Err(CooldisError::RuntimeFactory(
            "agent manifest skills.discover = true requires a workspace requirement ([workspace]) so bind can resolve the discovery scope"
                .to_string(),
        ));
    }
    Ok(())
}

fn validate_context_pipeline(context: &AgentManifestContextPipeline) -> CooldisResult<()> {
    if context.id != "default" {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent manifest context pipeline id must be \"default\", got {:?}",
            context.id
        )));
    }
    if context.sources.is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "agent manifest context pipeline requires at least one source".to_string(),
        ));
    }
    let mut source_ids = BTreeSet::new();
    let mut rest_count = 0usize;
    let mut fraction_sum = 0.0f64;
    for source in &context.sources {
        validate_record_name(&source.id)?;
        if !source_ids.insert(source.id.clone()) {
            return Err(CooldisError::RuntimeFactory(format!(
                "duplicate context source id {:?}",
                source.id
            )));
        }
        if !matches!(
            source.assembler.as_str(),
            KERNEL_ASSEMBLER_STATIC
                | KERNEL_ASSEMBLER_RECORD_SELECT
                | KERNEL_ASSEMBLER_ANCHORED_WINDOW
        ) {
            return Err(CooldisError::RuntimeFactory(format!(
                "context source {:?} assembler {:?} is not a V1 kernel assembler",
                source.id, source.assembler
            )));
        }
        if source.assembler == KERNEL_ASSEMBLER_STATIC && source.input.is_none() {
            return Err(CooldisError::RuntimeFactory(format!(
                "static context source {:?} requires input",
                source.id
            )));
        }
        match (source.pinned, source.budget_share) {
            (true, Some(_)) => {
                return Err(CooldisError::RuntimeFactory(format!(
                    "pinned context source {:?} must not declare budget_share",
                    source.id
                )));
            }
            (false, None) => {
                return Err(CooldisError::RuntimeFactory(format!(
                    "context source {:?} must declare budget_share",
                    source.id
                )));
            }
            (_, Some(AgentManifestBudgetShare::Fraction(value))) => {
                if !(value > 0.0 && value <= 1.0) {
                    return Err(CooldisError::RuntimeFactory(format!(
                        "context source {:?} budget_share fraction must be in (0, 1]",
                        source.id
                    )));
                }
                fraction_sum += value;
            }
            (_, Some(AgentManifestBudgetShare::Rest(AgentManifestBudgetRest::Rest))) => {
                rest_count += 1;
            }
            (true, None) => {}
        }
    }
    if rest_count > 1 {
        return Err(CooldisError::RuntimeFactory(
            "context pipeline may declare at most one rest budget_share".to_string(),
        ));
    }
    if fraction_sum > 1.0 {
        return Err(CooldisError::RuntimeFactory(format!(
            "context budget_share fractions sum to {fraction_sum}, expected <= 1.0"
        )));
    }
    Ok(())
}

pub fn validate_namespace(value: &str) -> CooldisResult<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "agent namespace cannot be empty".to_string(),
        ));
    }
    for segment in trimmed.split('/') {
        validate_record_name(segment).map_err(|err| {
            CooldisError::RuntimeFactory(format!("invalid agent namespace segment: {err}"))
        })?;
    }
    Ok(trimmed.to_string())
}

pub fn validate_version(value: &str) -> CooldisResult<()> {
    if value.is_empty() {
        return Err(CooldisError::RuntimeFactory(
            "agent version cannot be empty".to_string(),
        ));
    }
    if value.contains('/') || value.contains('\\') {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent version {value:?} must not contain path separators"
        )));
    }
    if value.starts_with('.') {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent version {value:?} must not start with dot"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_' | b':'))
    {
        return Err(CooldisError::RuntimeFactory(format!(
            "agent version {value:?} contains unsupported characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
