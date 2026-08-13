use sha2::Digest as _;
use std::io::Write as _;

const AGENT_RECORD_SCHEMA_VERSION: u32 = 1;
const FOLDER_FIRST_SYSTEM_PROMPT_RESOURCE: &str = "identity";
const FOLDER_FIRST_SYSTEM_PROMPT_PREFLIGHT_REF: &str =
    "resource://artifact/sha256:0000000000000000000000000000000000000000000000000000000000000000";

pub fn default_operations_registry_root() -> std::path::PathBuf {
    std::path::PathBuf::from(".verlet/operations")
}

pub fn default_blob_registry_root() -> std::path::PathBuf {
    std::path::PathBuf::from(".verlet/blobs")
}

pub fn default_blob_registry_root_for_agent_registry_root(
    root: impl AsRef<std::path::Path>,
) -> std::path::PathBuf {
    let root = root.as_ref();
    if root.file_name().and_then(|name| name.to_str()) == Some("agents")
        && let Some(parent) = root.parent()
    {
        return parent.join("blobs");
    }
    root.join("blobs")
}

#[derive(Clone, Debug)]
pub struct LocalAgentRegistry {
    root: std::path::PathBuf,
    blob_registry_root: std::path::PathBuf,
}

impl LocalAgentRegistry {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        let root = root.into();
        let blob_registry_root = default_blob_registry_root_for_agent_registry_root(&root);
        Self {
            root,
            blob_registry_root,
        }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn blob_registry_root(&self) -> &std::path::Path {
        &self.blob_registry_root
    }

    pub fn with_blob_registry_root(mut self, root: impl Into<std::path::PathBuf>) -> Self {
        self.blob_registry_root = root.into();
        self
    }

    pub fn plan_manifest_path(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> crate::kernel::runtime_host::VerletResult<AgentPublishPlan> {
        AgentPublishPlan::from_path_with_blob_registry(path.as_ref(), &self.blob_registry_root)
    }

    pub fn publish_manifest_path(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        self.publish_manifest_path_with_operation_registry(path, default_operations_registry_root())
    }

    pub fn publish_manifest_path_with_operation_registry(
        &self,
        path: impl AsRef<std::path::Path>,
        operation_registry_root: impl AsRef<std::path::Path>,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        let plan = self.plan_manifest_path(path)?;
        self.publish_plan_with_operation_registry(plan, operation_registry_root)
    }

    pub fn publish_plan(
        &self,
        plan: AgentPublishPlan,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        self.publish_plan_with_operation_registry(plan, default_operations_registry_root())
    }

    pub fn publish_plan_with_operation_registry(
        &self,
        mut plan: AgentPublishPlan,
        operation_registry_root: impl AsRef<std::path::Path>,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        plan.validate_resolved_refs_for_publish()?;
        plan.verify_operation_refs_for_publish(operation_registry_root.as_ref())?;
        let record = plan.into_record(now_ms());
        record.validate()?;
        self.ensure_version_does_not_collide_with_alias(&record.name, &record.version)?;
        self.ensure_alias_does_not_collide_with_version(&record.name, "latest")?;
        self.write_version_record_atomically(&record)?;
        self.write_record_atomically(&record)?;
        self.write_latest_alias_atomically(&record)?;
        Ok(record)
    }

    pub fn load_record(
        &self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        let path = self.record_path(&name)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read agent record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedAgentRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to decode agent record {}: {err}",
                path.display()
            ))
        })?;
        record.validate()?;
        if record.name != name {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent record {} names {:?}, expected {:?}",
                    path.display(),
                    record.name,
                    name
                ),
            ));
        }
        Ok(record)
    }

    pub fn load_ref(
        &self,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        self.load_ref_with_alias_receipt(agent_ref)
            .map(|(record, _alias)| record)
    }

    /// Load an agent ref and return an alias resolution receipt when the ref
    /// reached the record through a mutable alias.
    pub fn load_ref_with_alias_receipt(
        &self,
        agent_ref: &str,
    ) -> crate::kernel::runtime_host::VerletResult<(
        PublishedAgentRecord,
        Option<AgentAliasResolutionReceipt>,
    )> {
        let parsed = AgentRecordRef::parse(agent_ref)?;
        match parsed.version {
            Some(version) => {
                if verlet_operations::operation_store::validate_record_name(&version).is_ok() {
                    let path = self.alias_record_path(&parsed.name, &version)?;
                    if path.exists() {
                        return self
                            .resolve_alias(&parsed.name, &version)
                            .map(|(record, receipt)| (record, Some(receipt)));
                    }
                }
                self.load_version_record(&parsed.name, &version)
                    .map(|record| (record, None))
            }
            None => self.load_record(&parsed.name).map(|record| (record, None)),
        }
    }

    pub fn load_version_record(
        &self,
        name: &str,
        version: &str,
    ) -> crate::kernel::runtime_host::VerletResult<PublishedAgentRecord> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        verlet_agent::manifest_schema::validate_version(version)?;
        let path = self.version_record_path(&name, version)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read agent version record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedAgentRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to decode agent version record {}: {err}",
                path.display()
            ))
        })?;
        record.validate()?;
        if record.name != name || record.version != version {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent version record {} is {}@{}, expected {}@{}",
                    path.display(),
                    record.name,
                    record.version,
                    name,
                    version
                ),
            ));
        }
        Ok(record)
    }

    pub fn list_records(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<PublishedAgentRecord>> {
        let records_dir = self.root.join("records");
        if !records_dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&records_dir).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read agent records directory {}: {err}",
                records_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to read agent record entry in {}: {err}",
                    records_dir.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            names.push(verlet_operations::operation_store::validate_record_name(
                name,
            )?);
        }
        names.sort();
        names
            .into_iter()
            .map(|name| self.load_record(&name))
            .collect()
    }

    pub fn list_version_records(
        &self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<AgentVersionSummary>> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        let versions_dir = self.root.join("versions").join(&name);
        if !versions_dir.exists() {
            return Ok(Vec::new());
        }
        let mut records = Vec::new();
        for entry in std::fs::read_dir(&versions_dir).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read agent versions directory {}: {err}",
                versions_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to read agent version entry in {}: {err}",
                    versions_dir.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(version) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let record = self.load_version_record(&name, version)?;
            records.push(AgentVersionSummary {
                version: record.version,
                source_hash: record.source_hash,
                manifest_hash: record.manifest_hash,
                published_at_ms: record.published_at_ms,
                authored_source_present: record.authored_source.is_some(),
            });
        }
        records.sort_by_key(|record| record.published_at_ms);
        Ok(records)
    }

    /// Resolve a mutable alias (`latest`, `stable`, ...) to its immutable
    /// version record, producing a resolution receipt. Aliases live under
    /// `aliases/<name>/<alias>.json`; `latest` is maintained automatically on
    /// publish. Alias names follow record-name rules, and publish refuses a
    /// version that collides with an existing alias for the same agent (and
    /// vice versa), so the `@` slot stays unambiguous: alias record first,
    /// version record otherwise.
    pub fn resolve_alias(
        &self,
        name: &str,
        alias: &str,
    ) -> crate::kernel::runtime_host::VerletResult<(
        PublishedAgentRecord,
        AgentAliasResolutionReceipt,
    )> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        let alias = verlet_operations::operation_store::validate_record_name(alias)?;
        let path = self.alias_record_path(&name, &alias)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read agent alias record {}: {err}",
                path.display()
            ))
        })?;
        let alias_record: AgentAliasRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to decode agent alias record {}: {err}",
                path.display()
            ))
        })?;
        alias_record.validate()?;
        if alias_record.name != name || alias_record.alias != alias {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent alias record {} is {}@{}, expected {}@{}",
                    path.display(),
                    alias_record.name,
                    alias_record.alias,
                    name,
                    alias
                ),
            ));
        }
        let record = self.load_version_record(&name, &alias_record.version)?;
        if alias_record.manifest_hash != record.manifest_hash {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent alias {}@{} points to {} with manifest hash {}, but the version record has {}",
                    name,
                    alias,
                    alias_record.version,
                    alias_record.manifest_hash,
                    record.manifest_hash
                ),
            ));
        }
        let receipt = AgentAliasResolutionReceipt {
            ref_uri: agent_ref_uri(record.namespace.as_deref(), &name, &alias),
            alias,
            version: record.version.clone(),
            manifest_hash: record.manifest_hash.clone(),
            resolved_at_ms: now_ms(),
        };
        Ok((record, receipt))
    }

    pub fn alias_record_path(
        &self,
        name: &str,
        alias: &str,
    ) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        let alias = verlet_operations::operation_store::validate_record_name(alias)?;
        Ok(self
            .root
            .join("aliases")
            .join(name)
            .join(format!("{alias}.json")))
    }

    pub fn record_path(
        &self,
        name: &str,
    ) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        Ok(self.root.join("records").join(format!("{name}.json")))
    }

    pub fn version_record_path(
        &self,
        name: &str,
        version: &str,
    ) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        verlet_agent::manifest_schema::validate_version(version)?;
        Ok(self
            .root
            .join("versions")
            .join(name)
            .join(format!("{version}.json")))
    }

    fn write_record_atomically(
        &self,
        record: &PublishedAgentRecord,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let path = self.record_path(&record.name)?;
        write_json_atomically(&path, format!("agent record {:?}", record.name), record)
    }

    fn write_version_record_atomically(
        &self,
        record: &PublishedAgentRecord,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let path = self.version_record_path(&record.name, &record.version)?;
        if path.exists() {
            let existing = self.load_version_record(&record.name, &record.version)?;
            if existing.manifest_hash != record.manifest_hash {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "agent {:?}@{} already exists with manifest hash {}, refusing to replace it with {}",
                        record.name, record.version, existing.manifest_hash, record.manifest_hash
                    ),
                ));
            }
            return Ok(());
        }
        write_json_atomically(
            &path,
            format!("agent version record {:?}@{}", record.name, record.version),
            record,
        )
    }

    fn write_latest_alias_atomically(
        &self,
        record: &PublishedAgentRecord,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let alias = AgentAliasRecord {
            schema_version: AGENT_RECORD_SCHEMA_VERSION,
            name: record.name.clone(),
            alias: "latest".to_string(),
            version: record.version.clone(),
            manifest_hash: record.manifest_hash.clone(),
            updated_at_ms: record.published_at_ms,
        };
        self.write_alias_record_atomically(&alias)
    }

    fn write_alias_record_atomically(
        &self,
        alias: &AgentAliasRecord,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        alias.validate()?;
        self.ensure_alias_does_not_collide_with_version(&alias.name, &alias.alias)?;
        let path = self.alias_record_path(&alias.name, &alias.alias)?;
        write_json_atomically(
            &path,
            format!("agent alias record {:?}@{}", alias.name, alias.alias),
            alias,
        )
    }

    fn ensure_version_does_not_collide_with_alias(
        &self,
        name: &str,
        version: &str,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if version == "latest" {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!("agent {name:?} version must not be the reserved alias \"latest\""),
            ));
        }
        if verlet_operations::operation_store::validate_record_name(version).is_ok() {
            let alias_path = self.alias_record_path(name, version)?;
            if alias_path.exists() {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!("agent {name:?} version {version:?} collides with an existing alias"),
                ));
            }
        }
        Ok(())
    }

    fn ensure_alias_does_not_collide_with_version(
        &self,
        name: &str,
        alias: &str,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let version_path = self.version_record_path(name, alias)?;
        if version_path.exists() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!("agent alias {name}@{alias} collides with an existing version record"),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentPublishPlan {
    pub schema_version: u32,
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub authored_source: String,
    pub source_hash: String,
    pub manifest_hash: String,
    pub model_profile_count: usize,
    pub tool_count: usize,
    #[serde(default)]
    pub tool_refs: Vec<AgentToolRef>,
    pub resource_count: usize,
    /// Content-addressed artifact refs compiled from the manifest source.
    /// Floating skill refs are resolved and witnessed only at bind.
    #[serde(default)]
    pub resolved_refs: Vec<verlet_agent::manifest_schema::AgentManifestResolvedRef>,
    /// Plan-only verification receipts for `op://` rows. Published records do
    /// not persist this; publish verification is a gate.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ref_verifications: Vec<AgentManifestRefVerification>,
    pub ref_uri: String,
    pub resolved_manifest: serde_json::Value,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentManifestRefVerification {
    pub declared: String,
    pub status: AgentManifestRefVerificationStatus,
}

#[derive(
    Clone,
    Copy,
    Debug,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    strum::AsRefStr,
    strum::Display,
)]
#[strum(serialize_all = "kebab-case")]
pub enum AgentManifestRefVerificationStatus {
    #[serde(rename = "verified")]
    Verified,
    #[serde(rename = "unverified-offline")]
    UnverifiedOffline,
}

impl AgentPublishPlan {
    pub fn from_path(path: &std::path::Path) -> crate::kernel::runtime_host::VerletResult<Self> {
        Self::from_path_with_blob_registry(path, default_blob_registry_root())
    }

    pub fn from_path_with_blob_registry(
        path: &std::path::Path,
        blob_registry_root: impl AsRef<std::path::Path>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let source = std::fs::read_to_string(path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read agent manifest {}: {err}",
                path.display()
            ))
        })?;
        Self::from_source_with_folder_first_prompt(
            &source,
            path.parent().unwrap_or_else(|| std::path::Path::new(".")),
            blob_registry_root.as_ref(),
        )
    }

    pub fn from_source(source: &str) -> crate::kernel::runtime_host::VerletResult<Self> {
        Self::from_source_with_manifest(source, |value| {
            verlet_agent::manifest_schema::AgentManifestSchema::from_toml_value(value)
                .map_err(Into::into)
        })
    }

    fn from_source_with_folder_first_prompt(
        source: &str,
        manifest_dir: &std::path::Path,
        blob_registry_root: &std::path::Path,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let prompt_path = manifest_dir.join("prompts/system.md");
        Self::from_source_with_manifest(source, |value| {
            let mut manifest =
                verlet_agent::manifest_schema::AgentManifestSchema::from_toml_value_unvalidated(
                    value,
                )?;
            let lowering = prevalidate_folder_first_system_prompt(&manifest, &prompt_path)?;
            lower_folder_first_system_prompt(
                &mut manifest,
                &prompt_path,
                blob_registry_root,
                lowering,
            )?;
            if lowering == FolderFirstPromptLowering::Lower {
                manifest.validate()?;
            }
            Ok(manifest)
        })
    }

    fn from_source_with_manifest<F>(
        source: &str,
        manifest_fn: F,
    ) -> crate::kernel::runtime_host::VerletResult<Self>
    where
        F: FnOnce(
            &toml::Value,
        ) -> crate::kernel::runtime_host::VerletResult<
            verlet_agent::manifest_schema::AgentManifestSchema,
        >,
    {
        let source_hash = text_sha256(source.as_bytes());
        let value: toml::Value = toml::from_str(source).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "invalid agent manifest: {err}"
            ))
        })?;
        let manifest = manifest_fn(&value)?;
        let name =
            verlet_operations::operation_store::validate_record_name(&manifest.identity.name)?;
        let namespace = manifest
            .identity
            .namespace
            .clone()
            .map(|namespace| verlet_agent::manifest_schema::validate_namespace(&namespace))
            .transpose()?;
        let version = manifest
            .identity
            .version
            .clone()
            .unwrap_or_else(|| "0.1.0".to_string());
        verlet_agent::manifest_schema::validate_version(&version)?;
        let tool_refs = parse_agent_tool_refs(&manifest.tools)?;
        let resolved_refs = compile_resolved_refs(&manifest)?;
        let resolved_manifest = canonical_json_from_schema(&manifest)?;
        let manifest_hash = value_sha256(&resolved_manifest)?;
        let ref_uri = agent_ref_uri(namespace.as_deref(), &name, &version);
        Ok(Self {
            schema_version: AGENT_RECORD_SCHEMA_VERSION,
            kind: verlet_agent::manifest_schema::AGENT_MANIFEST_KIND.to_string(),
            name,
            namespace,
            version,
            description: manifest.identity.description.clone(),
            authored_source: source.to_string(),
            source_hash,
            manifest_hash,
            model_profile_count: manifest.model_profiles.len(),
            tool_count: manifest.tools.len(),
            tool_refs,
            resource_count: manifest.resources.len(),
            resolved_refs,
            ref_verifications: Vec::new(),
            ref_uri,
            resolved_manifest,
        })
    }

    pub fn has_operation_refs(&self) -> bool {
        self.tool_refs
            .iter()
            .any(|tool_ref| tool_ref.reference.starts_with("op://"))
    }

    pub fn mark_operation_refs_unverified_offline(&mut self) {
        self.ref_verifications = self
            .tool_refs
            .iter()
            .filter(|tool_ref| tool_ref.reference.starts_with("op://"))
            .map(|tool_ref| AgentManifestRefVerification {
                declared: tool_ref.reference.clone(),
                status: AgentManifestRefVerificationStatus::UnverifiedOffline,
            })
            .collect();
    }

    pub fn verify_operation_refs(
        &mut self,
        operation_registry_root: &std::path::Path,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let mut verifications = Vec::new();
        for tool_ref in &self.tool_refs {
            if !tool_ref.reference.starts_with("op://") {
                continue;
            }
            crate::agent::manifest_bind::verify_operation_ref(
                &tool_ref.name,
                &tool_ref.reference,
                operation_registry_root,
            )?;
            verifications.push(AgentManifestRefVerification {
                declared: tool_ref.reference.clone(),
                status: AgentManifestRefVerificationStatus::Verified,
            });
        }
        self.ref_verifications = verifications;
        Ok(())
    }

    pub fn verify_operation_refs_for_publish(
        &mut self,
        operation_registry_root: &std::path::Path,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if let Some(tool_ref) = self
            .tool_refs
            .iter()
            .find(|tool_ref| tool_ref.reference.starts_with("op://"))
            && !operation_registry_root.exists()
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "tool {:?} operation_ref {:?} requires an operations registry, but none was found at {}; seed it or pass --operations-registry-root <path>",
                    tool_ref.name,
                    tool_ref.reference,
                    operation_registry_root.display()
                ),
            ));
        }
        self.verify_operation_refs(operation_registry_root)
    }

    fn validate_resolved_refs_for_publish(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        for resolved_ref in &self.resolved_refs {
            resolved_ref.validate()?;
            if resolved_ref.status
                == verlet_agent::manifest_schema::AgentManifestRefStatus::UnresolvedOffline
            {
                let hint = if resolved_ref.declared.starts_with("op://")
                    && !resolved_ref.declared.contains("@sha256:")
                {
                    "; pass --resolve-ops to pin op:// authoring refs from the operations registry before agent publish"
                } else {
                    ""
                };
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "published agent record contains unresolved artifact ref {:?}{hint}",
                        resolved_ref.declared,
                    ),
                ));
            }
        }
        Ok(())
    }

    pub fn verification_status_for_ref(
        &self,
        declared: &str,
    ) -> Option<AgentManifestRefVerificationStatus> {
        self.ref_verifications
            .iter()
            .find(|verification| verification.declared == declared)
            .map(|verification| verification.status)
    }

    pub fn into_record(self, published_at_ms: u64) -> PublishedAgentRecord {
        PublishedAgentRecord {
            schema_version: self.schema_version,
            kind: self.kind,
            name: self.name,
            namespace: self.namespace,
            version: self.version,
            description: self.description,
            authored_source: Some(self.authored_source),
            source_hash: self.source_hash,
            manifest_hash: self.manifest_hash,
            model_profile_count: self.model_profile_count,
            tool_count: self.tool_count,
            tool_refs: self.tool_refs,
            resource_count: self.resource_count,
            resolved_refs: self.resolved_refs,
            ref_uri: self.ref_uri,
            resolved_manifest: self.resolved_manifest,
            published_at_ms,
        }
    }
}

/// A manifest version snapshot pairs `authored_source` and `resolved_manifest`
/// with their `source_hash` and `manifest_hash`.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PublishedAgentRecord {
    pub schema_version: u32,
    pub kind: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authored_source: Option<String>,
    pub source_hash: String,
    pub manifest_hash: String,
    pub model_profile_count: usize,
    pub tool_count: usize,
    #[serde(default)]
    pub tool_refs: Vec<AgentToolRef>,
    pub resource_count: usize,
    /// Artifact refs resolved when the manifest was compiled.
    #[serde(default)]
    pub resolved_refs: Vec<verlet_agent::manifest_schema::AgentManifestResolvedRef>,
    pub ref_uri: String,
    pub resolved_manifest: serde_json::Value,
    pub published_at_ms: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentVersionSummary {
    pub version: String,
    pub source_hash: String,
    pub manifest_hash: String,
    pub published_at_ms: u64,
    pub authored_source_present: bool,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum AgentManifestDiffKind {
    Added,
    Removed,
    Changed,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentManifestDiffChange {
    pub path: String,
    pub kind: AgentManifestDiffKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub before: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub after: Option<serde_json::Value>,
}

pub fn diff_canonical_json(
    before: &serde_json::Value,
    after: &serde_json::Value,
) -> Vec<AgentManifestDiffChange> {
    let mut changes = Vec::new();
    diff_canonical_json_at_path(before, after, "", &mut changes);
    changes.sort_by(|left, right| left.path.cmp(&right.path));
    changes
}

fn diff_canonical_json_at_path(
    before: &serde_json::Value,
    after: &serde_json::Value,
    path: &str,
    changes: &mut Vec<AgentManifestDiffChange>,
) {
    if before == after {
        return;
    }
    match (before, after) {
        (serde_json::Value::Object(before), serde_json::Value::Object(after)) => {
            let keys = before
                .keys()
                .chain(after.keys())
                .map(String::as_str)
                .collect::<std::collections::BTreeSet<_>>();
            for key in keys {
                let child_path = json_pointer_child(path, key);
                match (before.get(key), after.get(key)) {
                    (Some(before), Some(after)) => {
                        diff_canonical_json_at_path(before, after, &child_path, changes)
                    }
                    (Some(before), None) => changes.push(AgentManifestDiffChange {
                        path: child_path,
                        kind: AgentManifestDiffKind::Removed,
                        before: Some(before.clone()),
                        after: None,
                    }),
                    (None, Some(after)) => changes.push(AgentManifestDiffChange {
                        path: child_path,
                        kind: AgentManifestDiffKind::Added,
                        before: None,
                        after: Some(after.clone()),
                    }),
                    (None, None) => {}
                }
            }
        }
        (serde_json::Value::Array(before), serde_json::Value::Array(after)) => {
            for index in 0..before.len().max(after.len()) {
                let child_path = json_pointer_child(path, &index.to_string());
                match (before.get(index), after.get(index)) {
                    (Some(before), Some(after)) => {
                        diff_canonical_json_at_path(before, after, &child_path, changes)
                    }
                    (Some(before), None) => changes.push(AgentManifestDiffChange {
                        path: child_path,
                        kind: AgentManifestDiffKind::Removed,
                        before: Some(before.clone()),
                        after: None,
                    }),
                    (None, Some(after)) => changes.push(AgentManifestDiffChange {
                        path: child_path,
                        kind: AgentManifestDiffKind::Added,
                        before: None,
                        after: Some(after.clone()),
                    }),
                    (None, None) => {}
                }
            }
        }
        _ => changes.push(AgentManifestDiffChange {
            path: path.to_string(),
            kind: AgentManifestDiffKind::Changed,
            before: Some(before.clone()),
            after: Some(after.clone()),
        }),
    }
}

fn json_pointer_child(path: &str, segment: &str) -> String {
    format!("{path}/{}", segment.replace('~', "~0").replace('/', "~1"))
}

impl PublishedAgentRecord {
    pub fn validate(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        if self.schema_version != AGENT_RECORD_SCHEMA_VERSION {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "unsupported agent record schema_version {}",
                    self.schema_version
                ),
            ));
        }
        if self.kind != verlet_agent::manifest_schema::AGENT_MANIFEST_KIND {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent record kind must be {:?}, got {:?}",
                    verlet_agent::manifest_schema::AGENT_MANIFEST_KIND,
                    self.kind,
                ),
            ));
        }
        verlet_operations::operation_store::validate_record_name(&self.name)?;
        if let Some(namespace) = &self.namespace {
            verlet_agent::manifest_schema::validate_namespace(namespace)?;
        }
        verlet_agent::manifest_schema::validate_version(&self.version)?;
        validate_hash_label("source_hash", &self.source_hash)?;
        validate_hash_label("manifest_hash", &self.manifest_hash)?;
        for tool_ref in &self.tool_refs {
            tool_ref.validate()?;
        }
        for resolved_ref in &self.resolved_refs {
            resolved_ref.validate()?;
            if resolved_ref.status
                == verlet_agent::manifest_schema::AgentManifestRefStatus::UnresolvedOffline
            {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "published agent record contains unresolved artifact ref {:?}",
                        resolved_ref.declared
                    ),
                ));
            }
        }
        let expected_ref = agent_ref_uri(self.namespace.as_deref(), &self.name, &self.version);
        if self.ref_uri != expected_ref {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent record ref_uri {:?} does not match expected {:?}",
                    self.ref_uri, expected_ref
                ),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentToolRef {
    pub name: String,
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "ref")]
    pub reference: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation: Option<String>,
}

impl AgentToolRef {
    fn validate(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        verlet_operations::operation_store::validate_record_name(&self.name)?;
        validate_label("tool type", &self.kind)?;
        validate_tool_ref(&self.reference)?;
        if let Some(operation) = &self.operation
            && operation.trim().is_empty()
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "agent tool ref {:?} has an empty operation surface",
                    self.name
                ),
            ));
        }
        Ok(())
    }
}

/// A mutable registry pointer from an alias to an immutable version record.
/// Aliases are registry state, never manifest identity (audit section 1).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentAliasRecord {
    pub schema_version: u32,
    pub name: String,
    pub alias: String,
    pub version: String,
    pub manifest_hash: String,
    pub updated_at_ms: u64,
}

/// Receipt for one alias -> version -> manifest-hash resolution, recorded at
/// publish time (in the published record) and at run time (on the thread's
/// stream, ticket 0004).
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AgentAliasResolutionReceipt {
    pub ref_uri: String,
    pub alias: String,
    pub version: String,
    pub manifest_hash: String,
    pub resolved_at_ms: u64,
}

impl AgentAliasRecord {
    fn validate(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        if self.schema_version != AGENT_RECORD_SCHEMA_VERSION {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "unsupported agent alias schema_version {}",
                    self.schema_version
                ),
            ));
        }
        verlet_operations::operation_store::validate_record_name(&self.name)?;
        verlet_operations::operation_store::validate_record_name(&self.alias)?;
        verlet_agent::manifest_schema::validate_version(&self.version)?;
        validate_hash_label("manifest_hash", &self.manifest_hash)?;
        Ok(())
    }
}

pub fn agent_manifest_source_from_schema(
    manifest: &verlet_agent::manifest_schema::AgentManifestSchema,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let json = serde_json::to_value(manifest).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode agent manifest schema: {err}"
        ))
    })?;
    let object = json.as_object().ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "agent manifest schema did not encode as an object".to_string(),
        )
    })?;
    let mut root = toml::map::Map::new();
    insert_manifest_toml_value(&mut root, "agent", object.get("identity"))?;
    insert_manifest_toml_value(&mut root, "model_profiles", object.get("model_profiles"))?;
    insert_manifest_toml_value(&mut root, "tools", object.get("tools"))?;
    insert_manifest_toml_value(&mut root, "resources", object.get("resources"))?;
    if let Some(context) = object
        .get("context")
        .map(json_to_toml_value)
        .transpose()?
        .flatten()
    {
        let mut context_table = toml::map::Map::new();
        context_table.insert("pipelines".to_string(), toml::Value::Array(vec![context]));
        root.insert("context".to_string(), toml::Value::Table(context_table));
    }
    insert_manifest_toml_value(&mut root, "couplings", object.get("couplings"))?;
    insert_manifest_toml_value(&mut root, "policies", object.get("policies"))?;
    insert_manifest_toml_value(&mut root, "runtime", object.get("runtime"))?;
    toml::to_string_pretty(&toml::Value::Table(root)).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode agent manifest TOML: {err}"
        ))
    })
}

fn insert_manifest_toml_value(
    root: &mut toml::map::Map<String, toml::Value>,
    key: &str,
    value: Option<&serde_json::Value>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if let Some(value) = value.map(json_to_toml_value).transpose()?.flatten() {
        root.insert(key.to_string(), value);
    }
    Ok(())
}

fn json_to_toml_value(
    value: &serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<Option<toml::Value>> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::Bool(value) => Ok(Some(toml::Value::Boolean(*value))),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                Ok(Some(toml::Value::Integer(value)))
            } else if let Some(value) = number.as_u64() {
                let value = i64::try_from(value).map_err(|_| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                        "agent manifest number {value} is too large for TOML integer"
                    ))
                })?;
                Ok(Some(toml::Value::Integer(value)))
            } else if let Some(value) = number.as_f64() {
                Ok(Some(toml::Value::Float(value)))
            } else {
                Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!("agent manifest number {number} cannot be encoded as TOML"),
                ))
            }
        }
        serde_json::Value::String(value) => Ok(Some(toml::Value::String(value.clone()))),
        serde_json::Value::Array(values) => {
            let mut items = Vec::new();
            for value in values {
                if let Some(value) = json_to_toml_value(value)? {
                    items.push(value);
                }
            }
            Ok(Some(toml::Value::Array(items)))
        }
        serde_json::Value::Object(values) => {
            let mut table = toml::map::Map::new();
            for (key, value) in values {
                if let Some(value) = json_to_toml_value(value)? {
                    table.insert(key.clone(), value);
                }
            }
            Ok(Some(toml::Value::Table(table)))
        }
    }
}

#[derive(Debug)]
pub struct AgentRecordRef {
    pub name: String,
    pub version: Option<String>,
}

impl AgentRecordRef {
    pub fn parse(value: &str) -> crate::kernel::runtime_host::VerletResult<Self> {
        let trimmed = value.trim();
        let without_scheme = trimmed.strip_prefix("agent://").unwrap_or(trimmed);
        let name_and_version = without_scheme.rsplit('/').next().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory("empty agent ref".to_string())
        })?;
        let (name, version) = match name_and_version.split_once('@') {
            Some((name, version)) => (name, Some(version.to_string())),
            None => (name_and_version, None),
        };
        let name = verlet_operations::operation_store::validate_record_name(name)?;
        if let Some(version) = &version {
            verlet_agent::manifest_schema::validate_version(version)?;
        }
        Ok(Self { name, version })
    }
}

pub fn agent_ref_uri(namespace: Option<&str>, name: &str, version: &str) -> String {
    match namespace {
        Some(namespace) => format!("agent://{namespace}/{name}@{version}"),
        None => format!("agent://{name}@{version}"),
    }
}

fn parse_agent_tool_refs(
    tools: &[verlet_agent::manifest_schema::AgentManifestTool],
) -> crate::kernel::runtime_host::VerletResult<Vec<AgentToolRef>> {
    let mut refs = Vec::new();
    for tool in tools {
        let tool_ref = match tool {
            verlet_agent::manifest_schema::AgentManifestTool::Bash(tool) => AgentToolRef {
                name: tool.id.clone(),
                kind: "bash_tool".to_string(),
                reference: tool.operation_ref.clone(),
                operation: Some(tool.command.clone()),
            },
            verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => AgentToolRef {
                name: tool.id.clone(),
                kind: "direct_tool".to_string(),
                reference: tool.operation_ref.clone(),
                operation: Some(tool.tool_name.clone()),
            },
            verlet_agent::manifest_schema::AgentManifestTool::ProtocolImport(tool) => {
                AgentToolRef {
                    name: tool.id.clone(),
                    kind: "protocol_tool_import".to_string(),
                    reference: tool.server_ref.clone(),
                    operation: None,
                }
            }
        };
        tool_ref.validate()?;
        refs.push(tool_ref);
    }
    Ok(refs)
}

fn validate_tool_ref(value: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    if (value.starts_with("tool://") && value.len() > "tool://".len())
        || (value.starts_with("op://") && value.len() > "op://".len())
        || (value.starts_with("mcp://") && value.len() > "mcp://".len())
    {
        Ok(())
    } else {
        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("agent tool ref {value:?} must start with op://, mcp://, or legacy tool://"),
        ))
    }
}

fn compile_resolved_refs(
    manifest: &verlet_agent::manifest_schema::AgentManifestSchema,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<verlet_agent::manifest_schema::AgentManifestResolvedRef>,
> {
    let mut refs = Vec::new();
    for tool in &manifest.tools {
        match tool {
            verlet_agent::manifest_schema::AgentManifestTool::Bash(tool) => {
                refs.push(resolve_artifact_ref(&tool.operation_ref))
            }
            verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => {
                refs.push(resolve_artifact_ref(&tool.operation_ref))
            }
            verlet_agent::manifest_schema::AgentManifestTool::ProtocolImport(_) => {}
        }
    }
    for resource in &manifest.resources {
        if resource.kind == verlet_agent::manifest_schema::AgentManifestResourceKind::Skill {
            match verlet_operations::skill_package::DeclaredSkillPackageRef::parse(
                &resource.reference,
            )? {
                verlet_operations::skill_package::DeclaredSkillPackageRef::Floating { .. } => {
                    continue;
                }
                verlet_operations::skill_package::DeclaredSkillPackageRef::Pinned(reference) => {
                    refs.push(verlet_agent::manifest_schema::AgentManifestResolvedRef {
                        declared: resource.reference.clone(),
                        resolved: Some(resource.reference.clone()),
                        content_hash: Some(format!("sha256:{}", reference.artifact_hash)),
                        status: verlet_agent::manifest_schema::AgentManifestRefStatus::Resolved,
                    });
                    continue;
                }
            }
        }
        refs.push(resolve_artifact_ref(&resource.reference));
    }
    Ok(refs)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FolderFirstPromptLowering {
    Lower,
    Skip,
}

fn prevalidate_folder_first_system_prompt(
    manifest: &verlet_agent::manifest_schema::AgentManifestSchema,
    prompt_path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<FolderFirstPromptLowering> {
    let lowering = folder_first_system_prompt_lowering(manifest, prompt_path)?;
    match lowering {
        FolderFirstPromptLowering::Skip => manifest.validate()?,
        FolderFirstPromptLowering::Lower => {
            let mut lowered = manifest.clone();
            inject_folder_first_system_prompt_resource(
                &mut lowered,
                FOLDER_FIRST_SYSTEM_PROMPT_PREFLIGHT_REF.to_string(),
            )?;
            lowered.validate()?;
        }
    }
    Ok(lowering)
}

fn folder_first_system_prompt_lowering(
    manifest: &verlet_agent::manifest_schema::AgentManifestSchema,
    prompt_path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<FolderFirstPromptLowering> {
    if !prompt_path.exists() {
        return Ok(FolderFirstPromptLowering::Skip);
    }
    match identity_static_source_input(manifest) {
        Some(Some(input)) => {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "folder-first prompt lowering found {}, but the identity static context source already declares input {input:?}; drop the input so prompts/system.md can lower to the identity resource, or move the file out of prompts/system.md to keep the explicit input",
                    prompt_path.display()
                ),
            ));
        }
        Some(None) => {}
        None => {
            return Ok(FolderFirstPromptLowering::Skip);
        }
    }
    if manifest
        .resources
        .iter()
        .any(|resource| resource.name == FOLDER_FIRST_SYSTEM_PROMPT_RESOURCE)
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "folder-first prompt lowering found {}, but the manifest already declares resource {:?}; remove or rename that resource so prompts/system.md can lower to the identity resource, or move the file out of prompts/system.md and point the identity static source at a declared resource explicitly",
                prompt_path.display(),
                FOLDER_FIRST_SYSTEM_PROMPT_RESOURCE
            ),
        ));
    }
    Ok(FolderFirstPromptLowering::Lower)
}

fn lower_folder_first_system_prompt(
    manifest: &mut verlet_agent::manifest_schema::AgentManifestSchema,
    prompt_path: &std::path::Path,
    blob_registry_root: &std::path::Path,
    lowering: FolderFirstPromptLowering,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if lowering == FolderFirstPromptLowering::Skip {
        return Ok(());
    }
    let blob = verlet_operations::blob_store::LocalBlobRegistry::new(blob_registry_root)
        .publish_file(prompt_path, Some(FOLDER_FIRST_SYSTEM_PROMPT_RESOURCE))?;
    inject_folder_first_system_prompt_resource(manifest, blob.ref_uri)
}

fn inject_folder_first_system_prompt_resource(
    manifest: &mut verlet_agent::manifest_schema::AgentManifestSchema,
    reference: String,
) -> crate::kernel::runtime_host::VerletResult<()> {
    manifest
        .resources
        .push(verlet_agent::manifest_schema::AgentManifestResource {
            name: FOLDER_FIRST_SYSTEM_PROMPT_RESOURCE.to_string(),
            kind: verlet_agent::manifest_schema::AgentManifestResourceKind::Blob,
            reference,
            mount: verlet_agent::manifest_schema::AgentManifestResourceMount::Context,
            mode: verlet_agent::manifest_schema::AgentManifestResourceMode::Read,
        });
    let mut context = manifest.effective_context_pipeline();
    resolve_identity_static_source(&mut context)?;
    manifest.context = Some(context);
    Ok(())
}

fn identity_static_source_input(
    manifest: &verlet_agent::manifest_schema::AgentManifestSchema,
) -> Option<Option<String>> {
    manifest
        .effective_context_pipeline()
        .sources
        .into_iter()
        .find(|source| {
            source.id == "identity"
                && source.assembler == verlet_agent::manifest_schema::KERNEL_ASSEMBLER_STATIC
        })
        .map(|source| source.input)
}

fn resolve_identity_static_source(
    context: &mut verlet_agent::manifest_schema::AgentManifestContextPipeline,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let source = context
        .sources
        .iter_mut()
        .find(|source| {
            source.id == "identity"
                && source.assembler == verlet_agent::manifest_schema::KERNEL_ASSEMBLER_STATIC
        })
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "default context pipeline did not contain an identity static source".to_string(),
            )
        })?;
    source.input = Some(FOLDER_FIRST_SYSTEM_PROMPT_RESOURCE.to_string());
    Ok(())
}

fn resolve_artifact_ref(
    reference: &str,
) -> verlet_agent::manifest_schema::AgentManifestResolvedRef {
    match content_hash_from_ref(reference) {
        Some(content_hash) => verlet_agent::manifest_schema::AgentManifestResolvedRef {
            declared: reference.to_string(),
            resolved: Some(reference.to_string()),
            content_hash: Some(content_hash),
            status: verlet_agent::manifest_schema::AgentManifestRefStatus::Resolved,
        },
        None => verlet_agent::manifest_schema::AgentManifestResolvedRef {
            declared: reference.to_string(),
            resolved: None,
            content_hash: None,
            status: verlet_agent::manifest_schema::AgentManifestRefStatus::UnresolvedOffline,
        },
    }
}

fn content_hash_from_ref(reference: &str) -> Option<String> {
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

fn validate_label(label: &str, value: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("{label} {value:?} must use ASCII letters, numbers, '.', '_' or '-'"),
        ));
    }
    Ok(())
}

fn canonical_json_from_schema(
    value: &verlet_agent::manifest_schema::AgentManifestSchema,
) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
    let json = serde_json::to_value(value).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to canonicalize agent manifest: {err}"
        ))
    })?;
    Ok(sort_json(json))
}

pub(crate) fn canonical_json_from_authored_source(
    source: &str,
) -> crate::kernel::runtime_host::VerletResult<serde_json::Value> {
    let value: toml::Value = toml::from_str(source).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "invalid agent manifest: {err}"
        ))
    })?;
    let manifest = verlet_agent::manifest_schema::AgentManifestSchema::from_toml_value(&value)?;
    canonical_json_from_schema(&manifest)
}

fn sort_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(sort_json).collect())
        }
        serde_json::Value::Object(values) => serde_json::Value::Object(
            values
                .into_iter()
                .map(|(key, value)| (key, sort_json(value)))
                .collect(),
        ),
        other => other,
    }
}

fn value_sha256(value: &serde_json::Value) -> crate::kernel::runtime_host::VerletResult<String> {
    let bytes = serde_json::to_vec(value).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode resolved agent manifest: {err}"
        ))
    })?;
    Ok(text_sha256(&bytes))
}

fn text_sha256(bytes: &[u8]) -> String {
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    format!("sha256:{digest:x}")
}

fn validate_hash_label(label: &str, value: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(hash) = value.strip_prefix("sha256:") else {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("{label} must start with sha256:"),
        ));
    };
    validate_hex_hash(hash).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "{label} must be sha256:<64 lowercase hex>: {err}"
        ))
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

fn write_json_atomically<T: serde::Serialize>(
    path: &std::path::Path,
    label: String,
    value: &T,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(parent) = path.parent() else {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("{label} path {} has no parent directory", path.display()),
        ));
    };
    std::fs::create_dir_all(parent).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to create {label} directory {}: {err}",
            parent.display()
        ))
    })?;
    let tmp_path = parent.join(format!(".verlet.tmp.{}", uuid::Uuid::now_v7()));
    let bytes = serde_json::to_vec_pretty(value).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode {label}: {err}"
        ))
    })?;
    {
        let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to create temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.write_all(&bytes).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to write temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to sync temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
    }
    std::fs::rename(&tmp_path, path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to atomically install {label} {}: {err}",
            path.display()
        ))
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
