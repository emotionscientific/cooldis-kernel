//! Default manifest synthesis and publication (lexicon: "default manifest").
//!
//! Every thread binds a manifest; a `thread/start` that names none binds the
//! default manifest — the daemon's configured envelope, synthesized here and
//! published into the agent registry at startup like any other manifest.
//! Nothing about it is special at bind time: it flows through the normal
//! plan → publish → bind pipeline with full receipts.

/// Name of the synthesized default agent record (D1).
pub(super) const DEFAULT_AGENT_NAME: &str = "default";
/// Namespace marking kernel-synthesized records (D1).
pub(super) const DEFAULT_AGENT_NAMESPACE: &str = "verlet";
/// Namespace used by synthesized records written before the Verlet rename.
pub(super) const LEGACY_DEFAULT_AGENT_NAMESPACE: &str = concat!("cool", "dis");
/// The ref a ref-less `thread/start` binds (alias resolution via `@latest`).
pub(crate) const DEFAULT_AGENT_REF: &str = "agent://verlet/default@latest";
const DEFAULT_MANIFEST_LOCK_ATTEMPTS: usize = 250;
const DEFAULT_MANIFEST_LOCK_SLEEP: std::time::Duration = std::time::Duration::from_millis(20);

/// Publishes the synthesized default manifest into the agent registry,
/// idempotently by content (D3): if `verlet/default@latest` exists and its
/// manifest hash matches the fresh plan, return the existing record without
/// publishing; otherwise publish a new version (1.0.0, then patch-bump).
/// Errors fail daemon startup (fail-closed): an envelope that cannot be
/// declared does not run.
pub(super) fn ensure_default_manifest_published(
    config: &crate::adapters::app_server::VerletAppServerConfig,
    supports_streaming: bool,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::manifest::PublishedAgentRecord> {
    let registry =
        crate::agent::manifest::LocalAgentRegistry::new(config.agent_registry_root.clone())
            .with_blob_registry_root(config.blob_registry_root.clone());
    let _lock = DefaultManifestPublishLock::acquire(&registry)?;
    let existing = load_existing_default_manifest(&registry)?;
    if let Some(record) = existing.record() {
        ensure_default_record_identity(record)?;
    }

    let comparison_version = existing
        .record()
        .map(|record| record.version.as_str())
        .unwrap_or("1.0.0");
    let comparison_plan =
        default_manifest_publish_plan(config, supports_streaming, comparison_version)?;
    match existing {
        ExistingDefaultManifest::Latest(record) => {
            if record.manifest_hash == comparison_plan.manifest_hash {
                return Ok(record);
            }
            let publish_version = patch_bump_version(&record.version)?;
            let publish_plan =
                default_manifest_publish_plan(config, supports_streaming, &publish_version)?;
            return publish_default_manifest_plan(&registry, publish_plan, config);
        }
        ExistingDefaultManifest::VersionOnly(record) => {
            if record.manifest_hash == comparison_plan.manifest_hash {
                return publish_default_manifest_plan(&registry, comparison_plan, config);
            }
            let publish_version = patch_bump_version(&record.version)?;
            let publish_plan =
                default_manifest_publish_plan(config, supports_streaming, &publish_version)?;
            return publish_default_manifest_plan(&registry, publish_plan, config);
        }
        ExistingDefaultManifest::None => {}
    }

    publish_default_manifest_plan(&registry, comparison_plan, config)
}

fn publish_default_manifest_plan(
    registry: &crate::agent::manifest::LocalAgentRegistry,
    plan: crate::agent::manifest::AgentPublishPlan,
    config: &crate::adapters::app_server::VerletAppServerConfig,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::manifest::PublishedAgentRecord> {
    if !plan.has_operation_refs() {
        return registry.publish_plan(plan);
    }
    let operation_registry_root = config
        // lexicon-allow: capsule - existing app-server operation binding config field
        .capsule_bindings
        .registry_root
        .as_ref()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "default manifest op:// declarations require operation binding registry_root"
                    .to_string(),
            )
        })?;
    registry.publish_plan_with_operation_registry(plan, operation_registry_root)
}

enum ExistingDefaultManifest {
    Latest(crate::agent::manifest::PublishedAgentRecord),
    VersionOnly(crate::agent::manifest::PublishedAgentRecord),
    None,
}

impl ExistingDefaultManifest {
    fn record(&self) -> Option<&crate::agent::manifest::PublishedAgentRecord> {
        match self {
            Self::Latest(record) | Self::VersionOnly(record) => Some(record),
            Self::None => None,
        }
    }
}

fn load_existing_default_manifest(
    registry: &crate::agent::manifest::LocalAgentRegistry,
) -> crate::kernel::runtime_host::VerletResult<ExistingDefaultManifest> {
    if registry
        .alias_record_path(DEFAULT_AGENT_NAME, "latest")?
        .exists()
    {
        return registry
            .load_ref(DEFAULT_AGENT_REF)
            .map(ExistingDefaultManifest::Latest);
    }
    if registry
        .version_record_path(DEFAULT_AGENT_NAME, "1.0.0")?
        .exists()
    {
        return registry
            .load_version_record(DEFAULT_AGENT_NAME, "1.0.0")
            .map(ExistingDefaultManifest::VersionOnly);
    }
    Ok(ExistingDefaultManifest::None)
}

struct DefaultManifestPublishLock {
    path: std::path::PathBuf,
}

impl DefaultManifestPublishLock {
    fn acquire(
        registry: &crate::agent::manifest::LocalAgentRegistry,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let path = registry.root().join("locks").join("default-manifest");
        let parent = path.parent().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "default manifest publish lock path {} has no parent",
                path.display()
            ))
        })?;
        std::fs::create_dir_all(parent).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to create default manifest publish lock directory {}: {err}",
                parent.display()
            ))
        })?;
        for _ in 0..DEFAULT_MANIFEST_LOCK_ATTEMPTS {
            match std::fs::create_dir(&path) {
                Ok(()) => return Ok(Self { path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    std::thread::sleep(DEFAULT_MANIFEST_LOCK_SLEEP);
                }
                Err(err) => {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                        format!(
                            "failed to acquire default manifest publish lock {}: {err}",
                            path.display()
                        ),
                    ));
                }
            }
        }
        Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "timed out acquiring default manifest publish lock {}",
                path.display()
            ),
        ))
    }
}

impl Drop for DefaultManifestPublishLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir(&self.path);
    }
}

/// Synthesizes the typed default manifest from the daemon's configured
/// envelope (D2): one model profile from the configured provider/model,
/// config-declared operation bindings lowered to pinned `bash_tool` rows, no
/// resources, kernel-default context pipeline, runtime defaults taking
/// `default_cwd` from the daemon cwd (absolute) and `streaming` from the
/// provider surface, with override allowlist `[default_cwd]`.
fn synthesize_default_manifest_with_version(
    config: &crate::adapters::app_server::VerletAppServerConfig,
    supports_streaming: bool,
    version: &str,
) -> crate::kernel::runtime_host::VerletResult<verlet_agent::manifest_schema::AgentManifestSchema> {
    let tools = default_manifest_tools(config)?;
    let manifest = verlet_agent::manifest_schema::AgentManifestSchema {
        identity: verlet_agent::manifest_schema::AgentManifestIdentity {
            name: DEFAULT_AGENT_NAME.to_string(),
            namespace: Some(DEFAULT_AGENT_NAMESPACE.to_string()),
            version: Some(version.to_string()),
            display_name: None,
            description: None,
            kind: Some(verlet_agent::manifest_schema::AGENT_MANIFEST_KIND.to_string()),
            schema_version: Some(1),
            labels: Default::default(),
            publisher: None,
        },
        model_profiles: vec![verlet_agent::manifest_schema::AgentManifestModelProfile {
            id: "default".to_string(),
            provider_ref: format!("provider://{}", config.model_provider),
            model_ref: format!("model://{}/{}", config.model_provider, config.model),
            params: verlet_agent::manifest_schema::AgentManifestModelParams::default(),
            credentials: None,
            retry: None,
            fallbacks: Vec::new(),
        }],
        tools,
        resources: Vec::new(),
        workspace: None,
        skills: Default::default(),
        context: None,
        couplings: Vec::new(),
        policies: verlet_agent::manifest_schema::AgentManifestPolicies {
            allow_child_agents: true,
            ..verlet_agent::manifest_schema::AgentManifestPolicies::default()
        },
        runtime: verlet_agent::manifest_schema::AgentManifestRuntimeDefaults {
            default_cwd: absolute_path_string(&config.cwd)?,
            streaming: supports_streaming,
            overrides: verlet_agent::manifest_schema::AgentManifestRuntimeOverridePolicy {
                allow: vec![
                    verlet_agent::manifest_schema::AgentManifestRuntimeOverrideKey::DefaultCwd,
                ],
            },
            ..verlet_agent::manifest_schema::AgentManifestRuntimeDefaults::default()
        },
    };
    manifest.validate()?;
    Ok(manifest)
}

/// Lowers daemon-configured operation bindings into declared default-manifest
/// tools. The active registry records are resolved at synthesis time, so the
/// bind receipt shows pinned `op://...@sha256` rows instead of ambient loading.
fn default_manifest_tools(
    config: &crate::adapters::app_server::VerletAppServerConfig,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_agent::manifest_schema::AgentManifestTool>>
{
    // lexicon-allow: capsule - legacy config field name
    let bindings = &config.capsule_bindings;
    let mut tools = Vec::new();
    if let Some(registry_root) = bindings.registry_root.as_ref() {
        let registry =
            verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
        if let Ok(record) =
            registry.load_record(crate::operations::kernel_packages::VERLET_THREADS_PACKAGE)
        {
            for operation_name in [
                crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
                crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
                crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
                crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
                crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
            ] {
                let grants = record
                    .manifest
                    .operation(operation_name)
                    .map(|operation| {
                        operation
                            .required_capabilities
                            .iter()
                            .cloned()
                            .map(Into::into)
                            .collect()
                    })
                    .unwrap_or_default();
                tools.push(verlet_agent::manifest_schema::AgentManifestTool::Direct(
                    verlet_agent::manifest_schema::AgentManifestDirectTool {
                        id: format!(
                            "{}.{operation_name}",
                            crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
                        ),
                        tool_name: operation_name.to_string(),
                        operation_ref: format!(
                            "op://{}/{operation_name}@sha256:{}",
                            crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
                            record.active_artifact_hash
                        ),
                        effect_class: Default::default(),
                        grants,
                        attachment: Default::default(),
                    },
                ));
            }
        }
    }
    if bindings.global_operation_names.is_empty() && !bindings.load_all_active_when_unbound {
        return Ok(tools);
    }
    let registry_root = bindings.registry_root.as_ref().ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "default manifest operation declarations require operation binding registry_root"
                .to_string(),
        )
    })?;
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
    let mut records = std::collections::BTreeMap::new();
    for operation_name in &bindings.global_operation_names {
        let canonical_name =
            crate::operations::kernel_packages::canonical_kernel_package_name(operation_name);
        let record = registry.load_record(canonical_name).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "default manifest global operation {operation_name:?} was not found: {err}"
            ))
        })?;
        records.insert(record.name.clone(), record);
    }
    if bindings.load_all_active_when_unbound {
        for record in registry.list_records()? {
            if record.name == crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE {
                continue;
            }
            records.insert(record.name.clone(), record);
        }
    }

    for record in records.into_values() {
        if record.name == crate::operations::kernel_packages::VERLET_THREADS_PACKAGE {
            continue;
        }
        let grants = record
            .capability_grants
            .iter()
            .cloned()
            .map(Into::into)
            .collect::<Vec<_>>();
        for operation in &record.projections.operations {
            tools.push(verlet_agent::manifest_schema::AgentManifestTool::Bash(
                verlet_agent::manifest_schema::AgentManifestBashTool {
                    id: format!("{}.{}", record.name, operation.operation_name),
                    command: operation.operation_name.clone(),
                    operation_ref: format!(
                        "op://{}/{}@sha256:{}",
                        record.name, operation.operation_name, record.active_artifact_hash
                    ),
                    effect_class: Default::default(),
                    grants: grants.clone(),
                    attachment: Default::default(),
                },
            ));
        }
    }
    Ok(tools)
}

fn default_manifest_publish_plan(
    config: &crate::adapters::app_server::VerletAppServerConfig,
    supports_streaming: bool,
    version: &str,
) -> crate::kernel::runtime_host::VerletResult<crate::agent::manifest::AgentPublishPlan> {
    let manifest = synthesize_default_manifest_with_version(config, supports_streaming, version)?;
    let source = default_manifest_source(&manifest)?;
    crate::agent::manifest::AgentPublishPlan::from_source(&source)
}

fn default_manifest_source(
    manifest: &verlet_agent::manifest_schema::AgentManifestSchema,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let profile = manifest.model_profiles.first().ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "default manifest requires one model profile before publish".to_string(),
        )
    })?;
    let version = manifest.identity.version.as_deref().ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "default manifest requires a version before publish".to_string(),
        )
    })?;
    let source = DefaultManifestToml {
        agent: DefaultManifestAgentToml {
            name: &manifest.identity.name,
            namespace: manifest.identity.namespace.as_deref(),
            version,
            kind: manifest.identity.kind.as_deref(),
            schema_version: manifest.identity.schema_version,
        },
        model_profiles: vec![DefaultManifestModelProfileToml {
            id: &profile.id,
            provider_ref: &profile.provider_ref,
            model_ref: &profile.model_ref,
        }],
        tools: manifest
            .tools
            .iter()
            .map(|tool| match tool {
                verlet_agent::manifest_schema::AgentManifestTool::Bash(tool) => {
                    DefaultManifestToolToml {
                        tool_type: "bash_tool",
                        id: &tool.id,
                        command: Some(&tool.command),
                        tool_name: None,
                        operation_ref: &tool.operation_ref,
                        grants: &tool.grants,
                    }
                }
                verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => {
                    DefaultManifestToolToml {
                        tool_type: "direct_tool",
                        id: &tool.id,
                        command: None,
                        tool_name: Some(&tool.tool_name),
                        operation_ref: &tool.operation_ref,
                        grants: &tool.grants,
                    }
                }
                verlet_agent::manifest_schema::AgentManifestTool::ProtocolImport(_) => {
                    unreachable!("default manifest synthesis does not emit protocol imports")
                }
            })
            .collect(),
        policies: DefaultManifestPoliciesToml {
            allow_child_agents: manifest.policies.allow_child_agents,
        },
        runtime: DefaultManifestRuntimeToml {
            default_cwd: &manifest.runtime.default_cwd,
            streaming: manifest.runtime.streaming,
            overrides: DefaultManifestRuntimeOverridesToml {
                allow: manifest.runtime.overrides.allow.clone(),
            },
        },
    };
    toml::to_string(&source).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode default agent manifest: {err}"
        ))
    })
}

fn ensure_default_record_identity(
    record: &crate::agent::manifest::PublishedAgentRecord,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if record.name != DEFAULT_AGENT_NAME
        || !matches!(
            record.namespace.as_deref(),
            Some(DEFAULT_AGENT_NAMESPACE | LEGACY_DEFAULT_AGENT_NAMESPACE)
        )
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "agent registry latest default record is {} in namespace {:?}, expected {}/{}",
                record.name, record.namespace, DEFAULT_AGENT_NAMESPACE, DEFAULT_AGENT_NAME
            ),
        ));
    }
    Ok(())
}

fn patch_bump_version(version: &str) -> crate::kernel::runtime_host::VerletResult<String> {
    let mut parts = version.split('.');
    let major = parts
        .next()
        .ok_or_else(|| invalid_default_version(version))?
        .parse::<u64>()
        .map_err(|_| invalid_default_version(version))?;
    let minor = parts
        .next()
        .ok_or_else(|| invalid_default_version(version))?
        .parse::<u64>()
        .map_err(|_| invalid_default_version(version))?;
    let patch = parts
        .next()
        .ok_or_else(|| invalid_default_version(version))?
        .parse::<u64>()
        .map_err(|_| invalid_default_version(version))?;
    if parts.next().is_some() {
        return Err(invalid_default_version(version));
    }
    let next_patch = patch
        .checked_add(1)
        .ok_or_else(|| invalid_default_version(version))?;
    Ok(format!("{major}.{minor}.{next_patch}"))
}

fn invalid_default_version(version: &str) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
        "default manifest latest version {version:?} is not a patch-bumpable semver"
    ))
}

fn absolute_path_string(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<String> {
    if !path.is_absolute() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "configured instance cwd must be absolute when resolving default manifest path {}",
                path.display(),
            ),
        ));
    }
    Ok(path_string(path))
}

fn path_string(path: &std::path::Path) -> String {
    path.to_string_lossy().into_owned()
}

#[derive(serde::Serialize)]
struct DefaultManifestToml<'a> {
    agent: DefaultManifestAgentToml<'a>,
    model_profiles: Vec<DefaultManifestModelProfileToml<'a>>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tools: Vec<DefaultManifestToolToml<'a>>,
    policies: DefaultManifestPoliciesToml,
    runtime: DefaultManifestRuntimeToml<'a>,
}

#[derive(serde::Serialize)]
struct DefaultManifestAgentToml<'a> {
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    namespace: Option<&'a str>,
    version: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    schema_version: Option<u32>,
}

#[derive(serde::Serialize)]
struct DefaultManifestModelProfileToml<'a> {
    id: &'a str,
    provider_ref: &'a str,
    model_ref: &'a str,
}

#[derive(serde::Serialize)]
struct DefaultManifestToolToml<'a> {
    #[serde(rename = "type")]
    tool_type: &'a str,
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    command: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_name: Option<&'a str>,
    operation_ref: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    grants: &'a Vec<verlet_agent::manifest_schema::AgentManifestGrant>,
}

#[derive(serde::Serialize)]
struct DefaultManifestPoliciesToml {
    allow_child_agents: bool,
}

#[derive(serde::Serialize)]
struct DefaultManifestRuntimeToml<'a> {
    default_cwd: &'a str,
    streaming: bool,
    overrides: DefaultManifestRuntimeOverridesToml,
}

#[derive(serde::Serialize)]
struct DefaultManifestRuntimeOverridesToml {
    allow: Vec<verlet_agent::manifest_schema::AgentManifestRuntimeOverrideKey>,
}

#[cfg(test)]
mod tests;
