//! Default manifest synthesis and publication (lexicon: "default manifest").
//!
//! Every thread binds a manifest; a `thread/start` that names none binds the
//! default manifest — the daemon's configured envelope, synthesized here and
//! published into the agent registry at startup like any other manifest.
//! Nothing about it is special at bind time: it flows through the normal
//! plan → publish → bind pipeline with full receipts.

/// Name of the synthesized default agent record (D1).
pub(crate) const DEFAULT_AGENT_NAME: &str = "default";
/// Namespace marking kernel-synthesized records (D1).
pub(crate) const DEFAULT_AGENT_NAMESPACE: &str = "verlet";
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
pub(crate) fn ensure_default_manifest_published(
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
    // lexicon-allow: capsule - current config field name
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
                        attachment: Default::default(),
                    },
                ));
            }
        }
    }
    let mut records = std::collections::BTreeMap::new();
    if !bindings.global_operation_names.is_empty() || bindings.load_all_active_when_unbound {
        let registry_root = bindings.registry_root.as_ref().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "default manifest operation declarations require operation binding registry_root"
                    .to_string(),
            )
        })?;
        let registry =
            verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
        for operation_name in &bindings.global_operation_names {
            let record = registry.load_record(operation_name).map_err(|err| {
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
    }

    let mut reserved_tool_names = std::collections::BTreeMap::from([
        (
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION.to_string(),
            "kernel thread tool".to_string(),
        ),
        (
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION.to_string(),
            "kernel thread tool".to_string(),
        ),
        (
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION.to_string(),
            "kernel thread tool".to_string(),
        ),
        (
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION.to_string(),
            "kernel thread tool".to_string(),
        ),
        (
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION.to_string(),
            "kernel thread tool".to_string(),
        ),
    ]);
    for record in records.values() {
        if record.name == crate::operations::kernel_packages::VERLET_THREADS_PACKAGE {
            continue;
        }
        for operation in &record.projections.operations {
            let row_id = format!("{}.{}", record.name, operation.operation_name);
            reserved_tool_names.insert(
                operation.operation_name.clone(),
                format!("config-driven operation row {row_id:?}"),
            );
        }
    }
    tools.extend(installed_kit_tools(
        bindings.registry_root.as_deref(),
        &reserved_tool_names,
    )?);

    for record in records.into_values() {
        if record.name == crate::operations::kernel_packages::VERLET_THREADS_PACKAGE {
            continue;
        }
        let attachment_config =
            crate::capabilities::wasm_runner::attachment_config_from_capability_grants(
                &record.capability_grants,
            );
        let attachment = verlet_agent::manifest_schema::AgentManifestAttachment {
            allowed_secrets: attachment_config.allowed_secrets,
            allowed_private_network: attachment_config.allowed_private_network,
        };
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
                    attachment: attachment.clone(),
                },
            ));
        }
    }
    Ok(tools)
}

/// Direct-tool rows for every installed kit.
///
/// The kits root is derived from the operations registry root via
/// `verlet_operations::kit_package::kits_root_for_operations_registry_root`;
/// no registry root or a missing kits directory means no kit tools. For
/// each `InstalledKitRecord` (kits sorted by name) and each of its tool
/// rows, emit `AgentManifestTool::Direct` with id `kit.<kit>.<tool_name>`,
/// the record's pinned `operation_ref` verbatim, `effect_class` parsed from
/// its kebab-case string, and attachment derived from
/// `required_capabilities` through
/// `attachment_config_from_capability_grants`. An unreadable record or a
/// `tool_name` duplicated across installed kits or colliding with another
/// synthesized surface is a hard error naming both sources. Each pinned
/// operation ref and its capability-grant copy are verified against the
/// operation registry before attachment derivation, so a corrupted record
/// cannot silently broaden, drop, or project malformed authority.
fn installed_kit_tools(
    operations_registry_root: Option<&std::path::Path>,
    reserved_tool_names: &std::collections::BTreeMap<String, String>,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_agent::manifest_schema::AgentManifestTool>>
{
    let Some(operations_registry_root) = operations_registry_root else {
        return Ok(Vec::new());
    };
    let kits_root = verlet_operations::kit_package::kits_root_for_operations_registry_root(
        operations_registry_root,
    );
    let store = verlet_operations::kit_package::InstalledKitStore::new(&kits_root);
    let records = store.list().map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "default manifest failed to read installed-kit records from {}: {err}",
            kits_root.display()
        ))
    })?;
    let mut tool_owners = std::collections::BTreeMap::new();
    let mut rows = Vec::new();
    for record in records {
        for tool in record.tools {
            if let Some(source) = reserved_tool_names.get(&tool.tool_name) {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "default manifest installed kit {:?} tools.tool_name {:?} collides with {source}",
                        record.name, tool.tool_name,
                    ),
                ));
            }
            if let Some(first_kit) = tool_owners.insert(tool.tool_name.clone(), record.name.clone())
            {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "default manifest installed kit tool_name {:?} is duplicated across kits {:?} and {:?}",
                        tool.tool_name, first_kit, record.name
                    ),
                ));
            }
            let effect_class = match tool.effect_class.as_str() {
                "pure" => verlet_agent::manifest_schema::EffectClass::Pure,
                "idempotent" => verlet_agent::manifest_schema::EffectClass::Idempotent,
                "at-most-once" => verlet_agent::manifest_schema::EffectClass::AtMostOnce,
                _ => {
                    return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                        format!(
                            "default manifest installed kit {:?} tools.effect_class {:?} for tool_name {:?} must be one of pure, idempotent, at-most-once",
                            record.name, tool.effect_class, tool.tool_name
                        ),
                    ));
                }
            };
            let tool_id = format!("kit.{}.{}", record.name, tool.tool_name);
            let verification = crate::agent::manifest_bind::verify_operation_ref(
                &tool_id,
                &tool.operation_ref,
                operations_registry_root,
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "default manifest installed kit {:?} tool {:?} failed to verify its pinned operation_ref: {err}",
                    record.name, tool.tool_name
                ))
            })?;
            if tool.required_capabilities != verification.record.capability_grants {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "default manifest installed kit {:?} tools.required_capabilities for tool_name {:?} do not match pinned operation_ref {:?}: record has {:?}, installed-kit record has {:?}",
                        record.name,
                        tool.tool_name,
                        tool.operation_ref,
                        verification.record.capability_grants,
                        tool.required_capabilities,
                    ),
                ));
            }
            validate_installed_kit_attachment_capabilities(
                &record.name,
                &tool.tool_name,
                &tool.required_capabilities,
            )?;
            let attachment_config =
                crate::capabilities::wasm_runner::attachment_config_from_capability_grants(
                    &tool.required_capabilities,
                );
            rows.push((
                record.name.clone(),
                tool.tool_name.clone(),
                verlet_agent::manifest_schema::AgentManifestTool::Direct(
                    verlet_agent::manifest_schema::AgentManifestDirectTool {
                        id: tool_id,
                        tool_name: tool.tool_name,
                        operation_ref: tool.operation_ref,
                        effect_class,
                        attachment: verlet_agent::manifest_schema::AgentManifestAttachment {
                            allowed_secrets: attachment_config.allowed_secrets,
                            allowed_private_network: attachment_config.allowed_private_network,
                        },
                    },
                ),
            ));
        }
    }
    rows.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    Ok(rows.into_iter().map(|(_, _, tool)| tool).collect())
}

fn validate_installed_kit_attachment_capabilities(
    kit_name: &str,
    tool_name: &str,
    grants: &std::collections::BTreeSet<String>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    for grant in grants {
        if let Some(secret_name) = grant.strip_prefix("secret:") {
            let validated =
                verlet_metadata::secret_store::validate_secret_name(secret_name).map_err(
                    |err| {
                        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                            "default manifest installed kit {kit_name:?} tools.required_capabilities grant {grant:?} for tool_name {tool_name:?} has an invalid secret name: {err}"
                        ))
                    },
                )?;
            if validated != secret_name {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "default manifest installed kit {kit_name:?} tools.required_capabilities grant {grant:?} for tool_name {tool_name:?} must not contain surrounding whitespace"
                    ),
                ));
            }
            continue;
        }
        if grant == "net.http.private" {
            continue;
        }
        let Some(rule) = grant.strip_prefix("net.http.private:") else {
            continue;
        };
        let malformed = rule.is_empty()
            || rule.strip_prefix("*:").is_some_and(str::is_empty)
            || rule.split_once(':').is_some_and(|(method, origin)| {
                method.is_empty()
                    || (origin.is_empty()
                        && !matches!(method, "http" | "https")
                        && !method.contains('*'))
            });
        if malformed {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "default manifest installed kit {kit_name:?} tools.required_capabilities grant {grant:?} for tool_name {tool_name:?} has an empty private-network method or origin"
                ),
            ));
        }
    }
    Ok(())
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
                        effect_class: tool.effect_class,
                        attachment: &tool.attachment,
                    }
                }
                verlet_agent::manifest_schema::AgentManifestTool::Direct(tool) => {
                    DefaultManifestToolToml {
                        tool_type: "direct_tool",
                        id: &tool.id,
                        command: None,
                        tool_name: Some(&tool.tool_name),
                        operation_ref: &tool.operation_ref,
                        effect_class: tool.effect_class,
                        attachment: &tool.attachment,
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
        || record.namespace.as_deref() != Some(DEFAULT_AGENT_NAMESPACE)
    {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "default agent record {} uses unsupported namespace {:?}; republish the record with the current Verlet version",
                record.ref_uri, record.namespace
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
    #[serde(skip_serializing_if = "verlet_agent::manifest_schema::EffectClass::is_at_most_once")]
    effect_class: verlet_agent::manifest_schema::EffectClass,
    #[serde(
        skip_serializing_if = "verlet_agent::manifest_schema::AgentManifestAttachment::is_empty"
    )]
    attachment: &'a verlet_agent::manifest_schema::AgentManifestAttachment,
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
