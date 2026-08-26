//! The `kit` subcommand family: install, list, and remove tool kits.
//!
//! A kit is surface grammar (see `verlet_operations::kit_package`): install
//! lowers it to ordinary package publishes plus one installed-kit record.
//! After install, the daemon's default manifest synthesizes a `direct_tool`
//! row per installed tool at its next startup; nothing here touches threads
//! directly.

pub(crate) async fn run_kit(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_kit_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "install" => print_kit_install_help(),
            "list" => print_kit_list_help(),
            "remove" => print_kit_remove_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown kit subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "install" => kit_install(args).await,
        "list" => kit_list(args).await,
        "remove" => kit_remove(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown kit subcommand {other:?}"
        ))),
    }
}

pub(crate) fn print_kit_help() {
    println!("Usage: verlet kit <subcommand>");
    println!();
    println!("Subcommands:");
    println!("  install <path>   build, publish, and record a kit's tools");
    println!("  list             list installed kits");
    println!("  remove <name>    remove an installed kit's record");
}

fn print_kit_install_help() {
    println!("Usage: verlet kit install <kit-dir> [--registry-root <path>] [--kits-root <path>]");
    println!();
    println!("Loads verlet.kit.toml from <kit-dir>, builds and publishes each");
    println!("member package into the operation registry, and writes the");
    println!("installed-kit record. The daemon's default manifest picks up the");
    println!("kit's tools at its next startup.");
}

fn print_kit_list_help() {
    println!("Usage: verlet kit list [--kits-root <path>] [--json]");
}

fn print_kit_remove_help() {
    println!("Usage: verlet kit remove <name> [--kits-root <path>]");
    println!();
    println!("Removes the installed-kit record. Published operations stay in");
    println!("the registry; only the default-manifest availability goes away.");
}

/// Install pipeline, in order:
/// 1. `KitSource::load(<kit-dir>)` (validation happens there).
/// 2. For each member package, `build_tool_package` (the same proof gate as
///    `verlet tool publish --package`: fixtures run, interface computed),
///    then `LocalOperationRegistry::publish_artifact` with the package
///    identity name as the record name, exactly as `tool_publish` does.
/// 3. Resolve each kit tool row to its pinned ref
///    `op://<record>/<operation>@sha256:<active_artifact_hash>` and collect
///    the record's capability grants.
/// 4. Write the `InstalledKitRecord` via `InstalledKitStore::save` under
///    the kits root (default [`default_kits_root`]).
/// 5. Print a receipt: kit name, version, source hash, each published
///    record with artifact hash, each tool row with its pinned ref, the
///    record path, and a reminder that the default manifest refreshes at
///    the next daemon startup.
///
/// All-or-nothing on the record: the record is written only after every
/// member published. Publishes that succeeded before a later failure are
/// harmless (content-addressed, unreferenced) and are reported as such.
async fn kit_install(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_kit_install_args(args)?;
    let outcome = install_kit_from(
        &options.kit_path,
        &options.registry_root,
        &options.kits_root,
    )
    .await?;
    for line in outcome.receipt_lines() {
        println!("{line}");
    }
    Ok(())
}

/// What one kit install produced, for receipts. The chat setup window and
/// the CLI print the same [`Self::receipt_lines`].
pub(crate) struct KitInstallOutcome {
    pub installed: verlet_operations::kit_package::InstalledKitRecord,
    pub published: Vec<verlet_operations::operation_store::PublishedOperationRecord>,
    pub record_path: std::path::PathBuf,
}

impl KitInstallOutcome {
    pub(crate) fn receipt_lines(&self) -> Vec<String> {
        let mut lines = vec![
            format!("installed kit {}", self.installed.name),
            format!(
                "version {}",
                self.installed.version.as_deref().unwrap_or("<none>")
            ),
            format!("source sha256:{}", self.installed.source_hash),
        ];
        for record in &self.published {
            lines.push(format!(
                "published {} sha256:{}",
                record.name, record.active_artifact_hash
            ));
        }
        for tool in &self.installed.tools {
            lines.push(format!("tool {} {}", tool.tool_name, tool.operation_ref));
        }
        lines.push(format!("record {}", self.record_path.display()));
        lines.push("default manifest refreshes at the next daemon startup".to_string());
        lines
    }
}

/// The install pipeline behind both `verlet kit install` and the chat setup
/// window's kit step (EMO-611). See [`kit_install`] for the ordering
/// contract.
pub(crate) async fn install_kit_from(
    kit_path: &std::path::Path,
    registry_root: &std::path::Path,
    kits_root: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<KitInstallOutcome> {
    let source = verlet_operations::kit_package::KitSource::load(kit_path)?;
    let members = source.member_packages()?;
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
    let mut published = Vec::with_capacity(members.len());
    for member in members {
        let member_name = member.manifest.identity.name.clone();
        let build = match crate::cli::tool::build_tool_package(&member.manifest_path).await {
            Ok(build) => build,
            Err(err) => return Err(kit_install_member_error(&member_name, err, &published)),
        };
        if build.package.manifest.identity.name != member_name {
            return Err(kit_install_member_error(
                &member_name,
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "kit packages identity.name changed from {member_name:?} to {:?} between validation and build",
                    build.package.manifest.identity.name
                )),
                &published,
            ));
        }
        let record = match registry
            .publish_artifact(
                verlet_operations::operation_store::PublishOperationRequest {
                    name: build.package.manifest.identity.name.clone(),
                    artifact_path: build.artifact_path.clone(),
                    source: build.source.clone(),
                    interface: Some(build.interface.clone()),
                    capability_grants: build.interface.capability_requests(),
                    metadata: std::collections::BTreeMap::new(),
                },
            )
            .await
        {
            Ok(record) => record,
            Err(err) => {
                return Err(kit_install_member_error(
                    &member_name,
                    err.into(),
                    &published,
                ));
            }
        };
        published.push(record);
    }

    let tools = resolve_installed_tools(&source.manifest, &published)
        .map_err(|err| kit_install_record_error(err, &published))?;
    let installed = verlet_operations::kit_package::InstalledKitRecord {
        schema_version: verlet_operations::kit_package::INSTALLED_KIT_SCHEMA_VERSION,
        name: source.manifest.identity.name.clone(),
        version: source.manifest.identity.version.clone(),
        source: verlet_operations::kit_package::InstalledKitSource::Path {
            path: source.kit_root.clone(),
        },
        source_hash: source.source_hash.clone(),
        installed_at_ms: wall_clock_ms(),
        tools,
    };
    let store = verlet_operations::kit_package::InstalledKitStore::new(kits_root);
    store
        .save(&installed)
        .map_err(|err| kit_install_record_error(err.into(), &published))?;

    let record_path = store.record_path(&installed.name);
    Ok(KitInstallOutcome {
        installed,
        published,
        record_path,
    })
}

/// Human list: one line per kit (name, version, tool count, source); with
/// `--json`, the raw records.
async fn kit_list(args: Vec<std::ffi::OsString>) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_kit_list_args(args)?;
    let records =
        verlet_operations::kit_package::InstalledKitStore::new(options.kits_root).list()?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&records).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to encode installed-kit records: {err}"
                ))
            })?
        );
        return Ok(());
    }
    for record in records {
        let source = match record.source {
            verlet_operations::kit_package::InstalledKitSource::Path { path } => {
                format!("path:{}", path.display())
            }
            verlet_operations::kit_package::InstalledKitSource::Git { url, commit } => {
                format!("git:{url}@{commit}")
            }
        };
        println!(
            "{} {} {} tools {}",
            record.name,
            record.version.as_deref().unwrap_or("<none>"),
            record.tools.len(),
            source
        );
    }
    Ok(())
}

async fn kit_remove(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_kit_remove_args(args)?;
    let store = verlet_operations::kit_package::InstalledKitStore::new(options.kits_root);
    if store.remove(&options.name)? {
        println!("removed kit {}", options.name);
    } else {
        println!("kit {} was not installed", options.name);
    }
    Ok(())
}

/// Project-scoped kits root, sibling of the operations registry.
pub(crate) fn default_kits_root() -> std::path::PathBuf {
    std::path::PathBuf::from(".verlet/kits")
}

struct KitInstallArgs {
    kit_path: std::path::PathBuf,
    registry_root: std::path::PathBuf,
    kits_root: std::path::PathBuf,
}

struct KitListArgs {
    kits_root: std::path::PathBuf,
    json: bool,
}

struct KitRemoveArgs {
    name: String,
    kits_root: std::path::PathBuf,
}

fn parse_kit_install_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<KitInstallArgs> {
    let mut kit_path = None;
    let mut registry_root = None;
    let mut kits_root = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            "--kits-root" => {
                kits_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--kits-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown kit install argument {other:?}"
                )));
            }
            _ if kit_path.is_none() => kit_path = Some(std::path::PathBuf::from(arg)),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unexpected kit install path {other:?}"
                )));
            }
        }
    }
    let kit_path =
        kit_path.ok_or_else(|| crate::cli::usage_error("kit install requires <kit-dir>"))?;
    let registry_root = registry_root.unwrap_or_else(crate::cli::tool::default_registry_root);
    let kits_root = kits_root.unwrap_or_else(|| {
        verlet_operations::kit_package::kits_root_for_operations_registry_root(&registry_root)
    });
    Ok(KitInstallArgs {
        kit_path,
        registry_root,
        kits_root,
    })
}

fn parse_kit_list_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<KitListArgs> {
    let mut kits_root = None;
    let mut json = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--kits-root" => {
                kits_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--kits-root",
                )?)
            }
            "--json" => json = true,
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown kit list argument {other:?}"
                )));
            }
        }
    }
    Ok(KitListArgs {
        kits_root: kits_root.unwrap_or_else(default_kits_root),
        json,
    })
}

fn parse_kit_remove_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<KitRemoveArgs> {
    let mut name = None;
    let mut kits_root = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--kits-root" => {
                kits_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--kits-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown kit remove argument {other:?}"
                )));
            }
            _ if name.is_none() => {
                name = Some(arg.into_string().map_err(|value| {
                    crate::cli::usage_error(format!(
                        "kit remove <name> must be valid UTF-8, got {value:?}"
                    ))
                })?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unexpected kit remove name {other:?}"
                )));
            }
        }
    }
    Ok(KitRemoveArgs {
        name: name.ok_or_else(|| crate::cli::usage_error("kit remove requires <name>"))?,
        kits_root: kits_root.unwrap_or_else(default_kits_root),
    })
}

fn kit_install_member_error(
    member_name: &str,
    err: crate::kernel::runtime_host::VerletError,
    published: &[verlet_operations::operation_store::PublishedOperationRecord],
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
        "kit install failed for member {member_name:?}: {err}; no installed-kit record was written; {}",
        published_install_detail(published)
    ))
}

fn kit_install_record_error(
    err: crate::kernel::runtime_host::VerletError,
    published: &[verlet_operations::operation_store::PublishedOperationRecord],
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
        "kit install published every member but did not update the installed-kit record: {err}; an existing record, if any, remains unchanged; {}",
        published_install_detail(published)
    ))
}

fn resolve_installed_tools(
    manifest: &verlet_operations::kit_package::KitManifest,
    published: &[verlet_operations::operation_store::PublishedOperationRecord],
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_operations::kit_package::InstalledKitTool>>
{
    let records_by_name = published
        .iter()
        .map(|record| (record.name.as_str(), record))
        .collect::<std::collections::BTreeMap<_, _>>();
    manifest
        .tools
        .iter()
        .enumerate()
        .map(|(index, tool)| {
            let record = records_by_name.get(tool.package.as_str()).ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "kit tools.package {:?} at tools[{index}] was not published from the rebuilt package",
                    tool.package
                ))
            })?;
            if record.manifest.operation(&tool.operation).is_none() {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "kit tools.operation {:?} at tools[{index}] is absent from rebuilt package {:?}",
                        tool.operation, tool.package
                    ),
                ));
            }
            Ok(verlet_operations::kit_package::InstalledKitTool {
                tool_name: tool.tool_name.clone(),
                operation_ref: format!(
                    "op://{}/{operation}@sha256:{}",
                    record.name,
                    record.active_artifact_hash,
                    operation = tool.operation
                ),
                effect_class: tool.effect_class.clone(),
                required_capabilities: record.capability_grants.clone(),
            })
        })
        .collect()
}

fn published_install_detail(
    published: &[verlet_operations::operation_store::PublishedOperationRecord],
) -> String {
    let published = published
        .iter()
        .map(|record| format!("{}@sha256:{}", record.name, record.active_artifact_hash))
        .collect::<Vec<_>>();
    if published.is_empty() {
        "no member packages were published".to_string()
    } else {
        format!(
            "published member packages remain content-addressed and unreferenced: {}",
            published.join(", ")
        )
    }
}

fn wall_clock_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    #[test]
    fn kit_parsers_reject_flag_looking_values_and_duplicate_positionals() {
        let missing_value = super::parse_kit_list_args(vec![
            std::ffi::OsString::from("--kits-root"),
            std::ffi::OsString::from("--json"),
        ])
        .err()
        .expect("flag-looking value should fail")
        .to_string();
        assert!(missing_value.contains("--kits-root requires a value"));
        assert!(missing_value.contains("--json"));

        let duplicate_install = super::parse_kit_install_args(vec![
            std::ffi::OsString::from("first"),
            std::ffi::OsString::from("second"),
        ])
        .err()
        .expect("duplicate install path should fail")
        .to_string();
        assert!(duplicate_install.contains("unexpected kit install path"));

        let duplicate_remove = super::parse_kit_remove_args(vec![
            std::ffi::OsString::from("first"),
            std::ffi::OsString::from("second"),
        ])
        .err()
        .expect("duplicate remove name should fail")
        .to_string();
        assert!(duplicate_remove.contains("unexpected kit remove name"));
    }

    #[cfg(unix)]
    #[test]
    fn kit_path_flags_preserve_non_utf8_os_strings() {
        use std::os::unix::ffi::OsStringExt as _;

        let path = std::ffi::OsString::from_vec(b"kits-\xff".to_vec());
        let parsed =
            super::parse_kit_list_args(vec![std::ffi::OsString::from("--kits-root"), path.clone()])
                .unwrap();

        assert_eq!(parsed.kits_root, std::path::PathBuf::from(path));
    }

    #[test]
    fn installed_tool_resolution_rejects_operation_missing_after_build() {
        let manifest = verlet_operations::kit_package::KitManifest {
            kind: verlet_operations::kit_package::KIT_KIND.to_string(),
            schema_version: verlet_operations::kit_package::KIT_SCHEMA_VERSION,
            identity: verlet_operations::kit_package::KitIdentity {
                name: "fixture-kit".to_string(),
                version: None,
                description: None,
            },
            packages: vec![std::path::PathBuf::from("member")],
            tools: vec![verlet_operations::kit_package::KitToolDeclaration {
                tool_name: "read".to_string(),
                package: "member".to_string(),
                operation: "read".to_string(),
                effect_class: "idempotent".to_string(),
            }],
        };
        let published = published_record("member", &["replacement"]);

        let error = super::resolve_installed_tools(&manifest, &[published])
            .unwrap_err()
            .to_string();

        assert!(error.contains("tools.operation"), "{error}");
        assert!(error.contains("read"), "{error}");
        assert!(error.contains("rebuilt package"), "{error}");
    }

    fn published_record(
        name: &str,
        operations: &[&str],
    ) -> verlet_operations::operation_store::PublishedOperationRecord {
        let manifest = verlet_abi::WasmOperationManifest {
            abi: "cooldis.operation/0.1".to_string(),
            operations: operations
                .iter()
                .enumerate()
                .map(|(index, operation)| verlet_abi::WasmOperationDefinition {
                    id: (index + 1) as u32,
                    name: (*operation).to_string(),
                    input: Default::default(),
                    output: Default::default(),
                    events: Default::default(),
                    mode: Default::default(),
                    required_capabilities: Vec::new(),
                })
                .collect(),
        };
        let registered = verlet_operations::RegisteredOperation {
            name: name.to_string(),
            manifest: manifest.clone(),
            capability_grants: std::collections::BTreeSet::new(),
            metadata: std::collections::BTreeMap::new(),
        };
        verlet_operations::operation_store::PublishedOperationRecord {
            schema_version: 1,
            name: name.to_string(),
            active_artifact_hash: "a".repeat(64),
            manifest,
            projections: registered.projections(),
            interface: None,
            capability_grants: std::collections::BTreeSet::new(),
            metadata: std::collections::BTreeMap::new(),
            source: verlet_operations::operation_store::PublishedOperationSource::Kernel {
                package: "test".to_string(),
            },
            build: verlet_operations::operation_store::PublishedOperationBuild {
                artifact_path: std::path::PathBuf::from("<test>"),
                published_at_ms: 1,
            },
        }
    }
}
