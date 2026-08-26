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
    let source = verlet_operations::kit_package::KitSource::load(&options.kit_path)?;
    let members = source.member_packages()?;
    let registry =
        verlet_operations::operation_store::LocalOperationRegistry::new(&options.registry_root);
    let mut published = Vec::with_capacity(members.len());
    for member in members {
        let member_name = member.manifest.identity.name.clone();
        let build = match crate::cli::tool::build_tool_package(&member.manifest_path).await {
            Ok(build) => build,
            Err(err) => return Err(kit_install_member_error(&member_name, err, &published)),
        };
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

    let records_by_name = published
        .iter()
        .map(|record| (record.name.as_str(), record))
        .collect::<std::collections::BTreeMap<_, _>>();
    let tools = source
        .manifest
        .tools
        .iter()
        .map(|tool| {
            let record = records_by_name.get(tool.package.as_str()).ok_or_else(|| {
                crate::cli::usage_error(format!(
                    "kit tools.package {:?} was not published",
                    tool.package
                ))
            })?;
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
        .collect::<crate::kernel::runtime_host::VerletResult<Vec<_>>>()?;
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
    let store = verlet_operations::kit_package::InstalledKitStore::new(&options.kits_root);
    store.save(&installed).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "kit install published every member but failed to write the installed-kit record: {err}; no installed-kit record was written; {}",
            published_install_detail(&published)
        ))
    })?;

    println!("installed kit {}", installed.name);
    println!(
        "version {}",
        installed.version.as_deref().unwrap_or("<none>")
    );
    println!("source sha256:{}", installed.source_hash);
    for record in &published {
        println!(
            "published {} sha256:{}",
            record.name, record.active_artifact_hash
        );
    }
    for tool in &installed.tools {
        println!("tool {} {}", tool.tool_name, tool.operation_ref);
    }
    println!("record {}", store.record_path(&installed.name).display());
    println!("default manifest refreshes at the next daemon startup");
    Ok(())
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
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--kits-root" => kits_root = Some(required_path_value(&mut iter, "--kits-root")?),
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
            "--kits-root" => kits_root = Some(required_path_value(&mut iter, "--kits-root")?),
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
            "--kits-root" => kits_root = Some(required_path_value(&mut iter, "--kits-root")?),
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown kit remove argument {other:?}"
                )));
            }
            _ if name.is_none() => name = Some(arg.to_string_lossy().to_string()),
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

fn required_path_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    iter.next()
        .map(std::path::PathBuf::from)
        .ok_or_else(|| crate::cli::usage_error(format!("{flag} requires a value")))
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
