//! The `tool` subcommand family, package fixtures, and tool registries.

use crate::agent::agent_tool_router::AgentKernelToolProvider as _;
use std::io::Write as _;
#[cfg(test)]
mod tests;

pub(crate) async fn tool_manual(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_manual_args(args)?;
    if options.help {
        print_tool_manual_help();
        return Ok(());
    }
    let tool_name = options
        .tool_name
        .ok_or_else(|| crate::cli::usage_error("tool manual requires <published-tool>"))?;
    let registry_root = options.registry_root.unwrap_or_else(default_registry_root);
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
    let record = registry.load_record(&tool_name)?;
    let manuals = manuals_for_record(&record, options.operation.as_deref())?;
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &manuals).map_err(|err| {
            crate::cli::usage_error(format!("failed to encode manual JSON: {err}"))
        })?;
        println!();
        return Ok(());
    }
    print_manuals(&manuals);
    Ok(())
}

pub(crate) async fn run_tool(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_tool_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "build" => print_tool_build_help(),
            "list" => print_tool_list_help(),
            "publish" => print_tool_publish_help(),
            "run" => print_tool_run_help(),
            "manual" => print_tool_manual_help(),
            "source" => print_tool_source_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "build" => tool_build(args).await,
        "list" => tool_list(args).await,
        "publish" => tool_publish(args).await,
        "run" => tool_run(args).await,
        "manual" => tool_manual(args).await,
        "source" => tool_source(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown tool subcommand {subcommand:?}"
        ))),
    }
}

pub(crate) async fn tool_build(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_build_args(args)?;
    if let Some(package_path) = options.package_path.clone() {
        reject_package_build_overrides(&options)?;
        let build = build_tool_package(&package_path).await?;
        print_tool_package_build(&build);
        return Ok(());
    }
    let config = load_tool_config(options.config_path.as_deref())?;
    let name = options.name.or_else(|| config.name.clone());
    let module_path = options
        .module_path
        .or_else(|| config.module_path.clone())
        .ok_or_else(|| {
            crate::cli::usage_error("tool build requires --module-path or config module_path")
        })?;
    let release = options.release.unwrap_or(config.release.unwrap_or(true));
    let conversion = options.conversion.or(config.conversion);

    let audit = audit_strict_stateless_conversion(&module_path, conversion.as_ref())?;
    println!(
        "tool build {}",
        name.as_deref().unwrap_or(audit.crate_name.as_str())
    );
    println!("module {}", audit.manifest_path.display());
    println!("conversion stateless_wasm");
    for line in audit.provenance_lines() {
        println!("{line}");
    }
    if audit.is_rejected() {
        println!("policy rejected");
        for issue in &audit.issues {
            println!("reason {issue}");
        }
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "strict stateless conversion rejected".to_string(),
        ));
    }
    println!("policy accepted");

    let build = crate::operations::operation_builder::build_rust_wasm_module(
        crate::operations::operation_builder::RustWasmBuildOptions::new(module_path)
            .with_release(release),
    )?;
    let manifest = validate_wasm_artifact(
        build.artifact_path.clone(),
        std::collections::BTreeSet::new(),
    )
    .await?;
    println!("artifact {}", build.artifact_path.display());
    for operation in manifest.operations {
        println!(
            "operation {} {} -> {}",
            operation.name,
            json_label(&operation.input),
            json_label(&operation.output)
        );
    }
    Ok(())
}

pub(crate) async fn tool_list(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_registry_args(args, "tool list")?;
    if options.help {
        print_tool_list_help();
        return Ok(());
    }
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(
        options.registry_root.unwrap_or_else(default_registry_root),
    );
    let records = registry.list_records()?;
    println!(
        "{:<28} {:<16} {:<32} ACTIVE HASH",
        "NAME", "VERSION", "OPERATIONS"
    );
    for record in records {
        let version = record
            .interface
            .as_ref()
            .and_then(|interface| interface.identity.version.as_deref())
            .unwrap_or("-");
        let operations = record
            .manifest
            .operations
            .iter()
            .map(|operation| operation.name.as_str())
            .collect::<Vec<_>>()
            .join(",");
        println!(
            "{:<28} {:<16} {:<32} {}",
            record.name, version, operations, record.active_artifact_hash
        );
    }
    Ok(())
}

pub(crate) async fn tool_publish(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_publish_args(args)?;
    if let Some(package_path) = options.package_path.clone() {
        reject_package_publish_overrides(&options)?;
        let build = build_tool_package(&package_path).await?;
        print_tool_package_build(&build);
        let registry_root = options.registry_root.unwrap_or_else(default_registry_root);
        let registry =
            verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
        let record = registry
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
            .await?;

        println!("published {}", record.name);
        println!("artifact {}", record.active_artifact_hash);
        println!("record {}", registry.record_path(&record.name)?.display());
        for operation in record.manifest.operations {
            println!("operation {}", operation.name);
        }
        return Ok(());
    }
    Err(crate::cli::usage_error(
        "tool publish requires a package proof gate; author verlet.tool.toml and publish with `verlet tool publish --package <verlet.tool.toml>`",
    ))
}

pub(crate) fn manuals_for_record(
    record: &verlet_operations::operation_store::PublishedOperationRecord,
    operation: Option<&str>,
) -> crate::kernel::runtime_host::VerletResult<
    Vec<verlet_operations::tool_package::ToolOperationManual>,
> {
    let mut manuals = Vec::new();
    if let Some(interface) = &record.interface {
        for interface_operation in &interface.operations {
            if operation.is_some_and(|wanted| wanted != interface_operation.name) {
                continue;
            }
            if let Some(manual) = &interface_operation.manual {
                manuals.push(manual.clone());
                continue;
            }
            manuals.push(verlet_operations::tool_package::ToolOperationManual {
                schema_version: 0,
                tool_name: interface.identity.name.clone(),
                operation_name: interface_operation.name.clone(),
                summary: interface_operation
                    .description
                    .clone()
                    .unwrap_or_else(|| format!("Run {}.", interface_operation.name)),
                usage: vec![format!(
                    "verlet tool run {} {} --input '<input>'",
                    interface.identity.name, interface_operation.name
                )],
                input_schema: interface_operation.input_schema.clone(),
                output_schema: interface_operation.output_schema.clone(),
                required_capabilities: interface_operation.required_capabilities.clone(),
                examples: Vec::new(),
                exit_status: cli_manual_exit_status(),
                generated: true,
                warnings: vec![format!(
                    "operation {} has no persisted manual; generated fallback from interface",
                    interface_operation.name
                )],
            });
        }
    } else {
        for projection in &record.projections.operations {
            if operation.is_some_and(|wanted| wanted != projection.operation_name) {
                continue;
            }
            manuals.push(verlet_operations::tool_package::ToolOperationManual {
                schema_version: 0,
                tool_name: record.name.clone(),
                operation_name: projection.operation_name.clone(),
                summary: format!("Run {} from {}.", projection.operation_name, record.name),
                usage: vec![projection.process.command.clone()],
                input_schema: serde_json::to_value(&projection.input)
                    .unwrap_or(serde_json::Value::Null),
                output_schema: serde_json::to_value(&projection.output)
                    .unwrap_or(serde_json::Value::Null),
                required_capabilities: projection
                    .abi
                    .required_capabilities
                    .iter()
                    .cloned()
                    .collect(),
                examples: Vec::new(),
                exit_status: cli_manual_exit_status(),
                generated: true,
                warnings: vec![format!(
                    "operation {} has no tool interface; generated fallback from ABI projection",
                    projection.operation_name
                )],
            });
        }
    }
    if manuals.is_empty() {
        let target = operation
            .map(|value| format!(" operation {value:?}"))
            .unwrap_or_default();
        return Err(crate::cli::usage_error(format!(
            "published tool {:?} has no{target} manual",
            record.name
        )));
    }
    Ok(manuals)
}

pub(crate) fn cli_manual_exit_status() -> Vec<verlet_operations::tool_package::ToolManualExitStatus>
{
    vec![
        verlet_operations::tool_package::ToolManualExitStatus {
            code: 0,
            meaning: "operation succeeded".to_string(),
        },
        verlet_operations::tool_package::ToolManualExitStatus {
            code: 1,
            meaning: "operation failed at runtime".to_string(),
        },
        verlet_operations::tool_package::ToolManualExitStatus {
            code: 2,
            meaning: "caller supplied invalid input or arguments".to_string(),
        },
        verlet_operations::tool_package::ToolManualExitStatus {
            code: 126,
            meaning: "capability or policy denied execution".to_string(),
        },
        verlet_operations::tool_package::ToolManualExitStatus {
            code: 127,
            meaning: "tool or operation was not found".to_string(),
        },
    ]
}

pub(crate) fn print_manuals(manuals: &[verlet_operations::tool_package::ToolOperationManual]) {
    for (index, manual) in manuals.iter().enumerate() {
        if index > 0 {
            println!();
        }
        println!("NAME");
        println!(
            "  {} {} - {}",
            manual.tool_name, manual.operation_name, manual.summary
        );
        println!("USAGE");
        for usage in &manual.usage {
            println!("  {usage}");
        }
        println!("INPUT");
        println!("  {}", compact_json(&manual.input_schema));
        println!("OUTPUT");
        println!("  {}", compact_json(&manual.output_schema));
        println!("CAPABILITIES");
        if manual.required_capabilities.is_empty() {
            println!("  none");
        } else {
            for capability in &manual.required_capabilities {
                println!("  {capability}");
            }
        }
        if !manual.examples.is_empty() {
            println!("EXAMPLES");
            for example in &manual.examples {
                if let Some(command) = &example.command {
                    println!("  {}: {}", example.name, command);
                } else {
                    println!("  {}", example.name);
                }
            }
        }
        println!("EXIT STATUS");
        for status in &manual.exit_status {
            println!("  {} {}", status.code, status.meaning);
        }
        if !manual.warnings.is_empty() {
            println!("WARNINGS");
            for warning in &manual.warnings {
                println!("  {warning}");
            }
        }
    }
}

pub(crate) fn compact_json(value: &serde_json::Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

#[derive(Debug)]
pub(crate) struct BuiltToolPackage {
    package: verlet_operations::tool_package::ToolPackageSource,
    artifact_path: std::path::PathBuf,
    source: verlet_operations::operation_store::PublishedOperationSource,
    manifest: verlet_abi::WasmOperationManifest,
    interface: verlet_operations::tool_package::ToolInterfaceContract,
    receipt: verlet_operations::tool_package::ToolBuildReceipt,
}

pub(crate) async fn build_tool_package(
    package_path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<BuiltToolPackage> {
    let package = verlet_operations::tool_package::ToolPackageSource::load(package_path)?;
    reject_user_kernel_tool_package(&package)?;
    let (artifact_path, source) = build_tool_package_artifact(&package)?;
    let declared_capabilities = package_capability_requests(&package);
    let manifest =
        validate_wasm_artifact(artifact_path.clone(), declared_capabilities.clone()).await?;
    let registered = verlet_operations::RegisteredOperation {
        name: package.manifest.identity.name.clone(),
        manifest: manifest.clone(),
        capability_grants: declared_capabilities,
        metadata: std::collections::BTreeMap::new(),
    };
    let projections = registered.projections();
    let interface = verlet_operations::tool_package::ToolInterfaceContract::from_package(
        &package,
        &manifest,
        &projections,
    )?;
    let fixtures = run_tool_package_fixtures(&package, &artifact_path, &interface).await?;
    let receipt = verlet_operations::tool_package::ToolBuildReceipt::new(
        &package,
        &interface,
        &projections,
        fixtures,
        Some(artifact_path.clone()),
    )?;
    Ok(BuiltToolPackage {
        package,
        artifact_path,
        source,
        manifest,
        interface,
        receipt,
    })
}

pub(crate) fn reject_user_kernel_tool_package(
    package: &verlet_operations::tool_package::ToolPackageSource,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if package.manifest.runtime.kind == "kernel" {
        return Err(crate::cli::usage_error(
            "tool packages with runtime.kind = \"kernel\" are kernel-native records synthesized by Verlet startup; verlet tool build/publish cannot author or publish them",
        ));
    }
    Ok(())
}

pub(crate) fn build_tool_package_artifact(
    package: &verlet_operations::tool_package::ToolPackageSource,
) -> crate::kernel::runtime_host::VerletResult<(
    std::path::PathBuf,
    verlet_operations::operation_store::PublishedOperationSource,
)> {
    match (
        package.manifest.runtime.module_path.clone(),
        package.manifest.runtime.bin_path.clone(),
    ) {
        (Some(module_path), None) => {
            let release = package.manifest.runtime.release.unwrap_or(true);
            let build = crate::operations::operation_builder::build_rust_wasm_module(
                crate::operations::operation_builder::RustWasmBuildOptions::new(
                    module_path.clone(),
                )
                .with_release(release),
            )?;
            Ok((
                build.artifact_path,
                verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path,
                    release,
                },
            ))
        }
        (None, Some(bin_path)) => Ok((
            bin_path.clone(),
            verlet_operations::operation_store::PublishedOperationSource::Wasm { bin_path },
        )),
        (Some(_), Some(_)) => Err(crate::cli::usage_error(
            "tool package runtime cannot declare both module_path and bin_path",
        )),
        (None, None) => Err(crate::cli::usage_error(
            "tool package runtime requires module_path or bin_path",
        )),
    }
}

pub(crate) fn package_capability_requests(
    package: &verlet_operations::tool_package::ToolPackageSource,
) -> std::collections::BTreeSet<String> {
    package
        .manifest
        .operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect()
}

pub(crate) async fn run_tool_package_fixtures(
    package: &verlet_operations::tool_package::ToolPackageSource,
    artifact_path: &std::path::Path,
    interface: &verlet_operations::tool_package::ToolInterfaceContract,
) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_operations::tool_package::ToolFixtureRun>>
{
    let capability_requests = interface.capability_requests();
    let mut config = verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::path(
        artifact_path.to_path_buf(),
    ))
    .with_capability_grants(capability_requests.clone())
    .with_attachment_config(
        crate::capabilities::wasm_runner::attachment_config_from_capability_grants(
            &capability_requests,
        ),
    );
    config = config.with_vfs(package_fixture_vfs(package)?);
    if let Some(max_input_bytes) = package.manifest.runtime.max_input_bytes {
        config = config.with_max_input_bytes(size_limit("max_input_bytes", max_input_bytes)?);
    }
    if let Some(max_output_bytes) = package.manifest.runtime.max_output_bytes {
        config = config.with_max_output_bytes(size_limit("max_output_bytes", max_output_bytes)?);
    }
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(config)?;
    let mut runs = Vec::with_capacity(package.manifest.fixtures.len());
    for fixture in &package.manifest.fixtures {
        let input = std::fs::read(&fixture.input).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read fixture input {}: {err}",
                fixture.input.display()
            ))
        })?;
        let expected = std::fs::read(&fixture.expect).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read fixture expectation {}: {err}",
                fixture.expect.display()
            ))
        })?;
        let output = factory
            .invoke_operation_bytes(&fixture.operation, input)
            .await?
            .output;
        if !fixture_output_matches(&expected, &output) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                format!(
                    "tool package fixture {:?} failed for operation {:?}: expected {}, got {}",
                    fixture.name,
                    fixture.operation,
                    String::from_utf8_lossy(&expected),
                    String::from_utf8_lossy(&output)
                ),
            ));
        }
        runs.push(verlet_operations::tool_package::ToolFixtureRun {
            name: fixture.name.clone(),
            operation: fixture.operation.clone(),
            status: "passed".to_string(),
        });
    }
    Ok(runs)
}

/// Builds the read-only VFS mount available while package fixtures run.
pub(crate) fn package_fixture_vfs(
    package: &verlet_operations::tool_package::ToolPackageSource,
) -> crate::kernel::runtime_host::VerletResult<std::sync::Arc<verlet_vfs::VerletVfs>> {
    let vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(std::sync::Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    let fixture_root = package.package_root.join("fixtures");
    if !fixture_root.is_dir() {
        return Ok(vfs);
    }
    let fixture_fs =
        verlet_vfs::HostFileSystem::new(&fixture_root, verlet_vfs::HostFileSystemMode::ReadOnly)
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to prepare package fixture VFS for {}: {err}",
                    fixture_root.display()
                ))
            })?;
    vfs.mount("/fixtures", std::sync::Arc::new(fixture_fs))
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to mount fixtures: {err}"
            ))
        })?;
    Ok(vfs)
}

pub(crate) fn fixture_output_matches(expected: &[u8], actual: &[u8]) -> bool {
    match (
        serde_json::from_slice::<serde_json::Value>(expected),
        serde_json::from_slice::<serde_json::Value>(actual),
    ) {
        (Ok(expected), Ok(actual)) => expected == actual,
        _ => expected == actual,
    }
}

pub(crate) fn size_limit(
    label: &str,
    value: u64,
) -> crate::kernel::runtime_host::VerletResult<usize> {
    usize::try_from(value).map_err(|_| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "{label} {value} is too large for this platform"
        ))
    })
}

pub(crate) fn print_tool_package_build(build: &BuiltToolPackage) {
    println!("tool package {}", build.package.manifest.identity.name);
    println!("receipt tool_build_v0");
    println!("runtime {}", build.package.manifest.runtime.kind);
    println!("source_hash {}", build.receipt.source_hash);
    println!("interface_hash {}", build.receipt.interface_hash);
    println!("artifact {}", build.artifact_path.display());
    for operation in &build.manifest.operations {
        println!(
            "operation {} {} -> {}",
            operation.name,
            json_label(&operation.input),
            json_label(&operation.output)
        );
    }
    for capability in &build.receipt.capabilities {
        println!("capability {capability}");
    }
    for operation in &build.interface.operations {
        if let Some(command) = &operation.command {
            println!("command {}", command.name);
        }
        if let Some(mcp) = &operation.mcp {
            println!("mcp {}", mcp.tool_name);
        }
    }
    for fixture in &build.receipt.fixtures {
        println!("fixture {} {}", fixture.name, fixture.status);
    }
    for warning in &build.receipt.warnings {
        println!("warning {warning}");
    }
}

pub(crate) fn reject_package_build_overrides(
    options: &BuildArgs,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if options.name.is_some()
        || options.module_path.is_some()
        || options.config_path.is_some()
        || options.release.is_some()
        || options.conversion.is_some()
    {
        return Err(crate::cli::usage_error(
            "tool build --package reads package source, runtime, and policy from verlet.tool.toml",
        ));
    }
    Ok(())
}

pub(crate) fn reject_package_publish_overrides(
    options: &PublishArgs,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if options.name.is_some()
        || options.module_path.is_some()
        || options.bin_path.is_some()
        || options.config_path.is_some()
        || options.release.is_some()
        || !options.capability_grants.is_empty()
        || !options.metadata.is_empty()
        || options.strict_conversion
        || options.conversion.is_some()
    {
        return Err(crate::cli::usage_error(
            "tool publish --package reads name, source, capabilities, and metadata from verlet.tool.toml",
        ));
    }
    Ok(())
}

pub(crate) async fn tool_run(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_run_args(args)?;
    let config_file = load_tool_config(options.config_path.as_deref())?;
    let registered_name = options.registered_name;
    let (module_path, bin_path) = if registered_name.is_some() {
        (options.module_path, options.bin_path)
    } else {
        (
            options.module_path.or(config_file.module_path),
            options.bin_path.or(config_file.bin_path),
        )
    };
    let release = options
        .release
        .unwrap_or(config_file.release.unwrap_or(true));
    let registry_root = options
        .registry_root
        .or(config_file.registry_root)
        .unwrap_or_else(default_registry_root);
    let (mut config, manifest) = match (module_path, bin_path, registered_name) {
        (Some(module_path), None, None) => {
            let build = crate::operations::operation_builder::build_rust_wasm_module(
                crate::operations::operation_builder::RustWasmBuildOptions::new(module_path)
                    .with_release(release),
            )?;
            let config = verlet_wasm::WasmRuntimeConfig::new(
                verlet_wasm::WasmRuntimeArtifact::path(build.artifact_path),
            )
            .with_max_output_bytes(options.max_output_bytes);
            let manifest =
                crate::capabilities::wasm_runner::WasmRuntimeFactory::new(config.clone())?
                    .validate_operation_artifact()
                    .await?;
            (config, manifest)
        }
        (None, Some(bin_path), None) => {
            let config = verlet_wasm::WasmRuntimeConfig::new(
                verlet_wasm::WasmRuntimeArtifact::path(bin_path),
            )
            .with_max_output_bytes(options.max_output_bytes);
            let manifest =
                crate::capabilities::wasm_runner::WasmRuntimeFactory::new(config.clone())?
                    .validate_operation_artifact()
                    .await?;
            (config, manifest)
        }
        (None, None, Some(registered_name)) => {
            let registry =
                verlet_operations::operation_store::LocalOperationRegistry::new(registry_root);
            let record = registry.load_record(&registered_name)?;
            let resolved_secrets = if !verlet_metadata::secret_store::required_secret_names(
                &record.manifest,
            )
            .map_err(crate::cli::secret::secret_cli_error)?
            .is_empty()
            {
                let secret_store =
                    crate::cli::secret::open_secret_store(options.state_home.clone()).await?;
                let resolution = verlet_metadata::secret_store::resolve_manifest_secret_resolution(
                    &secret_store,
                    &record.manifest,
                )
                .await
                .map_err(crate::cli::secret::secret_cli_error)?;
                if !resolution.is_ready() {
                    return Err(crate::cli::usage_error(format!(
                        "missing required operation secrets: {}; import with `verlet secret import <name> --from-env <ENV>` or `verlet secret set <name> --value-stdin`",
                        resolution
                            .missing
                            .iter()
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                resolution.values
            } else {
                std::collections::BTreeMap::new()
            };
            let mut config = registry.load_runtime_config_for_record(&record).await?;
            config = config.with_attachment_config(
                crate::capabilities::wasm_runner::attachment_config_from_capability_grants(
                    &record.capability_grants,
                ),
            );
            if !resolved_secrets.is_empty() {
                config = config.with_secrets(resolved_secrets);
            }
            config = config.with_max_output_bytes(options.max_output_bytes);
            (config, record.manifest)
        }
        (Some(_), Some(_), _) => {
            return Err(crate::cli::usage_error(
                "--module-path and --bin-path are mutually exclusive",
            ));
        }
        (Some(_), None, Some(_)) | (None, Some(_), Some(_)) => {
            return Err(crate::cli::usage_error(
                "tool run cannot combine a published tool name with --module-path or --bin-path",
            ));
        }
        (None, None, None) => {
            return Err(crate::cli::usage_error(
                "tool run requires --module-path, --bin-path, or <published-name> <operation>",
            ));
        }
    };
    let vfs = load_vfs(options.mounts).await?;
    config = config.with_vfs(vfs);
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(config)?;
    if manifest.operation(&options.operation).is_none() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            format!("operation {:?} is not in wasm manifest", options.operation),
        ));
    }
    let output = factory
        .invoke_operation_bytes(&options.operation, options.input.into_bytes())
        .await?;
    std::io::stdout().write_all(&output.output).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "failed to write operation output: {err}"
        ))
    })?;
    std::io::stdout().flush().map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "failed to flush operation output: {err}"
        ))
    })?;
    Ok(())
}

pub(crate) async fn tool_source(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_tool_source_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "add" => print_tool_source_add_help(),
            "discover" => print_tool_source_discover_help(),
            "list" => print_tool_source_list_help(),
            "show" => print_tool_source_show_help(),
            "remove" => print_tool_source_remove_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool source subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "add" => tool_source_add(args).await,
        "discover" => tool_source_discover(args).await,
        "list" => tool_source_list(args).await,
        "show" => tool_source_show(args).await,
        "remove" => tool_source_remove(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown tool source subcommand {subcommand:?}"
        ))),
    }
}

pub(crate) async fn tool_source_add(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_source_add_args(args)?;
    if options.help {
        print_tool_source_add_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("tool source add requires <name>"))?;
    let transport = options
        .kind
        .ok_or_else(|| crate::cli::usage_error("tool source add requires --kind"))?;
    let url = options
        .url
        .ok_or_else(|| crate::cli::usage_error("tool source add requires --url"))?;
    let mut config = crate::adapters::mcp_client::McpRemoteServerConfig::new(name, transport, url)?;
    if let Some(secret) = options.bearer_secret {
        config = config.with_bearer_secret(secret)?;
    }
    for (name, value) in options.headers {
        config = config.with_header(name, value);
    }
    if !options.include_tools.is_empty() {
        config = config.with_include_tools(options.include_tools);
    }
    if let Some(timeout_ms) = options.timeout_ms {
        config = config.with_timeout_ms(timeout_ms);
    }
    if let Some(max_output_bytes) = options.max_output_bytes {
        config = config.with_max_output_bytes(max_output_bytes);
    }
    let registry = open_mcp_source_registry(options.state_home).await?;
    let record = registry.upsert_source_async(config).await?;
    println!("stored tool source {}", record.name);
    let transport: &str = record.transport.as_ref();
    println!("transport {transport}");
    println!("url {}", record.url);
    if let Some(secret) = record.bearer_secret {
        println!("bearer_secret {secret}");
    }
    Ok(())
}

pub(crate) async fn tool_source_discover(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_source_name_args(args, "tool source discover")?;
    if options.help {
        print_tool_source_discover_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("tool source discover requires <name>"))?;
    let registry = open_mcp_source_registry(options.state_home.clone()).await?;
    let record = registry
        .get_source_async(&name)
        .await?
        .ok_or_else(|| crate::cli::usage_error(format!("tool source {name:?} was not found")))?;
    let secret_store = crate::cli::secret::open_secret_store(options.state_home).await?;
    let provider = crate::adapters::mcp_client::McpRemoteToolProvider::connect(
        record.to_config(),
        Some(std::sync::Arc::new(secret_store)),
    )
    .await?;
    let tools = provider.tool_definitions().await;
    let updated = registry.update_discovered_tools_async(&name, tools).await?;
    println!("discovered tool source {}", updated.name);
    for tool in &updated.discovered_tools {
        println!("tool {}", tool.name);
    }
    Ok(())
}

pub(crate) async fn tool_source_list(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_source_list_args(args, "tool source list")?;
    if options.help {
        print_tool_source_list_help();
        return Ok(());
    }
    let registry = open_mcp_source_registry(options.state_home).await?;
    let records = registry.list_sources_async().await?;
    if options.json {
        let json = serde_json::Value::Array(
            records
                .iter()
                .map(|record| record.redacted_json())
                .collect(),
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&json).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to encode tool source list: {err}"
                ))
            })?
        );
        return Ok(());
    }
    if records.is_empty() {
        println!("no tool sources");
        return Ok(());
    }
    for record in records {
        let transport: &str = record.transport.as_ref();
        println!(
            "{} {} tools={}",
            record.name,
            transport,
            record.discovered_tools.len()
        );
    }
    Ok(())
}

pub(crate) async fn tool_source_show(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_source_show_args(args)?;
    if options.help {
        print_tool_source_show_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("tool source show requires <name>"))?;
    let registry = open_mcp_source_registry(options.state_home).await?;
    let record = registry
        .get_source_async(&name)
        .await?
        .ok_or_else(|| crate::cli::usage_error(format!("tool source {name:?} was not found")))?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&record.redacted_json()).map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to encode tool source: {err}"
                ))
            })?
        );
        return Ok(());
    }
    println!("name {}", record.name);
    let transport: &str = record.transport.as_ref();
    println!("transport {transport}");
    println!("url {}", record.url);
    if let Some(secret) = record.bearer_secret {
        println!("bearer_secret {secret}");
    }
    println!("tools {}", record.discovered_tools.len());
    for tool in &record.discovered_tools {
        println!("tool {}", tool.name);
    }
    Ok(())
}

pub(crate) async fn tool_source_remove(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_tool_source_name_args(args, "tool source remove")?;
    if options.help {
        print_tool_source_remove_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| crate::cli::usage_error("tool source remove requires <name>"))?;
    let registry = open_mcp_source_registry(options.state_home).await?;
    if registry.delete_source_async(&name).await? {
        println!("removed tool source {name}");
    } else {
        println!("tool source {name} was not found");
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct BuildArgs {
    name: Option<String>,
    module_path: Option<std::path::PathBuf>,
    package_path: Option<std::path::PathBuf>,
    config_path: Option<std::path::PathBuf>,
    release: Option<bool>,
    conversion: Option<ToolConversionConfig>,
}

#[derive(Debug)]
pub(crate) struct PublishArgs {
    name: Option<String>,
    module_path: Option<std::path::PathBuf>,
    bin_path: Option<std::path::PathBuf>,
    package_path: Option<std::path::PathBuf>,
    config_path: Option<std::path::PathBuf>,
    registry_root: Option<std::path::PathBuf>,
    release: Option<bool>,
    capability_grants: std::collections::BTreeSet<String>,
    metadata: std::collections::BTreeMap<String, serde_json::Value>,
    strict_conversion: bool,
    conversion: Option<ToolConversionConfig>,
}

#[derive(Debug)]
pub(crate) struct ToolRegistryArgs {
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct RunArgs {
    registered_name: Option<String>,
    module_path: Option<std::path::PathBuf>,
    bin_path: Option<std::path::PathBuf>,
    config_path: Option<std::path::PathBuf>,
    state_home: Option<std::path::PathBuf>,
    registry_root: Option<std::path::PathBuf>,
    operation: String,
    input: String,
    mounts: Vec<MountArg>,
    release: Option<bool>,
    max_output_bytes: usize,
}

#[derive(Debug)]
pub(crate) struct ToolManualArgs {
    tool_name: Option<String>,
    operation: Option<String>,
    registry_root: Option<std::path::PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct ToolSourceAddArgs {
    name: Option<String>,
    kind: Option<crate::adapters::mcp_client::McpRemoteTransport>,
    url: Option<String>,
    bearer_secret: Option<String>,
    headers: Vec<(String, String)>,
    include_tools: std::collections::BTreeSet<String>,
    timeout_ms: Option<u64>,
    max_output_bytes: Option<u64>,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct ToolSourceNameArgs {
    name: Option<String>,
    state_home: Option<std::path::PathBuf>,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct ToolSourceListArgs {
    state_home: Option<std::path::PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct ToolSourceShowArgs {
    name: Option<String>,
    state_home: Option<std::path::PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct MountArg {
    guest_path: std::path::PathBuf,
    host_path: std::path::PathBuf,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ToolConfigFile {
    name: Option<String>,
    module_path: Option<std::path::PathBuf>,
    bin_path: Option<std::path::PathBuf>,
    registry_root: Option<std::path::PathBuf>,
    release: Option<bool>,
    #[serde(default)]
    conversion: Option<ToolConversionConfig>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ToolConversionConfig {
    upstream_url: Option<String>,
    upstream_rev: Option<String>,
    upstream_crate: Option<String>,
}

pub(crate) fn parse_build_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<BuildArgs> {
    let mut name = None;
    let mut module_path = None;
    let mut package_path = None;
    let mut config_path = None;
    let mut release = None;
    let mut conversion = ToolConversionConfig::default();
    let mut has_conversion = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--name" => name = Some(required_string_value(&mut iter, "--name")?),
            "--module-path" => module_path = Some(required_path_value(&mut iter, "--module-path")?),
            "--package" => package_path = Some(required_path_value(&mut iter, "--package")?),
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            "--upstream-url" => {
                has_conversion = true;
                conversion.upstream_url = Some(required_string_value(&mut iter, "--upstream-url")?);
            }
            "--upstream-rev" => {
                has_conversion = true;
                conversion.upstream_rev = Some(required_string_value(&mut iter, "--upstream-rev")?);
            }
            "--upstream-crate" => {
                has_conversion = true;
                conversion.upstream_crate =
                    Some(required_string_value(&mut iter, "--upstream-crate")?);
            }
            "--debug" => release = Some(false),
            "--release" => release = Some(true),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool build argument {other:?}"
                )));
            }
        }
    }
    Ok(BuildArgs {
        name,
        module_path,
        package_path,
        config_path,
        release,
        conversion: has_conversion.then_some(conversion),
    })
}

pub(crate) fn parse_publish_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<PublishArgs> {
    let mut name = None;
    let mut module_path = None;
    let mut bin_path = None;
    let mut package_path = None;
    let mut config_path = None;
    let mut registry_root = None;
    let mut release = None;
    let mut capability_grants = std::collections::BTreeSet::new();
    let mut metadata = std::collections::BTreeMap::new();
    let mut strict_conversion = false;
    let mut conversion = ToolConversionConfig::default();
    let mut has_conversion = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--name" => name = Some(required_string_value(&mut iter, "--name")?),
            "--module-path" => module_path = Some(required_path_value(&mut iter, "--module-path")?),
            "--bin-path" => bin_path = Some(required_path_value(&mut iter, "--bin-path")?),
            "--package" => package_path = Some(required_path_value(&mut iter, "--package")?),
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--grant" => {
                capability_grants.insert(required_string_value(&mut iter, "--grant")?);
            }
            "--metadata" => {
                let (key, value) =
                    parse_metadata_arg(&required_string_value(&mut iter, "--metadata")?)?;
                metadata.insert(key, value);
            }
            "--strict-conversion" => strict_conversion = true,
            "--upstream-url" => {
                has_conversion = true;
                strict_conversion = true;
                conversion.upstream_url = Some(required_string_value(&mut iter, "--upstream-url")?);
            }
            "--upstream-rev" => {
                has_conversion = true;
                strict_conversion = true;
                conversion.upstream_rev = Some(required_string_value(&mut iter, "--upstream-rev")?);
            }
            "--upstream-crate" => {
                has_conversion = true;
                strict_conversion = true;
                conversion.upstream_crate =
                    Some(required_string_value(&mut iter, "--upstream-crate")?);
            }
            "--debug" => release = Some(false),
            "--release" => release = Some(true),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool publish argument {other:?}"
                )));
            }
        }
    }
    Ok(PublishArgs {
        name,
        module_path,
        bin_path,
        package_path,
        config_path,
        registry_root,
        release,
        capability_grants,
        metadata,
        strict_conversion,
        conversion: has_conversion.then_some(conversion),
    })
}

pub(crate) fn parse_tool_registry_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<ToolRegistryArgs> {
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
        }
    }
    Ok(ToolRegistryArgs {
        registry_root,
        help,
    })
}

pub(crate) fn parse_run_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<RunArgs> {
    let mut module_path = None;
    let mut bin_path = None;
    let mut config_path = None;
    let mut state_home = None;
    let mut registry_root = None;
    let mut positionals = Vec::new();
    let mut input = String::new();
    let mut mounts = Vec::new();
    let mut release = None;
    let mut max_output_bytes = 1_048_576;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--module-path" => module_path = Some(required_path_value(&mut iter, "--module-path")?),
            "--bin-path" => bin_path = Some(required_path_value(&mut iter, "--bin-path")?),
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--input" => input = required_string_value(&mut iter, "--input")?,
            "--mount" => mounts.push(parse_mount_arg(&required_string_value(
                &mut iter, "--mount",
            )?)?),
            "--debug" => release = Some(false),
            "--release" => release = Some(true),
            "--max-output-bytes" => {
                let value = required_string_value(&mut iter, "--max-output-bytes")?;
                max_output_bytes = value.parse().map_err(|_| {
                    crate::cli::usage_error("--max-output-bytes must be a positive integer")
                })?;
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool run argument {other:?}"
                )));
            }
            _ => {
                positionals.push(arg.to_string_lossy().to_string());
            }
        }
    }
    let (registered_name, operation) = if module_path.is_some() || bin_path.is_some() {
        if positionals.len() != 1 {
            return Err(crate::cli::usage_error(
                "tool run with --module-path or --bin-path accepts exactly one operation name",
            ));
        }
        (None, positionals.remove(0))
    } else {
        match positionals.len() {
            1 => (None, positionals.remove(0)),
            2 => (Some(positionals.remove(0)), positionals.remove(0)),
            _ => {
                return Err(crate::cli::usage_error(
                    "tool run requires <operation> for source/bin or <published-name> <operation>",
                ));
            }
        }
    };

    Ok(RunArgs {
        registered_name,
        module_path,
        bin_path,
        config_path,
        state_home,
        registry_root,
        operation,
        input,
        mounts,
        release,
        max_output_bytes,
    })
}

pub(crate) fn parse_tool_manual_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<ToolManualArgs> {
    let mut registry_root = None;
    let mut json = false;
    let mut help = false;
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--json" => json = true,
            "--help" | "-h" => help = true,
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool manual argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    if positionals.len() > 2 {
        return Err(crate::cli::usage_error(
            "tool manual accepts <published-tool> and optional <operation>",
        ));
    }
    Ok(ToolManualArgs {
        tool_name: positionals.first().cloned(),
        operation: positionals.get(1).cloned(),
        registry_root,
        json,
        help,
    })
}

pub(crate) fn parse_tool_source_add_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<ToolSourceAddArgs> {
    let mut name = None;
    let mut kind = None;
    let mut url = None;
    let mut bearer_secret = None;
    let mut headers = Vec::new();
    let mut include_tools = std::collections::BTreeSet::new();
    let mut timeout_ms = None;
    let mut max_output_bytes = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--kind" => {
                let value = required_string_value(&mut iter, "--kind")?;
                let transport: crate::adapters::mcp_client::McpRemoteTransport =
                    value.parse().map_err(|_| {
                        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                            "unsupported remote MCP transport {value:?}"
                        ))
                    })?;
                kind = Some(transport);
            }
            "--url" => url = Some(required_string_value(&mut iter, "--url")?),
            "--bearer-secret" => {
                bearer_secret = Some(required_string_value(&mut iter, "--bearer-secret")?)
            }
            "--header" => {
                headers.push(parse_header_arg(&required_string_value(
                    &mut iter, "--header",
                )?)?);
            }
            "--include-tool" => {
                include_tools.insert(required_string_value(&mut iter, "--include-tool")?);
            }
            "--timeout-ms" => {
                timeout_ms = Some(parse_u64_arg(
                    "--timeout-ms",
                    &required_string_value(&mut iter, "--timeout-ms")?,
                )?);
            }
            "--max-output-bytes" => {
                max_output_bytes = Some(parse_u64_arg(
                    "--max-output-bytes",
                    &required_string_value(&mut iter, "--max-output-bytes")?,
                )?);
            }
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool source add argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "tool source add accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(ToolSourceAddArgs {
        name,
        kind,
        url,
        bearer_secret,
        headers,
        include_tools,
        timeout_ms,
        max_output_bytes,
        state_home,
        help,
    })
}

pub(crate) fn parse_tool_source_name_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<ToolSourceNameArgs> {
    let mut name = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(format!(
                        "{command} accepts exactly one <name>"
                    )));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(ToolSourceNameArgs {
        name,
        state_home,
        help,
    })
}

pub(crate) fn parse_tool_source_list_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<ToolSourceListArgs> {
    let mut state_home = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--json" => json = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
        }
    }
    Ok(ToolSourceListArgs {
        state_home,
        json,
        help,
    })
}

pub(crate) fn parse_tool_source_show_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<ToolSourceShowArgs> {
    let mut name = None;
    let mut state_home = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--json" => json = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown tool source show argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(crate::cli::usage_error(
                        "tool source show accepts exactly one <name>",
                    ));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(ToolSourceShowArgs {
        name,
        state_home,
        json,
        help,
    })
}

pub(crate) fn required_path_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    iter.next()
        .map(std::path::PathBuf::from)
        .ok_or_else(|| crate::cli::usage_error(format!("{flag} requires a value")))
}

pub(crate) fn required_string_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &'static str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    iter.next()
        .map(|value| value.to_string_lossy().to_string())
        .ok_or_else(|| crate::cli::usage_error(format!("{flag} requires a value")))
}

pub(crate) fn parse_mount_arg(value: &str) -> crate::kernel::runtime_host::VerletResult<MountArg> {
    let Some((guest_path, host_path)) = value.split_once('=') else {
        return Err(crate::cli::usage_error(
            "--mount must use /guest/path=/host/path",
        ));
    };
    let guest_path = std::path::PathBuf::from(guest_path);
    if !guest_path.is_absolute() {
        return Err(crate::cli::usage_error(
            "--mount guest path must be absolute",
        ));
    }
    Ok(MountArg {
        guest_path,
        host_path: std::path::PathBuf::from(host_path),
    })
}

pub(crate) fn parse_header_arg(
    value: &str,
) -> crate::kernel::runtime_host::VerletResult<(String, String)> {
    let Some((name, header_value)) = value.split_once('=') else {
        return Err(crate::cli::usage_error("--header must use name=value"));
    };
    if name.trim().is_empty() {
        return Err(crate::cli::usage_error("--header name cannot be empty"));
    }
    Ok((name.trim().to_string(), header_value.to_string()))
}

pub(crate) fn parse_u64_arg(
    flag: &str,
    value: &str,
) -> crate::kernel::runtime_host::VerletResult<u64> {
    value
        .parse()
        .map_err(|_| crate::cli::usage_error(format!("{flag} must be a positive integer")))
}

pub(crate) fn parse_metadata_arg(
    value: &str,
) -> crate::kernel::runtime_host::VerletResult<(String, serde_json::Value)> {
    let Some((key, raw_value)) = value.split_once('=') else {
        return Err(crate::cli::usage_error("--metadata must use key=value"));
    };
    if key.trim().is_empty() {
        return Err(crate::cli::usage_error("--metadata key cannot be empty"));
    }
    let value = serde_json::from_str(raw_value)
        .unwrap_or_else(|_| serde_json::Value::String(raw_value.into()));
    Ok((key.to_string(), value))
}

pub(crate) fn default_registry_root() -> std::path::PathBuf {
    crate::agent::manifest::default_operations_registry_root()
}

pub(crate) async fn open_mcp_source_registry(
    state_home: Option<std::path::PathBuf>,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::mcp_client::SqliteMcpSourceRegistry>
{
    crate::adapters::mcp_client::SqliteMcpSourceRegistry::open_async(
        crate::cli::secret::metadata_store_path_for_state_home(
            state_home,
            crate::cli::secret::default_project_state_home(),
        ),
    )
    .await
    .map_err(|err| {
        if crate::adapters::app_server::instance::turso_cross_process_lock_error(&err.to_string()) {
            crate::adapters::app_server::instance::cross_process_database_guidance(
                "use the running daemon's mcpSource RPC or stop the daemon and retry",
            )
        } else {
            err
        }
    })
}

pub(crate) fn load_tool_config(
    path: Option<&std::path::Path>,
) -> crate::kernel::runtime_host::VerletResult<ToolConfigFile> {
    let discovered;
    let path = if let Some(path) = path {
        path
    } else {
        discovered = std::path::PathBuf::from("verlet.json");
        if !discovered.exists() {
            return Ok(ToolConfigFile::default());
        }
        discovered.as_path()
    };
    let bytes = std::fs::read(path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to read tool config {}: {err}",
            path.display()
        ))
    })?;
    let mut config: ToolConfigFile = serde_json::from_slice(&bytes).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to decode tool config {} as JSON: {err}",
            path.display()
        ))
    })?;
    let base = path.parent().unwrap_or_else(|| std::path::Path::new("."));
    relativize_config_paths(&mut config, base);
    Ok(config)
}

#[derive(Debug)]
pub(crate) struct ToolConversionAudit {
    manifest_path: std::path::PathBuf,
    crate_name: String,
    issues: Vec<String>,
    conversion: Option<ToolConversionConfig>,
}

impl ToolConversionAudit {
    fn is_rejected(&self) -> bool {
        !self.issues.is_empty()
    }

    fn provenance_lines(&self) -> Vec<String> {
        let Some(conversion) = &self.conversion else {
            return vec!["provenance local".to_string()];
        };
        vec![
            format!(
                "upstream_url {}",
                conversion.upstream_url.as_deref().unwrap_or("<unset>")
            ),
            format!(
                "upstream_rev {}",
                conversion.upstream_rev.as_deref().unwrap_or("<unset>")
            ),
            format!(
                "upstream_crate {}",
                conversion.upstream_crate.as_deref().unwrap_or("<unset>")
            ),
        ]
    }
}

pub(crate) fn audit_strict_stateless_conversion(
    module_path: &std::path::Path,
    conversion: Option<&ToolConversionConfig>,
) -> crate::kernel::runtime_host::VerletResult<ToolConversionAudit> {
    let manifest_path = resolve_cargo_manifest_path(module_path)?;
    let crate_root = manifest_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."));
    let manifest_text = std::fs::read_to_string(&manifest_path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to read Cargo manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: toml::Value = toml::from_str(&manifest_text).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to decode Cargo manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let crate_name = manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .unwrap_or("operation")
        .to_string();
    let dependencies = collect_cargo_dependency_names(&manifest);
    let mut issues = Vec::new();
    let denied = strict_conversion_denied_dependencies()
        .into_iter()
        .filter(|dependency| dependencies.contains(*dependency))
        .collect::<Vec<_>>();
    if !denied.is_empty() {
        issues.push(format!(
            "stateful/native dependency not allowed in stateless Wasm conversion: {}",
            denied.join(", ")
        ));
    }
    if crate_root.join("build.rs").exists() {
        issues.push("build.rs is not allowed in stateless conversion POC".to_string());
    }

    Ok(ToolConversionAudit {
        manifest_path,
        crate_name,
        issues,
        conversion: conversion.cloned(),
    })
}

pub(crate) fn resolve_cargo_manifest_path(
    module_path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    let path = if module_path.file_name() == Some(std::ffi::OsStr::new("Cargo.toml")) {
        module_path.to_path_buf()
    } else {
        module_path.join("Cargo.toml")
    };
    if !path.exists() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!("Rust Wasm module manifest not found at {}", path.display()),
        ));
    }
    Ok(path)
}

pub(crate) fn collect_cargo_dependency_names(
    manifest: &toml::Value,
) -> std::collections::BTreeSet<String> {
    let mut dependencies = std::collections::BTreeSet::new();
    collect_dependency_table(manifest.get("dependencies"), &mut dependencies);
    collect_dependency_table(manifest.get("build-dependencies"), &mut dependencies);
    collect_dependency_table(manifest.get("dev-dependencies"), &mut dependencies);
    if let Some(targets) = manifest.get("target").and_then(toml::Value::as_table) {
        for target in targets.values() {
            collect_dependency_table(target.get("dependencies"), &mut dependencies);
            collect_dependency_table(target.get("build-dependencies"), &mut dependencies);
            collect_dependency_table(target.get("dev-dependencies"), &mut dependencies);
        }
    }
    dependencies
}

pub(crate) fn collect_dependency_table(
    value: Option<&toml::Value>,
    dependencies: &mut std::collections::BTreeSet<String>,
) {
    let Some(table) = value.and_then(toml::Value::as_table) else {
        return;
    };
    dependencies.extend(table.keys().cloned());
}

pub(crate) fn strict_conversion_denied_dependencies() -> std::collections::BTreeSet<&'static str> {
    ["git2", "heed", "libc", "memmap2", "notify", "rayon"]
        .into_iter()
        .collect()
}

pub(crate) fn json_label(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

pub(crate) fn relativize_config_paths(config: &mut ToolConfigFile, base: &std::path::Path) {
    if let Some(path) = config.module_path.take() {
        config.module_path = Some(resolve_config_path(base, path));
    }
    if let Some(path) = config.bin_path.take() {
        config.bin_path = Some(resolve_config_path(base, path));
    }
    if let Some(path) = config.registry_root.take() {
        config.registry_root = Some(resolve_config_path(base, path));
    }
}

pub(crate) fn resolve_config_path(
    base: &std::path::Path,
    path: std::path::PathBuf,
) -> std::path::PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

pub(crate) async fn validate_wasm_artifact(
    artifact_path: std::path::PathBuf,
    capability_grants: std::collections::BTreeSet<String>,
) -> crate::kernel::runtime_host::VerletResult<verlet_abi::WasmOperationManifest> {
    let factory = crate::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::path(artifact_path))
            .with_capability_grants(capability_grants),
    )?;
    factory.validate_operation_artifact().await
}

pub(crate) async fn load_vfs(
    mounts: Vec<MountArg>,
) -> crate::kernel::runtime_host::VerletResult<std::sync::Arc<verlet_vfs::VerletVfs>> {
    let vfs = std::sync::Arc::new(verlet_vfs::VerletVfs::new(std::sync::Arc::new(
        bashkit::InMemoryFs::new(),
    )));
    for mount in mounts {
        let fs = std::sync::Arc::new(
            verlet_vfs::HostFileSystem::new(
                &mount.host_path,
                verlet_vfs::HostFileSystemMode::ReadOnly,
            )
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to open host mount {}: {err}",
                    mount.host_path.display()
                ))
            })?,
        );
        vfs.mount(
            &mount.guest_path,
            fs as std::sync::Arc<dyn verlet_vfs::VerletVfsBackend>,
        )
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to mount {} at {}: {err}",
                mount.host_path.display(),
                mount.guest_path.display()
            ))
        })?;
    }
    Ok(vfs)
}

pub(crate) fn print_tool_help() {
    println!(
        "verlet tool\n\
\n\
Usage:\n\
  verlet tool build --package verlet.tool.toml\n\
  verlet tool build --module-path <dir|Cargo.toml> [--name <name>] [--config verlet.json]\n\
  verlet tool list [--registry-root .verlet/operations]\n\
  verlet tool publish --package verlet.tool.toml [--registry-root .verlet/operations]\n\
  verlet tool run --module-path <dir|Cargo.toml> <operation> --input <text> [--mount /guest=/host]\n\
  verlet tool run --bin-path <module.wasm> <operation> --input <text> [--mount /guest=/host]\n\
  verlet tool run <published-name> <operation> --input <text> [--registry-root .verlet/operations] [--state-home .verlet/state]\n\
  verlet tool manual <published-name> [operation] [--json] [--registry-root .verlet/operations]\n\
  verlet tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>]\n\
  verlet tool source discover <name> [--state-home .verlet/state]\n\
  verlet tool source list [--json] [--state-home .verlet/state]\n\
  verlet tool source show <name> [--json] [--state-home .verlet/state]\n\
  verlet tool source remove <name> [--state-home .verlet/state]\n\
\n\
Tools are the public capability surface. A published tool may contain one or\n\
more ABI operations, and Verlet can project those operations as model tools,\n\
virtual-bash commands, HTTP routes, MCP exports, or other runtime surfaces.\n"
    );
}

pub(crate) fn print_tool_source_help() {
    println!(
        "verlet tool source\n\
\n\
Usage:\n\
  verlet tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>] [--include-tool <tool>] [--state-home .verlet/state]\n\
  verlet tool source discover <name> [--state-home .verlet/state]\n\
  verlet tool source list [--json] [--state-home .verlet/state]\n\
  verlet tool source show <name> [--json] [--state-home .verlet/state]\n\
  verlet tool source remove <name> [--state-home .verlet/state]\n\
\n\
Registers remote MCP servers as Verlet tool sources. MCP is imported through\n\
the tool boundary; source records store URLs, filters, and secret refs, not raw\n\
secret values.\n\
\n\
Tip: use this when Verlet should use someone else's MCP server. To let an\n\
external MCP client use Verlet, run the local Verlet MCP stdio adapter.\n"
    );
}

pub(crate) fn print_tool_source_add_help() {
    println!(
        "verlet tool source add\n\
\n\
Usage:\n\
  verlet tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>] [--header name=value] [--include-tool <tool>] [--state-home .verlet/state]\n\
\n\
Adds or updates a remote MCP tool source without discovering tools yet.\n"
    );
}

pub(crate) fn print_tool_source_discover_help() {
    println!(
        "verlet tool source discover\n\
\n\
Usage:\n\
  verlet tool source discover <name> [--state-home .verlet/state]\n\
\n\
Connects to the remote MCP source, imports tools/list, and stores the discovered\n\
tool definitions in the local metadata DB.\n"
    );
}

pub(crate) fn print_tool_source_list_help() {
    println!(
        "verlet tool source list\n\
\n\
Usage:\n\
  verlet tool source list [--json] [--state-home .verlet/state]\n\
\n\
Lists registered remote MCP tool sources with redacted auth metadata.\n"
    );
}

pub(crate) fn print_tool_source_show_help() {
    println!(
        "verlet tool source show\n\
\n\
Usage:\n\
  verlet tool source show <name> [--json] [--state-home .verlet/state]\n\
\n\
Shows one remote MCP source and its latest discovered tool snapshot.\n"
    );
}

pub(crate) fn print_tool_source_remove_help() {
    println!(
        "verlet tool source remove\n\
\n\
Usage:\n\
  verlet tool source remove <name> [--state-home .verlet/state]\n\
\n\
Removes a remote MCP source record from the local metadata DB.\n"
    );
}

pub(crate) fn print_tool_build_help() {
    println!(
        "verlet tool build\n\
\n\
Usage:\n\
  verlet tool build --package verlet.tool.toml\n\
  verlet tool build --module-path <dir|Cargo.toml> [--name <name>] [--config verlet.json]\n\
  verlet tool build --module-path <dir|Cargo.toml> --upstream-url <url> --upstream-rev <rev> --upstream-crate <crate>\n\
\n\
Builds a publishable Verlet tool package or source module: compile or load the\n\
artifact, validate the Verlet ABI, validate the declared interface, run\n\
fixtures when present, print a build receipt, and write nothing to the registry.\n"
    );
}

pub(crate) fn print_tool_list_help() {
    println!(
        "verlet tool list\n\
\n\
Usage:\n\
  verlet tool list [--registry-root .verlet/operations]\n\
\n\
Lists published operation records and their active artifact hashes.\n"
    );
}

pub(crate) fn print_tool_publish_help() {
    println!(
        "verlet tool publish\n\
\n\
Usage:\n\
  verlet tool publish --package verlet.tool.toml [--registry-root .verlet/operations]\n\
\n\
Publishes a package-validated Wasm tool artifact into the local operation\n\
registry. The package proof gate validates the declared interface and fixtures\n\
before the published tool can become visible through agent attachments.\n"
    );
}

pub(crate) fn print_tool_run_help() {
    println!(
        "verlet tool run\n\
\n\
Usage:\n\
  verlet tool run --module-path <dir|Cargo.toml> <operation> --input <text> [--mount /guest=/host]\n\
  verlet tool run --bin-path <module.wasm> <operation> --input <text> [--mount /guest=/host]\n\
  verlet tool run <published-name> <operation> --input <text> [--registry-root .verlet/operations] [--state-home .verlet/state]\n\
\n\
Runs an operation from source, a Wasm artifact, or a published tool record.\n"
    );
}

pub(crate) fn print_tool_manual_help() {
    println!(
        "verlet tool manual\n\
\n\
Usage:\n\
  verlet tool manual <published-name> [operation] [--json] [--registry-root .verlet/operations]\n\
\n\
Shows the caller-facing contract for a published tool operation. This is the\n\
manual surface agents should read before invoking a tool; implementation details such\n\
as source paths, transports, and secret refs belong in tool source/show output.\n"
    );
}
