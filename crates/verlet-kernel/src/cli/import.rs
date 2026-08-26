//! The `import` subcommand family.

use std::io::Write as _;
#[cfg(unix)]
use std::os::unix::fs::DirBuilderExt as _;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as _;

pub(crate) async fn run_import(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_import_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "build" => print_import_build_help(),
            "publish" => print_import_publish_help(),
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown import subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "build" => import_build(args).await,
        "publish" => import_publish(args).await,
        _ => Err(crate::cli::usage_error(format!(
            "unknown import subcommand {subcommand:?}"
        ))),
    }
}

pub(crate) async fn import_build(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_import_args(args, "import build")?;
    if options.help {
        print_import_build_help();
        return Ok(());
    }
    if options.registry_root.is_some() {
        return Err(crate::cli::usage_error(
            "import build does not accept --registry-root because it writes no registry record",
        ));
    }
    let package_path = options
        .package_path
        .ok_or_else(|| crate::cli::usage_error("import build requires --package <path>"))?;
    let build = build_import_package(&package_path).await?;
    print_import_package_build(&build);
    Ok(())
}

pub(crate) async fn import_publish(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_import_args(args, "import publish")?;
    if options.help {
        print_import_publish_help();
        return Ok(());
    }
    let package_path = options
        .package_path
        .ok_or_else(|| crate::cli::usage_error("import publish requires --package <path>"))?;
    let build = build_import_package(&package_path).await?;
    print_import_package_build(&build);
    let registry = verlet_operations::operation_store::LocalOperationRegistry::new(
        options
            .registry_root
            .unwrap_or_else(crate::cli::tool::default_registry_root),
    );
    let record = registry
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: build.plan.name.clone(),
                artifact_path: build.artifact_path.clone(),
                source: verlet_operations::operation_store::PublishedOperationSource::Import {
                    manifest_path: build.package.manifest_path.clone(),
                    spec_sha256: build.package.spec_sha256.clone(),
                },
                interface: Some(build.interface.clone()),
                capability_grants: build.plan.capability_requests(),
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
    Ok(())
}

#[derive(Debug)]
pub(crate) struct BuiltImportPackage {
    package: verlet_operations::import_package::ImportPackageSource,
    plan: verlet_operations::openapi_plan::OperationImportPlan,
    artifact_path: std::path::PathBuf,
    manifest: verlet_abi::WasmOperationManifest,
    interface: verlet_operations::tool_package::ToolInterfaceContract,
    receipt: verlet_operations::import_package::ImportBuildReceipt,
}

pub(crate) async fn build_import_package(
    package_path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<BuiltImportPackage> {
    let package = verlet_operations::import_package::ImportPackageSource::load(package_path)
        .map_err(import_error)?;
    let plan = verlet_operations::openapi_plan::OperationImportPlan::from_package(&package)
        .map_err(import_error)?;
    let artifact = crate::operations::openapi_import::render_openapi_import_artifact(&plan)?;
    let artifact_hash = verlet_operations::operation_store::wasm_sha256(&artifact);
    let output_dir =
        std::env::temp_dir().join(format!("verlet-import-build-{}", uuid::Uuid::now_v7()));
    let mut output_dir_builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    output_dir_builder.mode(0o700);
    output_dir_builder.create(&output_dir).map_err(|error| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to create import build directory {}: {error}",
            output_dir.display()
        ))
    })?;
    let artifact_path = output_dir.join(format!("{}-{artifact_hash}.wasm", plan.name));
    {
        let mut artifact_options = std::fs::OpenOptions::new();
        artifact_options.create_new(true).write(true);
        #[cfg(unix)]
        artifact_options.mode(0o600);
        let mut file = artifact_options.open(&artifact_path).map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to create import artifact {}: {error}",
                artifact_path.display()
            ))
        })?;
        file.write_all(&artifact).map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to write import artifact {}: {error}",
                artifact_path.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to sync import artifact {}: {error}",
                artifact_path.display()
            ))
        })?;
    }
    let capabilities = plan.capability_requests();
    let manifest =
        crate::cli::tool::validate_wasm_artifact(artifact_path.clone(), capabilities.clone())
            .await?;
    let runtime = verlet_operations::tool_package::ToolRuntimeContract {
        kind: "wasm32-unknown-unknown".to_string(),
        state: Some("stateless".to_string()),
        module_path: None,
        bin_path: Some(artifact_path.clone()),
        release: None,
        timeout_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
    };
    let identity = verlet_operations::tool_package::ToolPackageIdentity {
        name: plan.name.clone(),
        version: plan.version.clone(),
        description: plan.description.clone(),
        owner: None,
    };
    let operations = plan
        .operations
        .iter()
        .map(|operation| {
            let required_capabilities = operation.required_capabilities.clone();
            let summary = operation
                .description
                .clone()
                .unwrap_or_else(|| format!("Run imported operation {}.", operation.name));
            verlet_operations::tool_package::ToolOperationInterface {
                name: operation.name.clone(),
                description: operation.description.clone(),
                input_schema: operation.input_schema.clone(),
                output_schema: operation.output_schema.clone(),
                required_capabilities: required_capabilities.clone(),
                command: Some(verlet_operations::tool_package::ToolCommandContract {
                    name: operation.name.clone(),
                    stdin: Some("json".to_string()),
                    stdout: Some("json".to_string()),
                }),
                mcp: None,
                manual: Some(verlet_operations::tool_package::ToolOperationManual {
                    schema_version: 0,
                    tool_name: plan.name.clone(),
                    operation_name: operation.name.clone(),
                    summary,
                    usage: vec![format!(
                        "verlet tool run {} {} --input '<json>'",
                        plan.name, operation.name
                    )],
                    input_schema: operation.input_schema.clone(),
                    output_schema: operation.output_schema.clone(),
                    required_capabilities,
                    examples: Vec::new(),
                    exit_status: crate::cli::tool::cli_manual_exit_status(),
                    generated: operation.description.is_none(),
                    warnings: Vec::new(),
                }),
            }
        })
        .collect::<Vec<_>>();
    let interface = verlet_operations::tool_package::ToolInterfaceContract {
        schema_version: 0,
        identity,
        runtime,
        operations,
        fixtures: Vec::new(),
    };
    let registered = verlet_operations::RegisteredOperation {
        name: plan.name.clone(),
        manifest: manifest.clone(),
        capability_grants: capabilities.clone(),
        metadata: std::collections::BTreeMap::new(),
    };
    let projections = registered
        .projections()
        .with_tool_interface(Some(&interface));
    interface.validate_against_operation_record(&plan.name, &manifest, &projections)?;
    let receipt = verlet_operations::import_package::ImportBuildReceipt {
        kind: verlet_operations::import_package::IMPORT_BUILD_RECEIPT_KIND.to_string(),
        schema_version: verlet_operations::import_package::IMPORT_BUILD_RECEIPT_SCHEMA_VERSION,
        name: plan.name.clone(),
        source_hash: package.source_hash.clone(),
        spec_sha256: package.spec_sha256.clone(),
        artifact_hash,
        operations: plan
            .operations
            .iter()
            .map(
                |operation| verlet_operations::import_package::ImportOperationBuild {
                    name: operation.name.clone(),
                    input_schema: operation.input_schema.clone(),
                    output_schema: operation.output_schema.clone(),
                },
            )
            .collect(),
        capabilities,
        artifact_path: Some(artifact_path.clone()),
    };
    Ok(BuiltImportPackage {
        package,
        plan,
        artifact_path,
        manifest,
        interface,
        receipt,
    })
}

pub(crate) fn print_import_package_build(build: &BuiltImportPackage) {
    println!("import package {}", build.plan.name);
    println!("receipt import_build_v0");
    println!("source_hash {}", build.receipt.source_hash);
    println!("spec_sha256 {}", build.receipt.spec_sha256);
    println!("artifact_hash {}", build.receipt.artifact_hash);
    println!("artifact {}", build.artifact_path.display());
    for operation in &build.manifest.operations {
        println!(
            "operation {} {} -> {}",
            operation.name,
            crate::cli::tool::json_label(&operation.input),
            crate::cli::tool::json_label(&operation.output)
        );
    }
    for capability in &build.receipt.capabilities {
        println!("capability {capability}");
    }
}

pub(crate) fn import_error(
    error: verlet_operations::openapi_plan::OpenApiImportError,
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(error.to_string())
}

#[derive(Debug)]
pub(crate) struct ImportArgs {
    package_path: Option<std::path::PathBuf>,
    registry_root: Option<std::path::PathBuf>,
    help: bool,
}

pub(crate) fn parse_import_args(
    args: Vec<std::ffi::OsString>,
    command: &str,
) -> crate::kernel::runtime_host::VerletResult<ImportArgs> {
    let mut package_path = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--package" => {
                package_path = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--package",
                )?)
            }
            "--registry-root" => {
                registry_root = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--registry-root",
                )?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown {command} argument {other:?}"
                )));
            }
        }
    }
    Ok(ImportArgs {
        package_path,
        registry_root,
        help,
    })
}

pub(crate) fn print_import_help() {
    println!(
        "verlet import\n\
\n\
Usage:\n\
  verlet import build --package verlet.import.toml\n\
  verlet import publish --package verlet.import.toml [--registry-root .verlet/operations]\n\
\n\
Imports a witnessed local OpenAPI JSON document into a deterministic Wasm\n\
artifact and publishes its selected operations through the normal operation\n\
registry gate. OpenAPI remains an authoring input, not a runtime contract.\n"
    );
}

pub(crate) fn print_import_build_help() {
    println!(
        "verlet import build\n\
\n\
Usage:\n\
  verlet import build --package verlet.import.toml\n\
\n\
Verifies the vendored OpenAPI document hash, normalizes the selected operations,\n\
renders and validates deterministic Wasm bytes, and prints an import build\n\
receipt without writing an operation record.\n"
    );
}

pub(crate) fn print_import_publish_help() {
    println!(
        "verlet import publish\n\
\n\
Usage:\n\
  verlet import publish --package verlet.import.toml [--registry-root .verlet/operations]\n\
\n\
Builds a witnessed OpenAPI import and publishes its multi-operation artifact\n\
through the same capability and atomic-write gate as a Wasm tool package.\n"
    );
}
