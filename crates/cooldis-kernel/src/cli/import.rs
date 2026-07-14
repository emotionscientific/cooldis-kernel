//! The `import` subcommand family.

use super::*;

pub(super) async fn run_import(mut args: Vec<OsString>) -> CooldisResult<()> {
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
            other => return Err(usage_error(format!("unknown import subcommand {other:?}"))),
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "build" => import_build(args).await,
        "publish" => import_publish(args).await,
        _ => Err(usage_error(format!(
            "unknown import subcommand {subcommand:?}"
        ))),
    }
}

pub(super) async fn import_build(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_import_args(args, "import build")?;
    if options.help {
        print_import_build_help();
        return Ok(());
    }
    if options.registry_root.is_some() {
        return Err(usage_error(
            "import build does not accept --registry-root because it writes no registry record",
        ));
    }
    let package_path = options
        .package_path
        .ok_or_else(|| usage_error("import build requires --package <path>"))?;
    let build = build_import_package(&package_path).await?;
    print_import_package_build(&build);
    Ok(())
}

pub(super) async fn import_publish(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_import_args(args, "import publish")?;
    if options.help {
        print_import_publish_help();
        return Ok(());
    }
    let package_path = options
        .package_path
        .ok_or_else(|| usage_error("import publish requires --package <path>"))?;
    let build = build_import_package(&package_path).await?;
    print_import_package_build(&build);
    let registry =
        LocalOperationRegistry::new(options.registry_root.unwrap_or_else(default_registry_root));
    let record = registry
        .publish_artifact(PublishOperationRequest {
            name: build.plan.name.clone(),
            artifact_path: build.artifact_path.clone(),
            source: PublishedOperationSource::Import {
                manifest_path: build.package.manifest_path.clone(),
                spec_sha256: build.package.spec_sha256.clone(),
            },
            interface: Some(build.interface.clone()),
            capability_grants: build.plan.capability_requests(),
            metadata: BTreeMap::new(),
        })
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
pub(super) struct BuiltImportPackage {
    package: ImportPackageSource,
    plan: OperationImportPlan,
    artifact_path: PathBuf,
    manifest: WasmOperationManifest,
    interface: ToolInterfaceContract,
    receipt: ImportBuildReceipt,
}

pub(super) async fn build_import_package(package_path: &Path) -> CooldisResult<BuiltImportPackage> {
    let package = ImportPackageSource::load(package_path).map_err(import_error)?;
    let plan = OperationImportPlan::from_package(&package).map_err(import_error)?;
    let artifact = render_openapi_import_artifact(&plan)?;
    let artifact_hash = wasm_sha256(&artifact);
    let output_dir = std::env::temp_dir().join(format!("cooldis-import-build-{}", Uuid::now_v7()));
    let mut output_dir_builder = fs::DirBuilder::new();
    #[cfg(unix)]
    output_dir_builder.mode(0o700);
    output_dir_builder.create(&output_dir).map_err(|error| {
        CooldisError::RuntimeFactory(format!(
            "failed to create import build directory {}: {error}",
            output_dir.display()
        ))
    })?;
    let artifact_path = output_dir.join(format!("{}-{artifact_hash}.wasm", plan.name));
    {
        let mut artifact_options = fs::OpenOptions::new();
        artifact_options.create_new(true).write(true);
        #[cfg(unix)]
        artifact_options.mode(0o600);
        let mut file = artifact_options.open(&artifact_path).map_err(|error| {
            CooldisError::RuntimeFactory(format!(
                "failed to create import artifact {}: {error}",
                artifact_path.display()
            ))
        })?;
        file.write_all(&artifact).map_err(|error| {
            CooldisError::RuntimeFactory(format!(
                "failed to write import artifact {}: {error}",
                artifact_path.display()
            ))
        })?;
        file.sync_all().map_err(|error| {
            CooldisError::RuntimeFactory(format!(
                "failed to sync import artifact {}: {error}",
                artifact_path.display()
            ))
        })?;
    }
    let capabilities = plan.capability_requests();
    let manifest = validate_wasm_artifact(artifact_path.clone(), capabilities.clone()).await?;
    let runtime = ToolRuntimeContract {
        kind: "wasm32-unknown-unknown".to_string(),
        state: Some("stateless".to_string()),
        module_path: None,
        bin_path: Some(artifact_path.clone()),
        release: None,
        timeout_ms: None,
        max_input_bytes: None,
        max_output_bytes: None,
    };
    let identity = ToolPackageIdentity {
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
            ToolOperationInterface {
                name: operation.name.clone(),
                description: operation.description.clone(),
                input_schema: operation.input_schema.clone(),
                output_schema: operation.output_schema.clone(),
                required_capabilities: required_capabilities.clone(),
                command: Some(ToolCommandContract {
                    name: operation.name.clone(),
                    stdin: Some("json".to_string()),
                    stdout: Some("json".to_string()),
                }),
                mcp: None,
                manual: Some(ToolOperationManual {
                    schema_version: 0,
                    tool_name: plan.name.clone(),
                    operation_name: operation.name.clone(),
                    summary,
                    usage: vec![format!(
                        "cooldis tool run {} {} --input '<json>'",
                        plan.name, operation.name
                    )],
                    input_schema: operation.input_schema.clone(),
                    output_schema: operation.output_schema.clone(),
                    required_capabilities,
                    examples: Vec::new(),
                    exit_status: cli_manual_exit_status(),
                    generated: operation.description.is_none(),
                    warnings: Vec::new(),
                }),
            }
        })
        .collect::<Vec<_>>();
    let interface = ToolInterfaceContract {
        schema_version: 0,
        identity,
        runtime,
        operations,
        fixtures: Vec::new(),
    };
    let registered = RegisteredOperation {
        name: plan.name.clone(),
        manifest: manifest.clone(),
        capability_grants: capabilities.clone(),
        metadata: BTreeMap::new(),
    };
    interface.validate_against_operation_record(
        &plan.name,
        &manifest,
        &registered.projections(),
    )?;
    let receipt = ImportBuildReceipt {
        kind: crate::IMPORT_BUILD_RECEIPT_KIND.to_string(),
        schema_version: crate::IMPORT_BUILD_RECEIPT_SCHEMA_VERSION,
        name: plan.name.clone(),
        source_hash: package.source_hash.clone(),
        spec_sha256: package.spec_sha256.clone(),
        artifact_hash,
        operations: plan
            .operations
            .iter()
            .map(|operation| ImportOperationBuild {
                name: operation.name.clone(),
                input_schema: operation.input_schema.clone(),
                output_schema: operation.output_schema.clone(),
            })
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

pub(super) fn print_import_package_build(build: &BuiltImportPackage) {
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
            json_label(&operation.input),
            json_label(&operation.output)
        );
    }
    for capability in &build.receipt.capabilities {
        println!("capability {capability}");
    }
}

pub(super) fn import_error(error: crate::OpenApiImportError) -> CooldisError {
    CooldisError::RuntimeFactory(error.to_string())
}

#[derive(Debug)]
pub(super) struct ImportArgs {
    package_path: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    help: bool,
}

pub(super) fn parse_import_args(args: Vec<OsString>, command: &str) -> CooldisResult<ImportArgs> {
    let mut package_path = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--package" => package_path = Some(required_path_value(&mut iter, "--package")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
        }
    }
    Ok(ImportArgs {
        package_path,
        registry_root,
        help,
    })
}

pub(super) fn print_import_help() {
    println!(
        "cooldis import\n\
\n\
Usage:\n\
  cooldis import build --package cooldis.import.toml\n\
  cooldis import publish --package cooldis.import.toml [--registry-root .cooldis/operations]\n\
\n\
Imports a witnessed local OpenAPI JSON document into a deterministic Wasm\n\
artifact and publishes its selected operations through the normal operation\n\
registry gate. OpenAPI remains an authoring input, not a runtime contract.\n"
    );
}

pub(super) fn print_import_build_help() {
    println!(
        "cooldis import build\n\
\n\
Usage:\n\
  cooldis import build --package cooldis.import.toml\n\
\n\
Verifies the vendored OpenAPI document hash, normalizes the selected operations,\n\
renders and validates deterministic Wasm bytes, and prints an import build\n\
receipt without writing an operation record.\n"
    );
}

pub(super) fn print_import_publish_help() {
    println!(
        "cooldis import publish\n\
\n\
Usage:\n\
  cooldis import publish --package cooldis.import.toml [--registry-root .cooldis/operations]\n\
\n\
Builds a witnessed OpenAPI import and publishes its multi-operation artifact\n\
through the same capability and atomic-write gate as a Wasm tool package.\n"
    );
}
