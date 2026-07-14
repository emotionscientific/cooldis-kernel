use crate::daemon::handle_ingress::ThreadHandleIngressAdapter;
use crate::daemon::remote_store::endpoint::{CooldisDaemonSyncConfig, SqliteSyncEndpoint};
use crate::daemon::remote_store::endpoint_http::DaemonSyncHttpServer;
use crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority;
use crate::daemon::remote_store::process_executor::{
    ProcessRemoteThreadExecutor, RemoteChildBootstrapV1, is_remote_child_command, run_remote_child,
};
use crate::{
    APP_SERVER_ANTHROPIC_BEDROCK_MODEL, APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER,
    APP_SERVER_ANTHROPIC_MODEL, APP_SERVER_ANTHROPIC_PROVIDER, APP_SERVER_BIFROST_MODEL,
    APP_SERVER_BIFROST_PROVIDER, APP_SERVER_OPENAI_COMPATIBLE_MODEL,
    APP_SERVER_OPENAI_COMPATIBLE_PROVIDER, AgentManifestRefStatus, AppServerListenAddr,
    AppServerProviderConfig, BoundCoupling, BoundCouplingSet, CapsuleBindingsConfig,
    CodexTuiConnectConfig, CodexTuiEvent, CodexTuiTestClient, ConsoleAssetConfig, CooldisAppServer,
    CooldisAppServerConfig, CooldisDaemonClockRoute, CooldisDaemonIoBridge,
    CooldisDaemonQueueWorker, CooldisDaemonServiceSpec, CooldisDaemonServiceTarget, CooldisError,
    CooldisIngressConfig, CooldisIoConfig, CooldisIoRouteConfig, CooldisProviderConfig,
    CooldisResult, CooldisVfs, CouplingRunStatus, CouplingScheduler, CouplingSchedulerCycleReceipt,
    EventKind, EventRecord, EventSequence, EventStore, EventStreamId, HostFileSystem,
    HostFileSystemMode, ImportBuildReceipt, ImportOperationBuild, ImportPackageSource,
    JsonRpcNotification, LlmProviderAuthStore, LlmProviderCatalogStore, LoadedCooldisDaemonConfig,
    LocalAgentRegistry, LocalBlobRegistry, LocalOperationRegistry, LocalSkillRegistry,
    McpRemoteServerConfig, McpRemoteToolProvider, McpRemoteTransport, NewEventRecord,
    OperationImportPlan, PublishOperationRequest, PublishSkillPackageRequest, PublishedAgentRecord,
    PublishedOperationRecord, PublishedOperationSource, RegisteredOperation, RouteIngressSink,
    RustWasmBuildOptions, SecretSourceKind, SkillImportPlan, SqliteMcpSourceRegistry,
    SqliteMetadataStore, SqliteSecretStore, SqliteSessionStore, StreamRecordEnvelopeV1,
    SystemDaemonClock, TelegramWebhookServer, TelegramWebhookServerConfig, ThreadId,
    ToolBuildReceipt, ToolCommandContract, ToolFixtureRun, ToolInterfaceContract,
    ToolManualExitStatus, ToolOperationInterface, ToolOperationManual, ToolPackageIdentity,
    ToolPackageSource, ToolRuntimeContract, WasmOperationManifest, WasmOperationValueKind,
    WasmRuntimeArtifact, WasmRuntimeConfig, WasmRuntimeFactory,
    agent::agent_tool_router::AgentKernelToolProvider, build_rust_wasm_module,
    default_blob_registry_root, default_blob_registry_root_for_agent_registry_root,
    default_operations_registry_root, discover_cooldis_daemon_config_path,
    discover_cooldis_project, install_cooldis_daemon_service, load_cooldis_daemon_config,
    load_cooldis_daemon_config_layers, render_cooldis_daemon_service,
    render_openapi_import_artifact, required_secret_names, resolve_manifest_secret_resolution,
    uninstall_cooldis_daemon_service, wasm_sha256,
};
use bashkit::InMemoryFs;
use cooldis_abi::{COUPLING_DISCHARGE_ABI, COUPLING_INVOCATION_ABI};
use cooldis_io_core::{IngressPersistenceMode, IngressSink};
use cooldis_io_pgqrs::{PgqrsIngressQueue, PgqrsQueueConfig};
use cooldis_io_telegram::{TELEGRAM_PROTOCOL, TelegramBotClient, TelegramEgressAdapter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use uuid::Uuid;

mod chat;

pub async fn run() -> CooldisResult<()> {
    let mut args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args
        .first()
        .is_some_and(|command| is_remote_child_command(command))
    {
        return remote_child_run().await;
    }
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_help();
        return Ok(());
    }
    if args
        .first()
        .is_some_and(|arg| arg == "--version" || arg == "-V")
    {
        println!("cooldis {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    if args.is_empty() {
        print_help();
        return Ok(());
    }

    let command = args.remove(0);
    match command.to_string_lossy().as_ref() {
        "commands" => {
            print_commands_help();
            Ok(())
        }
        "help" => run_help(args),
        "init" => agent_init(args).await,
        "agent" => run_agent(args).await,
        "blob" => run_blob(args).await,
        "coupling" => run_coupling(args).await,
        "import" => run_import(args).await,
        "tool" => run_tool(args).await,
        "skill" => run_skill(args).await,
        "secret" => run_secret(args).await,
        "auth" => run_auth(args).await,
        "console" => run_console(args).await,
        "chat" => run_chat(args).await,
        "debug" => run_debug(args).await,
        "daemon" => run_daemon(args).await,
        "rpc" => run_rpc(args).await,
        other => Err(usage_error(format!(
            "unknown command {other:?}; use `cooldis --help`"
        ))),
    }
}

fn run_help(args: Vec<OsString>) -> CooldisResult<()> {
    let path = args
        .into_iter()
        .filter(|arg| arg != "--help" && arg != "-h")
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if path.is_empty() {
        print_help();
        return Ok(());
    }
    print_command_help(&path)
}

fn print_command_help(path: &[String]) -> CooldisResult<()> {
    match path {
        [command] if command == "commands" => print_commands_help(),
        [command] if command == "help" => print_help_help(),
        [command] if command == "console" => print_console_help(),
        [command] if command == "chat" => print_chat_help(),
        [command] if command == "init" => print_agent_init_help(),
        [command] if command == "agent" => print_agent_help(),
        [command] if command == "coupling" => print_coupling_help(),
        [command, subcommand] if command == "coupling" && subcommand == "init" => {
            print_coupling_init_help()
        }
        [command, subcommand] if command == "coupling" && subcommand == "run" => {
            print_coupling_run_help()
        }
        [command] if command == "blob" => print_blob_help(),
        [command, subcommand] if command == "blob" && subcommand == "publish" => {
            print_blob_publish_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "init" => {
            print_agent_init_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "plan" => {
            print_agent_plan_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "publish" => {
            print_agent_publish_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "list" => {
            print_agent_list_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "show" => {
            print_agent_show_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "run" => {
            print_agent_run_help()
        }
        [command] if command == "tool" => print_tool_help(),
        [command] if command == "import" => print_import_help(),
        [command, subcommand] if command == "import" && subcommand == "build" => {
            print_import_build_help()
        }
        [command, subcommand] if command == "import" && subcommand == "publish" => {
            print_import_publish_help()
        }
        [command] if command == "skill" => print_skill_help(),
        [command, subcommand] if command == "skill" && subcommand == "publish" => {
            print_skill_publish_help()
        }
        [command, subcommand] if command == "skill" && subcommand == "import" => {
            print_skill_import_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "build" => {
            print_tool_build_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "list" => {
            print_tool_list_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "publish" => {
            print_tool_publish_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "run" => print_tool_run_help(),
        [command, subcommand] if command == "tool" && subcommand == "manual" => {
            print_tool_manual_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "source" => {
            print_tool_source_help()
        }
        [command, subcommand, action] if command == "tool" && subcommand == "source" => {
            match action.as_str() {
                "add" => print_tool_source_add_help(),
                "discover" => print_tool_source_discover_help(),
                "list" => print_tool_source_list_help(),
                "show" => print_tool_source_show_help(),
                "remove" => print_tool_source_remove_help(),
                other => {
                    return Err(usage_error(format!(
                        "unknown tool source help command {other:?}"
                    )));
                }
            }
        }
        [command] if command == "auth" => print_auth_help(),
        [command, subcommand] if command == "auth" && subcommand == "status" => {
            print_auth_status_help()
        }
        [command, subcommand] if command == "auth" && subcommand == "set" => print_auth_set_help(),
        [command, subcommand] if command == "auth" && subcommand == "delete" => {
            print_auth_delete_help()
        }
        [command] if command == "secret" => print_secret_help(),
        [command, subcommand] if command == "secret" && subcommand == "import" => {
            print_secret_import_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "set" => {
            print_secret_set_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "list" => {
            print_secret_list_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "status" => {
            print_secret_status_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "delete" => {
            print_secret_delete_help()
        }
        [command] if command == "rpc" => print_rpc_help(),
        [command] if command == "debug" => print_debug_help(),
        [command, subcommand] if command == "debug" && subcommand == "rpc" => {
            print_debug_rpc_help()
        }
        [command] if command == "daemon" => print_daemon_help(),
        [command, subcommand] if command == "daemon" && subcommand == "run" => print_daemon_help(),
        [command, subcommand, action]
            if command == "daemon" && subcommand == "config" && action == "validate" =>
        {
            print_daemon_help()
        }
        [command, subcommand, _action] if command == "daemon" && subcommand == "service" => {
            print_daemon_help()
        }
        _ => {
            return Err(usage_error(format!(
                "unknown help command {:?}; use `cooldis commands`",
                path.join(" ")
            )));
        }
    }
    Ok(())
}

async fn run_debug(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_debug_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "rpc" => run_debug_rpc(args).await,
        other => Err(usage_error(format!(
            "unknown debug subcommand {other:?}; use `cooldis debug --help`"
        ))),
    }
}

async fn run_chat(args: Vec<OsString>) -> CooldisResult<()> {
    chat::run(args, chat::ChatInvocation::Chat).await
}

async fn tool_manual(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_manual_args(args)?;
    if options.help {
        print_tool_manual_help();
        return Ok(());
    }
    let tool_name = options
        .tool_name
        .ok_or_else(|| usage_error("tool manual requires <published-tool>"))?;
    let registry_root = options.registry_root.unwrap_or_else(default_registry_root);
    let registry = LocalOperationRegistry::new(registry_root);
    let record = registry.load_record(&tool_name)?;
    let manuals = manuals_for_record(&record, options.operation.as_deref())?;
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &manuals)
            .map_err(|err| usage_error(format!("failed to encode manual JSON: {err}")))?;
        println!();
        return Ok(());
    }
    print_manuals(&manuals);
    Ok(())
}

async fn run_agent(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_agent_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "init" => agent_init(args).await,
        "plan" => agent_plan(args).await,
        "publish" => agent_publish(args).await,
        "list" => agent_list(args).await,
        "show" => agent_show(args).await,
        "run" => agent_run(args).await,
        other => Err(usage_error(format!("unknown agent subcommand {other:?}"))),
    }
}

async fn run_coupling(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_coupling_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "init" => print_coupling_init_help(),
            "run" => print_coupling_run_help(),
            other => {
                return Err(usage_error(format!(
                    "unknown coupling subcommand {other:?}"
                )));
            }
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "init" => coupling_init(args).await,
        "run" => coupling_run(args).await,
        _ => Err(usage_error(format!(
            "unknown coupling subcommand {subcommand:?}"
        ))),
    }
}

async fn coupling_init(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_coupling_init_args(args)?;
    if options.help {
        print_coupling_init_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("coupling init requires <name>"))?;
    let root = options.out_path.unwrap_or_else(|| PathBuf::from(&name));
    write_coupling_project(&name, &root, options.force)?;
    println!("{}", root.display());
    Ok(())
}

async fn agent_init(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_init_args(args)?;
    if options.help {
        print_agent_init_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("agent init requires <name>"))?;
    let target = AgentInitTarget::from_options(&name, options.out_path);
    match target {
        AgentInitTarget::SingleFile(out_path) => {
            write_agent_manifest_file(&name, &out_path, options.force)?;
            println!("{}", out_path.display());
        }
        AgentInitTarget::ProjectDirectory(root) => {
            write_agent_project(&name, &root, options.force)?;
            println!("{}", root.display());
        }
    }
    Ok(())
}

async fn agent_plan(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_manifest_args(args, "agent plan")?;
    if options.help {
        print_agent_plan_help();
        return Ok(());
    }
    let manifest_path = options
        .manifest_path
        .ok_or_else(|| usage_error("agent plan requires <manifest>"))?;
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let mut plan = registry.plan_manifest_path(manifest_path)?;
    let operations_registry_root = agent_operations_registry_root(options.operations_registry_root);
    if operations_registry_root.exists() {
        plan.verify_operation_refs(&operations_registry_root)?;
    } else {
        plan.mark_operation_refs_unverified_offline();
    }
    println!("agent plan {}", plan.ref_uri);
    println!("name: {}", plan.name);
    println!("version: {}", plan.version);
    println!("source_hash: {}", plan.source_hash);
    println!("manifest_hash: {}", plan.manifest_hash);
    println!("models: {}", plan.model_profile_count);
    println!("tools: {}", plan.tool_count);
    println!("resources: {}", plan.resource_count);
    for resolved_ref in &plan.resolved_refs {
        match resolved_ref.status {
            AgentManifestRefStatus::Resolved => {
                let content_hash = resolved_ref.content_hash.as_deref().ok_or_else(|| {
                    CooldisError::RuntimeFactory(format!(
                        "resolved artifact ref {:?} is missing content_hash",
                        resolved_ref.declared
                    ))
                })?;
                let status = plan
                    .verification_status_for_ref(&resolved_ref.declared)
                    .map(|status| format!(" [{}]", status.as_str()))
                    .unwrap_or_default();
                println!(
                    "resolved_ref: {} -> {} ({}){}",
                    resolved_ref.declared,
                    resolved_ref
                        .resolved
                        .as_deref()
                        .unwrap_or(&resolved_ref.declared),
                    content_hash,
                    status
                );
            }
            AgentManifestRefStatus::UnresolvedOffline => {
                let status = plan
                    .verification_status_for_ref(&resolved_ref.declared)
                    .map(|status| format!(" [{}]", status.as_str()))
                    .unwrap_or_default();
                println!(
                    "unresolved-offline_ref: {}{}",
                    resolved_ref.declared, status
                );
            }
        }
    }
    for line in agent_context_source_lines(&plan.resolved_manifest) {
        println!("{line}");
    }
    println!("writes: agent record none");
    Ok(())
}

async fn agent_publish(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_manifest_args(args, "agent publish")?;
    if options.help {
        print_agent_publish_help();
        return Ok(());
    }
    let manifest_path = options
        .manifest_path
        .ok_or_else(|| usage_error("agent publish requires <manifest>"))?;
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let operations_registry_root = agent_operations_registry_root(options.operations_registry_root);
    if options.resolve_ops {
        let resolutions =
            resolve_manifest_operation_refs(&manifest_path, &operations_registry_root)?;
        for resolution in &resolutions {
            println!(
                "resolved operation_ref: {} -> {}",
                resolution.declared, resolution.resolved
            );
        }
    }
    let record = registry
        .publish_manifest_path_with_operation_registry(manifest_path, operations_registry_root)?;
    println!("published {}", record.ref_uri);
    println!("manifest_hash: {}", record.manifest_hash);
    for resolved_ref in &record.resolved_refs {
        let content_hash = resolved_ref.content_hash.as_deref().ok_or_else(|| {
            CooldisError::RuntimeFactory(format!(
                "resolved artifact ref {:?} is missing content_hash",
                resolved_ref.declared
            ))
        })?;
        println!(
            "resolved_ref: {} -> {} ({})",
            resolved_ref.declared,
            resolved_ref
                .resolved
                .as_deref()
                .unwrap_or(&resolved_ref.declared),
            content_hash
        );
    }
    for line in agent_context_source_lines(&record.resolved_manifest) {
        println!("{line}");
    }
    println!(
        "alias: {} -> {}",
        crate::agent_ref_uri(record.namespace.as_deref(), &record.name, "latest"),
        record.version
    );
    println!("record: {}", registry.record_path(&record.name)?.display());
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ManifestOperationRef {
    tool_id: String,
    reference: String,
    grants: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct OperationRefResolution {
    declared: String,
    resolved: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct UnpinnedOperationRef {
    record_name: String,
    operation_name: Option<String>,
}

fn resolve_manifest_operation_refs(
    manifest_path: &Path,
    operation_registry_root: &Path,
) -> CooldisResult<Vec<OperationRefResolution>> {
    let source = fs::read_to_string(manifest_path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to read agent manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let operation_refs = manifest_operation_refs_from_source(&source)?;
    let registry = LocalOperationRegistry::new(operation_registry_root);
    let mut resolutions = Vec::new();
    let mut replacements = BTreeMap::new();
    for operation_ref in operation_refs {
        let Some(parsed) = parse_resolvable_operation_ref(&operation_ref.reference)? else {
            continue;
        };
        let record = registry.load_record(&parsed.record_name).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "tool {:?} operation_ref {:?} was not found in the local operation registry: {err}; seed the operation registry or fix the op:// record name",
                operation_ref.tool_id,
                operation_ref.reference
            ))
        })?;
        let resolved = format!(
            "op://{}{}@sha256:{}",
            parsed.record_name,
            parsed
                .operation_name
                .as_deref()
                .map(|operation| format!("/{operation}"))
                .unwrap_or_default(),
            record.active_artifact_hash
        );
        crate::agent::manifest_bind::verify_operation_ref(
            &operation_ref.tool_id,
            &resolved,
            &operation_ref.grants,
            operation_registry_root,
        )?;
        match replacements.get(&operation_ref.reference) {
            Some(existing) if existing != &resolved => {
                return Err(CooldisError::RuntimeFactory(format!(
                    "operation_ref {:?} resolved inconsistently to {:?} and {:?}",
                    operation_ref.reference, existing, resolved
                )));
            }
            Some(_) => {}
            None => {
                replacements.insert(operation_ref.reference.clone(), resolved.clone());
                resolutions.push(OperationRefResolution {
                    declared: operation_ref.reference,
                    resolved,
                });
            }
        }
    }
    if replacements.is_empty() {
        return Ok(resolutions);
    }
    let rewritten = rewrite_operation_ref_values(&source, &replacements, manifest_path)?;
    write_text_atomically(
        manifest_path,
        format!("agent manifest operation refs {}", manifest_path.display()),
        &rewritten,
    )?;
    Ok(resolutions)
}

fn manifest_operation_refs_from_source(source: &str) -> CooldisResult<Vec<ManifestOperationRef>> {
    let value: toml::Value = toml::from_str(source)
        .map_err(|err| CooldisError::RuntimeFactory(format!("invalid agent manifest: {err}")))?;
    let Some(tools) = value.get("tools").and_then(toml::Value::as_array) else {
        return Ok(Vec::new());
    };
    let mut refs = Vec::new();
    for tool in tools {
        let Some(table) = tool.as_table() else {
            continue;
        };
        let Some(reference) = table.get("operation_ref").and_then(toml::Value::as_str) else {
            continue;
        };
        let tool_id = table
            .get("id")
            .and_then(toml::Value::as_str)
            .unwrap_or("<unknown>")
            .to_string();
        let grants = table
            .get("grants")
            .and_then(toml::Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(toml::Value::as_str)
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        refs.push(ManifestOperationRef {
            tool_id,
            reference: reference.to_string(),
            grants,
        });
    }
    Ok(refs)
}

fn parse_resolvable_operation_ref(reference: &str) -> CooldisResult<Option<UnpinnedOperationRef>> {
    let Some(body) = reference.strip_prefix("op://") else {
        return Ok(None);
    };
    if reference.contains("@sha256:") {
        return Ok(None);
    }
    let body = body.strip_suffix("@latest").unwrap_or(body);
    if body.contains('@') {
        return Err(CooldisError::RuntimeFactory(format!(
            "operation_ref {reference:?} cannot be resolved by --resolve-ops; use op://<record>, op://<record>/<operation>, op://<record>@latest, or op://<record>/<operation>@latest"
        )));
    }
    let segments = body.split('/').collect::<Vec<_>>();
    match segments.as_slice() {
        [record_name] if !record_name.is_empty() => Ok(Some(UnpinnedOperationRef {
            record_name: (*record_name).to_string(),
            operation_name: None,
        })),
        [record_name, operation_name] if !record_name.is_empty() && !operation_name.is_empty() => {
            Ok(Some(UnpinnedOperationRef {
                record_name: (*record_name).to_string(),
                operation_name: Some((*operation_name).to_string()),
            }))
        }
        _ => Err(CooldisError::RuntimeFactory(format!(
            "operation_ref {reference:?} must match op://<record>, op://<record>/<operation>, op://<record>@latest, or op://<record>/<operation>@latest"
        ))),
    }
}

fn rewrite_operation_ref_values(
    source: &str,
    replacements: &BTreeMap<String, String>,
    manifest_path: &Path,
) -> CooldisResult<String> {
    let mut touched = BTreeSet::new();
    let mut rewritten = String::with_capacity(source.len());
    for line in source.split_inclusive('\n') {
        rewritten.push_str(&rewrite_operation_ref_line(
            line,
            replacements,
            &mut touched,
        ));
    }
    let missing = replacements
        .keys()
        .filter(|reference| !touched.contains(*reference))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(CooldisError::RuntimeFactory(format!(
            "failed to rewrite operation_ref value(s) {} in {}; --resolve-ops supports single-line operation_ref string values",
            missing.join(", "),
            manifest_path.display()
        )));
    }
    Ok(rewritten)
}

fn rewrite_operation_ref_line(
    line: &str,
    replacements: &BTreeMap<String, String>,
    touched: &mut BTreeSet<String>,
) -> String {
    let mut index = 0;
    let bytes = line.as_bytes();
    while index < bytes.len() && matches!(bytes[index], b' ' | b'\t') {
        index += 1;
    }
    if !line[index..].starts_with("operation_ref") {
        return line.to_string();
    }
    let mut cursor = index + "operation_ref".len();
    if line[cursor..]
        .chars()
        .next()
        .is_some_and(|ch| ch == '_' || ch.is_ascii_alphanumeric())
    {
        return line.to_string();
    }
    while cursor < bytes.len() && matches!(bytes[cursor], b' ' | b'\t') {
        cursor += 1;
    }
    if bytes.get(cursor) != Some(&b'=') {
        return line.to_string();
    }
    cursor += 1;
    while cursor < bytes.len() && matches!(bytes[cursor], b' ' | b'\t') {
        cursor += 1;
    }
    let Some(&quote) = bytes.get(cursor) else {
        return line.to_string();
    };
    if quote != b'"' && quote != b'\'' {
        return line.to_string();
    }
    let value_start = cursor + 1;
    let mut value_end = value_start;
    while value_end < bytes.len() {
        if bytes[value_end] == quote
            && (quote == b'\'' || !is_escaped_basic_string_quote(bytes, value_end))
        {
            break;
        }
        value_end += 1;
    }
    if value_end >= bytes.len() {
        return line.to_string();
    }
    let value = &line[value_start..value_end];
    let Some(replacement) = replacements.get(value) else {
        return line.to_string();
    };
    touched.insert(value.to_string());
    let mut rewritten = String::with_capacity(line.len() + replacement.len());
    rewritten.push_str(&line[..value_start]);
    rewritten.push_str(replacement);
    rewritten.push_str(&line[value_end..]);
    rewritten
}

fn is_escaped_basic_string_quote(bytes: &[u8], quote_index: usize) -> bool {
    let mut backslashes = 0;
    let mut index = quote_index;
    while index > 0 {
        index -= 1;
        if bytes[index] == b'\\' {
            backslashes += 1;
        } else {
            break;
        }
    }
    backslashes % 2 == 1
}

fn write_text_atomically(path: &Path, label: String, body: &str) -> CooldisResult<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to create {label} directory {}: {err}",
            parent.display()
        ))
    })?;
    let tmp_path = parent.join(format!(".cooldis.tmp.{}", Uuid::now_v7()));
    {
        let mut file = fs::File::create(&tmp_path).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to create temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.write_all(body.as_bytes()).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to write temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to sync temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
    }
    fs::rename(&tmp_path, path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to atomically install {label} {}: {err}",
            path.display()
        ))
    })
}

async fn agent_list(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_registry_args(args, "agent list")?;
    if options.help {
        print_agent_list_help();
        return Ok(());
    }
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let records = registry.list_records()?;
    if records.is_empty() {
        println!("no published agents");
        return Ok(());
    }
    println!("{:<28} {:<16} REF", "NAME", "VERSION");
    for record in records {
        println!(
            "{:<28} {:<16} {}",
            record.name, record.version, record.ref_uri
        );
    }
    Ok(())
}

async fn agent_show(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_show_args(args)?;
    if options.help {
        print_agent_show_help();
        return Ok(());
    }
    let reference = options
        .reference
        .ok_or_else(|| usage_error("agent show requires <agent-ref-or-name>"))?;
    let registry = LocalAgentRegistry::new(agent_registry_root(options.registry_root));
    let record = registry.load_ref(&reference)?;
    print_agent_record_json(&record)
}

async fn agent_run(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_agent_run_args(args)?;
    if options.help {
        print_agent_run_help();
        return Ok(());
    }
    let reference = options
        .reference
        .clone()
        .ok_or_else(|| usage_error("agent run requires <agent-ref>"))?;
    let input = options
        .input
        .clone()
        .ok_or_else(|| usage_error("agent run requires --input <text>"))?;
    let root = PathBuf::from("/tmp").join(format!("cdis-agent-{}", Uuid::now_v7().simple()));
    let cwd = std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current directory: {err}")))?;
    let listen = AppServerListenAddr::WebSocket("127.0.0.1:0".parse().map_err(|err| {
        usage_error(format!(
            "failed to build local app-server listen address: {err}"
        ))
    })?);
    let mut config = CooldisAppServerConfig::local(listen, cwd);
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let agent_registry_root = agent_registry_root(options.registry_root.clone());
    config.blob_registry_root =
        default_blob_registry_root_for_agent_registry_root(&agent_registry_root);
    config.agent_registry_root = agent_registry_root;
    let app = CooldisAppServer::new_local(config).await?;
    let thread_start = app
        .local_json_rpc_request(
            "thread/start",
            json!({
            "agentRef": reference,
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or_else(|| usage_error("thread/start response missing thread id"))?
        .to_string();
    let receipt_ids = manifest_receipt_event_ids(&app, &thread_id).await?;
    let assistant_text = run_local_app_turn(&app, &thread_id, &input).await?;
    println!("{assistant_text}");
    println!("manifest.compile.completed: {}", receipt_ids.0);
    println!("manifest.bind.completed: {}", receipt_ids.1);
    let _ = std::fs::remove_dir_all(root);
    Ok(())
}

/// `cooldis debug rpc` — protocol-level debug client for a RUNNING daemon's
/// app-server websocket. Connects with `CodexTuiTestClient::connect_websocket`,
/// performs the initialize handshake, then dispatches a subcommand.
async fn run_debug_rpc(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_debug_rpc_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "call" => run_debug_rpc_call(args).await,
        "turn" => run_debug_rpc_turn(args).await,
        "tail" => run_debug_rpc_tail(args).await,
        other => Err(usage_error(format!(
            "unknown debug rpc subcommand {other:?}; use `cooldis debug rpc --help`"
        ))),
    }
}

/// Endpoint selection shared by all `debug rpc` subcommands:
/// `--url <ws://…>` wins; else `--config <cooldis.toml>` reads
/// `daemon.app_server.listen`; else default `ws://127.0.0.1:49200/rpc`.
/// `--url` and `--config` together is a usage error.
#[derive(Debug)]
struct DebugRpcEndpointArgs {
    url: Option<String>,
    config: Option<PathBuf>,
}

#[derive(Debug)]
struct DebugRpcCallArgs {
    method: String,
    params: Value,
    endpoint: DebugRpcEndpointArgs,
}

#[derive(Debug)]
enum DebugRpcThreadTarget {
    New,
    Existing(String),
}

#[derive(Debug)]
struct DebugRpcTurnArgs {
    target: DebugRpcThreadTarget,
    json: bool,
    text: String,
    endpoint: DebugRpcEndpointArgs,
}

#[derive(Debug)]
struct DebugRpcTailArgs {
    thread_id: String,
    endpoint: DebugRpcEndpointArgs,
}

enum DebugRpcTurnStreamResult {
    Completed,
    TurnError(String),
}

const DEBUG_RPC_DEFAULT_URL: &str = "ws://127.0.0.1:49200/rpc";
const DEBUG_RPC_TURN_TIMEOUT: Duration = Duration::from_secs(120);

/// One-shot JSON-RPC request: `cooldis debug rpc call <method> [PARAMS_JSON]`.
/// PARAMS_JSON is an inline JSON object (omitted = no params). Prints the
/// result pretty-printed to stdout. A JSON-RPC error response prints the error
/// to stderr and exits 1 (transport failures likewise).
async fn run_debug_rpc_call(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_debug_rpc_call_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    let result = client.request(&options.method, options.params).await?;
    serde_json::to_writer_pretty(std::io::stdout(), &result)
        .map_err(|err| usage_error(format!("failed to encode JSON-RPC result: {err}")))?;
    println!();
    client.close().await?;
    Ok(())
}

/// Run one turn and stream it: `cooldis debug rpc turn (--thread <id> | --new) [--json] <text>`.
/// `--thread` resumes the existing thread (thread/resume, excludeTurns true);
/// `--new` starts a fresh one and prints its id to stderr. Default output mode
/// streams agent-message delta text to stdout as it arrives (flushed per
/// delta), terminated by a newline at turn completion. `--json` instead emits
/// every notification scoped to the thread as one JSON object per line.
/// Exit codes: 0 turn completed, 2 turn error, 1 transport/protocol failure.
async fn run_debug_rpc_turn(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_debug_rpc_turn_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    let thread_id = match &options.target {
        DebugRpcThreadTarget::New => {
            let thread = client.thread_start(json!({})).await?;
            eprintln!("{}", thread.id);
            thread.id
        }
        DebugRpcThreadTarget::Existing(thread_id) => {
            client
                .request(
                    "thread/resume",
                    json!({
                        "threadId": thread_id,
                        "excludeTurns": true,
                    }),
                )
                .await?;
            thread_id.clone()
        }
    };
    let turn = client.turn_start_text(&thread_id, &options.text).await?;
    let stream_result =
        stream_debug_rpc_turn(&mut client, &thread_id, &turn.id, options.json).await?;
    let _ = client.close().await;
    match stream_result {
        DebugRpcTurnStreamResult::Completed => Ok(()),
        DebugRpcTurnStreamResult::TurnError(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    }
}

/// Subscribe and watch: `cooldis debug rpc tail --thread <id>`.
/// Resumes the thread for the subscription, then prints every received
/// notification as one JSON object per line until Ctrl-C/EOF.
async fn run_debug_rpc_tail(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_debug_rpc_tail_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    client
        .request(
            "thread/resume",
            json!({
                "threadId": options.thread_id,
                "excludeTurns": true,
            }),
        )
        .await?;
    loop {
        match client.next_event().await {
            Ok(CodexTuiEvent::Notification(notification)) => {
                print_jsonl_notification(&notification)?;
            }
            Ok(CodexTuiEvent::Error(error)) => {
                return Err(usage_error(format!(
                    "JSON-RPC error {}: {}",
                    error.error.code, error.error.message
                )));
            }
            Ok(CodexTuiEvent::Request(_) | CodexTuiEvent::Response(_)) => {}
            Err(err) if err.to_string().contains("websocket closed") => return Ok(()),
            Err(err) => return Err(err),
        }
    }
}

fn print_debug_rpc_help() {
    println!(
        "cooldis debug rpc\n\
\n\
Usage:\n\
  cooldis debug rpc call <method> [PARAMS_JSON] [--url <ws-url> | --config <cooldis.toml>]\n\
  cooldis debug rpc turn (--thread <id> | --new) [--json] <text> [--url <ws-url> | --config <cooldis.toml>]\n\
  cooldis debug rpc tail --thread <id> [--url <ws-url> | --config <cooldis.toml>]\n\
\n\
Protocol-level debug client for a running daemon's app-server websocket.\n\
Defaults to ws://127.0.0.1:49200/rpc when neither --url nor --config is given.\n\
call prints the JSON-RPC result; turn streams agent deltas as text (or all\n\
notifications as JSONL with --json); tail prints notifications until Ctrl-C.\n"
    );
}

fn parse_debug_rpc_call_args(args: Vec<OsString>) -> CooldisResult<DebugRpcCallArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => endpoint.url = Some(required_string_value(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_path_value(&mut iter, "--config")?),
            other if other.starts_with('-') => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc call argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    let method = positionals
        .first()
        .cloned()
        .ok_or_else(|| debug_rpc_usage_error("cooldis debug rpc call requires <method>"))?;
    if positionals.len() > 2 {
        return Err(debug_rpc_usage_error(
            "cooldis debug rpc call accepts at most one PARAMS_JSON argument",
        ));
    }
    let params = match positionals.get(1) {
        Some(raw) => serde_json::from_str(raw).map_err(|err| {
            debug_rpc_usage_error(format!("invalid PARAMS_JSON for debug rpc call: {err}"))
        })?,
        None => json!({}),
    };
    if !params.is_object() {
        return Err(debug_rpc_usage_error(
            "PARAMS_JSON for debug rpc call must be a JSON object",
        ));
    }
    Ok(DebugRpcCallArgs {
        method,
        params,
        endpoint,
    })
}

fn parse_debug_rpc_turn_args(args: Vec<OsString>) -> CooldisResult<DebugRpcTurnArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut thread_id = None;
    let mut new_thread = false;
    let mut json = false;
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => endpoint.url = Some(required_string_value(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_path_value(&mut iter, "--config")?),
            "--thread" => thread_id = Some(required_string_value(&mut iter, "--thread")?),
            "--new" => new_thread = true,
            "--json" => json = true,
            other if other.starts_with('-') => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc turn argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    let target = match (thread_id, new_thread) {
        (Some(_), true) => {
            return Err(debug_rpc_usage_error(
                "cooldis debug rpc turn requires exactly one of --thread or --new",
            ));
        }
        (Some(thread_id), false) => DebugRpcThreadTarget::Existing(thread_id),
        (None, true) => DebugRpcThreadTarget::New,
        (None, false) => {
            return Err(debug_rpc_usage_error(
                "cooldis debug rpc turn requires exactly one of --thread or --new",
            ));
        }
    };
    if positionals.is_empty() {
        return Err(debug_rpc_usage_error(
            "cooldis debug rpc turn requires <text>",
        ));
    }
    Ok(DebugRpcTurnArgs {
        target,
        json,
        text: positionals.join(" "),
        endpoint,
    })
}

fn parse_debug_rpc_tail_args(args: Vec<OsString>) -> CooldisResult<DebugRpcTailArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut thread_id = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => endpoint.url = Some(required_string_value(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_path_value(&mut iter, "--config")?),
            "--thread" => thread_id = Some(required_string_value(&mut iter, "--thread")?),
            other => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc tail argument {other:?}"
                )));
            }
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    Ok(DebugRpcTailArgs {
        thread_id: thread_id.ok_or_else(|| {
            debug_rpc_usage_error("cooldis debug rpc tail requires --thread <id>")
        })?,
        endpoint,
    })
}

fn validate_debug_rpc_endpoint_args(endpoint: &DebugRpcEndpointArgs) -> CooldisResult<()> {
    if endpoint.url.is_some() && endpoint.config.is_some() {
        return Err(debug_rpc_usage_error(
            "cooldis debug rpc accepts --url or --config, not both",
        ));
    }
    Ok(())
}

fn resolve_debug_rpc_endpoint(endpoint: &DebugRpcEndpointArgs) -> CooldisResult<String> {
    validate_debug_rpc_endpoint_args(endpoint)?;
    if let Some(url) = &endpoint.url {
        return Ok(url.clone());
    }
    if let Some(config_path) = &endpoint.config {
        let loaded = load_cooldis_daemon_config(Some(config_path))?;
        match loaded.config.app_server.listen_addr()? {
            AppServerListenAddr::WebSocket(_) => return Ok(loaded.config.app_server.listen),
            AppServerListenAddr::Unix(_) => {
                return Err(usage_error("daemon listens on a unix socket; pass --url"));
            }
        }
    }
    Ok(DEBUG_RPC_DEFAULT_URL.to_string())
}

async fn connect_debug_rpc_client(url: &str) -> CooldisResult<CodexTuiTestClient<TcpStream>> {
    CodexTuiTestClient::connect_websocket(
        url,
        CodexTuiConnectConfig {
            client_name: "cooldis-debug-rpc".to_string(),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await
}

async fn stream_debug_rpc_turn(
    client: &mut CodexTuiTestClient<TcpStream>,
    thread_id: &str,
    turn_id: &str,
    json_output: bool,
) -> CooldisResult<DebugRpcTurnStreamResult> {
    let deadline = tokio::time::sleep(DEBUG_RPC_TURN_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                if !json_output {
                    println!();
                }
                return Err(usage_error(format!(
                    "timed out after {}s waiting for turn {turn_id}",
                    DEBUG_RPC_TURN_TIMEOUT.as_secs()
                )));
            }
            event = client.next_event() => {
                match event? {
                    CodexTuiEvent::Notification(notification) => {
                        if json_output && notification_thread_id(&notification) == Some(thread_id) {
                            print_jsonl_notification(&notification)?;
                        }
                        if notification.method == "item/agentMessage/delta"
                            && notification_matches_thread_turn(&notification, thread_id, turn_id)
                            && !json_output
                            && let Some(delta) = notification_delta(&notification)
                        {
                            print!("{delta}");
                            flush_stdout()?;
                        }
                        if notification_is_turn_error(&notification, thread_id, turn_id) {
                            if !json_output {
                                println!();
                            }
                            return Ok(DebugRpcTurnStreamResult::TurnError(
                                notification_turn_error_message(&notification),
                            ));
                        }
                        if notification.method == "turn/completed"
                            && notification_turn_id(&notification) == Some(turn_id)
                        {
                            if !json_output {
                                println!();
                            }
                            return Ok(DebugRpcTurnStreamResult::Completed);
                        }
                    }
                    CodexTuiEvent::Error(error) => {
                        if !json_output {
                            println!();
                        }
                        return Err(usage_error(format!(
                            "JSON-RPC error {}: {}",
                            error.error.code, error.error.message
                        )));
                    }
                    CodexTuiEvent::Request(_) | CodexTuiEvent::Response(_) => {}
                }
            }
        }
    }
}

fn print_jsonl_notification(notification: &JsonRpcNotification) -> CooldisResult<()> {
    serde_json::to_writer(std::io::stdout(), notification)
        .map_err(|err| usage_error(format!("failed to encode notification JSON: {err}")))?;
    println!();
    flush_stdout()
}

fn notification_thread_id(notification: &JsonRpcNotification) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("threadId"))
        .and_then(Value::as_str)
}

fn notification_delta(notification: &JsonRpcNotification) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("delta"))
        .and_then(Value::as_str)
}

fn notification_is_turn_error(
    notification: &JsonRpcNotification,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    if notification.method == "error"
        && (notification_matches_thread_turn(notification, thread_id, turn_id)
            || notification_turn_id(notification) == Some(turn_id))
    {
        return true;
    }
    notification.method == "turn/completed"
        && notification_thread_id(notification) == Some(thread_id)
        && notification_turn_id(notification) == Some(turn_id)
        && notification
            .params
            .as_ref()
            .and_then(|params| params.get("turn"))
            .and_then(|turn| turn.get("status"))
            .and_then(Value::as_str)
            .is_some_and(|status| status == "failed" || status == "interrupted")
}

fn notification_turn_error_message(notification: &JsonRpcNotification) -> String {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("turn"))
        .and_then(|turn| turn.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| notification_error_message(notification))
}

fn debug_rpc_usage_error(message: impl Into<String>) -> CooldisError {
    usage_error(format!(
        "{}\nUsage: cooldis debug rpc --help",
        message.into()
    ))
}

async fn run_rpc(args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_rpc_help();
        return Ok(());
    }
    let options = parse_rpc_args(args)?;
    let mut config = CooldisAppServerConfig::local(
        options.listen.clone(),
        options
            .cwd
            .unwrap_or(std::env::current_dir().map_err(|err| {
                usage_error(format!("failed to read current working directory: {err}"))
            })?),
    );
    if let Some(runtime_home) = options.runtime_home {
        config.runtime_home = runtime_home;
    }
    if let Some(state_home) = options.state_home {
        config.state_home = state_home;
    }
    let server = CooldisAppServer::new_local(config).await?;
    eprintln!("cooldis rpc listening on {}", options.listen.display());
    server.serve(options.listen).await
}

async fn run_console(args: Vec<OsString>) -> CooldisResult<()> {
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_console_help();
        return Ok(());
    }
    let options = parse_console_args(args)?;
    if options.help {
        print_console_help();
        return Ok(());
    }

    let listener = TcpListener::bind(options.listen).await.map_err(|err| {
        usage_error(format!(
            "failed to bind Cooldis console listener {}: {err}",
            options.listen
        ))
    })?;
    let bound_addr = listener
        .local_addr()
        .map_err(|err| usage_error(format!("failed to inspect Cooldis console listener: {err}")))?;
    let listen = AppServerListenAddr::WebSocket(bound_addr);
    let assets = resolve_console_asset_root()?;
    let session_token = generate_console_session_token();
    let resolved = resolve_console_app_server_config(&options, listen.clone())?;
    let project_root = resolved.project_root.clone();
    let config_path = resolved.config_path.clone();
    let mut config = resolved.config;
    let state_home = config.state_home.clone();
    config.console_assets = Some(ConsoleAssetConfig {
        root: assets,
        session_token,
    });
    prepare_console_project_storage(&config)?;

    let server = CooldisAppServer::new_local(config).await?;
    let ui_url = format!("http://{bound_addr}/");
    let rpc_url = format!("ws://{bound_addr}/rpc");
    println!("cooldis console UI  {ui_url}");
    println!("cooldis console RPC {rpc_url}");
    println!("cooldis console Project {}", project_root.display());
    if let Some(config_path) = config_path {
        println!("cooldis console Config {}", config_path.display());
    } else {
        println!("cooldis console Config <defaults>");
    }
    println!("cooldis console State {}", state_home.display());
    if options.open {
        if let Err(err) = open_browser_url(&ui_url) {
            eprintln!("cooldis console could not open the browser: {err}");
        }
    }
    server.serve_websocket_listener(listener).await
}

async fn run_daemon(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_daemon_help();
        return Ok(());
    }

    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "run" => daemon_run(args).await,
        "config" => daemon_config(args).await,
        "service" => daemon_service(args).await,
        other => Err(usage_error(format!("unknown daemon subcommand {other:?}"))),
    }
}

async fn daemon_run(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_daemon_run_args(args)?;
    let loaded = load_cooldis_daemon_config(options.config_path.as_deref())?;
    let config = daemon_app_server_config_from_loaded(&loaded)?;
    let listen = config.listen.clone();

    let server = CooldisAppServer::new_local(config).await?;
    let _io_tasks = start_daemon_io(
        &loaded.config.io,
        &loaded.config.sync,
        loaded.path.clone(),
        &server,
    )
    .await?;
    eprintln!(
        "cooldis daemon listening on {}",
        loaded.config.app_server.listen
    );
    if let Some(path) = &loaded.path {
        eprintln!("cooldis daemon config {}", path.display());
    } else {
        eprintln!("cooldis daemon config <defaults>");
    }
    server.serve(listen).await
}

fn daemon_app_server_config_from_loaded(
    loaded: &LoadedCooldisDaemonConfig,
) -> CooldisResult<CooldisAppServerConfig> {
    let listen = loaded.config.app_server.listen_addr()?;
    let cwd = match loaded.config.runtime.cwd.clone() {
        Some(cwd) => cwd,
        None => std::env::current_dir().map_err(|err| {
            usage_error(format!("failed to read current working directory: {err}"))
        })?,
    };
    let mut config = CooldisAppServerConfig::local(listen, cwd);
    if let Some(runtime_home) = loaded.config.runtime.runtime_home.clone() {
        config.runtime_home = runtime_home;
    }
    if let Some(state_home) = loaded.config.runtime.state_home.clone() {
        config.state_home = state_home;
    }
    config.default_placement = loaded.config.runtime.placement.clone().unwrap_or_default();
    config.default_workspace = loaded.config.runtime.workspace.clone();
    if let Some(operations) = loaded.config.registries.operations.clone() {
        let operations = daemon_app_server_registry_root(operations)?;
        // lexicon-allow: capsule - existing app-server config field
        config.capsule_bindings.registry_root = Some(operations);
    }
    // lexicon-allow: capsule - existing app-server config field
    config.capsule_bindings.global_operation_names =
        loaded.config.operations.global_operation_names.clone();
    // lexicon-allow: capsule - existing app-server config field
    config.capsule_bindings.load_all_active_when_unbound =
        loaded.config.operations.load_all_active_when_unbound;
    if let Some(agents) = loaded.config.registries.agents.clone() {
        let agents = daemon_app_server_registry_root(agents)?;
        config.agent_registry_root = agents;
    }

    apply_chat_provider_config(
        &mut config,
        load_daemon_provider_config(&loaded.config.provider)?,
    );

    Ok(config)
}

fn daemon_app_server_registry_root(path: PathBuf) -> CooldisResult<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }

    Ok(std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current working directory: {err}")))?
        .join(path))
}

#[cfg(test)]
fn console_app_server_config(
    options: &ConsoleArgs,
    listen: AppServerListenAddr,
) -> CooldisResult<CooldisAppServerConfig> {
    resolve_console_app_server_config(options, listen).map(|resolved| resolved.config)
}

struct ResolvedConsoleAppServerConfig {
    config: CooldisAppServerConfig,
    project_root: PathBuf,
    config_path: Option<PathBuf>,
}

struct ConsoleEnvironment {
    selected_cwd: PathBuf,
    project_root: PathBuf,
    project_storage_root: PathBuf,
    user_home: PathBuf,
    config_paths: Vec<PathBuf>,
}

fn resolve_console_app_server_config(
    options: &ConsoleArgs,
    listen: AppServerListenAddr,
) -> CooldisResult<ResolvedConsoleAppServerConfig> {
    let env = resolve_console_environment(options)?;
    let loaded = load_cooldis_daemon_config_layers(&env.config_paths, env.project_root.clone())?;
    let mut config = CooldisAppServerConfig::local(listen.clone(), env.selected_cwd.clone());
    config.runtime_home = env.project_storage_root.join("runtime");
    config.state_home = env.project_storage_root.join("state");
    config.user_state_home = env.user_home.join("state");
    config.agent_registry_root = env.project_storage_root.join("agents");
    config.capsule_bindings.registry_root = Some(env.project_storage_root.join("operations"));

    if let Some(runtime_home) = loaded.config.runtime.runtime_home.clone() {
        config.runtime_home = runtime_home;
    }
    if let Some(state_home) = loaded.config.runtime.state_home.clone() {
        config.state_home = state_home;
    }
    config.default_placement = loaded.config.runtime.placement.clone().unwrap_or_default();
    config.default_workspace = loaded.config.runtime.workspace.clone();
    if options.cwd_explicit {
        config.cwd = env.selected_cwd;
    } else if let Some(cwd) = loaded.config.runtime.cwd.clone() {
        config.cwd = cwd;
    }
    if let Some(operations) = loaded.config.registries.operations.clone() {
        config.capsule_bindings.registry_root = Some(daemon_app_server_registry_root(operations)?);
    }
    if let Some(agents) = loaded.config.registries.agents.clone() {
        config.agent_registry_root = daemon_app_server_registry_root(agents)?;
    }
    config.capsule_bindings.global_operation_names =
        loaded.config.operations.global_operation_names.clone();
    config.capsule_bindings.load_all_active_when_unbound =
        loaded.config.operations.load_all_active_when_unbound;
    apply_chat_provider_config(
        &mut config,
        load_daemon_provider_config(&loaded.config.provider)?,
    );
    config.listen = listen;

    Ok(ResolvedConsoleAppServerConfig {
        config,
        project_root: env.project_root,
        config_path: loaded.path,
    })
}

fn resolve_console_environment(options: &ConsoleArgs) -> CooldisResult<ConsoleEnvironment> {
    let selected_cwd = absolute_path(&options.cwd)?;
    let project = discover_cooldis_project(&selected_cwd)?;
    let user_home = default_user_cooldis_home()?;
    let project_storage_root = console_project_storage_root(&project.root, &user_home);
    let mut config_paths = Vec::new();
    let user_config = user_home.join("config.toml");
    if user_config.is_file() {
        config_paths.push(user_config);
    }
    if let Some(project_config) = project.config_path {
        push_unique_path(&mut config_paths, project_config);
    }
    if let Some(config_path) = options.config_path.as_deref() {
        push_unique_path(&mut config_paths, absolute_path(config_path)?);
    }

    Ok(ConsoleEnvironment {
        selected_cwd,
        project_root: project.root,
        project_storage_root,
        user_home,
        config_paths,
    })
}

fn console_project_storage_root(project_root: &Path, user_home: &Path) -> PathBuf {
    let default_storage_root = project_root.join(".cooldis");
    if default_storage_root == user_home {
        return user_home.join("projects/home");
    }
    default_storage_root
}

fn prepare_console_project_storage(config: &CooldisAppServerConfig) -> CooldisResult<()> {
    let mut roots = vec![
        config.runtime_home.as_path(),
        config.state_home.as_path(),
        config.user_state_home.as_path(),
        config.agent_registry_root.as_path(),
    ];
    if let Some(registry_root) = config.capsule_bindings.registry_root.as_deref() {
        roots.push(registry_root);
    }
    for root in roots {
        fs::create_dir_all(root).map_err(|err| {
            io_error(format!(
                "failed to prepare Cooldis console directory {}: {err}",
                root.display()
            ))
        })?;
    }
    Ok(())
}

fn default_user_cooldis_home() -> CooldisResult<PathBuf> {
    if let Some(home) = std::env::var_os("COOLDIS_HOME").map(PathBuf::from) {
        return Ok(home);
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".cooldis"))
        .ok_or_else(|| usage_error("HOME is not set and COOLDIS_HOME was not provided"))
}

fn absolute_path(path: &Path) -> CooldisResult<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current working directory: {err}")))?
        .join(path))
}

fn push_unique_path(paths: &mut Vec<PathBuf>, path: PathBuf) {
    if !paths.iter().any(|existing| existing == &path) {
        paths.push(path);
    }
}

fn generate_console_session_token() -> String {
    format!("{}{}", Uuid::now_v7().simple(), Uuid::now_v7().simple())
}

fn resolve_console_asset_root() -> CooldisResult<PathBuf> {
    if let Some(path) = std::env::var_os("COOLDIS_CONSOLE_ASSET_DIR").map(PathBuf::from) {
        return console_asset_root_if_valid(path).ok_or_else(|| {
            usage_error(
                "COOLDIS_CONSOLE_ASSET_DIR must point at a built console directory containing index.html",
            )
        });
    }

    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        candidates.push(exe_asset_candidate(&exe));
        candidates.push(
            exe.parent()
                .unwrap_or(Path::new("."))
                .join("../share/cooldis/console"),
        );
        if let Ok(link) = std::fs::read_link(&exe) {
            let target = if link.is_absolute() {
                link
            } else {
                exe.parent().unwrap_or(Path::new(".")).join(link)
            };
            candidates.push(exe_asset_candidate(&target));
        }
    }
    candidates.push(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../apps/console/dist"));

    candidates
        .into_iter()
        .find_map(console_asset_root_if_valid)
        .ok_or_else(|| {
            usage_error(
                "Cooldis console assets were not found; run `scripts/build-console-assets.sh` or set COOLDIS_CONSOLE_ASSET_DIR",
            )
        })
}

fn exe_asset_candidate(exe: &Path) -> PathBuf {
    exe.parent()
        .unwrap_or(Path::new("."))
        .join("share/cooldis/console")
}

fn console_asset_root_if_valid(path: PathBuf) -> Option<PathBuf> {
    path.join("index.html").is_file().then_some(path)
}

fn open_browser_url(url: &str) -> CooldisResult<()> {
    browser_open_command(url)?
        .spawn()
        .map(|_| ())
        .map_err(|err| usage_error(format!("failed to open browser: {err}")))
}

#[cfg(target_os = "macos")]
fn browser_open_command(url: &str) -> CooldisResult<std::process::Command> {
    let mut command = std::process::Command::new("open");
    command.arg(url);
    Ok(command)
}

#[cfg(target_os = "linux")]
fn browser_open_command(url: &str) -> CooldisResult<std::process::Command> {
    let mut command = std::process::Command::new("xdg-open");
    command.arg(url);
    Ok(command)
}

#[cfg(target_os = "windows")]
fn browser_open_command(url: &str) -> CooldisResult<std::process::Command> {
    let mut command = std::process::Command::new("cmd");
    command.args(["/C", "start", "", url]);
    Ok(command)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
fn browser_open_command(_url: &str) -> CooldisResult<std::process::Command> {
    Err(usage_error(
        "automatic browser open is not supported on this platform",
    ))
}

async fn start_daemon_io(
    io: &CooldisIoConfig,
    sync: &CooldisDaemonSyncConfig,
    daemon_config_path: Option<PathBuf>,
    server: &CooldisAppServer,
) -> CooldisResult<Vec<JoinHandle<()>>> {
    let bridge = CooldisDaemonIoBridge::from_app_server(server);
    let mut tasks = Vec::new();
    // App-server construction already completed the startup recovery fold;
    // install settlement workers before external route listeners.
    start_thread_handle_ingress(&io.ingress, server, &bridge, &mut tasks).await?;
    let enabled_routes = io.routes.iter().filter(|route| route.enabled);
    for route in enabled_routes {
        bridge.validate_route_agent_ref(route).await?;
        match route.kind.as_str() {
            "clock.tick" => {
                let ingress = route.ingress.as_ref().unwrap_or(&io.ingress);
                let sink = route_sink_for_ingress(route, ingress, &bridge, &mut tasks).await?;
                start_clock_route(route, sink, server, &mut tasks).await?;
            }
            "telegram.bot" => {
                let ingress = route.ingress.as_ref().unwrap_or(&io.ingress);
                let egress_state_dsn = ingress.effective_queue_dsn();
                bridge
                    .register_egress_route_config(TELEGRAM_PROTOCOL, route.id.clone(), route)
                    .await?;
                let sink = route_sink_for_ingress(route, ingress, &bridge, &mut tasks).await?;
                start_telegram_route(route, sink, &bridge, egress_state_dsn, &mut tasks).await?;
            }
            other => {
                eprintln!(
                    "cooldis daemon IO route {} ({other}) has no listener in this daemon slice",
                    route.id
                );
            }
        }
    }
    start_daemon_sync(sync, daemon_config_path, server, &mut tasks).await?;

    if !io.routes.is_empty() {
        eprintln!(
            "cooldis daemon loaded {} IO route(s), {} task(s) active",
            io.routes.len(),
            tasks.len()
        );
    }
    Ok(tasks)
}

async fn start_daemon_sync(
    config: &CooldisDaemonSyncConfig,
    daemon_config_path: Option<PathBuf>,
    app_server: &CooldisAppServer,
    tasks: &mut Vec<JoinHandle<()>>,
) -> CooldisResult<()> {
    let Some(listen) = config.listen_addr()? else {
        return Ok(());
    };
    let store = SqliteSessionStore::open(app_server.session_store_path())
        .await
        .map_err(|error| CooldisError::History(error.to_string()))?;
    let clock: Arc<dyn crate::DaemonClock> = Arc::new(SystemDaemonClock);
    let authority = Arc::new(
        SqliteStreamLeaseAuthority::new(store.clone(), config.clone(), Arc::clone(&clock)).await?,
    );
    let endpoint =
        Arc::new(SqliteSyncEndpoint::new(store.clone(), Arc::clone(&authority), clock).await?);
    let server = DaemonSyncHttpServer::bind(listen, endpoint).await?;
    let sync_endpoint = server.display_addr()?;
    let child_root = app_server
        .session_store_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("remote-children");
    let executor = Arc::new(
        ProcessRemoteThreadExecutor::new(
            store,
            authority,
            sync_endpoint.clone(),
            daemon_config_path,
            child_root,
            std::env::current_exe().map_err(|error| {
                CooldisError::RuntimeFactory(format!(
                    "failed to locate executable for remote placement: {error}"
                ))
            })?,
        )
        .await?,
    );
    app_server
        .supervisor()
        .set_remote_thread_executor(app_server.tenant_id(), Some(executor))
        .await?;
    app_server.mark_remote_event_store_served();
    eprintln!(
        "cooldis daemon sync endpoint listening on {}",
        sync_endpoint
    );
    tasks.push(tokio::spawn(async move {
        if let Err(error) = server.serve().await {
            eprintln!("cooldis daemon sync endpoint stopped: {error}");
        }
    }));
    Ok(())
}

async fn remote_child_run() -> CooldisResult<()> {
    let mut encoded = Vec::new();
    std::io::stdin()
        .read_to_end(&mut encoded)
        .map_err(|error| {
            CooldisError::RuntimeExecution(format!(
                "failed to read remote child bootstrap: {error}"
            ))
        })?;
    let bootstrap =
        serde_json::from_slice::<RemoteChildBootstrapV1>(&encoded).map_err(|error| {
            CooldisError::RuntimeExecution(format!(
                "failed to decode remote child bootstrap: {error}"
            ))
        })?;
    let loaded = load_cooldis_daemon_config(bootstrap.daemon_config_path.as_deref())?;
    let config = daemon_app_server_config_from_loaded(&loaded)?;
    run_remote_child(config, bootstrap).await
}

/// Starts the push-first settlement lane independently of external route
/// policy. Handle outcomes always require the durable queue even when an
/// operator has explicitly made a protocol route best-effort direct.
async fn start_thread_handle_ingress(
    ingress: &CooldisIngressConfig,
    server: &CooldisAppServer,
    bridge: &CooldisDaemonIoBridge,
    tasks: &mut Vec<JoinHandle<()>>,
) -> CooldisResult<()> {
    let queue_name = ingress
        .persistence
        .queue_name
        .clone()
        .unwrap_or_else(|| "cooldis-ingress".to_string());
    let queue_config = PgqrsQueueConfig::new(ingress.effective_queue_dsn(), queue_name)
        .with_default_visibility_timeout_secs(ingress.persistence.visibility_timeout_secs);
    let queue = Arc::new(
        PgqrsIngressQueue::connect(queue_config)
            .await
            .map_err(io_error)?,
    );
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    let worker = CooldisDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "thread-handle-outcome-worker",
        ingress.persistence.visibility_timeout_secs,
    );
    tasks.push(tokio::spawn(worker.run()));
    tasks.push(tokio::spawn(
        ThreadHandleIngressAdapter::new(store, queue, server.tenant_id(), server.user_id()).run(),
    ));
    Ok(())
}

async fn route_sink_for_ingress(
    route: &CooldisIoRouteConfig,
    ingress: &CooldisIngressConfig,
    bridge: &CooldisDaemonIoBridge,
    tasks: &mut Vec<JoinHandle<()>>,
) -> CooldisResult<Arc<dyn IngressSink>> {
    let inner: Arc<dyn IngressSink> = match ingress.persistence.mode {
        IngressPersistenceMode::BestEffortDirect => bridge.direct_sink(),
        IngressPersistenceMode::DurableQueue => {
            let queue_config = PgqrsQueueConfig::from_persistence_config(
                ingress.effective_queue_dsn(),
                &ingress.persistence,
            )
            .map_err(io_error)?
            .ok_or_else(|| usage_error("durable queue persistence did not return a queue"))?;
            let queue = Arc::new(
                PgqrsIngressQueue::connect(queue_config)
                    .await
                    .map_err(io_error)?,
            );
            let worker = CooldisDaemonQueueWorker::new(
                queue.clone(),
                bridge.clone(),
                format!("{}-worker", route.id),
                ingress.persistence.visibility_timeout_secs,
            );
            tasks.push(tokio::spawn(worker.run()));
            queue
        }
    };
    Ok(Arc::new(RouteIngressSink::new(inner, route)))
}

async fn start_clock_route(
    route: &CooldisIoRouteConfig,
    sink: Arc<dyn IngressSink>,
    server: &CooldisAppServer,
    tasks: &mut Vec<JoinHandle<()>>,
) -> CooldisResult<()> {
    let store = SqliteSessionStore::open(server.session_store_path())
        .await
        .map_err(|err| CooldisError::History(err.to_string()))?;
    let clock =
        CooldisDaemonClockRoute::new(route.id.clone(), store, sink, Arc::new(SystemDaemonClock));
    eprintln!(
        "cooldis clock route {} polling active mandates every 30s",
        route.id
    );
    tasks.push(tokio::spawn(clock.run()));
    Ok(())
}

async fn start_telegram_route(
    route: &CooldisIoRouteConfig,
    sink: Arc<dyn IngressSink>,
    bridge: &CooldisDaemonIoBridge,
    egress_state_dsn: String,
    tasks: &mut Vec<JoinHandle<()>>,
) -> CooldisResult<()> {
    let telegram = route.telegram.as_ref().ok_or_else(|| {
        usage_error(format!(
            "telegram route {} requires [daemon.io.routes.telegram]",
            route.id
        ))
    })?;
    if let Some(bot_token) = telegram.bot_token_value()? {
        let client = match &telegram.api_base {
            Some(api_base) => TelegramBotClient::new(bot_token).with_api_base(api_base.clone()),
            None => TelegramBotClient::new(bot_token),
        };
        bridge
            .register_egress_adapter(
                TELEGRAM_PROTOCOL,
                route.id.clone(),
                Arc::new(TelegramEgressAdapter::with_client(route.id.clone(), client)),
            )
            .await;
    }
    let projector = bridge
        .start_egress_projector_sqlite_dsn(TELEGRAM_PROTOCOL, route.id.clone(), egress_state_dsn)
        .await
        .map_err(io_error)?;
    tasks.push(projector);
    let listen = telegram.listen.clone().ok_or_else(|| {
        usage_error(format!(
            "telegram route {} requires telegram.listen",
            route.id
        ))
    })?;
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: route.id.clone(),
            listen,
            path: telegram.path.clone(),
            secret_token: telegram.secret_token_value()?,
        },
        sink,
    )
    .await?;
    let addr = server.local_addr()?;
    eprintln!(
        "cooldis Telegram route {} listening on http://{}{}",
        route.id, addr, telegram.path
    );
    tasks.push(tokio::spawn(async move {
        if let Err(err) = server.serve().await {
            eprintln!("cooldis Telegram webhook server stopped: {err}");
        }
    }));
    Ok(())
}

async fn daemon_config(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty() {
        return Err(usage_error("daemon config requires a subcommand"));
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "validate" => {
            let options = parse_daemon_config_validate_args(args)?;
            let loaded = load_cooldis_daemon_config(options.config_path.as_deref())?;
            println!("cooldis daemon config ok");
            println!(
                "config {}",
                loaded
                    .path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<defaults>".to_string())
            );
            println!("app_server.listen {}", loaded.config.app_server.listen);
            println!(
                "io.ingress.persistence {}",
                ingress_persistence_mode_name(loaded.config.io.ingress.persistence.mode)
            );
            println!("io.routes {}", loaded.config.io.routes.len());
            Ok(())
        }
        other => Err(usage_error(format!(
            "unknown daemon config subcommand {other:?}"
        ))),
    }
}

async fn daemon_service(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty() {
        return Err(usage_error("daemon service requires a subcommand"));
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "print" => {
            let options = parse_daemon_service_print_args(args)?;
            let spec = daemon_service_spec_from_args(&options)?;
            print!("{}", render_cooldis_daemon_service(options.target, &spec));
            Ok(())
        }
        "install" => {
            let options = parse_daemon_service_print_args(args)?;
            let spec = daemon_service_spec_from_args(&options)?;
            let path = install_cooldis_daemon_service(options.target, &spec)?;
            println!("installed {}", path.display());
            println!("service was not started automatically");
            match options.target {
                CooldisDaemonServiceTarget::Launchd => {
                    println!("start with: launchctl load {}", path.display());
                }
                CooldisDaemonServiceTarget::Systemd => {
                    println!("start with: systemctl --user enable --now {}", spec.label);
                }
            }
            Ok(())
        }
        "uninstall" => {
            let options = parse_daemon_service_uninstall_args(args)?;
            match uninstall_cooldis_daemon_service(options.target, &options.label)? {
                Some(path) => println!("removed {}", path.display()),
                None => println!("service not installed for label {}", options.label),
            }
            Ok(())
        }
        other => Err(usage_error(format!(
            "unknown daemon service subcommand {other:?}"
        ))),
    }
}

fn daemon_service_spec_from_args(
    options: &DaemonServicePrintArgs,
) -> CooldisResult<CooldisDaemonServiceSpec> {
    load_cooldis_daemon_config(Some(&options.config_path))?;
    let mut spec =
        CooldisDaemonServiceSpec::new(options.executable.clone(), options.config_path.clone())
            .with_label(options.label.clone());
    if let Some(working_directory) = &options.working_directory {
        spec = spec.with_working_directory(working_directory.clone());
    }
    Ok(spec)
}

async fn run_import(mut args: Vec<OsString>) -> CooldisResult<()> {
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

async fn run_tool(mut args: Vec<OsString>) -> CooldisResult<()> {
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
            other => return Err(usage_error(format!("unknown tool subcommand {other:?}"))),
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
        _ => Err(usage_error(format!(
            "unknown tool subcommand {subcommand:?}"
        ))),
    }
}

async fn coupling_run(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_coupling_run_args(args)?;
    if options.help {
        print_coupling_run_help();
        return Ok(());
    }
    if !options.replay {
        return Err(usage_error("coupling run currently requires --replay"));
    }
    let artifact = options
        .artifact
        .as_deref()
        .ok_or_else(|| usage_error("coupling run --replay requires --artifact <path|op://ref>"))?;
    let mut coupling_set = load_replay_coupling_set(&options)?;
    if let Some(coupling_id) = &options.coupling_id {
        coupling_set = select_replay_coupling(&coupling_set, coupling_id)?;
    }
    let operation_registry_root =
        resolve_replay_artifact(artifact, options.registry_root.clone(), &mut coupling_set).await?;
    let events = load_replay_recorded_events(&options).await?;
    let replayed_event_count = events.len();
    let receipt = replay_coupling_events(&coupling_set, events, operation_registry_root).await?;
    let report = CouplingReplayReport::from_receipt(replayed_event_count, &coupling_set, receipt);
    if options.json {
        serde_json::to_writer_pretty(std::io::stdout(), &report)
            .map_err(|err| usage_error(format!("failed to encode coupling replay JSON: {err}")))?;
        println!();
    } else {
        print_coupling_replay_report(&report);
    }
    Ok(())
}

fn load_replay_coupling_set(options: &CouplingRunArgs) -> CooldisResult<BoundCouplingSet> {
    if let Some(path) = &options.coupling_file {
        return load_replay_coupling_set_file(path);
    }
    if let Some(path) = &options.export_bundle {
        let value = read_json_file(path)?;
        if let Some(coupling_set) = coupling_set_from_export_bundle(&value)? {
            return Ok(coupling_set);
        }
    }
    Err(usage_error(
        "coupling run --replay requires --coupling-file unless the export bundle contains a bound coupling set",
    ))
}

fn load_replay_coupling_set_file(path: &Path) -> CooldisResult<BoundCouplingSet> {
    let raw = fs::read_to_string(path)
        .map_err(|err| usage_error(format!("failed to read {}: {err}", path.display())))?;
    if let Ok(set) = serde_json::from_str::<BoundCouplingSet>(&raw) {
        return Ok(set);
    }
    if let Ok(coupling) = serde_json::from_str::<BoundCoupling>(&raw) {
        return Ok(BoundCouplingSet::new("replay-file", vec![coupling]));
    }
    if let Ok(set) = toml::from_str::<BoundCouplingSet>(&raw) {
        return Ok(set);
    }
    if let Ok(coupling) = toml::from_str::<BoundCoupling>(&raw) {
        return Ok(BoundCouplingSet::new("replay-file", vec![coupling]));
    }
    Err(usage_error(format!(
        "coupling file {} must be a serialized BoundCouplingSet or BoundCoupling",
        path.display()
    )))
}

fn coupling_set_from_export_bundle(value: &Value) -> CooldisResult<Option<BoundCouplingSet>> {
    for pointer in ["/boundCouplingSet", "/couplingSet"] {
        if let Some(candidate) = value.pointer(pointer)
            && !candidate.is_null()
        {
            return serde_json::from_value::<BoundCouplingSet>(candidate.clone())
                .map(Some)
                .map_err(|err| usage_error(format!("export bundle {pointer} is invalid: {err}")));
        }
    }
    if let Some(raw) = value
        .pointer("/thread/metadata/cooldis.agent.bound_coupling_set")
        .and_then(Value::as_str)
    {
        return serde_json::from_str::<BoundCouplingSet>(raw)
            .map(Some)
            .map_err(|err| {
                usage_error(format!(
                    "export bundle bound coupling metadata is invalid: {err}"
                ))
            });
    }
    Ok(None)
}

fn select_replay_coupling(
    coupling_set: &BoundCouplingSet,
    coupling_id: &str,
) -> CooldisResult<BoundCouplingSet> {
    let couplings = coupling_set
        .couplings
        .iter()
        .filter(|coupling| coupling.id == coupling_id)
        .cloned()
        .collect::<Vec<_>>();
    if couplings.is_empty() {
        return Err(usage_error(format!(
            "bound coupling id {coupling_id:?} was not found in replay coupling set"
        )));
    }
    Ok(BoundCouplingSet::new(
        coupling_set.snapshot_id.clone(),
        couplings,
    ))
}

async fn resolve_replay_artifact(
    artifact: &str,
    registry_root: Option<PathBuf>,
    coupling_set: &mut BoundCouplingSet,
) -> CooldisResult<PathBuf> {
    if artifact.starts_with("op://") {
        let parsed = parse_pinned_operation_ref(artifact)?;
        let root = registry_root.unwrap_or_else(default_registry_root);
        let record = LocalOperationRegistry::new(&root)
            .load_version_record(&parsed.name, &parsed.artifact_hash)
            .map_err(|err| {
                usage_error(format!(
                    "replay artifact {artifact:?} was not found in operation registry {}: {err}",
                    root.display()
                ))
            })?;
        apply_replay_operation_record(coupling_set, &record, parsed.operation.as_deref())?;
        return Ok(root);
    }

    let artifact_path = PathBuf::from(artifact);
    let root = registry_root.unwrap_or_else(|| {
        std::env::temp_dir()
            .join(format!("cooldis-coupling-replay-{}", Uuid::now_v7()))
            .join("operations")
    });
    let record = LocalOperationRegistry::new(&root)
        .publish_artifact(PublishOperationRequest {
            name: "replay-coupling".to_string(),
            artifact_path: artifact_path.clone(),
            source: PublishedOperationSource::Wasm {
                bin_path: artifact_path,
            },
            interface: None,
            capability_grants: BTreeSet::new(),
            metadata: BTreeMap::from([("coupling.replay.local_artifact".to_string(), json!(true))]),
        })
        .await?;
    apply_replay_operation_record(coupling_set, &record, None)?;
    Ok(root)
}

fn apply_replay_operation_record(
    coupling_set: &mut BoundCouplingSet,
    record: &PublishedOperationRecord,
    selected_operation: Option<&str>,
) -> CooldisResult<()> {
    for coupling in &mut coupling_set.couplings {
        let operation_name = select_replay_operation_name(
            &coupling.id,
            selected_operation.or(coupling.function.operation_name.as_deref()),
            &record.manifest,
        )?;
        validate_replay_coupling_operation(&coupling.id, &operation_name, &record.manifest)?;
        coupling.function_ref = format!(
            "op://{}/{operation_name}@sha256:{}",
            record.name, record.active_artifact_hash
        );
        coupling.function.name = record.name.clone();
        coupling.function.artifact_hash = record.active_artifact_hash.clone();
        coupling.function.operation_name = Some(operation_name);
    }
    Ok(())
}

fn select_replay_operation_name(
    coupling_id: &str,
    selected_operation: Option<&str>,
    manifest: &WasmOperationManifest,
) -> CooldisResult<String> {
    if let Some(operation_name) = selected_operation {
        if manifest.operation(operation_name).is_some() {
            return Ok(operation_name.to_string());
        }
        return Err(usage_error(format!(
            "replay artifact does not expose operation {operation_name:?} for coupling {coupling_id:?}"
        )));
    }
    if manifest.operations.len() == 1 {
        return Ok(manifest.operations[0].name.clone());
    }
    Err(usage_error(format!(
        "replay artifact for coupling {coupling_id:?} exposes multiple operations; use op://<record>/<operation>@sha256:<hash> or a bound coupling operation_name"
    )))
}

fn validate_replay_coupling_operation(
    coupling_id: &str,
    operation_name: &str,
    manifest: &WasmOperationManifest,
) -> CooldisResult<()> {
    let operation = manifest.operation(operation_name).ok_or_else(|| {
        usage_error(format!(
            "replay artifact does not expose operation {operation_name:?} for coupling {coupling_id:?}"
        ))
    })?;
    if operation.input != WasmOperationValueKind::Json {
        return Err(usage_error(format!(
            "coupling {coupling_id:?} operation {operation_name:?} must declare json input for {COUPLING_INVOCATION_ABI}"
        )));
    }
    if operation.output != WasmOperationValueKind::Json {
        return Err(usage_error(format!(
            "coupling {coupling_id:?} operation {operation_name:?} must declare json output for {COUPLING_DISCHARGE_ABI}"
        )));
    }
    if !operation.required_capabilities.is_empty() {
        return Err(usage_error(format!(
            "coupling {coupling_id:?} operation {operation_name:?} declares effect capabilities; replay couplings must be pure compute"
        )));
    }
    Ok(())
}

async fn load_replay_recorded_events(options: &CouplingRunArgs) -> CooldisResult<Vec<EventRecord>> {
    match (&options.journal, &options.thread_id, &options.export_bundle) {
        (Some(_), _, Some(_)) => Err(usage_error(
            "coupling run --replay accepts either --journal/--thread-id or --export, not both",
        )),
        (Some(journal), Some(thread_id), None) => {
            let store = SqliteSessionStore::open_read_only(journal)
                .await
                .map_err(|err| {
                    let message = err.to_string();
                    if turso_cross_process_lock_error(&message) {
                        cross_process_database_guidance("stop the daemon and retry")
                    } else {
                        usage_error(format!("failed to open journal read-only: {err}"))
                    }
                })?;
            let events = store.list_thread_events(*thread_id).await.map_err(|err| {
                usage_error(format!(
                    "failed to read recorded events for thread {thread_id}: {err}"
                ))
            })?;
            if events.is_empty() {
                return Err(usage_error(format!(
                    "journal {} has no events for thread {thread_id}",
                    journal.display()
                )));
            }
            Ok(events)
        }
        (Some(_), None, None) => Err(usage_error(
            "coupling run --replay with --journal requires --thread-id",
        )),
        (None, _, Some(export)) => load_replay_events_from_export(export),
        (None, _, None) => Err(usage_error(
            "coupling run --replay requires --journal/--thread-id or --export",
        )),
    }
}

fn load_replay_events_from_export(path: &Path) -> CooldisResult<Vec<EventRecord>> {
    let value = read_json_file(path)?;
    let streams = value
        .get("streams")
        .and_then(Value::as_array)
        .ok_or_else(|| usage_error("export bundle missing streams array"))?;
    let mut events = Vec::new();
    for stream in streams {
        let data = stream
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| usage_error("export bundle stream missing data array"))?;
        for event in data {
            events.push(event_record_from_export_value(event.clone())?);
        }
    }
    events.sort_by(|left, right| {
        (
            left.created_at_ms,
            left.stream_id.to_string(),
            left.sequence.get(),
            left.id.to_string(),
        )
            .cmp(&(
                right.created_at_ms,
                right.stream_id.to_string(),
                right.sequence.get(),
                right.id.to_string(),
            ))
    });
    if events.is_empty() {
        return Err(usage_error("export bundle contains no replayable events"));
    }
    Ok(events)
}

fn event_record_from_export_value(value: Value) -> CooldisResult<EventRecord> {
    let envelope = serde_json::from_value::<StreamRecordEnvelopeV1>(value)
        .map_err(|err| usage_error(format!("export stream record is invalid: {err}")))?;
    let kind = envelope
        .kind
        .parse::<EventKind>()
        .map_err(|err| usage_error(format!("export stream record kind is invalid: {err}")))?;
    Ok(EventRecord {
        id: envelope.event_id,
        stream_id: envelope.stream_id,
        sequence: envelope.sequence,
        coordinates: envelope.coordinates,
        created_at_ms: envelope.created_at_ms,
        kind,
        origin: envelope.origin,
        provenance: envelope.provenance,
        payload: envelope.payload,
    })
}

async fn replay_coupling_events(
    coupling_set: &BoundCouplingSet,
    recorded_events: Vec<EventRecord>,
    operation_registry_root: PathBuf,
) -> CooldisResult<CouplingSchedulerCycleReceipt> {
    let replay_store = CouplingReplayEventStore::default();
    let executor = crate::kernel::coupling_executor_registry::CouplingExecutorRegistry::new(Some(
        operation_registry_root,
    ));
    let scheduler = CouplingScheduler::new(&replay_store, &executor);
    let mut aggregate = CouplingSchedulerCycleReceipt {
        snapshot_id: coupling_set.snapshot_id.clone(),
        runs: Vec::new(),
        appended_events: Vec::new(),
    };
    for event in recorded_events {
        let appended = replay_store
            .append_recorded_event(event)
            .map_err(|err| CooldisError::History(err.to_string()))?;
        if appended.is_empty() {
            continue;
        }
        let receipt = scheduler.run_batch(coupling_set, appended).await?;
        aggregate.runs.extend(receipt.runs);
        aggregate.appended_events.extend(receipt.appended_events);
    }
    Ok(aggregate)
}

fn read_json_file(path: &Path) -> CooldisResult<Value> {
    let raw = fs::read_to_string(path)
        .map_err(|err| usage_error(format!("failed to read {}: {err}", path.display())))?;
    serde_json::from_str(&raw)
        .map_err(|err| usage_error(format!("failed to parse {} as JSON: {err}", path.display())))
}

fn parse_pinned_operation_ref(reference: &str) -> CooldisResult<ParsedOperationRef> {
    let body = reference
        .strip_prefix("op://")
        .ok_or_else(|| usage_error("operation ref must start with op://"))?;
    let (name_part, artifact_hash) = body.split_once("@sha256:").ok_or_else(|| {
        usage_error("operation ref must be pinned as op://<record>/<operation>@sha256:<hash>")
    })?;
    if artifact_hash.len() != 64 || !artifact_hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(usage_error(
            "operation ref has an invalid sha256 artifact hash",
        ));
    }
    let segments = name_part.split('/').collect::<Vec<_>>();
    let (name, operation) = match segments.as_slice() {
        [name] if !name.is_empty() => ((*name).to_string(), None),
        [name, operation] if !name.is_empty() && !operation.is_empty() => {
            ((*name).to_string(), Some((*operation).to_string()))
        }
        _ => {
            return Err(usage_error(
                "operation ref must match op://<record>@sha256:<hash> or op://<record>/<operation>@sha256:<hash>",
            ));
        }
    };
    Ok(ParsedOperationRef {
        name,
        operation,
        artifact_hash: artifact_hash.to_string(),
    })
}

fn print_coupling_replay_report(report: &CouplingReplayReport) {
    println!("DRY RUN ONLY: proposed coupling discharges; no stream was appended.");
    println!("mode {}", report.mode);
    println!("snapshot {}", report.snapshot_id);
    println!("replayed_events {}", report.replayed_event_count);
    println!("runs {}", report.runs.len());
    for run in &report.runs {
        if run.blocked {
            println!(
                "run {} BLOCKED {} trigger={} sequence={}",
                run.coupling_id,
                run.reason.as_deref().unwrap_or("blocked"),
                run.trigger_event_id,
                run.trigger_sequence
            );
        } else {
            println!(
                "run {} {} trigger={} sequence={} proposals={}",
                run.coupling_id,
                run.status,
                run.trigger_event_id,
                run.trigger_sequence,
                run.proposal_event_count
            );
        }
    }
    println!("proposal_events {}", report.proposal_events.len());
    for event in &report.proposal_events {
        println!(
            "proposal stream={} kind={} payload={}",
            event.stream,
            event.kind,
            compact_json(&event.payload)
        );
    }
}

async fn run_skill(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_skill_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "publish" => print_skill_publish_help(),
            "import" => print_skill_import_help(),
            other => return Err(usage_error(format!("unknown skill subcommand {other:?}"))),
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "publish" => skill_publish(args).await,
        "import" => skill_import(args).await,
        _ => Err(usage_error(format!(
            "unknown skill subcommand {subcommand:?}"
        ))),
    }
}

async fn run_blob(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_blob_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "publish" => print_blob_publish_help(),
            other => return Err(usage_error(format!("unknown blob subcommand {other:?}"))),
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "publish" => blob_publish(args).await,
        _ => Err(usage_error(format!(
            "unknown blob subcommand {subcommand:?}"
        ))),
    }
}

async fn run_secret(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_secret_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        match subcommand.to_string_lossy().as_ref() {
            "import" => print_secret_import_help(),
            "set" => print_secret_set_help(),
            "list" => print_secret_list_help(),
            "status" => print_secret_status_help(),
            "delete" => print_secret_delete_help(),
            other => return Err(usage_error(format!("unknown secret subcommand {other:?}"))),
        }
        return Ok(());
    }
    match subcommand.to_string_lossy().as_ref() {
        "import" => secret_import(args).await,
        "set" => secret_set(args).await,
        "list" => secret_list(args).await,
        "status" => secret_status(args).await,
        "delete" => secret_delete(args).await,
        _ => Err(usage_error(format!(
            "unknown secret subcommand {subcommand:?}"
        ))),
    }
}

async fn run_auth(mut args: Vec<OsString>) -> CooldisResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_auth_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "status" => auth_status(args).await,
        "set" => auth_set(args).await,
        "delete" => auth_delete(args).await,
        other => Err(usage_error(format!(
            "unknown auth subcommand {other:?}; use `cooldis auth --help`"
        ))),
    }
}

async fn import_build(args: Vec<OsString>) -> CooldisResult<()> {
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

async fn import_publish(args: Vec<OsString>) -> CooldisResult<()> {
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

async fn tool_build(args: Vec<OsString>) -> CooldisResult<()> {
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
        .ok_or_else(|| usage_error("tool build requires --module-path or config module_path"))?;
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
        return Err(CooldisError::RuntimeFactory(
            "strict stateless conversion rejected".to_string(),
        ));
    }
    println!("policy accepted");

    let build =
        build_rust_wasm_module(RustWasmBuildOptions::new(module_path).with_release(release))?;
    let manifest = validate_wasm_artifact(build.artifact_path.clone(), BTreeSet::new()).await?;
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

async fn tool_list(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_registry_args(args, "tool list")?;
    if options.help {
        print_tool_list_help();
        return Ok(());
    }
    let registry =
        LocalOperationRegistry::new(options.registry_root.unwrap_or_else(default_registry_root));
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

async fn tool_publish(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_publish_args(args)?;
    if let Some(package_path) = options.package_path.clone() {
        reject_package_publish_overrides(&options)?;
        let build = build_tool_package(&package_path).await?;
        print_tool_package_build(&build);
        let registry_root = options.registry_root.unwrap_or_else(default_registry_root);
        let registry = LocalOperationRegistry::new(registry_root);
        let record = registry
            .publish_artifact(PublishOperationRequest {
                name: build.package.manifest.identity.name.clone(),
                artifact_path: build.artifact_path.clone(),
                source: build.source.clone(),
                interface: Some(build.interface.clone()),
                capability_grants: build.interface.capability_requests(),
                metadata: BTreeMap::new(),
            })
            .await?;

        println!("published {}", record.name);
        println!("artifact {}", record.active_artifact_hash);
        println!("record {}", registry.record_path(&record.name)?.display());
        for operation in record.manifest.operations {
            println!("operation {}", operation.name);
        }
        return Ok(());
    }
    Err(usage_error(
        "tool publish requires a package proof gate; author cooldis.tool.toml and publish with `cooldis tool publish --package <cooldis.tool.toml>`",
    ))
}

async fn skill_publish(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_skill_publish_args(args)?;
    if options.help {
        print_skill_publish_help();
        return Ok(());
    }
    let package_dir = options
        .package_dir
        .ok_or_else(|| usage_error("skill publish requires <dir>"))?;
    let registry_root = skill_registry_root(options.registry_root);
    let registry = LocalSkillRegistry::new(registry_root);
    let record = registry.publish_directory(PublishSkillPackageRequest {
        package_dir,
        name: options.name,
    })?;
    println!("published {}", record.name);
    println!("artifact {}", record.active_artifact_hash);
    println!("ref {}", record.ref_uri());
    println!("floating skill://{}", record.name);
    println!("record {}", registry.record_path(&record.name)?.display());
    for skill in record.package.skills {
        println!("skill {}", skill.name);
    }
    Ok(())
}

async fn skill_import(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_skill_import_args(args)?;
    if options.help {
        print_skill_import_help();
        return Ok(());
    }
    let skill_dir = options
        .skill_dir
        .ok_or_else(|| usage_error("skill import requires <dir>"))?;
    let skill_registry_root = skill_registry_root(options.registry_root);
    let blob_registry_root = options
        .blob_registry_root
        .unwrap_or_else(default_blob_registry_root);
    let plan = SkillImportPlan::from_directory(&skill_dir, options.name.as_deref())?;

    let record_path = if options.dry_run {
        println!("dry-run {}", plan.package.name);
        None
    } else {
        let skill_registry = LocalSkillRegistry::new(&skill_registry_root);
        let published = plan.publish(
            &skill_registry,
            &LocalBlobRegistry::new(&blob_registry_root),
        )?;
        println!("published {}", published.skill.name);
        Some(skill_registry.record_path(&published.skill.name)?)
    };
    println!("artifact {}", plan.artifact_hash()?);
    println!("ref {}", plan.pinned_ref()?);
    println!("floating {}", plan.floating_ref());
    if let Some(record_path) = record_path {
        println!("record {}", record_path.display());
    }
    for skill in &plan.package.skills {
        println!("skill {}", skill.name);
    }
    for reference in &plan.references {
        println!("reference {reference} appended");
    }
    for asset in &plan.assets {
        println!("blob {} {}", asset.relative_path, asset.ref_uri);
    }
    for script in &plan.omitted_scripts {
        println!("omitted script {script}");
    }
    for hook in &plan.ignored_hooks {
        println!("ignored hook {hook}");
    }
    for file in &plan.skipped_files {
        println!("skipped file {file}");
    }
    println!("manifest fragment:");
    print!("{}", plan.manifest_fragment()?);
    Ok(())
}

async fn blob_publish(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_blob_publish_args(args)?;
    if options.help {
        print_blob_publish_help();
        return Ok(());
    }
    let file = options
        .file
        .ok_or_else(|| usage_error("blob publish requires <file>"))?;
    let registry_root = options
        .registry_root
        .unwrap_or_else(default_blob_registry_root);
    let registry = LocalBlobRegistry::new(registry_root);
    let record = registry.publish_file(&file, options.name.as_deref())?;
    println!("published blob");
    println!("artifact {}", record.artifact_hash);
    println!("content_hash {}", record.content_sha256);
    println!("ref {}", record.ref_uri);
    println!(
        "record {}",
        registry
            .version_record_path(&record.artifact_hash)?
            .display()
    );
    Ok(())
}

fn manuals_for_record(
    record: &PublishedOperationRecord,
    operation: Option<&str>,
) -> CooldisResult<Vec<ToolOperationManual>> {
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
            manuals.push(ToolOperationManual {
                schema_version: 0,
                tool_name: interface.identity.name.clone(),
                operation_name: interface_operation.name.clone(),
                summary: interface_operation
                    .description
                    .clone()
                    .unwrap_or_else(|| format!("Run {}.", interface_operation.name)),
                usage: vec![format!(
                    "cooldis tool run {} {} --input '<input>'",
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
            manuals.push(ToolOperationManual {
                schema_version: 0,
                tool_name: record.name.clone(),
                operation_name: projection.operation_name.clone(),
                summary: format!("Run {} from {}.", projection.operation_name, record.name),
                usage: vec![projection.process.command.clone()],
                input_schema: serde_json::to_value(&projection.input).unwrap_or(Value::Null),
                output_schema: serde_json::to_value(&projection.output).unwrap_or(Value::Null),
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
        return Err(usage_error(format!(
            "published tool {:?} has no{target} manual",
            record.name
        )));
    }
    Ok(manuals)
}

fn cli_manual_exit_status() -> Vec<ToolManualExitStatus> {
    vec![
        ToolManualExitStatus {
            code: 0,
            meaning: "operation succeeded".to_string(),
        },
        ToolManualExitStatus {
            code: 1,
            meaning: "operation failed at runtime".to_string(),
        },
        ToolManualExitStatus {
            code: 2,
            meaning: "caller supplied invalid input or arguments".to_string(),
        },
        ToolManualExitStatus {
            code: 126,
            meaning: "capability or policy denied execution".to_string(),
        },
        ToolManualExitStatus {
            code: 127,
            meaning: "tool or operation was not found".to_string(),
        },
    ]
}

fn print_manuals(manuals: &[ToolOperationManual]) {
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

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "null".to_string())
}

#[derive(Debug)]
struct BuiltImportPackage {
    package: ImportPackageSource,
    plan: OperationImportPlan,
    artifact_path: PathBuf,
    manifest: WasmOperationManifest,
    interface: ToolInterfaceContract,
    receipt: ImportBuildReceipt,
}

async fn build_import_package(package_path: &Path) -> CooldisResult<BuiltImportPackage> {
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

fn print_import_package_build(build: &BuiltImportPackage) {
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

fn import_error(error: crate::OpenApiImportError) -> CooldisError {
    CooldisError::RuntimeFactory(error.to_string())
}

#[derive(Debug)]
struct BuiltToolPackage {
    package: ToolPackageSource,
    artifact_path: PathBuf,
    source: PublishedOperationSource,
    manifest: WasmOperationManifest,
    interface: ToolInterfaceContract,
    receipt: ToolBuildReceipt,
}

async fn build_tool_package(package_path: &Path) -> CooldisResult<BuiltToolPackage> {
    let package = ToolPackageSource::load(package_path)?;
    reject_user_kernel_tool_package(&package)?;
    let (artifact_path, source) = build_tool_package_artifact(&package)?;
    let declared_capabilities = package_capability_requests(&package);
    let manifest =
        validate_wasm_artifact(artifact_path.clone(), declared_capabilities.clone()).await?;
    let registered = RegisteredOperation {
        name: package.manifest.identity.name.clone(),
        manifest: manifest.clone(),
        capability_grants: declared_capabilities,
        metadata: BTreeMap::new(),
    };
    let projections = registered.projections();
    let interface = ToolInterfaceContract::from_package(&package, &manifest, &projections)?;
    let fixtures = run_tool_package_fixtures(&package, &artifact_path, &interface).await?;
    let receipt = ToolBuildReceipt::new(
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

fn reject_user_kernel_tool_package(package: &ToolPackageSource) -> CooldisResult<()> {
    if package.manifest.runtime.kind == "kernel" {
        return Err(usage_error(
            "tool packages with runtime.kind = \"kernel\" are kernel-native records synthesized by Cooldis startup; cooldis tool build/publish cannot author or publish them",
        ));
    }
    Ok(())
}

fn build_tool_package_artifact(
    package: &ToolPackageSource,
) -> CooldisResult<(PathBuf, PublishedOperationSource)> {
    match (
        package.manifest.runtime.module_path.clone(),
        package.manifest.runtime.bin_path.clone(),
    ) {
        (Some(module_path), None) => {
            let release = package.manifest.runtime.release.unwrap_or(true);
            let build = build_rust_wasm_module(
                RustWasmBuildOptions::new(module_path.clone()).with_release(release),
            )?;
            Ok((
                build.artifact_path,
                PublishedOperationSource::Rust {
                    module_path,
                    release,
                },
            ))
        }
        (None, Some(bin_path)) => Ok((
            bin_path.clone(),
            PublishedOperationSource::Wasm { bin_path },
        )),
        (Some(_), Some(_)) => Err(usage_error(
            "tool package runtime cannot declare both module_path and bin_path",
        )),
        (None, None) => Err(usage_error(
            "tool package runtime requires module_path or bin_path",
        )),
    }
}

fn package_capability_requests(package: &ToolPackageSource) -> BTreeSet<String> {
    package
        .manifest
        .operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect()
}

async fn run_tool_package_fixtures(
    package: &ToolPackageSource,
    artifact_path: &Path,
    interface: &ToolInterfaceContract,
) -> CooldisResult<Vec<ToolFixtureRun>> {
    let mut config = WasmRuntimeConfig::new(WasmRuntimeArtifact::path(artifact_path.to_path_buf()))
        .with_capability_grants(interface.capability_requests());
    config = config.with_vfs(package_fixture_vfs(package)?);
    if let Some(max_input_bytes) = package.manifest.runtime.max_input_bytes {
        config = config.with_max_input_bytes(size_limit("max_input_bytes", max_input_bytes)?);
    }
    if let Some(max_output_bytes) = package.manifest.runtime.max_output_bytes {
        config = config.with_max_output_bytes(size_limit("max_output_bytes", max_output_bytes)?);
    }
    let factory = WasmRuntimeFactory::new(config)?;
    let mut runs = Vec::with_capacity(package.manifest.fixtures.len());
    for fixture in &package.manifest.fixtures {
        let input = fs::read(&fixture.input).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to read fixture input {}: {err}",
                fixture.input.display()
            ))
        })?;
        let expected = fs::read(&fixture.expect).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to read fixture expectation {}: {err}",
                fixture.expect.display()
            ))
        })?;
        let output = factory
            .invoke_operation_bytes(&fixture.operation, input)
            .await?
            .output;
        if !fixture_output_matches(&expected, &output) {
            return Err(CooldisError::RuntimeExecution(format!(
                "tool package fixture {:?} failed for operation {:?}: expected {}, got {}",
                fixture.name,
                fixture.operation,
                String::from_utf8_lossy(&expected),
                String::from_utf8_lossy(&output)
            )));
        }
        runs.push(ToolFixtureRun {
            name: fixture.name.clone(),
            operation: fixture.operation.clone(),
            status: "passed".to_string(),
        });
    }
    Ok(runs)
}

/// Builds the read-only VFS mount available while package fixtures run.
fn package_fixture_vfs(package: &ToolPackageSource) -> CooldisResult<Arc<CooldisVfs>> {
    let vfs = Arc::new(CooldisVfs::new(Arc::new(InMemoryFs::new())));
    let fixture_root = package.package_root.join("fixtures");
    if !fixture_root.is_dir() {
        return Ok(vfs);
    }
    let fixture_fs =
        HostFileSystem::new(&fixture_root, HostFileSystemMode::ReadOnly).map_err(|err| {
            CooldisError::RuntimeFactory(format!(
                "failed to prepare package fixture VFS for {}: {err}",
                fixture_root.display()
            ))
        })?;
    vfs.mount("/fixtures", Arc::new(fixture_fs))
        .map_err(|err| CooldisError::RuntimeFactory(format!("failed to mount fixtures: {err}")))?;
    Ok(vfs)
}

fn fixture_output_matches(expected: &[u8], actual: &[u8]) -> bool {
    match (
        serde_json::from_slice::<Value>(expected),
        serde_json::from_slice::<Value>(actual),
    ) {
        (Ok(expected), Ok(actual)) => expected == actual,
        _ => expected == actual,
    }
}

fn size_limit(label: &str, value: u64) -> CooldisResult<usize> {
    usize::try_from(value).map_err(|_| {
        CooldisError::RuntimeFactory(format!("{label} {value} is too large for this platform"))
    })
}

fn print_tool_package_build(build: &BuiltToolPackage) {
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

fn reject_package_build_overrides(options: &BuildArgs) -> CooldisResult<()> {
    if options.name.is_some()
        || options.module_path.is_some()
        || options.config_path.is_some()
        || options.release.is_some()
        || options.conversion.is_some()
    {
        return Err(usage_error(
            "tool build --package reads package source, runtime, and policy from cooldis.tool.toml",
        ));
    }
    Ok(())
}

fn reject_package_publish_overrides(options: &PublishArgs) -> CooldisResult<()> {
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
        return Err(usage_error(
            "tool publish --package reads name, source, capabilities, and metadata from cooldis.tool.toml",
        ));
    }
    Ok(())
}

async fn tool_run(args: Vec<OsString>) -> CooldisResult<()> {
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
            let build = build_rust_wasm_module(
                RustWasmBuildOptions::new(module_path).with_release(release),
            )?;
            let config = WasmRuntimeConfig::new(WasmRuntimeArtifact::path(build.artifact_path))
                .with_max_output_bytes(options.max_output_bytes);
            let manifest = WasmRuntimeFactory::new(config.clone())?
                .validate_operation_artifact()
                .await?;
            (config, manifest)
        }
        (None, Some(bin_path), None) => {
            let config = WasmRuntimeConfig::new(WasmRuntimeArtifact::path(bin_path))
                .with_max_output_bytes(options.max_output_bytes);
            let manifest = WasmRuntimeFactory::new(config.clone())?
                .validate_operation_artifact()
                .await?;
            (config, manifest)
        }
        (None, None, Some(registered_name)) => {
            let registry = LocalOperationRegistry::new(registry_root);
            let record = registry.load_record(&registered_name)?;
            let resolved_secrets = if !required_secret_names(&record.manifest)
                .map_err(secret_cli_error)?
                .is_empty()
            {
                let secret_store = open_secret_store(options.state_home.clone()).await?;
                let resolution =
                    resolve_manifest_secret_resolution(&secret_store, &record.manifest)
                        .await
                        .map_err(secret_cli_error)?;
                if !resolution.is_ready() {
                    return Err(usage_error(format!(
                        "missing required operation secrets: {}; import with `cooldis secret import <name> --from-env <ENV>` or `cooldis secret set <name> --value-stdin`",
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
                BTreeMap::new()
            };
            let mut config = registry.load_runtime_config_for_record(&record).await?;
            if !resolved_secrets.is_empty() {
                config = config.with_secrets(resolved_secrets);
            }
            config = config.with_max_output_bytes(options.max_output_bytes);
            (config, record.manifest)
        }
        (Some(_), Some(_), _) => {
            return Err(usage_error(
                "--module-path and --bin-path are mutually exclusive",
            ));
        }
        (Some(_), None, Some(_)) | (None, Some(_), Some(_)) => {
            return Err(usage_error(
                "tool run cannot combine a published tool name with --module-path or --bin-path",
            ));
        }
        (None, None, None) => {
            return Err(usage_error(
                "tool run requires --module-path, --bin-path, or <published-name> <operation>",
            ));
        }
    };
    let vfs = load_vfs(options.mounts).await?;
    config = config.with_vfs(vfs);
    let factory = WasmRuntimeFactory::new(config)?;
    if manifest.operation(&options.operation).is_none() {
        return Err(CooldisError::RuntimeExecution(format!(
            "operation {:?} is not in wasm manifest",
            options.operation
        )));
    }
    let output = factory
        .invoke_operation_bytes(&options.operation, options.input.into_bytes())
        .await?;
    std::io::stdout().write_all(&output.output).map_err(|err| {
        CooldisError::RuntimeExecution(format!("failed to write operation output: {err}"))
    })?;
    std::io::stdout().flush().map_err(|err| {
        CooldisError::RuntimeExecution(format!("failed to flush operation output: {err}"))
    })?;
    Ok(())
}

async fn tool_source(mut args: Vec<OsString>) -> CooldisResult<()> {
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
                return Err(usage_error(format!(
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
        _ => Err(usage_error(format!(
            "unknown tool source subcommand {subcommand:?}"
        ))),
    }
}

async fn tool_source_add(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_source_add_args(args)?;
    if options.help {
        print_tool_source_add_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("tool source add requires <name>"))?;
    let transport = options
        .kind
        .ok_or_else(|| usage_error("tool source add requires --kind"))?;
    let url = options
        .url
        .ok_or_else(|| usage_error("tool source add requires --url"))?;
    let mut config = McpRemoteServerConfig::new(name, transport, url)?;
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
    println!("transport {}", record.transport.as_str());
    println!("url {}", record.url);
    if let Some(secret) = record.bearer_secret {
        println!("bearer_secret {secret}");
    }
    Ok(())
}

async fn tool_source_discover(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_source_name_args(args, "tool source discover")?;
    if options.help {
        print_tool_source_discover_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("tool source discover requires <name>"))?;
    let registry = open_mcp_source_registry(options.state_home.clone()).await?;
    let record = registry
        .get_source_async(&name)
        .await?
        .ok_or_else(|| usage_error(format!("tool source {name:?} was not found")))?;
    let secret_store = open_secret_store(options.state_home).await?;
    let provider =
        McpRemoteToolProvider::connect(record.to_config(), Some(Arc::new(secret_store))).await?;
    let tools = provider.tool_definitions().await;
    let updated = registry.update_discovered_tools_async(&name, tools).await?;
    println!("discovered tool source {}", updated.name);
    for tool in &updated.discovered_tools {
        println!("tool {}", tool.name);
    }
    Ok(())
}

async fn tool_source_list(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_source_list_args(args, "tool source list")?;
    if options.help {
        print_tool_source_list_help();
        return Ok(());
    }
    let registry = open_mcp_source_registry(options.state_home).await?;
    let records = registry.list_sources_async().await?;
    if options.json {
        let json = Value::Array(
            records
                .iter()
                .map(|record| record.redacted_json())
                .collect(),
        );
        println!(
            "{}",
            serde_json::to_string_pretty(&json).map_err(|err| {
                CooldisError::RuntimeFactory(format!("failed to encode tool source list: {err}"))
            })?
        );
        return Ok(());
    }
    if records.is_empty() {
        println!("no tool sources");
        return Ok(());
    }
    for record in records {
        println!(
            "{} {} tools={}",
            record.name,
            record.transport.as_str(),
            record.discovered_tools.len()
        );
    }
    Ok(())
}

async fn tool_source_show(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_source_show_args(args)?;
    if options.help {
        print_tool_source_show_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("tool source show requires <name>"))?;
    let registry = open_mcp_source_registry(options.state_home).await?;
    let record = registry
        .get_source_async(&name)
        .await?
        .ok_or_else(|| usage_error(format!("tool source {name:?} was not found")))?;
    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&record.redacted_json()).map_err(|err| {
                CooldisError::RuntimeFactory(format!("failed to encode tool source: {err}"))
            })?
        );
        return Ok(());
    }
    println!("name {}", record.name);
    println!("transport {}", record.transport.as_str());
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

async fn tool_source_remove(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_tool_source_name_args(args, "tool source remove")?;
    if options.help {
        print_tool_source_remove_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("tool source remove requires <name>"))?;
    let registry = open_mcp_source_registry(options.state_home).await?;
    if registry.delete_source_async(&name).await? {
        println!("removed tool source {name}");
    } else {
        println!("tool source {name} was not found");
    }
    Ok(())
}

async fn secret_import(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_secret_import_args(args)?;
    if options.help {
        print_secret_import_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("secret import requires <name>"))?;
    let from_env = options
        .from_env
        .ok_or_else(|| usage_error("secret import requires --from-env <ENV>"))?;
    let store = open_secret_store(options.state_home).await?;
    let status = store
        .import_secret_from_env(&name, &from_env)
        .await
        .map_err(secret_cli_error)?;
    println!("imported secret {}", status.name);
    println!("source {}", secret_source_display(&status));
    Ok(())
}

async fn secret_set(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_secret_set_args(args)?;
    if options.help {
        print_secret_set_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("secret set requires <name>"))?;
    if !options.value_stdin {
        return Err(usage_error("secret set requires --value-stdin"));
    }
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(io_error)?;
    let value = trim_stdin_secret_value(value);
    let store = open_secret_store(options.state_home).await?;
    let status = store
        .set_secret(&name, value, SecretSourceKind::Stdin, None)
        .await
        .map_err(secret_cli_error)?;
    println!("stored secret {}", status.name);
    println!("source {}", secret_source_display(&status));
    Ok(())
}

async fn secret_list(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_secret_list_args(args, "secret list")?;
    if options.help {
        print_secret_list_help();
        return Ok(());
    }
    let store = open_secret_store(options.state_home).await?;
    let statuses = store.list().await.map_err(secret_cli_error)?;
    if statuses.is_empty() {
        println!("no secrets");
        return Ok(());
    }
    for status in statuses {
        println!(
            "{}\t{}\tupdated_at_ms={}",
            status.name,
            secret_source_display(&status),
            status.updated_at_ms
        );
    }
    Ok(())
}

async fn secret_status(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_secret_name_args(args, "secret status")?;
    if options.help {
        print_secret_status_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("secret status requires <name>"))?;
    let store = open_secret_store(options.state_home).await?;
    let status = store
        .status(&name)
        .await
        .map_err(secret_cli_error)?
        .ok_or_else(|| usage_error(format!("secret {name:?} was not found")))?;
    println!(
        "{}",
        serde_json::to_string_pretty(&status).map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to encode secret status: {err}"))
        })?
    );
    Ok(())
}

async fn secret_delete(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_secret_name_args(args, "secret delete")?;
    if options.help {
        print_secret_delete_help();
        return Ok(());
    }
    let name = options
        .name
        .ok_or_else(|| usage_error("secret delete requires <name>"))?;
    let store = open_secret_store(options.state_home).await?;
    if store.delete_secret(&name).await.map_err(secret_cli_error)? {
        println!("deleted secret {name}");
    } else {
        println!("secret {name} was not found");
    }
    Ok(())
}

async fn auth_status(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_auth_name_args(args, "auth status")?;
    if options.help {
        print_auth_status_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| usage_error("auth status requires <provider-id>"))?;
    let store = open_provider_store(options.state_home).await?;
    let provider = store
        .get_provider(&provider_id)
        .await
        .map_err(provider_cli_error)?
        .ok_or_else(|| usage_error(format!("provider {provider_id:?} was not found")))?;
    let status =
        crate::llm_provider_auth_status(&store, &provider, &crate::LlmProviderAuthContext::new())
            .await
            .map_err(provider_cli_error)?;
    let value = json!({
        "provider_id": provider.provider_id,
        "display_name": provider.display_name,
        "configured": status.configured,
        "source": status.source,
        "label": status.label,
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&value).map_err(|err| {
            CooldisError::RuntimeFactory(format!("failed to encode auth status: {err}"))
        })?
    );
    Ok(())
}

async fn auth_set(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_auth_set_args(args)?;
    if options.help {
        print_auth_set_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| usage_error("auth set requires <provider-id>"))?;
    if !options.api_key_stdin {
        return Err(usage_error("auth set requires --api-key-stdin"));
    }
    let mut value = String::new();
    std::io::stdin()
        .read_to_string(&mut value)
        .map_err(io_error)?;
    let value = trim_stdin_secret_value(value);
    if value.is_empty() {
        return Err(usage_error("auth set requires a non-empty API key"));
    }
    let store = open_provider_store(options.state_home).await?;
    if store
        .get_provider(&provider_id)
        .await
        .map_err(provider_cli_error)?
        .is_none()
    {
        return Err(usage_error(format!(
            "provider {provider_id:?} was not found"
        )));
    }
    store
        .set_credential(
            &provider_id,
            crate::LlmProviderCredential::ApiKey { key: value },
        )
        .await
        .map_err(provider_cli_error)?;
    println!("stored provider credential {provider_id}");
    Ok(())
}

async fn auth_delete(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_auth_name_args(args, "auth delete")?;
    if options.help {
        print_auth_delete_help();
        return Ok(());
    }
    let provider_id = options
        .provider_id
        .ok_or_else(|| usage_error("auth delete requires <provider-id>"))?;
    let store = open_provider_store(options.state_home).await?;
    store
        .delete_credential(&provider_id)
        .await
        .map_err(provider_cli_error)?;
    println!("deleted provider credential {provider_id}");
    Ok(())
}

#[derive(Debug)]
struct ImportArgs {
    package_path: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct BuildArgs {
    name: Option<String>,
    module_path: Option<PathBuf>,
    package_path: Option<PathBuf>,
    config_path: Option<PathBuf>,
    release: Option<bool>,
    conversion: Option<ToolConversionConfig>,
}

#[derive(Debug)]
struct PublishArgs {
    name: Option<String>,
    module_path: Option<PathBuf>,
    bin_path: Option<PathBuf>,
    package_path: Option<PathBuf>,
    config_path: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    release: Option<bool>,
    capability_grants: BTreeSet<String>,
    metadata: BTreeMap<String, Value>,
    strict_conversion: bool,
    conversion: Option<ToolConversionConfig>,
}

#[derive(Debug)]
struct SkillPublishArgs {
    package_dir: Option<PathBuf>,
    name: Option<String>,
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct SkillImportArgs {
    skill_dir: Option<PathBuf>,
    name: Option<String>,
    registry_root: Option<PathBuf>,
    blob_registry_root: Option<PathBuf>,
    dry_run: bool,
    help: bool,
}

#[derive(Debug)]
struct BlobPublishArgs {
    file: Option<PathBuf>,
    name: Option<String>,
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct ToolRegistryArgs {
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct RunArgs {
    registered_name: Option<String>,
    module_path: Option<PathBuf>,
    bin_path: Option<PathBuf>,
    config_path: Option<PathBuf>,
    state_home: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    operation: String,
    input: String,
    mounts: Vec<MountArg>,
    release: Option<bool>,
    max_output_bytes: usize,
}

#[derive(Debug)]
struct ToolManualArgs {
    tool_name: Option<String>,
    operation: Option<String>,
    registry_root: Option<PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
struct ToolSourceAddArgs {
    name: Option<String>,
    kind: Option<McpRemoteTransport>,
    url: Option<String>,
    bearer_secret: Option<String>,
    headers: Vec<(String, String)>,
    include_tools: BTreeSet<String>,
    timeout_ms: Option<u64>,
    max_output_bytes: Option<u64>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct ToolSourceNameArgs {
    name: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct ToolSourceListArgs {
    state_home: Option<PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
struct ToolSourceShowArgs {
    name: Option<String>,
    state_home: Option<PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
struct SecretImportArgs {
    name: Option<String>,
    from_env: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct SecretSetArgs {
    name: Option<String>,
    value_stdin: bool,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct SecretNameArgs {
    name: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct SecretListArgs {
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct AuthSetArgs {
    provider_id: Option<String>,
    api_key_stdin: bool,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct AuthNameArgs {
    provider_id: Option<String>,
    state_home: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct MountArg {
    guest_path: PathBuf,
    host_path: PathBuf,
}

#[derive(Debug)]
struct AgentInitArgs {
    name: Option<String>,
    out_path: Option<PathBuf>,
    force: bool,
    help: bool,
}

#[derive(Debug)]
struct CouplingInitArgs {
    name: Option<String>,
    out_path: Option<PathBuf>,
    force: bool,
    help: bool,
}

#[derive(Debug)]
enum AgentInitTarget {
    SingleFile(PathBuf),
    ProjectDirectory(PathBuf),
}

impl AgentInitTarget {
    fn from_options(name: &str, out_path: Option<PathBuf>) -> Self {
        match out_path {
            Some(path) if is_agent_manifest_file_path(&path) => Self::SingleFile(path),
            Some(path) => Self::ProjectDirectory(path),
            None => Self::ProjectDirectory(PathBuf::from(name)),
        }
    }
}

#[derive(Debug)]
struct AgentManifestArgs {
    manifest_path: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    operations_registry_root: Option<PathBuf>,
    resolve_ops: bool,
    help: bool,
}

#[derive(Debug)]
struct AgentRegistryArgs {
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct AgentShowArgs {
    reference: Option<String>,
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct AgentRunArgs {
    reference: Option<String>,
    input: Option<String>,
    registry_root: Option<PathBuf>,
    help: bool,
}

#[derive(Debug)]
struct CouplingRunArgs {
    replay: bool,
    artifact: Option<String>,
    coupling_file: Option<PathBuf>,
    coupling_id: Option<String>,
    journal: Option<PathBuf>,
    thread_id: Option<ThreadId>,
    export_bundle: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    json: bool,
    help: bool,
}

#[derive(Debug)]
struct ParsedOperationRef {
    name: String,
    operation: Option<String>,
    artifact_hash: String,
}

#[derive(Default)]
struct CouplingReplayEventStore {
    streams: std::sync::Mutex<BTreeMap<String, Vec<EventRecord>>>,
}

impl CouplingReplayEventStore {
    fn append_recorded_event(&self, event: EventRecord) -> crate::HistoryResult<Vec<EventRecord>> {
        let mut streams = self.streams.lock().map_err(|err| {
            crate::HistoryError::Storage(format!("replay event store lock poisoned: {err}"))
        })?;
        let stream = streams.entry(event.stream_id.to_string()).or_default();
        if stream.iter().any(|existing| existing.id == event.id) {
            return Ok(Vec::new());
        }
        stream.push(event.clone());
        stream.sort_by(|left, right| {
            (left.sequence.get(), left.id.to_string())
                .cmp(&(right.sequence.get(), right.id.to_string()))
        });
        Ok(vec![event])
    }
}

#[async_trait::async_trait]
impl EventStore for CouplingReplayEventStore {
    async fn append_events(
        &self,
        stream_id: &EventStreamId,
        records: Vec<NewEventRecord>,
    ) -> crate::HistoryResult<Vec<EventRecord>> {
        let mut streams = self.streams.lock().map_err(|err| {
            crate::HistoryError::Storage(format!("replay event store lock poisoned: {err}"))
        })?;
        let stream = streams.entry(stream_id.to_string()).or_default();
        let mut next_sequence = stream
            .iter()
            .map(|event| event.sequence.get())
            .max()
            .unwrap_or(0)
            + 1;
        let mut appended = Vec::with_capacity(records.len());
        for record in records {
            let event =
                EventRecord::from_new(stream_id.clone(), EventSequence::new(next_sequence), record);
            next_sequence += 1;
            stream.push(event.clone());
            appended.push(event);
        }
        Ok(appended)
    }

    async fn read_events(
        &self,
        stream_id: &EventStreamId,
        from_sequence: Option<EventSequence>,
    ) -> crate::HistoryResult<Vec<EventRecord>> {
        let streams = self.streams.lock().map_err(|err| {
            crate::HistoryError::Storage(format!("replay event store lock poisoned: {err}"))
        })?;
        let events = streams
            .get(&stream_id.to_string())
            .cloned()
            .unwrap_or_default();
        Ok(match from_sequence {
            Some(sequence) => events
                .into_iter()
                .filter(|event| event.sequence.get() >= sequence.get())
                .collect(),
            None => events,
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CouplingReplayReport {
    schema: &'static str,
    mode: &'static str,
    dry_run: bool,
    snapshot_id: String,
    replayed_event_count: usize,
    runs: Vec<CouplingReplayRunReport>,
    proposal_events: Vec<CouplingReplayProposalEvent>,
}

impl CouplingReplayReport {
    fn from_receipt(
        replayed_event_count: usize,
        coupling_set: &BoundCouplingSet,
        receipt: CouplingSchedulerCycleReceipt,
    ) -> Self {
        let mut proposal_ids = BTreeSet::new();
        let mut proposal_streams = BTreeMap::new();
        for run in &receipt.runs {
            let stream = coupling_set
                .couplings
                .iter()
                .find(|coupling| coupling.id == run.coupling_id)
                .map(|coupling| coupling.sink.stream.clone())
                .unwrap_or_else(|| run.trigger_stream_id.clone());
            for event_id in &run.discharged_event_ids {
                let id = event_id.to_string();
                proposal_ids.insert(id.clone());
                proposal_streams.insert(id, stream.clone());
            }
        }
        let proposal_events = receipt
            .appended_events
            .iter()
            .filter_map(|event| {
                let id = event.id.to_string();
                proposal_ids
                    .contains(&id)
                    .then(|| CouplingReplayProposalEvent {
                        stream: proposal_streams
                            .get(&id)
                            .cloned()
                            .unwrap_or_else(|| event.stream_id.to_string()),
                        stream_id: event.stream_id.to_string(),
                        kind: event.kind.to_string(),
                        payload: event.payload.clone(),
                        provenance: event.provenance.clone(),
                    })
            })
            .collect::<Vec<_>>();
        let runs = receipt
            .runs
            .into_iter()
            .map(CouplingReplayRunReport::from_run)
            .collect();
        Self {
            schema: "cooldis.coupling.replay/1",
            mode: "replay",
            dry_run: true,
            snapshot_id: receipt.snapshot_id,
            replayed_event_count,
            runs,
            proposal_events,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CouplingReplayRunReport {
    coupling_id: String,
    status: &'static str,
    scheduler_status: CouplingRunStatus,
    blocked: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
    trigger_event_id: String,
    trigger_stream_id: String,
    trigger_sequence: i64,
    depth: u32,
    source_event_ids: Vec<String>,
    proposal_event_count: usize,
    budget_spent: crate::CouplingBudgetSpent,
}

impl CouplingReplayRunReport {
    fn from_run(run: crate::CouplingRunReceipt) -> Self {
        let blocked = replay_run_is_blocked(&run);
        Self {
            coupling_id: run.coupling_id,
            status: if blocked {
                "blocked"
            } else {
                coupling_run_status_name(run.status)
            },
            scheduler_status: run.status,
            blocked,
            reason: run.reason,
            trigger_event_id: run.trigger_event_id.to_string(),
            trigger_stream_id: run.trigger_stream_id,
            trigger_sequence: run.trigger_sequence,
            depth: run.depth,
            source_event_ids: run
                .source_event_ids
                .into_iter()
                .map(|event_id| event_id.to_string())
                .collect(),
            proposal_event_count: run.discharged_event_ids.len(),
            budget_spent: run.budget_spent,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct CouplingReplayProposalEvent {
    stream: String,
    stream_id: String,
    kind: String,
    payload: Value,
    provenance: crate::EventProvenance,
}

fn replay_run_is_blocked(run: &crate::CouplingRunReceipt) -> bool {
    run.reason.as_deref() == Some("quota_exhausted")
        || run
            .reason
            .as_deref()
            .is_some_and(|reason| reason.starts_with("budget:"))
}

fn coupling_run_status_name(status: CouplingRunStatus) -> &'static str {
    match status {
        CouplingRunStatus::Completed => "completed",
        CouplingRunStatus::Failed => "failed",
        CouplingRunStatus::Skipped => "skipped",
    }
}

#[derive(Debug)]
struct RpcArgs {
    listen: AppServerListenAddr,
    runtime_home: Option<PathBuf>,
    state_home: Option<PathBuf>,
    cwd: Option<PathBuf>,
}

#[derive(Debug)]
struct ConsoleArgs {
    listen: std::net::SocketAddr,
    cwd: PathBuf,
    cwd_explicit: bool,
    config_path: Option<PathBuf>,
    open: bool,
    help: bool,
}

#[derive(Debug)]
struct DaemonRunArgs {
    config_path: Option<PathBuf>,
}

#[derive(Debug)]
struct DaemonConfigValidateArgs {
    config_path: Option<PathBuf>,
}

#[derive(Debug)]
struct DaemonServicePrintArgs {
    target: CooldisDaemonServiceTarget,
    config_path: PathBuf,
    executable: PathBuf,
    label: String,
    working_directory: Option<PathBuf>,
}

#[derive(Debug)]
struct DaemonServiceUninstallArgs {
    target: CooldisDaemonServiceTarget,
    label: String,
}

#[derive(Debug)]
struct ChatArgs {
    cwd: PathBuf,
    config_path: Option<PathBuf>,
    env_file: Option<PathBuf>,
    runtime_home: Option<PathBuf>,
    state_home: Option<PathBuf>,
    provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    api_key_env: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    stream: Option<bool>,
    attach: Option<String>,
    prompt: Option<String>,
    help: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ChatConfigFile {
    chat: Option<ChatConfigSection>,
    provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    api_key_env: Option<String>,
    region: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_session_token: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    stream: Option<bool>,
    env_file: Option<PathBuf>,
    #[serde(default, alias = "capsuleBindings")]
    capsule_bindings: Option<CapsuleBindingsConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ChatConfigSection {
    provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    api_key_env: Option<String>,
    region: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    aws_session_token: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    stream: Option<bool>,
    env_file: Option<PathBuf>,
    #[serde(default, alias = "capsuleBindings")]
    capsule_bindings: Option<CapsuleBindingsConfig>,
}

#[derive(Clone, Debug)]
enum ChatProviderConfig {
    Local,
    BifrostOpenAI {
        base_url: String,
        api_key: String,
        model: String,
        max_tokens: u32,
        stream: bool,
    },
    OpenAIChatCompletions {
        provider: String,
        base_url: String,
        api_key: String,
        model: String,
        max_tokens: u32,
        stream: bool,
        headers: Vec<(String, String)>,
    },
    AnthropicMessages {
        base_url: String,
        api_key: String,
        model: String,
        max_tokens: u32,
        stream: bool,
    },
    AnthropicBedrock {
        region: String,
        base_url: Option<String>,
        access_key_id: String,
        secret_access_key: String,
        session_token: Option<String>,
        model: String,
        max_tokens: u32,
        stream: bool,
    },
    CatalogOpenAIChatCompletions {
        provider_id: String,
        model: Option<String>,
        max_tokens: u32,
        stream: bool,
    },
}

fn apply_chat_provider_config(config: &mut CooldisAppServerConfig, provider: ChatProviderConfig) {
    match provider {
        ChatProviderConfig::Local => {}
        ChatProviderConfig::BifrostOpenAI {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            config.model = model.clone();
            config.model_provider = APP_SERVER_BIFROST_PROVIDER.to_string();
            config.provider = AppServerProviderConfig::BifrostOpenAIResponses {
                base_url,
                api_key,
                model,
                max_tokens,
                stream,
            };
        }
        ChatProviderConfig::OpenAIChatCompletions {
            provider,
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
            headers,
        } => {
            config.model = model.clone();
            config.model_provider = provider.clone();
            config.provider = AppServerProviderConfig::OpenAIChatCompletions {
                provider,
                base_url,
                api_key,
                model,
                max_tokens,
                stream,
                headers,
            };
        }
        ChatProviderConfig::AnthropicMessages {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            config.model = model.clone();
            config.model_provider = APP_SERVER_ANTHROPIC_PROVIDER.to_string();
            config.provider = AppServerProviderConfig::AnthropicMessages {
                base_url,
                api_key,
                model,
                max_tokens,
                stream,
            };
        }
        ChatProviderConfig::AnthropicBedrock {
            region,
            base_url,
            access_key_id,
            secret_access_key,
            session_token,
            model,
            max_tokens,
            stream,
        } => {
            config.model = model.clone();
            config.model_provider = APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER.to_string();
            config.provider = AppServerProviderConfig::AnthropicBedrock {
                region,
                base_url,
                access_key_id,
                secret_access_key,
                session_token,
                model,
                max_tokens,
                stream,
            };
        }
        ChatProviderConfig::CatalogOpenAIChatCompletions {
            provider_id,
            model,
            max_tokens,
            stream,
        } => {
            if let Some(model) = &model {
                config.model = model.clone();
            }
            config.model_provider = provider_id.clone();
            config.provider = AppServerProviderConfig::CatalogOpenAIChatCompletions {
                provider_id,
                model,
                max_tokens,
                stream,
            };
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ToolConfigFile {
    name: Option<String>,
    module_path: Option<PathBuf>,
    bin_path: Option<PathBuf>,
    registry_root: Option<PathBuf>,
    release: Option<bool>,
    #[serde(default)]
    conversion: Option<ToolConversionConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
struct ToolConversionConfig {
    upstream_url: Option<String>,
    upstream_rev: Option<String>,
    upstream_crate: Option<String>,
}

fn parse_agent_init_args(args: Vec<OsString>) -> CooldisResult<AgentInitArgs> {
    let mut name = None;
    let mut out_path = None;
    let mut force = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--out" => out_path = Some(required_path_value(&mut iter, "--out")?),
            "--force" => force = true,
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown agent init argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("agent init accepts exactly one <name>"));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentInitArgs {
        name,
        out_path,
        force,
        help,
    })
}

fn parse_coupling_init_args(args: Vec<OsString>) -> CooldisResult<CouplingInitArgs> {
    let mut name = None;
    let mut out_path = None;
    let mut force = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--out" => out_path = Some(required_path_value(&mut iter, "--out")?),
            "--force" => force = true,
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown coupling init argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("coupling init accepts exactly one <name>"));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(CouplingInitArgs {
        name,
        out_path,
        force,
        help,
    })
}

fn parse_agent_manifest_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<AgentManifestArgs> {
    let mut manifest_path = None;
    let mut registry_root = None;
    let mut operations_registry_root = None;
    let mut resolve_ops = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--resolve-ops" if command == "agent publish" => resolve_ops = true,
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--operations-registry-root" => {
                operations_registry_root = Some(required_path_value(
                    &mut iter,
                    "--operations-registry-root",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
            _ => {
                if manifest_path.is_some() {
                    return Err(usage_error(format!(
                        "{command} accepts exactly one <manifest>"
                    )));
                }
                manifest_path = Some(PathBuf::from(arg));
            }
        }
    }
    Ok(AgentManifestArgs {
        manifest_path,
        registry_root,
        operations_registry_root,
        resolve_ops,
        help,
    })
}

fn parse_agent_registry_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<AgentRegistryArgs> {
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
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
        }
    }
    Ok(AgentRegistryArgs {
        registry_root,
        help,
    })
}

fn parse_agent_show_args(args: Vec<OsString>) -> CooldisResult<AgentShowArgs> {
    let mut reference = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown agent show argument {other:?}"
                )));
            }
            _ => {
                if reference.is_some() {
                    return Err(usage_error(
                        "agent show accepts exactly one <agent-ref-or-name>",
                    ));
                }
                reference = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentShowArgs {
        reference,
        registry_root,
        help,
    })
}

fn parse_agent_run_args(args: Vec<OsString>) -> CooldisResult<AgentRunArgs> {
    let mut reference = None;
    let mut input = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--input" => input = Some(required_string_value(&mut iter, "--input")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown agent run argument {other:?}")));
            }
            _ => {
                if reference.is_some() {
                    return Err(usage_error("agent run accepts exactly one <agent-ref>"));
                }
                reference = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AgentRunArgs {
        reference,
        input,
        registry_root,
        help,
    })
}

fn parse_coupling_run_args(args: Vec<OsString>) -> CooldisResult<CouplingRunArgs> {
    let mut replay = false;
    let mut artifact = None;
    let mut coupling_file = None;
    let mut coupling_id = None;
    let mut journal = None;
    let mut thread_id = None;
    let mut export_bundle = None;
    let mut registry_root = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--replay" => replay = true,
            "--artifact" => artifact = Some(required_string_value(&mut iter, "--artifact")?),
            "--coupling-file" => {
                coupling_file = Some(required_path_value(&mut iter, "--coupling-file")?)
            }
            "--coupling-id" => {
                coupling_id = Some(required_string_value(&mut iter, "--coupling-id")?)
            }
            "--journal" | "--db" => journal = Some(required_path_value(&mut iter, "--journal")?),
            "--thread-id" | "--thread" => {
                let value = required_string_value(&mut iter, "--thread-id")?;
                thread_id = Some(
                    ThreadId::parse_str(&value)
                        .map_err(|err| usage_error(format!("invalid --thread-id: {err}")))?,
                );
            }
            "--export" => export_bundle = Some(required_path_value(&mut iter, "--export")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--json" => json = true,
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown coupling run argument {other:?}"
                )));
            }
            other => {
                return Err(usage_error(format!(
                    "unexpected coupling run positional argument {other:?}; use --artifact"
                )));
            }
        }
    }
    Ok(CouplingRunArgs {
        replay,
        artifact,
        coupling_file,
        coupling_id,
        journal,
        thread_id,
        export_bundle,
        registry_root,
        json,
        help,
    })
}

fn parse_import_args(args: Vec<OsString>, command: &str) -> CooldisResult<ImportArgs> {
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

fn parse_build_args(args: Vec<OsString>) -> CooldisResult<BuildArgs> {
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
                return Err(usage_error(format!(
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

fn parse_publish_args(args: Vec<OsString>) -> CooldisResult<PublishArgs> {
    let mut name = None;
    let mut module_path = None;
    let mut bin_path = None;
    let mut package_path = None;
    let mut config_path = None;
    let mut registry_root = None;
    let mut release = None;
    let mut capability_grants = BTreeSet::new();
    let mut metadata = BTreeMap::new();
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
                return Err(usage_error(format!(
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

fn parse_skill_publish_args(args: Vec<OsString>) -> CooldisResult<SkillPublishArgs> {
    let mut package_dir = None;
    let mut name = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--name" => name = Some(required_string_value(&mut iter, "--name")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown skill publish argument {other:?}"
                )));
            }
            _ => {
                if package_dir.is_some() {
                    return Err(usage_error("skill publish accepts exactly one <dir>"));
                }
                package_dir = Some(PathBuf::from(arg));
            }
        }
    }
    Ok(SkillPublishArgs {
        package_dir,
        name,
        registry_root,
        help,
    })
}

fn parse_skill_import_args(args: Vec<OsString>) -> CooldisResult<SkillImportArgs> {
    let mut skill_dir = None;
    let mut name = None;
    let mut registry_root = None;
    let mut blob_registry_root = None;
    let mut dry_run = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--name" => name = Some(required_string_value(&mut iter, "--name")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            "--blob-registry-root" => {
                blob_registry_root = Some(required_path_value(&mut iter, "--blob-registry-root")?)
            }
            "--dry-run" => dry_run = true,
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown skill import argument {other:?}"
                )));
            }
            _ => {
                if skill_dir.is_some() {
                    return Err(usage_error("skill import accepts exactly one <dir>"));
                }
                skill_dir = Some(PathBuf::from(arg));
            }
        }
    }
    Ok(SkillImportArgs {
        skill_dir,
        name,
        registry_root,
        blob_registry_root,
        dry_run,
        help,
    })
}

fn parse_blob_publish_args(args: Vec<OsString>) -> CooldisResult<BlobPublishArgs> {
    let mut file = None;
    let mut name = None;
    let mut registry_root = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--name" => name = Some(required_string_value(&mut iter, "--name")?),
            "--registry-root" => {
                registry_root = Some(required_path_value(&mut iter, "--registry-root")?)
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown blob publish argument {other:?}"
                )));
            }
            _ => {
                if file.is_some() {
                    return Err(usage_error("blob publish accepts exactly one <file>"));
                }
                file = Some(PathBuf::from(arg));
            }
        }
    }
    Ok(BlobPublishArgs {
        file,
        name,
        registry_root,
        help,
    })
}

fn parse_tool_registry_args(args: Vec<OsString>, command: &str) -> CooldisResult<ToolRegistryArgs> {
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
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
        }
    }
    Ok(ToolRegistryArgs {
        registry_root,
        help,
    })
}

fn parse_run_args(args: Vec<OsString>) -> CooldisResult<RunArgs> {
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
                max_output_bytes = value
                    .parse()
                    .map_err(|_| usage_error("--max-output-bytes must be a positive integer"))?;
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown tool run argument {other:?}")));
            }
            _ => {
                positionals.push(arg.to_string_lossy().to_string());
            }
        }
    }
    let (registered_name, operation) = if module_path.is_some() || bin_path.is_some() {
        if positionals.len() != 1 {
            return Err(usage_error(
                "tool run with --module-path or --bin-path accepts exactly one operation name",
            ));
        }
        (None, positionals.remove(0))
    } else {
        match positionals.len() {
            1 => (None, positionals.remove(0)),
            2 => (Some(positionals.remove(0)), positionals.remove(0)),
            _ => {
                return Err(usage_error(
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

fn parse_tool_manual_args(args: Vec<OsString>) -> CooldisResult<ToolManualArgs> {
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
                return Err(usage_error(format!(
                    "unknown tool manual argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    if positionals.len() > 2 {
        return Err(usage_error(
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

fn parse_tool_source_add_args(args: Vec<OsString>) -> CooldisResult<ToolSourceAddArgs> {
    let mut name = None;
    let mut kind = None;
    let mut url = None;
    let mut bearer_secret = None;
    let mut headers = Vec::new();
    let mut include_tools = BTreeSet::new();
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
                kind = Some(McpRemoteTransport::from_str(&value)?);
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
                return Err(usage_error(format!(
                    "unknown tool source add argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("tool source add accepts exactly one <name>"));
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

fn parse_tool_source_name_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<ToolSourceNameArgs> {
    let mut name = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error(format!("{command} accepts exactly one <name>")));
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

fn parse_tool_source_list_args(
    args: Vec<OsString>,
    command: &str,
) -> CooldisResult<ToolSourceListArgs> {
    let mut state_home = None;
    let mut json = false;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--json" => json = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other => return Err(usage_error(format!("unknown {command} argument {other:?}"))),
        }
    }
    Ok(ToolSourceListArgs {
        state_home,
        json,
        help,
    })
}

fn parse_tool_source_show_args(args: Vec<OsString>) -> CooldisResult<ToolSourceShowArgs> {
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
                return Err(usage_error(format!(
                    "unknown tool source show argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("tool source show accepts exactly one <name>"));
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

fn parse_secret_import_args(args: Vec<OsString>) -> CooldisResult<SecretImportArgs> {
    let mut name = None;
    let mut from_env = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--from-env" => from_env = Some(required_string_value(&mut iter, "--from-env")?),
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown secret import argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("secret import accepts exactly one <name>"));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(SecretImportArgs {
        name,
        from_env,
        state_home,
        help,
    })
}

fn parse_secret_set_args(args: Vec<OsString>) -> CooldisResult<SecretSetArgs> {
    let mut name = None;
    let mut value_stdin = false;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--value-stdin" => value_stdin = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!(
                    "unknown secret set argument {other:?}"
                )));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error("secret set accepts exactly one <name>"));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(SecretSetArgs {
        name,
        value_stdin,
        state_home,
        help,
    })
}

fn parse_secret_name_args(args: Vec<OsString>, command: &str) -> CooldisResult<SecretNameArgs> {
    let mut name = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
            _ => {
                if name.is_some() {
                    return Err(usage_error(format!("{command} accepts exactly one <name>")));
                }
                name = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(SecretNameArgs {
        name,
        state_home,
        help,
    })
}

fn parse_secret_list_args(args: Vec<OsString>, command: &str) -> CooldisResult<SecretListArgs> {
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
        }
    }
    Ok(SecretListArgs { state_home, help })
}

fn parse_auth_set_args(args: Vec<OsString>) -> CooldisResult<AuthSetArgs> {
    let mut provider_id = None;
    let mut api_key_stdin = false;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--api-key-stdin" => api_key_stdin = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown auth set argument {other:?}")));
            }
            _ => {
                if provider_id.is_some() {
                    return Err(usage_error("auth set accepts exactly one <provider-id>"));
                }
                provider_id = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AuthSetArgs {
        provider_id,
        api_key_stdin,
        state_home,
        help,
    })
}

fn parse_auth_name_args(args: Vec<OsString>, command: &str) -> CooldisResult<AuthNameArgs> {
    let mut provider_id = None;
    let mut state_home = None;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--state-home" => state_home = Some(required_path_value(&mut iter, "--state-home")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown {command} argument {other:?}")));
            }
            _ => {
                if provider_id.is_some() {
                    return Err(usage_error(format!(
                        "{command} accepts exactly one <provider-id>"
                    )));
                }
                provider_id = Some(arg.to_string_lossy().to_string());
            }
        }
    }
    Ok(AuthNameArgs {
        provider_id,
        state_home,
        help,
    })
}

fn parse_rpc_args(args: Vec<OsString>) -> CooldisResult<RpcArgs> {
    let mut listen = None;
    let mut runtime_home = None;
    let mut state_home = None;
    let mut cwd = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--listen" => {
                let value = required_string_value(&mut iter, "--listen")?;
                listen = Some(AppServerListenAddr::parse(&value)?);
            }
            "--runtime-home" => {
                runtime_home = Some(PathBuf::from(required_string_value(
                    &mut iter,
                    "--runtime-home",
                )?));
            }
            "--state-home" => {
                state_home = Some(PathBuf::from(required_string_value(
                    &mut iter,
                    "--state-home",
                )?));
            }
            "--cwd" => {
                cwd = Some(PathBuf::from(required_string_value(&mut iter, "--cwd")?));
            }
            other => {
                return Err(usage_error(format!("unknown rpc argument {other:?}")));
            }
        }
    }
    let listen = listen
        .ok_or_else(|| usage_error("rpc requires --listen unix://PATH or ws://HOST:PORT[/rpc]"))?;
    Ok(RpcArgs {
        listen,
        runtime_home,
        state_home,
        cwd,
    })
}

fn parse_console_args(args: Vec<OsString>) -> CooldisResult<ConsoleArgs> {
    let mut listen = "127.0.0.1:0"
        .parse::<std::net::SocketAddr>()
        .expect("default console listen address is valid");
    let mut cwd = std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current working directory: {err}")))?;
    let mut cwd_explicit = false;
    let mut config_path = None;
    let mut open = true;
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--no-open" => open = false,
            "--cwd" => {
                cwd = PathBuf::from(required_string_value(&mut iter, "--cwd")?);
                cwd_explicit = true;
            }
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            "--port" => {
                let port = required_string_value(&mut iter, "--port")?
                    .parse::<u16>()
                    .map_err(|_| usage_error("--port must be an integer from 0 to 65535"))?;
                listen = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            }
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown console argument {other:?}")));
            }
            other => {
                return Err(usage_error(format!(
                    "cooldis console does not accept positional argument {other:?}"
                )));
            }
        }
    }
    Ok(ConsoleArgs {
        listen,
        cwd,
        cwd_explicit,
        config_path,
        open,
        help,
    })
}

fn parse_daemon_run_args(args: Vec<OsString>) -> CooldisResult<DaemonRunArgs> {
    let mut config_path = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            other => {
                return Err(usage_error(format!(
                    "unknown daemon run argument {other:?}"
                )));
            }
        }
    }
    Ok(DaemonRunArgs { config_path })
}

fn parse_daemon_config_validate_args(
    args: Vec<OsString>,
) -> CooldisResult<DaemonConfigValidateArgs> {
    let mut config_path = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            other => {
                return Err(usage_error(format!(
                    "unknown daemon config validate argument {other:?}"
                )));
            }
        }
    }
    Ok(DaemonConfigValidateArgs { config_path })
}

fn parse_daemon_service_print_args(args: Vec<OsString>) -> CooldisResult<DaemonServicePrintArgs> {
    let mut target = default_daemon_service_target();
    let mut config_path = None;
    let mut executable = None;
    let mut label = "com.cooldis.daemon".to_string();
    let mut working_directory = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--target" => {
                let value = required_string_value(&mut iter, "--target")?;
                target = CooldisDaemonServiceTarget::parse(&value)?;
            }
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            "--bin" | "--executable" => executable = Some(required_path_value(&mut iter, "--bin")?),
            "--label" => label = required_string_value(&mut iter, "--label")?,
            "--working-directory" | "--cwd" => {
                working_directory = Some(required_path_value(&mut iter, "--working-directory")?)
            }
            other => {
                return Err(usage_error(format!(
                    "unknown daemon service print argument {other:?}"
                )));
            }
        }
    }

    let config_path = match config_path {
        Some(path) => path,
        None => discover_cooldis_daemon_config_path()?.ok_or_else(|| {
            usage_error("daemon service print requires --config when no cooldis.toml exists")
        })?,
    };
    let executable = match executable {
        Some(path) => path,
        None => std::env::current_exe()
            .map_err(|err| usage_error(format!("failed to read current executable: {err}")))?,
    };

    Ok(DaemonServicePrintArgs {
        target,
        config_path,
        executable,
        label,
        working_directory,
    })
}

fn parse_daemon_service_uninstall_args(
    args: Vec<OsString>,
) -> CooldisResult<DaemonServiceUninstallArgs> {
    let mut target = default_daemon_service_target();
    let mut label = "com.cooldis.daemon".to_string();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--target" => {
                let value = required_string_value(&mut iter, "--target")?;
                target = CooldisDaemonServiceTarget::parse(&value)?;
            }
            "--label" => label = required_string_value(&mut iter, "--label")?,
            other => {
                return Err(usage_error(format!(
                    "unknown daemon service uninstall argument {other:?}"
                )));
            }
        }
    }

    Ok(DaemonServiceUninstallArgs { target, label })
}

fn parse_chat_args(args: Vec<OsString>) -> CooldisResult<ChatArgs> {
    let mut cwd = std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current working directory: {err}")))?;
    let mut config_path = None;
    let mut env_file = None;
    let mut runtime_home = None;
    let mut state_home = None;
    let mut provider = None;
    let mut base_url = None;
    let mut api_key = None;
    let mut api_key_env = None;
    let mut model = None;
    let mut max_tokens = None;
    let mut stream = None;
    let mut attach = None;
    let mut positionals = Vec::new();
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--config" => config_path = Some(required_path_value(&mut iter, "--config")?),
            "--env-file" => env_file = Some(required_path_value(&mut iter, "--env-file")?),
            "--cwd" => cwd = PathBuf::from(required_string_value(&mut iter, "--cwd")?),
            "--runtime-home" => {
                runtime_home = Some(PathBuf::from(required_string_value(
                    &mut iter,
                    "--runtime-home",
                )?));
            }
            "--state-home" => {
                state_home = Some(PathBuf::from(required_string_value(
                    &mut iter,
                    "--state-home",
                )?));
            }
            "--provider" => provider = Some(required_string_value(&mut iter, "--provider")?),
            "--base-url" => base_url = Some(required_string_value(&mut iter, "--base-url")?),
            "--api-key" => api_key = Some(required_string_value(&mut iter, "--api-key")?),
            "--api-key-env" => {
                api_key_env = Some(required_string_value(&mut iter, "--api-key-env")?)
            }
            "--model" => model = Some(required_string_value(&mut iter, "--model")?),
            "--max-tokens" => {
                let value = required_string_value(&mut iter, "--max-tokens")?;
                max_tokens = Some(
                    value
                        .parse()
                        .map_err(|_| usage_error("--max-tokens must be a positive integer"))?,
                );
            }
            "--stream" => stream = Some(true),
            "--no-stream" => stream = Some(false),
            "--attach" => attach = Some(required_string_value(&mut iter, "--attach")?),
            other if other.starts_with('-') => {
                return Err(usage_error(format!("unknown chat argument {other:?}")));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    let prompt = if positionals.is_empty() {
        None
    } else {
        Some(positionals.join(" "))
    };
    Ok(ChatArgs {
        cwd,
        config_path,
        env_file,
        runtime_home,
        state_home,
        provider,
        base_url,
        api_key,
        api_key_env,
        model,
        max_tokens,
        stream,
        attach,
        prompt,
        help,
    })
}

fn required_path_value(
    iter: &mut impl Iterator<Item = OsString>,
    flag: &'static str,
) -> CooldisResult<PathBuf> {
    iter.next()
        .map(PathBuf::from)
        .ok_or_else(|| usage_error(format!("{flag} requires a value")))
}

fn required_string_value(
    iter: &mut impl Iterator<Item = OsString>,
    flag: &'static str,
) -> CooldisResult<String> {
    iter.next()
        .map(|value| value.to_string_lossy().to_string())
        .ok_or_else(|| usage_error(format!("{flag} requires a value")))
}

fn parse_mount_arg(value: &str) -> CooldisResult<MountArg> {
    let Some((guest_path, host_path)) = value.split_once('=') else {
        return Err(usage_error("--mount must use /guest/path=/host/path"));
    };
    let guest_path = PathBuf::from(guest_path);
    if !guest_path.is_absolute() {
        return Err(usage_error("--mount guest path must be absolute"));
    }
    Ok(MountArg {
        guest_path,
        host_path: PathBuf::from(host_path),
    })
}

fn parse_header_arg(value: &str) -> CooldisResult<(String, String)> {
    let Some((name, header_value)) = value.split_once('=') else {
        return Err(usage_error("--header must use name=value"));
    };
    if name.trim().is_empty() {
        return Err(usage_error("--header name cannot be empty"));
    }
    Ok((name.trim().to_string(), header_value.to_string()))
}

fn parse_u64_arg(flag: &str, value: &str) -> CooldisResult<u64> {
    value
        .parse()
        .map_err(|_| usage_error(format!("{flag} must be a positive integer")))
}

fn parse_metadata_arg(value: &str) -> CooldisResult<(String, Value)> {
    let Some((key, raw_value)) = value.split_once('=') else {
        return Err(usage_error("--metadata must use key=value"));
    };
    if key.trim().is_empty() {
        return Err(usage_error("--metadata key cannot be empty"));
    }
    let value = serde_json::from_str(raw_value).unwrap_or_else(|_| Value::String(raw_value.into()));
    Ok((key.to_string(), value))
}

fn default_registry_root() -> PathBuf {
    PathBuf::from(".cooldis").join("operations")
}

fn default_project_state_home() -> PathBuf {
    PathBuf::from(".cooldis").join("state")
}

fn default_user_state_home() -> CooldisResult<PathBuf> {
    Ok(default_user_cooldis_home()?.join("state"))
}

fn metadata_store_path_for_state_home(
    state_home: Option<PathBuf>,
    default_state_home: PathBuf,
) -> PathBuf {
    state_home
        .unwrap_or(default_state_home)
        .join("metadata.sqlite3")
}

async fn open_secret_store(state_home: Option<PathBuf>) -> CooldisResult<SqliteSecretStore> {
    SqliteSecretStore::open(metadata_store_path_for_state_home(
        state_home,
        default_user_state_home()?,
    ))
    .await
    .map_err(|err| {
        if turso_cross_process_lock_error(&err.to_string()) {
            cross_process_database_guidance("stop the daemon and retry")
        } else {
            secret_cli_error(err)
        }
    })
}

async fn open_provider_store(state_home: Option<PathBuf>) -> CooldisResult<SqliteMetadataStore> {
    let store = SqliteMetadataStore::open(metadata_store_path_for_state_home(
        state_home,
        default_user_state_home()?,
    ))
    .await
    .map_err(|err| {
        if turso_cross_process_lock_error(&err.to_string()) {
            cross_process_database_guidance(
                "use the running daemon's modelProvider RPC or stop the daemon and retry",
            )
        } else {
            provider_cli_error(err)
        }
    })?;
    crate::seed_default_llm_providers(&store)
        .await
        .map_err(provider_cli_error)?;
    Ok(store)
}

async fn open_mcp_source_registry(
    state_home: Option<PathBuf>,
) -> CooldisResult<SqliteMcpSourceRegistry> {
    SqliteMcpSourceRegistry::open_async(metadata_store_path_for_state_home(
        state_home,
        default_project_state_home(),
    ))
    .await
    .map_err(|err| {
        if turso_cross_process_lock_error(&err.to_string()) {
            cross_process_database_guidance(
                "use the running daemon's mcpSource RPC or stop the daemon and retry",
            )
        } else {
            err
        }
    })
}

fn cross_process_database_guidance(alternative: &str) -> CooldisError {
    usage_error(format!(
        "another process holds this database (most likely the cooldis daemon); {alternative}"
    ))
}

fn turso_cross_process_lock_error(message: &str) -> bool {
    // turso 0.7.0-pre.18 erases LimboError::LockingError through turso::Error.
    let Some((_, engine_error)) = message.split_once("sqlite engine error: ") else {
        return false;
    };
    engine_error == "Locking error: Failed locking file. File is locked by another process"
        || (engine_error.starts_with("Locking error: Failed locking file '")
            && engine_error.ends_with("'. File is locked by another process"))
}

fn secret_cli_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::RuntimeFactory(format!("secret store failed: {err}"))
}

fn provider_cli_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::RuntimeFactory(format!("provider store failed: {err}"))
}

fn secret_source_display(status: &crate::SecretStatus) -> String {
    match (&status.source_kind, status.source_label.as_deref()) {
        (SecretSourceKind::Env, Some(label)) => format!("env:{label}"),
        (SecretSourceKind::Env, None) => "env".to_string(),
        (SecretSourceKind::Stdin, _) => "stdin".to_string(),
        (SecretSourceKind::Local, Some(label)) => format!("local:{label}"),
        (SecretSourceKind::Local, None) => "local".to_string(),
    }
}

fn trim_stdin_secret_value(mut value: String) -> String {
    if value.ends_with('\n') {
        value.pop();
        if value.ends_with('\r') {
            value.pop();
        }
    }
    value
}

fn default_daemon_service_target() -> CooldisDaemonServiceTarget {
    if cfg!(target_os = "macos") {
        CooldisDaemonServiceTarget::Launchd
    } else {
        CooldisDaemonServiceTarget::Systemd
    }
}

fn ingress_persistence_mode_name(mode: IngressPersistenceMode) -> &'static str {
    match mode {
        IngressPersistenceMode::DurableQueue => "durable_queue",
        IngressPersistenceMode::BestEffortDirect => "best_effort_direct",
    }
}

fn load_tool_config(path: Option<&Path>) -> CooldisResult<ToolConfigFile> {
    let discovered;
    let path = if let Some(path) = path {
        path
    } else {
        discovered = PathBuf::from("cooldis.json");
        if !discovered.exists() {
            return Ok(ToolConfigFile::default());
        }
        discovered.as_path()
    };
    let bytes = fs::read(path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to read tool config {}: {err}",
            path.display()
        ))
    })?;
    let mut config: ToolConfigFile = serde_json::from_slice(&bytes).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to decode tool config {} as JSON: {err}",
            path.display()
        ))
    })?;
    let base = path.parent().unwrap_or_else(|| Path::new("."));
    relativize_config_paths(&mut config, base);
    Ok(config)
}

#[derive(Debug)]
struct ToolConversionAudit {
    manifest_path: PathBuf,
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

fn audit_strict_stateless_conversion(
    module_path: &Path,
    conversion: Option<&ToolConversionConfig>,
) -> CooldisResult<ToolConversionAudit> {
    let manifest_path = resolve_cargo_manifest_path(module_path)?;
    let crate_root = manifest_path.parent().unwrap_or_else(|| Path::new("."));
    let manifest_text = fs::read_to_string(&manifest_path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to read Cargo manifest {}: {err}",
            manifest_path.display()
        ))
    })?;
    let manifest: toml::Value = toml::from_str(&manifest_text).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
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

fn resolve_cargo_manifest_path(module_path: &Path) -> CooldisResult<PathBuf> {
    let path = if module_path.file_name() == Some(OsStr::new("Cargo.toml")) {
        module_path.to_path_buf()
    } else {
        module_path.join("Cargo.toml")
    };
    if !path.exists() {
        return Err(CooldisError::RuntimeFactory(format!(
            "Rust Wasm module manifest not found at {}",
            path.display()
        )));
    }
    Ok(path)
}

fn collect_cargo_dependency_names(manifest: &toml::Value) -> BTreeSet<String> {
    let mut dependencies = BTreeSet::new();
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

fn collect_dependency_table(value: Option<&toml::Value>, dependencies: &mut BTreeSet<String>) {
    let Some(table) = value.and_then(toml::Value::as_table) else {
        return;
    };
    dependencies.extend(table.keys().cloned());
}

fn strict_conversion_denied_dependencies() -> BTreeSet<&'static str> {
    ["git2", "heed", "libc", "memmap2", "notify", "rayon"]
        .into_iter()
        .collect()
}

fn json_label(value: &impl serde::Serialize) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
        .unwrap_or_else(|| "unknown".to_string())
}

fn load_chat_provider_config(args: &ChatArgs) -> CooldisResult<ChatProviderConfig> {
    let (mut config, config_base) = load_chat_config_file(args.config_path.as_deref())?;
    if let Some(provider) = args.provider.clone() {
        config.provider = Some(provider);
    }
    if let Some(base_url) = args.base_url.clone() {
        config.base_url = Some(base_url);
    }
    if let Some(api_key) = args.api_key.clone() {
        config.api_key = Some(api_key);
    }
    if let Some(api_key_env) = args.api_key_env.clone() {
        config.api_key_env = Some(api_key_env);
    }
    if let Some(model) = args.model.clone() {
        config.model = Some(model);
    }
    if let Some(max_tokens) = args.max_tokens {
        config.max_tokens = Some(max_tokens);
    }
    if let Some(stream) = args.stream {
        config.stream = Some(stream);
    }
    if let Some(env_file) = args.env_file.clone() {
        config.env_file = Some(env_file);
    }

    let provider = config.provider.as_deref().unwrap_or_else(|| {
        if config.aws_access_key_id.is_some() || config.aws_secret_access_key.is_some() {
            "anthropic_bedrock"
        } else if config.base_url.is_some() || config.model.is_some() || config.api_key.is_some() {
            "bifrost_openai"
        } else {
            "local"
        }
    });

    match provider {
        "local" | "local_offline" | "offline" => Ok(ChatProviderConfig::Local),
        "bifrost" | "bifrost_openai" | "openai" | "openai_responses" => {
            let env_file = config
                .env_file
                .clone()
                .map(|path| {
                    resolve_config_path(config_base.as_deref().unwrap_or(Path::new(".")), path)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_CHAT_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_BIFROST_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("COOLDIS_BIFROST_URL", &file_env))
                .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Bifrost chat provider requires chat.base_url, COOLDIS_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
                    )
                })?
                .trim_end_matches('/')
                .to_string();
            let api_key = config
                .api_key
                .clone()
                .or_else(|| {
                    config
                        .api_key_env
                        .as_deref()
                        .and_then(|name| env_or_file(name, &file_env))
                })
                .or_else(|| env_or_file("COOLDIS_BIFROST_KEY", &file_env))
                .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Bifrost chat provider requires chat.api_key, chat.api_key_env, COOLDIS_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("COOLDIS_BIFROST_OPENAI_MODEL", &file_env))
                .unwrap_or_else(|| APP_SERVER_BIFROST_MODEL.to_string());
            Ok(ChatProviderConfig::BifrostOpenAI {
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
            })
        }
        "anthropic" | "anthropic_messages" => {
            let env_file = config
                .env_file
                .clone()
                .map(|path| {
                    resolve_config_path(config_base.as_deref().unwrap_or(Path::new(".")), path)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_CHAT_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_ANTHROPIC_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("COOLDIS_ANTHROPIC_URL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_BASE_URL", &file_env))
                .unwrap_or_else(|| "https://api.anthropic.com".to_string())
                .trim_end_matches('/')
                .to_string();
            let api_key = config
                .api_key
                .clone()
                .or_else(|| {
                    config
                        .api_key_env
                        .as_deref()
                        .and_then(|name| env_or_file(name, &file_env))
                })
                .or_else(|| env_or_file("ANTHROPIC_API_KEY", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Anthropic chat provider requires chat.api_key, chat.api_key_env, or ANTHROPIC_API_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("COOLDIS_ANTHROPIC_MODEL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_MODEL", &file_env))
                .unwrap_or_else(|| APP_SERVER_ANTHROPIC_MODEL.to_string());
            Ok(ChatProviderConfig::AnthropicMessages {
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
            })
        }
        "anthropic_bedrock" | "bedrock" | "bedrock_anthropic" => {
            let env_file = config
                .env_file
                .clone()
                .map(|path| {
                    resolve_config_path(config_base.as_deref().unwrap_or(Path::new(".")), path)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_CHAT_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_BEDROCK_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_ANTHROPIC_BEDROCK_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let region = config
                .region
                .clone()
                .or_else(|| env_or_file("AWS_BEDROCK_REGION", &file_env))
                .or_else(|| env_or_file("AWS_REGION", &file_env))
                .or_else(|| env_or_file("AWS_DEFAULT_REGION", &file_env))
                .unwrap_or_else(|| "us-east-1".to_string());
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("COOLDIS_BEDROCK_BASE_URL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_BEDROCK_BASE_URL", &file_env))
                .map(|url| url.trim_end_matches('/').to_string());
            let access_key_id = config
                .aws_access_key_id
                .clone()
                .or_else(|| env_or_file("AWS_ACCESS_KEY_ID", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Anthropic Bedrock provider requires AWS_ACCESS_KEY_ID or chat.aws_access_key_id",
                    )
                })?;
            let secret_access_key = config
                .aws_secret_access_key
                .clone()
                .or_else(|| env_or_file("AWS_SECRET_ACCESS_KEY", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Anthropic Bedrock provider requires AWS_SECRET_ACCESS_KEY or chat.aws_secret_access_key",
                    )
                })?;
            let session_token = config
                .aws_session_token
                .clone()
                .or_else(|| env_or_file("AWS_SESSION_TOKEN", &file_env));
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("COOLDIS_ANTHROPIC_BEDROCK_MODEL", &file_env))
                .or_else(|| env_or_file("AWS_BEDROCK_MODEL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_DEFAULT_SONNET_MODEL", &file_env))
                .unwrap_or_else(|| APP_SERVER_ANTHROPIC_BEDROCK_MODEL.to_string());
            let stream = config.stream.unwrap_or(true);
            Ok(ChatProviderConfig::AnthropicBedrock {
                region,
                base_url,
                access_key_id,
                secret_access_key,
                session_token,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream,
            })
        }
        "bifrost_openai_chat"
        | "bifrost_chat"
        | "openai_chat"
        | "openai_chat_completions"
        | "openai_compatible"
        | "openai_compatible_openai"
        | "openai_compatible_chat"
        | "openai_compatible_serverless" => {
            let openai_compatible = provider_is_openai_compatible(provider);
            let env_file = config
                .env_file
                .clone()
                .map(|path| {
                    resolve_config_path(config_base.as_deref().unwrap_or(Path::new(".")), path)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_CHAT_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    if openai_compatible {
                        std::env::var("COOLDIS_OPENAI_COMPATIBLE_ENV_FILE")
                            .ok()
                            .map(PathBuf::from)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    std::env::var("COOLDIS_BIFROST_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            if openai_compatible
                && config.base_url.is_none()
                && config.api_key.is_none()
                && config.api_key_env.is_none()
                && !file_env.contains_key("COOLDIS_OPENAI_COMPATIBLE_API_KEY")
                && !file_env.contains_key("OPENAI_COMPATIBLE_API_KEY")
            {
                let model = config
                    .model
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env));
                return Ok(ChatProviderConfig::CatalogOpenAIChatCompletions {
                    provider_id: APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string(),
                    model,
                    max_tokens: config.max_tokens.unwrap_or(4096),
                    stream: config.stream.unwrap_or(true),
                });
            }
            let base_url = if openai_compatible {
                config
                    .base_url
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_URL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_BASE_URL", &file_env))
                    .unwrap_or_else(|| "https://api.example.invalid/v1".to_string())
            } else {
                config
                    .base_url
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_BIFROST_URL", &file_env))
                    .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                    .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
                    .ok_or_else(|| {
                        usage_error(
                            "OpenAI Chat Completions provider requires chat.base_url, COOLDIS_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
                        )
                    })?
            }
            .trim_end_matches('/')
            .to_string();
            let api_key = if openai_compatible {
                config
                    .api_key
                    .clone()
                    .or_else(|| {
                        config
                            .api_key_env
                            .as_deref()
                            .and_then(|name| env_or_file(name, &file_env))
                    })
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .ok_or_else(|| {
                        usage_error(
                            "OpenAI Compatible chat provider requires chat.api_key, chat.api_key_env, COOLDIS_OPENAI_COMPATIBLE_API_KEY, or OPENAI_COMPATIBLE_API_KEY",
                        )
                    })?
            } else {
                config
                    .api_key
                    .clone()
                    .or_else(|| {
                        config
                            .api_key_env
                            .as_deref()
                            .and_then(|name| env_or_file(name, &file_env))
                    })
                    .or_else(|| env_or_file("COOLDIS_BIFROST_KEY", &file_env))
                    .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                    .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                    .ok_or_else(|| {
                        usage_error(
                            "OpenAI Chat Completions provider requires chat.api_key, chat.api_key_env, COOLDIS_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                        )
                    })?
            };
            let model = if openai_compatible {
                config
                    .model
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env))
                    .unwrap_or_else(|| APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string())
            } else {
                config
                    .model
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_BIFROST_OPENAI_CHAT_MODEL", &file_env))
                    .or_else(|| env_or_file("COOLDIS_BIFROST_OPENAI_MODEL", &file_env))
                    .unwrap_or_else(|| APP_SERVER_BIFROST_MODEL.to_string())
            };
            Ok(ChatProviderConfig::OpenAIChatCompletions {
                provider: chat_completions_provider_name(provider),
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
                headers: provider_default_headers(provider),
            })
        }
        other => Err(usage_error(format!(
            "unknown chat provider {other:?}; expected local, bifrost_openai, openai_chat_completions, anthropic, anthropic_bedrock, or openai_compatible"
        ))),
    }
}

fn load_chat_capsule_bindings_config(args: &ChatArgs) -> CooldisResult<CapsuleBindingsConfig> {
    let (config, config_base) = load_chat_config_file(args.config_path.as_deref())?;
    let mut capsule_bindings = config.capsule_bindings.unwrap_or_default();
    if let Some(registry_root) = capsule_bindings.registry_root.take() {
        capsule_bindings.registry_root = Some(match config_base.as_deref() {
            Some(base) => resolve_config_path(base, registry_root),
            None => registry_root,
        });
    }
    Ok(capsule_bindings)
}

fn load_daemon_provider_config(
    config: &CooldisProviderConfig,
) -> CooldisResult<ChatProviderConfig> {
    match config.provider_name() {
        "local" | "local_offline" | "offline" => Ok(ChatProviderConfig::Local),
        "bifrost" | "bifrost_openai" => {
            let env_file = config
                .env_file
                .clone()
                .or_else(|| {
                    std::env::var("COOLDIS_DAEMON_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_BIFROST_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("COOLDIS_BIFROST_URL", &file_env))
                .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Bifrost daemon provider requires provider.base_url, COOLDIS_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
                    )
                })?
                .trim_end_matches('/')
                .to_string();
            let api_key = config
                .api_key
                .clone()
                .or_else(|| {
                    config
                        .api_key_env
                        .as_deref()
                        .and_then(|name| env_or_file(name, &file_env))
                })
                .or_else(|| env_or_file("COOLDIS_BIFROST_KEY", &file_env))
                .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Bifrost daemon provider requires provider.api_key, provider.api_key_env, COOLDIS_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("COOLDIS_BIFROST_OPENAI_MODEL", &file_env))
                .unwrap_or_else(|| APP_SERVER_BIFROST_MODEL.to_string());
            Ok(ChatProviderConfig::BifrostOpenAI {
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
            })
        }
        "anthropic" | "anthropic_messages" => {
            let env_file = config
                .env_file
                .clone()
                .or_else(|| {
                    std::env::var("COOLDIS_DAEMON_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_ANTHROPIC_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("COOLDIS_ANTHROPIC_URL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_BASE_URL", &file_env))
                .unwrap_or_else(|| "https://api.anthropic.com".to_string())
                .trim_end_matches('/')
                .to_string();
            let api_key = config
                .api_key
                .clone()
                .or_else(|| {
                    config
                        .api_key_env
                        .as_deref()
                        .and_then(|name| env_or_file(name, &file_env))
                })
                .or_else(|| env_or_file("ANTHROPIC_API_KEY", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Anthropic daemon provider requires provider.api_key, provider.api_key_env, or ANTHROPIC_API_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("COOLDIS_ANTHROPIC_MODEL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_MODEL", &file_env))
                .unwrap_or_else(|| APP_SERVER_ANTHROPIC_MODEL.to_string());
            Ok(ChatProviderConfig::AnthropicMessages {
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
            })
        }
        "anthropic_bedrock" | "bedrock" | "bedrock_anthropic" => {
            let env_file = config
                .env_file
                .clone()
                .or_else(|| {
                    std::env::var("COOLDIS_DAEMON_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_BEDROCK_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("COOLDIS_ANTHROPIC_BEDROCK_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let region = config
                .region
                .clone()
                .or_else(|| env_or_file("AWS_BEDROCK_REGION", &file_env))
                .or_else(|| env_or_file("AWS_REGION", &file_env))
                .or_else(|| env_or_file("AWS_DEFAULT_REGION", &file_env))
                .unwrap_or_else(|| "us-east-1".to_string());
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("COOLDIS_BEDROCK_BASE_URL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_BEDROCK_BASE_URL", &file_env))
                .map(|url| url.trim_end_matches('/').to_string());
            let access_key_id = config
                .aws_access_key_id
                .clone()
                .or_else(|| env_or_file("AWS_ACCESS_KEY_ID", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Anthropic Bedrock daemon provider requires AWS_ACCESS_KEY_ID or provider.aws_access_key_id",
                    )
                })?;
            let secret_access_key = config
                .aws_secret_access_key
                .clone()
                .or_else(|| env_or_file("AWS_SECRET_ACCESS_KEY", &file_env))
                .ok_or_else(|| {
                    usage_error(
                        "Anthropic Bedrock daemon provider requires AWS_SECRET_ACCESS_KEY or provider.aws_secret_access_key",
                    )
                })?;
            let session_token = config
                .aws_session_token
                .clone()
                .or_else(|| env_or_file("AWS_SESSION_TOKEN", &file_env));
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("COOLDIS_ANTHROPIC_BEDROCK_MODEL", &file_env))
                .or_else(|| env_or_file("AWS_BEDROCK_MODEL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_DEFAULT_SONNET_MODEL", &file_env))
                .unwrap_or_else(|| APP_SERVER_ANTHROPIC_BEDROCK_MODEL.to_string());
            let stream = config.stream.unwrap_or(true);
            Ok(ChatProviderConfig::AnthropicBedrock {
                region,
                base_url,
                access_key_id,
                secret_access_key,
                session_token,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream,
            })
        }
        "bifrost_openai_chat"
        | "bifrost_chat"
        | "openai_chat"
        | "openai_chat_completions"
        | "openai_compatible"
        | "openai_compatible_openai"
        | "openai_compatible_chat"
        | "openai_compatible_serverless" => {
            let provider_name = config.provider_name();
            let openai_compatible = provider_is_openai_compatible(provider_name);
            let env_file = config
                .env_file
                .clone()
                .or_else(|| {
                    std::env::var("COOLDIS_DAEMON_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .or_else(|| {
                    if openai_compatible {
                        std::env::var("COOLDIS_OPENAI_COMPATIBLE_ENV_FILE")
                            .ok()
                            .map(PathBuf::from)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    std::env::var("COOLDIS_BIFROST_ENV_FILE")
                        .ok()
                        .map(PathBuf::from)
                })
                .unwrap_or_else(|| PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            if openai_compatible
                && config.base_url.is_none()
                && config.api_key.is_none()
                && config.api_key_env.is_none()
                && !file_env.contains_key("COOLDIS_OPENAI_COMPATIBLE_API_KEY")
                && !file_env.contains_key("OPENAI_COMPATIBLE_API_KEY")
            {
                let model = config
                    .model
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env));
                return Ok(ChatProviderConfig::CatalogOpenAIChatCompletions {
                    provider_id: APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string(),
                    model,
                    max_tokens: config.max_tokens.unwrap_or(4096),
                    stream: config.stream.unwrap_or(true),
                });
            }
            let base_url = if openai_compatible {
                config
                    .base_url
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_URL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_BASE_URL", &file_env))
                    .unwrap_or_else(|| "https://api.example.invalid/v1".to_string())
            } else {
                config
                    .base_url
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_BIFROST_URL", &file_env))
                    .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                    .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
                    .ok_or_else(|| {
                        usage_error(
                            "OpenAI Chat Completions daemon provider requires provider.base_url, COOLDIS_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
                        )
                    })?
            }
            .trim_end_matches('/')
            .to_string();
            let api_key = if openai_compatible {
                config
                    .api_key
                    .clone()
                    .or_else(|| {
                        config
                            .api_key_env
                            .as_deref()
                            .and_then(|name| env_or_file(name, &file_env))
                    })
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .ok_or_else(|| {
                        usage_error(
                            "OpenAI Compatible daemon provider requires provider.api_key, provider.api_key_env, COOLDIS_OPENAI_COMPATIBLE_API_KEY, or OPENAI_COMPATIBLE_API_KEY",
                        )
                    })?
            } else {
                config
                    .api_key
                    .clone()
                    .or_else(|| {
                        config
                            .api_key_env
                            .as_deref()
                            .and_then(|name| env_or_file(name, &file_env))
                    })
                    .or_else(|| env_or_file("COOLDIS_BIFROST_KEY", &file_env))
                    .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                    .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                    .ok_or_else(|| {
                        usage_error(
                            "OpenAI Chat Completions daemon provider requires provider.api_key, provider.api_key_env, COOLDIS_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                        )
                    })?
            };
            let model = if openai_compatible {
                config
                    .model
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_OPENAI_COMPATIBLE_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env))
                    .unwrap_or_else(|| APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string())
            } else {
                config
                    .model
                    .clone()
                    .or_else(|| env_or_file("COOLDIS_BIFROST_OPENAI_CHAT_MODEL", &file_env))
                    .or_else(|| env_or_file("COOLDIS_BIFROST_OPENAI_MODEL", &file_env))
                    .unwrap_or_else(|| APP_SERVER_BIFROST_MODEL.to_string())
            };
            Ok(ChatProviderConfig::OpenAIChatCompletions {
                provider: chat_completions_provider_name(provider_name),
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
                headers: provider_default_headers(provider_name),
            })
        }
        other => Err(usage_error(format!(
            "unknown daemon provider {other:?}; expected local, bifrost_openai, openai_chat_completions, anthropic, anthropic_bedrock, or openai_compatible"
        ))),
    }
}

fn provider_is_openai_compatible(provider: &str) -> bool {
    matches!(
        provider,
        "openai_compatible"
            | "openai_compatible_openai"
            | "openai_compatible_chat"
            | "openai_compatible_serverless"
    )
}

fn chat_completions_provider_name(provider: &str) -> String {
    if provider_is_openai_compatible(provider) {
        APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string()
    } else {
        "openai_chat_completions".to_string()
    }
}

fn provider_default_headers(provider: &str) -> Vec<(String, String)> {
    if provider_is_openai_compatible(provider) {
        vec![("X-Example-Provider".to_string(), "required".to_string())]
    } else {
        Vec::new()
    }
}

fn load_chat_config_file(
    path: Option<&Path>,
) -> CooldisResult<(ChatConfigSection, Option<PathBuf>)> {
    let discovered;
    let path = if let Some(path) = path {
        path
    } else {
        discovered = PathBuf::from("cooldis.json");
        if !discovered.exists() {
            return Ok((ChatConfigSection::default(), None));
        }
        discovered.as_path()
    };
    let bytes = fs::read(path).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to read chat config {}: {err}",
            path.display()
        ))
    })?;
    let file: ChatConfigFile = serde_json::from_slice(&bytes).map_err(|err| {
        CooldisError::RuntimeFactory(format!(
            "failed to decode chat config {} as JSON: {err}",
            path.display()
        ))
    })?;
    let config = file.chat.unwrap_or(ChatConfigSection {
        provider: file.provider,
        base_url: file.base_url,
        api_key: file.api_key,
        api_key_env: file.api_key_env,
        region: file.region,
        aws_access_key_id: file.aws_access_key_id,
        aws_secret_access_key: file.aws_secret_access_key,
        aws_session_token: file.aws_session_token,
        model: file.model,
        max_tokens: file.max_tokens,
        stream: file.stream,
        env_file: file.env_file,
        // lexicon-allow: capsule - existing app-server operation binding API name
        capsule_bindings: file.capsule_bindings,
    });
    Ok((config, path.parent().map(|base| base.to_path_buf())))
}

fn read_env_file_if_exists(path: &Path) -> CooldisResult<BTreeMap<String, String>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }
    let text = fs::read_to_string(path).map_err(|err| {
        CooldisError::RuntimeFactory(format!("failed to read env file {}: {err}", path.display()))
    })?;
    Ok(parse_env_lines(&text))
}

fn parse_env_lines(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            Some((key.trim().to_string(), unquote_env_value(value.trim())))
        })
        .collect()
}

fn unquote_env_value(value: &str) -> String {
    if value.len() >= 2 {
        let bytes = value.as_bytes();
        if (bytes[0] == b'"' && bytes[value.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[value.len() - 1] == b'\'')
        {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}

fn env_or_file(name: &str, file_env: &BTreeMap<String, String>) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| file_env.get(name).cloned())
}

fn relativize_config_paths(config: &mut ToolConfigFile, base: &Path) {
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

fn resolve_config_path(base: &Path, path: PathBuf) -> PathBuf {
    if path.is_absolute() {
        path
    } else {
        base.join(path)
    }
}

async fn validate_wasm_artifact(
    artifact_path: PathBuf,
    capability_grants: BTreeSet<String>,
) -> CooldisResult<crate::WasmOperationManifest> {
    let factory = WasmRuntimeFactory::new(
        WasmRuntimeConfig::new(WasmRuntimeArtifact::path(artifact_path))
            .with_capability_grants(capability_grants),
    )?;
    factory.validate_operation_artifact().await
}

async fn load_vfs(mounts: Vec<MountArg>) -> CooldisResult<Arc<CooldisVfs>> {
    let vfs = Arc::new(CooldisVfs::new(Arc::new(InMemoryFs::new())));
    for mount in mounts {
        let fs = Arc::new(
            HostFileSystem::new(&mount.host_path, HostFileSystemMode::ReadOnly).map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to open host mount {}: {err}",
                    mount.host_path.display()
                ))
            })?,
        );
        vfs.mount(&mount.guest_path, fs as Arc<dyn crate::CooldisVfsBackend>)
            .map_err(|err| {
                CooldisError::RuntimeFactory(format!(
                    "failed to mount {} at {}: {err}",
                    mount.host_path.display(),
                    mount.guest_path.display()
                ))
            })?;
    }
    Ok(vfs)
}

struct PrivateAppServer {
    listen: AppServerListenAddr,
    root: PathBuf,
    task: JoinHandle<CooldisResult<()>>,
}

impl PrivateAppServer {
    async fn start(options: &ChatArgs) -> CooldisResult<Self> {
        let root = PathBuf::from("/tmp").join(format!("cdis-chat-{}", Uuid::now_v7().simple()));
        let listen = AppServerListenAddr::Unix(root.join("app-server.sock"));
        let provider = load_chat_provider_config(options)?;
        // lexicon-allow: capsule - existing app-server operation binding API name
        let capsule_bindings = load_chat_capsule_bindings_config(options)?;
        let mut config = CooldisAppServerConfig::local(listen.clone(), options.cwd.clone());
        config.runtime_home = options
            .runtime_home
            .clone()
            .unwrap_or_else(|| root.join("runtime"));
        config.state_home = options
            .state_home
            .clone()
            .unwrap_or_else(|| root.join("state"));
        // lexicon-allow: capsule - existing app-server operation binding API name
        config.capsule_bindings = capsule_bindings;
        apply_chat_provider_config(&mut config, provider);

        let server = CooldisAppServer::new_local(config).await?;
        let serve_listen = listen.clone();
        let task = tokio::spawn(async move { server.serve(serve_listen).await });
        wait_for_private_socket(socket_path(&listen)).await?;
        Ok(Self { listen, root, task })
    }

    fn socket_path(&self) -> &Path {
        socket_path(&self.listen)
    }

    fn shutdown(self) {}
}

impl Drop for PrivateAppServer {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn socket_path(listen: &AppServerListenAddr) -> &Path {
    match listen {
        AppServerListenAddr::Unix(path) => path.as_path(),
        AppServerListenAddr::WebSocket(_) => {
            unreachable!("private chat app-server always listens on a Unix socket")
        }
    }
}

async fn wait_for_private_socket(path: &Path) -> CooldisResult<()> {
    for _ in 0..100 {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(usage_error(format!(
        "timed out waiting for private app-server socket {}",
        path.display()
    )))
}

async fn manifest_receipt_event_ids(
    app: &CooldisAppServer,
    thread_id: &str,
) -> CooldisResult<(String, String)> {
    let response = app
        .local_json_rpc_request(
            "thread/events/list",
            json!({
                "threadId": thread_id,
                "kinds": [
                    EventKind::ManifestCompileCompleted.as_str(),
                    EventKind::ManifestBindCompleted.as_str(),
                ],
            }),
        )
        .await
        .map_err(|err| usage_error(format!("failed to read thread events: {err}")))?;
    let events = response["data"]
        .as_array()
        .ok_or_else(|| usage_error("thread/events/list response missing data array"))?;
    let compile = events
        .iter()
        .find(|event| event["kind"] == EventKind::ManifestCompileCompleted.as_str())
        .ok_or_else(|| usage_error("manifest.compile.completed receipt event was not found"))?;
    let bind = events
        .iter()
        .find(|event| event["kind"] == EventKind::ManifestBindCompleted.as_str())
        .ok_or_else(|| usage_error("manifest.bind.completed receipt event was not found"))?;
    let compile_id = compile["eventId"]
        .as_str()
        .ok_or_else(|| usage_error("manifest.compile.completed event missing eventId"))?;
    let bind_id = bind["eventId"]
        .as_str()
        .ok_or_else(|| usage_error("manifest.bind.completed event missing eventId"))?;
    Ok((compile_id.to_string(), bind_id.to_string()))
}

async fn run_local_app_turn(
    app: &CooldisAppServer,
    thread_id: &str,
    input: &str,
) -> CooldisResult<String> {
    let parsed = ThreadId::parse_str(thread_id)
        .map_err(|err| usage_error(format!("invalid thread id {thread_id:?}: {err}")))?;
    let handle = app.supervisor().get_thread(app.tenant_id(), parsed).await?;
    let mut events = handle.subscribe_events();
    app.local_json_rpc_request(
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": input, "text_elements": [] }],
        }),
    )
    .await?;
    let mut output = String::new();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(120), events.recv())
            .await
            .map_err(|_| usage_error(format!("timed out waiting for turn on {thread_id}")))?
            .map_err(|err| usage_error(format!("thread event stream closed: {err}")))?;
        match event {
            crate::ThreadEvent::Output { text, .. } => {
                output.push_str(&text);
            }
            crate::ThreadEvent::Runtime { event, .. } => match event.kind {
                crate::RuntimeEventKind::Terminal {
                    state: crate::RuntimeTerminalState::Completed,
                } => return Ok(output),
                crate::RuntimeEventKind::Terminal { state } => {
                    return Err(usage_error(format!(
                        "turn ended before completion: {state:?}"
                    )));
                }
                crate::RuntimeEventKind::Failed { message, .. } => {
                    return Err(usage_error(format!("turn failed: {message}")));
                }
                _ => {}
            },
            crate::ThreadEvent::Cancelled { reason, .. } => {
                return Err(usage_error(format!("turn cancelled: {reason}")));
            }
            crate::ThreadEvent::Stopped { .. } => {
                return Err(usage_error("thread stopped before turn completion"));
            }
            crate::ThreadEvent::Failed { message, .. } => {
                return Err(usage_error(format!("turn failed: {message}")));
            }
            _ => {}
        }
    }
}

fn notification_matches_thread_turn(
    notification: &JsonRpcNotification,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("threadId"))
        .and_then(Value::as_str)
        == Some(thread_id)
        && notification
            .params
            .as_ref()
            .and_then(|params| params.get("turnId"))
            .and_then(Value::as_str)
            == Some(turn_id)
}

fn notification_turn_id(notification: &JsonRpcNotification) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("turn"))
        .and_then(|turn| turn.get("id"))
        .and_then(Value::as_str)
}

fn notification_error_message(notification: &JsonRpcNotification) -> String {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .unwrap_or("unknown error")
        .to_string()
}

fn agent_registry_root(registry_root: Option<PathBuf>) -> PathBuf {
    registry_root.unwrap_or_else(|| PathBuf::from(".cooldis/agents"))
}

fn agent_operations_registry_root(registry_root: Option<PathBuf>) -> PathBuf {
    registry_root.unwrap_or_else(default_operations_registry_root)
}

fn skill_registry_root(registry_root: Option<PathBuf>) -> PathBuf {
    registry_root.unwrap_or_else(|| PathBuf::from(".cooldis/skills"))
}

fn is_agent_manifest_file_path(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("toml")
}

fn write_agent_manifest_file(name: &str, out_path: &Path, force: bool) -> CooldisResult<()> {
    if out_path.exists() && !force {
        return Err(usage_error(format!(
            "agent manifest {} already exists; pass --force to replace it",
            out_path.display()
        )));
    }
    if let Some(parent) = out_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent).map_err(io_error)?;
    }
    fs::write(out_path, render_agent_manifest_template(name)?).map_err(io_error)
}

fn write_agent_project(name: &str, root: &Path, force: bool) -> CooldisResult<()> {
    let manifest_path = root.join("cooldis.agent.toml");
    let system_prompt_path = root.join("prompts/system.md");
    let operation_refs_path = root.join("components/operations.toml");
    let coupling_templates_path = root.join("components/couplings.toml");
    let operation_slot_path = root.join("operations/README.md");
    let files = [
        manifest_path.as_path(),
        system_prompt_path.as_path(),
        operation_refs_path.as_path(),
        coupling_templates_path.as_path(),
        operation_slot_path.as_path(),
    ];
    if !force {
        for path in files {
            if path.exists() {
                return Err(usage_error(format!(
                    "agent project file {} already exists; pass --force to replace it",
                    path.display()
                )));
            }
        }
    }
    fs::create_dir_all(root.join("prompts")).map_err(io_error)?;
    fs::create_dir_all(root.join("components")).map_err(io_error)?;
    fs::create_dir_all(root.join("operations")).map_err(io_error)?;
    fs::write(&manifest_path, render_agent_manifest_template(name)?).map_err(io_error)?;
    fs::write(
        &system_prompt_path,
        render_agent_system_prompt_template(name)?,
    )
    .map_err(io_error)?;
    fs::write(
        &operation_refs_path,
        render_agent_operation_refs_template(name)?,
    )
    .map_err(io_error)?;
    fs::write(
        &coupling_templates_path,
        render_agent_coupling_templates_template(name)?,
    )
    .map_err(io_error)?;
    fs::write(
        &operation_slot_path,
        render_agent_operation_slot_template(name)?,
    )
    .map_err(io_error)
}

fn write_coupling_project(name: &str, root: &Path, force: bool) -> CooldisResult<()> {
    let package_name = crate::validate_record_name(name)?;
    let operation_name = coupling_operation_name(&package_name)?;
    let cargo_toml_path = root.join("Cargo.toml");
    let lib_path = root.join("src/lib.rs");
    let package_path = root.join("cooldis.tool.toml");
    let input_schema_path = root.join("schemas/coupling_invocation.input.json");
    let output_schema_path = root.join("schemas/coupling_discharge.output.json");
    let fixture_input_path = root.join("fixtures/invocation.json");
    let fixture_expect_path = root.join("fixtures/expect.discharge.json");
    let files = [
        cargo_toml_path.as_path(),
        lib_path.as_path(),
        package_path.as_path(),
        input_schema_path.as_path(),
        output_schema_path.as_path(),
        fixture_input_path.as_path(),
        fixture_expect_path.as_path(),
    ];
    if !force {
        for path in files {
            if path.exists() {
                return Err(usage_error(format!(
                    "coupling scaffold file {} already exists; pass --force to replace it",
                    path.display()
                )));
            }
        }
    }
    fs::create_dir_all(root.join("src")).map_err(io_error)?;
    fs::create_dir_all(root.join("schemas")).map_err(io_error)?;
    fs::create_dir_all(root.join("fixtures")).map_err(io_error)?;
    fs::write(&cargo_toml_path, render_coupling_cargo_toml(&package_name)?).map_err(io_error)?;
    fs::write(&lib_path, render_coupling_lib_rs(&operation_name)).map_err(io_error)?;
    fs::write(
        &package_path,
        render_coupling_tool_manifest(&package_name, &operation_name),
    )
    .map_err(io_error)?;
    fs::write(&input_schema_path, COUPLING_INVOCATION_SCHEMA).map_err(io_error)?;
    fs::write(&output_schema_path, COUPLING_DISCHARGE_SCHEMA).map_err(io_error)?;
    fs::write(
        &fixture_input_path,
        render_coupling_fixture_input(&package_name),
    )
    .map_err(io_error)?;
    fs::write(
        &fixture_expect_path,
        render_coupling_fixture_expect(&package_name),
    )
    .map_err(io_error)
}

fn render_agent_manifest_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Cooldis V1 folder-first agent manifest.\n\
# Prompt text lives in prompts/system.md. Add tools only after publishing\n\
# operation packages and replacing component refs with real op:// hashes.\n\
\n\
[agent]\n\
name = {name:?}\n\
version = \"0.1.0\"\n\
description = \"Describe what this agent is responsible for.\"\n\
kind = \"cooldis.agent-manifest\"\n\
schema_version = 1\n\
\n\
[[model_profiles]]\n\
id = \"default\"\n\
provider_ref = \"provider://local_offline\"\n\
model_ref = \"model://local_offline/echo\"\n\
\n\
[runtime]\n\
default_cwd = \".\"\n\
streaming = false\n\
"
    ))
}

fn render_agent_system_prompt_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "You are the {name} agent.\n\
\n\
Keep the user's goal explicit, call only declared operations, and surface the\n\
receipt or event evidence needed to resume or debug the run.\n"
    ))
}

fn render_agent_operation_refs_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Component refs for {name}.\n\
# V1 publication is component-first: publish operation packages, then publish\n\
# cooldis.agent.toml after replacing placeholder refs with real op:// hashes.\n\
\n\
[[operations]]\n\
name = \"example-tool\"\n\
source = \"../operations/example-tool\"\n\
operation_ref = \"op://example-tool@sha256:0000000000000000000000000000000000000000000000000000000000000000\"\n"
    ))
}

fn render_agent_coupling_templates_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    let mut out = format!(
        "# Coupling template catalog for {name}.\n\
# V1 couplings are declared as event-stream edges, not hidden callbacks.\n\
# Pick template ids here, then bind manifest coupling rows only after choosing\n\
# the published function_ref that implements the edge.\n"
    );
    for template in crate::coupling_template_catalog_v1().templates {
        out.push_str(&format!(
            "\n[[templates]]\n\
id = {:?}\n\
maturity = {:?}\n\
role = {:?}\n\
runtime_executable = {}\n\
must_have = {}\n\
channel_decision_required = {}\n\
summary = {:?}\n",
            template.id,
            coupling_template_maturity_toml_label(template.maturity),
            coupling_template_role_toml_label(template.role),
            template.runtime_executable,
            template.must_have,
            template.channel_decision_required,
            template.summary,
        ));
    }
    Ok(out)
}

fn coupling_template_maturity_toml_label(
    maturity: crate::CouplingTemplateMaturity,
) -> &'static str {
    match maturity {
        crate::CouplingTemplateMaturity::KernelBacked => "kernel_backed",
        crate::CouplingTemplateMaturity::InterfaceOnly => "interface_only",
        crate::CouplingTemplateMaturity::ReferenceOnly => "reference_only",
    }
}

fn coupling_template_role_toml_label(role: crate::CouplingRole) -> &'static str {
    match role {
        crate::CouplingRole::Projection => "projection",
        crate::CouplingRole::Controller => "controller",
    }
}

fn render_agent_operation_slot_template(name: &str) -> CooldisResult<String> {
    crate::validate_record_name(name)?;
    Ok(format!(
        "# Local operations for {name}\n\
\n\
Put custom operation packages under this directory. Each package should own a\n\
cooldis.tool.toml, schemas, fixtures, and source artifact. Publish operations\n\
before publishing cooldis.agent.toml.\n"
    ))
}

fn render_coupling_cargo_toml(name: &str) -> CooldisResult<String> {
    let crate_name = coupling_crate_name(name)?;
    let sdk_path = guest_sdk_dependency_path();
    Ok(format!(
        "[package]\n\
name = {}\n\
version = \"0.1.0\"\n\
edition = \"2024\"\n\
publish = false\n\
\n\
[workspace]\n\
\n\
[lib]\n\
crate-type = [\"cdylib\"]\n\
\n\
[dependencies]\n\
cooldis-guest-sdk = {{ path = {} }}\n\
serde = {{ version = \"1\", features = [\"derive\"] }}\n\
serde_json = \"1\"\n\
\n\
[profile.release]\n\
panic = \"abort\"\n",
        toml_string(&crate_name),
        toml_string(&sdk_path.display().to_string()),
    ))
}

fn render_coupling_lib_rs(operation_name: &str) -> String {
    COUPLING_LIB_RS_TEMPLATE.replace("__OPERATION_NAME__", operation_name)
}

fn render_coupling_tool_manifest(name: &str, operation_name: &str) -> String {
    format!(
        "kind = \"cooldis.tool\"\n\
schema_version = 0\n\
\n\
[identity]\n\
name = {}\n\
version = \"0.1.0\"\n\
description = \"Fold coupling invocation events into deterministic derived events.\"\n\
\n\
[runtime]\n\
kind = \"wasm32-unknown-unknown\"\n\
module_path = \".\"\n\
state = \"stateless\"\n\
release = true\n\
max_input_bytes = 262144\n\
max_output_bytes = 262144\n\
\n\
[[operations]]\n\
name = {}\n\
description = \"Emit one placement decision every configured number of selected events.\"\n\
input_schema = \"schemas/coupling_invocation.input.json\"\n\
output_schema = \"schemas/coupling_discharge.output.json\"\n\
required_capabilities = []\n\
\n\
[operations.command]\n\
name = {}\n\
stdin = \"json\"\n\
stdout = \"json\"\n\
\n\
[operations.mcp]\n\
tool_name = {}\n\
\n\
[[fixtures]]\n\
name = \"three_events\"\n\
operation = {}\n\
input = \"fixtures/invocation.json\"\n\
expect = \"fixtures/expect.discharge.json\"\n",
        toml_string(name),
        toml_string(operation_name),
        toml_string(&format!("{name} {operation_name}")),
        toml_string(operation_name),
        toml_string(operation_name),
    )
}

fn render_coupling_fixture_input(name: &str) -> String {
    format!(
        "{{\n\
  \"abi\": \"cooldis.coupling.invocation/0.1\",\n\
  \"trigger_event\": {{\n\
    \"id\": \"event-3\",\n\
    \"stream_id\": \"thread:session\",\n\
    \"sequence\": 3,\n\
    \"kind\": \"turn.completed\",\n\
    \"origin\": \"witnessed\",\n\
    \"payload\": {{}}\n\
  }},\n\
  \"selected_events\": [\n\
    {{\"id\": \"event-1\", \"stream_id\": \"thread:session\", \"sequence\": 1, \"kind\": \"turn.completed\", \"origin\": \"witnessed\", \"payload\": {{}}}},\n\
    {{\"id\": \"event-2\", \"stream_id\": \"thread:session\", \"sequence\": 2, \"kind\": \"turn.completed\", \"origin\": \"witnessed\", \"payload\": {{}}}},\n\
    {{\"id\": \"event-3\", \"stream_id\": \"thread:session\", \"sequence\": 3, \"kind\": \"turn.completed\", \"origin\": \"witnessed\", \"payload\": {{}}}}\n\
  ],\n\
  \"config\": {{\n\
    \"every\": 3,\n\
    \"sink_stream\": \"derived:counter\",\n\
    \"sink_kind\": \"placement.decision\"\n\
  }},\n\
  \"invocation_meta\": {{\n\
    \"coupling_id\": {},\n\
    \"thread_id\": \"session\",\n\
    \"depth\": 0\n\
  }}\n\
}}\n",
        toml_string(name),
    )
}

fn render_coupling_fixture_expect(name: &str) -> String {
    format!(
        "{{\n\
  \"abi\": \"cooldis.coupling.discharge/0.1\",\n\
  \"events\": [\n\
    {{\n\
      \"stream\": \"derived:counter\",\n\
      \"kind\": \"placement.decision\",\n\
      \"payload\": {{\n\
        \"schema\": \"cooldis.scaffold.counter_fold/1\",\n\
        \"count\": 3,\n\
        \"trigger_event_id\": \"event-3\",\n\
        \"coupling_id\": {}\n\
      }}\n\
    }}\n\
  ]\n\
}}\n",
        toml_string(name),
    )
}

fn coupling_operation_name(name: &str) -> CooldisResult<String> {
    let mut operation = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' => operation.push((byte as char).to_ascii_lowercase()),
            b'a'..=b'z' | b'0'..=b'9' | b'_' => operation.push(byte as char),
            b'-' | b'.' => operation.push('_'),
            _ => {}
        }
    }
    if operation
        .as_bytes()
        .first()
        .is_none_or(|byte| !byte.is_ascii_alphabetic() && *byte != b'_')
    {
        operation.insert_str(0, "coupling_");
    }
    crate::validate_record_name(&operation)?;
    Ok(operation)
}

fn coupling_crate_name(name: &str) -> CooldisResult<String> {
    let mut suffix = String::new();
    for byte in name.bytes() {
        match byte {
            b'A'..=b'Z' => suffix.push((byte as char).to_ascii_lowercase()),
            b'a'..=b'z' | b'0'..=b'9' => suffix.push(byte as char),
            b'-' | b'_' | b'.' => suffix.push('-'),
            _ => {}
        }
    }
    let suffix = suffix.trim_matches('-');
    if suffix.is_empty() {
        return Err(usage_error(
            "coupling name does not produce a Cargo package name",
        ));
    }
    Ok(format!("cooldis-coupling-{suffix}"))
}

fn guest_sdk_dependency_path() -> PathBuf {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../cooldis-guest-sdk");
    path.canonicalize().unwrap_or(path)
}

fn toml_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

fn print_agent_record_json(record: &PublishedAgentRecord) -> CooldisResult<()> {
    let json = serde_json::to_string_pretty(record)
        .map_err(|err| usage_error(format!("failed to encode agent record: {err}")))?;
    println!("{json}");
    Ok(())
}

fn agent_context_source_lines(manifest: &Value) -> Vec<String> {
    let resources = manifest
        .get("resources")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|resource| {
            let name = resource.get("name").and_then(Value::as_str)?;
            let ref_uri = resource
                .get("ref")
                .or_else(|| resource.get("reference"))
                .and_then(Value::as_str)?;
            Some((name, ref_uri))
        })
        .collect::<BTreeMap<_, _>>();
    manifest
        .get("context")
        .and_then(|context| context.get("sources"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|source| {
            source.get("assembler").and_then(Value::as_str) == Some("kernel://assembler/static")
        })
        .filter_map(|source| {
            let id = source.get("id").and_then(Value::as_str)?;
            let input = source.get("input").and_then(Value::as_str)?;
            let ref_uri = resources.get(input).copied().unwrap_or(input);
            let content_hash = content_hash_label_from_ref(ref_uri)?;
            Some(format!(
                "context_source: {id} -> {ref_uri} ({content_hash})"
            ))
        })
        .collect()
}

fn content_hash_label_from_ref(ref_uri: &str) -> Option<String> {
    if let Some(hash) = ref_uri.strip_prefix("resource://artifact/sha256:")
        && hash.len() == 64
    {
        return Some(format!("sha256:{hash}"));
    }
    let (_prefix, hash) = ref_uri.rsplit_once("@sha256:")?;
    (hash.len() == 64).then(|| format!("sha256:{hash}"))
}

fn flush_stdout() -> CooldisResult<()> {
    std::io::stdout()
        .flush()
        .map_err(|err| usage_error(format!("failed to flush stdout: {err}")))
}

fn usage_error(message: impl Into<String>) -> CooldisError {
    CooldisError::RuntimeFactory(message.into())
}

fn io_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::RuntimeFactory(err.to_string())
}

const COUPLING_LIB_RS_TEMPLATE: &str = r#"use cooldis_guest_sdk::prelude::*;
use serde_json::json;

#[derive(Deserialize)]
struct CouplingConfig {
    #[serde(default = "default_every")]
    every: u64,
    #[serde(default = "default_sink_stream")]
    sink_stream: String,
    #[serde(default = "default_sink_kind")]
    sink_kind: String,
}

#[coupling]
pub fn __OPERATION_NAME__(ctx: CouplingContext) -> Result<Discharge, GuestError> {
    let config: CouplingConfig = ctx.config()?;
    let every = config.every.max(1);
    let count = ctx.sources().len() as u64;
    if count == 0 || count % every != 0 {
        return Ok(Discharge::empty());
    }
    Discharge::empty().event(
        config.sink_stream,
        config.sink_kind,
        json!({
            "schema": "cooldis.scaffold.counter_fold/1",
            "count": count,
            "trigger_event_id": ctx.trigger().id.clone(),
            "coupling_id": ctx.meta().coupling_id.clone(),
        }),
    )
}

fn default_every() -> u64 {
    3
}

fn default_sink_stream() -> String {
    "derived:counter".to_string()
}

fn default_sink_kind() -> String {
    "placement.decision".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cooldis_guest_sdk::testkit;

    #[test]
    fn fixture_runs_natively_without_wasm_host() {
        let invocation =
            testkit::invocation_from_fixture_file("fixtures/invocation.json").unwrap();
        let discharge = testkit::invoke_coupling(__OPERATION_NAME__, invocation).unwrap();
        assert_eq!(discharge.events.len(), 1);
        assert_eq!(discharge.events[0].stream, "derived:counter");
        assert_eq!(discharge.events[0].kind, "placement.decision");
        assert_eq!(discharge.events[0].payload["count"], 3);
    }
}
"#;

const COUPLING_INVOCATION_SCHEMA: &str = r#"{
  "type": "object",
  "required": ["abi", "trigger_event", "invocation_meta"],
  "properties": {
    "abi": { "enum": ["cooldis.coupling.invocation/0.1"] },
    "trigger_event": { "type": "object", "additionalProperties": true },
    "selected_events": {
      "type": "array",
      "items": { "type": "object", "additionalProperties": true }
    },
    "config": { "type": "object", "additionalProperties": true },
    "invocation_meta": { "type": "object", "additionalProperties": true }
  },
  "additionalProperties": true
}
"#;

const COUPLING_DISCHARGE_SCHEMA: &str = r#"{
  "type": "object",
  "required": ["abi", "events"],
  "properties": {
    "abi": { "enum": ["cooldis.coupling.discharge/0.1"] },
    "events": {
      "type": "array",
      "items": { "type": "object", "additionalProperties": true }
    }
  },
  "additionalProperties": true
}
"#;

const ROOT_HELP: &str = "cooldis

Usage:
  cooldis <command> [args]
  cooldis help [COMMAND...]
  cooldis commands

Start here:
  cooldis console
  cooldis chat [PROMPT]
  cooldis init <name>

Explore:
  cooldis commands
  cooldis help <command>
  cooldis <command> --help
  man cooldis
";

const CANONICAL_COMMANDS: &[&str] = &[
    "cooldis",
    "cooldis commands",
    "cooldis help [COMMAND...]",
    "cooldis init <name> [--out <dir|manifest.toml>] [--force]",
    "cooldis console [--no-open] [--cwd <path>] [--config <cooldis.toml>] [--port <port>]",
    "cooldis chat [PROMPT] [--config <file>] [--cwd <path>] [--attach <unix://path|ws://host:port[/rpc]>]",
    "cooldis auth status <provider-id> [--state-home ~/.cooldis/state]",
    "cooldis auth set <provider-id> --api-key-stdin [--state-home ~/.cooldis/state]",
    "cooldis auth delete <provider-id> [--state-home ~/.cooldis/state]",
    "cooldis secret import <name> --from-env <ENV> [--state-home ~/.cooldis/state]",
    "cooldis secret set <name> --value-stdin [--state-home ~/.cooldis/state]",
    "cooldis secret list [--state-home ~/.cooldis/state]",
    "cooldis secret status <name> [--state-home ~/.cooldis/state]",
    "cooldis secret delete <name> [--state-home ~/.cooldis/state]",
    "cooldis agent init <name> [--out <dir|manifest.toml>] [--force]",
    "cooldis coupling init <name> [--out <dir>] [--force]",
    "cooldis agent plan <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]",
    "cooldis agent publish <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]",
    "cooldis agent list [--registry-root .cooldis/agents]",
    "cooldis agent show <agent-ref-or-name> [--registry-root .cooldis/agents]",
    "cooldis agent run <agent-ref> --input <text> [--registry-root .cooldis/agents]",
    "cooldis blob publish <file> [--registry-root .cooldis/blobs] [--name <name>]",
    "cooldis import build --package cooldis.import.toml",
    "cooldis import publish --package cooldis.import.toml [--registry-root .cooldis/operations]",
    "cooldis coupling run --replay --artifact <path|op://ref> --coupling-file <file> (--thread-id <id> --journal <db>|--export <bundle>) [--coupling-id <id>] [--registry-root .cooldis/operations] [--json]",
    "cooldis tool build --package cooldis.tool.toml",
    "cooldis tool build --module-path <dir|Cargo.toml> [--name <name>] [--config cooldis.json]",
    "cooldis tool list [--registry-root .cooldis/operations]",
    "cooldis tool publish --package cooldis.tool.toml [--registry-root .cooldis/operations]",
    "cooldis tool run --module-path <dir|Cargo.toml> <operation> --input <text> [--mount /guest=/host]",
    "cooldis tool run --bin-path <module.wasm> <operation> --input <text> [--mount /guest=/host]",
    "cooldis tool run <published-name> <operation> --input <text> [--registry-root .cooldis/operations] [--state-home .cooldis/state]",
    "cooldis tool manual <published-name> [operation] [--json] [--registry-root .cooldis/operations]",
    "cooldis skill publish <dir> [--registry-root .cooldis/skills] [--name <package>]",
    "cooldis skill import <dir> [--registry-root .cooldis/skills] [--blob-registry-root .cooldis/blobs] [--name <package>] [--dry-run]",
    "cooldis tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>] [--include-tool <tool>] [--state-home .cooldis/state]",
    "cooldis tool source discover <name> [--state-home .cooldis/state]",
    "cooldis tool source list [--json] [--state-home .cooldis/state]",
    "cooldis tool source show <name> [--json] [--state-home .cooldis/state]",
    "cooldis tool source remove <name> [--state-home .cooldis/state]",
    "cooldis rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--cwd <path>]",
    "cooldis debug rpc call <method> [PARAMS_JSON] [--url <ws-url> | --config <cooldis.toml>]",
    "cooldis debug rpc turn (--thread <id> | --new) [--json] <text> [--url <ws-url> | --config <cooldis.toml>]",
    "cooldis debug rpc tail --thread <id> [--url <ws-url> | --config <cooldis.toml>]",
    "cooldis daemon run [--config cooldis.toml]",
    "cooldis daemon config validate [--config cooldis.toml]",
    "cooldis daemon service print [--target launchd|systemd] --config cooldis.toml [--label com.cooldis.daemon]",
    "cooldis daemon service install [--target launchd|systemd] --config cooldis.toml [--label com.cooldis.daemon]",
    "cooldis daemon service uninstall [--target launchd|systemd] [--label com.cooldis.daemon]",
];

fn print_help() {
    print!("{ROOT_HELP}");
}

fn print_help_help() {
    println!(
        "cooldis help\n\
\n\
Usage:\n\
  cooldis help [COMMAND...]\n\
\n\
Prints root help or the help page for a canonical Cooldis command path.\n"
    );
}

fn print_commands_help() {
    println!("cooldis commands\n");
    println!("Usage:");
    println!("  cooldis commands");
    println!();
    print_command_group("Commands:", CANONICAL_COMMANDS);
}

fn print_command_group(title: &str, commands: &[&str]) {
    println!("{title}");
    for command in commands {
        println!("  {command}");
    }
}

fn print_agent_help() {
    println!(
        "cooldis agent\n\
\n\
Usage:\n\
  cooldis agent init <name> [--out <dir|manifest.toml>]\n\
  cooldis agent plan <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
  cooldis agent publish <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
  cooldis agent list [--registry-root .cooldis/agents]\n\
  cooldis agent show <agent-ref-or-name> [--registry-root .cooldis/agents]\n\
  cooldis agent run <agent-ref> --input <text> [--registry-root .cooldis/agents]\n\
\n\
Agents are declarative runtime artifacts. `plan` resolves the manifest and\n\
writes nothing; `publish` reruns the plan and writes an immutable local record.\n"
    );
}

fn print_coupling_help() {
    println!(
        "cooldis coupling\n\
\n\
Usage:\n\
  cooldis coupling init <name> [--out <dir>] [--force]\n\
  cooldis coupling run --replay --artifact <path|op://ref> --coupling-file <file> (--thread-id <id> --journal <db>|--export <bundle>) [--coupling-id <id>] [--registry-root .cooldis/operations] [--json]\n\
\n\
Couplings are event-stream edges. `init` scaffolds a Rust Wasm coupling\n\
package that uses #[coupling] and validates through `cooldis tool build\n\
--package`. `run --replay` runs a coupling artifact against a recorded\n\
thread in dry-run mode and prints proposed discharges without appending to\n\
the source journal.\n"
    );
}

fn print_coupling_init_help() {
    println!(
        "cooldis coupling init\n\
\n\
Usage:\n\
  cooldis coupling init <name> [--out <dir>] [--force]\n\
\n\
Writes a macro-authored coupling crate with cooldis.tool.toml, schemas,\n\
fixtures, and one native testkit test.\n"
    );
}

fn print_agent_init_help() {
    println!(
        "cooldis agent init\n\
\n\
Usage:\n\
  cooldis init <name> [--out <dir|manifest.toml>] [--force]\n\
  cooldis agent init <name> [--out <dir|manifest.toml>] [--force]\n\
\n\
Writes a folder-first Cooldis agent project by default. Use --out path.toml for\n\
the legacy single-manifest file form.\n"
    );
}

fn print_agent_plan_help() {
    println!(
        "cooldis agent plan\n\
\n\
Usage:\n\
  cooldis agent plan <manifest> [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
\n\
Validates and resolves an agent manifest, previews the publish record, and\n\
does not write an agent record. Folder-first prompts are lowered through the\n\
idempotent blob registry so the preview matches publish. When an operations\n\
registry is present, op:// refs are verified against it; otherwise they are\n\
reported unverified-offline.\n"
    );
}

fn print_agent_publish_help() {
    println!(
        "cooldis agent publish\n\
\n\
Usage:\n\
  cooldis agent publish <manifest> [--resolve-ops] [--registry-root .cooldis/agents] [--operations-registry-root .cooldis/operations]\n\
\n\
Reruns the agent plan and writes an immutable published agent record. Every\n\
op:// tool ref must exist in the operations registry and its row grants must\n\
cover the selected operation requirements. --resolve-ops rewrites op://name\n\
or op://name@latest authoring refs in the manifest file to pinned\n\
op://name@sha256:<hash> refs before publish verification runs.\n"
    );
}

fn print_agent_list_help() {
    println!(
        "cooldis agent list\n\
\n\
Usage:\n\
  cooldis agent list [--registry-root .cooldis/agents]\n\
\n\
Lists published agent records in the local registry.\n"
    );
}

fn print_agent_show_help() {
    println!(
        "cooldis agent show\n\
\n\
Usage:\n\
  cooldis agent show <agent-ref-or-name> [--registry-root .cooldis/agents]\n\
\n\
Prints the published agent record as JSON.\n"
    );
}

fn print_agent_run_help() {
    println!(
        "cooldis agent run\n\
\n\
Usage:\n\
  cooldis agent run <agent-ref> --input <text> [--registry-root .cooldis/agents]\n\
\n\
Starts a manifest-backed app-server thread, runs one turn, prints the assistant\n\
output, then prints the manifest compile and bind receipt event ids.\n"
    );
}

fn print_tool_help() {
    println!(
        "cooldis tool\n\
\n\
Usage:\n\
  cooldis tool build --package cooldis.tool.toml\n\
  cooldis tool build --module-path <dir|Cargo.toml> [--name <name>] [--config cooldis.json]\n\
  cooldis tool list [--registry-root .cooldis/operations]\n\
  cooldis tool publish --package cooldis.tool.toml [--registry-root .cooldis/operations]\n\
  cooldis tool run --module-path <dir|Cargo.toml> <operation> --input <text> [--mount /guest=/host]\n\
  cooldis tool run --bin-path <module.wasm> <operation> --input <text> [--mount /guest=/host]\n\
  cooldis tool run <published-name> <operation> --input <text> [--registry-root .cooldis/operations] [--state-home .cooldis/state]\n\
  cooldis tool manual <published-name> [operation] [--json] [--registry-root .cooldis/operations]\n\
  cooldis tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>]\n\
  cooldis tool source discover <name> [--state-home .cooldis/state]\n\
  cooldis tool source list [--json] [--state-home .cooldis/state]\n\
  cooldis tool source show <name> [--json] [--state-home .cooldis/state]\n\
  cooldis tool source remove <name> [--state-home .cooldis/state]\n\
\n\
Tools are the public capability surface. A published tool may contain one or\n\
more ABI operations, and Cooldis can project those operations as model tools,\n\
virtual-bash commands, HTTP routes, MCP exports, or other runtime surfaces.\n"
    );
}

fn print_import_help() {
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

fn print_skill_help() {
    println!(
        "cooldis skill\n\
\n\
Usage:\n\
  cooldis skill publish <dir> [--registry-root .cooldis/skills] [--name <package>]\n\
  cooldis skill import <dir> [--registry-root .cooldis/skills] [--blob-registry-root .cooldis/blobs] [--name <package>] [--dry-run]\n\
\n\
Skills are markdown context resources. Publishing turns a directory of\n\
<name>/SKILL.md files into one content-addressed skill:// package for agent\n\
manifest resource rows. Import compiles one ecosystem SKILL.md directory into\n\
the same package and blob registries.\n"
    );
}

fn print_blob_help() {
    println!(
        "cooldis blob\n\
\n\
Usage:\n\
  cooldis blob publish <file> [--registry-root .cooldis/blobs] [--name <name>]\n\
\n\
Blobs are immutable text or binary artifacts addressable as\n\
resource://artifact/sha256:<hash>. Agent manifests use blob resources for\n\
folder-first prompts and other static context inputs.\n"
    );
}

fn print_blob_publish_help() {
    println!(
        "cooldis blob publish\n\
\n\
Usage:\n\
  cooldis blob publish <file> [--registry-root .cooldis/blobs] [--name <name>]\n\
\n\
Publishes a file as a content-addressed blob artifact and prints the immutable\n\
resource://artifact/sha256:<hash> ref for manifest resource rows.\n"
    );
}

fn print_coupling_run_help() {
    println!(
        "cooldis coupling run\n\
\n\
Usage:\n\
  cooldis coupling run --replay --artifact <path|op://ref> --coupling-file <file> (--thread-id <id> --journal <db>|--export <bundle>) [--coupling-id <id>] [--registry-root .cooldis/operations] [--json]\n\
\n\
Replays recorded thread events through the bound coupling trigger, selector,\n\
quota, budget, and Wasm execution path. Output is proposals only; no source\n\
journal stream is appended. --json emits stable machine-readable replay\n\
receipts for tests and scripts.\n"
    );
}

fn print_skill_publish_help() {
    println!(
        "cooldis skill publish\n\
\n\
Usage:\n\
  cooldis skill publish <dir> [--registry-root .cooldis/skills] [--name <package>]\n\
\n\
Publishes a deterministic skill package from <dir>/<skill>/SKILL.md files.\n\
Optional frontmatter may declare name, description, and trigger_hint; without\n\
frontmatter, the skill name is the directory name and the description is the\n\
first non-heading markdown line. Prints both the pinned content-addressed ref\n\
and the floating package-name ref.\n"
    );
}

fn print_skill_import_help() {
    println!(
        "cooldis skill import\n\
\n\
Usage:\n\
  cooldis skill import <dir> [--registry-root .cooldis/skills] [--blob-registry-root .cooldis/blobs] [--name <package>] [--dry-run]\n\
\n\
Compiles one conventional SKILL.md directory into an ordinary published skill\n\
package. Markdown files directly under references/ are appended to the skill\n\
body and assets/ files publish as immutable blobs. Scripts are not converted;\n\
their paths are written into the body and model-visible index as degradation.\n\
Hook and MCP configuration remains inert and is reported as ignored. Files\n\
outside those classes are skipped and reported. Prints\n\
pinned and floating skill refs, blob refs, and ready-to-paste [[resources]]\n\
rows. --dry-run prints the same deterministic plan without registry writes.\n"
    );
}

fn print_tool_source_help() {
    println!(
        "cooldis tool source\n\
\n\
Usage:\n\
  cooldis tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>] [--include-tool <tool>] [--state-home .cooldis/state]\n\
  cooldis tool source discover <name> [--state-home .cooldis/state]\n\
  cooldis tool source list [--json] [--state-home .cooldis/state]\n\
  cooldis tool source show <name> [--json] [--state-home .cooldis/state]\n\
  cooldis tool source remove <name> [--state-home .cooldis/state]\n\
\n\
Registers remote MCP servers as Cooldis tool sources. MCP is imported through\n\
the tool boundary; source records store URLs, filters, and secret refs, not raw\n\
secret values.\n\
\n\
Tip: use this when Cooldis should use someone else's MCP server. To let an\n\
external MCP client use Cooldis, run the local Cooldis MCP stdio adapter.\n"
    );
}

fn print_tool_source_add_help() {
    println!(
        "cooldis tool source add\n\
\n\
Usage:\n\
  cooldis tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>] [--header name=value] [--include-tool <tool>] [--state-home .cooldis/state]\n\
\n\
Adds or updates a remote MCP tool source without discovering tools yet.\n"
    );
}

fn print_tool_source_discover_help() {
    println!(
        "cooldis tool source discover\n\
\n\
Usage:\n\
  cooldis tool source discover <name> [--state-home .cooldis/state]\n\
\n\
Connects to the remote MCP source, imports tools/list, and stores the discovered\n\
tool definitions in the local metadata DB.\n"
    );
}

fn print_tool_source_list_help() {
    println!(
        "cooldis tool source list\n\
\n\
Usage:\n\
  cooldis tool source list [--json] [--state-home .cooldis/state]\n\
\n\
Lists registered remote MCP tool sources with redacted auth metadata.\n"
    );
}

fn print_tool_source_show_help() {
    println!(
        "cooldis tool source show\n\
\n\
Usage:\n\
  cooldis tool source show <name> [--json] [--state-home .cooldis/state]\n\
\n\
Shows one remote MCP source and its latest discovered tool snapshot.\n"
    );
}

fn print_tool_source_remove_help() {
    println!(
        "cooldis tool source remove\n\
\n\
Usage:\n\
  cooldis tool source remove <name> [--state-home .cooldis/state]\n\
\n\
Removes a remote MCP source record from the local metadata DB.\n"
    );
}

fn print_tool_build_help() {
    println!(
        "cooldis tool build\n\
\n\
Usage:\n\
  cooldis tool build --package cooldis.tool.toml\n\
  cooldis tool build --module-path <dir|Cargo.toml> [--name <name>] [--config cooldis.json]\n\
  cooldis tool build --module-path <dir|Cargo.toml> --upstream-url <url> --upstream-rev <rev> --upstream-crate <crate>\n\
\n\
Builds a publishable Cooldis tool package or source module: compile or load the\n\
artifact, validate the Cooldis ABI, validate the declared interface, run\n\
fixtures when present, print a build receipt, and write nothing to the registry.\n"
    );
}

fn print_import_build_help() {
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

fn print_import_publish_help() {
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

fn print_tool_list_help() {
    println!(
        "cooldis tool list\n\
\n\
Usage:\n\
  cooldis tool list [--registry-root .cooldis/operations]\n\
\n\
Lists published operation records and their active artifact hashes.\n"
    );
}

fn print_tool_publish_help() {
    println!(
        "cooldis tool publish\n\
\n\
Usage:\n\
  cooldis tool publish --package cooldis.tool.toml [--registry-root .cooldis/operations]\n\
\n\
Publishes a package-validated Wasm tool artifact into the local operation\n\
registry. The package proof gate validates the declared interface and fixtures\n\
before the published tool can become visible through bindings and grants.\n"
    );
}

fn print_tool_run_help() {
    println!(
        "cooldis tool run\n\
\n\
Usage:\n\
  cooldis tool run --module-path <dir|Cargo.toml> <operation> --input <text> [--mount /guest=/host]\n\
  cooldis tool run --bin-path <module.wasm> <operation> --input <text> [--mount /guest=/host]\n\
  cooldis tool run <published-name> <operation> --input <text> [--registry-root .cooldis/operations] [--state-home .cooldis/state]\n\
\n\
Runs an operation from source, a Wasm artifact, or a published tool record.\n"
    );
}

fn print_tool_manual_help() {
    println!(
        "cooldis tool manual\n\
\n\
Usage:\n\
  cooldis tool manual <published-name> [operation] [--json] [--registry-root .cooldis/operations]\n\
\n\
Shows the caller-facing contract for a published tool operation. This is the\n\
manual surface agents should read before invoking a tool; implementation details such\n\
as source paths, transports, and secret refs belong in tool source/show output.\n"
    );
}

fn print_secret_help() {
    println!(
        "cooldis secret\n\
\n\
Usage:\n\
  cooldis secret import <name> --from-env <ENV> [--state-home ~/.cooldis/state]\n\
  cooldis secret set <name> --value-stdin [--state-home ~/.cooldis/state]\n\
  cooldis secret list [--state-home ~/.cooldis/state]\n\
  cooldis secret status <name> [--state-home ~/.cooldis/state]\n\
  cooldis secret delete <name> [--state-home ~/.cooldis/state]\n\
\n\
Stores local secret refs for host-mediated tool calls. List and status output\n\
redact values; tool runtimes receive only manifest-declared secret names.\n"
    );
}

fn print_secret_import_help() {
    println!(
        "cooldis secret import\n\
\n\
Usage:\n\
  cooldis secret import <name> --from-env <ENV> [--state-home ~/.cooldis/state]\n\
\n\
Imports a local environment variable into the Cooldis secret store under a\n\
stable secret name such as EXAMPLE_API_KEY.\n"
    );
}

fn print_secret_set_help() {
    println!(
        "cooldis secret set\n\
\n\
Usage:\n\
  cooldis secret set <name> --value-stdin [--state-home ~/.cooldis/state]\n\
\n\
Stores a secret value read from stdin. The stored value is never printed.\n"
    );
}

fn print_secret_list_help() {
    println!(
        "cooldis secret list\n\
\n\
Usage:\n\
  cooldis secret list [--state-home ~/.cooldis/state]\n\
\n\
Lists configured secret refs without printing secret values.\n"
    );
}

fn print_secret_status_help() {
    println!(
        "cooldis secret status\n\
\n\
Usage:\n\
  cooldis secret status <name> [--state-home ~/.cooldis/state]\n\
\n\
Prints redacted metadata for one secret ref.\n"
    );
}

fn print_secret_delete_help() {
    println!(
        "cooldis secret delete\n\
\n\
Usage:\n\
  cooldis secret delete <name> [--state-home ~/.cooldis/state]\n\
\n\
Deletes a local secret ref.\n"
    );
}

fn print_auth_help() {
    println!(
        "cooldis auth\n\
\n\
Usage:\n\
  cooldis auth status <provider-id> [--state-home ~/.cooldis/state]\n\
  cooldis auth set <provider-id> --api-key-stdin [--state-home ~/.cooldis/state]\n\
  cooldis auth delete <provider-id> [--state-home ~/.cooldis/state]\n\
\n\
Manages model-provider credentials in the local metadata store. Values are read\n\
from stdin and never printed.\n"
    );
}

fn print_auth_status_help() {
    println!(
        "cooldis auth status\n\
\n\
Usage:\n\
  cooldis auth status <provider-id> [--state-home ~/.cooldis/state]\n\
\n\
Prints redacted model-provider credential status.\n"
    );
}

fn print_auth_set_help() {
    println!(
        "cooldis auth set\n\
\n\
Usage:\n\
  cooldis auth set <provider-id> --api-key-stdin [--state-home ~/.cooldis/state]\n\
\n\
Stores a model-provider API key read from stdin. The stored value is never printed.\n"
    );
}

fn print_auth_delete_help() {
    println!(
        "cooldis auth delete\n\
\n\
Usage:\n\
  cooldis auth delete <provider-id> [--state-home ~/.cooldis/state]\n\
\n\
Deletes a stored model-provider credential.\n"
    );
}

fn print_rpc_help() {
    println!(
        "cooldis rpc\n\
\n\
Usage:\n\
  cooldis rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--cwd <path>]\n\
\n\
Starts the Cooldis control-plane RPC endpoint. This is the public entrypoint for\n\
remote operation when Cooldis is running in a sandbox, daemon, or managed host.\n"
    );
}

fn print_console_help() {
    println!(
        "cooldis console\n\
\n\
Usage:\n\
  cooldis console [--no-open] [--cwd <path>] [--config <cooldis.toml>] [--port <port>]\n\
\n\
Starts the bundled local browser console on 127.0.0.1. The command serves the\n\
console UI and the /rpc WebSocket endpoint from one loopback listener, prints\n\
the UI and RPC URLs, and opens the browser unless --no-open is set.\n"
    );
}

fn print_debug_help() {
    println!(
        "cooldis debug\n\
\n\
Usage:\n\
  cooldis debug rpc (call|turn|tail) ...   debug client for a running daemon (see `cooldis debug rpc --help`)\n\
\n\
Maintainer and protocol inspection tools. These commands are not the public\n\
local console flow; use `cooldis console` or `cooldis chat` for normal operation.\n"
    );
}

fn print_daemon_help() {
    println!(
        "cooldis daemon\n\
\n\
Usage:\n\
  cooldis daemon run [--config cooldis.toml]\n\
  cooldis daemon config validate [--config cooldis.toml]\n\
  cooldis daemon service print [--target launchd|systemd] --config cooldis.toml [--label com.cooldis.daemon]\n\
  cooldis daemon service install [--target launchd|systemd] --config cooldis.toml [--label com.cooldis.daemon]\n\
  cooldis daemon service uninstall [--target launchd|systemd] [--label com.cooldis.daemon]\n\
\n\
The daemon uses cooldis.toml. Service installation is explicit and writes the\n\
user-level launchd/systemd service file without starting it automatically.\n"
    );
}

fn print_chat_help() {
    println!(
        "cooldis chat\n\
\n\
Usage:\n\
  cooldis chat [PROMPT] [--config <file>] [--cwd <path>]\n\
  cooldis chat [PROMPT] --attach <unix://path|ws://host:port[/rpc]>\n\
  cooldis chat [PROMPT] --provider bifrost_openai --base-url <url> --api-key-env <env> [--model <model>]\n\
\n\
Starts the bundled local terminal console over the app-server RPC boundary. By\n\
default it launches a private local app-server; --attach connects to an existing\n\
endpoint. In the TUI, use /help for session commands.\n"
    );
}

#[cfg(test)]
mod tests;
