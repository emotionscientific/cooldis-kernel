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

mod agent;
mod auth;
mod blob;
mod chat;
mod console;
mod coupling;
mod daemon;
mod debug_rpc;
mod import;
mod rpc;
mod secret;
mod skill;
mod tool;

use agent::*;
use auth::*;
use blob::*;
use console::*;
use coupling::*;
use daemon::*;
use debug_rpc::*;
use import::*;
use rpc::*;
use secret::*;
use skill::*;
use tool::*;

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

async fn run_chat(args: Vec<OsString>) -> CooldisResult<()> {
    chat::run(args, chat::ChatInvocation::Chat).await
}

fn usage_error(message: impl Into<String>) -> CooldisError {
    CooldisError::RuntimeFactory(message.into())
}

fn io_error(err: impl std::fmt::Display) -> CooldisError {
    CooldisError::RuntimeFactory(err.to_string())
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_help_is_a_concise_starting_surface() {
        assert!(ROOT_HELP.contains(
            "Start here:\n  cooldis console\n  cooldis chat [PROMPT]\n  cooldis init <name>"
        ));
        assert!(ROOT_HELP.contains(
            "Explore:\n  cooldis commands\n  cooldis help <command>\n  cooldis <command> --help\n  man cooldis"
        ));
        assert!(!ROOT_HELP.contains("Example usage:"));
        assert!(!ROOT_HELP.contains("Advanced:"));
        assert!(!ROOT_HELP.contains("cooldis coupling run --replay"));
        assert!(!ROOT_HELP.contains("cooldis daemon run"));
    }
}
