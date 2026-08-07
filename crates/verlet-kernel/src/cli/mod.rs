mod agent;
mod auth;
mod blob;
mod chat;
mod console;
mod coupling;
mod daemon;
mod debug_bind;
mod debug_rpc;
mod identity;
mod import;
mod rpc;
mod secret;
mod skill;
mod tool;

pub async fn run() -> crate::VerletResult<()> {
    let mut args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|command| {
        crate::daemon::remote_store::process_executor::is_remote_child_command(command)
    }) {
        return crate::cli::daemon::remote_child_run().await;
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
        println!("verlet {}", env!("CARGO_PKG_VERSION"));
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
        "init" => crate::cli::agent::agent_init(args).await,
        "agent" => crate::cli::agent::run_agent(args).await,
        "blob" => crate::cli::blob::run_blob(args).await,
        "coupling" => crate::cli::coupling::run_coupling(args).await,
        "import" => crate::cli::import::run_import(args).await,
        "tool" => crate::cli::tool::run_tool(args).await,
        "skill" => crate::cli::skill::run_skill(args).await,
        "secret" => crate::cli::secret::run_secret(args).await,
        "auth" => crate::cli::auth::run_auth(args).await,
        "identity" => crate::cli::identity::run_identity(args).await,
        "console" => crate::cli::console::run_console(args).await,
        "chat" => run_chat(args).await,
        "debug" => crate::cli::debug_rpc::run_debug(args).await,
        "daemon" => crate::cli::daemon::run_daemon(args).await,
        "rpc" => crate::cli::rpc::run_rpc(args).await,
        other => Err(usage_error(format!(
            "unknown command {other:?}; use `verlet --help`"
        ))),
    }
}

fn run_help(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
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

fn print_command_help(path: &[String]) -> crate::VerletResult<()> {
    match path {
        [command] if command == "commands" => print_commands_help(),
        [command] if command == "help" => print_help_help(),
        [command] if command == "console" => crate::cli::console::print_console_help(),
        [command] if command == "chat" => crate::cli::console::print_chat_help(),
        [command] if command == "init" => crate::cli::agent::print_agent_init_help(),
        [command] if command == "agent" => crate::cli::agent::print_agent_help(),
        [command] if command == "coupling" => crate::cli::coupling::print_coupling_help(),
        [command, subcommand] if command == "coupling" && subcommand == "init" => {
            crate::cli::coupling::print_coupling_init_help()
        }
        [command, subcommand] if command == "coupling" && subcommand == "run" => {
            crate::cli::coupling::print_coupling_run_help()
        }
        [command] if command == "blob" => crate::cli::blob::print_blob_help(),
        [command, subcommand] if command == "blob" && subcommand == "publish" => {
            crate::cli::blob::print_blob_publish_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "init" => {
            crate::cli::agent::print_agent_init_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "plan" => {
            crate::cli::agent::print_agent_plan_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "publish" => {
            crate::cli::agent::print_agent_publish_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "list" => {
            crate::cli::agent::print_agent_list_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "versions" => {
            crate::cli::agent::print_agent_versions_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "diff" => {
            crate::cli::agent::print_agent_diff_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "show" => {
            crate::cli::agent::print_agent_show_help()
        }
        [command, subcommand] if command == "agent" && subcommand == "run" => {
            crate::cli::agent::print_agent_run_help()
        }
        [command] if command == "tool" => crate::cli::tool::print_tool_help(),
        [command] if command == "import" => crate::cli::import::print_import_help(),
        [command, subcommand] if command == "import" && subcommand == "build" => {
            crate::cli::import::print_import_build_help()
        }
        [command, subcommand] if command == "import" && subcommand == "publish" => {
            crate::cli::import::print_import_publish_help()
        }
        [command] if command == "skill" => crate::cli::skill::print_skill_help(),
        [command, subcommand] if command == "skill" && subcommand == "publish" => {
            crate::cli::skill::print_skill_publish_help()
        }
        [command, subcommand] if command == "skill" && subcommand == "import" => {
            crate::cli::skill::print_skill_import_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "build" => {
            crate::cli::tool::print_tool_build_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "list" => {
            crate::cli::tool::print_tool_list_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "publish" => {
            crate::cli::tool::print_tool_publish_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "run" => {
            crate::cli::tool::print_tool_run_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "manual" => {
            crate::cli::tool::print_tool_manual_help()
        }
        [command, subcommand] if command == "tool" && subcommand == "source" => {
            crate::cli::tool::print_tool_source_help()
        }
        [command, subcommand, action] if command == "tool" && subcommand == "source" => {
            match action.as_str() {
                "add" => crate::cli::tool::print_tool_source_add_help(),
                "discover" => crate::cli::tool::print_tool_source_discover_help(),
                "list" => crate::cli::tool::print_tool_source_list_help(),
                "show" => crate::cli::tool::print_tool_source_show_help(),
                "remove" => crate::cli::tool::print_tool_source_remove_help(),
                other => {
                    return Err(usage_error(format!(
                        "unknown tool source help command {other:?}"
                    )));
                }
            }
        }
        [command] if command == "auth" => crate::cli::auth::print_auth_help(),
        [command, subcommand] if command == "auth" && subcommand == "status" => {
            crate::cli::auth::print_auth_status_help()
        }
        [command, subcommand] if command == "auth" && subcommand == "set" => {
            crate::cli::auth::print_auth_set_help()
        }
        [command, subcommand] if command == "auth" && subcommand == "delete" => {
            crate::cli::auth::print_auth_delete_help()
        }
        [command] if command == "identity" => crate::cli::identity::print_identity_help(),
        [command, subcommand] if command == "identity" => match subcommand.as_str() {
            "bootstrap" => crate::cli::identity::print_identity_bootstrap_help(),
            "declare" => crate::cli::identity::print_identity_declare_help(),
            "mint" => crate::cli::identity::print_identity_mint_help(),
            "revoke-credential" => crate::cli::identity::print_identity_revoke_credential_help(),
            "revoke-principal" => crate::cli::identity::print_identity_revoke_principal_help(),
            "list" => crate::cli::identity::print_identity_list_help(),
            other => {
                return Err(usage_error(format!(
                    "unknown identity help command {other:?}"
                )));
            }
        },
        [command] if command == "secret" => crate::cli::secret::print_secret_help(),
        [command, subcommand] if command == "secret" && subcommand == "import" => {
            crate::cli::secret::print_secret_import_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "set" => {
            crate::cli::secret::print_secret_set_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "list" => {
            crate::cli::secret::print_secret_list_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "status" => {
            crate::cli::secret::print_secret_status_help()
        }
        [command, subcommand] if command == "secret" && subcommand == "delete" => {
            crate::cli::secret::print_secret_delete_help()
        }
        [command] if command == "rpc" => crate::cli::rpc::print_rpc_help(),
        [command] if command == "debug" => crate::cli::debug_rpc::print_debug_help(),
        [command, subcommand] if command == "debug" && subcommand == "bind" => {
            crate::cli::debug_bind::print_debug_bind_help()
        }
        [command, subcommand] if command == "debug" && subcommand == "rpc" => {
            crate::cli::debug_rpc::print_debug_rpc_help()
        }
        [command] if command == "daemon" => crate::cli::daemon::print_daemon_help(),
        [command, subcommand] if command == "daemon" && subcommand == "run" => {
            crate::cli::daemon::print_daemon_help()
        }
        [command, subcommand, action]
            if command == "daemon" && subcommand == "config" && action == "validate" =>
        {
            crate::cli::daemon::print_daemon_help()
        }
        [command, subcommand, _action] if command == "daemon" && subcommand == "service" => {
            crate::cli::daemon::print_daemon_help()
        }
        _ => {
            return Err(usage_error(format!(
                "unknown help command {:?}; use `verlet commands`",
                path.join(" ")
            )));
        }
    }
    Ok(())
}

async fn run_chat(args: Vec<std::ffi::OsString>) -> crate::VerletResult<()> {
    chat::run(args, chat::ChatInvocation::Chat).await
}

fn usage_error(message: impl Into<String>) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(message.into())
}

fn io_error(err: impl std::fmt::Display) -> crate::VerletError {
    crate::VerletError::RuntimeFactory(err.to_string())
}

const ROOT_HELP: &str = "verlet

Usage:
  verlet <command> [args]
  verlet help [COMMAND...]
  verlet commands

Start here:
  verlet console
  verlet chat [PROMPT]
  verlet init <name>

Explore:
  verlet commands
  verlet help <command>
  verlet <command> --help
  man verlet
";

const CANONICAL_COMMANDS: &[&str] = &[
    "verlet",
    "verlet commands",
    "verlet help [COMMAND...]",
    "verlet init <name> [--out <dir|manifest.toml>] [--force]",
    "verlet console [--no-open] [--cwd <path>] [--config <verlet.toml>] [--port <port>]",
    "verlet chat [PROMPT] [--config <file>] [--cwd <path>] [--attach <unix://path|ws://host:port[/rpc]>]",
    "verlet auth status <provider-id> [--state-home ~/.verlet/state]",
    "verlet auth set <provider-id> --api-key-stdin [--state-home ~/.verlet/state]",
    "verlet auth delete <provider-id> [--state-home ~/.verlet/state]",
    "verlet identity bootstrap <principal-id> --display <display> [--state-home ~/.verlet/state]",
    "verlet identity declare <principal-id> --kind adapter --display <display> --declared-by <principal-id> [--state-home ~/.verlet/state]",
    "verlet identity mint <principal-id> --minted-by <principal-id> [--expires-at-ms <ms>] [--state-home ~/.verlet/state]",
    "verlet identity revoke-credential <credential-id> --revoked-by <principal-id> [--state-home ~/.verlet/state]",
    "verlet identity revoke-principal <principal-id> --revoked-by <principal-id> [--state-home ~/.verlet/state]",
    "verlet identity list [--state-home ~/.verlet/state]",
    "verlet secret import <name> --from-env <ENV> [--state-home ~/.verlet/state]",
    "verlet secret set <name> --value-stdin [--state-home ~/.verlet/state]",
    "verlet secret list [--state-home ~/.verlet/state]",
    "verlet secret status <name> [--state-home ~/.verlet/state]",
    "verlet secret delete <name> [--state-home ~/.verlet/state]",
    "verlet agent init <name> [--out <dir|manifest.toml>] [--force]",
    "verlet coupling init <name> [--out <dir>] [--force]",
    "verlet agent plan <manifest> [--registry-root .verlet/agents] [--operations-registry-root .verlet/operations]",
    "verlet agent publish <manifest> [--registry-root .verlet/agents] [--operations-registry-root .verlet/operations]",
    "verlet agent list [--registry-root .verlet/agents]",
    "verlet agent versions <name> [--json] [--registry-root .verlet/agents]",
    "verlet agent diff <name> --from <version>[:authored|:resolved] --to <version>[:authored|:resolved] [--json] [--registry-root .verlet/agents]",
    "verlet agent show <agent-ref-or-name> [--registry-root .verlet/agents]",
    "verlet agent run <agent-ref> --input <text> [--registry-root .verlet/agents]",
    "verlet blob publish <file> [--registry-root .verlet/blobs] [--name <name>]",
    "verlet import build --package verlet.import.toml",
    "verlet import publish --package verlet.import.toml [--registry-root .verlet/operations]",
    "verlet coupling run --replay --artifact <path|op://ref> --coupling-file <file> (--thread-id <id> --journal <db>|--export <bundle>) [--coupling-id <id>] [--registry-root .verlet/operations] [--json]",
    "verlet tool build --package verlet.tool.toml",
    "verlet tool build --module-path <dir|Cargo.toml> [--name <name>] [--config verlet.json]",
    "verlet tool list [--registry-root .verlet/operations]",
    "verlet tool publish --package verlet.tool.toml [--registry-root .verlet/operations]",
    "verlet tool run --module-path <dir|Cargo.toml> <operation> --input <text> [--mount /guest=/host]",
    "verlet tool run --bin-path <module.wasm> <operation> --input <text> [--mount /guest=/host]",
    "verlet tool run <published-name> <operation> --input <text> [--registry-root .verlet/operations] [--state-home .verlet/state]",
    "verlet tool manual <published-name> [operation] [--json] [--registry-root .verlet/operations]",
    "verlet skill publish <dir> [--registry-root .verlet/skills] [--name <package>]",
    "verlet skill import <dir> [--registry-root .verlet/skills] [--blob-registry-root .verlet/blobs] [--name <package>] [--dry-run]",
    "verlet tool source add <name> --kind <mcp-http|mcp-sse> --url <url> [--bearer-secret <secret-name>] [--include-tool <tool>] [--state-home .verlet/state]",
    "verlet tool source discover <name> [--state-home .verlet/state]",
    "verlet tool source list [--json] [--state-home .verlet/state]",
    "verlet tool source show <name> [--json] [--state-home .verlet/state]",
    "verlet tool source remove <name> [--state-home .verlet/state]",
    "verlet rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--cwd <path>]",
    "verlet debug bind <thread-id> [--json] [--url <ws-url> | --config <verlet.toml> | --journal <db>]",
    "verlet debug rpc call <method> [PARAMS_JSON] [--url <ws-url> | --config <verlet.toml>]",
    "verlet debug rpc turn (--thread <id> | --new) [--json] <text> [--url <ws-url> | --config <verlet.toml>]",
    "verlet debug rpc tail --thread <id> [--url <ws-url> | --config <verlet.toml>]",
    "verlet daemon run [--config verlet.toml]",
    "verlet daemon config validate [--config verlet.toml]",
    "verlet daemon service print [--target launchd|systemd] --config verlet.toml [--label com.verlet.daemon]",
    "verlet daemon service install [--target launchd|systemd] --config verlet.toml [--label com.verlet.daemon]",
    "verlet daemon service uninstall [--target launchd|systemd] [--label com.verlet.daemon]",
];

fn print_help() {
    print!("{ROOT_HELP}");
}

fn print_help_help() {
    println!(
        "verlet help\n\
\n\
Usage:\n\
  verlet help [COMMAND...]\n\
\n\
Prints root help or the help page for a canonical Verlet command path.\n"
    );
}

fn print_commands_help() {
    println!("verlet commands\n");
    println!("Usage:");
    println!("  verlet commands");
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

    #[test]
    fn root_help_is_a_concise_starting_surface() {
        assert!(crate::cli::ROOT_HELP.contains(
            "Start here:\n  verlet console\n  verlet chat [PROMPT]\n  verlet init <name>"
        ));
        assert!(crate::cli::ROOT_HELP.contains(
            "Explore:\n  verlet commands\n  verlet help <command>\n  verlet <command> --help\n  man verlet"
        ));
        assert!(!crate::cli::ROOT_HELP.contains("Example usage:"));
        assert!(!crate::cli::ROOT_HELP.contains("Advanced:"));
        assert!(!crate::cli::ROOT_HELP.contains("verlet coupling run --replay"));
        assert!(!crate::cli::ROOT_HELP.contains("verlet daemon run"));
    }
}
