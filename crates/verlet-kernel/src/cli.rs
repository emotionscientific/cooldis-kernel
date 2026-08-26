mod agent;
mod auth;
mod blob;
mod chat;
mod console;
mod coupling;
mod daemon;
mod debug_bind;
mod debug_rpc;
mod host;
mod identity;
mod import;
mod kit;
mod rpc;
mod secret;
mod serve;
mod skill;
mod tool;

pub async fn run() -> crate::kernel::runtime_host::VerletResult<()> {
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
        "kit" => crate::cli::kit::run_kit(args).await,
        "tool" => {
            let client = client_command_preamble("tool", &args).await?;
            crate::cli::tool::run_tool(args, client).await
        }
        "skill" => crate::cli::skill::run_skill(args).await,
        "secret" => {
            let client = client_command_preamble("secret", &args).await?;
            crate::cli::secret::run_secret(args, client).await
        }
        "auth" => {
            let client = client_command_preamble("auth", &args).await?;
            crate::cli::auth::run_auth(args, client).await
        }
        "identity" => {
            let client = client_command_preamble("identity", &args).await?;
            crate::cli::identity::run_identity(args, client).await
        }
        "console" => crate::cli::console::run_console(args).await,
        "chat" => {
            let client = client_command_preamble("chat", &args).await?;
            run_chat(args, client).await
        }
        "debug" => crate::cli::debug_rpc::run_debug(args).await,
        "daemon" => crate::cli::daemon::run_daemon(args).await,
        "serve" => crate::cli::serve::run_serve(args).await,
        "host" => crate::cli::host::run_host(args).await,
        "rpc" => crate::cli::rpc::run_rpc(args).await,
        other => Err(usage_error(format!(
            "unknown command {other:?}; use `verlet --help`"
        ))),
    }
}

fn run_help(args: Vec<std::ffi::OsString>) -> crate::kernel::runtime_host::VerletResult<()> {
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

fn print_command_help(path: &[String]) -> crate::kernel::runtime_host::VerletResult<()> {
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
        [command] if command == "kit" => crate::cli::kit::print_kit_help(),
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
        [command, subcommand] if command == "auth" && subcommand == "login" => {
            crate::cli::auth::print_auth_login_help()
        }
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
        [command] if command == "serve" => crate::cli::serve::print_serve_help(),
        [command, subcommand, action]
            if command == "daemon" && subcommand == "config" && action == "validate" =>
        {
            crate::cli::daemon::print_daemon_help()
        }
        [command, subcommand, _action] if command == "daemon" && subcommand == "service" => {
            crate::cli::daemon::print_daemon_help()
        }
        [command] if command == "host" => crate::cli::host::print_host_help(),
        [command, subcommand] if command == "host" && subcommand == "run" => {
            crate::cli::host::print_host_run_help()
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

async fn run_chat(
    args: Vec<std::ffi::OsString>,
    client: Option<InstanceClient>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    chat::run(args, chat::ChatInvocation::Chat, client).await
}

#[cfg(unix)]
pub(crate) type InstanceClient =
    crate::adapters::operator_client::OperatorClient<tokio::net::UnixStream>;

#[cfg(not(unix))]
pub(crate) type InstanceClient =
    crate::adapters::operator_client::OperatorClient<tokio::net::TcpStream>;

#[derive(Debug)]
pub(crate) enum InstanceScope {
    Project {
        cwd: std::path::PathBuf,
        config_path: Option<std::path::PathBuf>,
        runtime_home: Option<std::path::PathBuf>,
        state_home: Option<std::path::PathBuf>,
    },
    User {
        state_home: Option<std::path::PathBuf>,
    },
}

struct InstanceTarget {
    project_root: std::path::PathBuf,
    state_root: std::path::PathBuf,
    user_state_root: std::path::PathBuf,
    runtime_home: std::path::PathBuf,
    cwd: std::path::PathBuf,
    config_path: Option<std::path::PathBuf>,
    idle_timeout: Option<std::time::Duration>,
}

const AUTO_SPAWN_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10 * 60);
const INSTANCE_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);
const INSTANCE_START_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

async fn client_command_preamble(
    command: &str,
    args: &[std::ffi::OsString],
) -> crate::kernel::runtime_host::VerletResult<Option<InstanceClient>> {
    if args
        .iter()
        .any(|arg| arg.as_os_str() == "--help" || arg.as_os_str() == "-h")
    {
        return Ok(None);
    }
    let scope = match command {
        "auth"
            if args.first().is_some_and(|subcommand| {
                matches!(
                    subcommand.to_string_lossy().as_ref(),
                    "login" | "status" | "set" | "delete"
                )
            }) =>
        {
            InstanceScope::User {
                state_home: option_path(args, "--state-home")?,
            }
        }
        "secret"
            if args.first().is_some_and(|subcommand| {
                matches!(
                    subcommand.to_string_lossy().as_ref(),
                    "import" | "set" | "list" | "status" | "delete"
                )
            }) =>
        {
            InstanceScope::User {
                state_home: option_path(args, "--state-home")?,
            }
        }
        "identity"
            if args.first().is_some_and(|subcommand| {
                matches!(
                    subcommand.to_string_lossy().as_ref(),
                    "declare" | "mint" | "revoke-credential" | "revoke-principal" | "list"
                )
            }) =>
        {
            match option_path(args, "--state-home")? {
                Some(state_home) => InstanceScope::Project {
                    cwd: std::env::current_dir().map_err(io_error)?,
                    config_path: None,
                    runtime_home: None,
                    state_home: Some(state_home),
                },
                None => InstanceScope::User { state_home: None },
            }
        }
        "tool"
            if args.first().is_some_and(|arg| arg == "source")
                && args.get(1).is_some_and(|subcommand| {
                    matches!(
                        subcommand.to_string_lossy().as_ref(),
                        "add" | "discover" | "list" | "show" | "remove"
                    )
                }) =>
        {
            InstanceScope::Project {
                cwd: std::env::current_dir().map_err(io_error)?,
                config_path: None,
                runtime_home: None,
                state_home: option_path(args, "--state-home")?,
            }
        }
        "chat" if !has_option(args, "--attach") => InstanceScope::Project {
            cwd: option_path(args, "--cwd")?.unwrap_or(std::env::current_dir().map_err(io_error)?),
            config_path: option_path(args, "--config")?,
            runtime_home: option_path(args, "--runtime-home")?,
            state_home: option_path(args, "--state-home")?,
        },
        _ => return Ok(None),
    };
    connect_instance(scope).await.map(Some)
}

fn has_option(args: &[std::ffi::OsString], name: &str) -> bool {
    args.iter().any(|arg| arg.as_os_str() == name)
}

fn option_path(
    args: &[std::ffi::OsString],
    name: &str,
) -> crate::kernel::runtime_host::VerletResult<Option<std::path::PathBuf>> {
    let Some(index) = args.iter().position(|arg| arg.as_os_str() == name) else {
        return Ok(None);
    };
    args.get(index + 1)
        .map(std::path::PathBuf::from)
        .map(Some)
        .ok_or_else(|| usage_error(format!("{name} requires a value")))
}

pub(crate) async fn connect_instance(
    scope: InstanceScope,
) -> crate::kernel::runtime_host::VerletResult<InstanceClient> {
    let (target, discovery_roots) = match scope {
        InstanceScope::Project {
            cwd,
            config_path,
            runtime_home,
            state_home,
        } => {
            let target =
                resolve_project_instance_target(cwd, config_path, runtime_home, state_home, None)?;
            let roots = vec![target.state_root.clone()];
            (target, roots)
        }
        InstanceScope::User { state_home } => {
            // Auth prefers the current project instance when it owns the
            // requested user root, then checks the user-root endpoint.
            let cwd = std::env::current_dir().map_err(io_error)?;
            let project = crate::daemon::daemon_config::discover_verlet_project(&cwd)?;
            if !project.found_project {
                let target = resolve_user_home_instance_target(state_home)?;
                let root = target.user_state_root.clone();
                (target, vec![root])
            } else {
                let mut target = resolve_project_instance_target(cwd, None, None, None, None)?;
                let requested_user_root = match state_home {
                    Some(state_home) => crate::cli::console::absolute_path(&state_home)?,
                    None => target.user_state_root.clone(),
                };
                let mut roots = Vec::new();
                if target.user_state_root == requested_user_root {
                    roots.push(target.state_root.clone());
                }
                if !roots.iter().any(|root| root == &requested_user_root) {
                    roots.push(requested_user_root.clone());
                }
                target.user_state_root = requested_user_root;
                (target, roots)
            }
        }
    };

    #[cfg(not(unix))]
    {
        let _ = discovery_roots;
        return Err(usage_error(format!(
            "could not start a server for {}: local instance routing requires a Unix socket",
            target.state_root.display()
        )));
    }

    #[cfg(unix)]
    {
        let mut last_error = None;
        for root in &discovery_roots {
            if let Some(endpoint) =
                crate::adapters::app_server::instance::resolve_instance_endpoint(root)
            {
                match connect_instance_endpoint(&endpoint).await {
                    Ok(client) => return Ok(client),
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
        }

        for root in [&target.state_root, &target.user_state_root] {
            if discovery_roots.iter().any(|candidate| candidate == root) {
                continue;
            }
            if let Some(endpoint) =
                crate::adapters::app_server::instance::resolve_instance_endpoint(root)
                && std::os::unix::net::UnixStream::connect(&endpoint.unix_socket).is_ok()
            {
                return Err(usage_error(format!(
                    "could not start a server for {}: instance root {} is owned by pid {}, socket {}; stop that process first",
                    target.state_root.display(),
                    root.display(),
                    endpoint.pid,
                    endpoint.unix_socket.display()
                )));
            }
        }

        spawn_instance_server(&target).map_err(|error| {
            usage_error(format!(
                "could not start a server for {}: {error}",
                target.state_root.display()
            ))
        })?;
        let deadline = tokio::time::Instant::now() + INSTANCE_START_TIMEOUT;
        loop {
            if let Some(endpoint) =
                crate::adapters::app_server::instance::resolve_instance_endpoint(&target.state_root)
            {
                match connect_instance_endpoint(&endpoint).await {
                    Ok(client) => return Ok(client),
                    Err(error) => last_error = Some(error.to_string()),
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(usage_error(format!(
                    "could not start a server for {}: timed out after 15s{}",
                    target.state_root.display(),
                    last_error
                        .as_deref()
                        .map(|error| format!("; last connection error: {error}"))
                        .unwrap_or_default()
                )));
            }
            tokio::time::sleep(INSTANCE_START_POLL_INTERVAL).await;
        }
    }
}

fn resolve_project_instance_target(
    cwd: std::path::PathBuf,
    config_path: Option<std::path::PathBuf>,
    runtime_home: Option<std::path::PathBuf>,
    state_home: Option<std::path::PathBuf>,
    user_state_home: Option<std::path::PathBuf>,
) -> crate::kernel::runtime_host::VerletResult<InstanceTarget> {
    let resolved = crate::cli::console::resolve_instance_app_server_config(
        cwd,
        config_path,
        runtime_home,
        state_home,
        user_state_home,
    )?;
    Ok(InstanceTarget {
        project_root: resolved.project_root,
        state_root: crate::cli::console::absolute_path(&resolved.config.state_home)?,
        user_state_root: crate::cli::console::absolute_path(&resolved.config.user_state_home)?,
        runtime_home: crate::cli::console::absolute_path(&resolved.config.runtime_home)?,
        cwd: crate::cli::console::absolute_path(&resolved.config.cwd)?,
        config_path: resolved.config_path,
        idle_timeout: resolved.idle_timeout,
    })
}

fn resolve_user_home_instance_target(
    state_home: Option<std::path::PathBuf>,
) -> crate::kernel::runtime_host::VerletResult<InstanceTarget> {
    let user_home =
        crate::cli::console::absolute_path(&crate::cli::console::default_user_verlet_home()?)?;
    let user_state_root = match state_home {
        Some(state_home) => crate::cli::console::absolute_path(&state_home)?,
        None => user_home.join("state"),
    };
    let user_config = user_home.join("config.toml");
    let (config_path, idle_timeout) = if user_config.is_file() {
        let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(&user_config))?;
        let idle_timeout = loaded.config.idle_timeout()?;
        (loaded.path, idle_timeout)
    } else {
        (None, None)
    };
    Ok(InstanceTarget {
        project_root: user_home.clone(),
        state_root: user_state_root.clone(),
        user_state_root,
        runtime_home: user_home.join("runtime"),
        cwd: user_home,
        config_path,
        idle_timeout,
    })
}

#[cfg(unix)]
async fn connect_instance_endpoint(
    endpoint: &crate::adapters::app_server::instance::InstanceEndpoint,
) -> crate::kernel::runtime_host::VerletResult<InstanceClient> {
    crate::adapters::operator_client::OperatorClient::connect_unix(
        endpoint.unix_socket.clone(),
        crate::adapters::operator_client::OperatorConnectConfig {
            client_name: "verlet-cli".to_string(),
            ..crate::adapters::operator_client::OperatorConnectConfig::default()
        },
    )
    .await
}

#[cfg(unix)]
fn spawn_instance_server(target: &InstanceTarget) -> crate::kernel::runtime_host::VerletResult<()> {
    std::fs::create_dir_all(&target.project_root).map_err(io_error)?;
    std::fs::create_dir_all(&target.state_root).map_err(io_error)?;
    let log_path = target.state_root.join("serve.log");
    let stdout = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(io_error)?;
    let stderr = stdout.try_clone().map_err(io_error)?;
    let mut command = std::process::Command::new(std::env::current_exe().map_err(io_error)?);
    command
        .arg("serve")
        .arg("--idle-timeout")
        .arg(
            humantime::format_duration(target.idle_timeout.unwrap_or(AUTO_SPAWN_IDLE_TIMEOUT))
                .to_string(),
        )
        .arg("--cwd")
        .arg(&target.cwd)
        .arg("--runtime-home")
        .arg(&target.runtime_home)
        .arg("--state-home")
        .arg(&target.state_root)
        .arg("--user-state-home")
        .arg(&target.user_state_root)
        .current_dir(&target.project_root)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::from(stdout))
        .stderr(std::process::Stdio::from(stderr));
    if let Some(config_path) = &target.config_path {
        command.arg("--config").arg(config_path);
    }
    {
        use std::os::unix::process::CommandExt as _;
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    Err(std::io::Error::last_os_error())
                } else {
                    Ok(())
                }
            });
        }
    }
    command.spawn().map(drop).map_err(io_error)
}

fn usage_error(message: impl Into<String>) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(message.into())
}

fn io_error(err: impl std::fmt::Display) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
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
    "verlet init <name> [--out <dir>] [--force]",
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
    "verlet agent init <name> [--out <dir>] [--force]",
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
    "verlet kit install <kit-dir> [--registry-root .verlet/operations] [--kits-root .verlet/kits]",
    "verlet kit list [--kits-root .verlet/kits] [--json]",
    "verlet kit remove <name> [--kits-root .verlet/kits]",
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
    "verlet serve [--config verlet.toml] [--idle-timeout <duration>]",
    "verlet daemon config validate [--config verlet.toml]",
    "verlet daemon service print [--target launchd|systemd] --config verlet.toml [--label com.verlet.daemon]",
    "verlet daemon service install [--target launchd|systemd] --config verlet.toml [--label com.verlet.daemon]",
    "verlet daemon service uninstall [--target launchd|systemd] [--label com.verlet.daemon]",
    "verlet host run --config <host.toml>",
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
