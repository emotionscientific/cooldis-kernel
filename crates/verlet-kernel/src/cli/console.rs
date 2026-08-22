//! The `console` subcommand family and its private app-server support.

#[cfg(test)]
mod tests;

pub(crate) async fn run_console(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
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

    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .map_err(|err| {
            crate::cli::usage_error(format!(
                "failed to bind Verlet console listener {}: {err}",
                options.listen
            ))
        })?;
    let bound_addr = listener.local_addr().map_err(|err| {
        crate::cli::usage_error(format!("failed to inspect Verlet console listener: {err}"))
    })?;
    let listen = crate::adapters::app_server::AppServerListenAddr::WebSocket(bound_addr);
    let assets = resolve_console_asset_root()?;
    let resolved = resolve_console_app_server_config(&options, listen.clone())?;
    let project_root = resolved.project_root.clone();
    let config_path = resolved.config_path.clone();
    let daemon_config = resolved.daemon_config;
    let mut config = resolved.config;
    let state_home = config.state_home.clone();
    config.console_assets = Some(crate::adapters::app_server::ConsoleAssetConfig {
        root: assets,
        session_token: String::new(),
    });
    prepare_console_project_storage(&config)?;

    let server = crate::adapters::app_server::VerletAppServer::new_local(config).await?;
    let _io_tasks = match crate::cli::daemon::start_daemon_io(
        &daemon_config.io,
        &daemon_config.sync,
        config_path.clone(),
        &server,
    )
    .await
    {
        Ok(tasks) => tasks,
        Err(error) => {
            if let Err(shutdown_error) = server.shutdown().await {
                eprintln!(
                    "failed to shut down Verlet console after I/O startup error {error}: {shutdown_error}"
                );
            }
            return Err(error);
        }
    };
    let ui_url = format!("http://{bound_addr}/");
    let rpc_url = format!("ws://{bound_addr}/rpc");
    println!("verlet console UI  {ui_url}");
    println!("verlet console RPC {rpc_url}");
    println!("verlet console Project {}", project_root.display());
    if let Some(config_path) = config_path {
        println!("verlet console Config {}", config_path.display());
    } else {
        println!("verlet console Config <defaults>");
    }
    println!("verlet console State {}", state_home.display());
    if options.open {
        if let Err(err) = open_browser_url(&ui_url) {
            eprintln!("verlet console could not open the browser: {err}");
        }
    }
    let serving = server.serve_websocket_listener(listener).await;
    let shutdown = server.shutdown().await;
    serving?;
    shutdown
}

#[cfg(test)]
pub(crate) fn console_app_server_config(
    options: &ConsoleArgs,
    listen: crate::adapters::app_server::AppServerListenAddr,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::app_server::VerletAppServerConfig> {
    resolve_console_app_server_config(options, listen).map(|resolved| resolved.config)
}

pub(crate) struct ResolvedConsoleAppServerConfig {
    pub(crate) config: crate::adapters::app_server::VerletAppServerConfig,
    pub(crate) project_root: std::path::PathBuf,
    pub(crate) config_path: Option<std::path::PathBuf>,
    pub(crate) idle_timeout: Option<std::time::Duration>,
    pub(crate) daemon_config: crate::daemon::daemon_config::VerletDaemonConfig,
}

pub(crate) struct ConsoleEnvironment {
    selected_cwd: std::path::PathBuf,
    project_root: std::path::PathBuf,
    project_storage_root: std::path::PathBuf,
    user_home: std::path::PathBuf,
    config_paths: Vec<std::path::PathBuf>,
}

pub(crate) fn resolve_console_app_server_config(
    options: &ConsoleArgs,
    listen: crate::adapters::app_server::AppServerListenAddr,
) -> crate::kernel::runtime_host::VerletResult<ResolvedConsoleAppServerConfig> {
    let env = resolve_console_environment(options)?;
    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config_layers(
        &env.config_paths,
        env.project_root.clone(),
    )?;
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        listen.clone(),
        env.selected_cwd.clone(),
    );
    config.runtime_home = env.project_storage_root.join("runtime");
    config.state_home = env.project_storage_root.join("state");
    config.user_state_home = env.user_home.join("state");
    config.agent_registry_root = env.project_storage_root.join("agents");
    config.blob_registry_root = env.project_storage_root.join("blobs");
    config.skill_registry_root = env.project_storage_root.join("skills");
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
        config.capsule_bindings.registry_root = Some(
            crate::cli::daemon::daemon_app_server_registry_root(operations)?,
        );
    }
    if let Some(agents) = loaded.config.registries.agents.clone() {
        config.agent_registry_root = crate::cli::daemon::daemon_app_server_registry_root(agents)?;
    }
    config.capsule_bindings.global_operation_names =
        loaded.config.operations.global_operation_names.clone();
    config.capsule_bindings.load_all_active_when_unbound =
        loaded.config.operations.load_all_active_when_unbound;
    apply_chat_provider_config(
        &mut config,
        crate::cli::daemon::load_daemon_provider_config(&loaded.config.provider)?,
    );
    config.listen = listen;

    let mut daemon_config = loaded.config;
    daemon_config.io.resolve_paths(&env.project_root);
    let idle_timeout = daemon_config.idle_timeout()?;
    Ok(ResolvedConsoleAppServerConfig {
        config,
        project_root: env.project_root,
        config_path: loaded.path,
        idle_timeout,
        daemon_config,
    })
}

pub(crate) fn resolve_instance_app_server_config(
    cwd: std::path::PathBuf,
    config_path: Option<std::path::PathBuf>,
    runtime_home: Option<std::path::PathBuf>,
    state_home: Option<std::path::PathBuf>,
    user_state_home: Option<std::path::PathBuf>,
) -> crate::kernel::runtime_host::VerletResult<ResolvedConsoleAppServerConfig> {
    let options = ConsoleArgs {
        listen: std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
        cwd,
        cwd_explicit: true,
        config_path,
        open: false,
        help: false,
    };
    let mut resolved = resolve_console_app_server_config(
        &options,
        crate::adapters::app_server::AppServerListenAddr::WebSocket(options.listen),
    )?;
    if let Some(runtime_home) = runtime_home {
        resolved.config.runtime_home = absolute_path(&runtime_home)?;
    }
    if let Some(state_home) = state_home {
        resolved.config.state_home = absolute_path(&state_home)?;
    }
    if let Some(user_state_home) = user_state_home {
        resolved.config.user_state_home = absolute_path(&user_state_home)?;
    }
    Ok(resolved)
}

pub(crate) fn resolve_console_environment(
    options: &ConsoleArgs,
) -> crate::kernel::runtime_host::VerletResult<ConsoleEnvironment> {
    let selected_cwd = absolute_path(&options.cwd)?;
    let project = crate::daemon::daemon_config::discover_verlet_project(&selected_cwd)?;
    let user_home = default_user_verlet_home()?;
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

pub(crate) fn console_project_storage_root(
    project_root: &std::path::Path,
    user_home: &std::path::Path,
) -> std::path::PathBuf {
    let storage_root = project_root.join(".verlet");
    if storage_root == user_home {
        return user_home.join("projects/home");
    }
    storage_root
}

pub(crate) fn prepare_console_project_storage(
    config: &crate::adapters::app_server::VerletAppServerConfig,
) -> crate::kernel::runtime_host::VerletResult<()> {
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
        std::fs::create_dir_all(root).map_err(|err| {
            crate::cli::io_error(format!(
                "failed to prepare Verlet console directory {}: {err}",
                root.display()
            ))
        })?;
    }
    Ok(())
}

pub(crate) fn default_user_verlet_home()
-> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    if let Some(home) = std::env::var_os("VERLET_HOME").map(std::path::PathBuf::from) {
        return Ok(home);
    }
    let home = std::env::var_os("HOME")
        .map(std::path::PathBuf::from)
        .ok_or_else(|| {
            crate::cli::usage_error("HOME is not set and VERLET_HOME was not provided")
        })?;
    Ok(home.join(".verlet"))
}

pub(crate) fn absolute_path(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .map_err(|err| {
            crate::cli::usage_error(format!("failed to read current working directory: {err}"))
        })?
        .join(path))
}

pub(crate) fn push_unique_path(paths: &mut Vec<std::path::PathBuf>, path: std::path::PathBuf) {
    if !paths.iter().any(|existing| existing == &path) {
        paths.push(path);
    }
}

pub(crate) fn resolve_console_asset_root()
-> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    if let Some(path) = std::env::var_os("VERLET_CONSOLE_ASSET_DIR").map(std::path::PathBuf::from) {
        return console_asset_root_if_valid(path).ok_or_else(|| {
            crate::cli::usage_error(
                "VERLET_CONSOLE_ASSET_DIR must point at a built console directory containing index.html",
            )
        });
    }

    let mut candidates = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        candidates.push(exe_asset_candidate(&exe));
        candidates.push(
            exe.parent()
                .unwrap_or(std::path::Path::new("."))
                .join("../share/verlet/console"),
        );
        if let Ok(link) = std::fs::read_link(&exe) {
            let target = if link.is_absolute() {
                link
            } else {
                exe.parent().unwrap_or(std::path::Path::new(".")).join(link)
            };
            candidates.push(exe_asset_candidate(&target));
        }
    }
    candidates
        .push(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../apps/console/dist"));

    candidates
        .into_iter()
        .find_map(console_asset_root_if_valid)
        .ok_or_else(|| {
            crate::cli::usage_error(
                "Verlet console assets were not found; run `scripts/build-console-assets.sh` or set VERLET_CONSOLE_ASSET_DIR",
            )
        })
}

pub(crate) fn exe_asset_candidate(exe: &std::path::Path) -> std::path::PathBuf {
    exe.parent()
        .unwrap_or(std::path::Path::new("."))
        .join("share/verlet/console")
}

pub(crate) fn console_asset_root_if_valid(path: std::path::PathBuf) -> Option<std::path::PathBuf> {
    path.join("index.html").is_file().then_some(path)
}

pub(crate) fn open_browser_url(url: &str) -> crate::kernel::runtime_host::VerletResult<()> {
    browser_open_command(url)?
        .spawn()
        .map(|_| ())
        .map_err(|err| crate::cli::usage_error(format!("failed to open browser: {err}")))
}

pub(crate) async fn open_browser_url_checked(
    url: &str,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let command = browser_open_command(url)?;
    wait_for_browser_open_command(command).await
}

async fn wait_for_browser_open_command(
    command: std::process::Command,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let mut command = tokio::process::Command::from(command);
    command
        .kill_on_drop(true)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let status = command
        .status()
        .await
        .map_err(|err| crate::cli::usage_error(format!("failed to open browser: {err}")))?;
    if status.success() {
        Ok(())
    } else {
        Err(crate::cli::usage_error(format!(
            "browser opener exited with {status}"
        )))
    }
}

#[cfg(target_os = "macos")]
pub(crate) fn browser_open_command(
    url: &str,
) -> crate::kernel::runtime_host::VerletResult<std::process::Command> {
    let mut command = std::process::Command::new("open");
    command.arg(url);
    Ok(command)
}

#[cfg(target_os = "linux")]
pub(crate) fn browser_open_command(
    url: &str,
) -> crate::kernel::runtime_host::VerletResult<std::process::Command> {
    let mut command = std::process::Command::new("xdg-open");
    command.arg(url);
    Ok(command)
}

#[cfg(target_os = "windows")]
pub(crate) fn browser_open_command(
    url: &str,
) -> crate::kernel::runtime_host::VerletResult<std::process::Command> {
    let mut command = std::process::Command::new("cmd");
    command.args(["/C", "start", "", url]);
    Ok(command)
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
pub(crate) fn browser_open_command(
    _url: &str,
) -> crate::kernel::runtime_host::VerletResult<std::process::Command> {
    Err(crate::cli::usage_error(
        "automatic browser open is not supported on this platform",
    ))
}

#[derive(Debug)]
pub(crate) struct ConsoleArgs {
    listen: std::net::SocketAddr,
    cwd: std::path::PathBuf,
    cwd_explicit: bool,
    config_path: Option<std::path::PathBuf>,
    open: bool,
    help: bool,
}

#[derive(Debug)]
pub(crate) struct ChatArgs {
    pub(crate) attach: Option<String>,
    pub(crate) prompt: Option<String>,
    pub(crate) help: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum ChatProviderConfig {
    Local,
    OpenAICodex {
        model: String,
        max_tokens: u32,
        stream: bool,
    },
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

pub(crate) fn apply_chat_provider_config(
    config: &mut crate::adapters::app_server::VerletAppServerConfig,
    provider: ChatProviderConfig,
) {
    match provider {
        ChatProviderConfig::Local => {}
        ChatProviderConfig::OpenAICodex {
            model,
            max_tokens,
            stream,
        } => {
            config.model = model.clone();
            config.model_provider =
                verlet_metadata::provider_store::OPENAI_CODEX_PROVIDER_ID.to_string();
            config.model_explicit = true;
            config.provider = crate::adapters::app_server::AppServerProviderConfig::OpenAICodex {
                model,
                max_tokens,
                stream,
            };
        }
        ChatProviderConfig::BifrostOpenAI {
            base_url,
            api_key,
            model,
            max_tokens,
            stream,
        } => {
            config.model = model.clone();
            config.model_provider =
                crate::adapters::app_server::APP_SERVER_BIFROST_PROVIDER.to_string();
            config.model_explicit = true;
            config.provider =
                crate::adapters::app_server::AppServerProviderConfig::BifrostOpenAIResponses {
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
            config.model_explicit = true;
            config.provider =
                crate::adapters::app_server::AppServerProviderConfig::OpenAIChatCompletions {
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
            config.model_provider =
                crate::adapters::app_server::APP_SERVER_ANTHROPIC_PROVIDER.to_string();
            config.model_explicit = true;
            config.provider =
                crate::adapters::app_server::AppServerProviderConfig::AnthropicMessages {
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
            config.model_provider =
                crate::adapters::app_server::APP_SERVER_ANTHROPIC_BEDROCK_PROVIDER.to_string();
            config.model_explicit = true;
            config.provider =
                crate::adapters::app_server::AppServerProviderConfig::AnthropicBedrock {
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
                config.model_explicit = true;
            }
            config.model_provider = provider_id.clone();
            config.provider = crate::adapters::app_server::AppServerProviderConfig::CatalogOpenAIChatCompletions {
                provider_id,
                model,
                max_tokens,
                stream,
            };
        }
    }
}

pub(crate) fn parse_console_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<ConsoleArgs> {
    let mut listen = "127.0.0.1:0"
        .parse::<std::net::SocketAddr>()
        .expect("default console listen address is valid");
    let mut cwd = std::env::current_dir().map_err(|err| {
        crate::cli::usage_error(format!("failed to read current working directory: {err}"))
    })?;
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
                cwd = std::path::PathBuf::from(crate::cli::tool::required_string_value(
                    &mut iter, "--cwd",
                )?);
                cwd_explicit = true;
            }
            "--config" => {
                config_path = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            "--port" => {
                let port = crate::cli::tool::required_string_value(&mut iter, "--port")?
                    .parse::<u16>()
                    .map_err(|_| {
                        crate::cli::usage_error("--port must be an integer from 0 to 65535")
                    })?;
                listen = std::net::SocketAddr::from(([127, 0, 0, 1], port));
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown console argument {other:?}"
                )));
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "verlet console does not accept positional argument {other:?}"
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

pub(crate) fn parse_chat_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<ChatArgs> {
    let mut attach = None;
    let mut positionals = Vec::new();
    let mut help = false;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => help = true,
            "--config" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--config")?;
            }
            "--cwd" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--cwd")?;
            }
            "--runtime-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--runtime-home")?;
            }
            "--state-home" => {
                let _ = crate::cli::tool::required_path_value(&mut iter, "--state-home")?;
            }
            "--attach" => {
                attach = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--attach",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(crate::cli::usage_error(format!(
                    "unknown chat argument {other:?}"
                )));
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
        attach,
        prompt,
        help,
    })
}

pub(crate) fn read_env_file_if_exists(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<std::collections::BTreeMap<String, String>> {
    if !path.exists() {
        return Ok(std::collections::BTreeMap::new());
    }
    let text = std::fs::read_to_string(path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to read env file {}: {err}",
            path.display()
        ))
    })?;
    Ok(parse_env_lines(&text))
}

pub(crate) fn parse_env_lines(text: &str) -> std::collections::BTreeMap<String, String> {
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

pub(crate) fn unquote_env_value(value: &str) -> String {
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

pub(crate) fn env_or_file(
    name: &str,
    file_env: &std::collections::BTreeMap<String, String>,
) -> Option<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| file_env.get(name).cloned())
}

pub(crate) async fn manifest_receipt_event_ids(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
) -> crate::kernel::runtime_host::VerletResult<(String, String)> {
    let compile_kind = verlet_history::EventKind::ManifestCompileCompleted;
    let bind_kind = verlet_history::EventKind::ManifestBindCompleted;
    let compile_kind_name: &str = compile_kind.as_ref();
    let bind_kind_name: &str = bind_kind.as_ref();
    let response = app
        .local_json_rpc_request(
            "thread/events/list",
            serde_json::json!({
                "threadId": thread_id,
                "kinds": [compile_kind_name, bind_kind_name],
            }),
        )
        .await
        .map_err(|err| crate::cli::usage_error(format!("failed to read thread events: {err}")))?;
    let events = response["data"]
        .as_array()
        .ok_or_else(|| crate::cli::usage_error("thread/events/list response missing data array"))?;
    let compile = events
        .iter()
        .find(|event| event["kind"] == compile_kind_name)
        .ok_or_else(|| {
            crate::cli::usage_error("manifest.compile.completed receipt event was not found")
        })?;
    let bind = events
        .iter()
        .find(|event| event["kind"] == bind_kind_name)
        .ok_or_else(|| {
            crate::cli::usage_error("manifest.bind.completed receipt event was not found")
        })?;
    let compile_id = compile["eventId"].as_str().ok_or_else(|| {
        crate::cli::usage_error("manifest.compile.completed event missing eventId")
    })?;
    let bind_id = bind["eventId"]
        .as_str()
        .ok_or_else(|| crate::cli::usage_error("manifest.bind.completed event missing eventId"))?;
    Ok((compile_id.to_string(), bind_id.to_string()))
}

pub(crate) async fn run_local_app_turn(
    app: &crate::adapters::app_server::VerletAppServer,
    thread_id: &str,
    input: &str,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let parsed = verlet_runtime_contracts::ThreadId::parse_str(thread_id).map_err(|err| {
        crate::cli::usage_error(format!("invalid thread id {thread_id:?}: {err}"))
    })?;
    let handle = app.supervisor().get_thread(app.tenant_id(), parsed).await?;
    let mut events = handle.subscribe_events();
    app.local_json_rpc_request(
        "turn/start",
        serde_json::json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": input, "text_elements": [] }],
        }),
    )
    .await?;
    let mut output = String::new();
    loop {
        let event = tokio::time::timeout(std::time::Duration::from_secs(120), events.recv())
            .await
            .map_err(|_| {
                crate::cli::usage_error(format!("timed out waiting for turn on {thread_id}"))
            })?
            .map_err(|err| crate::cli::usage_error(format!("thread event stream closed: {err}")))?;
        match event {
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                output.push_str(&text);
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                match event.kind {
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                        state: verlet_runtime_contracts::RuntimeTerminalState::Completed,
                    } => return Ok(output),
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                        state,
                    } => {
                        return Err(crate::cli::usage_error(format!(
                            "turn ended before completion: {state:?}"
                        )));
                    }
                    crate::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
                        message,
                        ..
                    } => {
                        return Err(crate::cli::usage_error(format!("turn failed: {message}")));
                    }
                    _ => {}
                }
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled { reason, .. } => {
                return Err(crate::cli::usage_error(format!("turn cancelled: {reason}")));
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { .. } => {
                return Err(crate::cli::usage_error(
                    "thread stopped before turn completion",
                ));
            }
            crate::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                return Err(crate::cli::usage_error(format!("turn failed: {message}")));
            }
            _ => {}
        }
    }
}

pub(crate) fn notification_matches_thread_turn(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("threadId"))
        .and_then(serde_json::Value::as_str)
        == Some(thread_id)
        && notification
            .params
            .as_ref()
            .and_then(|params| params.get("turnId"))
            .and_then(serde_json::Value::as_str)
            == Some(turn_id)
}

pub(crate) fn notification_turn_id(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("turn"))
        .and_then(|turn| turn.get("id"))
        .and_then(serde_json::Value::as_str)
}

pub(crate) fn notification_error_message(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
) -> String {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown error")
        .to_string()
}

pub(crate) fn print_console_help() {
    println!(
        "verlet console\n\
\n\
Usage:\n\
  verlet console [--no-open] [--cwd <path>] [--config <verlet.toml>] [--port <port>]\n\
\n\
Starts the configured server and bundled browser console on 127.0.0.1. The\n\
command serves the console UI and /rpc from one loopback listener, prints\n\
the UI and RPC URLs, and opens the browser unless --no-open is set.\n"
    );
}

pub(crate) fn print_chat_help() {
    println!(
        "verlet chat\n\
\n\
Usage:\n\
  verlet chat [PROMPT] [--config <file>] [--cwd <path>]\n\
  verlet chat [PROMPT] --attach <unix://path|ws://host:port[/rpc]>\n\
\n\
Starts the bundled local terminal console over the app-server RPC boundary. By\n\
default it discovers the project instance and auto-starts an idle-bounded\n\
server when needed. --attach selects an explicit endpoint. In the TUI, use\n\
/help for session commands.\n"
    );
}
