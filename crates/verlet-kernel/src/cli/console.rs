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
    let mut config = resolved.config;
    let state_home = config.state_home.clone();
    config.console_assets = Some(crate::adapters::app_server::ConsoleAssetConfig {
        root: assets,
        session_token: String::new(),
    });
    prepare_console_project_storage(&config)?;

    let server = crate::adapters::app_server::VerletAppServer::new_local(config).await?;
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
    server.serve_websocket_listener(listener).await
}

#[cfg(test)]
pub(crate) fn console_app_server_config(
    options: &ConsoleArgs,
    listen: crate::adapters::app_server::AppServerListenAddr,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::app_server::VerletAppServerConfig> {
    resolve_console_app_server_config(options, listen).map(|resolved| resolved.config)
}

pub(crate) struct ResolvedConsoleAppServerConfig {
    config: crate::adapters::app_server::VerletAppServerConfig,
    project_root: std::path::PathBuf,
    config_path: Option<std::path::PathBuf>,
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

    Ok(ResolvedConsoleAppServerConfig {
        config,
        project_root: env.project_root,
        config_path: loaded.path,
    })
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
    pub(crate) cwd: std::path::PathBuf,
    config_path: Option<std::path::PathBuf>,
    env_file: Option<std::path::PathBuf>,
    runtime_home: Option<std::path::PathBuf>,
    state_home: Option<std::path::PathBuf>,
    provider: Option<String>,
    base_url: Option<String>,
    api_key: Option<String>,
    api_key_env: Option<String>,
    model: Option<String>,
    max_tokens: Option<u32>,
    stream: Option<bool>,
    pub(crate) attach: Option<String>,
    pub(crate) prompt: Option<String>,
    pub(crate) help: bool,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ChatConfigFile {
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
    env_file: Option<std::path::PathBuf>,
    #[serde(default, alias = "capsuleBindings")]
    capsule_bindings: Option<crate::adapters::app_server::CapsuleBindingsConfig>,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(crate) struct ChatConfigSection {
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
    env_file: Option<std::path::PathBuf>,
    #[serde(default, alias = "capsuleBindings")]
    capsule_bindings: Option<crate::adapters::app_server::CapsuleBindingsConfig>,
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
    let mut cwd = std::env::current_dir().map_err(|err| {
        crate::cli::usage_error(format!("failed to read current working directory: {err}"))
    })?;
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
            "--config" => {
                config_path = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            "--env-file" => {
                env_file = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--env-file",
                )?)
            }
            "--cwd" => {
                cwd = std::path::PathBuf::from(crate::cli::tool::required_string_value(
                    &mut iter, "--cwd",
                )?)
            }
            "--runtime-home" => {
                runtime_home = Some(std::path::PathBuf::from(
                    crate::cli::tool::required_string_value(&mut iter, "--runtime-home")?,
                ));
            }
            "--state-home" => {
                state_home = Some(std::path::PathBuf::from(
                    crate::cli::tool::required_string_value(&mut iter, "--state-home")?,
                ));
            }
            "--provider" => {
                provider = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--provider",
                )?)
            }
            "--base-url" => {
                base_url = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--base-url",
                )?)
            }
            "--api-key" => {
                api_key = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--api-key",
                )?)
            }
            "--api-key-env" => {
                api_key_env = Some(crate::cli::tool::required_string_value(
                    &mut iter,
                    "--api-key-env",
                )?)
            }
            "--model" => {
                model = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--model",
                )?)
            }
            "--max-tokens" => {
                let value = crate::cli::tool::required_string_value(&mut iter, "--max-tokens")?;
                max_tokens = Some(value.parse().map_err(|_| {
                    crate::cli::usage_error("--max-tokens must be a positive integer")
                })?);
            }
            "--stream" => stream = Some(true),
            "--no-stream" => stream = Some(false),
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

pub(crate) fn load_chat_provider_config(
    args: &ChatArgs,
) -> crate::kernel::runtime_host::VerletResult<ChatProviderConfig> {
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
        "openai-codex" | "openai_codex" => Ok(ChatProviderConfig::OpenAICodex {
            model: config.model.clone().unwrap_or_else(|| {
                verlet_metadata::provider_store::OPENAI_CODEX_DEFAULT_MODEL.to_string()
            }),
            max_tokens: config.max_tokens.unwrap_or(4096),
            stream: config.stream.unwrap_or(true),
        }),
        "bifrost" | "bifrost_openai" | "openai" | "openai_responses" => {
            let env_file = config
                .env_file
                .clone()
                .map(|path| {
                    crate::cli::tool::resolve_config_path(
                        config_base.as_deref().unwrap_or(std::path::Path::new(".")),
                        path,
                    )
                })
                .or_else(|| {
                    std::env::var("VERLET_CHAT_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("VERLET_BIFROST_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("VERLET_BIFROST_URL", &file_env))
                .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Bifrost chat provider requires chat.base_url, VERLET_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
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
                .or_else(|| env_or_file("VERLET_BIFROST_KEY", &file_env))
                .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Bifrost chat provider requires chat.api_key, chat.api_key_env, VERLET_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("VERLET_BIFROST_OPENAI_MODEL", &file_env))
                .unwrap_or_else(|| {
                    crate::adapters::app_server::APP_SERVER_BIFROST_MODEL.to_string()
                });
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
                    crate::cli::tool::resolve_config_path(
                        config_base.as_deref().unwrap_or(std::path::Path::new(".")),
                        path,
                    )
                })
                .or_else(|| {
                    std::env::var("VERLET_CHAT_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("VERLET_ANTHROPIC_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| env_or_file("VERLET_ANTHROPIC_URL", &file_env))
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
                    crate::cli::usage_error(
                        "Anthropic chat provider requires chat.api_key, chat.api_key_env, or ANTHROPIC_API_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| env_or_file("VERLET_ANTHROPIC_MODEL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_MODEL", &file_env))
                .unwrap_or_else(|| {
                    crate::adapters::app_server::APP_SERVER_ANTHROPIC_MODEL.to_string()
                });
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
                    crate::cli::tool::resolve_config_path(
                        config_base.as_deref().unwrap_or(std::path::Path::new(".")),
                        path,
                    )
                })
                .or_else(|| {
                    std::env::var("VERLET_CHAT_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("VERLET_BEDROCK_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("VERLET_ANTHROPIC_BEDROCK_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
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
                .or_else(|| env_or_file("VERLET_BEDROCK_BASE_URL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_BEDROCK_BASE_URL", &file_env))
                .map(|url| url.trim_end_matches('/').to_string());
            let access_key_id = config
                .aws_access_key_id
                .clone()
                .or_else(|| env_or_file("AWS_ACCESS_KEY_ID", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Anthropic Bedrock provider requires AWS_ACCESS_KEY_ID or chat.aws_access_key_id",
                    )
                })?;
            let secret_access_key = config
                .aws_secret_access_key
                .clone()
                .or_else(|| env_or_file("AWS_SECRET_ACCESS_KEY", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
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
                .or_else(|| env_or_file("VERLET_ANTHROPIC_BEDROCK_MODEL", &file_env))
                .or_else(|| env_or_file("AWS_BEDROCK_MODEL", &file_env))
                .or_else(|| env_or_file("ANTHROPIC_DEFAULT_SONNET_MODEL", &file_env))
                .unwrap_or_else(|| {
                    crate::adapters::app_server::APP_SERVER_ANTHROPIC_BEDROCK_MODEL.to_string()
                });
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
            let openai_compatible = crate::cli::daemon::provider_is_openai_compatible(provider);
            let env_file = config
                .env_file
                .clone()
                .map(|path| {
                    crate::cli::tool::resolve_config_path(
                        config_base.as_deref().unwrap_or(std::path::Path::new(".")),
                        path,
                    )
                })
                .or_else(|| {
                    std::env::var("VERLET_CHAT_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    if openai_compatible {
                        std::env::var("VERLET_OPENAI_COMPATIBLE_ENV_FILE")
                            .ok()
                            .map(std::path::PathBuf::from)
                    } else {
                        None
                    }
                })
                .or_else(|| {
                    std::env::var("VERLET_BIFROST_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            let file_env = read_env_file_if_exists(&env_file)?;
            if openai_compatible
                && config.base_url.is_none()
                && config.api_key.is_none()
                && config.api_key_env.is_none()
                && !file_env.contains_key("VERLET_OPENAI_COMPATIBLE_API_KEY")
                && !file_env.contains_key("OPENAI_COMPATIBLE_API_KEY")
            {
                let model = config
                    .model
                    .clone()
                    .or_else(|| env_or_file("VERLET_OPENAI_COMPATIBLE_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env));
                return Ok(ChatProviderConfig::CatalogOpenAIChatCompletions {
                    provider_id: crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER
                        .to_string(),
                    model,
                    max_tokens: config.max_tokens.unwrap_or(4096),
                    stream: config.stream.unwrap_or(true),
                });
            }
            let base_url = if openai_compatible {
                config
                    .base_url
                    .clone()
                    .or_else(|| env_or_file("VERLET_OPENAI_COMPATIBLE_URL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_BASE_URL", &file_env))
                    .unwrap_or_else(|| "https://api.example.invalid/v1".to_string())
            } else {
                config
                    .base_url
                    .clone()
                    .or_else(|| env_or_file("VERLET_BIFROST_URL", &file_env))
                    .or_else(|| env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                    .or_else(|| env_or_file("LLM_PROXY_URL", &file_env))
                    .ok_or_else(|| {
                        crate::cli::usage_error(
                            "OpenAI Chat Completions provider requires chat.base_url, VERLET_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
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
                    .or_else(|| env_or_file("VERLET_OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .ok_or_else(|| {
                        crate::cli::usage_error(
                            "OpenAI Compatible chat provider requires chat.api_key, chat.api_key_env, VERLET_OPENAI_COMPATIBLE_API_KEY, or OPENAI_COMPATIBLE_API_KEY",
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
                    .or_else(|| env_or_file("VERLET_BIFROST_KEY", &file_env))
                    .or_else(|| env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                    .or_else(|| env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                    .ok_or_else(|| {
                        crate::cli::usage_error(
                            "OpenAI Chat Completions provider requires chat.api_key, chat.api_key_env, VERLET_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                        )
                    })?
            };
            let model = if openai_compatible {
                config
                    .model
                    .clone()
                    .or_else(|| env_or_file("VERLET_OPENAI_COMPATIBLE_MODEL", &file_env))
                    .or_else(|| env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env))
                    .unwrap_or_else(|| {
                        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string()
                    })
            } else {
                config
                    .model
                    .clone()
                    .or_else(|| env_or_file("VERLET_BIFROST_OPENAI_CHAT_MODEL", &file_env))
                    .or_else(|| env_or_file("VERLET_BIFROST_OPENAI_MODEL", &file_env))
                    .unwrap_or_else(|| {
                        crate::adapters::app_server::APP_SERVER_BIFROST_MODEL.to_string()
                    })
            };
            Ok(ChatProviderConfig::OpenAIChatCompletions {
                provider: crate::cli::daemon::chat_completions_provider_name(provider),
                base_url,
                api_key,
                model,
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
                headers: crate::cli::daemon::provider_default_headers(provider),
            })
        }
        other => Err(crate::cli::usage_error(format!(
            "unknown chat provider {other:?}; expected local, openai-codex, bifrost_openai, openai_chat_completions, anthropic, anthropic_bedrock, or openai_compatible"
        ))),
    }
}

pub(crate) fn load_chat_capsule_bindings_config(
    args: &ChatArgs,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::app_server::CapsuleBindingsConfig> {
    let (config, config_base) = load_chat_config_file(args.config_path.as_deref())?;
    let mut capsule_bindings = config.capsule_bindings.unwrap_or_default();
    if let Some(registry_root) = capsule_bindings.registry_root.take() {
        capsule_bindings.registry_root = Some(match config_base.as_deref() {
            Some(base) => crate::cli::tool::resolve_config_path(base, registry_root),
            None => registry_root,
        });
    }
    Ok(capsule_bindings)
}

pub(crate) fn load_chat_config_file(
    path: Option<&std::path::Path>,
) -> crate::kernel::runtime_host::VerletResult<(ChatConfigSection, Option<std::path::PathBuf>)> {
    let discovered;
    let path = if let Some(path) = path {
        path
    } else {
        discovered = std::path::PathBuf::from("verlet.json");
        if !discovered.exists() {
            return Ok((ChatConfigSection::default(), None));
        }
        discovered.as_path()
    };
    let bytes = std::fs::read(path).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to read chat config {}: {err}",
            path.display()
        ))
    })?;
    let file: ChatConfigFile = serde_json::from_slice(&bytes).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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

pub(crate) struct PrivateAppServer {
    listen: crate::adapters::app_server::AppServerListenAddr,
    root: std::path::PathBuf,
    server: crate::adapters::app_server::VerletAppServer,
    task: Option<tokio::task::JoinHandle<crate::kernel::runtime_host::VerletResult<()>>>,
}

impl PrivateAppServer {
    pub(crate) async fn start(
        options: &ChatArgs,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let root = std::path::PathBuf::from("/tmp")
            .join(format!("cdis-chat-{}", uuid::Uuid::now_v7().simple()));
        let listen =
            crate::adapters::app_server::AppServerListenAddr::Unix(root.join("app-server.sock"));
        let provider = load_chat_provider_config(options)?;
        // lexicon-allow: capsule - existing app-server operation binding API name
        let capsule_bindings = load_chat_capsule_bindings_config(options)?;
        let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
            listen.clone(),
            options.cwd.clone(),
        );
        config.runtime_home = options
            .runtime_home
            .clone()
            .unwrap_or_else(|| root.join("runtime"));
        config.state_home = options
            .state_home
            .clone()
            .unwrap_or_else(|| root.join("state"));
        config.user_state_home = crate::cli::secret::default_user_state_home()?;
        // lexicon-allow: capsule - existing app-server operation binding API name
        config.capsule_bindings = capsule_bindings;
        apply_chat_provider_config(&mut config, provider);

        let server = crate::adapters::app_server::VerletAppServer::new_local(config).await?;
        let serve_listen = listen.clone();
        let serve_server = server.clone();
        let task = tokio::spawn(async move { serve_server.serve(serve_listen).await });
        wait_for_private_socket(socket_path(&listen)).await?;
        Ok(Self {
            listen,
            root,
            server,
            task: Some(task),
        })
    }

    pub(crate) fn socket_path(&self) -> &std::path::Path {
        socket_path(&self.listen)
    }

    pub(crate) async fn shutdown(mut self) -> crate::kernel::runtime_host::VerletResult<()> {
        let shutdown = self.server.shutdown().await;
        let serving = match self.task.take() {
            Some(task) => task.await.map_err(|error| {
                crate::cli::usage_error(format!(
                    "private app-server task failed during shutdown: {error}"
                ))
            })?,
            None => Ok(()),
        };
        shutdown?;
        serving
    }
}

impl Drop for PrivateAppServer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

pub(crate) fn socket_path(
    listen: &crate::adapters::app_server::AppServerListenAddr,
) -> &std::path::Path {
    match listen {
        crate::adapters::app_server::AppServerListenAddr::Unix(path) => path.as_path(),
        crate::adapters::app_server::AppServerListenAddr::WebSocket(_) => {
            unreachable!("private chat app-server always listens on a Unix socket")
        }
    }
}

pub(crate) async fn wait_for_private_socket(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<()> {
    for _ in 0..100 {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    Err(crate::cli::usage_error(format!(
        "timed out waiting for private app-server socket {}",
        path.display()
    )))
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
Starts the bundled local browser console on 127.0.0.1. The command serves the\n\
console UI and the /rpc WebSocket endpoint from one loopback listener, prints\n\
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
  verlet chat [PROMPT] --provider openai-codex [--model <model>]\n\
  verlet chat [PROMPT] --provider bifrost_openai --base-url <url> --api-key-env <env> [--model <model>]\n\
\n\
Starts the bundled local terminal console over the app-server RPC boundary. By\n\
default it launches a private local app-server; --attach connects to an existing\n\
endpoint. In the TUI, use /help for session commands.\n"
    );
}
