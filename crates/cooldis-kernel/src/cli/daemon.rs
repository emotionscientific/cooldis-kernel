//! The `daemon` subcommand family and daemon configuration plumbing.

use super::*;

#[cfg(test)]
mod tests;

pub(super) async fn run_daemon(mut args: Vec<OsString>) -> CooldisResult<()> {
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

pub(super) async fn daemon_run(args: Vec<OsString>) -> CooldisResult<()> {
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

pub(super) fn daemon_app_server_config_from_loaded(
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

pub(super) fn daemon_app_server_registry_root(path: PathBuf) -> CooldisResult<PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }

    Ok(std::env::current_dir()
        .map_err(|err| usage_error(format!("failed to read current working directory: {err}")))?
        .join(path))
}

pub(super) async fn start_daemon_io(
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

pub(super) async fn start_daemon_sync(
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

pub(super) async fn remote_child_run() -> CooldisResult<()> {
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
pub(super) async fn start_thread_handle_ingress(
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

pub(super) async fn route_sink_for_ingress(
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

pub(super) async fn start_clock_route(
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

pub(super) async fn start_telegram_route(
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
    let secret_token = telegram.secret_token_value()?.ok_or_else(|| {
        usage_error(format!(
            "telegram route {} requires telegram.secret_token or telegram.secret_token_env",
            route.id
        ))
    })?;
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: route.id.clone(),
            listen,
            path: telegram.path.clone(),
            secret_token,
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

pub(super) async fn daemon_config(mut args: Vec<OsString>) -> CooldisResult<()> {
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

pub(super) async fn daemon_service(mut args: Vec<OsString>) -> CooldisResult<()> {
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

pub(super) fn daemon_service_spec_from_args(
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

#[derive(Debug)]
pub(super) struct DaemonRunArgs {
    config_path: Option<PathBuf>,
}

#[derive(Debug)]
pub(super) struct DaemonConfigValidateArgs {
    config_path: Option<PathBuf>,
}

#[derive(Debug)]
pub(super) struct DaemonServicePrintArgs {
    target: CooldisDaemonServiceTarget,
    config_path: PathBuf,
    executable: PathBuf,
    label: String,
    working_directory: Option<PathBuf>,
}

#[derive(Debug)]
pub(super) struct DaemonServiceUninstallArgs {
    target: CooldisDaemonServiceTarget,
    label: String,
}

pub(super) fn parse_daemon_run_args(args: Vec<OsString>) -> CooldisResult<DaemonRunArgs> {
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

pub(super) fn parse_daemon_config_validate_args(
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

pub(super) fn parse_daemon_service_print_args(
    args: Vec<OsString>,
) -> CooldisResult<DaemonServicePrintArgs> {
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

pub(super) fn parse_daemon_service_uninstall_args(
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

pub(super) fn default_daemon_service_target() -> CooldisDaemonServiceTarget {
    if cfg!(target_os = "macos") {
        CooldisDaemonServiceTarget::Launchd
    } else {
        CooldisDaemonServiceTarget::Systemd
    }
}

pub(super) fn ingress_persistence_mode_name(mode: IngressPersistenceMode) -> &'static str {
    match mode {
        IngressPersistenceMode::DurableQueue => "durable_queue",
        IngressPersistenceMode::BestEffortDirect => "best_effort_direct",
    }
}

pub(super) fn load_daemon_provider_config(
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

pub(super) fn provider_is_openai_compatible(provider: &str) -> bool {
    matches!(
        provider,
        "openai_compatible"
            | "openai_compatible_openai"
            | "openai_compatible_chat"
            | "openai_compatible_serverless"
    )
}

pub(super) fn chat_completions_provider_name(provider: &str) -> String {
    if provider_is_openai_compatible(provider) {
        APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string()
    } else {
        "openai_chat_completions".to_string()
    }
}

pub(super) fn provider_default_headers(provider: &str) -> Vec<(String, String)> {
    if provider_is_openai_compatible(provider) {
        vec![("X-Example-Provider".to_string(), "required".to_string())]
    } else {
        Vec::new()
    }
}

pub(super) fn print_daemon_help() {
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
