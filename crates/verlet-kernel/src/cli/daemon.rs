//! The `daemon` subcommand family and daemon configuration plumbing.

use std::io::Read as _;
#[cfg(test)]
mod tests;

pub(crate) async fn run_daemon(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
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
        "config" => daemon_config(args).await,
        "service" => daemon_service(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown daemon subcommand {other:?}"
        ))),
    }
}

pub(crate) fn daemon_app_server_config_from_loaded(
    loaded: &crate::daemon::daemon_config::LoadedVerletDaemonConfig,
) -> crate::kernel::runtime_host::VerletResult<crate::adapters::app_server::VerletAppServerConfig> {
    loaded.config.validate()?;
    let listen = loaded.config.app_server.listen_addr()?;
    let cwd = match loaded.config.runtime.cwd.clone() {
        Some(cwd) => cwd,
        None => std::env::current_dir().map_err(|err| {
            crate::cli::usage_error(format!("failed to read current working directory: {err}"))
        })?,
    };
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(listen, cwd);
    config.apply_daemon_identity_config(&loaded.config.identity);
    if let Some(runtime_home) = loaded.config.runtime.runtime_home.clone() {
        config.runtime_home = runtime_home;
    }
    if let Some(state_home) = loaded.config.runtime.state_home.clone() {
        config.state_home = state_home;
    }
    config.default_placement = loaded.config.runtime.placement.clone().unwrap_or_default();
    config.default_workspace = loaded.config.runtime.workspace.clone();
    config.lease_epoch = loaded.config.runtime.lease_epoch;
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

    crate::cli::console::apply_chat_provider_config(
        &mut config,
        load_daemon_provider_config(&loaded.config.provider)?,
    );

    Ok(config)
}

pub(crate) fn daemon_app_server_registry_root(
    path: std::path::PathBuf,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    if path.is_absolute() {
        return Ok(path);
    }

    Ok(std::env::current_dir()
        .map_err(|err| {
            crate::cli::usage_error(format!("failed to read current working directory: {err}"))
        })?
        .join(path))
}

pub(crate) async fn start_daemon_io(
    io: &crate::daemon::daemon_config::VerletIoConfig,
    sync: &crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig,
    daemon_config_path: Option<std::path::PathBuf>,
    server: &crate::adapters::app_server::VerletAppServer,
) -> crate::kernel::runtime_host::VerletResult<Vec<tokio::task::JoinHandle<()>>> {
    let bridge = crate::daemon::daemon_io::VerletDaemonIoBridge::from_app_server(server);
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
                    .register_egress_route_config(
                        verlet_io_telegram::TELEGRAM_PROTOCOL,
                        route.id.clone(),
                        route,
                    )
                    .await?;
                let sink = route_sink_for_ingress(route, ingress, &bridge, &mut tasks).await?;
                start_telegram_route(route, sink, &bridge, egress_state_dsn, &mut tasks).await?;
            }
            other => {
                eprintln!(
                    "verlet daemon IO route {} ({other}) has no listener in this daemon slice",
                    route.id
                );
            }
        }
    }
    start_daemon_sync(sync, daemon_config_path, server, &mut tasks).await?;

    if !io.routes.is_empty() {
        eprintln!(
            "verlet daemon loaded {} IO route(s), {} task(s) active",
            io.routes.len(),
            tasks.len()
        );
    }
    Ok(tasks)
}

pub(crate) async fn start_daemon_sync(
    config: &crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig,
    daemon_config_path: Option<std::path::PathBuf>,
    app_server: &crate::adapters::app_server::VerletAppServer,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(listen) = config.listen_addr()? else {
        return Ok(());
    };
    let store = verlet_history_sqlite::SqliteSessionStore::open(app_server.session_store_path())
        .await
        .map_err(|error| crate::kernel::runtime_host::VerletError::History(error.to_string()))?
        .with_lease_epoch(app_server.lease_epoch());
    let clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock> =
        std::sync::Arc::new(crate::daemon::clock_route::SystemDaemonClock);
    let authority = std::sync::Arc::new(
        crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
            store.clone(),
            config.clone(),
            std::sync::Arc::clone(&clock),
        )
        .await?,
    );
    let endpoint = std::sync::Arc::new(
        crate::daemon::remote_store::endpoint::SqliteSyncEndpoint::new(
            store.clone(),
            std::sync::Arc::clone(&authority),
            clock,
        )
        .await?,
    );
    let server =
        crate::daemon::remote_store::endpoint_http::DaemonSyncHttpServer::bind(listen, endpoint)
            .await?;
    let sync_endpoint = server.display_addr()?;
    let child_root = app_server
        .session_store_path()
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("remote-children");
    let executor = std::sync::Arc::new(
        crate::daemon::remote_store::process_executor::ProcessRemoteThreadExecutor::new(
            store,
            authority,
            sync_endpoint.clone(),
            daemon_config_path,
            child_root,
            std::env::current_exe().map_err(|error| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
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
    eprintln!("verlet daemon sync endpoint listening on {}", sync_endpoint);
    tasks.push(tokio::spawn(async move {
        if let Err(error) = server.serve().await {
            eprintln!("verlet daemon sync endpoint stopped: {error}");
        }
    }));
    Ok(())
}

pub(crate) async fn remote_child_run() -> crate::kernel::runtime_host::VerletResult<()> {
    let mut encoded = Vec::new();
    std::io::stdin()
        .read_to_end(&mut encoded)
        .map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "failed to read remote child bootstrap: {error}"
            ))
        })?;
    let bootstrap = serde_json::from_slice::<
        crate::daemon::remote_store::process_executor::RemoteChildBootstrapV1,
    >(&encoded)
    .map_err(|error| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "failed to decode remote child bootstrap: {error}"
        ))
    })?;
    let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(
        bootstrap.daemon_config_path.as_deref(),
    )?;
    let config = daemon_app_server_config_from_loaded(&loaded)?;
    crate::daemon::remote_store::process_executor::run_remote_child(config, bootstrap).await
}

/// Starts the push-first settlement lane independently of external route
/// policy. Handle outcomes always require the durable queue even when an
/// operator has explicitly made a protocol route best-effort direct.
pub(crate) async fn start_thread_handle_ingress(
    ingress: &crate::daemon::daemon_config::VerletIngressConfig,
    server: &crate::adapters::app_server::VerletAppServer,
    bridge: &crate::daemon::daemon_io::VerletDaemonIoBridge,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let queue_name = ingress
        .persistence
        .queue_name
        .clone()
        .unwrap_or_else(|| "verlet-ingress".to_string());
    let queue_config =
        verlet_io_pgqrs::PgqrsQueueConfig::new(ingress.effective_queue_dsn(), queue_name)
            .with_default_visibility_timeout_secs(ingress.persistence.visibility_timeout_secs);
    let queue = std::sync::Arc::new(
        verlet_io_pgqrs::PgqrsIngressQueue::connect(queue_config)
            .await
            .map_err(crate::cli::io_error)?,
    );
    let store = verlet_history_sqlite::SqliteSessionStore::open(server.session_store_path())
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        .with_lease_epoch(server.lease_epoch());
    let worker = crate::daemon::daemon_io::VerletDaemonQueueWorker::new(
        queue.clone(),
        bridge.clone(),
        "thread-handle-outcome-worker",
        ingress.persistence.visibility_timeout_secs,
    );
    tasks.push(tokio::spawn(worker.run()));
    tasks.push(tokio::spawn(
        crate::daemon::handle_ingress::ThreadHandleIngressAdapter::new(
            store,
            queue,
            server.tenant_id(),
            server.user_id(),
        )
        .run(),
    ));
    Ok(())
}

pub(crate) async fn route_sink_for_ingress(
    route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    ingress: &crate::daemon::daemon_config::VerletIngressConfig,
    bridge: &crate::daemon::daemon_io::VerletDaemonIoBridge,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> crate::kernel::runtime_host::VerletResult<std::sync::Arc<dyn verlet_io_core::IngressSink>> {
    let inner: std::sync::Arc<dyn verlet_io_core::IngressSink> = match ingress.persistence.mode {
        verlet_io_core::IngressPersistenceMode::BestEffortDirect => bridge.direct_sink(),
        verlet_io_core::IngressPersistenceMode::DurableQueue => {
            let queue_config = verlet_io_pgqrs::PgqrsQueueConfig::from_persistence_config(
                ingress.effective_queue_dsn(),
                &ingress.persistence,
            )
            .map_err(crate::cli::io_error)?
            .ok_or_else(|| {
                crate::cli::usage_error("durable queue persistence did not return a queue")
            })?;
            let queue = std::sync::Arc::new(
                verlet_io_pgqrs::PgqrsIngressQueue::connect(queue_config)
                    .await
                    .map_err(crate::cli::io_error)?,
            );
            let worker = crate::daemon::daemon_io::VerletDaemonQueueWorker::new(
                queue.clone(),
                bridge.clone(),
                format!("{}-worker", route.id),
                ingress.persistence.visibility_timeout_secs,
            );
            tasks.push(tokio::spawn(worker.run()));
            queue
        }
    };
    let (tenant_id, principal_id) = bridge.route_identity();
    Ok(std::sync::Arc::new(
        crate::daemon::daemon_io::RouteIngressSink::with_route_identity(
            inner,
            route,
            tenant_id,
            principal_id,
        ),
    ))
}

pub(crate) async fn start_clock_route(
    route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
    server: &crate::adapters::app_server::VerletAppServer,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let store = verlet_history_sqlite::SqliteSessionStore::open(server.session_store_path())
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        .with_lease_epoch(server.lease_epoch());
    let clock = crate::daemon::clock_route::VerletDaemonClockRoute::new(
        route.id.clone(),
        store,
        sink,
        std::sync::Arc::new(crate::daemon::clock_route::SystemDaemonClock),
    );
    eprintln!(
        "verlet clock route {} polling active mandates every 30s",
        route.id
    );
    tasks.push(tokio::spawn(clock.run()));
    Ok(())
}

pub(crate) async fn start_telegram_route(
    route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
    bridge: &crate::daemon::daemon_io::VerletDaemonIoBridge,
    egress_state_dsn: String,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let telegram = route.telegram.as_ref().ok_or_else(|| {
        crate::cli::usage_error(format!(
            "telegram route {} requires [daemon.io.routes.telegram]",
            route.id
        ))
    })?;
    if let Some(bot_token) = telegram.bot_token_value()? {
        let client = match &telegram.api_base {
            Some(api_base) => verlet_io_telegram::TelegramBotClient::new(bot_token)
                .with_api_base(api_base.clone()),
            None => verlet_io_telegram::TelegramBotClient::new(bot_token),
        };
        bridge
            .register_egress_adapter(
                verlet_io_telegram::TELEGRAM_PROTOCOL,
                route.id.clone(),
                std::sync::Arc::new(verlet_io_telegram::TelegramEgressAdapter::with_client(
                    route.id.clone(),
                    client,
                )),
            )
            .await;
    }
    let projector = bridge
        .start_egress_projector_sqlite_dsn(
            verlet_io_telegram::TELEGRAM_PROTOCOL,
            route.id.clone(),
            egress_state_dsn,
        )
        .await
        .map_err(crate::cli::io_error)?;
    tasks.push(projector);
    let listen = telegram.listen.clone().ok_or_else(|| {
        crate::cli::usage_error(format!(
            "telegram route {} requires telegram.listen",
            route.id
        ))
    })?;
    let secret_token = telegram.secret_token_value()?.ok_or_else(|| {
        crate::cli::usage_error(format!(
            "telegram route {} requires telegram.secret_token or telegram.secret_token_env",
            route.id
        ))
    })?;
    let server = crate::daemon::daemon_io::TelegramWebhookServer::bind(
        crate::daemon::daemon_io::TelegramWebhookServerConfig {
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
        "verlet Telegram route {} listening on http://{}{}",
        route.id, addr, telegram.path
    );
    tasks.push(tokio::spawn(async move {
        if let Err(err) = server.serve().await {
            eprintln!("verlet Telegram webhook server stopped: {err}");
        }
    }));
    Ok(())
}

pub(crate) async fn daemon_config(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty() {
        return Err(crate::cli::usage_error(
            "daemon config requires a subcommand",
        ));
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "validate" => {
            let options = parse_daemon_config_validate_args(args)?;
            let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(
                options.config_path.as_deref(),
            )?;
            println!("verlet daemon config ok");
            println!(
                "config {}",
                loaded
                    .path
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<defaults>".to_string())
            );
            println!("app_server.listen {}", loaded.config.app_server.listen);
            let mode_name: &'static str = loaded.config.io.ingress.persistence.mode.into();
            println!("io.ingress.persistence {mode_name}");
            println!("io.routes {}", loaded.config.io.routes.len());
            Ok(())
        }
        other => Err(crate::cli::usage_error(format!(
            "unknown daemon config subcommand {other:?}"
        ))),
    }
}

pub(crate) async fn daemon_service(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty() {
        return Err(crate::cli::usage_error(
            "daemon service requires a subcommand",
        ));
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "print" => {
            let options = parse_daemon_service_print_args(args)?;
            let spec = daemon_service_spec_from_args(&options)?;
            print!(
                "{}",
                crate::daemon::daemon_config::render_verlet_daemon_service(options.target, &spec)
            );
            Ok(())
        }
        "install" => {
            let options = parse_daemon_service_print_args(args)?;
            let spec = daemon_service_spec_from_args(&options)?;
            let path =
                crate::daemon::daemon_config::install_verlet_daemon_service(options.target, &spec)?;
            println!("installed {}", path.display());
            println!("service was not started automatically");
            match options.target {
                crate::daemon::daemon_config::VerletDaemonServiceTarget::Launchd => {
                    println!("start with: launchctl load {}", path.display());
                }
                crate::daemon::daemon_config::VerletDaemonServiceTarget::Systemd => {
                    println!("start with: systemctl --user enable --now {}", spec.label);
                }
            }
            Ok(())
        }
        "uninstall" => {
            let options = parse_daemon_service_uninstall_args(args)?;
            match crate::daemon::daemon_config::uninstall_verlet_daemon_service(
                options.target,
                &options.label,
            )? {
                Some(path) => println!("removed {}", path.display()),
                None => println!("service not installed for label {}", options.label),
            }
            Ok(())
        }
        other => Err(crate::cli::usage_error(format!(
            "unknown daemon service subcommand {other:?}"
        ))),
    }
}

pub(crate) fn daemon_service_spec_from_args(
    options: &DaemonServicePrintArgs,
) -> crate::kernel::runtime_host::VerletResult<crate::daemon::daemon_config::VerletDaemonServiceSpec>
{
    crate::daemon::daemon_config::load_verlet_daemon_config(Some(&options.config_path))?;
    let mut spec = crate::daemon::daemon_config::VerletDaemonServiceSpec::new(
        options.executable.clone(),
        options.config_path.clone(),
    )
    .with_label(options.label.clone());
    if let Some(working_directory) = &options.working_directory {
        spec = spec.with_working_directory(working_directory.clone());
    }
    Ok(spec)
}

#[derive(Debug)]
pub(crate) struct DaemonConfigValidateArgs {
    config_path: Option<std::path::PathBuf>,
}

#[derive(Debug)]
pub(crate) struct DaemonServicePrintArgs {
    target: crate::daemon::daemon_config::VerletDaemonServiceTarget,
    config_path: std::path::PathBuf,
    executable: std::path::PathBuf,
    label: String,
    working_directory: Option<std::path::PathBuf>,
}

#[derive(Debug)]
pub(crate) struct DaemonServiceUninstallArgs {
    target: crate::daemon::daemon_config::VerletDaemonServiceTarget,
    label: String,
}

pub(crate) fn parse_daemon_config_validate_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DaemonConfigValidateArgs> {
    let mut config_path = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--config" => {
                config_path = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown daemon config validate argument {other:?}"
                )));
            }
        }
    }
    Ok(DaemonConfigValidateArgs { config_path })
}

pub(crate) fn parse_daemon_service_print_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DaemonServicePrintArgs> {
    let mut target = default_daemon_service_target();
    let mut config_path = None;
    let mut executable = None;
    let mut label = "com.verlet.daemon".to_string();
    let mut working_directory = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--target" => {
                let value = crate::cli::tool::required_string_value(&mut iter, "--target")?;
                target = parse_daemon_service_target(&value)?;
            }
            "--config" => {
                config_path = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            "--bin" | "--executable" => {
                executable = Some(crate::cli::tool::required_path_value(&mut iter, "--bin")?)
            }
            "--label" => label = crate::cli::tool::required_string_value(&mut iter, "--label")?,
            "--working-directory" | "--cwd" => {
                working_directory = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--working-directory",
                )?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown daemon service print argument {other:?}"
                )));
            }
        }
    }

    let config_path = match config_path {
        Some(path) => path,
        None => crate::daemon::daemon_config::discover_verlet_daemon_config_path()?.ok_or_else(
            || {
                crate::cli::usage_error(
                    "daemon service print requires --config when no verlet.toml exists",
                )
            },
        )?,
    };
    let executable = match executable {
        Some(path) => path,
        None => std::env::current_exe().map_err(|err| {
            crate::cli::usage_error(format!("failed to read current executable: {err}"))
        })?,
    };

    Ok(DaemonServicePrintArgs {
        target,
        config_path,
        executable,
        label,
        working_directory,
    })
}

pub(crate) fn parse_daemon_service_uninstall_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DaemonServiceUninstallArgs> {
    let mut target = default_daemon_service_target();
    let mut label = "com.verlet.daemon".to_string();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--target" => {
                let value = crate::cli::tool::required_string_value(&mut iter, "--target")?;
                target = parse_daemon_service_target(&value)?;
            }
            "--label" => label = crate::cli::tool::required_string_value(&mut iter, "--label")?,
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown daemon service uninstall argument {other:?}"
                )));
            }
        }
    }

    Ok(DaemonServiceUninstallArgs { target, label })
}

fn parse_daemon_service_target(
    value: &str,
) -> crate::kernel::runtime_host::VerletResult<
    crate::daemon::daemon_config::VerletDaemonServiceTarget,
> {
    value.parse().map_err(|_| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "unknown daemon service target {value:?}; expected launchd or systemd"
        ))
    })
}

pub(crate) fn default_daemon_service_target()
-> crate::daemon::daemon_config::VerletDaemonServiceTarget {
    if cfg!(target_os = "macos") {
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Launchd
    } else {
        crate::daemon::daemon_config::VerletDaemonServiceTarget::Systemd
    }
}

pub(crate) fn load_daemon_provider_config(
    config: &crate::daemon::daemon_config::VerletProviderConfig,
) -> crate::kernel::runtime_host::VerletResult<crate::cli::console::ChatProviderConfig> {
    match config.provider_name() {
        "local" | "local_offline" | "offline" => Ok(crate::cli::console::ChatProviderConfig::Local),
        "openai-codex" | "openai_codex" => {
            Ok(crate::cli::console::ChatProviderConfig::OpenAICodex {
                model: config.model.clone().unwrap_or_else(|| {
                    verlet_metadata::provider_store::OPENAI_CODEX_DEFAULT_MODEL.to_string()
                }),
                max_tokens: config.max_tokens.unwrap_or(4096),
                stream: config.stream.unwrap_or(true),
            })
        }
        "bifrost" | "bifrost_openai" => {
            let env_file = config
                .env_file
                .clone()
                .or_else(|| {
                    std::env::var("VERLET_DAEMON_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("VERLET_BIFROST_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            let file_env = crate::cli::console::read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| crate::cli::console::env_or_file("VERLET_BIFROST_URL", &file_env))
                .or_else(|| crate::cli::console::env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                .or_else(|| crate::cli::console::env_or_file("LLM_PROXY_URL", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Bifrost daemon provider requires provider.base_url, VERLET_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
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
                        .and_then(|name| crate::cli::console::env_or_file(name, &file_env))
                })
                .or_else(|| crate::cli::console::env_or_file("VERLET_BIFROST_KEY", &file_env))
                .or_else(|| crate::cli::console::env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                .or_else(|| crate::cli::console::env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Bifrost daemon provider requires provider.api_key, provider.api_key_env, VERLET_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| {
                    crate::cli::console::env_or_file("VERLET_BIFROST_OPENAI_MODEL", &file_env)
                })
                .unwrap_or_else(|| {
                    crate::adapters::app_server::APP_SERVER_BIFROST_MODEL.to_string()
                });
            Ok(crate::cli::console::ChatProviderConfig::BifrostOpenAI {
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
                    std::env::var("VERLET_DAEMON_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .or_else(|| {
                    std::env::var("VERLET_ANTHROPIC_ENV_FILE")
                        .ok()
                        .map(std::path::PathBuf::from)
                })
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            let file_env = crate::cli::console::read_env_file_if_exists(&env_file)?;
            let base_url = config
                .base_url
                .clone()
                .or_else(|| crate::cli::console::env_or_file("VERLET_ANTHROPIC_URL", &file_env))
                .or_else(|| crate::cli::console::env_or_file("ANTHROPIC_BASE_URL", &file_env))
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
                        .and_then(|name| crate::cli::console::env_or_file(name, &file_env))
                })
                .or_else(|| crate::cli::console::env_or_file("ANTHROPIC_API_KEY", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Anthropic daemon provider requires provider.api_key, provider.api_key_env, or ANTHROPIC_API_KEY",
                    )
                })?;
            let model = config
                .model
                .clone()
                .or_else(|| crate::cli::console::env_or_file("VERLET_ANTHROPIC_MODEL", &file_env))
                .or_else(|| crate::cli::console::env_or_file("ANTHROPIC_MODEL", &file_env))
                .unwrap_or_else(|| {
                    crate::adapters::app_server::APP_SERVER_ANTHROPIC_MODEL.to_string()
                });
            Ok(crate::cli::console::ChatProviderConfig::AnthropicMessages {
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
                    std::env::var("VERLET_DAEMON_ENV_FILE")
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
            let file_env = crate::cli::console::read_env_file_if_exists(&env_file)?;
            let region = config
                .region
                .clone()
                .or_else(|| crate::cli::console::env_or_file("AWS_BEDROCK_REGION", &file_env))
                .or_else(|| crate::cli::console::env_or_file("AWS_REGION", &file_env))
                .or_else(|| crate::cli::console::env_or_file("AWS_DEFAULT_REGION", &file_env))
                .unwrap_or_else(|| "us-east-1".to_string());
            let base_url = config
                .base_url
                .clone()
                .or_else(|| crate::cli::console::env_or_file("VERLET_BEDROCK_BASE_URL", &file_env))
                .or_else(|| {
                    crate::cli::console::env_or_file("ANTHROPIC_BEDROCK_BASE_URL", &file_env)
                })
                .map(|url| url.trim_end_matches('/').to_string());
            let access_key_id = config
                .aws_access_key_id
                .clone()
                .or_else(|| crate::cli::console::env_or_file("AWS_ACCESS_KEY_ID", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Anthropic Bedrock daemon provider requires AWS_ACCESS_KEY_ID or provider.aws_access_key_id",
                    )
                })?;
            let secret_access_key = config
                .aws_secret_access_key
                .clone()
                .or_else(|| crate::cli::console::env_or_file("AWS_SECRET_ACCESS_KEY", &file_env))
                .ok_or_else(|| {
                    crate::cli::usage_error(
                        "Anthropic Bedrock daemon provider requires AWS_SECRET_ACCESS_KEY or provider.aws_secret_access_key",
                    )
                })?;
            let session_token = config
                .aws_session_token
                .clone()
                .or_else(|| crate::cli::console::env_or_file("AWS_SESSION_TOKEN", &file_env));
            let model = config
                .model
                .clone()
                .or_else(|| {
                    crate::cli::console::env_or_file("VERLET_ANTHROPIC_BEDROCK_MODEL", &file_env)
                })
                .or_else(|| crate::cli::console::env_or_file("AWS_BEDROCK_MODEL", &file_env))
                .or_else(|| {
                    crate::cli::console::env_or_file("ANTHROPIC_DEFAULT_SONNET_MODEL", &file_env)
                })
                .unwrap_or_else(|| {
                    crate::adapters::app_server::APP_SERVER_ANTHROPIC_BEDROCK_MODEL.to_string()
                });
            let stream = config.stream.unwrap_or(true);
            Ok(crate::cli::console::ChatProviderConfig::AnthropicBedrock {
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
                    std::env::var("VERLET_DAEMON_ENV_FILE")
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
            let file_env = crate::cli::console::read_env_file_if_exists(&env_file)?;
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
                    .or_else(|| {
                        crate::cli::console::env_or_file(
                            "VERLET_OPENAI_COMPATIBLE_MODEL",
                            &file_env,
                        )
                    })
                    .or_else(|| {
                        crate::cli::console::env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env)
                    });
                return Ok(
                    crate::cli::console::ChatProviderConfig::CatalogOpenAIChatCompletions {
                        provider_id:
                            crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER
                                .to_string(),
                        model,
                        max_tokens: config.max_tokens.unwrap_or(4096),
                        stream: config.stream.unwrap_or(true),
                    },
                );
            }
            let base_url = if openai_compatible {
                config
                    .base_url
                    .clone()
                    .or_else(|| crate::cli::console::env_or_file("VERLET_OPENAI_COMPATIBLE_URL", &file_env))
                    .or_else(|| crate::cli::console::env_or_file("OPENAI_COMPATIBLE_BASE_URL", &file_env))
                    .unwrap_or_else(|| "https://api.example.invalid/v1".to_string())
            } else {
                config
                    .base_url
                    .clone()
                    .or_else(|| crate::cli::console::env_or_file("VERLET_BIFROST_URL", &file_env))
                    .or_else(|| crate::cli::console::env_or_file("LLM_PROXY_PUBLIC_URL", &file_env))
                    .or_else(|| crate::cli::console::env_or_file("LLM_PROXY_URL", &file_env))
                    .ok_or_else(|| {
                        crate::cli::usage_error(
                            "OpenAI Chat Completions daemon provider requires provider.base_url, VERLET_BIFROST_URL, LLM_PROXY_PUBLIC_URL, or LLM_PROXY_URL",
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
                            .and_then(|name| crate::cli::console::env_or_file(name, &file_env))
                    })
                    .or_else(|| crate::cli::console::env_or_file("VERLET_OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .or_else(|| crate::cli::console::env_or_file("OPENAI_COMPATIBLE_API_KEY", &file_env))
                    .ok_or_else(|| {
                        crate::cli::usage_error(
                            "OpenAI Compatible daemon provider requires provider.api_key, provider.api_key_env, VERLET_OPENAI_COMPATIBLE_API_KEY, or OPENAI_COMPATIBLE_API_KEY",
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
                            .and_then(|name| crate::cli::console::env_or_file(name, &file_env))
                    })
                    .or_else(|| crate::cli::console::env_or_file("VERLET_BIFROST_KEY", &file_env))
                    .or_else(|| crate::cli::console::env_or_file("BIFROST_SYSTEM_VIRTUAL_KEY", &file_env))
                    .or_else(|| crate::cli::console::env_or_file("BIFROST_SYSTEM_KEY", &file_env))
                    .ok_or_else(|| {
                        crate::cli::usage_error(
                            "OpenAI Chat Completions daemon provider requires provider.api_key, provider.api_key_env, VERLET_BIFROST_KEY, or BIFROST_SYSTEM_VIRTUAL_KEY",
                        )
                    })?
            };
            let model = if openai_compatible {
                config
                    .model
                    .clone()
                    .or_else(|| {
                        crate::cli::console::env_or_file(
                            "VERLET_OPENAI_COMPATIBLE_MODEL",
                            &file_env,
                        )
                    })
                    .or_else(|| {
                        crate::cli::console::env_or_file("OPENAI_COMPATIBLE_MODEL", &file_env)
                    })
                    .unwrap_or_else(|| {
                        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string()
                    })
            } else {
                config
                    .model
                    .clone()
                    .or_else(|| {
                        crate::cli::console::env_or_file(
                            "VERLET_BIFROST_OPENAI_CHAT_MODEL",
                            &file_env,
                        )
                    })
                    .or_else(|| {
                        crate::cli::console::env_or_file("VERLET_BIFROST_OPENAI_MODEL", &file_env)
                    })
                    .unwrap_or_else(|| {
                        crate::adapters::app_server::APP_SERVER_BIFROST_MODEL.to_string()
                    })
            };
            Ok(
                crate::cli::console::ChatProviderConfig::OpenAIChatCompletions {
                    provider: chat_completions_provider_name(provider_name),
                    base_url,
                    api_key,
                    model,
                    max_tokens: config.max_tokens.unwrap_or(4096),
                    stream: config.stream.unwrap_or(true),
                    headers: provider_default_headers(provider_name),
                },
            )
        }
        other => Err(crate::cli::usage_error(format!(
            "unknown daemon provider {other:?}; expected local, openai-codex, bifrost_openai, openai_chat_completions, anthropic, anthropic_bedrock, or openai_compatible"
        ))),
    }
}

pub(crate) fn provider_is_openai_compatible(provider: &str) -> bool {
    matches!(
        provider,
        "openai_compatible"
            | "openai_compatible_openai"
            | "openai_compatible_chat"
            | "openai_compatible_serverless"
    )
}

pub(crate) fn chat_completions_provider_name(provider: &str) -> String {
    if provider_is_openai_compatible(provider) {
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string()
    } else {
        "openai_chat_completions".to_string()
    }
}

pub(crate) fn provider_default_headers(provider: &str) -> Vec<(String, String)> {
    if provider_is_openai_compatible(provider) {
        vec![("X-Example-Provider".to_string(), "required".to_string())]
    } else {
        Vec::new()
    }
}

pub(crate) fn print_daemon_help() {
    println!(
        "verlet daemon\n\
\n\
Usage:\n\
  verlet daemon config validate [--config verlet.toml]\n\
  verlet daemon service print [--target launchd|systemd] --config verlet.toml [--label com.verlet.daemon]\n\
  verlet daemon service install [--target launchd|systemd] --config verlet.toml [--label com.verlet.daemon]\n\
  verlet daemon service uninstall [--target launchd|systemd] [--label com.verlet.daemon]\n\
\n\
Server configuration uses verlet.toml. Service installation is explicit and writes the\n\
user-level launchd/systemd service file without starting it automatically.\n"
    );
}
