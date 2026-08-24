//! The foreground `serve` command.

#[cfg(test)]
mod tests;

pub(crate) async fn run_serve(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_serve_help();
        return Ok(());
    }
    let options = parse_serve_args(args)?;
    if options.help {
        print_serve_help();
        return Ok(());
    }
    let loaded =
        crate::daemon::daemon_config::load_verlet_daemon_config(options.config_path.as_deref())?;
    let idle_timeout = if options.no_idle_timeout {
        None
    } else {
        options.idle_timeout.or(loaded.config.idle_timeout()?)
    };
    let mut config = crate::cli::daemon::daemon_app_server_config_from_loaded(&loaded)?;
    if let Some(cwd) = options.cwd {
        config.cwd = crate::cli::console::absolute_path(&cwd)?;
    }
    if let Some(runtime_home) = options.runtime_home {
        config.runtime_home = crate::cli::console::absolute_path(&runtime_home)?;
    }
    if let Some(state_home) = options.state_home {
        config.state_home = crate::cli::console::absolute_path(&state_home)?;
        config.listen = crate::adapters::app_server::AppServerListenAddr::Unix(
            crate::adapters::app_server::instance::instance_unix_socket_path(&config.state_home)?,
        );
    }
    config.user_state_home = match options.user_state_home {
        Some(user_state_home) => crate::cli::console::absolute_path(&user_state_home)?,
        None => crate::cli::secret::default_user_state_home()?,
    };
    let listen = config.listen.clone();
    let state_home = config.state_home.clone();
    crate::cli::console::prepare_console_project_storage(&config)?;

    let server = match crate::adapters::app_server::VerletAppServer::new_local(config).await {
        Ok(server) => server,
        Err(error) => {
            if competing_server_started(&state_home, &error).await {
                eprintln!("verlet serve: {error}");
                return Ok(());
            }
            return Err(error);
        }
    };
    let _io_tasks = match crate::cli::daemon::start_daemon_io(
        &loaded.config.io,
        &loaded.config.sync,
        loaded.path.clone(),
        &server,
    )
    .await
    {
        Ok(tasks) => tasks,
        Err(error) => {
            if let Err(shutdown_error) = server.shutdown().await {
                eprintln!(
                    "failed to shut down Verlet serve after I/O startup error {error}: {shutdown_error}"
                );
            }
            return Err(error);
        }
    };
    eprintln!("verlet serve listening on {}", listen.display());
    if let Some(path) = &loaded.path {
        eprintln!("verlet serve config {}", path.display());
    } else {
        eprintln!("verlet serve config <defaults>");
    }
    eprintln!("verlet serve state {}", state_home.display());
    serve_until_stopped(&server, listen, idle_timeout).await
}

async fn serve_until_stopped(
    server: &crate::adapters::app_server::VerletAppServer,
    listen: crate::adapters::app_server::AppServerListenAddr,
    idle_timeout: Option<std::time::Duration>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let serving = server.serve(listen);
    tokio::pin!(serving);
    let serving_result = match idle_timeout {
        Some(idle_timeout) => {
            tokio::select! {
                result = &mut serving => result,
                () = server.wait_for_idle_timeout(idle_timeout) => {
                    eprintln!("verlet serve idle timeout expired");
                    server.shutdown().await?;
                    serving.await
                }
            }
        }
        None => serving.await,
    };
    let shutdown = server.shutdown().await;
    serving_result?;
    shutdown
}

#[cfg(unix)]
async fn competing_server_started(
    state_home: &std::path::Path,
    error: &crate::kernel::runtime_host::VerletError,
) -> bool {
    let message = error.to_string();
    if !message.contains("instance already running for ")
        && !crate::adapters::app_server::instance::is_cross_process_database_guidance(error)
        && !message.contains("app-server socket")
        && !message.contains("failed to bind Verlet endpoint socket")
    {
        return false;
    }
    let wait = if crate::adapters::app_server::instance::is_cross_process_database_guidance(error) {
        crate::cli::INSTANCE_START_TIMEOUT
    } else {
        std::time::Duration::from_secs(3)
    };
    let deadline = tokio::time::Instant::now() + wait;
    loop {
        if let Some(endpoint) =
            crate::adapters::app_server::instance::resolve_instance_endpoint(state_home)
            && tokio::net::UnixStream::connect(&endpoint.unix_socket)
                .await
                .is_ok()
        {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[cfg(not(unix))]
async fn competing_server_started(
    _state_home: &std::path::Path,
    _error: &crate::kernel::runtime_host::VerletError,
) -> bool {
    false
}

#[derive(Debug)]
pub(crate) struct ServeArgs {
    pub(crate) config_path: Option<std::path::PathBuf>,
    pub(crate) cwd: Option<std::path::PathBuf>,
    pub(crate) runtime_home: Option<std::path::PathBuf>,
    pub(crate) state_home: Option<std::path::PathBuf>,
    pub(crate) user_state_home: Option<std::path::PathBuf>,
    pub(crate) idle_timeout: Option<std::time::Duration>,
    pub(crate) no_idle_timeout: bool,
    pub(crate) help: bool,
}

pub(crate) fn parse_serve_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<ServeArgs> {
    let mut config_path = None;
    let mut cwd = None;
    let mut runtime_home = None;
    let mut state_home = None;
    let mut user_state_home = None;
    let mut idle_timeout = None;
    let mut no_idle_timeout = false;
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
            "--cwd" => cwd = Some(crate::cli::tool::required_path_value(&mut iter, "--cwd")?),
            "--runtime-home" => {
                runtime_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--runtime-home",
                )?)
            }
            "--state-home" => {
                state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--state-home",
                )?)
            }
            "--user-state-home" => {
                user_state_home = Some(crate::cli::tool::required_path_value(
                    &mut iter,
                    "--user-state-home",
                )?)
            }
            "--no-idle-timeout" => no_idle_timeout = true,
            "--idle-timeout" => {
                let raw = crate::cli::tool::required_string_value(&mut iter, "--idle-timeout")?;
                let duration = humantime::parse_duration(&raw).map_err(|error| {
                    crate::cli::usage_error(format!(
                        "--idle-timeout must be a duration such as 10m or 2s: {error}"
                    ))
                })?;
                if duration.is_zero() {
                    return Err(crate::cli::usage_error(
                        "--idle-timeout must be greater than zero",
                    ));
                }
                idle_timeout = Some(duration);
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown serve argument {other:?}"
                )));
            }
        }
    }
    if no_idle_timeout && idle_timeout.is_some() {
        return Err(crate::cli::usage_error(
            "--idle-timeout and --no-idle-timeout cannot be used together",
        ));
    }
    Ok(ServeArgs {
        config_path,
        cwd,
        runtime_home,
        state_home,
        user_state_home,
        idle_timeout,
        no_idle_timeout,
        help,
    })
}

pub(crate) fn print_serve_help() {
    println!(
        "verlet serve\n\
\n\
Usage:\n\
  verlet serve [--config verlet.toml] [--cwd <path>] [--runtime-home <path>] [--state-home <path>] [--idle-timeout <duration>]\n\
\n\
Runs the Verlet app-server in the foreground. Without --idle-timeout the server\n\
uses daemon.idle_timeout when configured; otherwise it continues until it\n\
receives an explicit shutdown signal.\n"
    );
}
