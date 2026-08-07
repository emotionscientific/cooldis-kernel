//! The `rpc` subcommand family.

pub(super) async fn run_rpc(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_rpc_help();
        return Ok(());
    }
    let options = parse_rpc_args(args)?;
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        options.listen.clone(),
        options
            .cwd
            .unwrap_or(std::env::current_dir().map_err(|err| {
                crate::cli::usage_error(format!("failed to read current working directory: {err}"))
            })?),
    );
    if let Some(runtime_home) = options.runtime_home {
        config.runtime_home = runtime_home;
    }
    if let Some(state_home) = options.state_home {
        config.state_home = state_home;
    }
    let state_home = config.state_home.clone();
    let server = crate::adapters::app_server::VerletAppServer::new_local(config).await?;
    eprintln!("verlet rpc listening on {}", options.listen.display());
    eprintln!("verlet rpc state home: {}", state_home.display());
    match &options.listen {
        crate::adapters::app_server::AppServerListenAddr::WebSocket(_) => eprintln!(
            "Before starting this server, mint a bearer token with `verlet identity` against this state home; WebSocket clients pass that token in VERLET_APP_SERVER_TOKEN."
        ),
        crate::adapters::app_server::AppServerListenAddr::Unix(_) => {
            eprintln!("Same-uid Unix socket peers need no token.");
        }
    }
    server.serve(options.listen).await
}

#[derive(Debug)]
pub(super) struct RpcArgs {
    listen: crate::adapters::app_server::AppServerListenAddr,
    runtime_home: Option<std::path::PathBuf>,
    state_home: Option<std::path::PathBuf>,
    cwd: Option<std::path::PathBuf>,
}

pub(super) fn parse_rpc_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<RpcArgs> {
    let mut listen = None;
    let mut runtime_home = None;
    let mut state_home = None;
    let mut cwd = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--listen" => {
                let value = crate::cli::tool::required_string_value(&mut iter, "--listen")?;
                listen = Some(crate::adapters::app_server::AppServerListenAddr::parse(
                    &value,
                )?);
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
            "--cwd" => {
                cwd = Some(std::path::PathBuf::from(
                    crate::cli::tool::required_string_value(&mut iter, "--cwd")?,
                ));
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown rpc argument {other:?}"
                )));
            }
        }
    }
    let listen = listen.ok_or_else(|| {
        crate::cli::usage_error("rpc requires --listen unix://PATH or ws://HOST:PORT[/rpc]")
    })?;
    Ok(RpcArgs {
        listen,
        runtime_home,
        state_home,
        cwd,
    })
}

pub(super) fn print_rpc_help() {
    println!(
        "verlet rpc\n\
\n\
Usage:\n\
  verlet rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--runtime-home <path>] [--state-home <path>] [--cwd <path>]\n\
\n\
Starts the Verlet control-plane RPC endpoint. This is the public entrypoint for\n\
remote operation when Verlet is running in a sandbox, daemon, or managed host.\n\
Without --state-home, the server uses a fresh temporary state home for each process.\n"
    );
}
