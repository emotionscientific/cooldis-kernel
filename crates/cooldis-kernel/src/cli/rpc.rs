//! The `rpc` subcommand family.

use super::*;

pub(super) async fn run_rpc(args: Vec<OsString>) -> CooldisResult<()> {
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
    let state_home = config.state_home.clone();
    let server = CooldisAppServer::new_local(config).await?;
    eprintln!("cooldis rpc listening on {}", options.listen.display());
    eprintln!("cooldis rpc state home: {}", state_home.display());
    match &options.listen {
        AppServerListenAddr::WebSocket(_) => eprintln!(
            "WebSocket clients must set COOLDIS_APP_SERVER_TOKEN to a bearer token minted with `cooldis identity` against this state home before the server starts."
        ),
        AppServerListenAddr::Unix(_) => {
            eprintln!("Same-uid Unix socket peers need no token.");
        }
    }
    server.serve(options.listen).await
}

#[derive(Debug)]
pub(super) struct RpcArgs {
    listen: AppServerListenAddr,
    runtime_home: Option<PathBuf>,
    state_home: Option<PathBuf>,
    cwd: Option<PathBuf>,
}

pub(super) fn parse_rpc_args(args: Vec<OsString>) -> CooldisResult<RpcArgs> {
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

pub(super) fn print_rpc_help() {
    println!(
        "cooldis rpc\n\
\n\
Usage:\n\
  cooldis rpc --listen <unix://PATH|ws://HOST:PORT[/rpc]> [--runtime-home <path>] [--state-home <path>] [--cwd <path>]\n\
\n\
Starts the Cooldis control-plane RPC endpoint. This is the public entrypoint for\n\
remote operation when Cooldis is running in a sandbox, daemon, or managed host.\n\
Without --state-home, the server uses a fresh temporary state home for each process.\n"
    );
}
