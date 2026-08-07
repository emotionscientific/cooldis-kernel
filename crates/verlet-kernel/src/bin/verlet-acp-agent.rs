#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("verlet-acp-agent: {err}");
        std::process::exit(1);
    }
}

async fn run() -> verlet::VerletResult<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    verlet::serve_acp_stdio(tokio::io::stdin(), tokio::io::stdout(), config).await
}

fn parse_args(args: Vec<std::ffi::OsString>) -> verlet::VerletResult<verlet::VerletAcpAgentConfig> {
    let mut config = verlet::VerletAcpAgentConfig::default();
    if let Ok(socket) = verlet::env_compat::var("VERLET_DAEMON_SOCKET") {
        if !socket.trim().is_empty() {
            config.daemon_socket = std::path::PathBuf::from(socket);
        }
    }
    if let Ok(listen) = verlet::env_compat::var("VERLET_DAEMON_LISTEN") {
        if !listen.trim().is_empty() {
            config.daemon_socket = parse_unix_listen(&listen)?;
        }
    }
    if let Ok(agent_ref) = verlet::env_compat::var("VERLET_ACP_AGENT_REF") {
        if !agent_ref.trim().is_empty() {
            config.agent_ref = Some(agent_ref);
        }
    }

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            "--version" | "-V" => {
                println!("verlet-acp-agent {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            "--socket" | "--daemon-socket" => {
                let value = required_value(&mut iter, "--socket")?;
                config.daemon_socket = std::path::PathBuf::from(value);
            }
            "--listen" | "--daemon-listen" => {
                let value = required_value(&mut iter, "--listen")?;
                config.daemon_socket = parse_unix_listen(&value)?;
            }
            "--timeout-ms" => {
                let value = required_value(&mut iter, "--timeout-ms")?;
                let timeout_ms = value.parse::<u64>().map_err(|err| {
                    usage_error(format!("invalid --timeout-ms value {value:?}: {err}"))
                })?;
                config.request_timeout = std::time::Duration::from_millis(timeout_ms);
            }
            "--agent-ref" => {
                let value = required_value(&mut iter, "--agent-ref")?;
                config.agent_ref = Some(value);
            }
            "--cwd" => {
                let value = required_value(&mut iter, "--cwd")?;
                config.cwd = Some(std::path::PathBuf::from(value));
            }
            other => return Err(usage_error(format!("unknown argument {other:?}"))),
        }
    }
    Ok(config)
}

fn required_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> verlet::VerletResult<String> {
    iter.next()
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or_else(|| usage_error(format!("{flag} requires a value")))
}

fn parse_unix_listen(value: &str) -> verlet::VerletResult<std::path::PathBuf> {
    let Some(path) = value.strip_prefix("unix://") else {
        return Err(usage_error(format!(
            "unsupported daemon listen address {value:?}; expected unix://PATH"
        )));
    };
    if path.is_empty() {
        return Err(usage_error("daemon listen address requires a path"));
    }
    Ok(std::path::PathBuf::from(path))
}

fn usage_error(message: impl Into<String>) -> verlet::VerletError {
    verlet::VerletError::RuntimeFactory(message.into())
}

fn print_help() {
    println!(
        "verlet-acp-agent\n\
\n\
Usage:\n\
  verlet-acp-agent [--socket PATH]\n\
  verlet-acp-agent [--listen unix://PATH]\n\
\n\
Options:\n\
  --agent-ref REF       published Verlet agent ref to use for ACP sessions\n\
  --cwd PATH            default working directory for ACP sessions\n\
  --timeout-ms MS       daemon request timeout\n\
\n\
Environment:\n\
  VERLET_DAEMON_SOCKET    daemon Unix socket path\n\
  VERLET_DAEMON_LISTEN    daemon listen address, unix://PATH\n\
  VERLET_ACP_AGENT_REF    default agent:// ref for ACP session/new\n\
\n\
The agent speaks ACP over stdio and projects ACP sessions onto Verlet threads.\n\
ACP is narrower than the Verlet app-server API: provider auth, operation\n\
registry mutation, placement policy, and secret management stay in Verlet."
    );
}
