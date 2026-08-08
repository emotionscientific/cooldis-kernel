#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("verlet-mcp-server: {err}");
        std::process::exit(1);
    }
}

async fn run() -> verlet::kernel::runtime_host::VerletResult<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    verlet::adapters::mcp_server::serve_mcp_stdio(tokio::io::stdin(), tokio::io::stdout(), config)
        .await
}

fn parse_args(
    args: Vec<std::ffi::OsString>,
) -> verlet::kernel::runtime_host::VerletResult<verlet::adapters::mcp_server::VerletMcpServerConfig>
{
    let mut config = verlet::adapters::mcp_server::VerletMcpServerConfig::default();
    if let Ok(socket) = verlet_runtime_contracts::env_compat::var("VERLET_DAEMON_SOCKET") {
        if !socket.trim().is_empty() {
            config.daemon_socket = std::path::PathBuf::from(socket);
        }
    }
    if let Ok(listen) = verlet_runtime_contracts::env_compat::var("VERLET_DAEMON_LISTEN") {
        if !listen.trim().is_empty() {
            config.daemon_socket = parse_unix_listen(&listen)?;
        }
    }

    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--help" | "-h" => {
                print_help();
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
            other => return Err(usage_error(format!("unknown argument {other:?}"))),
        }
    }
    Ok(config)
}

fn required_value(
    iter: &mut impl Iterator<Item = std::ffi::OsString>,
    flag: &str,
) -> verlet::kernel::runtime_host::VerletResult<String> {
    iter.next()
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or_else(|| usage_error(format!("{flag} requires a value")))
}

fn parse_unix_listen(
    value: &str,
) -> verlet::kernel::runtime_host::VerletResult<std::path::PathBuf> {
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

fn usage_error(message: impl Into<String>) -> verlet::kernel::runtime_host::VerletError {
    verlet::kernel::runtime_host::VerletError::RuntimeFactory(message.into())
}

fn print_help() {
    println!(
        "verlet-mcp-server\n\
\n\
Usage:\n\
  verlet-mcp-server [--socket PATH]\n\
  verlet-mcp-server [--listen unix://PATH]\n\
\n\
Environment:\n\
  VERLET_DAEMON_SOCKET   daemon Unix socket path\n\
  VERLET_DAEMON_LISTEN   daemon listen address, unix://PATH\n\
\n\
The server speaks MCP over stdio and proxies tools to the Verlet daemon app-server.\n\
\n\
Tip: run this when an external MCP client should use Verlet. To let Verlet use\n\
someone else's MCP server, register it with `verlet tool source add ...`."
    );
}
