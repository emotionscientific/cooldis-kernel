use cooldis::{CooldisError, CooldisMcpServerConfig, CooldisResult, serve_mcp_stdio};
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("cooldis-mcp-server: {err}");
        std::process::exit(1);
    }
}

async fn run() -> CooldisResult<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let config = parse_args(args)?;
    serve_mcp_stdio(tokio::io::stdin(), tokio::io::stdout(), config).await
}

fn parse_args(args: Vec<OsString>) -> CooldisResult<CooldisMcpServerConfig> {
    let mut config = CooldisMcpServerConfig::default();
    if let Ok(socket) = std::env::var("COOLDIS_DAEMON_SOCKET") {
        if !socket.trim().is_empty() {
            config.daemon_socket = PathBuf::from(socket);
        }
    }
    if let Ok(listen) = std::env::var("COOLDIS_DAEMON_LISTEN") {
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
                config.daemon_socket = PathBuf::from(value);
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
                config.request_timeout = Duration::from_millis(timeout_ms);
            }
            other => return Err(usage_error(format!("unknown argument {other:?}"))),
        }
    }
    Ok(config)
}

fn required_value(iter: &mut impl Iterator<Item = OsString>, flag: &str) -> CooldisResult<String> {
    iter.next()
        .map(|value| value.to_string_lossy().into_owned())
        .ok_or_else(|| usage_error(format!("{flag} requires a value")))
}

fn parse_unix_listen(value: &str) -> CooldisResult<PathBuf> {
    let Some(path) = value.strip_prefix("unix://") else {
        return Err(usage_error(format!(
            "unsupported daemon listen address {value:?}; expected unix://PATH"
        )));
    };
    if path.is_empty() {
        return Err(usage_error("daemon listen address requires a path"));
    }
    Ok(PathBuf::from(path))
}

fn usage_error(message: impl Into<String>) -> CooldisError {
    CooldisError::RuntimeFactory(message.into())
}

fn print_help() {
    println!(
        "cooldis-mcp-server\n\
\n\
Usage:\n\
  cooldis-mcp-server [--socket PATH]\n\
  cooldis-mcp-server [--listen unix://PATH]\n\
\n\
Environment:\n\
  COOLDIS_DAEMON_SOCKET   daemon Unix socket path\n\
  COOLDIS_DAEMON_LISTEN   daemon listen address, unix://PATH\n\
\n\
The server speaks MCP over stdio and proxies tools to the Cooldis daemon app-server.\n\
\n\
Tip: run this when an external MCP client should use Cooldis. To let Cooldis use\n\
someone else's MCP server, register it with `cooldis tool source add ...`."
    );
}
