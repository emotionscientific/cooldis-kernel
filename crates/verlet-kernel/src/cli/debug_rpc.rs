//! The `debug rpc` subcommand family.

use std::io::Write as _;
#[cfg(test)]
mod tests;

pub(crate) async fn run_debug(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_debug_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "bind" => crate::cli::debug_bind::run_debug_bind(args).await,
        "journal" => crate::cli::debug_journal::run_debug_journal(args).await,
        "rpc" => run_debug_rpc(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown debug subcommand {other:?}; use `verlet debug --help`"
        ))),
    }
}

/// `verlet debug rpc` — protocol-level debug client for a RUNNING daemon's
/// app-server websocket. Connects with `OperatorClient::connect_websocket`,
/// performs the initialize handshake, then dispatches a subcommand.
pub(crate) async fn run_debug_rpc(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_debug_rpc_help();
        return Ok(());
    }
    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "call" => run_debug_rpc_call(args).await,
        "turn" => run_debug_rpc_turn(args).await,
        "tail" => run_debug_rpc_tail(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown debug rpc subcommand {other:?}; use `verlet debug rpc --help`"
        ))),
    }
}

/// Endpoint selection shared by all `debug rpc` subcommands:
/// `--url <ws://…>` wins; else `--config <verlet.toml>` reads
/// `daemon.app_server.listen`; else default `ws://127.0.0.1:49200/rpc`.
/// `--url` and `--config` together is a usage error.
#[derive(Debug)]
pub(crate) struct DebugRpcEndpointArgs {
    pub(crate) url: Option<String>,
    pub(crate) config: Option<std::path::PathBuf>,
}

#[derive(Debug)]
pub(crate) struct DebugRpcCallArgs {
    method: String,
    params: serde_json::Value,
    endpoint: DebugRpcEndpointArgs,
}

#[derive(Debug)]
pub(crate) enum DebugRpcThreadTarget {
    New,
    Existing(String),
}

#[derive(Debug)]
pub(crate) struct DebugRpcTurnArgs {
    target: DebugRpcThreadTarget,
    json: bool,
    text: String,
    endpoint: DebugRpcEndpointArgs,
}

#[derive(Debug)]
pub(crate) struct DebugRpcTailArgs {
    thread_id: String,
    endpoint: DebugRpcEndpointArgs,
}

pub(crate) enum DebugRpcTurnStreamResult {
    Completed,
    TurnError(String),
}

pub(crate) const DEBUG_RPC_DEFAULT_URL: &str = "ws://127.0.0.1:49200/rpc";
pub(crate) const DEBUG_RPC_TURN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

/// One-shot JSON-RPC request: `verlet debug rpc call <method> [PARAMS_JSON]`.
/// PARAMS_JSON is an inline JSON object (omitted = no params). Prints the
/// result pretty-printed to stdout. A JSON-RPC error response prints the error
/// to stderr and exits 1 (transport failures likewise).
pub(crate) async fn run_debug_rpc_call(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_debug_rpc_call_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    let result = client.request(&options.method, options.params).await?;
    serde_json::to_writer_pretty(std::io::stdout(), &result).map_err(|err| {
        crate::cli::usage_error(format!("failed to encode JSON-RPC result: {err}"))
    })?;
    println!();
    client.close().await?;
    Ok(())
}

/// Run one turn and stream it: `verlet debug rpc turn (--thread <id> | --new) [--json] <text>`.
/// `--thread` resumes the existing thread (thread/resume, excludeTurns true);
/// `--new` starts a fresh one and prints its id to stderr. Default output mode
/// streams agent-message delta text to stdout as it arrives (flushed per
/// delta), terminated by a newline at turn completion. `--json` instead emits
/// every notification scoped to the thread as one JSON object per line.
/// Exit codes: 0 turn completed, 2 turn error, 1 transport/protocol failure.
pub(crate) async fn run_debug_rpc_turn(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_debug_rpc_turn_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    let thread_id = match &options.target {
        DebugRpcThreadTarget::New => {
            let thread = client.thread_start(serde_json::json!({})).await?;
            eprintln!("{}", thread.id);
            thread.id
        }
        DebugRpcThreadTarget::Existing(thread_id) => {
            client
                .request(
                    "thread/resume",
                    serde_json::json!({
                        "threadId": thread_id,
                        "excludeTurns": true,
                    }),
                )
                .await?;
            thread_id.clone()
        }
    };
    let turn = client.turn_start_text(&thread_id, &options.text).await?;
    let stream_result =
        stream_debug_rpc_turn(&mut client, &thread_id, &turn.id, options.json).await?;
    let _ = client.close().await;
    match stream_result {
        DebugRpcTurnStreamResult::Completed => Ok(()),
        DebugRpcTurnStreamResult::TurnError(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    }
}

/// Subscribe and watch: `verlet debug rpc tail --thread <id>`.
/// Resumes the thread for the subscription, then prints every received
/// notification as one JSON object per line until Ctrl-C/EOF.
pub(crate) async fn run_debug_rpc_tail(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let options = parse_debug_rpc_tail_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    client
        .request(
            "thread/resume",
            serde_json::json!({
                "threadId": options.thread_id,
                "excludeTurns": true,
            }),
        )
        .await?;
    loop {
        match client.next_event().await {
            Ok(crate::adapters::operator_client::OperatorEvent::Notification(notification)) => {
                print_jsonl_notification(&notification)?;
            }
            Ok(crate::adapters::operator_client::OperatorEvent::Error(error)) => {
                return Err(crate::cli::usage_error(format!(
                    "JSON-RPC error {}: {}",
                    error.error.code, error.error.message
                )));
            }
            Ok(
                crate::adapters::operator_client::OperatorEvent::Request(_)
                | crate::adapters::operator_client::OperatorEvent::Response(_),
            ) => {}
            Err(err) if rpc_connection_was_closed(&err) => return Ok(()),
            Err(err) => return Err(err),
        }
    }
}

fn rpc_connection_was_closed(err: &crate::kernel::runtime_host::VerletError) -> bool {
    matches!(
        err,
        crate::kernel::runtime_host::VerletError::RpcClient(message)
            if message == "Verlet RPC connection closed"
                || message.starts_with("Verlet RPC connection was closed by the endpoint:")
    )
}

pub(crate) fn print_debug_rpc_help() {
    println!(
        "verlet debug rpc\n\
\n\
Usage:\n\
  verlet debug rpc call <method> [PARAMS_JSON] [--url <ws-url> | --config <verlet.toml>]\n\
  verlet debug rpc turn (--thread <id> | --new) [--json] <text> [--url <ws-url> | --config <verlet.toml>]\n\
  verlet debug rpc tail --thread <id> [--url <ws-url> | --config <verlet.toml>]\n\
\n\
Protocol-level debug client for a running daemon's app-server websocket.\n\
Defaults to ws://127.0.0.1:49200/rpc when neither --url nor --config is given.\n\
call prints the JSON-RPC result; turn streams agent deltas as text (or all\n\
notifications as JSONL with --json); tail prints notifications until Ctrl-C.\n"
    );
}

pub(crate) fn parse_debug_rpc_call_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DebugRpcCallArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => {
                endpoint.url = Some(crate::cli::tool::required_string_value(&mut iter, "--url")?)
            }
            "--config" => {
                endpoint.config = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            other if other.starts_with('-') => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc call argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    let method = positionals
        .first()
        .cloned()
        .ok_or_else(|| debug_rpc_usage_error("verlet debug rpc call requires <method>"))?;
    if positionals.len() > 2 {
        return Err(debug_rpc_usage_error(
            "verlet debug rpc call accepts at most one PARAMS_JSON argument",
        ));
    }
    let params = match positionals.get(1) {
        Some(raw) => serde_json::from_str(raw).map_err(|err| {
            debug_rpc_usage_error(format!("invalid PARAMS_JSON for debug rpc call: {err}"))
        })?,
        None => serde_json::json!({}),
    };
    if !params.is_object() {
        return Err(debug_rpc_usage_error(
            "PARAMS_JSON for debug rpc call must be a JSON object",
        ));
    }
    Ok(DebugRpcCallArgs {
        method,
        params,
        endpoint,
    })
}

pub(crate) fn parse_debug_rpc_turn_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DebugRpcTurnArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut thread_id = None;
    let mut new_thread = false;
    let mut json = false;
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => {
                endpoint.url = Some(crate::cli::tool::required_string_value(&mut iter, "--url")?)
            }
            "--config" => {
                endpoint.config = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            "--thread" => {
                thread_id = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--thread",
                )?)
            }
            "--new" => new_thread = true,
            "--json" => json = true,
            other if other.starts_with('-') => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc turn argument {other:?}"
                )));
            }
            _ => positionals.push(arg.to_string_lossy().to_string()),
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    let target = match (thread_id, new_thread) {
        (Some(_), true) => {
            return Err(debug_rpc_usage_error(
                "verlet debug rpc turn requires exactly one of --thread or --new",
            ));
        }
        (Some(thread_id), false) => DebugRpcThreadTarget::Existing(thread_id),
        (None, true) => DebugRpcThreadTarget::New,
        (None, false) => {
            return Err(debug_rpc_usage_error(
                "verlet debug rpc turn requires exactly one of --thread or --new",
            ));
        }
    };
    if positionals.is_empty() {
        return Err(debug_rpc_usage_error(
            "verlet debug rpc turn requires <text>",
        ));
    }
    Ok(DebugRpcTurnArgs {
        target,
        json,
        text: positionals.join(" "),
        endpoint,
    })
}

pub(crate) fn parse_debug_rpc_tail_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<DebugRpcTailArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut thread_id = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => {
                endpoint.url = Some(crate::cli::tool::required_string_value(&mut iter, "--url")?)
            }
            "--config" => {
                endpoint.config = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            "--thread" => {
                thread_id = Some(crate::cli::tool::required_string_value(
                    &mut iter, "--thread",
                )?)
            }
            other => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc tail argument {other:?}"
                )));
            }
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    Ok(DebugRpcTailArgs {
        thread_id: thread_id
            .ok_or_else(|| debug_rpc_usage_error("verlet debug rpc tail requires --thread <id>"))?,
        endpoint,
    })
}

pub(crate) fn validate_debug_rpc_endpoint_args(
    endpoint: &DebugRpcEndpointArgs,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if endpoint.url.is_some() && endpoint.config.is_some() {
        return Err(debug_rpc_usage_error(
            "verlet debug rpc accepts --url or --config, not both",
        ));
    }
    Ok(())
}

pub(crate) fn resolve_debug_rpc_endpoint(
    endpoint: &DebugRpcEndpointArgs,
) -> crate::kernel::runtime_host::VerletResult<String> {
    validate_debug_rpc_endpoint_args(endpoint)?;
    if let Some(url) = &endpoint.url {
        return Ok(url.clone());
    }
    if let Some(config_path) = &endpoint.config {
        let loaded = crate::daemon::daemon_config::load_verlet_daemon_config(Some(config_path))?;
        match loaded.config.app_server.listen_addr()? {
            crate::adapters::app_server::AppServerListenAddr::WebSocket(_) => {
                return Ok(loaded.config.app_server.listen);
            }
            crate::adapters::app_server::AppServerListenAddr::Unix(_) => {
                return Err(crate::cli::usage_error(
                    "daemon listens on a unix socket; pass --url",
                ));
            }
        }
    }
    Ok(DEBUG_RPC_DEFAULT_URL.to_string())
}

pub(crate) async fn connect_debug_rpc_client(
    url: &str,
) -> crate::kernel::runtime_host::VerletResult<
    crate::adapters::operator_client::OperatorClient<tokio::net::TcpStream>,
> {
    crate::adapters::operator_client::OperatorClient::connect_websocket(
        url,
        crate::adapters::operator_client::OperatorConnectConfig {
            client_name: "verlet-debug-rpc".to_string(),
            ..crate::adapters::operator_client::OperatorConnectConfig::default()
        },
    )
    .await
}

pub(crate) async fn stream_debug_rpc_turn(
    client: &mut crate::adapters::operator_client::OperatorClient<tokio::net::TcpStream>,
    thread_id: &str,
    turn_id: &str,
    json_output: bool,
) -> crate::kernel::runtime_host::VerletResult<DebugRpcTurnStreamResult> {
    let deadline = tokio::time::sleep(DEBUG_RPC_TURN_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                if !json_output {
                    println!();
                }
                return Err(crate::cli::usage_error(format!(
                    "timed out after {}s waiting for turn {turn_id}",
                    DEBUG_RPC_TURN_TIMEOUT.as_secs()
                )));
            }
            event = client.next_event() => {
                match event? {
                    crate::adapters::operator_client::OperatorEvent::Notification(notification) => {
                        if json_output && notification_thread_id(&notification) == Some(thread_id) {
                            print_jsonl_notification(&notification)?;
                        }
                        if notification.method == "item/agentMessage/delta"
                            && crate::cli::console::notification_matches_thread_turn(&notification, thread_id, turn_id)
                            && !json_output
                            && let Some(delta) = notification_delta(&notification)
                        {
                            print!("{delta}");
                            flush_stdout()?;
                        }
                        if notification_is_turn_error(&notification, thread_id, turn_id) {
                            if !json_output {
                                println!();
                            }
                            return Ok(DebugRpcTurnStreamResult::TurnError(
                                notification_turn_error_message(&notification),
                            ));
                        }
                        if notification.method == "turn/completed"
                            && crate::cli::console::notification_turn_id(&notification) == Some(turn_id)
                        {
                            if !json_output {
                                println!();
                            }
                            return Ok(DebugRpcTurnStreamResult::Completed);
                        }
                    }
                    crate::adapters::operator_client::OperatorEvent::Error(error) => {
                        if !json_output {
                            println!();
                        }
                        return Err(crate::cli::usage_error(format!(
                            "JSON-RPC error {}: {}",
                            error.error.code, error.error.message
                        )));
                    }
                    crate::adapters::operator_client::OperatorEvent::Request(_) | crate::adapters::operator_client::OperatorEvent::Response(_) => {}
                }
            }
        }
    }
}

pub(crate) fn print_jsonl_notification(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
) -> crate::kernel::runtime_host::VerletResult<()> {
    serde_json::to_writer(std::io::stdout(), notification).map_err(|err| {
        crate::cli::usage_error(format!("failed to encode notification JSON: {err}"))
    })?;
    println!();
    flush_stdout()
}

pub(crate) fn notification_thread_id(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("threadId"))
        .and_then(serde_json::Value::as_str)
}

pub(crate) fn notification_delta(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("delta"))
        .and_then(serde_json::Value::as_str)
}

pub(crate) fn notification_is_turn_error(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    if notification.method == "error"
        && (crate::cli::console::notification_matches_thread_turn(notification, thread_id, turn_id)
            || crate::cli::console::notification_turn_id(notification) == Some(turn_id))
    {
        return true;
    }
    notification.method == "turn/completed"
        && notification_thread_id(notification) == Some(thread_id)
        && crate::cli::console::notification_turn_id(notification) == Some(turn_id)
        && notification
            .params
            .as_ref()
            .and_then(|params| params.get("turn"))
            .and_then(|turn| turn.get("status"))
            .and_then(serde_json::Value::as_str)
            .is_some_and(|status| status == "failed" || status == "interrupted")
}

pub(crate) fn notification_turn_error_message(
    notification: &crate::adapters::app_server::connection::JsonRpcNotification,
) -> String {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("turn"))
        .and_then(|turn| turn.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| crate::cli::console::notification_error_message(notification))
}

pub(crate) fn debug_rpc_usage_error(
    message: impl Into<String>,
) -> crate::kernel::runtime_host::VerletError {
    crate::cli::usage_error(format!(
        "{}\nUsage: verlet debug rpc --help",
        message.into()
    ))
}

pub(crate) fn flush_stdout() -> crate::kernel::runtime_host::VerletResult<()> {
    std::io::stdout()
        .flush()
        .map_err(|err| crate::cli::usage_error(format!("failed to flush stdout: {err}")))
}

pub(crate) fn print_debug_help() {
    println!(
        "verlet debug\n\
\n\
Usage:\n\
  verlet debug bind <thread-id> [--json] [--url <ws-url> | --config <verlet.toml> | --journal <db>]\n\
  verlet debug journal [--thread <thread-id>] [--kind <kind>] [--from-sequence <n>] [--to-sequence <n>] [--json] [--url <ws-url> | --config <verlet.toml> | --journal <db>]\n\
  verlet debug rpc (call|turn|tail) ...   debug client for a running daemon (see `verlet debug rpc --help`)\n\
\n\
Maintainer and protocol inspection tools. These commands are not the public\n\
local console flow; use `verlet console` or `verlet chat` for normal operation.\n"
    );
}
