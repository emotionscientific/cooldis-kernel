//! The `debug rpc` subcommand family.

use super::*;

#[cfg(test)]
mod tests;

pub(super) async fn run_debug(mut args: Vec<OsString>) -> CooldisResult<()> {
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
        "bind" => run_debug_bind(args).await,
        "rpc" => run_debug_rpc(args).await,
        other => Err(usage_error(format!(
            "unknown debug subcommand {other:?}; use `cooldis debug --help`"
        ))),
    }
}

/// `cooldis debug rpc` — protocol-level debug client for a RUNNING daemon's
/// app-server websocket. Connects with `CodexTuiTestClient::connect_websocket`,
/// performs the initialize handshake, then dispatches a subcommand.
pub(super) async fn run_debug_rpc(mut args: Vec<OsString>) -> CooldisResult<()> {
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
        other => Err(usage_error(format!(
            "unknown debug rpc subcommand {other:?}; use `cooldis debug rpc --help`"
        ))),
    }
}

/// Endpoint selection shared by all `debug rpc` subcommands:
/// `--url <ws://…>` wins; else `--config <cooldis.toml>` reads
/// `daemon.app_server.listen`; else default `ws://127.0.0.1:49200/rpc`.
/// `--url` and `--config` together is a usage error.
#[derive(Debug)]
pub(super) struct DebugRpcEndpointArgs {
    pub(super) url: Option<String>,
    pub(super) config: Option<PathBuf>,
}

#[derive(Debug)]
pub(super) struct DebugRpcCallArgs {
    method: String,
    params: Value,
    endpoint: DebugRpcEndpointArgs,
}

#[derive(Debug)]
pub(super) enum DebugRpcThreadTarget {
    New,
    Existing(String),
}

#[derive(Debug)]
pub(super) struct DebugRpcTurnArgs {
    target: DebugRpcThreadTarget,
    json: bool,
    text: String,
    endpoint: DebugRpcEndpointArgs,
}

#[derive(Debug)]
pub(super) struct DebugRpcTailArgs {
    thread_id: String,
    endpoint: DebugRpcEndpointArgs,
}

pub(super) enum DebugRpcTurnStreamResult {
    Completed,
    TurnError(String),
}

pub(super) const DEBUG_RPC_DEFAULT_URL: &str = "ws://127.0.0.1:49200/rpc";
pub(super) const DEBUG_RPC_TURN_TIMEOUT: Duration = Duration::from_secs(120);

/// One-shot JSON-RPC request: `cooldis debug rpc call <method> [PARAMS_JSON]`.
/// PARAMS_JSON is an inline JSON object (omitted = no params). Prints the
/// result pretty-printed to stdout. A JSON-RPC error response prints the error
/// to stderr and exits 1 (transport failures likewise).
pub(super) async fn run_debug_rpc_call(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_debug_rpc_call_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    let result = client.request(&options.method, options.params).await?;
    serde_json::to_writer_pretty(std::io::stdout(), &result)
        .map_err(|err| usage_error(format!("failed to encode JSON-RPC result: {err}")))?;
    println!();
    client.close().await?;
    Ok(())
}

/// Run one turn and stream it: `cooldis debug rpc turn (--thread <id> | --new) [--json] <text>`.
/// `--thread` resumes the existing thread (thread/resume, excludeTurns true);
/// `--new` starts a fresh one and prints its id to stderr. Default output mode
/// streams agent-message delta text to stdout as it arrives (flushed per
/// delta), terminated by a newline at turn completion. `--json` instead emits
/// every notification scoped to the thread as one JSON object per line.
/// Exit codes: 0 turn completed, 2 turn error, 1 transport/protocol failure.
pub(super) async fn run_debug_rpc_turn(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_debug_rpc_turn_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    let thread_id = match &options.target {
        DebugRpcThreadTarget::New => {
            let thread = client.thread_start(json!({})).await?;
            eprintln!("{}", thread.id);
            thread.id
        }
        DebugRpcThreadTarget::Existing(thread_id) => {
            client
                .request(
                    "thread/resume",
                    json!({
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

/// Subscribe and watch: `cooldis debug rpc tail --thread <id>`.
/// Resumes the thread for the subscription, then prints every received
/// notification as one JSON object per line until Ctrl-C/EOF.
pub(super) async fn run_debug_rpc_tail(args: Vec<OsString>) -> CooldisResult<()> {
    let options = parse_debug_rpc_tail_args(args)?;
    let url = resolve_debug_rpc_endpoint(&options.endpoint)?;
    let mut client = connect_debug_rpc_client(&url).await?;
    client
        .request(
            "thread/resume",
            json!({
                "threadId": options.thread_id,
                "excludeTurns": true,
            }),
        )
        .await?;
    loop {
        match client.next_event().await {
            Ok(CodexTuiEvent::Notification(notification)) => {
                print_jsonl_notification(&notification)?;
            }
            Ok(CodexTuiEvent::Error(error)) => {
                return Err(usage_error(format!(
                    "JSON-RPC error {}: {}",
                    error.error.code, error.error.message
                )));
            }
            Ok(CodexTuiEvent::Request(_) | CodexTuiEvent::Response(_)) => {}
            Err(err) if err.to_string().contains("websocket closed") => return Ok(()),
            Err(err) => return Err(err),
        }
    }
}

pub(super) fn print_debug_rpc_help() {
    println!(
        "cooldis debug rpc\n\
\n\
Usage:\n\
  cooldis debug rpc call <method> [PARAMS_JSON] [--url <ws-url> | --config <cooldis.toml>]\n\
  cooldis debug rpc turn (--thread <id> | --new) [--json] <text> [--url <ws-url> | --config <cooldis.toml>]\n\
  cooldis debug rpc tail --thread <id> [--url <ws-url> | --config <cooldis.toml>]\n\
\n\
Protocol-level debug client for a running daemon's app-server websocket.\n\
Defaults to ws://127.0.0.1:49200/rpc when neither --url nor --config is given.\n\
call prints the JSON-RPC result; turn streams agent deltas as text (or all\n\
notifications as JSONL with --json); tail prints notifications until Ctrl-C.\n"
    );
}

pub(super) fn parse_debug_rpc_call_args(args: Vec<OsString>) -> CooldisResult<DebugRpcCallArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut positionals = Vec::new();
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => endpoint.url = Some(required_string_value(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_path_value(&mut iter, "--config")?),
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
        .ok_or_else(|| debug_rpc_usage_error("cooldis debug rpc call requires <method>"))?;
    if positionals.len() > 2 {
        return Err(debug_rpc_usage_error(
            "cooldis debug rpc call accepts at most one PARAMS_JSON argument",
        ));
    }
    let params = match positionals.get(1) {
        Some(raw) => serde_json::from_str(raw).map_err(|err| {
            debug_rpc_usage_error(format!("invalid PARAMS_JSON for debug rpc call: {err}"))
        })?,
        None => json!({}),
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

pub(super) fn parse_debug_rpc_turn_args(args: Vec<OsString>) -> CooldisResult<DebugRpcTurnArgs> {
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
            "--url" => endpoint.url = Some(required_string_value(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_path_value(&mut iter, "--config")?),
            "--thread" => thread_id = Some(required_string_value(&mut iter, "--thread")?),
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
                "cooldis debug rpc turn requires exactly one of --thread or --new",
            ));
        }
        (Some(thread_id), false) => DebugRpcThreadTarget::Existing(thread_id),
        (None, true) => DebugRpcThreadTarget::New,
        (None, false) => {
            return Err(debug_rpc_usage_error(
                "cooldis debug rpc turn requires exactly one of --thread or --new",
            ));
        }
    };
    if positionals.is_empty() {
        return Err(debug_rpc_usage_error(
            "cooldis debug rpc turn requires <text>",
        ));
    }
    Ok(DebugRpcTurnArgs {
        target,
        json,
        text: positionals.join(" "),
        endpoint,
    })
}

pub(super) fn parse_debug_rpc_tail_args(args: Vec<OsString>) -> CooldisResult<DebugRpcTailArgs> {
    let mut endpoint = DebugRpcEndpointArgs {
        url: None,
        config: None,
    };
    let mut thread_id = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--url" => endpoint.url = Some(required_string_value(&mut iter, "--url")?),
            "--config" => endpoint.config = Some(required_path_value(&mut iter, "--config")?),
            "--thread" => thread_id = Some(required_string_value(&mut iter, "--thread")?),
            other => {
                return Err(debug_rpc_usage_error(format!(
                    "unknown debug rpc tail argument {other:?}"
                )));
            }
        }
    }
    validate_debug_rpc_endpoint_args(&endpoint)?;
    Ok(DebugRpcTailArgs {
        thread_id: thread_id.ok_or_else(|| {
            debug_rpc_usage_error("cooldis debug rpc tail requires --thread <id>")
        })?,
        endpoint,
    })
}

pub(super) fn validate_debug_rpc_endpoint_args(
    endpoint: &DebugRpcEndpointArgs,
) -> CooldisResult<()> {
    if endpoint.url.is_some() && endpoint.config.is_some() {
        return Err(debug_rpc_usage_error(
            "cooldis debug rpc accepts --url or --config, not both",
        ));
    }
    Ok(())
}

pub(super) fn resolve_debug_rpc_endpoint(endpoint: &DebugRpcEndpointArgs) -> CooldisResult<String> {
    validate_debug_rpc_endpoint_args(endpoint)?;
    if let Some(url) = &endpoint.url {
        return Ok(url.clone());
    }
    if let Some(config_path) = &endpoint.config {
        let loaded = load_cooldis_daemon_config(Some(config_path))?;
        match loaded.config.app_server.listen_addr()? {
            AppServerListenAddr::WebSocket(_) => return Ok(loaded.config.app_server.listen),
            AppServerListenAddr::Unix(_) => {
                return Err(usage_error("daemon listens on a unix socket; pass --url"));
            }
        }
    }
    Ok(DEBUG_RPC_DEFAULT_URL.to_string())
}

pub(super) async fn connect_debug_rpc_client(
    url: &str,
) -> CooldisResult<CodexTuiTestClient<TcpStream>> {
    CodexTuiTestClient::connect_websocket(
        url,
        CodexTuiConnectConfig {
            client_name: "cooldis-debug-rpc".to_string(),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await
}

pub(super) async fn stream_debug_rpc_turn(
    client: &mut CodexTuiTestClient<TcpStream>,
    thread_id: &str,
    turn_id: &str,
    json_output: bool,
) -> CooldisResult<DebugRpcTurnStreamResult> {
    let deadline = tokio::time::sleep(DEBUG_RPC_TURN_TIMEOUT);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => {
                if !json_output {
                    println!();
                }
                return Err(usage_error(format!(
                    "timed out after {}s waiting for turn {turn_id}",
                    DEBUG_RPC_TURN_TIMEOUT.as_secs()
                )));
            }
            event = client.next_event() => {
                match event? {
                    CodexTuiEvent::Notification(notification) => {
                        if json_output && notification_thread_id(&notification) == Some(thread_id) {
                            print_jsonl_notification(&notification)?;
                        }
                        if notification.method == "item/agentMessage/delta"
                            && notification_matches_thread_turn(&notification, thread_id, turn_id)
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
                            && notification_turn_id(&notification) == Some(turn_id)
                        {
                            if !json_output {
                                println!();
                            }
                            return Ok(DebugRpcTurnStreamResult::Completed);
                        }
                    }
                    CodexTuiEvent::Error(error) => {
                        if !json_output {
                            println!();
                        }
                        return Err(usage_error(format!(
                            "JSON-RPC error {}: {}",
                            error.error.code, error.error.message
                        )));
                    }
                    CodexTuiEvent::Request(_) | CodexTuiEvent::Response(_) => {}
                }
            }
        }
    }
}

pub(super) fn print_jsonl_notification(notification: &JsonRpcNotification) -> CooldisResult<()> {
    serde_json::to_writer(std::io::stdout(), notification)
        .map_err(|err| usage_error(format!("failed to encode notification JSON: {err}")))?;
    println!();
    flush_stdout()
}

pub(super) fn notification_thread_id(notification: &JsonRpcNotification) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("threadId"))
        .and_then(Value::as_str)
}

pub(super) fn notification_delta(notification: &JsonRpcNotification) -> Option<&str> {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("delta"))
        .and_then(Value::as_str)
}

pub(super) fn notification_is_turn_error(
    notification: &JsonRpcNotification,
    thread_id: &str,
    turn_id: &str,
) -> bool {
    if notification.method == "error"
        && (notification_matches_thread_turn(notification, thread_id, turn_id)
            || notification_turn_id(notification) == Some(turn_id))
    {
        return true;
    }
    notification.method == "turn/completed"
        && notification_thread_id(notification) == Some(thread_id)
        && notification_turn_id(notification) == Some(turn_id)
        && notification
            .params
            .as_ref()
            .and_then(|params| params.get("turn"))
            .and_then(|turn| turn.get("status"))
            .and_then(Value::as_str)
            .is_some_and(|status| status == "failed" || status == "interrupted")
}

pub(super) fn notification_turn_error_message(notification: &JsonRpcNotification) -> String {
    notification
        .params
        .as_ref()
        .and_then(|params| params.get("turn"))
        .and_then(|turn| turn.get("error"))
        .and_then(|error| error.get("message"))
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .unwrap_or_else(|| notification_error_message(notification))
}

pub(super) fn debug_rpc_usage_error(message: impl Into<String>) -> CooldisError {
    usage_error(format!(
        "{}\nUsage: cooldis debug rpc --help",
        message.into()
    ))
}

pub(super) fn flush_stdout() -> CooldisResult<()> {
    std::io::stdout()
        .flush()
        .map_err(|err| usage_error(format!("failed to flush stdout: {err}")))
}

pub(super) fn print_debug_help() {
    println!(
        "cooldis debug\n\
\n\
Usage:\n\
  cooldis debug bind <thread-id> [--json] [--url <ws-url> | --config <cooldis.toml> | --journal <db>]\n\
  cooldis debug rpc (call|turn|tail) ...   debug client for a running daemon (see `cooldis debug rpc --help`)\n\
\n\
Maintainer and protocol inspection tools. These commands are not the public\n\
local console flow; use `cooldis console` or `cooldis chat` for normal operation.\n"
    );
}
