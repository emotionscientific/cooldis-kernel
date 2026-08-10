use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use verlet::daemon::identity::IdentityAuthority as _;

#[path = "../support/model_catalog.rs"]
mod model_catalog_test_support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    model_catalog_test_support::disable_in_process_refresh();
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("cdis-app-server-{}", uuid::Uuid::now_v7().simple()));
    let socket = root.join("app-server.sock");
    let server_task = start_server(&root, &socket).await?;

    let mut client = connect_client(&socket, "verlet-app-server-smoke").await?;
    assert_eq!(
        client.initialize_result()["userAgent"],
        "verlet-app-server/0.1"
    );

    let account = client.account_read().await?;
    assert_eq!(account["requiresOpenaiAuth"], false);

    let models = client.model_list().await?;
    assert!(matches!(models["data"].as_array(), Some(models) if !models.is_empty()));

    let first_completed = client
        .run_prompt("smoke before restart", std::time::Duration::from_secs(5))
        .await?;
    let saw_first_delta = first_completed
        .notifications
        .iter()
        .any(|notification| notification.method == "item/agentMessage/delta");
    let saw_first_completed = first_completed
        .notifications
        .iter()
        .any(|notification| notification.method == "turn/completed");

    client.close().await?;
    server_task.abort();
    let _ = server_task.await;

    let restarted_task = start_server(&root, &socket).await?;
    let mut restarted = connect_client(&socket, "verlet-app-server-smoke-restarted").await?;
    let loaded = restarted.loaded_thread_list().await?;
    let loaded_ids = loaded["data"]
        .as_array()
        .ok_or("thread/loaded/list data was not an array")?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    if !loaded_ids.contains(&first_completed.thread_id.as_str()) {
        return Err(format!(
            "restarted app-server did not load saved thread {}; loaded: {loaded_ids:?}",
            first_completed.thread_id
        )
        .into());
    }

    restarted
        .request(
            "thread/resume",
            serde_json::json!({
                "threadId": first_completed.thread_id,
                "excludeTurns": true,
            }),
        )
        .await?;
    let second_turn = restarted
        .turn_start_text(&first_completed.thread_id, "smoke after restart")
        .await?;
    let second_completed = restarted
        .wait_for_turn_completed(
            &first_completed.thread_id,
            &second_turn.id,
            std::time::Duration::from_secs(5),
        )
        .await?;
    let saw_second_completed = second_completed
        .notifications
        .iter()
        .any(|notification| notification.method == "turn/completed");

    restarted.close().await?;
    restarted_task.abort();
    let _ = restarted_task.await;

    let tcp_addr = unused_loopback_addr()?;
    let (tcp_task, tcp_token) = start_tcp_server(&root, tcp_addr).await?;
    let health = tcp_health_response(tcp_addr, "/healthz").await?;
    if !health.starts_with("HTTP/1.1 200 OK") || !health.contains("{\"status\":\"ok\"}") {
        return Err(format!("unexpected TCP health response: {health:?}").into());
    }
    let mut tcp_client = connect_tcp_client(
        &format!("ws://{tcp_addr}/rpc"),
        "verlet-app-server-smoke-tcp",
        &tcp_token,
    )
    .await?;
    let tcp_completed = tcp_client
        .run_prompt(
            "smoke over tcp websocket",
            std::time::Duration::from_secs(5),
        )
        .await?;
    let saw_tcp_completed = tcp_completed
        .notifications
        .iter()
        .any(|notification| notification.method == "turn/completed");
    tcp_client.close().await?;
    tcp_task.abort();
    let _ = tcp_task.await;

    let _ = std::fs::remove_dir_all(root);

    if !saw_first_delta || !saw_first_completed || !saw_second_completed || !saw_tcp_completed {
        return Err("did not observe streamed assistant delta and turn completion".into());
    }
    println!("verlet app-server smoke ok: restart loaded saved thread and tcp websocket");
    Ok(())
}

async fn start_server(
    root: &std::path::Path,
    socket: &std::path::Path,
) -> Result<
    tokio::task::JoinHandle<verlet::kernel::runtime_host::VerletResult<()>>,
    Box<dyn std::error::Error>,
> {
    let listen = verlet::adapters::app_server::AppServerListenAddr::Unix(socket.to_path_buf());
    let mut config = verlet::adapters::app_server::VerletAppServerConfig::local(
        listen.clone(),
        std::env::current_dir()?,
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let server = verlet::adapters::app_server::VerletAppServer::new_local(config).await?;
    Ok(tokio::spawn(async move { server.serve(listen).await }))
}

async fn start_tcp_server(
    root: &std::path::Path,
    addr: std::net::SocketAddr,
) -> Result<
    (
        tokio::task::JoinHandle<verlet::kernel::runtime_host::VerletResult<()>>,
        String,
    ),
    Box<dyn std::error::Error>,
> {
    let listen = verlet::adapters::app_server::AppServerListenAddr::WebSocket(addr);
    let mut config = verlet::adapters::app_server::VerletAppServerConfig::local(
        listen.clone(),
        std::env::current_dir()?,
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    let server = verlet::adapters::app_server::VerletAppServer::new_local(config).await?;
    let store =
        verlet_history_sqlite::SqliteSessionStore::open(server.session_store_path()).await?;
    let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(
        store,
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock),
        None,
    )
    .await?;
    let principal = verlet::daemon::identity::PrincipalId::new(server.user_id());
    let token = authority
        .mint_credential(&principal, &principal, None)
        .await?
        .1;
    Ok((
        tokio::spawn(async move { server.serve(listen).await }),
        token,
    ))
}

async fn connect_client(
    socket: &std::path::Path,
    client_name: &str,
) -> Result<
    verlet::adapters::operator_client::OperatorClient<tokio::net::UnixStream>,
    Box<dyn std::error::Error>,
> {
    let mut last_error = None;
    for _ in 0..1_500 {
        match verlet::adapters::operator_client::OperatorClient::connect_unix(
            socket,
            verlet::adapters::operator_client::OperatorConnectConfig {
                client_name: client_name.to_string(),
                ..verlet::adapters::operator_client::OperatorConnectConfig::default()
            },
        )
        .await
        {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(format!(
        "timed out connecting to app-server socket {}; last error: {}",
        socket.display(),
        last_error.unwrap_or_else(|| "none".to_string())
    )
    .into())
}

async fn connect_tcp_client(
    url: &str,
    client_name: &str,
    token: &str,
) -> Result<
    verlet::adapters::operator_client::OperatorClient<tokio::net::TcpStream>,
    Box<dyn std::error::Error>,
> {
    let mut last_error = None;
    for _ in 0..1_500 {
        match verlet::adapters::operator_client::OperatorClient::connect_websocket(
            url,
            verlet::adapters::operator_client::OperatorConnectConfig {
                client_name: client_name.to_string(),
                bearer_token: Some(token.to_string()),
                ..verlet::adapters::operator_client::OperatorConnectConfig::default()
            },
        )
        .await
        {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(format!(
        "timed out connecting to app-server websocket {url}; last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    )
    .into())
}

async fn tcp_health_response(
    addr: std::net::SocketAddr,
    path: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut last_error = None;
    for _ in 0..1_500 {
        match tokio::net::TcpStream::connect(addr).await {
            Ok(mut stream) => {
                let request =
                    format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
                stream.write_all(request.as_bytes()).await?;
                let mut response = String::new();
                stream.read_to_string(&mut response).await?;
                return Ok(response);
            }
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(format!(
        "timed out connecting to app-server health endpoint {addr}; last error: {}",
        last_error.unwrap_or_else(|| "none".to_string())
    )
    .into())
}

fn unused_loopback_addr() -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}
