use cooldis::daemon::identity::{
    CooldisDaemonIdentityConfig, IdentityAuthority, IdentityMode, PrincipalId, PrincipalKind,
    SqliteIdentityAuthority,
};
use cooldis::{
    AppServerListenAddr, CodexTuiConnectConfig, CodexTuiTestClient, ConsoleAssetConfig,
    CooldisAppServer, CooldisAppServerConfig, JsonRpcErrorError, JsonRpcMessage,
    JsonRpcNotification, JsonRpcRequest, RequestId, SqliteSessionStore, SystemDaemonClock,
};
use cooldis_sqlite::params;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::{AUTHORIZATION, SEC_WEBSOCKET_PROTOCOL};

const OPERATOR_ID: &str = "operator:root";
const ADAPTER_ID: &str = "adapter:rpc";
const METHOD_NOT_AUTHORIZED_CODE: i64 = -32003;

#[tokio::test]
async fn dispatcher_authorizes_at_the_rpc_choke_point_and_witnesses_decisions() {
    let root = test_root("dispatcher-authorization");
    std::fs::create_dir_all(root.join("workspace")).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listen = AppServerListenAddr::WebSocket(addr);
    let mut config = app_config(&root, listen);
    let authority = identity_authority(&config).await;
    let operator = PrincipalId::new(OPERATOR_ID);
    let (_, _, operator_token) = authority
        .bootstrap_operator(&operator, "Root operator")
        .await
        .unwrap();
    let adapter = PrincipalId::new(ADAPTER_ID);
    authority
        .declare_principal(&operator, &adapter, PrincipalKind::Adapter, "RPC adapter")
        .await
        .unwrap();
    let (_, adapter_token) = authority
        .mint_credential(&operator, &adapter, None)
        .await
        .unwrap();
    drop(authority);

    config.apply_daemon_identity_config(&CooldisDaemonIdentityConfig {
        mode: IdentityMode::Managed,
        tenant_id: Some("test-tenant".to_string()),
        console_principal: Some(operator),
    });
    let app = CooldisAppServer::new(config).await.unwrap();
    let store_path = app.session_store_path().to_path_buf();
    let server = app.clone();
    let server_task = tokio::spawn(async move { server.serve_websocket_listener(listener).await });

    let mut operator_rpc = connect_rpc(addr, &operator_token).await;
    let thread = rpc_call(
        &mut operator_rpc,
        RequestId::Integer(2),
        "thread/start",
        json!({}),
    )
    .await
    .unwrap();
    let thread_id = thread["thread"]["id"].as_str().unwrap().to_string();
    let command = rpc_call(
        &mut operator_rpc,
        RequestId::Integer(3),
        "command/exec",
        json!({ "command": ["/bin/sh", "-c", "printf operator"] }),
    )
    .await
    .unwrap();
    assert_eq!(command["stdout"], "operator");
    rpc_call(
        &mut operator_rpc,
        RequestId::Integer(4),
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "operator ingress", "text_elements": [] }],
        }),
    )
    .await
    .unwrap();

    let mut adapter_rpc = connect_rpc(addr, &adapter_token).await;
    let denied_command = rpc_call(
        &mut adapter_rpc,
        RequestId::Integer(2),
        "command/exec",
        json!({ "command": ["/bin/sh", "-c", "printf adapter"] }),
    )
    .await
    .unwrap_err();
    assert_eq!(denied_command.code, METHOD_NOT_AUTHORIZED_CODE);
    assert!(!denied_command.message.contains("command/exec"));

    let denied_unknown = rpc_call(
        &mut adapter_rpc,
        RequestId::Integer(3),
        "future/host-method",
        json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(denied_unknown.code, denied_command.code);
    assert_eq!(denied_unknown.message, denied_command.message);

    let denied_interactive = rpc_call(
        &mut adapter_rpc,
        RequestId::Integer(4),
        "thread/list",
        json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(denied_interactive.code, METHOD_NOT_AUTHORIZED_CODE);

    rpc_call(
        &mut adapter_rpc,
        RequestId::Integer(5),
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": "adapter ingress", "text_elements": [] }],
        }),
    )
    .await
    .unwrap();

    let ingress_events = rpc_call(
        &mut operator_rpc,
        RequestId::Integer(5),
        "thread/events/list",
        json!({
            "threadId": thread_id,
            "stream": "control",
            "kinds": ["io.ingress.received"],
        }),
    )
    .await
    .unwrap();
    let adapter_ingress = ingress_events["data"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["payload"]["principal"]["principal_id"] == ADAPTER_ID)
        .expect("adapter ingress principal witness");
    let via = adapter_ingress["payload"]["principal"]["via"]
        .as_str()
        .unwrap();
    let adapter_session_id = via.strip_prefix("caller:").expect("caller attribution");
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE session_id = ?1 AND principal_id = ?2 AND closed_at_ms IS NULL",
            params![adapter_session_id, ADAPTER_ID],
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE principal_id = ?1 AND reason_json LIKE '%method_not_authorized%'",
            params![ADAPTER_ID],
        )
        .await,
        3
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_host_effects WHERE principal_id = ?1 AND method = 'command/exec' AND witnessed_at_ms > 0",
            params![OPERATOR_ID],
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_host_effects WHERE principal_id = ?1",
            params![ADAPTER_ID],
        )
        .await,
        0
    );

    let marker = root.join("unwitnessed-command-ran");
    execute_sql(&store_path, "DROP TABLE cooldis_identity_host_effects").await;
    let witness_failure = rpc_call(
        &mut operator_rpc,
        RequestId::Integer(6),
        "command/exec",
        json!({
            "command": ["/usr/bin/touch", marker.to_string_lossy()],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(witness_failure.code, -32000);
    assert!(
        !marker.exists(),
        "host effect ran without a durable witness"
    );

    operator_rpc.close(None).await.unwrap();
    adapter_rpc.close(None).await.unwrap();
    server_task.abort();
    let _ = server_task.await;
    drop(app);
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn tcp_boundary_authenticates_before_upgrade_and_witnesses_sessions() {
    let root = test_root("tcp");
    let assets = root.join("console");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<html><head></head><body>console</body></html>",
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let listen = AppServerListenAddr::WebSocket(addr);
    let mut config = app_config(&root, listen);
    config.console_assets = Some(ConsoleAssetConfig {
        root: assets,
        session_token: "replaced-at-construction".to_string(),
    });

    let authority = identity_authority(&config).await;
    let operator = PrincipalId::new(OPERATOR_ID);
    let (_, _, accepted_token) = authority
        .bootstrap_operator(&operator, "Root operator")
        .await
        .unwrap();
    let (_, expired_token) = authority
        .mint_credential(&operator, &operator, Some(1))
        .await
        .unwrap();
    let (revoked, revoked_token) = authority
        .mint_credential(&operator, &operator, None)
        .await
        .unwrap();
    authority
        .revoke_credential(&operator, &revoked.credential_id)
        .await
        .unwrap();
    let revoked_operator = PrincipalId::new("operator:revoked");
    authority
        .declare_principal(
            &operator,
            &revoked_operator,
            PrincipalKind::Operator,
            "Revoked operator",
        )
        .await
        .unwrap();
    let (_, revoked_operator_token) = authority
        .mint_credential(&operator, &revoked_operator, None)
        .await
        .unwrap();
    authority
        .revoke_principal(&operator, &revoked_operator)
        .await
        .unwrap();
    let expired_revoked_operator = PrincipalId::new("operator:expired-and-revoked");
    authority
        .declare_principal(
            &operator,
            &expired_revoked_operator,
            PrincipalKind::Operator,
            "Expired and revoked operator",
        )
        .await
        .unwrap();
    let (_, expired_revoked_operator_token) = authority
        .mint_credential(&operator, &expired_revoked_operator, Some(1))
        .await
        .unwrap();
    authority
        .revoke_principal(&operator, &expired_revoked_operator)
        .await
        .unwrap();
    let fully_revoked_operator = PrincipalId::new("operator:credential-and-principal-revoked");
    authority
        .declare_principal(
            &operator,
            &fully_revoked_operator,
            PrincipalKind::Operator,
            "Credential and principal revoked operator",
        )
        .await
        .unwrap();
    let (fully_revoked_credential, fully_revoked_token) = authority
        .mint_credential(&operator, &fully_revoked_operator, Some(1))
        .await
        .unwrap();
    authority
        .revoke_credential(&operator, &fully_revoked_credential.credential_id)
        .await
        .unwrap();
    authority
        .revoke_principal(&operator, &fully_revoked_operator)
        .await
        .unwrap();
    drop(authority);

    config.apply_daemon_identity_config(&CooldisDaemonIdentityConfig {
        mode: IdentityMode::Managed,
        tenant_id: Some("test-tenant".to_string()),
        console_principal: Some(operator),
    });
    let app = CooldisAppServer::new(config).await.unwrap();
    let store_path = app.session_store_path().to_path_buf();
    let server = app.clone();
    let server_task = tokio::spawn(async move { server.serve_websocket_listener(listener).await });

    let mut client = CodexTuiTestClient::connect_websocket(
        &format!("ws://{addr}/rpc"),
        CodexTuiConnectConfig {
            bearer_token: Some(accepted_token.clone()),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        client.initialize_result()["userAgent"],
        "cooldis-app-server/0.1"
    );
    client.close().await.unwrap();

    for request in [
        websocket_request(addr, "/rpc", None),
        websocket_request(addr, "/rpc", Some("unknown-token")),
        websocket_request(addr, "/rpc", Some(&expired_token)),
        websocket_request(addr, "/rpc", Some(&revoked_token)),
        websocket_request(addr, "/rpc", Some(&revoked_operator_token)),
        websocket_request(addr, "/rpc", Some(&expired_revoked_operator_token)),
        websocket_request(addr, "/rpc", Some(&fully_revoked_token)),
        websocket_request(addr, &format!("/rpc?token={accepted_token}"), None),
    ] {
        let response = raw_tcp_request(addr, &request).await;
        assert!(
            response.starts_with("HTTP/1.1 401 Unauthorized"),
            "unexpected authentication response: {response:?}"
        );
        assert!(!response.contains(&accepted_token));
        assert!(!response.contains(&expired_token));
        assert!(!response.contains(&revoked_token));
        assert!(!response.contains(&revoked_operator_token));
        assert!(!response.contains(&expired_revoked_operator_token));
        assert!(!response.contains(&fully_revoked_token));
        assert!(response.ends_with("authentication required"));
        assert!(!response.contains("credential_revoked"));
        assert!(!response.contains("credential_expired"));
        assert!(!response.contains("principal_revoked"));
    }

    let fragmented_request = websocket_request(addr, "/rpc", Some(&accepted_token));
    let fragmented_response = fragmented_tcp_response_head(addr, &fragmented_request).await;
    assert!(
        fragmented_response.starts_with("HTTP/1.1 101 Switching Protocols"),
        "fragmented authenticated handshake was not upgraded"
    );

    let case_insensitive_request =
        websocket_request_with_authorization(addr, &format!("   bEaReR\t  {accepted_token}   "));
    let case_insensitive_response = tcp_response_head(addr, &case_insensitive_request).await;
    assert!(
        case_insensitive_response.starts_with("HTTP/1.1 101 Switching Protocols"),
        "case-insensitive bearer handshake was not upgraded"
    );

    let index = raw_tcp_request(
        addr,
        &format!("GET / HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
    )
    .await;
    let console_token = injected_console_token(&index);
    let mut request = format!("ws://{addr}/rpc").into_client_request().unwrap();
    request.headers_mut().insert(
        SEC_WEBSOCKET_PROTOCOL,
        HeaderValue::from_str(&format!("cooldis-console-token.{console_token}")).unwrap(),
    );
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut console, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap_or_else(|_| panic!("console WebSocket handshake failed"));
    let expected_protocol = format!("cooldis-console-token.{console_token}");
    assert!(
        response
            .headers()
            .get(SEC_WEBSOCKET_PROTOCOL)
            .and_then(|value| value.to_str().ok())
            == Some(expected_protocol.as_str()),
        "server did not echo the authenticated console subprotocol"
    );
    console.send(Message::Close(None)).await.unwrap();

    let listed_protocol_request = format!(
        "GET /rpc HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: unrelated.v1\r\nSec-WebSocket-Protocol: metrics.v1, cooldis-console-token.{console_token}\r\n\r\n"
    );
    let listed_protocol_response = tcp_response_head(addr, &listed_protocol_request).await;
    assert!(
        listed_protocol_response.starts_with("HTTP/1.1 101 Switching Protocols"),
        "recognized non-first console subprotocol was not upgraded"
    );
    assert!(
        response_header(&listed_protocol_response, "sec-websocket-protocol")
            == Some(expected_protocol.as_str()),
        "server did not select the recognized console subprotocol"
    );

    for path in ["/healthz", "/readyz"] {
        let response = raw_tcp_request(
            addr,
            &format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n"),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.ends_with("{\"status\":\"ok\"}"));
        assert!(!response.contains(OPERATOR_ID));
        assert!(!response.contains("managed"));
    }

    wait_for_sql_count(
        &store_path,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE closed_at_ms IS NOT NULL",
        5,
    )
    .await;
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface IN ('websocket', 'console') AND principal_id = ?1",
            params![OPERATOR_ID],
        )
        .await,
        10
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'websocket'",
            (),
        )
        .await,
        8
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE reason_json LIKE '%credential_expired%'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE reason_json LIKE '%principal_revoked%'",
            (),
        )
        .await,
        2
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE reason_json LIKE '%credential_revoked%'",
            (),
        )
        .await,
        2
    );

    server_task.abort();
    let _ = server_task.await;
    drop(app);
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_boundary_maps_same_uid_only_in_local_mode_and_secures_socket() {
    use std::os::unix::fs::PermissionsExt;

    let local_root = test_root("unix-local");
    let local_socket = local_root.join("app-server.sock");
    let local_config = app_config(&local_root, AppServerListenAddr::Unix(local_socket.clone()));
    let local_app = CooldisAppServer::new_local(local_config).await.unwrap();
    let local_store = local_app.session_store_path().to_path_buf();
    let local_server = local_app.clone();
    let local_listen = AppServerListenAddr::Unix(local_socket.clone());
    let mut local_task = tokio::spawn(async move { local_server.serve(local_listen).await });
    wait_for_path(&local_socket, &mut local_task).await;
    assert_eq!(
        std::fs::metadata(&local_socket)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let mut local_client =
        CodexTuiTestClient::connect_unix(&local_socket, CodexTuiConnectConfig::default())
            .await
            .unwrap();
    local_client.close().await.unwrap();
    wait_for_sql_count(
        &local_store,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'unix_socket' AND credential_ref LIKE 'peer_uid:%' AND closed_at_ms IS NOT NULL",
        1,
    )
    .await;

    let managed_root = test_root("unix-managed");
    let managed_socket = managed_root.join("app-server.sock");
    let mut managed_config = app_config(
        &managed_root,
        AppServerListenAddr::Unix(managed_socket.clone()),
    );
    let authority = identity_authority(&managed_config).await;
    let operator = PrincipalId::new(OPERATOR_ID);
    let (_, credential, token) = authority
        .bootstrap_operator(&operator, "Root operator")
        .await
        .unwrap();
    let (_, expired_token) = authority
        .mint_credential(&operator, &operator, Some(1))
        .await
        .unwrap();
    let (revoked_credential, revoked_token) = authority
        .mint_credential(&operator, &operator, None)
        .await
        .unwrap();
    authority
        .revoke_credential(&operator, &revoked_credential.credential_id)
        .await
        .unwrap();
    let revoked_operator = PrincipalId::new("operator:revoked");
    authority
        .declare_principal(
            &operator,
            &revoked_operator,
            PrincipalKind::Operator,
            "Revoked operator",
        )
        .await
        .unwrap();
    let (_, revoked_operator_token) = authority
        .mint_credential(&operator, &revoked_operator, None)
        .await
        .unwrap();
    authority
        .revoke_principal(&operator, &revoked_operator)
        .await
        .unwrap();
    drop(authority);
    managed_config.apply_daemon_identity_config(&CooldisDaemonIdentityConfig {
        mode: IdentityMode::Managed,
        tenant_id: Some("test-tenant".to_string()),
        console_principal: None,
    });
    let managed_app = CooldisAppServer::new(managed_config).await.unwrap();
    let managed_store = managed_app.session_store_path().to_path_buf();
    let local_dispatch = managed_app
        .local_json_rpc_request("account/read", serde_json::json!({}))
        .await
        .unwrap_err();
    assert!(
        local_dispatch
            .to_string()
            .contains("local-mode operator principal")
    );
    let managed_server = managed_app.clone();
    let managed_listen = AppServerListenAddr::Unix(managed_socket.clone());
    let mut managed_task = tokio::spawn(async move { managed_server.serve(managed_listen).await });
    wait_for_path(&managed_socket, &mut managed_task).await;
    assert_eq!(
        std::fs::metadata(&managed_socket)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );

    let no_token = CodexTuiTestClient::connect_unix(
        &managed_socket,
        CodexTuiConnectConfig {
            bearer_token: None,
            ..CodexTuiConnectConfig::default()
        },
    )
    .await;
    let no_token = match no_token {
        Ok(_) => panic!("managed same-uid connection unexpectedly authenticated"),
        Err(error) => error,
    };
    assert!(no_token.to_string().contains("401"));

    let unknown_token = CodexTuiTestClient::connect_unix(
        &managed_socket,
        CodexTuiConnectConfig {
            bearer_token: Some("unknown-token".to_string()),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await;
    let unknown_token = match unknown_token {
        Ok(_) => panic!("unknown Unix bearer token unexpectedly authenticated"),
        Err(error) => error,
    };
    assert!(unknown_token.to_string().contains("401"));

    for rejected_token in [expired_token, revoked_token, revoked_operator_token] {
        let rejected = CodexTuiTestClient::connect_unix(
            &managed_socket,
            CodexTuiConnectConfig {
                bearer_token: Some(rejected_token),
                ..CodexTuiConnectConfig::default()
            },
        )
        .await;
        let rejected = match rejected {
            Ok(_) => panic!("inactive Unix bearer token unexpectedly authenticated"),
            Err(error) => error,
        };
        assert!(rejected.to_string().contains("401"));
    }

    let mut token_client = CodexTuiTestClient::connect_unix(
        &managed_socket,
        CodexTuiConnectConfig {
            bearer_token: Some(token.clone()),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await
    .unwrap();
    token_client.close().await.unwrap();
    let fragmented_request =
        websocket_request("127.0.0.1:80".parse().unwrap(), "/rpc", Some(&token));
    let fragmented_response =
        fragmented_unix_response_head(&managed_socket, &fragmented_request).await;
    assert!(
        fragmented_response.starts_with("HTTP/1.1 101 Switching Protocols"),
        "fragmented authenticated Unix handshake was not upgraded"
    );
    wait_for_sql_count(
        &managed_store,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE closed_at_ms IS NOT NULL",
        2,
    )
    .await;
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'unix_socket' AND credential_ref = ?1",
            params![credential.credential_id],
        )
        .await,
        4
    );
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'unix_socket' AND reason_json LIKE '%peer_mapping_disabled%'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'unix_socket' AND reason_json LIKE '%principal_revoked%'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'unix_socket' AND reason_json LIKE '%credential_unknown%'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'unix_socket' AND reason_json LIKE '%credential_expired%'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'unix_socket' AND reason_json LIKE '%credential_revoked%'",
            (),
        )
        .await,
        1
    );

    local_task.abort();
    managed_task.abort();
    let _ = local_task.await;
    let _ = managed_task.await;
    let _ = std::fs::remove_dir_all(local_root);
    let _ = std::fs::remove_dir_all(managed_root);
}

#[tokio::test]
async fn console_credential_lifecycle_keeps_one_active_credential_across_restarts() {
    let root = test_root("console-lifecycle");
    let assets = root.join("console");
    std::fs::create_dir_all(&assets).unwrap();
    std::fs::write(
        assets.join("index.html"),
        "<html><head></head><body>console</body></html>",
    )
    .unwrap();
    let operator = PrincipalId::new(OPERATOR_ID);
    let bootstrap_config = app_config(
        &root,
        AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap()),
    );
    let authority = identity_authority(&bootstrap_config).await;
    authority
        .bootstrap_operator(&operator, "Root operator")
        .await
        .unwrap();
    drop(authority);
    let store_path = bootstrap_config.state_home.join("session_history.sqlite3");
    let baseline = active_credential_count(&store_path, OPERATOR_ID).await;

    let mut generations = Vec::new();
    for _ in 0..4 {
        let mut config = app_config(
            &root,
            AppServerListenAddr::WebSocket("127.0.0.1:0".parse().unwrap()),
        );
        config.console_assets = Some(ConsoleAssetConfig {
            root: assets.clone(),
            session_token: "replaced-at-construction".to_string(),
        });
        config.apply_daemon_identity_config(&CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("test-tenant".to_string()),
            console_principal: Some(operator.clone()),
        });
        let app = CooldisAppServer::new(config).await.unwrap();
        generations.push(app);
        assert_eq!(
            active_credential_count(&store_path, OPERATOR_ID).await,
            baseline + 1,
            "a restart left more than one active console credential"
        );
    }

    let record_path = root.join("state").join("console-credential-id");
    assert!(record_path.is_file());
    drop(generations);
    assert_eq!(
        active_credential_count(&store_path, OPERATOR_ID).await,
        baseline,
        "graceful app-server shutdown did not revoke its console credential"
    );
    assert!(!record_path.exists());
    let _ = std::fs::remove_dir_all(root);
}

fn app_config(root: &Path, listen: AppServerListenAddr) -> CooldisAppServerConfig {
    let mut config = CooldisAppServerConfig::local(listen, root.join("workspace"));
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    config.tenant_id = "test-tenant".to_string();
    config
}

async fn identity_authority(config: &CooldisAppServerConfig) -> SqliteIdentityAuthority {
    let store = SqliteSessionStore::open(config.state_home.join("session_history.sqlite3"))
        .await
        .unwrap();
    SqliteIdentityAuthority::new(store, Arc::new(SystemDaemonClock), None)
        .await
        .unwrap()
}

async fn connect_rpc(addr: std::net::SocketAddr, token: &str) -> WebSocketStream<TcpStream> {
    let mut request = format!("ws://{addr}/rpc").into_client_request().unwrap();
    request.headers_mut().insert(
        AUTHORIZATION,
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    let stream = TcpStream::connect(addr).await.unwrap();
    let (mut websocket, _) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap();
    rpc_call(
        &mut websocket,
        RequestId::String("initialize".to_string()),
        "initialize",
        json!({
            "clientInfo": {
                "name": "boundary-auth-test",
                "title": null,
                "version": "0",
            },
            "capabilities": {
                "experimentalApi": false,
                "requestAttestation": false,
            },
        }),
    )
    .await
    .unwrap();
    websocket
        .send(Message::Text(
            serde_json::to_string(&JsonRpcMessage::Notification(JsonRpcNotification {
                method: "initialized".to_string(),
                params: None,
            }))
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    websocket
}

async fn rpc_call(
    websocket: &mut WebSocketStream<TcpStream>,
    id: RequestId,
    method: &str,
    params: Value,
) -> Result<Value, JsonRpcErrorError> {
    websocket
        .send(Message::Text(
            serde_json::to_string(&JsonRpcMessage::Request(JsonRpcRequest {
                id: id.clone(),
                method: method.to_string(),
                params: Some(params),
                trace: None,
            }))
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    loop {
        let message = websocket.next().await.unwrap().unwrap();
        let Message::Text(text) = message else {
            continue;
        };
        match serde_json::from_str::<JsonRpcMessage>(&text).unwrap() {
            JsonRpcMessage::Response(response) if response.id == id => return Ok(response.result),
            JsonRpcMessage::Error(error) if error.id == id => return Err(error.error),
            JsonRpcMessage::Request(_)
            | JsonRpcMessage::Notification(_)
            | JsonRpcMessage::Response(_)
            | JsonRpcMessage::Error(_) => {}
        }
    }
}

fn websocket_request(addr: std::net::SocketAddr, path: &str, token: Option<&str>) -> String {
    let authorization = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{authorization}\r\n"
    )
}

fn websocket_request_with_authorization(addr: std::net::SocketAddr, authorization: &str) -> String {
    format!(
        "GET /rpc HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\naUtHoRiZaTiOn:{authorization}\r\n\r\n"
    )
}

async fn raw_tcp_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
}

async fn fragmented_tcp_response_head(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let first = request.len() / 3;
    let second = first * 2;
    stream
        .write_all(&request.as_bytes()[..first])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    stream
        .write_all(&request.as_bytes()[first..second])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    stream
        .write_all(&request.as_bytes()[second..])
        .await
        .unwrap();
    read_response_head(&mut stream).await
}

#[cfg(unix)]
async fn fragmented_unix_response_head(path: &Path, request: &str) -> String {
    let mut stream = tokio::net::UnixStream::connect(path).await.unwrap();
    let first = request.len() / 3;
    let second = first * 2;
    stream
        .write_all(&request.as_bytes()[..first])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    stream
        .write_all(&request.as_bytes()[first..second])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    stream
        .write_all(&request.as_bytes()[second..])
        .await
        .unwrap();
    read_response_head(&mut stream).await
}

async fn tcp_response_head(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    read_response_head(&mut stream).await
}

async fn read_response_head<S>(stream: &mut S) -> String
where
    S: tokio::io::AsyncRead + Unpin,
{
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut byte = [0_u8; 1];
        while !response.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).await.unwrap();
            response.push(byte[0]);
        }
    })
    .await
    .unwrap();
    String::from_utf8(response).unwrap()
}

fn response_header<'a>(response: &'a str, expected_name: &str) -> Option<&'a str> {
    response.split("\r\n").find_map(|line| {
        let (name, value) = line.split_once(':')?;
        name.eq_ignore_ascii_case(expected_name)
            .then_some(value.trim())
    })
}

fn injected_console_token(response: &str) -> String {
    let marker = "sessionToken:";
    let start = response.find(marker).unwrap() + marker.len();
    let value = response[start..].split('}').next().unwrap();
    serde_json::from_str(value).unwrap()
}

async fn wait_for_path(
    path: &Path,
    task: &mut tokio::task::JoinHandle<cooldis::CooldisResult<()>>,
) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if path.exists() {
                break;
            }
            if task.is_finished() {
                let outcome = (&mut *task).await;
                panic!(
                    "app-server exited before creating {}: {outcome:?}",
                    path.display()
                );
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_for_sql_count(path: &Path, query: &str, expected: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if sql_count(path, query, ()).await >= expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn sql_count<P>(path: &Path, query: &str, params: P) -> i64
where
    P: cooldis_sqlite::IntoParams,
{
    let store = SqliteSessionStore::open(path).await.unwrap();
    let database = store.sqlite_database();
    let connection = database.connect().await.unwrap();
    let mut rows = connection.query(query, params).await.unwrap();
    rows.next().await.unwrap().unwrap().get(0).unwrap()
}

async fn execute_sql(path: &Path, statement: &str) {
    let store = SqliteSessionStore::open(path).await.unwrap();
    let database = store.sqlite_database();
    let connection = database.connect().await.unwrap();
    connection.execute_batch(statement).await.unwrap();
}

async fn active_credential_count(path: &Path, principal_id: &str) -> i64 {
    sql_count(
        path,
        "SELECT COUNT(*) FROM cooldis_identity_credentials WHERE principal_id = ?1 AND revoked_at_ms IS NULL",
        params![principal_id],
    )
    .await
}

fn test_root(label: &str) -> PathBuf {
    PathBuf::from("/tmp").join(format!("cdis-ba-{label}-{}", uuid::Uuid::new_v4().simple()))
}
