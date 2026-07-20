use cooldis::daemon::identity::{
    CooldisDaemonIdentityConfig, IdentityAuthority, IdentityMode, PrincipalId,
    SqliteIdentityAuthority,
};
use cooldis::{
    AppServerListenAddr, CodexTuiConnectConfig, CodexTuiTestClient, ConsoleAssetConfig,
    CooldisAppServer, CooldisAppServerConfig, SqliteSessionStore, SystemDaemonClock,
};
use cooldis_sqlite::params;
use futures_util::SinkExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;

const OPERATOR_ID: &str = "operator:root";

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
    drop(authority);

    let app = CooldisAppServer::new(
        config,
        CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("test-tenant".to_string()),
            console_principal: Some(operator),
        },
    )
    .await
    .unwrap();
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
    }

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
    let (mut console, _) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap();
    console.send(Message::Close(None)).await.unwrap();

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
        2,
    )
    .await;
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface IN ('websocket', 'console') AND principal_id = ?1",
            params![OPERATOR_ID],
        )
        .await,
        4
    );
    assert_eq!(
        sql_count(
            &store_path,
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'websocket'",
            (),
        )
        .await,
        5
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
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE reason_json LIKE '%credential_revoked%'",
            (),
        )
        .await,
        1
    );

    server_task.abort();
    let _ = server_task.await;
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
    let managed_config = app_config(
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
    drop(authority);
    let managed_app = CooldisAppServer::new(
        managed_config,
        CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("test-tenant".to_string()),
            console_principal: None,
        },
    )
    .await
    .unwrap();
    let managed_store = managed_app.session_store_path().to_path_buf();
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

    for rejected_token in [expired_token, revoked_token] {
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
            bearer_token: Some(token),
            ..CodexTuiConnectConfig::default()
        },
    )
    .await
    .unwrap();
    token_client.close().await.unwrap();
    wait_for_sql_count(
        &managed_store,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE closed_at_ms IS NOT NULL",
        1,
    )
    .await;
    assert_eq!(
        sql_count(
            &managed_store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'unix_socket' AND credential_ref = ?1",
            params![credential.credential_id],
        )
        .await,
        2
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

fn websocket_request(addr: std::net::SocketAddr, path: &str, token: Option<&str>) -> String {
    let authorization = token
        .map(|token| format!("Authorization: Bearer {token}\r\n"))
        .unwrap_or_default();
    format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n{authorization}\r\n"
    )
}

async fn raw_tcp_request(addr: std::net::SocketAddr, request: &str) -> String {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    String::from_utf8(response).unwrap()
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

fn test_root(label: &str) -> PathBuf {
    PathBuf::from("/tmp").join(format!("cdis-ba-{label}-{}", uuid::Uuid::new_v4().simple()))
}
