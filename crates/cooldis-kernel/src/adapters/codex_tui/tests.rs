use super::*;
use crate::{AppServerListenAddr, CooldisAppServer, CooldisAppServerConfig};
use uuid::Uuid;

#[test]
fn codex_tui_initialize_request_uses_codex_remote_shape() {
    let message = JsonRpcMessage::Request(JsonRpcRequest {
        id: RequestId::String("initialize".to_string()),
        method: "initialize".to_string(),
        params: Some(json!({
            "clientInfo": {
                "name": "codex",
                "title": null,
                "version": "0",
            },
            "capabilities": {
                "experimentalApi": true,
                "requestAttestation": false,
                "optOutNotificationMethods": null,
            },
        })),
        trace: None,
    });
    let encoded = serde_json::to_value(message).unwrap();
    assert_eq!(
        encoded,
        json!({
            "id": "initialize",
            "method": "initialize",
            "params": {
                "clientInfo": {
                    "name": "codex",
                    "title": null,
                    "version": "0",
                },
                "capabilities": {
                    "experimentalApi": true,
                    "requestAttestation": false,
                    "optOutNotificationMethods": null,
                },
            },
        })
    );
}

#[cfg(unix)]
#[tokio::test]
async fn codex_tui_driver_runs_prompt_against_app_server() {
    let root = PathBuf::from("/tmp").join(format!("cdis-tui-{}", Uuid::now_v7().simple()));
    let socket = root.join("app.sock");
    let listen = AppServerListenAddr::Unix(socket.clone());
    let mut config =
        CooldisAppServerConfig::local(listen.clone(), std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let server = CooldisAppServer::new_local(config).await.unwrap();
    let server_task = tokio::spawn(async move { server.serve(listen).await });

    wait_for_socket(&socket).await.unwrap();

    let mut client = CodexTuiTestClient::connect_unix(&socket, CodexTuiConnectConfig::default())
        .await
        .unwrap();
    assert_eq!(
        client.initialize_result()["userAgent"],
        "cooldis-app-server/0.1"
    );
    assert_eq!(
        client.account_read().await.unwrap()["requiresOpenaiAuth"],
        false
    );
    assert!(
        !client.model_list().await.unwrap()["data"]
            .as_array()
            .unwrap()
            .is_empty()
    );

    let completed = client
        .run_prompt("hello from copied tui", Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(completed.assistant_text, "local:hello from copied tui");
    assert!(
        completed
            .notifications
            .iter()
            .any(|notification| { notification.method == "item/agentMessage/delta" })
    );
    let control_page = client
        .request(
            "thread/events/list",
            json!({
                "threadId": completed.thread_id,
                "stream": "control",
            }),
        )
        .await
        .unwrap();
    let thread_page = client
        .request(
            "thread/events/list",
            json!({
                "threadId": completed.thread_id,
            }),
        )
        .await
        .unwrap();
    let admission = crate::kernel::admission::assert_admission_precedes_turn_values(
        control_page["data"].as_array().unwrap(),
        thread_page["data"].as_array().unwrap(),
    );
    assert_eq!(admission["payload"]["route_id"], "surface:app-server-rpc");
    assert_eq!(admission["payload"]["decision"], "queue");
    assert_eq!(admission["payload"]["admissible"], json!(["queue"]));

    client.close().await.unwrap();
    server_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
#[tokio::test]
async fn codex_tui_operator_client_covers_thread_lifecycle_methods() {
    let root = PathBuf::from("/tmp").join(format!("cdis-operator-{}", Uuid::now_v7().simple()));
    let socket = root.join("app.sock");
    let listen = AppServerListenAddr::Unix(socket.clone());
    let mut config =
        CooldisAppServerConfig::local(listen.clone(), std::env::current_dir().unwrap());
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    let server = CooldisAppServer::new_local(config).await.unwrap();
    let server_task = tokio::spawn(async move { server.serve(listen).await });

    wait_for_socket(&socket).await.unwrap();

    let mut client = CooldisOperatorClient::connect_unix(&socket, CodexTuiConnectConfig::default())
        .await
        .unwrap();
    let config = client.config_read(false).await.unwrap();
    assert_eq!(
        config["config"]["model"].as_str(),
        Some(crate::APP_SERVER_LOCAL_MODEL)
    );

    let thread = client.thread_start(json!({})).await.unwrap();
    client
        .thread_name_set(&thread.id, "operator smoke")
        .await
        .unwrap();
    let resumed = client.thread_resume(&thread.id, false).await.unwrap();
    assert_eq!(resumed.id, thread.id);
    assert_eq!(resumed.raw["name"].as_str(), Some("operator smoke"));

    let forked = client.thread_fork(&thread.id).await.unwrap();
    assert_ne!(forked.id, thread.id);
    assert_eq!(
        forked.raw["parentThreadId"].as_str(),
        Some(thread.id.as_str())
    );

    assert_eq!(
        client.thread_compact_start(&forked.id).await.unwrap(),
        json!({})
    );

    client.close().await.unwrap();
    server_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[cfg(unix)]
async fn wait_for_socket(path: &PathBuf) -> CooldisResult<()> {
    for _ in 0..100 {
        if path.exists() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(tui_error(format!(
        "timed out waiting for socket {}",
        path.display()
    )))
}
