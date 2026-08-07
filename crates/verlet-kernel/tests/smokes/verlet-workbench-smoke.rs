use verlet::daemon::identity::IdentityAuthority as _;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("cdis-workbench-{}", uuid::Uuid::now_v7().simple()));
    let result = run(&root).await;
    let cleanup = std::fs::remove_dir_all(&root);
    match (result, cleanup) {
        (Ok(()), Ok(())) => {
            println!("verlet workbench smoke ok: query surface and receipt events over websocket");
            Ok(())
        }
        (Ok(()), Err(err)) => {
            Err(format!("failed to remove temp state {}: {err}", root.display()).into())
        }
        (Err(err), _) => Err(err),
    }
}

async fn run(root: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let workspace = root.join("workspace");
    let agent_registry_root = root.join("agents");
    let operation_registry_root = root.join("operations");
    std::fs::create_dir_all(&workspace)?;
    std::fs::create_dir_all(&operation_registry_root)?;
    publish_agent_manifest(root, &agent_registry_root)?;
    publish_operation_record(&operation_registry_root)?;

    let addr = unused_loopback_addr()?;
    let listen = verlet::adapters::app_server::AppServerListenAddr::WebSocket(addr);
    let (server_task, token) = start_server(
        root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        listen.clone(),
    )
    .await?;
    let result: Result<(), Box<dyn std::error::Error>> = async {
        let mut client = connect_client(&format!("ws://{addr}/rpc"), &token).await?;
        assert_eq!(
            client.initialize_result()["userAgent"],
            "verlet-app-server/0.1"
        );

        let models = client.model_list().await?;
        assert_nonempty_array(&models, "model/list data")?;
        let threads = client.thread_list().await?;
        assert_array(&threads, "thread/list data")?;
        let config = client
            .request("config/read", serde_json::json!({ "includeLayers": false }))
            .await?;
        if config["config"]["model"].as_str().is_none() {
            return Err("config/read did not return config.model".into());
        }
        if !config["config"]["cwd"]
            .as_str()
            .is_some_and(|cwd| cwd.starts_with('/'))
        {
            return Err("config/read did not return an absolute config.cwd".into());
        }
        let agents = client.request("agent/list", serde_json::json!({})).await?;
        assert_nonempty_array(&agents, "agent/list data")?;
        let operations = client
            .request("operation/list", serde_json::json!({}))
            .await?;
        let operation_values = assert_array(&operations, "operation/list data")?;
        if !operation_values
            .iter()
            .any(|operation| operation["name"].as_str() == Some("workbench_lookup"))
        {
            return Err("operation/list did not expose the smoke operation".into());
        }

        let thread = client
            .thread_start(serde_json::json!({ "agentRef": "agent://workbench-runner@latest" }))
            .await?;
        let turn = client
            .turn_start_text(&thread.id, "workbench smoke receipt")
            .await?;
        let completed = client
            .wait_for_turn_completed(&thread.id, &turn.id, std::time::Duration::from_secs(5))
            .await?;
        if !completed
            .notifications
            .iter()
            .any(|notification| notification.method == "item/agentMessage/delta")
        {
            return Err("turn/start did not stream assistant deltas".into());
        }

        let events = client
            .request(
                "thread/events/list",
                serde_json::json!({
                    "threadId": thread.id,
                    "limit": 100,
                }),
            )
            .await?;
        assert_event_kind(&events, "manifest.compile.completed")?;
        assert_event_kind(&events, "manifest.bind.completed")?;
        let context_event = find_event(&events, "context.compile.completed")
            .ok_or("thread/events/list did not expose context.compile.completed")?;
        if context_event["origin"].as_str() != Some("discharged") {
            return Err("context compile receipt did not report discharged origin".into());
        }
        if !context_event["provenance"].is_object()
            || context_event["provenance"]["source_streams"]
                .as_array()
                .is_none_or(|streams| streams.is_empty())
        {
            return Err("context compile receipt did not expose non-empty provenance".into());
        }
        if !context_event["payload"].is_object() {
            return Err("context compile receipt payload was not an object".into());
        }

        client.close().await?;
        Ok(())
    }
    .await;
    server_task.abort();
    let _ = server_task.await;
    result
}

async fn start_server(
    root: &std::path::Path,
    workspace: &std::path::Path,
    agent_registry_root: &std::path::Path,
    operation_registry_root: &std::path::Path,
    listen: verlet::adapters::app_server::AppServerListenAddr,
) -> Result<
    (
        tokio::task::JoinHandle<verlet::kernel::runtime_host::VerletResult<()>>,
        String,
    ),
    Box<dyn std::error::Error>,
> {
    let mut config =
        verlet::adapters::app_server::VerletAppServerConfig::local(listen.clone(), workspace)
            .with_capsule_bindings(
                verlet::adapters::app_server::CapsuleBindingsConfig::default()
                    .with_registry_root(operation_registry_root),
            );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.to_path_buf();
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
    url: &str,
    token: &str,
) -> Result<
    verlet::adapters::codex_tui::CodexTuiTestClient<tokio::net::TcpStream>,
    Box<dyn std::error::Error>,
> {
    let mut last_error = None;
    for _ in 0..1_500 {
        match verlet::adapters::codex_tui::CodexTuiTestClient::connect_websocket(
            url,
            verlet::adapters::codex_tui::CodexTuiConnectConfig {
                client_name: "verlet-workbench-smoke".to_string(),
                bearer_token: Some(token.to_string()),
                ..verlet::adapters::codex_tui::CodexTuiConnectConfig::default()
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

fn publish_agent_manifest(
    root: &std::path::Path,
    agent_registry_root: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let manifest_path = root.join("workbench.verlet.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "workbench-runner"
version = "0.1.0"
display_name = "Workbench Runner"
description = "Smoke-test agent for the app-server query surface"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false
"#,
    )?;
    verlet::agent::manifest::LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path(&manifest_path)?;
    Ok(())
}

fn publish_operation_record(
    operation_registry_root: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "workbench_lookup";
    let artifact_path = operation_registry_root.join(format!("{name}.wasm"));
    let manifest = verlet_abi::WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![verlet_abi::WasmOperationDefinition {
            id: 1,
            name: "lookup".to_string(),
            input: verlet_abi::WasmOperationValueKind::Text,
            output: verlet_abi::WasmOperationValueKind::Text,
            events: verlet_abi::WasmOperationEventKind::None,
            mode: verlet_abi::WasmOperationMode::Sync,
            required_capabilities: Vec::new(),
        }],
    };
    let registered = verlet_operations::RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
    };
    let record = verlet_operations::operation_store::PublishedOperationRecord {
        schema_version: 1,
        name: name.to_string(),
        active_artifact_hash: verlet_operations::operation_store::wasm_sha256(
            b"workbench smoke operation placeholder",
        ),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: std::collections::BTreeSet::new(),
        metadata: std::collections::BTreeMap::new(),
        source: verlet_operations::operation_store::PublishedOperationSource::Wasm {
            bin_path: artifact_path.clone(),
        },
        build: verlet_operations::operation_store::PublishedOperationBuild {
            artifact_path,
            published_at_ms: unix_timestamp_ms(),
        },
    };
    let registry =
        verlet_operations::operation_store::LocalOperationRegistry::new(operation_registry_root);
    let record_path = registry.record_path(name)?;
    if let Some(parent) = record_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
    registry.load_record(name)?;
    Ok(())
}

fn assert_nonempty_array(
    result: &serde_json::Value,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let values = assert_array(result, label)?;
    if values.is_empty() {
        return Err(format!("{label} was empty").into());
    }
    Ok(())
}

fn assert_array<'a>(
    result: &'a serde_json::Value,
    label: &str,
) -> Result<&'a Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    result["data"]
        .as_array()
        .ok_or_else(|| format!("{label} was not an array").into())
}

fn assert_event_kind(
    events: &serde_json::Value,
    kind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    find_event(events, kind)
        .map(|_| ())
        .ok_or_else(|| format!("thread/events/list did not expose {kind}").into())
}

fn find_event<'a>(events: &'a serde_json::Value, kind: &str) -> Option<&'a serde_json::Value> {
    events["data"]
        .as_array()?
        .iter()
        .find(|event| event["kind"].as_str() == Some(kind))
}

fn unused_loopback_addr() -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?)
}

fn unix_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
