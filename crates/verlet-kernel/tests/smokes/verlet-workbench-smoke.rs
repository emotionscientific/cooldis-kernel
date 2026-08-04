use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use uuid::Uuid;
use verlet::daemon::identity::{IdentityAuthority, PrincipalId, SqliteIdentityAuthority};
use verlet::{
    AppServerListenAddr, CapsuleBindingsConfig, CodexTuiConnectConfig, CodexTuiTestClient,
    LocalAgentRegistry, LocalOperationRegistry, PublishedOperationBuild, PublishedOperationRecord,
    PublishedOperationSource, RegisteredOperation, SqliteSessionStore, SystemDaemonClock,
    VerletAppServer, VerletAppServerConfig, WasmOperationDefinition, WasmOperationEventKind,
    WasmOperationManifest, WasmOperationMode, WasmOperationValueKind, wasm_sha256,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from("/tmp").join(format!("cdis-workbench-{}", Uuid::now_v7().simple()));
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

async fn run(root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let workspace = root.join("workspace");
    let agent_registry_root = root.join("agents");
    let operation_registry_root = root.join("operations");
    std::fs::create_dir_all(&workspace)?;
    std::fs::create_dir_all(&operation_registry_root)?;
    publish_agent_manifest(root, &agent_registry_root)?;
    publish_operation_record(&operation_registry_root)?;

    let addr = unused_loopback_addr()?;
    let listen = AppServerListenAddr::WebSocket(addr);
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
            .request("config/read", json!({ "includeLayers": false }))
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
        let agents = client.request("agent/list", json!({})).await?;
        assert_nonempty_array(&agents, "agent/list data")?;
        let operations = client.request("operation/list", json!({})).await?;
        let operation_values = assert_array(&operations, "operation/list data")?;
        if !operation_values
            .iter()
            .any(|operation| operation["name"].as_str() == Some("workbench_lookup"))
        {
            return Err("operation/list did not expose the smoke operation".into());
        }

        let thread = client
            .thread_start(json!({ "agentRef": "agent://workbench-runner@latest" }))
            .await?;
        let turn = client
            .turn_start_text(&thread.id, "workbench smoke receipt")
            .await?;
        let completed = client
            .wait_for_turn_completed(&thread.id, &turn.id, Duration::from_secs(5))
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
                json!({
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
    root: &Path,
    workspace: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    listen: AppServerListenAddr,
) -> Result<(JoinHandle<verlet::VerletResult<()>>, String), Box<dyn std::error::Error>> {
    let mut config = VerletAppServerConfig::local(listen.clone(), workspace).with_capsule_bindings(
        CapsuleBindingsConfig::default().with_registry_root(operation_registry_root),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.to_path_buf();
    let server = VerletAppServer::new_local(config).await?;
    let store = SqliteSessionStore::open(server.session_store_path()).await?;
    let authority = SqliteIdentityAuthority::new(store, Arc::new(SystemDaemonClock), None).await?;
    let principal = PrincipalId::new(server.user_id());
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
) -> Result<CodexTuiTestClient<TcpStream>, Box<dyn std::error::Error>> {
    let mut last_error = None;
    for _ in 0..1_500 {
        match CodexTuiTestClient::connect_websocket(
            url,
            CodexTuiConnectConfig {
                client_name: "verlet-workbench-smoke".to_string(),
                bearer_token: Some(token.to_string()),
                ..CodexTuiConnectConfig::default()
            },
        )
        .await
        {
            Ok(client) => return Ok(client),
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(Duration::from_millis(20)).await;
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
    root: &Path,
    agent_registry_root: &Path,
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
    LocalAgentRegistry::new(agent_registry_root).publish_manifest_path(&manifest_path)?;
    Ok(())
}

fn publish_operation_record(
    operation_registry_root: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let name = "workbench_lookup";
    let artifact_path = operation_registry_root.join(format!("{name}.wasm"));
    let manifest = WasmOperationManifest {
        abi: "cooldis.operation/0.1".to_string(),
        operations: vec![WasmOperationDefinition {
            id: 1,
            name: "lookup".to_string(),
            input: WasmOperationValueKind::Text,
            output: WasmOperationValueKind::Text,
            events: WasmOperationEventKind::None,
            mode: WasmOperationMode::Sync,
            required_capabilities: Vec::new(),
        }],
    };
    let registered = RegisteredOperation {
        name: name.to_string(),
        manifest: manifest.clone(),
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
    };
    let record = PublishedOperationRecord {
        schema_version: 1,
        name: name.to_string(),
        active_artifact_hash: wasm_sha256(b"workbench smoke operation placeholder"),
        manifest,
        projections: registered.projections(),
        interface: None,
        capability_grants: BTreeSet::new(),
        metadata: BTreeMap::new(),
        source: PublishedOperationSource::Wasm {
            bin_path: artifact_path.clone(),
        },
        build: PublishedOperationBuild {
            artifact_path,
            published_at_ms: unix_timestamp_ms(),
        },
    };
    let registry = LocalOperationRegistry::new(operation_registry_root);
    let record_path = registry.record_path(name)?;
    if let Some(parent) = record_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&record_path, serde_json::to_vec_pretty(&record)?)?;
    registry.load_record(name)?;
    Ok(())
}

fn assert_nonempty_array(result: &Value, label: &str) -> Result<(), Box<dyn std::error::Error>> {
    let values = assert_array(result, label)?;
    if values.is_empty() {
        return Err(format!("{label} was empty").into());
    }
    Ok(())
}

fn assert_array<'a>(
    result: &'a Value,
    label: &str,
) -> Result<&'a Vec<Value>, Box<dyn std::error::Error>> {
    result["data"]
        .as_array()
        .ok_or_else(|| format!("{label} was not an array").into())
}

fn assert_event_kind(events: &Value, kind: &str) -> Result<(), Box<dyn std::error::Error>> {
    find_event(events, kind)
        .map(|_| ())
        .ok_or_else(|| format!("thread/events/list did not expose {kind}").into())
}

fn find_event<'a>(events: &'a Value, kind: &str) -> Option<&'a Value> {
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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
