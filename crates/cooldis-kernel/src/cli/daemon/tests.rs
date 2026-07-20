use super::*;
use crate::daemon::identity::{IdentityMode, PrincipalId};
use crate::{
    AgentManifestPlacementBinding, AgentManifestWorkspaceBinding, AgentManifestWorkspaceMode,
    CooldisDaemonConfig,
};

#[test]
fn parse_daemon_service_print_uses_explicit_target_and_config() {
    let args = vec![
        "--target",
        "systemd",
        "--config",
        "/tmp/cooldis.toml",
        "--bin",
        "/usr/local/bin/cooldis",
        "--label",
        "com.example.cooldis",
        "--working-directory",
        "/tmp/work",
    ]
    .into_iter()
    .map(OsString::from)
    .collect();

    let parsed = parse_daemon_service_print_args(args).unwrap();

    assert_eq!(parsed.target, CooldisDaemonServiceTarget::Systemd);
    assert_eq!(parsed.config_path, PathBuf::from("/tmp/cooldis.toml"));
    assert_eq!(parsed.executable, PathBuf::from("/usr/local/bin/cooldis"));
    assert_eq!(parsed.label, "com.example.cooldis");
    assert_eq!(parsed.working_directory, Some(PathBuf::from("/tmp/work")));
}

#[test]
fn parse_daemon_service_uninstall_accepts_target_and_label() {
    let args = vec!["--target", "launchd", "--label", "com.example.cooldis"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_daemon_service_uninstall_args(args).unwrap();

    assert_eq!(parsed.target, CooldisDaemonServiceTarget::Launchd);
    assert_eq!(parsed.label, "com.example.cooldis");
}

#[test]
fn parse_daemon_run_accepts_config_only() {
    let args = vec!["--config", "/tmp/cooldis.toml"]
        .into_iter()
        .map(OsString::from)
        .collect();

    let parsed = parse_daemon_run_args(args).unwrap();

    assert_eq!(parsed.config_path, Some(PathBuf::from("/tmp/cooldis.toml")));
}

#[test]
fn daemon_app_server_config_from_loaded_keeps_registry_defaults_when_unset() {
    let root = std::env::temp_dir().join(format!("cooldis-daemon-defaults-{}", Uuid::now_v7()));
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.app_server.listen = "unix:///tmp/cooldis-daemon-defaults.sock".to_string();
    daemon_config.runtime.cwd = Some(root.join("work"));

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    // lexicon-allow: capsule - existing app-server config field
    assert_eq!(
        app_config.capsule_bindings.registry_root,
        Some(PathBuf::from(".cooldis/operations"))
    );
    assert_eq!(
        app_config.agent_registry_root,
        PathBuf::from(".cooldis/agents")
    );
    assert_eq!(
        app_config.default_placement,
        AgentManifestPlacementBinding::default()
    );
    assert_eq!(app_config.default_workspace, None);
}

#[test]
fn daemon_app_server_config_from_loaded_applies_identity_config() {
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.identity.mode = IdentityMode::Managed;
    daemon_config.identity.tenant_id = Some("tenant-configured".to_string());
    daemon_config.identity.console_principal = Some(PrincipalId::new("operator-configured"));

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(app_config.tenant_id, "tenant-configured");
    assert_eq!(app_config.user_id, "operator-configured");
    assert_eq!(app_config.identity_mode, IdentityMode::Managed);
    assert_eq!(
        app_config.console_principal,
        Some(PrincipalId::new("operator-configured"))
    );
}

#[test]
fn daemon_app_server_config_from_loaded_revalidates_identity_config() {
    for (tenant_id, console_principal, expected_field) in [
        (
            Some("   ".to_string()),
            Some(PrincipalId::new("operator")),
            "tenant_id",
        ),
        (Some("tenant".to_string()), None, "console_principal"),
        (
            Some("tenant".to_string()),
            Some(PrincipalId::new("\t")),
            "console_principal",
        ),
    ] {
        let mut daemon_config = CooldisDaemonConfig::default();
        daemon_config.identity.mode = IdentityMode::Managed;
        daemon_config.identity.tenant_id = tenant_id;
        daemon_config.identity.console_principal = console_principal;

        let error =
            daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap_err();

        assert!(
            error.to_string().contains(expected_field),
            "expected {expected_field} validation error, got {error}"
        );
    }
}

#[test]
fn daemon_app_server_config_from_loaded_applies_placement_default() {
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.runtime.placement = Some(AgentManifestPlacementBinding {
        target: crate::PlacementTarget::Remote,
        executor_ref: Some("executor://cluster/default".to_string()),
        config: BTreeMap::new(),
    });

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(
        app_config.default_placement,
        AgentManifestPlacementBinding {
            target: crate::PlacementTarget::Remote,
            executor_ref: Some("executor://cluster/default".to_string()),
            config: BTreeMap::new(),
        }
    );
}

#[test]
fn daemon_app_server_config_from_loaded_applies_workspace_default() {
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.runtime.workspace = Some(AgentManifestWorkspaceBinding {
        host_path: PathBuf::from("/tmp/cooldis-workspace"),
        mode: AgentManifestWorkspaceMode::ReadWrite,
    });

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(
        app_config.default_workspace,
        Some(AgentManifestWorkspaceBinding {
            host_path: PathBuf::from("/tmp/cooldis-workspace"),
            mode: AgentManifestWorkspaceMode::ReadWrite,
        })
    );
}

#[test]
fn daemon_app_server_config_from_loaded_applies_registry_roots() {
    let root = std::env::temp_dir().join(format!("cooldis-daemon-registries-{}", Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("cooldis.toml");
    std::fs::write(
        &path,
        r#"
[daemon.runtime]
cwd = "work"

[daemon.app_server]
listen = "unix://run/cooldis.sock"

[daemon.registries]
operations = ".cooldis/operations"
agents = ".cooldis/agents"
"#,
    )
    .unwrap();
    let loaded = load_cooldis_daemon_config(Some(&path)).unwrap();

    let app_config = daemon_app_server_config_from_loaded(&loaded).unwrap();

    assert_eq!(app_config.cwd, root.join("work"));
    assert_eq!(
        // lexicon-allow: capsule - existing app-server config field
        app_config.capsule_bindings.registry_root,
        Some(root.join(".cooldis/operations"))
    );
    assert_eq!(app_config.agent_registry_root, root.join(".cooldis/agents"));

    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn daemon_app_server_config_from_loaded_applies_operations_policy() {
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.app_server.listen = "unix:///tmp/cooldis-daemon-operations.sock".to_string();
    daemon_config.operations.global_operation_names =
        vec!["http_fetch".to_string(), "json_query".to_string()];
    daemon_config.operations.load_all_active_when_unbound = true;

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(
        // lexicon-allow: capsule - existing app-server config field
        app_config.capsule_bindings.registry_root,
        Some(PathBuf::from(".cooldis/operations"))
    );
    assert_eq!(
        // lexicon-allow: capsule - existing app-server config field
        app_config.capsule_bindings.global_operation_names,
        vec!["http_fetch", "json_query"]
    );
    // lexicon-allow: capsule - existing app-server config field
    assert!(app_config.capsule_bindings.load_all_active_when_unbound);
}

#[test]
fn daemon_app_server_config_from_loaded_absolutizes_relative_registry_roots() {
    let current_dir = std::env::current_dir().unwrap();
    let mut daemon_config = CooldisDaemonConfig::default();
    daemon_config.app_server.listen = "unix:///tmp/cooldis-daemon-relative.sock".to_string();
    daemon_config.runtime.cwd = Some(PathBuf::from("config/work"));
    daemon_config.registries.operations = Some(PathBuf::from("config/.cooldis/operations"));
    daemon_config.registries.agents = Some(PathBuf::from("config/.cooldis/agents"));

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();

    assert_eq!(app_config.cwd, PathBuf::from("config/work"));
    assert_eq!(
        app_config.capsule_bindings.registry_root, // lexicon-allow: capsule - existing app-server config field
        Some(current_dir.join("config/.cooldis/operations"))
    );
    assert_eq!(
        app_config.agent_registry_root,
        current_dir.join("config/.cooldis/agents")
    );
}

#[tokio::test]
async fn daemon_default_operation_registry_binds_agent_manifest_without_registries_config() {
    let root = daemon_test_root("default-operation-bind");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = workspace.join(".cooldis/operations");
    let agent_registry_root = workspace.join(".cooldis/agents");
    let record =
        publish_daemon_test_operation(&operation_registry_root, "http_fetch", "http_fetch").await;
    publish_daemon_test_agent(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        "researcher",
        "http_fetch",
        "http_fetch",
        &format!(
            "op://http_fetch/http_fetch@sha256:{}",
            record.active_artifact_hash
        ),
    );
    let daemon_config = daemon_test_config(&root, &workspace);

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();
    let app = CooldisAppServer::new_local(app_config).await.unwrap();

    let operations = app
        .local_json_rpc_request("operation/list", json!({}))
        .await
        .unwrap();
    assert!(
        operations["data"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| operation["name"].as_str() == Some("http_fetch"))
    );
    let thread = app
        .local_json_rpc_request(
            "thread/start",
            json!({ "agentRef": "agent://researcher@latest" }),
        )
        .await
        .unwrap();
    assert!(thread["thread"]["id"].as_str().is_some());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_default_operation_registry_absent_rejects_agent_manifest_publish() {
    let root = daemon_test_root("default-operation-absent");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = workspace.join(".cooldis/operations");
    let agent_registry_root = workspace.join(".cooldis/agents");
    let err = publish_daemon_test_agent_result(
            &root,
            &agent_registry_root,
            &operation_registry_root,
            "researcher",
            "http_fetch",
            "http_fetch",
            "op://http_fetch/http_fetch@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap_err();
    assert!(err.to_string().contains("none was found"));
    assert!(!operation_registry_root.exists());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn daemon_operations_load_all_uses_default_registry_for_default_manifest() {
    let root = daemon_test_root("default-operation-load-all");
    let workspace = root.join("workspace");
    fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = workspace.join(".cooldis/operations");
    for operation_name in ["http_fetch", "file_read", "json_query"] {
        publish_daemon_test_operation(&operation_registry_root, operation_name, operation_name)
            .await;
    }
    let mut daemon_config = daemon_test_config(&root, &workspace);
    daemon_config.operations.load_all_active_when_unbound = true;

    let app_config =
        daemon_app_server_config_from_loaded(&loaded_daemon_config(daemon_config)).unwrap();
    let app = CooldisAppServer::new_local(app_config).await.unwrap();

    let default_agent = LocalAgentRegistry::new(workspace.join(".cooldis/agents"))
        .load_ref("agent://cooldis/default@latest")
        .unwrap();
    let tools = default_agent.resolved_manifest["tools"].as_array().unwrap();
    for command in ["http_fetch", "file_read", "json_query"] {
        let row = tools
            .iter()
            .find(|tool| tool["command"].as_str() == Some(command))
            .unwrap_or_else(|| panic!("missing default manifest bash command {command}"));
        assert_eq!(row["type"].as_str(), Some("bash_tool"));
    }
    let thread = app
        .local_json_rpc_request("thread/start", json!({}))
        .await
        .unwrap();
    assert!(thread["thread"]["id"].as_str().is_some());

    let _ = fs::remove_dir_all(root);
}

fn daemon_test_root(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("cooldis-daemon-{name}-{}", Uuid::now_v7()))
}

fn daemon_test_config(root: &Path, workspace: &Path) -> CooldisDaemonConfig {
    let mut config = CooldisDaemonConfig::default();
    config.app_server.listen = format!("unix://{}", root.join("daemon.sock").display());
    config.runtime.cwd = Some(workspace.to_path_buf());
    config.runtime.runtime_home = Some(root.join("runtime"));
    config.runtime.state_home = Some(root.join("state"));
    config
}

async fn publish_daemon_test_operation(
    registry_root: &Path,
    record_name: &str,
    operation_name: &str,
) -> PublishedOperationRecord {
    fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(daemon_test_operation_guest(operation_name))
        .expect("daemon test operation fixture should compile");
    let artifact_path = registry_root.join(format!("{record_name}.wasm"));
    fs::write(&artifact_path, wasm).unwrap();
    LocalOperationRegistry::new(registry_root)
        .publish_artifact(PublishOperationRequest {
            name: record_name.to_string(),
            artifact_path: artifact_path.clone(),
            source: PublishedOperationSource::Wasm {
                bin_path: artifact_path,
            },
            interface: None,
            capability_grants: Default::default(),
            metadata: Default::default(),
        })
        .await
        .unwrap()
}

fn publish_daemon_test_agent(
    root: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    name: &str,
    command: &str,
    tool_id: &str,
    operation_ref: &str,
) -> PublishedAgentRecord {
    publish_daemon_test_agent_result(
        root,
        agent_registry_root,
        operation_registry_root,
        name,
        command,
        tool_id,
        operation_ref,
    )
    .unwrap()
}

fn publish_daemon_test_agent_result(
    root: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    name: &str,
    command: &str,
    tool_id: &str,
    operation_ref: &str,
) -> CooldisResult<PublishedAgentRecord> {
    let manifest_path = root.join(format!("{name}.cooldis.agent.toml"));
    fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "{name}"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[[tools]]
type = "bash_tool"
id = "{tool_id}"
command = "{command}"
operation_ref = "{operation_ref}"

[runtime]
default_cwd = "."
streaming = false
"#
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, operation_registry_root)
}

fn daemon_test_operation_guest(operation_name: &str) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": operation_name,
            "input": "bytes",
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": []
        }]
    })
    .to_string();
    format!(
        r#"
            (module
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "ok")
              (func (export "__cooldis_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__cooldis_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 2
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
    )
}

fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b'\n' => "\\0a".to_string(),
            b'\r' => "\\0d".to_string(),
            b'\t' => "\\09".to_string(),
            b'"' => "\\22".to_string(),
            b'\\' => "\\5c".to_string(),
            0x20..=0x7e => (*byte as char).to_string(),
            _ => format!("\\{byte:02x}"),
        })
        .collect()
}

fn loaded_daemon_config(config: CooldisDaemonConfig) -> LoadedCooldisDaemonConfig {
    LoadedCooldisDaemonConfig {
        config,
        path: None,
        base_dir: std::env::current_dir().unwrap(),
    }
}
