use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use verlet_history::EventStore as _;
use verlet_history::SessionStore as _;
use verlet_metadata::provider_store::ThreadMetadataStore as _;

#[path = "../support/model_catalog.rs"]
mod model_catalog_test_support;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    model_catalog_test_support::disable_in_process_refresh();
    run_parent().await?;
    run_researcher().await?;
    if std::env::var("VERLET_MANIFEST_E2E_LIVE").ok().as_deref() == Some("1") {
        run_researcher_live().await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "verlet-manifest-e2e-smoke/tests.rs"]
mod tests;

const FIXTURE_IO_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_FIXTURE_HEADER_BYTES: usize = 16 * 1024;
const MAX_FIXTURE_BODY_BYTES: usize = 1024 * 1024;

async fn run_parent() -> Result<(), Box<dyn std::error::Error>> {
    let root = default_root();
    let workspace = root.join("workspace");
    let agent_registry_root = root.join("agents");
    tokio::fs::create_dir_all(&workspace).await?;
    let record = publish_manifest(&root, &agent_registry_root)?;

    let first_app = build_app(&root).await?;
    let thread_start = first_app
        .local_json_rpc_request(
            "thread/start",
            serde_json::json!({
                "agentRef": "agent://manifest-e2e@latest",
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or("thread/start response missing thread id")?
        .to_string();
    inspect_manifest_events(&root, &thread_id, &record.manifest_hash, 1, true).await?;
    let first_output = run_turn(&first_app, &thread_id, "first manifest turn").await?;
    if !first_output.contains("local:first manifest turn") {
        return Err(format!(
            "first local turn did not complete from the manifest thread: {}",
            compact(&first_output)
        )
        .into());
    }

    drop(first_app);

    let second_app = build_app(&root).await?;
    let loaded = second_app
        .local_json_rpc_request("thread/loaded/list", serde_json::json!({}))
        .await?;
    let loaded_ids = loaded["data"]
        .as_array()
        .ok_or("thread/loaded/list data was not an array")?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<Vec<_>>();
    if !loaded_ids.contains(&thread_id.as_str()) {
        return Err(format!(
            "restarted app-server did not load manifest thread {thread_id}; loaded: {loaded_ids:?}"
        )
        .into());
    }
    second_app
        .local_json_rpc_request(
            "thread/resume",
            serde_json::json!({
                "threadId": thread_id,
                "excludeTurns": true,
            }),
        )
        .await?;
    inspect_manifest_events(&root, &thread_id, &record.manifest_hash, 2, true).await?;
    let second_output = run_turn(&second_app, &thread_id, "second manifest turn").await?;
    if !second_output.contains("local:second manifest turn") {
        return Err(format!(
            "second local turn did not complete from the resumed manifest thread: {}",
            compact(&second_output)
        )
        .into());
    }
    let inspection = inspect_history(&root, &thread_id).await?;
    let export_inspection = inspect_debug_export(&second_app, &thread_id).await?;

    println!(
        "verlet manifest e2e smoke ok root={} thread={} manifest_hash={} messages={} binds={} exported_events={} exported_receipts={}",
        root.display(),
        thread_id,
        record.manifest_hash,
        inspection.message_count,
        inspection.bind_count,
        export_inspection.event_count,
        export_inspection.receipt_count
    );
    Ok(())
}

async fn run_researcher() -> Result<(), Box<dyn std::error::Error>> {
    let root = default_named_root("researcher-manifest-e2e");
    let workspace = root.join("workspace");
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    tokio::fs::create_dir_all(&workspace).await?;
    tokio::fs::create_dir_all(&operation_registry_root).await?;
    let repo = repo_root();
    let standard_ops = publish_standard_ops(&repo, &operation_registry_root).await?;
    let record = publish_researcher_manifest(
        &repo,
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &standard_ops,
    )?;

    let command = concat!(
        "printf '%s' ",
        "'{\"json\":{\"items\":[{\"name\":\"Ada\"},{\"name\":\"Linus\"}]},\"pointer\":\"/items/1/name\"}' ",
        "| json_query",
        " && printf '%s\\n' 'bash to file_read workspace' > /workspace/researcher-file-read.txt",
        " && printf '%s' '{\"path\":\"/workspace/researcher-file-read.txt\"}' | file_read"
    );
    let provider = MockChatServer::start_text(vec![
        chat_tool_call_sse(command),
        chat_text_sse("RESEARCHER_JSON_QUERY_FILE_READ_OK"),
    ])
    .await?;
    let app = build_researcher_app(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        &provider.base_url(),
    )
    .await?;
    let thread_start = app
        .local_json_rpc_request(
            "thread/start",
            serde_json::json!({
                "agentRef": "agent://researcher@latest",
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or("researcher thread/start response missing thread id")?
        .to_string();
    inspect_manifest_events(&root, &thread_id, &record.manifest_hash, 1, true).await?;
    inspect_researcher_bind_receipt(&root, &thread_id).await?;

    let trace = run_turn_trace_with_timeout(
        &app,
        &thread_id,
        "use the researcher json_query and file_read tools",
        std::time::Duration::from_secs(60),
    )
    .await?;
    if !trace.output.contains("RESEARCHER_JSON_QUERY_FILE_READ_OK") {
        return Err(format!(
            "researcher turn did not complete with marker: {}",
            compact(&trace.output)
        )
        .into());
    }
    assert_researcher_tool_trace(&trace.runtime_events)?;

    let requests = provider.requests(2).await?;
    if requests[0].path != "/v1/chat/completions" || requests[0].body["stream"] != true {
        return Err(format!(
            "researcher provider request did not use streaming chat completions: path={} body={}",
            requests[0].path,
            compact(&requests[0].body.to_string())
        )
        .into());
    }
    if !requests[0]
        .body
        .get("tools")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .any(|tool| tool["function"]["name"].as_str() == Some("bash"))
    {
        return Err("researcher provider request did not advertise the bash tool".into());
    }
    let first_request_body = requests[0].body.to_string();
    for expected_command in ["http_fetch", "file_read", "json_query"] {
        if !first_request_body.contains(expected_command) {
            return Err(format!(
                "researcher provider request did not advertise {expected_command:?}: {}",
                compact(&first_request_body)
            )
            .into());
        }
    }
    provider.join().await?;

    println!(
        "verlet researcher manifest e2e smoke ok root={} thread={} manifest_hash={} tools=json_query,file_read",
        root.display(),
        thread_id,
        record.manifest_hash
    );
    Ok(())
}

async fn build_app(
    root: &std::path::Path,
) -> Result<verlet::adapters::app_server::VerletAppServer, Box<dyn std::error::Error>> {
    let listen =
        verlet::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse()?);
    let mut config = verlet::adapters::app_server::VerletAppServerConfig::local(
        listen.clone(),
        root.join("workspace"),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    Ok(verlet::adapters::app_server::VerletAppServer::new_local(config).await?)
}

async fn build_researcher_app(
    root: &std::path::Path,
    workspace: &std::path::Path,
    agent_registry_root: &std::path::Path,
    operation_registry_root: &std::path::Path,
    provider_base_url: &str,
) -> Result<verlet::adapters::app_server::VerletAppServer, Box<dyn std::error::Error>> {
    let listen =
        verlet::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse()?);
    let mut config = verlet::adapters::app_server::VerletAppServerConfig::local(listen, workspace)
        .with_openai_chat_completions(
            "openai_compatible",
            provider_base_url,
            "test-key",
            verlet_metadata::provider_store::OPENAI_COMPATIBLE_DEFAULT_MODEL,
        )
        // lexicon-allow: capsule - existing app-server operation binding API name
        .with_capsule_bindings(
            // lexicon-allow: capsule - existing app-server operation binding API name
            verlet::adapters::app_server::CapsuleBindingsConfig::default()
                .with_registry_root(operation_registry_root),
        );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = agent_registry_root.to_path_buf();
    Ok(verlet::adapters::app_server::VerletAppServer::new_local(config).await?)
}

fn publish_manifest(
    root: &std::path::Path,
    agent_registry_root: &std::path::Path,
) -> Result<verlet::agent::manifest::PublishedAgentRecord, Box<dyn std::error::Error>> {
    let manifest_path = root.join("manifest-e2e.verlet.agent.toml");
    std::fs::write(
        &manifest_path,
        r#"
[agent]
name = "manifest-e2e"
version = "0.1.0"
description = "Manifest e2e local smoke."
kind = "verlet.agent-manifest"
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
    Ok(
        verlet::agent::manifest::LocalAgentRegistry::new(agent_registry_root)
            .publish_manifest_path(&manifest_path)?,
    )
}

#[derive(Clone, Debug)]
struct StandardOpHashes {
    http_fetch: String,
    file_read: String,
    json_query: String,
}

async fn publish_standard_ops(
    repo: &std::path::Path,
    registry_root: &std::path::Path,
) -> Result<StandardOpHashes, Box<dyn std::error::Error>> {
    let http_fetch = publish_tool_package(&repo.join("tools/http-fetch"), registry_root).await?;
    let file_read = publish_tool_package(&repo.join("tools/file-read"), registry_root).await?;
    let json_query = publish_tool_package(&repo.join("tools/json-query"), registry_root).await?;
    Ok(StandardOpHashes {
        http_fetch: http_fetch.active_artifact_hash,
        file_read: file_read.active_artifact_hash,
        json_query: json_query.active_artifact_hash,
    })
}

async fn publish_tool_package(
    package_path: &std::path::Path,
    registry_root: &std::path::Path,
) -> Result<verlet_operations::operation_store::PublishedOperationRecord, Box<dyn std::error::Error>>
{
    let package = verlet_operations::tool_package::ToolPackageSource::load(package_path)?;
    let (artifact_path, source) = build_tool_package_artifact(&package)?;
    let capability_grants = package_capability_requests(&package);
    let manifest = verlet::capabilities::wasm_runner::WasmRuntimeFactory::new(
        verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::path(
            artifact_path.clone(),
        ))
        .with_capability_grants(capability_grants.clone()),
    )?
    .validate_operation_artifact()
    .await?;
    let registered = verlet_operations::RegisteredOperation {
        name: package.manifest.identity.name.clone(),
        manifest: manifest.clone(),
        capability_grants: capability_grants.clone(),
        metadata: std::collections::BTreeMap::new(),
    };
    let interface = verlet_operations::tool_package::ToolInterfaceContract::from_package(
        &package,
        &manifest,
        &registered.projections(),
    )?;
    Ok(
        verlet_operations::operation_store::LocalOperationRegistry::new(registry_root)
            .publish_artifact(
                verlet_operations::operation_store::PublishOperationRequest {
                    name: package.manifest.identity.name.clone(),
                    artifact_path,
                    source,
                    interface: Some(interface),
                    capability_grants,
                    metadata: std::collections::BTreeMap::new(),
                },
            )
            .await?,
    )
}

fn build_tool_package_artifact(
    package: &verlet_operations::tool_package::ToolPackageSource,
) -> Result<
    (
        std::path::PathBuf,
        verlet_operations::operation_store::PublishedOperationSource,
    ),
    Box<dyn std::error::Error>,
> {
    match (
        package.manifest.runtime.module_path.clone(),
        package.manifest.runtime.bin_path.clone(),
    ) {
        (Some(module_path), None) => {
            let release = package.manifest.runtime.release.unwrap_or(true);
            let build = verlet::operations::operation_builder::build_rust_wasm_module(
                verlet::operations::operation_builder::RustWasmBuildOptions::new(&module_path)
                    .with_release(release),
            )?;
            Ok((
                build.artifact_path,
                verlet_operations::operation_store::PublishedOperationSource::Rust {
                    module_path,
                    release,
                },
            ))
        }
        (None, Some(bin_path)) => Ok((
            bin_path.clone(),
            verlet_operations::operation_store::PublishedOperationSource::Wasm { bin_path },
        )),
        (Some(_), Some(_)) => {
            Err("tool package runtime cannot declare both module_path and bin_path".into())
        }
        (None, None) => Err("tool package runtime requires module_path or bin_path".into()),
    }
}

fn package_capability_requests(
    package: &verlet_operations::tool_package::ToolPackageSource,
) -> std::collections::BTreeSet<String> {
    package
        .manifest
        .operations
        .iter()
        .flat_map(|operation| operation.required_capabilities.iter().cloned())
        .collect()
}

fn publish_researcher_manifest(
    repo: &std::path::Path,
    root: &std::path::Path,
    agent_registry_root: &std::path::Path,
    operation_registry_root: &std::path::Path,
    standard_ops: &StandardOpHashes,
) -> Result<verlet::agent::manifest::PublishedAgentRecord, Box<dyn std::error::Error>> {
    let manifest_path = root.join("researcher.verlet.agent.toml");
    let template = std::fs::read_to_string(
        repo.join("examples/agents/researcher/researcher.verlet.agent.toml.in"),
    )?;
    let rendered = render_researcher_template(&template, standard_ops)?;
    std::fs::write(&manifest_path, rendered)?;
    Ok(
        verlet::agent::manifest::LocalAgentRegistry::new(agent_registry_root)
            .publish_manifest_path_with_operation_registry(
                &manifest_path,
                operation_registry_root,
            )?,
    )
}

fn publish_researcher_exa_manifest(
    repo: &std::path::Path,
    root: &std::path::Path,
    agent_registry_root: &std::path::Path,
    operation_registry_root: &std::path::Path,
    standard_ops: &StandardOpHashes,
) -> Result<verlet::agent::manifest::PublishedAgentRecord, Box<dyn std::error::Error>> {
    let manifest_path = root.join("researcher-search.verlet.agent.toml");
    let template = std::fs::read_to_string(
        repo.join("examples/agents/researcher/researcher.verlet.agent.toml.in"),
    )?;
    let rendered = render_researcher_template(&template, standard_ops)?
        .replace("name = \"researcher\"", "name = \"researcher-search\"")
        + r#"

[[tools]]
type = "protocol_tool_import"
id = "search"
protocol = "mcp"
server_ref = "mcp://search"
include_tools = ["search"]
"#;
    std::fs::write(&manifest_path, rendered)?;
    Ok(
        verlet::agent::manifest::LocalAgentRegistry::new(agent_registry_root)
            .publish_manifest_path_with_operation_registry(
                &manifest_path,
                operation_registry_root,
            )?,
    )
}

fn render_researcher_template(
    template: &str,
    standard_ops: &StandardOpHashes,
) -> Result<String, Box<dyn std::error::Error>> {
    for placeholder in [
        "{HTTP_FETCH_SHA256}",
        "{FILE_READ_SHA256}",
        "{JSON_QUERY_SHA256}",
    ] {
        if !template.contains(placeholder) {
            return Err(format!("researcher template missing placeholder {placeholder}").into());
        }
    }
    let rendered = template
        .replace("{HTTP_FETCH_SHA256}", &standard_ops.http_fetch)
        .replace("{FILE_READ_SHA256}", &standard_ops.file_read)
        .replace("{JSON_QUERY_SHA256}", &standard_ops.json_query)
        .replace(
            "provider_ref = \"provider://local\"",
            "provider_ref = \"provider://openai_compatible\"",
        )
        .replace(
            "model_ref = \"model://local/default\"",
            &format!(
                "model_ref = \"model://openai_compatible/{}\"",
                verlet_metadata::provider_store::OPENAI_COMPATIBLE_DEFAULT_MODEL
            ),
        );
    if let Some(unresolved) = [
        "{HTTP_FETCH_SHA256}",
        "{FILE_READ_SHA256}",
        "{JSON_QUERY_SHA256}",
    ]
    .into_iter()
    .find(|placeholder| rendered.contains(placeholder))
    {
        return Err(format!("researcher template left unresolved placeholder {unresolved}").into());
    }
    Ok(rendered)
}

async fn run_turn(
    app: &verlet::adapters::app_server::VerletAppServer,
    thread_id: &str,
    input: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    Ok(run_turn_trace(app, thread_id, input).await?.output)
}

async fn run_turn_trace(
    app: &verlet::adapters::app_server::VerletAppServer,
    thread_id: &str,
    input: &str,
) -> Result<TurnTrace, Box<dyn std::error::Error>> {
    run_turn_trace_with_timeout(app, thread_id, input, std::time::Duration::from_secs(30)).await
}

async fn run_live_turn_trace(
    app: &verlet::adapters::app_server::VerletAppServer,
    thread_id: &str,
    input: &str,
) -> Result<TurnTrace, Box<dyn std::error::Error>> {
    run_turn_trace_with_timeout(app, thread_id, input, std::time::Duration::from_secs(180)).await
}

async fn run_turn_trace_with_timeout(
    app: &verlet::adapters::app_server::VerletAppServer,
    thread_id: &str,
    input: &str,
    timeout: std::time::Duration,
) -> Result<TurnTrace, Box<dyn std::error::Error>> {
    let parsed = verlet_runtime_contracts::ThreadId::parse_str(thread_id)?;
    let handle = app.supervisor().get_thread(app.tenant_id(), parsed).await?;
    let mut events = handle.subscribe_events();
    app.local_json_rpc_request(
        "turn/start",
        serde_json::json!({
            "threadId": thread_id,
            "input": [{ "type": "text", "text": input, "text_elements": [] }],
        }),
    )
    .await?;
    let mut output = String::new();
    let mut runtime_events = Vec::new();
    loop {
        let event = tokio::time::timeout(timeout, events.recv())
            .await
            .map_err(|_| {
                format!(
                    "timed out after {}ms waiting for thread event in manifest e2e turn thread={} input={:?} output_so_far={} runtime_events_seen={}",
                    timeout.as_millis(),
                    thread_id,
                    input,
                    compact(&output),
                    runtime_events.len()
                )
            })??;
        match event {
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Output { text, .. } => {
                output.push_str(&text);
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Runtime { event, .. } => {
                match event.kind {
                    verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                        state: verlet_runtime_contracts::RuntimeTerminalState::Completed,
                    } => {
                        return Ok(TurnTrace {
                            output,
                            runtime_events,
                        });
                    }
                    verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Terminal {
                        state,
                    } => {
                        return Err(format!("turn ended before completion: {state:?}").into());
                    }
                    verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::Failed {
                        message,
                        ..
                    } => return Err(message.into()),
                    other => runtime_events.push(other),
                }
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Failed { message, .. } => {
                return Err(message.into());
            }
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Cancelled {
                reason, ..
            } => return Err(reason.into()),
            verlet::kernel::runtime_host::runtime_api::ThreadEvent::Stopped { .. } => {
                return Err("thread stopped before turn completion".into());
            }
            _ => {}
        }
    }
}

struct TurnTrace {
    output: String,
    runtime_events: Vec<verlet::kernel::runtime_host::runtime_events::RuntimeEventKind>,
}

async fn inspect_manifest_events(
    root: &std::path::Path,
    thread_id: &str,
    manifest_hash: &str,
    expected_bind_count: usize,
    expect_alias: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let lifecycle = lifecycle_record(root, thread_id).await?;
    let session_store =
        verlet_history_sqlite::SqliteSessionStore::open(root.join("state/session_history.sqlite3"))
            .await?;
    let stream_id = verlet_history::EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await?;
    let compile_events = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestCompileCompleted)
        .collect::<Vec<_>>();
    let bind_events = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .collect::<Vec<_>>();
    if compile_events.len() != expected_bind_count || bind_events.len() != expected_bind_count {
        return Err(format!(
            "expected {expected_bind_count} manifest compile/bind event(s), found {}/{}",
            compile_events.len(),
            bind_events.len()
        )
        .into());
    }
    for event in compile_events.iter().chain(bind_events.iter()) {
        if event.origin != verlet_history::EventOrigin::Discharged {
            return Err(format!("manifest event {} was not discharged", event.id).into());
        }
        if event.provenance.source_streams.is_empty() {
            return Err(format!("manifest event {} had empty provenance", event.id).into());
        }
        if event.payload["manifest_hash"].as_str() != Some(manifest_hash) {
            return Err(format!("manifest event {} used the wrong manifest hash", event.id).into());
        }
    }
    if expect_alias
        && compile_events
            .first()
            .and_then(|event| event.payload.get("alias"))
            .and_then(|alias| alias.get("alias"))
            .and_then(serde_json::Value::as_str)
            != Some("latest")
    {
        return Err("first compile receipt did not include the @latest alias receipt".into());
    }
    Ok(())
}

async fn inspect_researcher_bind_receipt(
    root: &std::path::Path,
    thread_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let lifecycle = lifecycle_record(root, thread_id).await?;
    let session_store =
        verlet_history_sqlite::SqliteSessionStore::open(root.join("state/session_history.sqlite3"))
            .await?;
    let stream_id = verlet_history::EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await?;
    let bind = events
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .ok_or("researcher bind receipt was not recorded")?;
    let tool_ids = bind.payload["tool_ids"]
        .as_array()
        .ok_or("researcher bind receipt tool_ids was not an array")?
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    for expected in ["http_fetch", "file_read", "json_query"] {
        if !tool_ids.contains(expected) {
            return Err(format!(
                "researcher bind receipt missing tool id {expected:?}: {tool_ids:?}"
            )
            .into());
        }
    }
    let operation_bindings = bind.payload["operation_bindings"]
        .as_array()
        .ok_or("researcher bind receipt operation_bindings was not an array")?;
    let operation_names = operation_bindings
        .iter()
        .filter_map(|binding| binding["name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    for expected in ["http-fetch", "file-read", "json-query"] {
        if !operation_names.contains(expected) {
            return Err(format!(
                "researcher bind receipt missing operation binding {expected:?}: {operation_names:?}"
            )
            .into());
        }
    }
    Ok(())
}

fn assert_researcher_tool_trace(
    events: &[verlet::kernel::runtime_host::runtime_events::RuntimeEventKind],
) -> Result<(), Box<dyn std::error::Error>> {
    let started_bash = events.iter().any(|event| {
        matches!(
            event,
            verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallStarted { call_id, name, .. }
                if call_id == "call_researcher_bash" && name == "bash"
        )
    });
    if !started_bash {
        return Err("researcher turn did not start the expected bash tool call".into());
    }
    if !events.iter().any(|event| {
        matches!(
            event,
            verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                call_id,
                output,
                success,
                ..
            } if call_id == "call_researcher_bash" && *success
                && output.contains("\"exit_code\":0")
                && output.contains("Linus")
                && output.contains("bash to file_read workspace")
        )
    }) {
        let tool_results = events
            .iter()
            .filter_map(|event| {
                match event {
                verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                    output, success, ..
                } => Some(format!("success={success} output={}", compact(output))),
                _ => None,
            }
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(format!(
            "researcher turn did not record successful json_query and file_read bash results: {tool_results}"
        )
        .into());
    }
    Ok(())
}

async fn inspect_history(
    root: &std::path::Path,
    thread_id: &str,
) -> Result<SmokeInspection, Box<dyn std::error::Error>> {
    let lifecycle = lifecycle_record(root, thread_id).await?;
    let session_store =
        verlet_history_sqlite::SqliteSessionStore::open(root.join("state/session_history.sqlite3"))
            .await?;
    let session_context = session_store.build_context(&lifecycle.coordinates).await?;
    let transcript = session_context
        .messages
        .iter()
        .map(|message| format!("{message:?}"))
        .collect::<Vec<_>>()
        .join("\n");
    if session_context.messages.len() != 4 {
        return Err(format!(
            "expected 4 canonical messages after resume, found {}: {}",
            session_context.messages.len(),
            compact(&transcript)
        )
        .into());
    }
    for expected in [
        "first manifest turn",
        "local:first manifest turn",
        "second manifest turn",
        "local:second manifest turn",
    ] {
        if !transcript.contains(expected) {
            return Err(format!(
                "canonical history missing {expected:?}: {}",
                compact(&transcript)
            )
            .into());
        }
    }
    let stream_id = verlet_history::EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await?;
    let bind_count = events
        .iter()
        .filter(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .count();
    Ok(SmokeInspection {
        message_count: session_context.messages.len(),
        bind_count,
    })
}

async fn inspect_debug_export(
    app: &verlet::adapters::app_server::VerletAppServer,
    thread_id: &str,
) -> Result<DebugExportInspection, Box<dyn std::error::Error>> {
    let export = app
        .local_json_rpc_request(
            "thread/debug/export",
            serde_json::json!({
                "threadId": thread_id,
                "streams": ["thread", "control"],
                "includeThread": true,
            }),
        )
        .await?;
    if export["schema"].as_str() != Some("cooldis.debug.thread_export/1") {
        return Err(format!(
            "debug export used the wrong schema: {}",
            compact(&export.to_string())
        )
        .into());
    }
    if export["threadId"].as_str() != Some(thread_id) {
        return Err("debug export returned the wrong thread id".into());
    }
    if export["backend"]["kind"].as_str() != Some("sqlite") {
        return Err("debug export did not identify the sqlite backend".into());
    }
    assert_ack_classes(&export["ackClasses"], "debug export")?;
    if export["redaction"]["mode"].as_str() != Some("secret-shaped-json-keys") {
        return Err("debug export did not use the default redaction mode".into());
    }

    let streams = export["streams"]
        .as_array()
        .ok_or("debug export streams was not an array")?;
    let thread_stream = streams
        .iter()
        .find(|stream| stream["selector"].as_str() == Some("thread"))
        .ok_or("debug export did not include the thread stream")?;
    assert_ack_classes(&thread_stream["ackClasses"], "thread stream export")?;
    if thread_stream["streamId"].as_str() != Some(format!("thread:{thread_id}").as_str()) {
        return Err("debug export thread stream id did not match the thread".into());
    }
    if thread_stream["truncated"].as_bool() != Some(false) {
        return Err("debug export unexpectedly truncated the thread stream".into());
    }
    if thread_stream["range"]["tailCursor"].as_str().is_none() {
        return Err("debug export thread stream did not include a tail cursor".into());
    }
    let events = thread_stream["data"]
        .as_array()
        .ok_or("debug export thread data was not an array")?;
    if events.is_empty() {
        return Err("debug export thread stream was empty".into());
    }
    for event in events {
        assert_stream_record_v1(event, thread_id)?;
    }
    assert_debug_export_tail_stream_cursor(thread_stream, events)?;
    assert_exported_event(events, "manifest.compile.completed")?;
    assert_exported_event(events, "manifest.bind.completed")?;

    let control_stream = streams
        .iter()
        .find(|stream| stream["selector"].as_str() == Some("control"))
        .ok_or("debug export did not include the control stream")?;
    assert_ack_classes(&control_stream["ackClasses"], "control stream export")?;
    if control_stream["backend"]["kind"].as_str() != Some("sqlite") {
        return Err("debug export control stream did not identify sqlite backend".into());
    }

    let receipts = export["receipts"]
        .as_array()
        .ok_or("debug export receipts was not an array")?;
    assert_receipt_count(
        receipts,
        "manifest.compile.completed",
        "cooldis.event.manifest.compile.completed/1",
        2,
    )?;
    assert_receipt_count(
        receipts,
        "manifest.bind.completed",
        "cooldis.event.manifest.bind.completed/1",
        2,
    )?;

    Ok(DebugExportInspection {
        event_count: events.len(),
        receipt_count: receipts.len(),
    })
}

fn assert_ack_classes(
    value: &serde_json::Value,
    label: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let values = value
        .as_array()
        .ok_or_else(|| format!("{label} ackClasses was not an array"))?;
    let classes = values
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    for expected in ["local_committed", "query_projected"] {
        if !classes.contains(expected) {
            return Err(format!("{label} ackClasses missing {expected:?}: {classes:?}").into());
        }
    }
    Ok(())
}

fn assert_stream_record_v1(
    event: &serde_json::Value,
    thread_id: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if event["schema"].as_str() != Some(verlet_history::STREAM_RECORD_SCHEMA_V1) {
        return Err(format!(
            "exported event used the wrong stream schema: {}",
            compact(&event.to_string())
        )
        .into());
    }
    if event["stream_id"].as_str() != Some(format!("thread:{thread_id}").as_str()) {
        return Err("exported event stream_id did not match the thread".into());
    }
    if event["eventId"].as_str().is_none() {
        return Err("exported event did not include debug eventId evidence".into());
    }
    if event["sequence"].as_i64().is_none() {
        return Err("exported event did not include a sequence".into());
    }
    let payload_schema = event["payload_schema"]
        .as_str()
        .ok_or("exported event did not include payload_schema")?;
    if !payload_schema.starts_with("cooldis.event.") {
        return Err(format!("exported event payload_schema looked wrong: {payload_schema}").into());
    }
    Ok(())
}

fn assert_debug_export_tail_stream_cursor(
    stream: &serde_json::Value,
    events: &[serde_json::Value],
) -> Result<(), Box<dyn std::error::Error>> {
    if stream["streamCursor"] != serde_json::Value::Null {
        return Err("untruncated debug export unexpectedly returned streamCursor".into());
    }
    let last_event = events
        .last()
        .ok_or("debug export stream had no event for tail cursor assertion")?;
    for field in ["lastExportedStreamCursor", "tailStreamCursor"] {
        let cursor = &stream["range"][field];
        if cursor["schema"].as_str() != Some(verlet_history::STREAM_CURSOR_SCHEMA_V1) {
            return Err(format!("debug export range.{field} used the wrong schema").into());
        }
        if cursor["stream_id"].as_str() != stream["streamId"].as_str() {
            return Err(format!("debug export range.{field} stream_id mismatch").into());
        }
        if cursor["sequence"].as_i64() != last_event["sequence"].as_i64() {
            return Err(format!("debug export range.{field} sequence mismatch").into());
        }
        if cursor["event_id"].as_str() != last_event["event_id"].as_str() {
            return Err(format!("debug export range.{field} event_id mismatch").into());
        }
    }
    Ok(())
}

fn assert_exported_event(
    events: &[serde_json::Value],
    kind: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    if events
        .iter()
        .any(|event| event["kind"].as_str() == Some(kind))
    {
        Ok(())
    } else {
        Err(format!("debug export did not include exported event kind {kind:?}").into())
    }
}

fn assert_receipt_count(
    receipts: &[serde_json::Value],
    kind: &str,
    payload_schema: &str,
    minimum: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let count = receipts
        .iter()
        .filter(|receipt| {
            receipt["kind"].as_str() == Some(kind)
                && receipt["origin"].as_str() == Some("discharged")
                && receipt["payloadSchema"].as_str() == Some(payload_schema)
        })
        .count();
    if count < minimum {
        return Err(format!(
            "debug export had {count} receipt(s) for {kind:?}/{payload_schema:?}; expected at least {minimum}"
        )
        .into());
    }
    Ok(())
}

async fn lifecycle_record(
    root: &std::path::Path,
    thread_id: &str,
) -> Result<verlet_runtime_contracts::ThreadLifecycleRecord, Box<dyn std::error::Error>> {
    let parsed = verlet_runtime_contracts::ThreadId::parse_str(thread_id)?;
    let metadata_store = verlet_metadata::provider_store::SqliteMetadataStore::open(
        root.join("state/metadata.sqlite3"),
    )
    .await?;
    Ok(metadata_store
        .get_thread_lifecycle(parsed)
        .await?
        .ok_or("missing persisted thread lifecycle record")?)
}

struct SmokeInspection {
    message_count: usize,
    bind_count: usize,
}

struct DebugExportInspection {
    event_count: usize,
    receipt_count: usize,
}

fn default_root() -> std::path::PathBuf {
    default_named_root("manifest-e2e")
}

fn default_named_root(name: &str) -> std::path::PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join("scratch/live")
        .join(format!("{name}-{}", uuid::Uuid::now_v7()))
}

fn repo_root() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("verlet-kernel manifest dir should be under crates/")
        .to_path_buf()
}

fn chat_tool_call_sse(command: &str) -> String {
    let arguments = serde_json::json!({ "command": command }).to_string();
    let event = serde_json::json!({
        "choices": [{
            "delta": {
                "tool_calls": [{
                    "index": 0,
                    "id": "call_researcher_bash",
                    "function": {
                        "name": "bash",
                        "arguments": arguments
                    }
                }]
            },
            "finish_reason": "tool_calls"
        }],
        "usage": {"prompt_tokens": 5, "completion_tokens": 6}
    });
    format!("data: {event}\n\ndata: [DONE]\n\n")
}

fn chat_text_sse(text: &str) -> String {
    let event = serde_json::json!({
        "choices": [{
            "delta": {"content": text},
            "finish_reason": "stop"
        }],
        "usage": {"prompt_tokens": 7, "completion_tokens": 3}
    });
    format!("data: {event}\n\ndata: [DONE]\n\n")
}

struct MockChatServer {
    addr: std::net::SocketAddr,
    requests: std::sync::Arc<std::sync::Mutex<Vec<CapturedRequest>>>,
    handle: Option<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    path: String,
    body: serde_json::Value,
}

impl MockChatServer {
    async fn start_text(responses: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let requests = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_requests = std::sync::Arc::clone(&requests);
        let handle = tokio::spawn(async move {
            let mut responses = responses
                .into_iter()
                .collect::<std::collections::VecDeque<_>>();
            while let Some(response) = responses.pop_front() {
                let (socket, _) = listener.accept().await?;
                let request = handle_chat_connection(socket, &response).await?;
                captured_requests.lock().unwrap().push(request);
            }
            Ok(())
        });
        Ok(Self {
            addr,
            requests,
            handle: Some(handle),
        })
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn requests(
        &self,
        expected: usize,
    ) -> Result<Vec<CapturedRequest>, Box<dyn std::error::Error>> {
        tokio::time::timeout(FIXTURE_IO_TIMEOUT, async {
            loop {
                let requests = self.requests.lock().unwrap().clone();
                if requests.len() >= expected {
                    return requests;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "timed out waiting for captured provider requests".into())
    }

    async fn join(mut self) -> Result<(), Box<dyn std::error::Error>> {
        let handle = self
            .handle
            .take()
            .ok_or("mock chat server was already joined")?;
        match handle.await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err.to_string().into()),
            Err(err) => Err(err.into()),
        }
    }
}

impl Drop for MockChatServer {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn handle_chat_connection(
    mut socket: tokio::net::TcpStream,
    response_body: &str,
) -> Result<CapturedRequest, Box<dyn std::error::Error + Send + Sync>> {
    let request = read_http_json_request(&mut socket, None).await?;
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        response_body.len(),
        response_body
    );
    write_fixture_all(&mut socket, response.as_bytes()).await?;
    shutdown_fixture_stream(&mut socket).await?;

    Ok(request)
}

async fn read_http_json_request(
    stream: &mut tokio::net::TcpStream,
    expected_authorization: Option<&str>,
) -> Result<CapturedRequest, Box<dyn std::error::Error + Send + Sync>> {
    let mut buffer = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let read = read_fixture_chunk(stream, &mut chunk).await?;
        if read == 0 {
            return Err("connection closed before HTTP headers".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > MAX_FIXTURE_HEADER_BYTES {
            return Err("HTTP fixture request headers exceeded cap".into());
        }
        if header_end(&buffer).is_some() {
            break;
        }
    }

    let header_end = header_end(&buffer).ok_or("missing HTTP header terminator")?;
    let headers_text = String::from_utf8(buffer[..header_end].to_vec())?;
    let mut lines = headers_text.split("\r\n");
    let request_line = lines.next().ok_or("missing request line")?;
    let path = request_line
        .split_whitespace()
        .nth(1)
        .ok_or("missing request path")?
        .to_string();
    let headers = lines
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect::<Vec<_>>();
    if let Some(expected) = expected_authorization {
        let authorized = headers
            .iter()
            .any(|(name, value)| name.eq_ignore_ascii_case("authorization") && value == expected);
        if !authorized {
            return Err("HTTP fixture did not receive expected authorization header".into());
        }
    }
    let content_length = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, value)| value.parse::<usize>().ok())
        .unwrap_or(0);
    if content_length > MAX_FIXTURE_BODY_BYTES {
        return Err("HTTP fixture request body exceeded cap".into());
    }
    let body_start = header_end + 4;
    while buffer.len() - body_start < content_length {
        let read = read_fixture_chunk(stream, &mut chunk).await?;
        if read == 0 {
            return Err("connection closed before HTTP body".into());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&buffer[body_start..body_start + content_length])?;

    Ok(CapturedRequest { path, body })
}

async fn read_fixture_chunk(
    stream: &mut tokio::net::TcpStream,
    chunk: &mut [u8],
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(FIXTURE_IO_TIMEOUT, stream.read(chunk))
        .await
        .map_err(|_| "HTTP fixture read timed out")?
        .map_err(|err| err.into())
}

async fn write_fixture_all(
    stream: &mut tokio::net::TcpStream,
    bytes: &[u8],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(FIXTURE_IO_TIMEOUT, stream.write_all(bytes))
        .await
        .map_err(|_| "HTTP fixture write timed out")?
        .map_err(|err| err.into())
}

async fn shutdown_fixture_stream(
    stream: &mut tokio::net::TcpStream,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(FIXTURE_IO_TIMEOUT, stream.shutdown())
        .await
        .map_err(|_| "HTTP fixture shutdown timed out")?
        .map_err(|err| err.into())
}

fn header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

async fn run_researcher_live() -> Result<(), Box<dyn std::error::Error>> {
    let config = LiveResearcherConfig::load()?;
    let root = default_named_root("researcher-manifest-live");
    let workspace = root.join("workspace");
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    tokio::fs::create_dir_all(&workspace).await?;
    tokio::fs::create_dir_all(&operation_registry_root).await?;
    let repo = repo_root();
    let standard_ops = publish_standard_ops(&repo, &operation_registry_root).await?;
    let record = publish_researcher_manifest(
        &repo,
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &standard_ops,
    )?;

    let listen =
        verlet::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse()?);
    let mut config_app =
        verlet::adapters::app_server::VerletAppServerConfig::local(listen, &workspace)
            .with_openai_chat_completions(
                "openai_compatible",
                &config.base_url,
                &config.api_key,
                verlet_metadata::provider_store::OPENAI_COMPATIBLE_DEFAULT_MODEL,
            )
            // lexicon-allow: capsule - existing app-server operation binding API name
            .with_capsule_bindings(
                // lexicon-allow: capsule - existing app-server operation binding API name
                verlet::adapters::app_server::CapsuleBindingsConfig::default()
                    .with_registry_root(&operation_registry_root),
            );
    config_app.runtime_home = root.join("runtime");
    config_app.state_home = root.join("state");
    config_app.agent_registry_root = agent_registry_root.clone();
    let app = verlet::adapters::app_server::VerletAppServer::new_local(config_app).await?;
    let thread_start = app
        .local_json_rpc_request(
            "thread/start",
            serde_json::json!({
                "agentRef": "agent://researcher@latest",
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or("live researcher thread/start response missing thread id")?
        .to_string();
    inspect_manifest_events(&root, &thread_id, &record.manifest_hash, 1, true).await?;
    let live_trace = run_live_turn_trace(
        &app,
        &thread_id,
        concat!(
            "You must call the bash tool exactly once before answering. ",
            "Run this command exactly:\nprintf '%s' '{\"url\":\"https://example.com\",\"maxResponseBytes\":4096}' | http_fetch\n",
            "After the tool result is visible and successful, reply with exactly RESEARCHER_HTTP_FETCH_OK."
        ),
    )
    .await?;
    if !live_trace.output.contains("RESEARCHER_HTTP_FETCH_OK") {
        return Err(format!(
            "live researcher turn missed marker: {}",
            compact(&live_trace.output)
        )
        .into());
    }
    if !live_trace.runtime_events.iter().any(|event| {
        matches!(
            event,
            verlet::kernel::runtime_host::runtime_events::RuntimeEventKind::ToolCallResult {
                output, success, ..
            } if *success && output.contains("\"status\":200")
        )
    }) {
        return Err("live researcher turn did not record a successful http_fetch result".into());
    }

    run_researcher_exa_bind_variant(
        &config,
        &repo,
        &root,
        &operation_registry_root,
        &standard_ops,
    )
    .await?;
    println!(
        "verlet researcher manifest live smoke ok model={} root={} thread={}",
        config.model,
        root.display(),
        thread_id
    );
    Ok(())
}

async fn run_researcher_exa_bind_variant(
    config: &LiveResearcherConfig,
    repo: &std::path::Path,
    root: &std::path::Path,
    operation_registry_root: &std::path::Path,
    standard_ops: &StandardOpHashes,
) -> Result<(), Box<dyn std::error::Error>> {
    let variant_root = root.join("search-variant");
    let workspace = variant_root.join("workspace");
    let agent_registry_root = variant_root.join("agents");
    tokio::fs::create_dir_all(&workspace).await?;
    let record = publish_researcher_exa_manifest(
        repo,
        &variant_root,
        &agent_registry_root,
        operation_registry_root,
        standard_ops,
    )?;
    let mcp_fixture = SearchMcpFixture::start().await?;
    let listen =
        verlet::adapters::app_server::AppServerListenAddr::WebSocket("127.0.0.1:0".parse()?);
    let mut config_app =
        verlet::adapters::app_server::VerletAppServerConfig::local(listen, &workspace)
            .with_openai_chat_completions(
                "openai_compatible",
                &config.base_url,
                &config.api_key,
                verlet_metadata::provider_store::OPENAI_COMPATIBLE_DEFAULT_MODEL,
            )
            // lexicon-allow: capsule - existing app-server operation binding API name
            .with_capsule_bindings(
                // lexicon-allow: capsule - existing app-server operation binding API name
                verlet::adapters::app_server::CapsuleBindingsConfig::default()
                    .with_registry_root(operation_registry_root),
            );
    config_app.runtime_home = variant_root.join("runtime");
    config_app.state_home = variant_root.join("state");
    config_app.agent_registry_root = agent_registry_root;
    let metadata_path = config_app.state_home.join("metadata.sqlite3");
    let app = verlet::adapters::app_server::VerletAppServer::new_local(config_app).await?;
    verlet_metadata::secret_store::SqliteSecretStore::open(&metadata_path)
        .await?
        .set_secret(
            "EXAMPLE_API_KEY",
            "fixture-token",
            verlet_metadata::secret_store::SecretSourceKind::Local,
            Some("researcher-manifest-live".to_string()),
        )
        .await?;
    verlet::adapters::mcp_client::SqliteMcpSourceRegistry::open_async(&metadata_path)
        .await?
        .upsert_source_async(
            verlet::adapters::mcp_client::McpRemoteServerConfig::new(
                "search",
                verlet::adapters::mcp_client::McpRemoteTransport::StreamableHttp,
                mcp_fixture.url().to_string(),
            )?
            .with_bearer_secret("EXAMPLE_API_KEY")?,
        )
        .await?;
    let thread_start = app
        .local_json_rpc_request(
            "thread/start",
            serde_json::json!({
                "agentRef": "agent://researcher-search@latest",
            }),
        )
        .await?;
    let thread_id = thread_start["thread"]["id"]
        .as_str()
        .ok_or("researcher-search thread/start response missing thread id")?
        .to_string();
    inspect_manifest_events(&variant_root, &thread_id, &record.manifest_hash, 1, true).await?;
    let lifecycle = lifecycle_record(&variant_root, &thread_id).await?;
    let session_store = verlet_history_sqlite::SqliteSessionStore::open(
        variant_root.join("state/session_history.sqlite3"),
    )
    .await?;
    let stream_id = verlet_history::EventStreamId::for_thread(&lifecycle.coordinates);
    let events = session_store.read_events(&stream_id, None).await?;
    let bind = events
        .iter()
        .find(|event| event.kind == verlet_history::EventKind::ManifestBindCompleted)
        .ok_or("researcher-search bind receipt was not recorded")?;
    if bind.payload["tool_universes"][0]["server_ref"].as_str() != Some("mcp://search") {
        return Err(format!(
            "researcher-search bind receipt did not include mcp://search: {}",
            compact(&bind.payload.to_string())
        )
        .into());
    }
    mcp_fixture.wait_for_method("tools/list").await?;
    mcp_fixture.stop().await?;
    Ok(())
}

struct SearchMcpFixture {
    url: String,
    methods: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    handle: Option<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
}

impl SearchMcpFixture {
    async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let url = format!("http://{addr}/mcp");
        let methods = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured_methods = std::sync::Arc::clone(&methods);
        let handle = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await?;
                let request = read_mcp_http_json(&mut stream).await?;
                if let Some(method) = request.get("method").and_then(serde_json::Value::as_str) {
                    captured_methods.lock().unwrap().push(method.to_string());
                }
                let response = search_mcp_response(&request);
                write_mcp_http_json(&mut stream, &response).await?;
            }
        });
        Ok(Self {
            url,
            methods,
            handle: Some(handle),
        })
    }

    fn url(&self) -> &str {
        &self.url
    }

    async fn wait_for_method(&self, expected: &str) -> Result<(), Box<dyn std::error::Error>> {
        tokio::time::timeout(FIXTURE_IO_TIMEOUT, async {
            loop {
                let methods = self.methods.lock().unwrap().clone();
                if methods.iter().any(|method| method == expected) {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| format!("timed out waiting for MCP method {expected:?}"))?
    }

    async fn stop(mut self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(handle) = self.handle.take() {
            handle.abort();
            match handle.await {
                Err(err) if err.is_cancelled() => Ok(()),
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => Err(err.to_string().into()),
                Err(err) => Err(err.into()),
            }
        } else {
            Ok(())
        }
    }
}

impl Drop for SearchMcpFixture {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn read_mcp_http_json(
    stream: &mut tokio::net::TcpStream,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    Ok(read_http_json_request(stream, Some("Bearer fixture-token"))
        .await?
        .body)
}

async fn write_mcp_http_json(
    stream: &mut tokio::net::TcpStream,
    value: &serde_json::Value,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let body = value.to_string();
    let raw = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    write_fixture_all(stream, raw.as_bytes()).await?;
    Ok(())
}

fn search_mcp_response(request: &serde_json::Value) -> serde_json::Value {
    let id = request.get("id").cloned();
    match request.get("method").and_then(serde_json::Value::as_str) {
        Some("initialize") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "researcher-search-fixture", "version": "1"}
            }
        }),
        Some("notifications/initialized") => serde_json::json!({"jsonrpc": "2.0", "result": {}}),
        Some("tools/list") => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": {
                "tools": [{
                    "name": "search",
                    "description": "Search through an Example Search-style MCP server.",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": false,
                        "properties": {"query": {"type": "string"}},
                        "required": ["query"]
                    }
                }]
            }
        }),
        _ => serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {"code": -32601, "message": "unknown method"}
        }),
    }
}

struct LiveResearcherConfig {
    base_url: String,
    api_key: String,
    model: String,
}

impl LiveResearcherConfig {
    fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let base_url = std::env::var("VERLET_OPENAI_COMPATIBLE_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "https://api.example.invalid/v1".to_string());
        let api_key = std::env::var("VERLET_OPENAI_COMPATIBLE_API_KEY")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("OPENAI_COMPATIBLE_API_KEY")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            })
            .ok_or("missing VERLET_OPENAI_COMPATIBLE_API_KEY or OPENAI_COMPATIBLE_API_KEY")?;
        let model = std::env::var("VERLET_OPENAI_COMPATIBLE_MODEL")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| {
                verlet_metadata::provider_store::OPENAI_COMPATIBLE_DEFAULT_MODEL.to_string()
            });
        Ok(Self {
            base_url,
            api_key,
            model,
        })
    }
}

fn compact(value: &str) -> String {
    const MAX: usize = 240;
    let value = value.replace('\n', "\\n");
    if value.len() <= MAX {
        value
    } else {
        format!("{}...", &value[..MAX])
    }
}
