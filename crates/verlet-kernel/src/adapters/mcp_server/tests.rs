use tokio::io::AsyncBufReadExt as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use verlet_history::EventStore as _;

const SEARCH_FIXTURE_TEMPLATE: &str =
    include_str!("../../../tests/fixtures/search_operation.wat.tpl");
static MCP_CAPSULE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
async fn mcp_server_lists_tools_without_daemon_connection() {
    let (client, server) = tokio::io::duplex(64 * 1024);
    let config = crate::adapters::mcp_server::VerletMcpServerConfig {
        daemon_socket: std::path::PathBuf::from("/tmp/missing-verlet-daemon.sock"),
        request_timeout: std::time::Duration::from_secs(1),
    };
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        crate::adapters::mcp_server::serve_mcp_stdio(server_read, server_write, config).await
    });

    let (read, mut write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(read).lines();
    write
            .write_all(
                br#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
            )
            .await
            .unwrap();
    write.write_all(b"\n").await.unwrap();
    let init = read_json_response(&mut lines, 1).await;
    assert_eq!(init["result"]["serverInfo"]["name"], "verlet-mcp-server");

    write
        .write_all(br#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#)
        .await
        .unwrap();
    write.write_all(b"\n").await.unwrap();
    write
        .write_all(br#"{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}"#)
        .await
        .unwrap();
    write.write_all(b"\n").await.unwrap();
    let tools = read_json_response(&mut lines, 2).await;
    assert!(
        tools["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .any(|tool| tool["name"] == "verlet_prompt")
    );

    drop(write);
    server_task.abort();
    let _ = server_task.await;
}

#[tokio::test]
async fn mcp_server_runs_prompt_and_command_through_daemon() {
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("cdis-mcp-{}", uuid::Uuid::now_v7().simple()));
    let socket = root.join("app.sock");
    let listen = crate::adapters::app_server::AppServerListenAddr::Unix(socket.clone());
    let config = isolated_app_config(listen.clone(), &root);
    let app = crate::adapters::app_server::VerletAppServer::new_local(config)
        .await
        .unwrap();
    let session_store_path = app.session_store_path().to_path_buf();
    let serve_task = tokio::spawn(async move { app.serve(listen).await });
    wait_for_socket(&socket).await;

    let (client, server) = tokio::io::duplex(256 * 1024);
    let config = crate::adapters::mcp_server::VerletMcpServerConfig {
        daemon_socket: socket.clone(),
        request_timeout: std::time::Duration::from_secs(10),
    };
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        crate::adapters::mcp_server::serve_mcp_stdio(server_read, server_write, config).await
    });
    let (read, mut write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(read).lines();

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
    let _ = read_json_response(&mut lines, 1).await;
    send(
        &mut write,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .await;
    let legacy_prompt_request = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": concat!("cool", "dis_prompt"),
            "arguments": { "message": "hello mcp" },
        },
    })
    .to_string();
    send(&mut write, &legacy_prompt_request).await;
    let legacy_prompt = read_json_response(&mut lines, 2).await;
    assert!(
        legacy_prompt["error"]["message"]
            .as_str()
            .unwrap()
            .contains("unknown Verlet MCP tool")
    );

    send(
        &mut write,
        r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"verlet_prompt","arguments":{"message":"hello mcp"}}}"#,
    )
    .await;
    let prompt = read_json_response(&mut lines, 3).await;
    assert_eq!(
        prompt["result"]["structuredContent"]["assistantText"],
        "local:hello mcp"
    );
    let prompt_thread_id = prompt["result"]["structuredContent"]["threadId"]
        .as_str()
        .unwrap();
    assert_admission_surface(
        &session_store_path,
        prompt_thread_id,
        crate::kernel::admission::MCP_ADAPTER_SURFACE,
    )
    .await;

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"verlet_thread_start","arguments":{}}}"#,
        )
        .await;
    let root_start = read_json_response(&mut lines, 3).await;
    let root_thread_id = root_start["result"]["structuredContent"]["threadId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        root_start["result"]["structuredContent"]["thread"]["topology"]["lineage"]["type"].as_str(),
        Some("root")
    );

    let child_start = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 4,
        "method": "tools/call",
        "params": {
            "name": "verlet_thread_start",
            "arguments": {
                "topology": {
                    "initiation": {
                        "type": "thread",
                        "thread_id": root_thread_id,
                    },
                    "lineage": {
                        "type": "root",
                    },
                    "spawn_attribution": {
                        "source_thread_id": root_thread_id,
                    },
                    "controller_thread_id": root_thread_id,
                },
            },
        },
    });
    send(&mut write, &child_start.to_string()).await;
    let child_start = read_json_response(&mut lines, 4).await;
    assert_eq!(
        child_start["result"]["isError"].as_bool(),
        Some(false),
        "child start response: {child_start}"
    );
    let child_thread = &child_start["result"]["structuredContent"]["thread"];
    assert_eq!(
        child_thread["parentThreadId"].as_str(),
        Some(root_thread_id.as_str())
    );
    assert_eq!(
        child_thread["topology"]["spawn_attribution"]["source_thread_id"].as_str(),
        Some(root_thread_id.as_str())
    );

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"verlet_command_exec","arguments":{"command":["/bin/sh","-lc","printf MCP_EXEC_OK"]}}}"#,
        )
        .await;
    let command = read_json_response(&mut lines, 5).await;
    assert_eq!(
        command["result"]["structuredContent"]["stdout"],
        "MCP_EXEC_OK"
    );

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"verlet_prompt","arguments":{"message":"this should not reach the provider","thread":{"model":"not-declared"}}}}"#,
        )
        .await;
    let bad_model = read_json_response(&mut lines, 6).await;
    assert_eq!(
        bad_model["result"]["isError"].as_bool(),
        Some(true),
        "{bad_model}"
    );
    assert!(
        bad_model["result"]["structuredContent"]["error"]
            .as_str()
            .unwrap()
            .contains("declared model profiles"),
        "{bad_model}"
    );

    drop(write);
    server_task.abort();
    let _ = server_task.await;
    serve_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mcp_prompt_lets_model_shaped_agent_see_and_call_search_shell_command() {
    let _guard = MCP_CAPSULE_TEST_LOCK.lock().await;
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("cdis-mcp-search-{}", uuid::Uuid::now_v7().simple()));
    let socket = root.join("app.sock");
    let registry_root = root.join("capsules");
    let listen = crate::adapters::app_server::AppServerListenAddr::Unix(socket.clone());
    publish_exa_without_secret(&registry_root).await;
    let provider = std::sync::Arc::new(ModelVbinLifecycleClient::default());
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = provider.clone();
    let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIChatCompletions,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL,
    );
    runtime_config.max_tokens = 512;
    let capsule_bindings = crate::adapters::app_server::CapsuleBindingsConfig::default()
        .with_registry_root(&registry_root)
        .with_global_operation_name("search");
    let mut app_config = isolated_app_config(listen.clone(), &root);
    app_config.model_provider =
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string();
    app_config.model = crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string();
    app_config.capsule_bindings = capsule_bindings.clone();
    let runtime_factory =
        crate::adapters::app_server::runtime_factory_from_provider_parts_with_app_paths(
            runtime_config,
            provider_client,
            capsule_bindings,
            None,
            &app_config,
        );
    let app = crate::adapters::app_server::VerletAppServer::with_runtime_factory(
        app_config,
        runtime_factory,
    )
    .await
    .unwrap();
    let serve_task = tokio::spawn(async move { app.serve(listen).await });
    wait_for_socket(&socket).await;

    let (client, server) = tokio::io::duplex(256 * 1024);
    let config = crate::adapters::mcp_server::VerletMcpServerConfig {
        daemon_socket: socket.clone(),
        request_timeout: std::time::Duration::from_secs(10),
    };
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        crate::adapters::mcp_server::serve_mcp_stdio(server_read, server_write, config).await
    });
    let (read, mut write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(read).lines();

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
    let _ = read_json_response(&mut lines, 1).await;
    send(
        &mut write,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .await;
    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"verlet_prompt","arguments":{"message":"Use bash to call search for the query Verlet. If it fails because a credential is missing, report the failure."}}}"#,
        )
        .await;
    let prompt = read_json_response(&mut lines, 2).await;
    assert_eq!(
        prompt["result"]["structuredContent"]["assistantText"],
        "MODEL_SEARCH_FAILURE_REPORTED"
    );

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let bash_tool = requests[0]
        .tools
        .iter()
        .find(|tool| tool.name == "bash")
        .expect("MODEL-shaped provider request should include bash tool");
    assert!(bash_tool.description.contains("search"));
    let tool_result_text = text_from_canonical_messages(&requests[1].messages);
    assert!(tool_result_text.contains("search"));
    assert!(tool_result_text.contains(r#""exit_code":1"#));

    drop(write);
    server_task.abort();
    let _ = server_task.await;
    serve_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mcp_capsule_binding_tools_update_global_scope() {
    let _guard = MCP_CAPSULE_TEST_LOCK.lock().await;
    let root = std::path::PathBuf::from("/tmp")
        .join(format!("cdis-mcp-bind-{}", uuid::Uuid::now_v7().simple()));
    let socket = root.join("app.sock");
    let registry_root = root.join("capsules");
    let listen = crate::adapters::app_server::AppServerListenAddr::Unix(socket.clone());
    publish_exa_without_secret(&registry_root).await;
    let provider = std::sync::Arc::new(ModelVbinLifecycleClient::default());
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = provider.clone();
    let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIChatCompletions,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL,
    );
    runtime_config.max_tokens = 512;
    let capsule_bindings = crate::adapters::app_server::CapsuleBindingsConfig::default()
        .with_registry_root(&registry_root);
    let runtime_factory = crate::adapters::app_server::runtime_factory_from_provider_parts(
        runtime_config,
        provider_client,
        capsule_bindings.clone(),
    );
    let mut app_config = isolated_app_config(listen.clone(), &root);
    app_config.model_provider =
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string();
    app_config.model = crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string();
    app_config.capsule_bindings = capsule_bindings;
    let app = crate::adapters::app_server::VerletAppServer::with_runtime_factory(
        app_config,
        runtime_factory,
    )
    .await
    .unwrap();
    let serve_task = tokio::spawn(async move { app.serve(listen).await });
    wait_for_socket(&socket).await;

    let (client, server) = tokio::io::duplex(256 * 1024);
    let config = crate::adapters::mcp_server::VerletMcpServerConfig {
        daemon_socket: socket.clone(),
        request_timeout: std::time::Duration::from_secs(10),
    };
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        crate::adapters::mcp_server::serve_mcp_stdio(server_read, server_write, config).await
    });
    let (read, mut write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(read).lines();

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
    let _ = read_json_response(&mut lines, 1).await;
    send(
        &mut write,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .await;
    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"verlet_capsule_binding_set","arguments":{"scope":{"kind":"global"},"operationName":"search"}}}"#,
        )
        .await;
    let set = read_json_response(&mut lines, 2).await;
    assert_eq!(set["result"]["isError"].as_bool(), Some(false), "{set}");
    assert_eq!(
        set["result"]["structuredContent"]["binding"]["operationName"].as_str(),
        Some("search")
    );

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"verlet_capsule_binding_list","arguments":{"scope":{"kind":"global"}}}}"#,
        )
        .await;
    let list = read_json_response(&mut lines, 3).await;
    assert_eq!(
        list["result"]["structuredContent"]["data"][0]["operationName"].as_str(),
        Some("search"),
        "{list}"
    );

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"verlet_capsule_binding_resolve","arguments":{}}}"#,
        )
        .await;
    let resolve = read_json_response(&mut lines, 4).await;
    assert_eq!(
        resolve["result"]["structuredContent"]["snapshot"]["records"][0]["name"].as_str(),
        Some("search"),
        "{resolve}"
    );

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"verlet_capsule_binding_delete","arguments":{"scope":{"kind":"global"},"operationName":"search"}}}"#,
        )
        .await;
    let delete = read_json_response(&mut lines, 6).await;
    assert_eq!(
        delete["result"]["structuredContent"]["binding"]["target"]["kind"].as_str(),
        Some("tombstone"),
        "{delete}"
    );

    drop(write);
    server_task.abort();
    let _ = server_task.await;
    serve_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mcp_prompt_lets_model_shaped_agent_call_secret_backed_search_wasm() {
    let _guard = MCP_CAPSULE_TEST_LOCK.lock().await;
    let root = std::path::PathBuf::from("/tmp").join(format!(
        "cdis-mcp-search-secret-{}",
        uuid::Uuid::now_v7().simple()
    ));
    let socket = root.join("app.sock");
    let registry_root = root.join("capsules");
    let listen = crate::adapters::app_server::AppServerListenAddr::Unix(socket.clone());
    let (base_url, http_server) = spawn_http_server(
        r#"{"results":[{"title":"Verlet runtime","url":"https://verlet.local"}]}"#,
        vec![
            "POST /search HTTP/1.1",
            "content-type: application/json",
            "x-api-key: fixture-secret",
            r#"{"query":"verlet"}"#,
        ],
    )
    .await;
    let url = format!("{base_url}/search");
    let http_grant = format!("net.http.private:POST:{base_url}");
    publish_search_for_url(&registry_root, &url, &http_grant, br#"{"query":"verlet"}"#).await;
    let secret_store =
        verlet_metadata::secret_store::SqliteSecretStore::open(root.join("state/metadata.sqlite3"))
            .await
            .unwrap();
    secret_store
        .set_secret(
            "EXAMPLE_API_KEY",
            "fixture-secret",
            verlet_metadata::secret_store::SecretSourceKind::Env,
            Some("EXAMPLE_API_KEY".to_string()),
        )
        .await
        .unwrap();
    let provider = std::sync::Arc::new(ModelVbinLifecycleClient::expecting_search_success());
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = provider.clone();
    let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIChatCompletions,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL,
    );
    runtime_config.max_tokens = 512;
    let capsule_bindings = crate::adapters::app_server::CapsuleBindingsConfig::default()
        .with_registry_root(&registry_root)
        .with_global_operation_name("search");
    let mut app_config = isolated_app_config(listen.clone(), &root);
    app_config.model_provider =
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string();
    app_config.model = crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string();
    app_config.capsule_bindings = capsule_bindings.clone();
    let runtime_factory =
        crate::adapters::app_server::runtime_factory_from_provider_parts_with_app_paths(
            runtime_config,
            provider_client,
            capsule_bindings,
            Some(std::sync::Arc::new(secret_store)),
            &app_config,
        );
    let app = crate::adapters::app_server::VerletAppServer::with_runtime_factory(
        app_config,
        runtime_factory,
    )
    .await
    .unwrap();
    let serve_task = tokio::spawn(async move { app.serve(listen).await });
    wait_for_socket(&socket).await;

    let (client, server) = tokio::io::duplex(256 * 1024);
    let config = crate::adapters::mcp_server::VerletMcpServerConfig {
        daemon_socket: socket.clone(),
        request_timeout: std::time::Duration::from_secs(10),
    };
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        crate::adapters::mcp_server::serve_mcp_stdio(server_read, server_write, config).await
    });
    let (read, mut write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(read).lines();

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
    let _ = read_json_response(&mut lines, 1).await;
    send(
        &mut write,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .await;
    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"verlet_prompt","arguments":{"message":"Use bash to call search for the query Verlet."}}}"#,
        )
        .await;
    let prompt = read_json_response(&mut lines, 2).await;
    assert_eq!(
        prompt["result"]["structuredContent"]["assistantText"],
        "MODEL_SEARCH_SUCCESS_REPORTED"
    );
    assert_eq!(provider.requests().len(), 2);

    drop(write);
    server_task.abort();
    let _ = server_task.await;
    serve_task.abort();
    http_server.await.unwrap();
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn mcp_prompt_rejects_thread_capsule_bindings() {
    let _guard = MCP_CAPSULE_TEST_LOCK.lock().await;
    let root = std::path::PathBuf::from("/tmp").join(format!(
        "cdis-mcp-thread-bind-{}",
        uuid::Uuid::now_v7().simple()
    ));
    let socket = root.join("app.sock");
    let registry_root = root.join("capsules");
    let listen = crate::adapters::app_server::AppServerListenAddr::Unix(socket.clone());
    publish_exa_without_secret(&registry_root).await;
    let provider = std::sync::Arc::new(ModelVbinLifecycleClient::default());
    let provider_client: std::sync::Arc<dyn verlet_provider::ProviderClient> = provider.clone();
    let mut runtime_config = crate::adapters::agent_loop::AgentLoopConfig::new(
        verlet_history::ProviderApi::OpenAIChatCompletions,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER,
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL,
    );
    runtime_config.max_tokens = 512;
    let capsule_bindings = crate::adapters::app_server::CapsuleBindingsConfig::default()
        .with_registry_root(&registry_root);
    let runtime_factory = crate::adapters::app_server::runtime_factory_from_provider_parts(
        runtime_config,
        provider_client,
        capsule_bindings.clone(),
    );
    let mut app_config = isolated_app_config(listen.clone(), &root);
    app_config.model_provider =
        crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_PROVIDER.to_string();
    app_config.model = crate::adapters::app_server::APP_SERVER_OPENAI_COMPATIBLE_MODEL.to_string();
    app_config.capsule_bindings = capsule_bindings;
    let app = crate::adapters::app_server::VerletAppServer::with_runtime_factory(
        app_config,
        runtime_factory,
    )
    .await
    .unwrap();
    let serve_task = tokio::spawn(async move { app.serve(listen).await });
    wait_for_socket(&socket).await;

    let (client, server) = tokio::io::duplex(256 * 1024);
    let config = crate::adapters::mcp_server::VerletMcpServerConfig {
        daemon_socket: socket.clone(),
        request_timeout: std::time::Duration::from_secs(10),
    };
    let (server_read, server_write) = tokio::io::split(server);
    let server_task = tokio::spawn(async move {
        crate::adapters::mcp_server::serve_mcp_stdio(server_read, server_write, config).await
    });
    let (read, mut write) = tokio::io::split(client);
    let mut lines = tokio::io::BufReader::new(read).lines();

    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"test","version":"1"}}}"#,
        )
        .await;
    let _ = read_json_response(&mut lines, 1).await;
    send(
        &mut write,
        r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
    )
    .await;
    send(
            &mut write,
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"verlet_prompt","arguments":{"message":"Use bash to call search for the query Verlet. If it fails because a credential is missing, report the failure.","thread":{"capsuleBindings":{"operationNames":["search"]}}}}}"#,
        )
        .await;
    let prompt = read_json_response(&mut lines, 2).await;
    assert_eq!(
        prompt["result"]["isError"].as_bool(),
        Some(true),
        "{prompt}"
    );
    assert!(
        prompt["result"]["structuredContent"]["error"]
            .as_str()
            .unwrap()
            .contains("operations are declared in an agent manifest"),
        "{prompt}"
    );
    assert!(provider.requests().is_empty());

    drop(write);
    server_task.abort();
    let _ = server_task.await;
    serve_task.abort();
    let _ = std::fs::remove_dir_all(root);
}

async fn send<W>(writer: &mut W, message: &str)
where
    W: tokio::io::AsyncWrite + Unpin,
{
    writer.write_all(message.as_bytes()).await.unwrap();
    writer.write_all(b"\n").await.unwrap();
    writer.flush().await.unwrap();
}

async fn read_json_line<R>(lines: &mut tokio::io::Lines<R>) -> serde_json::Value
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    let line = lines.next_line().await.unwrap().unwrap();
    serde_json::from_str(&line).unwrap()
}

async fn read_json_response<R>(lines: &mut tokio::io::Lines<R>, id: i64) -> serde_json::Value
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    for _ in 0..32 {
        let message = read_json_line(lines).await;
        if message.get("id").and_then(serde_json::Value::as_i64) == Some(id) {
            return message;
        }
    }
    panic!("timed out waiting for JSON-RPC response id {id}");
}

async fn wait_for_socket(path: &std::path::Path) {
    for _ in 0..1_500 {
        if path.exists() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("timed out waiting for {}", path.display());
}

fn isolated_app_config(
    listen: crate::adapters::app_server::AppServerListenAddr,
    root: &std::path::Path,
) -> crate::adapters::app_server::VerletAppServerConfig {
    let mut config = crate::adapters::app_server::VerletAppServerConfig::local(
        listen,
        std::env::current_dir().unwrap(),
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.agent_registry_root = root.join("agents");
    config
}

async fn assert_admission_surface(store_path: &std::path::Path, thread_id: &str, surface: &str) {
    let store = verlet_history_sqlite::SqliteSessionStore::open(store_path)
        .await
        .unwrap();
    let control_events = store
        .read_events(
            &verlet_history::EventStreamId::new(format!("control:{thread_id}")),
            None,
        )
        .await
        .unwrap();
    let thread_events = store
        .read_events(
            &verlet_history::EventStreamId::new(format!("thread:{thread_id}")),
            None,
        )
        .await
        .unwrap();
    let admission = crate::kernel::admission::assert_admission_precedes_turn_records(
        &control_events,
        &thread_events,
    );
    assert_eq!(admission.payload["route_id"], format!("surface:{surface}"));
}

#[derive(Default)]
struct ModelVbinLifecycleClient {
    requests: std::sync::Mutex<Vec<verlet_provider::ProviderRequest>>,
    expected: ModelSearchExpectation,
}

#[derive(Clone, Copy, Default)]
enum ModelSearchExpectation {
    #[default]
    MissingSecret,
    Success,
}

impl ModelVbinLifecycleClient {
    fn expecting_search_success() -> Self {
        Self {
            requests: std::sync::Mutex::new(Vec::new()),
            expected: ModelSearchExpectation::Success,
        }
    }

    fn requests(&self) -> Vec<verlet_provider::ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl verlet_provider::ProviderClient for ModelVbinLifecycleClient {
    async fn complete(
        &self,
        request: &verlet_provider::ProviderRequest,
    ) -> verlet_provider::ProviderResult<verlet_provider::ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        let has_tool_result = request
            .messages
            .iter()
            .any(|message| matches!(message, verlet_history::CanonicalMessage::ToolResult { .. }));
        if !has_tool_result {
            let bash_description = request
                .tools
                .iter()
                .find(|tool| tool.name == "bash")
                .map(|tool| tool.description.as_str())
                .unwrap_or_default();
            assert!(
                bash_description.contains("search"),
                "bash tool description should advertise search: {bash_description}"
            );
            return Ok(verlet_provider::ProviderResponse {
                content: vec![verlet_history::CanonicalContent::tool_call(
                    "model_call_1",
                    "bash",
                    serde_json::json!({
                        "command": "command -v search && search '{\"query\":\"verlet\"}'"
                    }),
                )],
                usage: verlet_history::CanonicalUsage::default(),
                stop_reason: verlet_history::CanonicalStopReason::ToolUse,
            });
        }

        let text = text_from_canonical_messages(&request.messages);
        match self.expected {
            ModelSearchExpectation::MissingSecret => {
                assert!(
                    text.contains("search") && text.contains(r#""exit_code":1"#),
                    "expected bash tool result to report failed Example Search command, got: {text}"
                );
                Ok(verlet_provider::ProviderResponse {
                    content: vec![verlet_history::CanonicalContent::text(
                        "MODEL_SEARCH_FAILURE_REPORTED",
                    )],
                    usage: verlet_history::CanonicalUsage::default(),
                    stop_reason: verlet_history::CanonicalStopReason::EndTurn,
                })
            }
            ModelSearchExpectation::Success => {
                assert!(
                    text.contains("search")
                        && text.contains(r#""exit_code":0"#)
                        && text.contains("Verlet runtime"),
                    "expected bash tool result to report successful Example Search command, got: {text}"
                );
                Ok(verlet_provider::ProviderResponse {
                    content: vec![verlet_history::CanonicalContent::text(
                        "MODEL_SEARCH_SUCCESS_REPORTED",
                    )],
                    usage: verlet_history::CanonicalUsage::default(),
                    stop_reason: verlet_history::CanonicalStopReason::EndTurn,
                })
            }
        }
    }
}

async fn publish_exa_without_secret(registry_root: &std::path::Path) {
    publish_search_for_url(
        registry_root,
        "https://api.example.invalid/search",
        "net.http:POST:https://api.example.invalid",
        br#"{"query":"verlet"}"#,
    )
    .await;
}

async fn publish_search_for_url(
    registry_root: &std::path::Path,
    url: &str,
    http_grant: &str,
    body: &[u8],
) {
    std::fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(render_search_fixture(url, http_grant, body))
        .expect("Example Search WAT fixture should compile to wasm");
    let artifact_path = registry_root.join("search.wasm");
    std::fs::write(&artifact_path, wasm).unwrap();
    verlet_operations::operation_store::LocalOperationRegistry::new(registry_root)
        .publish_artifact(
            verlet_operations::operation_store::PublishOperationRequest {
                name: "search".to_string(),
                artifact_path: artifact_path.clone(),
                source: verlet_operations::operation_store::PublishedOperationSource::Wasm {
                    bin_path: artifact_path,
                },
                interface: None,
                capability_grants: [http_grant.to_string(), "secret:EXAMPLE_API_KEY".to_string()]
                    .into_iter()
                    .collect(),
                metadata: Default::default(),
            },
        )
        .await
        .unwrap();
}

async fn spawn_http_server(
    response_body: &'static str,
    request_contains: Vec<&'static str>,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        loop {
            let read = socket.read(&mut buffer).await.unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&buffer[..read]);
            let request_text = String::from_utf8_lossy(&request);
            if let Some(header_end) = request_text.find("\r\n\r\n") {
                let content_length = request_text
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or_default();
                if request.len() >= header_end + 4 + content_length {
                    break;
                }
            }
        }
        let request_text = String::from_utf8_lossy(&request);
        for expected in request_contains {
            assert!(
                request_text.contains(expected),
                "request did not contain {expected:?}: {request_text}"
            );
        }
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
            response_body.len(),
            response_body
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    (base_url, handle)
}

fn render_search_fixture(url: &str, http_grant: &str, body: &[u8]) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "search",
            "input": "json",
            "output": "json",
            "events": "jsonl",
            "mode": "sync",
            "required_capabilities": [http_grant, "secret:EXAMPLE_API_KEY"]
        }]
    })
    .to_string();
    let request = serde_json::json!({
        "abi": "cooldis.net.http/0.1",
        "method": "POST",
        "url": url,
        "headers": [["content-type", "application/json"]],
        "secret_headers": [["x-api-key", "EXAMPLE_API_KEY"]],
        "timeout_ms": 5000,
        "max_response_bytes": 2048
    })
    .to_string();
    SEARCH_FIXTURE_TEMPLATE
        .replace("{{manifest}}", &wat_bytes(manifest.as_bytes()))
        .replace("{{manifest_len}}", &manifest.len().to_string())
        .replace("{{request}}", &wat_bytes(request.as_bytes()))
        .replace("{{request_len}}", &request.len().to_string())
        .replace("{{body}}", &wat_bytes(body))
        .replace("{{body_len}}", &body.len().to_string())
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

fn text_from_canonical_messages(messages: &[verlet_history::CanonicalMessage]) -> String {
    messages
        .iter()
        .flat_map(|message| match message {
            verlet_history::CanonicalMessage::User { content, .. }
            | verlet_history::CanonicalMessage::Assistant { content, .. }
            | verlet_history::CanonicalMessage::ToolResult { content, .. } => content,
        })
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Thinking { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}
