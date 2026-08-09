//! Multi-tenant host facade acceptance tests (EMO-553).

use futures_util::SinkExt as _;
use futures_util::StreamExt as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

#[path = "support/test_mount.rs"]
mod support;

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Criterion: host serves two instances through one listener; each
/// credential reaches only its own instance.
#[tokio::test]
async fn each_credential_reaches_only_its_routed_instance() {
    let (pair, first_token, second_token) = HostedPair::start("credential-routing").await;

    let mut first = connect_rpc(pair.addr, &first_token).await;
    let first_started = rpc_call(&mut first, 1, "thread/start", serde_json::json!({}))
        .await
        .unwrap();
    let first_thread = first_started["thread"]["id"].as_str().unwrap().to_string();
    first.close(None).await.unwrap();

    let mut second = connect_rpc(pair.addr, &second_token).await;
    let second_started = rpc_call(&mut second, 2, "thread/start", serde_json::json!({}))
        .await
        .unwrap();
    let second_thread = second_started["thread"]["id"].as_str().unwrap().to_string();
    let cross_read = rpc_call(
        &mut second,
        3,
        "thread/read",
        serde_json::json!({ "threadId": first_thread }),
    )
    .await
    .unwrap_err();
    assert_eq!(cross_read.code, -32001);
    second.close(None).await.unwrap();

    let first_app = pair.host.instance(&pair.first_id).await.unwrap();
    let second_app = pair.host.instance(&pair.second_id).await.unwrap();
    let first_loaded = first_app
        .local_json_rpc_request("thread/loaded/list", serde_json::json!({}))
        .await
        .unwrap();
    let second_loaded = second_app
        .local_json_rpc_request("thread/loaded/list", serde_json::json!({}))
        .await
        .unwrap();
    assert!(json_string_array(&first_loaded["data"]).contains(&first_thread.as_str()));
    assert!(!json_string_array(&first_loaded["data"]).contains(&second_thread.as_str()));
    assert!(json_string_array(&second_loaded["data"]).contains(&second_thread.as_str()));
    assert!(!json_string_array(&second_loaded["data"]).contains(&first_thread.as_str()));

    wait_for_sql_count(
        first_app.session_store_path(),
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'host'",
        1,
    )
    .await;
    wait_for_sql_count(
        second_app.session_store_path(),
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'host'",
        1,
    )
    .await;

    // Selection is not authentication: moving B's digest to A must make A
    // reject it without probing the authority that originally minted it.
    pair.host
        .register_credential_route(&second_token, pair.first_id.clone());
    let rejected = raw_websocket_response(pair.addr, &second_token).await;
    assert!(rejected.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
    wait_for_sql_count(
        first_app.session_store_path(),
        "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'host'",
        1,
    )
    .await;
    assert_eq!(
        sql_count(
            second_app.session_store_path(),
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'host'",
        )
        .await,
        0,
    );

    // An unrouted token is host-logged only and creates no instance witness.
    let unrouted = raw_websocket_response(pair.addr, "unrouted-fixture-token").await;
    assert!(unrouted.starts_with("HTTP/1.1 401 Unauthorized\r\n"));
    assert_eq!(
        sql_count(
            first_app.session_store_path(),
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'host'",
        )
        .await,
        1,
    );
    assert_eq!(
        sql_count(
            second_app.session_store_path(),
            "SELECT COUNT(*) FROM cooldis_identity_auth_rejections WHERE surface = 'host'",
        )
        .await,
        0,
    );

    drop(first_app);
    drop(second_app);
    pair.shutdown().await;
}

/// Criterion: a tenant/instance identifier inside an RPC body that
/// disagrees with the connection's routed instance is rejected — the
/// routing context is the only authority (capsule-binding resolution's
/// client-supplied tenant id is the known offender).
#[tokio::test]
async fn cross_instance_identifier_in_rpc_body_is_rejected() {
    let (pair, first_token, _) = HostedPair::start("cross-identifier").await;
    let mut first = connect_rpc(pair.addr, &first_token).await;

    let error = rpc_call(
        &mut first,
        1,
        "capsule/binding/resolve",
        serde_json::json!({ "tenantId": "tenant-b" }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, -32602);
    assert!(error.message.contains("serving instance"));

    for (id, method, params) in [
        (
            10,
            "capsule/binding/set",
            serde_json::json!({
                "scope": { "kind": "tenant", "tenantId": "tenant-b" },
                "operationName": "cross-instance-fixture",
                "artifactHash": "sha256:cross-instance-fixture",
            }),
        ),
        (
            11,
            "capsule/binding/delete",
            serde_json::json!({
                "scope": { "kind": "tenant", "tenantId": "tenant-b" },
                "operationName": "cross-instance-fixture",
            }),
        ),
        (
            12,
            "capsule/binding/list",
            serde_json::json!({
                "scope": {
                    "kind": "thread",
                    "tenantId": "tenant-b",
                    "threadId": "thread-b",
                },
            }),
        ),
    ] {
        let error = rpc_call(&mut first, id, method, params).await.unwrap_err();
        assert_eq!(error.code, -32602);
        assert!(error.message.contains("serving instance"));
    }

    let first_app = pair.host.instance(&pair.first_id).await.unwrap();
    let standalone_error = first_app
        .local_json_rpc_request(
            "capsule/binding/resolve",
            serde_json::json!({ "tenantId": "tenant-b" }),
        )
        .await
        .unwrap_err();
    assert!(standalone_error.to_string().contains("serving instance"));

    let own = rpc_call(
        &mut first,
        2,
        "capsule/binding/resolve",
        serde_json::json!({ "tenantId": "tenant-a" }),
    )
    .await
    .unwrap();
    assert!(own["snapshot"]["records"].as_array().unwrap().is_empty());

    first.close(None).await.unwrap();
    drop(first_app);
    pair.shutdown().await;
}

/// Criterion: two-instance DST — one instance cut and recovered from its
/// journal while the peer keeps making invariant-checked progress
/// (`PairedCutPhase::{BeforeCut, VictimDown, AfterRecovery}`).
#[tokio::test]
async fn one_instance_cut_and_recovered_while_peer_progresses() {
    let provider = BlockingChatProvider::start().await;
    let (pair, _, _) =
        HostedPair::start_with_first_provider("paired-crash-cut", &provider.base_url()).await;
    let mut peer = HostedPeerProgress {
        host: pair.host.clone(),
        id: pair.second_id.clone(),
        victim_id: pair.first_id.clone(),
        expected_tenant: "tenant-b",
        observed_phases: Vec::new(),
        observed_threads: Vec::new(),
    };
    let victim = HostedCrashVictim {
        host: pair.host.clone(),
        id: pair.first_id.clone(),
        roots: pair.first_roots.clone(),
        workspace: pair.workspace.clone(),
        identity: identity("tenant-a", "operator-a"),
        provider,
        interrupted_thread: None,
    };

    let rebuilt =
        support::fault_plan::run_paired_crash_cut("queue-input-compile", victim, &mut peer).await;

    assert_eq!(
        peer.observed_phases,
        vec![
            support::fault_plan::PairedCutPhase::BeforeCut,
            support::fault_plan::PairedCutPhase::VictimDown,
            support::fault_plan::PairedCutPhase::AfterRecovery,
        ]
    );
    assert_eq!(peer.observed_threads.len(), 3);
    let recovered = rebuilt.host.instance(&rebuilt.id).await.unwrap();
    let interrupted_thread = rebuilt.interrupted_thread.as_deref().unwrap();
    let loaded = recovered
        .local_json_rpc_request("thread/loaded/list", serde_json::json!({}))
        .await
        .unwrap();
    assert!(json_string_array(&loaded["data"]).contains(&interrupted_thread));
    drop(recovered);
    drop(rebuilt);
    pair.shutdown().await;
}

/// Criterion: facade shutdown drains connections and shuts down every
/// instance cleanly; instance roots are reusable afterwards.
#[tokio::test]
async fn facade_shutdown_drains_connections_and_instances() {
    let (pair, first_token, second_token) = HostedPair::start("shutdown-drain").await;
    let first_store = pair.first_roots.state_home.join("session_history.sqlite3");
    let second_store = pair.second_roots.state_home.join("session_history.sqlite3");
    let mut first = connect_rpc(pair.addr, &first_token).await;
    let mut second = connect_rpc(pair.addr, &second_token).await;
    rpc_call(&mut first, 1, "account/read", serde_json::json!({}))
        .await
        .unwrap();
    rpc_call(&mut second, 2, "account/read", serde_json::json!({}))
        .await
        .unwrap();

    tokio::time::timeout(RPC_TIMEOUT, pair.host.shutdown())
        .await
        .expect("host shutdown did not drain live connections")
        .unwrap();
    assert!(pair.host.instance_ids().await.is_empty());
    assert!(websocket_ended(&mut first).await);
    assert!(websocket_ended(&mut second).await);
    for store in [&first_store, &second_store] {
        wait_for_sql_count(
            store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'host' AND closed_at_ms IS NOT NULL",
            1,
        )
        .await;
        assert_eq!(
            sql_count(
                store,
                "SELECT COUNT(*) FROM cooldis_identity_sessions AS opened WHERE surface = 'host' AND closed_at_ms IS NULL AND NOT EXISTS (SELECT 1 FROM cooldis_identity_sessions AS closed WHERE closed.session_id = opened.session_id AND closed.closed_at_ms IS NOT NULL)",
            )
            .await,
            0,
        );
    }

    let first_successor = hosted_config(
        pair.first_roots.clone(),
        &pair.workspace,
        &identity("tenant-a", "operator-a"),
        None,
    );
    let second_successor = hosted_config(
        pair.second_roots.clone(),
        &pair.workspace,
        &identity("tenant-b", "operator-b"),
        None,
    );
    drop(first_successor);
    drop(second_successor);

    pair.host.shutdown().await.unwrap();
    std::fs::remove_dir_all(&pair.root).unwrap();
}

#[tokio::test]
async fn instance_shutdown_closes_its_host_routed_session() {
    let (pair, first_token, _) = HostedPair::start("instance-session-close").await;
    let first_store = pair.first_roots.state_home.join("session_history.sqlite3");
    let mut first = connect_rpc(pair.addr, &first_token).await;
    wait_for_sql_count(
        &first_store,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'host' AND closed_at_ms IS NULL",
        1,
    )
    .await;

    pair.host.shutdown_instance(&pair.first_id).await.unwrap();

    let successor = hosted_config(
        pair.first_roots.clone(),
        &pair.workspace,
        &identity("tenant-a", "operator-a"),
        None,
    );
    drop(successor);
    assert!(websocket_ended(&mut first).await);
    wait_for_sql_count(
        &first_store,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'host' AND closed_at_ms IS NOT NULL",
        1,
    )
    .await;
    assert_eq!(
        sql_count(
            &first_store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions AS opened WHERE surface = 'host' AND closed_at_ms IS NULL AND NOT EXISTS (SELECT 1 FROM cooldis_identity_sessions AS closed WHERE closed.session_id = opened.session_id AND closed.closed_at_ms IS NOT NULL)",
        )
        .await,
        0,
    );
    pair.shutdown().await;
}

#[tokio::test]
async fn host_shutdown_breaks_a_connection_blocking_instance_shutdown() {
    let (pair, first_token, _) = HostedPair::start("shutdown-race").await;
    let marker = pair.workspace.join("blocking-command-started");
    let mut first = connect_rpc(pair.addr, &first_token).await;
    send_rpc_request(
        &mut first,
        40,
        "command/exec",
        serde_json::json!({
            "command": [
                "/bin/sh",
                "-c",
                "touch \"$1\"; sleep 10",
                "host-facade-test",
                marker.to_string_lossy(),
            ],
        }),
    )
    .await;
    tokio::time::timeout(RPC_TIMEOUT, async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let instance_shutdown = {
        let host = pair.host.clone();
        let id = pair.first_id.clone();
        tokio::spawn(async move { host.shutdown_instance(&id).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    // tight-timeout: deadlock detector; the fixed lock cycle would hold this forever
    tokio::time::timeout(std::time::Duration::from_secs(2), pair.host.shutdown())
        .await
        .expect("host shutdown deadlocked behind instance shutdown's dispatch gate")
        .unwrap();
    instance_shutdown.await.unwrap().unwrap();
    assert!(websocket_ended(&mut first).await);
    pair.shutdown().await;
}

#[tokio::test]
async fn standalone_shutdown_cancels_a_request_holding_the_dispatch_gate() {
    let root = test_root("standalone-shutdown-gate");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let config = standalone_config(&root, &workspace, addr);
    let store = config.state_home.join("session_history.sqlite3");
    let token = mint_operator_credential(&store, "standalone-operator").await;
    let app = verlet::adapters::app_server::VerletAppServer::new(config)
        .await
        .unwrap();
    let server_task = {
        let app = app.clone();
        tokio::spawn(async move { app.serve_websocket_listener(listener).await })
    };
    let marker = workspace.join("blocking-command-started");
    let mut websocket = connect_rpc(addr, &token).await;
    send_rpc_request(
        &mut websocket,
        41,
        "command/exec",
        serde_json::json!({
            "command": [
                "/bin/sh",
                "-c",
                "touch \"$1\"; sleep 10",
                "host-facade-test",
                marker.to_string_lossy(),
            ],
        }),
    )
    .await;
    tokio::time::timeout(RPC_TIMEOUT, async {
        while !marker.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    // tight-timeout: deadlock detector; the fixed lock cycle would hold this forever
    tokio::time::timeout(std::time::Duration::from_secs(2), app.shutdown())
        .await
        .expect("standalone shutdown deadlocked behind its dispatch gate")
        .unwrap();
    assert!(websocket_ended(&mut websocket).await);
    server_task.await.unwrap().unwrap();
    assert_eq!(
        sql_count(
            &store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'websocket' AND closed_at_ms IS NOT NULL",
        )
        .await,
        1,
    );
    assert_eq!(
        sql_count(
            &store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions AS opened WHERE surface = 'websocket' AND closed_at_ms IS NULL AND NOT EXISTS (SELECT 1 FROM cooldis_identity_sessions AS closed WHERE closed.session_id = opened.session_id AND closed.closed_at_ms IS NOT NULL)",
        )
        .await,
        0,
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn console_protocol_route_is_witnessed_only_as_host() {
    let (pair, first_token, _) = HostedPair::start("console-protocol-surface").await;
    let first_store = pair.first_roots.state_home.join("session_history.sqlite3");
    let mut first = connect_console_rpc(pair.addr, &first_token).await;
    first.close(None).await.unwrap();

    wait_for_sql_count(
        &first_store,
        "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface = 'host' AND closed_at_ms IS NOT NULL",
        1,
    )
    .await;
    assert_eq!(
        sql_count(
            &first_store,
            "SELECT COUNT(*) FROM cooldis_identity_sessions WHERE surface IN ('console', 'websocket')",
        )
        .await,
        0,
    );
    pair.shutdown().await;
}

#[tokio::test]
async fn host_rejects_a_second_websocket_listener() {
    let (pair, _, _) = HostedPair::start("one-listener").await;
    let second_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

    let error = pair
        .host
        .serve_websocket_listener(second_listener)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("listener already installed"));
    pair.shutdown().await;
}

struct HostedPair {
    root: std::path::PathBuf,
    workspace: std::path::PathBuf,
    host: verlet::adapters::host::VerletHost,
    first_id: verlet::adapters::host::InstanceId,
    second_id: verlet::adapters::host::InstanceId,
    first_roots: verlet::adapters::app_server::instance::InstanceRoots,
    second_roots: verlet::adapters::app_server::instance::InstanceRoots,
    addr: std::net::SocketAddr,
}

impl HostedPair {
    async fn start(label: &str) -> (Self, String, String) {
        Self::start_with_optional_first_provider(label, None).await
    }

    async fn start_with_first_provider(
        label: &str,
        provider_base_url: &str,
    ) -> (Self, String, String) {
        Self::start_with_optional_first_provider(label, Some(provider_base_url)).await
    }

    async fn start_with_optional_first_provider(
        label: &str,
        provider_base_url: Option<&str>,
    ) -> (Self, String, String) {
        let root = test_root(label);
        let workspace = root.join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        let first_roots =
            verlet::adapters::app_server::instance::InstanceRoots::under(root.join("first"));
        let second_roots =
            verlet::adapters::app_server::instance::InstanceRoots::under(root.join("second"));
        let first_id = verlet::adapters::host::InstanceId::new("first").unwrap();
        let second_id = verlet::adapters::host::InstanceId::new("second").unwrap();
        let first_token = mint_operator_credential(
            &first_roots.state_home.join("session_history.sqlite3"),
            "operator-a",
        )
        .await;
        let second_token = mint_operator_credential(
            &second_roots.state_home.join("session_history.sqlite3"),
            "operator-b",
        )
        .await;
        let host = verlet::adapters::host::VerletHost::new();
        host.start_instance(
            first_id.clone(),
            hosted_config(
                first_roots.clone(),
                &workspace,
                &identity("tenant-a", "operator-a"),
                provider_base_url,
            ),
        )
        .await
        .unwrap();
        host.start_instance(
            second_id.clone(),
            hosted_config(
                second_roots.clone(),
                &workspace,
                &identity("tenant-b", "operator-b"),
                None,
            ),
        )
        .await
        .unwrap();
        host.register_credential_route(&first_token, first_id.clone());
        host.register_credential_route(&second_token, second_id.clone());

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        host.serve_websocket_listener(listener).await.unwrap();

        (
            Self {
                root,
                workspace,
                host,
                first_id,
                second_id,
                first_roots,
                second_roots,
                addr,
            },
            first_token,
            second_token,
        )
    }

    async fn shutdown(self) {
        self.host.shutdown().await.unwrap();
        std::fs::remove_dir_all(self.root).unwrap();
    }
}

struct HostedCrashVictim {
    host: verlet::adapters::host::VerletHost,
    id: verlet::adapters::host::InstanceId,
    roots: verlet::adapters::app_server::instance::InstanceRoots,
    workspace: std::path::PathBuf,
    identity: verlet::daemon::identity::VerletDaemonIdentityConfig,
    provider: BlockingChatProvider,
    interrupted_thread: Option<String>,
}

struct HostedCrashState {
    host: verlet::adapters::host::VerletHost,
    id: verlet::adapters::host::InstanceId,
    roots: verlet::adapters::app_server::instance::InstanceRoots,
    workspace: std::path::PathBuf,
    identity: verlet::daemon::identity::VerletDaemonIdentityConfig,
    provider: BlockingChatProvider,
    interrupted_thread: String,
}

#[async_trait::async_trait]
impl support::fault_plan::CrashCutHost for HostedCrashVictim {
    type StoreState = HostedCrashState;

    async fn run_to_cut(&mut self, seam: support::fault_plan::CrashCutSeam) {
        assert_eq!(
            seam,
            support::fault_plan::CrashCutSeam::PersistedInputRuntimeNotify
        );
        let app = self.host.instance(&self.id).await.unwrap();
        let started = app
            .local_json_rpc_request("thread/start", serde_json::json!({}))
            .await
            .unwrap();
        let thread_id = started["thread"]["id"].as_str().unwrap().to_string();
        app.local_json_rpc_request(
            "turn/start",
            serde_json::json!({
                "threadId": thread_id,
                "input": [{ "type": "text", "text": "persist before cut", "text_elements": [] }],
            }),
        )
        .await
        .unwrap();
        self.provider.wait_until_requested().await;
        self.interrupted_thread = Some(thread_id);

        self.host.shutdown_instance(&self.id).await.unwrap();
        drop(app);
        self.provider.release();
    }

    fn tear_down(self) -> Self::StoreState {
        HostedCrashState {
            host: self.host,
            id: self.id,
            roots: self.roots,
            workspace: self.workspace,
            identity: self.identity,
            provider: self.provider,
            interrupted_thread: self.interrupted_thread.unwrap(),
        }
    }

    async fn rebuild(state: Self::StoreState) -> Self {
        state
            .host
            .start_instance(
                state.id.clone(),
                hosted_config(
                    state.roots.clone(),
                    &state.workspace,
                    &state.identity,
                    Some(&state.provider.base_url()),
                ),
            )
            .await
            .unwrap();
        Self {
            host: state.host,
            id: state.id,
            roots: state.roots,
            workspace: state.workspace,
            identity: state.identity,
            provider: state.provider,
            interrupted_thread: Some(state.interrupted_thread),
        }
    }

    async fn recover(&mut self) {
        let app = self.host.instance(&self.id).await.unwrap();
        let thread_id = self.interrupted_thread.as_deref().unwrap();
        app.local_json_rpc_request(
            "thread/resume",
            serde_json::json!({ "threadId": thread_id, "excludeTurns": false }),
        )
        .await
        .unwrap();
        let read = app
            .local_json_rpc_request(
                "thread/read",
                serde_json::json!({ "threadId": thread_id, "includeTurns": true }),
            )
            .await
            .unwrap();
        assert_eq!(read["thread"]["id"], thread_id);
        assert!(read.to_string().contains("persist before cut"));
        assert_eq!(app.tenant_id(), "tenant-a");
    }
}

struct HostedPeerProgress {
    host: verlet::adapters::host::VerletHost,
    id: verlet::adapters::host::InstanceId,
    victim_id: verlet::adapters::host::InstanceId,
    expected_tenant: &'static str,
    observed_phases: Vec<support::fault_plan::PairedCutPhase>,
    observed_threads: Vec<String>,
}

#[async_trait::async_trait]
impl support::fault_plan::PeerProgress for HostedPeerProgress {
    async fn step(&mut self, phase: support::fault_plan::PairedCutPhase) {
        assert_eq!(
            self.host.instance(&self.victim_id).await.is_some(),
            phase != support::fault_plan::PairedCutPhase::VictimDown,
        );
        let app = self.host.instance(&self.id).await.unwrap();
        assert_eq!(app.tenant_id(), self.expected_tenant);
        let started = app
            .local_json_rpc_request("thread/start", serde_json::json!({}))
            .await
            .unwrap();
        let thread_id = started["thread"]["id"].as_str().unwrap().to_string();
        let parsed = verlet_runtime_contracts::ThreadId::parse_str(&thread_id).unwrap();
        let handle = app
            .supervisor()
            .get_thread(self.expected_tenant, parsed)
            .await
            .unwrap();
        assert_eq!(handle.context().coordinates.tenant_id, self.expected_tenant);
        assert!(!self.observed_threads.contains(&thread_id));
        self.observed_phases.push(phase);
        self.observed_threads.push(thread_id);
    }
}

struct BlockingChatProvider {
    addr: std::net::SocketAddr,
    requested: std::sync::Arc<tokio::sync::Notify>,
    request_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    released: std::sync::Arc<std::sync::atomic::AtomicBool>,
    release: std::sync::Arc<tokio::sync::Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl BlockingChatProvider {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requested = std::sync::Arc::new(tokio::sync::Notify::new());
        let request_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let released = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let task_requested = std::sync::Arc::clone(&requested);
        let task_request_count = std::sync::Arc::clone(&request_count);
        let task_released = std::sync::Arc::clone(&released);
        let task_release = std::sync::Arc::clone(&release);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let requested = std::sync::Arc::clone(&task_requested);
                let request_count = std::sync::Arc::clone(&task_request_count);
                let released = std::sync::Arc::clone(&task_released);
                let release = std::sync::Arc::clone(&task_release);
                tokio::spawn(async move {
                    serve_chat_request(stream, requested, request_count, released, release).await;
                });
            }
        });
        Self {
            addr,
            requested,
            request_count,
            released,
            release,
            task,
        }
    }

    fn base_url(&self) -> String {
        format!("http://{}/v1", self.addr)
    }

    async fn wait_until_requested(&self) {
        tokio::time::timeout(RPC_TIMEOUT, async {
            loop {
                if self
                    .request_count
                    .load(std::sync::atomic::Ordering::Acquire)
                    > 0
                {
                    return;
                }
                self.requested.notified().await;
            }
        })
        .await
        .expect("victim runtime did not reach the provider cut");
    }

    fn release(&self) {
        self.released
            .store(true, std::sync::atomic::Ordering::Release);
        self.release.notify_waiters();
    }
}

impl Drop for BlockingChatProvider {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_chat_request(
    mut stream: tokio::net::TcpStream,
    requested: std::sync::Arc<tokio::sync::Notify>,
    request_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    released: std::sync::Arc<std::sync::atomic::AtomicBool>,
    release: std::sync::Arc<tokio::sync::Notify>,
) {
    read_http_request(&mut stream).await;
    let request_index = request_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    requested.notify_waiters();
    if request_index == 0 {
        while !released.load(std::sync::atomic::Ordering::Acquire) {
            release.notified().await;
        }
    }
    let event = serde_json::json!({
        "choices": [{ "delta": { "content": "recovered" }, "finish_reason": "stop" }],
        "usage": { "prompt_tokens": 1, "completion_tokens": 1 },
    });
    let body = format!("data: {event}\n\ndata: [DONE]\n\n");
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len(),
    );
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 1024];
    let content_length = tokio::time::timeout(RPC_TIMEOUT, async {
        loop {
            let read = stream.read(&mut chunk).await.unwrap();
            assert!(
                read > 0,
                "provider connection closed before request headers"
            );
            request.extend_from_slice(&chunk[..read]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8(request[..end].to_vec()).unwrap();
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                break (end + 4, content_length);
            }
        }
    })
    .await
    .unwrap();
    let (body_start, content_length) = content_length;
    while request.len() - body_start < content_length {
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0, "provider connection closed before request body");
        request.extend_from_slice(&chunk[..read]);
    }
}

fn hosted_config(
    roots: verlet::adapters::app_server::instance::InstanceRoots,
    workspace: &std::path::Path,
    identity: &verlet::daemon::identity::VerletDaemonIdentityConfig,
    provider_base_url: Option<&str>,
) -> verlet::adapters::app_server::VerletAppServerConfig {
    let environment = verlet::adapters::app_server::instance::InstanceEnvironment {
        provider_auth: verlet::adapters::app_server::instance::ProviderAuthSource::Injected(
            verlet_metadata::provider_store::LlmProviderAuthContext::new(),
        ),
        hook_shell: Some("/bin/sh".to_string()),
        process_ids: std::sync::Arc::new(verlet_process::process::DeterministicProcessIds::new()),
    };
    let config = verlet::adapters::app_server::VerletAppServerConfig::hosted(
        roots,
        environment,
        workspace,
        identity,
    )
    .unwrap();
    match provider_base_url {
        Some(base_url) => config.with_openai_chat_completions(
            "host-facade-fixture",
            base_url,
            "fixture-key",
            "fixture-model",
        ),
        None => config,
    }
}

fn standalone_config(
    root: &std::path::Path,
    workspace: &std::path::Path,
    addr: std::net::SocketAddr,
) -> verlet::adapters::app_server::VerletAppServerConfig {
    let mut config = verlet::adapters::app_server::VerletAppServerConfig::local(
        verlet::adapters::app_server::AppServerListenAddr::WebSocket(addr),
        workspace,
    );
    config.runtime_home = root.join("runtime");
    config.state_home = root.join("state");
    config.user_state_home = root.join("user-state");
    config.agent_registry_root = root.join("agents");
    config.blob_registry_root = root.join("blobs");
    config.skill_registry_root = root.join("skills");
    config.apply_daemon_identity_config(&identity("standalone-tenant", "standalone-operator"));
    config
}

fn identity(tenant: &str, operator: &str) -> verlet::daemon::identity::VerletDaemonIdentityConfig {
    verlet::daemon::identity::VerletDaemonIdentityConfig {
        mode: verlet::daemon::identity::IdentityMode::Local,
        tenant_id: Some(tenant.to_string()),
        console_principal: Some(verlet::daemon::identity::PrincipalId::new(operator)),
    }
}

async fn mint_operator_credential(path: &std::path::Path, operator: &str) -> String {
    let store = verlet_history_sqlite::SqliteSessionStore::open(path)
        .await
        .unwrap();
    let authority = verlet::daemon::identity::SqliteIdentityAuthority::new(
        store,
        std::sync::Arc::new(verlet::daemon::clock_route::SystemDaemonClock),
        None,
    )
    .await
    .unwrap();
    let operator = verlet::daemon::identity::PrincipalId::new(operator);
    let token = authority
        .bootstrap_operator(&operator, "Host facade operator")
        .await
        .unwrap()
        .2;
    drop(authority);
    token
}

async fn connect_rpc(
    addr: std::net::SocketAddr,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let mut request = format!("ws://{addr}/rpc").into_client_request().unwrap();
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::AUTHORIZATION,
        tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&format!("Bearer {token}"))
            .unwrap(),
    );
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut websocket, _) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap();
    rpc_call(
        &mut websocket,
        0,
        "initialize",
        serde_json::json!({
            "clientInfo": { "name": "host-facade-test", "version": "0" },
            "capabilities": {},
        }),
    )
    .await
    .unwrap();
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(
                &verlet::adapters::app_server::connection::JsonRpcMessage::Notification(
                    verlet::adapters::app_server::connection::JsonRpcNotification {
                        method: "initialized".to_string(),
                        params: None,
                    },
                ),
            )
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    websocket
}

async fn connect_console_rpc(
    addr: std::net::SocketAddr,
    token: &str,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let mut request = format!("ws://{addr}/rpc").into_client_request().unwrap();
    request.headers_mut().insert(
        tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL,
        tokio_tungstenite::tungstenite::http::HeaderValue::from_str(&format!(
            "verlet-console-token.{token}"
        ))
        .unwrap(),
    );
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut websocket, response) = tokio_tungstenite::client_async(request, stream)
        .await
        .unwrap();
    let selected = response
        .headers()
        .get(tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .unwrap();
    assert!(selected == format!("verlet-console-token.{token}"));
    rpc_call(
        &mut websocket,
        0,
        "initialize",
        serde_json::json!({
            "clientInfo": { "name": "host-facade-console-test", "version": "0" },
            "capabilities": {},
        }),
    )
    .await
    .unwrap();
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(
                &verlet::adapters::app_server::connection::JsonRpcMessage::Notification(
                    verlet::adapters::app_server::connection::JsonRpcNotification {
                        method: "initialized".to_string(),
                        params: None,
                    },
                ),
            )
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    websocket
}

async fn rpc_call(
    websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    id: i64,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, verlet::adapters::app_server::connection::JsonRpcErrorError> {
    let id = verlet::adapters::app_server::connection::RequestId::Integer(id);
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(
                &verlet::adapters::app_server::connection::JsonRpcMessage::Request(
                    verlet::adapters::app_server::connection::JsonRpcRequest {
                        id: id.clone(),
                        method: method.to_string(),
                        params: Some(params),
                        trace: None,
                    },
                ),
            )
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(RPC_TIMEOUT, async {
        loop {
            let message = websocket.next().await.unwrap().unwrap();
            let tokio_tungstenite::tungstenite::Message::Text(text) = message else {
                continue;
            };
            match serde_json::from_str::<verlet::adapters::app_server::connection::JsonRpcMessage>(
                &text,
            )
            .unwrap()
            {
                verlet::adapters::app_server::connection::JsonRpcMessage::Response(response)
                    if response.id == id =>
                {
                    return Ok(response.result);
                }
                verlet::adapters::app_server::connection::JsonRpcMessage::Error(error)
                    if error.id == id =>
                {
                    return Err(error.error);
                }
                _ => {}
            }
        }
    })
    .await
    .expect("timed out waiting for host-routed RPC response")
}

async fn send_rpc_request(
    websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    id: i64,
    method: &str,
    params: serde_json::Value,
) {
    websocket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(
                &verlet::adapters::app_server::connection::JsonRpcMessage::Request(
                    verlet::adapters::app_server::connection::JsonRpcRequest {
                        id: verlet::adapters::app_server::connection::RequestId::Integer(id),
                        method: method.to_string(),
                        params: Some(params),
                        trace: None,
                    },
                ),
            )
            .unwrap()
            .into(),
        ))
        .await
        .unwrap();
}

async fn raw_websocket_response(addr: std::net::SocketAddr, token: &str) -> String {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let request = format!(
        "GET /rpc HTTP/1.1\r\nHost: {addr}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nAuthorization: Bearer {token}\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(RPC_TIMEOUT, stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    String::from_utf8(response).unwrap()
}

async fn websocket_ended(
    websocket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> bool {
    tokio::time::timeout(RPC_TIMEOUT, async {
        loop {
            match websocket.next().await {
                None | Some(Err(_)) => return true,
                Some(Ok(message)) if message.is_close() => return true,
                Some(Ok(_)) => {}
            }
        }
    })
    .await
    .unwrap_or(false)
}

fn json_string_array(value: &serde_json::Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect()
}

async fn sql_count(path: &std::path::Path, query: &str) -> i64 {
    let database = verlet_sqlite::Db::open(path, verlet_sqlite::DbConfig::default())
        .await
        .unwrap();
    let connection = database.connect().await.unwrap();
    let mut rows = connection.query(query, ()).await.unwrap();
    rows.next().await.unwrap().unwrap().get(0).unwrap()
}

async fn wait_for_sql_count(path: &std::path::Path, query: &str, expected: i64) {
    tokio::time::timeout(RPC_TIMEOUT, async {
        loop {
            if sql_count(path, query).await >= expected {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

fn test_root(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "verlet-host-facade-{label}-{}",
        uuid::Uuid::now_v7()
    ))
}
