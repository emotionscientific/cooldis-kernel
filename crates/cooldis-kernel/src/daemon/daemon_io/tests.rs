use super::*;
use crate::{
    APP_SERVER_LOCAL_MODEL,
    APP_SERVER_LOCAL_PROVIDER,
    AppServerListenAddr,
    CanonicalContent,
    CanonicalProviderRuntimeConfig,
    CanonicalStopReason,
    CanonicalUsage,
    // lexicon-allow: capsule - existing app-server manifest binding config type
    CapsuleBindingsConfig,
    CooldisAppServerConfig,
    CooldisDaemonClockRoute,
    DaemonClock,
    EventKind,
    EventStore,
    LocalAgentRegistry,
    MandateCatchUpPolicy,
    MandateSchedulePayload,
    MandateStartRequest,
    ProviderApi,
    ProviderClient,
    ProviderRequest,
    ProviderResponse,
    ProviderResult,
    PublishOperationRequest,
    PublishedOperationSource,
    StreamCursorV1,
    THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA,
    ThreadSpawnedPayload,
    TimerFiredPayload,
    control_stream_id,
    revoke_mandate,
    start_mandate,
};
use chrono::{DateTime, TimeZone, Utc};
use cooldis_io_core::{
    ConversationKind, DeliveryReceipt, IngressContent, IoActor, IoConversation, IoDedupeKey,
    IoProtocolAdapter, IoProtocolCapabilities, IoSource, IoTarget,
};
use cooldis_io_pgqrs::{PgqrsIngressQueue, PgqrsQueueConfig, sqlite_dsn};
use serde_json::json;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;
use tokio::sync::{Mutex as TokioMutex, mpsc};

#[derive(Clone)]
struct CaptureSink {
    envelopes: Arc<TokioMutex<Vec<IngressEnvelope>>>,
}

#[async_trait]
impl IngressSink for CaptureSink {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        let ack = IngressAck::accepted(&envelope);
        self.envelopes.lock().await.push(envelope);
        Ok(ack)
    }
}

struct CaptureEgress {
    sender: mpsc::UnboundedSender<EgressEnvelope>,
}

impl IoProtocolAdapter for CaptureEgress {
    fn kind(&self) -> &'static str {
        "telegram.bot"
    }

    fn capabilities(&self) -> IoProtocolCapabilities {
        IoProtocolCapabilities {
            ingress: false,
            egress: true,
            streaming: false,
            durable_offsets: false,
            attachments: false,
        }
    }
}

#[async_trait]
impl EgressAdapter for CaptureEgress {
    async fn deliver(&self, envelope: EgressEnvelope) -> IoResult<DeliveryReceipt> {
        self.sender.send(envelope.clone()).unwrap();
        Ok(DeliveryReceipt::delivered(&envelope, "capture"))
    }
}

#[derive(Clone)]
struct ScriptedEgress {
    calls: Arc<TokioMutex<Vec<EgressEnvelope>>>,
    failures: Arc<TokioMutex<VecDeque<String>>>,
    external_ids: Arc<TokioMutex<VecDeque<String>>>,
}

impl ScriptedEgress {
    fn new(failures: impl IntoIterator<Item = impl Into<String>>, external_ids: &[&str]) -> Self {
        Self {
            calls: Arc::new(TokioMutex::new(Vec::new())),
            failures: Arc::new(TokioMutex::new(
                failures.into_iter().map(Into::into).collect(),
            )),
            external_ids: Arc::new(TokioMutex::new(
                external_ids.iter().map(|id| id.to_string()).collect(),
            )),
        }
    }

    async fn calls(&self) -> Vec<EgressEnvelope> {
        self.calls.lock().await.clone()
    }
}

impl IoProtocolAdapter for ScriptedEgress {
    fn kind(&self) -> &'static str {
        "telegram.bot"
    }

    fn capabilities(&self) -> IoProtocolCapabilities {
        IoProtocolCapabilities {
            ingress: false,
            egress: true,
            streaming: false,
            durable_offsets: false,
            attachments: false,
        }
    }
}

#[async_trait]
impl EgressAdapter for ScriptedEgress {
    async fn deliver(&self, envelope: EgressEnvelope) -> IoResult<DeliveryReceipt> {
        self.calls.lock().await.push(envelope.clone());
        if let Some(error) = self.failures.lock().await.pop_front() {
            return Err(IoError::Delivery(error));
        }
        let fallback_id = {
            let calls = self.calls.lock().await;
            format!("message-{}", calls.len())
        };
        let external_id = self
            .external_ids
            .lock()
            .await
            .pop_front()
            .unwrap_or(fallback_id);
        Ok(DeliveryReceipt::delivered(&envelope, external_id))
    }
}

fn test_envelope(text: &str) -> IngressEnvelope {
    IngressEnvelope::new(
        IoSource::new("telegram.bot", "main"),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
}

fn telegram_queue_envelope(text: &str) -> IngressEnvelope {
    telegram_queue_envelope_with_update(text, "999")
}

fn telegram_queue_envelope_with_update(text: &str, update_id: &str) -> IngressEnvelope {
    let source = IoSource::new("telegram.bot", "main");
    IngressEnvelope::new(
        source.clone(),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
    .with_actor(IoActor::new("telegram:user:42"))
    .with_dedupe_key(IoDedupeKey::for_source(
        &source,
        format!("update:{update_id}"),
    ))
    .with_metadata("cooldis_route_id", "main")
    .with_metadata("cooldis_route_policy", "queue_per_conversation")
    .with_metadata("telegram_message_id", "555")
}

fn coalesce_envelope(
    text: &str,
    update_id: &str,
    window_ms: u64,
    max_batch: usize,
) -> IngressEnvelope {
    telegram_queue_envelope_with_update(text, update_id)
        .with_metadata("cooldis_route_policy", "coalesce_bursts")
        .with_metadata("cooldis_coalesce_window_ms", window_ms.to_string())
        .with_metadata("cooldis_coalesce_max_batch", max_batch.to_string())
}

fn expired_coalesce_envelope(
    text: &str,
    update_id: &str,
    window_ms: u64,
    max_batch: usize,
) -> IngressEnvelope {
    let mut envelope = coalesce_envelope(text, update_id, window_ms, max_batch);
    envelope.received_at_ms = now_ms().saturating_sub(window_ms + 10);
    envelope
}

fn steer_coalesce_envelope(
    text: &str,
    update_id: &str,
    window_ms: u64,
    max_batch: usize,
) -> IngressEnvelope {
    let mut envelope = expired_coalesce_envelope(text, update_id, window_ms, max_batch)
        .with_metadata("cooldis_route_policy", "steer_when_active");
    envelope
        .metadata
        .insert("cooldis_coalesce_bursts".to_string(), "true".to_string());
    envelope
}

#[test]
fn coalesce_group_key_does_not_collapse_colon_bearing_components() {
    let mut left = coalesce_envelope("left", "9101", 20, 10)
        .with_metadata("cooldis_route_id", "a:b")
        .with_metadata("cooldis_route_threading", "f");
    left.source = IoSource::new("c", "d");
    left.conversation = IoConversation::new("e", ConversationKind::Direct);

    let mut right = coalesce_envelope("right", "9102", 20, 10)
        .with_metadata("cooldis_route_id", "a")
        .with_metadata("cooldis_route_threading", "f");
    right.source = IoSource::new("b", "c");
    right.conversation = IoConversation::new("d:e", ConversationKind::Direct);

    assert_ne!(coalesce_group_key(&left), coalesce_group_key(&right));
}

#[test]
fn coalesce_group_key_separates_per_actor_targets() {
    let first = coalesce_envelope("first", "9201", 20, 10)
        .with_metadata("cooldis_route_threading", "per_actor")
        .with_actor(IoActor::new("telegram:user:1"));
    let second = coalesce_envelope("second", "9202", 20, 10)
        .with_metadata("cooldis_route_threading", "per_actor")
        .with_actor(IoActor::new("telegram:user:2"));

    assert_ne!(coalesce_group_key(&first), coalesce_group_key(&second));
}

#[test]
fn coalesce_messages_sort_by_received_at_before_merging() {
    let mut early = coalesce_envelope("early", "9301", 20, 10);
    early.received_at_ms = 100;
    let mut late = coalesce_envelope("late", "9302", 20, 10);
    late.received_at_ms = 200;
    let mut messages = vec![
        LeasedIngressEnvelope::new("2", late),
        LeasedIngressEnvelope::new("1", early),
    ];

    sort_coalesce_messages(&mut messages);
    let merged = merged_coalesce_envelope(&messages).unwrap();

    assert_eq!(merged.content.text_projection(), "early\nlate");
}

fn observe_only_envelope(text: &str) -> IngressEnvelope {
    telegram_queue_envelope(text).with_metadata("cooldis_route_policy", "observe_only")
}

fn test_egress(text: &str) -> EgressEnvelope {
    let mut egress = EgressEnvelope::new(
        IoTarget {
            source: IoSource::new("telegram.bot", "main"),
            conversation: IoConversation::new("telegram:chat:123", ConversationKind::Direct),
            actor: None,
            metadata: BTreeMap::new(),
        },
        EgressKind::AssistantMessage {
            text: text.to_string(),
        },
        now_ms(),
    );
    egress
        .metadata
        .insert("telegram_message_id".to_string(), "555".to_string());
    egress
}

fn route_with_egress(
    egress_projection: Vec<crate::CooldisEgressProjectionRuleConfig>,
    typing_simulation: Option<crate::CooldisTypingSimulationConfig>,
) -> CooldisIoRouteConfig {
    route_with_egress_and_retry(
        egress_projection,
        typing_simulation,
        crate::CooldisEgressRetryConfig::default(),
    )
}

fn route_with_egress_and_retry(
    egress_projection: Vec<crate::CooldisEgressProjectionRuleConfig>,
    typing_simulation: Option<crate::CooldisTypingSimulationConfig>,
    egress_retry: crate::CooldisEgressRetryConfig,
) -> CooldisIoRouteConfig {
    CooldisIoRouteConfig {
        id: "main".to_string(),
        kind: "telegram.bot".to_string(),
        enabled: true,
        policy: None,
        threading: None,
        agent_ref: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection,
        typing_simulation,
        egress_retry,
        telegram: None,
        metadata: BTreeMap::new(),
    }
}

fn test_root(name: &str) -> PathBuf {
    std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("{name}-{}", uuid::Uuid::now_v7()))
}

async fn test_bridge() -> (
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
    PathBuf,
) {
    let fixture_root = test_root("bridge");
    let (server, bridge, rx) = test_bridge_at_root(&fixture_root).await;
    let session_store_path = server.session_store_path().to_path_buf();
    (bridge, rx, session_store_path)
}

async fn test_server() -> CooldisAppServer {
    test_server_at_root(&test_root("server")).await
}

async fn test_server_at_root(fixture_root: &Path) -> CooldisAppServer {
    let socket_path = fixture_root.join("app-server.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    apply_test_identity(&mut config, fixture_root);
    CooldisAppServer::new_local(config).await.unwrap()
}

async fn test_server_with_route_provider_at_root(
    fixture_root: &Path,
    workspace: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    client: Arc<RecordingRouteProviderClient>,
) -> CooldisAppServer {
    let socket_path = fixture_root.join("app-server-recording.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    // lexicon-allow: capsule - existing app-server manifest binding config type
    let bindings = CapsuleBindingsConfig::default().with_registry_root(operation_registry_root);
    let mut config = CooldisAppServerConfig::local(listen, workspace)
        // lexicon-allow: capsule - existing app-server config method
        .with_capsule_bindings(bindings);
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    config.agent_registry_root = agent_registry_root.to_path_buf();
    config.blob_registry_root =
        crate::default_blob_registry_root_for_agent_registry_root(agent_registry_root);
    apply_test_identity(&mut config, fixture_root);
    let runtime_config = CanonicalProviderRuntimeConfig::new(
        ProviderApi::Other(APP_SERVER_LOCAL_PROVIDER.to_string()),
        APP_SERVER_LOCAL_PROVIDER,
        APP_SERVER_LOCAL_MODEL,
    );
    let provider_client: Arc<dyn ProviderClient> = client;
    let runtime_factory =
        crate::adapters::app_server::runtime_factory_from_provider_parts_with_app_paths(
            runtime_config,
            provider_client,
            // lexicon-allow: capsule - existing app-server config field
            config.capsule_bindings.clone(),
            None,
            &config,
        );
    CooldisAppServer::with_runtime_factory(config, runtime_factory)
        .await
        .unwrap()
}

async fn test_bridge_at_root(
    fixture_root: &Path,
) -> (
    CooldisAppServer,
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
) {
    let server = test_server_at_root(fixture_root).await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let (tx, rx) = mpsc::unbounded_channel();
    bridge
        .register_egress_adapter(
            "telegram.bot",
            "main",
            Arc::new(CaptureEgress { sender: tx }),
        )
        .await;
    (server, bridge, rx)
}

async fn restarted_bridge_at_root(
    fixture_root: &Path,
) -> (
    CooldisAppServer,
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
) {
    let socket_path = fixture_root.join("app-server-restarted.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    config.user_state_home = fixture_root.join("user-state");
    apply_test_identity(&mut config, fixture_root);
    let server = CooldisAppServer::new_local(config).await.unwrap();
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let (tx, rx) = mpsc::unbounded_channel();
    bridge
        .register_egress_adapter(
            "telegram.bot",
            "main",
            Arc::new(CaptureEgress { sender: tx }),
        )
        .await;
    (server, bridge, rx)
}

fn apply_test_identity(config: &mut CooldisAppServerConfig, fixture_root: &Path) {
    let suffix = fixture_root
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("daemon-io");
    config.tenant_id = format!("app-server-{suffix}");
    config.user_id = format!("local-user-{suffix}");
}

#[derive(Default)]
struct RecordingRouteProviderClient {
    requests: StdMutex<Vec<ProviderRequest>>,
}

impl RecordingRouteProviderClient {
    fn requests(&self) -> Vec<ProviderRequest> {
        self.requests.lock().unwrap().clone()
    }
}

#[async_trait]
impl ProviderClient for RecordingRouteProviderClient {
    async fn complete(&self, request: &ProviderRequest) -> ProviderResult<ProviderResponse> {
        self.requests.lock().unwrap().push(request.clone());
        Ok(ProviderResponse {
            content: vec![CanonicalContent::text("daemon route ok")],
            usage: CanonicalUsage {
                input_tokens: request.messages.len() as u64,
                output_tokens: 3,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
            },
            stop_reason: CanonicalStopReason::EndTurn,
        })
    }
}

async fn wait_for_provider_requests(client: &RecordingRouteProviderClient, count: usize) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if client.requests().len() >= count {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {count} provider request(s)"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn publish_route_test_operation(registry_root: &Path) -> crate::PublishedOperationRecord {
    std::fs::create_dir_all(registry_root).unwrap();
    let wasm = wat::parse_str(route_test_operation_guest()).unwrap();
    let artifact_path = registry_root.join("lookup.wasm");
    std::fs::write(&artifact_path, wasm).unwrap();
    crate::LocalOperationRegistry::new(registry_root)
        .publish_artifact(PublishOperationRequest {
            name: "lookup".to_string(),
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

fn publish_route_agent_manifest(
    root: &Path,
    agent_registry_root: &Path,
    operation_registry_root: &Path,
    operation_hash: &str,
) -> crate::PublishedAgentRecord {
    let project = root.join("daemon-route-runner");
    std::fs::create_dir_all(project.join("prompts")).unwrap();
    std::fs::write(
        project.join("prompts/system.md"),
        "You are the daemon route prompt runner.\n",
    )
    .unwrap();
    let manifest_path = project.join("cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "daemon-route-runner"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false

[[tools]]
type = "direct_tool"
id = "lookup"
tool_name = "lookup"
operation_ref = "op://lookup/lookup@sha256:{operation_hash}"
"#
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path_with_operation_registry(&manifest_path, operation_registry_root)
        .unwrap()
}

fn publish_route_agent_manifest_with_missing_blob(
    root: &Path,
    agent_registry_root: &Path,
) -> crate::PublishedAgentRecord {
    let project = root.join("daemon-missing-blob");
    std::fs::create_dir_all(&project).unwrap();
    let manifest_path = project.join("cooldis.agent.toml");
    std::fs::write(
        &manifest_path,
        format!(
            r#"
[agent]
name = "daemon-missing-blob"
version = "0.1.0"
kind = "cooldis.agent-manifest"
schema_version = 1

[[model_profiles]]
id = "default"
provider_ref = "provider://local_offline"
model_ref = "model://local_offline/echo"

[runtime]
default_cwd = "."
streaming = false

[[resources]]
name = "system_prompt"
kind = "blob"
ref = "resource://artifact/sha256:{}"

[context]
[[context.pipelines]]
id = "default"

[[context.pipelines.sources]]
id = "identity"
assembler = "kernel://assembler/static"
input = "system_prompt"
pinned = true
"#,
            "f".repeat(64)
        ),
    )
    .unwrap();
    LocalAgentRegistry::new(agent_registry_root)
        .publish_manifest_path(&manifest_path)
        .unwrap()
}

fn route_test_operation_guest() -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "lookup",
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
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "lookup:")
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
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 1024
                i32.store
                local.get $source
                i32.const 1024
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                i32.const 7
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 1024
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

async fn register_route_state(
    bridge: &CooldisDaemonIoBridge,
    route: &CooldisIoRouteConfig,
    db: &Path,
) {
    bridge
        .register_egress_route_config("telegram.bot", "main", route)
        .await
        .unwrap();
    bridge
        .register_egress_state_sqlite_dsn("telegram.bot", "main", sqlite_dsn(db))
        .await
        .unwrap();
}

async fn submit_and_wait_for_assistant_event(
    bridge: &CooldisDaemonIoBridge,
    text: &str,
) -> (String, String) {
    let receipt = bridge.submit_envelope(test_envelope(text)).await.unwrap();
    let thread_id = receipt.thread_id.expect("receipt should include thread id");
    let expected = format!("local:{text}");
    wait_for_assistant_text(bridge, &thread_id, &expected).await;
    (thread_id, expected)
}

async fn wait_for_assistant_text(bridge: &CooldisDaemonIoBridge, thread_id: &str, expected: &str) {
    let parsed = ThreadId::parse_str(thread_id).unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        let handle = bridge
            .supervisor
            .get_thread(&bridge.tenant_id, parsed)
            .await
            .unwrap();
        let context = handle.session_context().await.unwrap();
        if context.entries.iter().any(|entry| {
            matches!(
                assistant_text_from_entry(entry).as_deref(),
                Some(text) if text == expected
            )
        }) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for assistant text {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn egress_receipts(
    bridge: &CooldisDaemonIoBridge,
    thread_id: &str,
    kind: EventKind,
) -> Vec<crate::EventRecord> {
    let parsed = ThreadId::parse_str(thread_id).unwrap();
    let handle = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parsed)
        .await
        .unwrap();
    handle
        .read_thread_events(None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == kind)
        .collect()
}

async fn egress_cursor(bridge: &CooldisDaemonIoBridge, thread_id: &str) -> Option<StreamCursorV1> {
    bridge
        .egress_cursor_for_thread("telegram.bot", "main", thread_id)
        .await
        .unwrap()
}

async fn only_thread_coordinates(bridge: &CooldisDaemonIoBridge) -> ThreadCoordinates {
    bridge
        .threads
        .lock()
        .await
        .values()
        .next()
        .cloned()
        .expect("admission should create a target thread")
}

async fn control_events_for(
    session_store_path: &Path,
    coordinates: &ThreadCoordinates,
) -> Vec<crate::EventRecord> {
    let session_store = crate::SqliteSessionStore::open(session_store_path).unwrap();
    session_store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .unwrap()
}

async fn thread_events_for(
    session_store_path: &Path,
    coordinates: &ThreadCoordinates,
) -> Vec<crate::EventRecord> {
    let session_store = crate::SqliteSessionStore::open(session_store_path).unwrap();
    session_store
        .read_events(&crate::EventStreamId::for_thread(coordinates), None)
        .await
        .unwrap()
}

async fn user_texts_for(
    bridge: &CooldisDaemonIoBridge,
    coordinates: &ThreadCoordinates,
) -> Vec<String> {
    let handle = bridge.supervisor.get_thread_at(coordinates).await.unwrap();
    handle
        .session_context()
        .await
        .unwrap()
        .entries
        .iter()
        .filter_map(|entry| match &entry.kind {
            SessionEntryKind::Message {
                message: CanonicalMessage::User { content, .. },
            }
            | SessionEntryKind::CustomContextMessage {
                message: CanonicalMessage::User { content, .. },
            } => Some(text_from_canonical_content(content)),
            _ => None,
        })
        .collect()
}

async fn wait_for_user_text(
    bridge: &CooldisDaemonIoBridge,
    coordinates: &ThreadCoordinates,
    expected: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if user_texts_for(bridge, coordinates)
            .await
            .contains(&expected.to_string())
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for user text {expected:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn admission_source_ids(event: &crate::EventRecord) -> Vec<String> {
    event.payload["source_ingress_event_ids"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap().to_string())
        .collect()
}

async fn drain_until_egress(
    bridge: &CooldisDaemonIoBridge,
    protocol: &str,
    instance_id: &str,
    expected: usize,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    let mut drained = 0;
    loop {
        drained += bridge
            .drain_egress_once(protocol, instance_id)
            .await
            .unwrap();
        if drained >= expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {expected} egress source(s), drained {drained}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test]
async fn egress_projection_strips_reaction_tag_and_preserves_order() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[reaction:(?P<emoji>[^\]]+)\]".to_string(),
            action: "reaction".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("hello[reaction:👍] friend"));

    assert_eq!(projected.len(), 2);
    assert!(matches!(
        projected[0].kind,
        EgressKind::AssistantMessage { ref text } if text == "hello friend"
    ));
    assert!(matches!(
        projected[1].kind,
        EgressKind::PlatformAction { ref action, ref payload }
            if action == "reaction"
                && payload["emoji"] == "👍"
                && payload["message_id"].is_null()
    ));
}

#[tokio::test]
async fn egress_projection_turns_no_response_tag_into_silence() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[no_response\]".to_string(),
            action: "silence".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("[no_response]"));

    assert_eq!(projected.len(), 1);
    assert!(matches!(
        projected[0].kind,
        EgressKind::Silence { reason: None }
    ));
}

#[tokio::test]
async fn egress_projection_leaves_text_without_tags_unchanged() {
    let route = route_with_egress(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"\[reaction:(?P<emoji>[^\]]+)\]".to_string(),
            action: "reaction".to_string(),
        }],
        None,
    );
    let config = RouteEgressConfig::from_route(&route).unwrap();

    let projected = config.project(test_egress("plain answer"));

    assert_eq!(projected.len(), 1);
    assert!(matches!(
        projected[0].kind,
        EgressKind::AssistantMessage { ref text } if text == "plain answer"
    ));
}

#[tokio::test(start_paused = true)]
async fn typing_simulation_sends_typing_action_and_delays_text() {
    assert_eq!(typing_delay_for_text("abcd", 2), Duration::from_secs(2));
    assert_eq!(
        typing_delay_for_text("abcdefghi", 1),
        Duration::from_secs(8)
    );

    let (bridge, mut rx, _) = test_bridge().await;
    let route = route_with_egress(
        Vec::new(),
        Some(crate::CooldisTypingSimulationConfig {
            chars_per_second: 2,
        }),
    );
    bridge
        .register_egress_route_config("telegram.bot", "main", &route)
        .await
        .unwrap();

    let deliver = tokio::spawn({
        let bridge = bridge.clone();
        async move {
            bridge.deliver_egress(test_egress("abcd")).await;
        }
    });

    let typing = rx.recv().await.unwrap();
    assert!(matches!(
        typing.kind,
        EgressKind::PlatformAction { ref action, .. } if action == "typing"
    ));
    assert!(rx.try_recv().is_err());

    tokio::time::advance(Duration::from_millis(1_999)).await;
    tokio::task::yield_now().await;
    assert!(rx.try_recv().is_err());

    tokio::time::advance(Duration::from_millis(1)).await;
    let text = rx.recv().await.unwrap();
    assert!(matches!(
        text.kind,
        EgressKind::AssistantMessage { ref text } if text == "abcd"
    ));
    deliver.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn typing_simulation_is_off_by_default() {
    let (bridge, mut rx, _) = test_bridge().await;

    bridge.deliver_egress(test_egress("no typing")).await;

    let text = rx.recv().await.unwrap();
    assert!(matches!(
        text.kind,
        EgressKind::AssistantMessage { ref text } if text == "no typing"
    ));
    assert!(rx.try_recv().is_err());
}

#[derive(Clone)]
struct FakeClock {
    now: Arc<StdMutex<DateTime<Utc>>>,
}

impl FakeClock {
    fn new(now: DateTime<Utc>) -> Self {
        Self {
            now: Arc::new(StdMutex::new(now)),
        }
    }

    fn set(&self, now: DateTime<Utc>) {
        *self.now.lock().unwrap() = now;
    }
}

impl DaemonClock for FakeClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap()
    }
}

async fn start_clock_thread_with_mandate(
    server: &CooldisAppServer,
    catch_up: MandateCatchUpPolicy,
) -> (
    SqliteSessionStore,
    ThreadCoordinates,
    crate::MandateStartReceipt,
) {
    let handle = server
        .supervisor()
        .start_thread(ThreadStartRequest {
            tenant_id: server.tenant_id().to_string(),
            user_id: server.user_id().to_string(),
            session_id: format!("clock-{}", uuid::Uuid::now_v7()),
            topology: ThreadTopology::root(),
            metadata: BTreeMap::new(),
        })
        .await
        .unwrap();
    let coordinates = handle.context().coordinates.clone();
    let store = SqliteSessionStore::open(server.session_store_path()).unwrap();
    let receipt = start_mandate(
        &store,
        &coordinates,
        MandateStartRequest {
            schedule: MandateSchedulePayload::Interval { every_ms: 60_000 },
            max_occurrences: Some(3),
            catch_up: Some(catch_up),
            input_template: Some("wake".to_string()),
            snapshot_id: None,
        },
        Utc::now(),
    )
    .await
    .unwrap();
    (store, coordinates, receipt)
}

fn event_time(event_ms: i64, offset_ms: i64) -> DateTime<Utc> {
    Utc.timestamp_millis_opt(event_ms + offset_ms)
        .single()
        .unwrap()
}

async fn timer_payloads(
    store: &SqliteSessionStore,
    coordinates: &ThreadCoordinates,
) -> Vec<TimerFiredPayload> {
    store
        .read_events(&control_stream_id(coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == EventKind::TimerFired)
        .map(|event| serde_json::from_value(event.payload).unwrap())
        .collect()
}

#[tokio::test]
async fn direct_sink_submits_ingress_to_runtime_and_emits_egress() {
    let (bridge, mut rx, _) = test_bridge().await;
    let db = test_root("direct-egress").join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &db).await;

    let ack = bridge
        .direct_sink()
        .submit(test_envelope("hello direct"))
        .await
        .unwrap();

    assert!(ack.accepted);
    drain_until_egress(&bridge, "telegram.bot", "main", 1).await;
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text.contains("hello direct")
    ));
}

#[tokio::test]
async fn route_agent_ref_binds_manifest_prompt_metadata_and_receipts() {
    let root = test_root("route-agent-binding");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_route_test_operation(&operation_registry_root).await;
    let agent_registry_root = root.join("agents");
    let agent = publish_route_agent_manifest(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &operation.active_artifact_hash,
    );
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client.clone(),
    )
    .await;
    let session_store_path = server.session_store_path().to_path_buf();
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://daemon-route-runner@latest".to_string());
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    sink.submit(test_envelope("hello route")).await.unwrap();

    wait_for_provider_requests(&client, 1).await;
    let requests = client.requests();
    assert_eq!(
        requests[0].system[0].text,
        "You are the daemon route prompt runner.\n"
    );
    assert!(
        requests[0].system[1]
            .text
            .contains("You are running as agent://daemon-route-runner@0.1.0"),
        "{:?}",
        requests[0].system
    );

    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    let metadata = &handle.context().metadata;
    assert_eq!(
        metadata.get("cooldis.agent.ref_uri").map(String::as_str),
        Some("agent://daemon-route-runner@0.1.0")
    );
    assert_eq!(
        metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(String::as_str),
        Some(agent.manifest_hash.as_str())
    );
    let static_segments = metadata
        .get(THREAD_AGENT_STATIC_CONTEXT_SEGMENTS_METADATA)
        .expect("static context segment metadata should be stamped");
    let static_segments: Vec<serde_json::Value> = serde_json::from_str(static_segments).unwrap();
    assert_eq!(static_segments[0]["id"].as_str(), Some("identity"));
    assert!(
        static_segments[0]["content_sha256"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );

    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert!(
        thread_events
            .iter()
            .any(|event| event.kind == EventKind::ManifestBindCompleted)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_without_agent_ref_stays_unbound() {
    let root = test_root("route-without-agent-ref");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client.clone(),
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let route = route_with_egress(Vec::new(), None);
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    sink.submit(test_envelope("hello unbound")).await.unwrap();

    wait_for_provider_requests(&client, 1).await;
    assert!(client.requests()[0].system.is_empty());
    let coordinates = only_thread_coordinates(&bridge).await;
    let handle = bridge.supervisor.get_thread_at(&coordinates).await.unwrap();
    assert!(
        !handle
            .context()
            .metadata
            .contains_key(THREAD_AGENT_MANIFEST_HASH_METADATA)
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_agent_ref_unknown_fails_with_registry_publish_hint() {
    let root = test_root("route-agent-missing");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client,
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://missing-route-agent@latest".to_string());

    let err = bridge.validate_route_agent_ref(&route).await.unwrap_err();
    let message = err.to_string();
    assert!(message.contains("io.routes.main.agent_ref"));
    assert!(message.contains(&agent_registry_root.display().to_string()));
    assert!(message.contains("cooldis agent publish"));
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn route_agent_ref_missing_blob_fails_startup_validation() {
    let root = test_root("route-agent-missing-blob");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let agent_registry_root = root.join("agents");
    publish_route_agent_manifest_with_missing_blob(&root, &agent_registry_root);
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client,
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.agent_ref = Some("agent://daemon-missing-blob@latest".to_string());

    let err = bridge.validate_route_agent_ref(&route).await.unwrap_err();
    let message = err.to_string();
    assert!(message.contains("io.routes.main.agent_ref"), "{message}");
    assert!(message.contains("did not bind"), "{message}");
    assert!(
        message.contains("blob resource \"system_prompt\""),
        "{message}"
    );
    assert!(message.contains("cooldis blob publish"), "{message}");
    assert!(bridge.threads.lock().await.is_empty());
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn fork_on_new_dm_child_inherits_route_agent_binding() {
    let root = test_root("route-agent-fork");
    let workspace = root.join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let operation_registry_root = root.join("operations");
    let operation = publish_route_test_operation(&operation_registry_root).await;
    let agent_registry_root = root.join("agents");
    let agent = publish_route_agent_manifest(
        &root,
        &agent_registry_root,
        &operation_registry_root,
        &operation.active_artifact_hash,
    );
    let client = Arc::new(RecordingRouteProviderClient::default());
    let server = test_server_with_route_provider_at_root(
        &root,
        &workspace,
        &agent_registry_root,
        &operation_registry_root,
        client.clone(),
    )
    .await;
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let mut route = route_with_egress(Vec::new(), None);
    route.policy = Some("fork_on_new_dm".to_string());
    route.agent_ref = Some("agent://daemon-route-runner@latest".to_string());
    let sink = RouteIngressSink::new(bridge.direct_sink(), &route);

    sink.submit(test_envelope("fork route")).await.unwrap();

    wait_for_provider_requests(&client, 1).await;
    let child_coordinates = only_thread_coordinates(&bridge).await;
    let child = bridge
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    assert_eq!(
        child
            .context()
            .metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(String::as_str),
        Some(agent.manifest_hash.as_str())
    );
    let parent_thread_id = child
        .context()
        .parent_thread_id
        .expect("fork child should reference parent");
    let parent = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parent_thread_id)
        .await
        .unwrap();
    assert_eq!(
        parent
            .context()
            .metadata
            .get(THREAD_AGENT_MANIFEST_HASH_METADATA)
            .map(String::as_str),
        Some(agent.manifest_hash.as_str())
    );
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn queue_worker_processes_sqlite_backed_envelope() {
    let (bridge, mut rx, _) = test_bridge().await;
    let egress_db = test_root("queue-egress").join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("queue-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue.submit(test_envelope("hello queue")).await.unwrap();

    let worker = CooldisDaemonQueueWorker::new(queue, bridge, "worker-test", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    drain_until_egress(&worker.bridge, "telegram.bot", "main", 1).await;

    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text.contains("hello queue")
    ));
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_releases_invalid_coalesce_metadata_for_retry() {
    let (bridge, _rx, _) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-invalid-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    let mut envelope = coalesce_envelope("bad", "9401", 20, 10);
    envelope.metadata.insert(
        "cooldis_coalesce_max_batch".to_string(),
        "not-a-number".to_string(),
    );
    queue.submit(envelope).await.unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-invalid", 30);
    let err = worker.drain_once().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("invalid coalesce_bursts max_batch")
    );

    let leased = queue
        .lease_ingress("worker-coalesce-invalid-retry", 1, 30)
        .await
        .unwrap();
    assert_eq!(leased.len(), 1);
    assert!(leased[0].attempt > 1);
    queue.complete_ingress(&leased[0].message_id).await.unwrap();
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_coalesces_window_expired_batch_into_one_turn_and_source_list() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("queue-coalesce-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(expired_coalesce_envelope("one", "1001", 20, 10))
        .await
        .unwrap();
    queue
        .submit(expired_coalesce_envelope("two", "1002", 20, 10))
        .await
        .unwrap();
    queue
        .submit(expired_coalesce_envelope("three", "1003", 20, 10))
        .await
        .unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 3);

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let ingress_events: Vec<_> = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
        .collect();
    let admission_events: Vec<_> = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .collect();
    assert_eq!(ingress_events.len(), 3);
    assert_eq!(admission_events.len(), 1);
    assert_eq!(
        admission_events[0].payload["decision"].as_str(),
        Some("coalesce")
    );
    assert_eq!(
        admission_source_ids(admission_events[0]),
        ingress_events
            .iter()
            .map(|event| event.id.to_string())
            .collect::<Vec<_>>()
    );

    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
            .count(),
        1
    );
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        1
    );
    assert!(
        user_texts_for(&bridge, &coordinates)
            .await
            .contains(&"one\ntwo\nthree".to_string())
    );
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_flushes_coalesce_batch_when_max_batch_is_reached() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-max-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(coalesce_envelope("first", "2001", 60_000, 2))
        .await
        .unwrap();
    queue
        .submit(coalesce_envelope("second", "2002", 60_000, 2))
        .await
        .unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-max", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 2);

    let coordinates = only_thread_coordinates(&bridge).await;
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let admission = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert_eq!(admission.payload["decision"].as_str(), Some("coalesce"));
    assert_eq!(admission_source_ids(admission).len(), 2);
    assert!(
        user_texts_for(&bridge, &coordinates)
            .await
            .contains(&"first\nsecond".to_string())
    );
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn queue_worker_recovers_held_coalesce_batch_after_restart() {
    let fixture_root = test_root("queue-coalesce-restart");
    let db = fixture_root.join("queue.sqlite");
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(coalesce_envelope("before", "3001", 1_000, 10))
        .await
        .unwrap();
    queue
        .submit(coalesce_envelope("restart", "3002", 1_000, 10))
        .await
        .unwrap();

    let (_server, bridge, _rx) = test_bridge_at_root(&fixture_root).await;
    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-hold", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 2);
    assert!(
        bridge.threads.lock().await.is_empty(),
        "held coalesce batches should not submit before the window expires"
    );
    drop(worker);
    drop(queue);

    tokio::time::sleep(Duration::from_millis(1_100)).await;

    let (restarted_server, restarted_bridge, _rx) = restarted_bridge_at_root(&fixture_root).await;
    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    let worker = CooldisDaemonQueueWorker::new(
        reopened.clone(),
        restarted_bridge.clone(),
        "worker-coalesce-restart",
        30,
    );
    assert_eq!(worker.drain_once().await.unwrap(), 2);

    let coordinates = only_thread_coordinates(&restarted_bridge).await;
    let control_events =
        control_events_for(restarted_server.session_store_path(), &coordinates).await;
    let admission = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert_eq!(admission.payload["decision"].as_str(), Some("coalesce"));
    assert_eq!(admission_source_ids(admission).len(), 2);
    assert!(
        user_texts_for(&restarted_bridge, &coordinates)
            .await
            .contains(&"before\nrestart".to_string())
    );
    let _ = std::fs::remove_file(db);
    let _ = std::fs::remove_dir_all(fixture_root);
}

#[tokio::test]
async fn coalesce_composes_with_steer_when_active_as_one_merged_turn() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let active = bridge
        .submit_envelope(telegram_queue_envelope_with_update("active", "4000"))
        .await
        .unwrap();
    let thread_id = active.thread_id.as_deref().unwrap();
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!(
            "queue-coalesce-steer-{}.sqlite",
            uuid::Uuid::now_v7()
        ));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(steer_coalesce_envelope("steer one", "4001", 20, 10))
        .await
        .unwrap();
    queue
        .submit(steer_coalesce_envelope("steer two", "4002", 20, 10))
        .await
        .unwrap();

    let worker =
        CooldisDaemonQueueWorker::new(queue.clone(), bridge.clone(), "worker-coalesce-steer", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 2);

    let coordinates = bridge
        .threads
        .lock()
        .await
        .values()
        .find(|coordinates| coordinates.thread_id.to_string() == thread_id)
        .cloned()
        .unwrap();
    let control_events = control_events_for(&session_store_path, &coordinates).await;
    let latest_admission = control_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .last()
        .unwrap();
    assert_eq!(
        latest_admission.payload["decision"].as_str(),
        Some("coalesce")
    );
    assert!(
        latest_admission.payload["admissible"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value.as_str() == Some("steer"))
    );
    let thread_events = thread_events_for(&session_store_path, &coordinates).await;
    assert_eq!(
        thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
            .count(),
        2
    );
    let latest_ingress_context = thread_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
        .last()
        .unwrap();
    assert_eq!(
        latest_ingress_context.payload["ingress_metadata"]["cooldis_coalesced_batch_size"].as_str(),
        Some("2")
    );
    assert_eq!(admission_source_ids(latest_admission).len(), 2);
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn fork_on_new_dm_invokes_thread_fork_and_witnesses_spawn_lineage() {
    let (bridge, _rx, session_store_path) = test_bridge().await;
    let receipt = bridge
        .submit_envelope(
            telegram_queue_envelope_with_update("fork me", "5001")
                .with_metadata("cooldis_route_policy", "fork_on_new_dm"),
        )
        .await
        .unwrap();
    assert!(receipt.thread_id.is_some());

    let child_thread_id = receipt.thread_id.as_deref().unwrap();
    let child_coordinates = bridge
        .threads
        .lock()
        .await
        .values()
        .find(|coordinates| coordinates.thread_id.to_string() == child_thread_id)
        .cloned()
        .expect("fork admission should bind the route to the child thread");
    let child_thread_events = thread_events_for(&session_store_path, &child_coordinates).await;
    assert_eq!(
        child_thread_events
            .iter()
            .filter(|event| event.kind == crate::EventKind::IoIngressReceived)
            .count(),
        1
    );
    wait_for_user_text(&bridge, &child_coordinates, "fork me").await;

    let session_store = crate::SqliteSessionStore::open(&session_store_path).unwrap();
    let child_handle = bridge
        .supervisor
        .get_thread_at(&child_coordinates)
        .await
        .unwrap();
    let parent_thread_id = child_handle
        .context()
        .parent_thread_id
        .expect("fork child should record parent thread id");
    let parent_coordinates = ThreadCoordinates {
        tenant_id: child_coordinates.tenant_id.clone(),
        user_id: child_coordinates.user_id.clone(),
        session_id: child_coordinates.session_id.clone(),
        thread_id: parent_thread_id,
    };
    let spawned_events = session_store
        .read_events(&control_stream_id(&parent_coordinates), None)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == crate::EventKind::ThreadSpawned)
        .collect::<Vec<_>>();
    assert_eq!(spawned_events.len(), 1);
    let spawned = &spawned_events[0];
    let spawned_payload: ThreadSpawnedPayload =
        serde_json::from_value(spawned.payload.clone()).unwrap();
    assert_eq!(
        spawned.payload["child_thread_id"].as_str(),
        Some(child_thread_id)
    );
    assert_eq!(
        spawned_payload.child_thread_id.to_string(),
        child_thread_id.to_string()
    );
    let fork = spawned_payload
        .fork
        .expect("thread.spawned fork provenance should be typed");
    assert_eq!(fork.mode, "clone");
    assert_eq!(fork.source_cut.thread_id, spawned_payload.parent_thread_id);
    assert_eq!(spawned.payload["fork"]["mode"].as_str(), Some("clone"));
    assert_eq!(
        spawned.payload["fork"]["sourceCut"]["threadId"].as_str(),
        spawned.payload["parent_thread_id"].as_str()
    );
    assert!(
        spawned.payload["inputs_hash"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
}

#[tokio::test]
async fn queue_worker_processes_envelope_after_queue_and_bridge_restart() {
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("queue-restart-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );
    queue
        .submit(telegram_queue_envelope("hello after restart"))
        .await
        .unwrap();
    drop(queue);

    let (bridge, mut rx, session_store_path) = test_bridge().await;
    let egress_db = test_root("queue-restart-egress").join("io.sqlite");
    register_route_state(&bridge, &route_with_egress(Vec::new(), None), &egress_db).await;
    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );

    let worker =
        CooldisDaemonQueueWorker::new(reopened.clone(), bridge.clone(), "worker-restart", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    drain_until_egress(&worker.bridge, "telegram.bot", "main", 1).await;

    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text.contains("hello after restart")
    ));

    let coordinates = bridge
        .threads
        .lock()
        .await
        .values()
        .next()
        .cloned()
        .expect("queue admission should create a target thread");
    let session_store = crate::SqliteSessionStore::open(&session_store_path).unwrap();
    let control_stream = crate::EventStreamId::new(format!("control:{}", coordinates.thread_id));
    let thread_stream = crate::EventStreamId::for_thread(&coordinates);
    let control_events = session_store
        .read_events(&control_stream, None)
        .await
        .unwrap();
    let ingress_pos = control_events
        .iter()
        .position(|event| event.kind == crate::EventKind::IoIngressReceived)
        .unwrap();
    let admission_pos = control_events
        .iter()
        .position(|event| event.kind == crate::EventKind::AdmissionDecided)
        .unwrap();
    assert!(ingress_pos < admission_pos);
    assert_eq!(
        control_events[ingress_pos].payload["route_id"].as_str(),
        Some("main")
    );
    assert_eq!(
        control_events[ingress_pos].payload["dedupe_key"].as_str(),
        Some("telegram.bot:main:update:999")
    );
    assert_eq!(
        control_events[ingress_pos].payload["external_conversation_id"].as_str(),
        Some("telegram:chat:123")
    );
    assert_eq!(
        control_events[ingress_pos].payload["external_actor_id"].as_str(),
        Some("telegram:user:42")
    );
    assert_eq!(
        control_events[ingress_pos].payload["external_message_id"].as_str(),
        Some("555")
    );
    assert!(
        control_events[ingress_pos].payload["envelope_digest"]
            .as_str()
            .is_some_and(|hash| hash.starts_with("sha256:"))
    );
    let policy_bound = control_events
        .iter()
        .find(|event| event.kind == crate::EventKind::PolicyBound)
        .unwrap();
    assert_eq!(
        control_events[admission_pos].payload["decision"].as_str(),
        Some("queue")
    );
    assert_eq!(
        control_events[admission_pos].payload["policy_hash"],
        policy_bound.payload["content_hash"]
    );
    assert!(
        control_events[admission_pos].payload["admissible"]
            .as_array()
            .is_some_and(|admissible| !admissible.is_empty())
    );
    assert_eq!(
        control_events[admission_pos].payload["source_ingress_event_ids"][0].as_str(),
        Some(control_events[ingress_pos].id.to_string().as_str())
    );
    let thread_events = session_store
        .read_events(&thread_stream, None)
        .await
        .unwrap();
    let turn_submitted_count = thread_events
        .iter()
        .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
        .count();
    assert_eq!(turn_submitted_count, 1);

    let observe_source = IoSource::new("telegram.bot", "main");
    reopened
        .submit(
            observe_only_envelope("observe after restart")
                .with_dedupe_key(IoDedupeKey::for_source(&observe_source, "update:1000")),
        )
        .await
        .unwrap();
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    let control_events_after = session_store
        .read_events(&control_stream, None)
        .await
        .unwrap();
    let observe_admission = control_events_after
        .iter()
        .filter(|event| event.kind == crate::EventKind::AdmissionDecided)
        .last()
        .unwrap();
    assert_eq!(
        observe_admission.payload["decision"].as_str(),
        Some("observe")
    );
    assert!(
        observe_admission.payload["admissible"]
            .as_array()
            .is_some_and(|admissible| !admissible.is_empty())
    );
    let thread_events_after = session_store
        .read_events(&thread_stream, None)
        .await
        .unwrap();
    assert_eq!(
        thread_events_after
            .iter()
            .filter(|event| event.kind == crate::EventKind::TurnSubmitted)
            .count(),
        turn_submitted_count
    );
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_restarts_after_due_coalesces_one_missed_tick() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-coalesce-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());

    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);

    let fired = timer_payloads(&store, &coordinates).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired[0].occurrence_index, 0);
    assert!(fired[0].catch_up);
    assert_eq!(route.enqueue_due_once().await.unwrap(), 0);
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_restarts_after_due_skips_missed_until_next_occurrence() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::SkipMissed).await;
    let after_first_due = event_time(mandate.event.created_at_ms, 90_000);
    let second_due = event_time(mandate.event.created_at_ms, 120_000);
    let clock = Arc::new(FakeClock::new(after_first_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-skip-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());

    assert_eq!(route.enqueue_due_once().await.unwrap(), 0);
    assert!(timer_payloads(&store, &coordinates).await.is_empty());
    clock.set(second_due);
    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);

    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    let fired = timer_payloads(&store, &coordinates).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired[0].occurrence_index, 1);
    assert!(!fired[0].catch_up);
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_duplicate_enqueue_before_ack_does_not_double_fire() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-dedupe-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());
    assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
    drop(route);
    drop(queue);

    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let restarted_route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), reopened.clone(), clock.clone());
    assert_eq!(restarted_route.enqueue_due_once().await.unwrap(), 0);

    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(reopened.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);
    assert_eq!(worker.drain_once().await.unwrap(), 0);

    let fired = timer_payloads(&store, &coordinates).await;
    assert_eq!(fired.len(), 1);
    assert_eq!(fired[0].mandate_event_id, mandate.event.id);
    assert_eq!(fired[0].occurrence_index, 0);
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn clock_route_revoke_prevents_further_ticks() {
    let server = test_server().await;
    let (store, coordinates, mandate) =
        start_clock_thread_with_mandate(&server, MandateCatchUpPolicy::CoalesceMissed).await;
    revoke_mandate(&store, &coordinates, mandate.event.id)
        .await
        .unwrap();
    let after_due = event_time(mandate.event.created_at_ms, 90_000);
    let clock = Arc::new(FakeClock::new(after_due));
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("clock-revoke-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "clock"))
            .await
            .unwrap(),
    );
    let route =
        CooldisDaemonClockRoute::new("clock-main", store.clone(), queue.clone(), clock.clone());

    assert_eq!(route.enqueue_due_once().await.unwrap(), 0);
    let bridge = CooldisDaemonIoBridge::from_app_server(&server);
    let worker = CooldisDaemonQueueWorker::new(queue.clone(), bridge, "clock-worker", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 0);
    assert!(timer_payloads(&store, &coordinates).await.is_empty());
    let _ = std::fs::remove_file(db);
}

#[tokio::test]
async fn egress_projector_delivers_after_bridge_restart_from_persisted_cursor() {
    let root = test_root("egress-restart");
    let db = root.join("io.sqlite");
    let route = route_with_egress(Vec::new(), None);

    let (server, bridge, mut first_rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "after restart").await;
    assert!(first_rx.try_recv().is_err());
    drop(bridge);
    drop(server);

    let (_restarted_server, restarted, mut rx) = restarted_bridge_at_root(&root).await;
    register_route_state(&restarted, &route, &db).await;

    assert_eq!(
        restarted
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text == "local:after restart"
    ));
    assert_eq!(
        restarted
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        0
    );

    let delivered = egress_receipts(&restarted, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0]
            .payload
            .get("external_message_id")
            .and_then(serde_json::Value::as_str),
        Some("capture")
    );
    assert!(egress_cursor(&restarted, &thread_id).await.is_some());
}

#[tokio::test]
async fn egress_projector_recovers_missing_projection_after_partial_receipt_cursor() {
    let root = test_root("egress-partial-projection-cursor");
    let db = root.join("io.sqlite");
    let route = route_with_egress(
        Vec::new(),
        Some(crate::CooldisTypingSimulationConfig {
            chars_per_second: 0,
        }),
    );

    let (_server, bridge, mut rx) = test_bridge_at_root(&root).await;
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, expected) =
        submit_and_wait_for_assistant_event(&bridge, "partial cursor").await;

    let parsed = ThreadId::parse_str(&thread_id).unwrap();
    let handle = bridge
        .supervisor
        .get_thread(&bridge.tenant_id, parsed)
        .await
        .unwrap();
    let context = handle.session_context().await.unwrap();
    let events = handle.read_thread_events(None).await.unwrap();
    let source_event = events
        .iter()
        .find(|event| {
            assistant_text_from_session_event(event, &context.entries).as_deref()
                == Some(expected.as_str())
        })
        .unwrap()
        .clone();
    let source_context = events.iter().find_map(ingress_context_from_event).unwrap();
    let mut source_envelope = EgressEnvelope::new(
        source_context.target,
        EgressKind::AssistantMessage {
            text: expected.clone(),
        },
        now_ms(),
    );
    source_envelope.source_ingress_id = source_context.source_ingress_id;
    source_envelope.metadata = source_context.metadata;
    let typing_envelope = sibling_egress(
        &source_envelope,
        EgressKind::PlatformAction {
            action: "typing".to_string(),
            payload: JsonValue::Object(JsonMap::new()),
        },
    );
    let binding = BoundEgressThread {
        route_id: "main".to_string(),
        scope_key: "test-scope".to_string(),
        coordinates: handle.context().coordinates.clone(),
    };
    let partial_dedupe_key = egress_dedupe_key(source_event.id, 0);
    let partial_receipt = append_egress_delivered_receipt(
        &handle,
        &binding,
        &source_event,
        0,
        &partial_dedupe_key,
        &typing_envelope,
        &DeliveryReceipt::delivered(&typing_envelope, "typing-before-crash"),
        1,
    )
    .await
    .unwrap();
    let state = bridge
        .egress_states
        .read()
        .await
        .get(&source_scope("telegram.bot", "main"))
        .cloned()
        .unwrap();
    state
        .store_cursor("main", &thread_id, &partial_receipt.cursor_v1())
        .unwrap();

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
    let egress = tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        egress.kind,
        EgressKind::AssistantMessage { ref text } if text == &expected
    ));
    assert!(rx.try_recv().is_err());

    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 2);
    let assistant_dedupe_key = egress_dedupe_key(source_event.id, 1);
    assert!(delivered.iter().any(|event| {
        event.payload["dedupe_key"].as_str() == Some(partial_dedupe_key.as_str())
    }));
    assert!(delivered.iter().any(|event| {
        event.payload["dedupe_key"].as_str() == Some(assistant_dedupe_key.as_str())
    }));
    let cursor = egress_cursor(&bridge, &thread_id).await.unwrap();
    assert!(cursor.sequence.get() > partial_receipt.sequence.get());
}

#[tokio::test]
async fn egress_projector_retries_transient_failures_and_records_attempts() {
    let root = test_root("egress-retry");
    let db = root.join("io.sqlite");
    let adapter = Arc::new(ScriptedEgress::new(
        ["telegram 500", "telegram 500"],
        &["telegram-message-3"],
    ));
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    bridge
        .register_egress_adapter("telegram.bot", "main", adapter.clone())
        .await;
    let route = route_with_egress_and_retry(
        Vec::new(),
        None,
        crate::CooldisEgressRetryConfig {
            max_attempts: 5,
            base_backoff_ms: 0,
        },
    );
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "retry me").await;

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );

    assert_eq!(adapter.calls().await.len(), 3);
    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].payload["attempts"].as_u64(), Some(3));
    assert_eq!(
        delivered[0].payload["external_message_id"].as_str(),
        Some("telegram-message-3")
    );
}

#[tokio::test]
async fn egress_projector_dead_letters_after_max_attempts() {
    let root = test_root("egress-dead-letter");
    let db = root.join("io.sqlite");
    let adapter = Arc::new(ScriptedEgress::new(
        ["telegram 500", "telegram 500", "telegram 500"],
        &[],
    ));
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    bridge
        .register_egress_adapter("telegram.bot", "main", adapter.clone())
        .await;
    let route = route_with_egress_and_retry(
        Vec::new(),
        None,
        crate::CooldisEgressRetryConfig {
            max_attempts: 3,
            base_backoff_ms: 0,
        },
    );
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "dead letter").await;

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );

    assert_eq!(adapter.calls().await.len(), 3);
    let failed = egress_receipts(&bridge, &thread_id, EventKind::IoEgressFailed).await;
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].payload["attempts"].as_u64(), Some(3));
    assert_eq!(failed[0].payload["dead_lettered"].as_bool(), Some(true));
    assert_eq!(
        bridge
            .egress_dead_letter_count("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn egress_projector_witnesses_silence_without_wire_call() {
    let root = test_root("egress-silence");
    let db = root.join("io.sqlite");
    let adapter = Arc::new(ScriptedEgress::new(std::iter::empty::<&str>(), &[]));
    let (_server, bridge, _rx) = test_bridge_at_root(&root).await;
    bridge
        .register_egress_adapter("telegram.bot", "main", adapter.clone())
        .await;
    let route = route_with_egress_and_retry(
        vec![crate::CooldisEgressProjectionRuleConfig {
            pattern: r"local:\[no_response\]".to_string(),
            action: "silence".to_string(),
        }],
        None,
        crate::CooldisEgressRetryConfig {
            max_attempts: 5,
            base_backoff_ms: 0,
        },
    );
    register_route_state(&bridge, &route, &db).await;
    let (thread_id, _) = submit_and_wait_for_assistant_event(&bridge, "[no_response]").await;

    assert_eq!(
        bridge
            .drain_egress_once("telegram.bot", "main")
            .await
            .unwrap(),
        1
    );

    assert!(adapter.calls().await.is_empty());
    let delivered = egress_receipts(&bridge, &thread_id, EventKind::IoEgressDelivered).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(
        delivered[0].payload["egress_kind"].as_str(),
        Some("silence")
    );
    assert_eq!(delivered[0].payload["attempts"].as_u64(), Some(1));
}

#[tokio::test]
async fn telegram_webhook_accepts_update_and_uses_sink() {
    let envelopes = Arc::new(TokioMutex::new(Vec::new()));
    let sink = Arc::new(CaptureSink {
        envelopes: envelopes.clone(),
    });
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: "main".to_string(),
            listen: "127.0.0.1:0".to_string(),
            path: "/telegram".to_string(),
            secret_token: Some("secret".to_string()),
        },
        sink,
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.serve());

    let response = post_json(
        addr,
        "/telegram",
        Some("secret"),
        json!({
            "update_id": 999,
            "message": {
                "message_id": 555,
                "chat": { "id": 123, "type": "private" },
                "date": 1777000000,
                "text": "hello webhook"
            }
        }),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let captured = envelopes.lock().await;
    assert_eq!(captured.len(), 1);
    assert_eq!(captured[0].content.text_projection(), "hello webhook");
}

#[tokio::test]
async fn telegram_webhook_queue_mode_writes_to_sqlite() {
    let db = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(format!("telegram-{}.sqlite", uuid::Uuid::now_v7()));
    let queue = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "telegram"))
            .await
            .unwrap(),
    );
    let server = TelegramWebhookServer::bind(
        TelegramWebhookServerConfig {
            route_id: "main".to_string(),
            listen: "127.0.0.1:0".to_string(),
            path: "/telegram".to_string(),
            secret_token: None,
        },
        queue.clone(),
    )
    .await
    .unwrap();
    let addr = server.local_addr().unwrap();
    tokio::spawn(server.serve());

    let response = post_json(
        addr,
        "/telegram",
        None,
        json!({
            "update_id": 1000,
            "message": {
                "message_id": 556,
                "chat": { "id": 456, "type": "private" },
                "date": 1777000000,
                "text": "queued webhook"
            }
        }),
    )
    .await;

    assert!(response.starts_with("HTTP/1.1 200 OK"));
    let leased = queue.lease_default("test", 1).await.unwrap();
    assert_eq!(leased.len(), 1);
    assert_eq!(
        leased[0].envelope.content.text_projection(),
        "queued webhook"
    );
    queue.complete_ingress(&leased[0].message_id).await.unwrap();
    let _ = std::fs::remove_file(db);
}

async fn post_json(
    addr: SocketAddr,
    path: &str,
    secret: Option<&str>,
    body: serde_json::Value,
) -> String {
    let body = body.to_string();
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    if let Some(secret) = secret {
        request.push_str(&format!("X-Telegram-Bot-Api-Secret-Token: {secret}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(&body);

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
}
