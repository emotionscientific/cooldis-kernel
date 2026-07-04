use super::*;
use crate::{AppServerListenAddr, CooldisAppServerConfig, EventStore};
use cooldis_io_core::{
    ConversationKind, DeliveryReceipt, IngressContent, IoActor, IoConversation, IoDedupeKey,
    IoProtocolAdapter, IoProtocolCapabilities, IoSource,
};
use cooldis_io_pgqrs::{PgqrsIngressQueue, PgqrsQueueConfig};
use serde_json::json;
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

fn test_envelope(text: &str) -> IngressEnvelope {
    IngressEnvelope::new(
        IoSource::new("telegram.bot", "main"),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
}

fn telegram_queue_envelope(text: &str) -> IngressEnvelope {
    let source = IoSource::new("telegram.bot", "main");
    IngressEnvelope::new(
        source.clone(),
        IoConversation::new("telegram:chat:123", ConversationKind::Direct),
        IngressContent::text(text),
        now_ms(),
    )
    .with_actor(IoActor::new("telegram:user:42"))
    .with_dedupe_key(IoDedupeKey::for_source(&source, "update:999"))
    .with_metadata("cooldis_route_id", "main")
    .with_metadata("cooldis_route_policy", "queue_per_conversation")
    .with_metadata("telegram_message_id", "555")
}

fn observe_only_envelope(text: &str) -> IngressEnvelope {
    telegram_queue_envelope(text).with_metadata("cooldis_route_policy", "observe_only")
}

async fn test_bridge() -> (
    CooldisDaemonIoBridge,
    mpsc::UnboundedReceiver<EgressEnvelope>,
    PathBuf,
) {
    let fixture_id = uuid::Uuid::now_v7().to_string();
    let fixture_root = std::env::temp_dir()
        .join("cooldis-daemon-io-tests")
        .join(&fixture_id);
    let socket_path = fixture_root.join("app-server.sock");
    let listen = AppServerListenAddr::parse(&format!("unix://{}", socket_path.display())).unwrap();
    let mut config = CooldisAppServerConfig::local(listen, std::env::current_dir().unwrap());
    config.runtime_home = fixture_root.join("runtime");
    config.state_home = fixture_root.join("state");
    let session_store_path = config.state_home.join("session_history.sqlite3");
    config.tenant_id = format!("app-server-{fixture_id}");
    config.user_id = format!("local-user-{fixture_id}");
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
    (bridge, rx, session_store_path)
}

#[tokio::test]
async fn direct_sink_submits_ingress_to_runtime_and_emits_egress() {
    let (bridge, mut rx, _) = test_bridge().await;

    let ack = bridge
        .direct_sink()
        .submit(test_envelope("hello direct"))
        .await
        .unwrap();

    assert!(ack.accepted);
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
async fn queue_worker_processes_sqlite_backed_envelope() {
    let (bridge, mut rx, _) = test_bridge().await;
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
    let reopened = Arc::new(
        PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&db, "ingress"))
            .await
            .unwrap(),
    );

    let worker =
        CooldisDaemonQueueWorker::new(reopened.clone(), bridge.clone(), "worker-restart", 30);
    assert_eq!(worker.drain_once().await.unwrap(), 1);

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

    reopened
        .submit(observe_only_envelope("observe after restart"))
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
