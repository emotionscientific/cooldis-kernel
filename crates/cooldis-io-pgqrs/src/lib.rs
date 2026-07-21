//! Durable pgqrs-backed ingress queue for Cooldis IO.
//!
//! This crate is intentionally a wrapper. Protocol adapters submit
//! `IngressEnvelope`s through `cooldis-io-core` traits, and the daemon can later
//! swap SQLite for Postgres without exposing pgqrs to Telegram, websocket, or
//! kernel bridge code.

use async_trait::async_trait;
use cooldis_io_core::{
    IngressAck, IngressEnvelope, IngressPersistenceConfig, IngressPersistenceMode,
    IngressQueueStore, IngressSink, IoError, IoResult, LeasedIngressEnvelope,
};
use pgqrs::error::Error as PgqrsError;
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const INGRESS_PAYLOAD_KIND: &str = "cooldis.ingress.v1";
const DEFAULT_QUEUE_NAME: &str = "cooldis-ingress";
const INGRESS_QUEUE_MAX_OBJECT_DEPTH: usize = 16;
const SQLITE_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PgqrsQueueConfig {
    pub dsn: String,
    pub queue_name: String,
    pub default_visibility_timeout_secs: u32,
}

impl PgqrsQueueConfig {
    pub fn new(dsn: impl Into<String>, queue_name: impl Into<String>) -> Self {
        Self {
            dsn: dsn.into(),
            queue_name: queue_name.into(),
            default_visibility_timeout_secs: 30,
        }
    }

    pub fn local_sqlite(path: impl AsRef<Path>, queue_name: impl Into<String>) -> Self {
        Self::new(sqlite_dsn(path.as_ref()), queue_name)
    }

    pub fn from_persistence_config(
        dsn: impl Into<String>,
        persistence: &IngressPersistenceConfig,
    ) -> IoResult<Option<Self>> {
        match persistence.mode {
            IngressPersistenceMode::DurableQueue => {
                let queue_name = persistence
                    .queue_name
                    .clone()
                    .unwrap_or_else(|| DEFAULT_QUEUE_NAME.to_string());
                Ok(Some(
                    Self::new(dsn, queue_name)
                        .with_default_visibility_timeout_secs(persistence.visibility_timeout_secs),
                ))
            }
            IngressPersistenceMode::BestEffortDirect => Ok(None),
        }
    }

    pub fn with_default_visibility_timeout_secs(mut self, seconds: u32) -> Self {
        self.default_visibility_timeout_secs = seconds;
        self
    }
}

#[derive(Clone, Debug)]
pub struct PgqrsIngressQueue {
    producer: pgqrs::Producer,
    consumer: pgqrs::Consumer,
    config: PgqrsQueueConfig,
    sqlite_dedupe_path: Option<PathBuf>,
}

impl PgqrsIngressQueue {
    pub async fn connect(config: PgqrsQueueConfig) -> IoResult<Self> {
        ensure_sqlite_file_exists(&config.dsn)?;

        let store_config = pgqrs_store_config(&config.dsn);
        let store = pgqrs::connect_with_config(&store_config)
            .await
            .map_err(queue_error)?;
        pgqrs::admin(&store).install().await.map_err(queue_error)?;

        match pgqrs::admin(&store).create_queue(&config.queue_name).await {
            Ok(_) | Err(PgqrsError::QueueAlreadyExists { .. }) => {}
            Err(err) => return Err(queue_error(err)),
        }

        let producer =
            pgqrs::store::Store::producer_ephemeral(&store, &config.queue_name, &store_config)
                .await
                .map_err(queue_error)?;
        let consumer = pgqrs::store::Store::consumer_ephemeral(&store, &config.queue_name)
            .await
            .map_err(queue_error)?;

        let sqlite_dedupe_path = sqlite_path_from_dsn(&config.dsn);
        if let Some(path) = &sqlite_dedupe_path {
            ensure_sqlite_dedupe_table(path)?;
        }

        Ok(Self {
            producer,
            consumer,
            config,
            sqlite_dedupe_path,
        })
    }

    pub fn config(&self) -> &PgqrsQueueConfig {
        &self.config
    }

    pub async fn lease_default(
        &self,
        worker_id: &str,
        max_messages: usize,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
        self.lease_ingress(
            worker_id,
            max_messages,
            self.config.default_visibility_timeout_secs,
        )
        .await
    }
}

fn pgqrs_store_config(dsn: &str) -> pgqrs::Config {
    let mut config = pgqrs::Config::default();
    config.dsn = dsn.to_string();
    config.validation_config.max_object_depth = INGRESS_QUEUE_MAX_OBJECT_DEPTH;
    config
}

#[async_trait]
impl IngressSink for PgqrsIngressQueue {
    async fn submit(&self, envelope: IngressEnvelope) -> IoResult<IngressAck> {
        envelope.require_witnessed()?;
        let claimed = self.try_claim_dedupe_key(&envelope)?;
        if !claimed {
            return Ok(IngressAck::rejected(&envelope, "duplicate dedupe key"));
        }

        let ack = IngressAck::accepted(&envelope);
        let payload = serde_json::to_value(IngressQueuePayload::new(envelope))
            .map_err(|err| IoError::Queue(format!("encode ingress envelope: {err}")))?;

        if let Err(err) = self.producer.enqueue(&payload).await.map_err(queue_error) {
            self.release_dedupe_key(&ack.dedupe_key)?;
            return Err(err);
        }
        Ok(ack)
    }
}

impl PgqrsIngressQueue {
    fn try_claim_dedupe_key(&self, envelope: &IngressEnvelope) -> IoResult<bool> {
        let Some(path) = &self.sqlite_dedupe_path else {
            return Ok(true);
        };
        let dedupe_key = envelope.effective_dedupe_key().ok_or_else(|| {
            IoError::InvalidEnvelope("effective dedupe key is required".to_string())
        })?;
        let connection = rusqlite::Connection::open(path)
            .map_err(|err| IoError::Queue(format!("open sqlite dedupe store: {err}")))?;
        connection
            .busy_timeout(SQLITE_BUSY_TIMEOUT)
            .map_err(|err| IoError::Queue(format!("configure sqlite dedupe store: {err}")))?;
        ensure_sqlite_dedupe_schema(&connection)?;
        let inserted = connection
            .execute(
                "INSERT OR IGNORE INTO cooldis_ingress_dedupe
                    (queue_name, dedupe_key, envelope_id, inserted_at_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    self.config.queue_name.as_str(),
                    dedupe_key.stable_key(),
                    envelope.id.as_str(),
                    now_ms() as i64
                ],
            )
            .map_err(|err| IoError::Queue(format!("claim ingress dedupe key: {err}")))?;
        Ok(inserted == 1)
    }

    fn release_dedupe_key(
        &self,
        dedupe_key: &Option<cooldis_io_core::IoDedupeKey>,
    ) -> IoResult<()> {
        let Some(path) = &self.sqlite_dedupe_path else {
            return Ok(());
        };
        let Some(dedupe_key) = dedupe_key else {
            return Ok(());
        };
        let connection = rusqlite::Connection::open(path)
            .map_err(|err| IoError::Queue(format!("open sqlite dedupe store: {err}")))?;
        connection
            .execute(
                "DELETE FROM cooldis_ingress_dedupe
                 WHERE queue_name = ?1 AND dedupe_key = ?2",
                params![self.config.queue_name.as_str(), dedupe_key.stable_key()],
            )
            .map_err(|err| IoError::Queue(format!("release ingress dedupe key: {err}")))?;
        Ok(())
    }
}

#[async_trait]
impl IngressQueueStore for PgqrsIngressQueue {
    async fn lease_ingress(
        &self,
        worker_id: &str,
        max_messages: usize,
        visibility_timeout_secs: u32,
    ) -> IoResult<Vec<LeasedIngressEnvelope>> {
        let messages = self
            .consumer
            .dequeue_many_with_delay(max_messages, visibility_timeout_secs)
            .await
            .map_err(queue_error)?;

        messages
            .into_iter()
            .map(|message| {
                let payload: IngressQueuePayload = serde_json::from_value(message.payload)
                    .map_err(|err| {
                        IoError::Queue(format!("decode ingress queue payload: {err}"))
                    })?;
                if payload.kind != INGRESS_PAYLOAD_KIND {
                    return Err(IoError::Queue(format!(
                        "unsupported ingress queue payload kind {:?}",
                        payload.kind
                    )));
                }

                let mut leased =
                    LeasedIngressEnvelope::new(message.id.to_string(), payload.envelope);
                leased.attempt = message.read_ct.max(0) as u32;
                leased.lease_owner = Some(worker_id.to_string());
                leased
                    .metadata
                    .insert("pgqrs_queue_id".to_string(), message.queue_id.to_string());
                if let Some(dequeued_at) = message.dequeued_at {
                    leased
                        .metadata
                        .insert("pgqrs_dequeued_at".to_string(), dequeued_at.to_rfc3339());
                }
                Ok(leased)
            })
            .collect()
    }

    async fn complete_ingress(&self, message_id: &str) -> IoResult<()> {
        let id = parse_message_id(message_id)?;
        self.consumer.archive(id).await.map_err(queue_error)?;
        Ok(())
    }

    async fn hold_ingress_until(&self, message_id: &str, visible_at_ms: u64) -> IoResult<()> {
        let id = parse_message_id(message_id)?;
        let visible_at = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(
            visible_at_ms.min(i64::MAX as u64) as i64,
        )
        .ok_or_else(|| IoError::Queue(format!("invalid visibility timestamp {visible_at_ms}ms")))?;
        let released = self
            .consumer
            .release_with_visibility(id, visible_at)
            .await
            .map_err(queue_error)?;
        if !released {
            return Err(IoError::Queue(format!(
                "message {message_id} was not held until {visible_at_ms}"
            )));
        }
        Ok(())
    }

    async fn retry_ingress(&self, message_id: &str, reason: &str) -> IoResult<()> {
        let id = parse_message_id(message_id)?;
        let released = self
            .consumer
            .release_messages(&[id])
            .await
            .map_err(queue_error)?;
        if released == 0 {
            return Err(IoError::Queue(format!(
                "message {message_id} was not released for retry: {reason}"
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IngressQueuePayload {
    kind: String,
    envelope: IngressEnvelope,
}

impl IngressQueuePayload {
    fn new(envelope: IngressEnvelope) -> Self {
        Self {
            kind: INGRESS_PAYLOAD_KIND.to_string(),
            envelope,
        }
    }
}

pub fn sqlite_dsn(path: &Path) -> String {
    format!("sqlite://{}", path.display())
}

fn parse_message_id(value: &str) -> IoResult<i64> {
    value
        .parse::<i64>()
        .map_err(|err| IoError::Queue(format!("invalid queue message id {value:?}: {err}")))
}

fn ensure_sqlite_file_exists(dsn: &str) -> IoResult<()> {
    let Some(path) = dsn.strip_prefix("sqlite://") else {
        return Ok(());
    };
    if path == ":memory:" || path.is_empty() {
        return Ok(());
    }

    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            IoError::Queue(format!(
                "create sqlite queue directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    if !path.exists() {
        std::fs::File::create(path).map_err(|err| {
            IoError::Queue(format!(
                "create sqlite queue file {}: {err}",
                path.display()
            ))
        })?;
    }
    Ok(())
}

fn sqlite_path_from_dsn(dsn: &str) -> Option<PathBuf> {
    let path = dsn.strip_prefix("sqlite://")?;
    if path == ":memory:" || path.is_empty() {
        return None;
    }
    Some(PathBuf::from(path))
}

fn ensure_sqlite_dedupe_table(path: &Path) -> IoResult<()> {
    let connection = rusqlite::Connection::open(path)
        .map_err(|err| IoError::Queue(format!("open sqlite dedupe store: {err}")))?;
    ensure_sqlite_dedupe_schema(&connection)
}

fn ensure_sqlite_dedupe_schema(connection: &rusqlite::Connection) -> IoResult<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cooldis_ingress_dedupe (
                queue_name TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                inserted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (queue_name, dedupe_key)
            );
            "#,
        )
        .map_err(|err| IoError::Queue(format!("initialize sqlite dedupe table: {err}")))?;
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn queue_error(err: PgqrsError) -> IoError {
    IoError::Queue(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cooldis_io_core::{
        ConversationKind, IngressContent, IoActor, IoConversation, IoDedupeKey, IoDelivery,
        IoPrincipal, IoSource,
    };
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_db_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir()
            .join("cooldis-io-pgqrs-tests")
            .join(format!("{name}-{nanos}.sqlite"))
    }

    fn envelope(text: &str) -> IngressEnvelope {
        let source = IoSource::new("telegram.bot", "main");
        IngressEnvelope::new(
            source.clone(),
            IoConversation::new("telegram:chat:123", ConversationKind::Direct),
            IngressContent::text(text),
            1_777_000_000_000,
        )
        .with_actor(IoActor::new("telegram:user:42"))
        .with_dedupe_key(IoDedupeKey::for_source(&source, format!("update:{text}")))
        .with_delivery(IoDelivery::new(format!("update:{text}")))
        .with_principal(IoPrincipal::new("tenant", "user", "route:main"))
    }

    #[tokio::test]
    async fn sqlite_queue_rejects_unwitnessed_submit_before_mutation() {
        let path = test_db_path("unwitnessed");
        let queue =
            PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&path, "cooldis-ingress"))
                .await
                .unwrap();
        let source = IoSource::new("telegram.bot", "main");
        let unwitnessed = IngressEnvelope::new(
            source.clone(),
            IoConversation::new("telegram:chat:123", ConversationKind::Direct),
            IngressContent::text("missing delivery"),
            1_777_000_000_000,
        )
        .with_dedupe_key(IoDedupeKey::for_source(&source, "update:missing"));

        let err = queue.submit(unwitnessed).await.unwrap_err();
        assert!(matches!(
            err,
            IoError::InvalidEnvelope(message) if message == "delivery is required"
        ));
        assert!(
            queue
                .lease_default("worker-1", 10)
                .await
                .unwrap()
                .is_empty()
        );
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn sqlite_queue_persists_submitted_ingress_across_reconnect() {
        let path = test_db_path("persist");
        let config = PgqrsQueueConfig::local_sqlite(&path, "cooldis-ingress");

        let queue = PgqrsIngressQueue::connect(config.clone()).await.unwrap();
        queue.submit(envelope("hello")).await.unwrap();
        drop(queue);

        let queue = PgqrsIngressQueue::connect(config).await.unwrap();
        let leased = queue.lease_default("worker-1", 10).await.unwrap();

        assert_eq!(leased.len(), 1);
        assert_eq!(leased[0].envelope.content.text_projection(), "hello");
        assert_eq!(leased[0].attempt, 1);
        assert_eq!(
            leased[0].envelope.delivery,
            Some(IoDelivery::new("update:hello"))
        );
        assert_eq!(
            leased[0].envelope.principal,
            Some(IoPrincipal::new("tenant", "user", "route:main"))
        );

        queue.complete_ingress(&leased[0].message_id).await.unwrap();
        assert!(
            queue
                .lease_default("worker-1", 10)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn retry_releases_leased_ingress_for_another_worker() {
        let path = test_db_path("retry");
        let queue =
            PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&path, "cooldis-ingress"))
                .await
                .unwrap();

        queue.submit(envelope("try again")).await.unwrap();
        let leased = queue.lease_default("worker-1", 1).await.unwrap();
        assert_eq!(leased.len(), 1);

        queue
            .retry_ingress(&leased[0].message_id, "transient")
            .await
            .unwrap();

        let leased_again = queue.lease_default("worker-2", 1).await.unwrap();
        assert_eq!(leased_again.len(), 1);
        assert_eq!(
            leased_again[0].envelope.content.text_projection(),
            "try again"
        );
        assert_eq!(leased_again[0].lease_owner.as_deref(), Some("worker-2"));
        assert!(leased_again[0].attempt >= 2);
    }

    #[tokio::test]
    async fn sqlite_queue_rejects_duplicate_dedupe_key_on_submit() {
        let path = test_db_path("dedupe");
        let queue =
            PgqrsIngressQueue::connect(PgqrsQueueConfig::local_sqlite(&path, "cooldis-ingress"))
                .await
                .unwrap();
        let source = IoSource::new("clock.tick", "main");
        let first = IngressEnvelope::new(
            source.clone(),
            IoConversation::new("thread:one", ConversationKind::System),
            IngressContent::Event {
                kind: "timer.fired".to_string(),
                payload: serde_json::json!({}),
            },
            1_777_000_000_000,
        )
        .with_dedupe_key(IoDedupeKey::for_source(&source, "mandate:0"))
        .with_delivery(IoDelivery::new("mandate:0"));
        let duplicate = IngressEnvelope::new(
            source.clone(),
            IoConversation::new("thread:one", ConversationKind::System),
            IngressContent::Event {
                kind: "timer.fired".to_string(),
                payload: serde_json::json!({}),
            },
            1_777_000_000_001,
        )
        .with_dedupe_key(IoDedupeKey::for_source(&source, "mandate:0"))
        .with_delivery(IoDelivery::new("mandate:0"));

        assert!(queue.submit(first).await.unwrap().accepted);
        let duplicate_ack = queue.submit(duplicate).await.unwrap();
        assert!(!duplicate_ack.accepted);
        assert_eq!(
            duplicate_ack.reason.as_deref(),
            Some("duplicate dedupe key")
        );

        let leased = queue.lease_default("worker-1", 10).await.unwrap();
        assert_eq!(leased.len(), 1);
        queue.complete_ingress(&leased[0].message_id).await.unwrap();
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn persistence_config_builds_pgqrs_config_only_for_durable_mode() {
        let durable = IngressPersistenceConfig::durable_queue("telegram-ingress")
            .with_visibility_timeout_secs(12);
        let config = PgqrsQueueConfig::from_persistence_config("sqlite://queue.sqlite", &durable)
            .unwrap()
            .unwrap();

        assert_eq!(config.dsn, "sqlite://queue.sqlite");
        assert_eq!(config.queue_name, "telegram-ingress");
        assert_eq!(config.default_visibility_timeout_secs, 12);

        let direct = IngressPersistenceConfig::best_effort_direct();
        assert!(
            PgqrsQueueConfig::from_persistence_config("sqlite://queue.sqlite", &direct)
                .unwrap()
                .is_none()
        );
    }
}
