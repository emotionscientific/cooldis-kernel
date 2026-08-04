//! Store-hosted durable ingress queue for the child direction
//! (ADR 0006 cross-runtime law 4).
//!
//! Parent-to-child submits and steers never ride a socket: the parent lands
//! a queue entry in the store it hosts, and the child runtime tails its own
//! queue prefix (through the endpoint pull surface) and admits each entry
//! through its OWN ingress lane — ADR 0003's protocol is unchanged on the
//! child side, this queue is just where the envelope waits.
//!
//! Dispatch identity has the same law as spawn: a retried submit with the
//! same target-scoped dispatch id folds into the existing entry and never
//! double-injects.
//! Correctness of at-most-once delivery rests on the child's ingress dedupe
//! key (derived from the dispatch id), not on queue acknowledgement —
//! acknowledgement is delivery bookkeeping, so redelivery after a lost ack
//! is safe by construction.

use crate::{
    EventOrigin, EventProvenance, EventRecordId, EventSequence, EventStreamId,
    STREAM_RECORD_SCHEMA_V1, SqliteSessionStore, StreamCursorV1, StreamRecordEnvelopeV1,
    ThreadCoordinates, ThreadId, VerletError, VerletResult,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::time::{SystemTime, UNIX_EPOCH};
use verlet_io_core::IngressEnvelope;
use verlet_runtime_contracts::DispatchId;
use verlet_sqlite::{TransactionBehavior, params};

/// Wire schema identifier for [`RemoteIngressQueueEntryV1`].
pub const SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1: &str = "cooldis.stream.sync_ingress_queue_entry/1";

/// Stream prefix projected by the pull endpoint for store-hosted queue rows.
///
/// This deliberately does not begin with `control:` or `thread:`: startup
/// recovery and spawn projection scan those namespaces as lifecycle truth,
/// while queue rows are transport bookkeeping with a different fold.
pub const SYNC_INGRESS_QUEUE_STREAM_PREFIX: &str = "sync-ingress:";

/// The exact pull stream assigned to one child queue.
pub fn remote_ingress_queue_stream_id(target_thread_id: ThreadId) -> EventStreamId {
    EventStreamId::new(format!(
        "{SYNC_INGRESS_QUEUE_STREAM_PREFIX}{target_thread_id}"
    ))
}

/// Parse a queue stream id without accepting adjacent textual prefixes.
pub fn remote_ingress_queue_target(stream_id: &EventStreamId) -> Option<ThreadId> {
    let value = stream_id
        .as_str()
        .strip_prefix(SYNC_INGRESS_QUEUE_STREAM_PREFIX)?;
    if value.is_empty() || value.contains(':') {
        return None;
    }
    ThreadId::parse_str(value).ok()
}

/// One durable parent-to-child envelope, keyed by dispatch identity.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteIngressQueueEntryV1 {
    pub schema: String,
    /// Dispatch identity of the submit/steer; the fold key.
    pub dispatch_id: DispatchId,
    /// The child thread this entry addresses, as the child's runtime knows
    /// it (the queue prefix is derived from it).
    pub target_thread_id: ThreadId,
    pub envelope: IngressEnvelope,
    pub enqueued_at_ms: i64,
}

/// Whether an enqueue inserted a new entry or folded to an existing one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteEnqueueDisposition {
    Enqueued,
    FoldedToExisting,
}

/// Receipt for one enqueue attempt.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteEnqueueReceipt {
    pub entry: RemoteIngressQueueEntryV1,
    pub disposition: RemoteEnqueueDisposition,
}

/// One ordered page from a child's durable queue.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteIngressQueuePage {
    pub entries: Vec<RemoteIngressQueueEntryV1>,
    /// Store position to pass as `after` on the next tail. An empty page
    /// returns the input position unchanged.
    pub next: Option<EventSequence>,
}

/// The store-hosted queue, both directions.
///
/// The parent side enqueues; the child side tails and acknowledges. Both
/// operate on durable store state only — there is no in-memory queue to
/// lose, and a daemon restart changes nothing about what is pending.
#[async_trait]
pub trait RemoteIngressQueue: Send + Sync {
    /// Parent side: land `entry` durably. An entry with the same
    /// (`target_thread_id`, `dispatch_id`) already present folds to it
    /// (same-payload adoption, per the spawn dispatch-identity law); it never
    /// inserts a duplicate and never silently replaces a different payload.
    /// The envelope's dedupe-key value must equal `dispatch_id`; a missing or
    /// mismatched key is rejected before mutation so child admission has the
    /// identity on which this queue's redelivery law depends.
    async fn enqueue(&self, entry: RemoteIngressQueueEntryV1)
    -> VerletResult<RemoteEnqueueReceipt>;

    /// Child side: entries for `target_thread_id` after `after`, oldest
    /// first. Position is store order. The child persists `page.next` only
    /// after acknowledging every entry in the page; before that point a
    /// restart replays the unacknowledged entries, which is safe because
    /// admission dedupes on dispatch identity.
    async fn tail_pending(
        &self,
        target_thread_id: ThreadId,
        after: Option<EventSequence>,
    ) -> VerletResult<RemoteIngressQueuePage>;

    /// Child side: mark `dispatch_id` delivered into the child's ingress
    /// lane. Bookkeeping only (see module doc); acknowledging an unknown or
    /// already-acknowledged id is a no-op.
    async fn acknowledge(
        &self,
        target_thread_id: ThreadId,
        dispatch_id: &DispatchId,
    ) -> VerletResult<()>;
}

/// SQLite implementation hosted alongside the daemon's event store.
///
/// Queue rows use a dedicated table rather than event streams. This keeps
/// transport acknowledgement out of the frozen lifecycle vocabulary while
/// still sharing the daemon's one engine owner and writer order. The pull
/// endpoint projects rows as verified-cursor stream envelopes under
/// [`SYNC_INGRESS_QUEUE_STREAM_PREFIX`].
#[derive(Clone)]
pub struct SqliteRemoteIngressQueue {
    store: SqliteSessionStore,
}

impl std::fmt::Debug for SqliteRemoteIngressQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteRemoteIngressQueue")
            .finish_non_exhaustive()
    }
}

impl SqliteRemoteIngressQueue {
    pub async fn new(store: SqliteSessionStore) -> VerletResult<Self> {
        let queue = Self { store };
        queue.init_schema().await?;
        Ok(queue)
    }

    async fn init_schema(&self) -> VerletResult<()> {
        let store = self.store.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS verlet_remote_ingress_queue (
                        target_thread_id TEXT NOT NULL,
                        sequence INTEGER NOT NULL,
                        event_id TEXT NOT NULL UNIQUE,
                        dispatch_id TEXT NOT NULL,
                        entry_json TEXT NOT NULL,
                        acknowledged_at_ms INTEGER,
                        PRIMARY KEY(target_thread_id, dispatch_id),
                        UNIQUE(target_thread_id, sequence)
                    );

                    CREATE INDEX IF NOT EXISTS idx_verlet_remote_ingress_queue_pending
                        ON verlet_remote_ingress_queue(
                            target_thread_id,
                            acknowledged_at_ms,
                            sequence
                        );
                    "#,
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    /// Project pending queue rows through the authenticated pull surface.
    pub(crate) async fn pull_after(
        &self,
        stream_id: &EventStreamId,
        cursor: Option<StreamCursorV1>,
    ) -> VerletResult<Vec<StreamRecordEnvelopeV1>> {
        let target_thread_id = remote_ingress_queue_target(stream_id).ok_or_else(|| {
            VerletError::History(format!("invalid remote ingress queue stream {stream_id}"))
        })?;
        let after = match cursor {
            Some(cursor) => {
                cursor.validate_stream_cursor_v1().map_err(history_error)?;
                if cursor.stream_id != *stream_id {
                    return Err(VerletError::History(
                        "remote ingress queue cursor belongs to a different stream".to_string(),
                    ));
                }
                self.verify_cursor(target_thread_id, &cursor).await?;
                Some(cursor.sequence)
            }
            None => None,
        };
        let rows = self.pending_rows(target_thread_id, after).await?;
        rows.into_iter()
            .map(|row| row.into_wire(stream_id.clone()))
            .collect()
    }

    async fn verify_cursor(
        &self,
        target_thread_id: ThreadId,
        cursor: &StreamCursorV1,
    ) -> VerletResult<()> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                "SELECT event_id FROM verlet_remote_ingress_queue
                 WHERE target_thread_id = ?1 AND sequence = ?2",
                params![target_thread_id.to_string(), cursor.sequence.get()],
            )
            .await
            .map_err(storage_error)?;
        let Some(row) = rows.next().await.map_err(storage_error)? else {
            return Err(VerletError::History(
                "remote ingress queue cursor sequence is absent".to_string(),
            ));
        };
        let event_id = row.get::<String>(0).map_err(storage_error)?;
        if event_id != cursor.event_id.to_string() {
            return Err(VerletError::History(
                "remote ingress queue cursor event id diverged".to_string(),
            ));
        }
        Ok(())
    }

    async fn pending_rows(
        &self,
        target_thread_id: ThreadId,
        after: Option<EventSequence>,
    ) -> VerletResult<Vec<StoredQueueRow>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let after = after.map(EventSequence::get).unwrap_or(0);
        let mut rows = connection
            .query(
                "SELECT sequence, event_id, entry_json
                 FROM verlet_remote_ingress_queue
                 WHERE target_thread_id = ?1
                   AND sequence > ?2
                   AND acknowledged_at_ms IS NULL
                 ORDER BY sequence",
                params![target_thread_id.to_string(), after],
            )
            .await
            .map_err(storage_error)?;
        let mut pending = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            let sequence = EventSequence::new(row.get::<i64>(0).map_err(storage_error)?);
            let event_id = parse_event_id(&row.get::<String>(1).map_err(storage_error)?)?;
            let encoded = row.get::<String>(2).map_err(storage_error)?;
            let entry = decode_entry(&encoded)?;
            if entry.target_thread_id != target_thread_id {
                return Err(VerletError::History(
                    "remote ingress queue row target does not match its partition".to_string(),
                ));
            }
            pending.push(StoredQueueRow {
                sequence,
                event_id,
                entry,
            });
        }
        Ok(pending)
    }
}

#[derive(Clone)]
struct StoredQueueRow {
    sequence: EventSequence,
    event_id: EventRecordId,
    entry: RemoteIngressQueueEntryV1,
}

impl StoredQueueRow {
    fn into_wire(self, stream_id: EventStreamId) -> VerletResult<StreamRecordEnvelopeV1> {
        let payload = serde_json::to_value(&self.entry).map_err(|error| {
            VerletError::History(format!("encode remote ingress queue entry: {error}"))
        })?;
        Ok(StreamRecordEnvelopeV1 {
            schema: STREAM_RECORD_SCHEMA_V1.to_string(),
            event_id: self.event_id,
            stream_id,
            sequence: self.sequence,
            coordinates: queue_coordinates(self.entry.target_thread_id),
            created_at_ms: self.entry.enqueued_at_ms,
            kind: "sync.ingress.queue.entry".to_string(),
            origin: EventOrigin::Witnessed,
            payload_schema: SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1.to_string(),
            trace_context: None,
            provenance: EventProvenance::default(),
            payload,
        })
    }
}

#[async_trait]
impl RemoteIngressQueue for SqliteRemoteIngressQueue {
    async fn enqueue(
        &self,
        entry: RemoteIngressQueueEntryV1,
    ) -> VerletResult<RemoteEnqueueReceipt> {
        validate_entry(&entry)?;
        let store = self.store.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            // The Immediate transaction is acquired before the duplicate read.
            // Concurrent same-id submissions therefore serialize as either the
            // sole insert or an adoption of that committed row.
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let target = entry.target_thread_id.to_string();
            let dispatch = entry.dispatch_id.to_string();
            let mut existing_rows = transaction
                .query(
                    "SELECT entry_json FROM verlet_remote_ingress_queue
                     WHERE target_thread_id = ?1 AND dispatch_id = ?2",
                    params![target.as_str(), dispatch.as_str()],
                )
                .await
                .map_err(storage_error)?;
            if let Some(row) = existing_rows.next().await.map_err(storage_error)? {
                let encoded = row.get::<String>(0).map_err(storage_error)?;
                drop(existing_rows);
                let existing = decode_entry(&encoded)?;
                if existing != entry {
                    return Err(VerletError::History(format!(
                        "remote ingress dispatch {} already exists with a different payload",
                        entry.dispatch_id
                    )));
                }
                transaction.commit().await.map_err(storage_error)?;
                return Ok(RemoteEnqueueReceipt {
                    entry: existing,
                    disposition: RemoteEnqueueDisposition::FoldedToExisting,
                });
            }
            drop(existing_rows);
            let mut sequence_rows = transaction
                .query(
                    "SELECT COALESCE(MAX(sequence), 0) + 1
                     FROM verlet_remote_ingress_queue
                     WHERE target_thread_id = ?1",
                    params![target.as_str()],
                )
                .await
                .map_err(storage_error)?;
            let sequence = sequence_rows
                .next()
                .await
                .map_err(storage_error)?
                .ok_or_else(|| {
                    VerletError::History(
                        "remote ingress queue sequence query returned no row".to_string(),
                    )
                })?
                .get::<i64>(0)
                .map_err(storage_error)?;
            drop(sequence_rows);
            let event_id = EventRecordId::new();
            let entry_json = serde_json::to_string(&entry).map_err(|error| {
                VerletError::History(format!("encode remote ingress queue entry: {error}"))
            })?;
            transaction
                .execute(
                    "INSERT INTO verlet_remote_ingress_queue (
                        target_thread_id, sequence, event_id, dispatch_id, entry_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![target, sequence, event_id.to_string(), dispatch, entry_json,],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(RemoteEnqueueReceipt {
                entry,
                disposition: RemoteEnqueueDisposition::Enqueued,
            })
        })
        .await
    }

    async fn tail_pending(
        &self,
        target_thread_id: ThreadId,
        after: Option<EventSequence>,
    ) -> VerletResult<RemoteIngressQueuePage> {
        let rows = self.pending_rows(target_thread_id, after).await?;
        let next = rows.last().map(|row| row.sequence).or(after);
        Ok(RemoteIngressQueuePage {
            entries: rows.into_iter().map(|row| row.entry).collect(),
            next,
        })
    }

    async fn acknowledge(
        &self,
        target_thread_id: ThreadId,
        dispatch_id: &DispatchId,
    ) -> VerletResult<()> {
        let store = self.store.clone();
        let dispatch_id = dispatch_id.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "UPDATE verlet_remote_ingress_queue
                     SET acknowledged_at_ms = COALESCE(acknowledged_at_ms, ?3)
                     WHERE target_thread_id = ?1 AND dispatch_id = ?2",
                    params![
                        target_thread_id.to_string(),
                        dispatch_id.to_string(),
                        now_ms(),
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }
}

fn validate_entry(entry: &RemoteIngressQueueEntryV1) -> VerletResult<()> {
    if entry.schema != SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1 {
        return Err(VerletError::History(
            "unsupported remote ingress queue entry schema".to_string(),
        ));
    }
    let Some(dedupe_key) = entry.envelope.dedupe_key.as_ref() else {
        return Err(VerletError::History(
            "remote ingress queue entry requires a dispatch dedupe key".to_string(),
        ));
    };
    if dedupe_key.key != entry.dispatch_id.as_str() {
        return Err(VerletError::History(
            "remote ingress queue dedupe key does not match dispatch id".to_string(),
        ));
    }
    if entry.enqueued_at_ms != 0 {
        return Err(VerletError::History(
            "remote ingress queue enqueued_at_ms must be zero for stable retry identity"
                .to_string(),
        ));
    }
    Ok(())
}

fn decode_entry(encoded: &str) -> VerletResult<RemoteIngressQueueEntryV1> {
    let entry = serde_json::from_str::<RemoteIngressQueueEntryV1>(encoded).map_err(|error| {
        VerletError::History(format!("decode remote ingress queue entry: {error}"))
    })?;
    validate_entry(&entry)?;
    Ok(entry)
}

fn parse_event_id(value: &str) -> VerletResult<EventRecordId> {
    uuid::Uuid::parse_str(value)
        .map(EventRecordId::from_uuid)
        .map_err(|error| VerletError::History(format!("decode queue event id: {error}")))
}

fn queue_coordinates(target_thread_id: ThreadId) -> ThreadCoordinates {
    ThreadCoordinates {
        tenant_id: "remote-ingress".to_string(),
        user_id: "remote-ingress".to_string(),
        session_id: "remote-ingress".to_string(),
        thread_id: target_thread_id,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn history_error(error: impl std::fmt::Display) -> VerletError {
    VerletError::History(error.to_string())
}

fn storage_error(error: impl std::fmt::Display) -> VerletError {
    VerletError::History(error.to_string())
}

async fn cancellation_safe<T>(
    future: impl Future<Output = VerletResult<T>> + Send + 'static,
) -> VerletResult<T>
where
    T: Send + 'static,
{
    tokio::spawn(future).await.map_err(|error| {
        VerletError::History(format!(
            "remote ingress queue transaction task failed: {error}"
        ))
    })?
}
