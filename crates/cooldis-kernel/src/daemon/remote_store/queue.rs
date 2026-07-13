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

use crate::{CooldisResult, EventSequence, ThreadId};
use async_trait::async_trait;
use cooldis_io_core::IngressEnvelope;
use cooldis_runtime_contracts::DispatchId;
use serde::{Deserialize, Serialize};

/// Wire schema identifier for [`RemoteIngressQueueEntryV1`].
pub const SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1: &str = "cooldis.stream.sync_ingress_queue_entry/1";

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
    async fn enqueue(
        &self,
        entry: RemoteIngressQueueEntryV1,
    ) -> CooldisResult<RemoteEnqueueReceipt>;

    /// Child side: entries for `target_thread_id` after `after`, oldest
    /// first. Position is store order. The child persists `page.next` only
    /// after acknowledging every entry in the page; before that point a
    /// restart replays the unacknowledged entries, which is safe because
    /// admission dedupes on dispatch identity.
    async fn tail_pending(
        &self,
        target_thread_id: ThreadId,
        after: Option<EventSequence>,
    ) -> CooldisResult<RemoteIngressQueuePage>;

    /// Child side: mark `dispatch_id` delivered into the child's ingress
    /// lane. Bookkeeping only (see module doc); acknowledging an unknown or
    /// already-acknowledged id is a no-op.
    async fn acknowledge(
        &self,
        target_thread_id: ThreadId,
        dispatch_id: &DispatchId,
    ) -> CooldisResult<()>;
}
