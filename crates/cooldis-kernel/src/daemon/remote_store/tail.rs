//! Parent-side stream tailing (ADR 0006 cross-runtime law 5).
//!
//! Push, status, and wait are folds and tails over the store. The parent tail
//! folds the child's terminal evidence into the existing parent-side
//! `thread.joined` record consumed by the handle adapter (EMO-419,
//! `daemon::handle_ingress`) — the terminal evidence landing in the parent
//! store IS the push; there is no separate notification to lose.
//! `thread_status` / `thread_wait` fold the same durable child stream.
//!
//! Engine caveat (pinned turso 0.7.0-pre.18): a held connection can retain
//! its pre-pull snapshot, so an implementation must reopen or checkpoint
//! after each pull before folding the new revision — otherwise the tail
//! reads a stale tail forever while records sit committed behind it.

use crate::{
    CooldisError, CooldisResult, EventRecord, EventStore, EventStreamId, SqliteSessionStore,
    StreamCursorV1,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Durable tail position over one child stream.
///
/// `cursor: None` means the tail has seen nothing yet and starts from the
/// beginning of the stream. A non-`None` cursor is verified against the
/// stream on every poll (the [`StreamCursorV1`] replay law), so a rewound
/// or diverged stream fails loudly instead of silently re-folding.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RemoteStreamTailCursor {
    pub stream_id: EventStreamId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<StreamCursorV1>,
}

/// One page of newly visible records plus the advanced cursor.
#[derive(Clone, Debug, PartialEq)]
pub struct RemoteStreamTailPage {
    pub records: Vec<EventRecord>,
    pub next: RemoteStreamTailCursor,
}

/// Polls newly durable records past a cursor.
///
/// The scan-based observation idiom is `handle_ingress`'s: per-stream
/// error isolation (one poisoned stream must not wedge the lane), and
/// consumers stay idempotent because everything downstream dedupes on
/// dispatch identity — a re-poll after a crash re-presents records safely.
#[async_trait]
pub trait RemoteStreamTail: Send + Sync {
    /// Records newly visible past `position`, oldest first, with the
    /// cursor to poll from next. An empty page returns the cursor
    /// unchanged.
    async fn poll(&self, position: &RemoteStreamTailCursor) -> CooldisResult<RemoteStreamTailPage>;
}

/// Parent-local SQLite tail.
///
/// Each poll delegates to a fresh store connection. No connection or read
/// transaction is retained across polls, so a revision committed by the sync
/// endpoint after the previous poll cannot remain hidden behind that poll's
/// snapshot.
#[derive(Clone)]
pub struct SqliteRemoteStreamTail {
    store: SqliteSessionStore,
}

impl SqliteRemoteStreamTail {
    pub fn new(store: SqliteSessionStore) -> Self {
        Self { store }
    }
}

impl std::fmt::Debug for SqliteRemoteStreamTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteRemoteStreamTail")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RemoteStreamTail for SqliteRemoteStreamTail {
    async fn poll(&self, position: &RemoteStreamTailCursor) -> CooldisResult<RemoteStreamTailPage> {
        let records = match position.cursor.as_ref() {
            Some(cursor) => {
                self.store
                    .read_events_after_cursor(&position.stream_id, cursor)
                    .await
            }
            None => self.store.read_events(&position.stream_id, None).await,
        }
        .map_err(|error| CooldisError::History(error.to_string()))?;
        let next = records
            .last()
            .map(|record| RemoteStreamTailCursor {
                stream_id: position.stream_id.clone(),
                cursor: Some(record.cursor_v1()),
            })
            .unwrap_or_else(|| position.clone());
        Ok(RemoteStreamTailPage { records, next })
    }
}
