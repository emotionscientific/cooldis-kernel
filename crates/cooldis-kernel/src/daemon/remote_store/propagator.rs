//! Child-side stream propagator: local-first append, asynchronous push.
//!
//! The remote child never writes the parent's store directly. It appends to
//! its own local store under the unchanged local law, and one propagator
//! per leased stream pushes the local tail to the parent endpoint under the
//! lease fence. Endpoint liveness therefore affects propagation lag only:
//! while the endpoint is unreachable the child keeps appending locally and
//! the propagator retries; when the endpoint returns, the stream converges
//! (ADR 0006 — correctness never depends on a live connection between
//! runtimes).
//!
//! Fence rejections are terminal, not retryable: a
//! [`LeaseFenceDecision::Superseded`] answer means write authority moved to
//! a replacement propagator, and the loser must stop — retrying a
//! superseded lease is never correct. Transport failures are the retryable
//! class.
//!
//! [`LeaseFenceDecision::Superseded`]: super::lease::LeaseFenceDecision::Superseded

use super::endpoint::{
    SYNC_PUSH_SCHEMA_V1, SyncLeaseRenewer, SyncPullSource, SyncPushGate, SyncPushOutcome,
    SyncPushRejectionReason, SyncPushRequestV1,
};
use super::lease::{LeaseFenceDecision, StreamLeaseGrantV1};
use crate::{
    CooldisError, CooldisResult, DaemonClock, EventSequence, EventStore, EventStreamId,
    SqliteSessionStore, StreamCursorV1, StreamRecordEnvelopeV1,
};
use async_trait::async_trait;
use cooldis_sqlite::{TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;

const DEFAULT_PUSH_BATCH_SIZE: usize = 128;
const MIN_RENEWAL_MARGIN_MS: i64 = 1_000;

/// Durable propagation position for one leased stream.
///
/// `pushed_through` is confirmed-by-ack, not sent: it advances only on an
/// accepted push, so a crash between send and ack re-pushes a batch. That
/// retry first receives a sequence conflict; the propagator then pulls and
/// compares the durable records before adopting the already-applied tail.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamPropagationState {
    pub stream_id: EventStreamId,
    pub lease: StreamLeaseGrantV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_through: Option<EventSequence>,
}

impl std::fmt::Debug for StreamPropagationState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamPropagationState")
            .field("stream_id", &self.stream_id)
            .field("lease_scope", &self.lease.scope)
            .field("pushed_through", &self.pushed_through)
            .finish_non_exhaustive()
    }
}

/// What one propagation attempt did.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PropagationStep {
    /// Nothing new past `pushed_through`; the stream is converged.
    Converged,
    /// A batch was accepted; `pushed_through` advanced to the acked tail.
    Advanced { pushed_through: EventSequence },
    /// The endpoint was unreachable or failed transiently; retry with
    /// backoff, keep appending locally.
    EndpointUnavailable,
    /// The lease lost its authority: superseded or unknown at the fence, or
    /// renewal returned no grant. Terminal: stop this propagator and surface
    /// the supersession to the runtime; recovery is a re-lease with lineage,
    /// never a retry of this lease. An `Expired` push rejection is NOT this
    /// outcome — the propagator renews through the endpoint (expiry is
    /// takeover eligibility, not authority loss) and retries; only a refused
    /// renewal lands here.
    LeaseFenced,
    /// The parent stream contains records different from the local batch at
    /// the expected position. Terminal and fail-closed: neither tail may be
    /// silently adopted over the other.
    StreamDiverged { actual_next_sequence: EventSequence },
}

/// Pushes one stream's local tail to the parent endpoint.
///
/// Implementations read the local store past `state.pushed_through`, push
/// through the endpoint's [`SyncPushGate`], renew the lease through
/// [`SyncLeaseRenewer`] within its deadline, and persist every advanced or
/// renewed state durably before acknowledging progress. A sequence conflict
/// is reconciled by pulling and comparing the remote batch: an identical
/// batch is adopted, while different records return
/// [`PropagationStep::StreamDiverged`].
///
/// [`SyncPushGate`]: super::endpoint::SyncPushGate
/// [`SyncLeaseRenewer`]: super::endpoint::SyncLeaseRenewer
#[async_trait]
pub trait StreamPropagator: Send + Sync {
    /// Run one bounded propagation attempt and report what happened. The
    /// caller owns pacing (backoff on [`PropagationStep::EndpointUnavailable`],
    /// shutdown on [`PropagationStep::LeaseFenced`]).
    async fn propagate_once(
        &self,
        state: &mut StreamPropagationState,
    ) -> CooldisResult<PropagationStep>;
}

/// SQLite persistence for a child's per-stream propagation positions.
///
/// State is stored beside the child's local event stream. Every advance and
/// lease renewal is committed through a cancellation-shielded transaction
/// before [`PropagationStep::Advanced`] or renewed progress is returned.
#[derive(Clone)]
pub struct SqlitePropagationStateStore {
    store: SqliteSessionStore,
}

impl std::fmt::Debug for SqlitePropagationStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlitePropagationStateStore")
            .finish_non_exhaustive()
    }
}

impl SqlitePropagationStateStore {
    pub async fn new(store: SqliteSessionStore) -> CooldisResult<Self> {
        let state_store = Self { store };
        state_store.init_schema().await?;
        Ok(state_store)
    }

    pub async fn load(
        &self,
        stream_id: &EventStreamId,
    ) -> CooldisResult<Option<StreamPropagationState>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                "SELECT state_json FROM cooldis_stream_propagation_state WHERE stream_id = ?1",
                params![stream_id.as_str()],
            )
            .await
            .map_err(storage_error)?;
        rows.next()
            .await
            .map_err(storage_error)?
            .map(|row| {
                let encoded = row.get::<String>(0).map_err(storage_error)?;
                serde_json::from_str(&encoded).map_err(|error| {
                    CooldisError::History(format!("decode stream propagation state: {error}"))
                })
            })
            .transpose()
    }

    pub async fn persist(&self, state: &StreamPropagationState) -> CooldisResult<()> {
        let state = state.clone();
        let store = self.store.clone();
        cancellation_safe(async move {
            let encoded = serde_json::to_string(&state).map_err(|error| {
                CooldisError::History(format!("encode stream propagation state: {error}"))
            })?;
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO cooldis_stream_propagation_state (stream_id, state_json)
                     VALUES (?1, ?2)
                     ON CONFLICT(stream_id) DO UPDATE SET state_json = excluded.state_json",
                    params![state.stream_id.as_str(), encoded],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn init_schema(&self) -> CooldisResult<()> {
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
                    CREATE TABLE IF NOT EXISTS cooldis_stream_propagation_state (
                        stream_id TEXT PRIMARY KEY NOT NULL,
                        state_json TEXT NOT NULL
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
}

/// Child-side local-first propagator over one durable SQLite stream.
///
/// The bearer is retained only in memory and sent only through the gate's
/// dedicated credential argument. The type's `Debug` implementation is
/// redacted. Callers own the long-running loop: use bounded exponential
/// backoff only for [`PropagationStep::EndpointUnavailable`], and stop
/// immediately on [`PropagationStep::LeaseFenced`] or
/// [`PropagationStep::StreamDiverged`].
pub struct LocalFirstStreamPropagator {
    local_store: SqliteSessionStore,
    push_gate: Arc<dyn SyncPushGate>,
    pull_source: Arc<dyn SyncPullSource>,
    lease_renewer: Arc<dyn SyncLeaseRenewer>,
    state_store: Arc<SqlitePropagationStateStore>,
    bearer_token: String,
    clock: Arc<dyn DaemonClock>,
    batch_size: usize,
}

impl std::fmt::Debug for LocalFirstStreamPropagator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalFirstStreamPropagator")
            .field("batch_size", &self.batch_size)
            .finish_non_exhaustive()
    }
}

impl LocalFirstStreamPropagator {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        local_store: SqliteSessionStore,
        push_gate: Arc<dyn SyncPushGate>,
        pull_source: Arc<dyn SyncPullSource>,
        lease_renewer: Arc<dyn SyncLeaseRenewer>,
        state_store: Arc<SqlitePropagationStateStore>,
        bearer_token: String,
        clock: Arc<dyn DaemonClock>,
    ) -> Self {
        Self {
            local_store,
            push_gate,
            pull_source,
            lease_renewer,
            state_store,
            bearer_token,
            clock,
            batch_size: DEFAULT_PUSH_BATCH_SIZE,
        }
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    async fn renew_if_due(
        &self,
        state: &mut StreamPropagationState,
    ) -> CooldisResult<Option<PropagationStep>> {
        let lease_duration_ms = state
            .lease
            .expires_at_ms
            .saturating_sub(state.lease.granted_at_ms);
        let renewal_margin_ms = (lease_duration_ms / 3).max(MIN_RENEWAL_MARGIN_MS);
        if self.clock.now().timestamp_millis()
            < state.lease.expires_at_ms.saturating_sub(renewal_margin_ms)
        {
            return Ok(None);
        }
        self.renew(state).await
    }

    async fn renew(
        &self,
        state: &mut StreamPropagationState,
    ) -> CooldisResult<Option<PropagationStep>> {
        let renewed = match self.lease_renewer.renew_lease(&self.bearer_token).await {
            Ok(renewed) => renewed,
            Err(_) => return Ok(Some(PropagationStep::EndpointUnavailable)),
        };
        let Some(renewed) = renewed else {
            return Ok(Some(PropagationStep::LeaseFenced));
        };
        if renewed.lease_id != state.lease.lease_id || renewed.scope != state.lease.scope {
            return Ok(Some(PropagationStep::LeaseFenced));
        }
        state.lease = renewed;
        self.state_store.persist(state).await?;
        Ok(None)
    }

    async fn reconcile_sequence_conflict(
        &self,
        state: &mut StreamPropagationState,
        batch: &[StreamRecordEnvelopeV1],
        actual_next_sequence: EventSequence,
    ) -> CooldisResult<PropagationStep> {
        let expected = batch[0].sequence;
        let cursor = if expected.get() == 1 {
            None
        } else {
            let previous_sequence = EventSequence::new(expected.get() - 1);
            let previous = self
                .local_store
                .read_events(&state.stream_id, Some(previous_sequence))
                .await
                .map_err(|error| CooldisError::History(error.to_string()))?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    CooldisError::History("local propagation cursor record is missing".to_string())
                })?;
            Some(StreamCursorV1::from_event(&previous))
        };
        let remote = match self
            .pull_source
            .pull_after(&self.bearer_token, &state.stream_id, cursor)
            .await
        {
            Ok(remote) => remote,
            Err(CooldisError::History(message)) if message == "sync pull not authorized" => {
                return Ok(PropagationStep::LeaseFenced);
            }
            Err(CooldisError::History(_)) => {
                return Ok(PropagationStep::StreamDiverged {
                    actual_next_sequence,
                });
            }
            Err(_) => return Ok(PropagationStep::EndpointUnavailable),
        };
        if remote.len() < batch.len() || remote[..batch.len()] != *batch {
            return Ok(PropagationStep::StreamDiverged {
                actual_next_sequence,
            });
        }
        if remote.len() > batch.len() {
            let local = self
                .local_store
                .read_events(&state.stream_id, Some(expected))
                .await
                .map_err(|error| CooldisError::History(error.to_string()))?
                .into_iter()
                .map(|event| event.to_stream_record_v1())
                .collect::<Vec<_>>();
            if remote.len() > local.len() || remote != local[..remote.len()] {
                return Ok(PropagationStep::StreamDiverged {
                    actual_next_sequence,
                });
            }
        }
        let pushed_through = batch
            .last()
            .expect("non-empty propagation batches are validated")
            .sequence;
        state.pushed_through = Some(pushed_through);
        self.state_store.persist(state).await?;
        Ok(PropagationStep::Advanced { pushed_through })
    }
}

#[async_trait]
impl StreamPropagator for LocalFirstStreamPropagator {
    async fn propagate_once(
        &self,
        state: &mut StreamPropagationState,
    ) -> CooldisResult<PropagationStep> {
        if !state.lease.scope.authorizes(&state.stream_id) {
            return Err(CooldisError::History(
                "propagation lease scope does not authorize its stream".to_string(),
            ));
        }
        if let Some(step) = self.renew_if_due(state).await? {
            return Ok(step);
        }
        let from_sequence = state
            .pushed_through
            .map(|sequence| EventSequence::new(sequence.get().saturating_add(1)));
        let local = self
            .local_store
            .read_events(&state.stream_id, from_sequence)
            .await
            .map_err(|error| CooldisError::History(error.to_string()))?;
        let batch = local
            .into_iter()
            .take(self.batch_size)
            .map(|event| event.to_stream_record_v1())
            .collect::<Vec<_>>();
        let Some(first) = batch.first() else {
            return Ok(PropagationStep::Converged);
        };
        let request = SyncPushRequestV1 {
            schema: SYNC_PUSH_SCHEMA_V1.to_string(),
            stream_id: state.stream_id.clone(),
            lease_id: state.lease.lease_id.clone(),
            expected_next_sequence: first.sequence,
            records: batch.clone(),
        };
        let mut renewed_after_expiry = false;
        loop {
            let outcome = match self
                .push_gate
                .push(&self.bearer_token, request.clone())
                .await
            {
                Ok(outcome) => outcome,
                Err(_) => return Ok(PropagationStep::EndpointUnavailable),
            };
            if !outcome.matches_request(&request) {
                return Ok(PropagationStep::EndpointUnavailable);
            }
            match outcome {
                SyncPushOutcome::Accepted { ack } => {
                    let expected_end = batch
                        .last()
                        .expect("non-empty propagation batches are validated")
                        .sequence;
                    debug_assert_eq!(ack.tail_sequence, expected_end);
                    state.pushed_through = Some(expected_end);
                    self.state_store.persist(state).await?;
                    return Ok(PropagationStep::Advanced {
                        pushed_through: expected_end,
                    });
                }
                SyncPushOutcome::Rejected { rejection } => match rejection.reason {
                    SyncPushRejectionReason::LeaseFence {
                        fence: LeaseFenceDecision::Expired,
                    } if !renewed_after_expiry => {
                        if let Some(step) = self.renew(state).await? {
                            return Ok(step);
                        }
                        renewed_after_expiry = true;
                    }
                    SyncPushRejectionReason::SequenceFenceConflict {
                        actual_next_sequence,
                    } => {
                        return self
                            .reconcile_sequence_conflict(state, &batch, actual_next_sequence)
                            .await;
                    }
                    _ => return Ok(PropagationStep::LeaseFenced),
                },
            }
        }
    }
}

fn storage_error(error: impl std::fmt::Display) -> CooldisError {
    CooldisError::History(error.to_string())
}

async fn cancellation_safe<T>(
    future: impl Future<Output = CooldisResult<T>> + Send + 'static,
) -> CooldisResult<T>
where
    T: Send + 'static,
{
    tokio::spawn(future).await.map_err(|error| {
        CooldisError::History(format!(
            "propagation state transaction task failed: {error}"
        ))
    })?
}
