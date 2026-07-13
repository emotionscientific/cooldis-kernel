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

use super::lease::StreamLeaseGrantV1;
use crate::{CooldisResult, EventSequence, EventStreamId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Durable propagation position for one leased stream.
///
/// `pushed_through` is confirmed-by-ack, not sent: it advances only on an
/// accepted push, so a crash between send and ack re-pushes a batch. That
/// retry first receives a sequence conflict; the propagator then pulls and
/// compares the durable records before adopting the already-applied tail.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamPropagationState {
    pub stream_id: EventStreamId,
    pub lease: StreamLeaseGrantV1,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pushed_through: Option<EventSequence>,
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
