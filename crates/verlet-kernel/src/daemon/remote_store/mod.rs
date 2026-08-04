//! Remote EventStore backend surface (ADR 0006 "Cross-runtime protocol").
//!
//! Remote placement makes the parent runtime a conductor of the child. The
//! correctness-bearing channel is the store, never a socket: a remotely
//! placed child persists its stream to a store the parent's daemon hosts,
//! and every cross-runtime interaction is witnessed ingress on the receiving
//! side, correlated by handle. This module fixes the vocabulary and
//! interfaces for that protocol; the implementations land in the EMO-422
//! ticket split.
//!
//! The laws, in dependency order:
//!
//! 1. **Daemon-owned endpoint** ([`endpoint`]). The engine holds an
//!    exclusive per-process file lock (ADR 0005), so the daemon endpoint is
//!    the only door to the parent's store, and the daemon's attestation
//!    authority sits at it. The engine's own push path has no fence
//!    (silent last-push-wins), so authorization lives in our endpoint:
//!    credential, then prefix scope, then credential/lease binding, then one
//!    atomic lease-and-sequence-fenced append. Rejects are witnessed.
//! 2. **Single-writer lease per thread stream** ([`lease`]). At most one
//!    live propagator per stream, granted at dispatch, carried with
//!    reservation lineage, enforced at push time. Provable from durable
//!    state alone. Expiry is takeover eligibility, not authority loss: an
//!    expired lease that was never superseded renews and resumes, so an
//!    endpoint outage of any length cannot strand a durable local tail.
//! 3. **Scoped write credentials** ([`lease`]). A child's credential
//!    authorizes exactly its own thread-stream prefix.
//! 4. **Store-hosted durable ingress queue** ([`queue`]). Parent-to-child
//!    submits and steers ride the store with dispatch identity; the child
//!    tails and admits through its own ingress lane.
//! 5. **Push, status, wait are folds and tails over the store**
//!    ([`tail`], [`propagator`]). The child appends locally and a
//!    propagator pushes the tail; the child's terminal event landing in
//!    the parent store IS the push. The parent tail folds it into the
//!    existing parent-side `thread.joined` evidence consumed by the EMO-419
//!    handle adapter; there is no second notification to lose. Endpoint
//!    liveness affects propagation lag only, never correctness.
//!
//! The optional live WebSocket lane is out of scope by law: it may carry
//! latency optimizations, but nothing correctness-bearing may depend on a
//! live connection between runtimes.

pub mod endpoint;
pub mod endpoint_http;
pub mod lease;
pub mod placement;
pub(crate) mod process_executor;
pub mod propagator;
pub mod queue;
pub mod tail;

#[cfg(test)]
mod tests {
    use super::endpoint::{SYNC_PUSH_REJECTION_SCHEMA_V1, SYNC_PUSH_SCHEMA_V1};
    use super::lease::{SYNC_STREAM_LEASE_SCHEMA_V1, SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1};
    use super::queue::SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1;

    #[test]
    fn v1_wire_schemas_use_the_stream_namespace() {
        for schema in [
            SYNC_STREAM_LEASE_SCHEMA_V1,
            SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1,
            SYNC_PUSH_SCHEMA_V1,
            SYNC_PUSH_REJECTION_SCHEMA_V1,
            SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1,
        ] {
            assert!(schema.starts_with("cooldis.stream."), "{schema}");
            assert!(schema.ends_with("/1"), "{schema}");
        }
    }
}
