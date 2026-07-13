//! Single-writer stream leases and scoped write credentials.
//!
//! ADR 0006 cross-runtime laws 2 and 3: every thread stream has at most one
//! live propagator. The propagator's write authority is a lease granted at
//! dispatch, carried with reservation lineage, and enforced at push time by
//! the daemon endpoint — the engine's sync path has no fence of its own, so
//! the fence lives here. A credential scopes its holder to exactly its own
//! stream prefix; the sandbox holds no authority beyond the streams it owns.
//!
//! Durable-state law: grants, renewals, releases, and supersessions are
//! durable rows in the daemon-owned store, and "exactly one live propagator
//! per stream" must be provable from that state alone. The authority never
//! consults in-memory bookkeeping to decide a fence, and a fence decision
//! made after a daemon restart must equal the decision made before it.
//!
//! Crash recovery is re-lease: a replacement propagator is granted a fresh
//! lease whose lineage names the lease it supersedes. Granting with lineage
//! atomically retires the predecessor — after the grant commits, a push
//! bearing the old lease is rejected fail-closed, witnessed, no window in
//! which both leases pass the fence.

use crate::{CooldisResult, EventSequence, EventStreamId, NewEventRecord, StreamAppendAckV1};
use async_trait::async_trait;
use cooldis_runtime_contracts::DispatchId;
use serde::{Deserialize, Serialize};

/// Wire schema identifier for [`StreamLeaseGrantV1`].
pub const SYNC_STREAM_LEASE_SCHEMA_V1: &str = "cooldis.stream.sync_lease/1";

/// Wire schema identifier for [`StreamWriteCredentialV1`].
pub const SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1: &str = "cooldis.stream.sync_write_credential/1";

/// Opaque identifier of one lease grant.
///
/// The id doubles as the fencing token a propagator presents on every push,
/// so it must be unguessable (mint from a CSPRNG, never sequential).
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamLeaseId(String);

impl StreamLeaseId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamLeaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The stream-id prefix a lease (and its credential) authorizes.
///
/// Scope is a colon-delimited prefix over [`EventStreamId`] text: it
/// authorizes the exact stream id and descendants beginning with `prefix:`.
/// It does not authorize adjacent textual prefixes (`thread:child-7` does
/// not authorize `thread:child-70`). An empty prefix never authorizes
/// anything (fail closed rather than authorize-everything).
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StreamPrefixScope(String);

impl StreamPrefixScope {
    pub fn new(prefix: impl Into<String>) -> Self {
        Self(prefix.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Whether `stream_id` falls inside this scope.
    pub fn authorizes(&self, stream_id: &EventStreamId) -> bool {
        if self.0.is_empty() {
            return false;
        }
        let Some(suffix) = stream_id.as_str().strip_prefix(&self.0) else {
            return false;
        };
        suffix.is_empty() || suffix.starts_with(':')
    }
}

/// Lineage from a lease to the lease it superseded.
///
/// `None` marks the first grant of a stream's life; `Some` marks a
/// re-lease (crash recovery, propagator replacement). The chain of
/// supersessions is the durable proof that write authority moved, not
/// multiplied.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamLeaseLineage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_lease_id: Option<StreamLeaseId>,
}

impl StreamLeaseLineage {
    fn is_empty(&self) -> bool {
        self.superseded_lease_id.is_none()
    }
}

/// One granted lease, as durably recorded and as returned to the grantee.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamLeaseGrantV1 {
    pub schema: String,
    pub lease_id: StreamLeaseId,
    pub scope: StreamPrefixScope,
    /// Dispatch identity of the propagator this lease was granted to; ties
    /// the lease to the spawn/placement flow that carried it.
    pub holder_dispatch_id: DispatchId,
    #[serde(default, skip_serializing_if = "StreamLeaseLineage::is_empty")]
    pub lineage: StreamLeaseLineage,
    pub granted_at_ms: i64,
    /// Renewal deadline. A lease past this instant fails the fence as
    /// [`LeaseFenceDecision::Expired`] until renewed or superseded.
    pub expires_at_ms: i64,
}

/// Decision of the push-time fence check for one presented lease.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "decision")]
pub enum LeaseFenceDecision {
    /// The presented lease is the live lease for the stream; the push may
    /// proceed to the sequence fence.
    Current,
    /// The presented lease was superseded by a re-lease. Reject fail-closed;
    /// the loser must stop propagating (its authority moved, retrying is
    /// never correct).
    Superseded,
    /// The presented lease outlived its renewal deadline without renewal.
    /// Recoverable, not terminal: expiry is takeover eligibility, never
    /// authority loss. The holder renews (which succeeds while the lease is
    /// still the latest grant for its scope) and retries the push — this is
    /// the offline-window recovery path.
    Expired,
    /// The authority has no durable record of the presented lease.
    Unknown,
}

impl LeaseFenceDecision {
    /// Whether this decision grants write authority.
    pub fn permits_push(&self) -> bool {
        matches!(self, Self::Current)
    }
}

/// Result of atomically applying both the lease fence and expected-tail
/// fence to one append.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseFencedAppendOutcome {
    /// The presented lease remained current through the append commit.
    Appended { ack: StreamAppendAckV1 },
    /// The lease did not authorize the append.
    LeaseRejected { fence: LeaseFenceDecision },
    /// The lease was current, but the durable stream tail did not match the
    /// caller's expected next sequence.
    SequenceFenceConflict { actual_next_sequence: EventSequence },
}

/// Grants, renews, and fences stream leases against durable state.
///
/// Implementations serialize grant/renew/release/supersede against
/// [`StreamLeaseAuthority::append_if_current`] so that no interleaving can
/// move authority after a check but before its append. A read-only
/// [`StreamLeaseAuthority::check_fence`] result is diagnostic only; it never
/// authorizes a later write.
///
/// Fence resolution must be unambiguous: the live lease for a stream is the
/// unique live grant whose scope authorizes it. To keep that unique,
/// granting a scope that differs from and overlaps a live scope (either
/// scope authorizes the other's prefix as a colon-delimited descendant)
/// fails closed — overlap is a grant-time error, never a fence-time
/// tiebreak. An exact-scope replacement instead follows the lineage rule.
#[async_trait]
pub trait StreamLeaseAuthority: Send + Sync {
    /// Grant a lease over `scope` to the propagator identified by
    /// `holder_dispatch_id`.
    ///
    /// A lineage naming the latest predecessor atomically supersedes it.
    /// Empty lineage is valid only when no lease has ever been granted for
    /// the scope; every replacement, including one after expiry or release,
    /// names the immediately preceding grant. An empty scope fails closed.
    async fn grant_lease(
        &self,
        scope: &StreamPrefixScope,
        holder_dispatch_id: &DispatchId,
        lineage: StreamLeaseLineage,
    ) -> CooldisResult<StreamLeaseGrantV1>;

    /// Extend the renewal deadline of a lease that is still the latest grant
    /// for its scope — including one whose deadline has already passed.
    /// Expiry is takeover eligibility, not authority loss: while no
    /// replacement grant has superseded it and it was not released, the
    /// holder is still the only propagator and renewal restores it after an
    /// offline window (the convergence law depends on this). Renewing a
    /// superseded, released, or unknown lease fails closed. Renewal
    /// serializes against [`Self::grant_lease`], so a takeover racing a
    /// comeback commits exactly one winner.
    async fn renew_lease(&self, lease_id: &StreamLeaseId) -> CooldisResult<StreamLeaseGrantV1>;

    /// Voluntarily end a lease (clean child shutdown). Releasing an
    /// already-superseded lease is a no-op, not an error.
    async fn release_lease(&self, lease_id: &StreamLeaseId) -> CooldisResult<()>;

    /// Read-only fence diagnosis: is `presented` the live write authority
    /// for `stream_id`? The presented grant's own durable scope must
    /// authorize the stream in addition to being the unique live grant that
    /// covers it.
    ///
    /// This result cannot authorize a subsequent append because a
    /// supersession may commit immediately after it returns. Push paths use
    /// [`Self::append_if_current`] instead.
    async fn check_fence(
        &self,
        stream_id: &EventStreamId,
        presented: &StreamLeaseId,
    ) -> CooldisResult<LeaseFenceDecision>;

    /// Atomically check the lease, check the expected tail, and append the
    /// records. Grant, renewal, release, and supersession serialize with the
    /// whole operation, so an old lease cannot append after a replacement
    /// grant commits.
    async fn append_if_current(
        &self,
        stream_id: &EventStreamId,
        presented: &StreamLeaseId,
        expected_next_sequence: EventSequence,
        records: Vec<NewEventRecord>,
    ) -> CooldisResult<LeaseFencedAppendOutcome>;
}

/// A scoped write credential as durably recorded.
///
/// The bearer token itself is secret material: it is returned exactly once
/// at mint time and never persisted in the clear — the store holds only a
/// digest sufficient for verification.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamWriteCredentialV1 {
    pub schema: String,
    pub credential_id: String,
    pub scope: StreamPrefixScope,
    pub lease_id: StreamLeaseId,
    pub minted_at_ms: i64,
}

/// The verified identity behind a presented bearer token.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedPushIdentity {
    pub credential_id: String,
    pub scope: StreamPrefixScope,
    pub lease_id: StreamLeaseId,
}

/// Mints and verifies the scoped credentials that ride with a lease.
#[async_trait]
pub trait SyncCredentialAuthority: Send + Sync {
    /// Mint a credential bound to `grant`'s scope and lease. Returns the
    /// durable record and the bearer token; the token crosses to the child
    /// through the dispatch flow and is never seen again by this authority
    /// except as a digest.
    async fn mint_credential(
        &self,
        grant: &StreamLeaseGrantV1,
    ) -> CooldisResult<(StreamWriteCredentialV1, String)>;

    /// Resolve a presented bearer token. `Ok(None)` is the fail-closed
    /// answer for unknown or revoked tokens — the endpoint witnesses the
    /// rejection; only transport/store failures are `Err`.
    async fn verify_token(&self, token: &str) -> CooldisResult<Option<VerifiedPushIdentity>>;

    /// Revoke a credential (lease release or supersession retires the
    /// credentials minted for it).
    async fn revoke_credential(&self, credential_id: &str) -> CooldisResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn prefix_scope_authorizes_only_its_prefix() {
        let scope = StreamPrefixScope::new("thread:child-7");
        assert!(scope.authorizes(&EventStreamId::new("thread:child-7")));
        assert!(scope.authorizes(&EventStreamId::new("thread:child-7:trace")));
        assert!(!scope.authorizes(&EventStreamId::new("thread:child-70")));
        assert!(!scope.authorizes(&EventStreamId::new("thread:child-8")));
        assert!(!scope.authorizes(&EventStreamId::new("daemon:control")));
    }

    #[test]
    fn empty_prefix_scope_authorizes_nothing() {
        let scope = StreamPrefixScope::new("");
        assert!(!scope.authorizes(&EventStreamId::new("thread:child-7")));
    }

    #[test]
    fn only_current_fence_decision_permits_push() {
        assert!(LeaseFenceDecision::Current.permits_push());
        assert!(!LeaseFenceDecision::Superseded.permits_push());
        assert!(!LeaseFenceDecision::Expired.permits_push());
        assert!(!LeaseFenceDecision::Unknown.permits_push());
    }

    #[test]
    fn lease_grant_decodes_without_lineage_and_ignores_future_optional_fields() {
        let grant: StreamLeaseGrantV1 = serde_json::from_value(json!({
            "schema": SYNC_STREAM_LEASE_SCHEMA_V1,
            "lease_id": "lease-1",
            "scope": "thread:child-7",
            "holder_dispatch_id": "dispatch-1",
            "granted_at_ms": 10,
            "expires_at_ms": 70,
            "future_optional_field": "ignored"
        }))
        .expect("V1 grant should decode without optional lineage");

        assert_eq!(grant.lineage, StreamLeaseLineage::default());
        let encoded = serde_json::to_value(grant).expect("grant should encode");
        assert!(encoded.get("lineage").is_none());
        assert!(encoded.get("future_optional_field").is_none());
    }
}
