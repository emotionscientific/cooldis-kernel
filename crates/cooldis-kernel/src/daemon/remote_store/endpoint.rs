//! Daemon-embedded sync endpoint (ADR 0006 cross-runtime law 1).
//!
//! The daemon is the only door to the parent's store. The engine takes an
//! exclusive per-process file lock (ADR 0005), so there is no lawful
//! topology in which a remote runtime opens the store files; and the
//! engine's logical sync path carries no Cooldis stream-lease token or
//! expected-tail fence, so a push is authorized here before the engine
//! sees it, in this order:
//!
//! 1. credential — resolve the bearer token
//!    ([`SyncCredentialAuthority::verify_token`]); unknown or revoked
//!    fails closed;
//! 2. prefix scope — the credential's [`StreamPrefixScope`] must authorize
//!    the pushed stream;
//! 3. credential/lease binding — the request must present the lease to which
//!    the verified credential was minted;
//! 4. lease and sequence fences —
//!    [`StreamLeaseAuthority::append_if_current`] applies both atomically,
//!    so neither a supersession nor a raced append can interleave after an
//!    authorization check.
//!
//! Every rejection is witnessed durably before the rejection is returned:
//! a rejected push is an observable fact about the stream's history, not a
//! transport error. The daemon's attestation authority sits at this
//! endpoint — the parent attests only its own ingestion and never
//! re-attests the child runtime's internal receipts.
//!
//! [`StreamPrefixScope`]: super::lease::StreamPrefixScope
//! [`StreamLeaseAuthority::append_if_current`]: super::lease::StreamLeaseAuthority::append_if_current
//! [`SyncCredentialAuthority::verify_token`]: super::lease::SyncCredentialAuthority::verify_token

use super::lease::{LeaseFenceDecision, StreamLeaseGrantV1, StreamLeaseId, StreamPrefixScope};
use crate::{
    AppServerListenAddr, CooldisError, CooldisResult, EventSequence, EventStreamId,
    StreamAppendAckV1, StreamCursorV1, StreamRecordEnvelopeV1,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Wire schema identifier for [`SyncPushRequestV1`].
pub const SYNC_PUSH_SCHEMA_V1: &str = "cooldis.stream.sync_push/1";

/// Wire schema identifier for [`SyncPushRejectionV1`].
pub const SYNC_PUSH_REJECTION_SCHEMA_V1: &str = "cooldis.stream.sync_push_rejection/1";

/// One push from a remote propagator: a contiguous batch of records for one
/// stream, fenced by the lease and by the expected next sequence.
///
/// Records ride the existing stream wire envelope
/// ([`StreamRecordEnvelopeV1`]); this protocol adds authority, it does not
/// re-encode history.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SyncPushRequestV1 {
    pub schema: String,
    pub stream_id: EventStreamId,
    pub lease_id: StreamLeaseId,
    /// The sequence the pusher believes comes next (1-based, per
    /// `append_events_fenced`). A mismatch is a fence conflict, never a
    /// partial append.
    pub expected_next_sequence: EventSequence,
    pub records: Vec<StreamRecordEnvelopeV1>,
}

/// Why a push was refused. Each variant maps to exactly one witnessed
/// rejection record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum SyncPushRejectionReason {
    /// The bearer token resolved to no live credential.
    CredentialUnknown,
    /// The credential is live but its scope does not cover the pushed
    /// stream.
    ScopeViolation { scope: StreamPrefixScope },
    /// The credential is scoped correctly but was minted for a different
    /// lease than the request presents.
    CredentialLeaseMismatch { credential_lease_id: StreamLeaseId },
    /// The typed request violated the V1 envelope contract (for example an
    /// unsupported schema id, empty batch, non-contiguous sequence, or a
    /// record naming a different stream).
    RequestInvalid { detail: String },
    /// The presented lease failed the fence (superseded, expired, or
    /// unknown; [`LeaseFenceDecision::Current`] never appears here).
    LeaseFence { fence: LeaseFenceDecision },
    /// Lease and scope passed but the stream tail moved past
    /// `expected_next_sequence`.
    SequenceFenceConflict { actual_next_sequence: EventSequence },
}

/// The witnessed record of one refused push.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SyncPushRejectionV1 {
    pub schema: String,
    pub stream_id: EventStreamId,
    pub lease_id: StreamLeaseId,
    pub reason: SyncPushRejectionReason,
    pub rejected_at_ms: i64,
}

/// Outcome of one push through the authorization pipeline.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum SyncPushOutcome {
    /// All checks passed and the batch appended atomically; the ack
    /// carries the new durable tail.
    Accepted { ack: StreamAppendAckV1 },
    /// A check failed. The rejection was durably witnessed before this
    /// value was returned; the pusher must treat `LeaseFence` rejections as
    /// terminal for its lease.
    Rejected { rejection: SyncPushRejectionV1 },
}

/// The endpoint-side authorization pipeline plus fenced append.
///
/// One implementation serves every stream the daemon hosts. Credential
/// verification and request validation happen before the lease authority's
/// atomic lease-and-store append transaction.
///
/// [`SyncCredentialAuthority`]: super::lease::SyncCredentialAuthority
/// [`StreamLeaseAuthority`]: super::lease::StreamLeaseAuthority
#[async_trait]
pub trait SyncPushGate: Send + Sync {
    /// Authorize and apply one push. `Err` is reserved for store or
    /// transport failure; a refused push is `Ok(Rejected { .. })` with its
    /// witness already committed.
    async fn push(
        &self,
        bearer_token: &str,
        request: SyncPushRequestV1,
    ) -> CooldisResult<SyncPushOutcome>;
}

/// Authenticated child-side lease renewal through the daemon endpoint.
///
/// The bearer credential identifies both the lease and its scope; the
/// endpoint never accepts a caller-selected replacement lease id. Renewal
/// rejection is fail-closed and witnessed before `Ok(None)` is returned.
#[async_trait]
pub trait SyncLeaseRenewer: Send + Sync {
    /// Renew the lease bound to `bearer_token`. Renewal succeeds while the
    /// lease is still the latest grant for its scope, INCLUDING after its
    /// deadline passed — expiry is takeover eligibility, not authority loss,
    /// and this call is how a propagator recovers write authority after an
    /// offline window (the convergence law depends on it). `Ok(None)` means
    /// the token is unknown/revoked or its lease is released or superseded.
    /// Transport/store failures are `Err`.
    async fn renew_lease(&self, bearer_token: &str) -> CooldisResult<Option<StreamLeaseGrantV1>>;
}

/// The endpoint-side pull surface.
///
/// Pull authorization is the same credential and scope check as push,
/// without the lease fence (reads do not move the tail). A remote
/// propagator pulls to converge after an offline window; the child's
/// runtime pulls its store-hosted ingress queue prefix.
#[async_trait]
pub trait SyncPullSource: Send + Sync {
    /// Records after `cursor` (from the start when `None`), in sequence
    /// order. The cursor is verified against the stream per
    /// [`StreamCursorV1`] replay law before anything is returned.
    async fn pull_after(
        &self,
        bearer_token: &str,
        stream_id: &EventStreamId,
        cursor: Option<StreamCursorV1>,
    ) -> CooldisResult<Vec<StreamRecordEnvelopeV1>>;
}

/// Operator configuration for the embedded sync endpoint. EMO-429 wires
/// this value into the daemon's `[daemon.sync]` section.
///
/// `listen: None` means the endpoint is not served and the daemon is
/// local-only — exactly today's behavior; remote placement then fails
/// closed at `resolve_manifest_placement` naming this capability.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonSyncConfig {
    /// Listen address for the sync endpoint, same grammar as the app-server
    /// listen address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen: Option<String>,
    /// Lease renewal deadline applied to new grants, in seconds.
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: u32,
}

impl Default for CooldisDaemonSyncConfig {
    fn default() -> Self {
        Self {
            listen: None,
            lease_ttl_secs: default_lease_ttl_secs(),
        }
    }
}

impl CooldisDaemonSyncConfig {
    /// Parse the configured endpoint address using the daemon app-server's
    /// listen grammar. `None` keeps the endpoint disabled.
    pub fn listen_addr(&self) -> CooldisResult<Option<AppServerListenAddr>> {
        self.listen
            .as_deref()
            .map(AppServerListenAddr::parse)
            .transpose()
    }

    /// Validate the standalone sync configuration before the endpoint is
    /// started.
    pub fn validate(&self) -> CooldisResult<()> {
        self.listen_addr()?;
        if self.lease_ttl_secs == 0 {
            return Err(CooldisError::RuntimeFactory(
                "daemon.sync.lease_ttl_secs must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }
}

fn default_lease_ttl_secs() -> u32 {
    60
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejection_reason_uses_stable_tagged_snake_case_wire_shape() {
        let reason = SyncPushRejectionReason::CredentialLeaseMismatch {
            credential_lease_id: StreamLeaseId::new("lease-1"),
        };
        assert_eq!(
            serde_json::to_value(reason).expect("reason should encode"),
            json!({
                "reason": "credential_lease_mismatch",
                "credential_lease_id": "lease-1"
            })
        );
    }

    #[test]
    fn sync_config_rejects_zero_ttl() {
        let config = CooldisDaemonSyncConfig {
            listen: None,
            lease_ttl_secs: 0,
        };
        assert!(config.validate().is_err());
    }
}
