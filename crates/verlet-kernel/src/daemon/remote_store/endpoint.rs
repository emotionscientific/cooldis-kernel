//! Daemon-embedded sync endpoint (ADR 0006 cross-runtime law 1).
//!
//! The daemon is the only door to the parent's store. The engine takes an
//! exclusive per-process file lock (ADR 0005), so there is no lawful
//! topology in which a remote runtime opens the store files; and the
//! engine's logical sync path carries no Verlet stream-lease token or
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
//! [`StreamPrefixScope`]: crate::daemon::remote_store::lease::StreamPrefixScope
//! [`StreamLeaseAuthority::append_if_current`]: crate::daemon::remote_store::lease::StreamLeaseAuthority::append_if_current
//! [`SyncCredentialAuthority::verify_token`]: crate::daemon::remote_store::lease::SyncCredentialAuthority::verify_token

use crate::daemon::remote_store::lease::StreamLeaseAuthority as _;
use crate::daemon::remote_store::lease::SyncCredentialAuthority as _;
use crate::daemon::remote_store::queue::RemoteIngressQueue as _;
use sha2::Digest as _;
use verlet_history::EventStore as _;

/// Wire schema identifier for [`SyncPushRequestV1`].
pub const SYNC_PUSH_SCHEMA_V1: &str = "cooldis.stream.sync_push/1";

/// Wire schema identifier for [`SyncPushRejectionV1`].
pub const SYNC_PUSH_REJECTION_SCHEMA_V1: &str = "cooldis.stream.sync_push_rejection/1";

/// Wire schema identifier for a redacted durable rejection witness.
pub const SYNC_PUSH_REJECTION_WITNESS_SCHEMA_V1: &str =
    "cooldis.stream.sync_push_rejection_witness/1";

/// Wire schema identifier for an authenticated cursor pull.
pub const SYNC_PULL_SCHEMA_V1: &str = "cooldis.stream.sync_pull/1";

/// Wire schema for queue-delivery acknowledgement bookkeeping.
pub const SYNC_INGRESS_QUEUE_ACK_SCHEMA_V1: &str = "cooldis.stream.sync_ingress_queue_ack/1";

/// Wire schema identifier for an authenticated lease renewal response.
pub const SYNC_LEASE_RENEWAL_SCHEMA_V1: &str = "cooldis.stream.sync_lease_renewal/1";

/// One push from a remote propagator: a contiguous batch of records for one
/// stream, fenced by the lease and by the expected next sequence.
///
/// Records ride the existing stream wire envelope
/// ([`StreamRecordEnvelopeV1`]); this protocol adds authority, it does not
/// re-encode history.
#[derive(Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncPushRequestV1 {
    pub schema: String,
    pub stream_id: verlet_history::EventStreamId,
    pub lease_id: crate::daemon::remote_store::lease::StreamLeaseId,
    /// The sequence the pusher believes comes next (1-based, per
    /// `append_events_fenced`). A mismatch is a fence conflict, never a
    /// partial append.
    pub expected_next_sequence: verlet_history::EventSequence,
    pub records: Vec<verlet_history::StreamRecordEnvelopeV1>,
}

impl std::fmt::Debug for SyncPushRequestV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncPushRequestV1")
            .field("schema", &self.schema)
            .field("stream_id", &self.stream_id)
            .field("expected_next_sequence", &self.expected_next_sequence)
            .field("record_count", &self.records.len())
            .finish_non_exhaustive()
    }
}

/// Authenticated pull request for verified cursor replay.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncPullRequestV1 {
    pub schema: String,
    pub stream_id: verlet_history::EventStreamId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<verlet_history::StreamCursorV1>,
}

/// One page from the daemon's pull surface.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncPullResponseV1 {
    pub schema: String,
    pub records: Vec<verlet_history::StreamRecordEnvelopeV1>,
}

/// Authenticated acknowledgement of one queue delivery.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncIngressQueueAckRequestV1 {
    pub schema: String,
    pub target_thread_id: verlet_runtime_contracts::ThreadId,
    pub dispatch_id: verlet_runtime_contracts::handle::DispatchId,
}

/// Result of renewing the lease bound to the bearer credential.
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncLeaseRenewalResponseV1 {
    pub schema: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grant: Option<crate::daemon::remote_store::lease::StreamLeaseGrantV1>,
}

impl std::fmt::Debug for SyncLeaseRenewalResponseV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncLeaseRenewalResponseV1")
            .field("schema", &self.schema)
            .field("grant_present", &self.grant.is_some())
            .finish()
    }
}

/// Why a push was refused. Each variant maps to exactly one witnessed
/// rejection record.
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum SyncPushRejectionReason {
    /// The bearer token resolved to no live credential.
    CredentialUnknown,
    /// The credential is live but its scope does not cover the pushed
    /// stream.
    ScopeViolation {
        scope: crate::daemon::remote_store::lease::StreamPrefixScope,
    },
    /// The credential is scoped correctly but was minted for a different
    /// lease than the request presents.
    CredentialLeaseMismatch {
        credential_lease_id: crate::daemon::remote_store::lease::StreamLeaseId,
    },
    /// The typed request violated the V1 envelope contract (for example an
    /// unsupported schema id, empty batch, non-contiguous sequence, or a
    /// record naming a different stream).
    RequestInvalid { detail: String },
    /// The presented lease failed the fence (superseded, expired, or
    /// unknown; [`LeaseFenceDecision::Current`] never appears here).
    LeaseFence {
        fence: crate::daemon::remote_store::lease::LeaseFenceDecision,
    },
    /// Lease and scope passed but the stream tail moved past
    /// `expected_next_sequence`.
    SequenceFenceConflict {
        actual_next_sequence: verlet_history::EventSequence,
    },
}

impl std::fmt::Debug for SyncPushRejectionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CredentialUnknown => f.write_str("CredentialUnknown"),
            Self::ScopeViolation { scope } => f
                .debug_struct("ScopeViolation")
                .field("scope", scope)
                .finish(),
            Self::CredentialLeaseMismatch { .. } => {
                f.write_str("CredentialLeaseMismatch { credential_lease_id: <redacted> }")
            }
            Self::RequestInvalid { detail } => f
                .debug_struct("RequestInvalid")
                .field("detail", detail)
                .finish(),
            Self::LeaseFence { fence } => {
                f.debug_struct("LeaseFence").field("fence", fence).finish()
            }
            Self::SequenceFenceConflict {
                actual_next_sequence,
            } => f
                .debug_struct("SequenceFenceConflict")
                .field("actual_next_sequence", actual_next_sequence)
                .finish(),
        }
    }
}

/// Typed response for one refused push.
///
/// The durable record is the redacted [`SyncPushRejectionWitnessV1`]; it
/// deliberately omits the live lease ids carried in this caller-facing
/// response.
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncPushRejectionV1 {
    pub schema: String,
    pub stream_id: verlet_history::EventStreamId,
    pub lease_id: crate::daemon::remote_store::lease::StreamLeaseId,
    pub reason: SyncPushRejectionReason,
    pub rejected_at_ms: i64,
}

impl std::fmt::Debug for SyncPushRejectionV1 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncPushRejectionV1")
            .field("schema", &self.schema)
            .field("stream_id", &self.stream_id)
            .field("reason", &self.reason)
            .field("rejected_at_ms", &self.rejected_at_ms)
            .finish_non_exhaustive()
    }
}

/// Outcome of one push through the authorization pipeline.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "outcome")]
pub enum SyncPushOutcome {
    /// All checks passed and the batch appended atomically; the ack
    /// carries the new durable tail.
    Accepted {
        ack: verlet_history::StreamAppendAckV1,
    },
    /// A check failed. The rejection was durably witnessed before this
    /// value was returned; the pusher must treat `LeaseFence` rejections as
    /// terminal for its lease.
    Rejected { rejection: SyncPushRejectionV1 },
}

impl SyncPushOutcome {
    /// Validate that a decoded outcome is the complete response to `request`.
    /// This is a transport trust boundary: a malformed or mismatched response
    /// must never advance child propagation state.
    pub(crate) fn matches_request(&self, request: &SyncPushRequestV1) -> bool {
        match self {
            Self::Accepted { ack } => {
                let Some(last) = request.records.last() else {
                    return false;
                };
                ack.schema == verlet_history::STREAM_APPEND_ACK_SCHEMA_V1
                    && ack.stream_id == request.stream_id
                    && ack.start_sequence == request.expected_next_sequence
                    && ack.end_sequence == last.sequence
                    && ack.tail_sequence == last.sequence
                    && ack.tail_event_id == last.event_id
                    && ack
                        .acks
                        .contains(&verlet_history::StreamAckClass::StreamCommitted)
            }
            Self::Rejected { rejection } => {
                rejection.schema == SYNC_PUSH_REJECTION_SCHEMA_V1
                    && rejection.stream_id == request.stream_id
                    && rejection.lease_id == request.lease_id
            }
        }
    }
}

/// Durable, inspectable proof that a push was rejected.
///
/// This is deliberately not the response's [`SyncPushRejectionV1`]: the
/// response echoes the caller's lease id for typed reconciliation, while the
/// durable witness stores only a request fingerprint and redacted details.
/// Neither a cleartext bearer nor a live lease id may cross this boundary.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SyncPushRejectionWitnessV1 {
    pub schema: String,
    pub request_fingerprint: String,
    pub stream_id: verlet_history::EventStreamId,
    pub reason: String,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub detail: serde_json::Value,
    pub rejected_at_ms: i64,
}

/// The endpoint-side authorization pipeline plus fenced append.
///
/// One implementation serves every stream the daemon hosts. Credential
/// verification and request validation happen before the lease authority's
/// atomic lease-and-store append transaction.
///
/// [`SyncCredentialAuthority`]: crate::daemon::remote_store::lease::SyncCredentialAuthority
/// [`StreamLeaseAuthority`]: crate::daemon::remote_store::lease::StreamLeaseAuthority
#[async_trait::async_trait]
pub trait SyncPushGate: Send + Sync {
    /// Authorize and apply one push. `Err` is reserved for store or
    /// transport failure; a refused push is `Ok(Rejected { .. })` with its
    /// witness already committed.
    async fn push(
        &self,
        bearer_token: &str,
        request: SyncPushRequestV1,
    ) -> crate::kernel::runtime_host::VerletResult<SyncPushOutcome>;
}

/// Authenticated child-side lease renewal through the daemon endpoint.
///
/// The bearer credential identifies both the lease and its scope; the
/// endpoint never accepts a caller-selected replacement lease id. Renewal
/// refusal is fail-closed; unlike a push rejection it does not claim a
/// stream mutation and therefore does not create a push-rejection witness.
#[async_trait::async_trait]
pub trait SyncLeaseRenewer: Send + Sync {
    /// Renew the lease bound to `bearer_token`. Renewal succeeds while the
    /// lease is still the latest grant for its scope, INCLUDING after its
    /// deadline passed — expiry is takeover eligibility, not authority loss,
    /// and this call is how a propagator recovers write authority after an
    /// offline window (the convergence law depends on it). `Ok(None)` means
    /// the token is unknown/revoked or its lease is released or superseded.
    /// Transport/store failures are `Err`.
    async fn renew_lease(
        &self,
        bearer_token: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<crate::daemon::remote_store::lease::StreamLeaseGrantV1>,
    >;
}

/// The endpoint-side pull surface.
///
/// Pull authorization is the same credential and scope check as push,
/// without the lease fence (reads do not move the tail). A remote
/// propagator pulls to converge after an offline window; the child's
/// runtime pulls its store-hosted ingress queue prefix.
#[async_trait::async_trait]
pub trait SyncPullSource: Send + Sync {
    /// Records after `cursor` (from the start when `None`), in sequence
    /// order. The cursor is verified against the stream per
    /// [`StreamCursorV1`] replay law before anything is returned.
    async fn pull_after(
        &self,
        bearer_token: &str,
        stream_id: &verlet_history::EventStreamId,
        cursor: Option<verlet_history::StreamCursorV1>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::StreamRecordEnvelopeV1>>;
}

/// Child-side bookkeeping after its own ingress lane accepted a queue row.
#[async_trait::async_trait]
pub trait SyncIngressQueueAcknowledger: Send + Sync {
    async fn acknowledge_ingress(
        &self,
        bearer_token: &str,
        request: SyncIngressQueueAckRequestV1,
    ) -> crate::kernel::runtime_host::VerletResult<()>;
}

/// SQLite-backed daemon endpoint composing credential, lease, and event-store
/// authorities without adding another append surface.
#[derive(Clone)]
pub struct SqliteSyncEndpoint {
    store: verlet_history_sqlite::SqliteSessionStore,
    authority: std::sync::Arc<crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority>,
    clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    ingress_queue: crate::daemon::remote_store::queue::SqliteRemoteIngressQueue,
}

impl std::fmt::Debug for SqliteSyncEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteSyncEndpoint").finish_non_exhaustive()
    }
}

impl SqliteSyncEndpoint {
    /// Initialize the durable rejection-witness table over the same engine
    /// owner used by the lease authority and event store.
    pub async fn new(
        store: verlet_history_sqlite::SqliteSessionStore,
        authority: std::sync::Arc<crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority>,
        clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let ingress_queue =
            crate::daemon::remote_store::queue::SqliteRemoteIngressQueue::new(store.clone())
                .await?;
        let endpoint = Self {
            store,
            authority,
            clock,
            ingress_queue,
        };
        endpoint.init_witness_schema().await?;
        Ok(endpoint)
    }

    /// List redacted rejection witnesses, optionally narrowed to one stream.
    pub async fn rejection_witnesses(
        &self,
        stream_id: Option<&verlet_history::EventStreamId>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<SyncPushRejectionWitnessV1>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let (sql, parameter) = match stream_id {
            Some(stream_id) => (
                "SELECT witness_json FROM cooldis_sync_push_rejections \
                 WHERE stream_id = ?1 ORDER BY rejected_at_ms, request_fingerprint",
                Some(stream_id.as_str()),
            ),
            None => (
                "SELECT witness_json FROM cooldis_sync_push_rejections \
                 ORDER BY rejected_at_ms, request_fingerprint",
                None,
            ),
        };
        let mut rows = match parameter {
            Some(stream_id) => {
                connection
                    .query(sql, verlet_sqlite::params![stream_id])
                    .await
            }
            None => connection.query(sql, ()).await,
        }
        .map_err(storage_error)?;
        let mut witnesses = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            let encoded = row.get::<String>(0).map_err(storage_error)?;
            witnesses.push(serde_json::from_str(&encoded).map_err(|error| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "decode sync rejection witness: {error}"
                ))
            })?);
        }
        Ok(witnesses)
    }

    async fn init_witness_schema(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        let store = self.store.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS cooldis_sync_push_rejections (
                        request_fingerprint TEXT PRIMARY KEY NOT NULL,
                        stream_id TEXT NOT NULL,
                        reason TEXT NOT NULL,
                        rejected_at_ms INTEGER NOT NULL,
                        witness_json TEXT NOT NULL
                    );

                    CREATE INDEX IF NOT EXISTS idx_cooldis_sync_push_rejections_stream
                        ON cooldis_sync_push_rejections(stream_id, rejected_at_ms);
                    "#,
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn reject(
        &self,
        request: &SyncPushRequestV1,
        reason: SyncPushRejectionReason,
    ) -> crate::kernel::runtime_host::VerletResult<SyncPushOutcome> {
        let rejected_at_ms = self.clock.now().timestamp_millis();
        let rejection = SyncPushRejectionV1 {
            schema: SYNC_PUSH_REJECTION_SCHEMA_V1.to_string(),
            stream_id: request.stream_id.clone(),
            lease_id: request.lease_id.clone(),
            reason,
            rejected_at_ms,
        };
        self.persist_rejection_witness(request, &rejection).await?;
        Ok(SyncPushOutcome::Rejected { rejection })
    }

    async fn persist_rejection_witness(
        &self,
        request: &SyncPushRequestV1,
        rejection: &SyncPushRejectionV1,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let request_fingerprint = rejection_fingerprint(request, &rejection.reason)?;
        let (reason, detail) = redacted_rejection_reason(&rejection.reason);
        let witness = SyncPushRejectionWitnessV1 {
            schema: SYNC_PUSH_REJECTION_WITNESS_SCHEMA_V1.to_string(),
            request_fingerprint,
            stream_id: request.stream_id.clone(),
            reason,
            detail,
            rejected_at_ms: rejection.rejected_at_ms,
        };
        let witness_json = serde_json::to_string(&witness).map_err(|error| {
            crate::kernel::runtime_host::VerletError::History(format!(
                "encode sync rejection witness: {error}"
            ))
        })?;
        let store = self.store.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT OR IGNORE INTO cooldis_sync_push_rejections (
                        request_fingerprint, stream_id, reason, rejected_at_ms, witness_json
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    verlet_sqlite::params![
                        witness.request_fingerprint.as_str(),
                        witness.stream_id.as_str(),
                        witness.reason.as_str(),
                        witness.rejected_at_ms,
                        witness_json,
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

#[async_trait::async_trait]
impl SyncPushGate for SqliteSyncEndpoint {
    async fn push(
        &self,
        bearer_token: &str,
        request: SyncPushRequestV1,
    ) -> crate::kernel::runtime_host::VerletResult<SyncPushOutcome> {
        let Some(identity) = self.authority.verify_token(bearer_token).await? else {
            return self
                .reject(&request, SyncPushRejectionReason::CredentialUnknown)
                .await;
        };
        if !identity.scope.authorizes(&request.stream_id) {
            return self
                .reject(
                    &request,
                    SyncPushRejectionReason::ScopeViolation {
                        scope: identity.scope,
                    },
                )
                .await;
        }
        if identity.lease_id != request.lease_id {
            return self
                .reject(
                    &request,
                    SyncPushRejectionReason::CredentialLeaseMismatch {
                        credential_lease_id: identity.lease_id,
                    },
                )
                .await;
        }
        let records = match validate_push_request(&request) {
            Ok(records) => records,
            Err(detail) => {
                return self
                    .reject(&request, SyncPushRejectionReason::RequestInvalid { detail })
                    .await;
            }
        };
        match self
            .authority
            .append_if_current(
                &request.stream_id,
                &request.lease_id,
                request.expected_next_sequence,
                records,
            )
            .await?
        {
            crate::daemon::remote_store::lease::LeaseFencedAppendOutcome::Appended { ack } => Ok(SyncPushOutcome::Accepted { ack }),
            crate::daemon::remote_store::lease::LeaseFencedAppendOutcome::LeaseRejected { fence } => {
                self.reject(&request, SyncPushRejectionReason::LeaseFence { fence })
                    .await
            }
            crate::daemon::remote_store::lease::LeaseFencedAppendOutcome::SequenceFenceConflict {
                actual_next_sequence,
            } => {
                self.reject(
                    &request,
                    SyncPushRejectionReason::SequenceFenceConflict {
                        actual_next_sequence,
                    },
                )
                .await
            }
        }
    }
}

#[async_trait::async_trait]
impl SyncLeaseRenewer for SqliteSyncEndpoint {
    async fn renew_lease(
        &self,
        bearer_token: &str,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<crate::daemon::remote_store::lease::StreamLeaseGrantV1>,
    > {
        let Some(identity) = self.authority.verify_token(bearer_token).await? else {
            return Ok(None);
        };
        self.authority
            .renew_lease(&identity.lease_id)
            .await
            .map(Some)
    }
}

#[async_trait::async_trait]
impl SyncPullSource for SqliteSyncEndpoint {
    async fn pull_after(
        &self,
        bearer_token: &str,
        stream_id: &verlet_history::EventStreamId,
        cursor: Option<verlet_history::StreamCursorV1>,
    ) -> crate::kernel::runtime_host::VerletResult<Vec<verlet_history::StreamRecordEnvelopeV1>>
    {
        let Some(identity) = self.authority.verify_token(bearer_token).await? else {
            return Err(not_authorized());
        };
        if !identity.scope.authorizes(stream_id) {
            return Err(not_authorized());
        }
        if crate::daemon::remote_store::queue::remote_ingress_queue_target(stream_id).is_some() {
            return self.ingress_queue.pull_after(stream_id, cursor).await;
        }
        if stream_id
            .as_str()
            .starts_with(crate::daemon::remote_store::queue::SYNC_INGRESS_QUEUE_STREAM_PREFIX)
        {
            return Err(crate::kernel::runtime_host::VerletError::History(
                "invalid remote ingress queue stream".to_string(),
            ));
        }
        let events = match cursor {
            Some(cursor) => {
                self.store
                    .read_events_after_cursor(stream_id, &cursor)
                    .await
            }
            None => self.store.read_events(stream_id, None).await,
        }
        .map_err(|error| crate::kernel::runtime_host::VerletError::History(error.to_string()))?;
        Ok(events
            .into_iter()
            .map(|event| event.to_stream_record_v1())
            .collect())
    }
}

#[async_trait::async_trait]
impl SyncIngressQueueAcknowledger for SqliteSyncEndpoint {
    async fn acknowledge_ingress(
        &self,
        bearer_token: &str,
        request: SyncIngressQueueAckRequestV1,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if request.schema != SYNC_INGRESS_QUEUE_ACK_SCHEMA_V1 {
            return Err(crate::kernel::runtime_host::VerletError::History(
                "unsupported remote ingress acknowledgement schema".to_string(),
            ));
        }
        let Some(identity) = self.authority.verify_token(bearer_token).await? else {
            return Err(not_authorized());
        };
        let stream_id = crate::daemon::remote_store::queue::remote_ingress_queue_stream_id(
            request.target_thread_id,
        );
        if !identity.scope.authorizes(&stream_id) {
            return Err(not_authorized());
        }
        self.ingress_queue
            .acknowledge(request.target_thread_id, &request.dispatch_id)
            .await
    }
}

fn validate_push_request(
    request: &SyncPushRequestV1,
) -> Result<Vec<verlet_history::NewEventRecord>, String> {
    if request.schema != SYNC_PUSH_SCHEMA_V1 {
        return Err("unsupported sync push schema".to_string());
    }
    if request.expected_next_sequence.get() < 1 {
        return Err("expected_next_sequence must be positive".to_string());
    }
    if request.records.is_empty() {
        return Err("sync push batch must not be empty".to_string());
    }
    let mut records = Vec::with_capacity(request.records.len());
    for (index, envelope) in request.records.iter().enumerate() {
        let expected_sequence = request
            .expected_next_sequence
            .get()
            .checked_add(index as i64)
            .ok_or_else(|| "sync push sequence overflow".to_string())?;
        if envelope.schema != verlet_history::STREAM_RECORD_SCHEMA_V1 {
            return Err("record uses an unsupported stream schema".to_string());
        }
        if envelope.stream_id != request.stream_id {
            return Err("record belongs to a different stream".to_string());
        }
        if envelope.sequence.get() != expected_sequence {
            return Err(
                "record sequences are not contiguous from expected_next_sequence".to_string(),
            );
        }
        if envelope.trace_context.is_some() {
            return Err("trace_context is not supported by the SQLite authority store".to_string());
        }
        let kind = envelope
            .kind
            .parse::<verlet_history::EventKind>()
            .map_err(|_| "record kind is not in the frozen event vocabulary".to_string())?;
        if envelope.payload_schema != kind.payload_schema_id() {
            return Err("record payload schema does not match its kind".to_string());
        }
        let event = verlet_history::EventRecord {
            id: envelope.event_id,
            stream_id: envelope.stream_id.clone(),
            sequence: envelope.sequence,
            coordinates: envelope.coordinates.clone(),
            created_at_ms: envelope.created_at_ms,
            kind,
            origin: envelope.origin,
            provenance: envelope.provenance.clone(),
            payload: envelope.payload.clone(),
        };
        event
            .validate_stream_record_v1()
            .map_err(|_| "record or payload violates the V1 stream schema".to_string())?;
        records.push(verlet_history::NewEventRecord {
            id: event.id,
            coordinates: event.coordinates,
            created_at_ms: event.created_at_ms,
            kind: event.kind,
            origin: event.origin,
            provenance: event.provenance,
            payload: event.payload,
        });
    }
    Ok(records)
}

fn rejection_fingerprint(
    request: &SyncPushRequestV1,
    reason: &SyncPushRejectionReason,
) -> crate::kernel::runtime_host::VerletResult<String> {
    let encoded = serde_json::to_vec(&(request, reason)).map_err(|error| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "encode sync rejection fingerprint: {error}"
        ))
    })?;
    Ok(format!("sha256:{:x}", sha2::Sha256::digest(encoded)))
}

fn redacted_rejection_reason(reason: &SyncPushRejectionReason) -> (String, serde_json::Value) {
    match reason {
        SyncPushRejectionReason::CredentialUnknown => {
            ("credential_unknown".to_string(), serde_json::Value::Null)
        }
        SyncPushRejectionReason::ScopeViolation { scope } => (
            "scope_violation".to_string(),
            serde_json::json!({ "scope": scope }),
        ),
        SyncPushRejectionReason::CredentialLeaseMismatch { .. } => (
            "credential_lease_mismatch".to_string(),
            serde_json::Value::Null,
        ),
        SyncPushRejectionReason::RequestInvalid { detail } => (
            "request_invalid".to_string(),
            serde_json::json!({ "detail": detail }),
        ),
        SyncPushRejectionReason::LeaseFence { fence } => (
            "lease_fence".to_string(),
            serde_json::json!({ "fence": fence }),
        ),
        SyncPushRejectionReason::SequenceFenceConflict {
            actual_next_sequence,
        } => (
            "sequence_fence_conflict".to_string(),
            serde_json::json!({ "actual_next_sequence": actual_next_sequence }),
        ),
    }
}

fn not_authorized() -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::History("sync pull not authorized".to_string())
}

fn storage_error(error: impl std::fmt::Display) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::History(error.to_string())
}

async fn cancellation_safe<T>(
    future: impl std::future::Future<Output = crate::kernel::runtime_host::VerletResult<T>>
    + Send
    + 'static,
) -> crate::kernel::runtime_host::VerletResult<T>
where
    T: Send + 'static,
{
    tokio::spawn(future).await.map_err(|error| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "sync endpoint transaction task failed: {error}"
        ))
    })?
}

/// Operator configuration for the embedded sync endpoint. EMO-429 wires
/// this value into the daemon's `[daemon.sync]` section.
///
/// `listen: None` means the endpoint is not served and the daemon is
/// local-only — exactly today's behavior; remote placement then fails
/// closed at `resolve_manifest_placement` naming this capability.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VerletDaemonSyncConfig {
    /// Listen address for the sync endpoint, same grammar as the app-server
    /// listen address.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen: Option<String>,
    /// Lease renewal deadline applied to new grants, in seconds.
    #[serde(default = "default_lease_ttl_secs")]
    pub lease_ttl_secs: u32,
}

impl Default for VerletDaemonSyncConfig {
    fn default() -> Self {
        Self {
            listen: None,
            lease_ttl_secs: default_lease_ttl_secs(),
        }
    }
}

impl VerletDaemonSyncConfig {
    /// Parse the configured endpoint address using the daemon app-server's
    /// listen grammar. `None` keeps the endpoint disabled.
    pub fn listen_addr(
        &self,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<crate::adapters::app_server::AppServerListenAddr>,
    > {
        self.listen
            .as_deref()
            .map(crate::adapters::app_server::AppServerListenAddr::parse)
            .transpose()
    }

    /// Validate the standalone sync configuration before the endpoint is
    /// started.
    pub fn validate(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        self.listen_addr()?;
        if self.lease_ttl_secs == 0 {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
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

    const RAW_PUSH_FIXTURE: &str = r#"{
        "schema":"cooldis.stream.sync_push/1",
        "stream_id":"thread:fixture",
        "lease_id":"lease-fixture",
        "expected_next_sequence":1,
        "records":[{
            "schema":"cooldis.stream.record/1",
            "event_id":"018f0000-0000-7000-8000-000000000001",
            "stream_id":"thread:fixture",
            "sequence":1,
            "coordinates":{
                "tenant_id":"tenant-fixture",
                "user_id":"user-fixture",
                "session_id":"session-fixture",
                "thread_id":"018f0000-0000-7000-8000-000000000002"
            },
            "created_at_ms":1700000000000,
            "kind":"session.entry.appended",
            "origin":"witnessed",
            "payload_schema":"cooldis.event.session.entry.appended/1",
            "payload":{"entry_id":"fixture"},
            "future_record_field":true
        }],
        "future_request_field":"ignored"
    }"#;

    const RAW_ACK_FIXTURE: &str = r#"{
        "outcome":"accepted",
        "ack":{
            "schema":"cooldis.stream.append_ack/1",
            "stream_id":"thread:fixture",
            "start_sequence":1,
            "end_sequence":1,
            "tail_sequence":1,
            "tail_event_id":"018f0000-0000-7000-8000-000000000001",
            "acks":["stream_committed","query_projected"],
            "future_ack_field":true
        },
        "future_outcome_field":"ignored"
    }"#;

    const RAW_REJECTION_FIXTURE: &str = r#"{
        "outcome":"rejected",
        "rejection":{
            "schema":"cooldis.stream.sync_push_rejection/1",
            "stream_id":"thread:fixture",
            "lease_id":"lease-fixture",
            "reason":{
                "reason":"sequence_fence_conflict",
                "actual_next_sequence":2,
                "future_reason_field":true
            },
            "rejected_at_ms":1700000000001,
            "future_rejection_field":true
        },
        "future_outcome_field":"ignored"
    }"#;

    #[test]
    fn rejection_reason_uses_stable_tagged_snake_case_wire_shape() {
        let reason = crate::daemon::remote_store::endpoint::SyncPushRejectionReason::CredentialLeaseMismatch {
            credential_lease_id: crate::daemon::remote_store::lease::StreamLeaseId::new("lease-1"),
        };
        assert_eq!(
            serde_json::to_value(reason).expect("reason should encode"),
            serde_json::json!({
                "reason": "credential_lease_mismatch",
                "credential_lease_id": "lease-1"
            })
        );
    }

    #[test]
    fn sync_config_rejects_zero_ttl() {
        let config = crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig {
            listen: None,
            lease_ttl_secs: 0,
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn raw_v1_push_ack_and_rejection_fixtures_are_stable_and_forward_decodable() {
        let push: crate::daemon::remote_store::endpoint::SyncPushRequestV1 =
            serde_json::from_str(RAW_PUSH_FIXTURE).unwrap();
        assert_eq!(
            push.schema,
            crate::daemon::remote_store::endpoint::SYNC_PUSH_SCHEMA_V1
        );
        assert_eq!(
            push.records[0].event_id.to_string(),
            "018f0000-0000-7000-8000-000000000001"
        );
        let encoded_push = serde_json::to_value(&push).unwrap();
        assert_eq!(
            encoded_push,
            serde_json::json!({
                "schema": "cooldis.stream.sync_push/1",
                "stream_id": "thread:fixture",
                "lease_id": "lease-fixture",
                "expected_next_sequence": 1,
                "records": [{
                    "schema": "cooldis.stream.record/1",
                    "event_id": "018f0000-0000-7000-8000-000000000001",
                    "stream_id": "thread:fixture",
                    "sequence": 1,
                    "coordinates": {
                        "tenant_id": "tenant-fixture",
                        "user_id": "user-fixture",
                        "session_id": "session-fixture",
                        "thread_id": "018f0000-0000-7000-8000-000000000002"
                    },
                    "created_at_ms": 1700000000000_i64,
                    "kind": "session.entry.appended",
                    "origin": "witnessed",
                    "payload_schema": "cooldis.event.session.entry.appended/1",
                    "provenance": {},
                    "payload": {"entry_id": "fixture"}
                }]
            })
        );

        let ack: crate::daemon::remote_store::endpoint::SyncPushOutcome =
            serde_json::from_str(RAW_ACK_FIXTURE).unwrap();
        assert!(matches!(
            ack,
            crate::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { ref ack }
                if ack.tail_sequence == verlet_history::EventSequence::new(1)
                    && ack.acks.len() == 2
        ));
        assert!(ack.matches_request(&push));
        let encoded_ack = serde_json::to_value(&ack).unwrap();
        assert_eq!(
            encoded_ack,
            serde_json::json!({
                "outcome": "accepted",
                "ack": {
                    "schema": "cooldis.stream.append_ack/1",
                    "stream_id": "thread:fixture",
                    "start_sequence": 1,
                    "end_sequence": 1,
                    "tail_sequence": 1,
                    "tail_event_id": "018f0000-0000-7000-8000-000000000001",
                    "acks": ["stream_committed", "query_projected"]
                }
            })
        );

        let rejection: crate::daemon::remote_store::endpoint::SyncPushOutcome =
            serde_json::from_str(RAW_REJECTION_FIXTURE).unwrap();
        assert!(matches!(
            rejection,
            crate::daemon::remote_store::endpoint::SyncPushOutcome::Rejected {
                rejection: crate::daemon::remote_store::endpoint::SyncPushRejectionV1 {
                    reason: crate::daemon::remote_store::endpoint::SyncPushRejectionReason::SequenceFenceConflict {
                        actual_next_sequence
                    },
                    ..
                }
            } if actual_next_sequence == verlet_history::EventSequence::new(2)
        ));
        assert!(rejection.matches_request(&push));
        let encoded_rejection = serde_json::to_value(&rejection).unwrap();
        assert_eq!(
            encoded_rejection,
            serde_json::json!({
                "outcome": "rejected",
                "rejection": {
                    "schema": "cooldis.stream.sync_push_rejection/1",
                    "stream_id": "thread:fixture",
                    "lease_id": "lease-fixture",
                    "reason": {
                        "reason": "sequence_fence_conflict",
                        "actual_next_sequence": 2
                    },
                    "rejected_at_ms": 1700000000001_i64
                }
            })
        );

        let mut invalid_ack = ack;
        let crate::daemon::remote_store::endpoint::SyncPushOutcome::Accepted { ack } =
            &mut invalid_ack
        else {
            unreachable!();
        };
        ack.tail_event_id = verlet_history::EventRecordId::new();
        assert!(!invalid_ack.matches_request(&push));
    }

    #[test]
    fn sync_wire_debug_redacts_live_lease_ids_without_changing_json() {
        let request: crate::daemon::remote_store::endpoint::SyncPushRequestV1 =
            serde_json::from_str(RAW_PUSH_FIXTURE).unwrap();
        assert!(!format!("{request:?}").contains("lease-fixture"));

        let outcome: crate::daemon::remote_store::endpoint::SyncPushOutcome =
            serde_json::from_str(RAW_REJECTION_FIXTURE).unwrap();
        assert!(!format!("{outcome:?}").contains("lease-fixture"));
        assert_eq!(
            serde_json::to_value(outcome).unwrap()["rejection"]["lease_id"],
            "lease-fixture"
        );
    }
}
