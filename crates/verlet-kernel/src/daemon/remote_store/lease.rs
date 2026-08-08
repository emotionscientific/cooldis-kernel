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

use sha2::Digest as _;

/// Wire schema identifier for [`StreamLeaseGrantV1`].
pub const SYNC_STREAM_LEASE_SCHEMA_V1: &str = "cooldis.stream.sync_lease/1";

/// Wire schema identifier for [`StreamWriteCredentialV1`].
pub const SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1: &str = "cooldis.stream.sync_write_credential/1";

/// Opaque identifier of one lease grant.
///
/// The id doubles as the fencing token a propagator presents on every push,
/// so it must be unguessable (mint from a CSPRNG, never sequential).
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
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
    pub fn authorizes(&self, stream_id: &verlet_history::EventStreamId) -> bool {
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
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StreamLeaseGrantV1 {
    pub schema: String,
    pub lease_id: StreamLeaseId,
    pub scope: StreamPrefixScope,
    /// Dispatch identity of the propagator this lease was granted to; ties
    /// the lease to the spawn/placement flow that carried it.
    pub holder_dispatch_id: verlet_runtime_contracts::handle::DispatchId,
    #[serde(default, skip_serializing_if = "StreamLeaseLineage::is_empty")]
    pub lineage: StreamLeaseLineage,
    pub granted_at_ms: i64,
    /// Renewal deadline. A lease past this instant fails the fence as
    /// [`LeaseFenceDecision::Expired`] until renewed or superseded.
    pub expires_at_ms: i64,
}

/// Decision of the push-time fence check for one presented lease.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
    Appended {
        ack: verlet_history::StreamAppendAckV1,
    },
    /// The lease did not authorize the append.
    LeaseRejected { fence: LeaseFenceDecision },
    /// The lease was current, but the durable stream tail did not match the
    /// caller's expected next sequence.
    SequenceFenceConflict {
        actual_next_sequence: verlet_history::EventSequence,
    },
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
#[async_trait::async_trait]
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
        holder_dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
        lineage: StreamLeaseLineage,
    ) -> crate::kernel::runtime_host::VerletResult<StreamLeaseGrantV1>;

    /// Extend the renewal deadline of a lease that is still the latest grant
    /// for its scope — including one whose deadline has already passed.
    /// Expiry is takeover eligibility, not authority loss: while no
    /// replacement grant has superseded it and it was not released, the
    /// holder is still the only propagator and renewal restores it after an
    /// offline window (the convergence law depends on this). Renewing a
    /// superseded, released, or unknown lease fails closed. Renewal
    /// serializes against [`Self::grant_lease`], so a takeover racing a
    /// comeback commits exactly one winner.
    async fn renew_lease(
        &self,
        lease_id: &StreamLeaseId,
    ) -> crate::kernel::runtime_host::VerletResult<StreamLeaseGrantV1>;

    /// Voluntarily end a lease (clean child shutdown). Releasing an
    /// already-superseded lease is a no-op, not an error.
    async fn release_lease(
        &self,
        lease_id: &StreamLeaseId,
    ) -> crate::kernel::runtime_host::VerletResult<()>;

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
        stream_id: &verlet_history::EventStreamId,
        presented: &StreamLeaseId,
    ) -> crate::kernel::runtime_host::VerletResult<LeaseFenceDecision>;

    /// Atomically check the lease, check the expected tail, and append the
    /// records. Grant, renewal, release, and supersession serialize with the
    /// whole operation, so an old lease cannot append after a replacement
    /// grant commits.
    async fn append_if_current(
        &self,
        stream_id: &verlet_history::EventStreamId,
        presented: &StreamLeaseId,
        expected_next_sequence: verlet_history::EventSequence,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> crate::kernel::runtime_host::VerletResult<LeaseFencedAppendOutcome>;
}

/// A scoped write credential as durably recorded.
///
/// The bearer token itself is secret material: it is returned exactly once
/// at mint time and never persisted in the clear — the store holds only a
/// digest sufficient for verification.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
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
#[async_trait::async_trait]
pub trait SyncCredentialAuthority: Send + Sync {
    /// Mint a credential bound to `grant`'s scope and lease. Returns the
    /// durable record and the bearer token; the token crosses to the child
    /// through the dispatch flow and is never seen again by this authority
    /// except as a digest.
    async fn mint_credential(
        &self,
        grant: &StreamLeaseGrantV1,
    ) -> crate::kernel::runtime_host::VerletResult<(StreamWriteCredentialV1, String)>;

    /// Resolve a presented bearer token. `Ok(None)` is the fail-closed
    /// answer for unknown or revoked tokens — the endpoint witnesses the
    /// rejection; only transport/store failures are `Err`.
    async fn verify_token(
        &self,
        token: &str,
    ) -> crate::kernel::runtime_host::VerletResult<Option<VerifiedPushIdentity>>;

    /// Revoke a credential (lease release or supersession retires the
    /// credentials minted for it).
    async fn revoke_credential(
        &self,
        credential_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<()>;
}

/// SQLite-backed durable authority for stream leases and their scoped write
/// credentials.
///
/// The authority shares the [`SqliteSessionStore`]'s engine handle so lease
/// rows, credential revocations, and event rows participate in one SQLite
/// writer order. Every read-then-write operation starts an `Immediate`
/// transaction before reading authority state. The only retained in-memory
/// values are immutable configuration and the injected clock; fence decisions
/// are always re-derived from durable rows.
#[derive(Clone)]
pub struct SqliteStreamLeaseAuthority {
    store: verlet_history_sqlite::SqliteSessionStore,
    clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    lease_ttl_ms: i64,
}

impl std::fmt::Debug for SqliteStreamLeaseAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteStreamLeaseAuthority")
            .field("lease_ttl_ms", &self.lease_ttl_ms)
            .finish_non_exhaustive()
    }
}

impl SqliteStreamLeaseAuthority {
    /// Initialize the daemon-owned authority tables in `store`.
    ///
    /// `clock` is the sole source of grant, renewal, release, revocation, and
    /// expiry time. Tests inject it; production passes [`crate::daemon::clock_route::SystemDaemonClock`].
    pub async fn new(
        store: verlet_history_sqlite::SqliteSessionStore,
        config: crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig,
        clock: std::sync::Arc<dyn crate::daemon::clock_route::DaemonClock>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        config.validate()?;
        let lease_ttl_ms = i64::from(config.lease_ttl_secs)
            .checked_mul(1_000)
            .ok_or_else(|| authority_error("daemon sync lease TTL overflows milliseconds"))?;
        let authority = Self {
            store,
            clock,
            lease_ttl_ms,
        };
        authority.init_schema().await?;
        Ok(authority)
    }

    async fn init_schema(&self) -> crate::kernel::runtime_host::VerletResult<()> {
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
                    CREATE TABLE IF NOT EXISTS cooldis_stream_leases (
                        lease_id TEXT PRIMARY KEY NOT NULL,
                        scope TEXT NOT NULL,
                        scope_generation INTEGER NOT NULL,
                        holder_dispatch_id TEXT NOT NULL,
                        predecessor_lease_id TEXT REFERENCES cooldis_stream_leases(lease_id),
                        granted_at_ms INTEGER NOT NULL,
                        expires_at_ms INTEGER NOT NULL,
                        released_at_ms INTEGER,
                        UNIQUE(scope, scope_generation)
                    );

                    CREATE INDEX IF NOT EXISTS idx_cooldis_stream_leases_scope_latest
                        ON cooldis_stream_leases(scope, scope_generation DESC);

                    CREATE TABLE IF NOT EXISTS cooldis_stream_write_credentials (
                        credential_id TEXT PRIMARY KEY NOT NULL,
                        token_digest TEXT UNIQUE NOT NULL,
                        scope TEXT NOT NULL,
                        lease_id TEXT NOT NULL REFERENCES cooldis_stream_leases(lease_id),
                        minted_at_ms INTEGER NOT NULL,
                        revoked_at_ms INTEGER
                    );

                    CREATE INDEX IF NOT EXISTS idx_cooldis_stream_write_credentials_lease
                        ON cooldis_stream_write_credentials(lease_id, revoked_at_ms);
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

#[derive(Clone, Debug, Eq, PartialEq)]
struct DurableLease {
    lease_id: StreamLeaseId,
    scope: StreamPrefixScope,
    scope_generation: i64,
    holder_dispatch_id: verlet_runtime_contracts::handle::DispatchId,
    predecessor_lease_id: Option<StreamLeaseId>,
    granted_at_ms: i64,
    expires_at_ms: i64,
    released_at_ms: Option<i64>,
}

impl DurableLease {
    fn grant(&self) -> StreamLeaseGrantV1 {
        StreamLeaseGrantV1 {
            schema: SYNC_STREAM_LEASE_SCHEMA_V1.to_string(),
            lease_id: self.lease_id.clone(),
            scope: self.scope.clone(),
            holder_dispatch_id: self.holder_dispatch_id.clone(),
            lineage: StreamLeaseLineage {
                superseded_lease_id: self.predecessor_lease_id.clone(),
            },
            granted_at_ms: self.granted_at_ms,
            expires_at_ms: self.expires_at_ms,
        }
    }
}

#[async_trait::async_trait]
impl StreamLeaseAuthority for SqliteStreamLeaseAuthority {
    async fn grant_lease(
        &self,
        scope: &StreamPrefixScope,
        holder_dispatch_id: &verlet_runtime_contracts::handle::DispatchId,
        lineage: StreamLeaseLineage,
    ) -> crate::kernel::runtime_host::VerletResult<StreamLeaseGrantV1> {
        if scope.as_str().is_empty() {
            return Err(authority_error("cannot grant an empty stream scope"));
        }
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let lease_ttl_ms = self.lease_ttl_ms;
        let scope = scope.clone();
        let holder_dispatch_id = holder_dispatch_id.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            let expires_at_ms = lease_expiry(now_ms, lease_ttl_ms)?;

            let predecessor = latest_lease_for_scope(&transaction, &scope).await?;
            let lineage_matches = match (&predecessor, &lineage.superseded_lease_id) {
                (None, None) => true,
                (Some(previous), Some(named)) => previous.lease_id == *named,
                _ => false,
            };
            if !lineage_matches {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error(
                    "lease lineage does not name the immediately preceding grant",
                ));
            }

            for live in latest_unreleased_leases(&transaction).await? {
                if live.scope != scope && scopes_overlap(&live.scope, &scope) {
                    transaction.rollback().await.map_err(storage_error)?;
                    return Err(authority_error(
                        "stream lease scope overlaps a different live scope",
                    ));
                }
            }

            let scope_generation = predecessor.as_ref().map_or(Ok(1), |previous| {
                previous
                    .scope_generation
                    .checked_add(1)
                    .ok_or_else(|| authority_error("lease scope generation overflow"))
            })?;
            let lease_id = StreamLeaseId::new(format!("lease_{}", uuid::Uuid::new_v4()));
            transaction
                .execute(
                    "INSERT INTO cooldis_stream_leases (
                        lease_id, scope, scope_generation, holder_dispatch_id,
                        predecessor_lease_id, granted_at_ms, expires_at_ms, released_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL)",
                    verlet_sqlite::params![
                        lease_id.as_str(),
                        scope.as_str(),
                        scope_generation,
                        holder_dispatch_id.as_str(),
                        lineage
                            .superseded_lease_id
                            .as_ref()
                            .map(StreamLeaseId::as_str),
                        now_ms,
                        expires_at_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            if let Some(previous) = &predecessor {
                revoke_lease_credentials(&transaction, &previous.lease_id, now_ms).await?;
            }
            transaction.commit().await.map_err(storage_error)?;
            Ok(StreamLeaseGrantV1 {
                schema: SYNC_STREAM_LEASE_SCHEMA_V1.to_string(),
                lease_id,
                scope,
                holder_dispatch_id,
                lineage,
                granted_at_ms: now_ms,
                expires_at_ms,
            })
        })
        .await
    }

    async fn renew_lease(
        &self,
        lease_id: &StreamLeaseId,
    ) -> crate::kernel::runtime_host::VerletResult<StreamLeaseGrantV1> {
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let lease_ttl_ms = self.lease_ttl_ms;
        let lease_id = lease_id.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let Some(mut lease) = lease_by_id(&transaction, &lease_id).await? else {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error("cannot renew an unknown stream lease"));
            };
            let latest = latest_lease_for_scope(&transaction, &lease.scope).await?;
            if lease.released_at_ms.is_some()
                || latest.as_ref().map(|row| &row.lease_id) != Some(&lease_id)
            {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error(
                    "cannot renew a superseded or released stream lease",
                ));
            }
            let now_ms = clock.now().timestamp_millis();
            let expires_at_ms = lease_expiry(now_ms, lease_ttl_ms)?.max(lease.expires_at_ms);
            transaction
                .execute(
                    "UPDATE cooldis_stream_leases
                     SET expires_at_ms = ?2
                     WHERE lease_id = ?1",
                    verlet_sqlite::params![lease_id.as_str(), expires_at_ms],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            lease.expires_at_ms = expires_at_ms;
            Ok(lease.grant())
        })
        .await
    }

    async fn release_lease(
        &self,
        lease_id: &StreamLeaseId,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let lease_id = lease_id.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            if let Some(lease) = lease_by_id(&transaction, &lease_id).await? {
                let latest = latest_lease_for_scope(&transaction, &lease.scope).await?;
                if lease.released_at_ms.is_none()
                    && latest.as_ref().map(|row| &row.lease_id) == Some(&lease_id)
                {
                    transaction
                        .execute(
                            "UPDATE cooldis_stream_leases
                             SET released_at_ms = ?2
                             WHERE lease_id = ?1",
                            verlet_sqlite::params![lease_id.as_str(), now_ms],
                        )
                        .await
                        .map_err(storage_error)?;
                    revoke_lease_credentials(&transaction, &lease_id, now_ms).await?;
                }
            }
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn check_fence(
        &self,
        stream_id: &verlet_history::EventStreamId,
        presented: &StreamLeaseId,
    ) -> crate::kernel::runtime_host::VerletResult<LeaseFenceDecision> {
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let stream_id = stream_id.clone();
        let presented = presented.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Deferred)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            let decision = fence_decision(&transaction, &stream_id, &presented, now_ms).await?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(decision)
        })
        .await
    }

    async fn append_if_current(
        &self,
        stream_id: &verlet_history::EventStreamId,
        presented: &StreamLeaseId,
        expected_next_sequence: verlet_history::EventSequence,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> crate::kernel::runtime_host::VerletResult<LeaseFencedAppendOutcome> {
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let stream_id = stream_id.clone();
        let presented = presented.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            let decision = fence_decision(&transaction, &stream_id, &presented, now_ms).await?;
            if decision != LeaseFenceDecision::Current {
                transaction.commit().await.map_err(storage_error)?;
                return Ok(LeaseFencedAppendOutcome::LeaseRejected { fence: decision });
            }

            let append = store
                .append_events_fenced_in_transaction(
                    &transaction,
                    &stream_id,
                    expected_next_sequence,
                    records,
                )
                .await;
            let appended = match append {
                Ok(appended) => appended,
                Err(verlet_history::HistoryError::AppendFenceConflict {
                    actual_next_sequence,
                    ..
                }) => {
                    transaction.rollback().await.map_err(storage_error)?;
                    return Ok(LeaseFencedAppendOutcome::SequenceFenceConflict {
                        actual_next_sequence: verlet_history::EventSequence::new(
                            actual_next_sequence,
                        ),
                    });
                }
                Err(error) => {
                    transaction.rollback().await.map_err(storage_error)?;
                    return Err(crate::kernel::runtime_host::VerletError::History(
                        error.to_string(),
                    ));
                }
            };
            let ack = match verlet_history::StreamAppendAckV1::from_appended(
                stream_id,
                &appended,
                vec![
                    verlet_history::StreamAckClass::StreamCommitted,
                    verlet_history::StreamAckClass::QueryProjected,
                ],
            ) {
                Ok(ack) => ack,
                Err(error) => {
                    transaction.rollback().await.map_err(storage_error)?;
                    return Err(crate::kernel::runtime_host::VerletError::History(
                        error.to_string(),
                    ));
                }
            };
            transaction.commit().await.map_err(storage_error)?;
            Ok(LeaseFencedAppendOutcome::Appended { ack })
        })
        .await
    }
}

#[async_trait::async_trait]
impl SyncCredentialAuthority for SqliteStreamLeaseAuthority {
    async fn mint_credential(
        &self,
        grant: &StreamLeaseGrantV1,
    ) -> crate::kernel::runtime_host::VerletResult<(StreamWriteCredentialV1, String)> {
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let grant = grant.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            let durable = lease_by_id(&transaction, &grant.lease_id).await?;
            let latest = latest_lease_for_scope(&transaction, &grant.scope).await?;
            if grant.schema != SYNC_STREAM_LEASE_SCHEMA_V1
                || durable.as_ref().is_none_or(|lease| {
                    lease.released_at_ms.is_some() || lease.scope != grant.scope
                })
                || latest.as_ref().map(|lease| &lease.lease_id) != Some(&grant.lease_id)
            {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error(
                    "cannot mint a credential for a non-current stream lease grant",
                ));
            }

            let credential_id = format!("credential_{}", uuid::Uuid::new_v4());
            let token = format!(
                "cooldis_sync_{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            let token_digest = token_digest(&token);
            transaction
                .execute(
                    "INSERT INTO cooldis_stream_write_credentials (
                        credential_id, token_digest, scope, lease_id, minted_at_ms, revoked_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, NULL)",
                    verlet_sqlite::params![
                        credential_id.as_str(),
                        token_digest,
                        grant.scope.as_str(),
                        grant.lease_id.as_str(),
                        now_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok((
                StreamWriteCredentialV1 {
                    schema: SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1.to_string(),
                    credential_id,
                    scope: grant.scope,
                    lease_id: grant.lease_id,
                    minted_at_ms: now_ms,
                },
                token,
            ))
        })
        .await
    }

    async fn verify_token(
        &self,
        token: &str,
    ) -> crate::kernel::runtime_host::VerletResult<Option<VerifiedPushIdentity>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let digest = token_digest(token);
        let mut rows = connection
            .query(
                "SELECT credential_id, scope, lease_id
                 FROM cooldis_stream_write_credentials AS credential
                 WHERE credential.token_digest = ?1
                   AND credential.revoked_at_ms IS NULL
                   AND EXISTS (
                       SELECT 1
                       FROM cooldis_stream_leases AS lease
                       WHERE lease.lease_id = credential.lease_id
                         AND lease.scope = credential.scope
                         AND lease.released_at_ms IS NULL
                         AND lease.scope_generation = (
                             SELECT MAX(latest.scope_generation)
                             FROM cooldis_stream_leases AS latest
                             WHERE latest.scope = lease.scope
                         )
                   )
                 LIMIT 1",
                verlet_sqlite::params![digest],
            )
            .await
            .map_err(storage_error)?;
        let identity = match rows.next().await.map_err(storage_error)? {
            Some(row) => Some(VerifiedPushIdentity {
                credential_id: row.get(0).map_err(storage_error)?,
                scope: StreamPrefixScope::new(row.get::<String>(1).map_err(storage_error)?),
                lease_id: StreamLeaseId::new(row.get::<String>(2).map_err(storage_error)?),
            }),
            None => None,
        };
        Ok(identity)
    }

    async fn revoke_credential(
        &self,
        credential_id: &str,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let store = self.store.clone();
        let clock = std::sync::Arc::clone(&self.clock);
        let credential_id = credential_id.to_string();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            transaction
                .execute(
                    "UPDATE cooldis_stream_write_credentials
                     SET revoked_at_ms = COALESCE(revoked_at_ms, ?2)
                     WHERE credential_id = ?1",
                    verlet_sqlite::params![credential_id, now_ms],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }
}

async fn fence_decision(
    connection: &verlet_sqlite::Connection,
    stream_id: &verlet_history::EventStreamId,
    presented: &StreamLeaseId,
    now_ms: i64,
) -> crate::kernel::runtime_host::VerletResult<LeaseFenceDecision> {
    let Some(lease) = lease_by_id(connection, presented).await? else {
        return Ok(LeaseFenceDecision::Unknown);
    };
    if !lease.scope.authorizes(stream_id) {
        return Ok(LeaseFenceDecision::Unknown);
    }
    let latest = latest_lease_for_scope(connection, &lease.scope).await?;
    if lease.released_at_ms.is_some() || latest.as_ref().map(|row| &row.lease_id) != Some(presented)
    {
        return Ok(LeaseFenceDecision::Superseded);
    }
    let covering = latest_unreleased_leases(connection)
        .await?
        .into_iter()
        .filter(|candidate| candidate.scope.authorizes(stream_id))
        .collect::<Vec<_>>();
    if covering.len() != 1 || covering[0].lease_id != *presented {
        return Ok(LeaseFenceDecision::Superseded);
    }
    if now_ms >= lease.expires_at_ms {
        return Ok(LeaseFenceDecision::Expired);
    }
    Ok(LeaseFenceDecision::Current)
}

async fn lease_by_id(
    connection: &verlet_sqlite::Connection,
    lease_id: &StreamLeaseId,
) -> crate::kernel::runtime_host::VerletResult<Option<DurableLease>> {
    let mut rows = connection
        .query(
            "SELECT lease_id, scope, scope_generation, holder_dispatch_id,
                    predecessor_lease_id, granted_at_ms, expires_at_ms, released_at_ms
             FROM cooldis_stream_leases
             WHERE lease_id = ?1
             LIMIT 1",
            verlet_sqlite::params![lease_id.as_str()],
        )
        .await
        .map_err(storage_error)?;
    rows.next()
        .await
        .map_err(storage_error)?
        .as_ref()
        .map(durable_lease_from_row)
        .transpose()
}

async fn latest_lease_for_scope(
    connection: &verlet_sqlite::Connection,
    scope: &StreamPrefixScope,
) -> crate::kernel::runtime_host::VerletResult<Option<DurableLease>> {
    let mut rows = connection
        .query(
            "SELECT lease_id, scope, scope_generation, holder_dispatch_id,
                    predecessor_lease_id, granted_at_ms, expires_at_ms, released_at_ms
             FROM cooldis_stream_leases
             WHERE scope = ?1
             ORDER BY scope_generation DESC
             LIMIT 1",
            verlet_sqlite::params![scope.as_str()],
        )
        .await
        .map_err(storage_error)?;
    rows.next()
        .await
        .map_err(storage_error)?
        .as_ref()
        .map(durable_lease_from_row)
        .transpose()
}

async fn latest_unreleased_leases(
    connection: &verlet_sqlite::Connection,
) -> crate::kernel::runtime_host::VerletResult<Vec<DurableLease>> {
    let mut rows = connection
        .query(
            "SELECT lease.lease_id, lease.scope, lease.scope_generation,
                    lease.holder_dispatch_id, lease.predecessor_lease_id,
                    lease.granted_at_ms, lease.expires_at_ms, lease.released_at_ms
             FROM cooldis_stream_leases AS lease
             WHERE lease.released_at_ms IS NULL
               AND lease.scope_generation = (
                   SELECT MAX(latest.scope_generation)
                   FROM cooldis_stream_leases AS latest
                   WHERE latest.scope = lease.scope
               )
             ORDER BY lease.scope",
            (),
        )
        .await
        .map_err(storage_error)?;
    let mut leases = Vec::new();
    while let Some(row) = rows.next().await.map_err(storage_error)? {
        leases.push(durable_lease_from_row(&row)?);
    }
    Ok(leases)
}

fn durable_lease_from_row(
    row: &verlet_sqlite::Row,
) -> crate::kernel::runtime_host::VerletResult<DurableLease> {
    Ok(DurableLease {
        lease_id: StreamLeaseId::new(row.get::<String>(0).map_err(storage_error)?),
        scope: StreamPrefixScope::new(row.get::<String>(1).map_err(storage_error)?),
        scope_generation: row.get(2).map_err(storage_error)?,
        holder_dispatch_id: verlet_runtime_contracts::handle::DispatchId::new(
            row.get::<String>(3).map_err(storage_error)?,
        ),
        predecessor_lease_id: row
            .get::<Option<String>>(4)
            .map_err(storage_error)?
            .map(StreamLeaseId::new),
        granted_at_ms: row.get(5).map_err(storage_error)?,
        expires_at_ms: row.get(6).map_err(storage_error)?,
        released_at_ms: row.get(7).map_err(storage_error)?,
    })
}

async fn revoke_lease_credentials(
    connection: &verlet_sqlite::Connection,
    lease_id: &StreamLeaseId,
    revoked_at_ms: i64,
) -> crate::kernel::runtime_host::VerletResult<()> {
    connection
        .execute(
            "UPDATE cooldis_stream_write_credentials
             SET revoked_at_ms = COALESCE(revoked_at_ms, ?2)
             WHERE lease_id = ?1",
            verlet_sqlite::params![lease_id.as_str(), revoked_at_ms],
        )
        .await
        .map_err(storage_error)?;
    Ok(())
}

fn scopes_overlap(left: &StreamPrefixScope, right: &StreamPrefixScope) -> bool {
    prefix_contains(left.as_str(), right.as_str()) || prefix_contains(right.as_str(), left.as_str())
}

fn prefix_contains(prefix: &str, candidate: &str) -> bool {
    if prefix.is_empty() {
        return false;
    }
    let Some(suffix) = candidate.strip_prefix(prefix) else {
        return false;
    };
    suffix.is_empty() || suffix.starts_with(':')
}

fn token_digest(token: &str) -> String {
    format!("sha256:{:x}", sha2::Sha256::digest(token.as_bytes()))
}

fn lease_expiry(now_ms: i64, lease_ttl_ms: i64) -> crate::kernel::runtime_host::VerletResult<i64> {
    now_ms
        .checked_add(lease_ttl_ms)
        .ok_or_else(|| authority_error("lease expiry timestamp overflow"))
}

fn authority_error(message: impl Into<String>) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::History(message.into())
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
            "sqlite authority transaction task failed: {error}"
        ))
    })?
}

#[cfg(test)]
mod tests {
    use crate::daemon::remote_store::lease::StreamLeaseAuthority as _;

    struct FixedClock {
        now_ms: i64,
    }

    impl crate::daemon::clock_route::DaemonClock for FixedClock {
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            chrono::DateTime::from_timestamp_millis(self.now_ms)
                .expect("test timestamp should be representable")
        }
    }

    #[test]
    fn prefix_scope_authorizes_only_its_prefix() {
        let scope = crate::daemon::remote_store::lease::StreamPrefixScope::new("thread:child-7");
        assert!(scope.authorizes(&verlet_history::EventStreamId::new("thread:child-7")));
        assert!(scope.authorizes(&verlet_history::EventStreamId::new("thread:child-7:trace")));
        assert!(!scope.authorizes(&verlet_history::EventStreamId::new("thread:child-70")));
        assert!(!scope.authorizes(&verlet_history::EventStreamId::new("thread:child-8")));
        assert!(!scope.authorizes(&verlet_history::EventStreamId::new("daemon:control")));
    }

    #[test]
    fn empty_prefix_scope_authorizes_nothing() {
        let scope = crate::daemon::remote_store::lease::StreamPrefixScope::new("");
        assert!(!scope.authorizes(&verlet_history::EventStreamId::new("thread:child-7")));
    }

    #[test]
    fn only_current_fence_decision_permits_push() {
        assert!(crate::daemon::remote_store::lease::LeaseFenceDecision::Current.permits_push());
        assert!(!crate::daemon::remote_store::lease::LeaseFenceDecision::Superseded.permits_push());
        assert!(!crate::daemon::remote_store::lease::LeaseFenceDecision::Expired.permits_push());
        assert!(!crate::daemon::remote_store::lease::LeaseFenceDecision::Unknown.permits_push());
    }

    #[test]
    fn lease_grant_decodes_without_lineage_and_ignores_future_optional_fields() {
        let grant: crate::daemon::remote_store::lease::StreamLeaseGrantV1 =
            serde_json::from_value(serde_json::json!({
                "schema": crate::daemon::remote_store::lease::SYNC_STREAM_LEASE_SCHEMA_V1,
                "lease_id": "lease-1",
                "scope": "thread:child-7",
                "holder_dispatch_id": "dispatch-1",
                "granted_at_ms": 10,
                "expires_at_ms": 70,
                "future_optional_field": "ignored"
            }))
            .expect("V1 grant should decode without optional lineage");

        assert_eq!(
            grant.lineage,
            crate::daemon::remote_store::lease::StreamLeaseLineage::default()
        );
        let encoded = serde_json::to_value(grant).expect("grant should encode");
        assert_eq!(
            encoded,
            serde_json::json!({
                "schema": crate::daemon::remote_store::lease::SYNC_STREAM_LEASE_SCHEMA_V1,
                "lease_id": "lease-1",
                "scope": "thread:child-7",
                "holder_dispatch_id": "dispatch-1",
                "granted_at_ms": 10,
                "expires_at_ms": 70,
            })
        );
    }

    #[test]
    fn write_credential_v1_encoding_is_stable_and_forward_decodable() {
        let fixture = serde_json::json!({
            "schema": crate::daemon::remote_store::lease::SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1,
            "credential_id": "credential-1",
            "scope": "thread:child-7",
            "lease_id": "lease-1",
            "minted_at_ms": 10,
            "future_optional_field": "ignored",
        });
        let credential: crate::daemon::remote_store::lease::StreamWriteCredentialV1 =
            serde_json::from_value(fixture).expect("V1 credential fixture should decode");

        assert_eq!(
            serde_json::to_value(credential).expect("credential should encode"),
            serde_json::json!({
                "schema": crate::daemon::remote_store::lease::SYNC_STREAM_WRITE_CREDENTIAL_SCHEMA_V1,
                "credential_id": "credential-1",
                "scope": "thread:child-7",
                "lease_id": "lease-1",
                "minted_at_ms": 10,
            })
        );
    }

    #[tokio::test]
    async fn sqlite_authority_uses_the_injected_clock_for_durable_grants() {
        let store = verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap();
        let authority = crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
            store,
            crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig {
                lease_ttl_secs: 5,
                ..crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default()
            },
            std::sync::Arc::new(FixedClock { now_ms: 12_000 }),
        )
        .await
        .unwrap();
        let scope = crate::daemon::remote_store::lease::StreamPrefixScope::new("thread:unit-child");
        let grant = authority
            .grant_lease(
                &scope,
                &verlet_runtime_contracts::handle::DispatchId::new("dispatch-unit"),
                crate::daemon::remote_store::lease::StreamLeaseLineage::default(),
            )
            .await
            .unwrap();

        assert_eq!(grant.granted_at_ms, 12_000);
        assert_eq!(grant.expires_at_ms, 17_000);
        assert_eq!(
            authority
                .check_fence(
                    &verlet_history::EventStreamId::new(scope.as_str()),
                    &grant.lease_id
                )
                .await
                .unwrap(),
            crate::daemon::remote_store::lease::LeaseFenceDecision::Current
        );
    }
}
