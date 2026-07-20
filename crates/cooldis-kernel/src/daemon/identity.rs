//! Identity plane v0: principals, credentials, and boundary authority.
//!
//! Design: `docs/adr/0008-identity-plane-v0.md` (accepted). The kernel is an
//! identity receptor, not an identity provider: this module defines the slot
//! a "who" plugs into (named principals, hash-only credentials, authority
//! classes) and the witnessed records that make boundary decisions part of
//! the permanent record. Nothing here provides identities; identity services
//! live above the kernel.
//!
//! The storage pattern follows the one existing inbound-auth surface,
//! `SyncCredentialAuthority` (`daemon/remote_store/lease.rs`): CSPRNG-minted
//! bearer secrets returned exactly once, digest-only persistence, and
//! append-only witnessed records with `.../1` schema ids. Identity records
//! are daemon-authority facts (dedicated durable tables), not thread-stream
//! events: the `EventKind` vocabulary is thread-scoped and frozen, and a
//! boundary session has no thread.

use async_trait::async_trait;
use cooldis_sqlite::{Connection, Row, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::future::Future;
use std::sync::Arc;
use uuid::Uuid;

use crate::{CooldisError, CooldisResult, DaemonClock, SqliteSessionStore};

/// Schema id for a principal declaration/revocation record.
pub const IDENTITY_PRINCIPAL_SCHEMA_V1: &str = "cooldis.identity.principal/1";
/// Schema id for a credential mint/revocation record.
pub const IDENTITY_CREDENTIAL_SCHEMA_V1: &str = "cooldis.identity.credential/1";
/// Schema id for a boundary session open/close witness.
pub const IDENTITY_SESSION_SCHEMA_V1: &str = "cooldis.identity.session/1";
/// Schema id for a witnessed authentication/authorization rejection.
pub const IDENTITY_AUTH_REJECTION_SCHEMA_V1: &str = "cooldis.identity.auth_rejection/1";

/// A named identity within the tenant (ADR 0008 D1).
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PrincipalId(String);

impl PrincipalId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PrincipalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The kind of a principal, from which its authority derives (ADR 0008 D1).
///
/// `Member` is schema-reserved and unimplemented in v0: the variant exists so
/// its later arrival is additive, but [`PrincipalKind::is_declarable`] is
/// false for it and declaration must be rejected. End users reach agents
/// through adapters; the envelope records them as adapter testimony.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    /// Full host authority plus interactive use: whoever operates the daemon.
    Operator,
    /// Ingress submission only: a bridge, webhook, or the scheduler sidecar.
    Adapter,
    /// Reserved, unimplemented (ADR 0008 D1). Declaration is rejected in v0.
    Member,
}

impl PrincipalKind {
    /// Whether a principal of this kind may be declared in v0.
    pub fn is_declarable(self) -> bool {
        !matches!(self, Self::Member)
    }

    /// Whether this kind reaches the given authority class (ADR 0008 D4).
    pub fn permits(self, class: AuthorityClass) -> bool {
        match self {
            Self::Operator => true,
            Self::Adapter => matches!(class, AuthorityClass::Ingress),
            Self::Member => matches!(class, AuthorityClass::Interactive | AuthorityClass::Ingress),
        }
    }
}

/// The authority class of a JSON-RPC method (ADR 0008 D4). The taxonomy is
/// the durable part of the design; kinds map onto it via
/// [`PrincipalKind::permits`].
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthorityClass {
    /// Touches the host: command exec, filesystem, process handles, secrets,
    /// debug. Operator only.
    Host,
    /// Interactive thread and agent use, and reads.
    Interactive,
    /// Envelope submission at the ingress boundary.
    Ingress,
}

/// Classify a dispatcher method name into its authority class.
///
/// Precedence (ADR 0008 D4): the explicit host list wins over prefix rules
/// (`thread/shellCommand` is host authority even though `thread/*` is
/// interactive); a method matching nothing classifies as `Host`, so an
/// unclassified method fails closed to operator-only.
///
/// The implementation ticket must reconcile this table against every arm of
/// `dispatch_request` (`adapters/app_server/connection.rs`) and pin the
/// reconciliation with an exhaustive test: every dispatchable method appears
/// here deliberately, or the test fails.
pub fn authority_class_for_method(method: &str) -> AuthorityClass {
    const HOST_METHODS: &[&str] = &["thread/shellCommand"];
    const HOST_PREFIXES: &[&str] = &[
        "command/",
        "fs/",
        "process/",
        "modelProvider/auth/",
        "mcpSource/",
        "debug/",
    ];
    const INTERACTIVE_PREFIXES: &[&str] = &["thread/", "turn/", "mandate/", "agent/", "session/"];
    const INGRESS_PREFIXES: &[&str] = &["ingress/", "io/ingress/"];

    if HOST_METHODS.contains(&method) || HOST_PREFIXES.iter().any(|p| method.starts_with(p)) {
        return AuthorityClass::Host;
    }
    if INGRESS_PREFIXES.iter().any(|p| method.starts_with(p)) {
        return AuthorityClass::Ingress;
    }
    if INTERACTIVE_PREFIXES.iter().any(|p| method.starts_with(p)) {
        return AuthorityClass::Interactive;
    }
    AuthorityClass::Host
}

/// A declared principal, as persisted (ADR 0008 D1). Declaration and
/// revocation are append-only witnessed records; the in-memory boundary set
/// is rebuilt from them and refreshed on change.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct PrincipalRecordV1 {
    pub schema: String,
    pub principal_id: PrincipalId,
    pub kind: PrincipalKind,
    pub display: String,
    /// The principal that declared this one: the attestation trail's root
    /// link. The bootstrap operator names itself.
    pub declared_by: PrincipalId,
    pub declared_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<i64>,
}

/// A credential binding a bearer secret to exactly one principal (ADR 0008
/// D2). The secret is returned once at mint and persisted only as a digest.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCredentialV1 {
    pub schema: String,
    pub credential_id: String,
    pub principal_id: PrincipalId,
    /// `sha256:<hex>` digest of the bearer secret. Never the secret itself.
    pub token_digest: String,
    /// The principal that authorized the mint ("who granted what").
    pub minted_by: PrincipalId,
    pub minted_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revoked_at_ms: Option<i64>,
}

/// Digest a bearer secret for persistence or lookup. Same convention as the
/// stream-sync authority (`daemon/remote_store/lease.rs`): high-entropy
/// random secrets, so a plain SHA-256 digest is sufficient and no
/// password-style slow hash applies.
pub fn identity_token_digest(token: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(token.as_bytes()))
}

/// How a connection proved itself (ADR 0008 D3).
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthenticationPath {
    /// A bearer token verified against the credential set.
    Credential { credential_id: String },
    /// Same-uid peer mapping on the Unix socket: local mode only.
    PeerUid { uid: u32 },
}

/// The identity a live connection carries after authentication. Attached to
/// the connection state and consulted at the dispatcher choke point before
/// every method (ADR 0008 D3/D4). A connection without one is admitted to no
/// method.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPrincipal {
    pub principal_id: PrincipalId,
    pub kind: PrincipalKind,
    pub auth: AuthenticationPath,
}

/// The boundary surface a session or rejection was witnessed on.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundarySurface {
    UnixSocket,
    Websocket,
    Console,
}

/// A witnessed boundary session (ADR 0008 D6): open and close of one
/// authenticated connection.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentitySessionV1 {
    pub schema: String,
    pub session_id: String,
    pub principal_id: PrincipalId,
    pub kind: PrincipalKind,
    pub surface: BoundarySurface,
    /// Credential id for token auth; `peer_uid:<uid>` for the local-mode
    /// peer mapping (which uses no credential).
    pub credential_ref: String,
    pub opened_at_ms: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub closed_at_ms: Option<i64>,
}

/// Why a boundary request was refused (ADR 0008 D3/D4). Persisted as a
/// witnessed rejection so pre-guard intrusion attempts are not invisible;
/// follows `SyncPushRejectionReason` (`daemon/remote_store/endpoint.rs`).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub enum IdentityAuthRejectionReason {
    /// No credential matches the presented token.
    CredentialUnknown,
    CredentialExpired {
        credential_id: String,
    },
    CredentialRevoked {
        credential_id: String,
    },
    PrincipalRevoked {
        principal_id: PrincipalId,
    },
    /// Authenticated, but the method is above the principal's authority.
    MethodNotAuthorized {
        method: String,
        class: AuthorityClass,
    },
    /// A same-uid peer connected while the mapping is off (managed mode).
    PeerMappingDisabled {
        uid: u32,
    },
}

/// A witnessed authentication/authorization rejection (ADR 0008 D6).
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityAuthRejectionV1 {
    pub schema: String,
    pub surface: BoundarySurface,
    pub reason: IdentityAuthRejectionReason,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal_id: Option<PrincipalId>,
    pub rejected_at_ms: i64,
}

/// The daemon's identity authority (ADR 0008 D1/D2/D6): the single owner of
/// principal and credential records and of boundary witnessing.
///
/// The implementation (`SqliteIdentityAuthority`, implementation ticket 1)
/// follows `SqliteStreamLeaseAuthority` (`daemon/remote_store/lease.rs`):
/// dedicated append-only tables on the daemon session store, an injected
/// clock, digest-only credential persistence, and `Ok(None)` as the
/// fail-closed verification outcome. Test conventions: inline unit tests
/// against `SqliteSessionStore::in_memory()` with an injected clock, plus an
/// integration file alongside `tests/remote_store_lease.rs`.
#[async_trait]
pub trait IdentityAuthority: Send + Sync {
    /// Declare a principal. Rejects kinds where
    /// [`PrincipalKind::is_declarable`] is false (the reserved `member`).
    async fn declare_principal(
        &self,
        declared_by: &PrincipalId,
        principal_id: &PrincipalId,
        kind: PrincipalKind,
        display: &str,
    ) -> CooldisResult<PrincipalRecordV1>;

    async fn revoke_principal(
        &self,
        revoked_by: &PrincipalId,
        principal_id: &PrincipalId,
    ) -> CooldisResult<()>;

    /// Mint a credential for a principal. Returns the persisted record and
    /// the bearer secret, which is shown exactly once and never persisted.
    async fn mint_credential(
        &self,
        minted_by: &PrincipalId,
        principal_id: &PrincipalId,
        expires_at_ms: Option<i64>,
    ) -> CooldisResult<(IdentityCredentialV1, String)>;

    async fn revoke_credential(
        &self,
        revoked_by: &PrincipalId,
        credential_id: &str,
    ) -> CooldisResult<()>;

    /// Resolve a bearer token to a principal. `Ok(None)` is the fail-closed
    /// rejection outcome: unknown, expired, or revoked credentials and
    /// revoked principals all resolve to nothing.
    async fn verify_token(&self, token: &str) -> CooldisResult<Option<ResolvedPrincipal>>;

    /// Resolve a same-uid Unix peer to the operator principal. Local mode
    /// only (ADR 0008 D3): in managed mode this returns `Ok(None)` and the
    /// attempt is witnessed with [`IdentityAuthRejectionReason::PeerMappingDisabled`].
    async fn resolve_peer_uid(&self, uid: u32) -> CooldisResult<Option<ResolvedPrincipal>>;

    async fn list_principals(&self) -> CooldisResult<Vec<PrincipalRecordV1>>;

    async fn list_credentials(
        &self,
        principal_id: &PrincipalId,
    ) -> CooldisResult<Vec<IdentityCredentialV1>>;

    /// Witness a boundary session opening (ADR 0008 D6).
    async fn witness_session_opened(&self, session: &IdentitySessionV1) -> CooldisResult<()>;

    /// Witness a boundary session closing.
    async fn witness_session_closed(
        &self,
        session_id: &str,
        closed_at_ms: i64,
    ) -> CooldisResult<()>;

    /// Witness a rejected authentication or authorization attempt.
    async fn witness_auth_rejected(&self, rejection: &IdentityAuthRejectionV1)
    -> CooldisResult<()>;
}

/// SQLite-backed durable authority for identity-plane records.
///
/// The authority shares the [`SqliteSessionStore`]'s engine handle. Every
/// read-then-write mutation begins an immediate transaction, all time comes
/// from the injected [`DaemonClock`], and bearer secrets are retained only as
/// SHA-256 digests.
#[derive(Clone)]
pub struct SqliteIdentityAuthority {
    store: SqliteSessionStore,
    clock: Arc<dyn DaemonClock>,
    peer_operator: Option<PrincipalId>,
}

impl std::fmt::Debug for SqliteIdentityAuthority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteIdentityAuthority")
            .field("peer_mapping_enabled", &self.peer_operator.is_some())
            .finish_non_exhaustive()
    }
}

impl SqliteIdentityAuthority {
    /// Initialize the daemon-owned identity tables in `store`.
    ///
    /// `peer_operator` enables the local-mode same-uid lookup and names the
    /// operator it resolves to. The boundary remains responsible for proving
    /// that the supplied uid belongs to the daemon user.
    pub async fn new(
        store: SqliteSessionStore,
        clock: Arc<dyn DaemonClock>,
        peer_operator: Option<PrincipalId>,
    ) -> CooldisResult<Self> {
        let authority = Self {
            store,
            clock,
            peer_operator,
        };
        authority.init_schema().await?;
        Ok(authority)
    }

    /// Declare the first operator and mint its credential atomically.
    ///
    /// The returned bearer secret is shown once by the CLI and is never
    /// persisted by this authority.
    pub async fn bootstrap_operator(
        &self,
        principal_id: &PrincipalId,
        display: &str,
    ) -> CooldisResult<(PrincipalRecordV1, IdentityCredentialV1, String)> {
        let store = self.store.clone();
        let clock = Arc::clone(&self.clock);
        let principal_id = principal_id.clone();
        let display = display.to_string();
        let credential_id = format!("credential_{}", Uuid::new_v4());
        let token = mint_identity_secret()?;
        let token_digest = identity_token_digest(&token);
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            if active_operator_exists(&transaction).await? {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error(
                    "identity bootstrap refused because an active operator already exists",
                ));
            }
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_principals (
                        schema, principal_id, kind, display, declared_by, declared_at_ms,
                        revoked_at_ms, revoked_by, bootstrap_root
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, 1)",
                    params![
                        IDENTITY_PRINCIPAL_SCHEMA_V1,
                        principal_id.as_str(),
                        principal_kind_text(PrincipalKind::Operator),
                        display.as_str(),
                        principal_id.as_str(),
                        now_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_credentials (
                        schema, credential_id, principal_id, token_digest, minted_by,
                        minted_at_ms, expires_at_ms, revoked_at_ms, revoked_by
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL)",
                    params![
                        IDENTITY_CREDENTIAL_SCHEMA_V1,
                        credential_id.as_str(),
                        principal_id.as_str(),
                        token_digest.as_str(),
                        principal_id.as_str(),
                        now_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok((
                PrincipalRecordV1 {
                    schema: IDENTITY_PRINCIPAL_SCHEMA_V1.to_string(),
                    principal_id: principal_id.clone(),
                    kind: PrincipalKind::Operator,
                    display,
                    declared_by: principal_id.clone(),
                    declared_at_ms: now_ms,
                    revoked_at_ms: None,
                },
                IdentityCredentialV1 {
                    schema: IDENTITY_CREDENTIAL_SCHEMA_V1.to_string(),
                    credential_id,
                    principal_id: principal_id.clone(),
                    token_digest,
                    minted_by: principal_id,
                    minted_at_ms: now_ms,
                    expires_at_ms: None,
                    revoked_at_ms: None,
                },
                token,
            ))
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
                    CREATE TABLE IF NOT EXISTS cooldis_identity_principals (
                        schema TEXT NOT NULL,
                        principal_id TEXT PRIMARY KEY NOT NULL,
                        kind TEXT NOT NULL,
                        display TEXT NOT NULL,
                        declared_by TEXT NOT NULL,
                        declared_at_ms INTEGER NOT NULL,
                        revoked_at_ms INTEGER,
                        revoked_by TEXT,
                        bootstrap_root INTEGER NOT NULL DEFAULT 0
                            CHECK (bootstrap_root IN (0, 1))
                    );

                    CREATE UNIQUE INDEX IF NOT EXISTS idx_cooldis_identity_active_bootstrap_root
                        ON cooldis_identity_principals(bootstrap_root)
                        WHERE bootstrap_root = 1 AND revoked_at_ms IS NULL;

                    CREATE INDEX IF NOT EXISTS idx_cooldis_identity_principals_active_kind
                        ON cooldis_identity_principals(kind, revoked_at_ms);

                    CREATE TABLE IF NOT EXISTS cooldis_identity_credentials (
                        schema TEXT NOT NULL,
                        credential_id TEXT PRIMARY KEY NOT NULL,
                        principal_id TEXT NOT NULL
                            REFERENCES cooldis_identity_principals(principal_id),
                        token_digest TEXT UNIQUE NOT NULL,
                        minted_by TEXT NOT NULL,
                        minted_at_ms INTEGER NOT NULL,
                        expires_at_ms INTEGER,
                        revoked_at_ms INTEGER,
                        revoked_by TEXT
                    );

                    CREATE INDEX IF NOT EXISTS idx_cooldis_identity_credentials_principal
                        ON cooldis_identity_credentials(principal_id, revoked_at_ms);

                    CREATE TABLE IF NOT EXISTS cooldis_identity_sessions (
                        schema TEXT NOT NULL,
                        session_id TEXT NOT NULL,
                        principal_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        surface TEXT NOT NULL,
                        credential_ref TEXT NOT NULL,
                        opened_at_ms INTEGER NOT NULL,
                        closed_at_ms INTEGER
                    );

                    CREATE INDEX IF NOT EXISTS idx_cooldis_identity_sessions_id
                        ON cooldis_identity_sessions(session_id, closed_at_ms);

                    CREATE TABLE IF NOT EXISTS cooldis_identity_auth_rejections (
                        rejection_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                        schema TEXT NOT NULL,
                        surface TEXT NOT NULL,
                        reason_json TEXT NOT NULL,
                        principal_id TEXT,
                        rejected_at_ms INTEGER NOT NULL
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

#[async_trait]
impl IdentityAuthority for SqliteIdentityAuthority {
    async fn declare_principal(
        &self,
        declared_by: &PrincipalId,
        principal_id: &PrincipalId,
        kind: PrincipalKind,
        display: &str,
    ) -> CooldisResult<PrincipalRecordV1> {
        if !kind.is_declarable() {
            return Err(authority_error(
                "member principals are reserved and cannot be declared in identity plane v0",
            ));
        }
        let store = self.store.clone();
        let clock = Arc::clone(&self.clock);
        let declared_by = declared_by.clone();
        let principal_id = principal_id.clone();
        let display = display.to_string();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            if let Some(status) = principal_status(&transaction, &principal_id).await? {
                transaction.rollback().await.map_err(storage_error)?;
                let message = match status {
                    PrincipalStatus::Active => {
                        format!("active principal {principal_id} is already declared")
                    }
                    PrincipalStatus::Revoked => {
                        format!("principal {principal_id} was already declared")
                    }
                };
                return Err(authority_error(message));
            }
            let now_ms = clock.now().timestamp_millis();
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_principals (
                        schema, principal_id, kind, display, declared_by, declared_at_ms,
                        revoked_at_ms, revoked_by, bootstrap_root
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, 0)",
                    params![
                        IDENTITY_PRINCIPAL_SCHEMA_V1,
                        principal_id.as_str(),
                        principal_kind_text(kind),
                        display.as_str(),
                        declared_by.as_str(),
                        now_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(PrincipalRecordV1 {
                schema: IDENTITY_PRINCIPAL_SCHEMA_V1.to_string(),
                principal_id,
                kind,
                display,
                declared_by,
                declared_at_ms: now_ms,
                revoked_at_ms: None,
            })
        })
        .await
    }

    async fn revoke_principal(
        &self,
        revoked_by: &PrincipalId,
        principal_id: &PrincipalId,
    ) -> CooldisResult<()> {
        let store = self.store.clone();
        let clock = Arc::clone(&self.clock);
        let revoked_by = revoked_by.clone();
        let principal_id = principal_id.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            let updated = transaction
                .execute(
                    "UPDATE cooldis_identity_principals
                     SET revoked_at_ms = COALESCE(revoked_at_ms, ?2),
                         revoked_by = COALESCE(revoked_by, ?3)
                     WHERE principal_id = ?1",
                    params![principal_id.as_str(), now_ms, revoked_by.as_str()],
                )
                .await
                .map_err(storage_error)?;
            if updated == 0 {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error("identity principal was not found"));
            }
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn mint_credential(
        &self,
        minted_by: &PrincipalId,
        principal_id: &PrincipalId,
        expires_at_ms: Option<i64>,
    ) -> CooldisResult<(IdentityCredentialV1, String)> {
        let store = self.store.clone();
        let clock = Arc::clone(&self.clock);
        let minted_by = minted_by.clone();
        let principal_id = principal_id.clone();
        let credential_id = format!("credential_{}", Uuid::new_v4());
        let token = mint_identity_secret()?;
        let token_digest = identity_token_digest(&token);
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            if !principal_is_active(&transaction, &principal_id).await? {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error(format!(
                    "cannot mint a credential for non-active principal {principal_id}"
                )));
            }
            let now_ms = clock.now().timestamp_millis();
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_credentials (
                        schema, credential_id, principal_id, token_digest, minted_by,
                        minted_at_ms, expires_at_ms, revoked_at_ms, revoked_by
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, NULL)",
                    params![
                        IDENTITY_CREDENTIAL_SCHEMA_V1,
                        credential_id.as_str(),
                        principal_id.as_str(),
                        token_digest.as_str(),
                        minted_by.as_str(),
                        now_ms,
                        expires_at_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok((
                IdentityCredentialV1 {
                    schema: IDENTITY_CREDENTIAL_SCHEMA_V1.to_string(),
                    credential_id,
                    principal_id,
                    token_digest,
                    minted_by,
                    minted_at_ms: now_ms,
                    expires_at_ms,
                    revoked_at_ms: None,
                },
                token,
            ))
        })
        .await
    }

    async fn revoke_credential(
        &self,
        revoked_by: &PrincipalId,
        credential_id: &str,
    ) -> CooldisResult<()> {
        let store = self.store.clone();
        let clock = Arc::clone(&self.clock);
        let revoked_by = revoked_by.clone();
        let credential_id = credential_id.to_string();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let now_ms = clock.now().timestamp_millis();
            let updated = transaction
                .execute(
                    "UPDATE cooldis_identity_credentials
                     SET revoked_at_ms = COALESCE(revoked_at_ms, ?2),
                         revoked_by = COALESCE(revoked_by, ?3)
                     WHERE credential_id = ?1",
                    params![credential_id.as_str(), now_ms, revoked_by.as_str()],
                )
                .await
                .map_err(storage_error)?;
            if updated == 0 {
                transaction.rollback().await.map_err(storage_error)?;
                return Err(authority_error("identity credential was not found"));
            }
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn verify_token(&self, token: &str) -> CooldisResult<Option<ResolvedPrincipal>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let digest = identity_token_digest(token);
        let now_ms = self.clock.now().timestamp_millis();
        let mut rows = connection
            .query(
                "SELECT credential.credential_id, principal.principal_id, principal.kind
                 FROM cooldis_identity_credentials AS credential
                 JOIN cooldis_identity_principals AS principal
                   ON principal.principal_id = credential.principal_id
                 WHERE credential.token_digest = ?1
                   AND credential.revoked_at_ms IS NULL
                   AND (credential.expires_at_ms IS NULL OR credential.expires_at_ms > ?2)
                   AND principal.revoked_at_ms IS NULL
                   AND principal.kind IN (?3, ?4)
                 LIMIT 1",
                params![
                    digest,
                    now_ms,
                    principal_kind_text(PrincipalKind::Operator),
                    principal_kind_text(PrincipalKind::Adapter),
                ],
            )
            .await
            .map_err(storage_error)?;
        let principal = match rows.next().await.map_err(storage_error)? {
            Some(row) => Some(ResolvedPrincipal {
                auth: AuthenticationPath::Credential {
                    credential_id: row.get(0).map_err(storage_error)?,
                },
                principal_id: PrincipalId::new(row.get::<String>(1).map_err(storage_error)?),
                kind: parse_principal_kind(&row.get::<String>(2).map_err(storage_error)?)?,
            }),
            None => None,
        };
        Ok(principal)
    }

    async fn resolve_peer_uid(&self, uid: u32) -> CooldisResult<Option<ResolvedPrincipal>> {
        let Some(principal_id) = self.peer_operator.as_ref() else {
            return Ok(None);
        };
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                "SELECT kind
                 FROM cooldis_identity_principals
                 WHERE principal_id = ?1
                   AND kind = ?2
                   AND revoked_at_ms IS NULL
                 LIMIT 1",
                params![
                    principal_id.as_str(),
                    principal_kind_text(PrincipalKind::Operator)
                ],
            )
            .await
            .map_err(storage_error)?;
        let Some(row) = rows.next().await.map_err(storage_error)? else {
            return Ok(None);
        };
        Ok(Some(ResolvedPrincipal {
            principal_id: principal_id.clone(),
            kind: parse_principal_kind(&row.get::<String>(0).map_err(storage_error)?)?,
            auth: AuthenticationPath::PeerUid { uid },
        }))
    }

    async fn list_principals(&self) -> CooldisResult<Vec<PrincipalRecordV1>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                "SELECT schema, principal_id, kind, display, declared_by, declared_at_ms,
                        revoked_at_ms
                 FROM cooldis_identity_principals
                 ORDER BY declared_at_ms, principal_id",
                (),
            )
            .await
            .map_err(storage_error)?;
        let mut principals = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            principals.push(principal_from_row(&row)?);
        }
        Ok(principals)
    }

    async fn list_credentials(
        &self,
        principal_id: &PrincipalId,
    ) -> CooldisResult<Vec<IdentityCredentialV1>> {
        let database = self.store.sqlite_database();
        let connection = database.connect().await.map_err(storage_error)?;
        let mut rows = connection
            .query(
                "SELECT schema, credential_id, principal_id, token_digest, minted_by,
                        minted_at_ms, expires_at_ms, revoked_at_ms
                 FROM cooldis_identity_credentials
                 WHERE principal_id = ?1
                 ORDER BY minted_at_ms, credential_id",
                params![principal_id.as_str()],
            )
            .await
            .map_err(storage_error)?;
        let mut credentials = Vec::new();
        while let Some(row) = rows.next().await.map_err(storage_error)? {
            credentials.push(credential_from_row(&row)?);
        }
        Ok(credentials)
    }

    async fn witness_session_opened(&self, session: &IdentitySessionV1) -> CooldisResult<()> {
        if session.schema != IDENTITY_SESSION_SCHEMA_V1 {
            return Err(authority_error(
                "identity session schema id is not supported",
            ));
        }
        if !session.kind.is_declarable() {
            return Err(authority_error(
                "member principals cannot open sessions in identity plane v0",
            ));
        }
        let store = self.store.clone();
        let session = session.clone();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_sessions (
                        schema, session_id, principal_id, kind, surface, credential_ref,
                        opened_at_ms, closed_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    params![
                        session.schema,
                        session.session_id,
                        session.principal_id.as_str(),
                        principal_kind_text(session.kind),
                        boundary_surface_text(session.surface),
                        session.credential_ref,
                        session.opened_at_ms,
                        session.closed_at_ms,
                    ],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn witness_session_closed(
        &self,
        session_id: &str,
        closed_at_ms: i64,
    ) -> CooldisResult<()> {
        let store = self.store.clone();
        let session_id = session_id.to_string();
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_sessions (
                        schema, session_id, principal_id, kind, surface, credential_ref,
                        opened_at_ms, closed_at_ms
                     )
                     SELECT schema, session_id, principal_id, kind, surface, credential_ref,
                            opened_at_ms, ?2
                     FROM cooldis_identity_sessions AS opened
                     WHERE opened.session_id = ?1
                       AND opened.closed_at_ms IS NULL
                       AND NOT EXISTS (
                           SELECT 1 FROM cooldis_identity_sessions AS closed
                           WHERE closed.session_id = opened.session_id
                             AND closed.closed_at_ms IS NOT NULL
                       )
                     ORDER BY rowid
                     LIMIT 1",
                    params![session_id, closed_at_ms],
                )
                .await
                .map_err(storage_error)?;
            transaction.commit().await.map_err(storage_error)?;
            Ok(())
        })
        .await
    }

    async fn witness_auth_rejected(
        &self,
        rejection: &IdentityAuthRejectionV1,
    ) -> CooldisResult<()> {
        if rejection.schema != IDENTITY_AUTH_REJECTION_SCHEMA_V1 {
            return Err(authority_error(
                "identity auth rejection schema id is not supported",
            ));
        }
        let store = self.store.clone();
        let rejection = rejection.clone();
        let reason_json = serde_json::to_string(&rejection.reason).map_err(|error| {
            authority_error(format!("failed to encode auth rejection: {error}"))
        })?;
        cancellation_safe(async move {
            let database = store.sqlite_database();
            let mut connection = database.connect().await.map_err(storage_error)?;
            let transaction = connection
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .await
                .map_err(storage_error)?;
            let principal_id = rejection
                .principal_id
                .as_ref()
                .map(|principal_id| principal_id.as_str().to_string());
            transaction
                .execute(
                    "INSERT INTO cooldis_identity_auth_rejections (
                        schema, surface, reason_json, principal_id, rejected_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5)",
                    params![
                        rejection.schema,
                        boundary_surface_text(rejection.surface),
                        reason_json,
                        principal_id,
                        rejection.rejected_at_ms,
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

async fn active_operator_exists(connection: &Connection) -> CooldisResult<bool> {
    let mut rows = connection
        .query(
            "SELECT 1
             FROM cooldis_identity_principals
             WHERE kind = ?1 AND revoked_at_ms IS NULL
             LIMIT 1",
            params![principal_kind_text(PrincipalKind::Operator)],
        )
        .await
        .map_err(storage_error)?;
    Ok(rows.next().await.map_err(storage_error)?.is_some())
}

async fn principal_is_active(
    connection: &Connection,
    principal_id: &PrincipalId,
) -> CooldisResult<bool> {
    let mut rows = connection
        .query(
            "SELECT 1
             FROM cooldis_identity_principals
             WHERE principal_id = ?1 AND revoked_at_ms IS NULL
             LIMIT 1",
            params![principal_id.as_str()],
        )
        .await
        .map_err(storage_error)?;
    Ok(rows.next().await.map_err(storage_error)?.is_some())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrincipalStatus {
    Active,
    Revoked,
}

async fn principal_status(
    connection: &Connection,
    principal_id: &PrincipalId,
) -> CooldisResult<Option<PrincipalStatus>> {
    let mut rows = connection
        .query(
            "SELECT revoked_at_ms
             FROM cooldis_identity_principals
             WHERE principal_id = ?1
             LIMIT 1",
            params![principal_id.as_str()],
        )
        .await
        .map_err(storage_error)?;
    match rows.next().await.map_err(storage_error)? {
        Some(row) => {
            let revoked_at_ms = row.get::<Option<i64>>(0).map_err(storage_error)?;
            Ok(Some(if revoked_at_ms.is_some() {
                PrincipalStatus::Revoked
            } else {
                PrincipalStatus::Active
            }))
        }
        None => Ok(None),
    }
}

fn principal_from_row(row: &Row) -> CooldisResult<PrincipalRecordV1> {
    Ok(PrincipalRecordV1 {
        schema: row.get(0).map_err(storage_error)?,
        principal_id: PrincipalId::new(row.get::<String>(1).map_err(storage_error)?),
        kind: parse_principal_kind(&row.get::<String>(2).map_err(storage_error)?)?,
        display: row.get(3).map_err(storage_error)?,
        declared_by: PrincipalId::new(row.get::<String>(4).map_err(storage_error)?),
        declared_at_ms: row.get(5).map_err(storage_error)?,
        revoked_at_ms: row.get(6).map_err(storage_error)?,
    })
}

fn credential_from_row(row: &Row) -> CooldisResult<IdentityCredentialV1> {
    Ok(IdentityCredentialV1 {
        schema: row.get(0).map_err(storage_error)?,
        credential_id: row.get(1).map_err(storage_error)?,
        principal_id: PrincipalId::new(row.get::<String>(2).map_err(storage_error)?),
        token_digest: row.get(3).map_err(storage_error)?,
        minted_by: PrincipalId::new(row.get::<String>(4).map_err(storage_error)?),
        minted_at_ms: row.get(5).map_err(storage_error)?,
        expires_at_ms: row.get(6).map_err(storage_error)?,
        revoked_at_ms: row.get(7).map_err(storage_error)?,
    })
}

fn principal_kind_text(kind: PrincipalKind) -> &'static str {
    match kind {
        PrincipalKind::Operator => "operator",
        PrincipalKind::Adapter => "adapter",
        PrincipalKind::Member => "member",
    }
}

fn parse_principal_kind(value: &str) -> CooldisResult<PrincipalKind> {
    match value {
        "operator" => Ok(PrincipalKind::Operator),
        "adapter" => Ok(PrincipalKind::Adapter),
        "member" => Ok(PrincipalKind::Member),
        other => Err(authority_error(format!(
            "unknown persisted identity principal kind {other:?}"
        ))),
    }
}

fn boundary_surface_text(surface: BoundarySurface) -> &'static str {
    match surface {
        BoundarySurface::UnixSocket => "unix_socket",
        BoundarySurface::Websocket => "websocket",
        BoundarySurface::Console => "console",
    }
}

fn mint_identity_secret() -> CooldisResult<String> {
    let mut random = [0_u8; 32];
    getrandom::fill(&mut random)
        .map_err(|error| authority_error(format!("failed to mint identity credential: {error}")))?;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut token = String::with_capacity("cooldis_id_".len() + random.len() * 2);
    token.push_str("cooldis_id_");
    for byte in random {
        token.push(HEX[(byte >> 4) as usize] as char);
        token.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(token)
}

fn authority_error(message: impl Into<String>) -> CooldisError {
    CooldisError::History(message.into())
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
            "sqlite identity authority transaction task failed: {error}"
        ))
    })?
}

/// Deployment mode (ADR 0008 D5).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityMode {
    /// Single-user developer box: a missing `[daemon.identity]` section
    /// synthesizes a default operator, and the same-uid peer mapping is on.
    #[default]
    Local,
    /// Hosted instance: an explicit identity section and a bootstrapped
    /// operator credential are required; starting without them is a hard
    /// error. Peer mapping and debug RPC default off.
    Managed,
}

/// The `[daemon.identity]` config section (ADR 0008 D5).
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonIdentityConfig {
    #[serde(default)]
    pub mode: IdentityMode,
    /// Replaces the legacy hard-coded app-server tenant. Required in managed
    /// mode; a local-mode default is synthesized when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// The principal the bundled console resolves to (in v0, the operator).
    /// In managed mode this is required and also serves as the daemon's
    /// legacy single-user coordinate until per-principal attribution lands.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub console_principal: Option<PrincipalId>,
}

impl CooldisDaemonIdentityConfig {
    /// The ratified hard-fail rule (ADR 0008 D5): managed mode without an
    /// explicit tenant identity and console principal refuses to start.
    /// Blank-after-trim values count as absent. Local mode validates
    /// vacuously.
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            IdentityMode::Local => Ok(()),
            IdentityMode::Managed => {
                if self.tenant_id.as_deref().unwrap_or("").trim().is_empty() {
                    return Err("managed mode requires [daemon.identity] tenant_id; \
                         see docs/adr/0008-identity-plane-v0.md D5"
                        .to_string());
                }
                if self
                    .console_principal
                    .as_ref()
                    .is_none_or(|principal| principal.as_str().trim().is_empty())
                {
                    return Err(
                        "managed mode requires [daemon.identity] console_principal; \
                         see docs/adr/0008-identity-plane-v0.md D5"
                            .to_string(),
                    );
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{TimeZone, Utc};
    use cooldis_sqlite::params;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicI64, Ordering};
    use tokio::sync::Barrier;

    struct TestClock {
        now_ms: AtomicI64,
    }

    impl TestClock {
        fn new(now_ms: i64) -> Self {
            Self {
                now_ms: AtomicI64::new(now_ms),
            }
        }

        fn set(&self, now_ms: i64) {
            self.now_ms.store(now_ms, Ordering::SeqCst);
        }
    }

    impl crate::DaemonClock for TestClock {
        fn now(&self) -> chrono::DateTime<Utc> {
            Utc.timestamp_millis_opt(self.now_ms.load(Ordering::SeqCst))
                .single()
                .expect("test timestamp should be representable")
        }
    }

    async fn authority(
        now_ms: i64,
        peer_operator: Option<PrincipalId>,
    ) -> (
        SqliteIdentityAuthority,
        crate::SqliteSessionStore,
        Arc<TestClock>,
    ) {
        let store = crate::SqliteSessionStore::in_memory().await.unwrap();
        let clock = Arc::new(TestClock::new(now_ms));
        let authority = SqliteIdentityAuthority::new(
            store.clone(),
            Arc::clone(&clock) as Arc<dyn crate::DaemonClock>,
            peer_operator,
        )
        .await
        .unwrap();
        (authority, store, clock)
    }

    #[test]
    fn member_kind_is_reserved_not_declarable() {
        assert!(PrincipalKind::Operator.is_declarable());
        assert!(PrincipalKind::Adapter.is_declarable());
        assert!(!PrincipalKind::Member.is_declarable());
    }

    #[test]
    fn kind_to_class_mapping_matches_adr() {
        assert!(PrincipalKind::Operator.permits(AuthorityClass::Host));
        assert!(PrincipalKind::Operator.permits(AuthorityClass::Interactive));
        assert!(PrincipalKind::Operator.permits(AuthorityClass::Ingress));
        assert!(!PrincipalKind::Adapter.permits(AuthorityClass::Host));
        assert!(!PrincipalKind::Adapter.permits(AuthorityClass::Interactive));
        assert!(PrincipalKind::Adapter.permits(AuthorityClass::Ingress));
        assert!(!PrincipalKind::Member.permits(AuthorityClass::Host));
    }

    #[test]
    fn host_list_precedence_beats_interactive_prefix() {
        assert_eq!(
            authority_class_for_method("thread/shellCommand"),
            AuthorityClass::Host
        );
        assert_eq!(
            authority_class_for_method("thread/start"),
            AuthorityClass::Interactive
        );
    }

    #[test]
    fn unknown_method_fails_closed_to_host() {
        assert_eq!(
            authority_class_for_method("someFuture/method"),
            AuthorityClass::Host
        );
    }

    #[test]
    fn managed_mode_without_tenant_hard_fails() {
        let config = CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            ..Default::default()
        };
        assert!(config.validate().is_err());
        let local = CooldisDaemonIdentityConfig::default();
        assert!(local.validate().is_ok());
    }

    #[test]
    fn managed_mode_rejects_blank_identity_fields() {
        let blank_tenant = CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("   ".to_string()),
            console_principal: Some(PrincipalId::new("operator:root")),
        };
        assert!(blank_tenant.validate().unwrap_err().contains("tenant_id"));

        let missing_console = CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("tenant-a".to_string()),
            console_principal: None,
        };
        assert!(
            missing_console
                .validate()
                .unwrap_err()
                .contains("console_principal")
        );

        let blank_console = CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("tenant-a".to_string()),
            console_principal: Some(PrincipalId::new(" ")),
        };
        assert!(
            blank_console
                .validate()
                .unwrap_err()
                .contains("console_principal")
        );

        let complete = CooldisDaemonIdentityConfig {
            mode: IdentityMode::Managed,
            tenant_id: Some("tenant-a".to_string()),
            console_principal: Some(PrincipalId::new("operator:root")),
        };
        assert!(complete.validate().is_ok());
    }

    #[test]
    fn token_digest_convention_matches_sync_authority() {
        assert!(identity_token_digest("secret").starts_with("sha256:"));
    }

    #[tokio::test]
    async fn principal_declaration_revocation_and_member_rejection_are_durable() {
        let operator = PrincipalId::new("operator:root");
        let adapter = PrincipalId::new("adapter:inbound");
        let (authority, _, clock) = authority(1_000, None).await;
        authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();

        let declared = authority
            .declare_principal(
                &operator,
                &adapter,
                PrincipalKind::Adapter,
                "Telegram adapter",
            )
            .await
            .unwrap();
        assert_eq!(declared.schema, IDENTITY_PRINCIPAL_SCHEMA_V1);
        assert_eq!(declared.declared_at_ms, 1_000);
        assert!(
            authority
                .declare_principal(
                    &operator,
                    &adapter,
                    PrincipalKind::Adapter,
                    "Duplicate adapter",
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("active principal")
        );
        assert!(
            authority
                .declare_principal(
                    &operator,
                    &PrincipalId::new("member:reserved"),
                    PrincipalKind::Member,
                    "Reserved member",
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("member")
        );

        clock.set(2_000);
        authority
            .revoke_principal(&operator, &adapter)
            .await
            .unwrap();
        clock.set(3_000);
        authority
            .revoke_principal(&operator, &adapter)
            .await
            .unwrap();
        let principals = authority.list_principals().await.unwrap();
        let adapter = principals
            .iter()
            .find(|record| record.principal_id == adapter)
            .unwrap();
        assert_eq!(adapter.revoked_at_ms, Some(2_000));
    }

    #[tokio::test]
    async fn revoking_an_unknown_principal_fails() {
        let (authority, _, _) = authority(1_000, None).await;
        let error = authority
            .revoke_principal(
                &PrincipalId::new("operator:root"),
                &PrincipalId::new("adapter:missing"),
            )
            .await
            .unwrap_err();

        assert!(error.to_string().contains("principal was not found"));
    }

    #[tokio::test]
    async fn revoking_an_unknown_credential_fails() {
        let (authority, _, _) = authority(1_000, None).await;
        let error = authority
            .revoke_credential(&PrincipalId::new("operator:root"), "credential_missing")
            .await
            .unwrap_err();

        assert!(error.to_string().contains("credential was not found"));
    }

    #[tokio::test]
    async fn revoking_principals_and_credentials_is_idempotent() {
        let operator = PrincipalId::new("operator:root");
        let adapter = PrincipalId::new("adapter:webhook");
        let (authority, _, clock) = authority(1_000, None).await;
        authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();
        authority
            .declare_principal(
                &operator,
                &adapter,
                PrincipalKind::Adapter,
                "Webhook adapter",
            )
            .await
            .unwrap();
        let (credential, _) = authority
            .mint_credential(&operator, &adapter, None)
            .await
            .unwrap();

        authority
            .revoke_credential(&operator, &credential.credential_id)
            .await
            .unwrap();
        authority
            .revoke_principal(&operator, &adapter)
            .await
            .unwrap();
        clock.set(2_000);
        authority
            .revoke_credential(&operator, &credential.credential_id)
            .await
            .unwrap();
        authority
            .revoke_principal(&operator, &adapter)
            .await
            .unwrap();

        let credential = authority
            .list_credentials(&adapter)
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let principal = authority
            .list_principals()
            .await
            .unwrap()
            .into_iter()
            .find(|record| record.principal_id == adapter)
            .unwrap();
        assert_eq!(credential.revoked_at_ms, Some(1_000));
        assert_eq!(principal.revoked_at_ms, Some(1_000));
    }

    #[tokio::test]
    async fn credential_verification_fails_closed_for_expiry_and_both_revocations() {
        let operator = PrincipalId::new("operator:root");
        let adapter = PrincipalId::new("adapter:webhook");
        let (authority, _, clock) = authority(10_000, None).await;
        let (_, _, bootstrap_token) = authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();
        authority
            .declare_principal(
                &operator,
                &adapter,
                PrincipalKind::Adapter,
                "Webhook adapter",
            )
            .await
            .unwrap();
        assert_eq!(
            authority
                .verify_token(&bootstrap_token)
                .await
                .unwrap()
                .unwrap()
                .principal_id,
            operator
        );

        let (_, expiring_token) = authority
            .mint_credential(&operator, &adapter, Some(11_000))
            .await
            .unwrap();
        clock.set(10_999);
        assert!(
            authority
                .verify_token(&expiring_token)
                .await
                .unwrap()
                .is_some()
        );
        clock.set(11_000);
        assert!(
            authority
                .verify_token(&expiring_token)
                .await
                .unwrap()
                .is_none()
        );

        let (revoked_credential, revoked_token) = authority
            .mint_credential(&operator, &adapter, None)
            .await
            .unwrap();
        authority
            .revoke_credential(&operator, &revoked_credential.credential_id)
            .await
            .unwrap();
        assert!(
            authority
                .verify_token(&revoked_token)
                .await
                .unwrap()
                .is_none()
        );

        let (_, principal_revoked_token) = authority
            .mint_credential(&operator, &adapter, None)
            .await
            .unwrap();
        authority
            .revoke_principal(&operator, &adapter)
            .await
            .unwrap();
        assert!(
            authority
                .verify_token(&principal_revoked_token)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            authority
                .mint_credential(&operator, &adapter, None)
                .await
                .unwrap_err()
                .to_string()
                .contains("active principal")
        );
    }

    #[tokio::test]
    async fn reserved_member_records_never_resolve_or_open_sessions() {
        let member = PrincipalId::new("member:reserved");
        let token = "test-only-member-token";
        let (authority, store, _) = authority(1_000, None).await;
        let database = store.sqlite_database();
        let connection = database.connect().await.unwrap();
        connection
            .execute(
                "INSERT INTO cooldis_identity_principals (
                    schema, principal_id, kind, display, declared_by, declared_at_ms,
                    revoked_at_ms, revoked_by, bootstrap_root
                 ) VALUES (?1, ?2, 'member', ?3, ?2, ?4, NULL, NULL, 0)",
                params![
                    IDENTITY_PRINCIPAL_SCHEMA_V1,
                    member.as_str(),
                    "Reserved member",
                    1_000_i64,
                ],
            )
            .await
            .unwrap();
        connection
            .execute(
                "INSERT INTO cooldis_identity_credentials (
                    schema, credential_id, principal_id, token_digest, minted_by,
                    minted_at_ms, expires_at_ms, revoked_at_ms, revoked_by
                 ) VALUES (?1, ?2, ?3, ?4, ?3, ?5, NULL, NULL, NULL)",
                params![
                    IDENTITY_CREDENTIAL_SCHEMA_V1,
                    "credential_member",
                    member.as_str(),
                    identity_token_digest(token),
                    1_000_i64,
                ],
            )
            .await
            .unwrap();

        assert!(authority.verify_token(token).await.unwrap().is_none());
        let error = authority
            .witness_session_opened(&IdentitySessionV1 {
                schema: IDENTITY_SESSION_SCHEMA_V1.to_string(),
                session_id: "session-member".to_string(),
                principal_id: member,
                kind: PrincipalKind::Member,
                surface: BoundarySurface::Websocket,
                credential_ref: "credential_member".to_string(),
                opened_at_ms: 1_000,
                closed_at_ms: None,
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("member"));
    }

    #[tokio::test]
    async fn minted_secret_is_256_bit_prefixed_and_only_its_digest_is_persisted() {
        let operator = PrincipalId::new("operator:root");
        let (authority, store, _) = authority(1_000, None).await;
        let (_, credential, token) = authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();
        assert!(token.starts_with("cooldis_id_"));
        assert_eq!(token.len(), "cooldis_id_".len() + 64);

        let database = store.sqlite_database();
        let connection = database.connect().await.unwrap();
        let mut rows = connection
            .query(
                "SELECT schema, credential_id, principal_id, token_digest, minted_by
                 FROM cooldis_identity_credentials
                 WHERE credential_id = ?1",
                params![credential.credential_id],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        let persisted = (0..5)
            .map(|index| row.get::<String>(index).unwrap())
            .collect::<Vec<_>>();
        let digest = &persisted[3];
        assert_eq!(digest.as_str(), identity_token_digest(&token));
        assert!(persisted.iter().all(|value| !value.contains(&token)));
    }

    #[tokio::test]
    async fn credential_digest_uniqueness_prevents_ambiguous_verification() {
        let operator = PrincipalId::new("operator:root");
        let (authority, store, _) = authority(1_000, None).await;
        let (_, credential, token) = authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();
        let database = store.sqlite_database();
        let connection = database.connect().await.unwrap();
        let collision = connection
            .execute(
                "INSERT INTO cooldis_identity_credentials (
                    schema, credential_id, principal_id, token_digest, minted_by,
                    minted_at_ms, expires_at_ms, revoked_at_ms, revoked_by
                 ) VALUES (?1, ?2, ?3, ?4, ?3, ?5, NULL, NULL, NULL)",
                params![
                    IDENTITY_CREDENTIAL_SCHEMA_V1,
                    "credential_collision",
                    operator.as_str(),
                    identity_token_digest(&token),
                    1_001_i64,
                ],
            )
            .await
            .unwrap_err();

        assert!(!collision.to_string().contains(&token));
        let resolved = authority.verify_token(&token).await.unwrap().unwrap();
        assert_eq!(resolved.principal_id, operator);
        assert_eq!(
            resolved.auth,
            AuthenticationPath::Credential {
                credential_id: credential.credential_id,
            }
        );
    }

    #[tokio::test]
    async fn concurrent_bootstrap_commits_exactly_one_active_root() {
        let (authority, _, _) = authority(1_000, None).await;
        let authority = Arc::new(authority);
        let barrier = Arc::new(Barrier::new(3));
        let spawn = |id: &'static str| {
            let authority = Arc::clone(&authority);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                authority
                    .bootstrap_operator(&PrincipalId::new(id), id)
                    .await
            })
        };
        let first = spawn("operator:first");
        let second = spawn("operator:second");
        barrier.wait().await;
        let first = first.await.unwrap();
        let second = second.await.unwrap();
        assert!(matches!(
            (&first, &second),
            (Ok(_), Err(_)) | (Err(_), Ok(_))
        ));
        let principals = authority.list_principals().await.unwrap();
        assert_eq!(
            principals
                .iter()
                .filter(|record| {
                    record.kind == PrincipalKind::Operator && record.revoked_at_ms.is_none()
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn bootstrap_after_root_revocation_preserves_one_active_root() {
        let first = PrincipalId::new("operator:first");
        let second = PrincipalId::new("operator:second");
        let (authority, _, clock) = authority(1_000, None).await;
        let (_, _, first_token) = authority
            .bootstrap_operator(&first, "First operator")
            .await
            .unwrap();
        clock.set(2_000);
        authority.revoke_principal(&first, &first).await.unwrap();
        let (_, _, second_token) = authority
            .bootstrap_operator(&second, "Second operator")
            .await
            .unwrap();

        assert!(
            authority
                .verify_token(&first_token)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            authority
                .verify_token(&second_token)
                .await
                .unwrap()
                .unwrap()
                .principal_id,
            second
        );
        let principals = authority.list_principals().await.unwrap();
        assert_eq!(principals.len(), 2);
        assert_eq!(
            principals
                .iter()
                .filter(|record| {
                    record.kind == PrincipalKind::Operator && record.revoked_at_ms.is_none()
                })
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn schema_initialization_is_idempotent_and_preserves_records() {
        let operator = PrincipalId::new("operator:root");
        let (authority, store, clock) = authority(1_000, None).await;
        let (_, credential, token) = authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();
        drop(authority);

        let reopened = SqliteIdentityAuthority::new(
            store,
            Arc::clone(&clock) as Arc<dyn crate::DaemonClock>,
            None,
        )
        .await
        .unwrap();
        assert_eq!(reopened.list_principals().await.unwrap().len(), 1);
        assert_eq!(
            reopened.list_credentials(&operator).await.unwrap(),
            vec![credential]
        );
        assert_eq!(
            reopened
                .verify_token(&token)
                .await
                .unwrap()
                .unwrap()
                .principal_id,
            operator
        );
    }

    #[tokio::test]
    async fn peer_mapping_and_witness_records_use_the_fixed_surface() {
        let operator = PrincipalId::new("operator:root");
        let (authority, store, _) = authority(1_000, Some(operator.clone())).await;
        authority
            .bootstrap_operator(&operator, "Root operator")
            .await
            .unwrap();
        assert_eq!(
            authority.resolve_peer_uid(501).await.unwrap(),
            Some(ResolvedPrincipal {
                principal_id: operator.clone(),
                kind: PrincipalKind::Operator,
                auth: AuthenticationPath::PeerUid { uid: 501 },
            })
        );

        let session = IdentitySessionV1 {
            schema: IDENTITY_SESSION_SCHEMA_V1.to_string(),
            session_id: "session-1".to_string(),
            principal_id: operator.clone(),
            kind: PrincipalKind::Operator,
            surface: BoundarySurface::UnixSocket,
            credential_ref: "peer_uid:501".to_string(),
            opened_at_ms: 1_000,
            closed_at_ms: None,
        };
        authority.witness_session_opened(&session).await.unwrap();
        authority
            .witness_session_closed("session-1", 2_000)
            .await
            .unwrap();
        authority
            .witness_auth_rejected(&IdentityAuthRejectionV1 {
                schema: IDENTITY_AUTH_REJECTION_SCHEMA_V1.to_string(),
                surface: BoundarySurface::Websocket,
                reason: IdentityAuthRejectionReason::CredentialUnknown,
                principal_id: None,
                rejected_at_ms: 2_100,
            })
            .await
            .unwrap();

        let database = store.sqlite_database();
        let connection = database.connect().await.unwrap();
        let mut rows = connection
            .query(
                "SELECT COUNT(*), MAX(closed_at_ms) FROM cooldis_identity_sessions WHERE session_id = ?1",
                params!["session-1"],
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 2);
        assert_eq!(row.get::<i64>(1).unwrap(), 2_000);
        let mut rows = connection
            .query("SELECT COUNT(*) FROM cooldis_identity_auth_rejections", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1
        );
    }
}
