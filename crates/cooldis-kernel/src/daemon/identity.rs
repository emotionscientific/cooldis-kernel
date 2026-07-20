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
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::CooldisResult;

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

/// The `[daemon.identity]` config section (ADR 0008 D5). Wiring into
/// `CooldisDaemonConfig` (defaults, presence, layer merge, validation) is
/// implementation ticket 4; this struct is the shape.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct CooldisDaemonIdentityConfig {
    #[serde(default)]
    pub mode: IdentityMode,
    /// Replaces the hard-coded `cooldis_app_server` tenant. Required in
    /// managed mode; a local-mode default is synthesized when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_id: Option<String>,
    /// The principal the bundled console resolves to (in v0, the operator).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub console_principal: Option<PrincipalId>,
}

impl CooldisDaemonIdentityConfig {
    /// The ratified hard-fail rule (ADR 0008 D5): managed mode without an
    /// explicit tenant identity refuses to start. Local mode validates
    /// vacuously.
    pub fn validate(&self) -> Result<(), String> {
        match self.mode {
            IdentityMode::Local => Ok(()),
            IdentityMode::Managed => {
                if self.tenant_id.as_deref().unwrap_or("").is_empty() {
                    return Err("managed mode requires [daemon.identity] tenant_id; \
                         see docs/adr/0008-identity-plane-v0.md D5"
                        .to_string());
                }
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn token_digest_convention_matches_sync_authority() {
        assert!(identity_token_digest("secret").starts_with("sha256:"));
    }
}
