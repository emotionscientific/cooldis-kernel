//! Multi-tenant host facade v0 (EMO-553): one process serving many kernel
//! instances behind one authenticated RPC listener.
//!
//! With the instance seam (EMO-551) and config hygiene (EMO-552) landed,
//! the host is deliberately small: a map of instances, a routing table
//! from presented credentials to instances, one listener, and lifecycle
//! (create, shutdown, replace, observe). Placement across hosts, native
//! one-store-many-owners multi-tenancy, and orchestrator provisioning are
//! out of scope (EMO-549 stage 4).
//!
//! Architect decisions (fixed; the implementation must not revisit them):
//!
//! 1. **Routing is selection, not authentication.** The credential a
//!    connection presents maps to exactly one instance at accept time via
//!    the explicitly registered route table (keyed by
//!    [`crate::daemon::identity::identity_token_digest`]). The route only
//!    selects which instance's own identity authority verifies the token;
//!    a credential the selected instance rejects is rejected outright, and
//!    no other instance is probed. Tenant or instance identifiers supplied
//!    in an RPC body never override the connection's routed instance —
//!    a disagreeing identifier is a rejected request.
//! 2. **Task ownership.** The listener and every per-connection task run
//!    on the host's own task set and are cancelled + awaited on host
//!    shutdown. Instance background tasks stay instance-owned (EMO-551)
//!    and end on that instance's shutdown. A connection dies with either
//!    owner: host shutdown cancels the task; instance shutdown closes the
//!    instance's dispatch gate and cancellation token under it.
//! 3. **Unrouted credentials.** A connection whose credential digest has
//!    no route is refused with the same HTTP 401 shape instances use, and
//!    recorded in the host process log only: no instance journal exists to
//!    witness it, and a host-owned witness stream is out of scope for v0.
//! 4. **Boundary witness.** Host-routed connections are witnessed by the
//!    routed instance on [`crate::daemon::identity::BoundarySurface::Host`],
//!    never `Websocket`/`Console` — the surface names how the connection
//!    reached the instance.

use crate::adapters::app_server::VerletAppServer;
use crate::adapters::app_server::VerletAppServerConfig;
use crate::adapters::app_server::lifecycle::InstanceTaskSet;
use crate::kernel::runtime_host::VerletResult;

/// Host-scoped name of one hosted instance. Distinct from tenant id (an
/// instance HAS a tenant; the host does not interpret it) and never taken
/// from an RPC body — the routed connection is the only source.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct InstanceId(String);

impl InstanceId {
    /// Non-empty, no whitespace; used in logs and error messages, so keep
    /// it printable. Never secret material.
    pub fn new(id: impl Into<String>) -> VerletResult<Self> {
        let id = id.into();
        if id.is_empty() || id.chars().any(char::is_whitespace) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!("instance id must be non-empty without whitespace: {id:?}"),
            ));
        }
        Ok(Self(id))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for InstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The host facade: owns the instances, the credential route table, and
/// the listener + connection tasks. Cloning shares the one host.
#[derive(Clone)]
pub struct VerletHost {
    // Skeleton-only allow: every field is read once the todo! bodies land.
    #[allow(dead_code)]
    inner: std::sync::Arc<VerletHostInner>,
}

#[allow(dead_code)] // skeleton-only: removed with the todo! bodies
struct VerletHostInner {
    /// The hosted instances. Instance construction and shutdown happen
    /// OUTSIDE this lock (both are slow: store opens, task drains) — the
    /// lock guards the map, not the lifecycle transitions; a per-id claim
    /// keeps concurrent start/shutdown of the same id single-file.
    instances: tokio::sync::RwLock<std::collections::HashMap<InstanceId, VerletAppServer>>,
    /// Credential-digest → instance selection table (decision 1). Values
    /// are digests from `identity_token_digest`; raw tokens are never
    /// stored, logged, or printed.
    credential_routes: std::sync::Mutex<std::collections::HashMap<String, InstanceId>>,
    /// Listener + per-connection tasks (decision 2).
    tasks: InstanceTaskSet,
    /// Set once by [`VerletHost::shutdown`]; afterwards every lifecycle
    /// and serve entry fails fast.
    shutdown: tokio::sync::Mutex<bool>,
}

impl Default for VerletHost {
    fn default() -> Self {
        Self::new()
    }
}

impl VerletHost {
    pub fn new() -> Self {
        todo!("EMO-553: empty instance map, empty route table, fresh task set")
    }

    /// Construct and register one instance under `id`. The config must be
    /// a hosted config ([`VerletAppServerConfig::hosted`]); root
    /// reservation (EMO-552) already guarantees two live instances cannot
    /// share storage, whichever host they belong to. Fails on duplicate
    /// `id`, after host shutdown, and on any constructor failure (the
    /// constructor's own cleanup applies, EMO-551).
    pub async fn start_instance(
        &self,
        id: InstanceId,
        config: VerletAppServerConfig,
    ) -> VerletResult<()> {
        let _ = (id, config);
        todo!("EMO-553: claim id, build VerletAppServer outside the map lock, insert")
    }

    /// Shut down and deregister one instance: drop its credential routes
    /// first (new connections stop routing to it), then
    /// [`VerletAppServer::shutdown`] (which drains instance tasks and
    /// closes its dispatch gate under any live host connection), then
    /// remove it from the map, releasing its root reservation with the
    /// last handle. Replace = `shutdown_instance` + `start_instance` over
    /// the same roots. Idempotent per id; unknown ids are an error.
    pub async fn shutdown_instance(&self, id: &InstanceId) -> VerletResult<()> {
        let _ = id;
        todo!("EMO-553: retire routes, shutdown instance outside the map lock, remove")
    }

    /// Observe: the ids of currently hosted instances, for logs and tests.
    pub async fn instance_ids(&self) -> Vec<InstanceId> {
        todo!("EMO-553: snapshot of the instance map keys")
    }

    /// Observe/dispatch: a handle to one hosted instance (cheap clone of
    /// its shared inner). In-process callers (the embedding orchestrator)
    /// dispatch through
    /// [`VerletAppServer::dispatch_authenticated_json_rpc`] on it.
    pub async fn instance(&self, id: &InstanceId) -> Option<VerletAppServer> {
        let _ = id;
        todo!("EMO-553: instance map lookup")
    }

    /// Register the route for one credential (decision 1): `token` is
    /// digested immediately and only the digest is kept. Non-authoritative
    /// — the routed instance's authority still verifies the token on every
    /// connection. Re-registering a digest moves the route (last write
    /// wins); routes may be registered before the instance starts, and a
    /// route to an absent instance refuses connections like an unrouted
    /// credential.
    pub fn register_credential_route(&self, token: &str, instance: InstanceId) {
        let _ = (token, instance);
        todo!("EMO-553: insert identity_token_digest(token) into the route table")
    }

    /// Drop the route for one credential; presented again, it refuses per
    /// decision 3. Retiring an unknown digest is a no-op.
    pub fn retire_credential_route(&self, token: &str) {
        let _ = token;
        todo!("EMO-553: remove identity_token_digest(token) from the route table")
    }

    /// The ONE listener (loopback-guarded like the standalone server).
    /// Accept loop and per-connection tasks run on the host task set.
    /// Per connection: peek the HTTP request, extract the bearer token
    /// (`request_bearer_token`), digest it, select the routed instance,
    /// and hand the un-consumed stream to
    /// [`VerletAppServer::serve_host_routed_tcp_stream`] — which
    /// authenticates against that instance's own authority and witnesses
    /// the session on `BoundarySurface::Host`. No route: refuse per
    /// decision 3 without touching any instance.
    pub async fn serve_websocket_listener(
        &self,
        listener: tokio::net::TcpListener,
    ) -> VerletResult<()> {
        let _ = listener;
        todo!("EMO-553: accept loop on the host task set, route by credential digest")
    }

    /// Host shutdown: cancel + drain the listener and every connection
    /// task, then shut down every remaining instance (each per
    /// [`VerletAppServer::shutdown`]), then clear the route table.
    /// Idempotent. Explicit shutdown is mandatory (EMO-551 policy: drop
    /// never tears down).
    pub async fn shutdown(&self) -> VerletResult<()> {
        todo!("EMO-553: drain host tasks, shut down instances, clear routes")
    }
}
