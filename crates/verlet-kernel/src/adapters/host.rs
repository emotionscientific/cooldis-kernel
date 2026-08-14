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

/// Host-scoped name of one hosted instance. Distinct from tenant id (an
/// instance HAS a tenant; the host does not interpret it) and never taken
/// from an RPC body — the routed connection is the only source.
#[derive(Clone, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub struct InstanceId(String);

impl InstanceId {
    /// Non-empty, no whitespace or control characters; used in logs and error
    /// messages, so keep it printable. Never secret material.
    pub fn new(id: impl Into<String>) -> crate::kernel::runtime_host::VerletResult<Self> {
        let id = id.into();
        if id.is_empty()
            || id
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "instance id must be non-empty without whitespace or control characters: {id:?}"
                ),
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

/// Bind policy for the host listener (EMO-564). The default keeps the
/// loopback guard; a deployment that serves the host over a private
/// network (Railway project networking) opts in explicitly through its
/// host config. There is no "public bind" tier: nothing here terminates
/// TLS, and the opt-in name says exactly what is being waived.
#[derive(Clone, Copy, Debug, Default)]
pub struct HostListenerOptions {
    pub allow_non_loopback: bool,
}

/// The host facade: owns the instances, the credential route table, and
/// the listener + connection tasks. Cloning shares the one host.
#[derive(Clone)]
pub struct VerletHost {
    inner: std::sync::Arc<VerletHostInner>,
}

struct VerletHostInner {
    /// The hosted instances. Instance construction and shutdown happen
    /// OUTSIDE this lock (both are slow: store opens, task drains) — the
    /// lock guards the map, not the lifecycle transitions; a per-id claim
    /// keeps concurrent start/shutdown of the same id single-file.
    instances: tokio::sync::RwLock<
        std::collections::HashMap<InstanceId, crate::adapters::app_server::VerletAppServer>,
    >,
    /// Credential-digest → instance selection table (decision 1). Values
    /// are digests from `identity_token_digest`; raw tokens are never
    /// stored, logged, or printed.
    credential_routes: std::sync::Mutex<std::collections::HashMap<String, InstanceId>>,
    /// Listener + per-connection tasks (decision 2).
    tasks: crate::adapters::app_server::lifecycle::InstanceTaskSet,
    /// The v0 host owns exactly one listener installation for its lifetime.
    listener_installed: std::sync::atomic::AtomicBool,
    /// Per-id lifecycle serialization. Claims live for the host lifetime so
    /// replacing an instance cannot race a stale claim being removed and
    /// recreated under a waiter.
    lifecycle_claims: std::sync::Mutex<
        std::collections::HashMap<InstanceId, std::sync::Arc<tokio::sync::Mutex<()>>>,
    >,
    /// Set once by [`VerletHost::shutdown`]; afterwards every lifecycle and
    /// serve entry fails fast. Construction holds a read guard through map
    /// insertion so no completed instance can appear behind shutdown's
    /// snapshot. Per-instance shutdown releases its guard before draining so
    /// global shutdown can cancel host-owned connections blocking that drain.
    shutdown: tokio::sync::RwLock<bool>,
}

impl Default for VerletHost {
    fn default() -> Self {
        Self::new()
    }
}

impl VerletHost {
    pub fn new() -> Self {
        Self {
            inner: std::sync::Arc::new(VerletHostInner {
                instances: tokio::sync::RwLock::new(std::collections::HashMap::new()),
                credential_routes: std::sync::Mutex::new(std::collections::HashMap::new()),
                tasks: crate::adapters::app_server::lifecycle::InstanceTaskSet::new(),
                listener_installed: std::sync::atomic::AtomicBool::new(false),
                lifecycle_claims: std::sync::Mutex::new(std::collections::HashMap::new()),
                shutdown: tokio::sync::RwLock::new(false),
            }),
        }
    }

    /// Construct and register one instance under `id`. The config must be
    /// a hosted config ([`crate::adapters::app_server::VerletAppServerConfig::hosted`]); root
    /// reservation (EMO-552) already guarantees two live instances cannot
    /// share storage, whichever host they belong to. Fails on duplicate
    /// `id`, after host shutdown, and on any constructor failure (the
    /// constructor's own cleanup applies, EMO-551).
    pub async fn start_instance(
        &self,
        id: InstanceId,
        config: crate::adapters::app_server::VerletAppServerConfig,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if *self.inner.shutdown.read().await {
            return Err(host_error("Verlet host is shut down"));
        }
        let claim = self.lifecycle_claim(&id);
        let _claim = claim.lock().await;
        let shutdown = self.inner.shutdown.read().await;
        if *shutdown {
            return Err(host_error("Verlet host is shut down"));
        }
        if self.inner.instances.read().await.contains_key(&id) {
            return Err(host_error(format!(
                "Verlet host instance {id} already exists"
            )));
        }
        if !config.is_hosted() {
            return Err(host_error(format!(
                "Verlet host instance {id} requires crate::adapters::app_server::VerletAppServerConfig::hosted"
            )));
        }

        let instance = crate::adapters::app_server::VerletAppServer::new(config).await?;
        self.inner.instances.write().await.insert(id, instance);
        drop(shutdown);
        Ok(())
    }

    /// Shut down and deregister one instance: drop its credential routes
    /// first (new connections stop routing to it), then
    /// [`crate::adapters::app_server::VerletAppServer::shutdown`] (which drains instance tasks and
    /// closes its dispatch gate under any live host connection), then
    /// remove it from the map, releasing its root reservation with the
    /// last handle. Replace = `shutdown_instance` + `start_instance` over
    /// the same roots. Idempotent per id; unknown ids are an error.
    pub async fn shutdown_instance(
        &self,
        id: &InstanceId,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        if *self.inner.shutdown.read().await {
            return Err(host_error("Verlet host is shut down"));
        }
        let claim = self.lifecycle_claim(id);
        let _claim = claim.lock().await;
        let shutdown = self.inner.shutdown.read().await;
        if *shutdown {
            return Err(host_error("Verlet host is shut down"));
        }
        let instance = self
            .inner
            .instances
            .read()
            .await
            .get(id)
            .cloned()
            .ok_or_else(|| host_error(format!("Verlet host instance {id} was not found")))?;

        self.inner
            .credential_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|_, routed_id| routed_id != id);
        // Global shutdown must be able to cancel host-owned connection tasks
        // while this instance waits for their per-request dispatch reads. The
        // per-id claim keeps replacement serialized after this guard drops;
        // if global shutdown races from here, the app-server shutdown mutex
        // makes the two instance shutdown calls idempotent.
        drop(shutdown);
        instance.shutdown().await?;
        self.inner.instances.write().await.remove(id);
        drop(instance);
        Ok(())
    }

    /// Observe: the ids of currently hosted instances, for logs and tests.
    pub async fn instance_ids(&self) -> Vec<InstanceId> {
        let mut ids = self
            .inner
            .instances
            .read()
            .await
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        ids.sort();
        ids
    }

    /// Observe/dispatch: a handle to one hosted instance (cheap clone of
    /// its shared inner). In-process callers (the embedding orchestrator)
    /// dispatch through
    /// [`crate::adapters::app_server::VerletAppServer::dispatch_authenticated_json_rpc`] on it.
    pub async fn instance(
        &self,
        id: &InstanceId,
    ) -> Option<crate::adapters::app_server::VerletAppServer> {
        self.inner.instances.read().await.get(id).cloned()
    }

    /// Register the route for one credential (decision 1): `token` is
    /// digested immediately and only the digest is kept. Non-authoritative
    /// — the routed instance's authority still verifies the token on every
    /// connection. Re-registering a digest moves the route (last write
    /// wins); routes may be registered before the instance starts, and a
    /// route to an absent instance refuses connections like an unrouted
    /// credential. Registration after host shutdown is a no-op because a
    /// drained host cannot serve again.
    pub fn register_credential_route(&self, token: &str, instance: InstanceId) {
        self.register_credential_route_digest(
            crate::daemon::identity::identity_token_digest(token),
            instance,
        );
    }

    /// Register the route for one credential by its digest (EMO-564): the
    /// host deployment's config carries digests only, so the host process
    /// never holds kernel access tokens. Same semantics as
    /// [`VerletHost::register_credential_route`] minus the digest step; the
    /// digest format is whatever `identity mint` printed
    /// (`identity_token_digest` of the token).
    pub fn register_credential_route_digest(
        &self,
        digest: impl Into<String>,
        instance: InstanceId,
    ) {
        let mut routes = self
            .inner
            .credential_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Serialize the shutdown check with route insertion. Shutdown cancels
        // the task set before taking this mutex to clear routes, so an insert
        // either happens before that clear or observes cancellation and is a
        // no-op; it can never land behind the final clear.
        if self.inner.tasks.is_shutdown() {
            return;
        }
        routes.insert(digest.into(), instance);
    }

    /// Drop the route for one credential; presented again, it refuses per
    /// decision 3. Retiring an unknown digest is a no-op. Routing is consulted
    /// only when a connection is accepted, so this does not end connections
    /// that are already authenticated. Credential validity and revocation are
    /// separate concerns owned by the routed instance's identity authority.
    pub fn retire_credential_route(&self, token: &str) {
        let digest = crate::daemon::identity::identity_token_digest(token);
        self.inner
            .credential_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&digest);
    }

    /// The ONE listener (loopback-guarded like the standalone server).
    /// Accept loop and per-connection tasks run on the host task set.
    /// Per connection: peek the HTTP request, extract the bearer token
    /// (`request_bearer_token`), digest it, select the routed instance,
    /// and hand the un-consumed stream to
    /// [`crate::adapters::app_server::VerletAppServer::serve_host_routed_tcp_stream`] — which
    /// authenticates against that instance's own authority and witnesses
    /// the session on `BoundarySurface::Host`. No route: refuse per
    /// decision 3 without touching any instance.
    pub async fn serve_websocket_listener(
        &self,
        listener: tokio::net::TcpListener,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.serve_websocket_listener_with_options(listener, HostListenerOptions::default())
            .await
    }

    /// [`VerletHost::serve_websocket_listener`] with an explicit bind
    /// policy (EMO-564). `allow_non_loopback: true` is the deployment
    /// opt-in for serving on a private network address (Railway project
    /// networking): the credential-per-instance authentication carries the
    /// boundary there; the loopback guard remains the unconfigured
    /// default and its error message is unchanged.
    pub async fn serve_websocket_listener_with_options(
        &self,
        listener: tokio::net::TcpListener,
        options: HostListenerOptions,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let shutdown = self.inner.shutdown.read().await;
        if *shutdown {
            return Err(host_error("Verlet host is shut down"));
        }
        let addr = listener.local_addr().map_err(|error| {
            host_error(format!(
                "failed to inspect Verlet host websocket listener: {error}"
            ))
        })?;
        if !options.allow_non_loopback && !addr.ip().is_loopback() {
            return Err(host_error(format!(
                "Verlet host websocket listen address {addr} is not loopback"
            )));
        }
        if self
            .inner
            .listener_installed
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::AcqRel,
                std::sync::atomic::Ordering::Acquire,
            )
            .is_err()
        {
            return Err(host_error(
                "Verlet host websocket listener already installed",
            ));
        }

        let host = self.clone();
        let cancellation = self.inner.tasks.cancellation();
        let accepted = self.inner.tasks.spawn(async move {
            loop {
                let accepted = tokio::select! {
                    _ = cancellation.cancelled() => return,
                    accepted = listener.accept() => accepted,
                };
                let (stream, peer) = match accepted {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        eprintln!("Verlet host websocket accept failed: {error}");
                        continue;
                    }
                };
                let connection_host = host.clone();
                host.inner.tasks.spawn_cancellable(async move {
                    if let Err(error) = connection_host.route_tcp_stream(stream).await {
                        eprintln!("Verlet host websocket connection from {peer} failed: {error}");
                    }
                });
            }
        });
        drop(shutdown);
        if !accepted {
            self.inner
                .listener_installed
                .store(false, std::sync::atomic::Ordering::Release);
            return Err(host_error("Verlet host is shut down"));
        }
        Ok(())
    }

    /// Host shutdown: cancel + drain the listener and every connection
    /// task, then shut down every remaining instance (each per
    /// [`crate::adapters::app_server::VerletAppServer::shutdown`]), then clear the route table.
    /// Idempotent. Explicit shutdown is mandatory (EMO-551 policy: drop
    /// never tears down).
    pub async fn shutdown(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        let mut shutdown = self.inner.shutdown.write().await;
        *shutdown = true;

        // The flag is the fail-fast lifecycle barrier, not proof that the
        // drain completed. If this future was previously cancelled, rerun the
        // idempotent task and instance drains to completion.
        self.inner.tasks.shutdown().await;
        let instances = self
            .inner
            .instances
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let mut first_error = None;
        for instance in &instances {
            if let Err(error) = instance.shutdown().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        self.inner.instances.write().await.clear();
        self.inner
            .credential_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        drop(instances);
        drop(shutdown);
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn lifecycle_claim(&self, id: &InstanceId) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let mut claims = self
            .inner
            .lifecycle_claims
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        std::sync::Arc::clone(
            claims
                .entry(id.clone())
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(()))),
        )
    }

    async fn route_tcp_stream(
        &self,
        stream: tokio::net::TcpStream,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let request = crate::adapters::app_server::peek_http_request(&stream).await?;
        if request.as_ref().is_some_and(is_health_check_request) {
            return respond_health_ok(stream).await;
        }
        // Requests that name the health path but do not exactly match its
        // public contract never enter credential routing. This keeps HEAD,
        // POST, and query variants on the uniform 401 surface even if they
        // carry an otherwise routed credential.
        if request
            .as_ref()
            .is_some_and(|request| request.path() == "/healthz")
        {
            drop(request);
            return crate::adapters::app_server::refuse_host_tcp_stream(stream).await;
        }
        let digest = request
            .as_ref()
            .and_then(crate::adapters::app_server::request_bearer_token)
            .map(|(token, _)| crate::daemon::identity::identity_token_digest(token));
        drop(request);
        let Some(digest) = digest else {
            eprintln!("Verlet host refused a connection without a routed credential");
            return crate::adapters::app_server::refuse_host_tcp_stream(stream).await;
        };
        let routed_id = self
            .inner
            .credential_routes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&digest)
            .cloned();
        let Some(routed_id) = routed_id else {
            eprintln!("Verlet host refused unrouted credential digest {digest}");
            return crate::adapters::app_server::refuse_host_tcp_stream(stream).await;
        };
        let instance = self.inner.instances.read().await.get(&routed_id).cloned();
        let Some(instance) = instance else {
            eprintln!(
                "Verlet host refused credential digest {digest} routed to absent instance {routed_id}"
            );
            return crate::adapters::app_server::refuse_host_tcp_stream(stream).await;
        };
        instance.serve_host_routed_tcp_stream(stream).await
    }
}

fn host_error(message: impl Into<String>) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(message.into())
}

/// `GET /healthz` on the host listener (EMO-564): the deployment health
/// probe. Matched before any credential handling; everything else
/// unauthenticated keeps the 401 refusal shape.
fn is_health_check_request(request: &crate::adapters::app_server::HttpRequestHead) -> bool {
    request.method() == "GET" && request.path() == "/healthz" && !request.has_query()
}

/// Answer a health probe with a minimal `200 OK` (no body detail — the
/// health endpoint discloses liveness only, never instance names or
/// counts) and close the stream.
async fn respond_health_ok(
    mut stream: tokio::net::TcpStream,
) -> crate::kernel::runtime_host::VerletResult<()> {
    use tokio::io::AsyncWriteExt as _;

    crate::adapters::app_server::consume_http_request_headers(&mut stream).await?;
    stream
        .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        .await
        .map_err(|error| {
            host_error(format!(
                "failed to write Verlet host health response: {error}"
            ))
        })?;
    stream.shutdown().await.map_err(|error| {
        host_error(format!(
            "failed to close Verlet host health response: {error}"
        ))
    })
}

#[cfg(test)]
mod tests {
    fn test_identity(
        tenant: &str,
        operator: &str,
    ) -> crate::daemon::identity::VerletDaemonIdentityConfig {
        crate::daemon::identity::VerletDaemonIdentityConfig {
            mode: crate::daemon::identity::IdentityMode::Local,
            tenant_id: Some(tenant.to_string()),
            console_principal: Some(crate::daemon::identity::PrincipalId::new(operator)),
        }
    }

    fn test_config(
        root: &std::path::Path,
        tenant: &str,
        operator: &str,
    ) -> crate::adapters::app_server::VerletAppServerConfig {
        let workspace = root.parent().unwrap().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        crate::adapters::app_server::VerletAppServerConfig::hosted(
            crate::adapters::app_server::instance::InstanceRoots::under(root),
            crate::adapters::app_server::instance::InstanceEnvironment {
                provider_auth: crate::adapters::app_server::instance::ProviderAuthSource::Injected(
                    verlet_metadata::provider_store::LlmProviderAuthContext::new(),
                ),
                hook_shell: Some("/bin/sh".to_string()),
                process_ids: std::sync::Arc::new(
                    verlet_process::process::DeterministicProcessIds::new(),
                ),
            },
            workspace,
            &test_identity(tenant, operator),
        )
        .unwrap()
    }

    fn test_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("verlet-host-unit-{label}-{}", uuid::Uuid::now_v7()))
    }

    #[test]
    fn instance_ids_reject_log_control_characters() {
        for invalid in ["", "two words", "line\nfeed", "escape\u{1b}", "nul\0byte"] {
            assert!(crate::adapters::host::InstanceId::new(invalid).is_err());
        }
        assert_eq!(
            crate::adapters::host::InstanceId::new("tenant-01")
                .unwrap()
                .as_str(),
            "tenant-01"
        );
    }

    #[tokio::test]
    async fn concurrent_starts_for_one_id_admit_exactly_one_instance() {
        let root = test_root("same-id-start");
        let host = crate::adapters::host::VerletHost::new();
        let id = crate::adapters::host::InstanceId::new("same").unwrap();
        let first = {
            let host = host.clone();
            let id = id.clone();
            let config = test_config(&root.join("first"), "tenant-first", "operator-first");
            async move { host.start_instance(id, config).await }
        };
        let second = {
            let host = host.clone();
            let id = id.clone();
            let config = test_config(&root.join("second"), "tenant-second", "operator-second");
            async move { host.start_instance(id, config).await }
        };

        let (first, second) = tokio::join!(first, second);
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        assert_eq!(host.instance_ids().await, vec![id]);

        host.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn shutdown_and_concurrent_start_cannot_leave_a_late_instance() {
        let root = test_root("shutdown-start-race");
        let host = crate::adapters::host::VerletHost::new();
        let start = {
            let host = host.clone();
            let config = test_config(&root.join("instance"), "tenant", "operator");
            tokio::spawn(async move {
                host.start_instance(
                    crate::adapters::host::InstanceId::new("instance").unwrap(),
                    config,
                )
                .await
            })
        };
        tokio::task::yield_now().await;
        let shutdown = host.shutdown();
        let (start, shutdown) = tokio::join!(start, shutdown);
        let _ = start.unwrap();
        shutdown.unwrap();

        assert!(host.instance_ids().await.is_empty());
        assert!(
            host.start_instance(
                crate::adapters::host::InstanceId::new("late").unwrap(),
                test_config(&root.join("late"), "late-tenant", "late-operator"),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("shut down")
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn shutdown_resumes_after_the_draining_caller_is_cancelled() {
        let host = crate::adapters::host::VerletHost::new();
        let started = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        let task_started = std::sync::Arc::clone(&started);
        let task_release = std::sync::Arc::clone(&release);
        host.inner.tasks.spawn(async move {
            task_started.notify_one();
            task_release.notified().await;
        });
        started.notified().await;

        let first = {
            let host = host.clone();
            tokio::spawn(async move { host.shutdown().await })
        };
        host.inner.tasks.cancellation().cancelled().await;
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        release.notify_one();
        host.shutdown().await.unwrap();
        assert_eq!(host.inner.tasks.task_count(), 0);
    }

    #[tokio::test]
    async fn instance_shutdown_retires_every_route_to_that_instance() {
        let root = test_root("route-retirement");
        let host = crate::adapters::host::VerletHost::new();
        let id = crate::adapters::host::InstanceId::new("instance").unwrap();
        host.start_instance(
            id.clone(),
            test_config(&root.join("instance"), "tenant", "operator"),
        )
        .await
        .unwrap();
        host.register_credential_route("first-secret", id.clone());
        host.register_credential_route("second-secret", id.clone());
        assert_eq!(
            host.inner
                .credential_routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len(),
            2
        );

        host.shutdown_instance(&id).await.unwrap();
        assert!(
            host.inner
                .credential_routes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        host.shutdown().await.unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn listener_requires_explicit_non_loopback_opt_in() {
        let host = crate::adapters::host::VerletHost::new();
        let guarded = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
        let error = host.serve_websocket_listener(guarded).await.unwrap_err();
        assert!(error.to_string().contains("is not loopback"), "{error}");

        let allowed = tokio::net::TcpListener::bind("0.0.0.0:0").await.unwrap();
        host.serve_websocket_listener_with_options(
            allowed,
            crate::adapters::host::HostListenerOptions {
                allow_non_loopback: true,
            },
        )
        .await
        .unwrap();
        host.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn healthz_is_the_only_unauthenticated_success() {
        use tokio::io::AsyncReadExt as _;
        use tokio::io::AsyncWriteExt as _;

        let host = crate::adapters::host::VerletHost::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        host.serve_websocket_listener(listener).await.unwrap();

        async fn request(
            addr: std::net::SocketAddr,
            method: &str,
            target: &str,
            extra_headers: &str,
        ) -> String {
            let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            stream
                .write_all(
                    format!("{method} {target} HTTP/1.1\r\nHost: {addr}\r\n{extra_headers}\r\n")
                        .as_bytes(),
                )
                .await
                .unwrap();
            let mut response = String::new();
            tokio::time::timeout(
                std::time::Duration::from_secs(30),
                stream.read_to_string(&mut response),
            )
            .await
            .unwrap()
            .unwrap();
            response
        }

        let extra_headers = (0..40)
            .map(|index| format!("X-Railway-Probe-{index}: accepted\r\n"))
            .collect::<String>();
        let health = request(addr, "GET", "/healthz", &extra_headers).await;
        assert!(health.starts_with("HTTP/1.1 200 OK\r\n"), "{health}");
        assert!(health.ends_with("\r\n\r\n"), "{health}");
        assert!(health.contains("Content-Length: 0\r\n"), "{health}");

        let other = request(addr, "GET", "/other", "").await;
        assert!(
            other.starts_with("HTTP/1.1 401 Unauthorized\r\n"),
            "{other}"
        );
        let queried = request(addr, "GET", "/healthz?detail=true", "").await;
        assert!(
            queried.starts_with("HTTP/1.1 401 Unauthorized\r\n"),
            "{queried}"
        );
        for method in ["HEAD", "POST"] {
            let response = request(addr, method, "/healthz", "").await;
            assert!(
                response.starts_with("HTTP/1.1 401 Unauthorized\r\n"),
                "{method} unexpectedly succeeded: {response}"
            );
        }

        // A client that abandons a health response is connection-local. The
        // accept task must continue serving later probes.
        let mut abandoned = tokio::net::TcpStream::connect(addr).await.unwrap();
        abandoned
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: abandoned\r\n\r\n")
            .await
            .unwrap();
        drop(abandoned);
        let after_abandon = request(addr, "GET", "/healthz", "").await;
        assert!(after_abandon.starts_with("HTTP/1.1 200 OK\r\n"));

        // A slow, incomplete health request occupies only its own bounded
        // connection task and does not stall the listener's accept loop.
        let mut slow = tokio::net::TcpStream::connect(addr).await.unwrap();
        slow.write_all(b"GET /healthz HTTP/1.1\r\nX-Slow: ")
            .await
            .unwrap();
        let while_slow = request(addr, "GET", "/healthz", "").await;
        assert!(while_slow.starts_with("HTTP/1.1 200 OK\r\n"));

        let mut malformed = tokio::net::TcpStream::connect(addr).await.unwrap();
        malformed
            .write_all(b"GET /healthz HTTP/1.1\r\nX-Binary: \xff\r\n\r\n")
            .await
            .unwrap();
        let mut malformed_response = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(30),
            malformed.read_to_string(&mut malformed_response),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            malformed_response.starts_with("HTTP/1.1 401 Unauthorized\r\n"),
            "{malformed_response}"
        );

        host.shutdown().await.unwrap();
        assert_eq!(host.inner.tasks.task_count(), 0);
        drop(slow);
    }
}
