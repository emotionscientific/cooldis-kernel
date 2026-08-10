//! The `host` subcommand family (EMO-564): run one multi-tenant host
//! process serving many kernel instances from a single config file.
//!
//! `verlet host run --config <host.toml>` is the cloud deployment entry
//! (EMO-549 stage 4): the `verlet-host` Railway service runs exactly this,
//! with every instance's roots on one persistent volume and the listener
//! bound on the project private network.
//!
//! Architect decisions (fixed; the implementation must not revisit them):
//!
//! 1. **Config carries credential digests, never tokens.** Routes are
//!    registered via [`crate::adapters::host::VerletHost::register_credential_route_digest`];
//!    the host process env holds no kernel access credentials. Provider
//!    API keys are the one secret class that reaches this process, and
//!    only as env var NAMES in config, resolved at boot.
//! 2. **Non-loopback bind is an explicit opt-in** (`allow_non_loopback`),
//!    threaded to [`crate::adapters::host::HostListenerOptions`]. Absent
//!    or false keeps the loopback guard and its existing error message.
//! 3. **Managed identity mode for every hosted instance**: `tenant_id`
//!    and `console_principal` are required per instance and validated
//!    with the standalone daemon's hard-fail rule (ADR 0008 D5).
//! 4. **Fail the whole boot before starting any instance** on any config
//!    error: parse failure, relative path, duplicate instance id,
//!    duplicate route digest, unresolvable provider env var. Instances
//!    must not come up behind a partially-invalid config.

#[cfg(test)]
mod tests;

/// The parsed `host.toml`. Section names mirror the daemon config file
/// (`crate::daemon::daemon_config`) where the concepts match.
///
/// ```toml
/// [listen]
/// addr = "[::]:7900"
/// allow_non_loopback = true
///
/// [[instance]]
/// id = "orch"
/// root = "/data/instances/orch"
/// cwd = "/data/instances/orch/workspace"
/// tenant_id = "orch"
/// console_principal = "operator:orch"
/// hook_shell = "/bin/sh"
/// clock = true
/// route_digests = ["sha256:<64 lowercase hex characters>"]
///
/// [instance.provider]
/// provider = "bifrost_openai"
/// base_url = "https://..."
/// api_key_env = "VERLET_HOST_ORCH_PROVIDER_KEY"
/// model = "..."
/// ```
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerletHostRunConfig {
    pub(crate) listen: HostListenConfig,
    #[serde(default)]
    pub(crate) instance: Vec<HostInstanceConfig>,
}

/// `[listen]`: the one TCP listener every instance is served through.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostListenConfig {
    /// TCP socket address (`"[::]:7900"`, `"127.0.0.1:7900"`).
    pub(crate) addr: String,
    /// Explicit opt-in for a non-loopback bind (decision 2).
    #[serde(default)]
    pub(crate) allow_non_loopback: bool,
}

/// One `[[instance]]` entry: everything needed to construct a hosted
/// kernel instance ([`crate::adapters::app_server::VerletAppServerConfig::hosted`]).
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostInstanceConfig {
    /// Host-scoped instance id ([`crate::adapters::host::InstanceId`] rules).
    pub(crate) id: String,
    /// Absolute instance directory; roots laid out via
    /// [`crate::adapters::app_server::instance::InstanceRoots::under`].
    pub(crate) root: std::path::PathBuf,
    /// Absolute working directory for the instance's agent processes.
    pub(crate) cwd: std::path::PathBuf,
    /// Required (decision 3).
    pub(crate) tenant_id: String,
    /// Required (decision 3).
    pub(crate) console_principal: String,
    /// Absolute shell path injected for agent hooks (EMO-552: hosted
    /// instances never read `SHELL`/`COMSPEC`).
    // lexicon-allow: hook - architect-fixed host config field from EMO-564.
    pub(crate) hook_shell: String,
    /// Run the instance-owned `clock.tick` route. Defaults to true when
    /// absent; set false only when mandates must remain externally driven.
    #[serde(default)]
    pub(crate) clock: Option<bool>,
    /// Credential digests routed to this instance (decision 1). May be
    /// empty (an instance reachable only in-process), but a digest listed
    /// under two instances is a config error.
    #[serde(default)]
    pub(crate) route_digests: Vec<String>,
    pub(crate) provider: HostInstanceProviderConfig,
}

impl HostInstanceConfig {
    fn clock_enabled(&self) -> bool {
        self.clock.unwrap_or(true)
    }
}

/// `[instance.provider]`: the injected provider auth for one instance
/// (EMO-552: hosted instances never snapshot the process environment).
/// Field names follow the daemon config `[provider]` section.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HostInstanceProviderConfig {
    /// `"local_offline"` (tests, smoke) or `"bifrost_openai"`.
    pub(crate) provider: String,
    #[serde(default)]
    pub(crate) base_url: Option<String>,
    /// Env var NAME holding the API key; resolved once at boot. The value
    /// never appears in config, argv, or logs.
    #[serde(default)]
    pub(crate) api_key_env: Option<String>,
    #[serde(default)]
    pub(crate) model: Option<String>,
    /// Secret resolved once while loading the config. This is not part of
    /// the TOML surface and its `Debug` implementation is always redacted.
    #[serde(skip)]
    resolved_api_key: Option<ResolvedProviderApiKey>,
}

#[derive(Clone)]
struct ResolvedProviderApiKey(String);

impl std::fmt::Debug for ResolvedProviderApiKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
}

/// Route `verlet host <subcommand>`.
pub(super) async fn run_host(
    mut args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args.is_empty()
        || args
            .first()
            .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_host_help();
        return Ok(());
    }

    let subcommand = args.remove(0);
    match subcommand.to_string_lossy().as_ref() {
        "run" => host_run(args).await,
        other => Err(crate::cli::usage_error(format!(
            "unknown host subcommand {other:?}"
        ))),
    }
}

/// `verlet host run --config <host.toml>`: load + validate the config,
/// boot every instance, register routes, install the listener, print one
/// liveness line (instance count + listen addr, no secrets, no instance
/// ids), then wait for SIGTERM/SIGINT and run
/// [`crate::adapters::host::VerletHost::shutdown`]. Exit is non-zero on
/// any boot or shutdown error.
pub(super) async fn host_run(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if args
        .first()
        .is_some_and(|arg| arg == "--help" || arg == "-h")
    {
        print_host_run_help();
        return Ok(());
    }
    let config_path = parse_host_run_args(args)?;
    let config = load_host_run_config(&config_path)?;
    serve_until_signal(config).await
}

fn parse_host_run_args(
    args: Vec<std::ffi::OsString>,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    let mut config_path = None;
    let mut iter = args.into_iter();
    while let Some(arg) = iter.next() {
        match arg.to_string_lossy().as_ref() {
            "--config" => {
                config_path = Some(crate::cli::tool::required_path_value(
                    &mut iter, "--config",
                )?)
            }
            other => {
                return Err(crate::cli::usage_error(format!(
                    "unknown host run argument {other:?}"
                )));
            }
        }
    }
    config_path.ok_or_else(|| crate::cli::usage_error("host run requires --config <host.toml>"))
}

/// Read and validate one `host.toml` (decision 4 — every error names the
/// offending instance id or field; collect-and-report like the daemon
/// config validator rather than first-error-wins where cheap).
///
/// Validation beyond serde: at least one instance; instance ids parse as
/// [`crate::adapters::host::InstanceId`] and are unique; `root`, `cwd`,
/// `hook_shell` absolute; `tenant_id`/`console_principal` non-blank
/// (decision 3); route digests non-blank and globally unique across
/// instances; each route has the exact `sha256:<64 lowercase hex>` shape
/// printed by `identity mint`; provider is a known name; `bifrost_openai`
/// requires `base_url`, `api_key_env`, and `model`, and the named env var
/// must resolve non-empty at load time; `local_offline` requires none of them.
pub(crate) fn load_host_run_config(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<VerletHostRunConfig> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        crate::cli::usage_error(format!(
            "failed to read Verlet host config {}: {error}",
            path.display()
        ))
    })?;
    let mut config = toml::from_str::<VerletHostRunConfig>(&text).map_err(|error| {
        crate::cli::usage_error(format!(
            "failed to parse Verlet host config {}: {error}",
            path.display()
        ))
    })?;
    let errors = validate_host_run_config(&mut config);
    if errors.is_empty() {
        Ok(config)
    } else {
        Err(crate::cli::usage_error(format!(
            "invalid Verlet host config {}:\n- {}",
            path.display(),
            errors.join("\n- ")
        )))
    }
}

fn validate_host_run_config(config: &mut VerletHostRunConfig) -> Vec<String> {
    let mut errors = Vec::new();
    if config.listen.addr.parse::<std::net::SocketAddr>().is_err() {
        errors.push(format!(
            "listen.addr must be a TCP socket address, got {:?}",
            config.listen.addr
        ));
    }
    if config.instance.is_empty() {
        errors.push("host config requires at least one [[instance]]".to_string());
    }

    let mut instance_ids = std::collections::BTreeSet::new();
    let mut route_digests = std::collections::BTreeMap::new();
    for instance in &mut config.instance {
        let scope = format!("instance {:?}", instance.id);
        if let Err(error) = crate::adapters::host::InstanceId::new(instance.id.clone()) {
            errors.push(format!("{scope}.id: {error}"));
        } else if !instance_ids.insert(instance.id.clone()) {
            errors.push(format!("instance id {:?} is duplicated", instance.id));
        }
        for (field, path) in [
            ("root", instance.root.as_path()),
            ("cwd", instance.cwd.as_path()),
            ("hook_shell", std::path::Path::new(&instance.hook_shell)),
        ] {
            if !path.is_absolute() {
                errors.push(format!(
                    "{scope}.{field} must be absolute: {}",
                    path.display()
                ));
            }
        }
        if instance.tenant_id.trim().is_empty() {
            errors.push(format!("{scope}.tenant_id must be non-blank"));
        }
        if instance.console_principal.trim().is_empty() {
            errors.push(format!("{scope}.console_principal must be non-blank"));
        }
        for (index, digest) in instance.route_digests.iter().enumerate() {
            if !is_identity_token_digest(digest) {
                errors.push(format!(
                    "{scope}.route_digests[{index}] must be a sha256 digest printed by `verlet identity mint`"
                ));
                continue;
            }
            if let Some((first_instance, first_index)) =
                route_digests.insert(digest.clone(), (instance.id.clone(), index))
            {
                errors.push(format!(
                    "{scope}.route_digests[{index}] duplicates instance {first_instance:?}.route_digests[{first_index}]"
                ));
            }
        }
        validate_host_provider(&scope, &mut instance.provider, &mut errors);
    }
    validate_instance_root_overlaps(&config.instance, &mut errors);
    errors
}

fn is_identity_token_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn validate_host_provider(
    scope: &str,
    provider: &mut HostInstanceProviderConfig,
    errors: &mut Vec<String>,
) {
    match provider.provider.as_str() {
        "local_offline" => {
            for (field, present) in [
                ("base_url", provider.base_url.is_some()),
                ("api_key_env", provider.api_key_env.is_some()),
                ("model", provider.model.is_some()),
            ] {
                if present {
                    errors.push(format!(
                        "{scope}.provider.{field} is not allowed for local_offline"
                    ));
                }
            }
        }
        "bifrost_openai" => {
            validate_required_provider_field(
                scope,
                "base_url",
                provider.base_url.as_deref(),
                errors,
            );
            validate_required_provider_field(
                scope,
                "api_key_env",
                provider.api_key_env.as_deref(),
                errors,
            );
            validate_required_provider_field(scope, "model", provider.model.as_deref(), errors);
            if let Some(name) = provider
                .api_key_env
                .as_deref()
                .filter(|name| !name.trim().is_empty())
            {
                match verlet_runtime_contracts::env_compat::var(name) {
                    Ok(value) if !value.trim().is_empty() => {
                        provider.resolved_api_key = Some(ResolvedProviderApiKey(value));
                    }
                    Ok(_) => errors.push(format!(
                        "{scope}.provider.api_key_env {name:?} resolved to an empty value"
                    )),
                    // `std::env::VarError::NotUnicode` owns the rejected
                    // environment value and its formatting may expose those
                    // secret bytes. Name the configured variable only.
                    Err(_) => errors.push(format!(
                        "{scope}.provider.api_key_env {name:?} did not resolve"
                    )),
                }
            }
        }
        other => errors.push(format!("{scope}.provider has unknown provider {other:?}")),
    }
}

fn validate_required_provider_field(
    scope: &str,
    field: &str,
    value: Option<&str>,
    errors: &mut Vec<String>,
) {
    if value.is_none_or(|value| value.trim().is_empty()) {
        errors.push(format!(
            "{scope}.provider.{field} is required for bifrost_openai"
        ));
    }
}

fn validate_instance_root_overlaps(instances: &[HostInstanceConfig], errors: &mut Vec<String>) {
    let roots = instances
        .iter()
        .filter(|instance| instance.root.is_absolute())
        .map(|instance| {
            let normalized = canonicalize_with_missing_tail(&instance.root);
            (instance, normalized)
        })
        .collect::<Vec<_>>();
    for first_index in 0..roots.len() {
        for second_index in (first_index + 1)..roots.len() {
            let (first, first_root) = &roots[first_index];
            let (second, second_root) = &roots[second_index];
            if first_root.starts_with(second_root) || second_root.starts_with(first_root) {
                errors.push(format!(
                    "instance roots overlap: {:?} {} and {:?} {}",
                    first.id,
                    first.root.display(),
                    second.id,
                    second.root.display()
                ));
            }
        }
    }
}

/// Resolve every existing path prefix (including symlinks) while retaining a
/// normalized missing tail. Host roots are commonly provisioned after config
/// validation; canonicalizing only the complete path would silently miss an
/// alias through an existing symlinked parent in that case.
fn canonicalize_with_missing_tail(path: &std::path::Path) -> std::path::PathBuf {
    for existing_prefix in path.ancestors() {
        if let Ok(canonical_prefix) = std::fs::canonicalize(existing_prefix) {
            if let Ok(missing_tail) = path.strip_prefix(existing_prefix) {
                return normalize_absolute_path(&canonical_prefix.join(missing_tail));
            }
        }
    }
    normalize_absolute_path(path)
}

fn normalize_absolute_path(path: &std::path::Path) -> std::path::PathBuf {
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            component => normalized.push(component.as_os_str()),
        }
    }
    normalized
}

/// Build the hosted app-server config for one `[[instance]]` entry:
/// `InstanceRoots::under(root)`, an injected
/// [`crate::adapters::app_server::instance::InstanceEnvironment`]
/// (provider auth resolved from the config per
/// [`HostInstanceProviderConfig`], `hook_shell`, random process ids), a
/// managed-mode identity config from `tenant_id`/`console_principal`,
/// and the chat provider wiring (`with_bifrost_openai` for
/// `bifrost_openai`; the local-offline default otherwise).
pub(crate) fn hosted_instance_config(
    instance: &HostInstanceConfig,
) -> crate::kernel::runtime_host::VerletResult<(
    crate::adapters::host::InstanceId,
    crate::adapters::app_server::VerletAppServerConfig,
)> {
    let id = crate::adapters::host::InstanceId::new(instance.id.clone())?;
    let provider_key = instance
        .provider
        .resolved_api_key
        .as_ref()
        .map(|key| key.0.clone());
    let provider_auth = match provider_key.as_deref() {
        Some(key) => verlet_metadata::provider_store::LlmProviderAuthContext::new()
            .with_runtime_api_key(
                crate::adapters::app_server::APP_SERVER_BIFROST_PROVIDER,
                key,
            ),
        None => verlet_metadata::provider_store::LlmProviderAuthContext::new(),
    };
    let environment = crate::adapters::app_server::instance::InstanceEnvironment {
        provider_auth: crate::adapters::app_server::instance::ProviderAuthSource::Injected(
            provider_auth,
        ),
        hook_shell: Some(instance.hook_shell.clone()),
        process_ids: std::sync::Arc::new(verlet_process::process::RandomProcessIds),
    };
    let identity = crate::daemon::identity::VerletDaemonIdentityConfig {
        mode: crate::daemon::identity::IdentityMode::Managed,
        tenant_id: Some(instance.tenant_id.clone()),
        console_principal: Some(crate::daemon::identity::PrincipalId::new(
            instance.console_principal.clone(),
        )),
    };
    identity.validate().map_err(|error| {
        crate::cli::usage_error(format!("instance {:?} identity: {error}", instance.id))
    })?;
    let config = crate::adapters::app_server::VerletAppServerConfig::hosted(
        crate::adapters::app_server::instance::InstanceRoots::under(&instance.root),
        environment,
        &instance.cwd,
        &identity,
    )?;
    let config = match instance.provider.provider.as_str() {
        "local_offline" => config,
        "bifrost_openai" => config.with_bifrost_openai(
            instance.provider.base_url.clone().ok_or_else(|| {
                crate::cli::usage_error(format!(
                    "instance {:?}.provider.base_url was not resolved",
                    instance.id
                ))
            })?,
            provider_key.ok_or_else(|| {
                crate::cli::usage_error(format!(
                    "instance {:?}.provider.api_key_env was not resolved",
                    instance.id
                ))
            })?,
            instance.provider.model.clone().ok_or_else(|| {
                crate::cli::usage_error(format!(
                    "instance {:?}.provider.model was not resolved",
                    instance.id
                ))
            })?,
        ),
        other => {
            return Err(crate::cli::usage_error(format!(
                "instance {:?}.provider has unknown provider {other:?}",
                instance.id
            )));
        }
    };
    Ok((id, config))
}

fn hosted_clock_io(
    id: &crate::adapters::host::InstanceId,
    root: &std::path::Path,
) -> (
    crate::daemon::daemon_config::VerletIoConfig,
    crate::daemon::daemon_config::VerletIoRouteConfig,
) {
    let mut io = crate::daemon::daemon_config::VerletIoConfig::default();
    io.resolve_paths(root);
    let route = crate::daemon::daemon_config::VerletIoRouteConfig {
        id: format!("clock-{id}"),
        kind: crate::daemon::clock_route::CLOCK_TICK_ROUTE_KIND.to_string(),
        enabled: true,
        policy: None,
        content_policies: None,
        threading: None,
        agent_ref: None,
        coalesce_bursts: None,
        ingress: None,
        egress_projection: Vec::new(),
        typing_simulation: None,
        egress_retry: crate::daemon::daemon_config::VerletEgressRetryConfig::default(),
        telegram: None,
        metadata: std::collections::BTreeMap::new(),
    };
    (io, route)
}

async fn start_hosted_clock(
    io: &crate::daemon::daemon_config::VerletIoConfig,
    route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    server: &crate::adapters::app_server::VerletAppServer,
    tasks: &mut Vec<tokio::task::JoinHandle<()>>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let bridge = crate::daemon::daemon_io::VerletDaemonIoBridge::from_app_server(server);
    let sink =
        crate::cli::daemon::route_sink_for_ingress(route, &io.ingress, &bridge, tasks).await?;
    crate::cli::daemon::start_clock_route(route, sink, server, tasks).await
}

/// Boot sequence + signal wait: construct [`crate::adapters::host::VerletHost`],
/// `start_instance` each (any failure shuts down what already started and
/// returns the error), register every route digest, bind the TCP
/// listener, `serve_websocket_listener_with_options` with the config's
/// bind policy, print the liveness line, then wait for SIGTERM/SIGINT
/// (`tokio::signal`) and run host shutdown.
pub(crate) async fn serve_until_signal(
    config: VerletHostRunConfig,
) -> crate::kernel::runtime_host::VerletResult<()> {
    #[cfg(unix)]
    {
        // Install both handlers before booting or printing liveness. Once an
        // operator can see the liveness line, an immediate signal must be
        // graceful rather than racing the platform's default action.
        let mut terminate = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        )
        .map_err(|error| {
            crate::cli::usage_error(format!("failed to install SIGTERM handler: {error}"))
        })?;
        let mut interrupt = tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::interrupt(),
        )
        .map_err(|error| {
            crate::cli::usage_error(format!("failed to install SIGINT handler: {error}"))
        })?;
        serve_until_shutdown(config, async move {
            tokio::select! {
                _ = terminate.recv() => Ok(()),
                _ = interrupt.recv() => Ok(()),
            }
        })
        .await
    }
    #[cfg(not(unix))]
    {
        let interrupt = tokio::spawn(async {
            tokio::signal::ctrl_c().await.map_err(|error| {
                crate::cli::usage_error(format!("failed to install Ctrl-C handler: {error}"))
            })
        });
        serve_until_shutdown(config, async move {
            interrupt.await.map_err(|error| {
                crate::cli::usage_error(format!("Ctrl-C signal task failed: {error}"))
            })?
        })
        .await
    }
}

async fn serve_until_shutdown<F>(
    config: VerletHostRunConfig,
    shutdown_signal: F,
) -> crate::kernel::runtime_host::VerletResult<()>
where
    F: std::future::Future<Output = crate::kernel::runtime_host::VerletResult<()>>,
{
    // Construct every config first. Hosted construction reserves canonical
    // roots, so an overlap or other constructor failure happens before any
    // instance starts; dropping this vector releases earlier reservations.
    let mut pending = Vec::with_capacity(config.instance.len());
    for instance in &config.instance {
        let (id, hosted) = hosted_instance_config(instance)?;
        pending.push((
            id,
            hosted,
            instance.route_digests.clone(),
            instance.clock_enabled(),
            instance.root.clone(),
        ));
    }

    let host = crate::adapters::host::VerletHost::new();
    let mut io_tasks = Vec::new();
    for (id, hosted, route_digests, clock_enabled, root) in pending.drain(..) {
        if let Err(error) = host.start_instance(id.clone(), hosted).await {
            return shutdown_after_boot_error(&host, &mut io_tasks, error).await;
        }
        if clock_enabled {
            let Some(server) = host.instance(&id).await else {
                let error = crate::cli::usage_error(format!(
                    "Verlet host instance {id} disappeared during boot"
                ));
                return shutdown_after_boot_error(&host, &mut io_tasks, error).await;
            };
            let (io, route) = hosted_clock_io(&id, &root);
            if let Err(error) = start_hosted_clock(&io, &route, &server, &mut io_tasks).await {
                return shutdown_after_boot_error(&host, &mut io_tasks, error).await;
            }
        }
        for digest in route_digests {
            host.register_credential_route_digest(digest, id.clone());
        }
    }

    let listener = match tokio::net::TcpListener::bind(&config.listen.addr).await {
        Ok(listener) => listener,
        Err(error) => {
            return shutdown_after_boot_error(
                &host,
                &mut io_tasks,
                crate::cli::usage_error(format!(
                    "failed to bind Verlet host listener {}: {error}",
                    config.listen.addr
                )),
            )
            .await;
        }
    };
    let listen_addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(error) => {
            return shutdown_after_boot_error(
                &host,
                &mut io_tasks,
                crate::cli::usage_error(format!(
                    "failed to inspect Verlet host listener {}: {error}",
                    config.listen.addr
                )),
            )
            .await;
        }
    };
    if let Err(error) = host
        .serve_websocket_listener_with_options(
            listener,
            crate::adapters::host::HostListenerOptions {
                allow_non_loopback: config.listen.allow_non_loopback,
            },
        )
        .await
    {
        return shutdown_after_boot_error(&host, &mut io_tasks, error).await;
    }
    eprintln!(
        "verlet host listening on {listen_addr} with {} instances",
        config.instance.len()
    );

    let signal_result = shutdown_signal.await;
    shutdown_io_tasks(&mut io_tasks).await;
    let shutdown_result = host.shutdown().await;
    signal_result?;
    shutdown_result
}

async fn shutdown_after_boot_error(
    host: &crate::adapters::host::VerletHost,
    io_tasks: &mut Vec<tokio::task::JoinHandle<()>>,
    boot_error: crate::kernel::runtime_host::VerletError,
) -> crate::kernel::runtime_host::VerletResult<()> {
    shutdown_io_tasks(io_tasks).await;
    match host.shutdown().await {
        Ok(()) => Err(boot_error),
        Err(shutdown_error) => Err(crate::cli::usage_error(format!(
            "{boot_error}; host cleanup also failed: {shutdown_error}"
        ))),
    }
}

async fn shutdown_io_tasks(tasks: &mut Vec<tokio::task::JoinHandle<()>>) {
    for task in tasks.iter() {
        task.abort();
    }
    while let Some(task) = tasks.pop() {
        let _ = task.await;
    }
}

/// Print help for `verlet host`.
pub(super) fn print_host_help() {
    println!(
        "verlet host\n\
         \n\
         Run a multi-tenant host process serving many kernel instances\n\
         behind one authenticated listener.\n\
         \n\
         Usage:\n\
        \x20 verlet host run --config <host.toml>\n\
         \n\
         Subcommands:\n\
        \x20 run    Boot the instances from a config file and serve until\n\
        \x20        SIGTERM/SIGINT\n\
         \n\
         Use `verlet help host run` for the config reference."
    );
}

/// Print help for `verlet host run`.
pub(super) fn print_host_run_help() {
    println!(
        "verlet host run\n\
         \n\
         Usage:\n\
        \x20 verlet host run --config <host.toml>\n\
         \n\
         The config file defines the one listener ([listen]: addr,\n\
         allow_non_loopback) and each hosted instance ([[instance]]: id,\n\
         root, cwd, tenant_id, console_principal, hook_shell, clock,\n\
         route_digests, [instance.provider]). Clock defaults to true.\n\
         Credential routes use the exact sha256 digest printed by\n\
         `verlet identity mint`; host\n\
         configuration never stores raw kernel access tokens."
    );
}
