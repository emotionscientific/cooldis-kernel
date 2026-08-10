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
/// route_digests = ["<digest printed by identity mint>"]
///
/// [instance.provider]
/// provider = "bifrost_openai"
/// base_url = "https://..."
/// api_key_env = "VERLET_HOST_ORCH_PROVIDER_KEY"
/// model = "..."
/// ```
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct VerletHostRunConfig {
    pub(crate) listen: HostListenConfig,
    #[serde(default)]
    pub(crate) instance: Vec<HostInstanceConfig>,
}

/// `[listen]`: the one TCP listener every instance is served through.
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
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
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
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
    pub(crate) hook_shell: String,
    /// Credential digests routed to this instance (decision 1). May be
    /// empty (an instance reachable only in-process), but a digest listed
    /// under two instances is a config error.
    #[serde(default)]
    pub(crate) route_digests: Vec<String>,
    pub(crate) provider: HostInstanceProviderConfig,
}

/// `[instance.provider]`: the injected provider auth for one instance
/// (EMO-552: hosted instances never snapshot the process environment).
/// Field names follow the daemon config `[provider]` section.
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
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
    // EMO-564: parse args (only `--config <path>`, required), then
    // load_host_run_config → build_hosted_instances → serve_until_signal.
    let _ = args;
    Err(crate::cli::usage_error(
        "EMO-564: `verlet host run` is not implemented yet",
    ))
}

/// Read and validate one `host.toml` (decision 4 — every error names the
/// offending instance id or field; collect-and-report like the daemon
/// config validator rather than first-error-wins where cheap).
///
/// Validation beyond serde: at least one instance; instance ids parse as
/// [`crate::adapters::host::InstanceId`] and are unique; `root`, `cwd`,
/// `hook_shell` absolute; `tenant_id`/`console_principal` non-blank
/// (decision 3); route digests non-blank and globally unique across
/// instances; provider is a known name; `bifrost_openai` requires
/// `base_url`, `api_key_env`, and `model`, and the named env var must
/// resolve non-empty at load time; `local_offline` requires none of them.
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
pub(crate) fn load_host_run_config(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<VerletHostRunConfig> {
    // EMO-564: std::fs::read_to_string + toml::from_str + the validation
    // listed above (mirror daemon_config's error style).
    let _ = path;
    Err(crate::cli::usage_error(
        "EMO-564: host config loading is not implemented yet",
    ))
}

/// Build the hosted app-server config for one `[[instance]]` entry:
/// `InstanceRoots::under(root)`, an injected
/// [`crate::adapters::app_server::instance::InstanceEnvironment`]
/// (provider auth resolved from the config per
/// [`HostInstanceProviderConfig`], `hook_shell`, random process ids), a
/// managed-mode identity config from `tenant_id`/`console_principal`,
/// and the chat provider wiring (`with_bifrost_openai` for
/// `bifrost_openai`; the local-offline default otherwise).
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
pub(crate) fn hosted_instance_config(
    instance: &HostInstanceConfig,
) -> crate::kernel::runtime_host::VerletResult<(
    crate::adapters::host::InstanceId,
    crate::adapters::app_server::VerletAppServerConfig,
)> {
    // EMO-564: as documented above. Root reservation and hosted-env
    // validation already live in VerletAppServerConfig::hosted; do not
    // duplicate them here.
    let _ = instance;
    Err(crate::cli::usage_error(
        "EMO-564: hosted instance config construction is not implemented yet",
    ))
}

/// Boot sequence + signal wait: construct [`crate::adapters::host::VerletHost`],
/// `start_instance` each (any failure shuts down what already started and
/// returns the error), register every route digest, bind the TCP
/// listener, `serve_websocket_listener_with_options` with the config's
/// bind policy, print the liveness line, then wait for SIGTERM/SIGINT
/// (`tokio::signal`) and run host shutdown.
// Skeleton-only (EMO-564): remove with the implementation.
#[allow(dead_code)]
pub(crate) async fn serve_until_signal(
    config: VerletHostRunConfig,
) -> crate::kernel::runtime_host::VerletResult<()> {
    // EMO-564: as documented above. Factor the signal wait so tests can
    // drive shutdown without delivering process signals (mirror the
    // daemon service tests' approach).
    let _ = config;
    Err(crate::cli::usage_error(
        "EMO-564: host serve loop is not implemented yet",
    ))
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
         root, cwd, tenant_id, console_principal, hook_shell,\n\
         route_digests, [instance.provider]). Credential routes are\n\
         configured as digests printed by `verlet identity mint`; the\n\
         host process never holds kernel access tokens."
    );
}
