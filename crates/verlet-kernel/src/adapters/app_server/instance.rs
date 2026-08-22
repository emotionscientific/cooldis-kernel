//! Hosted-instance config hygiene (EMO-552): explicit roots with
//! process-wide overlap rejection, and an injected environment that
//! replaces process-state reads at depth.
//!
//! Two instances sharing a `state_home` silently become one database
//! namespace, and independently reopened history stores to the same path
//! bypass the per-store write gate. Hosted construction therefore requires
//! explicit absolute roots; the constructor canonicalizes them and rejects
//! any overlap with roots already reserved in this process BEFORE opening
//! SQLite. Defaults derived from cwd/XDG remain a convenience of the
//! standalone daemon boundary only.
//!
//! Sharing policy (architect decision on the issue): ALL mutable naming
//! (catalog records, aliases including the default-manifest alias,
//! bindings, default manifests, secrets, dispatchers) is per instance —
//! separate registry roots, no alias namespacing scheme. Immutable CAS
//! artifact bytes may be shared in principle, but host v0 reserves every
//! root exclusively, blob store included; a shared dedup CAS store is a
//! future optimization with its own design.

use sha2::Digest as _;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct InstanceEndpoint {
    pub pid: u32,
    pub unix_socket: std::path::PathBuf,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ws_url: Option<String>,
    pub started_at: String,
    pub instance_id: String,
}

pub fn resolve_instance_endpoint(state_root: &std::path::Path) -> Option<InstanceEndpoint> {
    let record = std::fs::read(state_root.join(super::ENDPOINT_RECORD_NAME)).ok()?;
    let endpoint = serde_json::from_slice::<InstanceEndpoint>(&record).ok()?;
    if !endpoint.unix_socket.is_absolute()
        || endpoint.started_at.trim().is_empty()
        || endpoint.instance_id.trim().is_empty()
        || !process_is_live(endpoint.pid)
    {
        return None;
    }
    Some(endpoint)
}

pub(crate) fn refuse_live_instance(
    state_root: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let Some(endpoint) = resolve_instance_endpoint(state_root) else {
        return Ok(());
    };
    if endpoint.pid == std::process::id() && !instance_endpoint_is_active(&endpoint.instance_id) {
        return Ok(());
    }
    Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
        format!(
            "instance already running for {}, pid {}, socket {}",
            state_root.display(),
            endpoint.pid,
            endpoint.unix_socket.display()
        ),
    ))
}

pub(crate) fn register_instance_endpoint(instance_id: &str) {
    active_instance_endpoints()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(instance_id.to_string());
}

pub(crate) fn unregister_instance_endpoint(instance_id: &str) {
    active_instance_endpoints()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(instance_id);
}

fn instance_endpoint_is_active(instance_id: &str) -> bool {
    active_instance_endpoints()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(instance_id)
}

fn active_instance_endpoints() -> &'static std::sync::Mutex<std::collections::HashSet<String>> {
    static ACTIVE_ENDPOINTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<String>>,
    > = std::sync::OnceLock::new();
    ACTIVE_ENDPOINTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

pub(crate) fn write_instance_endpoint(
    state_root: &std::path::Path,
    endpoint: &InstanceEndpoint,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if !endpoint.unix_socket.is_absolute() {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            format!(
                "endpoint record unix socket must be absolute: {}",
                endpoint.unix_socket.display()
            ),
        ));
    }
    std::fs::create_dir_all(state_root)
        .map_err(|error| endpoint_record_error(state_root, error))?;
    let path = state_root.join(super::ENDPOINT_RECORD_NAME);
    let temporary = state_root.join(format!(
        ".{}.{}.tmp",
        super::ENDPOINT_RECORD_NAME,
        uuid::Uuid::now_v7()
    ));
    let mut bytes = serde_json::to_vec_pretty(endpoint).map_err(|error| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to encode endpoint record {}: {error}",
            path.display()
        ))
    })?;
    bytes.push(b'\n');
    let result = (|| -> std::io::Result<()> {
        let mut options = std::fs::OpenOptions::new();
        options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt as _;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        std::io::Write::write_all(&mut file, &bytes)?;
        file.sync_all()?;
        replace_endpoint_record(&temporary, &path)?;
        Ok(())
    })();
    if let Err(error) = result {
        let _ = std::fs::remove_file(&temporary);
        return Err(endpoint_record_error(state_root, error));
    }
    Ok(())
}

pub(crate) fn remove_owned_instance_endpoint(
    state_root: &std::path::Path,
    instance_id: &str,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let path = state_root.join(super::ENDPOINT_RECORD_NAME);
    let record = match std::fs::read(&path) {
        Ok(record) => record,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(endpoint_record_error(state_root, error)),
    };
    let endpoint = serde_json::from_slice::<InstanceEndpoint>(&record).map_err(|error| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to decode endpoint record {} during shutdown: {error}",
            path.display()
        ))
    })?;
    if endpoint.instance_id != instance_id {
        return Ok(());
    }
    std::fs::remove_file(&path).map_err(|error| endpoint_record_error(state_root, error))
}

pub(crate) fn absolute_path(
    path: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to resolve absolute endpoint path {}: {error}",
                path.display()
            ))
        })
}

pub(crate) fn instance_unix_socket_path(
    state_root: &std::path::Path,
) -> crate::kernel::runtime_host::VerletResult<std::path::PathBuf> {
    let state_root = absolute_path(state_root)?;
    let candidate = state_root.join("verlet.sock");
    if unix_socket_path_fits(&candidate) {
        return Ok(candidate);
    }

    let digest = sha2::Sha256::digest(state_root.to_string_lossy().as_bytes());
    let digest = format!("{digest:x}");
    let runtime_path = crate::daemon::daemon_config::default_verlet_daemon_socket_path();
    let runtime_dir = runtime_path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("/tmp"));
    let hashed = runtime_dir.join(format!("{}.sock", &digest[..32]));
    if unix_socket_path_fits(&hashed) {
        return Ok(hashed);
    }

    let temporary = std::env::temp_dir().join(format!("verlet-{}.sock", &digest[..24]));
    if unix_socket_path_fits(&temporary) {
        return Ok(temporary);
    }
    Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
        format!(
            "could not derive a Unix socket path within the platform limit for state root {}",
            state_root.display()
        ),
    ))
}

fn endpoint_record_error(
    state_root: &std::path::Path,
    error: impl std::fmt::Display,
) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
        "failed to update endpoint record {}: {error}",
        state_root.join(super::ENDPOINT_RECORD_NAME).display()
    ))
}

#[cfg(not(windows))]
fn replace_endpoint_record(
    temporary: &std::path::Path,
    path: &std::path::Path,
) -> std::io::Result<()> {
    std::fs::rename(temporary, path)
}

#[cfg(windows)]
fn replace_endpoint_record(
    temporary: &std::path::Path,
    path: &std::path::Path,
) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    std::fs::rename(temporary, path)
}

#[cfg(unix)]
fn process_is_live(pid: u32) -> bool {
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    if pid <= 0 {
        return false;
    }
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(windows)]
fn process_is_live(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let handle = unsafe {
        windows_sys::Win32::System::Threading::OpenProcess(
            windows_sys::Win32::System::Threading::PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        )
    };
    if handle.is_null() {
        return false;
    }
    let mut exit_code = 0_u32;
    let inspected = unsafe {
        windows_sys::Win32::System::Threading::GetExitCodeProcess(handle, &mut exit_code)
    };
    unsafe {
        windows_sys::Win32::Foundation::CloseHandle(handle);
    }
    inspected != 0 && exit_code == windows_sys::Win32::Foundation::STILL_ACTIVE as u32
}

#[cfg(not(any(unix, windows)))]
fn process_is_live(pid: u32) -> bool {
    pid == std::process::id()
}

#[cfg(unix)]
pub(crate) fn unix_socket_path_fits(path: &std::path::Path) -> bool {
    use std::os::unix::ffi::OsStrExt as _;
    path.as_os_str().as_bytes().len() <= 103
}

#[cfg(not(unix))]
pub(crate) fn unix_socket_path_fits(_path: &std::path::Path) -> bool {
    true
}

/// The filesystem roots one kernel instance owns exclusively. All paths
/// must be absolute; construction of a hosted instance canonicalizes them
/// (creating the directories first so symlinked parents resolve) and
/// reserves them process-wide via [`reserve_instance_roots`].
#[derive(Clone, Debug)]
pub struct InstanceRoots {
    pub runtime_home: std::path::PathBuf,
    pub state_home: std::path::PathBuf,
    pub user_state_home: std::path::PathBuf,
    pub agent_registry_root: std::path::PathBuf,
    pub blob_registry_root: std::path::PathBuf,
    pub skill_registry_root: std::path::PathBuf,
}

impl InstanceRoots {
    /// Standard layout: every root a distinct child of one absolute
    /// instance directory (`runtime/`, `state/`, `user-state/`, `agents/`,
    /// `blobs/`, `skills/`).
    pub fn under(instance_root: impl Into<std::path::PathBuf>) -> Self {
        let instance_root = instance_root.into();
        Self {
            runtime_home: instance_root.join("runtime"),
            state_home: instance_root.join("state"),
            user_state_home: instance_root.join("user-state"),
            agent_registry_root: instance_root.join("agents"),
            blob_registry_root: instance_root.join("blobs"),
            skill_registry_root: instance_root.join("skills"),
        }
    }
}

/// A process-wide claim on one instance's canonicalized roots. Held first by
/// the hosted config and then by the instance; dropping the current owner
/// releases the claim so the roots can be reused by a successor instance.
#[derive(Debug)]
pub struct InstanceRootReservation {
    canonical_roots: Vec<std::path::PathBuf>,
}

impl InstanceRootReservation {
    pub(crate) fn canonical_roots(&self) -> &[std::path::PathBuf] {
        &self.canonical_roots
    }
}

impl Drop for InstanceRootReservation {
    fn drop(&mut self) {
        release_reserved_roots(&self.canonical_roots);
    }
}

/// Canonicalize every root (creating missing directories first) and claim
/// them in the process-wide reservation table. Fails loudly — before any
/// SQLite open — when any pair of the supplied roots overlaps, or when any
/// supplied root overlaps a root already reserved by a live instance.
/// Overlap means equality OR one canonical path being a prefix of the
/// other, so symlinked aliases of the same directory are caught.
pub fn reserve_instance_roots(
    roots: &InstanceRoots,
) -> crate::kernel::runtime_host::VerletResult<InstanceRootReservation> {
    let named_roots = [
        ("runtime_home", &roots.runtime_home),
        ("state_home", &roots.state_home),
        ("user_state_home", &roots.user_state_home),
        ("agent_registry_root", &roots.agent_registry_root),
        ("blob_registry_root", &roots.blob_registry_root),
        ("skill_registry_root", &roots.skill_registry_root),
    ];
    for (name, path) in named_roots {
        if !path.is_absolute() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "hosted instance root {name} must be absolute: {}",
                    path.display()
                ),
            ));
        }
    }

    let mut canonical_roots = Vec::with_capacity(named_roots.len());
    for (name, path) in named_roots {
        std::fs::create_dir_all(path).map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to create hosted instance root {name} {}: {error}",
                path.display()
            ))
        })?;
        let canonical = std::fs::canonicalize(path).map_err(|error| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to canonicalize hosted instance root {name} {}: {error}",
                path.display()
            ))
        })?;
        canonical_roots.push((name, path, canonical));
    }

    for first_index in 0..canonical_roots.len() {
        for second_index in (first_index + 1)..canonical_roots.len() {
            let (first_name, first_original, first) = &canonical_roots[first_index];
            let (second_name, second_original, second) = &canonical_roots[second_index];
            if roots_overlap(first, second) {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                    format!(
                        "hosted instance roots overlap: {first_name} {} (canonical {}) and {second_name} {} (canonical {})",
                        first_original.display(),
                        first.display(),
                        second_original.display(),
                        second.display()
                    ),
                ));
            }
        }
    }

    for (_, original, canonical) in &canonical_roots {
        if resolve_instance_endpoint(canonical).is_some() {
            refuse_live_instance(original)?;
        }
    }

    let mut reserved = reserved_instance_roots()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (name, original, canonical) in &canonical_roots {
        if let Some(existing) = reserved
            .iter()
            .find(|existing| roots_overlap(canonical, existing))
        {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "hosted instance root {name} {} (canonical {}) overlaps reserved root {}",
                    original.display(),
                    canonical.display(),
                    existing.display()
                ),
            ));
        }
    }
    let canonical_roots = canonical_roots
        .into_iter()
        .map(|(_, _, canonical)| canonical)
        .collect::<Vec<_>>();
    reserved.extend(canonical_roots.iter().cloned());
    drop(reserved);

    Ok(InstanceRootReservation { canonical_roots })
}

fn release_reserved_roots(canonical_roots: &[std::path::PathBuf]) {
    let mut reserved = reserved_instance_roots()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for root in canonical_roots {
        reserved.remove(root);
    }
}

fn reserved_instance_roots()
-> &'static std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>> {
    static RESERVED_ROOTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<std::path::PathBuf>>,
    > = std::sync::OnceLock::new();
    RESERVED_ROOTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

fn roots_overlap(first: &std::path::Path, second: &std::path::Path) -> bool {
    first.starts_with(second) || second.starts_with(first)
}

/// Where an instance resolves LLM provider auth from. Hosted instances get
/// an injected context and never see this process's environment; the
/// standalone daemon keeps today's process-environment behavior.
#[derive(Clone)]
pub enum ProviderAuthSource {
    /// Snapshot the daemon process environment on demand (standalone
    /// boundary only; today's `LlmProviderAuthContext::from_process_env`).
    ProcessEnvironment,
    /// Use exactly this injected context; process env vars are invisible.
    Injected(verlet_metadata::provider_store::LlmProviderAuthContext),
}

impl std::fmt::Debug for ProviderAuthSource {
    /// The injected context carries credential material; never print it.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProcessEnvironment => f.write_str("ProviderAuthSource::ProcessEnvironment"),
            Self::Injected(_) => f.write_str("ProviderAuthSource::Injected(<redacted>)"),
        }
    }
}

impl ProviderAuthSource {
    /// The auth context this instance dispatches provider calls with.
    pub fn resolve(&self) -> verlet_metadata::provider_store::LlmProviderAuthContext {
        match self {
            Self::ProcessEnvironment => {
                verlet_metadata::provider_store::LlmProviderAuthContext::from_process_env()
            }
            Self::Injected(context) => context.clone(),
        }
    }
}

/// Per-instance replacements for state that library code currently reads
/// from the process at depth (env vars, cwd, global switches). The three
/// deep reads this retires: provider auth env snapshots, the hook shell
/// from `COMSPEC`/`SHELL`, and the process-global deterministic-ID switch
/// in verlet-process.
#[derive(Clone)]
pub struct InstanceEnvironment {
    pub provider_auth: ProviderAuthSource,
    /// Shell for agent hooks. `None` = today's `COMSPEC`/`SHELL` lookup
    /// (standalone boundary only); hosted instances inject a shell path.
    pub hook_shell: Option<String>,
    /// Source of process ids for this instance's process manager. Random
    /// in production; a per-instance deterministic source is what lets the
    /// EMO-553 two-instance DST run one harness per instance.
    pub process_ids: std::sync::Arc<dyn verlet_process::process::ProcessIdSource>,
}

impl InstanceEnvironment {
    /// Standalone-daemon behavior: process env for provider auth,
    /// `COMSPEC`/`SHELL` for hooks, random process ids.
    pub fn standalone() -> Self {
        Self {
            provider_auth: ProviderAuthSource::ProcessEnvironment,
            hook_shell: None,
            process_ids: std::sync::Arc::new(verlet_process::process::RandomProcessIds),
        }
    }

    pub(crate) fn validate_hosted(&self) -> crate::kernel::runtime_host::VerletResult<()> {
        if matches!(&self.provider_auth, ProviderAuthSource::ProcessEnvironment) {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "hosted instance provider auth must be injected".to_string(),
            ));
        }
        let Some(hook_shell) = self
            .hook_shell
            .as_deref()
            .filter(|shell| !shell.trim().is_empty())
        else {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "hosted instance hook shell must be injected".to_string(),
            ));
        };
        if !std::path::Path::new(hook_shell).is_absolute() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!("hosted instance hook shell must be absolute: {hook_shell}"),
            ));
        }
        Ok(())
    }
}

impl std::fmt::Debug for InstanceEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceEnvironment")
            .field("provider_auth", &self.provider_auth)
            .field("hook_shell", &self.hook_shell)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    fn test_root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "verlet-endpoint-record-{label}-{}",
            uuid::Uuid::now_v7()
        ))
    }

    fn endpoint(root: &std::path::Path, pid: u32) -> super::InstanceEndpoint {
        super::InstanceEndpoint {
            pid,
            unix_socket: root.join("verlet.sock"),
            ws_url: Some("ws://127.0.0.1:49200/rpc".to_string()),
            started_at: "2026-08-22T12:00:00Z".to_string(),
            instance_id: uuid::Uuid::now_v7().to_string(),
        }
    }

    #[test]
    fn endpoint_record_round_trips_for_a_live_process() {
        let root = test_root("live");
        let expected = endpoint(&root, std::process::id());

        super::write_instance_endpoint(&root, &expected).unwrap();

        assert_eq!(super::resolve_instance_endpoint(&root), Some(expected));
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn stale_endpoint_record_is_not_resolved() {
        let root = test_root("stale");
        let stale = endpoint(&root, u32::MAX);

        super::write_instance_endpoint(&root, &stale).unwrap();

        assert_eq!(super::resolve_instance_endpoint(&root), None);
        assert!(root.join(super::super::ENDPOINT_RECORD_NAME).is_file());
        let _ = std::fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn deep_state_root_uses_a_deterministic_short_socket_path() {
        let root = std::path::Path::new("/tmp").join("deep".repeat(40));

        let first = super::instance_unix_socket_path(&root).unwrap();
        let second = super::instance_unix_socket_path(&root).unwrap();

        assert_eq!(first, second);
        assert!(first.is_absolute());
        assert!(super::unix_socket_path_fits(&first));
        assert_ne!(first, root.join("verlet.sock"));
    }

    #[test]
    fn live_endpoint_record_refuses_root_reservation_before_store_open() {
        let root = test_root("reservation");
        let roots = super::InstanceRoots::under(&root);
        let live = endpoint(&roots.state_home, std::process::id());
        super::register_instance_endpoint(&live.instance_id);
        super::write_instance_endpoint(&roots.state_home, &live).unwrap();

        let error = super::reserve_instance_roots(&roots).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "runtime factory failed: instance already running for {}, pid {}, socket {}",
                roots.state_home.display(),
                live.pid,
                live.unix_socket.display()
            )
        );
        assert!(
            !roots
                .state_home
                .join(super::super::METADATA_DB_NAME)
                .exists()
        );
        super::unregister_instance_endpoint(&live.instance_id);
        let _ = std::fs::remove_dir_all(root);
    }
}
