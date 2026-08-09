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
//! Sharing policy (architect decision on the issue): immutable CAS
//! artifact bytes may be shared; ALL mutable naming (catalog records,
//! aliases including the default-manifest alias, bindings, default
//! manifests, secrets, dispatchers) is per instance — separate registry
//! roots, no alias namespacing scheme.

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

/// A process-wide claim on one instance's canonicalized roots. Held by the
/// instance for its lifetime; dropping it (instance shutdown or drop)
/// releases the claim so the roots can be reused by a successor instance.
pub struct InstanceRootReservation {
    canonical_roots: Vec<std::path::PathBuf>,
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
    let _ = roots;
    unimplemented!("EMO-552: canonicalize + overlap-reject + process-wide root reservation")
}

fn release_reserved_roots(canonical_roots: &[std::path::PathBuf]) {
    let _ = canonical_roots;
    unimplemented!("EMO-552: release reserved roots on reservation drop")
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
        unimplemented!("EMO-552: resolve provider auth from the configured source")
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
}

impl std::fmt::Debug for InstanceEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstanceEnvironment")
            .field("provider_auth", &self.provider_auth)
            .field("hook_shell", &self.hook_shell)
            .finish_non_exhaustive()
    }
}
