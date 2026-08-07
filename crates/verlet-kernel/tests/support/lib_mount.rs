//! The lib-side mount of the shared test-support tree.
//!
//! `src/lib.rs` mounts this file as `crate::support` under `#[cfg(test)]`, the
//! same module path every integration-test binary mounts `mod.rs` at. One
//! absolute path in both compilations is what lets the support files spell
//! every path out in full instead of reaching through relative parent paths.
//!
//! The `scenario_*` seams are the deliberate difference between the two
//! mounts: here they are the real implementations, which can reach
//! crate-private kernel APIs; `mod.rs` supplies panicking stubs plus a
//! `scenario_unit_harness()` that returns `false`, so integration binaries skip
//! the scenario bodies that need those seams.

#[path = "event_trace.rs"]
pub(crate) mod event_trace;
#[path = "fault.rs"]
pub(crate) mod fault;
#[path = "fault_plan.rs"]
pub(crate) mod fault_plan;
#[path = "invariant_claims.rs"]
pub(crate) mod invariant_claims;
#[path = "invariant_forks.rs"]
pub(crate) mod invariant_forks;
#[path = "invariants.rs"]
pub(crate) mod invariants;
#[path = "scenario.rs"]
pub(crate) mod scenario;
#[path = "scripted_provider.rs"]
pub(crate) mod scripted_provider;
#[path = "simulated_io.rs"]
pub(crate) mod simulated_io;
#[path = "store_parity.rs"]
pub(crate) mod store_parity;
#[path = "transcript.rs"]
pub(crate) mod transcript;

pub(crate) async fn scenario_app_server(
    config: verlet::adapters::app_server::VerletAppServerConfig,
    runtime_factory: std::sync::Arc<
        dyn verlet::kernel::runtime_host::runtime_api::AgentRuntimeFactory,
    >,
    decorate: impl FnOnce(
        std::sync::Arc<dyn verlet_history::RuntimeStore>,
    ) -> std::sync::Arc<dyn verlet_history::RuntimeStore>
    + Send
    + 'static,
) -> verlet::kernel::runtime_host::VerletResult<verlet::adapters::app_server::VerletAppServer> {
    verlet::adapters::app_server::VerletAppServer::with_runtime_factory_and_session_store_decorator(
        config,
        runtime_factory,
        decorate,
    )
    .await
}

pub(crate) fn scenario_unit_harness() -> bool {
    true
}

pub(crate) async fn scenario_fork_with_id(
    server: &verlet::adapters::app_server::VerletAppServer,
    parent: &verlet_runtime_contracts::ThreadCoordinates,
    child_thread_id: verlet_runtime_contracts::ThreadId,
) -> verlet::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadCoordinates> {
    let checkpoint = server
        .supervisor()
        .create_checkpoint_at(
            parent,
            None,
            Some("scenario-fork".to_string()),
            std::collections::BTreeMap::new(),
        )
        .await?;
    let child = server
        .supervisor()
        .fork_thread_from_checkpoint_with_id_at(checkpoint, child_thread_id)
        .await?;
    Ok(child.context().coordinates.clone())
}

pub(crate) async fn scenario_project_spawn_snapshot(
    host: verlet::kernel::runtime_host::RuntimeHost,
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> verlet::kernel::runtime_host::VerletResult<
    verlet::kernel::thread_spawn_projector::ThreadSpawnProjectionReceipt,
> {
    host.load_thread_with_topology_and_metadata(
        coordinates.clone(),
        verlet_runtime_contracts::ThreadTopology::root(),
        std::collections::BTreeMap::new(),
    )
    .await?;
    verlet::kernel::thread_spawn_projector::ThreadSpawnProjector::new(host)
        .with_snapshot_barrier(barrier)
        .project_control_stream(&coordinates)
        .await
}

pub(crate) fn scenario_ingress_binding_barrier(
    bridge: &verlet::daemon::daemon_io::VerletDaemonIoBridge,
) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
    bridge.ingress_binding_barrier()
}

pub(crate) fn scenario_pause_after_ingress_claim(
    bridge: &verlet::daemon::daemon_io::VerletDaemonIoBridge,
) -> (
    std::sync::Arc<std::sync::atomic::AtomicBool>,
    std::sync::Arc<tokio::sync::Notify>,
) {
    bridge.pause_after_ingress_claim()
}

pub(crate) fn scenario_thread_load_root_barrier(
    bridge: &verlet::daemon::daemon_io::VerletDaemonIoBridge,
) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
    bridge.thread_load_root_barrier()
}
