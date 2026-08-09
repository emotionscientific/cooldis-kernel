//! Multi-tenant host facade acceptance skeleton (EMO-553).
//!
//! One `VerletHost`, two hosted instances (disjoint `InstanceRoots::under`
//! trees in one tempdir, injected `InstanceEnvironment` with a
//! per-instance `DeterministicProcessIds` source), one listener. The
//! ignored tests below name the acceptance criteria; the implementation
//! replaces each `todo!` body and drops its `#[ignore]`. The two-instance
//! crash-cut scenario drives `run_paired_crash_cut`
//! (`support/fault_plan.rs`); if its harness needs crate-private seams,
//! the scenario body may move to the lib-side support mount like the
//! existing scenario suite — keep the test name.

#[path = "support/test_mount.rs"]
mod support;

/// Criterion: host serves two instances through one listener; each
/// credential reaches only its own instance.
#[tokio::test]
#[ignore = "EMO-553 skeleton: enabled by the implementation dispatch"]
async fn each_credential_reaches_only_its_routed_instance() {
    todo!("EMO-553: two instances, one listener, per-instance credentials; cross checks refused")
}

/// Criterion: a tenant/instance identifier inside an RPC body that
/// disagrees with the connection's routed instance is rejected — the
/// routing context is the only authority (capsule-binding resolution's
/// client-supplied tenant id is the known offender).
#[tokio::test]
#[ignore = "EMO-553 skeleton: enabled by the implementation dispatch"]
async fn cross_instance_identifier_in_rpc_body_is_rejected() {
    todo!("EMO-553: routed connection to instance A carrying instance B ids in params")
}

/// Criterion: two-instance DST — one instance cut and recovered from its
/// journal while the peer keeps making invariant-checked progress
/// (`PairedCutPhase::{BeforeCut, VictimDown, AfterRecovery}`).
#[tokio::test]
#[ignore = "EMO-553 skeleton: enabled by the implementation dispatch"]
async fn one_instance_cut_and_recovered_while_peer_progresses() {
    todo!("EMO-553: run_paired_crash_cut over a hosted pair")
}

/// Criterion: facade shutdown drains connections and shuts down every
/// instance cleanly; instance roots are reusable afterwards.
#[tokio::test]
#[ignore = "EMO-553 skeleton: enabled by the implementation dispatch"]
async fn facade_shutdown_drains_connections_and_instances() {
    todo!("EMO-553: live connections + two instances, VerletHost::shutdown, roots reusable")
}
