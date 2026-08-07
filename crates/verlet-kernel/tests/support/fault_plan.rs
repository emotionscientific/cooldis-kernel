#![allow(dead_code)]

//! Seed-derived fault plans (ADR 0004, Decision 1).
//!
//! A fault plan is the deterministic expansion of a seed into a schedule of
//! faults over a versioned vocabulary of named injection sites. Same seed,
//! same schedule; a vocabulary change is a new version, never a
//! reinterpretation of old seeds (lexicon: fault plan).
//!
//! This file is the fixed surface: the pinned PRNG, the vocabulary, and the
//! directive format plans expand into. The expansion itself
//! (`FaultPlan::derive`) is the fault-plan engine's implementation work
//! against this surface; directives apply through the existing wrappers in
//! `fault.rs`.
//!
//! Derivation shape for vocabulary v1 is part of the fixture-pinned contract.
//! A version-salted root is split independently by component label. Sparse
//! emits 0..=1 wrapper directives per lane, moderate 1..=2, and hostile
//! 2..=4. The process lane emits 0..=1, 1..=2, or exactly 2 cuts respectively.
//! Occurrences are drawn uniformly from 1..=3, 1..=6, or 1..=10. Wrapper
//! operations are uniform within their component; one quarter of wrapper
//! directives are deterministic delays (1, 10, or 50 ms), and the rest fail.
//! `After` timing is eligible only for the store append family and queue
//! `complete_ingress`; all other v1 directives fire before the operation.

/// Version of the fault vocabulary below. Bumped when sites, cuts, or the
/// derivation algorithm change; old seeds keep their meaning under the
/// version that produced them and are never reinterpreted.
pub const FAULT_VOCABULARY_VERSION: u32 = 1;

/// The pinned PRNG for all seed derivation: SplitMix64, implemented here so
/// no dependency default can drift underneath a recorded seed. The algorithm
/// is normative for fault plans and scenarios; the test at the bottom of
/// this file pins its output permanently.
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Value in `0..bound` via the widening multiply. Deterministic; the
    /// truncation bias is irrelevant at test scales.
    pub fn next_below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "next_below requires a positive bound");
        ((u128::from(self.next_u64()) * u128::from(bound)) >> 64) as u64
    }

    /// Derive an independent child generator for a labeled lane so adding
    /// directives for one component never shifts another component's
    /// schedule under the same seed.
    pub fn split(&mut self, label: &str) -> SplitMix64 {
        let mut state = self.next_u64();
        for byte in label.as_bytes() {
            state = (state ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01B3);
        }
        SplitMix64::new(state)
    }
}

/// How much probability mass a derivation spends across the schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Intensity {
    Sparse,
    Moderate,
    Hostile,
}

/// Which seam a directive drives: one of the three `fault.rs` wrappers, or
/// the process crash-cut harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum FaultComponent {
    Store,
    Queue,
    Provider,
    Process,
}

/// When the fault fires relative to its operation. `Before` is what the
/// wrappers implement today; `After` (effect durable, caller told it
/// failed) is a wrapper extension owned by the fault-plan engine ticket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum FaultTiming {
    Before,
    After,
}

/// What fires. Error construction is component-specific: the engine maps
/// `Fail` to the wrapped trait's error type when applying a directive.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum PlannedAction {
    Fail,
    Delay(std::time::Duration),
}

/// One planned fault: the nth occurrence (one-based, matching the
/// `fault.rs` rule format) of a named operation on a component.
///
/// Lane semantics differ in vocabulary v1: wrapper components (store,
/// queue, provider) match `nth` against live operation occurrences, while
/// the scenario runner consumes `Process` directives in derivation order,
/// one per abstract restart, so `nth` on a process directive is derivation
/// provenance rather than a matching key. Unifying the lanes is a
/// vocabulary-v2 decision (EMO-410); changing it under v1 would silently
/// re-key every recorded seed.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub struct FaultDirective {
    pub component: FaultComponent,
    /// Trait-method name for wrapper components (store `append_events`,
    /// queue `complete_ingress`, provider `complete`, ...); a named cut
    /// from `CUTS_V1` for `Process`.
    pub operation: &'static str,
    pub nth: usize,
    pub timing: FaultTiming,
    pub action: PlannedAction,
}

/// Vocabulary v1 — store operations a plan may fault (the
/// `FaultingRuntimeStore` operation names).
pub const STORE_OPERATIONS_V1: &[&str] = &[
    "append",
    "append_events",
    "append_events_fenced",
    "read_events",
    "read_events_after_cursor",
    "active_leaf",
    "select_branch",
    "build_context",
];

/// Vocabulary v1 — queue operations (the `FaultingIngressQueue` names).
pub const QUEUE_OPERATIONS_V1: &[&str] = &[
    "submit",
    "lease_ingress",
    "complete_ingress",
    "hold_ingress_until",
    "retry_ingress",
];

/// Vocabulary v1 — provider operations (the `FaultingProviderClient` names).
pub const PROVIDER_OPERATIONS_V1: &[&str] = &["complete", "stream"];

/// Vocabulary v1 — the named cuts a plan may kill the simulated host at.
/// Each name maps to an existing barrier or crash-cut seam; wiring the map
/// is the fault-plan engine's work.
pub const CUTS_V1: &[&str] = &[
    "queue-claim-submit",
    "queue-input-compile",
    "queue-apply",
    "observe-apply-complete",
    "reject-apply-complete",
    "ingress-binding",
    "thread-load-root",
    "spawn-snapshot",
];

/// A derived fault plan: a pure function of (seed, vocabulary version,
/// intensity). See ADR 0004 for the derivation contract.
#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct FaultPlan {
    pub seed: u64,
    pub vocabulary_version: u32,
    pub intensity: Intensity,
    pub directives: Vec<FaultDirective>,
}

impl FaultPlan {
    /// Expand `seed` into a schedule. Must draw through per-component
    /// `SplitMix64::split` lanes so one component's directives never shift
    /// another's under the same seed.
    pub fn derive(seed: u64, intensity: Intensity) -> Self {
        let version_salt = u64::from(FAULT_VOCABULARY_VERSION).wrapping_mul(0xD6E8_FEB8_6659_FD93);
        let lane = |label: &str| {
            let mut root = SplitMix64::new(seed ^ version_salt);
            root.split(label)
        };
        let mut directives = Vec::new();
        directives.extend(derive_wrapper_lane(
            lane("store"),
            FaultComponent::Store,
            STORE_OPERATIONS_V1,
            intensity,
        ));
        directives.extend(derive_wrapper_lane(
            lane("queue"),
            FaultComponent::Queue,
            QUEUE_OPERATIONS_V1,
            intensity,
        ));
        directives.extend(derive_wrapper_lane(
            lane("provider"),
            FaultComponent::Provider,
            PROVIDER_OPERATIONS_V1,
            intensity,
        ));
        directives.extend(derive_process_lane(lane("process"), intensity));
        Self {
            seed,
            vocabulary_version: FAULT_VOCABULARY_VERSION,
            intensity,
            directives,
        }
    }
}

fn derive_wrapper_lane(
    mut rng: SplitMix64,
    component: FaultComponent,
    operations: &'static [&'static str],
    intensity: Intensity,
) -> Vec<FaultDirective> {
    let count = match intensity {
        Intensity::Sparse => rng.next_below(2) as usize,
        Intensity::Moderate => 1 + rng.next_below(2) as usize,
        Intensity::Hostile => 2 + rng.next_below(3) as usize,
    };
    let occurrence_bound = match intensity {
        Intensity::Sparse => 3,
        Intensity::Moderate => 6,
        Intensity::Hostile => 10,
    };
    let mut occupied = std::collections::BTreeSet::new();
    (0..count)
        .map(|_| {
            let mut operation_index = rng.next_below(operations.len() as u64) as usize;
            let mut nth = 1 + rng.next_below(occurrence_bound) as usize;
            while !occupied.insert((operation_index, nth)) {
                nth += 1;
                if nth > occurrence_bound as usize {
                    nth = 1;
                    operation_index = (operation_index + 1) % operations.len();
                }
            }
            let operation = operations[operation_index];
            let timing = match component {
                FaultComponent::Store
                    if matches!(
                        operation,
                        "append" | "append_events" | "append_events_fenced"
                    ) =>
                {
                    if rng.next_below(4) == 0 {
                        FaultTiming::After
                    } else {
                        FaultTiming::Before
                    }
                }
                FaultComponent::Queue if operation == "complete_ingress" => {
                    if rng.next_below(2) == 0 {
                        FaultTiming::After
                    } else {
                        FaultTiming::Before
                    }
                }
                _ => FaultTiming::Before,
            };
            let action = if rng.next_below(4) == 0 {
                let delay = [1, 10, 50][rng.next_below(3) as usize];
                PlannedAction::Delay(std::time::Duration::from_millis(delay))
            } else {
                PlannedAction::Fail
            };
            FaultDirective {
                component,
                operation,
                nth,
                timing,
                action,
            }
        })
        .collect()
}

fn derive_process_lane(mut rng: SplitMix64, intensity: Intensity) -> Vec<FaultDirective> {
    let count = match intensity {
        Intensity::Sparse => rng.next_below(2) as usize,
        Intensity::Moderate => 1 + rng.next_below(2) as usize,
        Intensity::Hostile => 2,
    };
    let occurrence_bound = match intensity {
        Intensity::Sparse => 3,
        Intensity::Moderate => 6,
        Intensity::Hostile => 10,
    };
    let mut occupied = std::collections::BTreeSet::new();
    (0..count)
        .map(|_| {
            let mut operation_index = rng.next_below(CUTS_V1.len() as u64) as usize;
            let mut nth = 1 + rng.next_below(occurrence_bound) as usize;
            while !occupied.insert((operation_index, nth)) {
                nth += 1;
                if nth > occurrence_bound as usize {
                    nth = 1;
                    operation_index = (operation_index + 1) % CUTS_V1.len();
                }
            }
            FaultDirective {
                component: FaultComponent::Process,
                operation: CUTS_V1[operation_index],
                nth,
                timing: FaultTiming::Before,
                action: PlannedAction::Fail,
            }
        })
        .collect()
}

/// Existing v1 seam selected by a named process cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum CrashCutSeam {
    PauseAfterIngressClaim,
    PersistedInputRuntimeNotify,
    QueueCompleteBarrier,
    IngressBindingBarrier,
    ThreadLoadRootBarrier,
    SpawnSnapshotBarrier,
    ThreadTerminalJoinCommit,
}

/// Registry entry connecting the stable cut vocabulary to its existing seam.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct CrashCutRegistration {
    pub name: &'static str,
    pub seam: CrashCutSeam,
    pub seam_path: &'static str,
}

pub const CRASH_CUT_REGISTRY: &[CrashCutRegistration] = &[
    CrashCutRegistration {
        name: "queue-claim-submit",
        seam: CrashCutSeam::PauseAfterIngressClaim,
        seam_path: "daemon_io::VerletDaemonIoBridge::pause_after_ingress_claim",
    },
    CrashCutRegistration {
        name: "queue-input-compile",
        seam: CrashCutSeam::PersistedInputRuntimeNotify,
        seam_path: "daemon_io::tests::PersistedInputCutState::input_persisted",
    },
    CrashCutRegistration {
        name: "queue-apply",
        seam: CrashCutSeam::QueueCompleteBarrier,
        seam_path: "daemon_io::tests::ScriptedIngressQueue::block_next_complete",
    },
    CrashCutRegistration {
        name: "observe-apply-complete",
        seam: CrashCutSeam::QueueCompleteBarrier,
        seam_path: "daemon_io::tests::ScriptedIngressQueue::block_next_complete",
    },
    CrashCutRegistration {
        name: "reject-apply-complete",
        seam: CrashCutSeam::QueueCompleteBarrier,
        seam_path: "daemon_io::tests::ScriptedIngressQueue::block_next_complete",
    },
    CrashCutRegistration {
        name: "ingress-binding",
        seam: CrashCutSeam::IngressBindingBarrier,
        seam_path: "daemon_io::VerletDaemonIoBridge::ingress_binding_barrier",
    },
    CrashCutRegistration {
        name: "thread-load-root",
        seam: CrashCutSeam::ThreadLoadRootBarrier,
        seam_path: "daemon_io::VerletDaemonIoBridge::thread_load_root_barrier",
    },
    CrashCutRegistration {
        name: "spawn-snapshot",
        seam: CrashCutSeam::SpawnSnapshotBarrier,
        seam_path: "kernel::thread_spawn_projector::ThreadSpawnProjector::with_snapshot_barrier",
    },
    // Dedicated EMO-426 recovery cut. It is registered for the common
    // kill/rebuild/recover harness but deliberately excluded from CUTS_V1:
    // adding it to derived plans would reinterpret every version-1 seed.
    CrashCutRegistration {
        name: "thread-terminal-join-commit",
        seam: CrashCutSeam::ThreadTerminalJoinCommit,
        seam_path: "kernel::runtime_host::RuntimeServices::append_thread_joined_event_if_spawned",
    },
];

pub fn crash_cut(name: &str) -> Option<&'static CrashCutRegistration> {
    CRASH_CUT_REGISTRY
        .iter()
        .find(|registration| registration.name == name)
}

/// Minimal host contract required by the in-process crash-cut harness. Moving
/// `StoreState` through teardown and rebuild makes reuse of the same durable
/// state explicit in the type-level flow.
#[async_trait::async_trait]
pub trait CrashCutHost: Sized {
    type StoreState;

    async fn run_to_cut(&mut self, seam: CrashCutSeam);
    fn tear_down(self) -> Self::StoreState;
    async fn rebuild(store: Self::StoreState) -> Self;
    async fn recover(&mut self);
}

/// Run to `name`, kill the in-process host, rebuild over its surviving state,
/// and run recovery. Unknown names fail closed because a plan vocabulary entry
/// without a registry seam is a harness bug.
pub async fn run_crash_cut<H: CrashCutHost>(name: &str, mut host: H) -> H {
    let registration = crash_cut(name).unwrap_or_else(|| panic!("unregistered crash cut {name:?}"));
    host.run_to_cut(registration.seam).await;
    let store = host.tear_down();
    let mut rebuilt = H::rebuild(store).await;
    rebuilt.recover().await;
    rebuilt
}

#[cfg(test)]
mod tests {

    const FIXTURE_SEED: u64 = 0xE399_0004_D15E_A5E5;

    fn assert_json_fixture(relative: &str, actual: serde_json::Value) {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join(relative);
        if verlet_runtime_contracts::env_compat::var_os("VERLET_UPDATE_FIXTURES").is_some() {
            let mut text = serde_json::to_string_pretty(&actual).unwrap();
            text.push('\n');
            std::fs::write(&path, text)
                .unwrap_or_else(|err| panic!("write fixture {}: {err}", path.display()));
            return;
        }
        let expected_text = std::fs::read_to_string(&path).unwrap_or_else(|err| {
            panic!(
                "read fixture {}: {err}\n\nactual:\n{}\n",
                path.display(),
                serde_json::to_string_pretty(&actual).unwrap()
            )
        });
        let expected: serde_json::Value = serde_json::from_str(&expected_text)
            .unwrap_or_else(|err| panic!("parse fixture {}: {err}", path.display()));
        assert_eq!(expected, actual, "fixture {} differed", path.display());
    }

    #[test]
    fn splitmix64_output_is_pinned_permanently() {
        let mut rng = crate::support::fault_plan::SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn next_below_is_pinned_permanently() {
        let mut rng = crate::support::fault_plan::SplitMix64::new(42);
        let draws: Vec<u64> = (0..4).map(|_| rng.next_below(6)).collect();
        assert_eq!(draws, vec![4, 0, 1, 2]);
    }

    #[test]
    fn split_lanes_are_pinned_permanently() {
        let mut rng = crate::support::fault_plan::SplitMix64::new(7);
        let mut store_lane = rng.split("store");
        assert_eq!(store_lane.next_u64(), 0x1D60_7A07_C3D0_3D6E);
    }

    #[test]
    fn same_seed_and_intensity_derive_identical_directives() {
        for intensity in [
            crate::support::fault_plan::Intensity::Sparse,
            crate::support::fault_plan::Intensity::Moderate,
            crate::support::fault_plan::Intensity::Hostile,
        ] {
            assert_eq!(
                crate::support::fault_plan::FaultPlan::derive(FIXTURE_SEED, intensity),
                crate::support::fault_plan::FaultPlan::derive(FIXTURE_SEED, intensity)
            );
        }
    }

    #[test]
    fn sparse_derivation_is_fixture_pinned() {
        assert_json_fixture(
            "fault_plans/sparse.json",
            serde_json::to_value(crate::support::fault_plan::FaultPlan::derive(
                FIXTURE_SEED,
                crate::support::fault_plan::Intensity::Sparse,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn moderate_derivation_is_fixture_pinned() {
        assert_json_fixture(
            "fault_plans/moderate.json",
            serde_json::to_value(crate::support::fault_plan::FaultPlan::derive(
                FIXTURE_SEED,
                crate::support::fault_plan::Intensity::Moderate,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn hostile_derivation_is_fixture_pinned() {
        assert_json_fixture(
            "fault_plans/hostile.json",
            serde_json::to_value(crate::support::fault_plan::FaultPlan::derive(
                FIXTURE_SEED,
                crate::support::fault_plan::Intensity::Hostile,
            ))
            .unwrap(),
        );
    }

    #[test]
    fn process_lane_emits_at_most_two_known_cuts() {
        for intensity in [
            crate::support::fault_plan::Intensity::Sparse,
            crate::support::fault_plan::Intensity::Moderate,
            crate::support::fault_plan::Intensity::Hostile,
        ] {
            let plan = crate::support::fault_plan::FaultPlan::derive(FIXTURE_SEED, intensity);
            let process = plan
                .directives
                .iter()
                .filter(|directive| {
                    directive.component == crate::support::fault_plan::FaultComponent::Process
                })
                .collect::<Vec<_>>();
            assert!(process.len() <= 2);
            assert!(process.iter().all(|directive| {
                crate::support::fault_plan::CUTS_V1.contains(&directive.operation)
            }));
        }
    }

    #[test]
    fn every_v1_cut_is_registered_once() {
        let names = crate::support::fault_plan::CRASH_CUT_REGISTRY
            .iter()
            .map(|registration| registration.name)
            .collect::<Vec<_>>();
        assert_eq!(
            names.len(),
            names
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            "crash-cut registry names must be unique"
        );
        assert!(
            crate::support::fault_plan::CUTS_V1
                .iter()
                .all(|name| names.contains(name))
        );
        for name in crate::support::fault_plan::CUTS_V1 {
            assert_eq!(
                crate::support::fault_plan::crash_cut(name).unwrap().name,
                *name
            );
        }
    }

    #[derive(Debug)]
    struct ProbeStore {
        durable_effects: Vec<crate::support::fault_plan::CrashCutSeam>,
        recoveries: usize,
    }

    struct ProbeHost {
        store: ProbeStore,
    }

    #[async_trait::async_trait]
    impl crate::support::fault_plan::CrashCutHost for ProbeHost {
        type StoreState = ProbeStore;

        async fn run_to_cut(&mut self, seam: crate::support::fault_plan::CrashCutSeam) {
            self.store.durable_effects.push(seam);
        }

        fn tear_down(self) -> Self::StoreState {
            self.store
        }

        async fn rebuild(store: Self::StoreState) -> Self {
            Self { store }
        }

        async fn recover(&mut self) {
            self.store.recoveries += 1;
        }
    }

    async fn assert_cut_smoke(name: &str) {
        let rebuilt = crate::support::fault_plan::run_crash_cut(
            name,
            ProbeHost {
                store: ProbeStore {
                    durable_effects: Vec::new(),
                    recoveries: 0,
                },
            },
        )
        .await;
        assert_eq!(
            rebuilt.store.durable_effects,
            vec![crate::support::fault_plan::crash_cut(name).unwrap().seam]
        );
        assert_eq!(rebuilt.store.recoveries, 1);
    }

    macro_rules! crash_cut_smoke {
        ($test:ident, $name:literal) => {
            #[tokio::test]
            async fn $test() {
                assert_cut_smoke($name).await;
            }
        };
    }

    crash_cut_smoke!(
        queue_claim_submit_cut_kill_rebuild_recover,
        "queue-claim-submit"
    );
    crash_cut_smoke!(
        queue_input_compile_cut_kill_rebuild_recover,
        "queue-input-compile"
    );
    crash_cut_smoke!(queue_apply_cut_kill_rebuild_recover, "queue-apply");
    crash_cut_smoke!(
        observe_apply_complete_cut_kill_rebuild_recover,
        "observe-apply-complete"
    );
    crash_cut_smoke!(
        reject_apply_complete_cut_kill_rebuild_recover,
        "reject-apply-complete"
    );
    crash_cut_smoke!(ingress_binding_cut_kill_rebuild_recover, "ingress-binding");
    crash_cut_smoke!(
        thread_load_root_cut_kill_rebuild_recover,
        "thread-load-root"
    );
    crash_cut_smoke!(spawn_snapshot_cut_kill_rebuild_recover, "spawn-snapshot");
    crash_cut_smoke!(
        thread_terminal_join_commit_cut_kill_rebuild_recover,
        "thread-terminal-join-commit"
    );
}
