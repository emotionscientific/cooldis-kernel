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

use std::time::Duration;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Intensity {
    Sparse,
    Moderate,
    Hostile,
}

/// Which seam a directive drives: one of the three `fault.rs` wrappers, or
/// the process crash-cut harness.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultComponent {
    Store,
    Queue,
    Provider,
    Process,
}

/// When the fault fires relative to its operation. `Before` is what the
/// wrappers implement today; `After` (effect durable, caller told it
/// failed) is a wrapper extension owned by the fault-plan engine ticket.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FaultTiming {
    Before,
    After,
}

/// What fires. Error construction is component-specific: the engine maps
/// `Fail` to the wrapped trait's error type when applying a directive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlannedAction {
    Fail,
    Delay(Duration),
}

/// One planned fault: the nth occurrence (one-based, matching the
/// `fault.rs` rule format) of a named operation on a component.
#[derive(Clone, Debug)]
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
#[derive(Debug)]
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
        let _ = (seed, intensity);
        unimplemented!("the fault-plan engine implements derivation (ADR 0004, Decision 1)")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitmix64_output_is_pinned_permanently() {
        let mut rng = SplitMix64::new(0);
        assert_eq!(rng.next_u64(), 0xE220_A839_7B1D_CDAF);
        assert_eq!(rng.next_u64(), 0x6E78_9E6A_A1B9_65F4);
        assert_eq!(rng.next_u64(), 0x06C4_5D18_8009_454F);
    }

    #[test]
    fn next_below_is_pinned_permanently() {
        let mut rng = SplitMix64::new(42);
        let draws: Vec<u64> = (0..4).map(|_| rng.next_below(6)).collect();
        assert_eq!(draws, vec![4, 0, 1, 2]);
    }

    #[test]
    fn split_lanes_are_pinned_permanently() {
        let mut rng = SplitMix64::new(7);
        let mut store_lane = rng.split("store");
        assert_eq!(store_lane.next_u64(), 0x1D60_7A07_C3D0_3D6E);
    }
}
