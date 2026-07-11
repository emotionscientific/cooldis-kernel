#![allow(dead_code)]

//! Scenario engine surface (ADR 0004, Decision 2).
//!
//! A scenario is one seeded, bounded run: an operation sequence and a fault
//! plan derived from the same seed, executed with every declared invariant
//! checked after every step (lexicon: scenario). This file fixes the
//! operation alphabet, the invariant contract, the failure receipt, and the
//! corpus entry format; the generator, runner, and minimizer are
//! implementation work against this surface.

use super::fault_plan::{FaultPlan, Intensity};
use super::kernel_test::RuntimeStore;
use super::transcript::NormalizedTranscript;
use async_trait::async_trait;
use cooldis_io_core::IngressQueueStore;

/// Operation alphabet v1 (ADR 0004). Deliberately small; growing it is a
/// versioned vocabulary change, never a silent addition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScenarioOp {
    StartThread,
    SubmitTurn,
    Steer,
    Cancel,
    Fork,
    /// Kill the simulated host at the next planned cut, rebuild it over the
    /// surviving store state, and run recovery.
    Restart,
    DrainQueue,
    ShutdownAll,
}

/// Generation bounds: short enough to minimize, long enough to interleave.
#[derive(Clone, Copy, Debug)]
pub struct ScenarioBounds {
    pub max_ops: usize,
    pub intensity: Intensity,
}

/// One seeded scenario, reproducible from `(seed, harness version)` alone.
#[derive(Debug)]
pub struct Scenario {
    pub seed: u64,
    pub ops: Vec<ScenarioOp>,
    pub plan: FaultPlan,
}

impl Scenario {
    /// Derive the operation sequence and fault plan from the same seed,
    /// through independent `SplitMix64` split lanes.
    pub fn derive(seed: u64, bounds: ScenarioBounds) -> Self {
        let _ = (seed, bounds);
        unimplemented!("the scenario generator implements derivation (ADR 0004, Decision 2)")
    }
}

/// What an invariant may look at after each step. Deliberately store-first:
/// invariants check durable truth plus the normalized transcript, never
/// in-process convenience state. Anything an invariant needs that is not
/// reachable from here is a missing witness in the design, not a reason to
/// widen this surface casually.
pub struct ScenarioWorld<'a> {
    pub store: &'a (dyn RuntimeStore + Send + Sync),
    /// The ingress queue under test, when the scenario exercises ingress;
    /// bounded-queue invariants pass when it is absent.
    pub queue: Option<&'a (dyn IngressQueueStore + Send + Sync)>,
    pub transcript: &'a NormalizedTranscript,
    /// Index into `Scenario::ops` of the operation just executed.
    pub step: usize,
    /// True once `ShutdownAll` has completed, for terminal invariants.
    pub shut_down: bool,
}

/// One violation, named for its invariant with enough detail to read the
/// failure without re-running.
#[derive(Clone, Debug)]
pub struct InvariantViolation {
    pub invariant: &'static str,
    pub detail: String,
}

/// A named invariant checked after every scenario step. The v1 normative
/// set is ADR 0004 "The invariant set, v1"; each entry lands here carrying
/// its number in its name (for example "inv2-unique-active-topology").
#[async_trait]
pub trait ScenarioInvariant: Send + Sync {
    fn name(&self) -> &'static str;
    /// Return every violation found; empty when the invariant holds. Async
    /// because truth lives in the store.
    async fn check(&self, world: &ScenarioWorld<'_>) -> Vec<InvariantViolation>;
}

/// The receipt of a failing scenario: seed plus normalized transcript
/// (lexicon law), with the violations that fired and where.
#[derive(Debug)]
pub struct ScenarioFailure {
    pub seed: u64,
    pub vocabulary_version: u32,
    pub failing_step: usize,
    pub violations: Vec<InvariantViolation>,
    pub transcript: NormalizedTranscript,
}

/// Runner surface: execute the scenario on the paused-time harness under
/// its fault plan, checking `invariants` after every operation. `Ok(())`
/// when everything held; the failure receipt otherwise. Minimization
/// (shrink ops first, then directives, keeping the failure) belongs to the
/// same ticket and runs on the receipt before it is reported.
pub async fn run_scenario(
    scenario: Scenario,
    invariants: &[Box<dyn ScenarioInvariant>],
) -> Result<(), ScenarioFailure> {
    let _ = (scenario, invariants);
    unimplemented!("the scenario runner implements execution (ADR 0004, Decision 2)")
}

/// One fixed-corpus entry (`tests/fixtures/scenarios/corpus.json`): a seed,
/// the vocabulary version that gives it meaning, its generation bounds, and
/// the defect it pins.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct CorpusEntry {
    pub seed: u64,
    pub vocabulary_version: u32,
    pub max_ops: usize,
    /// "sparse" | "moderate" | "hostile".
    pub intensity: String,
    /// Provenance line naming the defect or gate finding this seed pins.
    pub pins: String,
}
