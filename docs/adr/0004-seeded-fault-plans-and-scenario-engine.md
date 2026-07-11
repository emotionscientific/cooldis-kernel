# ADR 0004 — Seeded fault plans and the scenario engine

Status: accepted (architect, 2026-07-11; plan of record ratified by the
anchor the same day). Implements the final funded phase of the deterministic
simulation testing track. Vocabulary: `fault plan` and `scenario` are
lexicon-named ahead of implementation per the naming law; this document
carries the mechanism.

## Context

Three consecutive composed-diff review gates each found High-severity,
schedule-dependent defects that no per-ticket review had seen: 18 findings
(7 High) at the wave-1 gate, 9 findings (4 High) at the wave-2 gate, with
the same pattern at the interim rescue reviews. Every High in that set was
an interleaving or crash-window defect: racing first deliveries creating two
active ingress routes, a coalesced queue path dropping the fork attempt
counter, recovery re-running fork effects across an unclosed crash cut, a
Failed-resident thread wedging a reservation.

The gate is a search over failure schedules, performed manually, once per
wave. The substrate to mechanize that search already exists in this repo:

- the paused-time deterministic harness (current-thread Tokio,
  `start_paused`, barrier cuts at lifecycle points) and its fault-injection
  wrappers over the store, queue, and provider seams
  (`tests/support/fault.rs`: `FaultScript` rules behind
  `FaultingRuntimeStore`, `FaultingProviderClient`, `FaultingIngressQueue`,
  keyed on trait-method operation names);
- the normalized typed transcript as oracle
  (`tests/support/transcript.rs`: `TypedTranscript` →
  `NormalizedTranscript` with first-seen alias normalization), with fixture
  assertion and regeneration;
- the store-parity lane (`tests/support/store_parity.rs`: one operation
  sequence against the in-memory and SQLite stores, equal transcripts);
- the true-process restart smoke lane in CI (`cooldis-restart-smoke`),
  which is a single hand-built fault schedule, plus the in-process named
  crash-cut fixtures in the daemon tests.

This ADR fixes the design that turns those parts into a continuous search:
seed-derived fault plans (phase 2 of the simulation consult) and the
scenario engine (phase 3, the funded stopping point). It also settles the
two protocol contracts the wave-2 gate carved out — durable envelope
ownership (EMO-397) and reservation-before-creation (EMO-398) — because
they are invariants the engine must check, and contracts get defined where
they are enforced.

## Decision 1 — fault plans

A **fault plan** is the deterministic expansion of a seed into a schedule of
faults over a versioned fault vocabulary. Law (lexicon): same seed, same
schedule; a vocabulary change is a new version, never a reinterpretation of
old seeds.

**Fault vocabulary v1** — named injection sites, all reachable through the
existing wrappers; no new seams are cut for v1:

- store: fail before append, fail after append (durable but reported
  failed), stale read, partial multi-stream coupling (first stream commits,
  second fails), close/reopen;
- queue: duplicate lease delivery, lease expiry mid-apply, complete fails
  after effect (effect-succeeded-ack-failed), redelivery burst;
- provider/egress: fail N times then succeed, deliver then fail receipt,
  late output after cancellation, pending response across a cut;
- process: death at a named cut, followed by host rebuild over the same
  store (the in-process analog of the restart smoke lane).

**Derivation.** A fault plan is a pure function of
`(seed, vocabulary version, intensity)` using a pinned PRNG algorithm
(SplitMix64, implemented in-repo — the workspace deliberately has no rand
dependency, and a library default could drift). For each named site the
plan decides, per occurrence index, whether a fault fires and which one.
The expansion target is the existing `FaultScript` rule format, so derived
plans drive the same wrappers hand-written tests already use. Intensity
profiles (sparse / moderate / hostile) scale probability mass without
touching determinism.

**Crash-cut harness.** Process-death faults kill the simulated host at the
named cut and rebuild it over the surviving store state, then run recovery
and check invariants. This generalizes the restart smoke lane from one
hand-built schedule to arbitrarily many derived ones, in-process and fast.

## Decision 2 — the scenario engine

A **scenario** is one seeded, bounded run: an operation sequence and a fault
plan derived from the same seed, executed with every declared invariant
checked after every step. Law (lexicon): reproducible from seed and harness
version alone; the failing receipt is seed plus normalized transcript; the
minimized failing sequence joins the fixed corpus.

**Operation alphabet v1** (bounded sequences over): start thread, submit
turn, steer, cancel, fork, restart (process death + recovery), drain queue,
shutdown_all. Deliberately small; growing the alphabet is a versioned
change. Compaction, checkpoint pruning, and multi-tenant operations enter
in later vocabulary versions once the v1 alphabet runs dry.

**Runner.** Executes the sequence on the paused-time harness under the
fault plan; after every operation, runs the invariant set (below) against
the live host, the store, and the normalized transcript so far. On failure:
print the seed and harness version, persist the normalized transcript,
minimize (shrink the operation sequence first, then the fault schedule) and
emit the minimized reproduction.

**Corpora.**

- *Fixed corpus* (in-repo fixtures, run on every PR): each entry is a seed,
  vocabulary version, and a provenance line naming the defect or gate
  finding it pins. Small — tens of entries, seconds of runtime.
- *Rotating corpus* (nightly CI): a widening sweep of fresh seeds; failures
  upload seed + transcript as artifacts, and the minimized reproduction is
  promoted into the fixed corpus with its fix. The nightly job also re-runs
  one seed twice and diffs normalized transcripts — the determinism-drift
  alarm.
- Nightly results publish a witnessed receipt alongside the existing
  release receipts (scenarios run, failures, corpus size).

## Decision 3 — durable envelope ownership (settles EMO-397)

**Finding.** An unsettled non-fork claim can be stranded: the conversation
rebinds to a fork child between attempts, redelivery folds the child's
control stream, the ancestry walk deliberately hides non-fork claims
(per-stream scoping, preserved on purpose), and the envelope re-applies
fresh while its claim sits unsettled on the parent stream.

**Contract.** The stream a claim was appended to durably owns that
envelope's outcome until settle. Redelivery resolves its fold target by
ownership first, current route second: if an ownership record names a
stream, the outcome fold runs against that stream regardless of where the
conversation is currently bound.

**Mechanism.** Ownership is recorded in the ingress state store, keyed by
the envelope's dedupe key, in the same transaction that admits the attempt
to claim — written before the claim is appended to the control stream.
Write ordering makes the failure modes benign: an ownership record without
a claim (death between record and append) is a tombstone superseded by the
next attempt; a claim always has its ownership record. Settle clears
nothing — the record ages out with the dedupe row, so late redeliveries
still resolve to the settled claim and dedupe correctly.

**Rejected.** Binding epochs with drain-before-redelivery: strictly more
machinery (a new primitive, a drain protocol), couples fork delivery
latency to old claims, and misses non-fork rebinds (route-policy changes),
which ownership handles for free. Widening the ancestry walk to all
intents: provisionally rejected at the EMO-384 review to preserve
per-stream scoping; nothing here reopens that.

**Invariant (checked by the engine).** After quiescence with redelivery
enabled, no unsettled claim exists on any control stream — the check is
global across streams, which is exactly the visibility a per-stream fold
cannot have and the scenario engine can.

## Decision 4 — reservation before creation (settles EMO-398)

**Findings.** (1) High: process death between fork child creation and the
parent's `thread.spawned` append makes recovery re-run the fork effects —
a second child, with the first left as durable orphan history. (2) Medium:
the atomic initial-route claim prevents two active roots, but the losing
provisional runtime has already written start history before it learns it
lost.

**Contract.** No thread-creating effect precedes a durable reservation
naming the thread id it will create.

- *Forks:* the fork claim carries a preallocated child thread id — the
  claim is the reservation. Recovery finds-or-creates by that id: child
  creation becomes idempotent, and a child created in the crash window is
  adopted by recovery instead of orphaned. Disposition of the
  creation-before-spawn cut: closed by idempotent adoption.
- *Roots:* the initial route row is taken before any start history is
  written. The losing runtime loses at reservation time, when it has
  written nothing. Disposition of the losing-root residue: eliminated, not
  cleaned up.

No new event kind and no new lexicon word: the fork reservation rides in
the claim payload (a claim already names its intended outcome; the child id
is part of that intent), and the root reservation is a write-ordering rule
on the existing route row.

**Invariants (checked by the engine).** At most one child per fork claim,
under every schedule including death at every cut. Every thread id in the
store was reserved before its first record. No unbound terminal
start-history residue after quiescence.

## The invariant set, v1

The normative list the runner checks after every operation. The first five
generalize existing hand-written assertions; the last four are wave-gate
findings and this ADR's contracts restated as laws:

1. Stream sequence is strictly monotonic per stream; replaying the journal
   re-derives the folded state (replay equivalence).
2. Unique active topology: at most one active runtime per thread; no thread
   executes after `shutdown_all` completes.
3. Queue policy is bounded: no lease outlives its visibility contract
   without redelivery; drained means empty.
4. No duplicate projected output for one correlation (egress publication
   dedupe holds under redelivery).
5. Terminal-event consistency: a Failed or Completed resident never wedges
   a reservation; recovery can always replace terminal residue.
6. Every ingress envelope with an admitted attempt reaches exactly one
   settled claim (execution or recovery), and no unsettled claim survives
   quiescence with redelivery enabled — across streams (Decision 3).
7. At most one child per fork claim; adopted children join the topology
   exactly once (Decision 4).
8. Every thread id was reserved before its first durable record
   (Decision 4).
9. Same seed, same normalized transcript (checked in the nightly drift
   lane rather than per-step).

Each future gate finding lands as a numbered addition to this list with a
fixed-corpus seed that exercises it: the ratchet, applied to concurrency.

## Consequences

- The composed-diff review gate remains mandatory per wave; the engine does
  not replace review. It changes what the gate finds: schedule-dependent
  defect classes get found nightly by search with a reproducing seed,
  instead of once per wave by a reviewer.
- Phases 4 and 5 of the simulation consult stay unfunded behind their entry
  criteria: a virtual network waits for a real class of raw-TCP protocol
  defects; symbol-level determinism overrides wait for measured same-seed
  drift after this phase is stable. This ADR is the strategic stopping
  point by design.
- Implementation is four tickets against the architect skeleton: the
  fault-plan engine and crash-cut harness; the invariant library; the
  scenario generator, runner, and minimizer; corpus and CI wiring. The
  EMO-397/398 contracts (Decisions 3–4) become implementation tickets of
  their own, sequenced with the invariant library so contract and check
  land together.
- Vocabulary note, flagged as ratchet debt: the ingress route pointer table
  uses the word "bindings", which the lexicon reserves for the attachment
  of published operations. Prose in this ADR says "route" / "route
  pointer"; renaming the identifier is an opportunistic cleanup for a
  scheduled ticket, not this phase.
