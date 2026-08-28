# How Verlet Is Tested

Verlet is a runtime for governing autonomous agents, and most of its code is
written by autonomous agents. Both halves of that sentence raise the same
question: why should anyone trust it?

This document is the answer. It describes the development process and the
testing discipline together, because they are the same machine: a process
designed so that correctness never depends on trusting any single mind, human
or model. It belongs to the genre of SQLite's "How SQLite Is Tested" and
TigerBeetle's simulator documentation, with one inversion. Those documents
defeat the assumption that mature software is simple. This one also has to
defeat the assumption that agent-written software is careless. The way to
defeat it is not argument but receipts, so this document ends with a dated
ledger of what the process has caught.

## Who writes the code, and who checks it

Three parties develop Verlet, and the process is built on their separation:

- **The anchor** is the human owner. The anchor ratifies changes to the
  frozen vocabulary (event kinds, lexicon terms), resolves every escalation
  that would change the meaning of an existing law, and decides what ships.
  Nothing reaches a release without a human decision at the gate.
- **The architect agent** owns design: architecture decisions, interfaces,
  module boundaries, skeletons, documentation, and review. The architect
  never delegates a decision and reviews every diff against acceptance
  criteria, architecture conformance, and maintainability.
- **The implementer agent** is a frontier model from a different vendor than
  the architect. It implements against fixed interfaces the architect wrote,
  under specs that pin down file structure, tests, and explicit
  out-of-scope lists.

Two rules make this adversarial rather than collaborative in the rubber-stamp
sense:

1. **Every substantive diff is reviewed by a model that did not write it,**
   in addition to the architect's own review. Before any merge of substantive
   work, a second independent thread reviews the composed diff for bugs, with
   authority to fix what it finds. This gate is not ceremonial: on one wave
   in mid 2026 it found 18 defects, 7 of them severe, in a diff that already
   had every test passing. The lesson generalizes: a green suite is a
   necessary condition, never a sufficient one.
2. **Deviations must be declared.** An implementer's report must list every
   design choice that contradicts a document, changes shipped behavior, or
   resolves an ambiguity the spec left open, even choices it believes
   obviously correct. "No deviations" is a falsifiable claim checked in
   review, not a pleasantry.

The reviews go both directions. Architect review of implementer work has
caught, among other things, a subscription resync path that would fail for
every webhook-admitted turn, and a crash-cut guard that compared against a
misspelled event kind and could never fire. Both were in changes whose full
test suites were green.

## What runs on every change

`scripts/verify.sh` is the single entry point, and it is what CI runs on
every push:

- format check across the workspace (a dedicated lint lane is tracked work,
  not yet part of the gate; this document does not get to claim it early);
- the full unit and integration battery (600+ tests as of mid 2026),
  including the daemon I/O battery: delivery, retry, dead-letter, cursor
  recovery, coalescing, dedupe, kill/restart projector coverage;
- guard rails: repository-level checks that pin invariants no single test
  owns (banned patterns, schema presence, doc/code congruence);
- process smokes that exercise real wasm guest execution and real sandboxed
  command execution end to end. The provider-backed plugin lane is opt-in, so
  bare CI does not claim to prove a live model integration.

A full CI run takes about eight minutes. There is no fast path that skips
the suite, and environmental excuses are not accepted grounds for a red lane:
the build system provides bounded, reproducible build lanes precisely so
that "it failed for unrelated reasons" stops being an available sentence.

The architecture matrix separates fast per-push feedback from slower local
emulation. The local per-push gates run on macOS arm64 and Linux arm64. Every
pull request runs the suite remotely on x86_64 Linux. A local x86_64 Linux run
is also part of the default release preflight before tagging and is available
on demand through `scripts/verify-linux.sh --amd64`. Run the on-demand lane for
changes involving pointer width, atomics, SIMD, architecture-conditional
dependencies, or Wasm runtime internals.

## The deterministic lane

Concurrency bugs do not reproduce on demand, so a class of tests runs under
manufactured determinism instead of luck:

- **Paused virtual time.** Tests run on a single-threaded async scheduler
  with the clock stopped. Time advances only when the test advances it, so a
  750 ms coalescing window or a 30 s lease expiry costs nothing to cross and
  cannot flake. Converting the first 13 timing-dependent tests to this
  harness cut their wall time from 2.65 s to 0.40 s and, more importantly,
  made their interleavings exact rather than probable.
- **Barriers at lifecycle cuts.** Tests can park the runtime at named points
  (after a durable write, before an in-memory publish) and interleave a
  competing actor exactly there. Races stop being described in comments and
  start being constructed in code.
- **Fault injection at the trait boundary.** Storage and queue traits have
  wrapping implementations that fail, delay, or duplicate specific
  operations on a seed-derived plan. The store that loses a write is a test
  fixture, not a production incident.
- **Transcript oracles.** The event-sourced substrate means a scenario's
  full effect is a typed event transcript. Tests normalize transcripts
  (stripping ids and timestamps, keeping order and causality) and assert on
  the whole shape, not on a few hand-picked fields.
- **Store parity.** The same battery runs across store implementations, so
  a behavior cannot quietly become an artifact of one backend.

## Crash testing

The runtime's central promise is durability across failure, so failure is a
first-class test input at two altitudes:

- **Process-death pinning tests** cut execution at exact protocol steps:
  after the durable marker commits but before the volatile submission is
  sent, between binding persistence and first-turn submission, mid-batch in
  a queue drain. Each known crash window in the ingress protocol carries a
  test shaped like the crash. Where the protocol has a documented gap, the
  gap's documentation and its test are the same commit; see the ADRs under
  `docs/adr/` for the protocol decisions these tests pin.
- **A true restart smoke** spawns the real daemon binary as a child process,
  drives it over a real local webhook, kills it with SIGKILL (no graceful
  shutdown, no flushing), restarts it against the same stores, and asserts
  continuity from durable state alone: the same routing key reaches the same
  thread, and the compiled context still contains the pre-kill entries. A
  deterministic crash cut, enabled only by a test-only environment variable,
  parks the daemon between the durable binding write and the first turn to
  prove the nastiest window (bound but never ran) does not duplicate
  threads on restart.

## Falsifiability

A test lane that cannot fail is decoration. Two practices keep the lanes
honest:

- **Known regressions are reintroduced on purpose.** When a restart
  continuity lane was added, both historical restart regressions (a
  memory-only binding write and a startup filter that hid fresh threads)
  were separately reintroduced at their exact original code sites, and the
  lane was required to fail on each before the work was accepted.
- **Guards are audited for vacuousness.** A guard that asserts "count of X
  is zero" proves nothing if X is misspelled and the count is zero for the
  wrong reason. Review treats a check that has never fired as a suspect,
  not a comfort.

## The ratchet

Every anomaly the process encounters (a failing test, a schema/code
incongruity, a design question the current model cannot answer cleanly) is
triaged into exactly one of three cases: the conceptual model extends
conservatively, or the change is escalated to the anchor because it would
alter the meaning of something that exists, or the model already answers and
the code is simply wrong. Two mechanical rules ride on top:

- **Every resolution leaves a deterministic guard behind:** a test, a lint,
  or a schema check that makes the resolved question impossible to silently
  reopen. Resolutions without guards are unfinished work.
- **Vocabulary is frozen.** Event kinds are a governed set; adding one
  requires human ratification before any implementation, because every
  durable record written today is a compatibility promise to every replay
  tomorrow.

The result is a monotone process: the set of things that can go wrong
silently shrinks and is not permitted to grow back.

## Why event sourcing makes this affordable

None of the above is heroic effort bolted onto an ordinary codebase. The
runtime is event-sourced: every state change that matters for resume is a
typed, provenance-bearing record on a stream, appends are fenced, and
durable state is reconstructible by folding records. The remaining places
where a mutable table still holds authority on its own are named, tracked,
and being converted; the claim is a ratchet direction with receipts, not a
finished fact. That architecture is why crashes are replayable facts rather than
mysteries, why transcripts can serve as oracles, why two stores can be
checked for parity, and why a restart is an assertable event rather than an
operational anxiety. The discipline Verlet enforces on the agents it runs
(everything witnessed, everything replayable, nothing trusted because it was
asserted) is the same discipline its own development runs on. We test the
runtime the way the runtime treats the world.

## Selected receipts

A sample of what the process has caught, dated, phrased as what would have
shipped otherwise:

- **2026-06, lifecycle wave:** deterministic race construction found and
  pinned five distinct lifecycle races (checkpoint/cancel, fork/shutdown,
  and neighbors) and two real fencing gaps in append paths.
- **2026-06, resync:** a lag-resync path recomputed status from a stale
  read; the deterministic lane forced the interleaving and the fix landed
  with a barrier test.
- **2026-07, composed-diff gate:** 18 defects, 7 severe, in a fully green
  wave: including a projector claim protocol race, a watchdog that could
  cancel the wrong turn, and a dedupe path that failed open.
- **2026-07, architect review:** a subscription resync that would fail for
  every webhook-admitted turn (the only record it trusted lacked the field
  it required); caught in review of a green diff, fixed with a degraded-mode
  test.
- **2026-07, guard audit:** a restart-lane guard comparing against a
  misspelled event kind, vacuously passing; fixed and re-proven against the
  lane's falsifiability check.
- **2026-07, cold review of this document:** an unprimed outside review of a
  fresh clone caught this document overclaiming its own gate (naming a lint
  lane the verify script does not run, and calling a stubbed provider lane
  "real"). Corrected the same day. The falsifiability rule applies to the
  process's descriptions of itself, and this ledger records the failure
  rather than hiding it.

## Where to look

- `scripts/verify.sh`: the whole gate, runnable locally.
- `docs/testing-guidelines.md`: the inward-facing rules for writing new
  tests (test-first, which boundary to start at, lane taxonomy).
- `docs/adr/`: protocol decisions, each with its crash windows named.
- `docs/kernel-invariants.md`: the invariants the batteries exist to hold.
- `.github/workflows/`: CI lanes, including the process-restart smoke.

The short version: assume nothing, witness everything, and make every
resolved question impossible to reopen quietly. Chaotic, but lawful.
