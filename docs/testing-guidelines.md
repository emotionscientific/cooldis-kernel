# Cooldis Testing Guidelines

Cooldis runtime work should be test-first by default.

Before implementing a runtime change, write or update the test that defines the
kernel contract. If the test cannot be written first because the harness is
missing, build the smallest reusable harness and pair it with the first failing
test.

## What To Test First

Start at the runtime boundary the change affects:

- ABI operation declaration, grant resolution, host dispatch, and receipt;
- Wasm guest behavior through host-mediated capabilities;
- external execution handles, polling, writes, cancellation, and terminal state;
- VFS, process, network, or secret capability enforcement;
- thread lifecycle: start, submit, fork, wait, cancel, shutdown, checkpoint,
  resume, rollback, and orphan handling;
- provider request compilation, streamed model events, tool calls, retries, and
  final output;
- persistence and restart behavior for events, receipts, and thread state.

Avoid starting with customer-owned interfaces. App-server schemas, auth,
dashboards, approval UI, product config, and marketplace semantics belong to
customers or adapters. Test the kernel contract first, then add projection tests
only to prove CLI, MCP, provider-tool, or app-server surfaces map faithfully to
that contract.

## Test Lanes

- **Unit contract tests** for pure reducers, compilers, validators, and policy
  decisions.
- **Normalized snapshots** for ABI declarations, compiled provider context,
  event sequences, and receipts.
- **Mock provider loop tests** for model request/response behavior without real
  network or real auth.
- **Mock external executor tests** for yielded handles, stdin, stdout/stderr,
  cancellation, timeout, and restart behavior.
- **Durability tests** that restart the runtime against the same store and
  continue from persisted state.
- **Process-backed smoke tests** for real binaries, daemons, sockets, and adapter
  wiring.

## Harness Patterns

The canonical kernel harness lives in
`crates/cooldis-kernel/tests/support/`. Integration tests declare
`mod support;`; inline module tests import the same helpers through the crate's
`#[cfg(test)]` `test_support` module. Keep additions in this module family as
plain builders and wrappers rather than introducing a test-framework crate or
macro DSL.

Use the existing helpers before adding bespoke setup:

- `event_trace.rs` provides `EventTrace`, event collectors, ordering assertions,
  and canonical text extraction;
- `scripted_provider.rs` provides `ScriptedProviderClient`, explicit response
  steps, and provider response/factory builders;
- `fault.rs` provides store, provider, and ingress-queue wrappers that fail or
  delay the Nth named trait operation. Scripts are explicit in each test; do
  not derive fault plans from seeds in this layer;
- `transcript.rs` records typed events and receipts, then replaces generated
  IDs, timestamps, and durations with stable aliases. Call `preserve_id` when
  the literal ID is itself the lineage assertion;
- `store_parity.rs` runs the canonical append, read, branch, cursor, replay, and
  fenced-append sequence against any `RuntimeStore`. The `store_parity`
  integration lane compares `InMemorySessionStore` with
  `SqliteSessionStore` and uses the normalized transcript as its oracle;
- runtime builders for tenants, hosts, threads, guests, providers, and stores;
- mock external executors that record received requests;
- temp runtime homes, state homes, workspaces, socket paths, and sqlite stores;
- deterministic IDs, clocks, and fixture data where possible;
- event collectors with bounded timeout waits, not blind sleeps;
- receipt and grant assertion helpers with stable denial-code checks.

Timing-logic tests use Tokio's paused clock:

```rust
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn retries_after_backoff() {
    // Arrange an explicit start/event barrier before moving the clock.
    started.notified().await;
    tokio::time::advance(Duration::from_millis(50)).await;
    completed.notified().await;
}
```

Use barriers (`Notify`, channels, status watches, or task joins) to prove that
the operation reached the intended phase before advancing time. A timeout may
remain as a virtual-time negative assertion. Process-backed, socket, live
provider, and SQLite platform-timing smokes keep real time because their
contract is the platform interaction itself.

## Seeded Fault Plans

`tests/support/fault_plan.rs` expands a `(seed, fault vocabulary version,
intensity)` tuple into a deterministic list of one-based fault directives. The
in-repo SplitMix64 implementation is pinned, and each component derives through
its own `store`, `queue`, `provider`, or `process` split lane. A vocabulary or
derivation change in one component must not shift another component's schedule
under the same seed. The exact v1 probability shape is documented beside the
implementation and pinned by the sparse, moderate, and hostile JSON fixtures in
`tests/fixtures/fault_plans/`.

To reproduce a reported schedule, derive the reported seed with its recorded
intensity and `FAULT_VOCABULARY_VERSION`; do not substitute the current version
for an older receipt. To inspect or deliberately regenerate the pinned v1
fixtures, run:

```bash
COOLDIS_UPDATE_FIXTURES=1 scripts/cargo-lane.sh test -p cooldis derivation_is_fixture_pinned
```

Review the fixture diff before keeping it. A normal test run compares the
serialized directives with those fixtures and fails on drift.

Apply wrapper directives with `FaultPlan::apply`, which configures the existing
`FaultingRuntimeStore`, `FaultingIngressQueue`, and `FaultingProviderClient`.
`Before` prevents the wrapped effect. `After` lets a successful wrapped effect
finish and then reports the scripted component error; use it for ambiguous
commit windows such as store appends and `complete_ingress`. Process directives
go through the named crash-cut registry and the in-process
run-to-cut/teardown/rebuild/recover helper.

Adding, removing, renaming, or reordering an operation or cut is a vocabulary
change. Bump `FAULT_VOCABULARY_VERSION`, retain the old fixtures when old
receipts still need replay support, document the new derivation contract, and
add fixtures for every intensity. Changing probabilities, occurrence bounds,
timing eligibility, action selection, lane seeding, or collision handling also
changes seed meaning and therefore requires the same version bump. Never update
fixtures merely to make unexplained drift green.

## Process Smoke Rules

Process smoke should prove wiring, not exhaust every edge case:

- launch the real binary or daemon;
- use temp homes and short socket paths;
- run one representative operation end to end;
- assert a stable status, event, or receipt;
- shut down cleanly;
- keep the smoke cheap enough to run before claiming runtime work is done.

For Cooldis runtime changes, use:

```bash
scripts/cargo-lane.sh test
scripts/cargo-lane.sh run --bin cooldis-vbash-smoke
scripts/cargo-lane.sh run --bin cooldis-wasm-smoke
```

Run app-server and MCP smokes when touching their projections.

## Continuous Integration

Pull requests and pushes to `main` run `scripts/verify.sh`. The default CI lane
checks formatting, runs the locked workspace test suite for all targets, and
runs the virtual-bash and Wasm smoke binaries.

Two provider-backed lanes remain opt-in and are disabled in regular CI. Set
`COOLDIS_VERIFY_LIVE_PLUGIN=1` to run the live plugin smoke, or set
`COOLDIS_VERIFY_LIVE_S3=1` to run the ignored real-S3 object-store test.

## Cargo Build Lanes

Concurrent local worktrees share two exclusive Cargo build lanes. `main` and
`integration/*` use the integration lane. Every other branch uses the feature
lane. Commands in one lane wait for each other, while the two lanes may build
at the same time.

Use `scripts/cargo-lane.sh` for direct Cargo commands. Workspace automation
selects the same wrapper for commands launched through `cargo`, `just`, or a
nested script. Do not set `CARGO_TARGET_DIR` or pass `--target-dir`; the wrapper
owns the target path and rotates it when lane ownership changes.
Local Cargo aliases and external subcommands are trusted configuration and must
not inject their own target paths.

The managed profile disables incremental output, keeps line-table debug
information for development and tests, and bounds compiler caching when
`sccache` is available. A missing `sccache` installation is a warning, not a
build failure.

## Scenario Invariant Library

ADR 0004's v1 scenario library checks these numbered invariants after each
scenario step:

- `inv1-replay-equivalence`;
- `inv2-unique-active-topology`;
- `inv3-bounded-queue`;
- `inv4-no-duplicate-projected-output`;
- `inv5-terminal-consistency`;
- `inv6-claims-settle`;
- `inv7-one-child-per-fork-claim`;
- `inv8-reserved-before-created`.

The runner executes against the real daemon/app-server lifecycle over a
temporary SQLite store, with deterministic test-only adapters for provider,
queue, placement, and crash-cut witnesses. Invariant inputs remain store-first:
durable events plus normalized non-mutating receipts. The fixed corpus is a
normal library test and must enumerate every seed it runs; missing, empty,
malformed, stale-vocabulary, or unknown-intensity entries fail closed:

```bash
scripts/cargo-lane.sh test -p cooldis --lib scenario_corpus_holds -- --nocapture
```

Run a fresh rotating sweep by supplying a base seed and count without mutating
process environment from inside the test:

```bash
COOLDIS_SCENARIO_SWEEP_BASE_SEED=40520260711 \
COOLDIS_SCENARIO_SWEEP_COUNT=24 \
scripts/cargo-lane.sh test -p cooldis --lib scenario_nightly_sweep -- --ignored --nocapture
```

The receipt reports the attempted count, per-intensity tallies, corpus size,
commit witness, and every scenario failure, determinism drift, or caught runner
panic. A caught panic is a failed sweep, but it must not suppress the receipt.

Each reproducible scenario failure joins the fixed corpus with a provenance
line naming the defect or gate finding it pins. Harness defects such as
same-seed drift or receipt suppression also require a focused regression; when
a sweep seed exposes one, its corpus provenance points to that regression.

### Nightly Failure Promotion

A minimized nightly scenario failure joins
`crates/cooldis-kernel/tests/fixtures/scenarios/corpus.json` in the same pull
request as its fix. Its `pins` line names the issue that owns the failure, so
the regression remains attributable and reproducible.

Corpus entries are never removed except by an explicit vocabulary-version bump
ticket. A vocabulary bump must account for the old seed meaning rather than
silently reinterpreting or pruning the entry.

Before claiming a runtime change is complete, run the required test command
through the lane wrapper. For the full workspace suite:

```bash
scripts/cargo-lane.sh test --workspace --all-targets --locked
```

## Terminology Lint

`crates/cooldis-kernel/tests/lexicon_lint.rs` keeps selected public kernel
terminology from drifting. It scans Rust source under
`crates/cooldis-kernel/src/` for deprecated vocabulary in identifiers,
serde-visible names, and event-kind constants, then compares existing debt to
`crates/cooldis-kernel/tests/lexicon_lint_baseline.txt`. Baseline entries use
`<relative path> <word> <count>` so unrelated line edits do not churn the file.

Use `// lexicon-allow: <word> - <reason>` on the offending line or the line
above for permanently faithful foreign vocabulary, such as wasm linear memory
or an external wire-format field. The baseline is debt, not permission: it only
shrinks and its counts must track reality. Do not add entries for new
violations. See [Kernel Invariants](kernel-invariants.md) for the public
terminology orientation.

## Threat-Model Lint

`crates/cooldis-kernel/tests/threat_model_lint.rs` parses
`docs/threat-model.md` as a strict registry. It enforces unique IDs, contiguous
per-area numbering, the status and severity vocabularies, required entry fields,
and affected-surface paths that resolve inside the repository. The companion
`crates/cooldis-kernel/tests/threat_model_ids.txt` is append-only and must match
the document order.

Do not delete or renumber an entry. Append new IDs to both files. When a threat
is resolved, change its status in place to `MITIGATED` and name the test or lint
that proves the mitigation. `ACCEPTED` requires the architect-approved rationale
in the mitigation field.

## Review Checklist

Before calling runtime work done:

- The failing test or harness test existed before the implementation.
- The test asserts a kernel-visible invariant, not private implementation shape.
- Customer interface assumptions are kept out of kernel tests.
- Real network, real auth, and ambient machine state are avoided outside marked
  live/smoke lanes.
- Time-sensitive tests use deterministic clocks or bounded waits.
- Process, sandbox, and external-exec lanes are isolated or serialized when
  parallelism can exhaust resources.
- Snapshot changes are reviewed as contract changes.
- The relevant focused test and repo-native verification command were run.
