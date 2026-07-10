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

Prefer shared helpers over bespoke test setup:

- runtime builders for tenants, hosts, threads, guests, providers, and stores;
- mock providers and mock external executors that record received requests;
- temp runtime homes, state homes, workspaces, socket paths, and sqlite stores;
- deterministic IDs, clocks, and fixture data where possible;
- event collectors with bounded timeout waits, not blind sleeps;
- snapshot renderers that hide nondeterministic IDs unless lineage is under
  test;
- receipt and grant assertion helpers with stable denial-code checks.

A useful future shape is:

```text
crates/cooldis-kernel/tests/common/
  runtime_builder.rs
  mock_provider.rs
  mock_external_executor.rs
  mock_wasm_guest.rs
  fixture_store.rs
  event_collector.rs
  context_snapshot.rs
  abi_snapshot.rs
  receipt_assertions.rs
```

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
scripts/cargo-lane.sh run --bin cooldis-live-smoke
scripts/cargo-lane.sh run --bin cooldis-vbash-smoke
scripts/cargo-lane.sh run --bin cooldis-wasm-smoke
```

Run app-server and MCP smokes when touching their projections.

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
