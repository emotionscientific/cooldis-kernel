# Verlet Kernel Threat Model

This document records security threats to the Verlet runtime kernel and its
first-party projections. It is a registry, not a claim that every listed risk
has been resolved. OPEN entries are work for architect disposition.

## Scope and trust boundaries

The protected assets are host command authority, filesystem and network
authority, provider and tool credentials, tenant and thread isolation, durable
history integrity, control-plane availability, and release artifact integrity.

The current local V1 deployment trusts the operating-system user that owns the
daemon. Unix sockets, loopback TCP, stdio projections, local state directories,
provider endpoints, remote child processes, and release automation are separate
boundaries inside that deployment. A loopback address is not an application
identity. An `initialize` protocol message establishes protocol state only.

## Registry discipline

Threat IDs have the form `TM-<AREA>-<NNN>`. IDs are append-only. Never delete or
renumber an entry. Resolution changes its status to MITIGATED and names the
deterministic guard, or to ACCEPTED and records the architect-approved rationale
in the mitigation field. New entries take the next number in their area.

Statuses are OPEN, MITIGATED, and ACCEPTED. Severity is impact under a plausible
deployment: High can expose host authority, secrets, durable integrity, release
integrity, or service-wide availability; Medium has a narrower prerequisite or
blast radius; Low is defense in depth. Severity does not replace disposition.

The lint in `crates/verlet-kernel/tests/threat_model_lint.rs` enforces the entry
shape, allowed areas and statuses, unique contiguous IDs, the append-only ID
baseline, and repo-relative affected-surface paths that resolve to files.

## External ingress

## TM-INGRESS-001: Telegram webhook authentication can fail open

- Status: OPEN
- Severity: High
- Threat: An enabled Telegram webhook accepts updates without authenticating the sender when neither `secret_token` nor `secret_token_env` is configured. An Internet-reachable listener can therefore admit forged turns and consume runtime authority.
- Affected surface: `crates/verlet-kernel/src/daemon/daemon_config.rs`, `crates/verlet-kernel/src/daemon/daemon_io.rs`
- Mitigation: Required: reject enabled Telegram routes without a secret, compare the header through a reviewed secret-check path, and document proxy trust assumptions.
- Deterministic guard: None. Required: configuration tests for absent secrets and request tests for missing, incorrect, and correct secret headers.

## TM-INGRESS-002: Privileged app-server RPC lacks an always-on identity check

- Status: MITIGATED
- Severity: High
- Threat: A process that reaches the daemon Unix socket or loopback WebSocket could invoke process, filesystem, provider-auth, approval, mandate, and binding methods without an application identity, and a guessable console token or loose socket permissions would widen that reach.
- Affected surface: `crates/verlet-kernel/src/adapters/app_server/mod.rs`, `crates/verlet-kernel/src/adapters/app_server/connection.rs`
- Mitigation: Existing: every RPC WebSocket connection resolves a principal before any method dispatch (bearer token on both transports, exact console subprotocol carrier, same-uid peer mapping in local mode only); failed authentication returns a uniform 401, opens no session, and is witnessed; the socket file is chmod 0o600; the console credential is minted from a 256-bit CSPRNG secret per construction, only its digest is persisted, and at most one is active per state home; every dispatched method is authorized by authority class at the dispatcher with unknown methods failing closed to Host.
- Deterministic guard: `crates/verlet-kernel/tests/boundary_auth.rs` pins accepted/rejected/expired/revoked tokens, unauthenticated 401s on both transports, socket mode, adapter/operator authorization splits, and the console credential lifecycle; `unix_peer_mapping_rejects_a_uid_other_than_the_daemon_euid` and the exhaustive classification test in `crates/verlet-kernel/src/adapters/app_server/tests.rs` pin mismatched peer rejection and fail on any unclassified dispatch arm.

## TM-INGRESS-003: MCP and ACP stdio delegate daemon authority without a principal

- Status: OPEN
- Severity: Medium
- Threat: The MCP and ACP processes accept requests from any writer on their stdio pipes and proxy them to the daemon socket without a caller identity or delegated capability set. A launcher, plugin host, or future network wrapper can accidentally grant broader daemon authority than intended.
- Affected surface: `crates/verlet-kernel/src/adapters/mcp_server.rs`, `crates/verlet-kernel/src/adapters/acp_agent.rs`, `crates/verlet-kernel/src/bin/verlet-mcp-server.rs`, `crates/verlet-kernel/src/bin/verlet-acp-agent.rs`
- Mitigation: Existing: the daemon side resolves the projection connection to a principal through a credential, or through the local-mode same-uid mapping, and authorizes each method at the dispatcher. The projection cannot exceed that resolved principal's authority, but a local launcher still delegates the full peer-mapped operator. Required: define the launcher as an explicit trust boundary, issue least-authority daemon credentials per projection, and require authentication in any non-stdio wrapper.
- Deterministic guard: `crates/verlet-kernel/tests/boundary_auth.rs` pins per-principal method authorization on the daemon socket. Required: projection tests that prove the delegated method set and reject credentials or methods outside it.

## TM-INGRESS-004: Debug RPC inherits the control plane's transport weakness

- Status: MITIGATED
- Severity: Medium
- Threat: The debug client can call an arbitrary JSON-RPC method on a configurable WebSocket endpoint; if the endpoint accepted unauthenticated connections, treating the client as a harmless diagnostic would expose the full control plane.
- Affected surface: `crates/verlet-kernel/src/cli/debug_rpc.rs`, `crates/verlet-kernel/src/adapters/codex_tui.rs`
- Mitigation: Existing: the app-server endpoint authenticates every connection, so the debug client succeeds only with a valid credential (supplied via `VERLET_APP_SERVER_TOKEN` or explicit configuration) and acts with exactly that principal's authority under the dispatcher gate.
- Deterministic guard: `crates/verlet-kernel/tests/boundary_auth.rs` pins the unauthenticated 401 on the WebSocket endpoint the debug client targets.

## TM-INGRESS-005: Unix peer mapping could become a privilege escalation loop

- Status: MITIGATED
- Severity: High
- Threat: Mapping a same-uid Unix peer to the operator principal is a convenience for the host user, but a daemon-spawned process reconnecting through the socket runs as the same uid; if peer mapping applied in a managed deployment, an agent workload could re-enter the control plane as the operator.
- Affected surface: `crates/verlet-kernel/src/adapters/app_server/mod.rs`, `crates/verlet-kernel/src/daemon/identity.rs`
- Mitigation: Existing: peer mapping applies in `local` mode only and compares the peer uid against the daemon's effective uid; `managed` mode never maps a peer and requires a credential on every connection, witnessing the refusal.
- Deterministic guard: `crates/verlet-kernel/tests/boundary_auth.rs` pins same-uid mapping in local mode and the managed-mode refusal with a `PeerMappingDisabled` witness; `unix_peer_mapping_rejects_a_uid_other_than_the_daemon_euid` in `crates/verlet-kernel/src/adapters/app_server/tests.rs` pins the mismatched-uid refusal and witness.

## TM-INGRESS-006: A managed daemon must not start with synthesized identity

- Status: MITIGATED
- Severity: High
- Threat: If a managed-mode daemon could start with a missing or partial `[daemon.identity]` section, it would silently run with synthesized local defaults (permissive tenant, no expected principals) while operators believed managed-mode guarantees applied.
- Affected surface: `crates/verlet-kernel/src/daemon/identity.rs`, `crates/verlet-kernel/src/daemon/daemon_config.rs`, `crates/verlet-kernel/src/cli/daemon.rs`
- Mitigation: Existing: `managed` mode hard-fails at startup unless `tenant_id` and `console_principal` are present and non-blank; the config layer merge is section-atomic so a managed overlay cannot inherit a lower layer's tenant; the CLI revalidates before constructing the app server; local-mode synthesis lives in exactly one code site.
- Deterministic guard: config tests pin blank-field hard fails and section-atomic merges (`crates/verlet-kernel/src/daemon/daemon_config/tests.rs`, `crates/verlet-kernel/src/daemon/identity.rs` unit tests); `crates/verlet-kernel/src/cli/daemon/tests.rs` pins a managed identity TOML reaching the initialized boundary authority through the production constructor.

## Authority and authorization

## TM-AUTHZ-001: Admission evidence does not authenticate the actor

- Status: OPEN
- Severity: High
- Threat: The default host and app-server admission policy deterministically records `queue`, but it does not establish who submitted the turn or whether that principal may act on the target thread. A valid admission record can therefore be mistaken for an authorization decision.
- Affected surface: `crates/verlet-kernel/src/kernel/admission.rs`, `crates/verlet-kernel/src/adapters/app_server/connection.rs`, `crates/verlet-kernel/src/daemon/daemon_io.rs`
- Mitigation: Existing: RPC-originated turns now require an authenticated principal before dispatch, the ingress record carries that principal with `via="caller:{session_id}"` pointing at the witnessed session, and the admission record references that ingress event; external io lanes carry their own witnessed principal per ADR 0007. Required: a per-thread ownership policy so a principal authorized for a method class is still checked against the specific target thread (grant algebra, future ticket family).
- Deterministic guard: `crates/verlet-kernel/tests/boundary_auth.rs` pins the caller stamp deriving from a witnessed session. Required: lifecycle tests proving cross-principal submissions against a non-owned thread are rejected once thread ownership exists.

## TM-AUTHZ-002: Manifest-declared grants are not independently authorized

- Status: OPEN
- Severity: High
- Threat: Bind verifies that a tool row declares every capability required by its pinned operation, but the manifest's own grant strings become the effective granted set. There is no independent operator allowlist or policy decision proving the publisher may grant host, network, secret, or child-thread authority.
- Affected surface: `crates/verlet-kernel/src/agent/manifest_bind.rs`, `crates/verlet-agent/src/manifest_schema.rs`
- Mitigation: Required: separate requested grants from operator-authorized grants, intersect them at bind, and record the authorizing principal, policy, and snapshot in the bind receipt.
- Deterministic guard: None. Required: bind tests where a correctly declared but unauthorized capability fails closed.

## TM-AUTHZ-003: Approval and mandate mutation have no caller authorization

- Status: OPEN
- Severity: High
- Threat: Boundary authority classes now gate approval and mandate methods, but the durable approval and mandate records identify the thread and decision without the authenticated actor or authorization policy. They cannot prove who approved or mutated the record, or prevent an executing agent principal from approving itself once agent principals exist.
- Affected surface: `crates/verlet-kernel/src/adapters/app_server/connection.rs`, `crates/verlet-kernel/src/kernel/mandate_lifecycle.rs`
- Mitigation: Existing: `approval/resolve` is Host-class (adapter principals are refused) and writes a durable host-effect witness row naming the acting session, principal, and method before the effect proceeds; mandate mutation requires an authenticated Interactive-or-above principal. Required: persist the acting principal into the approval and mandate records themselves, and prevent self-approval by the executing agent principal.
- Deterministic guard: the exhaustive table test in `crates/verlet-kernel/src/adapters/app_server/tests.rs` pins `approval/resolve` as Host and mandate mutation as Interactive; `crates/verlet-kernel/tests/boundary_auth.rs` pins the generic adapter-to-Host refusal and fail-closed host-effect witness path. Required: approval-specific actor-provenance, cross-tenant, and self-approval denied controls.

## TM-AUTHZ-004: Runtime coordinate checks isolate tenant and session topology

- Status: MITIGATED
- Severity: High
- Threat: A caller that substitutes tenant, user, session, or parent coordinates could read or control another runtime thread.
- Affected surface: `crates/verlet-kernel/src/kernel/supervisor.rs`, `crates/verlet-kernel/src/kernel/supervisor/tests.rs`, `crates/verlet-history/src/lib.rs`, `crates/verlet-history-sqlite/src/tests.rs`
- Mitigation: Existing: supervisor and history paths validate full coordinates, reject cross-tenant and cross-session topology, and validate referenced history scope.
- Deterministic guard: `crates/verlet-kernel/src/kernel/supervisor/tests.rs` and `crates/verlet-history-sqlite/src/tests.rs` pin unknown-tenant, scope-mismatch, and wrong-parent rejection paths.

## Execution substrate

## TM-EXEC-001: Host command execution has ambient daemon authority

- Status: OPEN
- Severity: High
- Threat: Host command backends run unsandboxed child processes as the daemon user, inherit the daemon environment by default, and can use arbitrary host paths and network access. A control-plane or grant bypass becomes host code execution with the daemon's full ambient authority.
- Affected surface: `crates/verlet-process/src/execution.rs`, `crates/verlet-process/src/live.rs`, `crates/verlet-kernel/src/adapters/app_server/connection.rs`
- Mitigation: Required: place untrusted execution in a sandbox with explicit filesystem, process, network, and environment grants; keep an explicitly named operator-only ambient host lane if needed.
- Deterministic guard: None. Required: sandbox escape and denied-capability tests, plus environment and working-directory allowlist tests.

## TM-EXEC-002: Remote placement is process separation, not a security sandbox

- Status: OPEN
- Severity: High
- Threat: A remote child is another local process under the same operating-system user. It inherits host namespaces and the parent environment, so a compromised child can reach resources outside its scoped sync lease even though its journal writes are fenced.
- Affected surface: `crates/verlet-kernel/src/daemon/remote_store/process_executor.rs`, `crates/verlet-kernel/src/daemon/remote_store/placement.rs`, `crates/verlet-kernel/src/daemon/remote_store/lease.rs`
- Mitigation: Required: distinguish local process placement from isolated placement, clear and rebuild the child environment, and use an enforceable sandbox or remote worker identity for hostile workloads.
- Deterministic guard: None. Required: child isolation tests covering environment, filesystem, network, process visibility, and sync-lease scope.

## TM-EXEC-003: Process output and virtual spill retention are bounded

- Status: MITIGATED
- Severity: Medium
- Threat: Commands that emit unlimited stdout or stderr can exhaust memory or the virtual spill filesystem.
- Affected surface: `crates/verlet-process/src/live.rs`, `crates/verlet-vbash/src/lib.rs`, `crates/verlet-vbash/src/harness.rs`, `crates/verlet-kernel/src/capabilities/execution.rs`
- Mitigation: Existing: normal process tools cap retained output, virtual bash retains at most 64 MiB per stream, and the spill VFS has a fixed aggregate ceiling.
- Deterministic guard: `crates/verlet-process/src/live.rs`, `crates/verlet-vbash/src/lib.rs`, and `crates/verlet-kernel/src/capabilities/execution/tests.rs` pin truncation, retention ceilings, and spill behavior.

## TM-EXEC-004: Explicit cancellation kills and reaps owned process groups

- Status: MITIGATED
- Severity: High
- Threat: Cancelling only a shell leader can leave grandchildren running or pipe-holding descendants blocking completion.
- Affected surface: `crates/verlet-process/src/execution.rs`, `crates/verlet-process/src/live.rs`
- Mitigation: Existing: host children start in their own process groups, cancellation sends termination and kill signals to the group, waits for the child, and performs bounded owned-group reaping.
- Deterministic guard: `crates/verlet-process/src/execution.rs` and `crates/verlet-process/src/live.rs` include process-backed tests for cancellation after the leader exits, partial output, and group cleanup.

## TM-EXEC-005: Daemon termination bypasses runtime shutdown

- Status: OPEN
- Severity: High
- Threat: The foreground daemon serve loop does not install SIGINT or SIGTERM handling that calls supervisor shutdown. Service-manager termination can therefore skip ordered thread shutdown, cancellation grace, process-group cleanup, and final lifecycle receipts.
- Affected surface: `crates/verlet-kernel/src/cli/daemon.rs`, `crates/verlet-kernel/src/adapters/app_server/mod.rs`, `crates/verlet-kernel/src/kernel/supervisor.rs`
- Mitigation: Required: wire operating-system termination to stop accepting ingress, drain or cancel active work through the supervisor, bound the grace period, and then force cleanup.
- Deterministic guard: None. Required: process smoke tests that send SIGTERM during active turns and commands, then verify terminal receipts and no surviving descendants.

## TM-EXEC-006: PID 1 reaping covers only explicitly killed process groups

- Status: OPEN
- Severity: Medium
- Threat: When Verlet is container PID 1, it has init responsibilities. Current reaping runs only after explicit cleanup of a known process group, so unrelated orphaned descendants or abrupt termination paths can accumulate zombies or escape the owned-group fold.
- Affected surface: `crates/verlet-process/src/execution.rs`, `crates/verlet-process/src/live.rs`, `crates/verlet-kernel/src/daemon/daemon_config.rs`
- Mitigation: Required: document that a real init must wrap Verlet, or implement and test complete PID 1 signal forwarding and child reaping.
- Deterministic guard: None. Required: a container smoke with Verlet as PID 1 that exercises normal exit, cancellation, daemon termination, grandchildren, and orphan reaping.

## Storage and journal integrity

## TM-STORE-001: Atomic append fences prevent stale decisions from winning

- Status: MITIGATED
- Severity: High
- Threat: A read-decide-append race can duplicate or overwrite first-wins control decisions when another writer advances the stream.
- Affected surface: `crates/verlet-history/src/lib.rs`, `crates/verlet-history-sqlite/src/lib.rs`, `crates/verlet-history-sqlite/src/tests.rs`
- Mitigation: Existing: the event-store contract fails closed when fenced appends are unsupported, and the in-memory and SQLite stores atomically compare the next sequence before appending an all-or-nothing batch.
- Deterministic guard: `crates/verlet-history-sqlite/src/tests.rs` runs the shared fenced-append conformance sequence against both stores and verifies conflict batches leave no mutation.

## TM-STORE-002: Provenance references are not referentially validated

- Status: OPEN
- Severity: High
- Threat: A discharged event is rejected only when provenance is completely empty. The append path does not prove that named source streams and event IDs exist, belong to the declared coordinate scope, match each other, or precede the discharge, so compromised internal or remote writers can forge plausible audit lineage.
- Affected surface: `crates/verlet-history/src/lib.rs`, `crates/verlet-history-sqlite/src/lib.rs`, `crates/verlet-kernel/src/daemon/remote_store/endpoint.rs`
- Mitigation: Required: define provenance referential laws and validate source identity, scope, ordering, and allowed external references atomically at the authoritative append boundary.
- Deterministic guard: None. Required: store-parity rejection tests for missing, cross-scope, mismatched-stream, future, and cyclic provenance references.

## TM-STORE-003: Empty provenance and invalid stream payloads fail closed

- Status: MITIGATED
- Severity: High
- Threat: Derived records without provenance or malformed payloads can corrupt the audit stream and make replay ambiguous.
- Affected surface: `crates/verlet-history/src/lib.rs`, `crates/verlet-history/src/tests.rs`, `crates/verlet-history-sqlite/src/tests.rs`
- Mitigation: Existing: discharged events require non-empty provenance, event payloads are checked against the stream schema before commit, and a bad batch commits no prefix.
- Deterministic guard: `crates/verlet-history/src/tests.rs` and `crates/verlet-history-sqlite/src/tests.rs` pin empty-provenance rejection and atomic invalid-payload rejection.

## TM-STORE-004: Durable journals and metadata have no retention budget

- Status: OPEN
- Severity: Medium
- Threat: Session entries, event records, observations, ingress queues, dead letters, metadata, and child state can grow for the lifetime of a state home. A busy or hostile ingress can exhaust disk even when per-request and per-process memory is bounded.
- Affected surface: `crates/verlet-history-sqlite/src/lib.rs`, `crates/verlet-kernel/src/daemon/daemon_io.rs`, `crates/verlet-kernel/src/daemon/remote_store/queue.rs`, `crates/verlet-metadata/src/lib.rs`
- Mitigation: Required: define per-tenant and per-state-home quotas, compaction and archival policy, backpressure before disk exhaustion, and observable operator thresholds.
- Deterministic guard: None. Required: quota and compaction tests that prove writes fail predictably without corrupting existing history.

## Secrets

## TM-SECRET-001: Tool secrets use named capabilities and redacted status surfaces

- Status: MITIGATED
- Severity: High
- Threat: Raw tool credentials can leak into manifests, model context, status APIs, logs, or tool results.
- Affected surface: `crates/verlet-metadata/src/secret_store.rs`, `crates/verlet-metadata/src/secret_store/tests.rs`, `crates/verlet-wasm/src/runner.rs`, `docs/secret-management.md`
- Mitigation: Existing: manifests declare `secret:<name>`, missing refs fail before invocation, status values are redacted, and the HTTP host import injects resolved values only into authorized secret headers.
- Deterministic guard: `crates/verlet-metadata/src/secret_store/tests.rs` and `crates/verlet-wasm/src/runner.rs` pin ref validation, missing-secret failure, redacted status, and header injection checks.

## TM-SECRET-002: Local credential databases store plaintext values

- Status: OPEN
- Severity: Medium
- Threat: Secret and provider credential values are stored as plaintext SQLite fields. Owner-only directory and file modes reduce cross-user exposure but do not protect backups, same-user compromise, copied state homes, or offline disk access.
- Affected surface: `crates/verlet-metadata/src/secret_store.rs`, `crates/verlet-metadata/src/provider_store.rs`
- Mitigation: Required: choose and document an at-rest protection model, preferably external secret-manager references or operating-system key storage, with migration and deletion semantics.
- Deterministic guard: None. Required: storage tests proving persisted records contain only protected material and old plaintext records migrate without logging values.

## TM-SECRET-003: Spawned commands and remote children inherit daemon credentials

- Status: OPEN
- Severity: High
- Threat: Host command and remote child creation do not clear the parent environment. Provider keys, cloud credentials, webhook secrets, and other daemon configuration inherited from the environment may become readable by agent-controlled code without a named secret grant.
- Affected surface: `crates/verlet-process/src/live.rs`, `crates/verlet-process/src/execution.rs`, `crates/verlet-kernel/src/daemon/remote_store/process_executor.rs`
- Mitigation: Required: start untrusted children from an empty environment, add an explicit non-secret allowlist, and inject named secrets only at the authorized effect boundary.
- Deterministic guard: None. Required: process-backed tests that seed sentinel parent secrets and prove they are absent unless explicitly granted.

## Supply chain

## TM-SUPPLY-001: Cargo resolution is locked in verification and release lanes

- Status: MITIGATED
- Severity: High
- Threat: Unreviewed dependency resolution drift can change the built runtime between review, CI, and release.
- Affected surface: `Cargo.lock`, `scripts/verify.sh`, `scripts/package-release-binary.sh`, `.github/workflows/verify.yml`
- Mitigation: Existing: the lockfile is committed and workspace tests, correctness clippy, executable smokes, and release builds use locked Cargo resolution.
- Deterministic guard: `scripts/verify.sh` and `scripts/package-release-binary.sh` fail when the lockfile cannot satisfy the build.

## TM-SUPPLY-002: CI actions and toolchains use mutable references

- Status: OPEN
- Severity: Medium
- Threat: Workflows select third-party actions by release tag and Rust by the mutable `stable` channel. Upstream tag movement or channel compromise can change privileged CI and release execution without a repository diff.
- Affected surface: `.github/workflows/verify.yml`, `.github/workflows/release.yml`
- Mitigation: Required: pin third-party actions and release toolchains to reviewed immutable digests or commits, with an explicit update process.
- Deterministic guard: None. Required: workflow lint that rejects non-immutable action and release-toolchain references.

## TM-SUPPLY-003: Release checksums are unsigned and share the artifact trust domain

- Status: OPEN
- Severity: High
- Threat: The installer verifies SHA-256, but the archive, checksum, manifest, and installer are published through the same GitHub release authority. Compromise of that publisher can replace all four consistently and deliver arbitrary code.
- Affected surface: `scripts/install.sh`, `scripts/write-release-manifest.sh`, `.github/workflows/release.yml`, `RELEASE.md`
- Mitigation: Required: publish signed provenance and artifact signatures from a protected identity, verify them in the installer, and document key rotation and recovery.
- Deterministic guard: None. Required: installer smokes that reject unsigned, wrong-identity, altered, and expired provenance.

## TM-SUPPLY-004: Dependency advisories are not a required CI gate

- Status: OPEN
- Severity: Medium
- Threat: Locked dependencies can remain reproducible while carrying known vulnerabilities or disallowed sources because the default gate does not run an advisory or policy scan.
- Affected surface: `scripts/verify.sh`, `.github/workflows/verify.yml`, `Cargo.toml`, `Cargo.lock`
- Mitigation: Required: add a pinned advisory database and dependency policy tool with explicit, expiring exceptions.
- Deterministic guard: None. Required: CI must fail on a controlled vulnerable fixture or expired exception and pass with the approved policy set.

## Resource exhaustion and denial of service

## TM-DOS-001: Telegram accepts unbounded concurrent slow requests

- Status: OPEN
- Severity: High
- Threat: The webhook bounds headers and bodies but applies no read deadline, connection limit, per-source rate limit, or pre-auth concurrency budget. Attackers can hold many tasks open before the optional secret check and starve the daemon.
- Affected surface: `crates/verlet-kernel/src/daemon/daemon_io.rs`
- Mitigation: Required: add header and body deadlines, a global connection semaphore, authenticated rate limits, and bounded overload responses.
- Deterministic guard: None. Required: process smokes for slow headers, slow bodies, connection floods, and recovery after overload.

## TM-DOS-002: App-server connections and outbound queues are unbounded

- Status: OPEN
- Severity: Medium
- Threat: Each accepted app-server connection creates detached tasks and an unbounded outbound channel, while a single WebSocket message may be 128 MiB. A local attacker or compromised client can consume memory and task capacity faster than the runtime drains it.
- Affected surface: `crates/verlet-kernel/src/adapters/app_server/mod.rs`, `crates/verlet-kernel/src/adapters/app_server/subscriptions.rs`
- Mitigation: Existing: the pre-authentication handshake is bounded (10-second per-stage deadline, 8 KiB header cap), so unauthenticated peers cannot hold accept-path resources indefinitely. Required: cap concurrent connections, use bounded outbound queues with explicit slow-consumer behavior, reduce or budget message size, and add request deadlines.
- Deterministic guard: `pre_upgrade_reads_and_upgrade_are_bounded_when_no_data_arrives` and `oversized_pre_upgrade_headers_fail_closed_with_one_witness` in `crates/verlet-kernel/src/adapters/app_server/tests.rs` pin the handshake deadline and header cap. Required: overload tests for connection count, message size, stalled writers, and subscription fanout.

## TM-DOS-003: Async process count, idle lifetime, and normal output are bounded

- Status: MITIGATED
- Severity: High
- Threat: Agent or control-plane callers can otherwise create unlimited live processes or retain completed process output indefinitely.
- Affected surface: `crates/verlet-process/src/live.rs`, `crates/verlet-kernel/src/capabilities/execution.rs`, `crates/verlet-kernel/src/agent/agent_process.rs`
- Mitigation: Existing: the manager defaults to 64 registered processes, cancels idle or expired running entries, removes ordinary idle terminal entries, bounds yield time, and clamps normal agent-process output to the configured default.
- Deterministic guard: `crates/verlet-process/src/live.rs` and `crates/verlet-kernel/src/capabilities/execution/tests.rs` pin process-limit, timeout, cancellation, and output-cap behavior.

## TM-DOS-004: Control RPC can disable command time and output bounds

- Status: OPEN
- Severity: High
- Threat: `command/exec` accepts `disableTimeout` and `disableOutputCap`. The non-streaming path collects complete child output before applying any presentation cap, so a reachable control-plane caller can allocate unbounded memory or hold a process indefinitely.
- Affected surface: `crates/verlet-kernel/src/adapters/app_server/connection.rs`, `crates/verlet-process/src/live.rs`
- Mitigation: Required: remove unbounded remote choices, replace them with operator-configured hard ceilings, stream through capped collectors, and reserve any unlimited local mode for a separate explicit command.
- Deterministic guard: None. Required: RPC tests that reject disabled bounds and process tests proving hard ceilings cannot be exceeded.
