# ADR 0005 — Turso as the embedded storage engine

Status: accepted (architect, 2026-07-11; wholesale adoption ratified by the
anchor the same day). Tracking issue: EMO-411. No new lexicon primitives:
`store` and `stream` keep their meanings; the engine behind a store is an
implementation choice below the naming layer.

## Context

Every persistent store in this workspace speaks SQLite through `rusqlite`
with the `bundled` feature: the C amalgamation compiled into the binary,
driven through a synchronous FFI. The store traits (`SessionStore`,
`EventStore`, `ObservationStore`, the metadata stores, `IngressQueueStore`)
are async, so each implementation holds an `Arc<Mutex<rusqlite::Connection>>`
and blocks inside async fns — a pattern tolerated, never chosen.

Turso (tursodatabase/turso, MIT) is a from-scratch Rust implementation of
SQLite: same on-disk file format, differential-tested against SQLite, with
an async-native driver, MVCC concurrent writes, change-data-capture, a
pluggable IO trait, and a client/server sync engine. Its compatibility
guarantees are explicit: databases move between engines freely, and any
undocumented behavioral deviation is a bug. A hands-on spike (2026-07-11,
results on EMO-409) verified the Rust driver, the sync round-trip against a
self-hosted server, and the pragma surface this repo depends on.

The verified inventory of our SQLite usage is small and boring: WAL journal
mode, `foreign_keys`, `busy_timeout`, `unchecked_transaction`,
`PRAGMA table_info` for migrations. No custom SQL functions, no incremental
blob API, no ATTACH, no savepoints. Everything on that list is supported by
Turso today.

## Decision

Adopt Turso as the embedded storage engine for all first-party stores,
behind a single engine-owner crate.

1. **One engine-owner crate: `cooldis-sqlite`.** A new crate owns the
   engine dependency, database configuration (journal mode, `foreign_keys`,
   busy timeout, read-only open), connection acquisition, the migration
   helpers (`table_info`-based column checks), and the DST IO hook. Store
   crates depend on `cooldis-sqlite` and never on an engine directly. The
   crate is named for the format, not the engine: if Turso had to be backed
   out, the crate's API stays and its internals return to rusqlite.
2. **Store traits do not move.** The swap happens entirely beneath
   `SessionStore`/`EventStore`/`ObservationStore` and the metadata store
   traits. Callers see the same async API they see today — except the
   implementations stop blocking.
3. **Depth of the first migration wave: engine swap + async-native
   implementations, together.** Rewriting a store's call sites is the same
   edit whether or not the mutex survives, so shipping (a) engine swap and
   (b) mutex deletion separately would touch every line twice. MVCC journal
   mode is NOT part of the first wave: WAL remains the default everywhere,
   and any store that wants `journal_mode = 'mvcc'` must justify it
   per-store later (MVCC is a Turso extension; a file in that mode is no
   longer freely openable by stock SQLite until checkpointed back, which
   weakens the rollback story for that file).
4. **DST IO hook is part of the seam, not an afterthought.**
   `cooldis-sqlite` exposes an IO-override so the scenario engine can drive
   the storage engine through simulated, fault-injectable, deterministic IO
   (`turso::Builder::with_io_impl`). Today's fault injection wraps store
   trait methods (`FaultingRuntimeStore`); this hook reaches below them into
   the engine's reads, writes, and syncs. The first seeded IO-fault scenario
   is specced as a follow-up ticket, wave-4 candidate for the DST track.
5. **One engine per database file, per process set.** Turso's compatibility
   guarantee excludes mixed SQLite/Turso access to the same file across
   processes. Rule: a database file is owned by exactly one engine at a
   time. Operational consequence: do not open a live daemon's database with
   the `sqlite3` CLI or any rusqlite-linked build while the daemon runs;
   post-migration debugging goes through `tursodb` or through daemon RPC.
   The rule is documented here and in the crate docs; it cannot be enforced
   mechanically across processes. Wave-3 finding (2026-07-11): Turso in
   fact enforces something stronger by default — the local open takes an
   exclusive per-process file lock, so a SECOND PROCESS is refused
   entirely while the first holds the file (cross-process shared-WAL
   coordination exists upstream behind a `host_shared_wal` build gate
   only). Consequence: first-party CLI flows that directly open a live
   daemon's database from another process now fail with a lock error
   where WAL rusqlite allowed a concurrent reader; the audit and
   RPC-rerouting of those flows is a follow-up ticket.
6. **Version policy: exact pin plus a standing fork as the patch lane**
   (anchor-ratified 2026-07-11). Default posture: `turso = "=0.6.1"` from
   crates.io (latest stable at decision time; the 0.7 pre-releases carry
   the sync work EMO-409 needs and will be evaluated there). Alongside the
   pin, upstream is forked into our org, parked at the release tag, and
   costs nothing day to day. When an engine bug needs fixing faster than
   upstream review, the fix lands on the fork and a `[patch.crates-io]`
   override points at it — one manifest stanza, removed again when upstream
   ships. Copying the source in-tree was considered and rejected: Turso is
   a multi-crate workspace, and a cut-paste fork owns all drift with none
   of the history. `cargo vendor` (all-deps hermetic builds) is a separate
   repo-wide policy question, deferred. Upgrades are deliberate, one
   commit, run through the full test suite plus the DST scenario corpus.
7. **Out of scope here.** pgqrs keeps its internal rusqlite; it is
   superseded later by the store-hosted ingress queue in EMO-409's design.
   The sync engine, remote EventStore backend, and self-hosted sync
   endpoint are EMO-409's to specify. This ADR is the local-engine
   decision only.
8. **The ingress IO-state enclave stays on rusqlite until pgqrs is
   replaced** (architect decision at the EMO-414 stop gate, 2026-07-11).
   Discovery during wave 3: the daemon egress-state/ownership tables
   (`daemon_io`), the `cooldis-io-pgqrs` dedupe schema, and pgqrs's own
   queue tables all share one database file (the ingress queue DSN;
   `effective_queue_dsn()` feeds all three). Under rule 5 that file is
   owned by rusqlite, so those two first-party connections cannot migrate
   independently. Splitting the schemas into a separate Turso file was
   considered and rejected: the `BEGIN IMMEDIATE` ingress ownership lock
   serializes against tables in that same file, cross-table atomicity
   would have to be re-proven, and the whole enclave is scheduled to
   dissolve into the store-hosted ingress queue (EMO-409). Consequence:
   `rusqlite` remains a direct dependency of `cooldis-kernel` (daemon_io
   egress state) and `cooldis-io-pgqrs` (dedupe) for exactly this file,
   annotated in both manifests; wave 3 migrates only sites with
   engine-exclusive files.

## Migration plan

Wave order, one Codex ticket per store, one commit per ticket, each behind
the unchanged store traits with existing tests green:

1. `cooldis-sqlite` skeleton (architect; this ADR's companion commit).
2. Pathfinder: `cooldis-metadata` (provider + secret stores — real usage,
   simple schemas, low blast radius).
3. `cooldis-history-sqlite` (the stream store; the crown jewel — carries the
   fenced-append paths, gets the composed-diff review gate and a DST
   scenario run before merge).
4. Small sites bundle: `mcp_client` cache plus direct-connection test and
   smoke helpers. (`daemon_io` egress state and the `cooldis-io-pgqrs`
   dedupe schema were originally in this wave; the stop-gate discovery in
   decision 8 keeps them on rusqlite until EMO-409 replaces pgqrs.)
5. DST seeded IO-fault scenario against the Turso-backed store.

Rollback per store is a dependency flip inside `cooldis-sqlite` or a revert
of the store's migration commit; data files need no conversion in either
direction (WAL-mode files are format-compatible both ways).

## Consequences

- The `Arc<Mutex<Connection>>`-inside-async pattern is deleted rather than
  documented.
- The binary loses its only C dependency; cross-compilation (static musl
  for sandbox packaging) drops the `cc` toolchain requirement.
- The storage engine becomes reachable by deterministic simulation — the
  first time fault plans can act below the store traits.
- We take a pre-1.0 engine into the durability path. Mitigations: exact
  version pin, format-compatible rollback, the narrowest usage profile
  (single-writer, append-heavy, no exotic SQL), and the DST corpus pointed
  at exactly this layer.
- EMO-409's remote EventStore work inherits an engine that already speaks
  the sync protocol, instead of bolting replication onto rusqlite.
