# Rust Wasm Operation Dev Kit

Status: local-first design sketch.

Verlet needs an authoring lane where an agent can turn an idea into a
deterministic operation without mutating the runtime that will execute it.

The dev kit is that lane:

```text
agent idea
-> scaffold Rust operation
-> declare tool package manifest, schemas, fixtures, projections, and capabilities
-> build wasm32-unknown-unknown
-> validate Verlet ABI
-> run fixtures against the actual artifact
-> publish to registry
-> attach to an agent
-> next turn sees tool / CLI / HTTP / MCP projection
```

The important boundary: the dev kit is not the runtime. It produces an immutable
Wasm artifact plus a manifest. Verlet validates, publishes, attaches, and
runs the artifact under the normal operation contract.

## SpacetimeDB Reference

SpacetimeDB is the closest working precedent for the publish UX we want to copy,
with a different execution domain.

Their user-facing path is:

```text
spacetime start
-> spacetime publish --module-path <module> <database>
-> CLI builds the module when needed
-> CLI optimizes/uploads the compiled program
-> server checks migrations/breaking changes
-> server creates or updates the database module
-> describe/call/sql immediately target the loaded module
```

I tested this against the local SpacetimeDB checkout and installed CLI 2.2.0:

```text
spacetime start --listen-addr 127.0.0.1:3011 --data-dir /tmp/... --in-memory
spacetime publish --server http://127.0.0.1:3011 --module-path templates/basic-rs/spacetimedb verlet-auth-publish-probe
spacetime describe --server http://127.0.0.1:3011 --json verlet-auth-publish-probe
spacetime call --server http://127.0.0.1:3011 verlet-auth-publish-probe add '"Ada"'
spacetime sql --server http://127.0.0.1:3011 verlet-auth-publish-probe 'select * from person'
```

The important felt shape:

- `publish` is the delightful command. It hides build/upload/update plumbing but
  still prints the server handoff.
- `--bin-path` and `--js-path` are escape hatches for prebuilt artifacts; source
  publish remains the happy path.
- Republish is an update, not a separate command. The server checks breaking
  changes and returns a migration plan before applying the new module.
- Durable publisher identity matters. Anonymous first publish can create a
  throwaway local database, but anonymous republish failed because the update
  came from a different unauthenticated identity. A server-issued local login
  made create and update work predictably.
- The proof after publish is immediate runtime visibility: `describe` showed the
  table/reducer schema, `call` executed a reducer, and `sql` returned the row
  created by the reducer.

The source path mirrors the UX:

```text
crates/cli/src/subcommands/publish.rs
  build source or read --bin-path/--js-path
  run pre-publish migration checks
  PUT /v1/database/:name_or_identity

crates/client-api/src/routes/database.rs
  authorize create/update
  derive migration policy
  publish_database(DatabaseDef { program_bytes, host_type, ... })

ControlStateDelegate::publish_database
  create new database or update according to module lifecycle conventions
```

For Verlet, the lesson is not "be a database." The lesson is: one publish
command should take source or an artifact, validate the contract, store immutable
bytes, update a durable record, and make the new callable surface visible without
restarting the whole host.

## Product Shape

```text
@verlet/wasm-kit       npm/npx CLI wrapper
verlet-guest-sdk       Rust guest crate and ABI helpers
verlet-wasm-operation  agent skill / authoring instructions
verlet tool publish      canonical runtime publish path
```

The npm package exists for reach. It should be pleasant for TypeScript-heavy
agent builders, but it must not become the authority layer. All runtime authority
stays in Verlet.

## Local V0

Local V0 should work before any package registry, marketplace, remote compiler,
or signature system exists.

The repo-native path today is:

```bash
cargo run --locked --bin verlet -- tool build --package verlet.tool.toml
cargo run --locked --bin verlet -- tool publish --package verlet.tool.toml
```

Once the wrapper exists, the same flow can become:

```bash
npx @verlet/wasm-kit init data-csv-profile
cd data-csv-profile

npx @verlet/wasm-kit plan

npx @verlet/wasm-kit publish --name data --scope thread
```

The CLI can initially be a thin wrapper around checked-in Rust templates and the
existing Verlet commands:

```text
init      copy template, patch package/name/operation ids
plan      verlet tool build --package verlet.tool.toml
build     cargo build --message-format=json --release --target wasm32-unknown-unknown
validate  WasmRuntimeFactory::validate_operation_artifact
publish   verlet tool publish --package verlet.tool.toml
run       verlet tool run ...
```

The repo-native package manifest is `verlet.tool.toml`. It is the V0 version of
the future marketplace package declaration:

```text
ToolPackageManifest   reads verlet.tool.toml
ToolInterfaceContract accepted black-box interface after validation
ToolBuildReceipt       dry-run receipt with source, runtime, operations, schemas,
                      fixtures, capabilities, and projections
Fixture runner        executes declared fixtures against the real Wasm artifact
Publish persistence   stores the accepted interface beside artifact/projections
Agent manifest        references published operations with pinned op:// refs
```

Raw `--module-path` and `--bin-path` remain useful for low-level conversion and
debugging. Package build/publish is the authoring happy path.

### Model-facing operation surfaces

An operation whose ABI input is a host envelope can derive a narrower model
surface from that same schema:

```toml
[operations.surface]
args_field = "args"
bound = ["root"]
```

The input schema remains the one authored ABI contract. Publish validation
requires its top-level properties to be exactly `args_field` plus the bound
parameters, all required, with `additionalProperties = false`. The
`args_field` subschema must also be a closed object and cannot expose a bound
parameter name. Direct model rows receive that subschema; attach-time
configuration supplies every bound parameter, and the router validates both
the model arguments and the assembled envelope. Bind and replay validate the
bound values against their property schemas. Surface operations are withheld
from the model's raw virtual-bash command projection. CLI invocation keeps the
full envelope because the caller is the host in that lane.

## Importing An Existing REST API Instead

When the desired tool is an existing REST API mirror, use a witnessed OpenAPI
package instead of authoring a Rust guest:

```bash
verlet import build --package verlet.import.toml
verlet import publish --package verlet.import.toml \
  --registry-root .verlet/operations
```

The import package pins a vendored JSON spec by sha256 and selects its operations
into one ordinary multi-operation Wasm record. The generated artifact calls the
same `cooldis_0.1.http_request` host import and passes the same publish gate as a
hand-authored tool. See [OpenAPI To ABI Operation Adapter](openapi-adapter.md)
for the supported subset, auth mapping, and response contract.

The first checked-in templates are test fixtures rather than a generated
scaffold:

- `crates/verlet-kernel/tests/fixtures/wasm-csv-profile/` for a pure JSON
  operation with no capabilities;
- `crates/verlet-kernel/tests/fixtures/wasm-employee-lookup/` for a host-import
  HTTP operation with exact package network capabilities.

V0 stores JSON schemas in the accepted interface contract and uses fixtures as
the executable proof. Full JSON Schema validation belongs to the next package
validator pass.

## Kits

A kit is a directory that groups one or more `verlet.tool.toml` packages and
declares the model-facing tools they provide together. Its manifest is
`verlet.kit.toml`:

```toml
kind = "verlet.kit"
schema_version = 0
packages = ["tools/file-read", "tools/file-write"]

[identity]
name = "filesystem"
version = "0.1.0"
description = "Read and write files through explicit VFS capabilities."

[[tools]]
tool_name = "file_read"
package = "file-read"
operation = "file_read"
effect_class = "idempotent"

[[tools]]
tool_name = "file_write"
package = "file-write"
operation = "file_write"
effect_class = "at-most-once"
```

Each `packages` entry is a relative directory confined to the kit root. The
`package` value in a tool row names that member package's `identity.name`, not
its directory. `operation` names an operation declared by the member package.
`tool_name` values must be unique within the kit. `effect_class` accepts
`pure`, `idempotent`, or `at-most-once` and defaults to `at-most-once`.

Install a local kit with:

```bash
verlet kit install . \
  --registry-root .verlet/operations \
  --kits-root .verlet/kits
```

Install first validates the complete kit. It then builds every member through
the same package proof gate as `verlet tool publish --package`, publishes each
artifact to the operation registry, resolves every tool row to a pinned
`op://<package>/<operation>@sha256:<artifact-hash>` reference, and writes one
installed-kit record. The record is written only after all members publish.
If a later member fails, earlier content-addressed publishes remain harmless
and unreferenced. Reinstall overwrites the installed-kit record.

Operation records are keyed by member package `identity.name` across the whole
registry, not per kit. If two kits each ship a member named `file-read`, the
second install moves that record's active pointer to its own artifact. Both
kits stay bindable because their installed-kit records pin exact
`@sha256` artifact hashes, which resolve against the record's version history
rather than the active pointer. The visible effect is only on unpinned lookups
of the shared name. Give member packages distinct `identity.name` values when
publishing kits for others to install.

Installed records are JSON files at `.verlet/kits/<kit-name>.json` by default.
`verlet kit list` reads those records, and `verlet kit remove <kit-name>`
deletes only the selected record. Published operations remain in the registry.
The daemon reads installed records when it synthesizes the default manifest at
startup. After `verlet kit install`, restart the daemon to make each installed
tool available as a pinned `direct_tool` row. After `verlet kit remove`, restart
the daemon again and the removed kit's tools are gone from the default
manifest. Tool rows are sorted by kit name and tool name, and duplicate model
tool names across kits fail daemon startup instead of shadowing one another.

The chat TUI wraps the same pipeline: the `/setup` window's "Install tools"
step (and its one-shot first-run offer) runs `verlet kit install` for the
recommended pi kit from a local kit directory. See
[Chat](chat.md).

### Pi File Tools Kit

`agent-tools/pi-kit/` is the first checked-in kit. It packages the four Wasm
modules under `agent-tools/wasm/` as five model tools:

| Tool | Package | Operation | Effect class | Capability |
| --- | --- | --- | --- | --- |
| `read` | `pi-read` | `read` | `idempotent` | none |
| `write` | `pi-write` | `write` | `at-most-once` | `fs.write` |
| `edit` | `pi-edit` | `edit` | `at-most-once` | `fs.write` |
| `find` | `pi-search` | `find` | `idempotent` | none |
| `grep` | `pi-search` | `grep` | `idempotent` | none |

Install the source kit from the repository with:

```bash
verlet kit install agent-tools/pi-kit
```

Every operation has a package fixture. Read also has a missing-file fixture,
and edit has an ambiguous-match fixture. Package fixture data is mounted at
`/fixtures` read-only. The write fixture mutates the fresh in-memory VFS. The
successful edit fixture can prove its unique match only up to the read-only
write boundary, where it expects `access denied`; the CLI integration test then
invokes the installed pinned `pi-edit` artifact against a writable mount and
proves the actual replacement.

Build a standalone copy with prebuilt Wasm blobs when the installer must not
have a Rust toolchain:

```bash
scripts/build-pi-kit-dist.sh
verlet kit install dist/pi-kit
```

The script builds all four modules through the managed Cargo lane, copies the
kit to `dist/pi-kit/`, and rewrites each member runtime from `module_path` to a
member-local `bin_path`. The `dist/` tree is gitignored. The publication home
for this distributable remains an anchor decision outside this repository.

Package-declared MCP names are stored in the published operation projections.
This lets the Pi package records retain registry-wide identities such as
`pi-read` while projecting the fixed MCP name `read`.

## Agent Skill Shape

The agent-facing skill should teach a model how to author a narrow deterministic
computer safely:

```text
1. Pick one operation name and one stable input/output schema.
2. Declare all required capabilities in the manifest.
3. Prefer JSON input/output for structured tools.
4. Do not import WASI or ambient host APIs.
5. Use verlet-guest-sdk host wrappers only.
6. Build and validate before publishing.
7. Publish, attach narrowly, then prove the new projection by invoking it.
```

The skill can call the npm wrapper, but the wrapper must still end at
`verlet tool publish` or the app-server equivalent.

The canonical publishable skill source lives under
`skills/verlet-tool-maker/`. The `.agents/skills/verlet-tool-maker` path is a
repo-dev projection so coding agents working inside this checkout can use the
same instructions. End-user distribution should come later through the npx
wrapper, the release binary, or a quick-start/template init helper that installs
the skill into the user's dev environment.

## Custom Coupling Guests

Custom coupling guests are normal Verlet Wasm operations with a narrower
contract:

```text
input:  cooldis.coupling.invocation/0.1
output: cooldis.coupling.discharge/0.1
```

The invocation JSON contains the trigger event, the selected source events,
manifest config, and invocation metadata (`coupling_id`, `thread_id`, `depth`).
The discharge JSON contains proposed events only:

```json
{
  "abi": "cooldis.coupling.discharge/0.1",
  "events": [
    {
      "stream": "derived:counter",
      "kind": "placement.decision",
      "payload": { "count": 3 }
    }
  ]
}
```

The kernel, not the guest, stamps origin and provenance and rejects undeclared
sink streams/kinds without partial appends. Coupling guests run as pure compute:
no HTTP, VFS, secrets, or other effectful imports are available. Put effectful
work behind tools and let couplings fold event streams into deterministic
derived events.

The supported Rust authoring surface is the SDK macro contract from ADR 0002:
write a plain typed function and let the macro own exports, operation dispatch,
JSON envelope encoding, and ABI status mapping.

```rust
use verlet_guest_sdk::prelude::*;
use serde_json::json;

#[derive(Deserialize)]
struct CounterConfig {
    #[serde(default = "default_every")]
    every: u64,
}

#[coupling]
pub fn fold_counter(ctx: CouplingContext) -> Result<Discharge, GuestError> {
    let config: CounterConfig = ctx.config()?;
    let count = ctx.sources().len() as u64;
    if count == 0 || count % config.every.max(1) != 0 {
        return Ok(Discharge::empty());
    }
    Discharge::empty().event(
        "derived:counter",
        "placement.decision",
        json!({
            "count": count,
            "trigger_event_id": ctx.trigger().id.clone(),
        }),
    )
}

fn default_every() -> u64 {
    3
}
```

`#[coupling]` wraps `fn(CouplingContext) -> Result<Discharge, GuestError>` and
exports one JSON operation with the Rust function name. `#[operation]` wraps a
pure `fn(Input) -> Result<Output, GuestError>`; operations that need host powers
take `&mut OperationContext` as their first argument. Guest input and output
types should derive `serde::Deserialize` and `serde::Serialize`.

Native tests should exercise the plain Rust function before building Wasm:

```rust
#[test]
fn fixture_runs_natively() {
    let invocation =
        verlet_guest_sdk::testkit::invocation_from_fixture_file("fixtures/invocation.json")?;
    let discharge = verlet_guest_sdk::testkit::invoke_coupling(fold_counter, invocation)?;
    assert_eq!(discharge.events.len(), 1);
}
```

`verlet coupling init <name>` scaffolds the macro-authored crate,
`verlet.tool.toml`, schemas, fixture JSON, and one native testkit test. The
same package then validates through the existing publish oracle:

```bash
verlet coupling init counter-coupling
cd counter-coupling
cargo test --locked
cargo build --locked --release --target wasm32-unknown-unknown
verlet tool build --package verlet.tool.toml
```

The minimal checked-in Rust example is `examples/wasm-counter-coupling`. It
emits one derived event every `config.every` matching source events without
hand-writing the raw operation exports.

### Coupling Replay Dev Loop

Use replay while iterating on a custom coupling. It runs the same scheduler
trigger, selector, quota, budget, and Wasm execution path against recorded
thread events, but it prints proposals only and does not append to the source
journal.

```bash
cargo build --release \
  --target wasm32-unknown-unknown \
  --manifest-path examples/wasm-counter-coupling/Cargo.toml

cargo run --locked --bin verlet -- coupling run --replay \
  --artifact examples/wasm-counter-coupling/target/wasm32-unknown-unknown/release/verlet_example_wasm_counter_coupling.wasm \
  --coupling-file ./counter.bound-coupling.json \
  --thread-id 018f9fe0-35a7-7a80-8f65-12e7e0b20b52 \
  --journal .verlet/state/session_history.sqlite3 \
  --json
```

The coupling file is the bound kernel contract (`BoundCoupling` or
`BoundCouplingSet`) produced by manifest binding. For a published artifact, pass
the pinned operation ref instead:

```bash
cargo run --locked --bin verlet -- coupling run --replay \
  --artifact op://counter/fold_counter@sha256:<hash> \
  --registry-root .verlet/operations \
  --coupling-file ./counter.bound-coupling.json \
  --thread-id <thread-id> \
  --journal .verlet/state/session_history.sqlite3
```

Replay output is marked dry-run. JSON output uses `proposalEvents` for proposed
sink discharges and `runs[].blocked` for quota or budget blocks, so tests can
assert both emitted events and would-have-been-blocked firings without scraping
human text.

## First No-Key Example: `data.csv_profile`

Use a CSV profiler instead of a network search wrapper. It is useful, exact, and
does not require credentials.

```text
published name: data
operation:      csv_profile
tool:           data_csv_profile

input:
  {
    "csv": "name,score\nAda,10\nLinus,8\n",
    "has_header": true
  }

output:
  {
    "rows": 2,
    "columns": [
      {"name":"name","non_empty":2,"empty":0,"numeric_count":0},
      {"name":"score","non_empty":2,"empty":0,"numeric_count":2,"min":8.0,"max":10.0,"mean":9.0}
    ]
  }
```

This gives the model a natural reason to call the tool:

```text
Here is a CSV. Which column looks risky and why?
```

The model can judge. Rust counts.

## Local Proof

The first repo-native proof should not depend on real network or credentials:

```text
authored source fixture
-> build Rust Wasm
-> publish as data
-> load LocalPluginCatalog
-> provider request includes data_csv_profile
-> scripted provider calls data_csv_profile
-> operation returns exact JSON profile
-> scripted provider answers from the profile
-> record/blob/hash/projection evidence exists on disk
```

That proves the local build/publish/projection loop. A provider-backed lane can
then prove the same operation is visible to a real model:

```text
spawn authoring subagent
-> subagent writes the Rust operation from template
-> build/validate/publish
-> attach to the provider-backed thread
-> model provider sees data_csv_profile and calls it
-> restart app-server
-> resume thread and prove the operation remains visible
```

## Acceptance Criteria

- The operation starts absent and appears only after publish/load/bind.
- Publish validates the manifest before the operation is visible.
- Provider-visible tools include `data_csv_profile` after registry load.
- Invocation returns deterministic JSON evidence that the model could not
  reliably invent.
- Registry records include an immutable artifact hash and a stable projection.
- Restart/reload keeps the operation available from persisted registry state.

## Later Package Shape

The npm package can grow without changing the runtime contract:

```text
@verlet/wasm-kit
  init <template>
  build
  validate
  publish --registry-root ... --scope ...
  pack-skill
  doctor
```

Templates should be boring and auditable:

- `json-transform`
- `csv-profile`
- `markdown-link-audit`
- `json-shape-diff`
- `vfs-file-helper`

The package should print the exact `verlet tool ...` command it ran so humans and
agents can inspect the authority transition.

## Later Push-Triggered Publish

Manual publish is the canonical V0 path:

```sh
verlet tool publish --package verlet.tool.toml
```

Teams can automate that today by running the same command from CI after their
own tests pass. A push-triggered Verlet update lane is a later automation layer
on top of the same package contract, not a different publish primitive.

That later lane needs trusted builder identity, source provenance, compatibility
checks, scoped promotion policy, rollback semantics, audit receipts, and optional
human approval before flipping active bindings.

## Encoding Trajectory

The dev kit authors against the SDK contract (typed functions plus
`verlet-guest-sdk`), never against the raw JSON-over-linear-memory encoding.
The WebAssembly component model (WIT) is the planned successor encoding;
because the SDK macro layer owns the wire format, that migration is a
recompile for guest authors, not a rewrite. Decision, rationale, and
migration triggers live in
[ADR 0002](adr/0002-guest-encoding-v1-component-model-later.md).

## Non-Goals

- No arbitrary in-process TypeScript extension execution.
- No hidden publish from prompt text alone.
- No advisory skill treated as enforcement policy.
- No remote compiler service in V0.
- No marketplace trust/signature model in V0.
- No guest source hand-writing raw ABI exports once the SDK macro layer
  lands; the SDK contract is the only supported authoring surface.
