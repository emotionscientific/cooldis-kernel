---
name: cooldis-tool-maker
description: Create, inspect, build, or publish a Cooldis tool package — a versioned black-box tool contract over the Wasm operation ABI, described by cooldis.tool.toml and proven by fixtures.
---

# Cooldis Tool Maker

Use this skill when an agent needs to create, inspect, build, or publish a
Cooldis tool package.

The goal is a versioned black-box tool contract, not framework glue. Cooldis
does not care how the source is written once the package declares what it needs,
what it accepts, what it returns, and which surfaces it exposes.

## Mental Model

Build a deterministic utility, wrap it in the Cooldis operation ABI, then
describe the accepted interface in `cooldis.tool.toml`.

```text
source or artifact
-> Cooldis Wasm operation ABI
-> cooldis.tool.toml package manifest
-> tool build receipt
-> fixture proof
-> published artifact + accepted interface contract
-> agent manifest references op://<name>/<operation>@sha256:<hash>
```

Prefer small tools with stable schemas. Keep the model-facing agent loop outside
the tool.

When a cookbook exists, start there before inventing a new shape. The first
serious recipe is `docs/cookbook-pdf-document-tool.md`: it shows how to turn a
real document-extraction problem into operations, command contracts, fixtures,
resources, grants, and benchmark receipts.

For command surfaces, follow `docs/command-contracts.md`: stdout is
compositional data, stderr is diagnostics/events, exit status must support bash
control flow, and every env/file/network/secret/effect requirement must be
declared instead of relying on ambient host authority.

## Authoring Steps

1. Pick one package identity, such as `data`, `employee`, or `pdf`.
2. Pick one or more operation names, such as `csv_profile` or `lookup`.
3. Create a Rust Wasm operation crate or adapt one of the repo fixtures.
4. Write JSON input and output schemas under `schemas/`.
5. Write at least one fixture input and expected output under `fixtures/`.
6. Declare runtime source in `cooldis.tool.toml`:
   - `runtime.kind = "wasm32-unknown-unknown"`
   - `runtime.module_path = "..."` for a Rust crate directory or `Cargo.toml`
   - `runtime.release = true` is the default; set `false` only for debug builds
   - `runtime.state = "stateless"` for V0 packages
7. Declare every required capability in the operation contract. Do not rely on
   ambient host access.
8. Declare surfaces:
   - `[operations.command]` for a Unix-shaped CLI surface
   - `[operations.mcp]` for MCP/import compatibility
9. Run `cooldis tool build --package cooldis.tool.toml`.
10. Fix schemas, capabilities, ABI exports, and fixtures until the build passes.
11. Publish with `cooldis tool publish --package cooldis.tool.toml`.
12. Reference the published tool from an agent manifest with
    `operation_ref = "op://<name>/<operation>@sha256:<hash>"`.

When working inside this source checkout before a release binary is installed,
use:

```sh
cargo run --locked --bin cooldis -- tool build --package cooldis.tool.toml
cargo run --locked --bin cooldis -- tool publish --package cooldis.tool.toml
```

When `cooldis` is installed on `PATH`, use:

```sh
cooldis tool build --package cooldis.tool.toml
cooldis tool publish --package cooldis.tool.toml
```

## Minimal Rust Tool Crate

V0 Rust tools compile to `wasm32-unknown-unknown` and expose the Cooldis
operation ABI. The minimal crate shape is:

```toml
[package]
name = "cooldis-wasm-my-tool"
version = "0.1.0"
edition = "2024"
publish = false

[workspace]

[lib]
crate-type = ["cdylib"]

[dependencies]
cooldis-guest-sdk = { path = "../path/to/cooldis-guest-sdk" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

[profile.release]
panic = "abort"
```

The Rust source must export:

- `__cooldis_describe_module__`, which writes an `OperationManifest`.
- `__cooldis_call_operation__`, which routes an operation id to the function
  implementation.

Start from these repo fixtures instead of guessing the ABI:

- Pure JSON utility:
  `crates/cooldis-kernel/tests/fixtures/wasm-csv-profile/`
- Internal HTTP utility using host imports:
  `crates/cooldis-kernel/tests/fixtures/wasm-employee-lookup/`

For the HTTP pattern, use `cooldis_guest_sdk::http_request` with an
`Invocation`, exact capability grants in `cooldis.tool.toml`, and no ambient
network access.

## Manifest Shape

```toml
kind = "cooldis.tool"
schema_version = 0

[identity]
name = "data"
version = "0.1.0"
description = "Profile tabular text."

[runtime]
kind = "wasm32-unknown-unknown"
module_path = "./wasm-tool"
state = "stateless"

[[operations]]
name = "csv_profile"
description = "Profile a CSV string."
input_schema = "schemas/csv_profile.input.json"
output_schema = "schemas/csv_profile.output.json"
required_capabilities = []

[operations.command]
name = "data profile"
stdin = "none"
stdout = "json"

[operations.mcp]
tool_name = "data_csv_profile"

[[fixtures]]
name = "basic"
operation = "csv_profile"
input = "fixtures/basic.input.json"
expect = "fixtures/basic.expect.json"
```

## Capability Rules

Declare capabilities as requests. At build/publish Cooldis reconciles them
against the Wasm manifest — every capability the module declares must appear in
the package's `required_capabilities`, and a capability an operation requires but
never declares is rejected with a teaching error. The accepted contract is what
agent rows later grant against. Note the current bound: capability *names* are
not checked against a closed family allowlist, so a typo (`net.htttp`) publishes
and only surfaces as a missing-grant or no-op at run — spell them exactly.

Examples:

```toml
required_capabilities = [
  "net.http.private",
  "net.http.private:GET:http://127.0.0.1:8123"
]
```

For V0, prefer:

- no capabilities for pure compute utilities;
- exact network grants for local/internal HTTP fixtures;
- secret references only through the Cooldis secret system;
- no WASI or ambient filesystem assumptions unless the ABI surface explicitly
  supports them.

## Agent Manifest Reference

A published tool is referenced from an agent manifest by a content-addressed
`op://` ref — never `tool://`. Pick the row type by how the agent should see
the operation:

```toml
# structured, model-visible tool
[[tools]]
type = "direct_tool"
id = "csv_profile"
tool_name = "csv_profile"
operation_ref = "op://data/csv_profile@sha256:<hash>"
grants = []
```

```toml
# the same operation as a virtual-bash command
[[tools]]
type = "bash_tool"
id = "csv_profile"
command = "data profile"
operation_ref = "op://data/csv_profile@sha256:<hash>"
grants = []
```

The `tools` entry is a reference to a published package, not a copy of the
tool. The package record owns the accepted interface; the agent record owns the
selected operation reference and the grants that ride on that row (there is no
manifest-global grant pool).

`op://` refs come in two forms: `op://<record>@sha256:<hash>` binds the whole
record (grants must cover every operation it declares);
`op://<record>/<operation>@sha256:<hash>` selects one operation. Read the
`<hash>` from the operation registry — the `active_artifact_hash` field in
`records/<name>.json` under the operations registry root, the console's
Registry view, or `cooldis man <record>` — never invent it.

## Validation Standard

A package is not done until all of these are true:

- `cooldis tool build --package cooldis.tool.toml` passes.
- Every declared fixture ran through the actual Wasm artifact.
- The receipt prints source hash, interface hash, operations, capabilities,
  command surface, MCP surface, and fixture status.
- `cooldis tool publish --package cooldis.tool.toml` writes a registry record.
- The registry record stores `interface`.
- An agent manifest can reference the tool with `op://<name>/<operation>@sha256:<hash>`.

The fixture runner is the current proof mechanism: every declared fixture is
executed against the actual Wasm artifact at build *and* publish (publishing
through the `--package` path re-runs the build, fixtures included), and a
mismatch fails the publish. Outputs are compared semantically when both sides
are valid JSON. Schemas are parsed and stored for interpretability but are not
yet deep-validated against the artifact's reported interface — full JSON Schema
validation is a later package-validator pass. A package that declares **no**
fixtures publishes with no proof; always ship at least one fixture per
operation.

## Good V0 Examples

- `data.csv_profile`: pure utility, JSON input/output, no credentials.
- `employee.lookup`: internal HTTP call with exact local network grant.
- `pdf.extract_text`: later utility for document ingestion. Prefer a deterministic
  text extraction crate before attempting OCR.

## Avoid

- Mixing agent behavior into tools.
- Inferring secret/network/filesystem powers from source code.
- Publishing a package without fixtures.
- Changing declared command or MCP names during validation. Reject invalid
  declarations instead.
- Treating MCP as the canonical contract. MCP is a surface/import shape; the
  Cooldis tool interface is the durable contract.
