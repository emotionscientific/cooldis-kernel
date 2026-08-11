# Getting Started

Verlet is early, so the first successful path is local and explicit.

## Install And Verify

Clone the repository and run the runtime checks:

```sh
cargo test --workspace --all-targets --locked
cargo run --locked --bin verlet-vbash-smoke
cargo run --locked --bin verlet-wasm-smoke
```

## Explore The CLI

The current public CLI surfaces are:

```sh
verlet --help
verlet commands
verlet console --help
verlet chat --help
verlet agent --help
verlet blob --help
verlet tool --help
verlet auth --help
verlet rpc --help
verlet daemon --help
```

For the canonical command list and command grouping, see [Verlet CLI](cli.md).

`verlet chat` opens the local terminal console. On first run it has no model
provider yet and opens an in-TUI setup window to connect one (an API key, a
ChatGPT-plan login, or a custom endpoint); see
[Provider Setup](provider-setup.md).

`verlet agent plan`, `publish`, `list`, and `show` are the first manifest
authoring slice. `verlet blob publish` stores static prompt and context files
as immutable resources. `verlet tool build`, `publish`, and `run` are the
local operation path.

## First Useful Loop

The intended local loop is:

```text
write a manifest
-> plan it
-> publish it
-> bind tools and resources
-> run locally
-> inspect events, receipts, and artifacts
```

Some commands in that loop are still being built. The docs mark incomplete
surfaces as reserved or partial rather than pretending the platform is finished.

## Where To Go Next

- New users: [Declarative Agents](concepts/declarative-agents.md)
- Platform buyers: [Permissions And Governance](concepts/permissions-and-governance.md)
- Runtime readers: [Runtime Primitives](developers/runtime-primitives.md)
- API and docs reviewers: [Public API Surface](developers/public-api-surface.md)
