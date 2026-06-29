# Getting Started

Cooldis is early, so the first successful path is local and explicit.

## Install And Verify

Clone the repository and run the runtime checks:

```sh
cargo test --workspace --all-targets --locked
cargo run --locked --bin cooldis-live-smoke
cargo run --locked --bin cooldis-vbash-smoke
cargo run --locked --bin cooldis-wasm-smoke
```

## Explore The CLI

The current public CLI surfaces are:

```sh
cooldis --help
cooldis agent --help
cooldis tool --help
cooldis rpc --help
cooldis daemon --help
cooldis dev chat --help
```

`cooldis agent plan`, `publish`, `list`, and `show` are the first manifest
authoring slice. `cooldis tool build`, `publish`, and `run` are the local
operation path.

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
