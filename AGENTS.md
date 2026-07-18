# Cooldis Agent Notes

Cooldis is a standalone Rust runtime repo, split from the product app repo.

Keep this repo focused on runtime primitives:

- multi-tenant host and supervisor;
- thread lifecycle, subthread relationships, events, cancellation, resume, and
  shutdown;
- agent loop and provider adapters;
- virtual bash, VFS, sandbox, procedure/reducer, and provider adapter seams.
- ABI docs and contracts: host-facing ABI powers, guest-declared operations,
  and CLI/API/tool projections.

Keep product logic out:

- auth, billing, invites, dashboards, Telegram, Railway product deployment, and
  app-specific ledgers belong in the product repo.

Working rules:

- Use `cargo test` before claiming runtime changes are done.
- Prefer larger, real lifecycle tests over narrow mocks when the cost is low.
  If a provider-backed path can run in roughly five seconds or less, wire it to
  a generic OpenAI-compatible or Anthropic-compatible provider and exercise the
  actual runtime surface instead of stopping at a synthetic unit smoke.
- Do not commit `scratch/` or `target/`.
- Prefer small, explicit runtime contracts over product-shaped abstractions.
- Treat network as the first ABI system surface. POSIX/process scoping is a
  later design pass and should not be implied by network-only changes.
- For CLI, virtual bash, and operation/tool projections, follow the Unix-shaped
  command standard in `docs/command-contracts.md`: stdout is compositional data,
  stderr is diagnostics/events, exit status drives bash control flow, and
  ambient host authority must be declared explicitly.
- For Agent Experience (AX), keep `README.md`, `docs/index.md`, and
  `docs/public-api-coverage.md` current when changing public positioning,
  coding-agent workflows, CLI/API/tool projections, or man-page/help surfaces.
