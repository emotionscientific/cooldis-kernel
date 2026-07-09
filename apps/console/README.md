# Cooldis Console

Bundled Svelte console for the Cooldis kernel. This is a local browser UI served
by `cooldis console`, not a desktop app or public DMG release path.

## Development

Build and check the UI from this directory:

```sh
bun install
bun run check
bun run build
```

Run the kernel command against the built assets:

```sh
cargo run --bin cooldis -- console --no-open --port 0
```

For Vite-only UI work:

```sh
bun run dev
```

The production console derives its default WebSocket endpoint from
`window.location` and the session token injected into `index.html` by the
kernel app-server.
