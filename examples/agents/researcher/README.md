# Researcher — example agent manifest

The first real agent manifest in the repository: a small research agent
composed from the standard operation set (`docs/standard-operations.md`),
declared entirely as registry content. It exists to prove the full path —
publish operations, publish a manifest that pins them, start a thread bound to
that manifest — and to be the second entry (after the kernel's default
manifest) in any agent picker.

## What it declares

- **Model profile** `default` over `provider://local`,
  `model://local/default` — replace these with a configured provider catalog
  entry when running against a live model.
- **Tools** (bash surface, content-addressed pins into the operation
  registry):
  - `http_fetch` — GET a URL; origin-gated by the `net.http:GET:<origin>`
    grants declared on the tool row. The example grants public HTTP(S) origins
    with wildcards, while private/loopback destinations still require
    `net.http.private`.
  - `file_read` — bounded reads from the thread's virtual filesystem.
  - `json_query` — RFC 6901 pointer extraction from a JSON document.
- **Runtime** defaults: thread cwd `.`, streaming on, `default_cwd` the only
  allowlisted runtime override.
- **Policies**: declared-origins network, VFS filesystem, no child agents.

A commented `protocol_tool_import` block shows how a live MCP universe would
join the manifest. It stays off in the committed example because it needs a
witnessed `mcp://` source record and any server credentials required by that
source.

## Publish

```sh
examples/agents/researcher/publish.sh
```

The manifest source is a template: operation refs are content-addressed, so
the script resolves each operation's active artifact hash from the local
operation registry (seeding the standard ops first if needed), renders
`researcher.verlet.agent.toml.in`, and publishes the result:

```sh
verlet agent list --registry-root .verlet/agents
verlet agent show researcher --registry-root .verlet/agents
```

## Start a thread from it

From the CLI:

```sh
verlet agent run agent://researcher@latest
```

From the console: the agent appears in Settings → Chat ("Default agent") and
in the chevron menu next to the sidebar "+" once the daemon serves this
registry root.
