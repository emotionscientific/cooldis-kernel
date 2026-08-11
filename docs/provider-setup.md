# Provider Setup

Verlet needs a model provider before `verlet chat` can run real turns. This
page covers the in-TUI setup window, environment-variable auth, custom
OpenAI-compatible and Anthropic-compatible providers, the OpenAI Codex
ChatGPT-plan login, and where credentials live.

## First Run

A fresh installation has no configured providers. `verlet chat` detects this
at startup and opens the setup window on the provider catalog automatically.
Pressing Esc skips setup; the footer then shows a reminder until a provider is
configured or a model is selected.

## The Setup Window

`/setup` (alias `/providers`) opens a modal window over the chat. Its home
screen lists the configured providers with their auth source, model count,
base URL for custom entries, and a marker on the provider that owns the active
model. Two actions sit under the list:

- `Connect a provider` opens a searchable list over the full provider catalog
  (about 160 providers derived from [models.dev](https://models.dev), plus
  `openai-codex`). Typing filters the list; substring matches rank before
  fuzzy matches.
- `Add custom provider` opens the custom provider form described below.

Selecting a configured provider opens a menu: pick a model from that provider,
replace or clear its credential, and, for custom providers, edit or delete the
record.

Esc backs out one screen at a time and closes the window from the home screen.
While the window is open it owns all keyboard input.

## Connecting A Catalog Provider

Select a provider from the catalog list. API-key providers go straight to a
masked key prompt; the key is never rendered and never appears in notices or
error text. The prompt names the provider's environment variable (for example
`ANTHROPIC_API_KEY`) as the alternative: a key present in the app-server's
environment satisfies auth without storing anything.

Saving a key creates the provider's store record on demand from the catalog
(base URL, API family, and model list included), so there is nothing to
configure beforehand. After the first successful credential the chat
auto-selects the provider's default model when the current model cannot serve
turns; otherwise it opens the model picker scoped to that provider.

## OpenAI Codex With A ChatGPT Plan

The `openai-codex` provider signs in through OAuth instead of an API key and
charges usage to the signed-in user's ChatGPT plan. In the setup window it
offers browser sign-in and device-code sign-in; the flow runs in the chat
client process and only the completed credential crosses the RPC boundary, so
it also works when chat is attached to a remote app-server. The same flows are
available from the shell:

```sh
verlet auth login openai-codex           # browser
verlet auth login openai-codex --device  # headless / remote
```

See [Chat Console](chat.md) for details on the callback port and token
refresh.

## Custom Providers

`Add custom provider` covers endpoints the catalog does not know: self-hosted
gateways, proxies, and local model servers. The form fields are:

- **name**: display name; the provider id is derived from it as a slug and
  can be edited.
- **api**: the wire family, one of OpenAI Chat Completions, OpenAI Responses,
  or Anthropic Messages.
- **base URL**: `https://` is required for remote hosts; plain `http://` is
  accepted only for loopback hosts such as `localhost` and `127.0.0.1`.
- **API key**: optional. Leave it empty for a keyless local server (for
  example an Ollama endpoint); the record is stored with `auth: none`, counts
  as configured, and requests carry no Authorization header. A key can be
  added later from the provider menu.
- **header / value**: one optional extra header sent with every request.
- **models**: model ids, separated by commas or spaces; the first id is the
  provider's default model.

Validation errors show inline in the form. Submitting saves the provider
record over `modelProvider/upsert` and then stores the key (when given)
through the normal credential path.

Example: a local Ollama server is `name Ollama`, api `OpenAI Chat
Completions`, base URL `http://localhost:11434/v1`, no key, and the model ids
you have pulled.

## Where Credentials Live

Stored keys and OAuth tokens live in the user provider store (the per-user
metadata database under the Verlet home), separate from provider metadata in
the project store. Credential values are never returned over RPC, never
logged, and never rendered by the TUI; status surfaces report only the source
(`stored`, `env`, `oauth`, or `none` for keyless providers) and a label.

`verlet auth status <provider>` shows the same status from the shell.

## Clearing Credentials

`Clear saved key` in the provider menu (or `verlet auth delete <provider>`,
or the `modelProvider/auth/delete` RPC) removes the stored credential. When
the provider record was created on demand from the catalog and never modified,
the record is removed too, returning the catalog entry to its unconfigured
state. A record you edited, and any custom provider, survives credential
deletion; delete custom providers explicitly from the provider menu.

Environment-variable credentials are not managed by Verlet; unset the
variable in the app-server's environment to withdraw them.

## RPC Surface

The window is a pure client of the app-server: `modelProvider/catalog` for
the merged catalog, `modelProvider/auth/set` and `auth/setOAuth` for
credentials, `modelProvider/upsert` and `delete` for custom records, and
`model/list` / `model/select` for models. See
[RPC Control Plane](app-server.md) for the wire contracts.
