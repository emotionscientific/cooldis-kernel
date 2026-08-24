# Secret Management

Verlet does not commit provider keys or generated env files. Provider
credentials should come from the process environment or a local ignored env
file, and provider examples should use generic OpenAI-compatible or
Anthropic-compatible API formats.

Secret management is separate from identity, RBAC, and authorization. Those
systems decide who may use a secret. Secret management decides where the secret
lives, how it is resolved, and how the runtime injects it without exposing raw
values to agents, capsules, logs, or docs.

## Secret Broker POC

Status: POC subject to change.

V1 includes a small local Secret Broker lane before adopting provider-specific
secret managers such as Bitwarden, 1Password, Vault, Doppler, or cloud secret
stores.

The goal is not to become an enterprise secret-management product. The goal is
to stop treating process environment variables as the runtime secret model.

```text
env vars are one source for secrets
stored local records are one source for secrets
external secret managers can be later sources for secrets

Verlet-owned secret refs are the runtime model
```

The POC shape:

```text
SecretStore
  keeps local secret values or secret references

SecretBinding
  allows an attached runtime object to request a named secret

SecretBroker
  resolves a named secret for a specific invocation after policy passes

host import
  injects the value into the effect boundary, never into model context
```

For local development, V1 supports:

```sh
verlet secret import EXAMPLE_API_KEY --from-env EXAMPLE_API_KEY
verlet secret set EXAMPLE_API_KEY --value-stdin
verlet secret list
verlet secret status EXAMPLE_API_KEY
verlet secret delete EXAMPLE_API_KEY
```

These commands are clients of the owning Verlet instance. Environment import
reads the variable in the client process and sends the value over the local
Unix socket; list and status return redacted metadata. The instance witnesses
the method name and acting operator, never the value.

When a published operation declares `secret:<name>` in its required
capabilities, `verlet tool run <published-name> <operation>` and manifest-backed
runtime catalog mounts resolve the names through the owning instance before
loading or invoking the runtime artifact. CLI resolution is Unix-socket-only.
Missing secret refs fail with the ref name and an import/set hint. Raw values
are never printed.

Remote runtimes can use the same secret reference shape without relying on the
remote process environment.

The concrete proof target is a provider-neutral HTTP operation:

```text
published HTTP Wasm operation
-> operation declares secret:EXAMPLE_API_KEY and net.http capabilities
-> agent attachment allows EXAMPLE_API_KEY and any private origins
-> EXAMPLE_API_KEY exists as a Verlet secret ref
-> model provider sees the operation as a normal tool
-> model provider calls the tool
-> host import injects the key only inside the outbound HTTP request
-> receipt records the secret name and policy decision, never the value
```

Provider-specific live proofs belong in maintainer-private harnesses and are not
part of the default release gate.

This belongs beside provider credential storage, but it is not provider
credentials. Provider credentials resolve LLM provider calls. Secret Broker
resolves named runtime secrets for tools, capsules, MCP clients, and future
resource adapters.

## OpenAI-Compatible

OpenAI Responses-compatible providers use the Responses wire format:

```toml
[daemon.provider]
provider = "openai"
base_url = "https://api.openai.com"
api_key_env = "OPENAI_API_KEY"
model = "gpt-4.1-mini"
stream = true
max_tokens = 4096
```

OpenAI Chat Completions-compatible providers use the Chat Completions wire
format:

```toml
[daemon.provider]
provider = "openai_chat_completions"
base_url = "https://api.openai.com"
api_key_env = "OPENAI_API_KEY"
model = "gpt-4.1-mini"
stream = false
max_tokens = 4096
```

Gateway deployments can use the same shapes with their own `base_url`,
`api_key_env`, and model id. Keep gateway-specific secret names in local
maintainer docs or ignored env files, not in committed public docs.

## Anthropic-Compatible

Anthropic-compatible providers use the Anthropic Messages wire format:

```toml
[daemon.provider]
provider = "anthropic"
base_url = "https://api.anthropic.com"
api_key_env = "ANTHROPIC_API_KEY"
model = "claude-sonnet-4-5-20250929"
stream = true
max_tokens = 4096
```

For AWS Bedrock Anthropic, prefer AWS-standard env vars or a local wrapper that
loads them for the child process:

```toml
[daemon.provider]
provider = "anthropic_bedrock"
region = "us-east-1"
model = "global.anthropic.claude-sonnet-4-5-20250929-v1:0"
stream = true
max_tokens = 4096
```

## Local Secrets

For local-only live tests:

- Store raw provider keys in a password manager or local shell environment.
- Reference secrets by environment-variable name in config, for example
  `OPENAI_API_KEY` or `ANTHROPIC_API_KEY`.
- Keep `.env` and `.env.*` ignored.
- Do not paste raw keys into committed config files, fixtures, docs, shell
  history, or test logs.
- Keep live-provider checks opt-in so normal verification stays deterministic.
