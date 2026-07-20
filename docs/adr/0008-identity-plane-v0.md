# ADR 0008: Identity Plane v0 (Principals, Credentials, and the Authenticated Boundary)

Status: accepted (ratified by the anchor 2026-07-20, with the member-kind
deferral described in D1)
Date: 2026-07-18 (revised 2026-07-20)

## Context

ADR 0007 gave every ingress envelope a principal: the admitted record now says
on whose authority an event acted. But that principal is stamped from the
daemon's own configuration, not proven. One hard-coded identity
(`cooldis_app_server` / `local_user`, in `crates/cooldis-kernel/src/adapters/app_server/mod.rs:225-226`)
is registered as the single tenant at startup (same file, `mod.rs:710-715`) and
stamped on all route ingress
(`crates/cooldis-kernel/src/daemon/daemon_io.rs:1066-1068`). Nothing at the
boundary asks a caller who it is.

Every locally reachable surface is unauthenticated, and the RPC dispatcher
behind them exposes full host authority. File references below are relative to
`crates/cooldis-kernel/src/`.

- The app-server Unix socket accepts any connection; the accept path discards
  peer credentials (`adapters/app_server/mod.rs:860`), and while the socket's
  parent directory is chmod `0o700` on first creation (same file, `1835-1844`),
  the socket file itself is left to the umask (`852`). Both the Unix socket and
  the TCP listener carry a WebSocket upgrade
  (`adapters/app_server/mod.rs:913-941`): the two surfaces share one handshake.
- The loopback WebSocket is loopback-enforced (`adapters/app_server/mod.rs:892-896`)
  but carries no token unless the bundled console is configured. `/healthz` and
  `/readyz` are served unauthenticated (same file, `951-961`).
- MCP, ACP, and `cooldis debug rpc` are clients that ride the Unix socket or the
  loopback WebSocket; they inherit its absent authentication.
- The RPC dispatcher is a flat `match method` with no per-method authorization
  (`adapters/app_server/connection.rs:863-869`, unknown-method fallthrough
  `1197-1200`). Any connected client can call `command/exec` (arbitrary argv,
  sandbox `dangerFullAccess`, same file `3633`), the `fs/*` methods (absolute
  paths with no workspace confinement, `4532-4541`), and the secret-writing
  methods (`modelProvider/auth/set` `947`, `mcpSource/upsert` `1132`).

Exactly one surface already authenticates: the remote-store stream sync
endpoint. `SyncCredentialAuthority` (`daemon/remote_store/lease.rs:270-283`)
mints credentials from a CSPRNG, persists only a digest (same file,
`249,720`), scopes each credential to a stream prefix (`StreamPrefixScope`,
`75`), fences writers with a single-writer lease (`189`), and witnesses
rejections. Every request verifies a bearer token before it acts
(`daemon/remote_store/endpoint.rs:518,584,602,647`). This is the shape to
reuse, not reinvent.

Security today rests entirely on loopback binding plus one directory's
permission bits. That is adequate for a single-user laptop and disqualifying
for a managed instance the moment a second principal, or an untrusted network
path, exists.

The invariant this ADR installs, stated once: **every connection to a
privileged surface resolves to an authenticated principal before it may call
any method, and each method it calls is authorized against that principal's
authority. A connection that has not resolved to a principal is admitted to no
method. Every authorization decision, allow or deny, is recorded with the
principal it was made for.**

Terms used below are the runtime's own (see `docs/kernel-invariants.md`): an
*event* is an entry in the append-only stream; a *witnessed* fact is one the
runtime recorded as such; a *fold* is the projection built by reducing over the
stream, and it is the authority a boundary cache is rebuilt from.

Scope discipline: this is a v0 sized to one internal tenant with a handful of
named principals. Multi-tenant boundary authentication, a role system, and the
per-method grant algebra are foreclosed consciously (see Out of scope), not
built.

One layering principle governs every decision below: **the kernel is an
identity receptor, not an identity provider.** The kernel needs a slot where
"who" plugs in, because host authority can only be gated inside the dispatcher
and because the record's attribution must be attested by the runtime itself,
not relayed from a fronting proxy. Everything that provides identities (user
accounts, login flows, SSO, directory sync) is a separate service above the
kernel: Cooldis Cloud for the managed lane, or a deployment's own identity
service for self-hosting. This mirrors the Unix split (user ids live in the
kernel; login systems live outside it) and rejects the Docker split (no
identity in the daemon at all, so socket access equals root and cannot be
fixed after the fact).

## Decision

### D1: Principals are declared records

A **principal** is a named identity within a tenant, declared as a witnessed
event in the stream and folded into a boundary-readable set. Fields:
`principal_id`, `kind`, `display`, `status` (active / revoked), and the
declaring principal (the "who granted what" attestation hook: a principal's
existence names who created it).

Kinds implemented in v0:

- `operator`: full host authority (command exec, filesystem, secrets, debug)
  plus interactive use. The identity that possesses the host box.
- `adapter`: ingress submission only; the identity a route binding names when
  it stamps `via="route:{route_id}"`.

One kind is **reserved but not implemented**: `member`, interactive thread and
agent use without host authority. The kind value exists in the record schema
so its later arrival is additive, but declaring a member principal is rejected
in v0. Rationale (the receptor-not-provider principle applied): no v0
deployment gives an end user direct daemon access. Customers reach agents
through adapters, and the envelope already records end-user attribution as
adapter-supplied testimony: the kernel attests which authenticated adapter
delivered the event, and records the external actor the adapter claims, as a
claim. That split (attested carrier, claimed author) is the formally honest
shape. Per-person accounts, when a deployment actually needs them, arrive as
an external identity service that maps humans onto member principals; the
kernel's job is only to have the slot ready.

Principals are declared and revoked through witnessed events; the fold is the
authority, and the boundary cache rebuilds from it at daemon start. Cache
refresh after a mid-run declaration is covered in D6. Agents are not principals
in v0: they act under the `mandate:` / `handle:` / `remote:` schemes ADR 0007
already defines, never under a credential of their own.

### D2: Credentials are hash-only witnessed records

A **credential** binds a bearer secret to exactly one principal:
`credential_id`, `principal_id`, the SHA-256 digest of a 256-bit CSPRNG secret,
optional expiry, and a revoked flag. Minting and revoking are witnessed events;
the secret material never enters the record, only its digest and metadata.
High-entropy random secrets make the stored digest non-invertible, so no
password-style slow hash is needed (these are tokens, not passwords).

The mint event names the principal that authorized it: with D1's declaration
event, this is the full "who granted what" trail, and it is a historical fact,
not re-derivable configuration. This reuses the `SyncCredentialAuthority`
vocabulary (CSPRNG mint, digest-only persistence) rather than introducing a
second credential shape; the two authorities stay distinct objects but share
the pattern.

"The secret never enters the record" is a statement about the daemon: the
daemon persists only digests. A client (MCP, ACP, debug RPC, an external
adapter) holds its own credential secret in its own configuration or
environment, which is the ordinary bearer-token custody model.

Bootstrap: an `operator`-run CLI command on the host box, in one step, declares
the first operator principal and mints its credential through direct store
access. Possession of the box is the root of trust in v0; no secret is ever
embedded in a daemon config file. A running daemon picks up the new principal
and credential at the next fold refresh (D6); a cold bootstrap precedes daemon
start and needs no refresh.

### D3: Authentication rides the shared WebSocket handshake

Both privileged transports (the Unix socket and the loopback TCP listener)
speak a WebSocket upgrade before any JSON-RPC flows
(`adapters/app_server/mod.rs:913-941`), so one authentication design covers
both, and it happens at the upgrade, before the existing initialize-first rule
(`adapters/app_server/connection.rs:838-843`).

- **Token carrier:** an `Authorization: Bearer` header on the upgrade request,
  verified against the credential fold, resolving the connection to a principal.
  The console's `Sec-WebSocket-Protocol` token carrier
  (`adapters/app_server/mod.rs:1734-1750`) is accepted as an equivalent because
  browsers cannot set arbitrary headers on a WebSocket. The `?token=`
  query-parameter carrier the same function also accepts is dropped: query
  strings leak into logs and history.
- **Auth failure:** the upgrade is answered `401` and the connection closes; no
  JSON-RPC session opens. The failure is witnessed (D6). There is no
  unauthenticated fallthrough to any method.
- **Unix-socket peer mapping (local-mode ergonomics):** in `local` mode (D5) a
  same-uid peer (checked via `SO_PEERCRED` / `getpeereid`) resolves to the
  configured `operator` principal without a token, so `cooldis` CLI usage on the
  host needs no credential. This is off in `managed` mode. The socket file gains
  an explicit `0o600` chmod regardless of mode.
- **Console:** the session token is regenerated from a CSPRNG (today it is two
  concatenated UUIDv7 values, `cli/console.rs:220-222`, which are time-ordered
  and guessable) and resolves the console connection to a configured principal
  (in v0, the operator: every v0 console user is whoever operates the daemon)
  instead of standing in for an anonymous transport secret. Static asset serving
  is unchanged.
- **MCP / ACP / debug RPC:** daemon clients, not surfaces of their own. Each
  presents a credential and inherits the principal it resolves to. Debug RPC
  resolves to `operator` only.
- **Health probes:** `/healthz` and `/readyz` stay unauthenticated for
  orchestrator probing; `readyz` reports liveness only and must not leak
  identity or configuration.
- **Stream-sync endpoint:** already authenticated (see Context); untouched here
  beyond noting the shared pattern.

The same-uid peer mapping has one sharp consequence that must be named: the
daemon runs agent host commands under its own uid, so with the mapping on, a
process an agent spawns via `command/exec` could reconnect to the socket and be
resolved as `operator`, escalating out of its sandbox. This is why the mapping
is a `local`-mode developer convenience only. `managed` deployments run with it
off, and their agents authenticate (or do not share the daemon's uid), so the
loop is closed there.

### D4: Authorization is authority classes at the dispatcher choke point

The dispatcher's single `match` (`adapters/app_server/connection.rs:869`) is the
one gate. After authentication a connection carries a resolved principal; before
dispatch, the method is checked against the principal's authority, derived
directly from `principal.kind`:

The method taxonomy has three classes; it is the durable part of this
decision and does not change when the reserved `member` kind ships:

- **Host authority:** `command/exec` and its family, `thread/shellCommand`,
  all `fs/*` methods, process-handle methods, `modelProvider/auth/*`,
  `mcpSource/upsert`, and debug methods.
- **Interactive:** `thread/*`, `turn/*`, `mandate/start`, and the read
  methods.
- **Ingress:** envelope submission; the route binding must name the adapter
  principal it stamps.

The v0 kind-to-class mapping: `operator` reaches all three classes; `adapter`
reaches ingress only. When `member` ships it will reach interactive and
ingress but never host authority; nothing about the taxonomy moves.

The host-authority list is explicit and takes precedence: `thread/shellCommand`
is host authority even though it matches the `thread/*` interactive prefix. A
method's most restrictive matching class wins.

An unauthorized call (an authenticated principal invoking a method above its
authority) returns a distinct authorization error, not the `-32601`
unknown-method code (`adapters/app_server/connection.rs:1197-1200`), so the
boundary does not double as a method-enumeration oracle beyond what a caller's
own class already reveals.

Authority is derived from `principal.kind` at the gate, not stored as separate
grant records. Default class-to-method mapping is re-derivable configuration,
which by ADR 0007's own D7 test is retrofittable and should not be persisted
before it is needed. When a real ask for per-method grants arrives (Out of
scope), grants become witnessed records then; the declaration and mint events
already carry the attestation trail in the meantime.

### D5: Tenant and mode come from configuration, not code

The daemon config file grows a `[daemon.identity]` section: `tenant_id`, a
`mode` of `local` or `managed`, the console principal binding, and (in
`managed` mode) the expected principals. The hard-coded `cooldis_app_server` /
`local_user` defaults are removed from code.

- `local` mode with no `[daemon.identity]` section synthesizes a single default
  `operator` principal and enables the Unix-socket peer mapping (D3), so an
  existing single-user developer keeps working with no migration and no token.
- `managed` mode requires an explicit `[daemon.identity]` section and a
  bootstrapped operator credential; starting `managed` without them is a hard
  error with a migration note. Peer mapping and debug RPC default off.

One tenant per daemon remains the deployment model. Per-tenant instances are the
managed-lane isolation boundary; multi-tenant boundary authentication is
foreclosed to a later version.

### D6: Sessions, audit identity, and the caller-auth ingress scheme

- **Sessions are witnessed.** RPC session open and close emit witnessed events
  carrying the resolved principal, the surface, and the credential id, or, for a
  peer-mapped `local` session that used no credential, the peer uid in the
  credential id's place. Failed authentication is witnessed too (an
  authentication-rejected event with the surface and the reason), so pre-guard
  intrusion attempts are not invisible, matching the sync endpoint's existing
  behavior.
- **Host-authority effects carry the principal.** Command exec, filesystem
  writes, and secret reads emit witnessed events carrying the session principal.
- **Caller-authenticated ingress gets its `via` scheme, named now.** ADR 0007
  reserved a caller-auth scheme for when boundary authentication landed; this is
  that landing. Ingress an authenticated caller submits is stamped
  `via="caller:{session_id}"`, pointing at the witnessed session event that
  proves the authentication, the same way `mandate:` points at the clock event
  and `handle:` at the dispatch. Choosing this string now matters because it
  enters permanent ingress records the moment phase 2 ships; leaving it unnamed
  would repeat the unretrofittable gap ADR 0007 closed.
- **Revocation and expiry are checked at connection open.** A revoked or expired
  credential resolves to no principal on the next connect. v0 does not forcibly
  tear down a live session whose credential is revoked mid-connection; that
  bounded gap is acceptable for a handful of trusted principals and is revisited
  when a credential must be killable in flight (the trigger to add
  per-connection recheck).
- **Cache refresh.** The boundary principal and credential caches rebuild from
  the fold at daemon start and refresh when a principal or credential event is
  appended, so a mid-run mint or revoke takes effect on subsequent connections
  without a restart.
- Projecting the ingress principal into the durable ingress event schema is
  EMO-494 and is not re-decided here; the session and host-effect events this
  section adds are new record shapes, not changes to an existing one.

## Consequences

- The managed instance can name its operator and its adapter principals, and
  can prove each caller is who it claims before any host authority is
  exercised. This is the precondition for standing the internal managed
  instance up.
- A single-user developer keeps working unchanged: `local` mode with no identity
  section plus the same-uid peer mapping means host CLI usage needs no token,
  while every remote or multi-principal path authenticates.
- The console stops being authenticated by a guessable token.
- The authority classes are coarse, and two future triggers are named. The
  first deployment where an end user needs direct daemon access (rather than
  reaching agents through an adapter) triggers implementing the reserved
  `member` kind, fed by an external identity service. The first need to grant
  one principal a single host method triggers the per-method grant algebra.
  The declaration and mint events already record the attestation trail, so
  both arrivals are additive.

## Out of scope (foreclosed consciously, not forgotten)

- **Member principals and per-person accounts.** The `member` kind value is
  reserved in the schema and rejected at declaration (D1). End users reach
  agents through adapters, attributed as adapter testimony on the envelope.
  When a deployment needs direct end-user daemon access, members ship, fed by
  an external identity service (Cloud-side or deployment-owned); the kernel
  never becomes an identity provider.
- **Role / membership model (RBAC) and the per-method grant algebra.** Triggered
  by the first per-method grant ask. v0 authority classes are not roles, and
  default authority is not persisted.
- **Agents as principals.** Agents act under ADR 0007 mandate/handle/remote
  schemes; giving an agent its own credential is a later, separate decision.
- **Cross-tenant boundary authentication.** One tenant per daemon; no boundary
  path authenticates across tenants.
- **Password auth, OIDC, SSO.** Cooldis Cloud concerns above the kernel, not
  kernel primitives.
- **Mid-connection revocation enforcement.** Checked at connect in v0 (D6);
  per-connection recheck waits for a killable-in-flight requirement.
- **Durable ingress principal projection.** EMO-494, design-gated separately.

## Related surfaces (public references)

Two shipped systems were compared while revising this design; both citations
are public.

- **Everruns** (github.com/everruns/everruns) independently converged on the
  same credential shape: high-entropy random tokens shown once at creation and
  stored only as SHA-256 digests. Its role ladder measures organization
  seniority rather than machine authority, its inbound channels authenticate
  with shared per-channel tokens bound to no named principal, and its internal
  worker fleet authenticates with a single shared token, which this design
  treats as a counterexample: workers get per-worker scoped credentials, never
  a fleet secret.
- **Anthropic's Managed Agents API** (platform.claude.com docs) has one
  data-plane authority class: any workspace API key holds full authority over
  every agent, session, and stored credential in the workspace. Sessions
  record no initiating user; end users exist only as caller-supplied metadata
  on credential containers. Its credential rotation re-resolving into running
  sessions is what prompted D6's explicit revocation-timing decision here.

Neither system carries a data-plane distinction like operator / adapter /
(reserved) member, and neither records who initiated a session in an attested
record. That gap is the part of this design that is not catch-up.

## Lexicon additions (naming-law receipt)

This ADR names four primitives the lexicon uses or now requires but has not
defined as headwords. All are conservative: they codify meanings already
load-bearing in the code and the envelope entry, and change no existing word. To
be added to `formalism/lexicon.md` before the phase-2 implementation tickets:

- **principal**: a named identity within a tenant, on whose authority effects
  act; declared as a witnessed record. Already load-bearing in the `envelope`
  entry ("resolves to a principal"). The `member` kind value is reserved, not
  implemented; it receives a full lexicon treatment when it ships.
- **credential**: a witnessed binding of a bearer secret (stored as a digest
  only) to one principal, with which a caller authenticates at a boundary.
- **tenant**: the isolation unit that owns principals, streams, and grants;
  already used throughout the code and the lexicon prose.
- The `caller:{session_id}` **`via` scheme** on the envelope's principal
  attribution, extending the schemes named with `envelope` (`route:`,
  `mandate:`, `handle:`, `remote:`).

## Phase-2 tickets (cut after ratification, skeletons planned)

Each kernel ticket is security-adjacent and dispatched with an explicit list of
failure modes to defend against (enforcement bypass, cross-principal confusion,
credential leakage, escalation through the peer mapping) and a second
independent review before it lands. With the member deferral, every ticket
builds two kinds, not three.

1. Principal and credential records (operator and adapter kinds; member
   reserved and rejected), the fold, and the CLI command that bootstraps the
   first operator (declare + mint in one step).
2. Boundary authentication: token verification on the shared WebSocket upgrade,
   the same-uid peer mapping, and the resolved principal on the connection;
   regenerate the console token from a CSPRNG.
3. Dispatcher authorization: the method taxonomy, the operator / adapter
   kind-to-class mapping with the precedence rule, the distinct authorization
   error, and the witnessed session, host-effect, and authentication-rejected
   events; stamp `via="caller:{session_id}"` on caller-authenticated ingress.
4. Config: `[daemon.identity]` with `mode`, removal of the hard-coded tenant,
   and the migration note.
5. Docs: `threat-model.md` update and an authentication section in
   `app-server.md`.
