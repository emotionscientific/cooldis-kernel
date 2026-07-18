# ADR 0007: Adapter Envelope Contract v0 (Delivery Provenance and Principal Attribution)

Status: accepted
Date: 2026-07-18

## Context

Managed deployments put the ingress boundary in front of systems the runtime
does not control: chat platforms, webhooks, queues, and the scheduler. The IO
contracts in `cooldis-io-core` already model the pipeline (envelope, queue,
dedupe, resolver, admission, bridge; ADR 0003 governs the admitted outcome),
but three attributes of an ingress envelope are either optional or absent, and
all three are unretrofittable: an event admitted without them produces a
permanent record that cannot say where it came from or on whose authority it
ran.

1. **Delivery identity is optional.** `IngressEnvelope.dedupe_key` is an
   `Option`, and there is no field naming the external delivery itself. The
   internal `ing_` id names our receipt of the event, not the event. An
   envelope submitted without a dedupe key silently loses redelivery
   protection.
2. **Principal attribution does not exist.** `resolve_target` in the daemon
   assigns every envelope the daemon's own configured `tenant_id` and
   `user_id`. Attribution is a deployment assumption, never a recorded fact.
   Nothing in the admitted record says which principal an ingress event acted
   for, and nothing validates tenant agreement between an envelope and the
   thread it resolves into.
3. **Existing sources smuggle both through untyped metadata.** The clock
   route writes `cooldis_tenant_id`, `cooldis_user_id`, `cooldis_session_id`,
   and `cooldis_mandate_event_id` as string metadata; `RouteIngressSink`
   writes `cooldis_route_id`. These are load-bearing routing and attribution
   facts traveling in a namespace with no schema and no validation.

The invariant this ADR installs, stated once: **an envelope names its external
delivery and resolves to a principal before admission, and redelivery dedupes
on its delivery identity. An arrival the runtime cannot attribute to a
delivery and a principal is not admissible as witnessed ingress.**

## Decision

### D1: Delivery identity becomes a typed field

```rust
pub struct IoDelivery {
    /// The external system's own identifier for this delivery: a Telegram
    /// update id, a webhook delivery id, a queue message id, or a scheduler
    /// occurrence ("{mandate_event_id}:{occurrence_index}").
    pub delivery_id: String,
    /// Redelivery attempt ordinal when the external system reports one.
    pub attempt: Option<u32>,
    pub metadata: BTreeMap<String, String>,
}
```

`IngressEnvelope` gains `delivery: Option<IoDelivery>`. The `Option` exists
only for wire compatibility with already-queued envelopes; the boundary
enforces presence (D3). The dedupe identity derives from the delivery: when
`dedupe_key` is unset and `delivery` is set, the effective dedupe key is
`IoDedupeKey::for_source(&source, &delivery.delivery_id)`. An explicitly set
`dedupe_key` still wins, which keeps every existing producer's dedupe
identity byte-stable through the migration.

### D2: Principal attribution becomes a typed field

```rust
pub struct IoPrincipal {
    pub tenant_id: String,
    pub principal_id: String,
    /// How attribution happened: "mandate:{event_id}", "route:{route_id}",
    /// or a caller-auth scheme once boundary authentication lands.
    pub via: String,
}
```

`IngressEnvelope` gains `principal: Option<IoPrincipal>`. Two stamping
patterns, by who can honestly attribute:

- **Self-attributing sources** stamp at construction. The clock route knows
  the mandate's coordinates from the record; its ticks carry the mandate
  holder's tenant and user with `via = "mandate:{mandate_event_id}"`.
- **Protocol adapters cannot attribute** and leave `principal` unset. The
  daemon stamps during resolution from the route binding that accepted the
  event (`via = "route:{route_id}"`). External actor identity
  (`envelope.actor`) remains provenance and is never itself the principal.

Internal durable authorities are self-attributing in the same sense as the
clock: handle dispatch/outcome envelopes stamp from their durable consumer
binding (`via = "handle:{dispatch_id}"`), and remote child ingress stamps
from its durable target coordinates (`via = "remote:{dispatch_id}"`).

There is deliberately no principal kind classification in v0. Whose authority
and how it was established are unretrofittable; classifying the principal is
not, and belongs to the full principal model.

### D3: Two enforcement points

```rust
impl IngressEnvelope {
    /// dedupe_key.clone() or the delivery-derived key (D1).
    pub fn effective_dedupe_key(&self) -> Option<IoDedupeKey>;
    /// Submit-boundary check: delivery present with a non-empty id, and an
    /// effective dedupe key exists.
    pub fn require_witnessed(&self) -> IoResult<()>;
    /// Admission-boundary check: require_witnessed, principal present, and
    /// principal.tenant_id equals the resolved target's tenant_id.
    pub fn require_attributed(&self, target: &ResolvedIoTarget) -> IoResult<()>;
}
```

Production submit paths (`RouteIngressSink`, `DirectRuntimeIngressSink`, the
pgqrs queue store) call `require_witnessed` and reject early with
`IoError::InvalidEnvelope`. The admission path calls `require_attributed`
after resolution and before the decision, which is the guarantee: no sink,
present or future, can get an unattributed envelope admitted, because the
check the record depends on does not live in the sinks. The tenant-agreement
clause closes cross-tenant injection: a resolver bug or hostile route cannot
land one tenant's envelope in another tenant's thread without the mismatch
being rejected and witnessed.

### D4: Legacy derivation, bounded

Envelopes queued before this contract deserialize with both new fields unset.
At lease time the daemon derives what can be derived honestly: if
`delivery` is unset but `dedupe_key` is set, `delivery_id` takes the dedupe
key's `key` (deterministic across redeliveries, and dedupe-stable by D1's
precedence rule). Legacy principal stamping happens at lease preparation,
only for rows carrying a non-empty `cooldis_route_id` metadata tag, using the
daemon's configured route identity; an untagged legacy row stays
unattributed and is terminally rejected at admission. An envelope with
neither `delivery` nor `dedupe_key` is rejected, in every era. Derivation is
a migration ramp for in-flight queue contents, not an alternative contract;
producers must set `delivery` directly.

### D5: Receipts carry attribution

`KernelIoReceipt` gains `principal: Option<IoPrincipal>`, copied from the
validated envelope, so the admission receipt records on whose authority every
outcome ran. The claim/settle protocol of ADR 0003 is unchanged; claims
reference envelope ids and inherit attribution transitively through them.

### D6: Existing sources migrate to the typed fields

- The clock route sets `delivery` (mandate event id + occurrence index, the
  same string it uses for dedupe today) and stamps `principal` from the
  mandate's coordinates. The `cooldis_tenant_id` and `cooldis_user_id`
  metadata entries are removed in favor of the typed principal; remaining
  routing metadata is out of scope here (D7).
- The Telegram adapter sets `delivery` from the update id it already uses
  for dedupe. It does not stamp `principal`.
- `RouteIngressSink` stamps `principal` from the daemon's route identity
  (the same tenant and user `resolve_target` uses today, now recorded per
  envelope instead of assumed).

## Consequences

- The envelope's serialized shape gains two optional fields; old records and
  queued envelopes deserialize unchanged. Dedupe identities are byte-stable
  through the migration, so in-flight redelivery cannot double-apply at
  upgrade.
- Admission gains a validation step whose rejections are witnessed like any
  other reject outcome.
- The untyped tenant and user metadata smuggle on clock ticks is deleted, and
  its information becomes schema.
- Pinning tests cover: unwitnessed submit rejection, unattributed admission
  rejection, tenant-mismatch rejection, legacy derivation determinism, and
  dedupe stability for clock and Telegram envelope shapes.

## Out of scope (deferred deliberately, not forgotten)

- **D7:** typed route policy. `cooldis_route_policy`, threading, and agent
  ref stay metadata; they are re-derivable configuration, not permanent
  attribution, and are retrofittable later.
- Surfacing `principal` in the durable ingress witness payload. In v0 the
  witness event commits to the full validated envelope (principal included)
  through `envelope_digest`, and the admission receipt carries the principal
  in memory, but the durable payload does not state it readably. Promoting it
  into the witness payload is a schema evolution of the ADR 0003 record shape
  and is ticketed separately rather than bundled here.
- Caller authentication at the boundary (which supplies stronger `via`
  values); this contract carries its outcome.
- Conversation optionality and trigger routing semantics (start versus steer
  versus resume as first-class outcomes); scheduler ticks keep their
  `ConversationKind::System` conversations.
- Adapter registry and lifecycle; egress-side attribution.
