# ADR 0003: Durable Ingress Outcome Protocol

Status: proposed (event kinds pending ratification; implementation gated on it)
Date: 2026-07-10

## Context

Durable ingress today records one applied-ingress marker per envelope: a
thread-stream `io.ingress.received` fact carrying `turn_id` and
`ingress_message_ids`, appended with an expected-tail fence after all fallible
admission work and immediately before the volatile turn submission
(`docs/io.md`, Ingress Persistence). The marker gives at-most-once apply: a
redelivered envelope can never run twice. Three limits of that design were
reported as architecture carve-outs by the pre-push review of the DST wave and
are the subject of this ADR:

1. **Fork admission is not durably idempotent.** The marker lands on the fork
   child's stream, so each racing first apply fences against its own child.
   Two overlapping applies of the same fork envelope can checkpoint and fork
   different children, and the pre-marker side effects (checkpoint, fork,
   `thread.spawned`, egress binding) cannot be rolled back when the loser is
   discovered late.
2. **Observe-only outcomes are not durable.** `ObserveOnly` has no turn id, so
   it cannot participate in the turn-keyed marker schema. Redelivery after a
   crash between apply and lease completion re-appends the control-stream
   ingress witness and `admission.decided`.
3. **The marker cannot be atomic with the volatile submit.** A process death
   after the marker commits but before the reserved in-process submission is
   sent loses the turn: redelivery dedupes against the marker instead of
   retrying. Interrupt admissions add a second cut because cancellation runs
   between the marker and the replacement submit.

The ratified ordering from the DST wave stands and is not re-litigated here: a
durable record of intent precedes the volatile submit, because a duplicate
turn is the worse failure. This ADR closes the loss window with more durable
state, not by reordering.

## Decision

Replace the single applied-ingress marker with a two-fact protocol on the
resolved thread's control stream: a **claim** and a **settle**, both witnessed
events.

```text
io.ingress.claimed     the runtime accepted sole responsibility for an
                       envelope's admitted outcome, before any
                       non-idempotent effect
io.ingress.settled     the claimed outcome reached its terminal state,
                       with evidence
```

Both kinds are additions to the frozen event-kind vocabulary and are therefore
ratification-gated; this ADR is not implementable until they are ratified.

### The outcome model

A claim names the envelope set it covers (`ingress_envelope_ids`, one or many
when coalesced) and exactly one intended outcome:

```text
turn     { turn_id, submission_mode, input_digest }
fork     { child_key, input_digest }
interrupt{ replacement_turn_id | none, cancel_reason, input_digest }
observe  { reason }
reject   { reason }
```

The claim carries provenance to the control-stream ingress witness event ids
and the `admission.decided` event id it executes. The settle carries
provenance to its claim and to the execution evidence (the turn's first
executing-side event, the fork child's `thread.spawned`, or nothing for
effect-free outcomes) plus a `settled_by` discriminator: `execution` or
`recovery`.

### Stream placement and uniqueness

The claim is appended, expected-tail fenced, to the control stream of the
thread the envelope resolves to through the durable conversation binding. That
resolution is deterministic from the envelope alone, and for fork admissions
it is the parent thread, so every racing apply of the same envelope contends
on the same fence. The fence plus a fold over existing claims gives the
uniqueness law:

```text
At most one claim exists per ingress envelope id.
Every claim settles exactly once.
A settled claim is terminal: redelivery dedupes against it.
```

### The ordering law

Per envelope, the apply path is:

```text
1. lease                      (queue, unchanged)
2. outcome fold               settled -> dedupe diagnostic, complete lease
                              claimed, unsettled -> recovery (below)
                              absent -> fresh apply
3. resolve + ensure thread    idempotent by the durable binding (unchanged)
4. ingress witness            control-stream io.ingress.received (unchanged)
5. admission.decided          (unchanged)
6. CLAIM                      fenced append; loser of the race folds the
                              winner's claim and stops with no side effects
7. effects                    bind egress; checkpoint + fork + thread.spawned
                              (fork only); cancel (interrupt only, idempotent)
8. volatile submit            reserved turn permit send (unchanged mechanics)
9. SETTLE                     evidence-linked terminal fact
10. complete lease            (queue)
```

Effect-free outcomes (observe, reject) append claim and settle in one fenced
batch at step 6 and skip 7 through 9. `admission.decided` with an observe
outcome is thereby terminal: redelivery can never repeat its control effects,
which were witnessed once and are referenced by the claim's provenance.
Rejects settle too; a redelivered rejected envelope dedupes instead of being
re-decided.

The invariant that distinguishes step 3 from step 7: everything before the
claim must be idempotent by construction (thread creation is keyed by the
durable conversation binding; witness and decision appends are keyed by the
claim's provenance on redelivery). Everything non-idempotent (fork, cancel,
submit) happens after the claim. This moves fork's checkpoint/fork/spawn from
before the marker to after the claim, which is what makes gap 1 closable: the
loser now loses at step 6, before it has created anything.

### Recovery

Recovery is driven by queue redelivery; there is no separate startup scan.
Because the lease completes only after the settle, any crash between claim and
lease completion redelivers the envelope, and step 2 finds the unsettled
claim. Recovery then acts by outcome:

```text
turn       execution evidence for turn_id in the journal?
             yes -> settle (settled_by = recovery, evidence linked)
             no  -> re-submit the same turn_id, then settle
fork       thread.spawned carrying this claim's id?
             yes -> complete binding + child submit idempotently, settle
             no  -> run step 7 effects from the claim, then settle
interrupt  re-run cancel (idempotent no-op if the target turn is absent or
           finished), then recover the replacement turn as `turn`
observe/   unreachable (claim and settle are one batch); a lone observe or
reject     reject claim is a corrupt-history error, surfaced loudly
```

Execution evidence means the earliest durable event the executing side
appends for the turn: any turn-trace event carrying the turn id, such as
`turn.completed`, a tool-call event, or a context-compile receipt. Evidence
written by the submitting side does not count; that is the same window the
claim exists to close. In particular, after the single-witness work (below)
the `turn.submitted` record for an ingress turn is the submitting side's
apply-time record, adopted by the executing side through turn-id idempotency,
so it is not execution evidence.

Recovery must be safe against a still-live original apply (a slow apply whose
lease expired), and re-submission must be safe when execution started but
left no evidence yet. Three requirements make it so:

- **Turn submission becomes idempotent on turn id.** The supervisor rejects or
  no-ops a reservation for a turn id that is already submitted or running.
  This is the volatile side's half of the protocol and is a hard requirement,
  not an optimization.
- **Turn input persistence becomes idempotent on turn id.** The executing
  side's first durable act is persisting the turn's user input entry. If a
  crash lands between that append and the first turn-trace event, recovery
  finds no evidence and re-submits; re-execution must adopt the existing
  entry for the turn id rather than appending a duplicate.
- **Cancellation is idempotent.** Cancelling an absent or finished turn is a
  witnessed no-op.

### The loss window, restated

The gap-3 window (durable marker committed, volatile submit never sent) is now
recoverable instead of lossy: the claim is durable before the submit, the
settle is durable after it, and redelivery converts the in-between crash into
evidence-checked resubmission of the same turn id. At-most-once at the send
cut composes with recovery into effective exactly-once at the protocol level,
with the duplicate-turn direction still structurally excluded.

### Relationship to the single-witness work (EMO-364)

EMO-364 re-kinds the apply-time thread-stream record so `io.ingress.received`
has exactly one witness per envelope on the control stream. This protocol
completes the separation of duties that work begins: idempotency authority
moves entirely to claim/settle on the control stream, and the apply-time
thread-stream record retains only its egress target-context role as a derived
record. The current marker semantics (turn-keyed dedupe via the thread-stream
fact) are superseded when this ADR lands; `docs/io.md`'s narrowed interim
claim is replaced by the final protocol statement.

One consequence of that work constrains this protocol: because the executing
side adopts the apply-time `turn.submitted` record instead of appending its
own, `turn.submitted` cannot distinguish submission from execution for
ingress turns. The recovery evidence definition above therefore excludes it,
and the turn-input idempotency requirement above carries the burden that the
executing side's own `turn.submitted` append used to make moot.

## Consequences

- Two new event kinds enter the frozen vocabulary (ratification-gated).
- The global applied-marker lookup (payload scan over thread streams) is
  replaced by a claim/settle fold over control streams; a rebuildable index
  keyed by envelope id may cache it as a view.
- `queued_message_was_applied` folds the same claim/settle state.
- Supervisor turn reservation gains turn-id idempotency; cancellation gains
  witnessed no-op semantics.
- Fork apply restructures so that checkpoint, fork, spawn, and egress binding
  follow the claim.
- Redelivery of an envelope mid-apply parks on the claim instead of racing it:
  the outcome fold sees an unsettled claim whose owner may still be live, and
  recovery's idempotent actions make either interleaving safe.
- The three failure classes each get a process-death-shaped pinning test
  (marker committed, submission never sent, recovery settles it; racing fork
  applies produce one child; observe-only redelivery appends nothing new).

## Out of scope

- Outbound send/receipt ambiguity (documented separately in `docs/io.md`).
- Cross-drain coalescing semantics (separate decision).
- `best_effort_direct` ingress, which remains documented as lossy.
- Re-litigating the ratified durable-intent-before-volatile-submit ordering.
