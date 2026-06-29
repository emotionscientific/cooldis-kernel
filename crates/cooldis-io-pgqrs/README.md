# cooldis-io-pgqrs

`cooldis-io-pgqrs` is the first durable ingress queue spike for Cooldis IO.

It wraps `pgqrs` behind the `cooldis-io-core` queue traits so protocol adapters
and kernel bridges do not depend on a queue framework directly. The default
build uses SQLite for the local spike; enable the `postgres` feature to point
the same wrapper at a managed Postgres DSN later.

`IngressPersistenceConfig` is the switch that decides whether this crate is
used. `durable_queue` builds a `PgqrsQueueConfig`; `best_effort_direct` returns
no pgqrs config so a local daemon can bypass queue storage and accept lossy
in-flight behavior on restart.

Current scope:

- persist normalized `IngressEnvelope`s into a pgqrs queue;
- lease envelopes with a visibility timeout;
- archive completed ingress messages;
- release failed messages for retry.

Not yet implemented here:

- a durable dedupe side table;
- egress outbox storage;
- worker loops that resolve/admit/apply leased envelopes to the kernel;
- daemon config loading.
