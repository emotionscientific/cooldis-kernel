# cooldis-io-core

`cooldis-io-core` defines the protocol-neutral contracts for getting external
events into Cooldis and projecting Cooldis events back out.

It intentionally does not know about Telegram, WebSockets, CLI rendering,
product billing, or the root `cooldis` crate's concrete kernel types. Protocol
adapters normalize their wire events into `IngressEnvelope`s, policy decides how
to admit them, and a daemon bridge maps the resulting decisions onto Cooldis
runtime calls.

The intended flow is:

```text
Protocol Adapter
-> IngressEnvelope
-> queue / dedupe
-> IoResolver
-> AdmissionPolicy
-> cooldisd runtime bridge
-> EgressEnvelope
-> Protocol Adapter
```

V1 should keep protocol adapters compiled in and hotswappable by config.
Out-of-process protocol plugins can be added later by speaking the same envelope
shape over JSON-RPC or another small transport.
