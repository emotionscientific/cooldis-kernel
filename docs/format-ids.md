# Frozen Format IDs

Verlet still contains identifiers that begin with `cooldis`. These are format
IDs, not product branding. They were written into durable records or included
in interface bytes before the project was renamed. Changing them would orphan
stored data or change interface hashes, so they never rename.

The frozen families are:

- event vocabulary version `cooldis.events/0.5`, event payload schema IDs under
  `cooldis.event.*`, and the `cooldis.stream.*`, `cooldis.context.*`, and
  `cooldis.debug.*` schema-ID namespaces;
- operation names recorded in receipts, including `cooldis.thread_spawn`,
  `cooldis.mandate_start`, `cooldis.process_exec`, and their sibling thread,
  mandate, process, notification, and kernel-control operations;
- tool-package kind `cooldis.tool`;
- operation ABI string `cooldis.operation/0.1`;
- operation metadata key `cooldis.runtime.kind`.

Readers and compatibility tooling must compare these values literally. Do not
derive a Verlet-prefixed replacement, rewrite stored records, or treat the old
prefix as a deprecation signal.

User-authored identifiers are different. Agent manifests use
`verlet.agent-manifest`, and the kernel-native operation packages are
`verlet-threads`, `verlet-schedule`, `verlet-process`, and `verlet-notify`.
Their pre-rename spellings remain input aliases only through v0.3.x and are
scheduled for removal in v0.4.0.
