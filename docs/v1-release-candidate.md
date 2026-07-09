# V1 Release Candidate Gate

This document describes the public V1 release-candidate gate for the Cooldis
kernel repo. Historical private live-provider runs and internal planning notes
are intentionally not part of this OSS tree.

## Default Gate

Run:

```sh
scripts/release-v1-candidate.sh
```

The default lane is deterministic and does not require provider credentials. It
checks:

- guard rails over tracked files;
- clippy correctness, suspicious, and perf lints;
- `scripts/verify.sh`;
- app-server restart, resume, and TCP health smoke;
- focused MCP server tests;
- manifest-backed `thread/start` end-to-end smoke;
- release binary package build and archive smoke;
- packaged-binary canonical CLI help smoke for `console`, `chat`, `auth`,
  `tool manual`, `rpc`, and `debug rpc`;
- packaged-binary secret import, set, list, status, delete redaction smoke;
- deterministic AX blind-test prompt bundle;
- packaged-binary folder-first init, operation publish, and agent publish
  smoke.

## Optional Public Lanes

```sh
scripts/release-v1-candidate.sh --docs
scripts/release-v1-candidate.sh --workbench
scripts/release-v1-candidate.sh --live-provider-protocols
```

- `--docs` builds workspace Rust docs with warnings denied.
- `--workbench` runs the app-server workbench query-surface smoke.
- `--live-provider-protocols` runs the public provider protocol wire smokes when
  credentials for those public protocols are configured locally.

## Approval Gate Surface

The V1 standard-operation catalog includes
`std::permission.approval_gate` as the reference executable template for the
abstract approval flow. The catalog marks it runtime-executable/reference-only:
it proves the durable `approval.requested`, `tool.call.suspended`, and
`approval/resolve` control surface without claiming a channel-specific HITL
integration.

## Maintainer-Private Lanes

Older provider-specific smokes were used during private dogfooding. They are not
shipped in this public checkout. The public release script fails closed if a
legacy private lane flag or environment variable is used.

## Tag Gate

Release tags must match the kernel crate version:

```sh
scripts/check-release-tag.sh v$(cargo metadata --format-version 1 --no-deps \
  | jq -r '.packages[] | select(.name == "cooldis-kernel") | .version')
```

GitHub Actions runs the tag check before publishing artifacts.

For normal maintainer release iteration, use the async helper after local
changes are committed:

```sh
scripts/release-async.sh v0.1.0-rc.N
```

The helper performs a local host-target package smoke, pushes the tag, prints
the Release workflow URL, and exits. Pass `--full-gate` to run this full V1 gate
before the tag push.

## Binary Artifacts

Release packaging is target-aware:

```sh
scripts/package-release-binary.sh --target x86_64-unknown-linux-gnu
```

Each archive includes the public binaries and the bundled browser console under
`share/cooldis/console`; the installed user-facing command is `cooldis console`.

See [Release Process](../RELEASE.md) for the supported target matrix and publish
flow.

## Acceptance Rule

A V1 candidate is releasable only when the deterministic default gate passes,
the tag matches the crate version, and every included public surface has either
canonical docs or an explicit gap in [Public API Coverage](public-api-coverage.md).
The public command surface starts at [Cooldis CLI](cli.md).
