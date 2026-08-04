# Security Policy

Verlet is experimental and has not reached a stable production support window.

## Supported Versions

Only the current `main` branch is considered for security fixes right now.
Released support windows will be documented here when the project reaches a
stable release.

## Reporting a Vulnerability

Please do not open a public issue for suspected vulnerabilities. Use GitHub's
private vulnerability reporting or repository security advisory flow for this
repository.

Include:

- affected commit, version, or branch;
- a concise description of the issue;
- steps to reproduce or a minimal proof of concept when safe to share privately;
- impact and any known mitigations;
- whether the report includes secrets, credentials, customer data, or live
  provider details.

We will acknowledge reports as quickly as practical and coordinate disclosure
case by case.

## Scope

Reports are especially useful when they involve:

- capability or grant bypasses;
- sandbox, placement, or host-execution escapes;
- secret exposure;
- unsafe provider or tool execution behavior;
- persistence, history, or receipt integrity issues;
- GitHub Actions or release-chain compromise.
