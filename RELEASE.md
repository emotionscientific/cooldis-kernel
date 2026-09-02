# Release

`scripts/release.sh` is the maintainer release button. Run it from a clean,
up-to-date `main` branch with an authenticated `gh` CLI:

```sh
just release v0.5.1
```

Use the next unpublished tag in a real release. Stable and prerelease tags are
accepted:

```text
v0.5.1
v0.6.0-rc.1
```

The version lives under `[workspace.package]` in the root `Cargo.toml`.

## What the Button Does

The button runs nine named steps.

1. `preflight` checks `main`, the worktree, the remote, GitHub authentication,
   Docker, the proposed tag, the Unreleased changelog, and the checked-in model
   catalog.
2. `bump` creates `release/<tag>`, updates the workspace version and lockfile,
   updates the current release sentence in `README.md`, rolls the changelog,
   and commits the four files as `release: <tag>`.
3. `review` prints the new changelog section and the commits since the previous
   tag. It asks for the one release confirmation.
4. `gate` runs the macOS verification, both Linux verification lanes, the V1
   candidate gate, and workspace documentation with warnings denied.
5. `land` pushes the release branch, creates or reuses its pull request,
   enables squash auto-merge, waits for the merge, and fast-forwards local
   `main`.
6. `tag` creates an annotated tag at the landed commit and pushes it.
7. `publish` waits for the GitHub Release workflow, verifies the exact asset
   set, and replaces generated notes with the curated changelog section.
8. `install-check` downloads the published installer into a temporary isolated
   root and checks the installed `verlet --version` output.
9. `tap` dispatches the Homebrew tap workflow, waits for it, verifies the
   formula, and writes `dist/release/<tag>/receipt.json`.

GitHub Actions remains the publisher of record for release binaries. The
button drives the workflow and waits for its result.

## Flags

`--yes` accepts the review confirmation. Use it only after reviewing the
printed changelog and commit list elsewhere.

`--dry-run` prints all nine steps and their commands. It does not modify the
worktree, push, create a pull request, create a tag, dispatch a workflow, or
write a receipt.

`--quick` replaces the V1 candidate gate with a host-target package, archive
smoke, installer smoke, and release manifest. The main verification and docs
gate still run.

`--skip-linux` skips both Docker Linux verification lanes. The macOS lane still
runs.

`--skip-catalog-check` bypasses catalog freshness. Use it only when the
catalog has already been reviewed for this release.

`--allow-empty-changelog` permits an empty Unreleased section.

`--from <step>` resumes at a named step. The step names are `preflight`,
`bump`, `review`, `gate`, `land`, `tag`, `publish`, `install-check`, and `tap`.

`--remote <name>` selects the Git remote. The default is `origin`.

## Human Work

Catalog changes stay manual. Base URLs determine where provider credentials
go. Run `scripts/update-model-catalog.sh`, inspect every change, and commit the
snapshot separately before starting the release button. The preflight creates
a temporary catalog refresh and stops if the checked-in snapshot differs.

The release confirmation is the only prompt. Read the complete changelog
section and the commit list before answering yes. Pass `--yes` when that review
has already happened.

## Resume After Failure

Fix the failing condition before resuming. Pass the name of the failed step:

```sh
scripts/release.sh v0.6.0 --from gate
scripts/release.sh v0.6.0 --from land
scripts/release.sh v0.6.0 --from publish
scripts/release.sh v0.6.0 --from tap
```

The button checks the required earlier state before continuing. `land` reuses
an existing release branch and pull request. `tag` accepts a tag already at the
expected commit. `publish` skips the workflow watch when the GitHub release
already exists. `tap` skips dispatch when the formula already names the
release.

Use these recovery points:

- A local test or docs failure resumes from `gate`.
- A push, pull request, or merge failure resumes from `land`.
- A tag creation or tag push failure resumes from `tag`.
- A release workflow, asset, or release-notes failure resumes from `publish`.
- A published installer failure resumes from `install-check` after the release
  is repaired.
- A Homebrew workflow or formula failure resumes from `tap`.

Do not move a tag that was published from the wrong commit. Repair the release
state explicitly, then resume.

## Binary Targets

GitHub Releases publish one archive per target:

```text
verlet-<version>-x86_64-unknown-linux-gnu.tar.gz
verlet-<version>-aarch64-unknown-linux-gnu.tar.gz
verlet-<version>-x86_64-apple-darwin.tar.gz
verlet-<version>-aarch64-apple-darwin.tar.gz
```

Each archive contains the public process entrypoints:

```text
verlet
verlet-acp-agent
verlet-mcp-server
```

The release also includes each archive checksum, `latest.json`, and
`install.sh`. The installer selects the correct archive, verifies its SHA-256
checksum, and links the binaries into the selected bin directory.

Install the latest stable release:

```sh
curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/latest/download/install.sh | sh
```

Install a release candidate directly:

```sh
curl -fsSL https://github.com/emotionscientific/verlet-kernel/releases/download/v0.6.0-rc.1/install.sh \
  | sh -s -- --version 0.6.0-rc.1 --repo emotionscientific/verlet-kernel
```

After installation, the normal local entrypoint is `verlet chat`. The
foreground server entrypoint is `verlet serve`.

## Local Packaging

Build the host target:

```sh
scripts/package-release-binary.sh --out-dir dist
```

Packaging builds the bundled Svelte console with Bun and includes
`share/verlet/console/*` in the release archive. Use
`scripts/build-console-assets.sh` to rebuild only the UI assets during local
iteration.

Build a specific target:

```sh
scripts/package-release-binary.sh \
  --out-dir dist \
  --target aarch64-apple-darwin
```

Smoke a package:

```sh
archive="$(find dist -maxdepth 1 -name 'verlet-*.tar.gz' | head -n 1)"
scripts/smoke-release-archive.sh "$archive"
scripts/smoke-install.sh "$archive"
```

The package smoke verifies the canonical CLI help surface, including
`verlet commands`, `verlet chat --help`, `verlet auth --help`,
`verlet serve --help`, `verlet tool manual --help`, and
`verlet debug rpc --help`.
