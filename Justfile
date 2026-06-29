set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

install-hooks:
    scripts/install-hooks.sh

fmt:
    cargo fmt --all -- --check

check:
    scripts/check-pre-commit.sh

clippy:
    cargo clippy --workspace --all-targets --locked -- -A clippy::all -D clippy::correctness -D clippy::suspicious -D clippy::perf

test:
    cargo test --workspace --all-targets --locked

smoke:
    cargo run --locked --bin cooldis-live-smoke

verify:
    scripts/verify.sh

package-release:
    scripts/package-release-binary.sh

package-release-target target:
    scripts/package-release-binary.sh --target "{{target}}"

check-release-tag tag:
    scripts/check-release-tag.sh "{{tag}}"

write-release-manifest:
    scripts/write-release-manifest.sh

smoke-release archive:
    scripts/smoke-release-archive.sh "{{archive}}"

smoke-install archive:
    scripts/smoke-install.sh "{{archive}}"

release-candidate:
    scripts/release-v1-candidate.sh

ci:
    scripts/check-ci.sh
