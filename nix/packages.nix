{ ... }:
{
  perSystem =
    {
      craneLib,
      ...
    }:
    let
      # Version tracks the kernel crate, which is what `verlet --version` reports.
      crateName = craneLib.crateNameFromCargoToml {
        cargoToml = ../crates/verlet-kernel/Cargo.toml;
      };

      commonArgs = {
        src = craneLib.cleanCargoSource ../.;
        strictDeps = true;

        # Deliberately no pkg-config/openssl: reqwest is rustls-only and
        # rusqlite is `bundled`, so SQLite compiles from vendored C with the
        # stdenv compiler. Nothing in the tree links a system library.
      };

      cargoArtifacts = craneLib.buildDepsOnly (commonArgs // { inherit (crateName) pname version; });
    in
    {
      packages.default = craneLib.buildPackage (
        commonArgs
        // {
          inherit cargoArtifacts;
          inherit (crateName) pname version;

          # The kernel crate also declares smoke and support-binary targets that
          # only make sense inside `scripts/verify.sh`; ship the runtime ones.
          cargoExtraArgs = "--locked --package verlet --bin verlet --bin verlet-mcp-server --bin verlet-acp-agent";

          # The suite wants fixtures that cleanCargoSource strips, a
          # wasm32-unknown-unknown cargo build, and network. CI owns it.
          doCheck = false;

          meta.mainProgram = "verlet";
        }
      );

      # `nix flake check` runs treefmt (added by the treefmt-nix module).
      # Clippy and the workspace test suite are intentionally not mirrored here:
      # both need `--all-targets`, which pulls in the JSON/wasm/sqlite fixtures
      # that cleanCargoSource drops, and the tests shell out to cargo and the
      # network. .github/workflows/verify.yml is the gate for those.
    };
}
