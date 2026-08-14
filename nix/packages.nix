{...}: {
  perSystem = {
    pkgs,
    craneLib,
    ...
  }: let
    src = craneLib.cleanCargoSource ../.;

    crateName = craneLib.crateNameFromCargoToml {
      cargoToml = ../crates/verlet-kernel/Cargo.toml;
    };

    commonArgs = {
      inherit src;
      inherit (crateName) pname version;
      strictDeps = true;

      nativeBuildInputs = with pkgs; [];
      buildInputs = with pkgs; [];
    };

    cargoArtifacts = craneLib.buildDepsOnly commonArgs;
  in {
    packages.default = craneLib.buildPackage (
      commonArgs
      // {
        inherit cargoArtifacts;

        cargoExtraArgs = "--locked --package verlet --bin verlet --bin verlet-mcp-server --bin verlet-acp-agent";

        # cleanCargoSource strips files needed for checks
        doCheck = false;

        meta.mainProgram = "verlet";
      }
    );

    _module.args = {inherit commonArgs;};
  };
}
