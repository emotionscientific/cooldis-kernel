{ inputs, ... }:
{
  perSystem =
    {
      system,
      lib,
      ...
    }:
    let
      pkgs = import inputs.nixpkgs {
        inherit system;
        overlays = [ inputs.rust-overlay.overlays.default ];
      };

      rustToolchain = pkgs.rust-bin.fromRustupToolchainFile ../rust-toolchain.toml;

      craneLib = (inputs.crane.mkLib pkgs).overrideToolchain (_: rustToolchain);
    in
    {
      _module.args = {
        inherit pkgs rustToolchain craneLib;
      };
    };
}
