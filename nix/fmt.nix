{ ... }:
{
  perSystem =
    { ... }:
    {
      treefmt.config = {
        projectRootFile = "flake.nix";

        programs = {
          nixfmt.enable = true;
          taplo.enable = true;
        };

        # No rustfmt here. treefmt applies one edition to every file, but
        # verlet-sqlite is edition 2021 and the rest of the workspace is 2024,
        # so a single setting always disagrees with somebody. `cargo fmt` reads
        # each crate's edition and is already gated by scripts/verify.sh.
      };
    };
}
