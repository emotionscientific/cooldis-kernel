{...}: {
  perSystem = {...}: {
    treefmt.config = {
      projectRootFile = "flake.nix";

      programs = {
        rustfmt.enable = true;
        alejandra.enable = true;
        taplo.enable = true;
      };
    };
  };
}
