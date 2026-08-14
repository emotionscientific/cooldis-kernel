{...}: {
  perSystem = {
    pkgs,
    rustToolchain,
    lib,
    commonArgs,
    ...
  }: {
    devShells.default = pkgs.mkShell {
      inherit (commonArgs) buildInputs;

      nativeBuildInputs =
        commonArgs.nativeBuildInputs
        ++ [
          rustToolchain
          pkgs.lldb
          pkgs.sccache
          pkgs.cargo-nextest

          pkgs.just
          pkgs.bun
        ]
        ++ lib.optionals pkgs.stdenv.isLinux [pkgs.wild];

      RUST_SRC_PATH = "${rustToolchain}/lib/rustlib/src/rust/library";

      shellHook =
        ''
          export RUSTC_WRAPPER=sccache
        ''
        + lib.optionalString pkgs.stdenv.isLinux ''
          export RUSTFLAGS="''${RUSTFLAGS:-} -C link-arg=-fuse-ld=wild"
        '';
    };
  };
}
