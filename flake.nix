{
  description = "Rust devshell";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-24.11";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = import nixpkgs {
          inherit system overlays;
        };
        overlays = [
          (import rust-overlay)
        ];
      in
      {
        devShells.default =
          with pkgs;
          mkShell {
            buildInputs = [
              mpd
              clang
              cmake
              llvmPackages.bintools
              rust-analyzer
              rust-bin.beta.latest.default
            ];
          };
      }
    );
}
