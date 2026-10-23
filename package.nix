{
  lib,
  rustPlatform,
  makeBinaryWrapper,
  nix,
}: let
  cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
in
  rustPlatform.buildRustPackage {
    pname = cargoToml.package.name;
    version = cargoToml.package.version;

    src = lib.cleanSource ./.;

    cargoLock = {
      lockFile = ./Cargo.lock;
    };

    nativeBuildInputs = [
      makeBinaryWrapper
    ];

    postFixup = ''
      wrapProgram $out/bin/dtmgr \
        --prefix PATH : ${lib.makeBinPath [nix]} \
        --add-flags "--nix"
    '';

    meta = {
      mainProgram = "dtmgr";
    };
  }
