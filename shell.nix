{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = [
    pkgs.openssl
    pkgs.pkg-config
  ];

  shellHook = ''
    echo "zero-mysql development environment"
  '';
}
