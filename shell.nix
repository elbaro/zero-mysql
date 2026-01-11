{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # For criterion benchmarks (plots)
    gnuplot

    # SSL support
    openssl
    pkg-config
  ];

  shellHook = ''
    export DATABASE_URL="mysql://test:1234@localhost/test"
  '';
}
