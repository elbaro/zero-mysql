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
}
