# pangea-compiler — Ruby Sinatra OCI image built via the canonical
# substrate ruby-workspace pattern (no manual vendor/ dir).
#
# Inputs:
#   pkgs           — Linux nixpkgs (x86_64-linux for amd64; aarch64-linux for arm64)
#   ruby-nix       — flake input from inscapist/ruby-nix (substrate convention)
#   substrate      — flake input
#   forge          — flake input (unused here but threaded through for symmetry)
#   pangeaInputs   — { "pangea-core" = <flake-input-src>; ... } map; the 12 path-gems
#
# The substrate workspace builder reads gemset.nix and rewrites every
# entry whose name appears in pangeaInputs to source-from-path against
# the corresponding flake input. Bundler then resolves locally with no
# vendor/ dir and no clones at build time.

{ pkgs
, ruby-nix
, substrate
, forge
, pangeaInputs
, system ? pkgs.system
}:

let
  # substrate's workspace.nix takes `nixpkgs` as a path/source so it
  # can `import { system; overlays; }`. Pass pkgs.path which is the
  # already-evaluated nixpkgs source tree.
  ws = (import "${substrate}/lib/build/ruby/workspace.nix" {
    nixpkgs = pkgs.path;
    inherit system ruby-nix substrate forge;
  }) {
    name = "pangea-compiler";
    self = ./.;          # the pangea-compiler subdir contains gemset.nix + Gemfile + app.rb
    pathGems = pangeaInputs;
    gemsetPath = "/gemset.nix";
  };

  env = ws.env;

  # The Sinatra app source — staged at /app inside the image so
  # __FILE__ resolves the way app.rb expects. RUBYLIB augments the
  # gem load path with the path-gems' lib/ dirs so `require
  # 'pangea-core'` works without rebundling.
  rubylibEnv = ws.rubylib;

  entrypoint = pkgs.writeShellScript "pangea-compiler-entrypoint" ''
    export RUBYLIB="${rubylibEnv}:''${RUBYLIB:-}"
    export DRY_TYPES_WARNINGS=false
    export PANGEA_WORKSPACE_BASE="''${PANGEA_WORKSPACE_BASE:-/var/pangea/workspaces}"
    cd /app
    exec ${env}/bin/bundle exec ruby /app/app.rb -o 0.0.0.0 -p 8082 "$@"
  '';

  appSource = pkgs.runCommand "pangea-compiler-source" { } ''
    mkdir -p $out/app
    cp -r ${./.}/. $out/app/
    rm -rf $out/app/vendor $out/app/.bundle 2>/dev/null || true
  '';
in
pkgs.dockerTools.buildLayeredImage {
  name = "pangea-compiler";
  tag = "latest";

  contents = [
    env
    appSource
    pkgs.coreutils
    pkgs.bashInteractive
    pkgs.cacert
  ];

  config = {
    Entrypoint = [ "${entrypoint}" ];
    ExposedPorts = { "8082/tcp" = { }; };
    Env = [
      "PATH=${env}/bin:${pkgs.coreutils}/bin"
      "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
      "PANGEA_WORKSPACE_BASE=/var/pangea/workspaces"
    ];
    Labels = {
      "org.opencontainers.image.source" = "https://github.com/pleme-io/pangea-operator";
      "org.opencontainers.image.description" = "Pangea Ruby DSL compiler sidecar (template_path mode)";
      "org.opencontainers.image.licenses" = "MIT";
    };
    User = "65534:65534";
    WorkingDir = "/app";
  };
}
