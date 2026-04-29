# pangea-compiler — Ruby Sinatra OCI image built via Nix.
#
# Consumed by pangea-operator's flake.nix as a sibling dockerImage
# target. Mirrors the substrate operator-image shape but for Ruby:
# bundix-resolved gemset → bundlerEnv → buildLayeredImage.

{ pkgs, system ? pkgs.system }:

let
  # Resolve the gemset.nix produced by `bundix`. `pangea-compiler` is the
  # gem name from pangea-compiler.gemspec, which Bundler treats as the
  # "current gem" — bundlerEnv handles it via the gemspec attr below.
  pangeaCompiler = pkgs.bundlerEnv {
    name = "pangea-compiler-env";
    ruby = pkgs.ruby_3_3;
    gemfile = ./Gemfile;
    lockfile = ./Gemfile.lock;
    gemset = ./gemset.nix;
    gemdir = ./.;
    # The Gemfile references the local gemspec via `gemspec` directive;
    # bundlerEnv picks it up automatically given gemdir.
  };

  # Tiny entrypoint that activates the bundler env + execs the Sinatra
  # app on the conventional /app/app.rb path.
  entrypoint = pkgs.writeShellScript "pangea-compiler-entrypoint" ''
    exec ${pangeaCompiler}/bin/bundle exec ruby /app/app.rb -o 0.0.0.0 -p 8082 "$@"
  '';

  # Stage the app source under /app inside the image so __FILE__ +
  # require_relative resolve the way the Ruby Sinatra app expects.
  appSource = pkgs.runCommand "pangea-compiler-source" { } ''
    mkdir -p $out/app
    cp -r ${./.}/. $out/app/
    # bundix-built paths win — drop the bundler vendor dir to avoid drift.
    rm -rf $out/app/vendor $out/app/.bundle 2>/dev/null || true
  '';
in
pkgs.dockerTools.buildLayeredImage {
  name = "pangea-compiler";
  # Static tag — `forge push --auto-tags` reads the git SHA from
  # the source tree at push time and stamps amd64-<sha>+amd64-latest.
  tag = "latest";

  contents = [
    pangeaCompiler
    appSource
    pkgs.coreutils
    pkgs.bashInteractive
    pkgs.cacert
  ];

  config = {
    Entrypoint = [ "${entrypoint}" ];
    ExposedPorts = { "8082/tcp" = { }; };
    Env = [
      "PATH=${pangeaCompiler}/bin:${pkgs.coreutils}/bin"
      "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
      "BUNDLE_PATH=${pangeaCompiler}/lib/ruby/gems/3.3.0"
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
