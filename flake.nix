{
  description = "Pangea Operator — Kubernetes controller for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crate2nix = {
      url = "github:nix-community/crate2nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    ruby-nix.url = "github:inscapist/ruby-nix";

    # 12 path-gem source-only inputs for pangea-compiler.
    pangea-akeyless      = { url = "github:pleme-io/pangea-akeyless";      flake = false; };
    pangea-architectures = { url = "github:pleme-io/pangea-architectures"; flake = false; };
    pangea-aws           = { url = "github:pleme-io/pangea-aws";           flake = false; };
    pangea-azure         = { url = "github:pleme-io/pangea-azure";         flake = false; };
    pangea-cloudflare    = { url = "github:pleme-io/pangea-cloudflare";    flake = false; };
    pangea-core          = { url = "github:pleme-io/pangea-core";          flake = false; };
    pangea-datadog       = { url = "github:pleme-io/pangea-datadog";       flake = false; };
    pangea-gcp           = { url = "github:pleme-io/pangea-gcp";           flake = false; };
    pangea-hcloud        = { url = "github:pleme-io/pangea-hcloud";        flake = false; };
    pangea-kubernetes    = { url = "github:pleme-io/pangea-kubernetes";    flake = false; };
    pangea-splunk        = { url = "github:pleme-io/pangea-splunk";        flake = false; };
    pangea-spot          = { url = "github:pleme-io/pangea-spot";          flake = false; };
  };

  outputs = inputs@{ self, nixpkgs, substrate, crate2nix, forge, ruby-nix, ... }:
    let
      lib = nixpkgs.lib;

      # Operator (Rust) — substrate service-flake outputs.
      base = (import "${substrate}/lib/build/rust/service-flake.nix" {
        inherit nixpkgs substrate forge crate2nix;
      }) {
        inherit self;
        serviceName = "pangea-operator";
        registry = "ghcr.io/pleme-io/pangea-operator";
        packageName = "pangea-operator";
        moduleDir = null;
        nixosModuleFile = null;
        extraContents = pkgs: with pkgs; [ opentofu packer git busybox ];
      };

      pangeaInputs = {
        "pangea-akeyless"      = inputs.pangea-akeyless;
        "pangea-architectures" = inputs.pangea-architectures;
        "pangea-aws"           = inputs.pangea-aws;
        "pangea-azure"         = inputs.pangea-azure;
        "pangea-cloudflare"    = inputs.pangea-cloudflare;
        "pangea-core"          = inputs.pangea-core;
        "pangea-datadog"       = inputs.pangea-datadog;
        "pangea-gcp"           = inputs.pangea-gcp;
        "pangea-hcloud"        = inputs.pangea-hcloud;
        "pangea-kubernetes"    = inputs.pangea-kubernetes;
        "pangea-splunk"        = inputs.pangea-splunk;
        "pangea-spot"          = inputs.pangea-spot;
      };

      # Compiler (Ruby) — call substrate ruby-workspace at output-level
      # for each Linux system we need, build a docker image from the
      # resulting bundlerEnv.
      mkCompilerImage = imageSystem:
        let
          imagePkgs = import nixpkgs { system = imageSystem; };
          ws = (import "${substrate}/lib/build/ruby/workspace.nix" {
            nixpkgs = nixpkgs;
            system = imageSystem;
            inherit ruby-nix substrate forge;
          }) {
            name = "pangea-compiler";
            self = ./pangea-compiler;
            pathGems = pangeaInputs;
            gemsetPath = "/gemset.nix";
          };

          # Materialise the 12 path-gems into /app/vendor/ so that
          # `bundle exec` at runtime — which walks Gemfile.lock and
          # verifies every `path: vendor/<gem>` entry exists on disk —
          # finds them. The Nix-built bundlerEnv already resolved every
          # gem at build time; this is purely to satisfy bundler's
          # runtime path check.
          #
          # Bundler's load-gemspec phase evaluates each .gemspec inline
          # — and every pangea-* gemspec computes `spec.files` via
          #   `git ls-files -z`.split("\x0")
          # If git fails, bundler still produces a Definition but the
          # downstream specs_changed? walk crashes with Errno::ENOENT.
          # Symlinks to read-only flake-input store paths cannot host a
          # `.git` dir, so copy the trees and git-init each one with a
          # single bootstrap commit so `git ls-files` returns a real
          # file list at runtime.
          appSource = imagePkgs.runCommand "pangea-compiler-source" {
            buildInputs = [ imagePkgs.git ];
          } ''
            mkdir -p $out/app
            cp -r ${./pangea-compiler}/. $out/app/
            chmod -R u+w $out/app
            rm -rf $out/app/vendor $out/app/.bundle 2>/dev/null || true
            mkdir -p $out/app/vendor

            export GIT_AUTHOR_NAME=pangea-compiler
            export GIT_AUTHOR_EMAIL=ops@pleme.io
            export GIT_COMMITTER_NAME=pangea-compiler
            export GIT_COMMITTER_EMAIL=ops@pleme.io
            export HOME=$TMPDIR

            ${lib.concatStringsSep "\n" (lib.mapAttrsToList (gemName: src: ''
              cp -r ${src} $out/app/vendor/${gemName}
              chmod -R u+w $out/app/vendor/${gemName}
              ( cd $out/app/vendor/${gemName} && \
                git init -q -b main && \
                git add -A && \
                git -c commit.gpgsign=false commit -q --allow-empty -m bootstrap )
            '') pangeaInputs)}
          '';

          entrypoint = imagePkgs.writeShellScript "pangea-compiler-entrypoint" ''
            export PATH="${ws.env}/bin:${imagePkgs.coreutils}/bin:${imagePkgs.git}/bin:''${PATH:-}"
            export RUBYLIB="${ws.rubylib}:''${RUBYLIB:-}"
            export DRY_TYPES_WARNINGS=false
            export PANGEA_WORKSPACE_BASE="''${PANGEA_WORKSPACE_BASE:-/var/pangea/workspaces}"
            cd /app
            exec ${ws.env}/bin/bundle exec ruby /app/app.rb -o 0.0.0.0 -p 8082 "$@"
          '';
        in imagePkgs.dockerTools.buildLayeredImage {
          name = "pangea-compiler";
          tag = "latest";
          # `git` is needed at startup: every pangea-*.gemspec computes its
          # spec.files list via `git ls-files`. Without it, bundler's
          # converge-paths phase crashes with `Errno::ENOENT - git`. Add
          # before the entrypoint so the binary is on PATH from layer 0.
          contents = [ ws.env appSource imagePkgs.coreutils imagePkgs.bashInteractive imagePkgs.cacert imagePkgs.git ];
          config = {
            Entrypoint = [ "${entrypoint}" ];
            ExposedPorts = { "8082/tcp" = { }; };
            Env = [
              "PATH=${ws.env}/bin:${imagePkgs.coreutils}/bin:${imagePkgs.git}/bin"
              "SSL_CERT_FILE=${imagePkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              "PANGEA_WORKSPACE_BASE=/var/pangea/workspaces"
            ];
            Labels = {
              "org.opencontainers.image.source" = "https://github.com/pleme-io/pangea-operator";
              "org.opencontainers.image.description" = "Pangea Ruby DSL compiler sidecar";
              "org.opencontainers.image.licenses" = "MIT";
            };
            User = "65534:65534";
            WorkingDir = "/app";
          };
        };

      compilerExtension = system:
        let
          pkgs = import nixpkgs { inherit system; };
          imageAmd64 = mkCompilerImage "x86_64-linux";
          imageArm64 = mkCompilerImage "aarch64-linux";
          mkPushApp = imagePath: archTag: pkgs.writeShellScript "pangea-compiler-push-${archTag}" ''
            set -euo pipefail
            export GITHUB_TOKEN="''${GITHUB_TOKEN:-''${GHCR_TOKEN:-$(cat "$HOME/.config/github/token" 2>/dev/null || true)}}"
            export GHCR_TOKEN="$GITHUB_TOKEN"
            echo "📦 Pushing pangea-compiler-${archTag} → ghcr.io/pleme-io/pangea-compiler"
            exec ${forge.packages.${system}.default}/bin/forge push \
              --image-path "${imagePath}" \
              --registry "ghcr.io/pleme-io/pangea-compiler" \
              --auto-tags \
              --retries 3
          '';
        in {
          packages.${system} = {
            dockerImage-compiler-amd64 = imageAmd64;
            dockerImage-compiler-arm64 = imageArm64;
          };
          apps.${system} = {
            push-image-compiler-amd64 = {
              type = "app";
              program = toString (mkPushApp imageAmd64 "amd64");
            };
            push-image-compiler-arm64 = {
              type = "app";
              program = toString (mkPushApp imageArm64 "arm64");
            };
          };
        };

      extended = lib.foldl' (acc: sys: lib.recursiveUpdate acc (compilerExtension sys))
        base
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    in extended;
}
