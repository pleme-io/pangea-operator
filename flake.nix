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

          appSource = imagePkgs.runCommand "pangea-compiler-source" { } ''
            mkdir -p $out/app
            cp -r ${./pangea-compiler}/. $out/app/
            rm -rf $out/app/vendor $out/app/.bundle 2>/dev/null || true
          '';

          entrypoint = imagePkgs.writeShellScript "pangea-compiler-entrypoint" ''
            export RUBYLIB="${ws.rubylib}:''${RUBYLIB:-}"
            export DRY_TYPES_WARNINGS=false
            export PANGEA_WORKSPACE_BASE="''${PANGEA_WORKSPACE_BASE:-/var/pangea/workspaces}"
            cd /app
            exec ${ws.env}/bin/bundle exec ruby /app/app.rb -o 0.0.0.0 -p 8082 "$@"
          '';
        in imagePkgs.dockerTools.buildLayeredImage {
          name = "pangea-compiler";
          tag = "latest";
          contents = [ ws.env appSource imagePkgs.coreutils imagePkgs.bashInteractive imagePkgs.cacert ];
          config = {
            Entrypoint = [ "${entrypoint}" ];
            ExposedPorts = { "8082/tcp" = { }; };
            Env = [
              "PATH=${ws.env}/bin:${imagePkgs.coreutils}/bin"
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
