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

    # Pangea Ruby gems consumed by pangea-compiler. Treated as
    # source-only flake inputs (`flake = false`) — substrate's
    # ruby-workspace builder rewrites the bundix gemset to source
    # them as path-gems from these inputs at evaluation time.
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
      # ── Operator (Rust) — substrate service-flake ────────────────
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

      # ── Compiler (Ruby) — substrate ruby-workspace + dockerTools ─
      # Per-system extension. The ruby-workspace builder reads
      # pangea-compiler/gemset.nix and rewrites the 12 path-gem
      # entries to point at the corresponding flake input's source
      # tree (no manual `vendor/` dir needed; no per-build clone).
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

      compilerExtension = system:
        let
          pkgs = import nixpkgs { inherit system; };
          imageFor = imageSystem:
            (import nixpkgs { system = imageSystem; }).callPackage ./pangea-compiler/image.nix {
              inherit ruby-nix substrate forge;
              pangeaInputs = pangeaInputs;
            };
          imageAmd64 = imageFor "x86_64-linux";
          imageArm64 = imageFor "aarch64-linux";
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

      lib = nixpkgs.lib;
      extended = lib.foldl' (acc: sys: lib.recursiveUpdate acc (compilerExtension sys))
        base
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    in extended;
}
