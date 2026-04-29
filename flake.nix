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
  };

  outputs = { self, nixpkgs, substrate, crate2nix, forge }:
    let
      # The substrate-driven service flake outputs (operator binary +
      # operator OCI image + standard release apps). Captured as a
      # value so we can extend its `apps` + `packages` with sibling
      # targets for the pangea-compiler image.
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

      # Per-system extension: the pangea-compiler Ruby Sinatra image
      # built via dockerTools, plus matching push apps that mirror
      # the operator's release shape (skopeo via forge).
      compilerExtension = system:
        let
          pkgs = import nixpkgs { inherit system; };
          imageAmd64 = (import nixpkgs { system = "x86_64-linux"; }).callPackage ./pangea-compiler/image.nix { };
          imageArm64 = (import nixpkgs { system = "aarch64-linux"; }).callPackage ./pangea-compiler/image.nix { };
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

      # Merge per-system extensions onto the base service-flake output
      # via lib.recursiveUpdate so we don't clobber the operator's
      # existing packages/apps.
      lib = nixpkgs.lib;
      extended = lib.foldl' (acc: sys: lib.recursiveUpdate acc (compilerExtension sys))
        base
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    in extended;
}
