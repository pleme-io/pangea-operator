{
  description = "Pangea Operator — Kubernetes controller for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crate2nix.url = "github:nix-community/crate2nix";
    forge = {
      url = "github:pleme-io/forge";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, substrate, crate2nix, forge }:
    let
      baseOutputs = (import "${substrate}/lib/build/rust/service-flake.nix" {
        inherit nixpkgs substrate forge crate2nix;
      }) {
        inherit self;
        serviceName = "pangea-operator";
        registry = "ghcr.io/pleme-io/pangea-operator";
        packageName = "pangea-operator";
        moduleDir = null;
        nixosModuleFile = null;
      };

      # Add opentofu and git to the Docker image for each system
      addRuntimeDeps = system: let
        pkgs = import nixpkgs { inherit system; };
        targetSystem = if system == "aarch64-darwin" then "aarch64-linux" else "x86_64-linux";
        targetPkgs = import nixpkgs { system = targetSystem; };

        baseImage = baseOutputs.packages.${system}.${"dockerImage-" + (if system == "aarch64-darwin" then "arm64" else "amd64")} or null;
      in if baseImage == null then {} else {
        # Override the Docker image to include opentofu and git
        ${"dockerImage-" + (if system == "aarch64-darwin" then "arm64" else "amd64")} =
          targetPkgs.dockerTools.buildLayeredImage {
            name = "pangea-operator-service";
            tag = "latest";
            architecture = if system == "aarch64-darwin" then "arm64" else "amd64";
            fromImage = baseImage;
            contents = with targetPkgs; [
              opentofu
              git
              busybox  # for basic shell commands
            ];
          };
      };
    in baseOutputs // {
      packages = baseOutputs.packages // {
        "aarch64-darwin" = (baseOutputs.packages."aarch64-darwin" or {}) // (addRuntimeDeps "aarch64-darwin");
        "x86_64-linux" = (baseOutputs.packages."x86_64-linux" or {}) // (addRuntimeDeps "x86_64-linux");
        "aarch64-linux" = (baseOutputs.packages."aarch64-linux" or {}) // (addRuntimeDeps "aarch64-linux");
      };
    };
}
