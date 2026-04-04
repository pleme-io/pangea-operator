{
  description = "Pangea Operator — Kubernetes controller for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
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
    (import "${substrate}/lib/build/rust/service-flake.nix" {
      inherit nixpkgs substrate forge crate2nix;
    }) {
      inherit self;
      serviceName = "pangea-operator";
      registry = "ghcr.io/pleme-io/pangea-operator";
      packageName = "pangea-operator";
      moduleDir = null;
      nixosModuleFile = null;

      # Runtime dependencies for the operator container image.
      # tofu: runs terraform/opentofu operations on infrastructure templates
      # git: clones git repository template sources
      # busybox: basic shell utilities for workspace management
      extraContents = pkgs: with pkgs; [ opentofu packer git busybox ];
    };
}
