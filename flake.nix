{
  description = "Pangea - Full-stack Rust platform for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-25.11";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # Reference nexus flake for nexus-deploy
    nexus.url = "path:../../..";
    devenv = {
      url = "github:cachix/devenv";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, crane, fenix, nexus }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Import individual flakes
        operatorFlake = (import ./pangea-operator/flake.nix).outputs {
          inherit self nixpkgs flake-utils;
          rust-overlay = rust-overlay;
          crane = crane;
          nexus = nexus;
        };

        cliFlake = (import ./pangea-cli/flake.nix).outputs {
          inherit self nixpkgs flake-utils;
          rust-overlay = rust-overlay;
          crane = crane;
          nexus = nexus;
        };

        webFlake = (import ./pangea-web/flake.nix).outputs {
          inherit self nixpkgs flake-utils fenix nexus;
        };

      in {
        # Aggregate packages from all subflakes
        packages = {
          default = operatorFlake.packages.${system}.default or null;

          # Operator
          pangea-operator = operatorFlake.packages.${system}.pangea-operator or null;
          pangea-operator-image = operatorFlake.packages.${system}.pangea-operator-image or null;

          # CLI
          pangea-cli = cliFlake.packages.${system}.pangea-cli or null;

          # Web
          pangea-web = webFlake.packages.${system}.pangea-web or null;
          pangea-web-image = webFlake.packages.${system}.pangea-web-image or null;
        };

        # Aggregate apps
        apps = {
          # Operator apps
          operator-build = operatorFlake.apps.${system}.build or null;
          operator-push = operatorFlake.apps.${system}.push or null;
          operator-release = operatorFlake.apps.${system}.release or null;
          operator-crds = operatorFlake.apps.${system}.crds or null;

          # Web apps
          web-build = webFlake.apps.${system}.build or null;
          web-push = webFlake.apps.${system}.push or null;
          web-release = webFlake.apps.${system}.release or null;

          # Full platform release
          release-all = {
            type = "app";
            program = toString (pkgs.writeShellScript "pangea-release-all" ''
              set -euo pipefail

              echo "Pangea Platform Release"
              echo "$(printf '=%.0s' {1..50})"
              echo ""

              REPO_ROOT=$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")
              PANGEA_DIR="$REPO_ROOT/pkgs/products/pangea"

              # Release operator
              echo "1/2: Releasing pangea-operator..."
              cd "$PANGEA_DIR/pangea-operator"
              nix run .#release

              echo ""

              # Release web
              echo "2/2: Releasing pangea-web..."
              cd "$PANGEA_DIR/pangea-web"
              nix run .#release

              echo ""
              echo "All components released successfully!"
            '');
          };
        };

        # Aggregate dev shells
        devShells = {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              # Rust toolchain
              (rust-bin.stable.latest.default.override {
                targets = [ "x86_64-unknown-linux-musl" "aarch64-unknown-linux-musl" "wasm32-unknown-unknown" ];
              })
              rust-analyzer

              # Build tools
              cargo-watch
              cargo-audit
              wasm-bindgen-cli
              binaryen
              trunk

              # Kubernetes
              kubectl
              kubernetes-helm
              k9s

              # Database
              postgresql_16
              sqlx-cli

              # Infrastructure
              opentofu

              # Utilities
              protobuf
              grpcurl
            ];

            shellHook = ''
              echo "Pangea Development Environment"
              echo ""
              echo "Components:"
              echo "  pangea-operator - Kubernetes operator with GraphQL API"
              echo "  pangea-cli      - Command-line client"
              echo "  pangea-web      - Yew/WASM frontend"
              echo "  pangea-types    - Shared types"
              echo ""
              echo "Quick Start:"
              echo "  cargo build                    - Build all components"
              echo "  cargo test                     - Run tests"
              echo "  cd pangea-operator && cargo run  - Run operator locally"
              echo "  cd pangea-web && trunk serve     - Run web dev server"
              echo ""
            '';

            RUST_LOG = "debug,pangea=trace";
            DATABASE_URL = "postgres://localhost/pangea_state";
          };

          # Individual component shells
          operator = operatorFlake.devShells.${system}.default or pkgs.mkShell {};
          web = webFlake.devShells.${system}.default or pkgs.mkShell {};
        };

        # Aggregate checks
        checks = operatorFlake.checks.${system} or {};
      }
    );
}
