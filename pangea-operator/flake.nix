{
  description = "Pangea Operator - Rust-based Kubernetes operator for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
    crane = {
      url = "github:ipetkov/crane";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # Reference nexus flake for nexus-deploy and nix-lib
    nexus.url = "path:../../../..";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane, nexus }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        # Get nexus-deploy from nexus flake
        nexusDeploy = nexus.packages.${system}.nexus-deploy or pkgs.hello;
        nexusDeployCmd = "${nexusDeploy}/bin/nexus-deploy";

        # Centralized tokens
        defaultAtticToken = let
          envToken = builtins.getEnv "ATTIC_TOKEN";
        in
          if envToken != ""
          then envToken
          else "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjE3OTE5Nzg4ODQsIm5iZiI6MTc2MDQyMTI4NCwic3ViIjoibmV4dXMtY2kiLCJodHRwczovL2p3dC5hdHRpYy5ycy92MSI6eyJjYWNoZXMiOnsiKiI6eyJyIjoxLCJ3IjoxLCJjYyI6MSwiY3IiOjF9fX19.QuIglOUwZWUsmBS_frqtd4s3w3jybRT-q_PqM2ooaWo";

        defaultGhcrToken = let
          ghcrToken = builtins.getEnv "GHCR_TOKEN";
          githubToken = builtins.getEnv "GITHUB_TOKEN";
        in
          if ghcrToken != ""
          then ghcrToken
          else if githubToken != ""
          then githubToken
          else "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";

        # Runtime tools environment helper
        mkRuntimeToolsEnv = { tools ? [] }:
          let
            runtimeTools = {
              skopeo = { package = pkgs.skopeo; binary = "skopeo"; };
              attic = { package = pkgs.attic-client; binary = "attic"; };
              git = { package = pkgs.git; binary = "git"; };
              docker = { package = pkgs.docker; binary = "docker"; };
            };
            mkExport = toolName:
              let
                tool = runtimeTools.${toolName};
                envVarName = "${pkgs.lib.toUpper toolName}_BIN";
                toolPath = "${tool.package}/bin/${tool.binary}";
              in
                "export ${envVarName}=\"${toolPath}\"";
          in
            pkgs.lib.concatMapStringsSep "\n" mkExport tools;

        # Use stable Rust toolchain with MUSL targets
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          targets = [ "x86_64-unknown-linux-musl" "aarch64-unknown-linux-musl" ];
        };

        # Set up crane with our toolchain
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # Common source filtering (include proto files)
        src = pkgs.lib.cleanSourceWith {
          src = craneLib.path ./.;
          filter = path: type:
            (craneLib.filterCargoSources path type) ||
            (pkgs.lib.hasSuffix ".proto" path);
        };

        # Common build arguments
        commonArgs = {
          inherit src;
          strictDeps = true;
          pname = "pangea-operator";
          version = self.shortRev or "dev";

          # Static linking for minimal container images
          CARGO_BUILD_TARGET = "x86_64-unknown-linux-musl";
          CARGO_BUILD_RUSTFLAGS = "-C target-feature=+crt-static";

          nativeBuildInputs = with pkgs; [
            pkg-config
            protobuf  # For gRPC proto compilation
          ];

          buildInputs = with pkgs; [
            pkgsStatic.openssl
          ] ++ pkgs.lib.optionals pkgs.stdenv.isDarwin [
            pkgs.libiconv
            pkgs.darwin.apple_sdk_11_0.frameworks.Security
            pkgs.darwin.apple_sdk_11_0.frameworks.SystemConfiguration
          ];
        };

        # Build dependencies separately for caching
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Build the binary
        pangeaOperator = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          doCheck = true;
        });

        # Version from git
        version = self.shortRev or "dev";

        # Registry configuration
        registryBase = "ghcr.io/pleme-io/nexus";
        toolName = "pangea-operator";
        registry = "${registryBase}/${toolName}";

        # Create /app directory with single binary
        pangeaApp = pkgs.runCommand "pangea-app" {} ''
          mkdir -p $out/app
          cp ${pangeaOperator}/bin/pangea-operator $out/app/pangea-operator
        '';

        # Docker image with operator binary, OpenTofu, and InSpec
        pangeaImage = pkgs.dockerTools.buildLayeredImage {
          name = registry;
          tag = version;

          contents = [
            pangeaApp
            pkgs.cacert        # CA certificates for TLS
            pkgs.opentofu      # Infrastructure execution
            pkgs.packer        # AMI build execution
            pkgs.tzdata        # Timezone data
          ];

          config = {
            Entrypoint = [ "/app/pangea-operator" ];
            Labels = {
              "org.opencontainers.image.title" = "pangea-operator";
              "org.opencontainers.image.description" = "Rust-based Kubernetes operator for Pangea infrastructure management";
              "org.opencontainers.image.source" = "https://github.com/pleme-io/nexus";
              "org.opencontainers.image.vendor" = "Pleme";
              "org.opencontainers.image.version" = version;
            };
            Env = [
              "RUST_LOG=info,pangea_operator=debug"
              "LOG_FORMAT=json"
              "HEALTH_ADDR=0.0.0.0:8080"
              "METRICS_ADDR=0.0.0.0:9090"
              "GRAPHQL_ADDR=0.0.0.0:8081"
              "GRPC_ADDR=0.0.0.0:50051"
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
            ];
            ExposedPorts = {
              "8080/tcp" = {};   # Health
              "8081/tcp" = {};   # GraphQL
              "9090/tcp" = {};   # Metrics
              "50051/tcp" = {};  # gRPC
            };
          };
        };

      in
      {
        # Packages
        packages = {
          default = pangeaOperator;
          pangea-operator = pangeaOperator;
          pangea-operator-image = pangeaImage;
        };

        # Development shell
        devShells.default = craneLib.devShell {
          packages = with pkgs; [
            rustToolchain
            rust-analyzer
            cargo-watch
            cargo-audit
            cargo-deny
            kubectl
            kubernetes-helm
            k9s
            opentofu
            packer
            protobuf
            grpcurl
            postgresql_16
            sqlx-cli
          ];

          RUST_LOG = "debug,pangea_operator=trace";
          DATABASE_URL = "postgres://localhost/pangea_state";
        };

        # CI checks
        checks = {
          inherit pangeaOperator;

          # Clippy
          clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets -- --deny warnings";
          });

          # Format check
          fmt = craneLib.cargoFmt {
            inherit src;
          };
        };

        # Apps for running locally and CI/CD
        apps = {
          default = flake-utils.lib.mkApp {
            drv = pangeaOperator;
          };

          # Build Docker image
          build = {
            type = "app";
            program = toString (pkgs.writeShellScript "pangea-operator-build" ''
              set -euo pipefail

              export ATTIC_TOKEN="${defaultAtticToken}"
              ${mkRuntimeToolsEnv { tools = ["attic" "git"]; }}

              echo "🔨 Building ${toolName} Docker image..."

              REPO_ROOT=$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")
              WORK_DIR="$REPO_ROOT/pkgs/products/pangea/pangea-operator"

              cd "$WORK_DIR"

              echo "Building pangea-operator-image..."
              nix build .#pangea-operator-image

              if [ -L "result" ]; then
                echo "✅ Docker image built successfully: ./result"
              else
                echo "❌ Build failed - no result symlink created"
                exit 1
              fi

              echo ""
              echo "📦 Next step: Run 'nix run .#push' to push to GitHub Packages"
            '');
          };

          # Push Docker image to GHCR
          push = {
            type = "app";
            program = toString (pkgs.writeShellScript "pangea-operator-push" ''
              set -euo pipefail

              export ATTIC_TOKEN="${defaultAtticToken}"
              export GITHUB_TOKEN="${defaultGhcrToken}"
              export GHCR_TOKEN="${defaultGhcrToken}"
              ${mkRuntimeToolsEnv { tools = ["skopeo" "attic"]; }}

              echo "📤 Pushing ${toolName} to GitHub Packages..."

              REPO_ROOT=$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")
              WORK_DIR="$REPO_ROOT/pkgs/products/pangea/pangea-operator"

              cd "$WORK_DIR"

              if [ ! -L "result" ]; then
                echo "❌ Error: No Docker image found. Run 'nix run .#build' first"
                exit 1
              fi

              exec ${nexusDeployCmd} push \
                --image-path "$(readlink -f result)" \
                --registry "${registry}" \
                --auto-tags \
                --retries 3
            '');
          };

          # Full release workflow
          release = {
            type = "app";
            program = toString (pkgs.writeShellScript "pangea-operator-release" ''
              set -euo pipefail

              export ATTIC_TOKEN="${defaultAtticToken}"
              export GITHUB_TOKEN="${defaultGhcrToken}"
              export GHCR_TOKEN="${defaultGhcrToken}"
              ${mkRuntimeToolsEnv { tools = ["skopeo" "attic" "git"]; }}

              echo "🚀 ${toolName} Release Workflow"
              echo "$(printf '=%.0s' {1..50})"
              echo ""

              REPO_ROOT=$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")
              WORK_DIR="$REPO_ROOT/pkgs/products/pangea/pangea-operator"

              cd "$WORK_DIR"

              # Step 1: Build
              echo "Step 1/2: Building Docker image..."
              nix build .#pangea-operator-image

              if [ ! -L "result" ]; then
                echo "❌ Build failed"
                exit 1
              fi

              echo "✅ Build complete"
              echo ""

              # Step 2: Push
              echo "Step 2/2: Pushing to GitHub Packages..."
              ${nexusDeployCmd} push \
                --image-path "$(readlink -f result)" \
                --registry "${registry}" \
                --auto-tags \
                --retries 3

              echo ""
              echo "✅ Release complete!"
              echo ""
              echo "The ${toolName} is now available at:"
              echo "  docker pull ${registry}:latest"
            '');
          };

          # Generate CRD manifests
          crds = {
            type = "app";
            program = toString (pkgs.writeShellScript "pangea-operator-crds" ''
              set -euo pipefail

              REPO_ROOT=$(${pkgs.git}/bin/git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")
              WORK_DIR="$REPO_ROOT/pkgs/products/pangea/pangea-operator"

              cd "$WORK_DIR"

              echo "📝 Generating CRD manifests..."
              ${pangeaOperator}/bin/pangea-operator --generate-crds > charts/pangea-operator/templates/crds/crds.yaml

              echo "✅ CRDs generated at charts/pangea-operator/templates/crds/crds.yaml"
            '');
          };
        };
      }
    );
}
