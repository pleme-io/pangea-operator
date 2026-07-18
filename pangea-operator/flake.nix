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
    # Hardened OCI base images (distroless-glibc, no shell, nonroot-by-
    # default) — the SAME fleet-wide primitive breathe + tool-image.nix
    # build against (org CLAUDE.md Pillar 8 + hardened-images-by-default).
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane, nexus, substrate }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs {
          inherit system overlays;
          config.allowUnfree = true;
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
          else "";  # env-only (ATTIC_TOKEN) — literal leaked JWT removed 2026-07-18, ROTATE

        defaultGhcrToken = let
          ghcrToken = builtins.getEnv "GHCR_TOKEN";
          githubToken = builtins.getEnv "GITHUB_TOKEN";
        in
          if ghcrToken != ""
          then ghcrToken
          else if githubToken != ""
          then githubToken
          else "";  # env-only (GITHUB_TOKEN/GHCR_TOKEN) — literal leaked PAT removed 2026-07-18, ROTATE

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

        # Hardened OCI base images (distroless-glibc: glibc + CA roots +
        # nonroot user, no shell) — the same substrate primitive breathe
        # and substrate's own tool-image.nix build against.
        hardened = import "${substrate}/lib/build/oci/hardened-base.nix" { inherit pkgs; };

        # Docker image with operator binary, OpenTofu, and Packer.
        #
        # OpenTofu/Packer/tzdata are preserved verbatim from the pre-hardening
        # image as `extraContents` — this pass is a hardening swap only, NOT
        # a removal of the (likely-stale, magma-cutover-predating) tofu/packer
        # bundling. See the org CLAUDE.md's MAGMA-NATIVE EXECUTION directive
        # + this repo's own executor_magma/PANGEA_FORBID_TOFU posture for why
        # that bundling looks like drift; investigated but NOT removed here.
        pangeaImage = hardened.mkPackageImage {
          service = "pangea-operator";
          base = hardened.bases.distroless-glibc;
          package = pangeaOperator;
          publishName = registry;
          publishTag = version;
          entrypoint = [ "${pangeaOperator}/bin/pangea-operator" ];
          env = [
            "RUST_LOG=info,pangea_operator=debug"
            "LOG_FORMAT=json"
            "HEALTH_ADDR=0.0.0.0:8080"
            "METRICS_ADDR=0.0.0.0:9090"
            "GRAPHQL_ADDR=0.0.0.0:8081"
            "GRPC_ADDR=0.0.0.0:50051"
            "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
          ];
          exposedPorts = {
            "8080/tcp" = {};   # Health
            "8081/tcp" = {};   # GraphQL
            "9090/tcp" = {};   # Metrics
            "50051/tcp" = {};  # gRPC
          };
          extraContents = [
            pkgs.opentofu      # Infrastructure execution
            pkgs.packer        # AMI build execution
            pkgs.tzdata        # Timezone data
          ];
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
