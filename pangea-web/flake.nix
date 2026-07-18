{
  description = "Pangea Web - Yew/WASM frontend for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    # Hardened OCI base images (distroless-glibc, no shell, nonroot-by-
    # default) — the SAME fleet-wide primitive breathe + pangea-operator's
    # own flake build against (org CLAUDE.md Pillar 8 + hardened-images-by-
    # default).
    substrate = {
      url = "github:pleme-io/substrate";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix, substrate }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs { inherit system; };

        # Fenix WASM toolchain
        fenixPkgs = fenix.packages.${system};
        wasmTarget = "wasm32-unknown-unknown";
        wasmToolchain = fenixPkgs.combine [
          fenixPkgs.stable.cargo
          fenixPkgs.stable.rustc
          fenixPkgs.targets.${wasmTarget}.stable.rust-std
        ];

        # Create rustPlatform with fenix toolchain
        rustPlatform = pkgs.makeRustPlatform {
          cargo = wasmToolchain;
          rustc = wasmToolchain;
        };

        # Build Hanabi (花火) BFF server for serving static files
        # Uses crate2nix for reproducible builds
        hanabiCargoNix = import ../../../platform/hanabi/Cargo.nix { inherit pkgs; };
        hanabi = hanabiCargoNix.rootCrate.build;

        # Version from git
        version = self.shortRev or "dev";

        # Registry configuration
        registryBase = "ghcr.io/pleme-io/nexus";
        toolName = "pangea-web";
        registry = "${registryBase}/${toolName}";

        # Build WASM using rustPlatform with proper cross-compilation
        wasmBuild = rustPlatform.buildRustPackage {
          pname = "pangea-web";
          inherit version;
          src = ./.;

          cargoLock.lockFile = ./Cargo.lock;

          # Cross-compile to WASM
          buildPhase = ''
            export HOME=$TMPDIR
            cargo build --release --target ${wasmTarget}
          '';

          # Skip install phase - we extract manually
          installPhase = ''
            mkdir -p $out/lib
            cp target/${wasmTarget}/release/pangea_web.wasm $out/lib/
          '';

          # Don't run tests (WASM tests require browser)
          doCheck = false;
        };

        # Post-process WASM with wasm-bindgen and wasm-opt
        pangeaWebWasm = pkgs.stdenv.mkDerivation {
          name = "pangea-web-wasm";
          src = ./.;

          nativeBuildInputs = [
            pkgs.wasm-bindgen-cli
            pkgs.binaryen
          ];

          wasmBinary = wasmBuild;

          buildPhase = ''
            mkdir -p out

            # Generate JS bindings
            wasm-bindgen \
              $wasmBinary/lib/pangea_web.wasm \
              --out-dir out \
              --target web \
              --no-typescript

            # Optimize WASM
            wasm-opt -O3 out/pangea_web_bg.wasm -o out/pangea_web_bg.wasm || true
          '';

          installPhase = ''
            mkdir -p $out

            # Copy wasm-bindgen output
            cp out/* $out/

            # Copy static assets
            cp index.html $out/
            cp styles.css $out/
          '';
        };

        # Hardened OCI base images (distroless-glibc: glibc + CA roots +
        # nonroot user, no shell) — the same substrate primitive breathe,
        # pangea-operator, and substrate's own tool-image.nix build against.
        hardened = import "${substrate}/lib/build/oci/hardened-base.nix" { inherit pkgs; };

        # WASM app + Hanabi config, pre-shaped onto the paths hanabi expects
        # (its default static dir + its own config file) — mirrors the
        # nested-`$out` trick pangea-operator's flake uses for its binary,
        # so `mkPackageImage`'s flat `extraContents` list merges them onto
        # the image root at the right paths.
        webStatic = pkgs.runCommand "pangea-web-static" {} ''
          mkdir -p $out/app/static
          cp -r ${pangeaWebWasm}/* $out/app/static/
        '';

        hanabiConfig = pkgs.writeTextDir "app/config/hanabi.yaml" ''
          server:
            static_dir: "/app/static"
            http_port: 8080
            health_port: 8081
            request_timeout_secs: 30
            max_concurrent_connections: 10000

          compression:
            enable_gzip: true
            enable_brotli: true

          preflight:
            enabled: false
            critical_files: []
            index_html_path: "index.html"

          cors:
            allowed_origins:
              - "*"
            allowed_methods:
              - "GET"
              - "POST"
              - "OPTIONS"
            allowed_headers:
              - "Content-Type"
              - "Accept"
            expose_headers: []
            max_age_secs: 3600
            allow_credentials: false
        '';

        # var/log + run stubs, preserved from the pre-hardening image's
        # `extraCommands` (hanabi's own config doesn't reference either path,
        # but this pass preserves the old image's contents verbatim rather
        # than second-guessing them). `/tmp` itself is already provided
        # (mode 1777) by the hardened base's own commonContents.
        webRuntimeDirs = pkgs.runCommand "pangea-web-runtime-dirs" {} ''
          mkdir -p $out/var/log $out/run
        '';

        # Docker image serving WASM with Hanabi (花火) BFF server
        # Hanabi provides: static file serving, compression, health checks, observability
        # Kubernetes deployment uses read-only root filesystem with tmpfs volumes
        #
        # uid/gid 101 preserved from the pre-hardening image (matches
        # charts/pangea-web/values.yaml's securityContext.runAsUser: 101 —
        # Kubernetes' own securityContext governs the real runtime identity
        # regardless of the image's baked-in User, but this keeps the image
        # self-consistent for non-Helm `docker run` use too).
        pangeaWebImage = hardened.mkPackageImage {
          service = "pangea-web";
          base = hardened.bases.distroless-glibc;
          package = hanabi;
          publishName = registry;
          publishTag = version;
          entrypoint = [ "${hanabi}/bin/hanabi" ];
          user = "101:101";
          workdir = "/app/static";
          env = [
            "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
            "HANABI_CONFIG=/app/config/hanabi.yaml"
          ];
          exposedPorts = {
            "8080/tcp" = {};
            "8081/tcp" = {};
          };
          # curl + busybox preserved from the pre-hardening image's `contents`
          # list verbatim (this pass is a hardening swap only — trimming them
          # would be a separate, deliberate minimization decision, not this
          # one). Note this does put a shell (busybox) back onto an otherwise
          # shell-less distroless-glibc base.
          extraContents = [ webStatic hanabiConfig webRuntimeDirs pkgs.curl pkgs.busybox ];
          writablePaths = [ "/var/log" "/run" ];
        };

      in {
        # Packages
        packages = {
          default = pangeaWebWasm;
          pangea-web = pangeaWebWasm;
          pangea-web-image = pangeaWebImage;
          wasm-build = wasmBuild;
        };

        # Development shell
        devShells.default = pkgs.mkShell {
          buildInputs = [
            wasmToolchain
            pkgs.wasm-bindgen-cli
            pkgs.binaryen
            pkgs.trunk
            pkgs.cargo-watch
            pkgs.python3
          ];

          shellHook = ''
            echo "Pangea Web Development Environment"
            echo ""
            echo "Target: wasm32-unknown-unknown"
            echo "Tools: cargo, wasm-bindgen, wasm-opt, trunk"
            echo ""
            echo "Quick Start:"
            echo "  trunk serve    - Start dev server with hot reload"
            echo "  trunk build    - Build for production"
            echo "  python -m http.server -d dist 8080  - Serve dist folder"
            echo ""
          '';

          CARGO_BUILD_TARGET = "wasm32-unknown-unknown";
        };
      }
    );
}
