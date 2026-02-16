{
  description = "Pangea Web - Yew/WASM frontend for infrastructure management";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, fenix }:
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

        # Docker image serving WASM with Hanabi (花火) BFF server
        # Hanabi provides: static file serving, compression, health checks, observability
        # Kubernetes deployment uses read-only root filesystem with tmpfs volumes
        pangeaWebImage = pkgs.dockerTools.buildLayeredImage {
          name = registry;
          tag = version;

          contents = [
            hanabi
            pkgs.cacert
            pkgs.curl
            pkgs.busybox
          ];

          fakeRootCommands = ''
            mkdir -p etc
            echo 'root:x:0:0:System administrator:/root:/bin/sh' > etc/passwd
            echo 'web:x:101:101:web:/app:/sbin/nologin' >> etc/passwd
            echo 'root:x:0:' > etc/group
            echo 'web:x:101:' >> etc/group
          '';

          extraCommands = ''
            # Copy WASM app to /app/static (Hanabi's default static directory)
            mkdir -p app/static
            cp -r ${pangeaWebWasm}/* app/static/

            # Create required directories
            mkdir -p var/log run tmp
            chmod -R 755 app/static
            chmod -R 777 var/log run tmp

            # Create Hanabi config for WASM serving
            mkdir -p app/config
            cat > app/config/hanabi.yaml << 'EOF'
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
EOF
          '';

          config = {
            Cmd = [ "${hanabi}/bin/hanabi" ];
            ExposedPorts = {
              "8080/tcp" = {};
              "8081/tcp" = {};
            };
            Env = [
              "SSL_CERT_FILE=${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt"
              "HANABI_CONFIG=/app/config/hanabi.yaml"
            ];
            WorkingDir = "/app/static";
            User = "web";
            Labels = {
              "org.opencontainers.image.title" = "pangea-web";
              "org.opencontainers.image.description" = "Yew/WASM frontend for Pangea infrastructure management";
              "org.opencontainers.image.source" = "https://github.com/pleme-io/nexus";
              "org.opencontainers.image.vendor" = "Pleme";
              "org.opencontainers.image.version" = version;
            };
          };
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
