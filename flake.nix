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

    # Path-gem source-only inputs for pangea-compiler — typed
    # primitive providers + composers.
    #
    # M1 update (2026-04-30, theory/PANGEA-WORKSPACE-RECONCILIATION.md):
    # pangea-architectures is now bundled into the compiler image so
    # the new /v1/architectures RPCs can introspect
    # Pangea::Architectures.constants at startup. Future M7 work
    # reverts to per-CR path-based loading via lib_path RPC argument
    # so workspace SDLC doesn't rebuild the image.
    pangea-akeyless      = { url = "github:pleme-io/pangea-akeyless";      flake = false; };
    pangea-aws           = { url = "github:pleme-io/pangea-aws";           flake = false; };
    pangea-azure         = { url = "github:pleme-io/pangea-azure";         flake = false; };
    pangea-cloudflare    = { url = "github:pleme-io/pangea-cloudflare";    flake = false; };
    pangea-core          = { url = "github:pleme-io/pangea-core";          flake = false; };
    pangea-datadog       = { url = "github:pleme-io/pangea-datadog";       flake = false; };
    pangea-gcp           = { url = "github:pleme-io/pangea-gcp";           flake = false; };
    pangea-hcloud        = { url = "github:pleme-io/pangea-hcloud";        flake = false; };
    pangea-kubernetes    = { url = "github:pleme-io/pangea-kubernetes";    flake = false; };
    pangea-porkbun       = { url = "github:pleme-io/pangea-porkbun";       flake = false; };
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
        "pangea-aws"           = inputs.pangea-aws;
        "pangea-azure"         = inputs.pangea-azure;
        "pangea-cloudflare"    = inputs.pangea-cloudflare;
        "pangea-core"          = inputs.pangea-core;
        "pangea-datadog"       = inputs.pangea-datadog;
        "pangea-gcp"           = inputs.pangea-gcp;
        "pangea-hcloud"        = inputs.pangea-hcloud;
        "pangea-kubernetes"    = inputs.pangea-kubernetes;
        "pangea-porkbun"       = inputs.pangea-porkbun;
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

          # Materialise the 12 path-gems into /app/vendor/ so that
          # `bundle exec` at runtime — which walks Gemfile.lock and
          # verifies every `path: vendor/<gem>` entry exists on disk —
          # finds them. The Nix-built bundlerEnv already resolved every
          # gem at build time; this is purely to satisfy bundler's
          # runtime path check.
          #
          # Bundler's load-gemspec phase evaluates each .gemspec inline
          # — and every pangea-* gemspec computes `spec.files` via
          #   `git ls-files -z`.split("\x0")
          # If git fails, bundler still produces a Definition but the
          # downstream specs_changed? walk crashes with Errno::ENOENT.
          # Symlinks to read-only flake-input store paths cannot host a
          # `.git` dir, so copy the trees and git-init each one with a
          # single bootstrap commit so `git ls-files` returns a real
          # file list at runtime.
          appSource = imagePkgs.runCommand "pangea-compiler-source" {
            buildInputs = [ imagePkgs.git ];
          } ''
            mkdir -p $out/app
            cp -r ${./pangea-compiler}/. $out/app/
            chmod -R u+w $out/app
            rm -rf $out/app/vendor $out/app/.bundle 2>/dev/null || true
            mkdir -p $out/app/vendor

            export GIT_AUTHOR_NAME=pangea-compiler
            export GIT_AUTHOR_EMAIL=ops@pleme.io
            export GIT_COMMITTER_NAME=pangea-compiler
            export GIT_COMMITTER_EMAIL=ops@pleme.io
            export HOME=$TMPDIR

            ${lib.concatStringsSep "\n" (lib.mapAttrsToList (gemName: src: ''
              cp -r ${src} $out/app/vendor/${gemName}
              chmod -R u+w $out/app/vendor/${gemName}
              ( cd $out/app/vendor/${gemName} && \
                git init -q -b main && \
                git add -A && \
                git -c commit.gpgsign=false commit -q --allow-empty -m bootstrap )
            '') pangeaInputs)}
          '';

          # The Sinatra app is a modular rack app — app.rb defines
          # PangeaCompiler < Sinatra::Base but never calls .run!.
          # config.ru rackup-mounts it; puma is the production server
          # (matches the original Dockerfile CMD).
          entrypoint = imagePkgs.writeShellScript "pangea-compiler-entrypoint" ''
            export PATH="${ws.env}/bin:${imagePkgs.coreutils}/bin:${imagePkgs.git}/bin:''${PATH:-}"
            export RUBYLIB="${ws.rubylib}:''${RUBYLIB:-}"
            export DRY_TYPES_WARNINGS=false
            export PANGEA_WORKSPACE_BASE="''${PANGEA_WORKSPACE_BASE:-/var/pangea/workspaces}"
            cd /app
            exec ${ws.env}/bin/bundle exec puma -b tcp://0.0.0.0:8082 "$@"
          '';
        in imagePkgs.dockerTools.buildLayeredImage {
          name = "pangea-compiler";
          tag = "latest";
          # `git` is needed at startup: every pangea-*.gemspec computes its
          # spec.files list via `git ls-files`. Without it, bundler's
          # converge-paths phase crashes with `Errno::ENOENT - git`. Add
          # before the entrypoint so the binary is on PATH from layer 0.
          contents = [ ws.env appSource imagePkgs.coreutils imagePkgs.bashInteractive imagePkgs.cacert imagePkgs.git ];
          config = {
            Entrypoint = [ "${entrypoint}" ];
            ExposedPorts = { "8082/tcp" = { }; };
            Env = [
              "PATH=${ws.env}/bin:${imagePkgs.coreutils}/bin:${imagePkgs.git}/bin"
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

      # M8.5.1 — embedded-ruby operator image.
      #
      # Built directly via substrate's mkCrate2nixDockerImage (M8.5.1.x +
      # M8.5.1.y) with rootFeatures + crateOverrides + imageName +
      # binaryName plumbed through. The factory handles dockerTools.buildLayeredImage,
      # PATH composition, Entrypoint, exposed ports, and SSL — we just
      # supply the Ruby-specific knobs.
      #
      # Used by chart 0.7.0 when useEmbeddedRuby=true.
      # See helmworks@494533b chart values + theory M8.5.
      mkEmbeddedOperatorImage = imageSystem:
        let
          imagePkgs = import nixpkgs { system = imageSystem; };
          ruby = imagePkgs.ruby_3_4;
          libclang = imagePkgs.llvmPackages.libclang;
          # bindgen needs libc headers (stdio.h, stddef.h, …) on its
          # clang invocation. Nix sandboxes don't expose them on the
          # default include path; the canonical fix is
          # BINDGEN_EXTRA_CLANG_ARGS pointing at stdenv.cc's libc dev.
          # rb-sys docs: https://oxidize-rb.github.io/rb-sys/
          bindgenClangArgs = "-I${imagePkgs.stdenv.cc.libc.dev}/include";
          rubySharedEnv = {
            LIBCLANG_PATH = "${libclang.lib}/lib";
            PKG_CONFIG_PATH = "${ruby}/lib/pkgconfig";
            BINDGEN_EXTRA_CLANG_ARGS = bindgenClangArgs;
          };

          # M8.5.2.2 — bundle foundational pangea-* gems + their
          # transitive deps (pangea-core, pangea-aws, pangea-cloudflare,
          # …, plus terraform-synthesizer, dry-struct, dry-types, …)
          # into the operator image's runtime closure. Reuse the same
          # ws (Ruby workspace) the compiler image builds: both
          # bundle the same Gemfile.lock + path-gem set.
          #
          # gemWs.rubylib only covers path-gems (lib/<gem>/lib);
          # Bundler-resolved gems (dry-struct, terraform-synthesizer,
          # …) live at ${gemWs.env}/lib/ruby/gems/<ruby>/gems/<name>-<v>/lib/.
          # Compute the full $LOAD_PATH at image-build time so the
          # embedded CRuby's RUBYLIB includes both sets.
          gemWs = (import "${substrate}/lib/build/ruby/workspace.nix" {
            nixpkgs = nixpkgs;
            system = imageSystem;
            inherit ruby-nix substrate forge;
          }) {
            name = "pangea-compiler";
            self = ./pangea-compiler;
            pathGems = pangeaInputs;
            gemsetPath = "/gemset.nix";
          };

          # Enumerate every gem's lib/ directory in the bundlerEnv
          # at Nix-build time. The bundlerEnv stores resolved gems at
          # `${env}/lib/ruby/gems/<ruby-version>/gems/<name>-<version>/lib/`.
          # Glob + colon-join. Plus the path-gems from gemWs.rubylib.
          fullRubylibFile = imagePkgs.runCommand "pangea-full-rubylib" { } ''
            paths=""
            for d in ${gemWs.env}/lib/ruby/gems/*/gems/*/lib; do
              if [ -d "$d" ]; then
                if [ -z "$paths" ]; then
                  paths="$d"
                else
                  paths="$paths:$d"
                fi
              fi
            done
            printf '%s' "$paths" > $out
          '';
          bundlerLibPaths = imagePkgs.lib.removeSuffix "\n" (builtins.readFile fullRubylibFile);
          fullRubylib = "${gemWs.rubylib}:${bundlerLibPaths}";

          builders = import "${substrate}/lib/build/rust/crate2nix-builders.nix" {
            pkgs = imagePkgs;
            crate2nix = crate2nix.packages.${imageSystem}.default;
          };
          arch = if imageSystem == "aarch64-linux" then "arm64" else "amd64";
        in builders.mkCrate2nixDockerImage {
          serviceName = "pangea-operator";
          packageName = "pangea-operator";
          imageName = "pangea-operator-embedded";
          binaryName = "pangea-operator";
          src = self;
          architecture = arch;
          serviceType = "graphql";
          rootFeatures = [ "default" "embedded_ruby" ];
          # Pre-bundled gems live in the runtime closure via gemWs.env;
          # operator's main.rs sees RUBYLIB at process start, embedded
          # CRuby's ruby_init() reads it, $LOAD_PATH includes every
          # foundational pangea-* + dry-* + terraform-synthesizer at
          # boot. Per-CR ArchitectureGem clones (M8.4.2) layer on top.
          extraContents = pkgs: with pkgs; [
            ruby_3_4 opentofu packer git busybox
            gemWs.env
          ];
          extraEnv = [
            "RUBYLIB=${fullRubylib}"
            "DRY_TYPES_WARNINGS=false"
          ];
          buildInputs = [ ruby imagePkgs.openssl imagePkgs.postgresql imagePkgs.sqlite ];
          nativeBuildInputs = [ libclang imagePkgs.pkg-config imagePkgs.cmake imagePkgs.perl ];
          crateOverrides = {
            rb-sys = oldAttrs: rubySharedEnv // {
              nativeBuildInputs = (oldAttrs.nativeBuildInputs or [])
                ++ [ libclang imagePkgs.pkg-config ruby imagePkgs.stdenv.cc.libc.dev ];
              buildInputs = (oldAttrs.buildInputs or []) ++ [ ruby ];
            };
            magnus = oldAttrs: {
              buildInputs = (oldAttrs.buildInputs or []) ++ [ ruby ];
            };
            pangea-ruby-eval = oldAttrs: rubySharedEnv // {
              nativeBuildInputs = (oldAttrs.nativeBuildInputs or [])
                ++ [ libclang imagePkgs.pkg-config imagePkgs.stdenv.cc.libc.dev ];
              buildInputs = (oldAttrs.buildInputs or []) ++ [ ruby ];
            };
          };
        };

      embeddedOperatorExtension = system:
        let
          pkgs = import nixpkgs { inherit system; };
          imageAmd64 = mkEmbeddedOperatorImage "x86_64-linux";
          imageArm64 = mkEmbeddedOperatorImage "aarch64-linux";
          # Push to the EXISTING pangea-operator registry path with a
          # `embedded-` tag prefix. Avoids the new-package permission
          # gate on ghcr (creating a new package path requires extra
          # token scopes). Tags: `embedded-amd64-<sha>` and
          # `embedded-amd64-latest`.
          #
          # Helm chart consumers under useEmbeddedRuby=true set
          # `image.tag: embedded-amd64-<sha>`.
          mkPushApp = imagePath: archTag: pkgs.writeShellScript "pangea-operator-embedded-push-${archTag}" ''
            set -euo pipefail
            export GITHUB_TOKEN="''${GITHUB_TOKEN:-''${GHCR_TOKEN:-$(cat "$HOME/.config/github/token" 2>/dev/null || true)}}"
            export GHCR_TOKEN="$GITHUB_TOKEN"
            # Resolve the current git sha (forge --auto-tags would do
            # this for us, but we want our own `embedded-` prefix).
            GIT_SHA="$(${pkgs.git}/bin/git -C "''${PWD}" rev-parse --short HEAD 2>/dev/null || echo unknown)"
            TAG_SHA="embedded-${archTag}-''${GIT_SHA}"
            TAG_LATEST="embedded-${archTag}-latest"
            echo "📦 Pushing pangea-operator-embedded-${archTag} → ghcr.io/pleme-io/pangea-operator (tags: ''${TAG_SHA}, ''${TAG_LATEST})"
            exec ${forge.packages.${system}.default}/bin/forge push \
              --image-path "${imagePath}" \
              --registry "ghcr.io/pleme-io/pangea-operator" \
              --tag "''${TAG_SHA}" \
              --tag "''${TAG_LATEST}" \
              --retries 3
          '';
        in {
          packages.${system} = {
            dockerImage-operator-embedded-amd64 = imageAmd64;
            dockerImage-operator-embedded-arm64 = imageArm64;
          };
          apps.${system} = {
            push-image-operator-embedded-amd64 = {
              type = "app";
              program = toString (mkPushApp imageAmd64 "amd64");
            };
            push-image-operator-embedded-arm64 = {
              type = "app";
              program = toString (mkPushApp imageArm64 "arm64");
            };
          };
        };

      extendedWithEmbedded = lib.foldl'
        (acc: sys: lib.recursiveUpdate acc (embeddedOperatorExtension sys))
        extended
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];

      # ruby-eval devShell — for `cargo test -p pangea-ruby-eval` while
      # M8.2.0 (the magnus viability proof) is in flight. Provides
      # CRuby + libclang (rb-sys's bindgen needs both) on every
      # supported system. Once the crate2nix wiring lands in M8.2.x,
      # this shell becomes optional for routine builds.
      rubyEvalShell = system:
        let
          pkgs = import nixpkgs { inherit system; };
          ruby = pkgs.ruby_3_4;
          # rb-sys's build script uses libclang via bindgen.
          clang = pkgs.llvmPackages.libclang;
        in {
          devShells.${system}.ruby-eval = pkgs.mkShell {
            name = "pangea-ruby-eval";
            packages = [
              ruby
              pkgs.pkg-config
              pkgs.cargo
              pkgs.rustc
              pkgs.rustfmt
              pkgs.clippy
              clang
            ];
            # rb-sys looks at RUBY_INCLUDE_DIR / RUBY_LIB_DIR or
            # falls back to running `ruby -e 'puts RbConfig::CONFIG[...]'`.
            # The latter works as long as `ruby` is on PATH; PKG_CONFIG_PATH
            # gives bindgen an additional fallback.
            shellHook = ''
              export LIBCLANG_PATH="${clang.lib}/lib"
              export PKG_CONFIG_PATH="${ruby}/lib/pkgconfig:''${PKG_CONFIG_PATH:-}"
              echo "pangea-ruby-eval shell — ruby $(${ruby}/bin/ruby --version)"
              echo "  cargo test -p pangea-ruby-eval --lib --tests -- --test-threads=1"
            '';
          };
        };

      withRubyEval = lib.foldl'
        (acc: sys: lib.recursiveUpdate acc (rubyEvalShell sys))
        extendedWithEmbedded
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    in withRubyEval;
}
