{
  description = "Pangea Operator — Kubernetes controller for infrastructure management";

  inputs = {
    nixpkgs.follows = "substrate/nixpkgs";
    substrate.url = "github:pleme-io/substrate";
    ruby-nix.url = "github:inscapist/ruby-nix";
    # Security-escape-hatch nixpkgs snapshot (2026-07-19, CVE remediation
    # for the embedded operator image — trivy run 29698809855 found 267
    # findings, 10 CRITICAL, across opentofu/packer/7 bundled terraform
    # providers + the pangea-compiler Gemfile). `nixpkgs` above stays on
    # substrate's fleet-pinned anchor (26.05.20260603, deliberately frozen
    # for zero release-skew across the whole fleet per substrate/flake.nix's
    # own comment — NOT something a single repo's CVE fix should move) —
    # same shape as pleme-io/hardened-images' `nixpkgs-vector` /
    # `nixpkgs-node-exporter` escape hatches: a SECOND, independent nixpkgs
    # snapshot used ONLY to source the handful of packages that need a
    # newer upstream release than the fleet anchor carries. Deliberately
    # NOT `inputs.nixpkgs.follows` — the whole point is a different rev.
    nixpkgs-security.url = "github:NixOS/nixpkgs/nixos-unstable";

    # Path-gem source-only inputs for the embedded-ruby operator image —
    # the same 13 typed primitive providers + composers the (now-sunset)
    # pangea-compiler sidecar bundled, RUBYLIB-mounted into the operator's
    # embedded CRuby (magnus) so `Pangea::Architectures.constants` resolves
    # at startup without a per-CR path-load.
    pangea-akeyless = {
      url = "github:pleme-io/pangea-akeyless";
      flake = false;
    };
    pangea-aws = {
      url = "github:pleme-io/pangea-aws";
      flake = false;
    };
    pangea-azure = {
      url = "github:pleme-io/pangea-azure";
      flake = false;
    };
    pangea-cloudflare = {
      url = "github:pleme-io/pangea-cloudflare";
      flake = false;
    };
    pangea-core = {
      url = "github:pleme-io/pangea-core";
      flake = false;
    };
    pangea-datadog = {
      url = "github:pleme-io/pangea-datadog";
      flake = false;
    };
    pangea-gcp = {
      url = "github:pleme-io/pangea-gcp";
      flake = false;
    };
    pangea-github = {
      url = "github:pleme-io/pangea-github";
      flake = false;
    };
    pangea-hcloud = {
      url = "github:pleme-io/pangea-hcloud";
      flake = false;
    };
    pangea-kubernetes = {
      url = "github:pleme-io/pangea-kubernetes";
      flake = false;
    };
    pangea-porkbun = {
      url = "github:pleme-io/pangea-porkbun";
      flake = false;
    };
    pangea-splunk = {
      url = "github:pleme-io/pangea-splunk";
      flake = false;
    };
    pangea-spot = {
      url = "github:pleme-io/pangea-spot";
      flake = false;
    };
  };

  outputs = inputs @ { self, nixpkgs, nixpkgs-security, substrate, ruby-nix, ... }:
    let
      lib = nixpkgs.lib;

      # substrate.rust.workspace dispatches over Cargo.gen.lock (the slim
      # gen delta, reconstructed to the full BuildSpec in pure Nix) — no
      # crate2nix, no Cargo.nix. This is the plain multi-target CLI/binary
      # surface (aarch64/x86_64-darwin + musl-static aarch64/x86_64-linux)
      # that `cargo-test`/`cargo-test-no-default-features` (via the
      # `.#ruby-eval` devShell, unaffected by this file) and release.yml's
      # `binaries` (cross-arch GH Release tarballs) + `chart` jobs consume.
      # UNCHANGED by the embedded-image restoration below — that's an
      # ADDITIONAL package, not a replacement of this one.
      base = substrate.rust.workspace {
        src = ./.;
        member = "pangea-operator";
      };

      pangeaInputs = {
        "pangea-akeyless" = inputs.pangea-akeyless;
        "pangea-aws" = inputs.pangea-aws;
        "pangea-azure" = inputs.pangea-azure;
        "pangea-cloudflare" = inputs.pangea-cloudflare;
        "pangea-core" = inputs.pangea-core;
        "pangea-datadog" = inputs.pangea-datadog;
        "pangea-gcp" = inputs.pangea-gcp;
        "pangea-github" = inputs.pangea-github;
        "pangea-hcloud" = inputs.pangea-hcloud;
        "pangea-kubernetes" = inputs.pangea-kubernetes;
        "pangea-porkbun" = inputs.pangea-porkbun;
        "pangea-splunk" = inputs.pangea-splunk;
        "pangea-spot" = inputs.pangea-spot;
      };

      # ── pangeaInputs ↔ Gemfile coverage invariant ─────────────────
      # The embedded operator image bundles each gem named in
      # `pangeaInputs`. The Gemfile must declare a matching
      # `gem '<name>', path: 'vendor/<name>'` line; without it, the
      # ruby-nix build silently produces an image that can't `require`
      # the gem. This Nix-time assertion fails evaluation with a clear
      # error naming exactly which gem is missing from Gemfile, so the
      # bug cannot recur silently.
      gemfileSrc = builtins.readFile ./pangea-compiler/Gemfile;
      gemfileMissing = lib.filter
        (gemName:
          !(lib.hasInfix "gem '${gemName}'," gemfileSrc
            || lib.hasInfix "gem \"${gemName}\"," gemfileSrc))
        (builtins.attrNames pangeaInputs);
      pangeaInputsChecked = lib.warnIf (gemfileMissing != [ ]) ''
        [pangea-operator] flake.nix declared pangea-* inputs NOT
        referenced in pangea-compiler/Gemfile:
          ${lib.concatStringsSep "\n  " gemfileMissing}

        The image build will fail at gemset.nix override time. Fix:
          1. Add `gem '<name>', path: 'vendor/<name>'` to Gemfile.
          2. Run the bundix regen pipeline (cd pangea-compiler &&
             bundle install && bundix --magic).
          3. Re-build: nix build .#dockerImage-operator-embedded-amd64
      '' pangeaInputs;

      # ── embedded-ruby operator image ──────────────────────────────
      #
      # Built via substrate's mkCrate2nixDockerImage — despite the name,
      # its default `useLockfileBuilder = true` dispatches to the SAME
      # gen/lockfile-builder pipeline `base` above uses (no crate2nix,
      # no Cargo.nix needed). `rootFeatures` is NOT honored on that path
      # (see pangea-operator/Cargo.toml's `[features] default = [...]`
      # comment: this was diagnosed 2026-06-02 and fixed by making
      # embedded_ruby + executor_magma the crate's DEFAULT features, so
      # every lockfile-builder build — including this one — links
      # libruby and compiles MagmaExecutor in without needing the
      # override to work). `rootFeatures` is still passed below purely
      # as documentation of intent.
      #
      # Built against a plain (non-static) nixpkgs for the target Linux
      # system — NOT the musl-static cross target `base`'s CLI binaries
      # use. embedded_ruby dynamically links libruby.so (magnus/rb-sys);
      # a static-musl binary has no dynamic linker to resolve it through.
      # `LD_LIBRARY_PATH=${ruby}/lib` below only makes sense — and only
      # works — against this dynamic-glibc build.
      mkEmbeddedOperatorImage = imageSystem:
        let
          imagePkgs = import nixpkgs { system = imageSystem; config.allowUnfree = true; };
          # The nixpkgs-security escape hatch (see the flake input comment)
          # — sources ONLY opentofu/packer/4 of the 7 mirrored terraform
          # providers, each verified 2026-07-19 against the real upstream
          # go.mod of the newer release nixpkgs-security packages:
          #   opentofu               1.11.8 -> 1.12.4  (grpc 1.79.3, the fix)
          #   packer                 1.15.3 -> 1.15.4  (go1.25.11 toolchain;
          #                          containerd/go-git/mongo-driver/x-pkgs
          #                          all substantially newer than the stale
          #                          pins driving the primary pin's findings)
          #   terraform-provider-github     6.12.1 -> 6.13.0 (grpc 1.79.3)
          #   terraform-provider-cloudflare 5.19.1 -> 5.22.0 (grpc 1.79.3)
          #   terraform-provider-aws        6.46.0 -> 6.55.0
          #   terraform-provider-kubernetes 3.1.0  -> 3.2.1
          # terraform-provider-random/-porkbun/-rabbitmq stay on the
          # PRIMARY pin below (`imagePkgs.terraform-providers`) — verified
          # 2026-07-19 that none has released ANY newer version upstream
          # (random 3.9.0 and rabbitmq 1.10.1 are both already the latest
          # GitHub release AND still the tip of their default branch;
          # porkbun 0.3.0 likewise, and its attribute doesn't even exist on
          # nixpkgs-security's terraform-providers set) — a channel bump
          # cannot help these three; see .trivyignore for the residual CVE
          # citations.
          securityPkgs = import nixpkgs-security { system = imageSystem; config.allowUnfree = true; };
          ruby = imagePkgs.ruby_3_3;
          libclang = imagePkgs.llvmPackages.libclang;
          # bindgen needs libc headers (stdio.h, stddef.h, …) on its
          # clang invocation. Nix sandboxes don't expose them on the
          # default include path; the canonical fix is
          # BINDGEN_EXTRA_CLANG_ARGS pointing at stdenv.cc's libc dev.
          bindgenClangArgs = "-I${imagePkgs.stdenv.cc.libc.dev}/include";
          rubySharedEnv = {
            LIBCLANG_PATH = "${libclang.lib}/lib";
            PKG_CONFIG_PATH = "${ruby}/lib/pkgconfig";
            BINDGEN_EXTRA_CLANG_ARGS = bindgenClangArgs;
          };

          # Bundle foundational pangea-* gems + their transitive deps
          # (pangea-core, pangea-aws, pangea-cloudflare, …, plus
          # terraform-synthesizer, dry-struct, dry-types, …) into the
          # operator image's runtime closure. `forge` is a required-but-
          # unused positional arg on ruby/workspace.nix (dead parameter,
          # confirmed unreferenced in its body) — pass null rather than
          # adding a flake input + push-app surface this repo doesn't use
          # (release.yml pushes via the shared image-push.yml reusable,
          # not a `nix run .#push-image-*` app).
          gemWs = (import "${substrate}/lib/build/ruby/workspace.nix" {
            inherit nixpkgs;
            system = imageSystem;
            inherit ruby-nix substrate;
            forge = null;
          }) {
            name = "pangea-compiler";
            self = ./pangea-compiler;
            pathGems = pangeaInputsChecked;
            gemsetPath = "/gemset.nix";
            # ABI-coherence: the gem-workspace interpreter MUST match the
            # libruby rb-sys/magnus embeds (imagePkgs.ruby_3_3), or magnus
            # gem load fails with 'incompatible libruby-3.4.9.so'.
            ruby = imagePkgs.ruby_3_3;
          };

          # Compute the full $LOAD_PATH at Nix-build time by reading every
          # installed gemspec's `full_require_paths` directly via
          # RubyGems (skips Bundler — the vendored path-gems aren't
          # available at this build context). Honors gemspec-overridden
          # require_paths (notably concurrent-ruby's `lib/concurrent-ruby`).
          fullRubylibFile = imagePkgs.runCommand "pangea-full-rubylib" { } ''
            ${gemWs.ruby}/bin/ruby -e '
              require "rubygems"
              ENV["GEM_PATH"] = ENV["GEM_PATH"] ? "${gemWs.env}/lib/ruby/gems/3.3.0:#{ENV["GEM_PATH"]}" : "${gemWs.env}/lib/ruby/gems/3.3.0"
              Gem.clear_paths
              paths = Gem::Specification.each.flat_map { |s| s.full_require_paths }.uniq
              paths.reject! { |p| p.start_with?("/build/") }
              puts paths.join(":")
            ' > $out
          '';
          bundlerLibPaths = imagePkgs.lib.removeSuffix "\n" (builtins.readFile fullRubylibFile);
          fullRubylib = "${gemWs.rubylib}:${bundlerLibPaths}";

          builders = import "${substrate}/lib/build/rust/crate2nix-builders.nix" {
            pkgs = imagePkgs;
          };
          arch = if imageSystem == "aarch64-linux" then "arm64" else "amd64";

          # Anchor every path-gem SOURCE into the image closure. RUBYLIB
          # (= fullRubylib) includes `gemWs.rubylib` = each
          # `${pangeaInputsChecked.<g>}/lib`, whose store path must exist
          # at runtime. Raw flake-input source paths placed directly in
          # `extraContents` are NOT derivations, so the image builder
          # drops them — reference them from a real runCommand output so
          # nix's reference scanner pulls each into this derivation's
          # closure.
          pathGemAnchor = imagePkgs.runCommand "pangea-pathgem-anchor" { } ''
            mkdir -p $out
            printf '%s\n' ${lib.concatMapStringsSep " " (src: lib.escapeShellArg "${src}") (builtins.attrValues pangeaInputsChecked)} > $out/path-gem-refs
          '';

          # ── magma provider-mirror (★★ MAGMA-NATIVE / StageProvider) ──
          # Bake provider plugin binaries into the IMAGE (durable, roll-
          # surviving) instead of relying on the ephemeral
          # `.terraform/providers` emptyDir, which is wiped on pod roll.
          # MAGMA_PROVIDER_DIR below points magma's locate_provider at
          # this closure. Provider set = union of `required_providers`
          # across the rio Pangea architectures; extend when a new one
          # appears (surfaces as a ProviderUnavailable anomaly otherwise).
          magmaProviderMirror = imagePkgs.buildEnv {
            name = "magma-provider-mirror";
            paths = (with securityPkgs.terraform-providers; [
              cloudflare_cloudflare
              integrations_github
              hashicorp_aws
              hashicorp_kubernetes
            ]) ++ (with imagePkgs.terraform-providers; [
              # No newer upstream release exists for these three on ANY
              # channel (verified 2026-07-19) — stays on the primary pin.
              # See .trivyignore for the residual CVE citations.
              hashicorp_random
              porkbun
              cyrilgdn_rabbitmq
            ]);
          };
        in
        builders.mkCrate2nixDockerImage {
          serviceName = "pangea-operator";
          packageName = "pangea-operator";
          imageName = "pangea-operator-embedded";
          binaryName = "pangea-operator";
          src = self;
          architecture = arch;
          serviceType = "graphql";
          # Real current ports (verified against src/config.rs
          # `prescribed_default()`) — the graphql/health builtin defaults
          # for serviceType="graphql" are the INVERSE of this operator's
          # actual assignment (graphql=8080/health=8081 by default vs.
          # health=8080/graphql=8081 here); passed explicitly so the
          # image's `ExposedPorts` metadata is accurate. The gRPC port
          # (50051) has no slot in this builder's 3-port model and is
          # not exposed via Docker metadata — harmless (Kubernetes
          # Service/probe definitions target ports directly, independent
          # of image EXPOSE metadata), not a runtime correctness gap.
          ports = {
            graphql = 8081;
            health = 8080;
            metrics = 9090;
          };
          rootFeatures = [ "default" "embedded_ruby" "executor_magma" ];
          extraContents = pkgs: (with pkgs; [
            ruby_3_3
            git
            busybox
            gemWs.env
          ]) ++ [
            # opentofu / packer sourced from nixpkgs-security (see the
            # flake input + securityPkgs comments above), not the primary
            # `pkgs`/`nixpkgs` — both had CVE-flagged embedded Go deps at
            # the primary pin's versions (opentofu: grpc-go CVE-2026-33186;
            # packer: stale containerd/go-git/mongo-driver/x-pkgs), fixed
            # by the newer upstream releases nixpkgs-security packages.
            securityPkgs.opentofu # TofuExecutor: unconditionally
            # constructed at controller startup, resolved whenever
            # spec.executor / PANGEA_EXECUTOR names tofu (PANGEA_FORBID_TOFU
            # is the explicit kill-switch) — a real, tested,
            # config-selectable fallback per MAGMA-NATIVE EXECUTION, not
            # dead weight.
            securityPkgs.packer
            # PackerExecutor: unconditionally built, driven by the
            # unconditionally-spawned PackerBuildController + sibling
            # AmiTestController reconciling AMI builds — a separate,
            # equally real concern from the tofu/magma question.
            pathGemAnchor
            magmaProviderMirror
          ];
          extraEnv = [
            "RUST_LOG=info,pangea_operator=debug"
            "LOG_FORMAT=json"
            "HEALTH_ADDR=0.0.0.0:8080"
            "METRICS_ADDR=0.0.0.0:9090"
            "GRAPHQL_ADDR=0.0.0.0:8081"
            "GRPC_ADDR=0.0.0.0:50051"
            "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
            "RUBYLIB=${fullRubylib}"
            "DRY_TYPES_WARNINGS=false"
            # libruby on the runtime linker path, EXPLICITLY — the
            # operator binary embeds CRuby via rb-sys/magnus and
            # dynamically links libruby-<ver>.so. Relying on nix's
            # incidental auto-RPATH is fragile to a nixpkgs bump;
            # pinning LD_LIBRARY_PATH at ruby's lib dir makes
            # resolution invariant to RPATH drift.
            "LD_LIBRARY_PATH=${ruby}/lib"
            # Durable, roll-surviving provider-plugin root. magma's
            # locate_provider walks this recursively + filename-matches,
            # so pointing it at the buildEnv root resolves every
            # provider's nixpkgs tree with no flattening needed.
            "MAGMA_PROVIDER_DIR=${magmaProviderMirror}"
          ];
          buildInputs = [ ruby imagePkgs.openssl imagePkgs.postgresql imagePkgs.sqlite ];
          nativeBuildInputs = [ libclang imagePkgs.pkg-config imagePkgs.cmake imagePkgs.perl ];
          crateOverrides = {
            rb-sys = oldAttrs: rubySharedEnv // {
              nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ])
                ++ [ libclang imagePkgs.pkg-config ruby imagePkgs.stdenv.cc.libc.dev ];
              buildInputs = (oldAttrs.buildInputs or [ ]) ++ [ ruby ];
            };
            magnus = oldAttrs: {
              buildInputs = (oldAttrs.buildInputs or [ ]) ++ [ ruby ];
            };
            pangea-ruby-eval = oldAttrs: rubySharedEnv // {
              nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ])
                ++ [ libclang imagePkgs.pkg-config imagePkgs.stdenv.cc.libc.dev ];
              buildInputs = (oldAttrs.buildInputs or [ ]) ++ [ ruby ];
            };
            # magma-protocol (tfplugin5/6 bindings, pulled in transitively
            # by executor_magma via magma-plugin) runs tonic-build in its
            # build.rs. That build.rs falls back to protoc-bin-vendored
            # when PROTOC is unset, but the vendored binary isn't
            # materialized in the sandbox and protoc_bin_path() panics.
            # Provide nix protobuf + set PROTOC so build.rs takes the
            # env-PROTOC branch.
            magma-protocol = oldAttrs: {
              nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ])
                ++ [ imagePkgs.protobuf ];
              PROTOC = "${imagePkgs.protobuf}/bin/protoc";
            };
            # The operator crate is enormous (embedded Ruby + magma + kube
            # + async-graphql + opentelemetry + sqlx + tonic). Cap
            # codegen + disable LTO so peak compile memory stays low
            # enough to survive small CI builders.
            pangea-operator = oldAttrs: {
              extraRustcOpts = (oldAttrs.extraRustcOpts or [ ])
                ++ [ "-Ccodegen-units=16" "-Copt-level=2" "-Clto=off" ];
            };
          };
        };

      # One image per NATIVE system only — registering both arches under
      # every host system (the pre-2026-07-17 shape) is what made `nix
      # flake check` attempt an unreachable aarch64-linux cross-build from
      # an x86_64-linux runner (ci.yml's own fix comment names this as the
      # real fix still owed to flake.nix). amd64 lands under
      # packages.x86_64-linux only; arm64 under packages.aarch64-linux only.
      embeddedOperatorExtension = system:
        let
          arch = if system == "aarch64-linux" then "arm64" else "amd64";
        in
        {
          packages.${system}."dockerImage-operator-embedded-${arch}" = mkEmbeddedOperatorImage system;
        };

      withEmbedded = lib.foldl'
        (acc: sys: lib.recursiveUpdate acc (embeddedOperatorExtension sys))
        base
        [ "x86_64-linux" "aarch64-linux" ];

      # ruby-eval devShell — the SECOND thing ef86809's gen-pattern
      # conversion silently dropped (confirmed live: ci.yml's `cargo-test`
      # job, which runs `nix develop .#ruby-eval -c cargo test` via the
      # shared nix-devshell-cargo-test.yml reusable, fails with "flake
      # ... does not provide attribute 'devShells.<system>.ruby-eval'").
      # CRuby + libclang (rb-sys's bindgen needs both) on every supported
      # system, matching the embedded image's ruby (3.3) so gem C-extensions
      # built against 3.3 load under the same libruby ABI at test time.
      rubyEvalShell = system:
        let
          pkgs = import nixpkgs { inherit system; config.allowUnfree = true; };
          ruby = pkgs.ruby_3_3;
          clang = pkgs.llvmPackages.libclang;
          # `stdenv.cc.libc.dev` is a glibc-only output — darwin's stdenv.cc.libc
          # has no `.dev` split (caught live: a local `nix build
          # .#devShells.aarch64-darwin.ruby-eval` failed eval with "attribute
          # 'dev' missing"). bindgen finds Xcode SDK headers on its own via
          # clang's built-in sysroot detection on darwin, so the explicit
          # libc include only applies — and is only needed — on Linux.
          bindgenLibcArgs = pkgs.lib.optionalString pkgs.stdenv.isLinux
            "-I${pkgs.stdenv.cc.libc.dev}/include ";
        in
        {
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
            shellHook = ''
              export LIBCLANG_PATH="${clang.lib}/lib"
              export PKG_CONFIG_PATH="${ruby}/lib/pkgconfig:''${PKG_CONFIG_PATH:-}"
              # bindgen (via rb-sys) needs libc headers (stdio.h, stddef.h, …)
              # on its clang invocation — nix doesn't expose them on the
              # default include path without this (Linux only; see above).
              export BINDGEN_EXTRA_CLANG_ARGS="${bindgenLibcArgs}''${BINDGEN_EXTRA_CLANG_ARGS:-}"
              echo "pangea-ruby-eval shell — ruby $(${ruby}/bin/ruby --version)"
              echo "  cargo test -p pangea-ruby-eval --lib --tests -- --test-threads=1"
            '';
          };
        };

      extended = lib.foldl'
        (acc: sys: lib.recursiveUpdate acc (rubyEvalShell sys))
        withEmbedded
        [ "x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin" ];
    in
    extended;
}
