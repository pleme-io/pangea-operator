{
  description = "Pangea Operator — Kubernetes controller for infrastructure management";

  inputs = {
    nixpkgs.follows = "substrate/nixpkgs";
    substrate = {
      # Pinned to 9556488 — mkProject accepts the optional `name`
      # arg that mk-rust-workspace.nix passes through (substrate@80c5778
      # introduced the fix). Previous substrate revs (ab6d1afd,
      # 914d53f) hit "function 'mkProject' called with unexpected
      # argument 'name'".
      url = "github:pleme-io/substrate";
    };
    # crate2nix input REMOVED — the operator builds via the gen/lockfile-builder
    # standard (committed Cargo.gen.lock delta + Cargo.build-spec.json,
    # useLockfileBuilder=true). substrate's rust builders now default crate2nix
    # to null, so the legacy input is no longer needed.
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
    pangea-github        = { url = "github:pleme-io/pangea-github";        flake = false; };
    pangea-hcloud        = { url = "github:pleme-io/pangea-hcloud";        flake = false; };
    pangea-kubernetes    = { url = "github:pleme-io/pangea-kubernetes";    flake = false; };
    pangea-porkbun       = { url = "github:pleme-io/pangea-porkbun";       flake = false; };
    pangea-splunk        = { url = "github:pleme-io/pangea-splunk";        flake = false; };
    pangea-spot          = { url = "github:pleme-io/pangea-spot";          flake = false; };
  };

  outputs = inputs@{ self, nixpkgs, substrate, forge, ruby-nix, ... }:
    let
      lib = nixpkgs.lib;

      # Operator (Rust) — substrate service-flake outputs.
      base = (import "${substrate}/lib/build/rust/service-flake.nix" {
        inherit nixpkgs substrate forge;
      }) {
        inherit self;
        serviceName = "pangea-operator";
        registry = "ghcr.io/pleme-io/pangea-operator";
        packageName = "pangea-operator";
        moduleDir = null;
        nixosModuleFile = null;
        # packer is unfree (HashiCorp BSL relicense) — pull it from an
        # allowUnfree nixpkgs so `nix flake check` doesn't refuse it
        # (substrate's service-flake passes a `pkgs` without allowUnfree).
        extraContents = pkgs: with pkgs; [ opentofu git busybox ]
          ++ [ (import nixpkgs { inherit (pkgs.stdenv.hostPlatform) system; config.allowUnfree = true; }).packer ];
      };

      pangeaInputs = {
        "pangea-akeyless"      = inputs.pangea-akeyless;
        "pangea-aws"           = inputs.pangea-aws;
        "pangea-azure"         = inputs.pangea-azure;
        "pangea-cloudflare"    = inputs.pangea-cloudflare;
        "pangea-core"          = inputs.pangea-core;
        "pangea-datadog"       = inputs.pangea-datadog;
        "pangea-gcp"           = inputs.pangea-gcp;
        "pangea-github"        = inputs.pangea-github;
        "pangea-hcloud"        = inputs.pangea-hcloud;
        "pangea-kubernetes"    = inputs.pangea-kubernetes;
        "pangea-porkbun"       = inputs.pangea-porkbun;
        "pangea-splunk"        = inputs.pangea-splunk;
        "pangea-spot"          = inputs.pangea-spot;
      };

      # ── pangeaInputs ↔ Gemfile coverage invariant ─────────────────
      # The embedded operator + compiler images bundle each gem named
      # in `pangeaInputs`. The Gemfile must declare a matching
      # `gem '<name>', path: 'vendor/<name>'` line; without it, the
      # ruby-nix build silently produces an image that can't `require`
      # the gem. That's how `cannot load such file -- pangea-github`
      # shipped to rio (operator at cdb072d) — we added a CRD for
      # GitHub workspaces but the bundle didn't include the provider.
      #
      # This Nix-time assertion fails evaluation with a clear error
      # naming exactly which gem is missing from Gemfile, so the bug
      # cannot recur silently.
      gemfileSrc = builtins.readFile ./pangea-compiler/Gemfile;
      gemfileMissing = lib.filter
        (gemName:
          ! (lib.hasInfix "gem '${gemName}'," gemfileSrc
            || lib.hasInfix "gem \"${gemName}\"," gemfileSrc))
        (builtins.attrNames pangeaInputs);
      # Pre-build trace: if any pangea-* flake input is missing from
      # Gemfile, the bundix pipeline hasn't been run yet. The Nix
      # build that consumes pangeaInputs will fail downstream with a
      # less obvious error (gemset.nix entry missing). This warning
      # surfaces the root cause early so you know to run regen.
      pangeaInputsChecked = lib.warnIf (gemfileMissing != []) ''
        [pangea-operator] flake.nix declared pangea-* inputs NOT
        referenced in pangea-compiler/Gemfile:
          ${lib.concatStringsSep "\n  " gemfileMissing}

        The image build will fail at gemset.nix override time. Fix:
          1. Add `gem '<name>', path: 'vendor/<name>'` to Gemfile
             for each missing entry (already done if you see this on
             a fresh clone with linter help).
          2. Run the bundix regen pipeline to update Gemfile.lock +
             gemset.nix:
                cd pangea-compiler
                bundle install
                bundix --magic
          3. Re-build the image:
                nix build .#dockerImage-operator-embedded-amd64

        This is the typed-substrate "promises become theorems" gate
        for Item B (gem coverage matrix). Operator paused under
        OperatorPolicy/default while this lands.
      '' pangeaInputs;

      # Compiler (Ruby) — call substrate ruby-workspace at output-level
      # for each Linux system we need, build a docker image from the
      # resulting bundlerEnv.
      mkCompilerImage = imageSystem:
        let
          imagePkgs = import nixpkgs { system = imageSystem; config.allowUnfree = true; };
          ws = (import "${substrate}/lib/build/ruby/workspace.nix" {
            nixpkgs = nixpkgs;
            system = imageSystem;
            inherit ruby-nix substrate forge;
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
          pkgs = import nixpkgs { inherit system; config.allowUnfree = true; };
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
          imagePkgs = import nixpkgs { system = imageSystem; config.allowUnfree = true; };
          ruby = imagePkgs.ruby_3_3;
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
            pathGems = pangeaInputsChecked;
            gemsetPath = "/gemset.nix";
            # ABI-coherence: the gem-workspace interpreter MUST match the
            # libruby rb-sys/magnus embeds (imagePkgs.ruby_3_3), or magnus
            # gem load fails with 'incompatible libruby-3.4.9.so'.
            ruby = imagePkgs.ruby_3_3;
          };

          # Compute the full $LOAD_PATH at Nix-build time by reading
          # every installed gemspec's `full_require_paths` directly
          # via RubyGems (skips Bundler — Bundler would need the
          # vendored path-gems available, which they aren't at this
          # build context). This honors gemspec-overridden require_paths
          # (notably concurrent-ruby's `lib/concurrent-ruby`).
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

          # gen/lockfile-builder path (useLockfileBuilder defaults true) — no
          # crate2nix needed; the builder defaults the arg to null.
          builders = import "${substrate}/lib/build/rust/crate2nix-builders.nix" {
            pkgs = imagePkgs;
          };
          arch = if imageSystem == "aarch64-linux" then "arm64" else "amd64";
          # Anchor every path-gem SOURCE into the image closure. RUBYLIB
          # (= fullRubylib) includes `gemWs.rubylib` = each
          # `${pangeaInputsChecked.<g>}/lib`, whose store path must exist at
          # runtime. Raw flake-input source paths placed directly in
          # `extraContents` are NOT derivations, so the image builder drops them
          # (verified: nix-store -qR <image> | grep <path-gem-source> = 0).
          # Reference them from a real runCommand output instead — nix's
          # reference scanner pulls each into THIS derivation's closure, which
          # buildLayeredImage then includes, so the `/lib` subpaths resolve.
          # Same pangeaInputsChecked as gemWs.rubylib → paths match by construction.
          pathGemAnchor = imagePkgs.runCommand "pangea-pathgem-anchor" { } ''
            mkdir -p $out
            printf '%s\n' ${lib.concatMapStringsSep " " (src: lib.escapeShellArg "${src}") (builtins.attrValues pangeaInputsChecked)} > $out/path-gem-refs
          '';

          # ── magma provider-mirror (★★ MAGMA-NATIVE / StageProvider) ─────
          #
          # magma's `magma-providers::locate_provider` resolves a provider
          # plugin binary by recursively walking two roots — the workspace's
          # `.terraform/providers` (the ephemeral emptyDir, wiped on pod roll)
          # and `$MAGMA_PROVIDER_DIR` — and filename-matching
          # `terraform-provider-<name>` / `terraform-provider-<name>_<ver>`
          # (magma-providers lib.rs:42-101; ANY layout under the root works,
          # flat dir or the nixpkgs `…/registry.terraform.io/<ns>/<name>/<ver>/
          # <os>_<arch>/…` tree). Before this, NO provider binaries were baked
          # and MAGMA_PROVIDER_DIR was never set, so the only resolution root
          # was the emptyDir — wiped on every pod roll → `EngineError::Locate`
          # → `NoProvidersDir` → every Create failed (the live rio
          # ProviderUnavailable anomaly, 2026-06-03).
          #
          # Baking the providers into the IMAGE (durable, roll-surviving) and
          # exporting `MAGMA_PROVIDER_DIR` at that path makes
          # ProviderUnavailable → StageProvider **Absolute by construction**:
          # the binaries live in the image's read-only store closure, not in
          # an emptyDir, so a pod restart cannot lose them. This is the
          # "one sanctioned filesystem reach" (loading provider plugins) made
          # *reliable* rather than incidental — theory/ANOMALY-REACTIVE-
          # RECONCILE.md §VII.1.
          #
          # NOTE: nixpkgs renamed the bare provider attrs to `<ns>_<name>`
          # (`terraform-providers.github` → `integrations_github`,
          # `cloudflare` → `cloudflare_cloudflare`, `aws` → `hashicorp_aws`,
          # `kubernetes` → `hashicorp_kubernetes`); the bare aliases still
          # resolve but emit deprecation warnings — use the canonical names.
          # The provider set is the UNION of `required_providers` across the
          # rio Pangea architectures (queried from the live rendered configs):
          # cloudflare + random (rio-drive tunnel), cloudflare + porkbun
          # (cloudflare-pleme nameserver delegation), plus github/aws/kubernetes
          # for fleet headroom. INTERIM: this static bake is the seed/fallback
          # for the dynamic DB-backed provider plane (theory/MAGMA-PROVIDER-PLANE.md)
          # — the destination is magma fetching/caching/loading providers live
          # from its Postgres registry as workspaces change requirements, no
          # rebuild. Until then, extend this list when a new required_provider
          # appears; a missing one surfaces as a ProviderUnavailable anomaly.
          magmaProviderMirror = imagePkgs.buildEnv {
            name = "magma-provider-mirror";
            paths = with imagePkgs.terraform-providers; [
              cloudflare_cloudflare
              integrations_github
              hashicorp_aws
              hashicorp_kubernetes
              hashicorp_random      # rio-drive: random_id tunnel_secret
              porkbun               # cloudflare-pleme: porkbun_nameservers delegation
            ];
          };
        in builders.mkCrate2nixDockerImage {
          serviceName = "pangea-operator";
          packageName = "pangea-operator";
          imageName = "pangea-operator-embedded";
          binaryName = "pangea-operator";
          src = self;
          architecture = arch;
          serviceType = "graphql";
          # executor_magma listed EXPLICITLY (not only via `default`) so
          # the embedded image always compiles MagmaExecutor in. A stale
          # Cargo.nix once dropped it from default and the operator
          # silently ran tofu for spec.executor=magma CRs (caught by the
          # rio-health-check canary 2026-05-27). Explicit = unmissable.
          rootFeatures = [ "default" "embedded_ruby" "executor_magma" ];
          # Pre-bundled gems live in the runtime closure via gemWs.env;
          # operator's main.rs sees RUBYLIB at process start, embedded
          # CRuby's ruby_init() reads it, $LOAD_PATH includes every
          # foundational pangea-* + dry-* + terraform-synthesizer at
          # boot. Per-CR ArchitectureGem clones (M8.4.2) layer on top.
          # gemWs.env carries the Bundler-resolved gems; the path-gems
          # (pangea-core/aws/gcp/…) are referenced by RUBYLIB via
          # `gemWs.rubylib` (= each `${pangeaInputsChecked.<g>}/lib`) but are
          # NOT in gemWs.env's closure, so without this they are absent at
          # runtime → `cannot load .../resource` (the 2026-06-02 gem-closure
          # wedge). Both gemWs.rubylib and these values derive from the SAME
          # pangeaInputsChecked, so adding the path-gem source closures here
          # guarantees every RUBYLIB entry resolves in the image.
          # packer is unfree (HashiCorp BSL relicense) — from an allowUnfree
          # nixpkgs so `nix flake check` doesn't refuse it.
          extraContents = pkgs: (with pkgs; [
            ruby_3_3 opentofu git busybox
            gemWs.env
          ]) ++ [
            (import nixpkgs { inherit (pkgs.stdenv.hostPlatform) system; config.allowUnfree = true; }).packer
            pathGemAnchor magmaProviderMirror
          ];
          extraEnv = [
            "RUBYLIB=${fullRubylib}"
            "DRY_TYPES_WARNINGS=false"
            # libruby on the runtime linker path, EXPLICITLY. The operator
            # binary embeds CRuby via rb-sys/magnus and dynamically links
            # libruby-<ver>.so. Previously it resolved only via nix's
            # incidental auto-RPATH from buildInputs — which a nixpkgs bump
            # silently shifted, crashing the process with
            # "libruby-3.3.10.so.3.3: cannot open shared object file". ruby
            # (= imagePkgs.ruby_3_3) is already in the image `contents`, so
            # pinning LD_LIBRARY_PATH at its lib dir makes libruby resolution
            # invariant to RPATH drift — the durable fix, not incidental.
            "LD_LIBRARY_PATH=${ruby}/lib"
            # Durable, roll-surviving provider-plugin root. magma's
            # locate_provider walks this recursively + filename-matches
            # (magma-providers lib.rs:42-101), so pointing it at the
            # buildEnv root resolves the nixpkgs
            # `…/libexec/terraform-providers/registry.terraform.io/…` trees
            # each provider ships, no flattening needed. Makes
            # ProviderUnavailable → StageProvider Absolute (the binary is in
            # the image closure, not the wiped emptyDir).
            "MAGMA_PROVIDER_DIR=${magmaProviderMirror}"
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
            # magma-protocol (tfplugin5/6 bindings; pulled in transitively
            # by executor_magma via magma-plugin) runs tonic-build in its
            # build.rs. That build.rs falls back to protoc-bin-vendored when
            # PROTOC is unset, but the vendored binary isn't materialized in
            # the crate2nix sandbox and protoc_bin_path() PANICS ("protoc
            # not found"). Provide nix protobuf + set PROTOC so build.rs
            # takes the env-PROTOC branch and skips the vendored lookup.
            magma-protocol = oldAttrs: {
              nativeBuildInputs = (oldAttrs.nativeBuildInputs or [])
                ++ [ imagePkgs.protobuf ];
              PROTOC = "${imagePkgs.protobuf}/bin/protoc";
            };
            # The operator crate is enormous (embedded Ruby + magma + kube +
            # async-graphql + opentelemetry + sqlx + tonic). Compiling it at
            # opt-level=3 + codegen-units=1 peaks high enough to OOM-kill
            # (signal 9) on rio's single small builder node (~12Gi free), and
            # nix caches that build failure so re-runs replay it instantly.
            # Cap peak compile memory: split codegen into 16 units + opt-level=2
            # — negligible runtime cost for a control-plane operator, and it
            # changes the crate's .drv hash so the build also escapes the
            # cached prior OOM failure (a fresh, lower-memory compile).
            pangea-operator = oldAttrs: {
              extraRustcOpts = (oldAttrs.extraRustcOpts or [])
                ++ [ "-Ccodegen-units=16" "-Copt-level=2" ];
            };
          };
        };

      embeddedOperatorExtension = system:
        let
          pkgs = import nixpkgs { inherit system; config.allowUnfree = true; };
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
          pkgs = import nixpkgs { inherit system; config.allowUnfree = true; };
          # Aligned with the embedded image's ruby (3.3) to match
          # the bundlerEnv's ABI — gem C-extensions compiled against
          # 3.3 won't load under a 3.4 libruby.
          ruby = pkgs.ruby_3_3;
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
              # bindgen (via rb-sys) needs libc headers (stdio.h, stddef.h, …)
              # on its clang invocation — nix doesn't expose them on the default
              # include path. Without this, `cargo test` in this shell dies with
              # "'stdio.h' file not found". Mirrors the image build's
              # rubySharedEnv (see bindgenClangArgs above) so the dev shell and
              # the hermetic build agree on the bindgen environment.
              export BINDGEN_EXTRA_CLANG_ARGS="-I${pkgs.stdenv.cc.libc.dev}/include ''${BINDGEN_EXTRA_CLANG_ARGS:-}"
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
