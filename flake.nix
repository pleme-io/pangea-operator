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

    # The OpenWrt router provider, built by the repo that owns its source.
    #
    # ── WHY AN INPUT AND NOT A LOCAL DERIVATION ──────────────────────────
    # It used to be packaged in `pleme-io/nix` — a PRIVATE repo — so this
    # PUBLIC flake could not reach it, and the mirror below and the plo host
    # carried different provider sets, joined at the node with a symlinkJoin.
    # That is fine while the operator is a host service and fatal the moment it
    # is a Pod: the image had no router provider at all, so every roteador
    # InfrastructureTemplate would plan straight into ProviderUnavailable.
    # ★★ TOOL DISTRIBUTION — the repo builds its own artifact, consumers are
    # config-only.
    openwrt-uci.url = "github:pleme-io/openwrt-uci";
    openwrt-uci.inputs.nixpkgs.follows = "nixpkgs";
    openwrt-uci.inputs.substrate.follows = "substrate";

    # The lava dashboard catalogue — the `.tlisp` architectures the Ruby-free
    # image resolves `spec.source.architecture.name` against.
    #
    # Source-only: nothing here is built, the five `dashboards/*.tlisp` files
    # are copied into the image at LAVA_DASHBOARD_CATALOG and evaluated at
    # reconcile time by the lava backend.
    #
    # WITHOUT THIS INPUT the noruby image set LAVA_DASHBOARD_CATALOG to a path
    # nothing populated, so every typed-architecture dashboard failed with
    #
    #   architecture "workload-overview" not found in the image catalogue at
    #   /usr/share/lava/dashboards: No such file or directory (os error 2)
    #
    # Measured 2026-08-13, the first time the image was ever started. The env
    # var had been correct since the variant was authored; the contents were
    # never wired, and a build that only proves the image BUILDS cannot notice
    # that the directory it points at is empty.
    lava-architectures = {
      url = "github:pleme-io/lava-architectures";
      flake = false;
    };

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
    pangea-grafana = {
      url = "github:pleme-io/pangea-grafana";
      flake = false;
    };

    # ── tsunagu source-fetch workaround (narrow, repo-local, temporary) ──
    # Cargo.lock pins tsunagu to a git rev
    # (git+https://github.com/pleme-io/tsunagu#242a5b56…), so substrate's
    # gen-based lockfile-builder normally fetches it via `pkgs.fetchgit`
    # as a fixed-output derivation inside the Nix build sandbox. On a
    # cold cache that FOD has to actually run `git clone` against a
    # PRIVATE repo, and the sandboxed builder has no git credentials —
    # "fatal: could not read Username for https://github.com". A shared
    # fix for that class (authenticating Nix's OWN fetchgit sandbox, not
    # just cargo's) is in flight in pleme-io/substrate's image-push.yml
    # (see its git log) but has not landed working yet as of 2026-07-24.
    #
    # This input routes tsunagu through Nix's FLAKE input fetcher
    # instead (`fetchTree`), which already authenticates successfully in
    # this exact CI via the daemon's `access-tokens` nix.conf entry —
    # proven by the 13 pangea-* path-gem inputs above building fine on
    # every push. `crateOverrides.tsunagu` below points the crate's
    # `src` at this input, bypassing `pkgs.fetchgit` for this one crate
    # entirely. Pinned to the SAME rev Cargo.lock names — bump this pin
    # in lockstep if `cargo update -p tsunagu` ever moves it.
    #
    # Remove this input + its two crateOverrides entries once the shared
    # image-push.yml fix lands and is confirmed working end-to-end —
    # this is a stopgap for THIS repo, not a replacement for the real
    # fix upstream.
    tsunagu = {
      url = "github:pleme-io/tsunagu/242a5b56f3f83fd59b4a2312716c578be720ef12";
      flake = false;
    };
  };

  outputs =
    inputs@{
      self,
      nixpkgs,
      nixpkgs-security,
      substrate,
      ruby-nix,
      openwrt-uci,
      ...
    }:
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
        # See the `tsunagu` flake-input comment above — routes this one
        # crate's source through the flake-input fetcher instead of
        # `pkgs.fetchgit`, sidestepping the sandboxed-fetchgit private-repo
        # credential gap. Stopgap; remove once the shared fix lands.
        crateOverrides = {
          tsunagu = _oldAttrs: { src = inputs.tsunagu; };
        };
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
      gemfileMissing = lib.filter (
        gemName:
        !(lib.hasInfix "gem '${gemName}'," gemfileSrc || lib.hasInfix "gem \"${gemName}\"," gemfileSrc)
      ) (builtins.attrNames pangeaInputs);
      pangeaInputsChecked = lib.warnIf (gemfileMissing != [ ]) ''
        [pangea-operator] flake.nix declared pangea-* inputs NOT
        referenced in pangea-compiler/Gemfile:
          ${lib.concatStringsSep "\n  " gemfileMissing}

        The image build will fail at gemset.nix override time. Fix:
          1. Add `gem '<name>', path: 'vendor/<name>'` to Gemfile.
          2. Run the bundix regen pipeline (cd pangea-compiler &&
             bundle install && bundix --magic).
          3. Re-check: nix flake check
      '' pangeaInputs;

      # ── Ruby-FREE operator image (distroless-static) ──────────────
      #
      # The second of two image variants, and the one the FedRAMP
      # boundary can actually receive. ADDITIVE: it adds a package and
      # touches nothing the embedded-ruby image below reads, so it cannot
      # change what the existing image builds.
      #
      # Why a second SPEC and not a second argument: gen bakes the
      # RESOLVED feature set into Cargo.build-spec.json, and rootFeatures
      # is not honored on the lockfile-builder path — there is no way to
      # ask a build for FEWER features than its spec carries. So the
      # variant reads `Cargo.noruby.build-spec.json`, produced by
      # tools/regen-noruby-spec.sh and committed beside the canonical one.
      # Verified: it resolves the canonical feature set minus `default`
      # and `embedded_ruby`.
      #
      # Why that reaches distroless-STATIC when the embedded image cannot:
      # `embedded_ruby` links libruby.so dynamically (magnus/rb-sys), and
      # a static binary has no dynamic linker to resolve it through —
      # which is the sole reason the image below is built against plain
      # glibc nixpkgs instead of the musl-static target every other
      # binary in this repo already uses. No libruby, no glibc, no shell,
      # no package manager: CA roots and /etc/passwd only.
      # ── magma provider-mirror, SHARED BY BOTH IMAGES ────────────────────
      # Hoisted 2026-08-27. It used to live inside mkEmbeddedOperatorImage,
      # so the Ruby-free image shipped ZERO providers -- and once the
      # `embedded-amd64` TAG was repointed at the noruby variant (2837c43),
      # every magma apply and every import prepass on camelot failed with
      #   locate provider "aws": no .terraform/providers dir ... run `init` first
      # 6 of 21 templates were Failed on exactly this. The block comment below
      # already stated the design; only one of the two images honoured it.
      #
      # The provider LIST must exist once. Two copies would diverge silently,
      # and the failure mode is a hard ProviderUnavailable months later.
      magmaProviderMirrorFor =
        imageSystem:
        let
          imagePkgs = import nixpkgs {
            system = imageSystem;
            config.allowUnfree = true;
          };
          securityPkgs = import nixpkgs-security {
            system = imageSystem;
            config.allowUnfree = true;
          };
          hardenedSecurityPkgs = (import "${substrate}/lib/security/mk-hardened-pkgs.nix" { inherit lib; }) {
            pkgs = securityPkgs;
            mitigations = [
              "terraform-provider-aws-grpc-bump"
              "terraform-provider-kubernetes-kin-openapi-port"
              "fleet-go-toolchain"
              "terraform-providers-go-module-floor"
            ];
          };
          inherit (import "${substrate}/lib/build/go/overlay.nix") assertGoFloor;
          atGoFloor = drv: assertGoFloor {
            inherit drv;
            what = "magma-provider-mirror's ${drv.pname or drv.name}";
          };

          mirror = imagePkgs.buildEnv {
            name = "magma-provider-mirror";
            paths =
              map atGoFloor (
                with hardenedSecurityPkgs.terraform-providers;
                [
                  cloudflare_cloudflare
                  integrations_github
                  hashicorp_kubernetes
                  hashicorp_aws
                  cullenmcdermott_porkbun
                  cyrilgdn_rabbitmq
                  datadog_datadog
                ]
                ++ [ openwrt-uci.packages.${imageSystem}.terraform-provider-openwrt ]
              );
          };
        in
        mirror;

      # ══ THE OPERATOR VARIANT CATALOG ═══════════════════════════════════════
      #
      # One row per build. The row IS the decision, and everything that differs
      # between variants is DERIVED from it — so the facts that must agree
      # cannot be set independently.
      #
      # ── WHY THIS EXISTS (the defect it retires) ──
      # Three builders hand-rolled the same shape, and two facts that are really
      # ONE choice were written separately in each:
      #
      #   which BUILD SPEC    decides whether `embedded_ruby` reaches the compiler
      #   which PACKAGE SET   decides whether libruby can be linked at all
      #
      # `mkEmbeddedOperatorImage` picked dynamic-glibc pkgs (correct for Ruby)
      # and the CANONICAL spec (which carries no Ruby), so it built the Ruby-free
      # binary and wrapped it in ruby/git/opentofu/packer it could not call — for
      # months, every layer green. Nothing compared the halves because nothing
      # OWNED both.
      #
      # Here `carriesRuby` owns both. A variant cannot select a Ruby package set
      # without the Ruby spec, or vice versa, because it selects neither — it
      # declares one bit and the builder derives the rest.
      operatorVariants = {
        noruby = {
          specFile = "Cargo.noruby.build-spec.json";
          carriesRuby = false;
          description = "Ruby-free, musl-static (the FedRAMP-boundary build)";
        };
        ruby = {
          specFile = "Cargo.ruby.build-spec.json";
          carriesRuby = true;
          description = "embedded CRuby via magnus, glibc-dynamic";
        };
      };

      # Build one variant's binary. `carriesRuby` decides the package set, the
      # crate overrides and the linkage; the caller decides nothing else.
      mkOperatorBin =
        {
          variant,
          buildSystem,
          auditable ? true,
        }:
        let
          v =
            operatorVariants.${variant} or (throw ''
              unknown operator variant "${variant}". Known:
              ${lib.concatMapStringsSep "\n" (n: "  ${n}: ${operatorVariants.${n}.description}") (
                builtins.attrNames operatorVariants
              )}
            '');

          basePkgs = import nixpkgs {
            system = buildSystem;
            config.allowUnfree = true;
            overlays = lib.optionals (!v.carriesRuby) [
              ((import "${substrate}/lib/build/rust/overlay.nix").mkRustOverlay {
                fenix = substrate.inputs.fenix;
                system = buildSystem;
                targets = [ "x86_64-unknown-linux-musl" ];
              })
            ];
          };

          # ── THE DERIVED PAIRING, and the correct default ──
          # Ruby-free -> pkgsStatic (musl; what every other binary here ships).
          # Ruby       -> plain dynamic glibc, because magnus/rb-sys links
          # libruby.so and a static-musl binary has no dynamic linker to resolve
          # it through. Getting this pairing wrong by hand is what produced the
          # mislabelled image.
          buildPkgs = if v.carriesRuby then basePkgs else basePkgs.pkgsStatic;

          ruby = basePkgs.ruby_3_3;
          libclang = basePkgs.llvmPackages.libclang.lib;

          rubyEnv = {
            LIBCLANG_PATH = "${libclang}/lib";
            RUBY = "${ruby}/bin/ruby";
            # bindgen runs its OWN libclang and does not inherit the nix
            # cc-wrapper's include flags, so `stdenv.cc.libc.dev` in
            # nativeBuildInputs is not enough — measured on rio 2026-09-06:
            #   ruby/defines.h:16:10: fatal error: 'stdio.h' file not found
            # Same expression the ruby-eval devShell carries as bindgenLibcArgs.
            BINDGEN_EXTRA_CLANG_ARGS = lib.optionalString buildPkgs.stdenv.isLinux "-I${buildPkgs.stdenv.cc.libc.dev}/include";
          };

          rubyOverrides = lib.optionalAttrs v.carriesRuby {
            rb-sys =
              oldAttrs:
              rubyEnv
              // {
                nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ]) ++ [
                  libclang
                  basePkgs.pkg-config
                  ruby
                  basePkgs.stdenv.cc.libc.dev
                ];
                buildInputs = (oldAttrs.buildInputs or [ ]) ++ [ ruby ];
              };
            magnus = oldAttrs: { buildInputs = (oldAttrs.buildInputs or [ ]) ++ [ ruby ]; };
            pangea-ruby-eval =
              oldAttrs:
              rubyEnv
              // {
                nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ]) ++ [
                  libclang
                  basePkgs.pkg-config
                  basePkgs.stdenv.cc.libc.dev
                ];
                buildInputs = (oldAttrs.buildInputs or [ ]) ++ [ ruby ];
              };
          };

          lockfileBuilder = import "${substrate}/lib/build/rust/lockfile-builder.nix" { pkgs = buildPkgs; };
          plemeCrateOverrides = (import "${substrate}/lib/build/rust/pleme-crate-overrides.nix") buildPkgs.stdenv.hostPlatform.rust.rustcTarget;

          project = lockfileBuilder.mkProject {
            src = self;
            name = "pangea-operator-${variant}";
            inherit (v) specFile;
            defaultCrateOverrides =
              buildPkgs.defaultCrateOverrides
              // plemeCrateOverrides
              // {
                # Stopgap: routes tsunagu through the flake-input fetcher rather
                # than a sandboxed fetchgit with no credentials.
                tsunagu = _oldAttrs: { src = inputs.tsunagu; };
                # executor_magma is a DEFAULT feature, so magma-protocol is in
                # EVERY variant's graph and needs protoc in all of them.
                magma-protocol = oldAttrs: {
                  nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ]) ++ [ basePkgs.protobuf ];
                  PROTOC = "${basePkgs.protobuf}/bin/protoc";
                };
                pangea-operator = oldAttrs: {
                  extraRustcOpts = (oldAttrs.extraRustcOpts or [ ]) ++ [
                    "-Ccodegen-units=16"
                    "-Copt-level=2"
                    "-Clto=off"
                  ];
                };
              }
              // rubyOverrides;
          };

          rawBin = project.workspaceMembers."pangea-operator".build;

          # Make the binary SCANNABLE, or a CVE gate over it is a green that
          # inspected nothing: trivy on a Nix image reports `packages: 0`
          # without the embedded audit data.
          auditData =
            (import "${substrate}/lib/build/rust/cargo-audit-data.nix" { }).mkCargoAuditData basePkgs
              {
                name = "pangea-operator-${variant}";
                lockFile = ./Cargo.lock;
                rootCrate = "pangea-operator";
              };
        in
        basePkgs.runCommand "pangea-operator-${variant}"
          {
            nativeBuildInputs = lib.optionals v.carriesRuby [ basePkgs.makeWrapper ];
            passthru = {
              inherit (v) carriesRuby specFile;
              inherit rawBin;
            };
          }
          (
            ''
              mkdir -p $out/bin
              cp ${rawBin}/bin/pangea-operator $out/bin/pangea-operator
              chmod +w $out/bin/pangea-operator
            ''
            + lib.optionalString auditable ''
              ${(import "${substrate}/lib/build/rust/cargo-audit-data.nix" { }).injectCommand {
                inherit auditData;
                target = "$out/bin/pangea-operator";
                objcopy = "${basePkgs.binutils}/bin/objcopy";
              }}
            ''
            + lib.optionalString v.carriesRuby ''
              # libruby on the runtime linker path EXPLICITLY — nix's incidental
              # auto-RPATH is fragile to a nixpkgs bump.
              mv $out/bin/pangea-operator $out/bin/.pangea-operator-unwrapped
              makeWrapper $out/bin/.pangea-operator-unwrapped $out/bin/pangea-operator --prefix LD_LIBRARY_PATH : ${ruby}/lib
            ''
          );

      # ── EVERY VARIANT IS VERIFIED AGAINST ITS OWN DECLARATION ──────────────
      # Not just the Ruby one. `--capabilities` reports the features the binary
      # was ACTUALLY compiled with; this asserts they match the catalog row in
      # BOTH directions. A check that only asserted "ruby has ruby" would pass
      # forever against a `noruby` variant that silently grew the feature.
      mkVariantCapabilityCheck =
        { variant, buildSystem }:
        let
          v = operatorVariants.${variant};
          checkPkgs = import nixpkgs { system = buildSystem; };
          bin = mkOperatorBin { inherit variant buildSystem; };
          expected = if v.carriesRuby then "true" else "false";
        in
        checkPkgs.runCommand "pangea-operator-${variant}-capabilities-match"
          { nativeBuildInputs = [ checkPkgs.jq ]; }
          ''
            caps=$(${bin}/bin/pangea-operator --capabilities)
            echo "$caps"

            actual=$(echo "$caps" | jq -r .embedded_ruby)
            if [ "$actual" != "${expected}" ]; then
              echo "FAIL: variant '${variant}' declares carriesRuby=${expected} but the binary reports embedded_ruby=$actual." >&2
              echo "The spec ${v.specFile} did not deliver what the catalog claims. Regenerate it, or fix the catalog row — they are one decision." >&2
              exit 1
            fi

            # The advertised backend list is DERIVED from the feature flags in
            # capabilities.rs, so a disagreement means that derivation broke.
            if [ "${expected}" = "true" ]; then
              echo "$caps" | jq -e '.backends | index("embedded")' >/dev/null || { echo "FAIL: embedded_ruby set but 'embedded' not advertised." >&2; exit 1; }
            else
              echo "$caps" | jq -e '.backends | index("embedded")' >/dev/null && { echo "FAIL: no embedded_ruby but 'embedded' IS advertised." >&2; exit 1; }
            fi

            echo "OK: '${variant}' matches its declaration (${v.description})."
            touch $out
          '';

      mkNoRubyOperatorImageSet =
        imageSystem:
        let
          # THE OVERLAY IS THE FIX, and substrate already wrote it.
          #
          # A bare `import nixpkgs` gives pkgsStatic no prebuilt toolchain, so
          # nixpkgs builds rustc -- and therefore LLVM -- from source for the
          # musl-static target. substrate's own overlay comment describes the
          # exact failure this repo then hit on rio:
          #
          #   "Static-musl builds via pkgsStatic otherwise drag in a
          #    from-source rustc + LLVM (a ~30-min build that also hits a
          #    static-link bug on recent LLVM); the prebuilt std makes the host
          #    rustc cross-compile straight to the target."
          #
          # `targets` contributes fenix's PREBUILT rust-std for the triple, so
          # the host rustc emits target objects and llvm-static is never
          # entered. fenix needs no new input here: substrate already carries
          # it, so this follows the same rev the rest of the fleet builds with
          # rather than pinning a second toolchain.
          #
          # NOTE the leg this does NOT change. lockfile-builder is the GEN
          # path -- "a pure-dispatch Rust builder over gen's typed
          # Cargo.build-spec.json" (its line 1), reading Cargo.gen.lock, which
          # this repo already has. It expects pkgsStatic and keeps it. The
          # crate2nix leg is NOT an alternative for a musl-static image:
          # gaveta's flake records that buildRustCrate's per-crate rustc
          # ignores CARGO_BUILD_TARGET and yields a glibc-dynamic binary that
          # cannot run in the image at all.
          imagePkgs = import nixpkgs {
            system = imageSystem;
            overlays = [
              ((import "${substrate}/lib/build/rust/overlay.nix").mkRustOverlay {
                fenix = substrate.inputs.fenix;
                system = imageSystem;
                targets = [ "x86_64-unknown-linux-musl" ];
              })
            ];
          };
          # musl-static: the binary must carry its own libc for a
          # distroless-static base to be legal.
          #
          # `pending-noruby-toolchain: pkgsStatic restages the TOOLCHAIN, not
          #  just the target.` MEASURED 2026-08-12 on rio (32 cores, from a
          # cold store): this build never reaches pangea-operator's own source.
          # It stops in `llvm-static-x86_64-unknown-linux-musl-21.1.8` at file
          # 3434/3992, linking libLLVM.so.21.1 -- a SHARED object -- for the
          # musl-static target:
          #
          #   ld.bfd: failed to set dynamic section sizes: bad value
          #   collect2: error: ld returned 1 exit status
          #
          # That is why GHCR carries ZERO `noruby-*` tags: the variant has never
          # been built anywhere, by anyone. The "3433 paths, 0 Ruby" measurement
          # this variant is trusted on was taken over the DERIVATION's requisites
          # (computable without building), so it is still true and still not
          # evidence the image exists.
          #
          # The fix is not a patch, it is the fleet's own proven pattern:
          # substrate's `crate2nix-builders.nix:399-401` builds musl binaries
          # with `pkgsCross.musl64` + `CARGO_BUILD_TARGET = muslTarget` -- a
          # NORMAL toolchain cross-compiling to musl -- and contains no
          # `pkgsStatic` anywhere. `pkgsStatic` asks nixpkgs to rebuild rustc
          # and LLVM themselves as static artifacts, which is a much larger and
          # much less travelled claim than "this binary carries its own libc".
          # Idiom-first, and the idiom already exists.
          staticPkgs = imagePkgs.pkgsStatic;
          lockfileBuilder = import "${substrate}/lib/build/rust/lockfile-builder.nix" { pkgs = staticPkgs; };
          plemeCrateOverrides = (import "${substrate}/lib/build/rust/pleme-crate-overrides.nix") staticPkgs.stdenv.hostPlatform.rust.rustcTarget;
          # ── ONE BUILDER, PARAMETERIZED BY THE CATALOG ──────────────────
          # Was 86 lines hand-rolling the project, the crate overrides, the
          # audit-data injection and the auditable wrapper — all now derived
          # from the `noruby` catalog row. The package set is derived too
          # (musl-static, because the row says carriesRuby = false), which is
          # the pairing that used to be a per-builder hand-decision.
          bin = mkOperatorBin {
            variant = "noruby";
            buildSystem = imageSystem;
          };

          hardened = import "${substrate}/lib/build/oci/hardened-base.nix" { pkgs = imagePkgs; };
        in
        substrate.lib.${imageSystem}.mkHardenedImageSet {
          pkgs = imagePkgs;
          system = imageSystem;
          sourceUrl = "https://github.com/pleme-io/pangea-operator";
          sourceRevision = self.rev or self.dirtyRev or "unknown";
          ociOverrides.operator = hardened;
          images.operator = {
            service = "pangea-operator";
            base = "distroless-static";
            package = bin;
            upstreamImage = "none: first-party Rust source";
            hardeningRationale = "musl-static operator binary on distroless-static: no shell, no libc, no package manager";
            publishName = "ghcr.io/pleme-io/pangea-operator";
            publishTag = (builtins.fromTOML (builtins.readFile ./Cargo.toml)).workspace.package.version;
            trivyAnalysableTargets = [
              "gobinary"
              "rustbinary"
            ];
            entrypoint = [ "${bin}/bin/pangea-operator" ];
            # The Ruby-free build cannot serve the embedded backend — main.rs
            # panics loudly on PANGEA_COMPILER_BACKEND=embedded rather than
            # silently falling back — so the image selects the lava backend
            # explicitly. Without this the default config (compiler.backend
            # = "embedded") would crash it on startup.
            env = [
              "PANGEA_COMPILER_BACKEND=lava"
              "LAVA_DASHBOARD_CATALOG=/usr/share/lava/dashboards"
              "LAVA_ARCHITECTURE_CATALOG=/usr/share/lava/architectures"
              # ── ★ THE PROVIDERS THIS IMAGE SHIPPED WITHOUT ────────────────
              # Added 2026-08-27. This env var and the mirror in extraContents
              # below were present ONLY in mkEmbeddedOperatorImage, so the
              # Ruby-free image carried no terraform provider binaries at all.
              # It went live the moment the `embedded-amd64` TAG was repointed
              # at this variant (2837c43): magma's locate_provider found
              # neither MAGMA_PROVIDER_DIR nor the workspace's
              # `.terraform/providers` (an emptyDir, wiped by the very pod roll
              # that deploys a new image), and every apply AND every import
              # prepass failed with
              #   locate provider "aws": no .terraform/providers dir … run `init` first
              # 6 of 21 camelot templates were Failed on this, in two shapes
              # that read as unrelated: three "Magma execution failed" and
              # three "Applying refused: import failed … still wants to CREATE
              # it". The second shape is the never-destroy guard working
              # correctly ON TOP of this bug — the import could not run, so the
              # plan fell back to create, so the guard refused to duplicate a
              # live resource.
              #
              # There is deliberately NO `tofu init` fallback here
              # (PANGEA_FORBID_TOFU + magma reads MAGMA_PROVIDER_DIR), so an
              # absent mirror is a hard failure rather than a slow path.
              "MAGMA_PROVIDER_DIR=${magmaProviderMirrorFor imageSystem}"
            ];
            # The catalogue those two env vars are useless without. Copied from
            # the lava-architectures input rather than vendored, so adding a
            # dashboard is a lock bump and not an edit here.
            #
            # The `find … -exec install` shape (rather than `cp -r`) is
            # deliberate: it fails loudly if `dashboards/` is missing or empty
            # upstream, where a `cp -r` of an absent directory would produce an
            # empty target and reproduce exactly the bug this closes — an env
            # var pointing at nothing, discovered only when a CR is applied
            # months later.
            # ── ★ A LIST, NOT A FUNCTION ────────────────────────────────
            # hardened.mkPackageImage declares `extraContents ? []` and uses it
            # as `[ package ] ++ extraContents` (substrate
            # lib/build/oci/hardened-base.nix:545,625). Passing a lambda made
            # the whole image fail at EVAL with "expected a list but found a
            # function", four seconds into a step named "Build image with Nix"
            # — which reads like a build failure and is not one.
            #
            # The lambda bought nothing: `imagePkgs` is already in scope here,
            # so the argument was discarded (`_:`) anyway. Substrate has a
            # SECOND builder whose extraContents genuinely is a function
            # (lib/build/go/tool-image-flake.nix), which is where the shape
            # came from — same option name, two contracts, and no error until
            # eval.
            extraContents = [
              # The provider closure MAGMA_PROVIDER_DIR above points at. Listed
              # here so nix actually copies it into the image — an env var
              # naming a store path that the image does not contain is the same
              # "points at nothing" failure the lava-catalogue comment below
              # guards against, and it fails identically at reconcile-time.
              (magmaProviderMirrorFor imageSystem)
              (lavaRubyGemsFor imageSystem)
              (imagePkgs.runCommand "lava-dashboard-catalog" { } ''
                mkdir -p $out/usr/share/lava/dashboards
                n=$(find ${inputs.lava-architectures}/dashboards -name '*.tlisp' | wc -l)
                if [ "$n" -eq 0 ]; then
                  echo "lava-architectures ships no dashboards/*.tlisp — the" \
                       "catalogue would be empty and every architecture CR" \
                       "would fail at reconcile" >&2
                  exit 1
                fi
                find ${inputs.lava-architectures}/dashboards -name '*.tlisp' \
                  -exec install -Dm444 {} $out/usr/share/lava/dashboards/ \;
                echo "lava catalogue: $n dashboards"
              '')
              # ── ★ THE ARCHITECTURE CATALOGUE ────────────────────────────
              # LavaCompilerBackend::compile resolves a named architecture from
              # /usr/share/lava/architectures. Nothing put files there, so a
              # compile by template_name would have failed at RECONCILE with
              # "architecture not found in the image catalogue" — after the
              # image shipped, on a cluster, for a template that looked
              # correctly configured.
              #
              # The same `find … -exec install` shape as the dashboards above,
              # and for the same reason: it fails loudly when the upstream
              # directory is missing or empty, where `cp -r` of an absent
              # directory produces an empty target and reproduces exactly the
              # bug this closes.
              (imagePkgs.runCommand "lava-architecture-catalog" { } ''
                mkdir -p $out/usr/share/lava/architectures
                n=$(find ${inputs.lava-architectures}/architectures -name '*.tlisp' | wc -l)
                if [ "$n" -eq 0 ]; then
                  echo "lava-architectures ships no architectures/*.tlisp — the" \
                       "catalogue would be empty and every compile by name would" \
                       "fail at reconcile" >&2
                  exit 1
                fi
                find ${inputs.lava-architectures}/architectures -name '*.tlisp' \
                  -exec install -Dm444 {} $out/usr/share/lava/architectures/ \;
                echo "lava catalogue: $n architectures"
              '')
            ];
            exposedPorts = {
              "8080/tcp" = { };
              "8081/tcp" = { };
              "9090/tcp" = { };
            };
          };
        };

      embeddedOperatorExtension =
        system:
        let
          arch = if system == "aarch64-linux" then "arm64" else "amd64";
          imageSet = mkNoRubyOperatorImageSet system;
        in
        {
          apps.${system} = removeAttrs imageSet.apps [ "release" ];
          hardenedImageCatalog.${system} = imageSet.catalog;
          packages.${system} = {
            "dockerImage-operator-noruby-${arch}" = imageSet.packages.operatorImage;
          }
          // lib.optionalAttrs (system == "x86_64-linux") {
            # Both variants' binaries, derived from the catalog. x86_64-linux
            # only: registering a variant under every host system is what once
            # made `nix flake check` attempt an unreachable cross-build.
            "pangea-operator-ruby" = mkOperatorBin {
              variant = "ruby";
              buildSystem = system;
            };
            "pangea-operator-noruby-bin" = mkOperatorBin {
              variant = "noruby";
              buildSystem = system;
            };
            # ── THE PROVIDER MIRROR, EXPOSED ──────────────────────────────
            # magma's `locate_provider` walks MAGMA_PROVIDER_DIR recursively and
            # filename-matches, so this buildEnv root resolves every provider's
            # nixpkgs tree with no flattening.
            #
            # It was reachable ONLY from inside mkEmbeddedOperatorImage, which
            # meant the in-cluster posture had providers and the HOST posture
            # (systemd on plo) had none — an operator with PANGEA_EXECUTOR=magma
            # and no MAGMA_PROVIDER_DIR resolves every provider to
            # ProviderUnavailable. Same closure, one definition, two consumers.
            "magma-provider-mirror" = magmaProviderMirrorFor system;
          };
          # EVERY variant is checked against its own catalog row. Derived with
          # mapAttrs over the catalog, so a new variant brings its own check
          # rather than needing one remembered.
          checks.${system} = lib.optionalAttrs (system == "x86_64-linux") ({
            "magma-provider-mirror-go-floor" =
              (import "${substrate}/lib/security/go-binary-floor-check.nix" {
                pkgs = nixpkgs.legacyPackages.${system};
              }).mkGoBinaryFloorCheck {
                name = "magma-provider-mirror";
                tree = magmaProviderMirrorFor system;
                expectBinaries = 8;
              };
          } // (
            lib.mapAttrs' (
              variantName: _v:
              lib.nameValuePair "pangea-operator-${variantName}-capabilities-match" (mkVariantCapabilityCheck {
                variant = variantName;
                buildSystem = system;
              })
            ) operatorVariants
          ));
        };

      lavaRubyGemsFor =
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
          gemset = import ./pangea-compiler/gemset.nix;
          sourceGems = pangeaInputs // {
            "pangea-grafana" = inputs.pangea-grafana;
          };
          publishedGems = lib.genAttrs [ "terraform-synthesizer" "abstract-synthesizer" ] (
            name:
            pkgs.fetchurl {
              url = "https://rubygems.org/gems/${name}-${gemset.${name}.version}.gem";
              sha256 = gemset.${name}.source.sha256;
            }
          );
          root = "usr/share/lava/ruby";
          names = builtins.attrNames sourceGems ++ builtins.attrNames publishedGems;
          drv = pkgs.runCommand "lava-ruby-gems" { } (
            lib.concatStrings (
              lib.mapAttrsToList (n: src: ''
                mkdir -p $out/${root}/${n} && cp -r ${src}/lib $out/${root}/${n}/lib
              '') sourceGems
              ++ lib.mapAttrsToList (n: gem: ''
                mkdir -p $out/${root}/${n} && tar -xOf ${gem} data.tar.gz | tar -xzf - -C $out/${root}/${n} lib
              '') publishedGems
              ++ map (n: ''
                test -n "$(find $out/${root}/${n}/lib -name '*.rb' -print -quit)"
              '') names
            )
          );
        in
        drv
        // {
          loadPath = lib.concatMapStringsSep ":" (n: "${drv}/${root}/${n}/lib") (lib.sort lib.lessThan names);
          gemNames = names;
        };

      withEmbedded = lib.foldl' (acc: sys: lib.recursiveUpdate acc (embeddedOperatorExtension sys)) base [
        "x86_64-linux"
        "aarch64-linux"
      ];

      # ruby-eval devShell — the SECOND thing ef86809's gen-pattern
      # conversion silently dropped (confirmed live: ci.yml's `cargo-test`
      # job, which runs `nix develop .#ruby-eval -c cargo test` via the
      # shared nix-devshell-cargo-test.yml reusable, fails with "flake
      # ... does not provide attribute 'devShells.<system>.ruby-eval'").
      # CRuby + libclang (rb-sys's bindgen needs both) on every supported
      # system, matching the embedded image's ruby (3.3) so gem C-extensions
      # built against 3.3 load under the same libruby ABI at test time.
      rubyEvalShell =
        system:
        let
          pkgs = import nixpkgs {
            inherit system;
            config.allowUnfree = true;
          };
          ruby = pkgs.ruby_3_3;
          clang = pkgs.llvmPackages.libclang;
          # `stdenv.cc.libc.dev` is a glibc-only output — darwin's stdenv.cc.libc
          # has no `.dev` split (caught live: a local `nix build
          # .#devShells.aarch64-darwin.ruby-eval` failed eval with "attribute
          # 'dev' missing"). bindgen finds Xcode SDK headers on its own via
          # clang's built-in sysroot detection on darwin, so the explicit
          # libc include only applies — and is only needed — on Linux.
          bindgenLibcArgs = pkgs.lib.optionalString pkgs.stdenv.isLinux "-I${pkgs.stdenv.cc.libc.dev}/include ";
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

      extended = lib.foldl' (acc: sys: lib.recursiveUpdate acc (rubyEvalShell sys // { packages.${sys}."lava-ruby-gems" = lavaRubyGemsFor sys; })) withEmbedded [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];

      # ── The module trio ──────────────────────────────────────────────────
      #
      # ★ WHY A CONTROLLER GETS A SYSTEM DAEMON AND NOT ONLY AN IMAGE.
      #
      # pangea-operator is a Kubernetes controller, and the reflex is that a
      # controller is a Deployment. It does not have to be: kube-rs reads a
      # kubeconfig, so the operator runs perfectly well OUT of the cluster it
      # reconciles — and on a node that hosts its own apiserver, out-of-cluster
      # is the simpler and stricter arrangement.
      #
      # This was not a preference. It was forced, and the forcing is worth
      # recording because it is the shape of the whole problem:
      #
      #   * engenho REFUSES its own ServiceAccount token (401, measured
      #     2026-09-01), so in-cluster config cannot work and a kubeconfig is
      #     mandatory either way.
      #   * The IaC state backend belongs on the host — it must outlive the
      #     cluster it serves, because deleting engenho's data_dir mints a NEW
      #     cluster (the per-cluster PKI seed makes that literal).
      #   * Reaching a host postgres from a pod means either a TCP listener with
      #     a password, or a hostPath mount of the socket — and engenho
      #     deliberately refuses hostPath ("HostPathUnsupported", host-fs risk),
      #     which is the right call and which I am not going to override.
      #
      # As a system daemon all three dissolve at once: the operator connects
      # over the unix socket under PEER AUTH, so there is no password to store,
      # rotate or leak — the kernel vouches for the uid. That is a credential
      # REMOVED from the system, not one protected.
      #
      # `withUserDaemon = false` deliberately: this reconciles cloud
      # infrastructure and holds the state ledger. It is not a per-user agent,
      # and offering a user-scoped arm would invite exactly the second
      # uncoordinated writer the operator's own single-writer design forbids.
      #
      # `daemonSubcommand = ""` because the binary IS the daemon — bare
      # invocation runs the controller. Its few argv flags
      # (--generate-crds / --generate-values-schema / --migrate) are one-shot
      # operator verbs, not modes of the service.
      #
      # Configuration is ENVIRONMENT, not a config file: the operator reads 31
      # env vars and no shikumi surface exists yet, so `withShikumiConfig` is
      # false rather than emitting a file nothing reads. Adopting shikumi is
      # worth doing and is a change to the OPERATOR, not to this wrapper.
      # `pending-shikumi: pangea-operator reads 31 env vars directly`
      trio = (import "${substrate}/lib/module-trio.nix" { inherit (nixpkgs) lib; }).mkModuleTrio {
        name = "pangea-operator";
        description = "pangea-operator — the declare/observe IaC control plane";
        binaryName = "pangea-operator";
        packageAttr = "pangea-operator";
        hmNamespace = "services";
        withSystemDaemon = true;
        withUserDaemon = false;
        daemonSubcommand = "";
        withShikumiConfig = false;
      };

      # `packageAttr` names an attribute the module resolves from `pkgs`, so the
      # overlay is what makes the trio's default reachable. Without it a
      # consumer must set `services.pangea-operator.package` by hand, which is
      # the kind of required-override that turns a default into a trap.
      withTrio = lib.recursiveUpdate extended {
        # ── ★ AND IT MUST ACTUALLY PROVIDE THE PACKAGE ─────────────────────
        # This was shipped EMPTY (`_final: _prev: { }`) directly beneath the
        # comment above explaining why it matters. The comment was right and the
        # code did nothing, so the first consumer hit
        # `error: attribute 'pangea-operator' missing` from the trio's own
        # default — the exact trap the comment names, reintroduced by the line
        # meant to close it.
        #
        # `optionalAttrs` rather than a bare reference: on a system this flake
        # does not build for, the overlay contributes nothing instead of
        # throwing while some unrelated attribute is being evaluated.
        # ── ★ STATIC NAMES, LAZY VALUE — no condition, deliberately ────────
        # An overlay's attribute NAMES are forced early, while the package-set
        # fixpoint is still being assembled. Guarding this with
        # `lib.optionalAttrs (extended.packages ? ${prev.system})` — the
        # obvious "be safe on unsupported systems" move — makes the NAME SET
        # depend on an evaluation, and the consumer gets
        # `error: infinite recursion encountered` from deep inside nixpkgs'
        # `aliases.nix`, naming neither this file nor the overlay.
        #
        # So: one unconditional name, whose VALUE is lazy. Nothing is forced
        # until something actually reads `pkgs.pangea-operator`, and on a
        # system this flake does not build for that read fails with a missing
        # attribute that names the system — an honest error at the point of
        # use, which is strictly better than a guard that cannot be evaluated.
        # `stdenv.hostPlatform.system`, not the deprecated `prev.system`, which
        # emits `evaluation warning: 'system' has been renamed to/replaced by
        # 'stdenv.hostPlatform.system'` in every consumer that touches this
        # overlay.
        overlays.default = _final: prev: {
          pangea-operator = extended.packages.${prev.stdenv.hostPlatform.system}.default;
        };
        nixosModules.default = trio.nixosModule;
        nixosModules.pangea-operator = trio.nixosModule;
        darwinModules.default = trio.darwinModule;
        darwinModules.pangea-operator = trio.darwinModule;
        homeManagerModules.default = trio.homeManagerModule;
        homeManagerModules.pangea-operator = trio.homeManagerModule;
      };
    in
    withTrio;
}
