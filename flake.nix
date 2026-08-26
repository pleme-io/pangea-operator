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
      mkNoRubyOperatorImage = imageSystem:
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
          lockfileBuilder =
            import "${substrate}/lib/build/rust/lockfile-builder.nix" {
              pkgs = staticPkgs;
            };
          plemeCrateOverrides =
            (import "${substrate}/lib/build/rust/pleme-crate-overrides.nix")
              staticPkgs.stdenv.hostPlatform.rust.rustcTarget;
          project = lockfileBuilder.mkProject {
            src = self;
            name = "pangea-operator-noruby";
            specFile = "Cargo.noruby.build-spec.json";
            defaultCrateOverrides =
              staticPkgs.defaultCrateOverrides // plemeCrateOverrides // {
                # Same stopgap as the `base` build above — routes tsunagu
                # through the flake-input fetcher rather than a sandboxed
                # fetchgit with no credentials.
                tsunagu = _oldAttrs: { src = inputs.tsunagu; };
                # magma-protocol needs protoc, and this override is a COPY of
                # the one mkEmbeddedOperatorImage already carries — not a new
                # discovery. `executor_magma` is a DEFAULT cargo feature, so it
                # is in the Ruby-free spec too, and its build.rs falls back to
                # protoc-bin-vendored when PROTOC is unset. The vendored binary
                # is not materialized in the sandbox, so protoc_bin_path()
                # panics:
                #
                #   internal: protoc not found
                #   /build/protoc-bin-vendored-linux-x86_64-3.2.0/bin/protoc
                #
                # Measured on rio, 2026-08-12, at the FIRST build that ever got
                # past the toolchain — the llvm-static blocker had been hiding
                # this one behind it the whole time.
                #
                # The duplication is the real finding. Two image builders in one
                # flake maintain two hand-copied override sets over the same
                # workspace, so a fix landed in one is invisible to the other
                # until its build reaches the same crate. `pending-operator-image-overrides:
                # hoist the shared crate overrides to one binding both
                # mkEmbeddedOperatorImage and mkNoRubyOperatorImage read.`
                magma-protocol = oldAttrs: {
                  nativeBuildInputs = (oldAttrs.nativeBuildInputs or [ ])
                    ++ [ imagePkgs.protobuf ];
                  PROTOC = "${imagePkgs.protobuf}/bin/protoc";
                };
              };
          };
          rawBin = project.workspaceMembers."pangea-operator".build;

          # ── MAKE THE BINARY SCANNABLE, or the CVE gate on this image is a
          #    GREEN THAT INSPECTED NOTHING. Measured 2026-08-12 on the first
          #    build that ever succeeded:
          #
          #      trivy image --input pangea-operator-noruby.tar.gz
          #        -> results: 0   packages: 0   Metadata OS: None
          #
          #    Zero findings because zero targets. A distroless-static image
          #    whose only content is a static Rust binary has no OS package DB
          #    and no embedded dependency metadata, so trivy emits JSON with no
          #    `Results` key and EXITS 0. `scanBeforePush: true` +
          #    `scanFailOnSeverity: UNKNOWN` passes it. The image's own
          #    .trivyignore.noruby being empty does not help: an empty
          #    suppression surface over an empty measurement is still empty.
          #
          #    substrate diagnosed this on 2026-07-30 and PROVED it at the byte
          #    level (cargo-audit-data.nix's header): stripping `.dep-v0` from
          #    nixpkgs' ripgrep flips trivy from a real `Type: rustbinary` row
          #    to Results-absent + exit 0. It then built the fix and, until now,
          #    NOTHING IN THE FLEET CONSUMED IT -- zero call sites repo-wide.
          #    This is the first.
          #
          #    What it proves, stated so it is not rounded up: a CVE-database
          #    verdict against the RESOLVED DEPENDENCY GRAPH, keyed to the
          #    scanner DB at scan epoch. It is not a closure theorem -- that is
          #    closureInfo + vulnix, which can never see a crate advisory
          #    because the whole runtime closure is one derivation NVD has no
          #    entry for. The two are complements.
          auditData =
            (import "${substrate}/lib/build/rust/cargo-audit-data.nix" { })
              .mkCargoAuditData imagePkgs {
                name = "pangea-operator-noruby";
                lockFile = ./Cargo.lock;
                rootCrate = "pangea-operator";
              };
          bin = imagePkgs.runCommand "pangea-operator-noruby-auditable" { } ''
            mkdir -p $out/bin
            cp ${rawBin}/bin/pangea-operator $out/bin/pangea-operator
            chmod +w $out/bin/pangea-operator
            ${(import "${substrate}/lib/build/rust/cargo-audit-data.nix" { }).injectCommand {
              inherit auditData;
              target = "$out/bin/pangea-operator";
              objcopy = "${imagePkgs.binutils}/bin/objcopy";
            }}
          '';

          hardened =
            import "${substrate}/lib/build/oci/hardened-base.nix" {
              pkgs = imagePkgs;
            };
          arch = if imageSystem == "aarch64-linux" then "arm64" else "amd64";
        in
        hardened.mkPackageImage {
          service = "pangea-operator";
          base = hardened.bases.distroless-static;
          package = bin;
          publishName = "pangea-operator-noruby";
          publishTag = arch;
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
            "8080/tcp" = {};
            "8081/tcp" = {};
            "9090/tcp" = {};
          };
        };

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
          # ── Round 3 (2026-07-24, GHSA-hrxh-6v49-42gf grpc-go < 1.82.1) ──
          # The fix line moved past every one of Round 1's bumps (all sat
          # at v1.79.3). Bumping nixpkgs-security's OWN pin again (checked
          # through nixos-unstable rev e2587ca, 2026-07-23 -- the tip of
          # the branch) does NOT help: none of the six escape-hatch
          # packages moved version at all between the two pins, so their
          # vendored grpc-go is unchanged. Two of the nine findings DO have
          # a real, fresh upstream fix (terraform-provider-aws 6.56.0,
          # packer 1.16.0 -- both confirmed via go.mod at the tag: grpc
          # v1.82.1) that nixpkgs-security simply hasn't packaged yet;
          # `hardenedSecurityPkgs` below sources those two directly via the
          # substrate cve-mitigations catalog (the SAME "version+hash bump
          # now, don't wait for the channel" shape as the nixpkgs-security
          # escape hatch itself, applied one layer further). The remaining
          # seven (github/cloudflare/kubernetes/random/porkbun/rabbitmq/
          # opentofu) have NO newer upstream release anywhere that picks up
          # grpc >= 1.82.1 (re-verified 2026-07-24 against each repo's full
          # release list, not just `/latest`) -- see .trivyignore's Round 3
          # section for the residual GHSA-hrxh-6v49-42gf citation.
          hardenedSecurityPkgs = (import "${substrate}/lib/security/mk-hardened-pkgs.nix" { inherit lib; }) {
            pkgs = securityPkgs;
            # packer carries TWO mitigations and they compose in order: the
            # -grpc- one moves 1.15.4 -> 1.16.0 (version + src), and the
            # -gogit- one then patches THAT release's go.mod, since 1.16.0 is
            # the tip of hashicorp's release list and still pins the
            # vulnerable go-git v5.19.1. Both were confirmed against a built
            # binary rather than a manifest.
            mitigations = [
              "terraform-provider-aws-grpc-bump"
              "packer-grpc-bump"
              "packer-gogit-bump"
            ];
            # terraform-provider-kubernetes-kin-openapi-bump is authored in
            # substrate's catalog but deliberately NOT referenced here — a
            # real CI build confirmed it breaks the provider's own source
            # (see the magmaProviderMirror comment below). Left unwired as
            # a record; a real fix needs a source patch to
            # terraform-provider-kubernetes's manifest/openapi package, not
            # just a vendored dependency bump.
          };
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
              # cloudflare/github/kubernetes: no newer upstream release
              # picks up grpc >= 1.82.1 (Round 3, .trivyignore) — stay on
              # the nixpkgs-security escape hatch, unchanged since Round 1.
              #
              # kubernetes ALSO carries the GHSA-jpcw-4wr7-c3vq /
              # GHSA-r277-6w6q-xmqw kin-openapi findings (.trivyignore
              # Round 4 + Round 5) — a vendored go.mod patch bumping
              # kin-openapi 0.111.0 -> 0.144.0 was attempted
              # (substrate's terraform-provider-kubernetes-kin-openapi-bump
              # cve-mitigation, left in the catalog unwired as a record) and
              # CONFIRMED, via a real CI build, to break the provider's own
              # source: kin-openapi 0.144.0 changed `Schema.Type` from a
              # plain string to `*openapi3.Types` and diverged the
              # openapi2/openapi3 SchemaRef split, and
              # terraform-provider-kubernetes's manifest/openapi/*.go still
              # uses the pre-change shape (confirmed compile failure, CI run
              # 30178928379: "cannot use ... as *openapi3.Types value").
              # This is the empirical version of Round 4's own risk
              # assessment ("a bare go.mod replace is materially riskier
              # than a version bump") — not a lower-risk case, a confirmed
              # one.
              cloudflare_cloudflare
              integrations_github
              hashicorp_kubernetes
            ]) ++ [
              # aws: Round 3's real fix — sourced via hardenedSecurityPkgs
              # (the cve-mitigations catalog), not the bare escape hatch.
              hardenedSecurityPkgs.terraform-providers.hashicorp_aws
            ] ++ (with imagePkgs.terraform-providers; [
              # No newer upstream release exists for these three on ANY
              # channel (verified 2026-07-19, re-verified 2026-07-24) —
              # stays on the primary pin. See .trivyignore for the
              # residual CVE citations.
              hashicorp_random
              porkbun
              cyrilgdn_rabbitmq
              # datadog: needed for the absorbed Datadog estate
              # (pangea-datadog-absorb). On the primary pin deliberately —
              # it carries no CVE escape-hatch requirement, so it does not
              # belong in the securityPkgs block above. There is no
              # `tofu init` fallback (PANGEA_FORBID_TOFU + magma's
              # locate_provider reads MAGMA_PROVIDER_DIR), so an absent
              # provider here is a hard ProviderUnavailable, not a slow path.
              datadog_datadog
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
            # opentofu stays on the BARE escape hatch — its newest tag
            # (v1.12.5) doesn't bump grpc past v1.79.3 either (Round 3,
            # .trivyignore); no fix to layer on yet.
            securityPkgs.opentofu # TofuExecutor: unconditionally
            # constructed at controller startup, resolved whenever
            # spec.executor / PANGEA_EXECUTOR names tofu (PANGEA_FORBID_TOFU
            # is the explicit kill-switch) — a real, tested,
            # config-selectable fallback per MAGMA-NATIVE EXECUTION, not
            # dead weight.
            # packer: Round 3's real fix — 1.16.0 via hardenedSecurityPkgs
            # (the cve-mitigations catalog; the bare nixpkgs-security escape
            # hatch still only has 1.15.4).
            hardenedSecurityPkgs.packer # PackerExecutor: unconditionally
            # built, driven by the unconditionally-spawned
            # PackerBuildController + sibling AmiTestController reconciling
            # AMI builds — a separate, equally real concern from the
            # tofu/magma question.
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
            # See the `tsunagu` flake-input comment near the top of this
            # file — this is the load-bearing half for the embedded
            # image build (release.yml's "Build image with Nix" step,
            # the one confirmed failing on the sandboxed-fetchgit
            # private-repo credential gap). Stopgap; remove once the
            # shared image-push.yml fix lands.
            tsunagu = _oldAttrs: { src = inputs.tsunagu; };
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
          packages.${system} = {
            "dockerImage-operator-embedded-${arch}" = mkEmbeddedOperatorImage system;
            # Additive sibling — see mkNoRubyOperatorImage.
            "dockerImage-operator-noruby-${arch}" = mkNoRubyOperatorImage system;
          };
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
