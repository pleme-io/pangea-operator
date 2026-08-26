#!/usr/bin/env bash
# Regenerate the Ruby-free build spec.
#
# `Cargo.noruby.build-spec.json` is the second of two build specs this
# repo commits. gen bakes the RESOLVED feature set into a spec and Nix's
# rootFeatures is not honored on the lockfile-builder path, so the spec
# IS the feature decision — there is no way to ask a build for fewer
# features than its spec carries. Two variants therefore need two specs.
#
# Run this whenever Cargo.lock moves. It is NOT covered by the D2
# pre-commit tie, which judges Cargo.lock against Cargo.gen.lock and
# knows nothing about the variant; a stale variant spec is silent, which
# is exactly why this script prints a diff of what changed instead of
# regenerating quietly.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
spec="$root/Cargo.noruby.build-spec.json"

# Keep this list in lockstep with the default set in
# pangea-operator/Cargo.toml MINUS the two Ruby-linking features. A
# feature added to the default set and not added here ships in the Ruby
# image and silently vanishes from the Ruby-free one.
#
# BOTH must be omitted. magma-rubygems links libruby through magnus/rb-sys
# exactly as pangea-ruby-eval does, and it used to ride inside
# executor_magma — so dropping embedded_ruby alone took the operator from
# 76 runtime deps to 75 and left Ruby in the closure anyway.
FEATURES="graphql,grpc,executor_magma"

before=""
[[ -f "$spec" ]] && before="$(cat "$spec")"

gen build "$root" \
  --no-default-features \
  --features "$FEATURES" \
  --out Cargo.noruby.build-spec.json

if [[ -n "$before" ]] && [[ "$before" == "$(cat "$spec")" ]]; then
  echo "regen-noruby-spec: unchanged"
  exit 0
fi
echo "regen-noruby-spec: $spec CHANGED — review before committing"

# ── ★ WHAT THIS ASSERTS, AND WHY IT CHANGED (2026-08-26) ─────────────
# The property worth gating is that NO SHIPPED SPEC CARRIES embedded_ruby.
# Asserted rather than assumed, because a spec that quietly regained Ruby
# would build fine, ship, and simply not be the thing anyone asked for —
# nothing would error, and the tag would still say noruby.
#
# It used to assert something subtly different: that canonical MINUS
# variant contained embedded_ruby — i.e. "the variant is the one that
# drops Ruby". That held only while `default` still included Ruby. Ruby
# has now left the default feature set, so canonical does not carry it
# either, the difference can never contain it, and the old guard could
# never pass again. It failed on exactly the change it was written to
# protect: Ruby actually leaving.
#
# The variant is now DEGENERATE — it resolves to the same features as
# canonical, minus the `default` marker. It is deliberately retained
# rather than deleted (★★ MODULARIZE, DON'T DELETE): the two-spec
# machinery is what makes a Ruby-bearing build expressible again, and
# reinstating one is a feature list, not a rebuild of this tooling.
python3 - "$root" <<'PY'
import json, sys, pathlib
root = pathlib.Path(sys.argv[1])
canon = json.loads((root / "Cargo.build-spec.json").read_text())
variant = json.loads((root / "Cargo.noruby.build-spec.json").read_text())
key = next(k for k in canon["crates"] if k.startswith("pangea-operator"))
c = set(canon["crates"][key]["features"])
v = set(variant["crates"][key]["features"])
# The invariant: Ruby ships in NEITHER spec. Checked on both sides, because
# the whole point is that the image is Ruby-free, not that the two specs
# differ in a particular way.
ruby = [name for name, feats in (("canonical", c), ("variant", v)) if "embedded_ruby" in feats]
if ruby:
    sys.exit(
        f"regen-noruby-spec: FAILED — embedded_ruby is present in: {sorted(ruby)}.\n"
        f"  canonical: {sorted(c)}\n  variant  : {sorted(v)}\n"
        "A build that carries Ruby must not ship under a Ruby-free tag."
    )
if not (c - v):
    print("regen-noruby-spec: variant is degenerate (same features as canonical) — "
          "expected now that Ruby has left the default feature set")
else:
    print(f"regen-noruby-spec: variant subtracts {sorted(c - v)}")
print("regen-noruby-spec: embedded_ruby absent from both specs — correct")
PY
