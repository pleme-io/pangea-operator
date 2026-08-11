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

# The property that makes this spec worth having, asserted rather than
# assumed: a variant that resolved to the same features as the canonical
# spec builds fine, ships, and is simply not the variant anyone asked
# for. Nothing would error.
python3 - "$root" <<'PY'
import json, sys, pathlib
root = pathlib.Path(sys.argv[1])
canon = json.loads((root / "Cargo.build-spec.json").read_text())
variant = json.loads((root / "Cargo.noruby.build-spec.json").read_text())
key = next(k for k in canon["crates"] if k.startswith("pangea-operator"))
c = set(canon["crates"][key]["features"])
v = set(variant["crates"][key]["features"])
missing = {"embedded_ruby", "default"} - (c - v)
if missing:
    sys.exit(
        f"regen-noruby-spec: FAILED — the variant still carries {sorted(missing)}.\n"
        f"  canonical: {sorted(c)}\n  variant  : {sorted(v)}\n"
        "A variant that resolves to the same features as canonical is not a variant."
    )
print(f"regen-noruby-spec: variant subtracts {sorted(c - v)} — correct")
PY
