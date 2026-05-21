<!-- Thanks! A few prompts to help reviewers. -->

## Summary

<!-- 1–3 sentences: what does this PR change and why? -->

## Related issue

<!-- "Closes #123" or "Refs #123" — leave blank if standalone -->

## Type

- [ ] Bug fix
- [ ] New reconciler / CRD field
- [ ] Refactor / cleanup
- [ ] Docs
- [ ] Test / CI
- [ ] Chore (deps, build, chart polish)
- [ ] Breaking change to a public CRD or values schema (please justify)

## Checklist

- [ ] `cargo test --workspace` passes
- [ ] `nix flake check` passes
- [ ] If chart values changed: documented in `values.yaml` + chart README
- [ ] If CRDs changed: regenerated `templates/crds/crds.yaml`
- [ ] If breaking: noted in `CHANGELOG.md` under `[Unreleased]`
