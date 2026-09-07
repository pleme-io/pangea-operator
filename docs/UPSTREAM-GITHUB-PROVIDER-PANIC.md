# terraform-provider-github 6.13.0 panics on a branch-protection plan

**Status:** open upstream, worked around by omission. Measured 2026-09-06 on plo.

## The panic

```
panic: interface conversion: interface {} is nil, not string
  github/resource_github_branch_protection_migration.go:27
  terraform-provider-github 6.13.0 (integrations/github)
```

The provider process dies, which kills the whole plan — not just the
`github_branch_protection` resources. A 1005-repo reconciliation in which **5
repos** declare branch protection therefore cannot plan at all.

## What triggers it

A plan containing `github_branch_protection` resources that are **absent from
prior state**. Verified directly before working around it:

```
github_branch_protection in state: 0
total resources in state:       2011   (1005 repos + 1005 actions perms + 1)
```

The panicking frame is a **state migration**, and it runs against a resource
with no prior state to migrate — so the field it converts to `string` is nil.
`resource_github_branch_protection_migration.go:27` does that conversion
unguarded.

That ordering is what makes it invisible until adoption is involved. A
greenfield `terraform apply` that creates branch protection from nothing does
not hit it; a plan that has just *imported* 2008 sibling resources and is
computing creates for the remainder does.

## Why it is worked around by omission rather than by shaping our output

The emitted resource is well-formed — the same shape the Ruby path has emitted
into production. Changing it to dodge a nil dereference inside a migration
function would be shaping our IR around someone else's bug, and the shape it
would have to take is not knowable from outside the provider.

Omission is safe **because state holds none of them**. Removing a resource from
config while it is in state means DESTROY; removing one that was never in state
means UNMANAGED. The 5 repos keep their existing GitHub branch protection
untouched — it is simply not managed by this template until the panic is fixed.

Repos affected: `cse-lint`, `tear`, `shigoto` (profile `standard`);
`tatara-rust-ast`, `shiken` (profile `pilot`).

## Re-enabling

Set `has_branch_protection` back to the resolver's real value (it reports the
truth; the suppression is applied when building the CR's `spec.variables`, not
in `org_resolve`). Then plan. If the panic is gone, the 5 resources adopt or
create normally.

## What to report upstream

The unguarded conversion at `resource_github_branch_protection_migration.go:27`,
with the reproduction being **a plan where the resource is not in prior state**.
A migration function is only meaningful for state that exists; being reached
with nil state is the actual defect, and guarding the conversion is a smaller
fix than whatever produces the nil.
