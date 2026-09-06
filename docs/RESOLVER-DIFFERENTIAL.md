# Two resolvers, one policy — Ruby is the authority, Rust is the default

`github-org-repos.tlisp` renders a GitHub org's repository posture, and its
header states the split it depends on: **lava is a pure evaluator with no I/O**,
so RESOLVE (read `org.yaml`, call the GitHub API, learn live state) stays with
the caller and arrives as typed record fields. RENDER is the architecture's job.

Two implementations of RESOLVE exist. **Both are maintained. Neither is being
deleted.**

| | `pangea-architectures/bin/lava-resolve-org` | `pangea-operator/src/org_resolve.rs` |
|---|---|---|
| Language | Ruby, 228 lines | Rust |
| Status | **the authority** | **the default** |
| Policy source | reads the live gem constants | mirrors them, pinned by tests |
| Runs via | `nix develop -c ruby bin/lava-resolve-org` | `pangea-operator --resolve-org` |
| Reaches | the whole `pangea-*` gem chain | nothing outside this binary |

The Rust one is the default because it removes Ruby from the reconcile path.
The Ruby one remains the authority because **it reads the policy rather than
copying it**, and it is where a policy change lands first.

## Why the Ruby one cannot simply be replaced

Its own header says it, and it was right:

> A Rust resolver would reimplement them and diverge silently, which is the
> exact failure the architecture's header refuses.

The record the architecture wants is not `org.yaml` verbatim. It needs the
`Dry::Struct` defaults, the branch-protection presets, and the CI-shim
templates — all three live in the gem. Copying them into Rust is a projection,
and a projection drifts.

Measured 2026-09-06 against the live 1005-repo catalogue, the Rust resolver had
**six** divergences:

| field | Rust did | authority | rows affected |
|---|---|---|---|
| `delete_branch_on_merge` | hardcoded `true` | row value, default `true` | **847 declare false** |
| `has_issues` | hardcoded `true` | row value, default `true` | 98 declare false |
| `actions_enabled` | hardcoded `true` | tri-state; `nil` ⇒ `visibility != internal` | 974 rely on it |
| `standard_labels` | defaulted `true` | `types.rb:363` defaults **false** | 4 rely on it |
| `has_branch_protection` | `bp != "none"` | `!archived && bp != "none"` | any archived repo with a preset |
| `bp_strict` | derived from preset | **constant `false`** — the no-change value | 5 protected repos |

`has_issues` and `delete_branch_on_merge` were not even fields on `OrgRepoRow`,
so a row's declared value had no path into the record at all.

## ★ The trap that produced two of those, worth reading before touching presets

There are **two tables named for branch-protection profiles**, and the one whose
fields read like policy is the dead one:

- `Pangea::Architectures::OpenSourceRepo::PROFILES` — carries
  `required_reviews` and `dismiss_stale_reviews`. **Read nowhere.** It is a
  validity whitelist. `lava-resolve-org` says so explicitly.
- `Pangea::Helpers::Github::BRANCH_PROTECTION_PROFILES`
  (`pangea-github/lib/pangea/helpers/github_presets.rb:75`) — carries
  `enforce_admins` / `require_signed_commits` / `required_linear_history`.
  **This is what the emitter fetches.**

In the emitting table, `pilot` and `standard` are **byte-identical**; only
`hardened` differs. A fix pinned to the whitelist invented a pilot-vs-standard
distinction that does not exist in any output, and derived `bp_strict` from it —
which would have shown up as a live plan diff against 5 real repos.

**Read what the emitter fetches, not what looks authoritative.**

## Running each

```
# authority (needs the gem chain; the nix devShell provides it)
cd pangea-architectures
nix develop -c ruby bin/lava-resolve-org \
  --org-yaml workspaces/pleme-io-opensource/org.yaml --owner pleme-io

# default
pangea-operator --resolve-org \
  --catalogue .../workspaces/pleme-io-opensource/org.yaml --owner pleme-io
```

Both emit the same record shape: the union of every field the architecture
references, as string scalars (`"true"` / `"false"` / `""`), one object per repo.

## What is proven, and what is not

**Proven.** Every field the Rust resolver emits was compared against the Ruby
derivation by reading both, and the differences above were fixed. 13 tests pin
each default with the gem's `file:line` and the row count that relies on it.

**NOT proven: runtime equivalence.** The comparison above is *static* — a human
reading two programs. Nothing yet runs both against the same `org.yaml` and
diffs the output. That harness is the real oracle, and until it is green:

> **Do not let an apply touch real repositories on the strength of the Rust
> resolver alone.** Static comparison found five bugs on its first pass, which
> is evidence that it catches things — and equally that a sixth could be sitting
> where nobody looked.

The blocker on that harness is cost, not design: the Ruby side needs the full
gem chain built, which is the dependency the Rust path exists to remove. The
shape it should take is the fleet's own oracle pattern — freeze the Ruby output
for a pinned catalogue subset as a committed golden, and gate the Rust resolver
against it, so regenerating the golden is an explicit reviewable act rather than
a live cross-repo build on every test run.

## Divergences that are deliberate

One, and it is named rather than left to be discovered: on an **unknown**
branch-protection profile the Ruby `fetch`es with a block and **raises**; the
Rust `parse` returns `None` (unprotected). Under-claiming protection is the safe
direction for a resolver — the plan then proposes adding it — but the loud
failure belongs at catalogue-validation time, where the typo can be reported
against the row that contains it. Neither behaviour is currently wired to a
validator.
