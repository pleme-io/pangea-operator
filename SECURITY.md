# Security Policy

## Reporting a vulnerability

**Do not open public issues for security bugs.**

Use GitHub's [Private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability) on this repository — the "Security" tab → "Report a vulnerability".

If that's not available to you, email `security@pleme.io` <!-- TODO: replace with real address before launch --> with:

- A description of the issue
- Affected versions
- Reproduction steps
- Suggested fix (optional)

## Response targets

| Step | Target |
|---|---|
| Acknowledgement | 5 business days |
| Triage + severity assessment | 10 business days |
| Coordinated disclosure window | 90 days from acknowledgement |

We will keep you informed of progress and credit you in the release notes unless you request otherwise.

## Scope

In scope:

- The operator binary (`pangea-operator/`)
- The pangea-cli helper (`pangea-cli/`)
- The shared types crate (`pangea-types/`)
- The embedded Ruby evaluator (`pangea-ruby-eval/`)
- The Helm chart at `charts/pangea-operator/`
- The release pipelines under `.github/workflows/`

Out of scope (report upstream):

- The `tofu` / `terraform` binary itself → [opentofu/opentofu](https://github.com/opentofu/opentofu)
- The Ruby interpreter via `magnus` → [matsadler/magnus](https://github.com/matsadler/magnus)
- Kubernetes itself → [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes)

## Supported versions

| Version | Status |
|---|---|
| `0.1.x` | Active |
| `< 0.1` | Pre-public; internal track only |

Security fixes land in the next patch release on the current minor line.
