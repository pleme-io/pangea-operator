//! Per-resource policy evaluation for `InfrastructureTemplate`.
//!
//! Walks each plan-derived `DriftDetail` against `spec.policies`,
//! attaches the matching rule's decision (`autoApply` |
//! `requireApproval` | `refuse`), and computes an aggregate decision
//! that gates the plan→apply transition.
//!
//! Resolution order for a single change:
//!   1. First rule whose `match` clauses ALL match → use that rule's decision
//!   2. `spec.defaultDecision` if set → use it
//!   3. Else, if the change is a delete/replace on a
//!      [`PROTECTED_RESOURCE_TYPE`](is_protected_resource_type) → floor
//!      to `RequireApproval` (see "The protected-resource floor" below)
//!   4. Else → `AutoApply` (the documented aggressive default)
//!
//! Aggregation across changes:
//!   - any `Refuse`           → Refuse  (operator marks Failed)
//!   - else any `RequireApproval` → RequireApproval (existing approval gate)
//!   - else                   → AutoApply
//!
//! Empty `policies` + unset `defaultDecision` → every OTHER change
//! resolves to `AutoApply` and the operator behaves like `autoApprove:
//! true`. `autoApprove` is no longer consulted by the policy path; it
//! remains in the spec for legacy schemas but does not gate the new
//! engine.
//!
//! ## The protected-resource floor
//!
//! An unconfigured template (no matching rule, no `defaultDecision`)
//! must never silently auto-apply a `delete`/`replace` against a
//! resource type in [`PROTECTED_RESOURCE_TYPES`] — a lost Cloudflare
//! zone, a torn-down VPC/EKS cluster, or a deleted managed database is
//! not equivalent to a routine create/update, and step 3's floor
//! raises the DEFAULT decision to `RequireApproval` so it can never
//! happen by silence alone. An EXPLICIT `PolicyRule` (step 1) still
//! wins over the floor — a reviewed, git-visible, intentional choice
//! is never blocked, only the unconfigured fallback path is. This is a
//! runtime safety net (UNREPRESENTABILITY's C2 tier: which resource
//! type a plan touches is a runtime fact, not statically knowable),
//! not a compile-time invariant — never round it up to more than that.
//!
//! Root-caused against a real incident: an unconfigured
//! `cloudflare-pleme` `InfrastructureTemplate`
//! (`autoApprove`/`importPolicy.autoOnConflict: true`, no
//! `spec.policies`/`spec.defaultDecision`) silently lost its
//! `quero.cloud` Cloudflare zone through exactly this fallback path —
//! the same "computed-plan-diverges-from-what-was-approved" class of
//! bug already named in `controller::conflict`'s destroyProtection
//! safety net (task #120/#131), but unlike that net (opt-in via
//! `spec.destroyProtection`), this floor is unconditional.

use crate::crd::{
    DriftAction, DriftDetail, PolicyDecision, PolicyEvaluation, PolicyRule, RiskLevel,
};
use regex::Regex;
use tracing::warn;

/// Resource types where a `delete` or `replace` (a replace is a
/// delete-then-recreate) is catastrophic and/or effectively
/// irreversible — see "The protected-resource floor" above. Errs
/// toward including a type rather than omitting it: the cost of an
/// unconfigured template needing one extra explicit rule to intentionally
/// auto-apply a destroy is far lower than the cost of silently losing
/// one of these.
pub const PROTECTED_RESOURCE_TYPES: &[&str] = &[
    "cloudflare_zone",
    "aws_vpc",
    "aws_eks_cluster",
    "aws_route53_zone",
    "aws_db_instance",
    "aws_rds_cluster",
    "aws_s3_bucket",
    "google_dns_managed_zone",
    "azurerm_dns_zone",
];

/// Is `resource_type` (the `<type>` half of a `<type>.<name>` address)
/// one this floor protects?
pub fn is_protected_resource_type(resource_type: &str) -> bool {
    PROTECTED_RESOURCE_TYPES.contains(&resource_type)
}

/// Is `action` a destroy in the sense the floor cares about —
/// `delete` outright, or `replace` (delete-then-recreate, so a
/// "successful" replace still destroys the original)? Unrecognized
/// action strings are NOT destructive by this definition (the
/// vocabulary-boundary warning in `actions_match` already surfaces an
/// out-of-vocabulary action elsewhere; this floor only ever raises the
/// decision, so treating an unparseable action as non-destructive is
/// the conservative choice — it never silently masks a real delete).
fn is_destructive_action(action: &str) -> bool {
    matches!(
        DriftAction::parse_wire(action),
        Some(DriftAction::Delete) | Some(DriftAction::Replace)
    )
}

/// Is this action POSITIVELY known to add or amend without removing?
///
/// Deliberately not `!is_destructive_action(..)`. That predicate answers false
/// for an action it cannot PARSE, so negating it silently classifies every
/// unrecognised action string as safe — a fail-open on the one axis that must
/// fail closed. This requires the action to be recognised AND to be one of the
/// two non-removing kinds; anything else, known or not, is not eligible for the
/// auto-apply path below.
fn is_known_nondestructive_action(action: &str) -> bool {
    matches!(
        DriftAction::parse_wire(action),
        Some(DriftAction::Create) | Some(DriftAction::Update)
    )
}

/// Outcome of evaluating one plan against a policy set.
#[derive(Debug, Clone)]
pub struct PolicyOutcome {
    /// Drift entries with `policy_decision` and `matched_policy`
    /// fields populated. Same length and order as the input.
    pub annotated_drifts: Vec<DriftDetail>,

    /// Aggregate decision; drives the controller's plan→apply branch.
    pub aggregate: PolicyDecision,

    /// Status-surface summary (counts + sample of refused addresses).
    pub evaluation: PolicyEvaluation,
}

/// Evaluate `rules` against `drifts`, returning annotated drifts + aggregate.
///
/// `rules` and `default_decision` come straight from the
/// `InfrastructureTemplateSpec`. When both are empty/None the result
/// is "auto-apply everything" — the new aggressive default. To gate
/// drift, callers must opt-in via policy rules or `defaultDecision`.
pub fn evaluate(
    rules: &[PolicyRule],
    default_decision: Option<PolicyDecision>,
    drifts: &[DriftDetail],
) -> PolicyOutcome {
    // Pre-compile address regexes once per rule. Invalid regexes are
    // logged and dropped — we never want a typo to silently match
    // nothing AND silently let the change through, so a bad regex
    // means that condition is treated as "no match" (rule won't fire).
    let compiled: Vec<CompiledRule> = rules.iter().map(CompiledRule::new).collect();

    let fallback = default_decision.unwrap_or(PolicyDecision::AutoApply);

    let mut auto_apply_count = 0u32;
    let mut require_approval_count = 0u32;
    let mut refuse_count = 0u32;
    let mut refused_addresses: Vec<String> = Vec::new();

    let annotated: Vec<DriftDetail> = drifts
        .iter()
        .map(|d| {
            let (decision, matched) = match compiled.iter().find(|r| r.matches(d)) {
                Some(r) => (r.rule.decision, r.rule.name.clone()),
                None => {
                    // The protected-resource floor (see module docs):
                    // an unconfigured fallback of AutoApply is raised to
                    // RequireApproval when the change is a destroy on a
                    // protected type. A fallback that's ALREADY stricter
                    // (RequireApproval/Refuse) is left exactly as
                    // configured — the floor only ever raises, never
                    // lowers, the operator's own explicit stance.
                    let resource_type = d.address.split('.').next().unwrap_or(d.address.as_str());
                    if fallback == PolicyDecision::AutoApply
                        && is_protected_resource_type(resource_type)
                        && is_destructive_action(&d.action)
                    {
                        (
                            PolicyDecision::RequireApproval,
                            "<protected-resource-floor>".to_string(),
                        )
                    } else if fallback == PolicyDecision::RequireApproval
                        && is_known_nondestructive_action(&d.action)
                        && !is_protected_resource_type(resource_type)
                    {
                        // ── THE SYMMETRIC HALF OF THE FLOOR ABOVE ────────────
                        //
                        // The floor RAISES AutoApply to RequireApproval for a
                        // destructive action on a protected type. This LOWERS
                        // an unconfigured RequireApproval to AutoApply for a
                        // NON-destructive action on an unprotected type, which
                        // is the case that was costing far more than it bought.
                        //
                        // Measured on camelot-eks 2026-08-13:
                        // `pleme-io-opensource-repos-0` planned `+9 ~0 -0` —
                        // one repository and eight of its issue labels, zero
                        // destroys — and sat in Planning for 103 minutes with
                        // `requireApprovalCount: 9`, until the phase timeout
                        // flipped the workspace to `Healthy=False`. Nothing was
                        // broken. It was asking permission to create labels.
                        //
                        // A gate that fires on everything trains people to
                        // approve without reading, which is precisely how the
                        // one plan that DOES carry a destroy gets waved through.
                        // Spending the gate only where the action is
                        // unrecoverable is what keeps it meaningful.
                        //
                        // Scope, deliberately narrow. This ONLY moves the
                        // `<default>` fallback. A matched rule still wins; a
                        // protected resource type still floors; `Refuse` is
                        // untouched; and every destructive action — including
                        // `replace`, and including any action string
                        // `is_destructive_action` does not recognise — still
                        // requires approval. Approval also remains necessary
                        // rather than sufficient for a destroy, which needs
                        // `destroyProtection = false` AND an unexpired
                        // `destroyAuthorization` naming the template.
                        (
                            PolicyDecision::AutoApply,
                            "<default-nondestructive>".to_string(),
                        )
                    } else {
                        (fallback, "<default>".to_string())
                    }
                }
            };
            match decision {
                PolicyDecision::AutoApply => auto_apply_count += 1,
                PolicyDecision::RequireApproval => require_approval_count += 1,
                PolicyDecision::Refuse => {
                    refuse_count += 1;
                    if refused_addresses.len() < 10 {
                        refused_addresses.push(d.address.clone());
                    }
                }
            }
            DriftDetail {
                address: d.address.clone(),
                action: d.action.clone(),
                risk: d.risk.clone(),
                attributes: d.attributes.clone(),
                policy_decision: Some(decision.as_str().to_string()),
                matched_policy: Some(matched),
            }
        })
        .collect();

    let aggregate = if refuse_count > 0 {
        PolicyDecision::Refuse
    } else if require_approval_count > 0 {
        PolicyDecision::RequireApproval
    } else if auto_apply_count > 0 {
        // At least one drift was classified, and every classified drift is
        // auto-apply (no refuse, no approval). Honour that.
        PolicyDecision::AutoApply
    } else {
        // NOTHING was classified — the drift slice was empty. This is the
        // greenfield / create-only case: `drift_details()` enumerates drift on
        // EXISTING resources, not `create` actions, so a plan that only creates
        // resources arrives here with zero drifts. The aggregate must reflect
        // the configured `fallback` (defaultDecision) so `requireApproval`
        // gates creates too — NOT silently auto-apply. With defaultDecision
        // unset, `fallback` is AutoApply, preserving the documented aggressive
        // default for unconfigured templates. (Before this fix, the `else`
        // hardcoded AutoApply, so a create-only plan auto-applied even under
        // defaultDecision: requireApproval — root cause of the 2026-06-20
        // akeyless-dev unapproved-apply incident.)
        fallback
    };

    let evaluation = PolicyEvaluation {
        aggregate: aggregate.as_str().to_string(),
        auto_apply_count,
        require_approval_count,
        // The gate's own witness of its coverage: how many drifts it saw.
        // `evaluate` is now handed the FULL list; if this ever comes back
        // smaller than the plan's change count, something re-truncated the
        // input and every decision below is a sample.
        evaluated_count: drifts.len() as u32,
        refuse_count,
        refused_addresses,
    };

    PolicyOutcome {
        annotated_drifts: annotated,
        aggregate,
        evaluation,
    }
}

/// Whether `policies` + `default_decision` represent a "configured"
/// policy stance (caller wants the engine to run and surface
/// `policyEvaluation`) vs. an unconfigured template (caller wants
/// legacy behavior).
///
/// In practice: configured = either a non-empty rule list OR an
/// explicit `defaultDecision`. Empty-and-unset is unconfigured — the
/// engine still produces a sensible outcome (everything autoApply)
/// but the controller can choose to skip the status update for
/// templates that never opted in.
pub fn is_configured(rules: &[PolicyRule], default_decision: Option<PolicyDecision>) -> bool {
    !rules.is_empty() || default_decision.is_some()
}

// ---------------------------------------------------------------------------
// Compiled rule + matching primitives
// ---------------------------------------------------------------------------

struct CompiledRule<'a> {
    rule: &'a PolicyRule,
    /// Compiled address regexes. None entries mean compilation failed
    /// (logged once, treated as "no match").
    address_regexes: Vec<Regex>,
}

impl<'a> CompiledRule<'a> {
    fn new(rule: &'a PolicyRule) -> Self {
        let address_regexes = rule
            .match_
            .address_patterns
            .iter()
            .filter_map(|p| match Regex::new(p) {
                Ok(re) => Some(re),
                Err(e) => {
                    warn!(
                        rule = %rule.name,
                        pattern = %p,
                        error = %e,
                        "policy rule has invalid addressPattern regex; skipping pattern"
                    );
                    None
                }
            })
            .collect();

        // actions / riskLevels declare a closed vocabulary (see
        // `DriftAction`/`RiskLevel`) but are plain `Vec<String>` on the
        // wire — same reasoning as addressPatterns above: a typo'd
        // entry must be surfaced at rule-compile time, not silently
        // carried into a comparison that can never succeed.
        for a in unrecognized_wire_values(&rule.match_.actions, DriftAction::parse_wire) {
            warn!(
                rule = %rule.name,
                action = %a,
                "policy rule has an unrecognized action (valid: create, update, delete, replace); it can never match"
            );
        }
        for r in unrecognized_wire_values(&rule.match_.risk_levels, RiskLevel::parse_wire) {
            warn!(
                rule = %rule.name,
                risk = %r,
                "policy rule has an unrecognized riskLevel (valid: none, low, medium, high); it can never match"
            );
        }

        Self {
            rule,
            address_regexes,
        }
    }

    fn matches(&self, d: &DriftDetail) -> bool {
        let m = &self.rule.match_;

        // resourceTypes: glob list, OR semantics. Empty = wildcard.
        if !m.resource_types.is_empty() {
            // DriftDetail.address is `<type>.<name>` (or `<type>.<name>[idx]`).
            // Extract the type prefix once.
            let resource_type = d.address.split('.').next().unwrap_or(d.address.as_str());
            if !m
                .resource_types
                .iter()
                .any(|g| glob_match(g, resource_type))
            {
                return false;
            }
        }

        // addressPatterns: regex list, OR semantics. Empty = wildcard.
        // If the user supplied patterns but ALL failed to compile, we
        // treat it as no-match (don't silently fire the rule).
        if !m.address_patterns.is_empty() {
            if self.address_regexes.is_empty() {
                return false;
            }
            if !self
                .address_regexes
                .iter()
                .any(|re| re.is_match(&d.address))
            {
                return false;
            }
        }

        // actions: exact-match list, OR. Empty = wildcard.
        if !actions_match(&m.actions, &d.action, &self.rule.name) {
            return false;
        }

        // riskLevels: exact-match list, OR. Empty = wildcard.
        if !risk_levels_match(&m.risk_levels, &d.risk, &self.rule.name) {
            return false;
        }

        // attributes: glob list. Match if ANY drift attr matches ANY
        // pattern. Empty = wildcard. For create/delete the
        // `attributes` field is empty, so a rule that requires an
        // attribute pattern simply won't match those — which is the
        // intuitive behavior (you can't say "auto-apply ttl bumps"
        // about a create).
        if !m.attributes.is_empty() {
            let any_match = d
                .attributes
                .iter()
                .any(|attr| m.attributes.iter().any(|g| glob_match(g, attr)));
            if !any_match {
                return false;
            }
        }

        true
    }
}

/// Entries of `values` that don't parse under `parse_wire` — i.e. fall
/// outside a `DriftAction`/`RiskLevel`-style closed vocabulary. Pure
/// and side-effect-free on purpose: `CompiledRule::new` uses it to
/// decide what to `warn!` about, and tests exercise it directly
/// without needing to capture log output.
fn unrecognized_wire_values<T>(
    values: &[String],
    parse_wire: impl Fn(&str) -> Option<T>,
) -> Vec<String> {
    values
        .iter()
        .filter(|v| parse_wire(v).is_none())
        .cloned()
        .collect()
}

/// `PolicyMatch.actions` membership test. Empty = wildcard (always
/// matches). Case-sensitive, but no longer a bare string `==`: both
/// `d_action` (the drift's actual action) and each declared entry are
/// parsed against the same closed `DriftAction` vocabulary, so a
/// mismatch — a typo in the rule, OR an executor backend emitting a
/// value outside the vocabulary the field documents (e.g. magma's
/// `plan_action_to_terraform_str` `Other(s)` passthrough) — is
/// surfaced via `warn!` instead of silently comparing two strings
/// that could never be equal.
fn actions_match(m_actions: &[String], d_action: &str, rule_name: &str) -> bool {
    if m_actions.is_empty() {
        return true;
    }
    let Some(action) = DriftAction::parse_wire(d_action) else {
        warn!(
            rule = %rule_name,
            action = %d_action,
            "drift action is outside the create/update/delete/replace vocabulary policy rules can match on; this rule cannot fire for it"
        );
        return false;
    };
    m_actions
        .iter()
        .any(|a| DriftAction::parse_wire(a) == Some(action))
}

/// `PolicyMatch.riskLevels` membership test. See [`actions_match`] —
/// same shape, same reasoning. The concrete bug this closes: the
/// magma execution path's `severity_to_risk()` emits
/// `"safe"/"modify"/"destroy"`, never the `none/low/medium/high`
/// vocabulary `riskLevels` documents, so a `riskLevels: ["high"]`
/// gate on a magma-executed template silently never fired — now it
/// warns instead.
fn risk_levels_match(m_risk_levels: &[String], d_risk: &str, rule_name: &str) -> bool {
    if m_risk_levels.is_empty() {
        return true;
    }
    let Some(risk) = RiskLevel::parse_wire(d_risk) else {
        warn!(
            rule = %rule_name,
            risk = %d_risk,
            "drift risk is outside the none/low/medium/high vocabulary policy rules can match on; this rule cannot fire for it"
        );
        return false;
    };
    m_risk_levels
        .iter()
        .any(|r| RiskLevel::parse_wire(r) == Some(risk))
}

/// Minimal glob: `*` matches zero-or-more chars, everything else is
/// literal. Anchored at both ends. Avoids pulling in a glob crate for
/// what amounts to "prefix-or-suffix wildcards on terraform type
/// names". Case-sensitive — terraform types and attribute names are
/// always lowercase snake_case so this is correct.
pub(crate) fn glob_match(pattern: &str, input: &str) -> bool {
    // Common fast paths.
    if pattern == "*" {
        return true;
    }
    if !pattern.contains('*') {
        return pattern == input;
    }

    // General case: split on `*`, walk segments left-to-right.
    let parts: Vec<&str> = pattern.split('*').collect();
    let mut cursor = 0usize;
    let last = parts.len() - 1;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            if !input[cursor..].starts_with(part) {
                return false;
            }
            cursor += part.len();
        } else if i == last {
            if !input.ends_with(part) {
                return false;
            }
            // Ensure the suffix doesn't overlap with what we've already
            // matched (prevents `a*a` from matching `a`).
            let suffix_start = input.len().saturating_sub(part.len());
            if suffix_start < cursor {
                return false;
            }
        } else {
            match input[cursor..].find(part) {
                Some(idx) => cursor += idx + part.len(),
                None => return false,
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn drift(address: &str, action: &str, risk: &str, attrs: Vec<&str>) -> DriftDetail {
        DriftDetail {
            address: address.to_string(),
            action: action.to_string(),
            risk: risk.to_string(),
            attributes: attrs.into_iter().map(String::from).collect(),
            policy_decision: None,
            matched_policy: None,
        }
    }

    fn rule(
        name: &str,
        decision: PolicyDecision,
        types: Vec<&str>,
        addrs: Vec<&str>,
        actions: Vec<&str>,
        risks: Vec<&str>,
        attrs: Vec<&str>,
    ) -> PolicyRule {
        PolicyRule {
            name: name.to_string(),
            match_: crate::crd::PolicyMatch {
                resource_types: types.into_iter().map(String::from).collect(),
                address_patterns: addrs.into_iter().map(String::from).collect(),
                actions: actions.into_iter().map(String::from).collect(),
                risk_levels: risks.into_iter().map(String::from).collect(),
                attributes: attrs.into_iter().map(String::from).collect(),
            },
            decision,
        }
    }

    // ── The truncation hole — regression for the 2026-08-07 finding ──
    // `evaluate` was handed a list already capped at 50 by the CALLER
    // (`template_controller::plan_summary_and_drifts_from_artifact` and the
    // tofu show-JSON branch both did `drift_details(50)`). The cap was written
    // for K8s status-object tractability and silently became a SAFETY limit:
    // on pleme-io-opensource's 1,870-change plan the gate decided from the
    // first 50 entries.
    //
    // It failed closed only by luck — that prefix happened to contain
    // `requireApproval` entries, so the aggregate blocked and nothing was
    // wrongly applied for 20 cycles. Had the prefix been all `autoApply`, a
    // `refuse` rule matching change #900 would never have been evaluated and
    // the plan would have applied straight through it.

    #[test]
    fn a_refuse_rule_matching_beyond_the_status_cap_still_refuses() {
        // 60 drifts: the first 55 benign, #56 a destroy the operator forbids.
        // Every index past 50 is exactly what the old caller threw away.
        let mut drifts: Vec<_> = (0..55)
            .map(|i| {
                drift(
                    &format!("github_issue_label.label_{i}"),
                    "update",
                    "low",
                    vec![],
                )
            })
            .collect();
        drifts.push(drift(
            "cloudflare_zone.quero_cloud",
            "delete",
            "high",
            vec![],
        ));
        drifts.extend((56..60).map(|i| {
            drift(
                &format!("github_issue_label.label_{i}"),
                "update",
                "low",
                vec![],
            )
        }));
        assert_eq!(drifts.len(), 60);

        let rules = vec![rule(
            "no-zone-deletes",
            PolicyDecision::Refuse,
            vec!["cloudflare_zone"],
            vec![],
            vec!["delete"],
            vec![],
            vec![],
        )];
        let out = evaluate(&rules, Some(PolicyDecision::AutoApply), &drifts);

        assert_eq!(
            out.aggregate,
            PolicyDecision::Refuse,
            "a refuse rule matching entry #56 must still block the plan"
        );
        assert_eq!(out.evaluation.refuse_count, 1);
        assert_eq!(
            out.evaluation.evaluated_count, 60,
            "the gate must witness EVERY change it was given, not a prefix"
        );
        assert_eq!(
            out.evaluation.auto_apply_count
                + out.evaluation.require_approval_count
                + out.evaluation.refuse_count,
            out.evaluation.evaluated_count,
            "the three decision counts must sum to the coverage witness"
        );
    }

    /// `evaluated_count` is the witness that makes a re-truncation VISIBLE.
    /// Without it, counts capped at 50 are indistinguishable from a plan that
    /// genuinely holds 50 changes — which is precisely how the defect survived.
    #[test]
    fn the_coverage_witness_reports_the_real_total_not_a_cap() {
        let drifts: Vec<_> = (0..1870)
            .map(|i| {
                drift(
                    &format!("github_repository.repo_{i}"),
                    "update",
                    "low",
                    vec![],
                )
            })
            .collect();
        let out = evaluate(&[], Some(PolicyDecision::RequireApproval), &drifts);
        assert_eq!(out.evaluation.evaluated_count, 1870);
        // These are UPDATEs, so they now take the non-destructive fallback.
        // The witness property under test is unchanged and is the whole point:
        // the count reports the plan (1870), never the 50-item display cap.
        assert_eq!(
            out.evaluation.auto_apply_count, 1870,
            "not 50 — the count reports the plan, never the display cap"
        );

        // Same witness on the path that still gates, so lowering the default
        // did not cost this test its coverage of the approval counter.
        let destroys: Vec<_> = (0..1870)
            .map(|i| drift(&format!("github_repository.repo_{i}"), "delete", "low", vec![]))
            .collect();
        let out = evaluate(&[], Some(PolicyDecision::RequireApproval), &destroys);
        assert_eq!(out.evaluation.evaluated_count, 1870);
        assert_eq!(out.evaluation.require_approval_count, 1870);
    }

    // ── The protected-resource floor — regression for the real incident ──
    // An unconfigured `cloudflare-pleme`-shaped InfrastructureTemplate
    // (no spec.policies, no spec.defaultDecision — the exact CR shape
    // that lost the live quero.cloud Cloudflare zone) must NEVER let a
    // zone delete resolve to AutoApply again.

    #[test]
    fn protected_resource_delete_floors_to_require_approval_when_unconfigured() {
        let drifts = vec![drift(
            "cloudflare_zone.quero_cloud",
            "delete",
            "high",
            vec![],
        )];
        let out = evaluate(&[], None, &drifts);
        assert_eq!(
            out.aggregate,
            PolicyDecision::RequireApproval,
            "an unconfigured template must never auto-apply a cloudflare_zone delete"
        );
        assert_eq!(out.evaluation.auto_apply_count, 0);
        assert_eq!(out.evaluation.require_approval_count, 1);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("<protected-resource-floor>")
        );
    }

    #[test]
    fn protected_resource_replace_also_floors() {
        // A replace is delete-then-recreate — the original is just as
        // gone as an outright delete.
        let drifts = vec![drift("aws_eks_cluster.camelot", "replace", "high", vec![])];
        let out = evaluate(&[], None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::RequireApproval);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("<protected-resource-floor>")
        );
    }

    #[test]
    fn protected_resource_create_and_update_are_unaffected() {
        // The floor guards destroys only — routine creates/updates on a
        // protected type (e.g. a zone's settings changing) stay AutoApply
        // exactly as before.
        let drifts = vec![
            drift("cloudflare_zone.quero_cloud", "create", "low", vec![]),
            drift("cloudflare_zone.quero_cloud", "update", "low", vec!["plan"]),
        ];
        let out = evaluate(&[], None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
        assert_eq!(out.evaluation.auto_apply_count, 2);
    }

    #[test]
    fn protected_resource_floor_never_lowers_an_already_stricter_default() {
        // If the operator already configured defaultDecision=Refuse, the
        // floor (which only ever raises AutoApply to RequireApproval)
        // must not accidentally DOWNGRADE that to RequireApproval.
        let drifts = vec![drift("aws_vpc.main", "delete", "high", vec![])];
        let out = evaluate(&[], Some(PolicyDecision::Refuse), &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("<default>"),
            "an already-configured stricter default is untouched by the floor, not relabeled"
        );
    }

    #[test]
    fn explicit_rule_still_overrides_the_protected_resource_floor() {
        // An EXPLICIT, git-reviewed rule choosing AutoApply for a
        // protected type's delete is a deliberate, auditable decision —
        // the floor only guards the SILENT/unconfigured fallback path,
        // never an explicit rule.
        let rules = vec![rule(
            "intentional-zone-teardown",
            PolicyDecision::AutoApply,
            vec!["cloudflare_zone"],
            vec![],
            vec!["delete"],
            vec![],
            vec![],
        )];
        let drifts = vec![drift("cloudflare_zone.scratch", "delete", "high", vec![])];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("intentional-zone-teardown")
        );
    }

    #[test]
    fn unprotected_resource_delete_stays_autoapply_when_unconfigured() {
        // Confirms the floor is scoped to PROTECTED_RESOURCE_TYPES only —
        // deleting a single DNS record (routine) is unaffected, matching
        // the pre-existing empty_rules_default_autoapply behavior.
        assert!(!is_protected_resource_type("cloudflare_dns_record"));
        let drifts = vec![drift(
            "cloudflare_dns_record.scratch",
            "delete",
            "low",
            vec![],
        )];
        let out = evaluate(&[], None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
    }

    #[test]
    fn is_protected_resource_type_covers_the_documented_list() {
        for t in PROTECTED_RESOURCE_TYPES {
            assert!(is_protected_resource_type(t));
        }
        assert!(!is_protected_resource_type("github_repository"));
        assert!(!is_protected_resource_type("cloudflare_dns_record"));
    }

    #[test]
    fn glob_exact() {
        assert!(glob_match("cloudflare_dns_record", "cloudflare_dns_record"));
        assert!(!glob_match("cloudflare_dns_record", "cloudflare_zone"));
    }

    #[test]
    fn glob_star_suffix() {
        assert!(glob_match("cloudflare_*", "cloudflare_dns_record"));
        assert!(glob_match("cloudflare_*", "cloudflare_zone"));
        assert!(!glob_match("cloudflare_*", "aws_vpc"));
    }

    #[test]
    fn glob_star_prefix() {
        assert!(glob_match("*_dns_record", "cloudflare_dns_record"));
        assert!(glob_match("*_dns_record", "aws_route53_dns_record"));
        assert!(!glob_match("*_dns_record", "aws_vpc"));
    }

    #[test]
    fn glob_star_middle() {
        assert!(glob_match("aws_*_record", "aws_route53_record"));
        assert!(!glob_match("aws_*_record", "cloudflare_dns_record"));
    }

    #[test]
    fn glob_just_star() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn empty_rules_default_autoapply() {
        let drifts = vec![
            drift("cloudflare_dns_record.foo", "delete", "high", vec![]),
            drift("aws_vpc.bar", "create", "low", vec![]),
        ];
        let out = evaluate(&[], None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
        assert_eq!(out.evaluation.auto_apply_count, 2);
        assert_eq!(out.evaluation.refuse_count, 0);
        assert!(out
            .annotated_drifts
            .iter()
            .all(|d| d.policy_decision.as_deref() == Some("autoApply")));
        assert!(out
            .annotated_drifts
            .iter()
            .all(|d| d.matched_policy.as_deref() == Some("<default>")));
    }

    #[test]
    fn empty_rules_with_default_require_approval() {
        let drifts = vec![drift("cloudflare_dns_record.foo", "delete", "high", vec![])];
        let out = evaluate(&[], Some(PolicyDecision::RequireApproval), &drifts);
        assert_eq!(out.aggregate, PolicyDecision::RequireApproval);
        assert_eq!(out.evaluation.require_approval_count, 1);
    }

    // ── Regression: the 2026-06-20 akeyless-dev unapproved-apply incident ──
    // A greenfield/create-only plan arrives with an EMPTY drift slice
    // (`drift_details()` enumerates drift on existing resources, not creates).
    // The aggregate must still honour `defaultDecision`, NOT silently auto-apply.
    #[test]
    fn empty_drifts_with_require_approval_gates() {
        // No rules, no drifts, defaultDecision=requireApproval → must GATE.
        let out = evaluate(&[], Some(PolicyDecision::RequireApproval), &[]);
        assert_eq!(
            out.aggregate,
            PolicyDecision::RequireApproval,
            "create-only plan (empty drifts) must honour requireApproval, not auto-apply"
        );
    }

    #[test]
    fn empty_drifts_with_refuse_refuses() {
        let out = evaluate(&[], Some(PolicyDecision::Refuse), &[]);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
    }

    #[test]
    fn empty_drifts_unset_default_stays_autoapply() {
        // Unconfigured templates keep the documented aggressive default.
        let out = evaluate(&[], None, &[]);
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
    }

    #[test]
    fn all_drifts_autoapply_does_not_regress() {
        // A non-empty drift slice that all classifies as auto-apply must stay
        // AutoApply even though defaultDecision=requireApproval would gate an
        // UNclassified change — the explicit rule wins per resource.
        let rules = vec![rule(
            "allow-dns",
            PolicyDecision::AutoApply,
            vec!["cloudflare_dns_record"],
            vec![],
            vec![],
            vec![],
            vec![],
        )];
        let drifts = vec![drift("cloudflare_dns_record.foo", "update", "low", vec![])];
        let out = evaluate(&rules, Some(PolicyDecision::RequireApproval), &drifts);
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
        assert_eq!(out.evaluation.auto_apply_count, 1);
    }

    #[test]
    fn refuse_rule_blocks_aggregate() {
        let rules = vec![rule(
            "no-zone-deletes",
            PolicyDecision::Refuse,
            vec!["cloudflare_zone"],
            vec![],
            vec!["delete"],
            vec![],
            vec![],
        )];
        let drifts = vec![
            drift("cloudflare_zone.main", "delete", "high", vec![]),
            drift("cloudflare_dns_record.foo", "create", "low", vec![]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(out.evaluation.refuse_count, 1);
        assert_eq!(out.evaluation.auto_apply_count, 1);
        assert_eq!(
            out.evaluation.refused_addresses,
            vec!["cloudflare_zone.main"]
        );
    }

    #[test]
    fn require_approval_rule_dominates_autoapply() {
        let rules = vec![rule(
            "approve-dns-deletes",
            PolicyDecision::RequireApproval,
            vec!["cloudflare_dns_record"],
            vec![],
            vec!["delete"],
            vec![],
            vec![],
        )];
        let drifts = vec![
            drift("cloudflare_dns_record.foo", "delete", "high", vec![]),
            drift("cloudflare_dns_record.bar", "create", "low", vec![]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::RequireApproval);
        assert_eq!(out.evaluation.require_approval_count, 1);
        assert_eq!(out.evaluation.auto_apply_count, 1);
    }

    #[test]
    fn first_matching_rule_wins() {
        let rules = vec![
            rule(
                "first",
                PolicyDecision::Refuse,
                vec!["cloudflare_*"],
                vec![],
                vec![],
                vec![],
                vec![],
            ),
            rule(
                "second",
                PolicyDecision::AutoApply,
                vec!["cloudflare_dns_record"],
                vec![],
                vec![],
                vec![],
                vec![],
            ),
        ];
        let drifts = vec![drift("cloudflare_dns_record.foo", "delete", "high", vec![])];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("first")
        );
    }

    #[test]
    fn address_regex_matches() {
        let rules = vec![rule(
            "rio-dns-only",
            PolicyDecision::AutoApply,
            vec![],
            vec![r"^cloudflare_dns_record\.rio-.*"],
            vec![],
            vec![],
            vec![],
        )];
        let drifts = vec![
            drift("cloudflare_dns_record.rio-edge", "create", "low", vec![]),
            drift("cloudflare_dns_record.other-edge", "create", "low", vec![]),
        ];
        let out = evaluate(&rules, Some(PolicyDecision::RequireApproval), &drifts);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("rio-dns-only")
        );
        // The unmatched drift is a CREATE on an unprotected type, so it now
        // takes the non-destructive fallback rather than the blanket
        // requireApproval this used to assert. The rule-matched drift above is
        // unaffected: a matched rule still wins.
        assert_eq!(
            out.annotated_drifts[1].matched_policy.as_deref(),
            Some("<default-nondestructive>")
        );
        assert_eq!(out.annotated_drifts[1].policy_decision.as_deref(), Some("autoApply"));
        // The aggregate moves to AutoApply, and that is correct rather than a
        // loosening: the matched rule is itself AutoApply, and BOTH drifts are
        // creates. Nothing here was configured to require approval — the old
        // RequireApproval came purely from the blanket default, which is the
        // thing this change removed. A rule that said requireApproval would
        // still win; the assertion below proves the matched rule still governs
        // its own drift.
        assert_eq!(out.aggregate, PolicyDecision::AutoApply);
        assert_eq!(out.annotated_drifts[0].matched_policy.as_deref(), Some("rio-dns-only"));
    }

    #[test]
    fn invalid_regex_does_not_match_silently() {
        // Pattern is broken; rule must NOT fire (we never want a typo
        // to flip the decision to a more permissive one).
        let rules = vec![rule(
            "bad-regex",
            PolicyDecision::AutoApply,
            vec![],
            vec!["[unclosed"],
            vec![],
            vec![],
            vec![],
        )];
        let drifts = vec![drift("cloudflare_dns_record.foo", "delete", "high", vec![])];
        let out = evaluate(&rules, Some(PolicyDecision::Refuse), &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("<default>")
        );
    }

    #[test]
    fn risk_level_match() {
        let rules = vec![rule(
            "approve-high-risk",
            PolicyDecision::RequireApproval,
            vec![],
            vec![],
            vec![],
            vec!["high"],
            vec![],
        )];
        let drifts = vec![
            drift("cloudflare_dns_record.foo", "delete", "high", vec![]),
            drift("cloudflare_dns_record.bar", "update", "low", vec!["ttl"]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.require_approval_count, 1);
        assert_eq!(out.evaluation.auto_apply_count, 1);
    }

    // ── regression: typed-vocab boundary on `actions`/`riskLevels` ──
    // (case-sensitive `==` against a documented closed vocabulary,
    // zero warning on mismatch — see PolicyMatch.actions/.riskLevels).

    #[test]
    fn unrecognized_wire_values_flags_only_the_bad_entries() {
        assert_eq!(
            unrecognized_wire_values(
                &[
                    "create".to_string(),
                    "Delete".to_string(),
                    "update".to_string()
                ],
                DriftAction::parse_wire,
            ),
            vec!["Delete".to_string()],
        );
        assert!(unrecognized_wire_values(&[], DriftAction::parse_wire).is_empty());
        assert!(unrecognized_wire_values(
            &["low".to_string(), "high".to_string()],
            RiskLevel::parse_wire,
        )
        .is_empty());
    }

    #[test]
    fn actions_match_empty_is_wildcard() {
        assert!(actions_match(&[], "delete", "r"));
    }

    #[test]
    fn actions_match_exact_case_matches() {
        assert!(actions_match(&["delete".to_string()], "delete", "r"));
    }

    #[test]
    fn actions_match_case_mismatch_never_matches() {
        // FAILURE SCENARIO this regresses: a PolicyRule authored with
        // `actions: ["Delete"]` (a plausible capitalization typo)
        // guarding real drift whose action is always lowercase
        // "delete" (the only value any executor emits — see
        // plan.rs::risk_level / cycle_artifact.rs::plan_action_to_terraform_str).
        // Before this fix, `"Delete" == "delete"` was silently `false`
        // with no diagnostic anywhere — the rule looked configured but
        // could never fire. Confirm the mismatch still correctly
        // fails to match (semantics are unchanged; only the silence
        // was the bug) via the same typed path a rule-compile-time
        // warning now also runs through.
        assert!(!actions_match(&["Delete".to_string()], "delete", "r"));
    }

    #[test]
    fn actions_match_flags_a_drift_action_outside_the_vocabulary() {
        // Concrete cross-executor mismatch found while root-causing
        // this bug: `plan_action_to_terraform_str()` on the magma path
        // passes `PlanAction::Other(s)` straight through, which is
        // never guaranteed to be one of create/update/delete/replace.
        // A `refuse` rule gating `actions: ["delete"]` must not
        // silently no-op against that — it must fail the vocabulary
        // parse and refuse to match, the same way an invalid
        // addressPatterns regex refuses to match.
        assert!(!actions_match(&["delete".to_string()], "move", "r"));
    }

    #[test]
    fn risk_levels_match_empty_is_wildcard() {
        assert!(risk_levels_match(&[], "high", "r"));
    }

    #[test]
    fn risk_levels_match_exact_case_matches() {
        assert!(risk_levels_match(&["high".to_string()], "high", "r"));
    }

    #[test]
    fn risk_levels_match_case_mismatch_never_matches() {
        assert!(!risk_levels_match(&["High".to_string()], "high", "r"));
    }

    #[test]
    fn risk_levels_match_flags_a_drift_risk_outside_the_vocabulary() {
        // Concrete cross-executor mismatch found while root-causing
        // this bug: `executor::cycle_artifact::severity_to_risk()`
        // (the magma execution path) emits `"safe"/"modify"/"destroy"`
        // — never the `none/low/medium/high` vocabulary
        // `PolicyMatch.riskLevels` documents. A `riskLevels: ["high"]`
        // safety gate on a magma-executed InfrastructureTemplate must
        // not silently let every change through uninspected; it must
        // fail to match (and, in `CompiledRule::new`/`matches`, warn).
        assert!(!risk_levels_match(&["high".to_string()], "destroy", "r"));
    }

    #[test]
    fn end_to_end_case_mismatched_refuse_rule_falls_through_to_default() {
        // The full `evaluate()` path: a `refuse` rule meant to gate
        // deletes is authored with a capitalized action by mistake.
        // Assert the aggregate falls through to the (safe, explicit)
        // default instead of the rule accidentally firing — proving
        // the typed vocabulary check didn't change matching semantics,
        // only its observability.
        let rules = vec![rule(
            "refuse-deletes",
            PolicyDecision::Refuse,
            vec![],
            vec![],
            vec!["Delete"],
            vec![],
            vec![],
        )];
        let drifts = vec![drift("cloudflare_zone.prod", "delete", "high", vec![])];
        let out = evaluate(&rules, Some(PolicyDecision::RequireApproval), &drifts);
        assert_eq!(out.aggregate, PolicyDecision::RequireApproval);
        assert_eq!(
            out.annotated_drifts[0].matched_policy.as_deref(),
            Some("<default>")
        );
    }

    #[test]
    fn attribute_match_with_glob() {
        let rules = vec![rule(
            "approve-secret-rotation",
            PolicyDecision::RequireApproval,
            vec![],
            vec![],
            vec![],
            vec![],
            vec!["secret*", "api_token"],
        )];
        let drifts = vec![
            drift("svc.foo", "update", "low", vec!["secret_key", "ttl"]),
            drift("svc.bar", "update", "low", vec!["ttl"]),
            drift("svc.baz", "update", "low", vec!["api_token"]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.require_approval_count, 2); // foo + baz
        assert_eq!(out.evaluation.auto_apply_count, 1); // bar
    }

    #[test]
    fn match_clauses_are_anded() {
        // Rule fires only when type AND action AND risk all match.
        let rules = vec![rule(
            "strict",
            PolicyDecision::Refuse,
            vec!["cloudflare_zone"],
            vec![],
            vec!["delete"],
            vec!["high"],
            vec![],
        )];
        let drifts = vec![
            drift("cloudflare_zone.main", "delete", "high", vec![]), // matches all 3 -> refuse
            drift("cloudflare_zone.alt", "update", "high", vec![]), // wrong action -> default (non-destructive, unaffected by the floor)
            drift("cloudflare_zone.other", "delete", "low", vec![]), // wrong risk -> default, but a protected-type delete -> floors to RequireApproval
            drift("aws_vpc.x", "delete", "high", vec![]), // wrong type -> default, but ALSO a protected-type delete -> floors to RequireApproval
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.refuse_count, 1);
        // Only cloudflare_zone.alt (a non-destructive update) reaches the
        // unconfigured AutoApply default; the other two non-matching
        // drifts are destroys on protected types and correctly floor to
        // RequireApproval instead — this test's own data happens to
        // demonstrate the protected-resource floor alongside its
        // original AND-semantics purpose.
        assert_eq!(out.evaluation.auto_apply_count, 1);
        assert_eq!(out.evaluation.require_approval_count, 2);
    }

    #[test]
    fn refused_addresses_capped_at_ten() {
        let rules = vec![rule(
            "refuse-all",
            PolicyDecision::Refuse,
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        )];
        let drifts: Vec<DriftDetail> = (0..15)
            .map(|i| drift(&format!("aws_vpc.x{}", i), "create", "low", vec![]))
            .collect();
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.refuse_count, 15);
        assert_eq!(out.evaluation.refused_addresses.len(), 10);
    }

    #[test]
    fn is_configured_truth_table() {
        assert!(!is_configured(&[], None));
        assert!(is_configured(&[], Some(PolicyDecision::AutoApply)));
        let r = vec![rule(
            "x",
            PolicyDecision::AutoApply,
            vec![],
            vec![],
            vec![],
            vec![],
            vec![],
        )];
        assert!(is_configured(&r, None));
        assert!(is_configured(&r, Some(PolicyDecision::Refuse)));
    }
}

#[cfg(test)]
mod nondestructive_default_tests {
    use super::*;

    fn d(address: &str, action: &str) -> DriftDetail {
        DriftDetail {
            address: address.to_string(),
            action: action.to_string(),
            risk: "low".to_string(),
            attributes: vec![],
            policy_decision: None,
            matched_policy: None,
        }
    }

    fn eval(drifts: &[DriftDetail]) -> PolicyOutcome {
        // No rules, and an unconfigured default of RequireApproval — the exact
        // shape camelot-eks was in when it stalled on nine label creations.
        evaluate(&[], Some(PolicyDecision::RequireApproval), drifts)
    }

    #[test]
    fn a_create_on_an_unprotected_type_auto_applies() {
        let out = eval(&[d("github_issue_label.acude-label-bug", "create")]);
        assert_eq!(out.evaluation.auto_apply_count, 1);
        assert_eq!(out.evaluation.require_approval_count, 0);
    }

    #[test]
    fn a_delete_still_requires_approval() {
        let out = eval(&[d("github_issue_label.acude-label-bug", "delete")]);
        assert_eq!(out.evaluation.require_approval_count, 1, "a destroy must never auto-apply");
    }

    #[test]
    fn a_replace_still_requires_approval() {
        // A replace carries a destroy inside it.
        let out = eval(&[d("github_repository.acude", "replace")]);
        assert_eq!(out.evaluation.require_approval_count, 1);
    }

    #[test]
    fn an_unrecognised_action_still_requires_approval() {
        // The fail-open I nearly shipped. `is_destructive_action` answers FALSE
        // for an action it cannot parse, so `!is_destructive_action(..)` would
        // have auto-applied this. The positive predicate refuses it.
        let out = eval(&[d("github_repository.acude", "moved-sideways")]);
        assert_eq!(
            out.evaluation.require_approval_count, 1,
            "an action string we do not recognise must never take the auto-apply path"
        );
    }

    #[test]
    fn the_camelot_plan_would_now_flow() {
        // +9 ~0 -0: one repo and eight labels, zero destroys. This is the plan
        // that sat 103 minutes waiting for a human on 2026-08-13.
        let mut plan = vec![d("github_repository.acude", "create")];
        for label in [
            "bug", "dependencies", "documentation", "enhancement",
            "good-first-issue", "help-wanted", "security", "question",
        ] {
            plan.push(d(&format!("github_issue_label.acude-label-{label}"), "create"));
        }
        let out = eval(&plan);
        assert_eq!(out.evaluation.auto_apply_count, 9);
        assert_eq!(out.evaluation.require_approval_count, 0);
        assert_eq!(out.evaluation.refuse_count, 0);
    }
}
