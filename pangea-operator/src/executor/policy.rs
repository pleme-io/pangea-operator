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
//!   3. Else → `AutoApply` (the documented aggressive default)
//!
//! Aggregation across changes:
//!   - any `Refuse`           → Refuse  (operator marks Failed)
//!   - else any `RequireApproval` → RequireApproval (existing approval gate)
//!   - else                   → AutoApply
//!
//! Empty `policies` + unset `defaultDecision` → every change resolves
//! to `AutoApply` and the operator behaves like `autoApprove: true`.
//! `autoApprove` is no longer consulted by the policy path; it remains
//! in the spec for legacy schemas but does not gate the new engine.

use crate::crd::{DriftDetail, PolicyDecision, PolicyEvaluation, PolicyRule};
use regex::Regex;
use tracing::warn;

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
                None => (fallback, "<default>".to_string()),
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
    } else {
        PolicyDecision::AutoApply
    };

    let evaluation = PolicyEvaluation {
        aggregate: aggregate.as_str().to_string(),
        auto_apply_count,
        require_approval_count,
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
        Self { rule, address_regexes }
    }

    fn matches(&self, d: &DriftDetail) -> bool {
        let m = &self.rule.match_;

        // resourceTypes: glob list, OR semantics. Empty = wildcard.
        if !m.resource_types.is_empty() {
            // DriftDetail.address is `<type>.<name>` (or `<type>.<name>[idx]`).
            // Extract the type prefix once.
            let resource_type = d
                .address
                .split('.')
                .next()
                .unwrap_or(d.address.as_str());
            if !m.resource_types.iter().any(|g| glob_match(g, resource_type)) {
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
            if !self.address_regexes.iter().any(|re| re.is_match(&d.address)) {
                return false;
            }
        }

        // actions: exact-match list, OR. Empty = wildcard.
        if !m.actions.is_empty() && !m.actions.iter().any(|a| a == &d.action) {
            return false;
        }

        // riskLevels: exact-match list, OR. Empty = wildcard.
        if !m.risk_levels.is_empty() && !m.risk_levels.iter().any(|r| r == &d.risk) {
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

    #[test]
    fn refuse_rule_blocks_aggregate() {
        let rules = vec![
            rule("no-zone-deletes", PolicyDecision::Refuse,
                 vec!["cloudflare_zone"], vec![], vec!["delete"], vec![], vec![]),
        ];
        let drifts = vec![
            drift("cloudflare_zone.main", "delete", "high", vec![]),
            drift("cloudflare_dns_record.foo", "create", "low", vec![]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(out.evaluation.refuse_count, 1);
        assert_eq!(out.evaluation.auto_apply_count, 1);
        assert_eq!(out.evaluation.refused_addresses, vec!["cloudflare_zone.main"]);
    }

    #[test]
    fn require_approval_rule_dominates_autoapply() {
        let rules = vec![
            rule("approve-dns-deletes", PolicyDecision::RequireApproval,
                 vec!["cloudflare_dns_record"], vec![], vec!["delete"], vec![], vec![]),
        ];
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
            rule("first", PolicyDecision::Refuse,
                 vec!["cloudflare_*"], vec![], vec![], vec![], vec![]),
            rule("second", PolicyDecision::AutoApply,
                 vec!["cloudflare_dns_record"], vec![], vec![], vec![], vec![]),
        ];
        let drifts = vec![drift("cloudflare_dns_record.foo", "delete", "high", vec![])];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(out.annotated_drifts[0].matched_policy.as_deref(), Some("first"));
    }

    #[test]
    fn address_regex_matches() {
        let rules = vec![
            rule("rio-dns-only", PolicyDecision::AutoApply,
                 vec![], vec![r"^cloudflare_dns_record\.rio-.*"],
                 vec![], vec![], vec![]),
        ];
        let drifts = vec![
            drift("cloudflare_dns_record.rio-edge", "create", "low", vec![]),
            drift("cloudflare_dns_record.other-edge", "create", "low", vec![]),
        ];
        let out = evaluate(&rules, Some(PolicyDecision::RequireApproval), &drifts);
        assert_eq!(out.annotated_drifts[0].matched_policy.as_deref(), Some("rio-dns-only"));
        assert_eq!(out.annotated_drifts[1].matched_policy.as_deref(), Some("<default>"));
        assert_eq!(out.aggregate, PolicyDecision::RequireApproval);
    }

    #[test]
    fn invalid_regex_does_not_match_silently() {
        // Pattern is broken; rule must NOT fire (we never want a typo
        // to flip the decision to a more permissive one).
        let rules = vec![
            rule("bad-regex", PolicyDecision::AutoApply,
                 vec![], vec!["[unclosed"],
                 vec![], vec![], vec![]),
        ];
        let drifts = vec![drift("cloudflare_dns_record.foo", "delete", "high", vec![])];
        let out = evaluate(&rules, Some(PolicyDecision::Refuse), &drifts);
        assert_eq!(out.aggregate, PolicyDecision::Refuse);
        assert_eq!(out.annotated_drifts[0].matched_policy.as_deref(), Some("<default>"));
    }

    #[test]
    fn risk_level_match() {
        let rules = vec![
            rule("approve-high-risk", PolicyDecision::RequireApproval,
                 vec![], vec![], vec![], vec!["high"], vec![]),
        ];
        let drifts = vec![
            drift("cloudflare_dns_record.foo", "delete", "high", vec![]),
            drift("cloudflare_dns_record.bar", "update", "low", vec!["ttl"]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.require_approval_count, 1);
        assert_eq!(out.evaluation.auto_apply_count, 1);
    }

    #[test]
    fn attribute_match_with_glob() {
        let rules = vec![
            rule("approve-secret-rotation", PolicyDecision::RequireApproval,
                 vec![], vec![], vec![], vec![],
                 vec!["secret*", "api_token"]),
        ];
        let drifts = vec![
            drift("svc.foo", "update", "low", vec!["secret_key", "ttl"]),
            drift("svc.bar", "update", "low", vec!["ttl"]),
            drift("svc.baz", "update", "low", vec!["api_token"]),
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.require_approval_count, 2); // foo + baz
        assert_eq!(out.evaluation.auto_apply_count, 1);      // bar
    }

    #[test]
    fn match_clauses_are_anded() {
        // Rule fires only when type AND action AND risk all match.
        let rules = vec![
            rule("strict", PolicyDecision::Refuse,
                 vec!["cloudflare_zone"], vec![], vec!["delete"], vec!["high"], vec![]),
        ];
        let drifts = vec![
            drift("cloudflare_zone.main", "delete", "high", vec![]),  // matches all 3
            drift("cloudflare_zone.alt",  "update", "high", vec![]),  // wrong action
            drift("cloudflare_zone.other","delete", "low",  vec![]),  // wrong risk
            drift("aws_vpc.x",            "delete", "high", vec![]),  // wrong type
        ];
        let out = evaluate(&rules, None, &drifts);
        assert_eq!(out.evaluation.refuse_count, 1);
        assert_eq!(out.evaluation.auto_apply_count, 3);
    }

    #[test]
    fn refused_addresses_capped_at_ten() {
        let rules = vec![
            rule("refuse-all", PolicyDecision::Refuse,
                 vec![], vec![], vec![], vec![], vec![]),
        ];
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
        let r = vec![rule("x", PolicyDecision::AutoApply, vec![], vec![], vec![], vec![], vec![])];
        assert!(is_configured(&r, None));
        assert!(is_configured(&r, Some(PolicyDecision::Refuse)));
    }
}
