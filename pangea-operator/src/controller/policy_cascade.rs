//! Hierarchical policy cascade resolver — M3 of
//! `theory/PANGEA-WORKSPACE-RECONCILIATION.md`.
//!
//! Walks the four-level cascade
//!   ArchitectureGem → WorkspaceCatalog → InfrastructureTemplate → Resource
//! and merges typed policy fields with **safety precedence**:
//! `refuse > requireApproval > autoApply > alert`.
//!
//! Pure Rust; no I/O. Easy to unit-test exhaustively. Used by:
//!   - the InfrastructureTemplate reconciler (apply-time policy)
//!   - the admission webhook (admit-time validation)
//!   - `kubectl pangea policy explain <resource>` (operator UX)

use crate::crd::architecture_gem::{
    ApprovalRouting, DriftReaction, GemPolicy, SettlingExhaustionAction, SettlingPolicy,
};
use crate::crd::workspace_catalog::WorkspacePolicy;

/// Resolved policy at a Resource — every field is `Some(...)` or has
/// a documented default. Whoever computed this is responsible for
/// recording the cascade source for `policy explain`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedPolicy {
    pub drift_reaction: DriftReaction,
    pub destroy_protection: bool,
    pub reconcile_interval: String,
    pub settling_policy: SettlingPolicy,
    pub approval_routing: Option<ApprovalRouting>,
}

/// Cascade source of each resolved field. Returned alongside the
/// resolved policy by `resolve_with_provenance` so `policy explain`
/// can show "drift_reaction came from the workspace; destroy_protection
/// came from the gem; …".
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PolicyProvenance {
    pub drift_reaction: CascadeLevel,
    pub destroy_protection: CascadeLevel,
    pub reconcile_interval: CascadeLevel,
    pub settling_policy: CascadeLevel,
    pub approval_routing: CascadeLevel,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CascadeLevel {
    #[default]
    Default,
    Gem,
    Workspace,
    Template,
    Resource,
}

/// Inputs to the resolver — one slot per cascade level. None means
/// "this level didn't override". Innermost wins, with safety precedence
/// for `drift_reaction`.
#[derive(Debug, Default)]
pub struct CascadeInputs<'a> {
    pub gem: Option<&'a GemPolicy>,
    pub workspace: Option<&'a WorkspacePolicy>,
    pub template: Option<&'a TemplatePolicy>,
    pub resource: Option<&'a ResourcePolicy>,
}

/// Template-level policy — same shape as workspace but lives on the
/// `InfrastructureTemplate` CR. Subset of fields the existing CRD
/// already supports; this struct mirrors them for the cascade
/// computation.
#[derive(Debug, Clone, Default)]
pub struct TemplatePolicy {
    pub drift_reaction: Option<DriftReaction>,
    pub destroy_protection: Option<bool>,
    pub reconcile_interval: Option<String>,
    pub settling_policy: Option<SettlingPolicy>,
    pub approval_routing: Option<ApprovalRouting>,
}

/// Resource-level policy — innermost level. Currently sourced from
/// the existing `policies` array on InfrastructureTemplate (typed
/// match selectors); future per-resource CR support may attach
/// directly.
#[derive(Debug, Clone, Default)]
pub struct ResourcePolicy {
    pub drift_reaction: Option<DriftReaction>,
    pub destroy_protection: Option<bool>,
    pub reconcile_interval: Option<String>,
    pub settling_policy: Option<SettlingPolicy>,
    pub approval_routing: Option<ApprovalRouting>,
}

/// Resolve the cascade. `drift_reaction` uses safety precedence
/// (`refuse > requireApproval > autoApply > alert`); all other fields
/// use innermost-wins.
pub fn resolve(inputs: &CascadeInputs) -> ResolvedPolicy {
    resolve_with_provenance(inputs).0
}

pub fn resolve_with_provenance(inputs: &CascadeInputs) -> (ResolvedPolicy, PolicyProvenance) {
    let mut prov = PolicyProvenance::default();

    // drift_reaction — safety precedence, regardless of cascade depth.
    let (drift_reaction, drift_level) = resolve_drift_reaction(inputs);
    prov.drift_reaction = drift_level;

    // destroy_protection — innermost wins.
    let (destroy_protection, dp_level) = innermost_bool(
        inputs.gem.and_then(|g| g.destroy_protection),
        // workspace policy doesn't expose destroy_protection in M3;
        // future expansion adds it. For now propagate gem.
        None,
        inputs.template.and_then(|t| t.destroy_protection),
        inputs.resource.and_then(|r| r.destroy_protection),
    );
    prov.destroy_protection = dp_level;

    // reconcile_interval — innermost wins.
    let (reconcile_interval, ri_level) = innermost_string(
        None, // gem doesn't expose reconcile_interval (it's metadata-level)
        inputs.workspace.and_then(|w| w.reconcile_interval.clone()),
        inputs.template.and_then(|t| t.reconcile_interval.clone()),
        inputs.resource.and_then(|r| r.reconcile_interval.clone()),
    );
    prov.reconcile_interval = ri_level;

    // settling_policy — innermost wins.
    let (settling_policy, sp_level) = innermost_clone::<SettlingPolicy>(
        inputs.gem.and_then(|g| g.settling_policy.as_ref()),
        inputs.workspace.and_then(|w| w.settling_policy.as_ref()),
        inputs.template.and_then(|t| t.settling_policy.as_ref()),
        inputs.resource.and_then(|r| r.settling_policy.as_ref()),
    );
    prov.settling_policy = sp_level;

    // approval_routing — innermost wins.
    let (approval_routing, ar_level) = innermost_clone::<ApprovalRouting>(
        inputs.gem.and_then(|g| g.approval_routing.as_ref()),
        inputs.workspace.and_then(|w| w.approval_routing.as_ref()),
        inputs.template.and_then(|t| t.approval_routing.as_ref()),
        inputs.resource.and_then(|r| r.approval_routing.as_ref()),
    );
    prov.approval_routing = ar_level;

    let resolved = ResolvedPolicy {
        drift_reaction,
        destroy_protection: destroy_protection.unwrap_or(false),
        reconcile_interval: reconcile_interval.unwrap_or_else(|| "5m".to_string()),
        settling_policy: settling_policy.unwrap_or_else(default_settling_policy),
        approval_routing,
    };

    (resolved, prov)
}

fn default_settling_policy() -> SettlingPolicy {
    SettlingPolicy {
        max_consecutive_drift_cycles: 4,
        on_exhaustion: SettlingExhaustionAction::Fail,
    }
}

/// Drift-reaction resolution with safety precedence.
///
/// 1. If ANY level (gem, workspace, template, resource) declares
///    `Refuse`, the result is `Refuse` — even if a lower level
///    declared `AutoApply`. Only `breakGlass` (handled by admission
///    webhook, NOT this resolver) can override.
/// 2. Otherwise, if any level declares `RequireApproval`, the
///    result is `RequireApproval`.
/// 3. Otherwise, innermost-wins among (`AutoApply`, `Alert`, default).
/// 4. Default if no level declared anything: `RequireApproval`
///    (safe default — humans gate first apply).
fn resolve_drift_reaction(inputs: &CascadeInputs) -> (DriftReaction, CascadeLevel) {
    let chain: [(Option<&DriftReaction>, CascadeLevel); 4] = [
        (
            inputs.gem.and_then(|g| g.drift_reaction.as_ref()),
            CascadeLevel::Gem,
        ),
        (
            inputs.workspace.and_then(|w| w.drift_reaction.as_ref()),
            CascadeLevel::Workspace,
        ),
        (
            inputs.template.and_then(|t| t.drift_reaction.as_ref()),
            CascadeLevel::Template,
        ),
        (
            inputs.resource.and_then(|r| r.drift_reaction.as_ref()),
            CascadeLevel::Resource,
        ),
    ];

    // 1. Any Refuse wins.
    for (val, level) in chain.iter() {
        if let Some(DriftReaction::Refuse) = val {
            return (DriftReaction::Refuse, *level);
        }
    }
    // 2. Any RequireApproval wins.
    for (val, level) in chain.iter() {
        if let Some(DriftReaction::RequireApproval) = val {
            return (DriftReaction::RequireApproval, *level);
        }
    }
    // 3. Innermost-wins between AutoApply / Alert.
    let mut last: Option<(DriftReaction, CascadeLevel)> = None;
    for (val, level) in chain.iter() {
        if let Some(v) = val {
            match v {
                DriftReaction::AutoApply | DriftReaction::Alert => {
                    last = Some(((*v).clone(), *level));
                }
                _ => {}
            }
        }
    }
    last.unwrap_or((DriftReaction::RequireApproval, CascadeLevel::Default))
}

fn innermost_bool(
    gem: Option<bool>,
    workspace: Option<bool>,
    template: Option<bool>,
    resource: Option<bool>,
) -> (Option<bool>, CascadeLevel) {
    if let Some(v) = resource {
        (Some(v), CascadeLevel::Resource)
    } else if let Some(v) = template {
        (Some(v), CascadeLevel::Template)
    } else if let Some(v) = workspace {
        (Some(v), CascadeLevel::Workspace)
    } else if let Some(v) = gem {
        (Some(v), CascadeLevel::Gem)
    } else {
        (None, CascadeLevel::Default)
    }
}

fn innermost_string(
    gem: Option<String>,
    workspace: Option<String>,
    template: Option<String>,
    resource: Option<String>,
) -> (Option<String>, CascadeLevel) {
    if let Some(v) = resource {
        (Some(v), CascadeLevel::Resource)
    } else if let Some(v) = template {
        (Some(v), CascadeLevel::Template)
    } else if let Some(v) = workspace {
        (Some(v), CascadeLevel::Workspace)
    } else if let Some(v) = gem {
        (Some(v), CascadeLevel::Gem)
    } else {
        (None, CascadeLevel::Default)
    }
}

fn innermost_clone<T: Clone>(
    gem: Option<&T>,
    workspace: Option<&T>,
    template: Option<&T>,
    resource: Option<&T>,
) -> (Option<T>, CascadeLevel) {
    if let Some(v) = resource {
        (Some(v.clone()), CascadeLevel::Resource)
    } else if let Some(v) = template {
        (Some(v.clone()), CascadeLevel::Template)
    } else if let Some(v) = workspace {
        (Some(v.clone()), CascadeLevel::Workspace)
    } else if let Some(v) = gem {
        (Some(v.clone()), CascadeLevel::Gem)
    } else {
        (None, CascadeLevel::Default)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ga_policy(reaction: Option<DriftReaction>) -> GemPolicy {
        GemPolicy {
            drift_reaction: reaction,
            ..Default::default()
        }
    }

    fn wp_policy(reaction: Option<DriftReaction>) -> WorkspacePolicy {
        WorkspacePolicy {
            drift_reaction: reaction,
            ..Default::default()
        }
    }

    fn tp_policy(reaction: Option<DriftReaction>) -> TemplatePolicy {
        TemplatePolicy {
            drift_reaction: reaction,
            ..Default::default()
        }
    }

    fn rp_policy(reaction: Option<DriftReaction>) -> ResourcePolicy {
        ResourcePolicy {
            drift_reaction: reaction,
            ..Default::default()
        }
    }

    #[test]
    fn empty_cascade_defaults_to_require_approval() {
        let inputs = CascadeInputs::default();
        let (resolved, prov) = resolve_with_provenance(&inputs);
        assert_eq!(resolved.drift_reaction, DriftReaction::RequireApproval);
        assert_eq!(prov.drift_reaction, CascadeLevel::Default);
    }

    #[test]
    fn refuse_at_gem_wins_over_autoapply_at_resource() {
        let gem = ga_policy(Some(DriftReaction::Refuse));
        let res = rp_policy(Some(DriftReaction::AutoApply));
        let inputs = CascadeInputs {
            gem: Some(&gem),
            resource: Some(&res),
            ..Default::default()
        };
        let (resolved, prov) = resolve_with_provenance(&inputs);
        assert_eq!(resolved.drift_reaction, DriftReaction::Refuse);
        assert_eq!(prov.drift_reaction, CascadeLevel::Gem);
    }

    #[test]
    fn refuse_at_resource_wins_too() {
        let gem = ga_policy(Some(DriftReaction::AutoApply));
        let res = rp_policy(Some(DriftReaction::Refuse));
        let inputs = CascadeInputs {
            gem: Some(&gem),
            resource: Some(&res),
            ..Default::default()
        };
        let resolved = resolve(&inputs);
        assert_eq!(resolved.drift_reaction, DriftReaction::Refuse);
    }

    #[test]
    fn require_approval_beats_autoapply_regardless_of_level() {
        let ws = wp_policy(Some(DriftReaction::RequireApproval));
        let tpl = tp_policy(Some(DriftReaction::AutoApply));
        let inputs = CascadeInputs {
            workspace: Some(&ws),
            template: Some(&tpl),
            ..Default::default()
        };
        let (resolved, prov) = resolve_with_provenance(&inputs);
        assert_eq!(resolved.drift_reaction, DriftReaction::RequireApproval);
        assert_eq!(prov.drift_reaction, CascadeLevel::Workspace);
    }

    #[test]
    fn innermost_wins_between_autoapply_and_alert() {
        let gem = ga_policy(Some(DriftReaction::AutoApply));
        let res = rp_policy(Some(DriftReaction::Alert));
        let inputs = CascadeInputs {
            gem: Some(&gem),
            resource: Some(&res),
            ..Default::default()
        };
        let (resolved, prov) = resolve_with_provenance(&inputs);
        assert_eq!(resolved.drift_reaction, DriftReaction::Alert);
        assert_eq!(prov.drift_reaction, CascadeLevel::Resource);
    }

    #[test]
    fn destroy_protection_innermost_wins() {
        let gem = GemPolicy {
            destroy_protection: Some(true),
            ..Default::default()
        };
        let tpl = TemplatePolicy {
            destroy_protection: Some(false),
            ..Default::default()
        };
        let inputs = CascadeInputs {
            gem: Some(&gem),
            template: Some(&tpl),
            ..Default::default()
        };
        let (resolved, prov) = resolve_with_provenance(&inputs);
        assert!(!resolved.destroy_protection);
        assert_eq!(prov.destroy_protection, CascadeLevel::Template);
    }

    #[test]
    fn reconcile_interval_falls_through_levels() {
        let ws = WorkspacePolicy {
            reconcile_interval: Some("10m".to_string()),
            ..Default::default()
        };
        let inputs = CascadeInputs {
            workspace: Some(&ws),
            ..Default::default()
        };
        let (resolved, prov) = resolve_with_provenance(&inputs);
        assert_eq!(resolved.reconcile_interval, "10m");
        assert_eq!(prov.reconcile_interval, CascadeLevel::Workspace);
    }

    #[test]
    fn reconcile_interval_default_5m() {
        let inputs = CascadeInputs::default();
        let resolved = resolve(&inputs);
        assert_eq!(resolved.reconcile_interval, "5m");
    }

    #[test]
    fn settling_policy_uses_default_when_none_set() {
        let inputs = CascadeInputs::default();
        let resolved = resolve(&inputs);
        assert_eq!(resolved.settling_policy.max_consecutive_drift_cycles, 4);
        assert_eq!(
            resolved.settling_policy.on_exhaustion,
            SettlingExhaustionAction::Fail
        );
    }

    #[test]
    fn cascade_level_default_is_default_variant() {
        assert_eq!(CascadeLevel::default(), CascadeLevel::Default);
    }

    #[test]
    fn break_glass_is_NOT_handled_by_resolver() {
        // The resolver always honors `Refuse`. breakGlass is webhook
        // territory — the webhook may strip a `Refuse` if a special
        // annotation is present BEFORE feeding into the resolver.
        // This test guards that contract.
        let gem = ga_policy(Some(DriftReaction::Refuse));
        let inputs = CascadeInputs {
            gem: Some(&gem),
            ..Default::default()
        };
        let resolved = resolve(&inputs);
        assert_eq!(resolved.drift_reaction, DriftReaction::Refuse);
    }
}
