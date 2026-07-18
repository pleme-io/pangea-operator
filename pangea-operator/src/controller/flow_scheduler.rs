//! Smart scheduler for InfrastructureFlow step execution.
//!
//! Determines which steps are ready to execute, prioritizes critical path
//! steps, respects parallelism limits, and cascades failures to dependents.
//!
//! # Substrate delegation
//!
//! The step-execution POLICY (ready/warmable step detection, parallelism-capped
//! batching, critical-path prioritization, failure cascade) is genuinely
//! `InfrastructureFlow`-specific and stays here. The underlying dependency
//! TOPOLOGY — step depth (distance from root) and the critical-path walk — is
//! now [`shigoto_dag::Dag`], not the hand-rolled recursive DFS-with-memo this
//! module used to carry (previously modeled on kenshi's scheduler.rs/dag.rs).
//!
//! That hand-rolled `compute_depths` had no cycle protection: a cyclic
//! `depends_on` graph (which the CRD's own validation should reject, but this
//! scheduler had no independent defense against) would recurse forever and
//! stack-overflow the process. `Dag::waves` returns a typed `DagError::Cycle`
//! instead — a cycle now degrades to "no computed depths" (every step reads
//! back as depth 0 / non-critical, matching the `unwrap_or(0)` this module
//! already used for absent depths) rather than crashing.
//!
//! `Dag` v0.1 exposes `predecessors()` (upstream neighbors) but no
//! successors/outgoing-neighbors accessor, so [`cascade_failure`]'s forward
//! walk (a failed step → its transitive dependents) still keeps its own small
//! `dependents: HashMap<String, Vec<String>>` reverse-index, built directly
//! from `step.depends_on` — that is the one piece of local adjacency this
//! module still owns, and it is a plain index, not a topological algorithm.

use crate::crd::{FlowStep, FlowStepStatus};
use shigoto_dag::Dag;
use shigoto_types::{JobId, JobKindId, JobScope, JobSubject};
use std::collections::{BTreeMap, HashMap, HashSet};

/// Status of a step from the scheduler's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepState {
    Pending,
    WarmingUp,
    Running,
    Ready,
    Failed,
    Cancelled,
}

impl StepState {
    pub fn from_phase(phase: Option<&str>) -> Self {
        match phase {
            None => StepState::Pending,
            Some("Ready") => StepState::Ready,
            Some("Failed") => StepState::Failed,
            Some("Cancelled") => StepState::Cancelled,
            Some("WarmingUp") => StepState::WarmingUp,
            Some(_) => StepState::Running, // Compiling, Initializing, Planning, Applying, etc.
        }
    }
}

/// A step scheduled for execution with priority metadata.
#[derive(Debug, Clone)]
pub struct ScheduledStep {
    pub name: String,
    /// Lower priority number = execute first. Critical path gets 0.
    pub priority: u32,
    /// Depth in the DAG (distance from root).
    pub depth: usize,
    /// Whether this step is on the critical path.
    pub critical: bool,
}

/// The fixed work-kind every flow-step node uses in the DAG. `Dag` is generic
/// over `JobId { scope, kind, subject }`; this module only ever needs one
/// flat namespace (global scope, one kind), so `subject` alone carries the
/// step's identity.
fn step_kind() -> JobKindId {
    JobKindId::new("infrastructure-flow-step")
}

fn step_id(name: &str) -> JobId {
    JobId { scope: JobScope::Global, kind: step_kind(), subject: JobSubject::Pinned(name.to_string()) }
}

fn step_name(id: &JobId) -> Option<String> {
    match &id.subject {
        JobSubject::Pinned(s) => Some(s.clone()),
        _ => None,
    }
}

/// Flow scheduler that determines execution order and parallelism.
pub struct FlowScheduler<'a> {
    steps: &'a [FlowStep],
    statuses: &'a BTreeMap<String, FlowStepStatus>,
    parallelism: u32,
    /// Adjacency: step -> dependents. See the module doc — this is the one
    /// traversal direction `shigoto_dag::Dag` doesn't expose, kept as a plain
    /// reverse-index (not a topological algorithm).
    dependents: HashMap<String, Vec<String>>,
    /// Step depths (distance from root), via `Dag::waves`.
    depths: HashMap<String, usize>,
    /// Critical path (longest chain), via `Dag::predecessors`.
    critical_set: HashSet<String>,
}

impl<'a> FlowScheduler<'a> {
    pub fn new(
        steps: &'a [FlowStep],
        statuses: &'a BTreeMap<String, FlowStepStatus>,
        parallelism: u32,
    ) -> Self {
        let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
        let mut dag = Dag::new();
        for step in steps {
            dag.ensure_node(step_id(&step.name));
            for dep in &step.depends_on {
                dependents.entry(dep.clone()).or_default().push(step.name.clone());
                // Edge direction: upstream (dep) → downstream (step) — matches
                // `Dag::predecessors(step)` returning step's direct upstreams.
                dag.add_edge(step_id(dep), step_id(&step.name));
            }
        }

        let depths = Self::compute_depths(&dag);
        let critical_set = Self::compute_critical_path(&dag, &depths);

        Self { steps, statuses, parallelism, dependents, depths, critical_set }
    }

    /// Get the current state of a step.
    fn step_state(&self, name: &str) -> StepState {
        self.statuses
            .get(name)
            .and_then(|s| s.phase.as_deref())
            .map(|p| StepState::from_phase(Some(p)))
            .unwrap_or(StepState::Pending)
    }

    /// Find steps whose dependencies are all Ready and that haven't started.
    pub fn find_ready_steps(&self) -> Vec<&FlowStep> {
        self.steps
            .iter()
            .filter(|step| {
                let state = self.step_state(&step.name);
                if state != StepState::Pending {
                    return false;
                }
                // All dependencies must be Ready
                step.depends_on.iter().all(|dep| self.step_state(dep) == StepState::Ready)
            })
            .collect()
    }

    /// Find steps that can be warmed up (dependencies are in-progress, not yet Ready).
    /// These steps can have their workspaces pre-initialized.
    pub fn find_warmable_steps(&self) -> Vec<&FlowStep> {
        self.steps
            .iter()
            .filter(|step| {
                let state = self.step_state(&step.name);
                if state != StepState::Pending {
                    return false;
                }
                // At least one dependency is Running (in-progress)
                let any_running = step
                    .depends_on
                    .iter()
                    .any(|dep| self.step_state(dep) == StepState::Running);
                // No dependencies have Failed or been Cancelled
                let none_failed = step
                    .depends_on
                    .iter()
                    .all(|dep| {
                        let s = self.step_state(dep);
                        s != StepState::Failed && s != StepState::Cancelled
                    });
                any_running && none_failed
            })
            .collect()
    }

    /// Get the next batch of steps to execute, respecting parallelism.
    /// Critical path steps are prioritized.
    pub fn next_batch(&self) -> Vec<ScheduledStep> {
        let running_count = self
            .steps
            .iter()
            .filter(|s| {
                let state = self.step_state(&s.name);
                state == StepState::Running || state == StepState::WarmingUp
            })
            .count() as u32;

        if running_count >= self.parallelism {
            return vec![];
        }

        let available_slots = (self.parallelism - running_count) as usize;
        let ready = self.find_ready_steps();

        let mut scheduled: Vec<ScheduledStep> = ready
            .into_iter()
            .map(|step| {
                let depth = self.depths.get(&step.name).copied().unwrap_or(0);
                let critical = self.critical_set.contains(&step.name);
                let priority = if critical { 0 } else { (depth as u32) + 1 };

                ScheduledStep {
                    name: step.name.clone(),
                    priority,
                    depth,
                    critical,
                }
            })
            .collect();

        // Sort by priority (critical path first, then by depth)
        scheduled.sort_by_key(|s| (s.priority, s.depth));
        scheduled.truncate(available_slots);
        scheduled
    }

    /// Get all transitive dependents of a step (for failure cascade).
    pub fn cascade_failure(&self, failed_step: &str) -> Vec<String> {
        let mut cancelled = Vec::new();
        let mut queue = vec![failed_step.to_string()];
        let mut visited = HashSet::new();

        while let Some(current) = queue.pop() {
            if !visited.insert(current.clone()) {
                continue;
            }
            if let Some(deps) = self.dependents.get(&current) {
                for dep in deps {
                    let state = self.step_state(dep);
                    if state == StepState::Pending || state == StepState::WarmingUp {
                        cancelled.push(dep.clone());
                        queue.push(dep.clone());
                    }
                }
            }
        }

        cancelled
    }

    /// Compute depth (distance from root) for every step via
    /// [`shigoto_dag::Dag::waves`] — wave 0 = roots (no deps), wave N = every
    /// step whose longest upstream chain has length N. Replaces the
    /// hand-rolled recursive DFS-with-memo (see module doc: that version had
    /// no cycle protection). On a cycle, `waves` returns a typed
    /// `DagError::Cycle`; this degrades to an empty depth map — every step
    /// then reads back as depth 0 / non-critical via the existing
    /// `unwrap_or(0)` call sites, never a crash. The `InfrastructureFlow`
    /// CRD's own validation is the authoritative cycle gate; this is a
    /// defensive fallback, not a promotion of cycles to a supported state.
    fn compute_depths(dag: &Dag) -> HashMap<String, usize> {
        match dag.waves(None) {
            Ok(waves) => {
                let mut depths = HashMap::new();
                for (depth, wave) in waves.into_iter().enumerate() {
                    for id in wave {
                        if let Some(name) = step_name(&id) {
                            depths.insert(name, depth);
                        }
                    }
                }
                depths
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "InfrastructureFlow step graph has a dependency cycle; \
                     critical-path prioritization degrades to depth 0 for every step"
                );
                HashMap::new()
            }
        }
    }

    /// Compute the critical path (longest dependency chain) by walking back
    /// from the deepest step to a root, at each hop taking the direct
    /// predecessor ([`shigoto_dag::Dag::predecessors`]) with the greatest
    /// depth. Ties resolve to whichever predecessor `Dag` returns last —
    /// callers only need "on A longest chain", not a specific one among
    /// equal-length ties (this module's own tests only assert membership +
    /// path length, never a specific tie winner).
    fn compute_critical_path(dag: &Dag, depths: &HashMap<String, usize>) -> HashSet<String> {
        let max_depth = depths.values().copied().max().unwrap_or(0);
        let mut path = HashSet::new();

        // Find the deepest node (end of critical path)
        let deepest = depths.iter().filter(|(_, &d)| d == max_depth).map(|(name, _)| name.clone()).next();

        if let Some(mut current) = deepest {
            path.insert(current.clone());
            loop {
                let preds = dag.predecessors(&step_id(&current));
                let deepest_dep = preds
                    .iter()
                    .filter_map(step_name)
                    .max_by_key(|name| depths.get(name).copied().unwrap_or(0));
                match deepest_dep {
                    Some(dep) => {
                        path.insert(dep.clone());
                        current = dep;
                    }
                    None => break,
                }
            }
        }

        path
    }

    /// Summary stats for the flow.
    pub fn stats(&self) -> SchedulerStats {
        let mut pending = 0;
        let mut running = 0;
        let mut ready = 0;
        let mut failed = 0;
        let mut cancelled = 0;

        for step in self.steps {
            match self.step_state(&step.name) {
                StepState::Pending => pending += 1,
                StepState::WarmingUp | StepState::Running => running += 1,
                StepState::Ready => ready += 1,
                StepState::Failed => failed += 1,
                StepState::Cancelled => cancelled += 1,
            }
        }

        SchedulerStats {
            total: self.steps.len() as u32,
            pending,
            running,
            ready,
            failed,
            cancelled,
            critical_path_length: self.critical_set.len() as u32,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SchedulerStats {
    pub total: u32,
    pub pending: u32,
    pub running: u32,
    pub ready: u32,
    pub failed: u32,
    pub cancelled: u32,
    pub critical_path_length: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crd::{FlowStep, FlowStepStatus, FlowTemplateRef};

    fn step(name: &str, deps: &[&str]) -> FlowStep {
        FlowStep {
            name: name.into(),
            template_ref: FlowTemplateRef { name: None, source: None },
            depends_on: deps.iter().map(|s| s.to_string()).collect(),
            variables: None,
            auto_approve: None,
            destroy_protection: None,
            refresh_interval: None,
            retry: None,
        }
    }

    fn status(phase: &str) -> FlowStepStatus {
        FlowStepStatus {
            phase: Some(phase.into()),
            outputs: None,
            template_name: None,
            dependencies_ready: true,
            last_error: None,
            state: None,
            warmed_up: false,
        }
    }

    #[test]
    fn test_find_ready_steps_all_roots() {
        let steps = vec![step("a", &[]), step("b", &[]), step("c", &[])];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 3);
        assert_eq!(sched.find_ready_steps().len(), 3);
    }

    #[test]
    fn test_find_ready_steps_blocked() {
        let steps = vec![step("a", &[]), step("b", &["a"])];
        let statuses = BTreeMap::new(); // a is Pending, so b is blocked
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let ready = sched.find_ready_steps();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "a");
    }

    #[test]
    fn test_find_ready_steps_after_dep_ready() {
        let steps = vec![step("a", &[]), step("b", &["a"])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Ready"));
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let ready = sched.find_ready_steps();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "b");
    }

    #[test]
    fn test_next_batch_parallelism_limit() {
        let steps = vec![step("a", &[]), step("b", &[]), step("c", &[])];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let batch = sched.next_batch();
        assert_eq!(batch.len(), 2); // Limited by parallelism
    }

    #[test]
    fn test_critical_path_linear() {
        let steps = vec![step("a", &[]), step("b", &["a"]), step("c", &["b"])];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 3);
        assert!(sched.critical_set.contains("a"));
        assert!(sched.critical_set.contains("b"));
        assert!(sched.critical_set.contains("c"));
    }

    #[test]
    fn test_critical_path_diamond() {
        // a → b, a → c, b → d, c → d (diamond)
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
            step("d", &["b", "c"]),
        ];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 4);
        // Critical path is a → b → d or a → c → d (both length 2)
        assert!(sched.critical_set.contains("a"));
        assert!(sched.critical_set.contains("d"));
        assert_eq!(sched.critical_set.len(), 3); // a + one of {b,c} + d
    }

    #[test]
    fn test_cascade_failure() {
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["b"]),
            step("d", &["a"]), // independent of b/c
        ];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Failed"));
        let sched = FlowScheduler::new(&steps, &statuses, 4);
        let cancelled = sched.cascade_failure("a");
        assert!(cancelled.contains(&"b".to_string()));
        assert!(cancelled.contains(&"c".to_string()));
        assert!(cancelled.contains(&"d".to_string()));
    }

    #[test]
    fn test_cascade_partial() {
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &[]),  // independent root
            step("d", &["c"]), // depends only on c
        ];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Failed"));
        statuses.insert("c".into(), status("Ready"));
        let sched = FlowScheduler::new(&steps, &statuses, 4);
        let cancelled = sched.cascade_failure("a");
        assert!(cancelled.contains(&"b".to_string()));
        assert!(!cancelled.contains(&"c".to_string())); // c is independent
        assert!(!cancelled.contains(&"d".to_string())); // d only depends on c
    }

    #[test]
    fn test_find_warmable_steps() {
        let steps = vec![step("a", &[]), step("b", &["a"])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Planning")); // Running
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let warmable = sched.find_warmable_steps();
        assert_eq!(warmable.len(), 1);
        assert_eq!(warmable[0].name, "b");
    }

    #[test]
    fn test_stats() {
        let steps = vec![step("a", &[]), step("b", &["a"]), step("c", &["a"])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Ready"));
        statuses.insert("b".into(), status("Planning"));
        let sched = FlowScheduler::new(&steps, &statuses, 3);
        let stats = sched.stats();
        assert_eq!(stats.total, 3);
        assert_eq!(stats.ready, 1);
        assert_eq!(stats.running, 1);
        assert_eq!(stats.pending, 1);
    }

    #[test]
    fn test_critical_path_priority() {
        // a → b → d (critical, depth 2)
        // a → c (short, depth 1)
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
            step("d", &["b"]),
        ];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Ready"));
        let sched = FlowScheduler::new(&steps, &statuses, 1);
        let batch = sched.next_batch();
        assert_eq!(batch.len(), 1);
        // b should be prioritized (on critical path a→b→d)
        assert_eq!(batch[0].name, "b");
        assert!(batch[0].critical);
    }

    #[test]
    fn test_step_state_from_phase_none() {
        assert_eq!(StepState::from_phase(None), StepState::Pending);
    }

    #[test]
    fn test_step_state_from_phase_ready() {
        assert_eq!(StepState::from_phase(Some("Ready")), StepState::Ready);
    }

    #[test]
    fn test_step_state_from_phase_failed() {
        assert_eq!(StepState::from_phase(Some("Failed")), StepState::Failed);
    }

    #[test]
    fn test_step_state_from_phase_cancelled() {
        assert_eq!(StepState::from_phase(Some("Cancelled")), StepState::Cancelled);
    }

    #[test]
    fn test_step_state_from_phase_warming_up() {
        assert_eq!(StepState::from_phase(Some("WarmingUp")), StepState::WarmingUp);
    }

    #[test]
    fn test_step_state_from_phase_running_phases() {
        for phase in ["Compiling", "Initializing", "Planning", "Applying", "Destroying"] {
            assert_eq!(
                StepState::from_phase(Some(phase)),
                StepState::Running,
                "Phase '{}' should map to Running",
                phase
            );
        }
    }

    #[test]
    fn test_step_state_from_phase_unknown_maps_to_running() {
        assert_eq!(StepState::from_phase(Some("SomeUnknown")), StepState::Running);
    }

    #[test]
    fn test_next_batch_empty_when_parallelism_full() {
        let steps = vec![step("a", &[]), step("b", &[])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Planning"));
        statuses.insert("b".into(), status("Planning"));
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let batch = sched.next_batch();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_next_batch_counts_warming_as_running() {
        let steps = vec![step("a", &[]), step("b", &[])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("WarmingUp"));
        let sched = FlowScheduler::new(&steps, &statuses, 1);
        let batch = sched.next_batch();
        assert!(batch.is_empty());
    }

    #[test]
    fn test_warmable_not_found_when_dep_failed() {
        let steps = vec![step("a", &[]), step("b", &["a"])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Failed"));
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let warmable = sched.find_warmable_steps();
        assert!(warmable.is_empty());
    }

    #[test]
    fn test_cascade_failure_no_dependents() {
        let steps = vec![step("a", &[]), step("b", &[])];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let cancelled = sched.cascade_failure("a");
        assert!(!cancelled.contains(&"b".to_string()));
    }

    #[test]
    fn test_cascade_failure_skips_running_steps() {
        let steps = vec![
            step("a", &[]),
            step("b", &["a"]),
            step("c", &["a"]),
        ];
        let mut statuses = BTreeMap::new();
        statuses.insert("b".into(), status("Planning")); // Running
        let sched = FlowScheduler::new(&steps, &statuses, 3);
        let cancelled = sched.cascade_failure("a");
        assert!(!cancelled.contains(&"b".to_string())); // Running, not cascaded
        assert!(cancelled.contains(&"c".to_string()));  // Pending, cascaded
    }

    #[test]
    fn test_stats_all_cancelled() {
        let steps = vec![step("a", &[]), step("b", &[])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("Cancelled"));
        statuses.insert("b".into(), status("Cancelled"));
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        let stats = sched.stats();
        assert_eq!(stats.cancelled, 2);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.running, 0);
        assert_eq!(stats.ready, 0);
        assert_eq!(stats.failed, 0);
    }

    #[test]
    fn test_stats_warming_counted_as_running() {
        let steps = vec![step("a", &[])];
        let mut statuses = BTreeMap::new();
        statuses.insert("a".into(), status("WarmingUp"));
        let sched = FlowScheduler::new(&steps, &statuses, 1);
        let stats = sched.stats();
        assert_eq!(stats.running, 1);
    }

    #[test]
    fn test_empty_flow() {
        let steps: Vec<FlowStep> = vec![];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 3);
        assert!(sched.find_ready_steps().is_empty());
        assert!(sched.find_warmable_steps().is_empty());
        assert!(sched.next_batch().is_empty());
        let stats = sched.stats();
        assert_eq!(stats.total, 0);
    }

    #[test]
    fn test_single_step_flow() {
        let steps = vec![step("only", &[])];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 1);
        let ready = sched.find_ready_steps();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].name, "only");

        let batch = sched.next_batch();
        assert_eq!(batch.len(), 1);
        assert!(batch[0].critical);
    }

    /// Regression: a cyclic `depends_on` graph must degrade to "no computed
    /// depths / no critical path" instead of the stack overflow the original
    /// hand-rolled recursive `compute_depths` was exposed to (no memoized
    /// recursion ever detected re-entry into an in-progress node). Also
    /// proves the scheduler stays USABLE on a cycle: ready-step detection
    /// reads `depends_on` + status directly (never the DAG), so it is
    /// unaffected — only critical-path prioritization degrades.
    #[test]
    fn test_cyclic_depends_on_does_not_crash_and_depths_degrade_to_zero() {
        let steps = vec![step("a", &["b"]), step("b", &["a"])];
        let statuses = BTreeMap::new();
        let sched = FlowScheduler::new(&steps, &statuses, 2);
        // No stack overflow reaching this line is the primary assertion.
        assert!(sched.depths.is_empty(), "a cycle yields no computed depths");
        assert!(sched.critical_set.is_empty(), "no critical path over an unresolved cycle");
        // Both steps are blocked on each other (neither's dependency is ever
        // Ready), so nothing is ready to schedule — but computing that must
        // not panic or hang.
        assert!(sched.find_ready_steps().is_empty());
        assert!(sched.next_batch().is_empty());
    }
}
