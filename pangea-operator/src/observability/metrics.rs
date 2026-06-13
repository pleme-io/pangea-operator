//! Prometheus metrics for the Pangea Operator.

use prometheus::{
    Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};

/// Prometheus metrics for the Pangea Operator.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,

    /// Total number of reconciliations.
    pub reconciliations_total: IntCounter,

    /// Total namespace reconciliations.
    pub namespace_reconciliations_total: IntCounter,

    /// Number of templates by phase.
    pub templates_by_phase: IntGaugeVec,

    /// Reconciliation duration histogram.
    pub reconciliation_duration_seconds: HistogramVec,

    /// Total drift detections.
    pub drift_detected_total: IntCounter,

    /// PostgreSQL state operations.
    pub pg_state_operations_total: IntCounterVec,

    /// Active reconciliations gauge.
    pub active_reconciliations: IntGauge,

    /// Template source compilation duration.
    pub compilation_duration_seconds: Histogram,

    /// Failed reconciliations.
    pub reconciliation_errors_total: IntCounterVec,

    // -----------------------------------------------------------------
    // Policy engine + state-settling metrics (chart 0.4.0)
    //
    // Cardinality budget: each metric is keyed by (template, namespace)
    // → grows linearly with number of InfrastructureTemplates. Safe up
    // to a few hundred templates per cluster. The decision/action/risk
    // labels are bounded enums (3-4 values each).
    // -----------------------------------------------------------------

    /// Per-resource policy decisions emitted during plan evaluation.
    /// Labels: template, namespace, decision (autoApply|requireApproval|refuse).
    /// Use to alert on unexpected `refuse` rates or to track audit-mode
    /// rollouts before flipping a rule to enforce.
    pub policy_decisions_total: IntCounterVec,

    /// Current consecutive-drift-cycle count per template. 0 = settled.
    /// Crossing `spec.settlingPolicy.maxConsecutiveDriftCycles` is the
    /// alert trigger; PrometheusRule below uses this directly.
    pub consecutive_drift_cycles: IntGaugeVec,

    /// Settled flag per template: 1 when `Settled=True`, 0 when False.
    /// The single most useful "is this template healthy" gauge.
    pub settled: IntGaugeVec,

    /// Number of resources currently in the stuck-set per template.
    /// Non-zero only when settling has escalated.
    pub stuck_resources: IntGaugeVec,

    /// Settling-failure escalations. Labels: template, namespace,
    /// reason (StuckByCount|StuckByFingerprint).
    pub settling_failures_total: IntCounterVec,

    /// Per-template count of pending changes by action+risk. Lets a
    /// dashboard show "5 high-risk deletes pending across the fleet"
    /// without parsing each driftDetail.
    pub template_drift_detail: IntGaugeVec,

    /// Reconciles skipped by `policy_gate` because of OperatorPolicy.
    /// Labels: `controller` (template/namespace/dashboard/...), `reason`
    /// (globalSuspend|controllerSuspend). Increment on every skipped
    /// reconcile so a dashboard can prove "operator paused for 4h".
    /// Mirrors `OperatorPolicy.status.reconcilesSkipped` but with
    /// per-controller labels for fairness diagnosis.
    pub policy_skipped_total: IntCounterVec,

    /// Compile failures by template, with the failure-mode reason as
    /// a label. Used to alert on stuck-Compiling templates BEFORE
    /// `consecutiveCompileFailures` crosses settlingPolicy threshold.
    /// Labels: `namespace`, `name`. Reason is exposed via
    /// `pangea_template_consecutive_compile_failures` (gauge below).
    pub compile_failures_total: IntCounterVec,

    /// Current `consecutiveCompileFailures` count per template. Lets
    /// dashboards alert on "template X is stuck compiling" before
    /// settling policy escalates to Failed (default 5 cycles).
    pub consecutive_compile_failures: IntGaugeVec,

    /// Current recommended escalation rung depth per template (0-4 per
    /// EscalationAction::depth — Retry=0, RefreshSource=1, ReloadGems=2,
    /// RecycleWorkers=3, PauseAndAlert=4). Set by `handle_compile_failure`
    /// after the ladder's pick. Powers dashboards like "how many
    /// templates are currently above depth 2", "which workspace has
    /// been at depth 3 the longest".
    ///
    /// Pairs with `escalation_actions_total` (counter) for selection
    /// histogram + `consecutive_compile_failures` for the orthogonal
    /// settling signal. See `controller::escalation` +
    /// `project_escalation_ladder.md`.
    pub escalation_recommended_depth: IntGaugeVec,

    /// Total times each escalation action was recommended, by template
    /// + action label. Lets dashboards count "how often did each rung
    /// fire" and answer "is the ladder converging or constantly
    /// escalating".
    /// Labels: namespace, name, action (Retry|RefreshSource|ReloadGems|
    /// RecycleWorkers|PauseAndAlert).
    pub escalation_actions_total: IntCounterVec,

    /// Per-workspace `Active` carve-out gauge. Set to 1 for each
    /// workspace whose effective `SuspensionDecision` is
    /// `Active { .. }` after running the precedence ladder; 0
    /// otherwise. Lets dashboards answer "what's still reconciling
    /// under a global pause?" — the canonical use case is monitoring
    /// the one or two workspaces explicitly carved out during a
    /// fleet-wide rewrite or freeze.
    /// Labels:
    ///   * kind      — "template" or "catalog"
    ///   * namespace — empty for catalogs
    ///   * name      — workspace name (template name or catalog name)
    ///   * source    — which precedence layer produced the Active
    ///                 ("workspace" = direct entry, "catalog" = cascade)
    pub workspace_active_override: IntGaugeVec,

    /// Per-controller reconcile throughput + error rate.
    /// Labels:
    ///   * controller — `ControllerKind::name()` value (template,
    ///                  namespace, workspaceCatalog, …)
    ///   * result     — "ok" | "error" | "skip"
    /// Bumped at the entry/exit of every controller's reconcile
    /// function. Fills the gap previously covered only by the
    /// template-specific `reconciliations_total` and
    /// `namespace_reconciliations_total` counters; lets dashboards
    /// pivot reconcile rate by controller-class and surface
    /// per-controller error rates without forking metrics.
    pub controller_reconciliations_total: IntCounterVec,

    // -----------------------------------------------------------------
    // Per-CRD-class phase gauges (added during U3 audit).
    //
    // The chart's PrometheusRule referenced these gauges in alert
    // expressions, but they were never defined in the operator. The
    // alerts therefore SILENTLY DID NOT FIRE — Prometheus accepts a
    // PromQL query against a non-existent metric and just returns no
    // data, which `> 0` evaluates as false. Adding the metric
    // definitions here closes the drift; controllers record on phase
    // transitions to keep the gauge current.
    //
    // Cardinality: each metric is keyed by (namespace, name, phase).
    // For ~hundreds of CRs per cluster and ~5-10 phases each, total
    // series stays under ~5k per metric — comfortable for a single
    // Prometheus shard.
    // -----------------------------------------------------------------

    /// Number of PackerBuild CRs by phase.
    /// Labels: namespace, name, phase
    /// Used by alert: PackerBuildFailed.
    pub packer_builds_by_phase: IntGaugeVec,

    /// Number of ImagePipeline CRs by phase.
    /// Labels: namespace, name, phase
    /// Used by alert: ImagePipelineStuck.
    pub image_pipelines_by_phase: IntGaugeVec,

    /// Number of AmiTest CRs by phase.
    /// Labels: namespace, name, phase
    /// Used by alert: AmiTestFailed.
    pub ami_tests_by_phase: IntGaugeVec,

    /// Number of ComplianceSchedule CRs by phase.
    /// Labels: namespace, name, phase
    /// Used by alert: ComplianceNonCompliant.
    pub compliance_schedules_by_phase: IntGaugeVec,

    /// Number of targets gated by a ComplianceBinding due to non-
    /// compliance. Labels: namespace, name (of binding).
    /// Used by alert: ComplianceBindingGating.
    pub compliance_bindings_gated_targets: IntGaugeVec,

    // -----------------------------------------------------------------
    // Compile dispatcher metrics (added during S1 — Tier 3 observability
    // for the magnus owner thread).
    //
    // Why these matter: the embedded ruby owner thread is the only
    // serialization point across templates' compile phases. To know
    // whether a future RubyPool change helps, we need to separate
    // "how long did this request wait in line" from "how long did the
    // actual compile take". The existing pangea_compilation_duration_
    // seconds conflates the two.
    //
    // The dispatcher records:
    //   - depth: current count of pending requests, sampled on every
    //     send. A growing depth = serialization is the bottleneck.
    //   - wait time: per-request, T1 (owner picks up) − T0 (caller
    //     enqueued). When the queue is short this is near-zero;
    //     under contention it grows linearly with depth × per-compile
    //     duration.
    // -----------------------------------------------------------------

    /// Instantaneous depth of the magnus dispatcher queue (mpsc channel
    /// to the ruby owner thread). Bumped before every send, decremented
    /// when the owner picks up the request. With one owner thread, a
    /// non-zero value indicates compile contention.
    pub compile_queue_depth: IntGauge,

    /// End-to-end seconds for a compile-dispatcher request — measured
    /// from the caller's `send` to the response arriving. Includes
    /// both queue-wait and actual compile work. Pair with
    /// `compilation_duration_seconds` (which the owner thread emits
    /// for the actual compile, T2-T1) to derive queue-only wait:
    ///
    ///   queue_wait ≈ compile_request_seconds − compilation_duration_seconds
    ///
    /// Adding the owner-side instrumentation lands in S2 (RubyPool)
    /// so the two metrics together prove that pool parallelism shifts
    /// the queue-wait component without changing actual compile cost.
    pub compile_request_seconds: Histogram,

    // -----------------------------------------------------------------
    // X2 — output-bindings metrics.
    //
    // Crossplane analog: connection-secret writes. Pangea variant is
    // per-binding instead of all-outputs-to-one-secret. Each apply
    // cycle increments once per binding, with the result label
    // distinguishing publish/missing/errored.
    //
    // Cardinality: O(templates × distinct_results). The result label
    // is bounded (3 values), so growth is linear in templates only.
    // -----------------------------------------------------------------

    /// Counter incremented once per output-binding per apply cycle.
    /// Labels: template, namespace, result (published|output_missing|errored).
    /// Use to alert on `errored` rates (RBAC misconfig in target ns) or
    /// `output_missing` rates (binding declares an output the template
    /// no longer produces — author drift).
    pub output_bindings_published_total: IntCounterVec,

    // -----------------------------------------------------------------
    // Magma apply-outcome metrics (ASM-observe-operator-metrics).
    //
    // Until these landed, a magma apply's outcome was only visible
    // through the generic `pangea_tofu_operations_total{operation,result}`
    // (a binary apply/success|failure) and the per-CR
    // `status.lastCycle`. Neither surfaced the load-bearing number from
    // a real apply: the live pleme-io-opensource cutover applied 1494
    // resources with 14 FAILED — the generic counter only recorded
    // "apply=failure", giving no fleet-scrapeable signal of the 14
    // partial failures. magma's apply path already parses the
    // `{applied, failed, phase}` shape out of `magma-bundle.json`
    // (see executor/magma_bundle.rs::read_apply_outcome); these
    // first-class metrics record it where the controller reads the
    // bundle at cycle-receipt time.
    //
    // Cardinality: keyed by (template, namespace) — grows linearly with
    // the InfrastructureTemplate count, identical to the settling /
    // policy metrics above. The `phase` label on the counter is a
    // bounded enum (Succeeded|Failed). Safe to a few hundred templates.
    // -----------------------------------------------------------------

    /// Total magma apply runs, by template + terminal phase.
    /// Labels: template, namespace, phase (Succeeded|Failed).
    /// `Succeeded` = the apply's lifecycle FSM reached `Stable` with
    /// zero failed changes; `Failed` = `Failed` phase or any failed
    /// change. Lets a dashboard count apply attempts + the
    /// success/failure split per template without parsing CR status.
    pub magma_apply_total: IntCounterVec,

    /// Resources successfully applied in the LAST magma apply per
    /// template (gauge — `outcome.applied.len()`). Labels: template,
    /// namespace. The "1494" half of the `applied 1494 / failed 14`
    /// shape.
    pub magma_resources_applied: IntGaugeVec,

    /// Resources that FAILED to apply in the LAST magma apply per
    /// template (gauge — `outcome.failed.len()`). Labels: template,
    /// namespace. THE signal that would have surfaced the 14 failures
    /// in the live cutover — `pangea_magma_resources_failed > 0` is the
    /// canonical "this template's apply is partially failing" alert
    /// expression (lareira-observe's `PangeaTemplateFailing`).
    pub magma_resources_failed: IntGaugeVec,

    /// Cumulative count of failed changes across ALL magma applies per
    /// template (counter — incremented by `outcome.failed.len()` each
    /// apply). Labels: template, namespace. The counter complements the
    /// gauge: the gauge answers "is it failing now?", the counter
    /// answers "how much has this template's apply churned failures
    /// over time" (rate() surfaces a flapping template).
    pub magma_failed_changes_total: IntCounterVec,
}

impl Metrics {
    /// Create new metrics instance.
    pub fn new() -> Self {
        let registry = Registry::new();

        let reconciliations_total = IntCounter::with_opts(Opts::new(
            "pangea_reconciliations_total",
            "Total number of InfrastructureTemplate reconciliations",
        ))
        .expect("metric can be created");

        let namespace_reconciliations_total = IntCounter::with_opts(Opts::new(
            "pangea_namespace_reconciliations_total",
            "Total number of PangeaNamespace reconciliations",
        ))
        .expect("metric can be created");

        let templates_by_phase = IntGaugeVec::new(
            Opts::new(
                "pangea_templates_by_phase",
                "Number of InfrastructureTemplates by phase",
            ),
            &["phase"],
        )
        .expect("metric can be created");

        let reconciliation_duration_seconds = HistogramVec::new(
            HistogramOpts::new(
                "pangea_reconciliation_duration_seconds",
                "Duration of reconciliation operations",
            )
            .buckets(vec![0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0]),
            &["phase"],
        )
        .expect("metric can be created");

        let drift_detected_total = IntCounter::with_opts(Opts::new(
            "pangea_drift_detected_total",
            "Total number of drift detections",
        ))
        .expect("metric can be created");

        let pg_state_operations_total = IntCounterVec::new(
            Opts::new(
                "pangea_pg_state_operations_total",
                "Total PostgreSQL state backend operations",
            ),
            &["operation"],
        )
        .expect("metric can be created");

        let active_reconciliations = IntGauge::with_opts(Opts::new(
            "pangea_active_reconciliations",
            "Number of currently active reconciliations",
        ))
        .expect("metric can be created");

        let compilation_duration_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "pangea_compilation_duration_seconds",
                "Duration of Ruby DSL compilation",
            )
            .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        )
        .expect("metric can be created");

        let reconciliation_errors_total = IntCounterVec::new(
            Opts::new(
                "pangea_reconciliation_errors_total",
                "Total reconciliation errors by type",
            ),
            &["error_type"],
        )
        .expect("metric can be created");

        let policy_decisions_total = IntCounterVec::new(
            Opts::new(
                "pangea_policy_decisions_total",
                "Per-resource policy decisions emitted during plan evaluation",
            ),
            &["template", "target_namespace", "decision"],
        )
        .expect("metric can be created");

        let consecutive_drift_cycles = IntGaugeVec::new(
            Opts::new(
                "pangea_consecutive_drift_cycles",
                "Current consecutive-drift-cycle count per template (0 = settled)",
            ),
            &["template", "target_namespace"],
        )
        .expect("metric can be created");

        let settled = IntGaugeVec::new(
            Opts::new(
                "pangea_settled",
                "1 when Settled condition is True, 0 when False",
            ),
            &["template", "target_namespace"],
        )
        .expect("metric can be created");

        let stuck_resources = IntGaugeVec::new(
            Opts::new(
                "pangea_stuck_resources",
                "Number of resources in the stuck set per template",
            ),
            &["template", "target_namespace"],
        )
        .expect("metric can be created");

        let settling_failures_total = IntCounterVec::new(
            Opts::new(
                "pangea_settling_failures_total",
                "Settling-failure escalations by reason",
            ),
            &["template", "target_namespace", "reason"],
        )
        .expect("metric can be created");

        let template_drift_detail = IntGaugeVec::new(
            Opts::new(
                "pangea_template_drift_detail",
                "Pending changes per template by action and risk",
            ),
            &["template", "target_namespace", "action", "risk"],
        )
        .expect("metric can be created");

        // policy_skipped_total carries three labels:
        //   * controller — which reconciler skipped (e.g. "template")
        //   * source     — which precedence layer fired
        //                  ("global", "controller", "catalog", "workspace")
        //   * target     — for per-workspace skips, the "<ns>/<name>" key
        //                  for templates or "<catalog>" for catalogs;
        //                  empty for global/controller skips
        // The third label keeps cardinality bounded by the number of
        // workspaces (typically <100) — well below Prometheus's
        // recommended ~10k cardinality limit for a single counter.
        let policy_skipped_total = IntCounterVec::new(
            Opts::new(
                "pangea_policy_skipped_total",
                "Reconciles skipped by OperatorPolicy gate (Item J observability)",
            ),
            &["controller", "source", "target"],
        )
        .expect("metric can be created");

        let compile_failures_total = IntCounterVec::new(
            Opts::new(
                "pangea_compile_failures_total",
                "Template compile failures (Item J observability)",
            ),
            &["target_namespace", "name"],
        )
        .expect("metric can be created");

        let escalation_recommended_depth = IntGaugeVec::new(
            Opts::new(
                "pangea_escalation_recommended_depth",
                "Recommended escalation rung depth per template \
                 (0=Retry, 1=RefreshSource, 2=ReloadGems, 3=RecycleWorkers, 4=PauseAndAlert) \
                 (controller-detection-axis fix step)",
            ),
            &["target_namespace", "name"],
        )
        .expect("metric can be created");

        let escalation_actions_total = IntCounterVec::new(
            Opts::new(
                "pangea_escalation_actions_total",
                "Total times each escalation action was recommended by template+action",
            ),
            &["target_namespace", "name", "action"],
        )
        .expect("metric can be created");

        let consecutive_compile_failures = IntGaugeVec::new(
            Opts::new(
                "pangea_template_consecutive_compile_failures",
                "Current consecutive compile failure count per template",
            ),
            &["target_namespace", "name"],
        )
        .expect("metric can be created");

        let workspace_active_override = IntGaugeVec::new(
            Opts::new(
                "pangea_workspace_active_override",
                "1 for each workspace whose effective state is Active during a more-general pause; \
                 lets dashboards answer 'what's still reconciling under a global pause?'",
            ),
            &["kind", "target_namespace", "name", "source"],
        )
        .expect("metric can be created");

        let controller_reconciliations_total = IntCounterVec::new(
            Opts::new(
                "pangea_controller_reconciliations_total",
                "Reconciles by controller and result (ok|error|skip)",
            ),
            &["controller", "result"],
        )
        .expect("metric can be created");

        let packer_builds_by_phase = IntGaugeVec::new(
            Opts::new(
                "pangea_packer_builds_by_phase",
                "PackerBuild CRs by phase",
            ),
            &["target_namespace", "name", "phase"],
        )
        .expect("metric can be created");

        let image_pipelines_by_phase = IntGaugeVec::new(
            Opts::new(
                "pangea_image_pipelines_by_phase",
                "ImagePipeline CRs by phase",
            ),
            &["target_namespace", "name", "phase"],
        )
        .expect("metric can be created");

        let ami_tests_by_phase = IntGaugeVec::new(
            Opts::new(
                "pangea_ami_tests_by_phase",
                "AmiTest CRs by phase",
            ),
            &["target_namespace", "name", "phase"],
        )
        .expect("metric can be created");

        let compliance_schedules_by_phase = IntGaugeVec::new(
            Opts::new(
                "pangea_compliance_schedules_by_phase",
                "ComplianceSchedule CRs by phase",
            ),
            &["target_namespace", "name", "phase"],
        )
        .expect("metric can be created");

        let compliance_bindings_gated_targets = IntGaugeVec::new(
            Opts::new(
                "pangea_compliance_bindings_gated_targets",
                "Number of targets currently gated by a ComplianceBinding due to non-compliance",
            ),
            &["target_namespace", "name"],
        )
        .expect("metric can be created");

        let compile_queue_depth = IntGauge::with_opts(Opts::new(
            "pangea_compile_queue_depth",
            "Instantaneous depth of the magnus compile dispatcher queue",
        ))
        .expect("metric can be created");

        let compile_request_seconds = Histogram::with_opts(
            HistogramOpts::new(
                "pangea_compile_request_seconds",
                "End-to-end seconds for a compile-dispatcher request — caller send to response received. Includes queue wait + compile work.",
            )
            .buckets(vec![0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 30.0]),
        )
        .expect("metric can be created");

        let output_bindings_published_total = IntCounterVec::new(
            Opts::new(
                "pangea_output_bindings_published_total",
                "Output bindings processed during a successful apply, by per-binding result (X2).",
            ),
            &["template", "target_namespace", "result"],
        )
        .expect("metric can be created");

        let magma_apply_total = IntCounterVec::new(
            Opts::new(
                "pangea_magma_apply_total",
                "Total magma apply runs by template and terminal phase (Succeeded|Failed)",
            ),
            &["template", "target_namespace", "phase"],
        )
        .expect("metric can be created");

        let magma_resources_applied = IntGaugeVec::new(
            Opts::new(
                "pangea_magma_resources_applied",
                "Resources applied in the last magma apply per template (outcome.applied.len())",
            ),
            &["template", "target_namespace"],
        )
        .expect("metric can be created");

        let magma_resources_failed = IntGaugeVec::new(
            Opts::new(
                "pangea_magma_resources_failed",
                "Resources that failed to apply in the last magma apply per template (outcome.failed.len())",
            ),
            &["template", "target_namespace"],
        )
        .expect("metric can be created");

        let magma_failed_changes_total = IntCounterVec::new(
            Opts::new(
                "pangea_magma_failed_changes_total",
                "Cumulative failed changes across all magma applies per template",
            ),
            &["template", "target_namespace"],
        )
        .expect("metric can be created");

        // Register all metrics
        registry
            .register(Box::new(reconciliations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(namespace_reconciliations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(templates_by_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(reconciliation_duration_seconds.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(drift_detected_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(pg_state_operations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(active_reconciliations.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compilation_duration_seconds.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(reconciliation_errors_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(policy_decisions_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(consecutive_drift_cycles.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(settled.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(stuck_resources.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(settling_failures_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(template_drift_detail.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(policy_skipped_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compile_failures_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(consecutive_compile_failures.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(escalation_recommended_depth.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(escalation_actions_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(workspace_active_override.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(controller_reconciliations_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(packer_builds_by_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(image_pipelines_by_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(ami_tests_by_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compliance_schedules_by_phase.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compliance_bindings_gated_targets.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compile_queue_depth.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(compile_request_seconds.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(output_bindings_published_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(magma_apply_total.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(magma_resources_applied.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(magma_resources_failed.clone()))
            .expect("metric can be registered");
        registry
            .register(Box::new(magma_failed_changes_total.clone()))
            .expect("metric can be registered");

        // Process-level metrics (process_cpu_seconds_total /
        // process_resident_memory_bytes / process_open_fds /
        // process_max_fds / process_threads / process_start_time_seconds).
        // The `ProcessCollector` is gated by prometheus to
        // `target_os = "linux"` (it reads /proc via procfs), so the
        // registration is cfg-gated: live on the Linux operator pod,
        // a no-op on macOS dev hosts. `register` may fail only if a
        // collector with the same desc is already registered — which
        // cannot happen for a fresh per-`Metrics` registry — so the
        // `let _ =` swallow is safe and keeps `new()` infallible.
        #[cfg(target_os = "linux")]
        {
            let process_collector =
                prometheus::process_collector::ProcessCollector::for_self();
            let _ = registry.register(Box::new(process_collector));
        }

        Self {
            registry,
            reconciliations_total,
            namespace_reconciliations_total,
            templates_by_phase,
            reconciliation_duration_seconds,
            drift_detected_total,
            pg_state_operations_total,
            active_reconciliations,
            compilation_duration_seconds,
            reconciliation_errors_total,
            policy_decisions_total,
            consecutive_drift_cycles,
            settled,
            stuck_resources,
            settling_failures_total,
            template_drift_detail,
            policy_skipped_total,
            compile_failures_total,
            consecutive_compile_failures,
            escalation_recommended_depth,
            escalation_actions_total,
            workspace_active_override,
            controller_reconciliations_total,
            packer_builds_by_phase,
            image_pipelines_by_phase,
            ami_tests_by_phase,
            compliance_schedules_by_phase,
            compliance_bindings_gated_targets,
            compile_queue_depth,
            compile_request_seconds,
            output_bindings_published_total,
            magma_apply_total,
            magma_resources_applied,
            magma_resources_failed,
            magma_failed_changes_total,
        }
    }

    /// Convenience: bump the per-controller reconcile counter.
    /// Wraps the with_label_values+inc dance so controllers don't
    /// have to spell out the label set.
    pub fn record_reconcile(&self, controller: crate::crd::ControllerKind, result: &str) {
        self.controller_reconciliations_total
            .with_label_values(&[controller.name(), result])
            .inc();
    }

    /// Sibling of `record_reconcile` for the controllers that don't
    /// fit `ControllerKind` (which is scoped to policy-gate-governed
    /// reconcilers). Currently:
    ///
    ///   * `operator_policy` — owns the policy CR; bypasses the gate
    ///     by design (otherwise globalSuspend would freeze the very
    ///     controller that surfaces the pause status).
    ///   * `fleet_status` — read-only fleet aggregator; bypasses the
    ///     gate so observability stays live during pauses.
    ///
    /// Keeping the metric label-space wider than `ControllerKind`
    /// (whose primary purpose is naming entries in
    /// `OperatorPolicy.spec.controllerSuspend`) lets the
    /// `pangea_controller_reconciliations_total` denominator cover
    /// 100% of controllers — important so the
    /// `PangeaControllerReconcileRateHigh` alert can detect a hot
    /// loop in either of these too. (operator_policy and
    /// fleet_status both had observed self-trigger loops on rio
    /// 2026-05-07; the alert needs to see them.)
    ///
    /// `name` should be the snake_case controller name to match the
    /// rest of the label space: "template", "namespace", …,
    /// "operator_policy", "fleet_status".
    pub fn record_reconcile_named(&self, name: &str, result: &str) {
        self.controller_reconciliations_total
            .with_label_values(&[name, result])
            .inc();
    }

    /// Start a phase-duration timer for `reconciliation_duration_seconds{phase}`.
    /// The returned `HistogramTimer` records the elapsed wall-clock on drop,
    /// so call sites can simply hold it for the duration of a phase handler:
    ///
    /// ```ignore
    /// let _timer = state.metrics.record_phase_duration("compiling");
    /// // ... phase work ...
    /// // timer auto-records on scope exit
    /// ```
    ///
    /// Convention: phase strings should mirror `Phase` enum variants
    /// in lowercase ("compiling", "initializing", "planning",
    /// "applying", "destroying", "import"). Stable label values
    /// keep dashboards / alerts queries consistent.
    pub fn record_phase_duration(
        &self,
        phase: &str,
    ) -> prometheus::HistogramTimer {
        self.reconciliation_duration_seconds
            .with_label_values(&[phase])
            .start_timer()
    }

    /// X2: bump the output-bindings counter for one cycle's per-binding
    /// outcome. `result` should be one of "published" / "output_missing"
    /// / "errored" — keeps the label set bounded.
    pub fn record_output_binding(&self, template: &str, namespace: &str, result: &str) {
        self.output_bindings_published_total
            .with_label_values(&[template, namespace, result])
            .inc();
    }

    /// Record the outcome of one magma apply for a template. This is
    /// the first-class signal for the magma reconcile loop: the
    /// `{applied, failed, phase}` shape the operator already parses out
    /// of `magma-bundle.json`.
    ///
    /// Sets:
    ///   * `pangea_magma_apply_total{template,namespace,phase}` += 1
    ///     (phase = "Succeeded" when `failed == 0`, else "Failed")
    ///   * `pangea_magma_resources_applied{template,namespace}` = applied
    ///   * `pangea_magma_resources_failed{template,namespace}` = failed
    ///   * `pangea_magma_failed_changes_total{template,namespace}` += failed
    ///
    /// The two gauges are "last apply" snapshots (idempotently
    /// overwritten each apply); the two counters are monotonic. Called
    /// from `record_reconcile_cycle` whenever a magma bundle yields an
    /// apply outcome. tofu cycles never call this.
    pub fn record_magma_apply(
        &self,
        template: &str,
        namespace: &str,
        applied: u64,
        failed: u64,
    ) {
        let phase = if failed == 0 { "Succeeded" } else { "Failed" };
        self.magma_apply_total
            .with_label_values(&[template, namespace, phase])
            .inc();
        // i64 is the gauge's native width; apply counts (low thousands
        // — the live cutover was 1494) never approach i64::MAX, but
        // saturate defensively rather than wrap.
        self.magma_resources_applied
            .with_label_values(&[template, namespace])
            .set(applied.min(i64::MAX as u64) as i64);
        self.magma_resources_failed
            .with_label_values(&[template, namespace])
            .set(failed.min(i64::MAX as u64) as i64);
        if failed > 0 {
            self.magma_failed_changes_total
                .with_label_values(&[template, namespace])
                .inc_by(failed);
        }
    }

    /// Idempotently materialize the magma apply-outcome series for a
    /// template at reconcile time, BEFORE any apply has run.
    ///
    /// ## Why this exists (the never-fires-until-first-failure gap)
    ///
    /// `record_magma_apply` only touches a `(template, target_namespace)`
    /// series on the first non-NoOp apply. Until then,
    /// `pangea_magma_resources_failed{...}` does not exist as a series —
    /// and Prometheus evaluates an alert expression against a
    /// non-existent series as "no data", so `... > 0` is never true.
    /// The canonical `PangeaTemplateFailing` alert therefore could not
    /// fire on a template's *first* apply failure (the worst time to be
    /// blind), because the series only came into being once a failure
    /// had already happened.
    ///
    /// Seeding at reconcile entry materializes the gauges at 0 and the
    /// `Succeeded` counter at its current value (via `inc_by(0)`), so
    /// the series exist from the first reconcile. A subsequent real
    /// `record_magma_apply` overwrites the gauges with the live counts —
    /// the seed is a floor, never a clobber of real data
    /// (`get_metric_with_label_values` is create-if-absent, and
    /// `inc_by(0)` is a no-op on an existing counter).
    ///
    /// Label arity + order are IDENTICAL to `record_magma_apply` so the
    /// seeded series and the recorded series are the same series — a
    /// drift in label order would create a second, never-updated series.
    pub fn seed_template(&self, template: &str, namespace: &str) {
        // Counter: touch the Succeeded series with a no-op increment so
        // it exists at its real value from reconcile #1. The Failed
        // counter series is intentionally NOT seeded — counters are for
        // rate(), and a phantom Failed=0 would not change rate() but a
        // real Failed only ever appears when a failure happens.
        self.magma_apply_total
            .with_label_values(&[template, namespace, "Succeeded"])
            .inc_by(0);
        // Gauges: materialize at 0 so the "is it failing now?" alert has
        // a series to evaluate before the first apply. `set(0)` is the
        // create-if-absent path; a later real apply overwrites it.
        self.magma_resources_failed
            .with_label_values(&[template, namespace])
            .set(0);
        self.magma_resources_applied
            .with_label_values(&[template, namespace])
            .set(0);
    }

    /// Compile-specific timer. Distinct from `record_phase_duration("compiling")`
    /// because compile latency has a wider distribution (gem download +
    /// Ruby load + Rhai/Lisp eval) than other phases — its histogram has
    /// finer-grained buckets at the long tail. Both metrics fire when the
    /// compile phase runs; dashboards can pick whichever is more useful.
    pub fn record_compile_duration(&self) -> prometheus::HistogramTimer {
        self.compilation_duration_seconds.start_timer()
    }

    /// Set a per-CR phase gauge to 1 for the current phase and 0 for
    /// every other phase value. The reset-then-set shape means the
    /// gauge is exactly truthful: each (namespace, name) tuple has at
    /// most one phase=1 series at any moment.
    ///
    /// Generic over the phase enum's `Display` impl so each
    /// per-CRD recorder reuses the same shape. Wrapper helpers below
    /// hand the right gauge in for each CRD class.
    fn set_phase_gauge<P: std::fmt::Display>(
        gauge: &IntGaugeVec,
        namespace: &str,
        name: &str,
        phase: &P,
        all_phases: &[&str],
    ) {
        let cur = format!("{phase}");
        for p in all_phases {
            let v = if *p == cur { 1 } else { 0 };
            gauge.with_label_values(&[namespace, name, p]).set(v);
        }
    }

    /// Bump the PackerBuild phase gauge.
    pub fn set_packer_build_phase(&self, namespace: &str, name: &str, phase: &crate::crd::PackerBuildPhase) {
        Self::set_phase_gauge(
            &self.packer_builds_by_phase,
            namespace, name, phase,
            &["Pending","Compiling","Validating","Building","Extracting","Ready","Failed"],
        );
    }

    /// Bump the ImagePipeline phase gauge.
    pub fn set_image_pipeline_phase(&self, namespace: &str, name: &str, phase: &crate::crd::ImagePipelinePhase) {
        Self::set_phase_gauge(
            &self.image_pipelines_by_phase,
            namespace, name, phase,
            &["Pending","Building","Testing","Planning","AwaitingApproval","Applying","Verifying","Completed","Failed","RollingBack"],
        );
    }

    /// Bump the AmiTest phase gauge.
    pub fn set_ami_test_phase(&self, namespace: &str, name: &str, phase: &crate::crd::AmiTestPhase) {
        Self::set_phase_gauge(
            &self.ami_tests_by_phase,
            namespace, name, phase,
            &["Pending","Resolving","Running","Passed","Failed","Cleaning"],
        );
    }

    /// Bump the ComplianceSchedule phase gauge.
    pub fn set_compliance_schedule_phase(&self, namespace: &str, name: &str, phase: &crate::crd::ComplianceSchedulePhase) {
        Self::set_phase_gauge(
            &self.compliance_schedules_by_phase,
            namespace, name, phase,
            &["Idle","Running","Compliant","NonCompliant","Error"],
        );
    }

    /// Set the gated-targets count for a ComplianceBinding.
    pub fn set_compliance_binding_gated_targets(&self, namespace: &str, name: &str, count: i64) {
        self.compliance_bindings_gated_targets
            .with_label_values(&[namespace, name])
            .set(count);
    }

    /// Gather metrics in Prometheus text format.
    pub fn gather(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

/// RAII guard for `pangea_active_reconciliations`: increments the gauge
/// on construction and decrements it on `Drop`. Holding one for the
/// body of a reconcile makes the gauge reflect the live in-flight
/// count — including on the early-return / error paths, because `Drop`
/// runs regardless of how the scope exits. Before this, the gauge was
/// declared but never inc/dec'd, so it sat flat at 0.
///
/// The gauge handle is a cheap `Arc`-backed clone, so constructing a
/// guard per reconcile is allocation-free beyond the clone.
#[must_use = "the guard decrements pangea_active_reconciliations on drop; bind it to a name"]
pub struct ActiveReconcileGuard {
    gauge: IntGauge,
}

impl ActiveReconcileGuard {
    /// Increment `pangea_active_reconciliations` and return a guard that
    /// decrements it on drop. Bind it for the duration of a reconcile.
    pub fn enter(metrics: &Metrics) -> Self {
        let gauge = metrics.active_reconciliations.clone();
        gauge.inc();
        Self { gauge }
    }
}

impl Drop for ActiveReconcileGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_new_does_not_panic() {
        let _metrics = Metrics::new();
    }

    #[test]
    fn test_metrics_default_does_not_panic() {
        let _metrics = Metrics::default();
    }

    #[test]
    fn test_gather_returns_valid_prometheus_text() {
        let metrics = Metrics::new();
        let output = metrics.gather();
        assert!(output.is_empty() || output.contains("pangea_"));
    }

    #[test]
    fn test_reconciliation_counter_increments() {
        let metrics = Metrics::new();
        metrics.reconciliations_total.inc();
        metrics.reconciliations_total.inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_reconciliations_total"));
        assert!(output.contains(" 2"));
    }

    #[test]
    fn record_reconcile_named_emits_label_pair_used_by_anomaly_alert() {
        // Pin the metric label format that
        // `PangeaControllerReconcileRateHigh` (chart 0.8.14) groups
        // by — `controller=NAME, result=ok`. Without this test, a
        // future rename (e.g. snake → camelCase or addition of a
        // namespace label) could silently break the alert grouping
        // and we'd discover the regression only when the next hot
        // loop went undetected. The test calls the named-variant
        // path used by the two self-driving controllers
        // (operator_policy + fleet_status — intentionally absent from
        // `ControllerKind`).
        let metrics = Metrics::new();
        metrics.record_reconcile_named("operator_policy", "ok");
        metrics.record_reconcile_named("operator_policy", "ok");
        metrics.record_reconcile_named("fleet_status", "ok");
        metrics.record_reconcile_named("fleet_status", "error");

        let output = metrics.gather();

        // The exact strings the alert PromQL groups by — must match.
        assert!(
            output.contains(r#"controller="operator_policy""#),
            "operator_policy series missing — alert grouping breaks:\n{output}"
        );
        assert!(
            output.contains(r#"controller="fleet_status""#),
            "fleet_status series missing — alert grouping breaks:\n{output}"
        );
        assert!(output.contains(r#"result="ok""#));
        assert!(output.contains(r#"result="error""#));

        // The exact metric name the alert PromQL queries.
        assert!(
            output.contains("pangea_controller_reconciliations_total"),
            "metric name renamed — alert PromQL won't match:\n{output}"
        );
    }

    #[test]
    fn record_reconcile_typed_and_named_share_same_metric_series() {
        // Defense-in-depth: the typed `record_reconcile(kind, result)`
        // and the string `record_reconcile_named(name, result)` paths
        // both target the same underlying IntCounterVec, so a
        // controller migrated from one path to the other (or vice
        // versa) keeps producing samples on the same series. If they
        // ever diverged (e.g. a separate counter got introduced for
        // the named path), the alert would only see one half of the
        // fleet.
        let metrics = Metrics::new();
        metrics.record_reconcile(crate::crd::ControllerKind::Template, "ok");
        metrics.record_reconcile_named("template", "ok");
        let output = metrics.gather();

        // Two increments, one series — the line should show count = 2.
        let template_lines: Vec<&str> = output
            .lines()
            .filter(|l| l.starts_with("pangea_controller_reconciliations_total"))
            .filter(|l| l.contains(r#"controller="template""#))
            .filter(|l| l.contains(r#"result="ok""#))
            .collect();
        assert_eq!(
            template_lines.len(),
            1,
            "typed + named paths produced multiple series — they must coalesce:\n{}",
            template_lines.join("\n")
        );
        assert!(
            template_lines[0].ends_with(" 2"),
            "expected count = 2 (one typed + one named on the same series), got: {}",
            template_lines[0]
        );
    }

    #[test]
    fn test_namespace_reconciliation_counter() {
        let metrics = Metrics::new();
        metrics.namespace_reconciliations_total.inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_namespace_reconciliations_total"));
    }

    #[test]
    fn test_templates_by_phase_gauge() {
        let metrics = Metrics::new();
        metrics.templates_by_phase.with_label_values(&["Ready"]).set(5);
        metrics.templates_by_phase.with_label_values(&["Failed"]).set(1);

        let output = metrics.gather();
        assert!(output.contains("pangea_templates_by_phase"));
        assert!(output.contains("Ready"));
        assert!(output.contains("Failed"));
    }

    #[test]
    fn test_active_reconciliations_gauge() {
        let metrics = Metrics::new();
        metrics.active_reconciliations.set(3);
        assert_eq!(metrics.active_reconciliations.get(), 3);

        metrics.active_reconciliations.inc();
        assert_eq!(metrics.active_reconciliations.get(), 4);

        metrics.active_reconciliations.dec();
        assert_eq!(metrics.active_reconciliations.get(), 3);
    }

    #[test]
    fn test_reconciliation_errors_counter() {
        let metrics = Metrics::new();
        metrics.reconciliation_errors_total.with_label_values(&["kube_error"]).inc();
        metrics.reconciliation_errors_total.with_label_values(&["timeout"]).inc();
        metrics.reconciliation_errors_total.with_label_values(&["timeout"]).inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_reconciliation_errors_total"));
    }

    #[test]
    fn test_drift_detected_counter() {
        let metrics = Metrics::new();
        metrics.drift_detected_total.inc();

        let output = metrics.gather();
        assert!(output.contains("pangea_drift_detected_total"));
    }

    #[test]
    fn test_histogram_records_observation() {
        let metrics = Metrics::new();
        metrics.reconciliation_duration_seconds.with_label_values(&["Planning"]).observe(1.5);
        metrics.compilation_duration_seconds.observe(0.3);

        let output = metrics.gather();
        assert!(output.contains("pangea_reconciliation_duration_seconds"));
        assert!(output.contains("pangea_compilation_duration_seconds"));
    }

    #[test]
    fn test_metrics_clone() {
        let metrics = Metrics::new();
        metrics.reconciliations_total.inc();
        let cloned = metrics.clone();
        cloned.reconciliations_total.inc();

        let output = metrics.gather();
        assert!(output.contains(" 2"));
    }

    #[test]
    fn test_policy_decisions_counter() {
        let metrics = Metrics::new();
        metrics
            .policy_decisions_total
            .with_label_values(&["t1", "ns", "autoApply"])
            .inc_by(3);
        metrics
            .policy_decisions_total
            .with_label_values(&["t1", "ns", "refuse"])
            .inc();
        let output = metrics.gather();
        assert!(output.contains("pangea_policy_decisions_total"));
        assert!(output.contains("autoApply"));
        assert!(output.contains("refuse"));
    }

    #[test]
    fn test_settled_gauge_set_clear() {
        let metrics = Metrics::new();
        metrics.settled.with_label_values(&["t1", "ns"]).set(1);
        assert_eq!(metrics.settled.with_label_values(&["t1", "ns"]).get(), 1);
        metrics.settled.with_label_values(&["t1", "ns"]).set(0);
        assert_eq!(metrics.settled.with_label_values(&["t1", "ns"]).get(), 0);
    }

    #[test]
    fn test_consecutive_drift_cycles_gauge() {
        let metrics = Metrics::new();
        metrics
            .consecutive_drift_cycles
            .with_label_values(&["t1", "ns"])
            .set(4);
        let output = metrics.gather();
        assert!(output.contains("pangea_consecutive_drift_cycles"));
        assert!(output.contains("4"));
    }

    #[test]
    fn test_settling_failures_counter() {
        let metrics = Metrics::new();
        metrics
            .settling_failures_total
            .with_label_values(&["t1", "ns", "StuckByFingerprint"])
            .inc();
        metrics
            .settling_failures_total
            .with_label_values(&["t1", "ns", "StuckByCount"])
            .inc();
        let output = metrics.gather();
        assert!(output.contains("StuckByFingerprint"));
        assert!(output.contains("StuckByCount"));
    }

    #[test]
    fn test_template_drift_detail_gauge() {
        let metrics = Metrics::new();
        metrics
            .template_drift_detail
            .with_label_values(&["t1", "ns", "delete", "high"])
            .set(2);
        let output = metrics.gather();
        assert!(output.contains("pangea_template_drift_detail"));
        assert!(output.contains("delete"));
        assert!(output.contains("high"));
    }

    #[test]
    fn test_stuck_resources_gauge() {
        let metrics = Metrics::new();
        metrics.stuck_resources.with_label_values(&["t1", "ns"]).set(7);
        assert_eq!(metrics.stuck_resources.with_label_values(&["t1", "ns"]).get(), 7);
    }

    // ── U3: phase-gauge helper coverage ──

    #[test]
    fn set_packer_build_phase_emits_one_active_label() {
        // The reset-then-set shape means the gauge tells the truth:
        // exactly one phase=1 series per (namespace,name) tuple.
        // Drift here would let an old phase keep =1 while the new
        // phase also =1, so the alert PromQL would double-count.
        use crate::crd::PackerBuildPhase;
        let metrics = Metrics::new();
        metrics.set_packer_build_phase("ns", "build1", &PackerBuildPhase::Building);

        let g = &metrics.packer_builds_by_phase;
        assert_eq!(g.with_label_values(&["ns", "build1", "Building"]).get(), 1);
        assert_eq!(g.with_label_values(&["ns", "build1", "Failed"]).get(), 0);
        assert_eq!(g.with_label_values(&["ns", "build1", "Ready"]).get(), 0);

        // Transition: only the new phase should be 1.
        metrics.set_packer_build_phase("ns", "build1", &PackerBuildPhase::Ready);
        assert_eq!(g.with_label_values(&["ns", "build1", "Building"]).get(), 0);
        assert_eq!(g.with_label_values(&["ns", "build1", "Ready"]).get(), 1);
    }

    #[test]
    fn set_image_pipeline_phase_covers_all_phases() {
        use crate::crd::ImagePipelinePhase;
        let metrics = Metrics::new();
        metrics.set_image_pipeline_phase("ns", "pipe1", &ImagePipelinePhase::AwaitingApproval);
        let g = &metrics.image_pipelines_by_phase;
        assert_eq!(
            g.with_label_values(&["ns", "pipe1", "AwaitingApproval"]).get(),
            1
        );
        assert_eq!(g.with_label_values(&["ns", "pipe1", "Completed"]).get(), 0);
    }

    #[test]
    fn set_compliance_binding_gated_targets_records_count() {
        let metrics = Metrics::new();
        metrics.set_compliance_binding_gated_targets("ns", "binding1", 5);
        assert_eq!(
            metrics
                .compliance_bindings_gated_targets
                .with_label_values(&["ns", "binding1"])
                .get(),
            5
        );
        // Reset to zero on next reconcile is supported by direct
        // re-set.
        metrics.set_compliance_binding_gated_targets("ns", "binding1", 0);
        assert_eq!(
            metrics
                .compliance_bindings_gated_targets
                .with_label_values(&["ns", "binding1"])
                .get(),
            0
        );
    }

    #[test]
    fn new_metrics_register_all_u3_phase_gauges() {
        // Smoke test that the registry actually contains every newly-
        // added metric. A missing register() call would silently emit
        // nothing — exactly the bug U3 was designed to catch
        // upstream of this commit. Prometheus only emits metrics that
        // have at least one observed sample, so we record a sentinel
        // before gathering.
        use crate::crd::{
            AmiTestPhase, ComplianceSchedulePhase, ImagePipelinePhase, PackerBuildPhase,
        };
        let metrics = Metrics::new();
        metrics.set_packer_build_phase("u3", "x", &PackerBuildPhase::Pending);
        metrics.set_image_pipeline_phase("u3", "x", &ImagePipelinePhase::Pending);
        metrics.set_ami_test_phase("u3", "x", &AmiTestPhase::Pending);
        metrics.set_compliance_schedule_phase("u3", "x", &ComplianceSchedulePhase::Idle);
        metrics.set_compliance_binding_gated_targets("u3", "x", 0);

        let text = metrics.gather();
        assert!(text.contains("pangea_packer_builds_by_phase"));
        assert!(text.contains("pangea_image_pipelines_by_phase"));
        assert!(text.contains("pangea_ami_tests_by_phase"));
        assert!(text.contains("pangea_compliance_schedules_by_phase"));
        assert!(text.contains("pangea_compliance_bindings_gated_targets"));
    }

    // ── S1: compile-dispatcher metrics ──

    #[test]
    fn compile_queue_depth_starts_at_zero_and_inc_dec_round_trip() {
        // The dispatcher's instrumented<F> wrapper bumps depth before
        // send and decrements after response. Verify the gauge can
        // round-trip cleanly.
        let metrics = Metrics::new();
        assert_eq!(metrics.compile_queue_depth.get(), 0);
        metrics.compile_queue_depth.inc();
        metrics.compile_queue_depth.inc();
        assert_eq!(metrics.compile_queue_depth.get(), 2);
        metrics.compile_queue_depth.dec();
        assert_eq!(metrics.compile_queue_depth.get(), 1);
        metrics.compile_queue_depth.dec();
        assert_eq!(metrics.compile_queue_depth.get(), 0);
    }

    #[test]
    fn compile_request_seconds_observes_into_buckets() {
        // Per-request duration histogram. Default buckets cover
        // the expected range (1ms → 30s).
        let metrics = Metrics::new();
        // No samples yet — count should be 0.
        let initial = metrics.compile_request_seconds.get_sample_count();
        metrics.compile_request_seconds.observe(0.123);
        metrics.compile_request_seconds.observe(0.456);
        metrics.compile_request_seconds.observe(2.5);
        assert_eq!(metrics.compile_request_seconds.get_sample_count(), initial + 3);
        // Sum is the cumulative observed seconds.
        assert!(metrics.compile_request_seconds.get_sample_sum() > 3.0);
    }

    #[test]
    fn compile_dispatcher_metrics_appear_in_gather_text() {
        // Smoke test that the registry actually contains the new
        // metrics. Pre-S1 the alert pipeline (and any future
        // dashboard) couldn't have referenced them; now they do.
        let metrics = Metrics::new();
        // Sentinel observation so the histogram emits its bucket
        // lines (Prometheus skips metrics with zero observations).
        metrics.compile_queue_depth.set(0);
        metrics.compile_request_seconds.observe(0.0);
        let text = metrics.gather();
        assert!(text.contains("pangea_compile_queue_depth"));
        assert!(text.contains("pangea_compile_request_seconds"));
    }

    // ── Observability-fix coverage ──────────────────────────────────

    #[test]
    fn seed_template_materializes_failed_gauge_at_zero_before_any_apply() {
        // The failure-signal gap: before this, the failed gauge series
        // did not exist until the first apply FAILED, so the
        // PangeaTemplateFailing alert (`... > 0`) could not fire on a
        // template's first failure. Seeding makes the series exist at
        // 0 from reconcile #1, so the alert has something to evaluate.
        let metrics = Metrics::new();
        // No apply has run; seed at reconcile entry.
        metrics.seed_template("cloudflare-pleme", "pangea-system");
        let text = metrics.gather();
        // The series exists in gather() output before any apply.
        assert!(
            text.contains("pangea_magma_resources_failed"),
            "failed gauge series missing after seed:\n{text}"
        );
        // And it is exactly 0 (the floor), not absent.
        let failed_line = text
            .lines()
            .find(|l| {
                l.starts_with("pangea_magma_resources_failed")
                    && l.contains(r#"template="cloudflare-pleme""#)
            })
            .expect("seeded failed series should be present");
        assert!(
            failed_line.ends_with(" 0"),
            "seeded failed gauge must be 0, got: {failed_line}"
        );
        // The applied gauge + Succeeded counter are seeded too.
        assert!(text.contains("pangea_magma_resources_applied"));
        assert!(text.contains("pangea_magma_apply_total"));
    }

    #[test]
    fn seed_then_record_overwrites_not_double_counts() {
        // The seed is a floor, not a clobber of real data: a later
        // record_magma_apply overwrites the gauges with live counts on
        // the SAME series (identical label arity/order), and the
        // Succeeded counter's inc_by(0) seed leaves a subsequent
        // real apply's count intact.
        let metrics = Metrics::new();
        metrics.seed_template("t1", "ns1");
        metrics.record_magma_apply("t1", "ns1", 1494, 14);
        // Gauge now reflects the real apply, not the seed's 0.
        assert_eq!(
            metrics.magma_resources_failed.with_label_values(&["t1", "ns1"]).get(),
            14
        );
        assert_eq!(
            metrics.magma_resources_applied.with_label_values(&["t1", "ns1"]).get(),
            1494
        );
        // The Succeeded counter was seeded with inc_by(0); a Failed
        // apply increments the Failed phase, leaving Succeeded at 0.
        let text = metrics.gather();
        // Exactly one Succeeded series for (t1, ns1), still 0 — the
        // seed didn't create a phantom increment.
        let succeeded_line = text
            .lines()
            .find(|l| {
                l.starts_with("pangea_magma_apply_total")
                    && l.contains(r#"phase="Succeeded""#)
                    && l.contains(r#"template="t1""#)
            })
            .expect("seeded Succeeded series should exist");
        assert!(
            succeeded_line.ends_with(" 0"),
            "seeded Succeeded counter must stay 0 after a Failed apply, got: {succeeded_line}"
        );
    }

    #[test]
    fn per_template_metrics_emit_target_namespace_not_colliding_namespace() {
        // The scrape renames a metric-emitted `namespace` to
        // `exported_namespace` because it collides with the pod's
        // namespace label. Renaming the operator-emitted label to
        // `target_namespace` removes the collision. Lock the label key
        // on a per-template metric here.
        let metrics = Metrics::new();
        metrics.seed_template("t1", "ns1");
        metrics
            .settled
            .with_label_values(&["t1", "ns1"])
            .set(1);
        let text = metrics.gather();
        // Per-template metrics carry `target_namespace`, never a bare
        // `namespace` label.
        assert!(
            text.contains(r#"target_namespace="ns1""#),
            "per-template metric missing target_namespace label:\n{text}"
        );
        // No per-template series should still emit the colliding
        // `namespace="ns1"` key. (Check the exact `namespace=` key, not
        // the substring `target_namespace=`.)
        let colliding: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with("pangea_"))
            .filter(|l| l.contains(r#"{namespace="ns1""#) || l.contains(r#",namespace="ns1""#))
            .collect();
        assert!(
            colliding.is_empty(),
            "per-template metric still emits colliding `namespace` label:\n{}",
            colliding.join("\n")
        );
    }

    #[test]
    fn reconciliation_errors_total_increments_on_error_path() {
        // The shipped PangeaReconciliationErrors alert queries
        // pangea_reconciliation_errors_total, which was never recorded.
        // The central error path (run_error_policy) now increments it.
        let metrics = Metrics::new();
        // Simulate the central error arm bumping the counter.
        metrics
            .reconciliation_errors_total
            .with_label_values(&["template"])
            .inc();
        let text = metrics.gather();
        assert!(
            text.contains("pangea_reconciliation_errors_total"),
            "reconciliation_errors_total series missing:\n{text}"
        );
        let line = text
            .lines()
            .find(|l| l.starts_with("pangea_reconciliation_errors_total"))
            .expect("error counter line present");
        assert!(line.ends_with(" 1"), "expected count 1, got: {line}");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_collector_exports_resident_memory() {
        // The process feature + process_collector export
        // process_resident_memory_bytes / process_cpu_seconds_total /
        // FDs / threads. Linux-only (procfs); the registration is
        // cfg-gated to target_os = "linux" in Metrics::new().
        let metrics = Metrics::new();
        let text = metrics.gather();
        assert!(
            text.contains("process_resident_memory_bytes"),
            "process_collector not exporting RSS:\n{text}"
        );
        assert!(text.contains("process_cpu_seconds_total"));
    }
}
