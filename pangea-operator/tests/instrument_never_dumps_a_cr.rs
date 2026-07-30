//! No `#[instrument]` may auto-record its arguments. `skip_all` or nothing.
//!
//! ## The outage this seals
//!
//! `#[instrument]` Debug-records every function argument it is not told to
//! skip, and the JSON formatter stamps the resulting span fields onto *every*
//! event emitted inside that span. Eleven functions were written as
//!
//! ```ignore
//! #[instrument(skip(state), fields(name = %template.name_any(), ...))]
//! async fn reconcile_template(template: Arc<InfrastructureTemplate>, ...)
//! ```
//!
//! — skipping `state` but not the CR, even though `fields(...)` right there
//! already supplies the only identity anyone wanted. For a small CR nobody
//! notices. For `pleme-io-opensource` (846 repos, a status carrying 367
//! `DriftDetail`s) it made an ordinary one-line INFO event **49 KB**, several
//! times a minute.
//!
//! `tracing_subscriber`'s stdout writer is a synchronous mutex, so those
//! writes happen on the tokio worker thread that emitted them and back up
//! against the container runtime's pipe. Enough of them and no worker is left
//! to run `async fn health() -> &'static str { "OK" }` inside the kubelet's 5s
//! probe timeout. Three misses and the container is restarted — mid-cycle,
//! discarding ~22 minutes of plan+apply, which is then redone, which logs
//! again. A self-reinforcing loop where more work makes the kill more certain.
//!
//! Observed 2026-07-30 on camelot-eks: 54 restarts, `Applying` for 28 hours
//! without converging, every condition reading `Healthy=True` throughout —
//! because a SIGTERM'd cycle is indistinguishable from a clean one.
//!
//! ## Why the predicate is `skip_all` and not "skips its CR"
//!
//! The first cut of this guard looked for the specific broken shape: an
//! attribute that skips `state` while naming some `x` in
//! `fields(name = %x.name_any())`. That is a *shape guess*, and it fails two
//! ways. It cannot see a function whose arguments are named differently, and —
//! worse — it blesses `skip(state, template)`, which is only a snapshot of
//! today's parameter list. Add a third argument tomorrow and the bug is back,
//! with the guard still green.
//!
//! `skip_all` is total: no argument is ever auto-recorded, whatever the
//! signature grows into. So the predicate here is total too — *every*
//! `#[instrument]` uses `skip_all` — which is both stronger and simpler to
//! check than any attempt to reason about which arguments happen to be big.
//! Explicit `fields(...)` are unaffected; that is where identity belongs.
//!
//! This is the honest tier: **CI-caught**, not unrepresentable. A truly
//! unrepresentable seal would mean the crate could not name `tracing`'s
//! `#[instrument]` at all — only a local attribute macro that never emits an
//! argument record. That macro is the named next rung, not what is built here.

use std::path::Path;

#[test]
fn every_instrument_attribute_uses_skip_all() {
    let mut offenders = Vec::new();
    let mut checked = 0usize;

    for path in rust_sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src")) {
        let src = std::fs::read_to_string(&path).expect("readable source");

        for (idx, line) in src.lines().enumerate() {
            let line = line.trim();
            if !line.starts_with("#[instrument") {
                continue;
            }
            checked += 1;

            // A bare `#[instrument]` records EVERY argument — the worst form.
            // Anything with an explicit `skip(...)` is an enumerated list that
            // goes stale the next time a parameter is added.
            if !line.contains("skip_all") {
                offenders.push(format!(
                    "{}:{}\n      {line}",
                    path.strip_prefix(env!("CARGO_MANIFEST_DIR"))
                        .unwrap_or(&path)
                        .display(),
                    idx + 1
                ));
            }
        }
    }

    assert!(
        checked > 0,
        "found no #[instrument] attributes at all — the guard is scanning the wrong tree"
    );

    assert!(
        offenders.is_empty(),
        "#[instrument] must use `skip_all` so no argument is ever Debug-recorded into a \
         span. A bare attribute records everything; an enumerated `skip(...)` only \
         records everything you add to the signature later. This is what restarted the \
         operator mid-cycle for 28 hours on 2026-07-30 (see this file's header). Put \
         identity in `fields(...)` instead.\n\n  {}\n",
        offenders.join("\n  ")
    );
}

/// Guard the guard: if the scan silently stops finding files, the assertion
/// above passes for the wrong reason. Pin it against the known tree shape.
#[test]
fn the_scan_reaches_the_controllers() {
    let files = rust_sources(&Path::new(env!("CARGO_MANIFEST_DIR")).join("src"));
    let names: Vec<String> = files
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();

    for expected in ["template_controller.rs", "flow_controller.rs", "main.rs"] {
        assert!(
            names.iter().any(|n| n == expected),
            "recursive scan missed {expected}; it found {} files",
            names.len()
        );
    }
}

fn rust_sources(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    out
}
