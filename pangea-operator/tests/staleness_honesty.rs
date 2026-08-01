//! Staleness-honesty integration tests — the freshness model's two
//! I/O fns (`git_rev_parse_head`, `observe_head`) driven against real
//! temp git remotes (file:// URLs, no network), plus the pure
//! decision law they feed. The kube-free pure tests
//! (`evaluate_source_freshness` / `ready_drift_decision`, including
//! "a stale compile can never produce Settled") live in
//! `src/controller/template/freshness.rs`'s unit tests.
//!
//! Tier-honest: these tests pin a C2 external-world observation
//! pipeline — they prove the operator OBSERVES staleness correctly,
//! not that staleness is unrepresentable (no compile error can prove
//! a remote's HEAD).

mod common;

use common::git_fixtures::{add_commit, git_run, make_remote_repo};
use pangea_operator::controller::template::freshness::{
    evaluate_source_freshness, git_rev_parse_head, observe_head, Freshness,
};

#[tokio::test]
async fn moved_remote_head_is_observed_as_stale() {
    // v1: remote + clone agree.
    let (remote, v1) = make_remote_repo("v1").await;
    let url = format!("file://{}", remote.path().display());

    let clone_parent = tempfile::TempDir::new().expect("clone tempdir");
    git_run(clone_parent.path(), &["clone", "-q", &url, "repo"]).await;
    let repo_dir = clone_parent.path().join("repo");

    let compiled = git_rev_parse_head(&repo_dir)
        .await
        .expect("rev-parse HEAD of clone");
    assert_eq!(compiled, v1, "clone's HEAD is the v1 commit");

    // No new commit: observed HEAD == compiled ⇒ Fresh.
    let head = observe_head(&url, "main", &[])
        .await
        .expect("ls-remote against unmoved remote");
    assert_eq!(head, v1);
    assert_eq!(
        evaluate_source_freshness(Some(&compiled), &head),
        Freshness::Fresh,
        "unmoved remote must read Fresh"
    );

    // The remote moves: observe_head sees v2 while the compile is
    // still anchored at v1 ⇒ Stale carrying both revisions — the
    // exact signal handle_ready's gate turns into a Compiling bounce.
    let v2 = add_commit(remote.path(), "v2").await;
    assert_ne!(v1, v2);
    let head = observe_head(&url, "main", &[])
        .await
        .expect("ls-remote against moved remote");
    assert_eq!(
        head, v2,
        "observe_head must see the new commit (1 RTT, no clone)"
    );
    assert_eq!(
        evaluate_source_freshness(Some(&compiled), &head),
        Freshness::Stale {
            compiled: Some(v1.clone()),
            head: v2.clone(),
        },
        "moved remote must read Stale (compiled vs head)"
    );
}

#[tokio::test]
async fn sha_pinned_ref_short_circuits_observation() {
    // A 40-hex SHA ref IS its own HEAD — ls-remote can't list a bare
    // commit, and a pinned source is definitionally at its revision.
    let sha = "0123456789abcdef0123456789abcdef01234567";
    let head = observe_head("https://example.invalid/never-contacted", sha, &[])
        .await
        .expect("SHA ref must not touch the remote");
    assert_eq!(head, sha);
    assert_eq!(
        evaluate_source_freshness(Some(sha), &head),
        Freshness::Fresh
    );
}

#[tokio::test]
async fn unreachable_remote_errors_so_caller_maps_to_unknown() {
    // The Unknown arm's ingress: observe_head must surface a typed
    // error (the gate maps it to Freshness::Unknown + the
    // source_freshness_check_failures_total counter), never a bogus
    // revision.
    let err = observe_head("file:///nonexistent/definitely/not/a/repo", "main", &[]).await;
    assert!(
        err.is_err(),
        "unreachable remote must error, not fabricate a HEAD"
    );
}

#[tokio::test]
async fn deleted_ref_errors_rather_than_passing_fresh() {
    let (remote, _v1) = make_remote_repo("v1").await;
    let url = format!("file://{}", remote.path().display());
    let err = observe_head(&url, "no-such-branch", &[]).await;
    assert!(
        err.is_err(),
        "a ref absent on the remote must error (caller maps to Unknown)"
    );
}
