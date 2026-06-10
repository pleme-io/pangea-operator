//! Temp-git-remote fixtures, extracted from `src/ruby/gem_cache.rs`'s
//! in-module test helpers so integration tests (`staleness_honesty`,
//! `load_path_xor`) drive the same shape: a local `file://` "remote"
//! with deterministic commits, no network, no ssh keys. The in-crate
//! copies in `gem_cache.rs` remain — unit tests inside `src/` cannot
//! depend on the `tests/` tree.

#![allow(dead_code)] // not every integration-test binary uses every helper

use std::path::Path;
use std::process::Stdio;
use tempfile::TempDir;
use tokio::process::Command;

/// Run one git command in `dir`, panicking (with stderr) on failure —
/// fixture setup must never half-succeed silently.
pub async fn git_run(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .expect("git spawn");
    assert!(
        out.status.success(),
        "git {:?} failed: {}",
        args,
        String::from_utf8_lossy(&out.stderr)
    );
}

/// Build a "remote" git repo with one initial commit on `main`
/// touching `lib/test_gem/marker.txt`. Returns the dir + the
/// initial commit SHA.
pub async fn make_remote_repo(content: &str) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path();
    git_run(path, &["init", "-q", "-b", "main"]).await;
    git_run(path, &["config", "user.email", "test@example.com"]).await;
    git_run(path, &["config", "user.name", "test"]).await;
    git_run(path, &["config", "commit.gpgsign", "false"]).await;
    let lib_dir = path.join("lib").join("test_gem");
    tokio::fs::create_dir_all(&lib_dir).await.unwrap();
    tokio::fs::write(lib_dir.join("marker.txt"), content)
        .await
        .unwrap();
    git_run(path, &["add", "-A"]).await;
    git_run(path, &["commit", "-q", "-m", "initial"]).await;
    let sha = rev_parse_head(path).await;
    (dir, sha)
}

/// Push a new commit overwriting `marker.txt`. Returns the new SHA.
pub async fn add_commit(remote: &Path, content: &str) -> String {
    tokio::fs::write(
        remote.join("lib").join("test_gem").join("marker.txt"),
        content,
    )
    .await
    .unwrap();
    git_run(remote, &["add", "-A"]).await;
    git_run(remote, &["commit", "-q", "-m", "update"]).await;
    rev_parse_head(remote).await
}

/// `git rev-parse HEAD` in `dir` (fixture-side twin of the production
/// `freshness::git_rev_parse_head`, kept separate so a production
/// regression cannot silently validate itself).
pub async fn rev_parse_head(dir: &Path) -> String {
    let out = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "HEAD"])
        .output()
        .await
        .expect("rev-parse");
    assert!(out.status.success(), "git rev-parse HEAD failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}
