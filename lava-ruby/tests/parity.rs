use std::path::{Path, PathBuf};

use lava_ruby::{render_pangea_template, RenderConfig};

const GEMS: &[&str] = &[
    "pangea-architectures",
    "pangea-core",
    "pangea-config",
    "pangea-github",
    "pangea-akeyless",
    "pangea-aws",
    "pangea-azure",
    "pangea-cloudflare",
    "pangea-datadog",
    "pangea-gcp",
    "pangea-hcloud",
    "pangea-porkbun",
    "pangea-splunk",
    "pangea-kubernetes",
    "pangea-grafana",
    "pangea-spot",
    "pangea-tailscale",
    "pangea-dashboards",
    "terraform-synthesizer",
    "abstract-synthesizer",
];

const FIXTURE_TOKEN: &str = "ghp_PARITY_FIXTURE_TOKEN_NOT_A_REAL_SECRET";

pub fn corpus_config(workspace_root: &Path) -> RenderConfig {
    RenderConfig {
        load_path: GEMS
            .iter()
            .map(|g| workspace_root.join(g).join("lib"))
            .collect(),
        env: vec![
            ("HOME".into(), "/nonexistent-lava-home".into()),
            ("GITHUB_PLEME_IO_PAT".into(), FIXTURE_TOKEN.into()),
            ("GITHUB_TOKEN".into(), FIXTURE_TOKEN.into()),
            ("PANGEA_GITHUB_SKIP_LIVE_VISIBILITY".into(), "1".into()),
        ],
        cwd: None,
    }
}

#[test]
fn pinned_goldens_are_byte_identical() {
    let Some(root) = std::env::var_os("LAVA_RUBY_CORPUS_ROOT").map(PathBuf::from) else {
        eprintln!("BLIND: LAVA_RUBY_CORPUS_ROOT unset; the parity matrix needs the pleme-io checkout that holds pangea-architectures");
        return;
    };
    let arch = root.join("pangea-architectures");
    let goldens = arch.join("spec/parity/goldens");
    let cases: Vec<(String, PathBuf)> = std::fs::read_dir(&goldens)
        .expect("goldens dir")
        .flatten()
        .filter_map(|e| {
            let name = e
                .file_name()
                .to_string_lossy()
                .strip_suffix(".tf.json")?
                .to_string();
            let tpl = if name == "pleme_io_opensource" {
                arch.join("workspaces/pleme-io-opensource/pleme_io_opensource.rb")
            } else {
                arch.join("spec/fixtures/parity").join(format!("{name}.rb"))
            };
            Some((name, tpl))
        })
        .filter(|(n, _)| {
            std::env::var("LAVA_RUBY_PARITY_FULL").is_ok() || n != "pleme_io_opensource"
        })
        .collect();
    assert!(
        !cases.is_empty(),
        "no goldens found under {}",
        goldens.display()
    );
    let cfg = corpus_config(&root);
    let mut failures = vec![];
    for (name, tpl) in &cases {
        let want = std::fs::read_to_string(goldens.join(format!("{name}.tf.json"))).unwrap();
        match render_pangea_template(tpl, &cfg) {
            Ok(got) if got == want => {}
            Ok(got) => {
                let line = got
                    .lines()
                    .zip(want.lines())
                    .position(|(a, b)| a != b)
                    .map(|i| i + 1)
                    .unwrap_or(0);
                failures.push(format!(
                    "{name}: bytes differ (first divergent line {line}; got {} B, want {} B)",
                    got.len(),
                    want.len()
                ));
            }
            Err(e) => failures.push(format!("{name}: {e}")),
        }
    }
    eprintln!(
        "parity: {}/{} byte-identical",
        cases.len() - failures.len(),
        cases.len()
    );
    assert!(
        failures.is_empty(),
        "parity regressions:\n{}",
        failures.join("\n")
    );
}
