use serde::{Deserialize, Serialize};

use crate::org_resolve::OrgCatalogue;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkPrApproval {
    FirstTimeContributorsNewToGithub,
    FirstTimeContributors,
    AllExternalContributors,
}

impl ForkPrApproval {
    #[must_use]
    pub fn as_api(self) -> &'static str {
        match self {
            Self::FirstTimeContributorsNewToGithub => "first_time_contributors_new_to_github",
            Self::FirstTimeContributors => "first_time_contributors",
            Self::AllExternalContributors => "all_external_contributors",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SettingAction {
    Unchanged,
    Updated,
    WouldUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SettingOutcome {
    pub repo: String,
    pub setting: &'static str,
    pub declared: String,
    pub before: String,
    pub after: String,
    pub action: SettingAction,
}

pub fn declared_fork_pr_approvals(
    catalogue: &OrgCatalogue,
    only: Option<&[String]>,
) -> Result<Vec<(String, ForkPrApproval)>, String> {
    Ok(catalogue
        .resolved_rows()?
        .into_iter()
        .filter(|(row, _)| only.is_none_or(|o| o.iter().any(|n| n == &row.name)))
        .filter_map(|(row, _)| row.overlay.fork_pr_approval.map(|p| (row.name, p)))
        .collect())
}

#[must_use]
pub fn plan(declared: ForkPrApproval, observed: &str, dry_run: bool) -> SettingAction {
    if observed == declared.as_api() {
        SettingAction::Unchanged
    } else if dry_run {
        SettingAction::WouldUpdate
    } else {
        SettingAction::Updated
    }
}

fn endpoint(owner: &str, repo: &str) -> String {
    format!("https://api.github.com/repos/{owner}/{repo}/actions/permissions/fork-pr-contributor-approval")
}

fn request(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    token: &str,
) -> reqwest::RequestBuilder {
    client
        .request(method, url)
        .header("User-Agent", "pangea-operator-org-settings")
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .bearer_auth(token)
}

pub async fn get_fork_pr_approval(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    token: &str,
) -> Result<String, String> {
    let url = endpoint(owner, repo);
    let resp = request(client, reqwest::Method::GET, &url, token)
        .send()
        .await
        .map_err(|e| format!("GET {url}: {e}"))?;
    let status = resp.status().as_u16();
    if status != 200 {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("GET {url}: decoding body: {e}"))?;
    body.get("approval_policy")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("GET {url}: no approval_policy in the response"))
}

pub async fn put_fork_pr_approval(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    token: &str,
    policy: ForkPrApproval,
) -> Result<(), String> {
    let url = endpoint(owner, repo);
    let resp = request(client, reqwest::Method::PUT, &url, token)
        .json(&serde_json::json!({ "approval_policy": policy.as_api() }))
        .send()
        .await
        .map_err(|e| format!("PUT {url}: {e}"))?;
    match resp.status().as_u16() {
        200 | 204 => Ok(()),
        other => Err(format!(
            "PUT {url}: HTTP {other} (the credential needs repository administration: write)"
        )),
    }
}

pub async fn reconcile_fork_pr_approval(
    catalogue: &OrgCatalogue,
    owner: &str,
    token: &str,
    only: Option<&[String]>,
    dry_run: bool,
) -> Result<Vec<SettingOutcome>, String> {
    let client = reqwest::Client::new();
    let mut out = Vec::new();
    for (repo, declared) in declared_fork_pr_approvals(catalogue, only)? {
        let before = get_fork_pr_approval(&client, owner, &repo, token).await?;
        let action = plan(declared, &before, dry_run);
        let after = if action == SettingAction::Updated {
            put_fork_pr_approval(&client, owner, &repo, token, declared).await?;
            let observed = get_fork_pr_approval(&client, owner, &repo, token).await?;
            if observed != declared.as_api() {
                return Err(format!(
                    "{owner}/{repo}: PUT approval_policy={} succeeded but GET reads {observed}",
                    declared.as_api()
                ));
            }
            observed
        } else {
            before.clone()
        };
        out.push(SettingOutcome {
            repo,
            setting: "fork_pr_approval",
            declared: declared.as_api().to_string(),
            before,
            after,
            action,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn catalogue(yaml: &str) -> OrgCatalogue {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn the_policy_parses_as_the_api_spells_it_and_refuses_anything_else() {
        let c = catalogue(
            "repos:\n  - name: helmworks\n    fork_pr_approval: all_external_contributors\n  - name: other\n",
        );
        assert_eq!(
            declared_fork_pr_approvals(&c, None).unwrap(),
            vec![(
                "helmworks".to_string(),
                ForkPrApproval::AllExternalContributors
            )]
        );
        assert!(serde_yaml::from_str::<OrgCatalogue>(
            "repos:\n  - name: x\n    fork_pr_approval: everybody\n"
        )
        .is_err());
    }

    #[test]
    fn a_workspace_default_reaches_every_row_and_a_row_overrides_it() {
        let c = catalogue(
            "repo_defaults:\n  fork_pr_approval: first_time_contributors\nrepos:\n  - name: a\n  - name: b\n    fork_pr_approval: all_external_contributors\n",
        );
        assert_eq!(
            declared_fork_pr_approvals(&c, None).unwrap(),
            vec![
                ("a".to_string(), ForkPrApproval::FirstTimeContributors),
                ("b".to_string(), ForkPrApproval::AllExternalContributors),
            ]
        );
        let only = vec!["b".to_string()];
        assert_eq!(
            declared_fork_pr_approvals(&c, Some(&only)).unwrap().len(),
            1
        );
    }

    #[test]
    fn plan_writes_only_on_a_difference_and_never_on_a_dry_run() {
        let p = ForkPrApproval::AllExternalContributors;
        assert_eq!(
            plan(p, "all_external_contributors", false),
            SettingAction::Unchanged
        );
        assert_eq!(
            plan(p, "first_time_contributors", false),
            SettingAction::Updated
        );
        assert_eq!(
            plan(p, "first_time_contributors", true),
            SettingAction::WouldUpdate
        );
    }

    #[test]
    fn every_policy_round_trips_through_its_api_spelling() {
        for p in [
            ForkPrApproval::FirstTimeContributorsNewToGithub,
            ForkPrApproval::FirstTimeContributors,
            ForkPrApproval::AllExternalContributors,
        ] {
            let parsed: ForkPrApproval =
                serde_json::from_value(serde_json::json!(p.as_api())).unwrap();
            assert_eq!(parsed, p);
        }
    }
}
