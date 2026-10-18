use serde::{Deserialize, Serialize};

use crate::org_resolve::{OrgCatalogue, RepoOverlay};

pub trait RepoSetting: Copy + Eq {
    const NAME: &'static str;
    const PATH: &'static str;
    const FIELD: &'static str;
    fn as_api(self) -> &'static str;
    fn declared(overlay: &RepoOverlay) -> Option<Self>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ForkPrApproval {
    FirstTimeContributorsNewToGithub,
    FirstTimeContributors,
    AllExternalContributors,
}

impl RepoSetting for ForkPrApproval {
    const NAME: &'static str = "fork_pr_approval";
    const PATH: &'static str = "fork-pr-contributor-approval";
    const FIELD: &'static str = "approval_policy";

    fn as_api(self) -> &'static str {
        match self {
            Self::FirstTimeContributorsNewToGithub => "first_time_contributors_new_to_github",
            Self::FirstTimeContributors => "first_time_contributors",
            Self::AllExternalContributors => "all_external_contributors",
        }
    }

    fn declared(overlay: &RepoOverlay) -> Option<Self> {
        overlay.fork_pr_approval
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionsAccessLevel {
    None,
    User,
    Organization,
    Enterprise,
}

impl RepoSetting for ActionsAccessLevel {
    const NAME: &'static str = "actions_access_level";
    const PATH: &'static str = "access";
    const FIELD: &'static str = "access_level";

    fn as_api(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::User => "user",
            Self::Organization => "organization",
            Self::Enterprise => "enterprise",
        }
    }

    fn declared(overlay: &RepoOverlay) -> Option<Self> {
        overlay.actions_access_level
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

pub fn declared_settings<S: RepoSetting>(
    catalogue: &OrgCatalogue,
    only: Option<&[String]>,
) -> Result<Vec<(String, S)>, String> {
    Ok(catalogue
        .resolved_rows()?
        .into_iter()
        .filter(|(row, _)| only.is_none_or(|o| o.iter().any(|n| n == &row.name)))
        .filter_map(|(row, _)| S::declared(&row.overlay).map(|p| (row.name, p)))
        .collect())
}

#[must_use]
pub fn plan<S: RepoSetting>(declared: S, observed: &str, dry_run: bool) -> SettingAction {
    if observed == declared.as_api() {
        SettingAction::Unchanged
    } else if dry_run {
        SettingAction::WouldUpdate
    } else {
        SettingAction::Updated
    }
}

fn endpoint<S: RepoSetting>(owner: &str, repo: &str) -> String {
    format!(
        "https://api.github.com/repos/{owner}/{repo}/actions/permissions/{}",
        S::PATH
    )
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

pub async fn get_setting<S: RepoSetting>(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    token: &str,
) -> Result<String, String> {
    let url = endpoint::<S>(owner, repo);
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
    body.get(S::FIELD)
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("GET {url}: no {} in the response", S::FIELD))
}

pub async fn put_setting<S: RepoSetting>(
    client: &reqwest::Client,
    owner: &str,
    repo: &str,
    token: &str,
    value: S,
) -> Result<(), String> {
    let url = endpoint::<S>(owner, repo);
    let mut body = serde_json::Map::new();
    body.insert(S::FIELD.to_string(), serde_json::json!(value.as_api()));
    let resp = request(client, reqwest::Method::PUT, &url, token)
        .json(&serde_json::Value::Object(body))
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

pub async fn reconcile_setting<S: RepoSetting>(
    client: &reqwest::Client,
    catalogue: &OrgCatalogue,
    owner: &str,
    token: &str,
    only: Option<&[String]>,
    dry_run: bool,
) -> Result<Vec<SettingOutcome>, String> {
    let mut out = Vec::new();
    for (repo, declared) in declared_settings::<S>(catalogue, only)? {
        let before = get_setting::<S>(client, owner, &repo, token).await?;
        let action = plan(declared, &before, dry_run);
        let after = if action == SettingAction::Updated {
            put_setting(client, owner, &repo, token, declared).await?;
            let observed = get_setting::<S>(client, owner, &repo, token).await?;
            if observed != declared.as_api() {
                return Err(format!(
                    "{owner}/{repo}: PUT {}={} succeeded but GET reads {observed}",
                    S::FIELD,
                    declared.as_api()
                ));
            }
            observed
        } else {
            before.clone()
        };
        out.push(SettingOutcome {
            repo,
            setting: S::NAME,
            declared: declared.as_api().to_string(),
            before,
            after,
            action,
        });
    }
    Ok(out)
}

pub async fn reconcile_repo_settings(
    catalogue: &OrgCatalogue,
    owner: &str,
    token: &str,
    only: Option<&[String]>,
    dry_run: bool,
) -> Result<Vec<SettingOutcome>, String> {
    let client = reqwest::Client::new();
    let mut out =
        reconcile_setting::<ForkPrApproval>(&client, catalogue, owner, token, only, dry_run)
            .await?;
    out.extend(
        reconcile_setting::<ActionsAccessLevel>(&client, catalogue, owner, token, only, dry_run)
            .await?,
    );
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
            declared_settings::<ForkPrApproval>(&c, None).unwrap(),
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
            declared_settings::<ForkPrApproval>(&c, None).unwrap(),
            vec![
                ("a".to_string(), ForkPrApproval::FirstTimeContributors),
                ("b".to_string(), ForkPrApproval::AllExternalContributors),
            ]
        );
        let only = vec!["b".to_string()];
        assert_eq!(
            declared_settings::<ForkPrApproval>(&c, Some(&only))
                .unwrap()
                .len(),
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

    #[test]
    fn the_access_level_parses_as_the_api_spells_it_and_refuses_anything_else() {
        let c = catalogue(
            "repos:\n  - name: pleme-actions\n    actions_access_level: organization\n  - name: other\n",
        );
        assert_eq!(
            declared_settings::<ActionsAccessLevel>(&c, None).unwrap(),
            vec![(
                "pleme-actions".to_string(),
                ActionsAccessLevel::Organization
            )]
        );
        assert!(declared_settings::<ForkPrApproval>(&c, None)
            .unwrap()
            .is_empty());
        assert!(serde_yaml::from_str::<OrgCatalogue>(
            "repo_defaults:\n  actions_access_level: everyone\n"
        )
        .is_err());
    }

    #[test]
    fn every_access_level_round_trips_through_its_api_spelling() {
        for l in [
            ActionsAccessLevel::None,
            ActionsAccessLevel::User,
            ActionsAccessLevel::Organization,
            ActionsAccessLevel::Enterprise,
        ] {
            let parsed: ActionsAccessLevel =
                serde_json::from_value(serde_json::json!(l.as_api())).unwrap();
            assert_eq!(parsed, l);
        }
        assert_eq!(
            endpoint::<ActionsAccessLevel>("pleme-io", "pleme-actions"),
            "https://api.github.com/repos/pleme-io/pleme-actions/actions/permissions/access"
        );
    }
}
