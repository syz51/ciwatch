//! GitHub Actions: `workflow_job` webhooks and an optional reconcile poll.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use hmac::{Hmac, KeyInit, Mac};
use reqwest::StatusCode;
use serde::Deserialize;
use sha2::Sha256;

use crate::config::GithubPoll;
use crate::model::{Job, PoolFacts, Provider, State};
use crate::poll::{Failures, Source, fetch_json, status_of};
use crate::pools::Pools;
use crate::store::Store;

pub const SOURCE_POLL: &str = "github-poll";
pub const SOURCE_WEBHOOK: &str = "github-webhook";

/// Check an `X-Hub-Signature-256` header against the raw body.
pub fn verify_signature(secret: &[u8], body: &[u8], header: Option<&str>) -> bool {
    let Some(hex_sig) = header.and_then(|h| h.strip_prefix("sha256=")) else {
        return false;
    };
    let Ok(sig) = hex::decode(hex_sig) else {
        return false;
    };
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC accepts any key length");
    mac.update(body);
    mac.verify_slice(&sig).is_ok()
}

#[derive(Debug, Deserialize)]
pub struct WorkflowJobEvent {
    pub workflow_job: GhJob,
    pub repository: GhRepository,
}

#[derive(Debug, Deserialize)]
pub struct GhRepository {
    pub full_name: String,
}

/// A job as both the webhook and `GET /actions/runs/{id}/jobs` report it.
#[derive(Debug, Deserialize)]
pub struct GhJob {
    pub id: u64,
    pub run_id: u64,
    pub name: String,
    pub workflow_name: Option<String>,
    pub head_branch: Option<String>,
    pub head_sha: Option<String>,
    pub status: String,
    pub conclusion: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub html_url: Option<String>,
    #[serde(default)]
    pub labels: Vec<String>,
    pub runner_name: Option<String>,
    pub runner_group_name: Option<String>,
}

fn state_of(status: &str, conclusion: Option<&str>) -> State {
    match status {
        "queued" | "requested" | "pending" => State::Queued,
        "waiting" => State::Waiting,
        "in_progress" => State::Running,
        _ => match conclusion {
            Some("success" | "neutral") => State::Success,
            Some("cancelled") => State::Cancelled,
            Some("skipped") => State::Skipped,
            _ => State::Failure,
        },
    }
}

pub fn to_job(job: GhJob, repository: &str, pools: &Pools, now: DateTime<Utc>) -> Job {
    let state = state_of(&job.status, job.conclusion.as_deref());
    // GitHub reports an empty runner name for jobs that have not started.
    let runner = job.runner_name.filter(|r| !r.is_empty());
    let runner_group = job.runner_group_name.filter(|g| !g.is_empty());
    let pool = pools.classify(&PoolFacts {
        provider: Provider::Github,
        repository,
        name: &job.name,
        labels: &job.labels,
        runner: runner.as_deref(),
        runner_group: runner_group.as_deref(),
    });
    Job {
        provider: Provider::Github,
        id: job.id.to_string(),
        repository: repository.to_owned(),
        pipeline: job.workflow_name.unwrap_or_default(),
        run_id: job.run_id.to_string(),
        name: job.name,
        provider_status: job.conclusion.unwrap_or(job.status),
        state,
        branch: job.head_branch,
        revision: job.head_sha,
        pool,
        runner,
        labels: job.labels,
        url: job.html_url,
        created_at: job.created_at,
        // GitHub fills started_at with the queue time before a job starts.
        started_at: job.started_at.filter(|_| state.progress() > 0),
        finished_at: job.completed_at.filter(|_| !state.is_active()),
        observed_at: now,
    }
}

#[derive(Debug, Deserialize)]
struct RunList {
    workflow_runs: Vec<RunRef>,
}

#[derive(Debug, Deserialize)]
struct RunRef {
    id: u64,
}

#[derive(Debug, Deserialize)]
struct JobList {
    jobs: Vec<GhJob>,
}

pub struct Poller {
    client: reqwest::Client,
    config: GithubPoll,
    store: Arc<Store>,
    pools: Arc<Pools>,
}

impl Poller {
    pub fn new(client: reqwest::Client, config: GithubPoll, store: Arc<Store>, pools: Arc<Pools>) -> Self {
        Self {
            client,
            config,
            store,
            pools,
        }
    }

    /// Refresh one repository's active runs: those GitHub lists as not yet
    /// finished, and those ciwatch still holds as active, so a missed
    /// completion webhook is repaired.
    async fn poll_repository(&self, repo: &str, failures: &mut Failures) {
        let mut runs: HashSet<String> = self.store.active_run_ids(Provider::Github, repo);
        for status in RUN_STATUSES {
            let path = format!("/repos/{repo}/actions/runs");
            match self
                .get::<RunList>(&path, &[("status", status), ("per_page", "100")])
                .await
            {
                Ok(list) => runs.extend(list.workflow_runs.into_iter().map(|r| r.id.to_string())),
                Err(e) => failures.add(e),
            }
        }
        for run in runs {
            // `filter=all` includes earlier attempts, whose jobs may also be
            // held as active.
            let path = format!("/repos/{repo}/actions/runs/{run}/jobs");
            match self
                .get::<JobList>(&path, &[("filter", "all"), ("per_page", "100")])
                .await
            {
                Ok(list) => {
                    let now = Utc::now();
                    for job in list.jobs {
                        self.store.upsert(to_job(job, repo, &self.pools, now));
                    }
                }
                // The run was deleted or its repository moved: nothing will
                // ever finish it, so stop showing its jobs.
                Err(e) if status_of(&e) == Some(StatusCode::NOT_FOUND) => {
                    tracing::info!(repo, run, "run is gone; dropping its jobs");
                    self.store.remove_run(Provider::Github, repo, &run);
                }
                Err(e) => failures.add(e),
            }
        }
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str, query: &[(&str, &str)]) -> Result<T> {
        let request = self
            .client
            .get(format!("{}{path}", self.config.api_url))
            .query(query)
            .bearer_auth(&self.config.token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28");
        fetch_json(request, path).await
    }
}

/// Run statuses that mean "not finished" in `GET /actions/runs?status=`.
const RUN_STATUSES: [&str; 5] = ["requested", "pending", "queued", "waiting", "in_progress"];

impl Source for Poller {
    fn name(&self) -> &'static str {
        SOURCE_POLL
    }

    fn interval(&self) -> std::time::Duration {
        self.config.interval
    }

    fn store(&self) -> &Store {
        &self.store
    }

    async fn poll_once(&self) -> Result<()> {
        let mut failures = Failures::default();
        for repo in &self.config.repositories {
            self.poll_repository(repo, &mut failures).await;
        }
        failures.into_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &[u8], body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn signature_checks() {
        let body = br#"{"zen":"hi"}"#;
        let good = sign(b"secret", body);
        assert!(verify_signature(b"secret", body, Some(&good)));
        assert!(!verify_signature(b"other", body, Some(&good)));
        assert!(!verify_signature(b"secret", b"{}", Some(&good)));
        assert!(!verify_signature(b"secret", body, None));
        assert!(!verify_signature(b"secret", body, Some("sha1=abcd")));
        assert!(!verify_signature(b"secret", body, Some("sha256=zz")));
    }

    #[test]
    fn maps_states() {
        assert_eq!(state_of("queued", None), State::Queued);
        assert_eq!(state_of("waiting", None), State::Waiting);
        assert_eq!(state_of("in_progress", None), State::Running);
        assert_eq!(state_of("completed", Some("success")), State::Success);
        assert_eq!(state_of("completed", Some("timed_out")), State::Failure);
        assert_eq!(state_of("completed", Some("cancelled")), State::Cancelled);
        assert_eq!(state_of("completed", Some("skipped")), State::Skipped);
    }

    #[test]
    fn queued_job_has_no_start_or_runner() {
        let job: GhJob = serde_json::from_value(serde_json::json!({
            "id": 7, "run_id": 3, "name": "build", "workflow_name": "CI",
            "head_branch": "main", "head_sha": "abc", "status": "queued", "conclusion": null,
            "created_at": "2026-11-07T10:00:00Z", "started_at": "2026-11-07T10:00:00Z",
            "completed_at": null, "html_url": "https://github.com/o/r/actions/runs/3/job/7",
            "labels": ["sai-ephemeral"], "runner_name": "", "runner_group_name": ""
        }))
        .unwrap();
        let job = to_job(job, "o/r", &Pools::default(), Utc::now());
        assert_eq!(job.state, State::Queued);
        assert_eq!(job.started_at, None);
        assert_eq!(job.runner, None);
        assert_eq!(job.pool, "unassigned");
    }
}
