//! CircleCI: polls API v2, since CircleCI sends webhooks only for finished
//! work.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::config::CircleciConfig;
use crate::model::{Job, PoolFacts, Provider, State};
use crate::pools::Pools;
use crate::store::Store;

pub const SOURCE: &str = "circleci-poll";

/// Pages of pipelines read per project and poll, at most.
const MAX_PIPELINE_PAGES: usize = 5;
/// Pages of workflows or jobs read per parent, at most.
const MAX_CHILD_PAGES: usize = 10;

#[derive(Debug, Deserialize)]
struct Page<T> {
    items: Vec<T>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Pipeline {
    id: String,
    number: u64,
    created_at: DateTime<Utc>,
    #[serde(default)]
    vcs: Option<Vcs>,
}

#[derive(Debug, Default, Deserialize)]
struct Vcs {
    branch: Option<String>,
    revision: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Workflow {
    id: String,
    name: String,
    status: String,
    created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct CircleJob {
    id: Option<String>,
    name: String,
    status: String,
    job_number: Option<u64>,
    started_at: Option<DateTime<Utc>>,
    stopped_at: Option<DateTime<Utc>>,
}

fn job_state(status: &str) -> State {
    match status {
        "queued" | "not_running" => State::Queued,
        "blocked" | "on_hold" => State::Waiting,
        "running" => State::Running,
        "success" => State::Success,
        "canceled" => State::Cancelled,
        "not_run" | "retried" => State::Skipped,
        // failed, infrastructure_fail, timedout, terminated-unknown,
        // unauthorized and anything new.
        _ => State::Failure,
    }
}

fn workflow_finished(status: &str) -> bool {
    !matches!(status, "running" | "on_hold" | "failing")
}

/// `gh/org/repo` becomes `org/repo`; other slugs are kept whole.
pub fn repository_of(slug: &str) -> String {
    match slug.split_once('/') {
        Some(("gh" | "github" | "bb" | "bitbucket", rest)) => rest.to_owned(),
        _ => slug.to_owned(),
    }
}

/// The web app's path for a project slug.
fn app_slug(slug: &str) -> String {
    match slug.split_once('/') {
        Some(("gh", rest)) => format!("github/{rest}"),
        Some(("bb", rest)) => format!("bitbucket/{rest}"),
        _ => slug.to_owned(),
    }
}

pub struct Poller {
    client: reqwest::Client,
    config: CircleciConfig,
    store: Arc<Store>,
    pools: Arc<Pools>,
    /// Workflows already recorded as finished, with when they were settled.
    settled: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl Poller {
    pub fn new(
        client: reqwest::Client,
        config: CircleciConfig,
        store: Arc<Store>,
        pools: Arc<Pools>,
    ) -> Self {
        Self {
            client,
            config,
            store,
            pools,
            settled: Mutex::new(HashMap::new()),
        }
    }

    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.config.interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            match self.poll_once().await {
                Ok(()) => self.store.record_success(SOURCE, Utc::now()),
                Err(e) => {
                    tracing::warn!(error = format!("{e:#}"), "CircleCI poll failed");
                    self.store.record_error(SOURCE, Utc::now(), format!("{e:#}"));
                }
            }
        }
    }

    pub async fn poll_once(&self) -> Result<()> {
        let now = Utc::now();
        let since = now - self.config.lookback;
        self.settled().retain(|_, at| *at >= since);
        for slug in &self.config.projects {
            for pipeline in self.recent_pipelines(slug, since).await? {
                let workflows: Vec<Workflow> = self
                    .get_all(&format!("/pipeline/{}/workflow", pipeline.id))
                    .await?;
                for workflow in workflows {
                    if self.settled().contains_key(&workflow.id) {
                        continue;
                    }
                    let jobs: Vec<CircleJob> =
                        self.get_all(&format!("/workflow/{}/job", workflow.id)).await?;
                    let mut all_finished = workflow_finished(&workflow.status);
                    let observed = Utc::now();
                    for job in jobs {
                        let job = self.to_job(slug, &pipeline, &workflow, job, observed);
                        all_finished &= !job.state.is_active();
                        self.store.upsert(job);
                    }
                    if all_finished {
                        self.settled().insert(workflow.id, observed);
                    }
                }
            }
        }
        Ok(())
    }

    async fn recent_pipelines(&self, slug: &str, since: DateTime<Utc>) -> Result<Vec<Pipeline>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_PIPELINE_PAGES {
            let page: Page<Pipeline> = self
                .get(&format!("/project/{slug}/pipeline"), token.as_deref())
                .await?;
            let mut reached_old = false;
            for pipeline in page.items {
                if pipeline.created_at < since {
                    reached_old = true;
                } else {
                    out.push(pipeline);
                }
            }
            match page.next_page_token {
                Some(next) if !reached_old => token = Some(next),
                _ => break,
            }
        }
        Ok(out)
    }

    fn to_job(
        &self,
        slug: &str,
        pipeline: &Pipeline,
        workflow: &Workflow,
        job: CircleJob,
        now: DateTime<Utc>,
    ) -> Job {
        let repository = repository_of(slug);
        let state = job_state(&job.status);
        let pool = self.pools.classify(&PoolFacts {
            provider: Provider::Circleci,
            repository: &repository,
            name: &job.name,
            labels: &[],
            runner: None,
            runner_group: None,
        });
        let workflow_url = format!(
            "{}/pipelines/{}/{}/workflows/{}",
            self.config.app_url,
            app_slug(slug),
            pipeline.number,
            workflow.id
        );
        let url = match job.job_number {
            Some(n) => format!("{workflow_url}/jobs/{n}"),
            None => workflow_url,
        };
        let vcs = pipeline.vcs.as_ref();
        Job {
            provider: Provider::Circleci,
            // Jobs that have not started yet may have no id.
            id: job.id.unwrap_or_else(|| format!("{}/{}", workflow.id, job.name)),
            repository,
            pipeline: workflow.name.clone(),
            run_id: workflow.id.clone(),
            name: job.name,
            state,
            provider_status: job.status,
            branch: vcs.and_then(|v| v.branch.clone()),
            revision: vcs.and_then(|v| v.revision.clone()),
            pool,
            runner: None,
            labels: Vec::new(),
            url: Some(url),
            created_at: workflow.created_at.or(Some(pipeline.created_at)),
            started_at: job.started_at,
            finished_at: job.stopped_at.filter(|_| !state.is_active()),
            observed_at: now,
        }
    }

    fn settled(&self) -> std::sync::MutexGuard<'_, HashMap<String, DateTime<Utc>>> {
        self.settled.lock().expect("settled lock poisoned")
    }

    async fn get_all<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<Vec<T>> {
        let mut out = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..MAX_CHILD_PAGES {
            let page: Page<T> = self.get(path, token.as_deref()).await?;
            out.extend(page.items);
            match page.next_page_token {
                Some(next) => token = Some(next),
                None => break,
            }
        }
        Ok(out)
    }

    async fn get<T: serde::de::DeserializeOwned>(&self, path: &str, page_token: Option<&str>) -> Result<T> {
        let url = format!("{}{path}", self.config.api_url);
        let mut request = self.client.get(&url).header("Circle-Token", &self.config.token);
        if let Some(token) = page_token {
            request = request.query(&[("page-token", token)]);
        }
        request
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .with_context(|| format!("GET {path}"))?
            .json()
            .await
            .with_context(|| format!("decoding {path}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_job_states() {
        assert_eq!(job_state("queued"), State::Queued);
        assert_eq!(job_state("not_running"), State::Queued);
        assert_eq!(job_state("blocked"), State::Waiting);
        assert_eq!(job_state("on_hold"), State::Waiting);
        assert_eq!(job_state("running"), State::Running);
        assert_eq!(job_state("success"), State::Success);
        assert_eq!(job_state("infrastructure_fail"), State::Failure);
        assert_eq!(job_state("canceled"), State::Cancelled);
        assert_eq!(job_state("not_run"), State::Skipped);
    }

    #[test]
    fn slugs() {
        assert_eq!(repository_of("gh/syz51/vps"), "syz51/vps");
        assert_eq!(
            repository_of("circleci/org-id/project-id"),
            "circleci/org-id/project-id"
        );
        assert_eq!(app_slug("gh/syz51/vps"), "github/syz51/vps");
    }
}
