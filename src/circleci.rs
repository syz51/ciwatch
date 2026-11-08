//! CircleCI: polls API v2, since CircleCI sends webhooks only for finished
//! work.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use serde::Deserialize;

use crate::config::CircleciConfig;
use crate::model::{Job, PoolFacts, Provider, State};
use crate::poll::{Failures, Source, fetch_json, status_of};
use crate::pools::Pools;
use crate::store::Store;

pub const SOURCE: &str = "circleci-poll";

/// Pages of pipelines read per project and poll, at most.
const MAX_PIPELINE_PAGES: usize = 5;
/// Pages of workflows or jobs read per parent, at most.
const MAX_CHILD_PAGES: usize = 10;
/// How long after a pipeline is created a 404 for its workflows means they
/// do not exist yet. CircleCI creates an API-triggered pipeline before its
/// workflows; after this grace the 404 is an error.
const NEW_PIPELINE_GRACE: Duration = Duration::minutes(5);

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
    pipeline_number: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct CircleJob {
    name: String,
    status: String,
    job_number: Option<u64>,
    started_at: Option<DateTime<Utc>>,
    stopped_at: Option<DateTime<Utc>>,
}

/// What a workflow's jobs inherit from their pipeline.
struct RunContext<'a> {
    slug: &'a str,
    pipeline_number: Option<u64>,
    branch: Option<String>,
    revision: Option<String>,
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

    fn settled(&self) -> MutexGuard<'_, HashMap<String, DateTime<Utc>>> {
        self.settled.lock().expect("settled lock poisoned")
    }

    /// Refresh one project's recent pipelines. A failed pipeline is recorded
    /// in `failures` and does not stop the others.
    async fn poll_project(
        &self,
        slug: &str,
        since: DateTime<Utc>,
        seen: &mut HashSet<String>,
        failures: &mut Failures,
    ) -> Result<()> {
        for pipeline in self.recent_pipelines(slug, since).await? {
            let label = format!("project {slug} pipeline {} ({})", pipeline.number, pipeline.id);
            if let Err(e) = self.poll_pipeline(slug, pipeline, seen).await {
                failures.add(e.context(label));
            }
        }
        Ok(())
    }

    async fn poll_pipeline(&self, slug: &str, pipeline: Pipeline, seen: &mut HashSet<String>) -> Result<()> {
        let workflows: Vec<Workflow> =
            match self.get_all(&format!("/pipeline/{}/workflow", pipeline.id)).await {
                Ok(w) => w,
                Err(e)
                    if status_of(&e) == Some(StatusCode::NOT_FOUND)
                        && Utc::now() - pipeline.created_at < NEW_PIPELINE_GRACE =>
                {
                    tracing::debug!(slug, pipeline = %pipeline.id, "new pipeline has no workflows yet");
                    return Ok(());
                }
                Err(e) => return Err(e),
            };
        let vcs = pipeline.vcs.unwrap_or_default();
        for workflow in workflows {
            seen.insert(workflow.id.clone());
            if self.settled().contains_key(&workflow.id) {
                continue;
            }
            let context = RunContext {
                slug,
                pipeline_number: Some(pipeline.number),
                branch: vcs.branch.clone(),
                revision: vcs.revision.clone(),
            };
            self.refresh_workflow(&workflow, &context).await?;
        }
        Ok(())
    }

    /// Refresh workflows the store still holds as active but whose pipeline
    /// has left the lookback window, such as a long approval hold.
    async fn poll_held_workflows(&self, seen: &HashSet<String>, failures: &mut Failures) {
        for template in self.store.active_runs(Provider::Circleci) {
            if seen.contains(&template.run_id) {
                continue;
            }
            let Some(slug) = self
                .config
                .projects
                .iter()
                .find(|s| repository_of(s).eq_ignore_ascii_case(&template.repository))
            else {
                continue;
            };
            let path = format!("/workflow/{}", template.run_id);
            let workflow: Workflow = match self.get(&path, None).await {
                Ok(w) => w,
                Err(e) if status_of(&e) == Some(StatusCode::NOT_FOUND) => {
                    self.store
                        .remove_run(Provider::Circleci, &template.repository, &template.run_id);
                    continue;
                }
                Err(e) => {
                    failures.add(e);
                    continue;
                }
            };
            let context = RunContext {
                slug,
                pipeline_number: workflow.pipeline_number,
                branch: template.branch.clone(),
                revision: template.revision.clone(),
            };
            if let Err(e) = self.refresh_workflow(&workflow, &context).await {
                failures.add(e);
            }
        }
    }

    async fn refresh_workflow(&self, workflow: &Workflow, context: &RunContext<'_>) -> Result<()> {
        let jobs: Vec<CircleJob> = self.get_all(&format!("/workflow/{}/job", workflow.id)).await?;
        let finished = workflow_finished(&workflow.status);
        let observed = Utc::now();
        for job in jobs {
            self.store
                .upsert(self.to_job(context, workflow, job, finished, observed));
        }
        if finished {
            self.settled().insert(workflow.id.clone(), observed);
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
        context: &RunContext<'_>,
        workflow: &Workflow,
        job: CircleJob,
        workflow_finished: bool,
        now: DateTime<Utc>,
    ) -> Job {
        let repository = repository_of(context.slug);
        let mut state = job_state(&job.status);
        // A finished workflow will never start the jobs it left blocked or
        // on hold.
        if workflow_finished && state.is_active() {
            state = State::Skipped;
        }
        let pool = self.pools.classify(&PoolFacts {
            provider: Provider::Circleci,
            repository: &repository,
            name: &job.name,
            labels: &[],
            runner: None,
            runner_group: None,
        });
        let workflow_url = context.pipeline_number.map(|n| {
            format!(
                "{}/pipelines/{}/{}/workflows/{}",
                self.config.app_url,
                app_slug(context.slug),
                n,
                workflow.id
            )
        });
        let url = match (workflow_url, job.job_number) {
            (Some(w), Some(n)) => Some(format!("{w}/jobs/{n}")),
            (w, _) => w,
        };
        Job {
            provider: Provider::Circleci,
            // Job names are unique within a workflow, and unlike the job's
            // own id this key exists before the job starts.
            id: format!("{}/{}", workflow.id, job.name),
            repository,
            pipeline: workflow.name.clone(),
            run_id: workflow.id.clone(),
            name: job.name,
            state,
            provider_status: job.status,
            branch: context.branch.clone(),
            revision: context.revision.clone(),
            pool,
            runner: None,
            labels: Vec::new(),
            url,
            // The API gives no per-job queue time; the store keeps the time
            // ciwatch first saw the job.
            created_at: None,
            started_at: job.started_at,
            finished_at: job.stopped_at.filter(|_| !state.is_active()),
            observed_at: now,
        }
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
        let mut request = self
            .client
            .get(format!("{}{path}", self.config.api_url))
            .header("Circle-Token", &self.config.token);
        if let Some(token) = page_token {
            request = request.query(&[("page-token", token)]);
        }
        fetch_json(request, path).await
    }
}

impl Source for Poller {
    fn name(&self) -> &'static str {
        SOURCE
    }

    fn interval(&self) -> std::time::Duration {
        self.config.interval
    }

    fn store(&self) -> &Store {
        &self.store
    }

    async fn poll_once(&self) -> Result<()> {
        let since = Utc::now() - self.config.lookback;
        self.settled().retain(|_, at| *at >= since);
        let mut failures = Failures::default();
        let mut seen = HashSet::new();
        for slug in &self.config.projects {
            if let Err(e) = self.poll_project(slug, since, &mut seen, &mut failures).await {
                failures.add(e.context(format!("project {slug}")));
            }
        }
        self.poll_held_workflows(&seen, &mut failures).await;
        failures.into_result()
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
