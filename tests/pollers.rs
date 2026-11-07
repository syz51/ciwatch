//! Pollers against local mock APIs that serve recorded-shape responses.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use chrono::{Duration, Utc};
use serde_json::{Value, json};

use ciwatch::config::{CircleciConfig, GithubPoll};
use ciwatch::model::{Provider, State as JobState};
use ciwatch::pools::{PoolRule, Pools};
use ciwatch::store::Store;
use ciwatch::{circleci, github, http_client};

async fn serve(app: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

#[derive(Clone, Default)]
struct Hits(Arc<Mutex<HashMap<String, usize>>>);

impl Hits {
    fn hit(&self, path: &str) {
        *self.0.lock().unwrap().entry(path.to_owned()).or_default() += 1;
    }
    fn get(&self, path: &str) -> usize {
        self.0.lock().unwrap().get(path).copied().unwrap_or(0)
    }
}

#[derive(Clone)]
struct CircleMock {
    hits: Hits,
    pipelines_page1: Value,
    pipelines_page2: Value,
}

async fn circle_route(
    State(mock): State<CircleMock>,
    Path(rest): Path<String>,
    Query(query): Query<HashMap<String, String>>,
    headers: HeaderMap,
) -> Response {
    if headers.get("circle-token").and_then(|v| v.to_str().ok()) != Some("cc-token") {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    mock.hits.hit(&rest);
    let body = match rest.as_str() {
        "project/gh/syz51/vps/pipeline" => match query.get("page-token").map(String::as_str) {
            None => mock.pipelines_page1.clone(),
            Some("page2") => mock.pipelines_page2.clone(),
            Some(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        "pipeline/p1/workflow" => json!({ "items": [
            { "id": "w1", "name": "pr-checks", "status": "running", "created_at": "2026-11-07T10:00:00Z" },
            { "id": "w2", "name": "lint", "status": "success", "created_at": "2026-11-07T10:00:00Z" },
        ], "next_page_token": null }),
        // Two pages of jobs for the running workflow.
        "workflow/w1/job" => match query.get("page-token").map(String::as_str) {
            None => json!({ "items": [
                { "id": "j-run", "name": "rust-controllers", "status": "running", "job_number": 10,
                  "started_at": "2026-11-07T10:01:00Z", "stopped_at": null },
                { "id": "j-queue", "name": "validate", "status": "queued", "job_number": 11,
                  "started_at": null, "stopped_at": null },
            ], "next_page_token": "jobs2" }),
            Some("jobs2") => json!({ "items": [
                { "name": "deploy", "status": "blocked", "started_at": null, "stopped_at": null },
            ], "next_page_token": null }),
            Some(_) => return StatusCode::BAD_REQUEST.into_response(),
        },
        "workflow/w2/job" => json!({ "items": [
            { "id": "j-done", "name": "fmt", "status": "success", "job_number": 9,
              "started_at": "2026-11-07T10:00:10Z", "stopped_at": "2026-11-07T10:00:40Z" },
        ], "next_page_token": null }),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    axum::Json(body).into_response()
}

#[tokio::test]
async fn circleci_poll_maps_live_and_finished_jobs() {
    let now = Utc::now();
    let mock = CircleMock {
        hits: Hits::default(),
        pipelines_page1: json!({ "items": [{
            "id": "p1", "number": 42, "state": "created",
            "created_at": (now - Duration::minutes(10)).to_rfc3339(),
            "vcs": { "branch": "feature", "revision": "abc123def" }
        }], "next_page_token": "page2" }),
        // Older than the lookback window: polling stops here.
        pipelines_page2: json!({ "items": [{
            "id": "p-old", "number": 41, "state": "created",
            "created_at": (now - Duration::hours(10)).to_rfc3339(),
            "vcs": { "branch": "main", "revision": "fff" }
        }], "next_page_token": "page3" }),
    };
    let hits = mock.hits.clone();
    let base = serve(
        Router::new()
            .route("/{*rest}", get(circle_route))
            .with_state(mock),
    )
    .await;

    let store = Arc::new(Store::new(Duration::hours(24)));
    let pools = Arc::new(Pools::new(vec![PoolRule {
        name: "circleci-linux".into(),
        provider: Some(Provider::Circleci),
        repository: Some("syz51/vps".into()),
        labels_any: Vec::new(),
        runner_prefix: None,
        runner_group: None,
        job_prefix: Some("rust".into()),
    }]));
    let poller = circleci::Poller::new(
        http_client().unwrap(),
        CircleciConfig {
            token: "cc-token".into(),
            projects: vec!["gh/syz51/vps".into()],
            interval: std::time::Duration::from_secs(30),
            lookback: Duration::hours(6),
            api_url: base,
            app_url: "https://app.circleci.com".into(),
        },
        store.clone(),
        pools,
    );

    poller.poll_once().await.unwrap();
    let jobs: HashMap<String, _> = store
        .snapshot()
        .into_iter()
        .map(|j| (j.name.clone(), j))
        .collect();
    assert_eq!(jobs.len(), 4, "{jobs:?}");

    let running = &jobs["rust-controllers"];
    assert_eq!(running.state, JobState::Running);
    assert_eq!(running.provider, Provider::Circleci);
    assert_eq!(running.repository, "syz51/vps");
    assert_eq!(running.pool, "circleci-linux");
    assert_eq!(running.branch.as_deref(), Some("feature"));
    assert_eq!(running.revision.as_deref(), Some("abc123def"));
    assert_eq!(
        running.url.as_deref(),
        Some("https://app.circleci.com/pipelines/github/syz51/vps/42/workflows/w1/jobs/10")
    );

    assert_eq!(jobs["validate"].state, JobState::Queued);
    assert_eq!(jobs["validate"].pool, "circleci");
    let blocked = &jobs["deploy"];
    assert_eq!(blocked.state, JobState::Waiting);
    assert_eq!(blocked.id, "w1/deploy");
    assert_eq!(
        blocked.url.as_deref(),
        Some("https://app.circleci.com/pipelines/github/syz51/vps/42/workflows/w1")
    );
    let done = &jobs["fmt"];
    assert_eq!(done.state, JobState::Success);
    assert_eq!(
        done.finished_at.unwrap().to_rfc3339(),
        "2026-11-07T10:00:40+00:00"
    );

    // The old page was read once and the page after it never.
    assert_eq!(hits.get("project/gh/syz51/vps/pipeline"), 2);

    // A second poll re-reads the running workflow but not the finished one.
    poller.poll_once().await.unwrap();
    assert_eq!(hits.get("workflow/w1/job"), 4);
    assert_eq!(hits.get("workflow/w2/job"), 1);
}

#[tokio::test]
async fn circleci_poll_reports_auth_failure() {
    let mock = CircleMock {
        hits: Hits::default(),
        pipelines_page1: json!({ "items": [], "next_page_token": null }),
        pipelines_page2: json!({ "items": [], "next_page_token": null }),
    };
    let base = serve(
        Router::new()
            .route("/{*rest}", get(circle_route))
            .with_state(mock),
    )
    .await;
    let poller = circleci::Poller::new(
        http_client().unwrap(),
        CircleciConfig {
            token: "wrong".into(),
            projects: vec!["gh/syz51/vps".into()],
            interval: std::time::Duration::from_secs(30),
            lookback: Duration::hours(6),
            api_url: base,
            app_url: "https://app.circleci.com".into(),
        },
        Arc::new(Store::new(Duration::hours(1))),
        Arc::new(Pools::default()),
    );
    let err = format!("{:#}", poller.poll_once().await.unwrap_err());
    assert!(err.contains("401"), "{err}");
}

fn gh_job(id: u64, run: u64, status: &str, conclusion: Option<&str>, runner: Option<&str>) -> Value {
    json!({
        "id": id, "run_id": run, "name": format!("job-{id}"), "workflow_name": "CI",
        "head_branch": "main", "head_sha": "abc", "status": status, "conclusion": conclusion,
        "created_at": "2026-11-07T10:00:00Z", "started_at": "2026-11-07T10:01:00Z",
        "completed_at": conclusion.map(|_| "2026-11-07T10:03:00Z"),
        "html_url": format!("https://github.com/o/r/actions/runs/{run}/job/{id}"),
        "labels": ["ubuntu-latest"], "runner_name": runner,
        "runner_group_name": runner.map(|_| "GitHub Actions"),
    })
}

#[tokio::test]
async fn github_poll_refreshes_listed_and_held_runs() {
    let hits = Hits::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let app = Router::new()
        .route(
            "/repos/o/r/actions/runs",
            get(
                |Query(q): Query<HashMap<String, String>>, headers: HeaderMap| async move {
                    assert_eq!(headers["authorization"], "Bearer gh-token");
                    let runs = if q.get("status").map(String::as_str) == Some("in_progress") {
                        json!([{ "id": 55 }])
                    } else {
                        json!([])
                    };
                    axum::Json(json!({ "total_count": 0, "workflow_runs": runs }))
                },
            ),
        )
        .route(
            "/repos/o/r/actions/runs/{run}/jobs",
            get({
                let hits = hits.clone();
                let calls = calls.clone();
                move |Path(run): Path<u64>| async move {
                    hits.hit(&run.to_string());
                    calls.fetch_add(1, Ordering::SeqCst);
                    let jobs = match run {
                        55 => json!([gh_job(1, 55, "in_progress", None, Some("GitHub Actions 3"))]),
                        // The completion webhook for this run was missed.
                        44 => json!([gh_job(
                            2,
                            44,
                            "completed",
                            Some("failure"),
                            Some("GitHub Actions 9")
                        )]),
                        _ => json!([]),
                    };
                    axum::Json(json!({ "total_count": 1, "jobs": jobs }))
                }
            }),
        );
    let base = serve(app).await;

    let store = Arc::new(Store::new(Duration::hours(1)));
    let pools = Arc::new(Pools::default());
    let stale: github::GhJob =
        serde_json::from_value(gh_job(2, 44, "in_progress", None, Some("GitHub Actions 9"))).unwrap();
    store.upsert(github::to_job(stale, "o/r", &pools, Utc::now()));

    let poller = github::Poller::new(
        http_client().unwrap(),
        GithubPoll {
            token: "gh-token".into(),
            repositories: vec!["o/r".into()],
            interval: std::time::Duration::from_secs(60),
            api_url: base,
        },
        store.clone(),
        pools,
    );
    poller.poll_once().await.unwrap();

    assert_eq!(hits.get("55"), 1);
    assert_eq!(hits.get("44"), 1);
    let jobs: HashMap<String, _> = store.snapshot().into_iter().map(|j| (j.id.clone(), j)).collect();
    assert_eq!(jobs["1"].state, JobState::Running);
    assert_eq!(jobs["1"].pool, "github-hosted");
    assert_eq!(jobs["2"].state, JobState::Failure);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
