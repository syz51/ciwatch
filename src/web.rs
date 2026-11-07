//! HTTP routes: the page, its JSON, metrics and the GitHub webhook.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::github;
use crate::model::Job;
use crate::pools::Pools;
use crate::store::{SourceHealth, Store, WebhookCounts, WebhookOutcome};

const INDEX_HTML: &str = include_str!("../static/index.html");
const APP_JS: &str = include_str!("../static/app.js");
const APP_CSS: &str = include_str!("../static/app.css");

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
    pub pools: Arc<Pools>,
    /// `None` turns the webhook route off.
    pub webhook_secret: Option<Arc<[u8]>>,
}

/// Every route on one listener: the page, its JSON, metrics and the webhook.
pub fn router(state: AppState) -> Router {
    page_router(state.clone()).merge(webhook_router(state))
}

/// The page, `/api/jobs`, `/metrics` and `/healthz`, without the webhook, for
/// a listener next to a separate [`webhook_router`] one.
pub fn page_router(state: AppState) -> Router {
    Router::new()
        .route(
            "/",
            get(|| async { asset(INDEX_HTML, "text/html; charset=utf-8") }),
        )
        .route(
            "/app.js",
            get(|| async { asset(APP_JS, "text/javascript; charset=utf-8") }),
        )
        .route(
            "/app.css",
            get(|| async { asset(APP_CSS, "text/css; charset=utf-8") }),
        )
        .route("/api/jobs", get(jobs))
        .route("/metrics", get(metrics))
        .route("/healthz", get(|| async { "ok\n" }))
        .with_state(state)
}

/// Only `POST /webhooks/github`, for a listener that is reachable from the
/// internet; every other path is 404.
pub fn webhook_router(state: AppState) -> Router {
    Router::new()
        .route("/webhooks/github", post(github_webhook))
        .with_state(state)
}

fn asset(body: &'static str, content_type: &'static str) -> Response {
    let mut response = body.into_response();
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static("default-src 'self'; frame-ancestors 'none'"),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    response
}

#[derive(Serialize)]
struct JobsResponse {
    generated_at: DateTime<Utc>,
    sources: BTreeMap<String, SourceHealth>,
    jobs: Vec<Job>,
}

async fn jobs(State(state): State<AppState>) -> impl IntoResponse {
    axum::Json(JobsResponse {
        generated_at: Utc::now(),
        sources: state.store.sources(),
        jobs: state.store.snapshot(),
    })
}

async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    let webhooks: Option<WebhookCounts> = state.webhook_secret.as_ref().map(|_| state.store.webhooks());
    let body = crate::metrics::render(
        &state.store.snapshot(),
        &state.store.sources(),
        webhooks,
        Utc::now(),
    );
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4; charset=utf-8")],
        body,
    )
}

async fn github_webhook(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(secret) = state.webhook_secret.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let signature = headers.get("x-hub-signature-256").and_then(|v| v.to_str().ok());
    if !github::verify_signature(secret, &body, signature) {
        state.store.record_webhook(WebhookOutcome::Rejected);
        return (StatusCode::UNAUTHORIZED, "bad signature\n").into_response();
    }
    let event = headers.get("x-github-event").and_then(|v| v.to_str().ok());
    if event != Some("workflow_job") {
        state.store.record_webhook(WebhookOutcome::Ignored);
        return (StatusCode::OK, "ignored\n").into_response();
    }
    let payload: github::WorkflowJobEvent = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => {
            state.store.record_webhook(WebhookOutcome::Rejected);
            tracing::warn!(error = %e, "undecodable workflow_job delivery");
            return (StatusCode::BAD_REQUEST, "undecodable workflow_job payload\n").into_response();
        }
    };
    let now = Utc::now();
    let repository = payload.repository.full_name;
    state.store.upsert(github::to_job(
        payload.workflow_job,
        &repository,
        &state.pools,
        now,
    ));
    state.store.record_webhook(WebhookOutcome::Accepted);
    state.store.record_success(github::SOURCE_WEBHOOK, now);
    (StatusCode::ACCEPTED, "accepted\n").into_response()
}
