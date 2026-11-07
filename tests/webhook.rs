use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, KeyInit, Mac};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::Sha256;
use tower::ServiceExt;

use ciwatch::pools::{PoolRule, Pools};
use ciwatch::store::Store;
use ciwatch::web::{AppState, router};

const SECRET: &[u8] = b"webhook-secret";

fn app() -> axum::Router {
    let pools = Pools::new(vec![PoolRule {
        name: "sai".into(),
        provider: None,
        repository: None,
        labels_any: vec!["sai-ephemeral".into()],
        runner_prefix: None,
        runner_group: None,
        job_prefix: None,
    }]);
    router(AppState {
        store: Arc::new(Store::new(chrono::Duration::hours(1))),
        pools: Arc::new(pools),
        webhook_secret: Some(Arc::from(SECRET)),
    })
}

fn sign(body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(SECRET).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

fn delivery(action: &str, status: &str, conclusion: Option<&str>, runner: Option<&str>) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "action": action,
        "workflow_job": {
            "id": 101, "run_id": 55, "name": "test", "workflow_name": "CI",
            "head_branch": "feature", "head_sha": "0123456789abcdef",
            "status": status, "conclusion": conclusion,
            "created_at": "2026-11-07T10:00:00Z",
            "started_at": "2026-11-07T10:01:00Z",
            "completed_at": conclusion.map(|_| "2026-11-07T10:05:00Z"),
            "html_url": "https://github.com/syz51/email-agent/actions/runs/55/job/101",
            "labels": ["sai-ephemeral", "email-agent-ephemeral"],
            "runner_name": runner, "runner_group_name": runner.map(|_| "Default"),
        },
        "repository": { "full_name": "syz51/email-agent" },
    }))
    .unwrap()
}

async fn post(app: &axum::Router, event: &str, body: Vec<u8>, signature: Option<String>) -> StatusCode {
    let mut request = Request::post("/webhooks/github").header("x-github-event", event);
    if let Some(sig) = signature {
        request = request.header("x-hub-signature-256", sig);
    }
    app.clone()
        .oneshot(request.body(Body::from(body)).unwrap())
        .await
        .unwrap()
        .status()
}

async fn get_json(app: &axum::Router, path: &str) -> Value {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

async fn get_text(app: &axum::Router, path: &str) -> String {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    String::from_utf8(response.into_body().collect().await.unwrap().to_bytes().to_vec()).unwrap()
}

#[tokio::test]
async fn signed_deliveries_move_a_job_through_its_states() {
    let app = app();

    let body = delivery("queued", "queued", None, None);
    assert_eq!(
        post(&app, "workflow_job", body.clone(), Some(sign(&body))).await,
        StatusCode::ACCEPTED
    );
    let jobs = get_json(&app, "/api/jobs").await;
    let job = &jobs["jobs"][0];
    assert_eq!(job["state"], "queued");
    assert_eq!(job["pool"], "sai");
    assert_eq!(job["started_at"], Value::Null);
    assert!(get_text(&app, "/metrics").await.contains(
        r#"ciwatch_jobs{provider="github",pool="sai",repository="syz51/email-agent",state="queued"} 1"#
    ));

    let body = delivery("in_progress", "in_progress", None, Some("sai-runner-7"));
    assert_eq!(
        post(&app, "workflow_job", body.clone(), Some(sign(&body))).await,
        StatusCode::ACCEPTED
    );
    let job = &get_json(&app, "/api/jobs").await["jobs"][0];
    assert_eq!(job["state"], "running");
    assert_eq!(job["runner"], "sai-runner-7");
    assert_eq!(job["started_at"], "2026-11-07T10:01:00Z");

    let body = delivery("completed", "completed", Some("success"), Some("sai-runner-7"));
    assert_eq!(
        post(&app, "workflow_job", body.clone(), Some(sign(&body))).await,
        StatusCode::ACCEPTED
    );

    // A late, out-of-order delivery does not reopen the job.
    let body = delivery("in_progress", "in_progress", None, Some("sai-runner-7"));
    assert_eq!(
        post(&app, "workflow_job", body.clone(), Some(sign(&body))).await,
        StatusCode::ACCEPTED
    );

    let jobs = get_json(&app, "/api/jobs").await;
    assert_eq!(jobs["jobs"].as_array().unwrap().len(), 1);
    let job = &jobs["jobs"][0];
    assert_eq!(job["state"], "success");
    assert_eq!(job["finished_at"], "2026-11-07T10:05:00Z");
    assert_eq!(job["revision"], "0123456789abcdef");

    let metrics = get_text(&app, "/metrics").await;
    assert!(metrics.contains(r#"ciwatch_webhook_deliveries_total{result="accepted"} 4"#));
    assert!(metrics.contains(r#"ciwatch_source_last_success_timestamp_seconds{source="github-webhook"}"#));
}

#[tokio::test]
async fn bad_or_missing_signatures_are_refused() {
    let app = app();
    let body = delivery("queued", "queued", None, None);
    assert_eq!(
        post(&app, "workflow_job", body.clone(), None).await,
        StatusCode::UNAUTHORIZED
    );
    let wrong = sign(b"something else");
    assert_eq!(
        post(&app, "workflow_job", body, Some(wrong)).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(get_json(&app, "/api/jobs").await["jobs"], json!([]));
    assert!(
        get_text(&app, "/metrics")
            .await
            .contains(r#"ciwatch_webhook_deliveries_total{result="rejected"} 2"#)
    );
}

#[tokio::test]
async fn other_events_are_ignored_and_bad_payloads_rejected() {
    let app = app();
    let ping = br#"{"zen":"Keep it logically awesome."}"#.to_vec();
    assert_eq!(
        post(&app, "ping", ping.clone(), Some(sign(&ping))).await,
        StatusCode::OK
    );
    let junk = br#"{"workflow_job":{}}"#.to_vec();
    assert_eq!(
        post(&app, "workflow_job", junk.clone(), Some(sign(&junk))).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(get_json(&app, "/api/jobs").await["jobs"], json!([]));
}

#[tokio::test]
async fn webhook_route_is_off_without_a_secret() {
    let app = router(AppState {
        store: Arc::new(Store::new(chrono::Duration::hours(1))),
        pools: Arc::new(Pools::default()),
        webhook_secret: None,
    });
    let body = delivery("queued", "queued", None, None);
    assert_eq!(
        post(&app, "workflow_job", body, None).await,
        StatusCode::NOT_FOUND
    );
    assert!(!get_text(&app, "/metrics").await.contains("webhook"));
}

#[tokio::test]
async fn page_assets_are_served_with_a_strict_policy() {
    let app = app();
    for path in ["/", "/app.js", "/app.css"] {
        let response = app
            .clone()
            .oneshot(Request::get(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            response.headers()["content-security-policy"],
            "default-src 'self'; frame-ancestors 'none'"
        );
    }
}
