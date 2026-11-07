//! An HTTP/1 server with the limits a listener reachable from untrusted
//! clients needs, which `axum::serve` does not set.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::server::graceful::GracefulShutdown;
use hyper_util::service::TowerToHyperService;
use tokio::net::TcpListener;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// A connection that has not sent a request's headers by then is closed.
    pub header_read: Duration,
    /// A request, including reading its body, that takes longer gets 408.
    pub request: Duration,
    /// Further connections are closed as soon as they are accepted.
    pub max_connections: usize,
    /// After shutdown starts, connections still open by then are dropped.
    pub drain: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            header_read: Duration::from_secs(10),
            request: Duration::from_secs(30),
            max_connections: 256,
            drain: Duration::from_secs(10),
        }
    }
}

/// Serves `app` on `listener` until `stop` completes, then lets open
/// connections finish for at most `limits.drain`.
pub async fn serve(listener: TcpListener, app: Router, limits: Limits, stop: impl Future<Output = ()>) {
    let request_limit = limits.request;
    let app = app.layer(middleware::from_fn(
        move |request: Request, next: Next| async move {
            match tokio::time::timeout(request_limit, next.run(request)).await {
                Ok(response) => response,
                Err(_) => request_timed_out(),
            }
        },
    ));
    let service = TowerToHyperService::new(app);
    let mut http = hyper::server::conn::http1::Builder::new();
    http.timer(TokioTimer::new())
        .header_read_timeout(limits.header_read);
    let permits = Arc::new(Semaphore::new(limits.max_connections));
    let graceful = GracefulShutdown::new();
    // Dropping the set at return aborts connections the drain left open.
    let mut connections = JoinSet::new();

    tokio::pin!(stop);
    loop {
        while connections.try_join_next().is_some() {}
        let (stream, peer) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    // Usually out of file descriptors; give connections time to close.
                    tracing::warn!(error = %e, "accepting a connection failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                }
            },
            () = &mut stop => break,
        };
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            tracing::debug!(%peer, "connection limit reached, closing");
            continue;
        };
        let connection = graceful.watch(http.serve_connection(TokioIo::new(stream), service.clone()));
        connections.spawn(async move {
            if let Err(e) = connection.await {
                tracing::debug!(%peer, error = %e, "connection ended with an error");
            }
            drop(permit);
        });
    }
    drop(listener);
    if tokio::time::timeout(limits.drain, graceful.shutdown())
        .await
        .is_err()
    {
        tracing::warn!(after = ?limits.drain, "dropping connections still open at shutdown");
    }
}

fn request_timed_out() -> Response {
    (StatusCode::REQUEST_TIMEOUT, "request timed out\n").into_response()
}
