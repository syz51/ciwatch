//! The loop and HTTP helper both pollers share.

use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::Utc;

use crate::store::Store;

pub trait Source {
    /// The name health and metrics report this source under.
    fn name(&self) -> &'static str;
    fn interval(&self) -> Duration;
    fn store(&self) -> &Store;
    fn poll_once(&self) -> impl Future<Output = Result<()>> + Send;
}

/// Poll forever, recording each round's outcome in the store.
pub async fn run<S: Source>(source: S) {
    let mut ticker = tokio::time::interval(source.interval());
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        match source.poll_once().await {
            Ok(()) => source.store().record_success(source.name(), Utc::now()),
            Err(e) => {
                let message = format!("{e:#}");
                tracing::warn!(source = source.name(), error = %message, "poll failed");
                source.store().record_error(source.name(), Utc::now(), message);
            }
        }
    }
}

/// Send a GET and decode its JSON body.
pub async fn fetch_json<T: serde::de::DeserializeOwned>(
    request: reqwest::RequestBuilder,
    path: &str,
) -> Result<T> {
    request
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .with_context(|| format!("GET {path}"))?
        .json()
        .await
        .with_context(|| format!("decoding {path}"))
}

/// The HTTP status an error from [`fetch_json`] carries, if any.
pub fn status_of(error: &anyhow::Error) -> Option<reqwest::StatusCode> {
    error
        .downcast_ref::<reqwest::Error>()
        .and_then(reqwest::Error::status)
}

/// Collects the failures of one round so that one bad repository or project
/// does not stop the others from being refreshed.
#[derive(Default)]
pub struct Failures(Vec<String>);

impl Failures {
    pub fn add(&mut self, error: anyhow::Error) {
        self.0.push(format!("{error:#}"));
    }

    pub fn into_result(self) -> Result<()> {
        match self.0.len() {
            0 => Ok(()),
            1 => Err(anyhow!("{}", self.0[0])),
            n => Err(anyhow!("{n} requests failed; first: {}", self.0[0])),
        }
    }
}
