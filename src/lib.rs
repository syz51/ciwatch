//! ciwatch: one live view of CI jobs across GitHub Actions, CircleCI and
//! self-hosted runner pools.

pub mod circleci;
pub mod config;
pub mod github;
pub mod metrics;
pub mod model;
pub mod poll;
pub mod pools;
pub mod server;
pub mod state_file;
pub mod store;
pub mod web;

/// The HTTP client every poller shares.
pub fn http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent(concat!("ciwatch/", env!("CARGO_PKG_VERSION")))
        .timeout(std::time::Duration::from_secs(30))
        .build()
}
