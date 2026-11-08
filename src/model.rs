//! The provider-neutral job model every adapter maps into.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Provider {
    Github,
    Circleci,
}

impl Provider {
    pub fn as_str(self) -> &'static str {
        match self {
            Provider::Github => "github",
            Provider::Circleci => "circleci",
        }
    }
}

/// Where a job is in its life. Providers' own status words are kept in
/// [`Job::provider_status`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Waiting for a runner or executor.
    Queued,
    /// Held by something other than capacity: an approval, an environment
    /// protection rule, a concurrency group or an upstream job.
    Waiting,
    Running,
    Success,
    Failure,
    Cancelled,
    Skipped,
}

impl State {
    pub const ALL: [State; 7] = [
        State::Queued,
        State::Waiting,
        State::Running,
        State::Success,
        State::Failure,
        State::Cancelled,
        State::Skipped,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            State::Queued => "queued",
            State::Waiting => "waiting",
            State::Running => "running",
            State::Success => "success",
            State::Failure => "failure",
            State::Cancelled => "cancelled",
            State::Skipped => "skipped",
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, State::Queued | State::Waiting | State::Running)
    }

    /// Progress order. An update never moves a job backwards, so a late
    /// "queued" delivery cannot undo "running" and a poll cannot revive a
    /// finished job.
    pub fn progress(self) -> u8 {
        match self {
            State::Queued | State::Waiting => 0,
            State::Running => 1,
            _ => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct JobKey {
    pub provider: Provider,
    pub id: String,
}

/// One job (OpenTelemetry: a pipeline task run) on any provider.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Job {
    pub provider: Provider,
    /// Provider-unique job id.
    pub id: String,
    /// `owner/name` for GitHub and GitHub-backed CircleCI projects, otherwise
    /// the CircleCI project slug.
    pub repository: String,
    /// Workflow name.
    pub pipeline: String,
    /// GitHub workflow run id or CircleCI workflow id.
    pub run_id: String,
    pub name: String,
    pub state: State,
    pub provider_status: String,
    pub branch: Option<String>,
    pub revision: Option<String>,
    pub pool: String,
    pub runner: Option<String>,
    pub labels: Vec<String>,
    pub url: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// When ciwatch last received or polled this job.
    pub observed_at: DateTime<Utc>,
}

impl Job {
    pub fn key(&self) -> JobKey {
        JobKey {
            provider: self.provider,
            id: self.id.clone(),
        }
    }
}

/// The facts pool rules match on.
#[derive(Debug, Clone, Copy)]
pub struct PoolFacts<'a> {
    pub provider: Provider,
    pub repository: &'a str,
    pub name: &'a str,
    pub labels: &'a [String],
    pub runner: Option<&'a str>,
    pub runner_group: Option<&'a str>,
}
