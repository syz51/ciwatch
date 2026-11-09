//! In-memory job store and source health. [`crate::state_file`] can save
//! the jobs and restore them at start.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, RwLock};

use chrono::{DateTime, Duration, Utc};
use serde::Serialize;

use crate::model::{Job, JobKey, Provider};

#[derive(Debug, Clone, Default, Serialize)]
pub struct SourceHealth {
    pub last_success: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_error_at: Option<DateTime<Utc>>,
    pub errors_total: u64,
}

#[derive(Debug, Default, Clone, Copy, Serialize)]
pub struct WebhookCounts {
    pub accepted: u64,
    pub ignored: u64,
    pub rejected: u64,
}

#[derive(Debug, Clone, Copy)]
pub enum WebhookOutcome {
    Accepted,
    Ignored,
    Rejected,
}

pub struct Store {
    jobs: RwLock<HashMap<JobKey, Job>>,
    sources: Mutex<BTreeMap<String, SourceHealth>>,
    webhooks: Mutex<WebhookCounts>,
    retention: Duration,
    /// Counts changes to `jobs`, so a saver can tell whether to write.
    changes: AtomicU64,
}

impl Store {
    pub fn new(retention: Duration) -> Self {
        Self {
            jobs: RwLock::new(HashMap::new()),
            sources: Mutex::new(BTreeMap::new()),
            webhooks: Mutex::new(WebhookCounts::default()),
            retention,
            changes: AtomicU64::new(0),
        }
    }

    /// A number that grows whenever a job is added, removed or changed in
    /// more than `observed_at`.
    pub fn changes(&self) -> u64 {
        self.changes.load(Ordering::Acquire)
    }

    fn changed(&self) {
        self.changes.fetch_add(1, Ordering::AcqRel);
    }

    /// Add jobs saved by an earlier run, then drop those older than the
    /// retention window. An active job is added only when `refreshed` says a
    /// poller will update it; otherwise nothing would ever finish it. A job
    /// already held is kept as it is.
    pub fn restore(&self, saved: Vec<Job>, now: DateTime<Utc>, refreshed: impl Fn(&Job) -> bool) {
        {
            let mut jobs = self.jobs.write().expect("job store lock poisoned");
            for job in saved {
                if !job.state.is_active() || refreshed(&job) {
                    jobs.entry(job.key()).or_insert(job);
                }
            }
        }
        self.changed();
        self.prune(now);
    }

    /// Insert or update a job. A job never moves back to an earlier state; a
    /// stale update only refreshes `observed_at`. A job with no `created_at`
    /// gets the time ciwatch first saw it.
    pub fn upsert(&self, mut job: Job) {
        let mut jobs = self.jobs.write().expect("job store lock poisoned");
        match jobs.get_mut(&job.key()) {
            Some(existing) if existing.state.progress() > job.state.progress() => {
                existing.observed_at = existing.observed_at.max(job.observed_at);
                return;
            }
            Some(existing) => {
                job.created_at = existing.created_at.or(job.created_at);
                job.started_at = job.started_at.or(existing.started_at);
                if job.runner.is_none() && existing.runner.is_some() {
                    // The pool was classified with the runner known; an
                    // update without it cannot classify better.
                    job.runner = existing.runner.clone();
                    job.pool = existing.pool.clone();
                }
                let observed_only = Job {
                    observed_at: existing.observed_at,
                    ..job.clone()
                } == *existing;
                *existing = job;
                if observed_only {
                    // A poll that only saw the job again is not worth a save.
                    return;
                }
            }
            None => {
                job.created_at = job.created_at.or(Some(job.observed_at));
                jobs.insert(job.key(), job);
            }
        }
        drop(jobs);
        self.changed();
    }

    /// Run ids of jobs from `provider` and `repository` (compared without
    /// case, as GitHub does) that are still active.
    pub fn active_run_ids(&self, provider: Provider, repository: &str) -> HashSet<String> {
        let jobs = self.jobs.read().expect("job store lock poisoned");
        jobs.values()
            .filter(|j| {
                j.provider == provider && j.repository.eq_ignore_ascii_case(repository) && j.state.is_active()
            })
            .map(|j| j.run_id.clone())
            .collect()
    }

    /// One active job per run from `provider`, as a template for refreshing
    /// that run.
    pub fn active_runs(&self, provider: Provider) -> Vec<Job> {
        let jobs = self.jobs.read().expect("job store lock poisoned");
        let mut seen = HashSet::new();
        jobs.values()
            .filter(|j| j.provider == provider && j.state.is_active() && seen.insert(j.run_id.clone()))
            .cloned()
            .collect()
    }

    /// Forget every job of one run.
    pub fn remove_run(&self, provider: Provider, repository: &str, run_id: &str) {
        let mut jobs = self.jobs.write().expect("job store lock poisoned");
        let before = jobs.len();
        jobs.retain(|_, j| {
            !(j.provider == provider && j.repository.eq_ignore_ascii_case(repository) && j.run_id == run_id)
        });
        if jobs.len() != before {
            drop(jobs);
            self.changed();
        }
    }

    pub fn len(&self) -> usize {
        self.jobs.read().expect("job store lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Every job, in no particular order.
    pub fn jobs(&self) -> Vec<Job> {
        self.jobs
            .read()
            .expect("job store lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Active jobs first, oldest first; then finished jobs, newest first.
    pub fn snapshot(&self) -> Vec<Job> {
        let mut jobs: Vec<Job> = self
            .jobs
            .read()
            .expect("job store lock poisoned")
            .values()
            .cloned()
            .collect();
        jobs.sort_by(|a, b| {
            b.state
                .is_active()
                .cmp(&a.state.is_active())
                .then_with(|| {
                    if a.state.is_active() {
                        a.created_at.cmp(&b.created_at)
                    } else {
                        b.finished_at.cmp(&a.finished_at)
                    }
                })
                .then_with(|| a.key().cmp(&b.key()))
        });
        jobs
    }

    /// Drop jobs not seen within the retention window.
    pub fn prune(&self, now: DateTime<Utc>) {
        let cutoff = now - self.retention;
        let mut jobs = self.jobs.write().expect("job store lock poisoned");
        let before = jobs.len();
        jobs.retain(|_, j| j.observed_at >= cutoff);
        if jobs.len() != before {
            drop(jobs);
            self.changed();
        }
    }

    pub fn record_success(&self, source: &str, at: DateTime<Utc>) {
        let mut sources = self.sources.lock().expect("source lock poisoned");
        let health = sources.entry(source.to_owned()).or_default();
        health.last_success = Some(at);
    }

    pub fn record_error(&self, source: &str, at: DateTime<Utc>, error: String) {
        let mut sources = self.sources.lock().expect("source lock poisoned");
        let health = sources.entry(source.to_owned()).or_default();
        health.last_error = Some(error);
        health.last_error_at = Some(at);
        health.errors_total += 1;
    }

    pub fn register_source(&self, source: &str) {
        self.sources
            .lock()
            .expect("source lock poisoned")
            .entry(source.to_owned())
            .or_default();
    }

    pub fn sources(&self) -> BTreeMap<String, SourceHealth> {
        self.sources.lock().expect("source lock poisoned").clone()
    }

    pub fn record_webhook(&self, outcome: WebhookOutcome) {
        let mut counts = self.webhooks.lock().expect("webhook lock poisoned");
        match outcome {
            WebhookOutcome::Accepted => counts.accepted += 1,
            WebhookOutcome::Ignored => counts.ignored += 1,
            WebhookOutcome::Rejected => counts.rejected += 1,
        }
    }

    pub fn webhooks(&self) -> WebhookCounts {
        *self.webhooks.lock().expect("webhook lock poisoned")
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::model::State;

    pub(crate) fn job(id: &str, state: State, observed_at: DateTime<Utc>) -> Job {
        Job {
            provider: Provider::Github,
            id: id.to_owned(),
            repository: "o/r".into(),
            pipeline: "CI".into(),
            run_id: "1".into(),
            name: "test".into(),
            state,
            provider_status: state.as_str().into(),
            branch: None,
            revision: None,
            pool: "unassigned".into(),
            runner: None,
            labels: Vec::new(),
            url: None,
            created_at: None,
            started_at: None,
            finished_at: None,
            observed_at,
        }
    }

    #[test]
    fn late_updates_do_not_move_a_job_backwards() {
        let store = Store::new(Duration::hours(1));
        let t = Utc::now();
        store.upsert(job("1", State::Success, t));
        store.upsert(job("1", State::Running, t + Duration::seconds(5)));
        let jobs = store.snapshot();
        assert_eq!(jobs[0].state, State::Success);
        assert_eq!(jobs[0].observed_at, t + Duration::seconds(5));
    }

    #[test]
    fn progress_keeps_earlier_facts() {
        let store = Store::new(Duration::hours(1));
        let t = Utc::now();
        let mut running = job("1", State::Running, t);
        running.runner = Some("sai-1".into());
        running.created_at = Some(t);
        store.upsert(running);
        store.upsert(job("1", State::Failure, t));
        let jobs = store.snapshot();
        assert_eq!(jobs[0].state, State::Failure);
        assert_eq!(jobs[0].runner.as_deref(), Some("sai-1"));
        assert_eq!(jobs[0].created_at, Some(t));
    }

    #[test]
    fn prune_drops_jobs_older_than_retention() {
        let store = Store::new(Duration::hours(1));
        let t = Utc::now();
        store.upsert(job("old", State::Success, t - Duration::hours(2)));
        store.upsert(job("new", State::Success, t));
        store.prune(t);
        let ids: Vec<_> = store.snapshot().into_iter().map(|j| j.id).collect();
        assert_eq!(ids, ["new"]);
    }

    #[test]
    fn restore_prunes_and_keeps_held_jobs() {
        let store = Store::new(Duration::hours(1));
        let t = Utc::now();
        store.upsert(job("held", State::Running, t));
        let before = store.changes();
        store.restore(
            vec![
                job("old", State::Success, t - Duration::hours(2)),
                job("kept", State::Failure, t - Duration::minutes(30)),
                job("held", State::Success, t - Duration::minutes(30)),
            ],
            t,
            |_| true,
        );
        assert!(store.changes() > before);
        let mut jobs = store.snapshot();
        jobs.sort_by(|a, b| a.id.cmp(&b.id));
        let ids: Vec<_> = jobs.iter().map(|j| (j.id.as_str(), j.state)).collect();
        assert_eq!(ids, [("held", State::Running), ("kept", State::Failure)]);
    }

    #[test]
    fn changes_count_only_real_changes() {
        let store = Store::new(Duration::hours(1));
        let t = Utc::now();
        let start = store.changes();
        store.prune(t);
        store.remove_run(Provider::Github, "o/r", "1");
        assert_eq!(store.changes(), start);
        store.upsert(job("1", State::Queued, t));
        let after_upsert = store.changes();
        assert!(after_upsert > start);
        // Seen again, or a late update: only observed_at moves.
        store.upsert(job("1", State::Queued, t + Duration::seconds(30)));
        assert_eq!(store.changes(), after_upsert);
        store.upsert(job("1", State::Running, t + Duration::seconds(60)));
        let after_start = store.changes();
        assert!(after_start > after_upsert);
        store.upsert(job("1", State::Queued, t + Duration::seconds(90)));
        assert_eq!(store.changes(), after_start);
        assert_eq!(store.jobs()[0].observed_at, t + Duration::seconds(90));
        store.remove_run(Provider::Github, "O/R", "1");
        assert!(store.changes() > after_start);
    }

    #[test]
    fn snapshot_orders_active_first() {
        let store = Store::new(Duration::hours(1));
        let t = Utc::now();
        let mut done = job("done", State::Success, t);
        done.finished_at = Some(t);
        store.upsert(done);
        let mut newer = job("newer", State::Queued, t);
        newer.created_at = Some(t);
        store.upsert(newer);
        let mut older = job("older", State::Running, t);
        older.created_at = Some(t - Duration::minutes(5));
        store.upsert(older);
        let ids: Vec<_> = store.snapshot().into_iter().map(|j| j.id).collect();
        assert_eq!(ids, ["older", "newer", "done"]);
    }
}
