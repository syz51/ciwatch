//! Prometheus text exposition.

use std::collections::BTreeMap;
use std::fmt::Write;

use chrono::{DateTime, Utc};

use crate::model::{Job, Provider, State};
use crate::store::{SourceHealth, WebhookCounts};

fn escape(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

pub fn render(
    jobs: &[Job],
    sources: &BTreeMap<String, SourceHealth>,
    webhooks: Option<WebhookCounts>,
    now: DateTime<Utc>,
) -> String {
    let mut counts: BTreeMap<(Provider, &str, &str, State), u64> = BTreeMap::new();
    let mut oldest_queued: BTreeMap<(Provider, &str), DateTime<Utc>> = BTreeMap::new();
    for job in jobs {
        *counts
            .entry((
                job.provider,
                job.pool.as_str(),
                job.repository.as_str(),
                job.state,
            ))
            .or_default() += 1;
        if job.state == State::Queued
            && let Some(created) = job.created_at
        {
            oldest_queued
                .entry((job.provider, job.pool.as_str()))
                .and_modify(|t| *t = (*t).min(created))
                .or_insert(created);
        }
    }

    let mut out = String::new();
    out.push_str("# HELP ciwatch_jobs Jobs ciwatch holds, by provider, pool, repository and state.\n");
    out.push_str("# TYPE ciwatch_jobs gauge\n");
    for ((provider, pool, repository, state), n) in &counts {
        let _ = writeln!(
            out,
            "ciwatch_jobs{{provider=\"{}\",pool=\"{}\",repository=\"{}\",state=\"{}\"}} {n}",
            provider.as_str(),
            escape(pool),
            escape(repository),
            state.as_str()
        );
    }

    out.push_str(
        "# HELP ciwatch_oldest_queued_job_age_seconds Age of the oldest queued job, by provider and pool.\n",
    );
    out.push_str("# TYPE ciwatch_oldest_queued_job_age_seconds gauge\n");
    for ((provider, pool), created) in &oldest_queued {
        let age = (now - *created).num_seconds().max(0);
        let _ = writeln!(
            out,
            "ciwatch_oldest_queued_job_age_seconds{{provider=\"{}\",pool=\"{}\"}} {age}",
            provider.as_str(),
            escape(pool)
        );
    }

    out.push_str("# HELP ciwatch_source_last_success_timestamp_seconds When each source last polled or delivered successfully.\n");
    out.push_str("# TYPE ciwatch_source_last_success_timestamp_seconds gauge\n");
    for (name, health) in sources {
        if let Some(at) = health.last_success {
            let _ = writeln!(
                out,
                "ciwatch_source_last_success_timestamp_seconds{{source=\"{}\"}} {}",
                escape(name),
                at.timestamp()
            );
        }
    }
    out.push_str("# HELP ciwatch_source_errors_total Failed polls per source.\n");
    out.push_str("# TYPE ciwatch_source_errors_total counter\n");
    for (name, health) in sources {
        let _ = writeln!(
            out,
            "ciwatch_source_errors_total{{source=\"{}\"}} {}",
            escape(name),
            health.errors_total
        );
    }

    if let Some(w) = webhooks {
        out.push_str("# HELP ciwatch_webhook_deliveries_total GitHub webhook deliveries by result.\n");
        out.push_str("# TYPE ciwatch_webhook_deliveries_total counter\n");
        for (result, n) in [
            ("accepted", w.accepted),
            ("ignored", w.ignored),
            ("rejected", w.rejected),
        ] {
            let _ = writeln!(out, "ciwatch_webhook_deliveries_total{{result=\"{result}\"}} {n}");
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::tests::job;

    #[test]
    fn counts_and_escapes() {
        let now = Utc::now();
        let mut a = job("1", State::Queued, now);
        a.pool = "sai \"x\"".into();
        a.created_at = Some(now - chrono::Duration::seconds(90));
        let mut b = job("2", State::Queued, now);
        b.pool = a.pool.clone();
        b.created_at = Some(now - chrono::Duration::seconds(30));
        let c = job("3", State::Running, now);
        let text = render(&[a, b, c], &BTreeMap::new(), None, now);
        assert!(text.contains(
            r#"ciwatch_jobs{provider="github",pool="sai \"x\"",repository="o/r",state="queued"} 2"#
        ));
        assert!(text.contains(
            r#"ciwatch_jobs{provider="github",pool="unassigned",repository="o/r",state="running"} 1"#
        ));
        assert!(
            text.contains(r#"ciwatch_oldest_queued_job_age_seconds{provider="github",pool="sai \"x\""} 90"#)
        );
        assert!(!text.contains("webhook"));
    }
}
