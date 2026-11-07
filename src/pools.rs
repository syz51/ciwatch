//! Assigning each job to a runner pool.

use serde::Deserialize;

use crate::model::{PoolFacts, Provider};

/// GitHub's group name for its hosted runners.
const GITHUB_HOSTED_GROUP: &str = "GitHub Actions";

/// One `[[pools]]` entry. Every condition that is set must match; the first
/// matching rule names the pool.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolRule {
    pub name: String,
    pub provider: Option<Provider>,
    pub repository: Option<String>,
    /// Matches when the job asks for any of these labels.
    #[serde(default)]
    pub labels_any: Vec<String>,
    pub runner_prefix: Option<String>,
    pub runner_group: Option<String>,
    pub job_prefix: Option<String>,
}

impl PoolRule {
    pub fn has_condition(&self) -> bool {
        self.provider.is_some()
            || self.repository.is_some()
            || !self.labels_any.is_empty()
            || self.runner_prefix.is_some()
            || self.runner_group.is_some()
            || self.job_prefix.is_some()
    }

    fn matches(&self, facts: &PoolFacts<'_>) -> bool {
        self.provider.is_none_or(|p| p == facts.provider)
            && self.repository.as_deref().is_none_or(|r| r == facts.repository)
            && (self.labels_any.is_empty() || self.labels_any.iter().any(|l| facts.labels.contains(l)))
            && self
                .runner_prefix
                .as_deref()
                .is_none_or(|p| facts.runner.is_some_and(|r| r.starts_with(p)))
            && self
                .runner_group
                .as_deref()
                .is_none_or(|g| facts.runner_group == Some(g))
            && self
                .job_prefix
                .as_deref()
                .is_none_or(|p| facts.name.starts_with(p))
    }
}

#[derive(Debug, Clone, Default)]
pub struct Pools {
    rules: Vec<PoolRule>,
}

impl Pools {
    pub fn new(rules: Vec<PoolRule>) -> Self {
        Self { rules }
    }

    pub fn classify(&self, facts: &PoolFacts<'_>) -> String {
        if let Some(rule) = self.rules.iter().find(|r| r.matches(facts)) {
            return rule.name.clone();
        }
        match facts.provider {
            Provider::Circleci => "circleci".to_owned(),
            Provider::Github if facts.runner_group == Some(GITHUB_HOSTED_GROUP) => "github-hosted".to_owned(),
            Provider::Github if facts.runner.is_some() => "self-hosted".to_owned(),
            // Queued with no runner yet and no rule for its labels.
            Provider::Github => "unassigned".to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn facts<'a>(labels: &'a [String], runner: Option<&'a str>, group: Option<&'a str>) -> PoolFacts<'a> {
        PoolFacts {
            provider: Provider::Github,
            repository: "syz51/email-agent",
            name: "test",
            labels,
            runner,
            runner_group: group,
        }
    }

    fn rule(name: &str) -> PoolRule {
        PoolRule {
            name: name.to_owned(),
            provider: None,
            repository: None,
            labels_any: Vec::new(),
            runner_prefix: None,
            runner_group: None,
            job_prefix: None,
        }
    }

    #[test]
    fn first_matching_rule_wins() {
        let pools = Pools::new(vec![
            PoolRule {
                labels_any: vec!["sai-ephemeral".into()],
                ..rule("sai")
            },
            PoolRule {
                labels_any: vec!["email-agent-ephemeral".into()],
                ..rule("svps")
            },
        ]);
        let labels = vec!["email-agent-ephemeral".into(), "sai-ephemeral".into()];
        assert_eq!(pools.classify(&facts(&labels, None, None)), "sai");
        let labels = vec!["email-agent-ephemeral".into()];
        assert_eq!(pools.classify(&facts(&labels, None, None)), "svps");
    }

    #[test]
    fn all_set_conditions_must_match() {
        let pools = Pools::new(vec![PoolRule {
            labels_any: vec!["linux".into()],
            runner_prefix: Some("sai-".into()),
            ..rule("sai")
        }]);
        let labels = vec!["linux".into()];
        assert_eq!(pools.classify(&facts(&labels, None, None)), "unassigned");
        assert_eq!(pools.classify(&facts(&labels, Some("sai-17"), None)), "sai");
    }

    #[test]
    fn defaults_without_rules() {
        let pools = Pools::default();
        assert_eq!(
            pools.classify(&facts(&[], Some("GitHub Actions 12"), Some("GitHub Actions"))),
            "github-hosted"
        );
        assert_eq!(
            pools.classify(&facts(&[], Some("box"), Some("Default"))),
            "self-hosted"
        );
        assert_eq!(pools.classify(&facts(&[], None, None)), "unassigned");
        let circle = PoolFacts {
            provider: Provider::Circleci,
            ..facts(&[], None, None)
        };
        assert_eq!(pools.classify(&circle), "circleci");
    }
}
