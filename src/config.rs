//! Configuration file parsing and validation.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail, ensure};
use serde::Deserialize;

use crate::pools::PoolRule;

/// Where a secret comes from. Values never appear in the file itself.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum SecretSource {
    Env(String),
    File(PathBuf),
}

impl SecretSource {
    fn resolve(&self, base: &Path) -> Result<String> {
        let value = match self {
            SecretSource::Env(name) => {
                std::env::var(name).with_context(|| format!("environment variable {name} is not set"))?
            }
            SecretSource::File(path) => {
                let path = base.join(path);
                std::fs::read_to_string(&path)
                    .with_context(|| format!("reading secret file {}", path.display()))?
            }
        };
        let value = value.trim_end_matches(['\r', '\n']).to_owned();
        ensure!(!value.is_empty(), "secret is empty");
        Ok(value)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default = "default_listen")]
    listen: SocketAddr,
    #[serde(default = "default_retention_hours")]
    retention_hours: u64,
    state_file: Option<PathBuf>,
    github: Option<RawGithub>,
    circleci: Option<RawCircleci>,
    #[serde(default)]
    pools: Vec<PoolRule>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGithub {
    webhook_secret: Option<SecretSource>,
    webhook_listen: Option<SocketAddr>,
    token: Option<SecretSource>,
    #[serde(default)]
    repositories: Vec<String>,
    #[serde(default = "default_github_poll_secs")]
    poll_interval_secs: u64,
    #[serde(default = "default_github_api")]
    api_url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawCircleci {
    token: SecretSource,
    projects: Vec<String>,
    #[serde(default = "default_circleci_poll_secs")]
    poll_interval_secs: u64,
    #[serde(default = "default_lookback_hours")]
    lookback_hours: u64,
    #[serde(default = "default_circleci_api")]
    api_url: String,
    #[serde(default = "default_circleci_app")]
    app_url: String,
}

fn default_listen() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 8080))
}
fn default_retention_hours() -> u64 {
    24
}
fn default_github_poll_secs() -> u64 {
    60
}
fn default_github_api() -> String {
    "https://api.github.com".into()
}
fn default_circleci_poll_secs() -> u64 {
    30
}
fn default_lookback_hours() -> u64 {
    6
}
fn default_circleci_api() -> String {
    "https://circleci.com/api/v2".into()
}
fn default_circleci_app() -> String {
    "https://app.circleci.com".into()
}

#[derive(Debug, Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub retention: chrono::Duration,
    /// Where jobs are saved across restarts, resolved against the config
    /// file's directory.
    pub state_file: Option<PathBuf>,
    pub github: Option<GithubConfig>,
    pub circleci: Option<CircleciConfig>,
    pub pools: Vec<PoolRule>,
}

#[derive(Debug, Clone)]
pub struct GithubConfig {
    pub webhook_secret: Option<String>,
    /// When set, the webhook is served only on this address and not on
    /// `listen`, so it can be exposed without exposing the page.
    pub webhook_listen: Option<SocketAddr>,
    /// Present when repositories are polled.
    pub poll: Option<GithubPoll>,
}

#[derive(Debug, Clone)]
pub struct GithubPoll {
    pub token: String,
    pub repositories: Vec<String>,
    pub interval: Duration,
    pub api_url: String,
}

#[derive(Debug, Clone)]
pub struct CircleciConfig {
    pub token: String,
    pub projects: Vec<String>,
    pub interval: Duration,
    pub lookback: chrono::Duration,
    pub api_url: String,
    pub app_url: String,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text =
            std::fs::read_to_string(path).with_context(|| format!("reading config {}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        Self::parse(&text, base)
    }

    /// Parse config text; relative secret files resolve against `base`.
    pub fn parse(text: &str, base: &Path) -> Result<Self> {
        let raw: RawConfig = toml::from_str(text).context("parsing config")?;
        let retention = hours("retention_hours", raw.retention_hours)?;
        if let Some(path) = &raw.state_file {
            ensure!(
                path.file_name().is_some() && !path.as_os_str().as_encoded_bytes().ends_with(b"/"),
                "state_file {} does not name a file",
                path.display()
            );
        }

        let github = raw
            .github
            .map(|g| -> Result<GithubConfig> {
                let webhook_secret = g
                    .webhook_secret
                    .as_ref()
                    .map(|s| s.resolve(base))
                    .transpose()
                    .context("github.webhook_secret")?;
                let poll = match (&g.token, g.repositories.is_empty()) {
                    (Some(token), false) => {
                        for repo in &g.repositories {
                            ensure!(
                                is_owner_repo(repo),
                                "github.repositories entry {repo:?} is not owner/name"
                            );
                        }
                        ensure!(
                            g.poll_interval_secs >= 10,
                            "github.poll_interval_secs must be at least 10"
                        );
                        Some(GithubPoll {
                            token: token.resolve(base).context("github.token")?,
                            repositories: g.repositories,
                            interval: Duration::from_secs(g.poll_interval_secs),
                            api_url: http_url("github.api_url", &g.api_url)?,
                        })
                    }
                    (None, true) => None,
                    (Some(_), true) => bail!("github.token is set but github.repositories is empty"),
                    (None, false) => bail!("github.repositories needs github.token to poll"),
                };
                ensure!(
                    webhook_secret.is_some() || poll.is_some(),
                    "[github] needs webhook_secret, or token and repositories"
                );
                if let Some(addr) = g.webhook_listen {
                    ensure!(
                        webhook_secret.is_some(),
                        "github.webhook_listen needs github.webhook_secret"
                    );
                    ensure!(
                        !binds_overlap(addr, raw.listen),
                        "github.webhook_listen and listen bind the same port"
                    );
                }
                Ok(GithubConfig {
                    webhook_secret,
                    webhook_listen: g.webhook_listen,
                    poll,
                })
            })
            .transpose()?;

        let circleci = raw
            .circleci
            .map(|c| -> Result<CircleciConfig> {
                ensure!(!c.projects.is_empty(), "circleci.projects is empty");
                for slug in &c.projects {
                    ensure!(
                        slug.split('/').count() == 3 && slug.split('/').all(|p| !p.is_empty()),
                        "circleci.projects entry {slug:?} is not a vcs/org/repo slug"
                    );
                }
                ensure!(
                    c.poll_interval_secs >= 5,
                    "circleci.poll_interval_secs must be at least 5"
                );
                Ok(CircleciConfig {
                    token: c.token.resolve(base).context("circleci.token")?,
                    projects: c.projects,
                    interval: Duration::from_secs(c.poll_interval_secs),
                    lookback: hours("circleci.lookback_hours", c.lookback_hours)?,
                    api_url: http_url("circleci.api_url", &c.api_url)?,
                    app_url: http_url("circleci.app_url", &c.app_url)?,
                })
            })
            .transpose()?;

        ensure!(
            github.is_some() || circleci.is_some(),
            "configure at least one of [github] and [circleci]"
        );
        for rule in &raw.pools {
            ensure!(!rule.name.is_empty(), "a [[pools]] entry has an empty name");
            ensure!(
                rule.has_condition(),
                "pool {:?} has no condition and would match every job",
                rule.name
            );
        }

        Ok(Config {
            listen: raw.listen,
            retention,
            state_file: raw.state_file.map(|p| base.join(p)),
            github,
            circleci,
            pools: raw.pools,
        })
    }
}

/// The longest retention or lookback accepted: one year.
const MAX_HOURS: u64 = 24 * 366;

fn hours(key: &str, value: u64) -> Result<chrono::Duration> {
    ensure!(
        (1..=MAX_HOURS).contains(&value),
        "{key} must be between 1 and {MAX_HOURS}"
    );
    Ok(chrono::Duration::hours(i64::try_from(value)?))
}

/// An absolute http(s) URL, returned without a trailing slash.
fn http_url(key: &str, value: &str) -> Result<String> {
    let url = reqwest::Url::parse(value).with_context(|| format!("{key} {value:?} is not a URL"))?;
    ensure!(
        matches!(url.scheme(), "http" | "https") && url.host().is_some(),
        "{key} {value:?} must be an http or https URL"
    );
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "{key} {value:?} must not have a query or fragment"
    );
    Ok(value.trim_end_matches('/').to_owned())
}

/// Whether binding both addresses would conflict: the same fixed port on the
/// same address, or with either on every address (`0.0.0.0`, `[::]`).
fn binds_overlap(a: SocketAddr, b: SocketAddr) -> bool {
    a.port() != 0
        && a.port() == b.port()
        && (a.ip() == b.ip() || a.ip().is_unspecified() || b.ip().is_unspecified())
}

fn is_owner_repo(s: &str) -> bool {
    let mut parts = s.split('/');
    matches!((parts.next(), parts.next(), parts.next()), (Some(o), Some(r), None) if !o.is_empty() && !r.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A directory holding a `secret` file, removed on drop. Each test names
    /// its own, since tests run in parallel.
    struct SecretDir(PathBuf);

    impl SecretDir {
        fn new(test: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ciwatch-config-{}-{test}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("secret"), "s3cret\n").unwrap();
            Self(dir)
        }
    }

    impl Drop for SecretDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn parses_a_full_config() {
        let dir = SecretDir::new("full");
        let base = &dir.0;
        let config = Config::parse(
            r#"
            listen = "0.0.0.0:9000"
            [github]
            webhook_secret = { file = "secret" }
            token = { file = "secret" }
            repositories = ["syz51/vps"]
            [circleci]
            token = { file = "secret" }
            projects = ["gh/syz51/vps"]
            [[pools]]
            name = "sai"
            labels_any = ["sai-ephemeral"]
            "#,
            base,
        )
        .unwrap();
        let github = config.github.unwrap();
        assert_eq!(github.webhook_secret.as_deref(), Some("s3cret"));
        assert_eq!(github.poll.unwrap().repositories, ["syz51/vps"]);
        assert_eq!(config.circleci.unwrap().projects, ["gh/syz51/vps"]);
        assert_eq!(config.pools.len(), 1);
        assert_eq!(config.state_file, None);
    }

    #[test]
    fn resolves_the_state_file() {
        let dir = SecretDir::new("state-file");
        for (value, expected) in [
            (
                "/var/lib/ciwatch/state.json",
                PathBuf::from("/var/lib/ciwatch/state.json"),
            ),
            ("state.json", dir.0.join("state.json")),
        ] {
            let text =
                format!("state_file = \"{value}\"\n[github]\nwebhook_secret = {{ file = \"secret\" }}");
            let config = Config::parse(&text, &dir.0).unwrap_or_else(|e| panic!("{text:?}: {e:#}"));
            assert_eq!(config.state_file, Some(expected));
        }
    }

    #[test]
    fn rejects_bad_configs() {
        let dir = SecretDir::new("bad");
        let base = &dir.0;
        for (text, expected) in [
            (
                "retention_hours = 100000000\n[github]\nwebhook_secret = { file = \"secret\" }",
                "between 1 and",
            ),
            (
                "[github]\ntoken = { file = \"secret\" }\nrepositories = [\"a/b\"]\napi_url = \"api.github.com\"",
                "github.api_url",
            ),
            (
                "[circleci]\ntoken = { file = \"secret\" }\nprojects = [\"gh/a/b\"]\napp_url = \"ftp://x\"",
                "http or https",
            ),
            ("", "at least one"),
            (
                "state_file = \"/var/lib/ciwatch/\"\n[github]\nwebhook_secret = { file = \"secret\" }",
                "does not name a file",
            ),
            (
                "state_file = \"..\"\n[github]\nwebhook_secret = { file = \"secret\" }",
                "does not name a file",
            ),
            ("[github]\nrepositories = [\"a/b\"]", "needs github.token"),
            (
                "[github]\nwebhook_secret = { file = \"secret\" }\nunknown = 1",
                "unknown field",
            ),
            (
                "[circleci]\ntoken = { file = \"secret\" }\nprojects = [\"syz51/vps\"]",
                "vcs/org/repo",
            ),
            (
                "[github]\nwebhook_secret = { file = \"secret\" }\n[[pools]]\nname = \"all\"",
                "no condition",
            ),
            (
                "[github]\nwebhook_secret = { file = \"missing\" }",
                "reading secret file",
            ),
            (
                "[github]\ntoken = { file = \"secret\" }\nrepositories = [\"a/b\"]\nwebhook_listen = \"127.0.0.1:8081\"",
                "webhook_listen needs github.webhook_secret",
            ),
            (
                "listen = \"0.0.0.0:8080\"\n[github]\nwebhook_secret = { file = \"secret\" }\nwebhook_listen = \"127.0.0.1:8080\"",
                "bind the same port",
            ),
            (
                "listen = \"127.0.0.1:8080\"\n[github]\nwebhook_secret = { file = \"secret\" }\nwebhook_listen = \"[::]:8080\"",
                "bind the same port",
            ),
            (
                "[github]\nwebhook_secret = { file = \"secret\" }\nwebhook_listen = \"127.0.0.1:8080\"",
                "bind the same port",
            ),
        ] {
            let err = format!("{:#}", Config::parse(text, base).unwrap_err());
            assert!(err.contains(expected), "{text:?}: {err}");
        }
    }

    #[test]
    fn accepts_a_separate_webhook_listener() {
        let dir = SecretDir::new("webhook-listen");
        for (listen, webhook) in [
            ("127.0.0.1:8080", "172.19.0.1:8080"),
            ("127.0.0.1:8080", "0.0.0.0:8081"),
            ("127.0.0.1:0", "127.0.0.1:0"),
        ] {
            let text = format!(
                "listen = \"{listen}\"\n[github]\nwebhook_secret = {{ file = \"secret\" }}\nwebhook_listen = \"{webhook}\""
            );
            let config = Config::parse(&text, &dir.0).unwrap_or_else(|e| panic!("{text:?}: {e:#}"));
            assert_eq!(
                config.github.unwrap().webhook_listen,
                Some(webhook.parse().unwrap())
            );
        }
    }
}
