//! The optional state file: jobs saved across restarts.
//!
//! The file is JSON, `{"version": 1, "jobs": [...]}`, with each job in the
//! `/api/jobs` shape. It is replaced atomically, so a reader sees either the
//! old file or the new one. A file that cannot be read starts ciwatch empty;
//! it never stops the server.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::model::Job;
use crate::store::Store;

/// The name health and metrics report saves and loads under.
pub const SOURCE: &str = "state-file";
/// How often the jobs are saved when they changed.
pub const SAVE_INTERVAL: Duration = Duration::from_secs(30);

const FORMAT_VERSION: u32 = 1;

#[derive(Serialize)]
struct SavedRef<'a> {
    version: u32,
    jobs: &'a [Job],
}

#[derive(Deserialize)]
struct Saved {
    version: u32,
    jobs: Vec<Job>,
}

/// Read saved jobs; `None` when the file does not exist.
pub fn read(path: &Path) -> Result<Option<Vec<Job>>> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).with_context(|| format!("reading state file {}", path.display())),
    };
    let saved: Saved =
        serde_json::from_slice(&bytes).with_context(|| format!("decoding state file {}", path.display()))?;
    ensure!(
        saved.version == FORMAT_VERSION,
        "state file {} has version {}, expected {FORMAT_VERSION}",
        path.display(),
        saved.version
    );
    Ok(Some(saved.jobs))
}

/// Replace the file with `jobs`: write a temporary file next to it, flush it
/// to disk, rename it over the file, then flush the directory.
pub fn write(path: &Path, jobs: &[Job]) -> Result<()> {
    let tmp = temp_path(path);
    let written = write_synced(&tmp, jobs).and_then(|()| {
        fs::rename(&tmp, path).with_context(|| format!("renaming {} to {}", tmp.display(), path.display()))
    });
    if let Err(e) = written {
        // Best effort: the error being returned is the one that matters.
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    sync_dir(path)
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn write_synced(tmp: &Path, jobs: &[Job]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let file = options
        .open(tmp)
        .with_context(|| format!("creating {}", tmp.display()))?;
    let mut out = BufWriter::new(file);
    serde_json::to_writer(
        &mut out,
        &SavedRef {
            version: FORMAT_VERSION,
            jobs,
        },
    )
    .with_context(|| format!("writing {}", tmp.display()))?;
    out.flush()
        .with_context(|| format!("writing {}", tmp.display()))?;
    out.get_ref()
        .sync_all()
        .with_context(|| format!("syncing {}", tmp.display()))
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> Result<()> {
    let dir = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("syncing directory {}", dir.display()))
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> Result<()> {
    Ok(())
}

/// Restore saved jobs into the store at start. Logs and records any failure
/// under [`SOURCE`]; the store stays empty then.
pub fn load(path: &Path, store: &Store, now: DateTime<Utc>) {
    store.register_source(SOURCE);
    match read(path) {
        Ok(Some(jobs)) => {
            let saved = jobs.len();
            store.restore(jobs, now);
            tracing::info!(path = %path.display(), saved, kept = store.snapshot().len(), "restored jobs");
        }
        Ok(None) => tracing::info!(path = %path.display(), "no state file yet; starting empty"),
        Err(e) => {
            let message = format!("{e:#}");
            tracing::error!(error = %message, "cannot load the state file; starting empty");
            store.record_error(SOURCE, now, message);
        }
    }
}

/// Saves the store when it changed since the last successful save.
pub struct Saver {
    path: PathBuf,
    store: Arc<Store>,
    saved: u64,
}

impl Saver {
    /// A saver that treats the store's current contents as saved. Create it
    /// before [`load`], so the first tick rewrites the file without the jobs
    /// the restore pruned, and shows early whether the file can be written.
    pub fn new(path: PathBuf, store: Arc<Store>) -> Self {
        let saved = store.changes();
        Self { path, store, saved }
    }

    /// Save if the store changed, recording the outcome under [`SOURCE`].
    pub async fn save_if_changed(&mut self) {
        let changes = self.store.changes();
        if changes == self.saved {
            return;
        }
        let jobs = self.store.snapshot();
        let path = self.path.clone();
        let written = tokio::task::spawn_blocking(move || write(&path, &jobs))
            .await
            .context("the state file writer panicked")
            .and_then(|r| r);
        match written {
            Ok(()) => {
                self.saved = changes;
                self.store.record_success(SOURCE, Utc::now());
            }
            Err(e) => {
                let message = format!("{e:#}");
                tracing::error!(error = %message, "cannot save the state file");
                self.store.record_error(SOURCE, Utc::now(), message);
            }
        }
    }

    /// Save every [`SAVE_INTERVAL`] when changed, and once more when `stop`
    /// fires or its sender is dropped. Only this task writes the file, so a
    /// save in progress is never raced by the last one.
    pub async fn run(mut self, mut stop: tokio::sync::oneshot::Receiver<()>) {
        let mut ticker = tokio::time::interval(SAVE_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = ticker.tick() => self.save_if_changed().await,
                _ = &mut stop => {
                    self.save_if_changed().await;
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::State;
    use crate::store::tests::job;

    /// A directory removed on drop. Each test names its own, since tests run
    /// in parallel.
    struct TestDir(PathBuf);

    impl TestDir {
        fn new(test: &str) -> Self {
            let dir = std::env::temp_dir().join(format!("ciwatch-state-{}-{test}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir_all(&dir).unwrap();
            Self(dir)
        }

        fn file(&self) -> PathBuf {
            self.0.join("state.json")
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn ids(store: &Store) -> Vec<String> {
        let mut ids: Vec<_> = store.snapshot().into_iter().map(|j| j.id).collect();
        ids.sort();
        ids
    }

    #[test]
    fn writes_and_reads_back() {
        let dir = TestDir::new("roundtrip");
        let t = Utc::now();
        let mut done = job("1", State::Success, t);
        done.finished_at = Some(t);
        done.runner = Some("sai-ephemeral-1".into());
        done.labels = vec!["sai-ephemeral".into()];
        let jobs = vec![done, job("2", State::Waiting, t)];
        write(&dir.file(), &jobs).unwrap();
        assert_eq!(read(&dir.file()).unwrap(), Some(jobs));
        assert!(!temp_path(&dir.file()).exists());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.file()).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn a_missing_file_reads_as_none() {
        let dir = TestDir::new("missing");
        assert_eq!(read(&dir.file()).unwrap(), None);
    }

    #[test]
    fn load_drops_jobs_older_than_retention() {
        let dir = TestDir::new("prune");
        let t = Utc::now();
        write(
            &dir.file(),
            &[
                job("old", State::Success, t - chrono::Duration::hours(25)),
                job("recent", State::Failure, t - chrono::Duration::hours(23)),
            ],
        )
        .unwrap();
        let store = Store::new(chrono::Duration::hours(24));
        load(&dir.file(), &store, t);
        assert_eq!(ids(&store), ["recent"]);
        assert_eq!(store.sources()[SOURCE].errors_total, 0);
    }

    #[test]
    fn a_corrupt_file_starts_empty_and_records_the_error() {
        let dir = TestDir::new("corrupt");
        for (i, text) in [
            "{\"version\": 1, \"jobs\": [",
            "not json",
            "{\"version\": 99, \"jobs\": []}",
        ]
        .into_iter()
        .enumerate()
        {
            fs::write(dir.file(), text).unwrap();
            let store = Store::new(chrono::Duration::hours(24));
            load(&dir.file(), &store, Utc::now());
            assert!(store.snapshot().is_empty(), "case {i}");
            let health = &store.sources()[SOURCE];
            assert_eq!(health.errors_total, 1, "case {i}");
            assert!(
                health.last_error.as_deref().unwrap().contains("state file"),
                "case {i}"
            );
        }
    }

    #[test]
    fn an_unreadable_file_starts_empty() {
        let dir = TestDir::new("unreadable");
        // A directory in place of the file cannot be read as one.
        fs::create_dir(dir.file()).unwrap();
        let store = Store::new(chrono::Duration::hours(24));
        load(&dir.file(), &store, Utc::now());
        assert!(store.snapshot().is_empty());
        assert_eq!(store.sources()[SOURCE].errors_total, 1);
    }

    #[test]
    fn a_failed_write_keeps_the_previous_file() {
        let dir = TestDir::new("atomic");
        let t = Utc::now();
        let first = vec![job("1", State::Success, t)];
        write(&dir.file(), &first).unwrap();
        // The temporary file's path is taken, so the next write fails before
        // it can touch the file.
        fs::create_dir(temp_path(&dir.file())).unwrap();
        assert!(write(&dir.file(), &[job("2", State::Success, t)]).is_err());
        assert_eq!(read(&dir.file()).unwrap(), Some(first));
        // A leftover temporary file from a crash is overwritten.
        fs::remove_dir(temp_path(&dir.file())).unwrap();
        fs::write(temp_path(&dir.file()), "partial").unwrap();
        let second = vec![job("2", State::Failure, t)];
        write(&dir.file(), &second).unwrap();
        assert_eq!(read(&dir.file()).unwrap(), Some(second));
        assert!(!temp_path(&dir.file()).exists());
    }

    #[tokio::test]
    async fn saves_only_after_a_change() {
        let dir = TestDir::new("saver");
        let store = Arc::new(Store::new(chrono::Duration::hours(24)));
        let mut saver = Saver::new(dir.file(), store.clone());
        saver.save_if_changed().await;
        assert!(!dir.file().exists());

        store.upsert(job("1", State::Success, Utc::now()));
        saver.save_if_changed().await;
        assert_eq!(read(&dir.file()).unwrap().unwrap().len(), 1);
        assert!(store.sources()[SOURCE].last_success.is_some());

        // Unchanged: the file is left alone, even when it went away.
        fs::remove_file(dir.file()).unwrap();
        saver.save_if_changed().await;
        assert!(!dir.file().exists());

        // A restart restores what was saved.
        store.upsert(job("2", State::Failure, Utc::now()));
        saver.save_if_changed().await;
        let restarted = Store::new(chrono::Duration::hours(24));
        load(&dir.file(), &restarted, Utc::now());
        assert_eq!(ids(&restarted), ["1", "2"]);
    }

    #[tokio::test]
    async fn the_first_save_after_a_load_drops_pruned_jobs() {
        let dir = TestDir::new("rewrite");
        let t = Utc::now();
        write(
            &dir.file(),
            &[
                job("old", State::Success, t - chrono::Duration::hours(25)),
                job("recent", State::Success, t),
            ],
        )
        .unwrap();
        let store = Arc::new(Store::new(chrono::Duration::hours(24)));
        let mut saver = Saver::new(dir.file(), store.clone());
        load(&dir.file(), &store, t);
        saver.save_if_changed().await;
        let saved: Vec<_> = read(&dir.file())
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|j| j.id)
            .collect();
        assert_eq!(saved, ["recent"]);
    }

    #[tokio::test]
    async fn run_saves_once_more_on_stop() {
        let dir = TestDir::new("stop");
        let store = Arc::new(Store::new(chrono::Duration::hours(24)));
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let saver = tokio::spawn(Saver::new(dir.file(), store.clone()).run(stop_rx));
        store.upsert(job("1", State::Success, Utc::now()));
        stop_tx.send(()).unwrap();
        saver.await.unwrap();
        assert_eq!(read(&dir.file()).unwrap().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_failed_save_is_retried() {
        let dir = TestDir::new("retry");
        let store = Arc::new(Store::new(chrono::Duration::hours(24)));
        let missing = dir.0.join("missing").join("state.json");
        let mut saver = Saver::new(missing.clone(), store.clone());
        store.upsert(job("1", State::Success, Utc::now()));
        saver.save_if_changed().await;
        assert_eq!(store.sources()[SOURCE].errors_total, 1);
        fs::create_dir(dir.0.join("missing")).unwrap();
        saver.save_if_changed().await;
        assert_eq!(read(&missing).unwrap().unwrap().len(), 1);
    }
}
