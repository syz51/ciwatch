use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::task::JoinSet;

use ciwatch::config::Config;
use ciwatch::pools::Pools;
use ciwatch::server::{self, Limits};
use ciwatch::state_file::{self, Saver};
use ciwatch::store::Store;
use ciwatch::web::{AppState, page_router, router, webhook_router};
use ciwatch::{circleci, github, http_client};

/// One live view of CI jobs across GitHub Actions, CircleCI and runner pools.
#[derive(Parser)]
#[command(version)]
struct Args {
    /// Path to the TOML configuration file.
    #[arg(short, long, default_value = "ciwatch.toml")]
    config: PathBuf,
    /// Validate the configuration and exit.
    #[arg(long)]
    check: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    if args.check {
        println!("configuration OK");
        return Ok(());
    }

    let store = Arc::new(Store::new(config.retention));
    // Restore before the pollers start, so they refresh restored active runs.
    let mut saver = config.state_file.clone().map(|path| {
        let saver = Saver::new(path.clone(), store.clone());
        state_file::load(&path, &store, chrono::Utc::now(), |job| config.polls(job));
        let (stop, stopped) = tokio::sync::oneshot::channel();
        (stop, tokio::spawn(saver.run(stopped)))
    });
    let pools = Arc::new(Pools::new(config.pools.clone()));
    let client = http_client().context("building HTTP client")?;
    let mut tasks = JoinSet::new();

    let mut webhook_secret = None;
    let mut webhook_listen = None;
    if let Some(gh) = &config.github {
        webhook_listen = gh.webhook_listen;
        if let Some(secret) = &gh.webhook_secret {
            store.register_source(github::SOURCE_WEBHOOK);
            webhook_secret = Some(Arc::from(secret.as_bytes()));
        }
        if let Some(poll) = &gh.poll {
            store.register_source(github::SOURCE_POLL);
            let poller = github::Poller::new(client.clone(), poll.clone(), store.clone(), pools.clone());
            tasks.spawn(ciwatch::poll::run(poller));
        }
    }
    if let Some(cc) = &config.circleci {
        store.register_source(circleci::SOURCE);
        let poller = circleci::Poller::new(client.clone(), cc.clone(), store.clone(), pools.clone());
        tasks.spawn(ciwatch::poll::run(poller));
    }
    let pruner = store.clone();
    tasks.spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(60));
        loop {
            ticker.tick().await;
            pruner.prune(chrono::Utc::now());
        }
    });

    let state = AppState {
        store,
        pools,
        webhook_secret,
    };
    // With a separate webhook listener, the main one serves no webhook.
    let listeners = match webhook_listen {
        Some(addr) => vec![
            (config.listen, page_router(state.clone())),
            (addr, webhook_router(state)),
        ],
        None => vec![(config.listen, router(state))],
    };

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        stop_tx.send_replace(true);
    });
    let stopped = |mut rx: tokio::sync::watch::Receiver<bool>| async move {
        if rx.wait_for(|stop| *stop).await.is_err() {
            tracing::error!("the shutdown signal task ended without a signal; shutting down");
        }
    };

    let mut servers = JoinSet::new();
    for (addr, app) in listeners {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding {addr}"))?;
        tracing::info!(listen = %addr, "ciwatch serving");
        servers.spawn(server::serve(
            listener,
            app,
            Limits::default(),
            stopped(stop_rx.clone()),
        ));
    }

    // Servers return only after shutdown; background tasks loop forever; the
    // saver, kept out of `tasks` so that it is never aborted mid-write, runs
    // until its stop signal. Any of them ending otherwise means it panicked.
    let saver_ended = async {
        match &mut saver {
            Some((_, handle)) => handle.await,
            None => std::future::pending().await,
        }
    };
    let mut result = tokio::select! {
        Some(served) = servers.join_next() => served.context("HTTP server task"),
        Some(ended) = tasks.join_next() => Err(anyhow::anyhow!("a background task stopped: {ended:?}")),
        ended = saver_ended => {
            saver = None;
            Err(anyhow::anyhow!("the state file saver stopped: {ended:?}"))
        }
    };
    if result.is_ok() {
        // Shutdown: the other listener drains too.
        while let Some(other) = servers.join_next().await {
            if let Err(e) = other {
                result = Err(anyhow::Error::new(e).context("HTTP server task"));
            }
        }
    } else {
        servers.abort_all();
    }
    tasks.shutdown().await;
    // Last, so the final save holds what the draining listeners received.
    if let Some((stop, saver)) = saver {
        let _ = stop.send(());
        if let Err(e) = saver.await {
            tracing::error!(error = %e, "the state file saver failed");
        }
    }
    result
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!(error = %e, "cannot listen for Ctrl-C");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutting down");
}
