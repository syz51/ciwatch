use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::task::JoinSet;

use ciwatch::config::Config;
use ciwatch::pools::Pools;
use ciwatch::store::Store;
use ciwatch::web::{AppState, router, webhook_router};
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
    let (app, hooks) = match webhook_listen {
        Some(addr) => {
            let page = AppState {
                webhook_secret: None,
                ..state.clone()
            };
            (router(page), Some((addr, webhook_router(state))))
        }
        None => (router(state), None),
    };

    let (stop_tx, stop_rx) = tokio::sync::watch::channel(());
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = stop_tx.send(());
    });
    let stopped = |mut rx: tokio::sync::watch::Receiver<()>| async move {
        let _ = rx.changed().await;
    };

    let mut servers = JoinSet::new();
    for (addr, app) in std::iter::once((config.listen, app)).chain(hooks) {
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("binding {addr}"))?;
        tracing::info!(listen = %addr, "ciwatch serving");
        let stop = stopped(stop_rx.clone());
        servers.spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(stop)
                .await
                .with_context(|| format!("serving HTTP on {addr}"))
        });
    }

    // Servers return only after shutdown or on error; background tasks loop
    // forever, so one ending means it panicked.
    let served = tokio::select! {
        Some(result) = servers.join_next() => {
            let result = result.context("HTTP server task").and_then(|r| r);
            if result.is_ok() {
                // Shutdown: let the other listener finish gracefully too.
                while let Some(other) = servers.join_next().await {
                    other.context("HTTP server task").and_then(|r| r)?;
                }
            } else {
                servers.abort_all();
            }
            result
        }
        Some(ended) = tasks.join_next() => {
            Err(anyhow::anyhow!("a background task stopped: {ended:?}"))
        }
    };
    tasks.shutdown().await;
    served
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
