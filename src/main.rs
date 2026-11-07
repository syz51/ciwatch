use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use tokio::task::JoinSet;

use ciwatch::config::Config;
use ciwatch::pools::Pools;
use ciwatch::store::Store;
use ciwatch::web::{AppState, router};
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
    if let Some(gh) = &config.github {
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

    let app = router(AppState {
        store,
        pools,
        webhook_secret,
    });
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .with_context(|| format!("binding {}", config.listen))?;
    tracing::info!(listen = %config.listen, "ciwatch serving");

    // The background tasks loop forever; one ending means it panicked.
    let served = tokio::select! {
        result = axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()) => {
            result.context("serving HTTP")
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
