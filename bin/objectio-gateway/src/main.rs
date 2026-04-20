//! ObjectIO Gateway — standalone binary entrypoint.
//!
//! Parses CLI, installs the global tracing subscriber, and hands off
//! to `objectio_gateway::run()`. The same `run()` is invoked in-
//! process by `bin/objectio-aio` for the monolithic dev mode.

use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = objectio_gateway::Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    objectio_gateway::run(args, async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await
}
