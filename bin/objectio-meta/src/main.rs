//! ObjectIO Metadata Service — standalone binary entrypoint.
//!
//! Parses CLI args, installs the global tracing subscriber, and calls
//! into the library crate. The heavy lifting lives in `lib.rs` so
//! `bin/objectio-aio` can re-use it in-process.

use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = objectio_meta::Args::parse();

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| args.log_level.clone().into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    objectio_meta::run(args, async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await
}
