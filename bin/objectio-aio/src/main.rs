//! ObjectIO all-in-one — the real monolith.
//!
//! Runs meta + OSD(s) + gateway as tokio tasks inside **one**
//! process. Shares a single tracing subscriber, a single Ctrl-C
//! handler, and a single memory space. Built on top of each
//! service's `lib.rs::run(args, shutdown)` entrypoint.
//!
//! Still UX-identical to the supervisor version: one binary, one
//! command, one log, one Ctrl-C. What changes is:
//!
//!   - Zero subprocess overhead; tasks share the tokio runtime.
//!   - Inter-service RPC is still gRPC on loopback (we didn't
//!     strip the tonic layer — that's a bigger surgery). So the
//!     behavior against the wire protocol is identical to prod.
//!   - Port conflicts affect only the one user-facing gateway port.
//!     Internal ports are OS-assigned ephemeral.
//!
//! Defaults tuned for quick testing:
//!   - 1 meta (single-voter Raft), 1 OSD, 1 gateway
//!   - replication = 1 (no redundancy)
//!   - tempdir data (wiped on exit) unless --data is passed

use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(name = "objectio-aio")]
#[command(version)]
#[command(about = "ObjectIO all-in-one — single-process local cluster for testing.")]
struct Args {
    /// Public port for the S3 / Iceberg / Delta Sharing / console
    /// endpoint. The only port the caller needs to know about.
    #[arg(long, default_value_t = 9000)]
    port: u16,

    /// Root data directory. Meta + each OSD get subdirs. Empty =
    /// tempdir (wiped on exit).
    #[arg(long)]
    data: Option<PathBuf>,

    /// Number of OSDs to spawn. Defaults to 1 for the replication=1
    /// quick-test pool; use 3+ for 2+1 EC.
    #[arg(long, default_value_t = 1)]
    osds: usize,

    /// Log level. Honored by RUST_LOG if present; otherwise this is
    /// used as the global default.
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Fail fast on --port conflict instead of falling back to an
    /// ephemeral port.
    #[arg(long)]
    strict_port: bool,
}

/// Pick a free loopback port by binding to :0 and releasing.
fn ephemeral_port() -> Result<u16> {
    let l = StdTcpListener::bind("127.0.0.1:0").context("bind 127.0.0.1:0")?;
    let port = l.local_addr()?.port();
    drop(l);
    Ok(port)
}

fn port_free(port: u16) -> bool {
    StdTcpListener::bind(("0.0.0.0", port)).is_ok()
}

/// Listen for Ctrl-C on the task that owns main, then broadcast
/// shutdown to every subsystem. Returns when broadcast fires.
async fn wait_ctrl_c(tx: broadcast::Sender<()>) {
    if tokio::signal::ctrl_c().await.is_ok() {
        info!("shutdown requested");
        let _ = tx.send(());
    }
}

/// Build a broadcast-receiver → Output=() future for a subsystem.
fn sub_shutdown(tx: &broadcast::Sender<()>) -> impl std::future::Future<Output = ()> + Send + 'static {
    let mut rx = tx.subscribe();
    async move {
        let _ = rx.recv().await;
    }
}

/// Poll a port until something is listening there. Used to sequence
/// start-up: OSD can't register until meta is up, gateway can't
/// resolve buckets until OSD is up.
async fn wait_listening(port: u16, label: &str, max: u64) -> Result<()> {
    for i in 0..(max * 10) {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        if i % 10 == 0 {
            info!("waiting for {label} on :{port}…");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("{label} did not listen on :{port} within {max}s"))
}

/// POST /init to the meta raft admin port so the single-voter cluster
/// bootstraps itself. 200 = first-time init, 409 = already done.
async fn bootstrap_raft(port: u16) -> Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let body = b"{}".as_ref();
    let req = format!(
        "POST /init HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .context("raft admin connect")?;
    s.write_all(req.as_bytes()).await?;
    s.write_all(body).await?;
    let mut resp = Vec::with_capacity(512);
    let mut buf = [0u8; 512];
    loop {
        match s.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => resp.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let head = String::from_utf8_lossy(&resp[..resp.len().min(128)]);
    if head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.1 409") {
        info!("raft bootstrap: {}", head.lines().next().unwrap_or(""));
        Ok(())
    } else {
        Err(anyhow!(
            "raft bootstrap failed: {}",
            head.lines().next().unwrap_or("<empty>")
        ))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Single tracing subscriber for everyone.
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| format!("{},warn", args.log_level).into()),
        )
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();

    // Data root.
    let (data_root, _tempdir) = match args.data {
        Some(p) => (p, None),
        None => {
            let td = tempfile::tempdir().context("tempdir")?;
            info!(
                "no --data given, using temp dir {} (wiped on exit)",
                td.path().display()
            );
            (td.path().to_path_buf(), Some(td))
        }
    };
    std::fs::create_dir_all(&data_root)?;

    // Gateway port — the one user-facing knob.
    let gateway_port = if port_free(args.port) {
        args.port
    } else if args.strict_port {
        return Err(anyhow!(
            "port {} in use and --strict-port set",
            args.port
        ));
    } else {
        let p = ephemeral_port()?;
        warn!(
            "port {} in use — S3 API falling back to ephemeral port {p}. \
             Pass --strict-port to fail instead.",
            args.port
        );
        p
    };

    let meta_grpc = ephemeral_port()?;
    let meta_admin = ephemeral_port()?;
    let osd_base = ephemeral_port()?;

    // Shutdown broadcast — every subsystem subscribes, Ctrl-C fans out.
    let (shutdown_tx, _) = broadcast::channel::<()>(4);

    // ------------------------------------------------------------
    // Meta — single-voter Raft, auto-bootstrapped below.
    //
    // We build each service's Args via parse_from() with a synthetic
    // argv so aio stays insulated from new Args fields downstream —
    // any field we don't pass keeps its clap default_value, which is
    // exactly what standalone meta/osd/gateway would use when run
    // without that flag.
    // ------------------------------------------------------------
    let meta_data = data_root.join("meta");
    std::fs::create_dir_all(&meta_data)?;
    let meta_args = <objectio_meta::Args as clap::Parser>::parse_from([
        "objectio-meta",
        "--node-id", "1",
        "--listen", &format!("127.0.0.1:{meta_grpc}"),
        "--replication", "1",
        "--data-dir", &meta_data.display().to_string(),
        "--metrics-port", "0",
        "--admin-port", &meta_admin.to_string(),
        "--log-level", &args.log_level,
    ]);
    let meta_handle: JoinHandle<Result<()>> = {
        let sd = sub_shutdown(&shutdown_tx);
        tokio::spawn(async move { objectio_meta::run(meta_args, sd).await })
    };
    wait_listening(meta_grpc, "meta gRPC", 20).await?;
    bootstrap_raft(meta_admin).await?;

    // ------------------------------------------------------------
    // OSDs.
    // ------------------------------------------------------------
    let mut osd_handles: Vec<JoinHandle<Result<()>>> = Vec::new();
    let mut osd_ports: Vec<u16> = Vec::new();
    for i in 0..args.osds {
        let port = osd_base + i as u16;
        let osd_dir = data_root.join(format!("osd-{i}"));
        std::fs::create_dir_all(osd_dir.join("disk0"))?;
        std::fs::create_dir_all(&osd_dir.join("state"))?;
        let disk = osd_dir.join("disk0/disk.raw");
        let state = osd_dir.join("state");

        let osd_args = <objectio_osd::Args as clap::Parser>::parse_from([
            "objectio-osd",
            "--listen", &format!("127.0.0.1:{port}"),
            "--meta-endpoint", &format!("http://127.0.0.1:{meta_grpc}"),
            "--data-dir", &state.display().to_string(),
            "--disks", &disk.display().to_string(),
            "--advertise-addr", &format!("http://127.0.0.1:{port}"),
            "--log-level", &args.log_level,
        ]);
        let sd = sub_shutdown(&shutdown_tx);
        osd_handles.push(tokio::spawn(async move {
            objectio_osd::run(osd_args, sd).await
        }));
        wait_listening(port, &format!("osd-{i}"), 20).await?;
        osd_ports.push(port);
    }

    // ------------------------------------------------------------
    // Gateway.
    // ------------------------------------------------------------
    let gw_args = <objectio_gateway::Args as clap::Parser>::parse_from([
        "objectio-gateway",
        "--listen", &format!("0.0.0.0:{gateway_port}"),
        "--meta-endpoint", &format!("http://127.0.0.1:{meta_grpc}"),
        "--external-endpoint", &format!("http://localhost:{gateway_port}"),
        "--no-auth",
        "--log-level", &args.log_level,
    ]);
    let gw_handle: JoinHandle<Result<()>> = {
        let sd = sub_shutdown(&shutdown_tx);
        tokio::spawn(async move { objectio_gateway::run(gw_args, sd).await })
    };
    wait_listening(gateway_port, "gateway", 20).await?;

    // Banner.
    let addr: SocketAddr = format!("127.0.0.1:{gateway_port}").parse()?;
    eprintln!();
    eprintln!("━━━ ObjectIO ready ━━━");
    eprintln!("  S3 / Iceberg / Delta Sharing : http://{addr}");
    eprintln!("  Console                       : http://{addr}/_console/");
    eprintln!("  Data directory                : {}", data_root.display());
    eprintln!(
        "  Internals (loopback only)     : meta=:{meta_grpc} osd={osd_ports:?} \
         (monolith — same process)"
    );
    eprintln!();
    eprintln!(
        "  aws --endpoint-url http://localhost:{gateway_port} --no-sign-request \
         s3 mb s3://test"
    );
    eprintln!();

    // Wait for Ctrl-C → broadcast shutdown → join every task.
    wait_ctrl_c(shutdown_tx.clone()).await;

    // Collect results; warn on any task that errored, don't propagate
    // (best-effort shutdown).
    for (label, handle) in [("meta", meta_handle), ("gateway", gw_handle)]
        .into_iter()
        .chain(
            osd_handles
                .into_iter()
                .enumerate()
                .map(|(i, h)| (Box::leak(format!("osd-{i}").into_boxed_str()) as &str, h)),
        )
    {
        match handle.await {
            Ok(Ok(())) => info!("{label} exited cleanly"),
            Ok(Err(e)) => warn!("{label} error: {e}"),
            Err(e) => warn!("{label} join error: {e}"),
        }
    }
    Ok(())
}
