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

use std::io::Read;
use std::net::TcpListener as StdTcpListener;
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

    /// Interface the S3 endpoint binds to. Defaults to `0.0.0.0`
    /// so the cluster is reachable from other hosts on the LAN —
    /// pass `127.0.0.1` to restrict to loopback (useful for CI
    /// runners or shared dev boxes).
    #[arg(long, default_value = "0.0.0.0")]
    listen_addr: String,

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

fn port_free(addr: &str, port: u16) -> bool {
    StdTcpListener::bind((addr, port)).is_ok()
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
fn sub_shutdown(
    tx: &broadcast::Sender<()>,
) -> impl std::future::Future<Output = ()> + Send + 'static {
    let mut rx = tx.subscribe();
    async move {
        let _ = rx.recv().await;
    }
}

/// Poll a closure every 100ms until it returns `Some`, or give up
/// after `max`. Used by the admin-creds reader since meta writes
/// the file asynchronously after start-up.
async fn for_a_moment<T>(mut f: impl FnMut() -> Option<T>, max: Duration) -> Option<T> {
    let start = std::time::Instant::now();
    while start.elapsed() < max {
        if let Some(v) = f() {
            return Some(v);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

/// Parse KEY=VAL lines from meta's admin-creds.env.
fn parse_creds(path: &std::path::Path) -> Option<(String, String)> {
    let body = std::fs::read_to_string(path).ok()?;
    let mut ak = None;
    let mut sk = None;
    for line in body.lines() {
        let line = line.trim_start_matches("export ").trim();
        if let Some((k, v)) = line.split_once('=') {
            match k {
                "AWS_ACCESS_KEY_ID" => ak = Some(v.trim().to_string()),
                "AWS_SECRET_ACCESS_KEY" => sk = Some(v.trim().to_string()),
                _ => {}
            }
        }
    }
    match (ak, sk) {
        (Some(a), Some(s)) => Some((a, s)),
        _ => None,
    }
}

/// Poll a port until something is listening there. Used to sequence
/// start-up: OSD can't register until meta is up, gateway can't
/// resolve buckets until OSD is up.
async fn wait_listening(port: u16, label: &str, max: u64) -> Result<()> {
    for i in 0..(max * 10) {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        if i % 10 == 0 {
            info!("waiting for {label} on :{port}…");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("{label} did not listen on :{port} within {max}s"))
}

/// Minimal HTTP GET/POST over a one-shot TCP connection. Returns (status_line, body).
async fn http_oneshot(
    port: u16,
    method: &str,
    path: &str,
    body: &[u8],
) -> Result<(String, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let req = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .context("raft admin connect")?;
    s.write_all(req.as_bytes()).await?;
    if !body.is_empty() {
        s.write_all(body).await?;
    }
    let mut resp = Vec::with_capacity(1024);
    let mut buf = [0u8; 1024];
    loop {
        match s.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => resp.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let raw = String::from_utf8_lossy(&resp).into_owned();
    let status = raw.lines().next().unwrap_or("").to_string();
    let body_start = raw.find("\r\n\r\n").map(|i| i + 4).unwrap_or(raw.len());
    let body = raw[body_start..].to_string();
    Ok((status, body))
}

/// Bootstrap the meta raft cluster as a single voter. Idempotent: skips
/// `/init` when `/status` shows voters already present (warm restart).
async fn bootstrap_raft(port: u16) -> Result<()> {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Warm-restart shortcut: if /status reports any voters, the cluster
    // is already initialized — skip /init to avoid openraft's internal
    // "Can not initialize" ERROR log on every restart.
    if let Ok((status, body)) = http_oneshot(port, "GET", "/status", &[]).await
        && status.starts_with("HTTP/1.1 200")
        && body.contains("\"voters\"")
        && !body.contains("\"voters\":[]")
    {
        info!("raft already initialized — skipping /init");
        return Ok(());
    }
    let (status, _body) = http_oneshot(port, "POST", "/init", b"{}").await?;
    if status.starts_with("HTTP/1.1 200") || status.starts_with("HTTP/1.1 409") {
        info!("raft bootstrap: {}", status);
        Ok(())
    } else {
        Err(anyhow!("raft bootstrap failed: {}", status))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Single tracing subscriber for everyone. Default: --log-level for
    // our own crates, warn for the noisy deps (hyper, h2, tower, tonic,
    // openraft internals). Override via RUST_LOG — e.g.
    //   RUST_LOG=debug          everything
    //   RUST_LOG=objectio=debug just our crates
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!(
                    "{lvl},hyper=warn,h2=warn,tower=warn,tonic=warn,\
                     openraft::raft::core=warn,\
                     tokio_util=warn,reqwest=warn",
                    lvl = args.log_level
                )
                .into()
            }),
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

    // Gateway port — the one user-facing knob. Check availability
    // on the requested bind address (not just 0.0.0.0), so e.g.
    // --listen-addr 127.0.0.1 --port 9000 doesn't fight with another
    // process that bound 0.0.0.0:9000 (which would block loopback).
    let gateway_port = if port_free(&args.listen_addr, args.port) {
        args.port
    } else if args.strict_port {
        return Err(anyhow!(
            "{}:{} in use and --strict-port set",
            args.listen_addr,
            args.port
        ));
    } else {
        let p = ephemeral_port()?;
        warn!(
            "{}:{} in use — S3 API falling back to ephemeral port {p}. \
             Pass --strict-port to fail instead.",
            args.listen_addr, args.port
        );
        p
    };

    let meta_grpc = ephemeral_port()?;
    let meta_admin = ephemeral_port()?;
    let osd_base = ephemeral_port()?;

    // ------------------------------------------------------------
    // SSE master key. Gateway's bin logs a scary warning when this
    // env var isn't set and then generates an in-memory key that
    // renders every SSE-encrypted object unreadable across restart.
    // For aio we persist one in the data root so SSE just works —
    // subsequent runs against the same --data dir reuse it, temp-
    // dir runs get a fresh key (and nothing to decrypt post-reset).
    // ------------------------------------------------------------
    let master_key_path = data_root.join("master_key");
    let master_key_b64 = match std::fs::read_to_string(&master_key_path) {
        Ok(s) if s.trim().len() == 44 => s.trim().to_string(),
        _ => {
            use std::io::Write;
            let mut k = [0u8; 32];
            // Minimal RNG — we already depend on `tempfile` which pulls
            // `rand`, but we avoid it at this layer to stay dep-light.
            // Use /dev/urandom (macOS + linux).
            std::fs::File::open("/dev/urandom")
                .context("open /dev/urandom")?
                .read(&mut k)
                .context("read /dev/urandom")?;
            use base64::Engine;
            let b64 = base64::engine::general_purpose::STANDARD.encode(k);
            let mut f = std::fs::File::create(&master_key_path)?;
            f.write_all(b64.as_bytes())?;
            info!("Generated SSE master key at {}", master_key_path.display());
            b64
        }
    };
    // SAFETY: setting env vars is unsafe in Rust 2024 because another
    // thread might be reading them concurrently. We're still on the
    // single main thread here — no other tasks have been spawned yet.
    unsafe {
        std::env::set_var("OBJECTIO_MASTER_KEY", &master_key_b64);
    }

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
        "--node-id",
        "1",
        "--listen",
        &format!("127.0.0.1:{meta_grpc}"),
        "--replication",
        "1",
        "--data-dir",
        &meta_data.display().to_string(),
        "--metrics-port",
        "0",
        "--admin-port",
        &meta_admin.to_string(),
        "--log-level",
        &args.log_level,
    ]);
    let meta_handle: JoinHandle<Result<()>> = {
        let sd = sub_shutdown(&shutdown_tx);
        tokio::spawn(async move { objectio_meta::run(meta_args, sd).await })
    };
    wait_listening(meta_grpc, "meta gRPC", 20).await?;
    bootstrap_raft(meta_admin).await?;

    // Meta writes admin-creds.env to its data dir on first boot (and
    // on every subsequent boot when an admin user already exists).
    // Read it so we can surface AK/SK in the banner.
    let creds_path = meta_data.join("admin-creds.env");
    let (ak, sk) = for_a_moment(|| parse_creds(&creds_path), Duration::from_secs(5))
        .await
        .unwrap_or_default();

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
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--meta-endpoint",
            &format!("http://127.0.0.1:{meta_grpc}"),
            "--data-dir",
            &state.display().to_string(),
            "--disks",
            &disk.display().to_string(),
            "--advertise-addr",
            &format!("http://127.0.0.1:{port}"),
            "--log-level",
            &args.log_level,
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
    // Gateway binds on the caller's requested interface. external-endpoint
    // (used for OIDC callback + Iceberg vended creds + Delta Sharing
    // presign) stays as `localhost` when loopback-only; otherwise falls
    // back to the bind address for LAN-reachable mode.
    let external_host = if args.listen_addr == "127.0.0.1" || args.listen_addr == "localhost" {
        "localhost".to_string()
    } else {
        args.listen_addr.clone()
    };
    let gw_args = <objectio_gateway::Args as clap::Parser>::parse_from([
        "objectio-gateway",
        "--listen",
        &format!("{}:{}", args.listen_addr, gateway_port),
        "--meta-endpoint",
        &format!("http://127.0.0.1:{meta_grpc}"),
        "--external-endpoint",
        &format!("http://{external_host}:{gateway_port}"),
        "--no-auth",
        "--log-level",
        &args.log_level,
    ]);
    let gw_handle: JoinHandle<Result<()>> = {
        let sd = sub_shutdown(&shutdown_tx);
        tokio::spawn(async move { objectio_gateway::run(gw_args, sd).await })
    };
    wait_listening(gateway_port, "gateway", 20).await?;

    // Banner — show the address the caller can actually reach us at.
    let display_host = if args.listen_addr == "0.0.0.0" {
        // Bound to all interfaces: show localhost for the copy-paste
        // curl/aws hint, but also tell the user LAN access is on.
        "localhost".to_string()
    } else {
        args.listen_addr.clone()
    };
    eprintln!();
    eprintln!("━━━ ObjectIO ready ━━━");
    eprintln!("  S3 / Iceberg / Delta Sharing : http://{display_host}:{gateway_port}");
    eprintln!("  Console                       : http://{display_host}:{gateway_port}/_console/");
    if args.listen_addr == "0.0.0.0" {
        eprintln!(
            "  LAN bind                      : 0.0.0.0 — reachable from \
             other hosts on the network"
        );
    }
    eprintln!("  Data directory                : {}", data_root.display());
    eprintln!(
        "  Internals (loopback only)     : meta=:{meta_grpc} osd={osd_ports:?} \
         (monolith — same process)"
    );
    if !ak.is_empty() {
        eprintln!("  Admin access key              : {ak}");
        eprintln!("  Admin secret key              : {sk}");
    }
    eprintln!(
        "  SSE master key                : persisted at {} (set OBJECTIO_MASTER_KEY \
         to override)",
        data_root.join("master_key").display()
    );
    eprintln!();
    if !ak.is_empty() {
        eprintln!("  AWS_ACCESS_KEY_ID={ak} AWS_SECRET_ACCESS_KEY={sk} \\");
        eprintln!("    aws --endpoint-url http://{display_host}:{gateway_port} s3 mb s3://test");
    } else {
        eprintln!(
            "  aws --endpoint-url http://{display_host}:{gateway_port} --no-sign-request \
             s3 mb s3://test"
        );
    }
    eprintln!();

    // Wait for Ctrl-C → broadcast shutdown → join every task.
    wait_ctrl_c(shutdown_tx.clone()).await;

    // Second Ctrl-C = hard exit. Some background tasks (OSD chunk
    // flushes, meta raft machinery) occasionally refuse to yield in
    // under a couple of seconds; the user shouldn't have to wait.
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            eprintln!("\n[aio] second Ctrl-C — forcing exit");
            std::process::exit(130);
        }
    });

    // Collect results under a hard deadline. If a service refuses to
    // shut down within `graceful` seconds, we exit the process anyway
    // — leaking the task beats hanging the shell.
    let graceful = Duration::from_secs(5);
    let join_all = async {
        for (label, handle) in [("meta", meta_handle), ("gateway", gw_handle)]
            .into_iter()
            .chain(osd_handles.into_iter().enumerate().map(|(i, h)| {
                (Box::leak(format!("osd-{i}").into_boxed_str()) as &str, h)
            }))
        {
            match handle.await {
                Ok(Ok(())) => info!("{label} exited cleanly"),
                Ok(Err(e)) => warn!("{label} error: {e}"),
                Err(e) => warn!("{label} join error: {e}"),
            }
        }
    };
    match tokio::time::timeout(graceful, join_all).await {
        Ok(()) => info!("all services shut down cleanly"),
        Err(_) => eprintln!(
            "[aio] graceful shutdown timed out after {}s — exiting",
            graceful.as_secs()
        ),
    }
    // Explicitly exit. Services spawn detached background tasks
    // (metrics timers, openraft background workers, tonic keepalives)
    // that don't get joined; without this, the runtime keeps the
    // process alive even after every run() returned.
    std::process::exit(0);
}
