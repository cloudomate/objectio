//! ObjectIO all-in-one — one command, one Ctrl-C, one cluster.
//!
//! Runs meta + OSD + gateway as child processes under a single parent.
//! Defaults are tuned for quick testing, not production:
//!
//!   - **1 meta, 1 OSD, 1 gateway** — minimum functional cluster.
//!   - **replication_count = 1** (no redundancy) — maximises speed,
//!     the user is expected to understand the disk is the single point
//!     of failure.
//!   - **Ephemeral internal ports** — meta and OSD bind to OS-picked
//!     ports on 127.0.0.1; the gateway's S3 endpoint is the only
//!     user-facing port (default :9000, overridable with --port).
//!   - **Auto-bootstraps Raft** — single-voter cluster on meta-0, no
//!     `POST /init` required.
//!   - **Tempdir data by default** — cleared when the process exits.
//!     Pass `--data <path>` to persist.
//!
//! On Ctrl-C the parent sends SIGTERM to every child, waits up to 5s
//! for graceful shutdown, and SIGKILLs anything still standing.

use std::ffi::OsString;
use std::net::TcpListener as StdTcpListener;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{info, warn};

#[derive(Parser, Debug)]
#[command(name = "objectio-aio")]
#[command(version)]
#[command(about = "ObjectIO all-in-one — single-command local cluster for testing.")]
struct Args {
    /// Public port for the S3 / Iceberg / Delta Sharing / console
    /// endpoint. The only port the caller needs to know about.
    #[arg(long, default_value_t = 9000)]
    port: u16,

    /// Root data directory. All meta + OSD state lives under
    /// `<data>/meta` and `<data>/osd-*`. Empty = tempdir (wiped on exit).
    #[arg(long)]
    data: Option<PathBuf>,

    /// Number of OSDs to spawn. Defaults to 1 for the replication=1
    /// quick-test pool; use 3+ if you switch the pool to 2+1 EC.
    #[arg(long, default_value_t = 1)]
    osds: usize,

    /// Log level for the aio supervisor AND for every child service
    /// (child processes inherit via RUST_LOG env var).
    #[arg(long, default_value = "info")]
    log_level: String,

    /// Disable the auto-fallback on port conflict and fail loudly
    /// instead. Use this in CI where an unexpected port change should
    /// block the run.
    #[arg(long)]
    strict_port: bool,
}

fn install_tracing(level: &str) {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| format!("objectio_aio={level},warn").into());
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .init();
}

/// Pick a free loopback port by binding to :0, noting the OS-assigned
/// port, and releasing the listener. The OS may hand the same port to
/// another process in the race window — fine for our purposes since
/// the subsequent child binds to it within milliseconds.
fn ephemeral_port() -> Result<u16> {
    let l = StdTcpListener::bind("127.0.0.1:0").context("bind 127.0.0.1:0")?;
    let port = l.local_addr()?.port();
    drop(l);
    Ok(port)
}

/// Check if a port is currently free on all interfaces. Used to detect
/// conflicts on the user-facing gateway port before we try to spawn.
fn port_free(port: u16) -> bool {
    StdTcpListener::bind(("0.0.0.0", port)).is_ok()
}

/// Find an ObjectIO service binary. In normal deployments each lives
/// alongside the aio binary in PATH or in the same directory; in
/// `cargo run -p objectio-aio` they sit under `target/debug/`.
fn find_binary(name: &str) -> Result<PathBuf> {
    // 1. Same directory as the current exe — the "cargo install"
    //    case and the Dockerfile `all` stage.
    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    // 2. PATH lookup.
    if let Ok(p) = which::which(name) {
        return Ok(p);
    }
    Err(anyhow!(
        "could not locate `{name}` binary next to objectio-aio or on PATH"
    ))
}

/// A spawned child service, plus the piped stdout/stderr pumps.
struct Service {
    name: &'static str,
    child: Child,
}

impl Service {
    async fn spawn(
        name: &'static str,
        binary: &Path,
        args: &[OsString],
        env: &[(&str, String)],
    ) -> Result<Self> {
        let mut cmd = Command::new(binary);
        cmd.args(args)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        for (k, v) in env {
            cmd.env(k, v);
        }
        let mut child = cmd
            .spawn()
            .with_context(|| format!("spawn {name} ({})", binary.display()))?;

        // Prefix every output line with the service name so the
        // combined stderr stream stays readable.
        if let Some(out) = child.stdout.take() {
            tokio::spawn(pump(name, "out", out));
        }
        if let Some(err) = child.stderr.take() {
            tokio::spawn(pump(name, "err", err));
        }

        Ok(Service { name, child })
    }

    /// SIGTERM the child, wait up to 5s, then SIGKILL.
    async fn shutdown(mut self) {
        use tokio::time::timeout;
        let pid = self.child.id();
        info!("shutdown: {} (pid {pid:?})", self.name);
        #[cfg(unix)]
        {
            if let Some(id) = pid {
                let _ = nix_kill(id as i32, 15); // SIGTERM
            }
        }
        match timeout(Duration::from_secs(5), self.child.wait()).await {
            Ok(Ok(status)) => info!("{}: exit {status}", self.name),
            _ => {
                warn!("{}: SIGTERM timed out, killing", self.name);
                let _ = self.child.kill().await;
            }
        }
    }
}

#[cfg(unix)]
fn nix_kill(pid: i32, sig: i32) -> std::io::Result<()> {
    use std::os::raw::c_int;
    unsafe extern "C" {
        fn kill(pid: c_int, sig: c_int) -> c_int;
    }
    let rc = unsafe { kill(pid, sig) };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

async fn pump<R: tokio::io::AsyncRead + Unpin>(svc: &'static str, kind: &'static str, rdr: R) {
    let mut lines = BufReader::new(rdr).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        // Child already includes its own timestamp + level. Keep the
        // prefix short so it doesn't dominate.
        eprintln!("[{svc}/{kind}] {line}");
    }
}

/// Write a minimal TOML config for one of the service binaries, into
/// the given directory. Returns the config file's path.
fn write_config(dir: &Path, name: &str, body: &str) -> Result<PathBuf> {
    std::fs::create_dir_all(dir)?;
    let path = dir.join(format!("{name}.toml"));
    std::fs::write(&path, body)?;
    Ok(path)
}

/// Poll a port until something's listening on it, up to `max` seconds.
/// Fails loudly if the service never opens its socket.
async fn wait_listening(port: u16, label: &str, max: u64) -> Result<()> {
    for i in 0..(max * 10) {
        // Probe — a successful connect means a server answered.
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        if i % 10 == 0 {
            info!("waiting for {label} on :{port}…");
        }
        sleep(Duration::from_millis(100)).await;
    }
    Err(anyhow!("{label} did not start listening on :{port} within {max}s"))
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    install_tracing(&args.log_level);

    // Resolve the data dir (persistent vs tempdir) — keep the temp
    // handle alive so it doesn't disappear while the cluster runs.
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

    // Resolve the user-facing gateway port. If it's in use: either
    // fail (--strict-port) or fall back to an ephemeral port and tell
    // the user loudly.
    let gateway_port = if port_free(args.port) {
        args.port
    } else if args.strict_port {
        return Err(anyhow!(
            "port {} is already in use and --strict-port is set",
            args.port
        ));
    } else {
        let p = ephemeral_port()?;
        warn!(
            "port {} is already in use — S3 API will fall back to ephemeral port {p}. \
             Pass --strict-port to fail instead.",
            args.port
        );
        p
    };

    // Internal ports — always ephemeral.
    let meta_grpc = ephemeral_port()?;
    let meta_admin = ephemeral_port()?;
    let osd_base = ephemeral_port()?; // osd-0 uses this, osd-N uses +N

    // ------------------------------------------------------------
    // Locate child binaries alongside aio (same dir) or on PATH.
    // ------------------------------------------------------------
    let meta_bin = find_binary("objectio-meta")?;
    let osd_bin = find_binary("objectio-osd")?;
    let gw_bin = find_binary("objectio-gateway")?;

    // ------------------------------------------------------------
    // Write minimal configs so each child has something to read.
    // Each service supports CLI flags that override the config file,
    // so values passed as args win and the TOML just fills defaults.
    // ------------------------------------------------------------
    let meta_cfg = write_config(
        &data_root.join("meta"),
        "meta",
        &format!(
            "[meta]\nlisten = \"127.0.0.1:{meta_grpc}\"\ndata_dir = \"{}\"\n\
             [storage]\ndata_dir = \"{}\"\n",
            data_root.join("meta").display(),
            data_root.join("meta").display()
        ),
    )?;
    let gw_cfg = write_config(
        &data_root.join("gateway"),
        "gateway",
        "[gateway]\n",
    )?;

    // ------------------------------------------------------------
    // Spawn meta first. It auto-bootstraps a single-voter Raft when
    // no peers are configured on first boot, so we don't need to
    // POST /init from aio.
    // ------------------------------------------------------------
    let env_all = vec![
        ("RUST_LOG", format!("{},warn", args.log_level)),
        ("RUST_BACKTRACE", "1".into()),
    ];
    let env_all_refs: Vec<(&str, String)> = env_all
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();

    let services: Arc<Mutex<Vec<Service>>> = Arc::new(Mutex::new(Vec::new()));

    let meta_data = data_root.join("meta");
    std::fs::create_dir_all(&meta_data)?;
    let meta_args: Vec<OsString> = vec![
        "--config".into(),
        meta_cfg.clone().into(),
        "--listen".into(),
        format!("127.0.0.1:{meta_grpc}").into(),
        "--metrics-port".into(),
        "0".into(), // disabled / ephemeral
        "--admin-port".into(),
        meta_admin.to_string().into(),
        "--node-id".into(),
        "1".into(),
        "--data-dir".into(),
        meta_data.into(),
    ];
    let meta = Service::spawn("meta", &meta_bin, &meta_args, &env_all_refs).await?;
    services.lock().await.push(meta);
    wait_listening(meta_grpc, "meta gRPC", 20).await?;

    // Bootstrap Raft (single-voter cluster). The meta admin HTTP server
    // accepts POST /init with no body.
    init_raft(meta_admin).await?;

    // ------------------------------------------------------------
    // Spawn the OSDs.
    // ------------------------------------------------------------
    let mut osd_grpc_ports = Vec::new();
    for i in 0..args.osds {
        let port = osd_base + i as u16;
        let osd_dir = data_root.join(format!("osd-{i}"));
        std::fs::create_dir_all(osd_dir.join("disk0"))?;
        std::fs::create_dir_all(osd_dir.join("state"))?;
        let disk = osd_dir.join("disk0/disk.raw");
        let state = osd_dir.join("state");
        let osd_cfg = write_config(
            &osd_dir,
            "osd",
            &format!(
                "[osd]\nlisten = \"127.0.0.1:{port}\"\n\
                 meta_endpoint = \"http://127.0.0.1:{meta_grpc}\"\n\
                 data_dir = \"{}\"\n\
                 [storage]\ndata_dir = \"{}\"\ndisks = [\"{}\"]\n\
                 block_size = 4194304\ndisk_file = \"disk.raw\"\n",
                state.display(),
                state.display(),
                disk.display(),
            ),
        )?;
        let osd_args: Vec<OsString> = vec![
            "--config".into(),
            osd_cfg.into(),
            "--listen".into(),
            format!("127.0.0.1:{port}").into(),
            "--meta-endpoint".into(),
            format!("http://127.0.0.1:{meta_grpc}").into(),
            "--data-dir".into(),
            state.clone().into(),
            "--disks".into(),
            disk.into(),
        ];
        let osd = Service::spawn(
            Box::leak(format!("osd-{i}").into_boxed_str()),
            &osd_bin,
            &osd_args,
            &env_all_refs,
        )
        .await?;
        services.lock().await.push(osd);
        wait_listening(port, &format!("osd-{i}"), 20).await?;
        osd_grpc_ports.push(port);
    }

    // ------------------------------------------------------------
    // Spawn the gateway.
    // ------------------------------------------------------------
    let gw_args: Vec<OsString> = vec![
        "--config".into(),
        gw_cfg.into(),
        "--listen".into(),
        format!("0.0.0.0:{gateway_port}").into(),
        "--meta-endpoint".into(),
        format!("http://127.0.0.1:{meta_grpc}").into(),
        "--external-endpoint".into(),
        format!("http://localhost:{gateway_port}").into(),
        "--no-auth".into(), // aio quick-test defaults to no-auth for convenience
    ];
    let gw = Service::spawn("gateway", &gw_bin, &gw_args, &env_all_refs).await?;
    services.lock().await.push(gw);
    wait_listening(gateway_port, "gateway", 20).await?;

    // Banner. Grep-friendly and copy-pasteable.
    eprintln!();
    eprintln!("━━━ ObjectIO ready ━━━");
    eprintln!("  S3 / Iceberg / Delta Sharing : http://localhost:{gateway_port}");
    eprintln!("  Console                       : http://localhost:{gateway_port}/_console/");
    eprintln!("  Data directory                : {}", data_root.display());
    eprintln!("  Internals (loopback only)     : meta=:{meta_grpc} osd={osd_grpc_ports:?}");
    eprintln!();
    eprintln!("  aws --endpoint-url http://localhost:{gateway_port} --no-sign-request s3 mb s3://test");
    eprintln!();

    // ------------------------------------------------------------
    // Wait for Ctrl-C, then shut down every child in reverse order.
    // ------------------------------------------------------------
    tokio::signal::ctrl_c().await.context("ctrl-c handler")?;
    info!("shutdown requested");
    let mut svcs = services.lock().await;
    while let Some(s) = svcs.pop() {
        s.shutdown().await;
    }

    Ok(())
}

/// POST http://127.0.0.1:{port}/init with an empty JSON body.
async fn init_raft(port: u16) -> Result<()> {
    // Wait for the admin server to come up (it binds AFTER the Raft
    // handle is constructed, which is AFTER the gRPC server starts).
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let body = b"{}".as_ref();
    let req = format!(
        "POST /init HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    use tokio::io::AsyncWriteExt;
    let mut s = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .context("connect raft admin")?;
    s.write_all(req.as_bytes()).await?;
    s.write_all(body).await?;
    let mut resp = Vec::with_capacity(512);
    let mut buf = [0u8; 512];
    loop {
        match tokio::io::AsyncReadExt::read(&mut s, &mut buf).await {
            Ok(0) => break,
            Ok(n) => resp.extend_from_slice(&buf[..n]),
            Err(_) => break,
        }
    }
    let head = String::from_utf8_lossy(&resp[..resp.len().min(128)]);
    if head.starts_with("HTTP/1.1 200") || head.starts_with("HTTP/1.1 409") {
        // 200 = we just bootstrapped. 409 = already initialised (persisted
        // Raft state from a previous aio run using the same --data dir).
        info!("raft bootstrap: {}", head.lines().next().unwrap_or(""));
        Ok(())
    } else {
        Err(anyhow!(
            "raft bootstrap failed — response: {}",
            head.lines().next().unwrap_or("<empty>")
        ))
    }
}
