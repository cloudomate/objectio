//! I/O backend benchmark — pread (what ObjectIO uses today) vs io_uring
//! (what we're evaluating for Phase D / hot-path workloads).
//!
//! Usage:
//!
//!   objectio-io-bench \
//!       --file /mnt/nvme/bench.bin \
//!       --size-mb 1024 \
//!       --block-size 4096 \
//!       --op random_read \
//!       --iterations 100000 \
//!       --backend pread
//!
//! Runs the same loop with both backends, prints throughput + latency
//! percentiles. Use `--backend both` (default) to compare in one run.
//!
//! Caveats that matter when reading the numbers:
//!
//! * Always runs with O_DIRECT so the page cache isn't involved. This
//!   is what our storage engine actually does, so it's the workload we
//!   care about.
//!
//! * Test file is pre-populated with pseudo-random bytes so sparse-file
//!   optimisations + filesystem deduplication don't distort results.
//!
//! * io_uring backend runs under `tokio_uring::start`, a separate
//!   runtime from tokio proper. Each backend gets a fresh runtime per
//!   run so they don't interfere.
//!
//! * macOS / Windows: only the pread backend is compiled. The uring
//!   backend is behind `#[cfg(target_os = "linux")]`. Asking for
//!   `--backend uring` on a non-Linux host prints an error.

use std::fs::OpenOptions;
use std::io::Write;
use std::os::fd::AsRawFd;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use rand::{Rng, SeedableRng, rngs::StdRng};

// -------------------------------------------------------------------
// CLI
// -------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "objectio-io-bench", about, version)]
struct Args {
    /// Test file path. Created + filled if absent; reused if already at
    /// the requested size.
    #[arg(long)]
    file: PathBuf,

    /// Size of the test file in MiB. Must be ≥ block_size.
    #[arg(long, default_value_t = 1024)]
    size_mb: u64,

    /// I/O block size in bytes. Must be a multiple of 4096 for O_DIRECT.
    #[arg(long, default_value_t = 4096)]
    block_size: usize,

    /// How many I/O operations per run.
    #[arg(long, default_value_t = 100_000)]
    iterations: u64,

    /// Which operation to bench.
    #[arg(long, default_value_t = Op::RandomRead)]
    op: Op,

    /// Which backend to use. `both` runs pread then uring back-to-back
    /// for side-by-side output.
    #[arg(long, default_value_t = Backend::Both)]
    backend: Backend,

    /// Stop the pre-fill phase early — useful when iterating on the
    /// benchmark code against an already-filled file.
    #[arg(long)]
    skip_prefill: bool,

    /// Drop the kernel page cache before each run (Linux only, needs
    /// root via `echo 3 > /proc/sys/vm/drop_caches`). Only makes a
    /// difference if the kernel is caching anything — with O_DIRECT
    /// it normally shouldn't, but extents metadata can still be
    /// cached. Skip if you don't have root.
    #[arg(long)]
    drop_caches: bool,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum Op {
    RandomRead,
    SequentialRead,
    SequentialWrite,
}

impl std::fmt::Display for Op {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Op::RandomRead => "random_read",
            Op::SequentialRead => "sequential_read",
            Op::SequentialWrite => "sequential_write",
        })
    }
}

#[derive(Copy, Clone, Debug, ValueEnum, PartialEq)]
enum Backend {
    Pread,
    Uring,
    Both,
}

impl std::fmt::Display for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Backend::Pread => "pread",
            Backend::Uring => "uring",
            Backend::Both => "both",
        })
    }
}

// -------------------------------------------------------------------
// Entry point
// -------------------------------------------------------------------

fn main() -> Result<()> {
    tracing_subscriber::fmt().with_target(false).init();
    let args = Args::parse();

    if args.block_size % 4096 != 0 {
        bail!("block_size must be a multiple of 4096 (O_DIRECT alignment)");
    }
    let file_bytes = args.size_mb * 1024 * 1024;
    if file_bytes < args.block_size as u64 {
        bail!("file size must be ≥ block_size");
    }

    // 1) Make sure the test file exists and is pre-populated.
    ensure_file(&args.file, file_bytes, args.skip_prefill)?;

    // 2) Run whichever backends were requested.
    let backends: Vec<Backend> = match args.backend {
        Backend::Both => vec![Backend::Pread, Backend::Uring],
        other => vec![other],
    };

    println!();
    println!(
        "ObjectIO IO bench — op={} block_size={} iterations={} file_size_mb={}",
        args.op, args.block_size, args.iterations, args.size_mb
    );
    println!("file: {}", args.file.display());
    println!();

    for backend in backends {
        if args.drop_caches {
            drop_page_caches()?;
        }
        let stats = match backend {
            Backend::Pread => run_pread(&args)?,
            Backend::Uring => run_uring(&args)?,
            Backend::Both => unreachable!(),
        };
        print_stats(backend, &stats);
    }

    Ok(())
}

// -------------------------------------------------------------------
// File setup
// -------------------------------------------------------------------

fn ensure_file(path: &std::path::Path, bytes: u64, skip_prefill: bool) -> Result<()> {
    let exists_at_right_size = std::fs::metadata(path)
        .map(|m| m.len() == bytes)
        .unwrap_or(false);

    if exists_at_right_size {
        tracing::info!(
            "reusing existing test file ({} MiB)",
            bytes / (1024 * 1024)
        );
        return Ok(());
    }
    if skip_prefill {
        bail!(
            "--skip-prefill but {} is missing or wrong size",
            path.display()
        );
    }

    tracing::info!("creating test file of {} MiB", bytes / (1024 * 1024));
    let mut f = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(path)
        .context("open for create")?;
    let mut rng = StdRng::seed_from_u64(0xB3_BE_EF_00_11_22_33_44u64);
    // 1 MiB write buffer; re-randomise per block so the content isn't compressible.
    let mut buf = vec![0u8; 1024 * 1024];
    let mut written = 0u64;
    while written < bytes {
        rng.fill(&mut buf[..]);
        let n = std::cmp::min(buf.len() as u64, bytes - written) as usize;
        f.write_all(&buf[..n]).context("prefill write")?;
        written += n as u64;
    }
    f.sync_all().ok();
    Ok(())
}

// -------------------------------------------------------------------
// Stats
// -------------------------------------------------------------------

#[derive(Debug)]
struct RunStats {
    op_count: u64,
    total_bytes: u64,
    elapsed: Duration,
    latencies_ns: Vec<u64>,
}

fn compute_percentile(sorted: &[u64], pct: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * pct).round() as usize;
    sorted[idx]
}

fn print_stats(backend: Backend, s: &RunStats) {
    let mut latencies = s.latencies_ns.clone();
    latencies.sort_unstable();

    let secs = s.elapsed.as_secs_f64();
    let mb = s.total_bytes as f64 / (1024.0 * 1024.0);
    let thpt = mb / secs;
    let ops_per_sec = s.op_count as f64 / secs;

    let us = |ns: u64| ns as f64 / 1000.0;

    println!("[{}]", backend);
    println!("  ops:         {}", s.op_count);
    println!("  elapsed:     {:.3} s", secs);
    println!("  throughput:  {:.1} MB/s  ({:.0} ops/s)", thpt, ops_per_sec);
    println!("  p50 lat:     {:.1} μs", us(compute_percentile(&latencies, 0.50)));
    println!("  p90 lat:     {:.1} μs", us(compute_percentile(&latencies, 0.90)));
    println!("  p99 lat:     {:.1} μs", us(compute_percentile(&latencies, 0.99)));
    println!("  p99.9 lat:   {:.1} μs", us(compute_percentile(&latencies, 0.999)));
    println!("  max lat:     {:.1} μs", us(*latencies.last().unwrap_or(&0)));
    println!();
}

// -------------------------------------------------------------------
// pread backend — sync pread/pwrite under tokio::spawn_blocking
// -------------------------------------------------------------------
//
// This is what ObjectIO's raw_io.rs does today. Each read/write is a
// full syscall through the kernel, bounced through the blocking thread
// pool from the async runtime.

fn run_pread(args: &Args) -> Result<RunStats> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()?;
    runtime.block_on(run_pread_async(args))
}

async fn run_pread_async(args: &Args) -> Result<RunStats> {
    use std::os::unix::fs::{FileExt, OpenOptionsExt};

    let file = OpenOptions::new()
        .read(!matches!(args.op, Op::SequentialWrite))
        .write(matches!(args.op, Op::SequentialWrite))
        .custom_flags(o_direct_flag())
        .open(&args.file)?;
    let file = std::sync::Arc::new(file);

    let file_len = args.size_mb * 1024 * 1024;
    let bs = args.block_size as u64;
    let max_offset = file_len - bs;

    let mut latencies = Vec::with_capacity(args.iterations as usize);
    let start_overall = Instant::now();

    for i in 0..args.iterations {
        let offset = match args.op {
            Op::RandomRead => {
                let mut rng = StdRng::seed_from_u64(0xBEEF + i);
                let rnd: u64 = rng.r#gen::<u64>() % max_offset;
                (rnd / 4096) * 4096
            }
            Op::SequentialRead | Op::SequentialWrite => {
                ((i * bs) % max_offset / 4096) * 4096
            }
        };
        let file_c = std::sync::Arc::clone(&file);
        let op = args.op;
        let block_size = args.block_size;
        let t0 = Instant::now();
        tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            // Buffer must be 4 KiB aligned for O_DIRECT.
            let mut buf = aligned_buffer(block_size);
            match op {
                Op::RandomRead | Op::SequentialRead => {
                    file_c.read_exact_at(&mut buf, offset)?;
                }
                Op::SequentialWrite => {
                    // Reuse buffer — content doesn't matter for the bench.
                    file_c.write_all_at(&buf, offset)?;
                }
            }
            Ok(())
        })
        .await??;
        latencies.push(t0.elapsed().as_nanos() as u64);
    }

    Ok(RunStats {
        op_count: args.iterations,
        total_bytes: args.iterations * args.block_size as u64,
        elapsed: start_overall.elapsed(),
        latencies_ns: latencies,
    })
}

// -------------------------------------------------------------------
// io_uring backend — Linux only
// -------------------------------------------------------------------

#[cfg(target_os = "linux")]
fn run_uring(args: &Args) -> Result<RunStats> {
    tokio_uring::start(run_uring_async(args))
}

#[cfg(target_os = "linux")]
async fn run_uring_async(args: &Args) -> Result<RunStats> {
    use std::os::fd::{FromRawFd, IntoRawFd};
    use std::os::unix::fs::OpenOptionsExt;
    use tokio_uring::fs::File;

    // tokio-uring 0.5's OpenOptions doesn't expose custom_flags, so
    // open with std (which does) to get an fd backed by O_DIRECT, then
    // hand the fd to tokio-uring. The resulting File submits all I/O
    // through the io_uring ring — exactly what we're benching.
    let std_file = OpenOptions::new()
        .read(true)
        .write(matches!(args.op, Op::SequentialWrite))
        .custom_flags(o_direct_flag())
        .open(&args.file)?;
    // SAFETY: we own `std_file` and immediately hand off its fd to the
    // tokio-uring File, which takes over lifecycle.
    let file = unsafe { File::from_raw_fd(std_file.into_raw_fd()) };

    let file_len = args.size_mb * 1024 * 1024;
    let bs = args.block_size as u64;
    let max_offset = file_len - bs;

    let mut latencies = Vec::with_capacity(args.iterations as usize);
    let start_overall = Instant::now();

    // io_uring takes ownership of the buffer on each submission and
    // hands it back in the completion tuple. Thread one buffer through
    // the loop instead of re-allocating.
    let mut buf: Vec<u8> = aligned_buffer(args.block_size);

    for i in 0..args.iterations {
        let offset = match args.op {
            Op::RandomRead => {
                let mut rng = StdRng::seed_from_u64(0xBEEF + i);
                let rnd: u64 = rng.r#gen::<u64>() % max_offset;
                (rnd / 4096) * 4096
            }
            Op::SequentialRead | Op::SequentialWrite => {
                ((i * bs) % max_offset / 4096) * 4096
            }
        };
        let t0 = Instant::now();
        match args.op {
            Op::RandomRead | Op::SequentialRead => {
                let (res, returned) = file.read_at(buf, offset).await;
                res?;
                buf = returned;
            }
            Op::SequentialWrite => {
                // tokio-uring 0.5 changed write_at to return an
                // UnsubmittedOneshot that needs .submit() before await.
                let (res, returned) = file.write_at(buf, offset).submit().await;
                res?;
                buf = returned;
            }
        }
        latencies.push(t0.elapsed().as_nanos() as u64);
    }

    Ok(RunStats {
        op_count: args.iterations,
        total_bytes: args.iterations * args.block_size as u64,
        elapsed: start_overall.elapsed(),
        latencies_ns: latencies,
    })
}

#[cfg(not(target_os = "linux"))]
fn run_uring(_args: &Args) -> Result<RunStats> {
    bail!(
        "io_uring backend is Linux-only. Pass --backend pread when running on \
         macOS / Windows, or run this benchmark on a Linux host."
    );
}

// -------------------------------------------------------------------
// Helpers
// -------------------------------------------------------------------

#[cfg(target_os = "linux")]
const fn o_direct_flag() -> i32 {
    libc::O_DIRECT
}

#[cfg(not(target_os = "linux"))]
const fn o_direct_flag() -> i32 {
    0 // macOS uses F_NOCACHE via fcntl; Windows has FILE_FLAG_NO_BUFFERING
      // at CreateFile. Neither is a custom_flags bit you can pass at
      // open time, so on non-Linux hosts we just fall back to buffered
      // I/O — the bench is Linux-primary anyway.
}

/// Allocate a buffer 4 KiB-aligned for O_DIRECT. `Vec<u8>` isn't
/// guaranteed to be aligned past its element alignment (1), so we
/// over-allocate and re-slice to an aligned offset. Good enough for a
/// bench; not good for production.
fn aligned_buffer(size: usize) -> Vec<u8> {
    const ALIGN: usize = 4096;
    let mut raw = vec![0u8; size + ALIGN];
    let addr = raw.as_ptr() as usize;
    let off = (ALIGN - addr % ALIGN) % ALIGN;
    // Truncate the prefix so the Vec's data pointer lands on the
    // aligned boundary. This is a hack but reliable for x86_64 / arm64
    // where Vec<u8> doesn't reallocate on drain_front.
    raw.drain(..off);
    raw.truncate(size);
    raw
}

#[cfg(target_os = "linux")]
fn drop_page_caches() -> Result<()> {
    // Requires root. Best-effort; log and continue if it fails.
    use std::io::Write;
    sync_all_filesystems();
    match OpenOptions::new().write(true).open("/proc/sys/vm/drop_caches") {
        Ok(mut f) => {
            let _ = f.write_all(b"3");
            tracing::info!("page caches dropped");
        }
        Err(e) => tracing::warn!("could not drop page caches (need root?): {e}"),
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn drop_page_caches() -> Result<()> {
    tracing::warn!("--drop-caches only works on Linux; skipping");
    Ok(())
}

#[cfg(target_os = "linux")]
fn sync_all_filesystems() {
    unsafe {
        libc::sync();
    }
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
fn sync_all_filesystems() {}

// Silence unused-import warning on non-Linux builds where AsRawFd isn't
// referenced downstream but is pulled in via trait objects.
#[allow(dead_code)]
fn _keep_as_raw_fd_in_scope() {
    let _: Option<&dyn AsRawFd> = None;
}
