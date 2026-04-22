//! Pluggable I/O backend — abstracts over `pread`/`pwrite` and
//! `io_uring` so callers can stay async and the runtime can pick the
//! best-available primitive without changing shard-level logic.
//!
//! Why this exists:
//!
//! The existing [`RawFile`](crate::raw_io::RawFile) uses sync
//! `read_at` / `write_at`. Callers are inside `async fn` handlers on
//! the tokio reactor — which means every disk I/O **blocks a reactor
//! thread** for the duration of the syscall. Under any real
//! concurrency that's a correctness problem, not just a performance
//! one.
//!
//! This module offers two improvements:
//!
//! 1. **Async API, always**. `PreadBackend` wraps the sync calls in
//!    `tokio::task::spawn_blocking`, so the reactor is free during
//!    disk waits. Just swapping the caller over to `IoBackend::read_at`
//!    is already a win even without touching io_uring.
//!
//! 2. **`UringBackend` (Linux, feature `io-uring`)**. Submits I/O
//!    directly to the io_uring ring — no blocking pool, no context
//!    switches between async and sync halves. Measurements on NVMe:
//!    +25% throughput at 4 MiB stripes, -78% p99 at 64 KiB, -90%
//!    p99.9 at WAL writes. See
//!    `objectio-docs/architecture/design/storage-io-levels.md`.
//!
//! Backend selection:
//!
//! * [`IoBackend::best_available`] — build + runtime feature check,
//!   returns UringBackend on Linux-with-feature, PreadBackend
//!   everywhere else.
//! * Callers can also pin a specific backend for testing or when a
//!   particular workload needs a specific primitive (e.g. a
//!   Phase D vector-graph region that always wants uring).
//!
//! Buffer ownership differs between backends:
//!
//! * `pread` uses `&mut [u8]` — the caller owns storage, the kernel
//!   fills it.
//! * io_uring needs to own the buffer for the lifetime of the
//!   submission (ABA safety), so the tokio-uring API takes `Vec<u8>`
//!   and returns it in the completion tuple.
//!
//! The trait paper-overs this with `OwnedBuf` (a `Vec<u8>` on both
//! sides). Converting `&mut [u8]` → `Vec<u8>` → `&mut [u8]` at the
//! boundary costs one allocation per op for the pread path — fine
//! for the first migration, can be optimised later if it shows up in
//! profiles.

use std::os::unix::fs::FileExt;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;

use objectio_common::{Error, Result};

/// Buffer type used across the trait. Owned so it can be threaded
/// through io_uring submissions; for the pread path the caller gets
/// it back via the return value of [`IoBackend::read_at_owned`].
pub type OwnedBuf = Vec<u8>;

/// Describes which backend is actually running. Set at open time,
/// useful for logging / metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendKind {
    /// `pread` / `pwrite` wrapped in `tokio::task::spawn_blocking`.
    /// Available on every OS.
    Pread,
    /// Native io_uring. Only available on Linux with the `io-uring`
    /// feature compiled in.
    Uring,
}

/// Async I/O primitives over an opened file / block device.
///
/// Implementations must be `Send + Sync` so a single backend can be
/// shared across tokio tasks. Operations are positional (absolute
/// offset) — no implicit cursor state.
#[async_trait]
pub trait IoBackend: Send + Sync + std::fmt::Debug {
    /// Which concrete backend is this?
    fn kind(&self) -> BackendKind;

    /// File length in bytes (when the underlying handle is a regular
    /// file; block devices may return the device size).
    fn size(&self) -> u64;

    /// Read `buf.len()` bytes at `offset` into `buf`. Ownership of
    /// the buffer is moved in and moved back out (needed for io_uring).
    async fn read_at_owned(&self, buf: OwnedBuf, offset: u64) -> Result<OwnedBuf>;

    /// Write all of `buf` at `offset`. Ownership moves in and back out.
    async fn write_at_owned(&self, buf: OwnedBuf, offset: u64) -> Result<OwnedBuf>;

    /// Flush writes to disk (fsync).
    async fn sync(&self) -> Result<()>;

    /// Flush data only (fdatasync — no metadata update).
    async fn sync_data(&self) -> Result<()>;
}

/// Pick the best backend the current build + host supports.
///
/// Priority: `UringBackend` if compiled in AND we're on Linux AND
/// `io_uring_setup` succeeds, otherwise `PreadBackend`.
///
/// `direct_io: true` (production default) asks the kernel to skip the
/// page cache — O_DIRECT on Linux, F_NOCACHE on macOS. The caller
/// becomes responsible for aligning both offsets and buffers to 4 KiB.
/// Set `false` for small metadata-style files where the alignment
/// overhead isn't worth it (and for tests).
#[allow(clippy::missing_errors_doc)]
pub fn best_available<P: AsRef<Path>>(
    path: P,
    read_only: bool,
    direct_io: bool,
) -> Result<Arc<dyn IoBackend>> {
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    {
        if let Ok(backend) =
            uring_backend::UringBackend::open(path.as_ref(), read_only, direct_io)
        {
            return Ok(Arc::new(backend));
        }
        tracing::warn!(
            "io_uring open failed (kernel too old or feature disabled); falling back to pread"
        );
    }
    Ok(Arc::new(pread_backend::PreadBackend::open(
        path.as_ref(),
        read_only,
        direct_io,
    )?))
}

/// Always-available pread / pwrite backend.
pub fn pread<P: AsRef<Path>>(
    path: P,
    read_only: bool,
    direct_io: bool,
) -> Result<Arc<dyn IoBackend>> {
    Ok(Arc::new(pread_backend::PreadBackend::open(
        path.as_ref(),
        read_only,
        direct_io,
    )?))
}

// --------------------------------------------------------------
// Pread backend — always available.
// --------------------------------------------------------------

mod pread_backend {
    use super::*;
    use std::fs::{File, OpenOptions};
    #[cfg(target_os = "linux")]
    use std::os::unix::fs::OpenOptionsExt;

    #[derive(Debug)]
    pub struct PreadBackend {
        path: String,
        file: Arc<File>,
        size: u64,
        read_only: bool,
    }

    impl PreadBackend {
        pub fn open(path: &Path, read_only: bool, direct_io: bool) -> Result<Self> {
            let mut opts = OpenOptions::new();
            opts.read(true).write(!read_only);
            if !read_only {
                opts.create(false);
            }

            #[cfg(target_os = "linux")]
            if direct_io {
                opts.custom_flags(libc::O_DIRECT);
            }

            let file = opts
                .open(path)
                .map_err(|e| Error::Storage(format!("open {}: {e}", path.display())))?;

            // macOS F_NOCACHE is set post-open via fcntl.
            #[cfg(target_os = "macos")]
            if direct_io {
                use std::os::fd::AsRawFd;
                unsafe {
                    if libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) == -1 {
                        tracing::warn!(
                            "F_NOCACHE not accepted on {} — falling back to buffered",
                            path.display()
                        );
                    }
                }
            }

            // Suppress unused-variable warning on non-Linux/macOS.
            #[cfg(not(any(target_os = "linux", target_os = "macos")))]
            let _ = direct_io;
            let size = file
                .metadata()
                .map_err(|e| Error::Storage(format!("stat {}: {e}", path.display())))?
                .len();
            Ok(Self {
                path: path.display().to_string(),
                file: Arc::new(file),
                size,
                read_only,
            })
        }
    }

    #[async_trait]
    impl IoBackend for PreadBackend {
        fn kind(&self) -> BackendKind {
            BackendKind::Pread
        }
        fn size(&self) -> u64 {
            self.size
        }

        async fn read_at_owned(&self, mut buf: OwnedBuf, offset: u64) -> Result<OwnedBuf> {
            let file = Arc::clone(&self.file);
            let path = self.path.clone();
            // spawn_blocking frees the reactor for the duration of the
            // syscall. Worth it on any block size — see the benchmark.
            let buf = tokio::task::spawn_blocking(move || {
                file.read_exact_at(&mut buf, offset)
                    .map_err(|e| Error::Storage(format!("read {path} @ {offset}: {e}")))
                    .map(|()| buf)
            })
            .await
            .map_err(|e| Error::Storage(format!("join blocking read: {e}")))??;
            Ok(buf)
        }

        async fn write_at_owned(&self, buf: OwnedBuf, offset: u64) -> Result<OwnedBuf> {
            if self.read_only {
                return Err(Error::Storage(format!(
                    "{} opened read-only — write rejected",
                    self.path
                )));
            }
            let file = Arc::clone(&self.file);
            let path = self.path.clone();
            let buf = tokio::task::spawn_blocking(move || {
                file.write_all_at(&buf, offset)
                    .map_err(|e| Error::Storage(format!("write {path} @ {offset}: {e}")))
                    .map(|()| buf)
            })
            .await
            .map_err(|e| Error::Storage(format!("join blocking write: {e}")))??;
            Ok(buf)
        }

        async fn sync(&self) -> Result<()> {
            let file = Arc::clone(&self.file);
            let path = self.path.clone();
            tokio::task::spawn_blocking(move || {
                file.sync_all()
                    .map_err(|e| Error::Storage(format!("sync {path}: {e}")))
            })
            .await
            .map_err(|e| Error::Storage(format!("join blocking sync: {e}")))?
        }

        async fn sync_data(&self) -> Result<()> {
            let file = Arc::clone(&self.file);
            let path = self.path.clone();
            tokio::task::spawn_blocking(move || {
                file.sync_data()
                    .map_err(|e| Error::Storage(format!("sync_data {path}: {e}")))
            })
            .await
            .map_err(|e| Error::Storage(format!("join blocking sync_data: {e}")))?
        }
    }
}

// --------------------------------------------------------------
// Uring backend — Linux + feature-gated.
// --------------------------------------------------------------

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_backend {
    use super::*;
    use std::fs::OpenOptions;
    use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd, AsRawFd};
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::Mutex;

    // tokio-uring uses a dedicated per-thread runtime. To call it from
    // a regular tokio task we spawn a worker thread that owns a
    // tokio-uring runtime and runs a submission loop; each op is sent
    // in over a channel and the response comes back over a oneshot.
    //
    // This isn't free — the channel hop costs ~1 μs — but it avoids
    // forcing the entire process onto tokio-uring's runtime and
    // preserves compatibility with the rest of the codebase (meta RPC,
    // tracing, metrics all built on stock tokio).

    #[derive(Debug)]
    pub struct UringBackend {
        path: String,
        size: u64,
        read_only: bool,
        // Submission channel to the uring worker thread.
        tx: crossbeam_channel::Sender<Op>,
        // Keep the worker thread joinable on drop.
        _join: Mutex<Option<std::thread::JoinHandle<()>>>,
    }

    enum Op {
        Read {
            buf: Vec<u8>,
            offset: u64,
            reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
        },
        Write {
            buf: Vec<u8>,
            offset: u64,
            reply: tokio::sync::oneshot::Sender<Result<Vec<u8>>>,
        },
        Sync {
            reply: tokio::sync::oneshot::Sender<Result<()>>,
        },
        SyncData {
            reply: tokio::sync::oneshot::Sender<Result<()>>,
        },
        Shutdown,
    }

    impl UringBackend {
        pub fn open(path: &Path, read_only: bool, direct_io: bool) -> Result<Self> {
            let mut opts = OpenOptions::new();
            opts.read(true).write(!read_only);
            if direct_io {
                opts.custom_flags(libc::O_DIRECT);
            }
            let std_file = opts
                .open(path)
                .map_err(|e| Error::Storage(format!("open {}: {e}", path.display())))?;
            let size = std_file
                .metadata()
                .map_err(|e| Error::Storage(format!("stat {}: {e}", path.display())))?
                .len();
            // Transfer fd ownership into an OwnedFd we can send to the
            // worker thread.
            let fd = unsafe { OwnedFd::from_raw_fd(std_file.into_raw_fd()) };

            let (tx, rx) = crossbeam_channel::unbounded::<Op>();
            let path_str = path.display().to_string();
            let worker_path = path_str.clone();
            let join = std::thread::Builder::new()
                .name(format!("obio-uring:{}", path.display()))
                .spawn(move || run_worker(fd, worker_path, rx))
                .map_err(|e| Error::Storage(format!("spawn uring worker: {e}")))?;

            Ok(Self {
                path: path_str,
                size,
                read_only,
                tx,
                _join: Mutex::new(Some(join)),
            })
        }
    }

    impl Drop for UringBackend {
        fn drop(&mut self) {
            let _ = self.tx.send(Op::Shutdown);
            if let Some(jh) = self._join.lock().unwrap().take() {
                let _ = jh.join();
            }
        }
    }

    fn run_worker(fd: OwnedFd, path: String, rx: crossbeam_channel::Receiver<Op>) {
        // tokio-uring owns the current thread for its reactor.
        let raw = fd.as_raw_fd();
        // Don't let OwnedFd drop close the fd — tokio-uring takes over.
        std::mem::forget(fd);

        tokio_uring::start(async move {
            let file = unsafe { tokio_uring::fs::File::from_raw_fd(raw) };
            loop {
                // The crossbeam receiver is sync; hop through
                // spawn_blocking to wait for the next op without
                // blocking the uring reactor.
                let op = match tokio::task::spawn_blocking({
                    let rx = rx.clone();
                    move || rx.recv()
                })
                .await
                {
                    Ok(Ok(op)) => op,
                    _ => break,
                };

                match op {
                    Op::Read { buf, offset, reply } => {
                        let (res, buf) = file.read_at(buf, offset).await;
                        let r = res
                            .map_err(|e| {
                                Error::Storage(format!("uring read {path} @ {offset}: {e}"))
                            })
                            .map(|_| buf);
                        let _ = reply.send(r);
                    }
                    Op::Write { buf, offset, reply } => {
                        let (res, buf) = file.write_at(buf, offset).submit().await;
                        let r = res
                            .map_err(|e| {
                                Error::Storage(format!("uring write {path} @ {offset}: {e}"))
                            })
                            .map(|_| buf);
                        let _ = reply.send(r);
                    }
                    Op::Sync { reply } => {
                        let r = file
                            .sync_all()
                            .await
                            .map_err(|e| Error::Storage(format!("uring sync {path}: {e}")));
                        let _ = reply.send(r);
                    }
                    Op::SyncData { reply } => {
                        let r = file
                            .sync_data()
                            .await
                            .map_err(|e| Error::Storage(format!("uring sync_data {path}: {e}")));
                        let _ = reply.send(r);
                    }
                    Op::Shutdown => break,
                }
            }
            // tokio-uring File::close doesn't yet exist in 0.5; drop
            // implicitly closes via the fd we passed in.
            drop(file);
        });
    }

    #[async_trait]
    impl IoBackend for UringBackend {
        fn kind(&self) -> BackendKind {
            BackendKind::Uring
        }
        fn size(&self) -> u64 {
            self.size
        }

        async fn read_at_owned(&self, buf: OwnedBuf, offset: u64) -> Result<OwnedBuf> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx
                .send(Op::Read {
                    buf,
                    offset,
                    reply: tx,
                })
                .map_err(|_| Error::Storage("uring worker channel closed".into()))?;
            rx.await
                .map_err(|_| Error::Storage("uring worker dropped reply".into()))?
        }

        async fn write_at_owned(&self, buf: OwnedBuf, offset: u64) -> Result<OwnedBuf> {
            if self.read_only {
                return Err(Error::Storage(format!(
                    "{} opened read-only — write rejected",
                    self.path
                )));
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx
                .send(Op::Write {
                    buf,
                    offset,
                    reply: tx,
                })
                .map_err(|_| Error::Storage("uring worker channel closed".into()))?;
            rx.await
                .map_err(|_| Error::Storage("uring worker dropped reply".into()))?
        }

        async fn sync(&self) -> Result<()> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx
                .send(Op::Sync { reply: tx })
                .map_err(|_| Error::Storage("uring worker channel closed".into()))?;
            rx.await
                .map_err(|_| Error::Storage("uring worker dropped reply".into()))?
        }

        async fn sync_data(&self) -> Result<()> {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.tx
                .send(Op::SyncData { reply: tx })
                .map_err(|_| Error::Storage("uring worker channel closed".into()))?;
            rx.await
                .map_err(|_| Error::Storage("uring worker dropped reply".into()))?
        }
    }
}

// --------------------------------------------------------------
// Tests
// --------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use tempfile::tempdir;

    fn make_test_file(dir: &Path, size: usize) -> std::path::PathBuf {
        let path = dir.join("test.bin");
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .unwrap();
        // Non-zero content so the read-back check isn't vacuous
        let data: Vec<u8> = (0..size).map(|i| (i & 0xff) as u8).collect();
        f.write_all(&data).unwrap();
        f.sync_all().unwrap();
        path
    }

    /// Returns a 4 KiB-aligned `Vec<u8>` of the given length.
    /// O_DIRECT on Linux requires buffer *addresses* to be aligned —
    /// `vec![0u8; N]` isn't. Over-allocate + drain-front so the Vec's
    /// data pointer lands on the aligned boundary.
    fn aligned_vec(size: usize) -> Vec<u8> {
        const ALIGN: usize = 4096;
        let mut raw = vec![0u8; size + ALIGN];
        let addr = raw.as_ptr() as usize;
        let off = (ALIGN - addr % ALIGN) % ALIGN;
        raw.drain(..off);
        raw.truncate(size);
        debug_assert_eq!(raw.as_ptr() as usize % ALIGN, 0);
        raw
    }

    #[tokio::test]
    async fn pread_roundtrip() {
        let dir = tempdir().unwrap();
        // 64 KiB file — big enough to exercise O_DIRECT alignment but
        // fast to build. Both offset and length must be multiples of
        // 4096 on Linux O_DIRECT.
        let path = make_test_file(dir.path(), 64 * 1024);
        // direct_io=false — test buffers aren't 4 KiB-aligned. Production
        // callers pass true and align their buffers explicitly.
        let backend = pread(&path, true, false).unwrap();
        assert_eq!(backend.kind(), BackendKind::Pread);
        assert_eq!(backend.size(), 64 * 1024);

        // Read the second 4 KiB page, verify it matches the pattern.
        let buf = aligned_vec(4096);
        let got = backend.read_at_owned(buf, 4096).await.unwrap();
        for (i, b) in got.iter().enumerate() {
            assert_eq!(*b, ((4096 + i) & 0xff) as u8, "byte {i} mismatched");
        }
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    #[test]
    fn uring_roundtrip() {
        let dir = tempdir().unwrap();
        let path = make_test_file(dir.path(), 64 * 1024);

        // UringBackend spawns its own reactor thread; this test runs
        // synchronously and hops into uring via best_available().
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let backend = best_available(&path, true, false).unwrap();
            // Kind depends on whether io_uring_setup succeeded — either
            // is valid, but we prefer uring when built with the feature.
            assert!(matches!(
                backend.kind(),
                BackendKind::Uring | BackendKind::Pread
            ));

            let buf = aligned_vec(4096);
            let got = backend.read_at_owned(buf, 4096).await.unwrap();
            for (i, b) in got.iter().enumerate() {
                assert_eq!(*b, ((4096 + i) & 0xff) as u8, "byte {i} mismatched");
            }
        });
    }
}
