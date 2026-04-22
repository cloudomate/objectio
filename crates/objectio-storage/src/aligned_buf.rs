//! `AlignedBuf` — owned, aligned, drop-safe byte buffer for the
//! `IoBackend` hot path.
//!
//! Why another buffer type:
//!
//! - Plain `Vec<u8>` isn't guaranteed aligned past `align_of::<u8>()`
//!   (= 1). For large allocations on glibc it *happens* to land on
//!   page boundaries (mmap-backed), but that's an allocator
//!   implementation detail — not a contract. Small allocations
//!   (< 128 KiB) have no such accident and can land on odd addresses.
//!   O_DIRECT / io_uring submissions to such buffers return EINVAL.
//!
//! - The existing `AlignedBuffer` in [`raw_io`](crate::raw_io) already
//!   solves the allocation + matching-Drop problem, but its API is
//!   borrow-shaped (`as_mut_slice()`) which doesn't compose with
//!   tokio-uring's ownership-transfer submission model.
//!
//! `AlignedBuf` is what `AlignedBuffer` wants to be for the async
//! path: owned, page-aligned by construction, with the
//! `IoBuf` / `IoBufMut` traits implemented behind `--features
//! io-uring` so it can be handed straight into a
//! `tokio_uring::fs::File::read_at` / `write_at`.
//!
//! Invariants:
//!
//! - `self.ptr` is valid, non-null, and was allocated with
//!   `std::alloc::alloc_zeroed(self.layout())`.
//! - `self.cap` matches the layout's size; Drop deallocates with the
//!   same layout.
//! - `self.len <= self.cap`. `len` tracks the "initialised prefix",
//!   which is what tokio-uring reports back after a read. Writes
//!   look at `self.as_slice()` which is the full initialised range.
//!
//! Safety: the raw-pointer field means the type is `!Send + !Sync`
//! by default. We manually impl `Send` because the owning
//! `AlignedBuf` is the only handle to that allocation — moving it
//! across threads is fine. We do NOT impl `Sync` (no shared-read
//! use case justifies it).

use std::alloc::{Layout, alloc_zeroed, dealloc};
use std::ptr::NonNull;
use std::slice;

/// 4 KiB — matches the O_DIRECT alignment requirement on Linux and
/// is the default VM page size on every arch we target.
pub const DEFAULT_ALIGN: usize = 4096;

/// Owned, aligned buffer. See module docs.
#[derive(Debug)]
pub struct AlignedBuf {
    ptr: NonNull<u8>,
    /// Initialised length visible to callers reading `as_slice()`.
    /// Starts at `cap` because we always zero-fill on construction.
    len: usize,
    /// Total allocation size in bytes. Immutable after construction.
    cap: usize,
    /// Power-of-two alignment (≥ 4 KiB in practice).
    align: usize,
}

impl AlignedBuf {
    /// Allocate a zero-filled buffer with the default 4 KiB alignment.
    #[must_use]
    pub fn new(size: usize) -> Self {
        Self::with_alignment(size, DEFAULT_ALIGN)
    }

    /// Allocate with a caller-specified alignment. Panics if the
    /// alignment isn't a power of two or if `alloc_zeroed` fails
    /// (OOM) — consistent with how `Vec` handles allocation failure.
    #[must_use]
    pub fn with_alignment(size: usize, align: usize) -> Self {
        assert!(align.is_power_of_two(), "alignment must be a power of two");
        let layout = Layout::from_size_align(size.max(1), align)
            .expect("invalid layout for AlignedBuf (size overflows isize?)");
        // SAFETY: layout size > 0, valid pow-of-two alignment.
        let ptr = unsafe { alloc_zeroed(layout) };
        let ptr = NonNull::new(ptr).unwrap_or_else(|| std::alloc::handle_alloc_error(layout));
        Self {
            ptr,
            len: size,
            cap: size,
            align,
        }
    }

    /// Raw pointer — exported for `IoBuf` / `IoBufMut`. Not for
    /// general caller use.
    #[must_use]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    /// Mutable raw pointer — exported for `IoBufMut`.
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[must_use]
    pub fn capacity(&self) -> usize {
        self.cap
    }

    #[must_use]
    pub fn alignment(&self) -> usize {
        self.align
    }

    /// Immutable view of the initialised prefix. Safe because we
    /// always zero-init on `new` / `with_alignment`, so every byte in
    /// `[0, len)` is validly written.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        // SAFETY: ptr is valid, len <= cap, bytes are zero-initialised
        // at construction; safe to expose as &[u8].
        unsafe { slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Mutable view of the full capacity. Callers can freely overwrite
    /// any prefix; `len` stays at `cap` unless explicitly shrunk.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: exclusive ownership via &mut self, ptr is valid.
        unsafe { slice::from_raw_parts_mut(self.ptr.as_ptr(), self.cap) }
    }

    /// Override the initialised-length high-water mark (what
    /// `as_slice()` returns). Caller asserts that the range
    /// `[0, new_len)` has been written and is safe to read.
    ///
    /// # Safety
    /// `new_len <= capacity()`.
    pub unsafe fn set_len(&mut self, new_len: usize) {
        debug_assert!(new_len <= self.cap);
        self.len = new_len;
    }

    fn layout(&self) -> Layout {
        // SAFETY: constructed with from_size_align, so the same call
        // here always succeeds.
        Layout::from_size_align(self.cap.max(1), self.align).expect("same layout as constructor")
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: ptr + layout match the pair passed to alloc_zeroed.
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout());
        }
    }
}

// The raw-pointer field makes `AlignedBuf` `!Send + !Sync` by
// default. Manual `Send`: we're the sole owner of the allocation;
// transferring ownership across threads is safe. No `Sync` impl —
// we do not offer shared-reference read paths that cross threads.
//
// SAFETY: AlignedBuf behaves like `Box<[u8]>` w.r.t. ownership.
unsafe impl Send for AlignedBuf {}

// --------------------------------------------------------------
// tokio-uring interop
// --------------------------------------------------------------
//
// IoBuf / IoBufMut are the contract tokio-uring uses to take a
// caller-owned buffer for the lifetime of a submission. The
// requirements are:
//
//   * `'static` — we store the handle in the in-flight submission
//     list; no borrows can outlive it.
//   * Stable pointer for the full lifetime of the buffer (no
//     re-allocation underneath us).
//   * `Unpin` — submitted buffers live inside pinned futures.
//
// AlignedBuf satisfies all of these: it doesn't move its allocation
// and doesn't implement Unpin-opt-out markers.
//
// SAFETY notes for each impl are inline.
#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod uring_interop {
    use super::AlignedBuf;

    // SAFETY: AlignedBuf is 'static (no borrowed data) and its
    // allocation pointer is stable from `new()` to `drop()`.
    unsafe impl tokio_uring::buf::IoBuf for AlignedBuf {
        fn stable_ptr(&self) -> *const u8 {
            AlignedBuf::as_ptr(self)
        }

        fn bytes_init(&self) -> usize {
            self.len
        }

        fn bytes_total(&self) -> usize {
            self.cap
        }
    }

    // SAFETY: same stability invariants as IoBuf; the mutable pointer
    // is exclusive because tokio-uring takes ownership on submission.
    unsafe impl tokio_uring::buf::IoBufMut for AlignedBuf {
        fn stable_mut_ptr(&mut self) -> *mut u8 {
            AlignedBuf::as_mut_ptr(self)
        }

        unsafe fn set_init(&mut self, pos: usize) {
            // Called by tokio-uring after a successful read to mark
            // the kernel-written prefix as initialised. We zero-fill
            // on alloc so bytes beyond `pos` are also validly defined,
            // but we still track `len` to match tokio-uring's contract.
            unsafe {
                AlignedBuf::set_len(self, pos);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_zero_filled_and_aligned() {
        let buf = AlignedBuf::new(4096);
        assert_eq!(buf.len(), 4096);
        assert_eq!(buf.capacity(), 4096);
        assert_eq!(buf.alignment(), 4096);
        assert_eq!(buf.as_ptr() as usize % 4096, 0, "ptr must be 4 KiB aligned");
        // All zero on fresh alloc
        assert!(buf.as_slice().iter().all(|&b| b == 0));
    }

    #[test]
    fn write_read_round_trip() {
        let mut buf = AlignedBuf::new(4096);
        let slice = buf.as_mut_slice();
        for (i, b) in slice.iter_mut().enumerate() {
            *b = (i & 0xff) as u8;
        }
        for (i, &b) in buf.as_slice().iter().enumerate() {
            assert_eq!(b, (i & 0xff) as u8);
        }
    }

    #[test]
    fn non_default_alignment() {
        let buf = AlignedBuf::with_alignment(8192, 8192);
        assert_eq!(buf.as_ptr() as usize % 8192, 0);
    }

    #[test]
    fn send_is_implemented() {
        fn assert_send<T: Send>() {}
        assert_send::<AlignedBuf>();
    }

    #[test]
    fn drop_with_matching_layout() {
        // If this test isn't UB-detected by miri / ASAN, we're
        // deallocating with the right Layout. Existence of the test
        // at least pairs alloc and dealloc lexically.
        for size in [1, 512, 4096, 16_384, 1 << 20] {
            drop(AlignedBuf::new(size));
        }
    }
}
