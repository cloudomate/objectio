//! Platform-specific raw disk I/O
//!
//! Provides direct disk access bypassing the OS page cache:
//! - Linux: O_DIRECT flag
//! - macOS: F_NOCACHE fcntl

use objectio_common::{Error, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;

/// Alignment requirement for direct I/O (typically 4KB or 512 bytes)
pub const ALIGNMENT: usize = 4096;

/// Raw file handle with direct I/O support
pub struct RawFile {
    file: File,
    path: String,
    size: u64,
    read_only: bool,
}

impl RawFile {
    /// Open a file for raw I/O
    pub fn open(path: impl AsRef<Path>, read_only: bool) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let mut options = OpenOptions::new();
        options.read(true);

        if !read_only {
            options.write(true);
        }

        // Platform-specific direct I/O flags
        #[cfg(target_os = "linux")]
        {
            // O_DIRECT bypasses page cache on Linux
            options.custom_flags(libc::O_DIRECT);
        }

        let file = options.open(&path).map_err(|e| {
            Error::Storage(format!("failed to open {}: {}", path_str, e))
        })?;

        // On macOS, use F_NOCACHE after opening
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                if libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) == -1 {
                    return Err(Error::Storage(format!(
                        "failed to set F_NOCACHE on {}: {}",
                        path_str,
                        std::io::Error::last_os_error()
                    )));
                }
            }
        }

        // Get file/device size
        let is_block_device = Self::is_block_device(&path).unwrap_or(false);
        let size = if is_block_device {
            Self::get_block_device_size(&file, &path_str)?
        } else {
            file.metadata().map_err(|e| {
                Error::Storage(format!("failed to get metadata for {}: {}", path_str, e))
            })?.len()
        };

        Ok(Self {
            file,
            path: path_str,
            size,
            read_only,
        })
    }

    /// Create a new file for raw I/O with the given size
    ///
    /// For regular files, this creates/truncates the file and sets its size.
    /// For block devices, this opens the device (size parameter is ignored,
    /// actual device size is used).
    pub fn create(path: impl AsRef<Path>, size: u64) -> Result<Self> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        // Check if this is a block device
        let is_block_device = Self::is_block_device(&path)?;

        let mut options = OpenOptions::new();
        options.read(true).write(true);

        if !is_block_device {
            options.create(true).truncate(true);
        }

        // Platform-specific direct I/O flags
        #[cfg(target_os = "linux")]
        {
            options.custom_flags(libc::O_DIRECT);
        }

        let file = options.open(&path).map_err(|e| {
            Error::Storage(format!("failed to create {}: {}", path_str, e))
        })?;

        let actual_size = if is_block_device {
            // Get block device size
            Self::get_block_device_size(&file, &path_str)?
        } else {
            // Set file size for regular files
            file.set_len(size).map_err(|e| {
                Error::Storage(format!("failed to set size for {}: {}", path_str, e))
            })?;
            size
        };

        // On macOS, use F_NOCACHE after opening
        #[cfg(target_os = "macos")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                if libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1) == -1 {
                    return Err(Error::Storage(format!(
                        "failed to set F_NOCACHE on {}: {}",
                        path_str,
                        std::io::Error::last_os_error()
                    )));
                }
            }
        }

        Ok(Self {
            file,
            path: path_str,
            size: actual_size,
            read_only: false,
        })
    }

    /// Check if path is a block device
    fn is_block_device(path: impl AsRef<Path>) -> Result<bool> {
        use std::os::unix::fs::FileTypeExt;
        let metadata = std::fs::metadata(&path).map_err(|e| {
            // If file doesn't exist, it's not a block device
            if e.kind() == std::io::ErrorKind::NotFound {
                return Error::Storage("file not found".into());
            }
            Error::Storage(format!("failed to get metadata: {}", e))
        });

        match metadata {
            Ok(m) => Ok(m.file_type().is_block_device()),
            Err(_) => Ok(false), // File doesn't exist, will be created as regular file
        }
    }

    /// Get block device size using ioctl
    #[cfg(target_os = "linux")]
    fn get_block_device_size(file: &File, path: &str) -> Result<u64> {
        use std::os::unix::io::AsRawFd;

        // BLKGETSIZE64 ioctl
        const BLKGETSIZE64: libc::c_ulong = 0x80081272;

        let mut size: u64 = 0;
        let ret = unsafe {
            libc::ioctl(file.as_raw_fd(), BLKGETSIZE64, &mut size)
        };

        if ret == -1 {
            return Err(Error::Storage(format!(
                "failed to get block device size for {}: {}",
                path,
                std::io::Error::last_os_error()
            )));
        }

        Ok(size)
    }

    /// Get block device size (non-Linux fallback)
    #[cfg(not(target_os = "linux"))]
    fn get_block_device_size(file: &File, path: &str) -> Result<u64> {
        // On macOS, use seek to end
        use std::io::{Seek, SeekFrom};
        let mut f = file;
        let size = f.seek(SeekFrom::End(0)).map_err(|e| {
            Error::Storage(format!("failed to get device size for {}: {}", path, e))
        })?;
        f.seek(SeekFrom::Start(0)).map_err(|e| {
            Error::Storage(format!("failed to seek to start for {}: {}", path, e))
        })?;
        Ok(size)
    }

    /// Get the file size
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Get the file path
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Read data at the given offset
    ///
    /// For direct I/O, both offset and buffer size must be aligned to ALIGNMENT
    pub fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.check_alignment(offset, buf.len())?;

        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::Storage(format!("seek failed on {}: {}", self.path, e))
        })?;

        file.read(buf).map_err(|e| {
            Error::Storage(format!("read failed on {}: {}", self.path, e))
        })
    }

    /// Write data at the given offset
    ///
    /// For direct I/O, both offset and buffer size must be aligned to ALIGNMENT
    pub fn write_at(&self, offset: u64, buf: &[u8]) -> Result<usize> {
        if self.read_only {
            return Err(Error::Storage("file is read-only".into()));
        }

        self.check_alignment(offset, buf.len())?;

        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset)).map_err(|e| {
            Error::Storage(format!("seek failed on {}: {}", self.path, e))
        })?;

        file.write(buf).map_err(|e| {
            Error::Storage(format!("write failed on {}: {}", self.path, e))
        })
    }

    /// Sync data to disk
    pub fn sync(&self) -> Result<()> {
        self.file.sync_all().map_err(|e| {
            Error::Storage(format!("sync failed on {}: {}", self.path, e))
        })
    }

    /// Sync data only (not metadata) to disk
    pub fn sync_data(&self) -> Result<()> {
        self.file.sync_data().map_err(|e| {
            Error::Storage(format!("sync_data failed on {}: {}", self.path, e))
        })
    }

    /// Check alignment requirements
    fn check_alignment(&self, offset: u64, size: usize) -> Result<()> {
        if offset as usize % ALIGNMENT != 0 {
            return Err(Error::Storage(format!(
                "offset {} is not aligned to {}",
                offset, ALIGNMENT
            )));
        }
        if size % ALIGNMENT != 0 {
            return Err(Error::Storage(format!(
                "size {} is not aligned to {}",
                size, ALIGNMENT
            )));
        }
        Ok(())
    }
}

/// Aligned buffer for direct I/O operations
///
/// On Linux with O_DIRECT, the buffer must be aligned to the filesystem's
/// block size (typically 512 or 4096 bytes). This struct guarantees proper
/// alignment using platform-specific allocation.
#[derive(Debug)]
pub struct AlignedBuffer {
    data: Vec<u8>,
    #[allow(dead_code)]
    alignment: usize,
}

impl AlignedBuffer {
    /// Create a new aligned buffer of the given size
    pub fn new(size: usize) -> Self {
        Self::with_alignment(size, ALIGNMENT)
    }

    /// Create a new aligned buffer with custom alignment
    #[cfg(target_os = "linux")]
    pub fn with_alignment(size: usize, alignment: usize) -> Self {
        use std::alloc::{alloc_zeroed, Layout};

        // Round up size to alignment
        let aligned_size = (size + alignment - 1) / alignment * alignment;

        // Use aligned allocation for O_DIRECT compatibility
        let layout = Layout::from_size_align(aligned_size, alignment)
            .expect("Invalid layout for aligned buffer");

        let data = unsafe {
            let ptr = alloc_zeroed(layout);
            if ptr.is_null() {
                panic!("Failed to allocate aligned buffer");
            }
            Vec::from_raw_parts(ptr, aligned_size, aligned_size)
        };

        Self { data, alignment }
    }

    /// Create a new aligned buffer with custom alignment (non-Linux)
    #[cfg(not(target_os = "linux"))]
    pub fn with_alignment(size: usize, alignment: usize) -> Self {
        // On macOS, F_NOCACHE doesn't require strict alignment
        let aligned_size = (size + alignment - 1) / alignment * alignment;
        let data = vec![0u8; aligned_size];
        Self { data, alignment }
    }

    /// Get the buffer as a slice
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get the buffer as a mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get the buffer size
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Copy data into the buffer (pads with zeros if source is smaller)
    pub fn copy_from(&mut self, src: &[u8]) {
        let copy_len = src.len().min(self.data.len());
        self.data[..copy_len].copy_from_slice(&src[..copy_len]);
        if copy_len < self.data.len() {
            self.data[copy_len..].fill(0);
        }
    }

    /// Get a view of the actual data (up to the given size)
    pub fn data(&self, size: usize) -> &[u8] {
        &self.data[..size.min(self.data.len())]
    }
}

impl Default for AlignedBuffer {
    fn default() -> Self {
        Self::new(ALIGNMENT)
    }
}

#[cfg(target_os = "linux")]
impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        use std::alloc::{dealloc, Layout};

        if !self.data.is_empty() {
            let layout = Layout::from_size_align(self.data.capacity(), self.alignment)
                .expect("Invalid layout for deallocation");

            unsafe {
                // Take ownership of the Vec's buffer
                let ptr = self.data.as_mut_ptr();
                // Prevent Vec from deallocating
                std::mem::forget(std::mem::take(&mut self.data));
                // Deallocate with the correct layout
                dealloc(ptr, layout);
            }
        }
    }
}

impl AsRef<[u8]> for AlignedBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for AlignedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_aligned_buffer() {
        let mut buf = AlignedBuffer::new(4096);
        assert_eq!(buf.len(), 4096);

        buf.copy_from(b"hello");
        assert_eq!(&buf.data(5), b"hello");
    }

    #[test]
    fn test_raw_file_create_and_read() {
        let temp = NamedTempFile::new().unwrap();
        let path = temp.path();

        // Create a file with some size
        {
            let file = RawFile::create(path, 8192).unwrap();
            let mut buf = AlignedBuffer::new(4096);
            buf.copy_from(b"test data pattern");
            file.write_at(0, buf.as_slice()).unwrap();
            file.sync().unwrap();
        }

        // Read it back
        {
            let file = RawFile::open(path, true).unwrap();
            let mut buf = AlignedBuffer::new(4096);
            file.read_at(0, buf.as_mut_slice()).unwrap();
            assert!(buf.as_slice().starts_with(b"test data pattern"));
        }
    }
}
