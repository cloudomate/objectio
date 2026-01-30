//! Disk detection and preparation

use anyhow::{bail, Result};
use std::fs;
use std::path::Path;
use tracing::{debug, info, warn};

/// ObjectIO disk magic bytes (first 8 bytes of superblock)
const OBJECTIO_MAGIC: &[u8] = b"OBJIO001";

/// Detect available disks that can be used for ObjectIO
pub fn detect_available_disks() -> Result<Vec<String>> {
    let mut available = Vec::new();

    // On Linux, scan /sys/block for block devices
    #[cfg(target_os = "linux")]
    {
        let sys_block = Path::new("/sys/block");
        if sys_block.exists() {
            for entry in fs::read_dir(sys_block)? {
                let entry = entry?;
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                // Skip loop devices, ram disks, and other virtual devices
                if name_str.starts_with("loop")
                    || name_str.starts_with("ram")
                    || name_str.starts_with("dm-")
                    || name_str.starts_with("sr")
                    || name_str.starts_with("fd")
                {
                    continue;
                }

                let dev_path = format!("/dev/{}", name_str);

                // Check if device is suitable
                if is_disk_suitable(&dev_path)? {
                    available.push(dev_path);
                }
            }
        }
    }

    // On macOS, use diskutil (simplified)
    #[cfg(target_os = "macos")]
    {
        // On macOS, we typically use files instead of raw devices for development
        // In production, would use proper disk detection
        debug!("macOS: disk detection not fully implemented, use --disks explicitly");
    }

    Ok(available)
}

/// List all block devices (including those in use)
pub fn list_all_disks() -> Result<Vec<String>> {
    let mut all = Vec::new();

    #[cfg(target_os = "linux")]
    {
        let sys_block = Path::new("/sys/block");
        if sys_block.exists() {
            for entry in fs::read_dir(sys_block)? {
                let entry = entry?;
                let name = entry.file_name();
                let name_str = name.to_string_lossy();

                // Skip virtual devices
                if name_str.starts_with("loop")
                    || name_str.starts_with("ram")
                    || name_str.starts_with("dm-")
                {
                    continue;
                }

                let dev_path = format!("/dev/{}", name_str);
                if Path::new(&dev_path).exists() {
                    all.push(dev_path);
                }
            }
        }
    }

    Ok(all)
}

/// Check if a disk is suitable for ObjectIO
fn is_disk_suitable(path: &str) -> Result<bool> {
    let path = Path::new(path);

    // Must exist
    if !path.exists() {
        return Ok(false);
    }

    // Check if already mounted
    #[cfg(target_os = "linux")]
    {
        let mounts = fs::read_to_string("/proc/mounts").unwrap_or_default();
        if mounts.contains(&format!("{} ", path.display())) {
            debug!("{} is mounted, skipping", path.display());
            return Ok(false);
        }
    }

    // Check if it's the system disk (has root partition)
    // This is a simplified check - production would be more thorough
    #[cfg(target_os = "linux")]
    {
        let path_str = path.to_string_lossy();

        // Check if any partition of this disk is mounted as root
        let mounts = fs::read_to_string("/proc/mounts").unwrap_or_default();
        for line in mounts.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 && parts[1] == "/" {
                // This is the root mount
                if parts[0].starts_with(&*path_str) {
                    debug!("{} contains root partition, skipping", path.display());
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

/// Check disk status
pub fn check_disk(path: &str) -> Result<String> {
    let path = Path::new(path);

    if !path.exists() {
        return Ok("not found".to_string());
    }

    // Try to read the first few bytes to check for ObjectIO magic
    match fs::read(path) {
        Ok(data) if data.len() >= OBJECTIO_MAGIC.len() => {
            if &data[..OBJECTIO_MAGIC.len()] == OBJECTIO_MAGIC {
                // Try to read disk ID from superblock
                if data.len() >= 24 {
                    let id_bytes = &data[8..24];
                    let id_str = uuid::Uuid::from_slice(id_bytes)
                        .map(|u| u.to_string())
                        .unwrap_or_else(|_| "unknown".to_string());
                    return Ok(format!("ObjectIO disk (ID: {})", id_str));
                }
                return Ok("ObjectIO disk".to_string());
            }
            Ok("not ObjectIO disk".to_string())
        }
        Ok(_) => Ok("empty or too small".to_string()),
        Err(e) => Ok(format!("cannot read: {}", e)),
    }
}

/// Prepare a disk for ObjectIO use
///
/// The block_size parameter configures the storage block size.
/// Use None for the default (64KB), or specify a custom size.
pub fn prepare_disk(path: &str, force: bool, block_size: Option<u32>) -> Result<String> {
    let disk_path = Path::new(path);

    if !disk_path.exists() {
        bail!("Disk not found: {}", path);
    }

    // Check if already an ObjectIO disk
    let status = check_disk(path)?;
    if status.contains("ObjectIO disk") && !force {
        bail!("Disk {} is already initialized as ObjectIO disk. Use --force to reinitialize.", path);
    }

    // Check if disk is mounted
    #[cfg(target_os = "linux")]
    {
        let mounts = fs::read_to_string("/proc/mounts").unwrap_or_default();
        if mounts.contains(&format!("{} ", path)) {
            bail!("Disk {} is currently mounted. Unmount it first.", path);
        }
    }

    info!("Initializing disk: {}", path);

    // Use objectio-storage's DiskManager to initialize
    // For safety, we'll do a basic initialization here

    // Get disk size
    let metadata = fs::metadata(disk_path)?;
    let size = if metadata.is_file() {
        metadata.len()
    } else {
        // For block devices, try to get size
        get_block_device_size(path)?
    };

    if size < 1024 * 1024 * 100 {
        // Minimum 100MB
        bail!("Disk too small: {} bytes (minimum 100MB required)", size);
    }

    // Initialize the disk using objectio-storage
    let disk = objectio_storage::DiskManager::init(path, size, block_size)
        .map_err(|e| anyhow::anyhow!("Failed to initialize disk: {}", e))?;

    let disk_id = disk.id().to_string();
    let block_size_str = if let Some(bs) = block_size {
        format!("{} MB", bs / 1024 / 1024)
    } else {
        "default (64 KB)".to_string()
    };
    info!("Disk initialized successfully: {} (ID: {}, block_size: {})", path, disk_id, block_size_str);

    Ok(disk_id)
}

/// Get size of a block device
fn get_block_device_size(path: &str) -> Result<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        // Try using ioctl to get device size
        let file = fs::File::open(path)?;
        let metadata = file.metadata()?;

        // For block devices, size() returns 0, need to use ioctl
        if metadata.rdev() != 0 {
            // It's a block device
            // Read size from /sys/block/<device>/size
            let dev_name = Path::new(path)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("");

            let size_path = format!("/sys/block/{}/size", dev_name);
            if let Ok(size_str) = fs::read_to_string(&size_path) {
                let sectors: u64 = size_str.trim().parse().unwrap_or(0);
                return Ok(sectors * 512); // Assume 512-byte sectors
            }
        }

        Ok(metadata.len())
    }

    #[cfg(not(target_os = "linux"))]
    {
        let metadata = fs::metadata(path)?;
        Ok(metadata.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_disk_nonexistent() {
        let status = check_disk("/nonexistent/path").unwrap();
        assert!(status.contains("not found"));
    }
}
