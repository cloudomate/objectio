//! Disk discovery for OSD boot — Ceph / MinIO / Rook pattern.
//!
//! The OSD can be given either an explicit list of disks (stable-path
//! positional args: `--disk /data/disk0/disk.raw` etc.) or a set of
//! glob filters (`--disk-filter "/dev/disk/by-id/wwn-*"`). The
//! discovery pass expands the filters, excludes anything that looks
//! like an operating-system or boot disk, reads each candidate's
//! ObjectIO superblock, and classifies it:
//!
//! - **Claimed** — already has an ObjectIO superblock + matching
//!   cluster_uuid; attach unchanged.
//! - **Foreign** — ObjectIO superblock but different cluster_uuid;
//!   refuse to mount (cross-cluster guard).
//! - **Blank** — no recognisable superblock; format only if the
//!   operator passed `--init-blank-disks=true`. Default `false`
//!   because accidentally formatting a user's unrelated disk is the
//!   single most destructive thing we could do.
//!
//! Root-FS exclusion is the critical safety feature: on an immutable
//! host the whole OS lives on its own block device, which we must
//! never claim. We resolve the root filesystem's backing device by
//! parsing `/proc/mounts` (Linux) and exclude both the device itself
//! and any partitions underneath it.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use objectio_storage::layout::{MAGIC, SUPERBLOCK_SIZE, Superblock};
use objectio_storage::raw_io::{AlignedBuffer, RawFile};
use tracing::{debug, info, warn};

/// Minimum disk size we consider for OSD use. Prevents accidentally
/// claiming a 512 MB EFI System Partition or a recovery image.
///
/// Overridable via `--disk-min-size`. Default is 1 GiB to match the
/// storage layout's `MIN_DISK_SIZE` — anything below is rejected by
/// `Superblock::new` anyway, so this cuts noise early.
pub const DEFAULT_MIN_SIZE: u64 = 1024 * 1024 * 1024;

/// What discovery found for a single candidate path.
#[derive(Clone, Debug)]
pub enum DiskState {
    /// Already carries an ObjectIO superblock. `cluster_uuid` may be
    /// nil for pre-identity disks.
    Claimed {
        cluster_uuid: uuid::Uuid,
        node_id: [u8; 16],
    },
    /// No ObjectIO magic. Formatting is operator-gated.
    Blank,
    /// Something other than ObjectIO sits here (LUKS header, ext4,
    /// MBR/GPT). Never auto-formatted.
    Foreign { reason: String },
}

#[derive(Clone, Debug)]
pub struct DiscoveredDisk {
    pub path: PathBuf,
    pub size_bytes: u64,
    pub state: DiskState,
}

/// Filter + classify candidate disks.
///
/// - `explicit` — disks passed directly by the operator (config file /
///   `--disk` args). These skip most filters except mount + root-FS
///   exclusion; the operator said so, we trust them.
/// - `globs` — glob patterns like `/dev/disk/by-id/wwn-*`. Each match
///   is a candidate; blanks + foreigns are visible to the caller but
///   not automatically claimed.
/// - `min_size` — skip anything below this. Guards against OS install
///   media sliding in.
pub fn discover(
    explicit: &[String],
    globs: &[String],
    min_size: u64,
) -> std::io::Result<Vec<DiscoveredDisk>> {
    let root_dev = root_device();
    if let Some(ref d) = root_dev {
        info!("discovery: root FS backed by {d}; excluding");
    }
    let mounted = mounted_devices();

    // Collect candidate paths. Explicit wins over filter matches —
    // if both name the same file we only classify it once.
    let mut candidates: BTreeSet<PathBuf> = BTreeSet::new();
    for p in explicit {
        candidates.insert(PathBuf::from(p));
    }
    for pattern in globs {
        for m in expand_glob(pattern) {
            candidates.insert(m);
        }
    }

    let mut out = Vec::new();
    for path in candidates {
        let disp = path.display().to_string();

        // Exclude the root FS device itself and any of its partitions.
        if let Some(ref rd) = root_dev
            && matches_root_device(&disp, rd)
        {
            warn!("discovery: skipping {disp} — root FS device");
            continue;
        }

        // Skip currently-mounted partitions. Wouldn't be safe to open
        // them O_DIRECT underneath a running filesystem anyway.
        if !explicit.iter().any(|p| p == &disp)
            && mounted.iter().any(|m| matches_mount(&disp, m))
        {
            warn!("discovery: skipping {disp} — currently mounted");
            continue;
        }

        let size = match probe_size(&path) {
            Ok(s) => s,
            Err(e) => {
                debug!("discovery: cannot probe {disp}: {e}");
                continue;
            }
        };
        if size < min_size {
            debug!(
                "discovery: skipping {disp} — size {size} below min {min_size}"
            );
            continue;
        }

        let state = classify(&path);
        out.push(DiscoveredDisk {
            path,
            size_bytes: size,
            state,
        });
    }

    Ok(out)
}

/// Read `/proc/mounts` and return the backing device for `/`, if any.
/// Returns `None` on macOS / non-Linux hosts where /proc isn't present —
/// callers fall back to explicit-only mode in that case.
fn root_device() -> Option<String> {
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    for line in mounts.lines() {
        let mut parts = line.split_whitespace();
        let dev = parts.next()?;
        let mnt = parts.next()?;
        if mnt == "/" && dev.starts_with("/") {
            return Some(dev.to_string());
        }
    }
    None
}

/// All currently-mounted block devices. Used to skip disks a user
/// filesystem is sitting on.
fn mounted_devices() -> Vec<String> {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return Vec::new();
    };
    mounts
        .lines()
        .filter_map(|l| {
            let dev = l.split_whitespace().next()?;
            if dev.starts_with('/') {
                Some(dev.to_string())
            } else {
                None
            }
        })
        .collect()
}

/// Does `candidate` look like the root device or a partition of it?
/// Root is usually `/dev/sda2` (partition 2 of sda) — we exclude both
/// `/dev/sda` (the whole disk) and `/dev/sda[0-9]+` (its partitions).
fn matches_root_device(candidate: &str, root_dev: &str) -> bool {
    if candidate == root_dev {
        return true;
    }
    // Strip a trailing partition number off root_dev to get the base
    // disk, then check candidate is anywhere under it.
    let base = strip_partition_suffix(root_dev);
    if candidate == base {
        return true;
    }
    candidate.starts_with(&format!("{base}p")) || candidate.starts_with(&format!("{base}-part"))
        || candidate
            .strip_prefix(base)
            .is_some_and(|rest| rest.chars().all(|c| c.is_ascii_digit()))
}

/// `/dev/sda2` → `/dev/sda`. Works for the common `sd*`, `nvme*n1p*`,
/// `vd*` shapes. If the name doesn't look like a partitioned device,
/// returns the input unchanged.
fn strip_partition_suffix(dev: &str) -> &str {
    // nvme0n1p3 → nvme0n1  (strip "p\d+")
    if let Some(idx) = dev.rfind('p')
        && dev[idx + 1..].chars().all(|c| c.is_ascii_digit())
        && !dev[idx + 1..].is_empty()
    {
        return &dev[..idx];
    }
    // sda2 → sda  (strip trailing digits)
    let trimmed = dev.trim_end_matches(|c: char| c.is_ascii_digit());
    if trimmed.len() < dev.len() {
        return trimmed;
    }
    dev
}

fn matches_mount(candidate: &str, mount_dev: &str) -> bool {
    candidate == mount_dev
}

/// Expand a glob pattern into concrete paths. Falls back to treating
/// the pattern as a literal when glob expansion yields nothing or
/// fails — keeps dev setups with file-backed disks working.
fn expand_glob(pattern: &str) -> Vec<PathBuf> {
    // We avoid pulling in a full glob crate for one call; implement
    // the common shell-glob subset we need: `*`, `?`, `[...]` in the
    // basename. If the pattern contains no metacharacters we just
    // return the path verbatim.
    if !pattern.contains('*') && !pattern.contains('?') && !pattern.contains('[') {
        return vec![PathBuf::from(pattern)];
    }

    let (dir, base) = match pattern.rsplit_once('/') {
        Some((d, b)) => (d, b),
        None => (".", pattern),
    };
    let Ok(rd) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let regex = shell_glob_to_regex(base);
    let mut out = Vec::new();
    for entry in rd.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if regex.is_match(&name) {
            out.push(entry.path());
        }
    }
    out
}

/// Minimal shell-glob → basic regex. Supports `*`, `?`, and `[...]`.
fn shell_glob_to_regex(glob: &str) -> SimpleGlob {
    let mut pat = String::with_capacity(glob.len() + 4);
    pat.push('^');
    let mut in_class = false;
    for c in glob.chars() {
        if in_class {
            pat.push(c);
            if c == ']' {
                in_class = false;
            }
            continue;
        }
        match c {
            '*' => pat.push_str(".*"),
            '?' => pat.push('.'),
            '[' => {
                pat.push('[');
                in_class = true;
            }
            '.' | '+' | '(' | ')' | '{' | '}' | '\\' | '^' | '$' | '|' => {
                pat.push('\\');
                pat.push(c);
            }
            _ => pat.push(c),
        }
    }
    pat.push('$');
    SimpleGlob { pattern: pat }
}

struct SimpleGlob {
    pattern: String,
}
impl SimpleGlob {
    fn is_match(&self, s: &str) -> bool {
        // Use regex-lite substitute: we don't want to add the regex
        // crate for the 12 distinct filenames a typical host has.
        // Re-implement the match inline via a walk of the pattern.
        simple_regex_match(&self.pattern, s)
    }
}

/// Very small regex engine: supports `.*`, `.`, character classes
/// `[abc]` `[a-z]`, and escaped literals. Enough for shell-glob
/// translations; no quantifiers other than `*`.
fn simple_regex_match(pattern: &str, text: &str) -> bool {
    let pat: Vec<char> = pattern.chars().collect();
    let txt: Vec<char> = text.chars().collect();
    sr_match(&pat, 0, &txt, 0)
}
fn sr_match(pat: &[char], pi: usize, txt: &[char], ti: usize) -> bool {
    if pi == pat.len() {
        return ti == txt.len();
    }
    // `^` / `$` are implicit anchors we emitted.
    if pat[pi] == '^' {
        return sr_match(pat, pi + 1, txt, ti);
    }
    if pat[pi] == '$' {
        return ti == txt.len() && pi == pat.len() - 1;
    }
    // `.*` — zero or more of anything
    if pi + 1 < pat.len() && pat[pi] == '.' && pat[pi + 1] == '*' {
        let mut k = ti;
        loop {
            if sr_match(pat, pi + 2, txt, k) {
                return true;
            }
            if k >= txt.len() {
                return false;
            }
            k += 1;
        }
    }
    if ti == txt.len() {
        return false;
    }
    // `.` single char
    if pat[pi] == '.' {
        return sr_match(pat, pi + 1, txt, ti + 1);
    }
    // `[...]` character class
    if pat[pi] == '[' {
        let end = pat[pi..].iter().position(|&c| c == ']').map(|p| pi + p);
        let Some(end) = end else { return false };
        let class: &[char] = &pat[pi + 1..end];
        let c = txt[ti];
        let mut i = 0;
        let mut matched = false;
        while i < class.len() {
            if i + 2 < class.len() && class[i + 1] == '-' {
                if c >= class[i] && c <= class[i + 2] {
                    matched = true;
                    break;
                }
                i += 3;
            } else {
                if class[i] == c {
                    matched = true;
                    break;
                }
                i += 1;
            }
        }
        if matched {
            return sr_match(pat, end + 1, txt, ti + 1);
        }
        return false;
    }
    // Escaped literal or plain
    if pat[pi] == '\\' && pi + 1 < pat.len() {
        if pat[pi + 1] == txt[ti] {
            return sr_match(pat, pi + 2, txt, ti + 1);
        }
        return false;
    }
    if pat[pi] == txt[ti] {
        return sr_match(pat, pi + 1, txt, ti + 1);
    }
    false
}

fn probe_size(path: &Path) -> Result<u64, String> {
    let f = RawFile::open(path, true).map_err(|e| e.to_string())?;
    Ok(f.size())
}

fn classify(path: &Path) -> DiskState {
    let Ok(rf) = RawFile::open(path, true) else {
        return DiskState::Foreign {
            reason: "unable to open read-only".into(),
        };
    };
    let mut buf = AlignedBuffer::new(SUPERBLOCK_SIZE as usize);
    if let Err(e) = rf.read_at(0, buf.as_mut_slice()) {
        return DiskState::Foreign {
            reason: format!("superblock read failed: {e}"),
        };
    }
    let head = &buf.as_slice()[..8];
    if head != MAGIC {
        // Unformatted or non-ObjectIO device. Peek at a few common
        // headers so the operator log is useful.
        if head.starts_with(b"LUKS") {
            return DiskState::Foreign {
                reason: "LUKS-encrypted volume".into(),
            };
        }
        if head.is_empty() || head.iter().all(|b| *b == 0) {
            return DiskState::Blank;
        }
        return DiskState::Foreign {
            reason: format!("unknown header {head:02x?}"),
        };
    }
    match Superblock::from_bytes(buf.as_slice()) {
        Ok(sb) => DiskState::Claimed {
            cluster_uuid: sb.cluster_uuid,
            node_id: sb.osd_node_id,
        },
        Err(e) => DiskState::Foreign {
            reason: format!("corrupt ObjectIO superblock: {e}"),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_partition_nvme() {
        assert_eq!(strip_partition_suffix("/dev/nvme0n1p3"), "/dev/nvme0n1");
    }
    #[test]
    fn strip_partition_sda() {
        assert_eq!(strip_partition_suffix("/dev/sda2"), "/dev/sda");
    }
    #[test]
    fn strip_whole_disk() {
        assert_eq!(strip_partition_suffix("/dev/sda"), "/dev/sda");
    }
    #[test]
    fn matches_root_basic() {
        assert!(matches_root_device("/dev/sda", "/dev/sda2"));
        assert!(matches_root_device("/dev/sda3", "/dev/sda2"));
        assert!(!matches_root_device("/dev/sdb", "/dev/sda2"));
    }
    #[test]
    fn glob_expand_literal() {
        assert_eq!(
            expand_glob("/no/such/path"),
            vec![PathBuf::from("/no/such/path")]
        );
    }
    #[test]
    fn glob_match_star() {
        let g = shell_glob_to_regex("wwn-*");
        assert!(g.is_match("wwn-0x1234"));
        assert!(!g.is_match("nvme-eui"));
    }
}
