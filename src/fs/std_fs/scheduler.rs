// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Whether the I/O scheduler of the disks under a folder serves each queue in
//! turn and idles for its next request (BFQ); see
//! [`Fs::idles_between_io_queues`](crate::fs::Fs::idles_between_io_queues).
//!
//! The folder's own device number names its disk where sysfs lists it; else
//! the mount it lies on gives the block device it was mounted from
//! (`/proc/self/mountinfo`; Btrfs and overlay report a device number of their
//! own for their files, which names no disk). A partition's scheduler is its
//! disk's, and a stacked device (device mapper, md) has none of its own: the
//! disks under it, listed in `slaves/`, do its I/O. Any of them under BFQ
//! answers yes. A folder whose disks cannot be read answers no, the behaviour
//! of a backend that says nothing.
//!
//! No path is resolved here: resolving one reads a link for each of its
//! components, about forty calls an ask that took 0.47 ms. The kernel follows
//! sysfs's links as it walks a path, and takes `..` from the folder a link
//! leads to.

use std::fs;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

/// How deep a stack of devices is followed: device mapper over md over a
/// partition is three.
const MAX_STACK: u8 = 8;

/// Whether a disk under `folder` runs BFQ.
pub(super) fn idles_between_queues(folder: &Path) -> bool {
    let Some(device) = block_device(folder) else {
        return false;
    };
    let (major, minor) = split_device(device);
    stalls_under(
        &Path::new("/sys/dev/block").join(format!("{major}:{minor}")),
        0,
    )
}

/// The device number of the block device `folder` lies on.
fn block_device(folder: &Path) -> Option<u64> {
    // A tree being created has no folder yet: its nearest existing ancestor
    // lies on the same mount.
    let (existing, meta) = folder.ancestors().find_map(|at| {
        // The last ancestor of a relative path is empty: the working folder.
        let at = if at.as_os_str().is_empty() {
            Path::new(".")
        } else {
            at
        };
        fs::metadata(at).ok().map(|meta| (at, meta))
    })?;
    if let Some(device) = listed_device(meta.dev(), Path::new("/sys/dev/block")) {
        return Some(device);
    }
    let mount = mount_id(existing)?;
    // As bytes: a mount point anywhere in the table may hold a byte that is
    // not UTF-8, which proc(5) writes as it is.
    let mountinfo = read_bytes(Path::new("/proc/self/mountinfo"))?;
    // The one device the mount names. A file system over several (Btrfs
    // names one member, bcachefs lists them joined by `:`) is judged by that
    // one or answers no: schedulers mixed inside one file system are rare,
    // and a wrong no only leaves the batch policy off, never data at risk.
    let source = mount_source(&mountinfo, mount)?;
    Some(fs::metadata(source).ok()?.rdev())
}

/// `device` when it names a block device the folder `block` (sysfs's
/// `/sys/dev/block`) lists; `None` for a device number of a file system's own
/// (major 0) or one no disk answers to.
fn listed_device(device: u64, block: &Path) -> Option<u64> {
    let (major, minor) = split_device(device);
    (major != 0 && block.join(format!("{major}:{minor}")).exists()).then_some(device)
}

/// The id of the mount `path` lies on, as `/proc/self/mountinfo` numbers its
/// mounts: the `mnt_id` the `fdinfo` of a descriptor of it reports (proc(5)).
fn mount_id(path: &Path) -> Option<u64> {
    // A descriptor that only names the folder: no permission to read it is
    // needed.
    let folder = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH)
        .open(path)
        .ok()?;
    let info = read_text(Path::new(&format!(
        "/proc/self/fdinfo/{}",
        folder.as_raw_fd()
    )))?;
    fdinfo_mount_id(&info)
}

/// The text of a file of `/proc` or sysfs, read into a buffer sized ahead:
/// they report no size, and a read that starts from none probes them a few
/// bytes at a time, a call each.
fn read_text(path: &Path) -> Option<String> {
    String::from_utf8(read_bytes(path)?).ok()
}

/// The bytes of a file of `/proc` or sysfs, read as [`read_text`] reads.
fn read_bytes(path: &Path) -> Option<Vec<u8>> {
    use std::io::Read as _;
    let mut bytes = Vec::with_capacity(16 * 1024);
    fs::File::open(path).ok()?.read_to_end(&mut bytes).ok()?;
    Some(bytes)
}

/// The `mnt_id` line of a descriptor's `fdinfo` text.
fn fdinfo_mount_id(info: &str) -> Option<u64> {
    info.lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .and_then(|id| id.trim().parse().ok())
}

/// The source of mount `id`, from the text of `/proc/self/mountinfo`, when it
/// is a device node (proc(5): the mount id is field 1, the source the second
/// field after the ` - ` separator).
fn mount_source(mountinfo: &[u8], id: u64) -> Option<PathBuf> {
    use std::os::unix::ffi::OsStringExt as _;
    let source = mountinfo.split(|&b| b == b'\n').find_map(|line| {
        let at = line.windows(3).position(|sep| sep == b" - ")?;
        let (head, tail) = (line.get(..at)?, line.get(at + 3..)?);
        let first = head.split(|&b| b == b' ').next()?;
        if core::str::from_utf8(first).ok()?.parse::<u64>().ok()? != id {
            return None;
        }
        tail.split(|&b| b == b' ').nth(1)
    })?;
    let source = unescape(source);
    source
        .starts_with(b"/dev/")
        .then(|| PathBuf::from(std::ffi::OsString::from_vec(source)))
}

/// Undoes the octal escapes proc(5) writes for a space, tab, newline and
/// backslash in a path.
fn unescape(field: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(field.len());
    let mut rest = field;
    while let Some(at) = rest.iter().position(|&b| b == b'\\') {
        out.extend_from_slice(rest.get(..at).unwrap_or_default());
        let after = rest.get(at + 1..).unwrap_or_default();
        let escaped = after
            .get(..3)
            .and_then(|digits| core::str::from_utf8(digits).ok())
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        if let (Some(byte), Some(tail)) = (escaped, after.get(3..)) {
            out.push(byte);
            rest = tail;
        } else {
            out.push(b'\\');
            rest = after;
        }
    }
    out.extend_from_slice(rest);
    out
}

/// The major and minor numbers of a Linux device number (`gnu_dev_major`,
/// `gnu_dev_minor`).
fn split_device(device: u64) -> (u64, u64) {
    let major = ((device >> 32) & 0xffff_f000) | ((device >> 8) & 0x0fff);
    let minor = ((device >> 12) & 0xffff_ff00) | (device & 0x00ff);
    (major, minor)
}

/// Whether the device whose sysfs folder (or a link to it) is `device`, or a
/// disk under it, runs BFQ.
fn stalls_under(device: &Path, depth: u8) -> bool {
    if depth > MAX_STACK {
        return false;
    }
    // A partition's queue is its disk's, the folder above it.
    let disk = if device.join("partition").exists() {
        device.join("..")
    } else {
        device.to_path_buf()
    };
    if let Some(line) = read_text(&disk.join("queue").join("scheduler"))
        && active_scheduler(&line) == Some("bfq")
    {
        return true;
    }
    // A stacked device lists the disks under it; a partition of one lists
    // none, its device does.
    let Ok(below) = fs::read_dir(disk.join("slaves")) else {
        return false;
    };
    below
        .filter_map(Result::ok)
        .any(|slave| stalls_under(&slave.path(), depth + 1))
}

/// The scheduler a `queue/scheduler` line names active: the one in brackets
/// (`none [mq-deadline] kyber bfq`). A device without a scheduler lists only
/// `none`.
fn active_scheduler(line: &str) -> Option<&str> {
    let start = line.find('[')? + 1;
    let end = start + line.get(start..)?.find(']')?;
    line.get(start..end)
}

#[cfg(test)]
mod tests;
