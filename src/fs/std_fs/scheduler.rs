// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Whether the I/O scheduler of the disks under a folder leaves writeback a
//! writer started behind the syncs that follow it.
//!
//! BFQ serves the asynchronous queue that started writeback lands in last,
//! and idles for each queue's next request: a sync waits out writeback that
//! never gets its turn (see [`Fs::writeback_stalls_syncs`](crate::fs::Fs::writeback_stalls_syncs)).
//!
//! The folder's mount gives the block device it was mounted from
//! (`/proc/self/mountinfo`; Btrfs reports a device number of its own for its
//! files, so the files' own device number names no disk). A partition's
//! scheduler is its disk's, and a stacked device (device mapper, md) has none
//! of its own: the disks under it, listed in `slaves/`, do its I/O. Any of
//! them under BFQ answers yes. A folder whose disks cannot be read answers no,
//! the behaviour of a backend that says nothing.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

/// How deep a stack of devices is followed: device mapper over md over a
/// partition is three.
const MAX_STACK: u8 = 8;

/// Whether a disk under `folder` runs an I/O scheduler that leaves started
/// writeback behind the syncs that follow it.
pub(super) fn writeback_stalls_syncs(folder: &Path) -> bool {
    let Some(device) = block_device(folder) else {
        return false;
    };
    let (major, minor) = split_device(device);
    stalls_under(
        &Path::new("/sys/dev/block").join(format!("{major}:{minor}")),
        0,
    )
}

/// The device number of the block device `folder`'s mount came from.
fn block_device(folder: &Path) -> Option<u64> {
    // A tree being created has no folder yet: its nearest existing ancestor
    // lies on the same mount.
    let existing = folder
        .ancestors()
        .find_map(|at| fs::canonicalize(at).ok())?;
    let mountinfo = fs::read_to_string("/proc/self/mountinfo").ok()?;
    let source = mount_source(&mountinfo, &existing)?;
    Some(fs::metadata(source).ok()?.rdev())
}

/// The source of the mount `path` lies on, from the text of
/// `/proc/self/mountinfo`, when it is a device node: the mount point that is
/// the longest prefix of `path` wins (proc(5): the mount point is field 5, the
/// source the second field after the ` - ` separator).
fn mount_source(mountinfo: &str, path: &Path) -> Option<PathBuf> {
    let mut best: Option<(usize, &str)> = None;
    for line in mountinfo.lines() {
        let Some((head, tail)) = line.split_once(" - ") else {
            continue;
        };
        let Some(point) = head.split(' ').nth(4) else {
            continue;
        };
        let point = unescape(point);
        if !path.starts_with(&point) {
            continue;
        }
        let Some(source) = tail.split(' ').nth(1) else {
            continue;
        };
        let depth = Path::new(&point).components().count();
        // A later line mounted over the same point hides the earlier one.
        if best.is_none_or(|(known, _)| depth >= known) {
            best = Some((depth, source));
        }
    }
    let source = unescape(best?.1);
    source.starts_with("/dev/").then(|| PathBuf::from(source))
}

/// Undoes the octal escapes proc(5) writes for a space, tab, newline and
/// backslash in a path.
fn unescape(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut rest = field;
    while let Some((before, after)) = rest.split_once('\\') {
        out.push_str(before);
        let escaped = after
            .get(..3)
            .and_then(|digits| u8::from_str_radix(digits, 8).ok());
        match (escaped, after.get(3..)) {
            (Some(byte), Some(tail)) => {
                out.push(char::from(byte));
                rest = tail;
            }
            _ => {
                out.push('\\');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// The major and minor numbers of a Linux device number (`gnu_dev_major`,
/// `gnu_dev_minor`).
fn split_device(device: u64) -> (u64, u64) {
    let major = ((device >> 32) & 0xffff_f000) | ((device >> 8) & 0x0fff);
    let minor = ((device >> 12) & 0xffff_ff00) | (device & 0x00ff);
    (major, minor)
}

/// Whether the device whose sysfs folder is `device`, or a disk under it,
/// runs BFQ.
fn stalls_under(device: &Path, depth: u8) -> bool {
    if depth > MAX_STACK {
        return false;
    }
    let Ok(device) = fs::canonicalize(device) else {
        return false;
    };
    // A partition's queue is its disk's, the folder above it.
    let disk = if device.join("partition").exists() {
        device
            .parent()
            .map_or_else(|| device.clone(), Path::to_path_buf)
    } else {
        device.clone()
    };
    if let Ok(line) = fs::read_to_string(disk.join("queue").join("scheduler"))
        && active_scheduler(&line) == Some("bfq")
    {
        return true;
    }
    let Ok(below) = fs::read_dir(device.join("slaves")) else {
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
