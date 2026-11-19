// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A batch of syncs submitted from the calling thread to one `io_uring`.
//!
//! The kernel runs each sync in a worker that shares the submitting thread's
//! I/O context (`create_io_thread` clones with `CLONE_IO`), so the syncs run
//! side by side and an I/O scheduler that serves one context at a time and
//! idles for its next request (BFQ, `slice_idle`) sees one context, not one
//! per thread. Syncs run on threads of their own each had a context of their
//! own, and BFQ idled between them: a flush of a table and a blob file took 4,
//! 17, 24, 45 or about 100 ms where one context took 5.

use super::super::{SyncFailure, SyncTarget};
use crate::io;
use core::cell::RefCell;
use io_uring::{IoUring, opcode, squeue, types};
use std::fs::{File, OpenOptions};
use std::os::unix::io::AsRawFd;

/// The fewest entries a ring is set up with: a flush of a table, a blob file
/// and their folders fits.
const MIN_ENTRIES: u32 = 8;

/// The most targets open and in flight at once: a persist of many deferred
/// flushes goes in chunks of this many, never holding a descriptor for each
/// of its files.
const MAX_IN_FLIGHT: usize = 64;

std::thread_local! {
    /// The ring this thread's batches go through, kept between them: setting
    /// one up maps its queues and its first sync starts the kernel's workers,
    /// work a flush would otherwise pay each time.
    // no-std: unreachable without std (the kernel backend is std-only).
    static RING: RefCell<Option<IoUring>> = const { RefCell::new(None) };
    /// Whether a ring refused the sync opcode on this thread: a partial
    /// `io_uring` (a sandboxing runtime) that sets rings up and syncs nothing
    /// through them. The batches then go another way without asking again.
    static NO_RING_SYNC: core::cell::Cell<bool> = const { core::cell::Cell::new(false) };
}

/// How a batch went through a ring.
#[derive(Debug, PartialEq, Eq)]
enum Through {
    /// Every target synced.
    Synced,
    /// The ring does not run the sync opcode: nothing is known to be synced.
    Refused,
}

/// Whether a sync's completion `errno` says the ring does not run the opcode
/// at all, rather than that the sync failed.
const fn refuses_sync(errno: i32) -> bool {
    errno == libc::EINVAL || errno == libc::EOPNOTSUPP
}

/// What the completions of a batch's syncs say, as they are reaped.
#[derive(Default)]
struct Completions {
    /// The first sync that failed, with every target a sync failed on: its
    /// error is the batch's, since a sync done again through a new handle may
    /// not report the writeback error this one consumed.
    failed: Option<SyncFailure>,
    /// The targets whose completion answered as a ring that does not run the
    /// opcode would, with the first such error.
    refused: Vec<(usize, i32)>,
    /// How many completions were taken.
    reaped: usize,
}

impl Completions {
    /// Takes the `result` of the completion of target `at`.
    fn note(&mut self, at: usize, result: i32) {
        self.reaped += 1;
        if result >= 0 {
            return;
        }
        if refuses_sync(-result) {
            self.refused.push((at, -result));
        } else {
            self.fail(at, -result);
        }
    }

    /// Records the sync of target `at` as failed with `errno`.
    fn fail(&mut self, at: usize, errno: i32) {
        self.failed
            .get_or_insert_with(|| {
                SyncFailure::unspent(io::Error::from(std::io::Error::from_raw_os_error(errno)))
            })
            .spent
            .push(at);
    }

    /// The batch's outcome. A ring that does not run the opcode answers every
    /// sync alike, so only a batch whose every sync answered so is refused;
    /// beside a sync that ran, the same `errno` is that target's own (a
    /// `/dev/null` has no sync) and fails the batch like any other.
    fn outcome(mut self) -> Result<Through, SyncFailure> {
        if self.failed.is_none() && !self.refused.is_empty() && self.refused.len() == self.reaped {
            return Ok(Through::Refused);
        }
        for (at, errno) in core::mem::take(&mut self.refused) {
            self.fail(at, errno);
        }
        self.failed.map_or(Ok(Through::Synced), Err)
    }
}

/// Syncs every target of `targets` through this thread's ring and waits for
/// all of them: `None` when no ring can be set up, for the caller to sync
/// another way. Linux has one data sync for every [`crate::fs::SyncMode`].
pub(super) fn sync_on_ring(targets: &[SyncTarget<'_>]) -> Option<Result<(), SyncFailure>> {
    if NO_RING_SYNC.get() {
        return None;
    }
    let entries = u32::try_from(targets.len().min(MAX_IN_FLIGHT))
        .ok()?
        .next_power_of_two()
        .max(MIN_ENTRIES);
    RING.with_borrow_mut(|slot| {
        let fits = slot
            .as_ref()
            .is_some_and(|ring| ring.params().sq_entries() >= entries);
        if !fits {
            match IoUring::new(entries) {
                Ok(ring) => *slot = Some(ring),
                Err(e) => {
                    log::debug!("no io_uring for a batch of syncs, syncing on threads: {e}");
                    return None;
                }
            }
        }
        let ring = slot.as_mut()?;
        let mut refused = false;
        let synced = targets
            .chunks(MAX_IN_FLIGHT)
            .enumerate()
            .try_for_each(|(chunk_at, chunk)| {
                match sync_through(ring, chunk_at * MAX_IN_FLIGHT, chunk)? {
                    Through::Synced => Ok(()),
                    Through::Refused => {
                        refused = true;
                        Err(SyncFailure::unspent(io::Error::other(
                            "io_uring refused the sync opcode",
                        )))
                    }
                }
            });
        if synced.is_err() {
            // A batch that failed may have left entries in flight: the next
            // batch gets a ring of its own rather than their completions.
            *slot = None;
        }
        if refused {
            // The caller syncs every target another way: a sync done twice
            // is done once.
            log::debug!("io_uring refused the sync opcode, syncing on threads");
            NO_RING_SYNC.set(true);
            return None;
        }
        Some(synced)
    })
}

/// Opens every target, submits a sync of each to `ring` and reaps them all;
/// `base` is the index of the first target in the batch.
fn sync_through(
    ring: &mut IoUring,
    base: usize,
    targets: &[SyncTarget<'_>],
) -> Result<Through, SyncFailure> {
    // Held open until every sync is reaped. A failed open syncs nothing; one
    // the descriptors ran out for is tried again in halves, the opened ones
    // closed first, so a process near its limit still gets the batch synced.
    let opened: Vec<Handle> = match targets.iter().map(open).collect::<io::Result<_>>() {
        Ok(opened) => opened,
        Err(e) if e.kind() == io::ErrorKind::TooManyOpenFiles && targets.len() > 1 => {
            let (first, second) = targets.split_at(targets.len() / 2);
            return match sync_through(ring, base, first)? {
                Through::Synced => sync_through(ring, base + first.len(), second),
                Through::Refused => Ok(Through::Refused),
            };
        }
        Err(e) => return Err(SyncFailure::unspent(e)),
    };
    // A handle kept open with no descriptor of the kernel behind it cannot be
    // synced here: the batch goes the way that syncs through the handle.
    let fds: Option<Vec<i32>> = opened
        .iter()
        .map(|handle| match handle {
            Handle::Opened(file) => Some(file.as_raw_fd()),
            Handle::Kept(fd) => Some(*fd),
            Handle::Unreachable => None,
        })
        .collect();
    let Some(fds) = fds else {
        return Ok(Through::Refused);
    };
    for (at, (&fd, target)) in fds.iter().zip(targets).enumerate() {
        let sync = opcode::Fsync::new(types::Fd(fd));
        let sync = match target {
            SyncTarget::FileData { .. } | SyncTarget::OpenFile { .. } => {
                sync.flags(types::FsyncFlags::DATASYNC)
            }
            // A folder's entries are its data and its size alike: `fsync`, as
            // `StdFs::sync_directory_with` issues.
            SyncTarget::Folder { .. } => sync,
        };
        if let Err(error) = push(ring, &sync.build().user_data(at as u64)) {
            // Those pushed before may run: none is reported synced.
            return Err(SyncFailure {
                error,
                spent: (base..base + at).collect(),
            });
        }
    }
    let mut completions = Completions::default();
    let mut reaped = alloc::vec![false; opened.len()];
    let mut left = opened.len();
    while left > 0 {
        if let Err(e) = ring.submit_and_wait(left) {
            // Interrupted: the submitted entries stay in flight, and the next
            // wait collects them.
            if e.kind() != std::io::ErrorKind::Interrupted {
                // A submission the kernel took none of (a sandbox refusing
                // `io_uring_enter`) ran no sync: the batch goes another way.
                if completions.reaped == 0 && ring.submission().len() == opened.len() {
                    return Ok(Through::Refused);
                }
                // Whatever was submitted is not waited out here; the ring is
                // dropped, and the kernel finishes its entries before freeing
                // it. Each sync not reaped may have run and failed, so it is
                // spent with those that did fail.
                let mut failure = completions
                    .failed
                    .unwrap_or_else(|| SyncFailure::unspent(io::Error::from(e)));
                failure.spent.extend(
                    reaped
                        .iter()
                        .enumerate()
                        .filter(|(_, done)| !**done)
                        .map(|(at, _)| base + at),
                );
                return Err(failure);
            }
        }
        for done in ring.completion() {
            left -= 1;
            // Each entry carries its target's index in the chunk, as pushed.
            let Some((at, slot)) = usize::try_from(done.user_data())
                .ok()
                .and_then(|at| Some((at, reaped.get_mut(at)?)))
            else {
                continue;
            };
            *slot = true;
            completions.note(base + at, done.result());
        }
    }
    completions.outcome()
}

/// Queues `entry`. The ring has an entry for every target, so a full queue
/// is no state a batch reaches; it is reported, not assumed away.
fn push(ring: &mut IoUring, entry: &squeue::Entry) -> io::Result<()> {
    // SAFETY: the entry names a descriptor of `opened` in `sync_through`, open
    // until every entry is reaped (or the ring that holds them is dropped), or
    // one of a handle the caller holds across the batch; the kernel takes its
    // own reference to the file at submission. The entry carries no buffer.
    let pushed = unsafe { ring.submission().push(entry) };
    pushed.map_err(|_| io::Error::other("io_uring submission queue full"))
}

/// What a target is synced through.
enum Handle {
    /// A file or folder opened for the sync.
    Opened(File),
    /// The descriptor of a handle the caller keeps open across the batch.
    Kept(i32),
    /// A handle the caller keeps open with no descriptor behind it.
    Unreachable,
}

/// The file or folder `target` names, open for its sync.
fn open(target: &SyncTarget<'_>) -> io::Result<Handle> {
    match *target {
        SyncTarget::FileData { path, .. } => {
            Ok(Handle::Opened(OpenOptions::new().write(true).open(path)?))
        }
        SyncTarget::OpenFile { file, .. } => {
            Ok(file.backing_fd().map_or(Handle::Unreachable, Handle::Kept))
        }
        SyncTarget::Folder { path, .. } => {
            let folder = File::open(path)?;
            if !folder.metadata()?.is_dir() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "sync_directory: path is not a directory",
                ));
            }
            Ok(Handle::Opened(folder))
        }
    }
}

#[cfg(test)]
mod tests;
