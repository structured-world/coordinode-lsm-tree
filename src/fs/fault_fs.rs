// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Programmable fault-injection [`Fs`] backend for crash- and error-path
//! testing.
//!
//! [`FaultFs`] wraps any inner [`Fs`] and, before delegating each operation,
//! consults a shared [`FaultInjector`] that can be programmed to make specific
//! operations fail (return an [`io::Error`]) or short-write. This lets a test
//! drive the storage engine through I/O failures it could not otherwise induce
//! against a real disk: a manifest write that fails mid-compaction, an `fsync`
//! that errors before a rename is durable, a write that stops accepting bytes
//! part-way through.
//!
//! Faults are matched by operation kind ([`FaultOp`]) plus an optional path
//! substring, with a skip count (let the first N matches pass) and a fire count
//! (how many matches to fault before the rule is exhausted). Rules are armed via
//! the [`FaultRule`] builder.
//!
//! This is a test/dev surface: it is gated behind the `std` feature and is not
//! part of the production storage path.
//!
//! # Examples
//!
//! ```
//! use lsm_tree::fs::{FaultFs, FaultOp, FaultRule, Fault, StdFs};
//! use lsm_tree::io::ErrorKind;
//!
//! let fs = FaultFs::new(StdFs);
//! // Make the first rename whose path contains "manifest" fail once.
//! fs.injector().arm(
//!     FaultRule::new(FaultOp::Rename, Fault::Error(ErrorKind::Other))
//!         .on_path("manifest")
//!         .once(),
//! );
//! // `fs` can now be installed via `Config::with_fs(fs)`; the engine sees the
//! // injected failure on the targeted rename and nowhere else.
//! ```

use super::{
    BlockRead, FileHint, Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions,
    QueuedRead, ReadDone, ReadQueue, ReadWake, SyncMode,
};
use crate::io;
use crate::path::{Path, PathBuf};
use alloc::boxed::Box;
use alloc::string::String;
use alloc::sync::Arc;
use alloc::vec::Vec;

/// The kind of filesystem operation a [`FaultRule`] targets.
///
/// Each variant names a hookable point in the [`Fs`] / [`FsFile`] surface that
/// [`FaultFs`] consults the injector for before delegating. Operations not
/// listed here (directory listing, metadata, capability probes, locking) are
/// delegated unconditionally; add a variant when a test needs to fault one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum FaultOp {
    /// [`Fs::open`] — opening (or creating) a file.
    Open,
    /// [`Fs::create_dir_all`] — recursive directory creation.
    CreateDirAll,
    /// [`Fs::create_dir`] — single-directory creation.
    CreateDir,
    /// [`Fs::remove_file`] — file removal.
    RemoveFile,
    /// [`Fs::rename`] — rename (the atomic version-pointer / manifest swap).
    /// Matched against the destination (`to`) path.
    Rename,
    /// [`Fs::sync_directory`] — directory fsync.
    SyncDirectory,
    /// Buffered/streaming write through [`std::io::Write::write`] on an open
    /// file. The only op for which [`Fault::ShortWrite`] is meaningful.
    Write,
    /// [`std::io::Write::flush`] on an open file.
    Flush,
    /// Positional read via [`FsFile::read_at`].
    ReadAt,
    /// Streaming read via [`std::io::Read::read`] on an open file.
    Read,
    /// [`FsFile::sync_all`] (and `sync_all_with`) — full durability barrier.
    SyncAll,
    /// [`FsFile::sync_data`] (and `sync_data_with`) — data durability barrier.
    SyncData,
    /// [`FsFile::set_len`] — truncate / extend.
    SetLen,
    /// [`Fs::metadata`] — path metadata probe (existence / size). Failing it
    /// models a stat that races file creation or an inaccessible path, e.g. to
    /// exercise TOCTOU windows between an existence check and an open.
    Metadata,
    /// [`FsFile::hard_link_count`] — link-count probe consulted before
    /// in-place mutation. Failing it exercises the fail-closed path (treat
    /// the file as shared).
    HardLinkCount,
    /// [`Fs::hard_link`] — the destination link of a copy-style publish.
    /// Failing it with [`io::ErrorKind::Unsupported`] models a backend that
    /// leaves the trait default in place (no hard-link support).
    HardLink,
    /// [`Fs::available_space`] — the free-space probe every space decision
    /// consults. Failing it models a backend that cannot report free space
    /// (an object store, a quota-only deployment), which the engine reads as
    /// "physically unbounded" — so a test arms it to prove the LOGICAL quota
    /// still bounds what the engine admits.
    AvailableSpace,
    /// [`Fs::allocated_size`] — the physical-allocation probe manifest repair
    /// uses to detect a hole-punched SST. Failing it exercises the fail-closed
    /// path (a probe error must not be read as "unpunched").
    AllocatedSize,
    /// [`Fs::punch_hole`] — the tight-space per-block prefix reclaim. Failing
    /// it exercises the partial-punch geometry recovery (the reclaim stops at
    /// the first failed punch so the resulting hole pattern stays classifiable).
    PunchHole,
    /// [`Fs::same_file`] — the physical-identity probe that decides whether two
    /// directory spellings name one file. Failing it models a host that refuses
    /// `canonicalize` / `stat`, and exercises the fail-closed path (an
    /// unanswerable alias probe must not be read as "these are distinct
    /// files"). Matched against the FIRST path.
    SameFile,
    /// [`Fs::reflink_file`] — the copy-on-write clone a checkpoint prefers on a
    /// filesystem that offers it. Paired with [`Self::HardLink`], failing it
    /// models a publish that cannot place its destination file whichever
    /// strategy the filesystem admits, without depending on the host's
    /// reflink support. Matched against the DESTINATION path.
    Reflink,
    /// [`FsFile::start_writeback`] — the writeback hint a writer gives as it
    /// goes. Failing it models a backend or kernel that refuses the hint.
    StartWriteback,
    /// [`Fs::volume_id`] — the volume a path lies on. Any fault armed on it
    /// makes the probe answer `None`, as a backend that cannot tell the
    /// volume of that path does.
    VolumeId,
    /// [`Fs::remove_dir_all`]: the removal of a directory and all it holds.
    RemoveDirAll,
    /// [`Fs::remove_dir`]: the removal of an empty directory.
    RemoveDir,
}

/// What a sync seen by a [`FaultFs`] made durable (see
/// [`FaultInjector::take_syncs`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[non_exhaustive]
pub enum SyncKind {
    /// A file's data and metadata: [`FsFile::sync_all`] and its mode variant.
    File,
    /// A file's data and the size a read of it needs: [`FsFile::sync_data`]
    /// and its mode variant.
    FileData,
    /// A directory's entries: [`Fs::sync_directory`] and its mode variant.
    Directory,
    /// Everything synced on a device before: [`Fs::sync_device`].
    Device,
}

/// What a matched [`FaultRule`] does to the operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Fault {
    /// Fail the operation by returning an [`io::Error`] of this kind.
    Error(io::ErrorKind),
    /// Accept only this many bytes of a write and report `Ok(n)`, silently
    /// dropping the rest. `0` models a stuck writer (the engine's `write_all`
    /// turns it into [`io::ErrorKind::WriteZero`]). Only meaningful for
    /// [`FaultOp::Write`]; ignored on any other op.
    ShortWrite(usize),
}

/// A single fault-injection rule, built fluently and armed on a
/// [`FaultInjector`].
///
/// A rule matches an operation when its [`FaultOp`] equals the operation kind
/// and, if [`on_path`](Self::on_path) was set, the operation's path contains the
/// substring. For each matching operation the rule first burns its
/// [`skip`](Self::skip) budget (letting that many matches pass untouched), then
/// fires its [`Fault`] up to [`times`](Self::times) more matches before becoming
/// exhausted.
#[derive(Clone, Debug)]
pub struct FaultRule {
    op: FaultOp,
    path_substr: Option<String>,
    offset: Option<u64>,
    skip: u64,
    count: u64,
    fault: Fault,
}

impl FaultRule {
    /// Creates a rule that, by default, fires `fault` on **every** matching
    /// `op` (no path filter, no skip, unlimited fires). Refine with
    /// [`on_path`](Self::on_path), [`at_offset`](Self::at_offset),
    /// [`skip`](Self::skip), [`times`](Self::times), or [`once`](Self::once).
    #[must_use]
    pub const fn new(op: FaultOp, fault: Fault) -> Self {
        Self {
            op,
            path_substr: None,
            offset: None,
            skip: 0,
            count: u64::MAX,
            fault,
        }
    }

    /// Restricts the rule to operations whose path contains `substr`.
    ///
    /// For [`FaultOp::Rename`] the destination (`to`) path is matched.
    #[must_use]
    pub fn on_path(mut self, substr: impl Into<String>) -> Self {
        self.path_substr = Some(substr.into());
        self
    }

    /// Restricts a [`FaultOp::ReadAt`] rule to positioned reads at exactly
    /// `offset`. Lets a test fault ONE section of a file (e.g. a single data
    /// block) while every other positioned read on the same file proceeds, so
    /// a fault can isolate one decode-load from the reads a recovery pass makes
    /// on the same path. Only [`FaultOp::ReadAt`] carries an offset: setting one
    /// on any other op makes the rule UNMATCHABLE (it never fires), so the debug
    /// assertion below catches that mistake instead of silently disabling it.
    #[must_use]
    pub fn at_offset(mut self, offset: u64) -> Self {
        debug_assert!(
            matches!(self.op, FaultOp::ReadAt),
            "at_offset only applies to FaultOp::ReadAt; setting it on {:?} makes the rule \
             unmatchable",
            self.op,
        );
        self.offset = Some(offset);
        self
    }

    /// Lets the first `n` matching operations pass untouched before the rule
    /// starts firing.
    #[must_use]
    pub const fn skip(mut self, n: u64) -> Self {
        self.skip = n;
        self
    }

    /// Caps the rule at `n` fires; after that it is exhausted and matching
    /// operations pass untouched. The default is unlimited.
    #[must_use]
    pub const fn times(mut self, n: u64) -> Self {
        self.count = n;
        self
    }

    /// Fires exactly once, then is exhausted. Shorthand for `.times(1)`.
    #[must_use]
    pub const fn once(self) -> Self {
        self.times(1)
    }

    /// Returns `true` if this rule matches an operation of `op` at `path`.
    ///
    /// Offset-scoped rules ([`at_offset`](Self::at_offset)) never match here:
    /// only the offset-aware [`FaultInjector::check_read_at`] path can fire
    /// them, since a positioned offset is meaningful only for
    /// [`FaultOp::ReadAt`].
    fn matches(&self, op: FaultOp, path: Option<&Path>) -> bool {
        if self.op != op || self.offset.is_some() {
            return false;
        }
        self.matches_path(path)
    }

    /// Returns `true` if this [`FaultOp::ReadAt`] rule matches a positioned read
    /// at `offset` on `path`. An unset [`at_offset`](Self::at_offset) matches
    /// any offset; a set one matches only that exact offset.
    fn matches_read_at(&self, path: Option<&Path>, offset: u64) -> bool {
        if self.op != FaultOp::ReadAt {
            return false;
        }
        if self.offset.is_some_and(|want| want != offset) {
            return false;
        }
        self.matches_path(path)
    }

    fn matches_path(&self, path: Option<&Path>) -> bool {
        match (&self.path_substr, path) {
            (None, _) => true,
            (Some(sub), Some(p)) => p.to_string_lossy().contains(sub.as_str()),
            (Some(_), None) => false,
        }
    }
}

/// Shared, programmable fault state consulted by a [`FaultFs`].
///
/// Held behind an [`Arc`] so a test can program rules through one handle while
/// the wrapped [`FaultFs`] (installed deep inside the engine) consults the same
/// state. Interior mutability is a [`spin::Mutex`] — fault checks are off the
/// production path, so a tiny spin lock keeps the type `no_std`-friendly and
/// poison-free.
#[derive(Default)]
pub struct FaultInjector {
    rules: spin::Mutex<Vec<FaultRule>>,
    /// Observed `(path, mode)` pairs from every file-level `sync_all_with` /
    /// `sync_data_with`, in call order — lets a test assert WHICH durability
    /// mode a component synced its files at (e.g. a salvage writer honouring
    /// `Config::sync_mode`), which the pass-through delegation cannot show.
    sync_log: spin::Mutex<Vec<(PathBuf, SyncMode)>>,
    /// Observed `(path, mode)` pairs from every `sync_directory_with`, in call
    /// order: which of a directory's syncs flush the device is a cost.
    directory_sync_log: spin::Mutex<Vec<(PathBuf, SyncMode)>>,
    /// Every sync of any kind, with its path, in call order: how many syncs
    /// an operation costs is part of what it does.
    syncs: spin::Mutex<Vec<(PathBuf, SyncKind)>>,
    /// Observed `(path, offset, len)` of every `start_writeback`, in call
    /// order: a writeback changes no byte, so only this shows it was asked.
    writeback_log: spin::Mutex<Vec<(PathBuf, u64, u64)>>,
    /// Positional reads served, counted whether or not a rule matched — the
    /// pass-through delegation is otherwise invisible, so a test that cares
    /// about how MUCH a path reads (a verification walking each block once
    /// rather than once per gate) has nothing to assert on.
    reads: core::sync::atomic::AtomicUsize,
    /// Bumped by [`Self::clear`], under the rules' lock: what screening spent
    /// before a clear is never given back into the state after it.
    generation: core::sync::atomic::AtomicU64,
    /// Files opened, counted whether or not a rule matched. A pass that reads
    /// several sections of one file can open it once and lend the handle, or
    /// open it per section; only the count tells the two apart.
    opens: core::sync::atomic::AtomicUsize,
    /// Every handle open now.
    open_handles: spin::Mutex<Vec<OpenHandle>>,
    next_handle: core::sync::atomic::AtomicU64,
    /// Whether a removal of a file with an open handle fails, as on a backend
    /// that refuses to unlink an open file.
    refuse_open_removal: core::sync::atomic::AtomicBool,
    /// How many more syncs pass before the power is off, when set: see
    /// [`Self::power_off_after_syncs`].
    power: spin::Mutex<Option<usize>>,
    /// Called on a positional read at the paired offset of a file whose path
    /// contains the paired text, before the read is served, so a test can
    /// hold a read in flight while other readers run.
    #[cfg(test)]
    read_hooks: spin::Mutex<Vec<(String, u64, ReadHook)>>,
    /// Called on a removal of a path containing the paired text, before the
    /// removal is done, so a test can see the state the removal happens in.
    #[cfg(test)]
    removal_hooks: spin::Mutex<Vec<(String, RemovalHook)>>,
}

/// What a test runs inside a positional read at one offset.
#[cfg(test)]
pub type ReadHook = Arc<dyn Fn() + Send + Sync>;

/// What a test runs before a removal, given the removal's kind and path.
#[cfg(test)]
pub type RemovalHook = Arc<dyn Fn(FaultOp, &Path) + Send + Sync>;

std::thread_local! {
    /// The injectors whose batches and queue waits this thread is inside:
    /// only there does a handle opened through that injector's wrapper show
    /// its descriptor, so no backend reads one past the read rules of the
    /// wrapper it was opened through, another wrapper's included.
    static LENT: core::cell::RefCell<Vec<usize>> = const { core::cell::RefCell::new(Vec::new()) };
    /// How deep this thread is in a lent handle asking the handle it wraps
    /// for its descriptor: a wrapper over another one sees the descriptor the
    /// inner handle holds, and each screens the read with its own rules.
    static PASSING: core::cell::Cell<u32> = const { core::cell::Cell::new(0) };
}

/// The identity a loan is kept under: the injector's address.
fn loan_key(injector: &FaultInjector) -> usize {
    core::ptr::from_ref(injector) as usize
}

/// Runs `f` with the descriptors of the handles opened through `injector`'s
/// wrapper shown, on this thread only.
fn lend_descriptors<R>(injector: &FaultInjector, f: impl FnOnce() -> R) -> R {
    /// Takes the thread's loan back however `f` ends.
    struct Loan(usize);
    impl Drop for Loan {
        fn drop(&mut self) {
            LENT.with(|lent| {
                let mut lent = lent.borrow_mut();
                if let Some(at) = lent.iter().rposition(|key| *key == self.0) {
                    lent.remove(at);
                }
            });
        }
    }
    let key = loan_key(injector);
    LENT.with(|lent| lent.borrow_mut().push(key));
    let _loan = Loan(key);
    f()
}

/// Runs `f`, a lent handle asking the handle it wraps for its descriptor.
fn pass_through<R>(f: impl FnOnce() -> R) -> R {
    /// Ends the pass however `f` ends.
    struct Pass;
    impl Drop for Pass {
        fn drop(&mut self) {
            // Every pass ends once, after it began.
            PASSING.with(|depth| depth.set(depth.get() - 1));
        }
    }
    // As deep as wrappers are nested in one another, far below `u32::MAX`.
    PASSING.with(|depth| depth.set(depth.get() + 1));
    let _pass = Pass;
    f()
}

/// Whether a handle opened through `injector`'s wrapper shows its descriptor
/// here: inside that wrapper's loan, or asked by a lent handle wrapping it.
fn descriptors_lent(injector: &FaultInjector) -> bool {
    PASSING.with(|depth| depth.get() > 0)
        || LENT.with(|lent| lent.borrow().contains(&loan_key(injector)))
}

/// A handle open through a [`FaultFs`].
struct OpenHandle {
    id: u64,
    /// The descriptor the inner backend reads it by, if any.
    fd: Option<i32>,
    /// The path it was opened at: the one its reads are matched against,
    /// through `FaultFile::read_at` or by its descriptor alike.
    opened: PathBuf,
    /// The path of the file it holds now: a rename moves the file under its
    /// open handles.
    current: PathBuf,
}

impl FaultInjector {
    /// Creates an injector with no rules (every operation passes untouched).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes a removal of a file that has an open handle fail with
    /// `PermissionDenied`, as on a backend that refuses to unlink an open file.
    #[cfg(test)]
    pub(crate) fn refuse_removing_open_files(&self) {
        self.refuse_open_removal
            .store(true, core::sync::atomic::Ordering::Relaxed);
    }

    /// Whether removing `path` is refused because a handle to it is open.
    fn removal_refused(&self, path: &Path) -> bool {
        self.refuse_open_removal
            .load(core::sync::atomic::Ordering::Relaxed)
            && self
                .open_handles
                .lock()
                .iter()
                .any(|open| open.current == path)
    }

    /// Records a handle opened on `path`, read by the inner backend through
    /// `fd` when it has one, and returns its id.
    fn note_opened(&self, path: &Path, fd: Option<i32>) -> u64 {
        let id = self
            .next_handle
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        self.open_handles.lock().push(OpenHandle {
            id,
            fd,
            opened: path.to_path_buf(),
            current: path.to_path_buf(),
        });
        id
    }

    fn note_closed(&self, handle: u64) {
        let mut open = self.open_handles.lock();
        if let Some(at) = open.iter().position(|open| open.id == handle) {
            open.swap_remove(at);
        }
    }

    fn note_renamed(&self, from: &Path, to: &Path) {
        for open in self.open_handles.lock().iter_mut() {
            if open.current == from {
                open.current = to.to_path_buf();
            }
        }
    }

    /// The path the read rules match a read of the handle the inner backend
    /// reads through `fd` against: the one it was opened at, as
    /// `FaultFile::read_at` matches it.
    fn path_of(&self, fd: i32) -> Option<PathBuf> {
        self.open_handles
            .lock()
            .iter()
            .find(|open| open.fd == Some(fd))
            .map(|open| open.opened.clone())
    }

    /// Takes a positional read of `path` at `offset` through the read rules:
    /// counts it, runs its hooks, and returns the error a rule fails it with.
    fn screen_read(&self, path: Option<&Path>, offset: u64) -> Option<io::Error> {
        self.count_read();
        #[cfg(test)]
        if let Some(path) = path {
            self.run_read_hooks(path, offset);
        }
        match self.check_read_at(path, offset) {
            Some(Fault::Error(kind)) => Some(fault_error(kind, FaultOp::ReadAt)),
            _ => None,
        }
    }

    /// Takes the request of `len` bytes of `file` at `offset` through the read
    /// rules when it has a descriptor, which an inner backend that reads by
    /// descriptor reads it by, past `FaultFile::read_at`. A file without one
    /// reaches `read_at`, which screens it there. A request with nothing to
    /// read is finished without a read, and spends no rule.
    ///
    /// Also returns what screening spent, so it can be given back.
    fn screen_by_fd(
        &self,
        file: &dyn FsFile,
        offset: u64,
        len: usize,
    ) -> (Option<io::Error>, Screened) {
        let fd = if len == 0 { None } else { file.backing_fd() };
        let Some(fd) = fd else {
            return (None, Screened::Not);
        };
        let path = self.path_of(fd);
        #[cfg(test)]
        if let Some(path) = &path {
            self.run_read_hooks(path, offset);
        }
        let (fault, rule, generation) = self.check_read_at_counted(path.as_deref(), offset);
        let error = match fault {
            Some(Fault::Error(kind)) => Some(fault_error(kind, FaultOp::ReadAt)),
            _ => None,
        };
        (error, Screened::Counted { rule, generation })
    }

    /// Screens `reqs` in order as [`Self::screen_by_fd`] does, up to the first
    /// a rule fails: how many come before it, its error, and what screening
    /// spent on each request screened, the failing one included, so what was
    /// spent on a request the inner backend never read can be given back.
    fn screen_batch(&self, reqs: &[BlockRead<'_>]) -> (usize, Option<io::Error>, Vec<Screened>) {
        // A request partly filled reads from `filled` bytes in. A batch the
        // inner backend refuses whole was turned away before this, by its
        // `Fs::check_descriptor_batch`. A request without a descriptor whose
        // offset overflows is read on its own after the others, so they are
        // read, and screened.
        let resume = |req: &BlockRead<'_>| req.offset.checked_add(req.buf.filled() as u64);
        let mut spent = Vec::with_capacity(reqs.len());
        for (at, req) in reqs.iter().enumerate() {
            let offset = resume(req).unwrap_or(req.offset);
            let len = req.buf.capacity() - req.buf.filled();
            // A request with nothing to read, or without a descriptor, is not
            // screened here: nothing is counted or spent for it.
            let fd = if len == 0 {
                None
            } else {
                req.file.backing_fd()
            };
            let Some(fd) = fd else {
                spent.push(Screened::Not);
                continue;
            };
            let path = self.path_of(fd);
            #[cfg(test)]
            if let Some(path) = &path {
                self.run_read_hooks(path, offset);
            }
            let (fault, rule, generation) = self.check_read_at_counted(path.as_deref(), offset);
            spent.push(Screened::Counted { rule, generation });
            if let Some(Fault::Error(kind)) = fault {
                return (at, Some(fault_error(kind, FaultOp::ReadAt)), spent);
            }
        }
        (reqs.len(), None, spent)
    }

    /// Gives back what screening spent on the requests at `unread`: the read
    /// count and the rule budget. Increments only, so a concurrent screening
    /// of other reads loses nothing; and only into the state it was spent
    /// from, so a [`Self::clear`] since leaves the new state as it is.
    fn refund(&self, spent: &[Screened], unread: impl IntoIterator<Item = usize>) {
        let mut rules = self.rules.lock();
        let now = self.generation.load(core::sync::atomic::Ordering::Relaxed);
        for at in unread {
            let Some(Screened::Counted { rule, generation }) = spent.get(at) else {
                continue;
            };
            if *generation != now {
                continue;
            }
            self.reads
                .fetch_sub(1, core::sync::atomic::Ordering::Relaxed);
            match rule {
                Some((index, Spent::Skip)) => {
                    if let Some(rule) = rules.get_mut(*index) {
                        rule.skip += 1;
                    }
                }
                Some((index, Spent::Count)) => {
                    if let Some(rule) = rules.get_mut(*index) {
                        rule.count += 1;
                    }
                }
                Some((_, Spent::Nothing)) | None => {}
            }
        }
    }

    /// Lets `syncs` more syncs of files and directories through, then fails
    /// every later one, and every rename and removal, with
    /// [`io::ErrorKind::Other`]: what a host whose power went out after the
    /// last sync let through still makes nothing durable. Over a
    /// [`CrashFs`](crate::fs::CrashFs), whose crash then keeps what those
    /// syncs made durable, it is a power loss at that point of a run, however
    /// the code that sees the failures goes on.
    ///
    /// Renames and removals are refused rather than let through because the
    /// crash simulator does not bring a removed name back. A device sync
    /// ([`Fs::sync_device`]) is neither counted nor refused.
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::fs::{CrashFs, FaultFs, Fs, MemFs};
    /// use std::path::Path;
    ///
    /// let crash = CrashFs::new(MemFs::new()).tracking_folders();
    /// let fs = FaultFs::new(crash.clone());
    /// fs.injector().power_off_after_syncs(1);
    /// fs.create_dir_all(Path::new("/kept"))?;
    /// fs.sync_directory(Path::new("/"))?;
    /// fs.create_dir_all(Path::new("/lost"))?;
    /// assert!(fs.sync_directory(Path::new("/")).is_err());
    /// crash.crash();
    /// assert!(crash.exists(Path::new("/kept"))?);
    /// assert!(!crash.exists(Path::new("/lost"))?);
    /// # Ok::<(), lsm_tree::io::Error>(())
    /// ```
    pub fn power_off_after_syncs(&self, syncs: usize) {
        *self.power.lock() = Some(syncs);
    }

    /// Whether the power is off for an operation of `op`, counting it when it
    /// is a sync that still passes.
    fn powered_off(&self, op: FaultOp) -> bool {
        let mut power = self.power.lock();
        let Some(left) = power.as_mut() else {
            return false;
        };
        match op {
            FaultOp::SyncDirectory | FaultOp::SyncAll | FaultOp::SyncData => {
                if *left == 0 {
                    return true;
                }
                *left -= 1;
                false
            }
            FaultOp::Rename | FaultOp::RemoveFile | FaultOp::RemoveDir | FaultOp::RemoveDirAll => {
                *left == 0
            }
            _ => false,
        }
    }

    /// Arms `rule`. Rules are evaluated in arm order; the first rule that
    /// matches an operation owns that occurrence (whether it is skipping,
    /// firing, or exhausted), so a later rule never fires on an operation an
    /// earlier rule already claimed.
    pub fn arm(&self, rule: FaultRule) {
        self.rules.lock().push(rule);
    }

    /// Runs `hook` inside every positional read at `offset` of a file whose
    /// path contains `path`, before the read is served.
    #[cfg(test)]
    pub(crate) fn hook_read_at(&self, path: impl Into<String>, offset: u64, hook: ReadHook) {
        self.read_hooks.lock().push((path.into(), offset, hook));
    }

    /// Runs `hook` before every removal (of a file, a folder, or a folder and
    /// all it holds) of a path containing `path`.
    #[cfg(test)]
    pub(crate) fn hook_removal(&self, path: impl Into<String>, hook: RemovalHook) {
        self.removal_hooks.lock().push((path.into(), hook));
    }

    /// Runs the hooks armed for a removal of kind `op` of `path`, outside the
    /// hook list's lock so a hook may use the backend.
    #[cfg(test)]
    fn run_removal_hooks(&self, op: FaultOp, path: &Path) {
        let spelled = path.to_string_lossy();
        let hooks: Vec<RemovalHook> = self
            .removal_hooks
            .lock()
            .iter()
            .filter(|(within, _)| spelled.contains(within.as_str()))
            .map(|(_, hook)| Arc::clone(hook))
            .collect();
        for hook in hooks {
            hook(op, path);
        }
    }

    /// Runs the hooks armed for a read at `offset` of `path`, outside the
    /// hook list's lock so a hook may block.
    #[cfg(test)]
    fn run_read_hooks(&self, path: &Path, offset: u64) {
        let path = path.to_string_lossy();
        let hooks: Vec<ReadHook> = self
            .read_hooks
            .lock()
            .iter()
            .filter(|(within, at, _)| *at == offset && path.contains(within.as_str()))
            .map(|(_, _, hook)| Arc::clone(hook))
            .collect();
        for hook in hooks {
            hook();
        }
    }

    /// Records one positional read. Called on every `read_at`, matched or not.
    pub(crate) fn count_read(&self) {
        self.reads
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    /// Positional reads served since the injector was created (or since the
    /// last [`Self::clear`]). Lets a test assert how much a path READS, not
    /// just what it does when a read fails.
    #[must_use]
    pub fn read_count(&self) -> usize {
        self.reads.load(core::sync::atomic::Ordering::Relaxed)
    }

    /// Records one file open. Called on every `open`, matched or not.
    pub(crate) fn count_open(&self) {
        self.opens
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
    }

    /// Files opened since the injector was created (or since the last
    /// [`Self::clear`]).
    #[must_use]
    pub fn open_count(&self) -> usize {
        self.opens.load(core::sync::atomic::Ordering::Relaxed)
    }

    /// Removes all armed rules AND the recorded observations (sync modes, the
    /// read count and the open count), and turns the power back on (see
    /// [`Self::power_off_after_syncs`]), resetting the injector so a later
    /// phase cannot see state recorded before the `clear()`.
    pub fn clear(&self) {
        {
            // Under the rules' lock, as a refund and a counted screening are:
            // one of them sees the state before the clear or the one after,
            // never a mix.
            let mut rules = self.rules.lock();
            rules.clear();
            self.generation
                .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
            self.reads.store(0, core::sync::atomic::Ordering::Relaxed);
        }
        self.sync_log.lock().clear();
        self.directory_sync_log.lock().clear();
        self.syncs.lock().clear();
        self.writeback_log.lock().clear();
        #[cfg(test)]
        self.read_hooks.lock().clear();
        #[cfg(test)]
        self.removal_hooks.lock().clear();
        *self.power.lock() = None;
        self.opens.store(0, core::sync::atomic::Ordering::Relaxed);
    }

    /// Records one sync of `kind` on `path`.
    fn note_sync(&self, path: &Path, kind: SyncKind) {
        self.syncs.lock().push((path.to_path_buf(), kind));
    }

    /// Every sync, of any kind and mode, issued since the last call, in call
    /// order: a file sync with the file's path, a directory or device sync
    /// with the directory's.
    #[must_use]
    pub fn take_syncs(&self) -> Vec<(PathBuf, SyncKind)> {
        core::mem::take(&mut *self.syncs.lock())
    }

    /// Records one file-level mode-carrying sync observation (see
    /// [`Self::sync_modes_for`]).
    fn record_sync(&self, path: &Path, mode: SyncMode) {
        self.sync_log.lock().push((path.to_path_buf(), mode));
    }

    /// The [`SyncMode`]s observed via `sync_all_with` / `sync_data_with` on
    /// files whose path contains `substr`, in call order.
    #[must_use]
    pub fn sync_modes_for(&self, substr: &str) -> Vec<SyncMode> {
        self.sync_log
            .lock()
            .iter()
            .filter(|(p, _)| p.to_string_lossy().contains(substr))
            .map(|(_, m)| *m)
            .collect()
    }

    /// The [`SyncMode`]s of the directory syncs of folders whose path contains
    /// `substr`, in call order.
    #[must_use]
    pub fn directory_sync_modes_for(&self, substr: &str) -> Vec<SyncMode> {
        self.directory_sync_log
            .lock()
            .iter()
            .filter(|(p, _)| p.to_string_lossy().contains(substr))
            .map(|(_, m)| *m)
            .collect()
    }

    /// The `(offset, len)` ranges `start_writeback` was asked for on files
    /// whose path contains `substr`, in call order.
    #[must_use]
    pub fn writebacks_for(&self, substr: &str) -> Vec<(u64, u64)> {
        self.writeback_log
            .lock()
            .iter()
            .filter(|(p, ..)| p.to_string_lossy().contains(substr))
            .map(|&(_, offset, len)| (offset, len))
            .collect()
    }

    /// Consults the rules for an operation of `op` at `path`, advancing the
    /// matched rule's skip/fire counters. Returns the [`Fault`] to apply, or
    /// `None` to let the operation proceed normally.
    fn check(&self, op: FaultOp, path: Option<&Path>) -> Option<Fault> {
        if self.powered_off(op) {
            return Some(Fault::Error(io::ErrorKind::Other));
        }
        let mut rules = self.rules.lock();
        for rule in rules.iter_mut() {
            if !rule.matches(op, path) {
                continue;
            }
            return Self::fire(rule);
        }
        None
    }

    /// Consults the rules for a positioned [`FaultOp::ReadAt`] at `offset` on
    /// `path`, honouring an [`at_offset`](FaultRule::at_offset) scope. Advances
    /// the matched rule's skip/fire counters exactly like [`check`](Self::check).
    fn check_read_at(&self, path: Option<&Path>, offset: u64) -> Option<Fault> {
        let mut rules = self.rules.lock();
        for rule in rules.iter_mut() {
            if !rule.matches_read_at(path, offset) {
                continue;
            }
            return Self::fire(rule);
        }
        None
    }

    /// Counts a read and consults the rules for it as [`Self::check_read_at`]
    /// does, also saying which rule matched, what it spent and in which state
    /// (see [`Self::clear`]), so the occurrence can be given back.
    fn check_read_at_counted(
        &self,
        path: Option<&Path>,
        offset: u64,
    ) -> (Option<Fault>, Option<(usize, Spent)>, u64) {
        let mut rules = self.rules.lock();
        self.count_read();
        let generation = self.generation.load(core::sync::atomic::Ordering::Relaxed);
        for (index, rule) in rules.iter_mut().enumerate() {
            if !rule.matches_read_at(path, offset) {
                continue;
            }
            let (fault, spent) = Self::fire_traced(rule);
            return (fault, Some((index, spent)), generation);
        }
        (None, None, generation)
    }

    /// Advances a matched rule's skip/fire budget and returns the [`Fault`] to
    /// apply, or `None` when the rule is still skipping or is exhausted. The
    /// first matching rule owns the occurrence.
    fn fire(rule: &mut FaultRule) -> Option<Fault> {
        Self::fire_traced(rule).0
    }

    /// [`Self::fire`], also saying which budget it spent.
    fn fire_traced(rule: &mut FaultRule) -> (Option<Fault>, Spent) {
        if rule.skip > 0 {
            rule.skip -= 1;
            return (None, Spent::Skip);
        }
        if rule.count == 0 {
            return (None, Spent::Nothing);
        }
        if rule.count == u64::MAX {
            return (Some(rule.fault), Spent::Nothing);
        }
        rule.count -= 1;
        (Some(rule.fault), Spent::Count)
    }
}

/// The callback a batch read hands each finished request to.
type OnRead<'f> = &'f mut dyn for<'a, 'b> FnMut(usize, &'a BlockRead<'b>);

/// What a rule spent on one occurrence.
#[derive(Clone, Copy)]
enum Spent {
    Skip,
    Count,
    Nothing,
}

/// What screening did with one request of a batch.
enum Screened {
    /// Not screened here: nothing to read, or no descriptor.
    Not,
    /// Counted as a read, and the rule that matched it and what it spent, in
    /// the injector's state `generation`.
    Counted {
        rule: Option<(usize, Spent)>,
        generation: u64,
    },
}

/// Builds the [`io::Error`] an [`Fault::Error`] surfaces, tagged with the op.
fn fault_error(kind: io::ErrorKind, op: FaultOp) -> io::Error {
    io::Error::new(kind, alloc::format!("injected fault on {op:?}"))
}

/// Same as [`fault_error`] but bridged to [`std::io::Error`] for the
/// [`std::io`] trait impls on [`FaultFile`].
fn fault_error_std(kind: io::ErrorKind, op: FaultOp) -> std::io::Error {
    fault_error(kind, op).into()
}

/// A fault-injecting [`Fs`] that wraps an inner backend.
///
/// Every operation consults the shared [`FaultInjector`] (obtained via
/// [`injector`](Self::injector)) before delegating to the inner backend. Open
/// file handles are wrapped in a `FaultFile` that carries the same injector,
/// so per-file read / write / sync faults are honoured too.
///
/// Identity-bearing probes ([`Fs::backend_id`], [`Fs::volume_id`],
/// [`Fs::capabilities`]) are forwarded verbatim so the engine's
/// correctness decisions (cross-backend hard-link safety, volume-independence,
/// FS-aware optimizations) see the real inner backend, not the wrapper.
pub struct FaultFs<F> {
    inner: F,
    injector: Arc<FaultInjector>,
}

impl<F: Fs> FaultFs<F> {
    /// Wraps `inner` with a fresh, empty [`FaultInjector`].
    #[must_use]
    pub fn new(inner: F) -> Self {
        Self {
            inner,
            injector: Arc::new(FaultInjector::new()),
        }
    }

    /// Wraps `inner`, sharing an existing injector (e.g. one already programmed,
    /// or shared across several wrappers).
    #[must_use]
    pub fn with_injector(inner: F, injector: Arc<FaultInjector>) -> Self {
        Self { inner, injector }
    }

    /// Returns a handle to the shared injector so a test can arm rules.
    #[must_use]
    pub fn injector(&self) -> Arc<FaultInjector> {
        Arc::clone(&self.injector)
    }

    /// Reads `reqs` through an inner backend that reads by descriptor: each
    /// request is screened by the read rules first, and a request a rule
    /// fails ends the batch there.
    ///
    /// Every request handed to the inner backend is a read this layer made,
    /// whatever the inner backend did with it: a ring attempts every one,
    /// in any order, and may fail several. When the inner backend fails, the
    /// one thing given back is what this layer spent on the request it would
    /// have failed itself: that request was never handed down, and its
    /// failure never reached the caller, so the rule is still owed to it.
    fn read_screened(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: Option<OnRead<'_>>,
    ) -> io::Result<()> {
        lend_descriptors(&self.injector, || {
            // A batch whose descriptor requests the backend refuses together
            // reads none of them: no rule is spent on them. The backend still
            // reads its other requests, each through `read_at`, and reports
            // as it does unwrapped: the batch goes down as it came.
            if self.inner.check_descriptor_batch(reqs).is_err() {
                return match on_read {
                    Some(on_read) => self.inner.read_blocks_batched_each(reqs, on_read),
                    None => self.inner.read_blocks_batched(reqs),
                };
            }
            let (clean, failure, spent) = self.injector.screen_batch(reqs);
            let (head, _) = reqs.split_at_mut(clean);
            let result = match on_read {
                Some(on_read) => self.inner.read_blocks_batched_each(head, on_read),
                None => self.inner.read_blocks_batched(head),
            };
            if let Err(e) = result {
                if failure.is_some() {
                    self.injector.refund(&spent, [clean]);
                }
                return Err(e);
            }
            failure.map_or(Ok(()), Err)
        })
    }
}

impl<F: Fs> Fs for FaultFs<F> {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        self.injector.count_open();
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Open, Some(path)) {
            return Err(fault_error(kind, FaultOp::Open));
        }
        let inner = self.inner.open(path, opts)?;
        // The descriptor a wrapper below shows only to a handle wrapping it.
        let fd = pass_through(|| inner.backing_fd());
        let handle = self.injector.note_opened(path, fd);
        Ok(Box::new(FaultFile {
            inner,
            path: path.to_path_buf(),
            handle,
            injector: Arc::clone(&self.injector),
        }))
    }

    // The inner backend batches and queues the reads. One that reads a file
    // by its descriptor (the ring) goes past `FaultFile::read_at`, so each
    // request is screened by the read rules here first, and a request a rule
    // fails ends the batch there. One that reads through `read_at` is
    // screened there, and only there.

    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        if !self.inner.reads_by_descriptor() {
            return self.inner.read_blocks_batched(reqs);
        }
        self.read_screened(reqs, None)
    }

    fn read_blocks_batched_each(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: &mut dyn FnMut(usize, &BlockRead<'_>),
    ) -> io::Result<()> {
        if !self.inner.reads_by_descriptor() {
            return self.inner.read_blocks_batched_each(reqs, on_read);
        }
        self.read_screened(reqs, Some(on_read))
    }

    fn check_descriptor_batch(&self, reqs: &[BlockRead<'_>]) -> io::Result<()> {
        lend_descriptors(&self.injector, || self.inner.check_descriptor_batch(reqs))
    }

    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        if !self.inner.reads_by_descriptor() {
            return self.inner.read_queue();
        }
        Box::new(FaultReadQueue {
            inner: self.inner.read_queue(),
            injector: &self.injector,
            submitted: Vec::new(),
            failed: Vec::new(),
            screened: Vec::new(),
        })
    }

    fn reads_by_descriptor(&self) -> bool {
        self.inner.reads_by_descriptor()
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::CreateDirAll, Some(path)) {
            return Err(fault_error(kind, FaultOp::CreateDirAll));
        }
        self.inner.create_dir_all(path)
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::CreateDir, Some(path)) {
            return Err(fault_error(kind, FaultOp::CreateDir));
        }
        self.inner.create_dir(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        self.inner.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        #[cfg(test)]
        self.injector.run_removal_hooks(FaultOp::RemoveFile, path);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::RemoveFile, Some(path)) {
            return Err(fault_error(kind, FaultOp::RemoveFile));
        }
        if self.injector.removal_refused(path) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "removal of a file with an open handle",
            ));
        }
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        #[cfg(test)]
        self.injector.run_removal_hooks(FaultOp::RemoveDirAll, path);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::RemoveDirAll, Some(path)) {
            return Err(fault_error(kind, FaultOp::RemoveDirAll));
        }
        self.inner.remove_dir_all(path)
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        #[cfg(test)]
        self.injector.run_removal_hooks(FaultOp::RemoveDir, path);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::RemoveDir, Some(path)) {
            return Err(fault_error(kind, FaultOp::RemoveDir));
        }
        self.inner.remove_dir(path)
    }

    fn same_file(&self, a: &Path, b: &Path) -> io::Result<bool> {
        // Rules match the FIRST path: an alias probe is asked about one file
        // under two spellings, and `a` is the one a test names.
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SameFile, Some(a)) {
            return Err(fault_error(kind, FaultOp::SameFile));
        }
        self.inner.same_file(a, b)
    }

    fn read_link(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        // Reading a link inspects the entry, as a stat does.
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Metadata, Some(path)) {
            return Err(fault_error(kind, FaultOp::Metadata));
        }
        self.inner.read_link(path)
    }

    fn symlink(&self, target: &Path, link: &Path, to_dir: bool) -> io::Result<()> {
        self.inner.symlink(target, link, to_dir)
    }

    fn symlink_to_dir(&self, path: &Path) -> io::Result<bool> {
        self.inner.symlink_to_dir(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        // Rename rules match the destination path: that is the stable name
        // (a published table, a sidecar) a test targets.
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Rename, Some(to)) {
            return Err(fault_error(kind, FaultOp::Rename));
        }
        self.inner.rename(from, to)?;
        self.injector.note_renamed(from, to);
        Ok(())
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Metadata, Some(path)) {
            return Err(fault_error(kind, FaultOp::Metadata));
        }
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        self.injector.note_sync(path, SyncKind::Directory);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncDirectory, Some(path)) {
            return Err(fault_error(kind, FaultOp::SyncDirectory));
        }
        self.inner.sync_directory(path)
    }

    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        self.injector.note_sync(path, SyncKind::Directory);
        self.injector
            .directory_sync_log
            .lock()
            .push((path.to_path_buf(), mode));
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncDirectory, Some(path)) {
            return Err(fault_error(kind, FaultOp::SyncDirectory));
        }
        self.inner.sync_directory_with(path, mode)
    }

    fn sync_device(&self, path: &Path) -> io::Result<()> {
        self.injector.note_sync(path, SyncKind::Device);
        self.inner.sync_device(path)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        // A `Metadata` fault on an existence probe models a stat that RACED file
        // creation and saw the path absent (the TOCTOU window `FaultOp::Metadata`
        // documents): report `Ok(false)` so the caller proceeds to create/open,
        // where a concurrent creator's file then surfaces as `AlreadyExists`.
        if let Some(Fault::Error(_)) = self.injector.check(FaultOp::Metadata, Some(path)) {
            return Ok(false);
        }
        self.inner.exists(path)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::HardLink, Some(dst)) {
            return Err(fault_error(kind, FaultOp::HardLink));
        }
        self.inner.hard_link(src, dst)
    }

    fn backend_id(&self) -> Option<u64> {
        self.inner.backend_id()
    }

    fn volume_id(&self, path: &Path) -> Option<u64> {
        if self.injector.check(FaultOp::VolumeId, Some(path)).is_some() {
            return None;
        }
        self.inner.volume_id(path)
    }

    fn capabilities(&self, path: &Path) -> FsCapabilities {
        self.inner.capabilities(path)
    }

    fn try_disable_cow(&self, path: &Path) -> io::Result<()> {
        self.inner.try_disable_cow(path)
    }

    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::PunchHole, Some(path)) {
            return Err(fault_error(kind, FaultOp::PunchHole));
        }
        self.inner.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Reflink, Some(dst)) {
            return Err(fault_error(kind, FaultOp::Reflink));
        }
        self.inner.reflink_file(src, dst)
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        self.inner.truncate_file(path)
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::HardLinkCount, Some(path)) {
            return Err(fault_error(kind, FaultOp::HardLinkCount));
        }
        self.inner.hard_link_count(path)
    }

    fn available_space(&self, path: &Path) -> io::Result<u64> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::AvailableSpace, Some(path)) {
            return Err(fault_error(kind, FaultOp::AvailableSpace));
        }
        self.inner.available_space(path)
    }

    fn allocated_size(&self, path: &Path) -> io::Result<Option<u64>> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::AllocatedSize, Some(path)) {
            return Err(fault_error(kind, FaultOp::AllocatedSize));
        }
        self.inner.allocated_size(path)
    }

    fn extent_is_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        // Shares the `AllocatedSize` fault op: both are allocation probes, and a
        // test arming one means "the backend cannot answer allocation questions".
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::AllocatedSize, Some(path)) {
            return Err(fault_error(kind, FaultOp::AllocatedSize));
        }
        self.inner.extent_is_hole(path, offset, len)
    }

    fn extent_contains_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        // Same allocation-probe fault op as `extent_is_hole`, for the same reason.
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::AllocatedSize, Some(path)) {
            return Err(fault_error(kind, FaultOp::AllocatedSize));
        }
        self.inner.extent_contains_hole(path, offset, len)
    }
}

/// The [`ReadQueue`] of a [`FaultFs`]: the inner backend's queue, each read
/// screened by the read rules as the wait that issues it hands it on: a read
/// never issued spends no rule, and a rule armed before the issuing wait
/// applies to it. A wait for none issues the reads submitted since only when
/// the inner queue issues at such a wait; otherwise it neither screens nor
/// hands them over, they count as held, and the next wait for some issues
/// them; one the inner queue is given there and keeps rather than issues
/// ([`ReadQueue::take_kept`]) comes back unscreened. A read a rule fails
/// never reaches the inner queue and is handed back failed by that wait.
struct FaultReadQueue<'a> {
    inner: Box<dyn ReadQueue + 'a>,
    injector: &'a FaultInjector,
    /// Reads submitted since the last wait, not screened yet.
    submitted: Vec<QueuedRead>,
    failed: Vec<ReadDone>,
    /// What screening spent on each read handed on during the current wait,
    /// by the address of its buffer, which the inner queue keeps.
    screened: Vec<(usize, Screened)>,
}

impl ReadQueue for FaultReadQueue<'_> {
    fn submit(&mut self, read: QueuedRead) {
        self.submitted.push(read);
    }

    fn outstanding(&self) -> usize {
        self.inner.outstanding() + self.submitted.len() + self.failed.len()
    }

    // A submitted read is issued, and a failed one handed back, only by a
    // wait, never on its own.
    fn held(&self) -> usize {
        self.inner.held() + self.submitted.len() + self.failed.len()
    }

    fn issues_at_wait_for_none(&self) -> bool {
        self.inner.issues_at_wait_for_none()
    }

    // What the last wait for none got back from the queue below, unscreened
    // here again: a wrapper above takes it back the same way, and screens it
    // at the wait that issues it.
    fn take_kept(&mut self) -> Vec<QueuedRead> {
        core::mem::take(&mut self.submitted)
    }

    fn wait(&mut self, min: usize, on_done: &mut dyn FnMut(ReadDone)) {
        let injector = self.injector;
        lend_descriptors(injector, || self.wait_lent(min, on_done));
    }

    fn set_wake(&mut self, wake: Arc<dyn ReadWake>) -> bool {
        self.inner.set_wake(wake)
    }
}

impl FaultReadQueue<'_> {
    /// [`ReadQueue::wait`], with the handles' descriptors lent to the inner
    /// queue, which issues the reads by them.
    fn wait_lent(&mut self, min: usize, on_done: &mut dyn FnMut(ReadDone)) {
        if min == 0 && !self.inner.issues_at_wait_for_none() {
            // Only what the inner queue has issued already, and is ready.
            self.inner.wait(0, on_done);
            return;
        }
        let mut sent = 0usize;
        for read in core::mem::take(&mut self.submitted) {
            let len = read.buf.len();
            let (error, spent) = self
                .injector
                .screen_by_fd(read.file.as_ref(), read.offset, len);
            if let Some(error) = error {
                self.failed.push(ReadDone {
                    tag: read.tag,
                    buf: read.buf,
                    result: Err(error),
                });
            } else {
                self.screened.push((read.buf.as_ptr() as usize, spent));
                self.inner.submit(read);
                sent += 1;
            }
        }
        let handed = self.failed.len();
        for done in self.failed.drain(..) {
            on_done(done);
        }
        // What the failed reads already handed over counts toward `min`; past
        // it the inner queue only hands over what is ready. A queue that
        // issues only at a wait for some is still made to issue what this
        // wait handed it: this wait counted those reads, so it reads them.
        let mut inner_min = min.saturating_sub(handed);
        if inner_min == 0 && sent > 0 && !self.inner.issues_at_wait_for_none() {
            inner_min = 1;
        }
        self.inner.wait(inner_min, on_done);
        // A read the inner queue kept rather than issued (a ring with no room
        // at a wait for none) was not read: what screening spent on it is
        // given back, and the wait that issues it screens it then.
        for read in self.inner.take_kept() {
            let key = read.buf.as_ptr() as usize;
            if let Some(at) = self.screened.iter().position(|(held, _)| *held == key) {
                let (_, spent) = self.screened.swap_remove(at);
                self.injector.refund(core::slice::from_ref(&spent), [0]);
            }
            self.submitted.push(read);
        }
        self.screened.clear();
    }
}

/// A fault-injecting [`FsFile`] wrapping an inner handle.
///
/// Created by [`FaultFs::open`]; carries the open path (for path-filtered
/// per-file rules) and the shared injector. Read/write/sync operations consult
/// the injector; metadata, locking, and hints delegate unconditionally.
struct FaultFile {
    inner: Box<dyn FsFile>,
    path: PathBuf,
    /// This handle's id among the injector's open handles.
    handle: u64,
    injector: Arc<FaultInjector>,
}

impl Drop for FaultFile {
    fn drop(&mut self) {
        self.injector.note_closed(self.handle);
    }
}

// `crate::io::{Read, Write, Seek}` are supertrait aliases of the std traits
// under `feature = "std"` (with blanket impls), so implementing the std traits
// here is what makes `FaultFile` satisfy the `FsFile: Read + Write + Seek`
// bound. The module is std-gated, so this is always the active shape.

impl std::io::Read for FaultFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Read, Some(&self.path)) {
            return Err(fault_error_std(kind, FaultOp::Read));
        }
        self.inner.read(buf)
    }
}

impl std::io::Write for FaultFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self.injector.check(FaultOp::Write, Some(&self.path)) {
            Some(Fault::Error(kind)) => Err(fault_error_std(kind, FaultOp::Write)),
            Some(Fault::ShortWrite(n)) => {
                // Honour the contract deterministically: accept EXACTLY `take`
                // bytes regardless of how the inner backend chunks its writes
                // (a plain `write` may return fewer than `take`).
                let take = n.min(buf.len());
                if take == 0 {
                    return Ok(0);
                }
                let (head, _) = buf.split_at(take);
                self.inner.write_all(head)?;
                Ok(take)
            }
            None => self.inner.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Flush, Some(&self.path)) {
            return Err(fault_error_std(kind, FaultOp::Flush));
        }
        self.inner.flush()
    }
}

impl std::io::Seek for FaultFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for FaultFile {
    fn sync_all(&self) -> io::Result<()> {
        self.injector.note_sync(&self.path, SyncKind::File);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncAll, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncAll));
        }
        self.inner.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        self.injector.note_sync(&self.path, SyncKind::FileData);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncData, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncData));
        }
        self.inner.sync_data()
    }

    fn sync_all_with(&self, mode: SyncMode) -> io::Result<()> {
        self.injector.note_sync(&self.path, SyncKind::File);
        self.injector.record_sync(&self.path, mode);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncAll, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncAll));
        }
        self.inner.sync_all_with(mode)
    }

    fn sync_data_with(&self, mode: SyncMode) -> io::Result<()> {
        self.injector.note_sync(&self.path, SyncKind::FileData);
        self.injector.record_sync(&self.path, mode);
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncData, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncData));
        }
        self.inner.sync_data_with(mode)
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Metadata, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::Metadata));
        }
        self.inner.metadata()
    }

    fn hard_link_count(&self) -> io::Result<u64> {
        if let Some(Fault::Error(kind)) = self
            .injector
            .check(FaultOp::HardLinkCount, Some(&self.path))
        {
            return Err(fault_error(kind, FaultOp::HardLinkCount));
        }
        self.inner.hard_link_count()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SetLen, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SetLen));
        }
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if let Some(error) = self.injector.screen_read(Some(&self.path), offset) {
            return Err(error);
        }
        self.inner.read_at(buf, offset)
    }

    // Shown only inside a batch or a queue wait of the wrapper this handle
    // was opened through, which screens each read by it with its rules
    // before its inner backend reads it, layer by layer through a wrapper
    // below: any other backend reading the descriptor, another wrapper's
    // included, would go past these read rules, so it gets none and reads
    // through `read_at`.
    fn backing_fd(&self) -> Option<i32> {
        if descriptors_lent(&self.injector) {
            pass_through(|| self.inner.backing_fd())
        } else {
            None
        }
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        self.inner.lock_exclusive()
    }

    fn try_lock_exclusive(&self) -> io::Result<bool> {
        self.inner.try_lock_exclusive()
    }

    fn hint(&self, hint: FileHint) -> io::Result<()> {
        self.inner.hint(hint)
    }

    fn set_write_lifetime(&self, lifetime: super::WriteLifetime) -> io::Result<()> {
        self.inner.set_write_lifetime(lifetime)
    }

    fn start_writeback(&self, offset: u64, len: u64) -> io::Result<()> {
        self.injector
            .writeback_log
            .lock()
            .push((self.path.clone(), offset, len));
        if let Some(Fault::Error(kind)) = self
            .injector
            .check(FaultOp::StartWriteback, Some(&self.path))
        {
            return Err(fault_error(kind, FaultOp::StartWriteback));
        }
        self.inner.start_writeback(offset, len)
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;
