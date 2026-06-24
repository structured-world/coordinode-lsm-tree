// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

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
    FileHint, Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, SyncMode,
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
    skip: u64,
    count: u64,
    fault: Fault,
}

impl FaultRule {
    /// Creates a rule that, by default, fires `fault` on **every** matching
    /// `op` (no path filter, no skip, unlimited fires). Refine with
    /// [`on_path`](Self::on_path), [`skip`](Self::skip),
    /// [`times`](Self::times), or [`once`](Self::once).
    #[must_use]
    pub const fn new(op: FaultOp, fault: Fault) -> Self {
        Self {
            op,
            path_substr: None,
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
    fn matches(&self, op: FaultOp, path: Option<&Path>) -> bool {
        if self.op != op {
            return false;
        }
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
}

impl FaultInjector {
    /// Creates an injector with no rules (every operation passes untouched).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Arms `rule`. Rules are evaluated in arm order; the first rule that
    /// matches an operation owns that occurrence (whether it is skipping,
    /// firing, or exhausted), so a later rule never fires on an operation an
    /// earlier rule already claimed.
    pub fn arm(&self, rule: FaultRule) {
        self.rules.lock().push(rule);
    }

    /// Removes all armed rules.
    pub fn clear(&self) {
        self.rules.lock().clear();
    }

    /// Consults the rules for an operation of `op` at `path`, advancing the
    /// matched rule's skip/fire counters. Returns the [`Fault`] to apply, or
    /// `None` to let the operation proceed normally.
    fn check(&self, op: FaultOp, path: Option<&Path>) -> Option<Fault> {
        let mut rules = self.rules.lock();
        for rule in rules.iter_mut() {
            if !rule.matches(op, path) {
                continue;
            }
            // First matching rule owns this occurrence.
            if rule.skip > 0 {
                rule.skip -= 1;
                return None;
            }
            if rule.count == 0 {
                return None;
            }
            if rule.count != u64::MAX {
                rule.count -= 1;
            }
            return Some(rule.fault);
        }
        None
    }
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
/// file handles are wrapped in a [`FaultFile`] that carries the same injector,
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
}

impl<F: Fs> Fs for FaultFs<F> {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Open, Some(path)) {
            return Err(fault_error(kind, FaultOp::Open));
        }
        let inner = self.inner.open(path, opts)?;
        Ok(Box::new(FaultFile {
            inner,
            path: path.to_path_buf(),
            injector: Arc::clone(&self.injector),
        }))
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
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::RemoveFile, Some(path)) {
            return Err(fault_error(kind, FaultOp::RemoveFile));
        }
        self.inner.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        // Rename rules match the destination path: that is the stable name
        // (manifest, version pointer) a test targets.
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::Rename, Some(to)) {
            return Err(fault_error(kind, FaultOp::Rename));
        }
        self.inner.rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncDirectory, Some(path)) {
            return Err(fault_error(kind, FaultOp::SyncDirectory));
        }
        self.inner.sync_directory(path)
    }

    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncDirectory, Some(path)) {
            return Err(fault_error(kind, FaultOp::SyncDirectory));
        }
        self.inner.sync_directory_with(path, mode)
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.hard_link(src, dst)
    }

    fn backend_id(&self) -> Option<u64> {
        self.inner.backend_id()
    }

    fn volume_id(&self, path: &Path) -> Option<u64> {
        self.inner.volume_id(path)
    }

    fn capabilities(&self, path: &Path) -> FsCapabilities {
        self.inner.capabilities(path)
    }

    fn try_disable_cow(&self, path: &Path) -> io::Result<()> {
        self.inner.try_disable_cow(path)
    }

    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<()> {
        self.inner.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        self.inner.reflink_file(src, dst)
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        self.inner.truncate_file(path)
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        self.inner.hard_link_count(path)
    }

    fn available_space(&self, path: &Path) -> io::Result<u64> {
        self.inner.available_space(path)
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
    injector: Arc<FaultInjector>,
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
                let take = n.min(buf.len());
                let (head, _) = buf.split_at(take);
                self.inner.write(head)
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
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncAll, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncAll));
        }
        self.inner.sync_all()
    }

    fn sync_data(&self) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncData, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncData));
        }
        self.inner.sync_data()
    }

    fn sync_all_with(&self, mode: SyncMode) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncAll, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncAll));
        }
        self.inner.sync_all_with(mode)
    }

    fn sync_data_with(&self, mode: SyncMode) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SyncData, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SyncData));
        }
        self.inner.sync_data_with(mode)
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::SetLen, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::SetLen));
        }
        self.inner.set_len(size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if let Some(Fault::Error(kind)) = self.injector.check(FaultOp::ReadAt, Some(&self.path)) {
            return Err(fault_error(kind, FaultOp::ReadAt));
        }
        self.inner.read_at(buf, offset)
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
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;
