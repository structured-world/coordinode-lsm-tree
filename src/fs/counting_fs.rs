// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a store writes through an [`Fs`], by what it writes for.
//!
//! [`CountingFs`] wraps a backend and counts every write-side operation that
//! passes through it: the bytes each `write` accepted, the write calls and the
//! short ones among them, file and directory syncs and their failures, and the
//! creations, renames, removals, links, clones and punched holes. A file is
//! counted under the [`WriteOrigin`] it was opened with
//! ([`FsOpenOptions::write_origin`]); one opened without is counted as
//! unattributed, so the counters together hold everything written, each byte
//! once. Directory operations name no file handle and are counted apart.
//!
//! These are the bytes the store hands to its backend. The page cache's
//! writeback, the file system's own metadata and journal, and the device's
//! garbage collection and erase amplification all lie below this line; the
//! figures are host writes, never NAND writes. A write-ahead log the caller
//! keeps outside the tree is not written through it and is not counted.
//!
//! The counters live as long as the [`WriteIoStats`] they are kept in: a tree
//! starts its own at open, so a restart starts again from zero.

use super::{
    BlockRead, FileHint, Fs, FsCapabilities, FsDirEntry, FsFile, FsMetadata, FsOpenOptions,
    ReadQueue, SyncMode, WriteLifetime,
};
use crate::io;
#[cfg(not(feature = "std"))]
use crate::io::{Read, Seek, Write};
use crate::path::{Path, PathBuf};
use alloc::{boxed::Box, sync::Arc, vec::Vec};
use portable_atomic::{AtomicU64, Ordering::Relaxed};

/// What a file is written for: the activity whose bytes it holds.
///
/// Given to a backend through [`FsOpenOptions::write_origin`] when a file is
/// opened for writing, so a counting backend charges what is written through
/// the handle to it. Advisory: a backend may ignore it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WriteOrigin {
    /// Tables a flush writes from sealed memtables.
    FlushTable,
    /// Blob files holding the separated values of a flush's tables.
    FlushBlob,
    /// Tables an ingestion writes.
    IngestTable,
    /// Blob files holding the separated values an ingestion writes.
    IngestBlob,
    /// Tables a compaction writes.
    CompactionTable,
    /// Blob files a compaction writes: values it relocates or writes anew.
    CompactionBlob,
    /// The manifest: its snapshots and the edit log appended to them.
    Manifest,
    /// Other engine metadata: dictionaries, pins, restriction sidecars, heal
    /// attestations, the empty files a table file pool makes ahead of its
    /// tables, the lock file.
    Metadata,
    /// Repair, salvage, and healing of damaged files in place.
    Repair,
    /// The copies a checkpoint makes of a tree's files where it cannot link
    /// them. The manifest it writes is [`Self::Manifest`]'s.
    Checkpoint,
}

impl WriteOrigin {
    /// Every origin, in the order of their slots.
    pub const ALL: [Self; 10] = [
        Self::FlushTable,
        Self::FlushBlob,
        Self::IngestTable,
        Self::IngestBlob,
        Self::CompactionTable,
        Self::CompactionBlob,
        Self::Manifest,
        Self::Metadata,
        Self::Repair,
        Self::Checkpoint,
    ];

    /// A stable name for an export, in `snake_case`.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::FlushTable => "flush_table",
            Self::FlushBlob => "flush_blob",
            Self::IngestTable => "ingest_table",
            Self::IngestBlob => "ingest_blob",
            Self::CompactionTable => "compaction_table",
            Self::CompactionBlob => "compaction_blob",
            Self::Manifest => "manifest",
            Self::Metadata => "metadata",
            Self::Repair => "repair",
            Self::Checkpoint => "checkpoint",
        }
    }

    /// Whether the files of this origin are blob files.
    #[must_use]
    pub const fn is_blob(self) -> bool {
        matches!(
            self,
            Self::FlushBlob | Self::IngestBlob | Self::CompactionBlob
        )
    }

    const fn slot(self) -> usize {
        self as usize
    }
}

/// The slot of files opened without an origin, after every origin's.
const UNATTRIBUTED: usize = WriteOrigin::ALL.len();

/// What was written through the files of one origin.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FileWrites {
    /// Bytes the files' `write` calls accepted: each byte once, however the
    /// writer split or retried it.
    pub bytes: u64,
    /// Write calls that accepted bytes.
    pub writes: u64,
    /// Of those, the calls that accepted fewer bytes than they were handed.
    pub short_writes: u64,
    /// Write calls that failed.
    pub write_errors: u64,
    /// Files opened for writing.
    pub opened: u64,
    /// Of those, the opens allowed to create the file.
    pub created: u64,
    /// File syncs, data-only ones included.
    pub syncs: u64,
    /// File syncs that failed.
    pub sync_errors: u64,
    /// Ranges whose writeback the files started ahead of a sync, without
    /// waiting for it: no durability point, and no bytes beyond `bytes`. The
    /// standard backend issues `sync_file_range` for each on Linux.
    pub writebacks: u64,
    /// Lengths set on the files.
    pub truncated: u64,
    /// Bytes of the files a store installed: in a version for tables and blob
    /// files. What was written and never installed is the output of work that
    /// failed or was given up, kept in the same `bytes`.
    pub installed: u64,
}

impl FileWrites {
    fn checked_sub(&self, earlier: &Self) -> Option<Self> {
        Some(Self {
            bytes: self.bytes.checked_sub(earlier.bytes)?,
            writes: self.writes.checked_sub(earlier.writes)?,
            short_writes: self.short_writes.checked_sub(earlier.short_writes)?,
            write_errors: self.write_errors.checked_sub(earlier.write_errors)?,
            opened: self.opened.checked_sub(earlier.opened)?,
            created: self.created.checked_sub(earlier.created)?,
            syncs: self.syncs.checked_sub(earlier.syncs)?,
            sync_errors: self.sync_errors.checked_sub(earlier.sync_errors)?,
            writebacks: self.writebacks.checked_sub(earlier.writebacks)?,
            truncated: self.truncated.checked_sub(earlier.truncated)?,
            installed: self.installed.checked_sub(earlier.installed)?,
        })
    }

    fn add(&mut self, other: &Self) {
        self.bytes += other.bytes;
        self.writes += other.writes;
        self.short_writes += other.short_writes;
        self.write_errors += other.write_errors;
        self.opened += other.opened;
        self.created += other.created;
        self.syncs += other.syncs;
        self.sync_errors += other.sync_errors;
        self.writebacks += other.writebacks;
        self.truncated += other.truncated;
        self.installed += other.installed;
    }
}

/// Operations on directory entries and devices: they name no file handle, so
/// they are not split by origin.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct DirectoryWrites {
    /// Directory syncs.
    pub syncs: u64,
    /// Directory syncs that failed.
    pub sync_errors: u64,
    /// Device syncs.
    pub device_syncs: u64,
    /// Device syncs that failed.
    pub device_sync_errors: u64,
    /// Directories created.
    pub dirs_created: u64,
    /// Directories removed, each removal of a tree of them once.
    pub dirs_removed: u64,
    /// Files removed.
    pub removed: u64,
    /// Entries renamed.
    pub renamed: u64,
    /// Hard links made: a new name for bytes already written, none copied.
    pub linked: u64,
    /// Symbolic links made.
    pub symlinked: u64,
    /// Files cloned. A backend that cannot clone copies inside the call, and
    /// those bytes are not seen here.
    pub cloned: u64,
    /// Holes punched into files.
    pub holes_punched: u64,
    /// Files truncated to empty by path.
    pub truncated: u64,
}

impl DirectoryWrites {
    fn checked_sub(&self, earlier: &Self) -> Option<Self> {
        Some(Self {
            syncs: self.syncs.checked_sub(earlier.syncs)?,
            sync_errors: self.sync_errors.checked_sub(earlier.sync_errors)?,
            device_syncs: self.device_syncs.checked_sub(earlier.device_syncs)?,
            device_sync_errors: self
                .device_sync_errors
                .checked_sub(earlier.device_sync_errors)?,
            dirs_created: self.dirs_created.checked_sub(earlier.dirs_created)?,
            dirs_removed: self.dirs_removed.checked_sub(earlier.dirs_removed)?,
            removed: self.removed.checked_sub(earlier.removed)?,
            renamed: self.renamed.checked_sub(earlier.renamed)?,
            linked: self.linked.checked_sub(earlier.linked)?,
            symlinked: self.symlinked.checked_sub(earlier.symlinked)?,
            cloned: self.cloned.checked_sub(earlier.cloned)?,
            holes_punched: self.holes_punched.checked_sub(earlier.holes_punched)?,
            truncated: self.truncated.checked_sub(earlier.truncated)?,
        })
    }
}

/// What a tree took in from its caller, before any encoding: the payload its
/// writes are measured against.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct AcceptedWrites {
    /// Values written, merge operands included.
    pub values: u64,
    /// Point deletions.
    pub deletions: u64,
    /// Range deletions.
    pub range_deletions: u64,
    /// Records an ingestion took in.
    pub ingested: u64,
    /// The bytes of their keys, values, operands and range bounds.
    pub payload_bytes: u64,
}

impl AcceptedWrites {
    fn checked_sub(&self, earlier: &Self) -> Option<Self> {
        Some(Self {
            values: self.values.checked_sub(earlier.values)?,
            deletions: self.deletions.checked_sub(earlier.deletions)?,
            range_deletions: self.range_deletions.checked_sub(earlier.range_deletions)?,
            ingested: self.ingested.checked_sub(earlier.ingested)?,
            payload_bytes: self.payload_bytes.checked_sub(earlier.payload_bytes)?,
        })
    }
}

/// A reading of [`WriteIoStats`]: every counter as of one moment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct WriteIoSnapshot {
    /// The files of each origin, in [`WriteOrigin::ALL`] order.
    pub by_origin: [FileWrites; WriteOrigin::ALL.len()],
    /// The files opened with no origin.
    pub unattributed: FileWrites,
    /// Directory and device operations.
    pub directory: DirectoryWrites,
    /// What the tree took in: zero for a backend counted on its own.
    pub accepted: AcceptedWrites,
    /// The counter set the reading came from (see [`WriteIoStats`]).
    lifetime: u64,
}

impl WriteIoSnapshot {
    /// The files of `origin`.
    #[must_use]
    pub fn origin(&self, origin: WriteOrigin) -> FileWrites {
        self.by_origin
            .get(origin.slot())
            .copied()
            .unwrap_or_default()
    }

    /// Every origin's files, unattributed ones last as `None`.
    pub fn iter(&self) -> impl Iterator<Item = (Option<WriteOrigin>, FileWrites)> + '_ {
        WriteOrigin::ALL
            .iter()
            .map(|origin| (Some(*origin), self.origin(*origin)))
            .chain(core::iter::once((None, self.unattributed)))
    }

    /// The files of every origin, unattributed ones included, summed.
    #[must_use]
    pub fn files(&self) -> FileWrites {
        let mut total = self.unattributed;
        for files in &self.by_origin {
            total.add(files);
        }
        total
    }

    /// The bytes written to blob files.
    #[must_use]
    pub fn blob_bytes(&self) -> u64 {
        WriteOrigin::ALL
            .iter()
            .filter(|origin| origin.is_blob())
            .map(|origin| self.origin(*origin).bytes)
            .sum()
    }

    /// Every sync issued: of files, of directories and of devices.
    #[must_use]
    pub fn syncs(&self) -> u64 {
        self.files().syncs + self.directory.syncs + self.directory.device_syncs
    }

    /// The bytes written for each byte of payload taken in: the host write
    /// amplification of what this reading covers.
    ///
    /// `None` when no payload was taken in, as for a backend counted on its
    /// own, whose writes come from no single tree's payload. It counts the
    /// bytes handed to the backend, not what a device writes.
    #[must_use]
    #[expect(
        clippy::cast_precision_loss,
        reason = "a ratio of byte counts; precision loss past 2^53 bytes is immaterial"
    )]
    pub fn host_write_amplification(&self) -> Option<f64> {
        let payload = self.accepted.payload_bytes;
        (payload > 0).then(|| self.files().bytes as f64 / payload as f64)
    }

    /// What happened between `earlier` and this reading of the same counters.
    ///
    /// `None` when the readings come from two counter sets, as across a
    /// reopen, whatever their values: no difference of them means anything.
    #[must_use]
    pub fn since(&self, earlier: &Self) -> Option<Self> {
        if self.lifetime != earlier.lifetime {
            return None;
        }
        let mut by_origin = [FileWrites::default(); WriteOrigin::ALL.len()];
        for ((delta, now), then) in by_origin
            .iter_mut()
            .zip(&self.by_origin)
            .zip(&earlier.by_origin)
        {
            *delta = now.checked_sub(then)?;
        }
        Some(Self {
            by_origin,
            unattributed: self.unattributed.checked_sub(&earlier.unattributed)?,
            directory: self.directory.checked_sub(&earlier.directory)?,
            accepted: self.accepted.checked_sub(&earlier.accepted)?,
            lifetime: self.lifetime,
        })
    }
}

/// The counters of one slot of files.
#[derive(Debug, Default)]
struct FileCounters {
    bytes: AtomicU64,
    writes: AtomicU64,
    short_writes: AtomicU64,
    write_errors: AtomicU64,
    opened: AtomicU64,
    created: AtomicU64,
    syncs: AtomicU64,
    sync_errors: AtomicU64,
    writebacks: AtomicU64,
    truncated: AtomicU64,
    installed: AtomicU64,
}

impl FileCounters {
    fn read(&self) -> FileWrites {
        FileWrites {
            bytes: self.bytes.load(Relaxed),
            writes: self.writes.load(Relaxed),
            short_writes: self.short_writes.load(Relaxed),
            write_errors: self.write_errors.load(Relaxed),
            opened: self.opened.load(Relaxed),
            created: self.created.load(Relaxed),
            syncs: self.syncs.load(Relaxed),
            sync_errors: self.sync_errors.load(Relaxed),
            writebacks: self.writebacks.load(Relaxed),
            truncated: self.truncated.load(Relaxed),
            installed: self.installed.load(Relaxed),
        }
    }

    fn synced(&self, result: &io::Result<()>) {
        self.syncs.fetch_add(1, Relaxed);
        if result.is_err() {
            self.sync_errors.fetch_add(1, Relaxed);
        }
    }

    /// A write call handed `handed` bytes, of which it accepted `accepted`,
    /// `None` when it failed.
    fn wrote(&self, handed: usize, accepted: Option<usize>) {
        if let Some(accepted) = accepted {
            self.bytes.fetch_add(accepted as u64, Relaxed);
            self.writes.fetch_add(1, Relaxed);
            if accepted < handed {
                self.short_writes.fetch_add(1, Relaxed);
            }
        } else {
            self.write_errors.fetch_add(1, Relaxed);
        }
    }
}

#[derive(Debug, Default)]
struct DirectoryCounters {
    syncs: AtomicU64,
    sync_errors: AtomicU64,
    device_syncs: AtomicU64,
    device_sync_errors: AtomicU64,
    dirs_created: AtomicU64,
    dirs_removed: AtomicU64,
    removed: AtomicU64,
    renamed: AtomicU64,
    linked: AtomicU64,
    symlinked: AtomicU64,
    cloned: AtomicU64,
    holes_punched: AtomicU64,
    truncated: AtomicU64,
}

impl DirectoryCounters {
    fn read(&self) -> DirectoryWrites {
        DirectoryWrites {
            syncs: self.syncs.load(Relaxed),
            sync_errors: self.sync_errors.load(Relaxed),
            device_syncs: self.device_syncs.load(Relaxed),
            device_sync_errors: self.device_sync_errors.load(Relaxed),
            dirs_created: self.dirs_created.load(Relaxed),
            dirs_removed: self.dirs_removed.load(Relaxed),
            removed: self.removed.load(Relaxed),
            renamed: self.renamed.load(Relaxed),
            linked: self.linked.load(Relaxed),
            symlinked: self.symlinked.load(Relaxed),
            cloned: self.cloned.load(Relaxed),
            holes_punched: self.holes_punched.load(Relaxed),
            truncated: self.truncated.load(Relaxed),
        }
    }
}

#[derive(Debug, Default)]
struct AcceptedCounters {
    values: AtomicU64,
    deletions: AtomicU64,
    range_deletions: AtomicU64,
    ingested: AtomicU64,
    payload_bytes: AtomicU64,
}

/// The counters a [`CountingFs`] keeps, shared by the files it opens.
///
/// Every counter only grows; [`Self::snapshot`] reads them all, and the
/// difference of two readings ([`WriteIoSnapshot::since`]) is what happened
/// in between. Each set is numbered when it is made, and its readings carry
/// the number, so readings of two sets are never set against each other.
#[derive(Debug)]
pub struct WriteIoStats {
    files: [FileCounters; WriteOrigin::ALL.len() + 1],
    directory: DirectoryCounters,
    accepted: AcceptedCounters,
    lifetime: u64,
}

/// The number the next counter set takes: distinct for every set this process
/// makes, which is as far as a reading can travel.
static NEXT_LIFETIME: AtomicU64 = AtomicU64::new(1);

impl Default for WriteIoStats {
    fn default() -> Self {
        Self {
            files: Default::default(),
            directory: DirectoryCounters::default(),
            accepted: AcceptedCounters::default(),
            lifetime: NEXT_LIFETIME.fetch_add(1, Relaxed),
        }
    }
}

impl WriteIoStats {
    /// Reads every counter.
    #[must_use]
    pub fn snapshot(&self) -> WriteIoSnapshot {
        let mut by_origin = [FileWrites::default(); WriteOrigin::ALL.len()];
        for (reading, counters) in by_origin.iter_mut().zip(&self.files) {
            *reading = counters.read();
        }
        WriteIoSnapshot {
            by_origin,
            unattributed: self
                .files
                .get(UNATTRIBUTED)
                .map(FileCounters::read)
                .unwrap_or_default(),
            directory: self.directory.read(),
            accepted: AcceptedWrites {
                values: self.accepted.values.load(Relaxed),
                deletions: self.accepted.deletions.load(Relaxed),
                range_deletions: self.accepted.range_deletions.load(Relaxed),
                ingested: self.accepted.ingested.load(Relaxed),
                payload_bytes: self.accepted.payload_bytes.load(Relaxed),
            },
            lifetime: self.lifetime,
        }
    }

    fn slot(&self, origin: Option<WriteOrigin>) -> Option<&FileCounters> {
        self.files
            .get(origin.map_or(UNATTRIBUTED, WriteOrigin::slot))
    }
}

/// What a tree charges to its own counters besides what passes its backend.
#[cfg(feature = "metrics")]
impl WriteIoStats {
    /// Records `bytes` of `origin`'s files installed.
    pub(crate) fn installed(&self, origin: WriteOrigin, bytes: u64) {
        if let Some(counters) = self.slot(Some(origin)) {
            counters.installed.fetch_add(bytes, Relaxed);
        }
    }

    /// Records a value of `payload` bytes, key included, taken in.
    pub(crate) fn accepted_value(&self, payload: usize) {
        self.accepted.values.fetch_add(1, Relaxed);
        self.accepted
            .payload_bytes
            .fetch_add(payload as u64, Relaxed);
    }

    /// Records a point deletion of a `key` bytes long key taken in.
    pub(crate) fn accepted_deletion(&self, key: usize) {
        self.accepted.deletions.fetch_add(1, Relaxed);
        self.accepted.payload_bytes.fetch_add(key as u64, Relaxed);
    }

    /// Records a range deletion of `bounds` bytes of bounds taken in.
    pub(crate) fn accepted_range_deletion(&self, bounds: usize) {
        self.accepted.range_deletions.fetch_add(1, Relaxed);
        self.accepted
            .payload_bytes
            .fetch_add(bounds as u64, Relaxed);
    }

    /// Records `records` ingested records of `payload` bytes taken in.
    pub(crate) fn accepted_ingestion(&self, records: u64, payload: u64) {
        self.accepted.ingested.fetch_add(records, Relaxed);
        self.accepted.payload_bytes.fetch_add(payload, Relaxed);
    }
}

/// A backend that counts what is written through it, into [`WriteIoStats`].
///
/// Every operation is passed to the backend it wraps unchanged; a file opened
/// only for reading is the backend's own handle, so reads cost nothing more.
/// One wrapped around a backend several trees share counts them all; a tree
/// counts its own writes the same way with the `metrics` feature.
///
/// # Examples
///
/// ```
/// use lsm_tree::fs::{CountingFs, Fs, FsOpenOptions, MemFs, WriteOrigin};
/// use lsm_tree::io::Write as _;
/// use std::path::Path;
/// use std::sync::Arc;
///
/// let fs = CountingFs::new(Arc::new(MemFs::new()));
/// fs.create_dir_all(Path::new("/db"))?;
/// let mut file = fs.open(
///     Path::new("/db/1"),
///     &FsOpenOptions::new()
///         .write(true)
///         .create_new(true)
///         .write_origin(WriteOrigin::Metadata),
/// )?;
/// file.write_all(b"hello")?;
/// file.sync_all()?;
///
/// let written = fs.stats().snapshot().origin(WriteOrigin::Metadata);
/// assert_eq!(written.bytes, 5);
/// assert_eq!(written.syncs, 1);
/// # Ok::<(), lsm_tree::io::Error>(())
/// ```
pub struct CountingFs {
    inner: Arc<dyn Fs>,
    stats: Arc<WriteIoStats>,
}

impl CountingFs {
    /// Counts what is written through `inner` into counters of its own.
    #[must_use]
    pub fn new(inner: Arc<dyn Fs>) -> Self {
        Self::with_stats(inner, Arc::default())
    }

    /// Counts what is written through `inner` into `stats`, which other
    /// backends may count into too.
    #[must_use]
    pub fn with_stats(inner: Arc<dyn Fs>, stats: Arc<WriteIoStats>) -> Self {
        Self { inner, stats }
    }

    /// The counters this backend counts into.
    #[must_use]
    pub fn stats(&self) -> &Arc<WriteIoStats> {
        &self.stats
    }

    fn count<T>(counter: &AtomicU64, result: io::Result<T>) -> io::Result<T> {
        if result.is_ok() {
            counter.fetch_add(1, Relaxed);
        }
        result
    }
}

impl Fs for CountingFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let file = self.inner.open(path, opts)?;
        let writes = opts.write || opts.append || opts.create || opts.create_new || opts.truncate;
        if !writes {
            return Ok(file);
        }
        let Some(counters) = self.stats.slot(opts.write_origin) else {
            return Ok(file);
        };
        counters.opened.fetch_add(1, Relaxed);
        if opts.create || opts.create_new {
            counters.created.fetch_add(1, Relaxed);
        }
        Ok(Box::new(CountingFile {
            inner: file,
            stats: Arc::clone(&self.stats),
            origin: opts.write_origin,
        }))
    }

    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        self.inner.read_blocks_batched(reqs)
    }

    fn read_blocks_batched_each(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: &mut dyn FnMut(usize, &BlockRead<'_>),
    ) -> io::Result<()> {
        self.inner.read_blocks_batched_each(reqs, on_read)
    }

    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        self.inner.read_queue()
    }

    fn reads_by_descriptor(&self) -> bool {
        self.inner.reads_by_descriptor()
    }

    fn check_descriptor_batch(&self, reqs: &[BlockRead<'_>]) -> io::Result<()> {
        self.inner.check_descriptor_batch(reqs)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        Self::count(
            &self.stats.directory.dirs_created,
            self.inner.create_dir_all(path),
        )
    }

    fn create_dir(&self, path: &Path) -> io::Result<()> {
        Self::count(
            &self.stats.directory.dirs_created,
            self.inner.create_dir(path),
        )
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        self.inner.read_dir(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        Self::count(&self.stats.directory.removed, self.inner.remove_file(path))
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        Self::count(
            &self.stats.directory.dirs_removed,
            self.inner.remove_dir_all(path),
        )
    }

    fn remove_dir(&self, path: &Path) -> io::Result<()> {
        Self::count(
            &self.stats.directory.dirs_removed,
            self.inner.remove_dir(path),
        )
    }

    fn same_file(&self, a: &Path, b: &Path) -> io::Result<bool> {
        self.inner.same_file(a, b)
    }

    fn read_link(&self, path: &Path) -> io::Result<Option<PathBuf>> {
        self.inner.read_link(path)
    }

    fn symlink(&self, target: &Path, link: &Path, to_dir: bool) -> io::Result<()> {
        Self::count(
            &self.stats.directory.symlinked,
            self.inner.symlink(target, link, to_dir),
        )
    }

    fn symlink_to_dir(&self, path: &Path) -> io::Result<bool> {
        self.inner.symlink_to_dir(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        Self::count(&self.stats.directory.renamed, self.inner.rename(from, to))
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        self.inner.metadata(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        let result = self.inner.sync_directory(path);
        self.directory_synced(&result);
        result
    }

    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        let result = self.inner.sync_directory_with(path, mode);
        self.directory_synced(&result);
        result
    }

    fn sync_device(&self, path: &Path) -> io::Result<()> {
        let result = self.inner.sync_device(path);
        self.stats.directory.device_syncs.fetch_add(1, Relaxed);
        if result.is_err() {
            self.stats
                .directory
                .device_sync_errors
                .fetch_add(1, Relaxed);
        }
        result
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        self.inner.exists(path)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        Self::count(&self.stats.directory.linked, self.inner.hard_link(src, dst))
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
        Self::count(
            &self.stats.directory.holes_punched,
            self.inner.punch_hole(path, offset, len),
        )
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        Self::count(
            &self.stats.directory.cloned,
            self.inner.reflink_file(src, dst),
        )
    }

    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        Self::count(
            &self.stats.directory.truncated,
            self.inner.truncate_file(path),
        )
    }

    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        self.inner.hard_link_count(path)
    }

    fn available_space(&self, path: &Path) -> io::Result<u64> {
        self.inner.available_space(path)
    }

    fn allocated_size(&self, path: &Path) -> io::Result<Option<u64>> {
        self.inner.allocated_size(path)
    }

    fn extent_is_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        self.inner.extent_is_hole(path, offset, len)
    }

    fn extent_contains_hole(&self, path: &Path, offset: u64, len: u64) -> io::Result<Option<bool>> {
        self.inner.extent_contains_hole(path, offset, len)
    }
}

impl CountingFs {
    fn directory_synced(&self, result: &io::Result<()>) {
        self.stats.directory.syncs.fetch_add(1, Relaxed);
        if result.is_err() {
            self.stats.directory.sync_errors.fetch_add(1, Relaxed);
        }
    }
}

/// A file opened for writing through a [`CountingFs`].
struct CountingFile {
    inner: Box<dyn FsFile>,
    stats: Arc<WriteIoStats>,
    origin: Option<WriteOrigin>,
}

impl CountingFile {
    fn counters(&self) -> Option<&FileCounters> {
        self.stats.slot(self.origin)
    }

    fn synced(&self, result: io::Result<()>) -> io::Result<()> {
        if let Some(counters) = self.counters() {
            counters.synced(&result);
        }
        result
    }
}

#[cfg(feature = "std")]
impl std::io::Read for CountingFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}
#[cfg(not(feature = "std"))]
impl Read for CountingFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.inner.read(buf)
    }
}

#[cfg(feature = "std")]
impl std::io::Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let result = self.inner.write(buf);
        if let Some(counters) = self.counters() {
            counters.wrote(buf.len(), result.as_ref().ok().copied());
        }
        result
    }

    fn write_vectored(&mut self, bufs: &[std::io::IoSlice<'_>]) -> std::io::Result<usize> {
        let result = self.inner.write_vectored(bufs);
        if let Some(counters) = self.counters() {
            let handed = bufs.iter().map(|buf| buf.len()).sum();
            counters.wrote(handed, result.as_ref().ok().copied());
        }
        result
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
#[cfg(not(feature = "std"))]
impl Write for CountingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let result = self.inner.write(buf);
        if let Some(counters) = self.counters() {
            counters.wrote(buf.len(), result.as_ref().ok().copied());
        }
        result
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(feature = "std")]
impl std::io::Seek for CountingFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.inner.seek(pos)
    }
}
#[cfg(not(feature = "std"))]
impl Seek for CountingFile {
    fn seek(&mut self, pos: crate::io::SeekFrom) -> io::Result<u64> {
        self.inner.seek(pos)
    }
}

impl FsFile for CountingFile {
    fn sync_all(&self) -> io::Result<()> {
        self.synced(self.inner.sync_all())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.synced(self.inner.sync_data())
    }

    fn sync_all_with(&self, mode: SyncMode) -> io::Result<()> {
        self.synced(self.inner.sync_all_with(mode))
    }

    fn sync_data_with(&self, mode: SyncMode) -> io::Result<()> {
        self.synced(self.inner.sync_data_with(mode))
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        self.inner.metadata()
    }

    fn hard_link_count(&self) -> io::Result<u64> {
        self.inner.hard_link_count()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        let result = self.inner.set_len(size);
        if result.is_ok()
            && let Some(counters) = self.counters()
        {
            counters.truncated.fetch_add(1, Relaxed);
        }
        result
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        self.inner.read_at(buf, offset)
    }

    fn read_many(&self, regions: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        self.inner.read_many(regions)
    }

    fn backing_fd(&self) -> Option<i32> {
        self.inner.backing_fd()
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

    fn start_writeback(&self, offset: u64, len: u64) -> io::Result<()> {
        let result = self.inner.start_writeback(offset, len);
        if result.is_ok()
            && let Some(counters) = self.counters()
        {
            counters.writebacks.fetch_add(1, Relaxed);
        }
        result
    }

    fn set_write_lifetime(&self, lifetime: WriteLifetime) -> io::Result<()> {
        self.inner.set_write_lifetime(lifetime)
    }
}

#[cfg(all(test, feature = "std"))]
mod tests;
