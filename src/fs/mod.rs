// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Pluggable filesystem abstraction for I/O backends.
//!
//! The [`Fs`](crate::fs::Fs) trait is intended to abstract the filesystem operations
//! that lsm-tree performs, allowing alternative backends such as
//! `io_uring`, in-memory filesystems for deterministic testing, or cloud
//! blob storage. Call-site migration is tracked in separate issues.
//!
//! The default implementation [`StdFs`](crate::fs::StdFs) delegates to [`std::fs`] and
//! is a zero-sized type, so it adds no runtime overhead when used as a
//! monomorphized generic parameter.
//!
//! # Platform-specific backends
//!
//! - **Linux 5.6+**: `IoUringFs` - batched SQE submission via `io_uring`
//!   (feature-gated: `io-uring`)
//! - **Windows**: IOCP (`IoCompletionPort`) could provide similar batched
//!   completion semantics - not yet implemented, tracked for when Windows
//!   becomes a production target
//! - **macOS / BSD**: no batched I/O API exists (`dispatch_io` and `kqueue`
//!   do not help for storage I/O patterns); [`StdFs`](crate::fs::StdFs) is the correct choice

mod aligned_buf;
// `direct_io` is std-only (touches `std::fs::OpenOptions`). It is
// gated behind the `std` feature so a `no_std + alloc` build of
// this crate does not even attempt to compile it. The wider
// `fs::*` backend (Fs / FsFile traits, std_fs, io_uring_fs)
// still depends on `std::io::{Read, Write, Seek}` + `std::path::Path`
// - those have no `core::*` equivalents, so feature-gating just
// this submodule does not yet make a no-std build work end-to-end.
// Porting the traits off std::io / std::path is tracked as #311
// (prerequisite); the wider no-std migration epic is #274. This
// gate is the first concrete step.
#[cfg(feature = "std")]
mod direct_io;
mod mem_fs;
mod std_fs;

pub use aligned_buf::AlignedBuf;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
mod io_uring_fs;

pub use mem_fs::MemFs;
pub use std_fs::StdFs;
pub(crate) use std_fs::is_cross_device;

#[cfg(all(target_os = "linux", feature = "io-uring"))]
pub use io_uring_fs::{IoUringFs, is_io_uring_available};

// `Read` / `Write` / `Seek` come from `crate::io`, the local mirror
// of `std::io` that compiles under `no_std + alloc`. Under `feature =
// "std"` the blanket impls in `crate::io` forward to `std::io::*`, so
// existing std-backed backends (`std_fs`, `io_uring_fs`) satisfy
// these bounds without any change to their own impls.
//
// `io::{Result, Error, ErrorKind}` still come from `std::io` here
// (no public re-export - `use std::io;` is a local module alias).
// The trait *bounds* are what blocked no-std for this module; the
// surrounding `io::Result<T>` return type still resolves to
// `std::io::Result<T>` and will be migrated to `crate::io::Result<T>`
// in a follow-up so we keep the diff scoped to what this issue
// actually unblocks. `std::path::Path` likewise stays for now -
// the path migration is the second blocker tracked separately.
use crate::io::{Read, Seek, Write};
use std::io;
use std::path::{Path, PathBuf};

/// Options for opening a file through the [`Fs`] trait.
///
/// Mirrors the builder API of [`std::fs::OpenOptions`].
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors std::fs::OpenOptions which uses bool flags for each mode"
)]
// `non_exhaustive` paired with the `direct_io` field landing in the
// same release. The new field already breaks struct-literal
// callers; bundling `non_exhaustive` in the same semver-major bump
// confines the break to one release and lets every future field
// land as semver-minor. Builder methods (`.read()`, `.write()`, …,
// `.direct_io()`) cover every field, so callers using the builder
// API are unaffected.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct FsOpenOptions {
    /// Open for reading.
    pub read: bool,
    /// Open for writing.
    pub write: bool,
    /// Create the file if it does not exist.
    pub create: bool,
    /// Create a new file and fail if it already exists.
    pub create_new: bool,
    /// Truncate the file to zero length on open.
    pub truncate: bool,
    /// Open in append mode, so writes go to the end of the file.
    pub append: bool,
    /// Bypass the kernel page cache for this file (`O_DIRECT` on Linux).
    ///
    /// When set, the caller is responsible for issuing reads and writes
    /// at offsets aligned to the filesystem's logical block size, with
    /// userspace buffers aligned to the same boundary and lengths that
    /// are a multiple of that block size.
    ///
    /// `direct_io` is a HINT, not a guarantee. The flag is honoured only
    /// on Linux and Android, and only on architectures where the
    /// `asm-generic/fcntl.h` value `O_DIRECT = 0o40000` is authoritative on
    /// `x86`, `x86_64`, `aarch64`, `riscv32`/`riscv64`, `loongarch64`, and
    /// `s390x`. On Linux
    /// architectures with a divergent `O_DIRECT` bit (arm `0o200000`,
    /// mips `0o100000`, parisc, sparc) the flag is silently dropped to
    /// avoid passing the wrong bit to `open(2)`. Other platforms - macOS
    /// (would need `F_NOCACHE` post-open via `fcntl`, not wired here),
    /// Windows (would need `FILE_FLAG_NO_BUFFERING` at `CreateFile` time,
    /// not wired here), other Unixes - also silently drop the flag.
    ///
    /// Callers must therefore treat `direct_io` as best-effort:
    /// correctness must not depend on cache bypass being in effect, and
    /// any alignment requirements imposed by the kernel only apply when
    /// the flag is actually honoured (you cannot tell from this API
    /// alone whether it was). See [`AlignedBuf`] for an aligned heap
    /// buffer suitable for `O_DIRECT` reads and writes when the flag is
    /// honoured.
    pub direct_io: bool,
}

impl Default for FsOpenOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl FsOpenOptions {
    /// Creates a new set of options with everything disabled.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            read: false,
            write: false,
            create: false,
            create_new: false,
            truncate: false,
            append: false,
            direct_io: false,
        }
    }

    /// Sets the `read` flag.
    #[must_use]
    pub const fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Sets the `write` flag.
    #[must_use]
    pub const fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Sets the `create` flag.
    #[must_use]
    pub const fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Sets the `create_new` flag.
    #[must_use]
    pub const fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Sets the `truncate` flag.
    #[must_use]
    pub const fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// Sets the `append` flag.
    #[must_use]
    pub const fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Sets the `direct_io` flag.
    #[must_use]
    pub const fn direct_io(mut self, direct_io: bool) -> Self {
        self.direct_io = direct_io;
        self
    }
}

/// Metadata about a file or directory.
#[derive(Clone, Debug)]
pub struct FsMetadata {
    /// Size in bytes. For directories the value is platform-dependent.
    pub len: u64,
    /// Whether this entry is a directory.
    pub is_dir: bool,
    /// Whether this entry is a regular file.
    pub is_file: bool,
}

/// Static capability profile of a filesystem backend.
///
/// Different filesystems offer different guarantees - per-block integrity
/// checks, copy-on-write semantics, O(1) reflink clones, native snapshots.
/// The storage engine queries [`Fs::capabilities`] to make FS-aware decisions
/// (skip redundant checksums where the FS already verifies, disable `CoW` on
/// write-once SST files, prefer reflink over hard-link for checkpoints).
///
/// The [`Default`] is conservative: every field `false`, i.e. "assume the
/// backend offers no special guarantees". A backend opts into an optimization
/// only by reporting the corresponding capability `true`, so an unknown or
/// third-party backend is always treated safely.
//
// no-std: pure data type - `Copy` struct of `bool`s, no allocator needed.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
#[expect(
    clippy::struct_excessive_bools,
    reason = "a capability profile is a flat set of independent yes/no FS guarantees; \
              each flag is queried on its own, so distinct bools read clearer than \
              bitflags or a state machine (same rationale as FsOpenOptions)"
)]
pub struct FsCapabilities {
    /// The filesystem detects single-byte corruption on read and returns an
    /// I/O error (EIO) rather than silently handing back wrong bytes. True for
    /// ZFS, Btrfs, `ReFS`, and S3-backed backends (service-side validation).
    ///
    /// When set, the engine may skip computing its own file checksum on the
    /// read path and rely on the FS surfacing corruption.
    pub per_block_integrity_on_read: bool,

    /// The filesystem has a background scrub mechanism (`zfs scrub`,
    /// `btrfs scrub`) that detects and repairs latent corruption out-of-band.
    pub background_scrub: bool,

    /// The filesystem is inherently copy-on-write (Btrfs, ZFS, APFS). Implies
    /// atomic full-file rewrite, but a fragmentation penalty on the
    /// append-then-read SST access pattern - the engine can disable per-file
    /// `CoW` on write-once SSTs to recover throughput.
    pub copy_on_write: bool,

    /// The filesystem supports an O(1) reflink data clone (`FICLONE` on Linux,
    /// `clonefile(2)` on macOS/APFS, block cloning on `ReFS`). A reflinked file
    /// shares blocks copy-on-write yet has an independent inode.
    pub reflink: bool,

    /// The filesystem supports instant native snapshots (Btrfs subvolume
    /// snapshot, ZFS snapshot, APFS snapshot).
    pub native_snapshot: bool,
}

/// A directory entry returned by [`Fs::read_dir`].
#[derive(Clone, Debug)]
pub struct FsDirEntry {
    /// Full path to the entry.
    pub path: PathBuf,
    /// File name component (without parent path).
    // String (not OsString) - lsm-tree uses numeric file names for tables/blobs.
    // StdFs::read_dir returns InvalidData for non-UTF-8 names (not lossy) since
    // any such name indicates filesystem corruption for this crate's usage.
    pub file_name: String,
    /// Whether this entry is a directory.
    pub is_dir: bool,
}

/// Access-pattern hint passed to [`FsFile::hint`].
///
/// Backends translate it to the platform's nearest equivalent
/// (`posix_fadvise` on Linux, no-op on macOS / Windows for now). The
/// hint is advisory - backends are free to ignore it - and only
/// influences kernel readahead / page-cache eviction heuristics, not
/// correctness.
///
/// Used at SST/blob open sites to tell the OS what we're about to do
/// with the file (sequential scan, random point read, write-once-and-
/// evict, …). Picking the right hint cuts page-cache double-buffering
/// on cold-path compaction reads and prevents readahead waste on
/// point-read SST files.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum FileHint {
    /// No hint - leave the kernel's default caching / readahead policy in
    /// place. Use when the access pattern is unknown or genuinely mixed.
    #[default]
    Default,

    /// File will be read mostly forward, in order
    /// (`POSIX_FADV_SEQUENTIAL`). Tells the kernel to ramp up readahead
    /// and evict already-read pages aggressively. Use for compaction
    /// input files and full-range scans.
    Sequential,

    /// File will be read at scattered offsets with no useful prefetch
    /// pattern (`POSIX_FADV_RANDOM`). Tells the kernel to disable
    /// readahead so it doesn't waste bandwidth speculatively loading
    /// pages we won't touch. Use for SST files opened for point-read
    /// service.
    Random,

    /// File is being written and we won't need it cached afterwards -
    /// drop pages from the page cache when the write completes
    /// (`POSIX_FADV_DONTNEED`). Use for compaction output and memtable
    /// flush output to keep them from evicting hot pages of files we're
    /// still reading from.
    WriteOnce,
}

/// Durability level for a sync (fsync) operation.
///
/// The distinction is only observable on macOS, where Rust's
/// [`std::fs::File::sync_all`] / [`sync_data`](std::fs::File::sync_data)
/// both issue `fcntl(F_FULLFSYNC)` - a full hardware barrier that flushes
/// the drive's write cache to the platters (~4 ms on a modern SSD). On
/// every other platform `sync_all` is already a plain `fsync` and both
/// variants behave identically.
///
/// [`Self::Normal`] is the default: it matches the durability that
/// `RocksDB` and `SQLite` give out of the box (plain `fsync`, which on macOS
/// reaches the drive cache but not necessarily the platters). [`Self::Full`]
/// opts into `F_FULLFSYNC` on macOS for callers that need power-loss
/// durability at the cost of a much slower flush.
//
// no-std: pure data type - `Copy` enum, no allocator needed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum SyncMode {
    /// Plain `fsync` on every platform. On macOS this does NOT issue
    /// `F_FULLFSYNC`, so it is dramatically faster but only guarantees the
    /// data reached the drive's write cache (same as `RocksDB` / `SQLite`
    /// defaults). The default.
    #[default]
    Normal,

    /// Full durability. On macOS this issues `fcntl(F_FULLFSYNC)` so the
    /// data survives power loss; elsewhere it is identical to
    /// [`Self::Normal`]. Slower - opt in only when the workload needs
    /// power-loss durability without an external journal.
    Full,
}

/// Filesystem operations on an open file handle.
///
/// Extends [`Read`] + [`Write`] + [`Seek`] with persistence and
/// metadata operations needed by the storage engine.
pub trait FsFile: Read + Write + Seek + Send + Sync {
    /// Flushes all OS-internal buffers and metadata to durable storage.
    ///
    /// Equivalent to [`sync_all_with`](Self::sync_all_with) with
    /// [`SyncMode::Full`] - the strongest durability the platform offers
    /// (`F_FULLFSYNC` on macOS).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_all(&self) -> io::Result<()>;

    /// Flushes file data (but not necessarily metadata) to durable storage.
    ///
    /// Equivalent to [`sync_data_with`](Self::sync_data_with) with
    /// [`SyncMode::Full`].
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_data(&self) -> io::Result<()>;

    /// Flushes all buffers and metadata at the requested durability
    /// [`SyncMode`].
    ///
    /// The default implementation ignores `mode` and delegates to
    /// [`sync_all`](Self::sync_all) (full durability); backends where the
    /// mode is observable (the std `File` backend on macOS) override this.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_all_with(&self, mode: SyncMode) -> io::Result<()> {
        let _ = mode;
        self.sync_all()
    }

    /// Flushes file data at the requested durability [`SyncMode`].
    ///
    /// The default implementation ignores `mode` and delegates to
    /// [`sync_data`](Self::sync_data).
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_data_with(&self, mode: SyncMode) -> io::Result<()> {
        let _ = mode;
        self.sync_data()
    }

    /// Returns metadata for this open file handle.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if metadata cannot be retrieved.
    fn metadata(&self) -> io::Result<FsMetadata>;

    /// Truncates or extends the file to the specified length.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the length change fails.
    fn set_len(&self, size: u64) -> io::Result<()>;

    /// Reads bytes from the file at the given offset without changing the
    /// file cursor position.
    ///
    /// Equivalent to `pread(2)` on Unix. Multiple threads can call this
    /// concurrently on the same file handle without synchronization.
    ///
    /// Implementations must provide *fill-or-EOF* semantics: on success,
    /// this method either fills `buf` completely and returns
    /// `Ok(buf.len())`, or returns `Ok(n)` with `n < buf.len()` only if
    /// the read has reached EOF. Callers may rely on a short read
    /// indicating EOF and therefore do not need a retry loop.
    ///
    /// Implementations are responsible for handling OS-level short reads
    /// and interrupts internally (for example, by retrying on `EINTR`)
    /// so that the above guarantee holds unless an error is returned.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the read fails.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize>;

    /// Acquires an exclusive (write) lock on this file.
    ///
    /// Blocks until the lock is acquired.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if locking fails or is unsupported.
    fn lock_exclusive(&self) -> io::Result<()>;

    /// Advise the kernel about the expected access pattern for this file.
    ///
    /// Implementations translate the [`FileHint`] to the platform's
    /// closest primitive (`posix_fadvise` on Linux, no-op on macOS /
    /// Windows / in-memory backends for now). The default trait impl
    /// is a no-op so backends that have nothing useful to do here
    /// don't need to override it.
    ///
    /// The hint is advisory - backends may ignore it - and only
    /// influences kernel readahead / page-cache eviction heuristics, not
    /// correctness.
    ///
    /// # Errors
    ///
    /// Returns an I/O error only if the underlying syscall fails with a
    /// non-`EINVAL` error. A backend that doesn't support the requested
    /// hint should treat the call as a no-op and return `Ok(())` rather
    /// than fail - the hint is a performance lever, not a correctness
    /// requirement.
    fn hint(&self, _hint: FileHint) -> io::Result<()> {
        Ok(())
    }
}

/// Pluggable filesystem abstraction.
///
/// Intended to cover all filesystem operations that lsm-tree performs.
/// The default implementation [`StdFs`] delegates to [`std::fs`].
///
/// # Object safety
///
/// `Fs` is object-safe and can be used as `Arc<dyn Fs>` directly:
/// ```
/// # use lsm_tree::fs::{Fs, StdFs};
/// # use std::sync::Arc;
/// let _: Arc<dyn Fs> = Arc::new(StdFs);
/// ```
pub trait Fs: Send + Sync + 'static {
    /// Opens a file at `path` with the given options.
    ///
    /// Returns a boxed file handle. For syscall-backed implementations
    /// like [`StdFs`], the allocation and dynamic dispatch overhead is
    /// typically negligible compared to the underlying I/O operations.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened.
    // Box<dyn FsFile> is intentionally 'static (the default) - file handles are
    // owned values that do not borrow from the Fs instance that created them.
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>>;

    /// Recursively creates all directories leading to `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if directory creation fails.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Atomically creates a single directory at `path`.
    ///
    /// Unlike [`create_dir_all`](Self::create_dir_all), this method must
    /// FAIL with [`io::ErrorKind::AlreadyExists`] when `path` already
    /// exists - it is the POSIX `mkdir(2)` primitive used by
    /// [`AbstractTree::create_checkpoint`](crate::AbstractTree::create_checkpoint) to
    /// claim its target directory without a TOCTOU window.
    ///
    /// The parent directory must already exist; this method does not
    /// recurse.
    ///
    /// # Default implementation
    ///
    /// Returns [`io::ErrorKind::Unsupported`]. Backends that want to be
    /// usable as a checkpoint target MUST override this method.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::AlreadyExists`] if `path` already exists,
    /// [`io::ErrorKind::NotFound`] if the parent directory does not
    /// exist, or another I/O error if creation fails for backend-specific
    /// reasons.
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Fs::create_dir is not implemented for this backend",
        ))
    }

    /// Returns all entries in a directory (order is unspecified).
    ///
    /// Returns a `Vec` rather than a streaming iterator because
    /// `read_dir` is a cold-path operation (recovery, compaction file
    /// listing) where directory sizes are expected to remain small.
    /// Callers that need a specific order must sort the result.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be read or if any
    /// individual entry fails.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>>;

    /// Removes a single file.
    ///
    /// If the file does not exist, implementations must return
    /// [`io::ErrorKind::NotFound`]. Callers such as version GC rely on
    /// this to perform idempotent deletes.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be removed.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Recursively removes a directory and all of its contents.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the directory cannot be removed.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Renames a file from `from` to `to`.
    ///
    /// If `to` already exists as a regular file, it is atomically replaced.
    /// This is required by [`rewrite_atomic`](crate::file::rewrite_atomic)
    /// for crash-safe version pointer updates.
    ///
    /// lsm-tree only renames files (table files, version pointers), never
    /// directories. [`MemFs`] rejects directory renames with `InvalidInput`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the rename fails.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Returns metadata for the file or directory at `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if metadata cannot be retrieved.
    fn metadata(&self, path: &Path) -> io::Result<FsMetadata>;

    /// Ensures directory metadata is persisted to durable storage.
    ///
    /// On platforms that do not support directory fsync (e.g. Windows),
    /// this may be a no-op.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_directory(&self, path: &Path) -> io::Result<()>;

    /// Ensures directory metadata is persisted at the requested durability
    /// [`SyncMode`].
    ///
    /// The default implementation ignores `mode` and delegates to
    /// [`sync_directory`](Self::sync_directory); backends where the mode is
    /// observable (the std backend on macOS) override this.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the sync operation fails.
    fn sync_directory_with(&self, path: &Path, mode: SyncMode) -> io::Result<()> {
        let _ = mode;
        self.sync_directory(path)
    }

    /// Returns `Ok(true)` if a file or directory exists at `path`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the existence of `path` cannot be determined
    /// (for example, due to permission issues or transient backend failures).
    fn exists(&self, path: &Path) -> io::Result<bool>;

    /// Creates a hard link `dst` that refers to the same inode as `src`.
    ///
    /// Used by [`AbstractTree::create_checkpoint`](crate::AbstractTree::create_checkpoint)
    /// to snapshot SST and blob files in O(1) per file without duplicating
    /// data on disk. After the link is created, deleting either path leaves
    /// the other intact; the inode is reclaimed only after the last link
    /// is removed.
    ///
    /// # Cross-filesystem behaviour
    ///
    /// Hard links cannot span filesystems. `hard_link` is a PURE link: on a
    /// cross-device situation (Unix `EXDEV` / [`io::ErrorKind::CrossesDevices`])
    /// implementations surface the error rather than silently byte-copying.
    /// Callers that want a cross-filesystem copy use the checkpoint driver's
    /// `link_or_copy_cross_fs`, which detects the cross-device (and
    /// `Unsupported` / `NotFound`) error and performs a `SyncMode`-aware
    /// streamed copy. Keeping the copy in one place means the copied file's
    /// durability honors `Config::sync_mode` instead of an unconditional
    /// barrier hidden inside `hard_link`.
    ///
    /// In-memory backends ([`MemFs`]) have no inodes; they implement this
    /// as a byte copy that produces an independent file with the same
    /// contents (their "link" semantics).
    ///
    /// # Default implementation
    ///
    /// Returns [`io::ErrorKind::Unsupported`]. Backends are free to leave
    /// this default in place: the checkpoint driver's
    /// `link_or_copy_cross_fs` helper treats `Unsupported` (and `NotFound`)
    /// as a signal to fall back to a streamed byte copy, so snapshots
    /// still succeed - they just lose the O(1) hard-link optimisation
    /// and pay full-bytes worth of disk on the target volume. Backends
    /// that DO support real hard links (most kernel filesystems) should
    /// override this for the inode-sharing benefit.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if `src` does not exist, `dst` already exists,
    /// the destination's parent directory is missing, or `src` and `dst`
    /// are on different filesystems (cross-device - surfaced, not copied).
    fn hard_link(&self, src: &Path, dst: &Path) -> io::Result<()> {
        let _ = (src, dst);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Fs::hard_link is not implemented for this backend",
        ))
    }

    /// Identifies the **path namespace** this backend resolves against.
    ///
    /// Two `Fs` values may safely participate in a cross-backend
    /// [`Fs::hard_link`] call only if their `backend_id()` values are
    /// both `Some` and equal: that contract states "we resolve paths
    /// against the same underlying inode table, so a hard link from
    /// `src` here to `dst` there links the SAME file we'd see by
    /// reading `src` through `Self`."
    ///
    /// Examples of equal backend IDs:
    /// - All [`crate::fs::StdFs`] instances on the same host (one
    ///   shared kernel filesystem).
    /// - An `IoUringFs` and a [`crate::fs::StdFs`] on the
    ///   same host (the uring backend delegates path resolution to the
    ///   kernel).
    ///
    /// Examples of distinct backend IDs:
    /// - Two independent [`crate::fs::MemFs`] instances (each has its
    ///   own in-memory tree).
    /// - A [`crate::fs::MemFs`] vs any kernel-backed backend (a
    ///   hard-link attempt would resolve `src` against the host
    ///   filesystem and silently capture an unrelated file if one
    ///   happens to exist at the same path - a checkpoint correctness
    ///   bug).
    ///
    /// The default returns `None`, meaning "no shared-namespace
    /// guarantee" - safe-by-default for third-party backends that have
    /// not opted in. Callers MUST treat `None` as a veto on
    /// cross-backend [`Fs::hard_link`] and stream-copy instead, even
    /// when both sides return `None`.
    fn backend_id(&self) -> Option<u64> {
        None
    }

    /// Reports the [`FsCapabilities`] of the filesystem backing `path`.
    ///
    /// Capabilities are a property of the *mount* `path` lives on, not of the
    /// backend as a whole: one [`StdFs`] can serve a data directory on Btrfs and
    /// a WAL on ext4. Callers pass the directory whose mount they care about
    /// (typically the tree's data dir).
    ///
    /// The default is conservative - every capability `false`, i.e. "assume no
    /// special FS guarantees". A backend overrides this to opt into FS-aware
    /// optimizations (skip redundant checksums, disable `CoW` on SSTs, reflink
    /// checkpoints). Unknown / third-party backends keep the safe default.
    fn capabilities(&self, path: &Path) -> FsCapabilities {
        let _ = path;
        FsCapabilities::default()
    }

    /// Requests that per-file copy-on-write be disabled for the file at `path`.
    ///
    /// On `CoW` filesystems (Btrfs) write-once SST files gain no benefit from `CoW`
    /// and suffer a fragmentation penalty; clearing the per-file `CoW` flag
    /// (`FS_NOCOW_FL` via `FS_IOC_SETFLAGS`) recovers throughput. The flag only
    /// takes effect on a file with no data blocks yet, so callers invoke this
    /// immediately after creating the (empty) SST file and before writing.
    ///
    /// Path-based (rather than handle-based) so the trait stays object-safe and
    /// portable: backends with no fd concept ([`MemFs`], a future Windows
    /// backend) implement it as a no-op.
    ///
    /// # Default implementation
    ///
    /// No-op returning `Ok(())`. Backends on non-`CoW` filesystems, or that
    /// cannot express the request, correctly leave this default in place -
    /// disabling `CoW` is a throughput optimization, never a correctness
    /// requirement.
    ///
    /// # Errors
    ///
    /// Returns an I/O error only if the backend supports the operation and the
    /// underlying syscall fails for a reason other than "not applicable".
    fn try_disable_cow(&self, path: &Path) -> io::Result<()> {
        let _ = path;
        Ok(())
    }

    /// Clones the file at `src` to `dst` with O(1) reflink semantics when the
    /// backend supports it (`FICLONE` on Linux, `clonefile(2)` on macOS/APFS,
    /// `ReFS` block cloning on Windows).
    ///
    /// A reflinked clone shares data blocks copy-on-write but has an
    /// independent inode: later modifications to either path do not affect the
    /// other, and there is no max-links-per-inode constraint. This makes it a
    /// safer, cheaper alternative to [`hard_link`](Self::hard_link) for
    /// checkpoint / backup tooling.
    ///
    /// # Default implementation
    ///
    /// Falls back to a streamed byte copy through this backend's own
    /// [`open`](Self::open) - correct on every backend, just without the O(1)
    /// block-sharing benefit. `dst` is created with `create_new`, so the call
    /// fails if it already exists. Backends that support real reflink override
    /// this for the O(1) path.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if `src` cannot be read, `dst` already exists or
    /// cannot be created, or the clone / copy fails.
    fn reflink_file(&self, src: &Path, dst: &Path) -> io::Result<()> {
        copy_file_streamed(self, src, dst)
    }

    /// Returns the data blocks of the file at `path` to the filesystem,
    /// leaving a zero-length file in place.
    ///
    /// This is the synchronous half of obsolete-file reclaim: it frees the
    /// space immediately (so a `walkdir + sum(len)` footprint scan reflects the
    /// reclaim at once) and is O(1) — a metadata operation, not a data rewrite
    /// — while the directory-entry `unlink` can be deferred to a background
    /// deleter. Reclaim is split this way so a caller measuring disk usage
    /// right after a logical delete sees the drop without the foreground thread
    /// blocking on the unlink.
    ///
    /// # Default implementation
    ///
    /// Opens the file for writing and sets its length to zero. Portable across
    /// every backend. A backend MAY override with a filesystem-specific
    /// primitive when one is genuinely cheaper (none beats `set_len(0)` for the
    /// length-based footprint today; the hook exists so an FS that grows one —
    /// reported via [`Fs::capabilities`] — can plug it in without touching the
    /// call sites). [`MemFs`] truncates its in-memory buffer.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the file cannot be opened or truncated.
    fn truncate_file(&self, path: &Path) -> io::Result<()> {
        self.open(path, &FsOpenOptions::new().write(true))?
            .set_len(0)
    }

    /// Number of hard links to the file at `path` (the inode's link count).
    ///
    /// Used to keep [`Self::truncate_file`] safe: truncation frees the inode's
    /// blocks for **every** hard link, so a file a checkpoint has linked (via
    /// [`Self::hard_link`]) must not be truncated — only unlinked. A caller
    /// truncates only when this returns `Ok(1)` (it owns the sole link).
    ///
    /// # Default implementation
    ///
    /// Returns an `Unsupported` error so a backend that cannot report link
    /// counts makes callers conservatively skip truncation (correctness over
    /// the reclaim optimization). [`StdFs`] overrides it on Unix via `nlink`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the count cannot be determined (including the
    /// `Unsupported` default).
    fn hard_link_count(&self, path: &Path) -> io::Result<u64> {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "hard_link_count is not supported by this backend",
        ))
    }
}

/// Streamed independent copy of `src` to `dst` through `fs`'s own [`Fs::open`].
///
/// The fallback for [`Fs::reflink_file`] on backends without O(1) reflink, and
/// the slow path a real reflink takes when the FS declines the clone (non-CoW
/// mount, cross-device). `dst` is created with `create_new`, so an existing
/// target is an error rather than a silent overwrite. The copy is fsynced so a
/// clone is durable on return.
pub(crate) fn copy_file_streamed<F: Fs + ?Sized>(fs: &F, src: &Path, dst: &Path) -> io::Result<()> {
    let mut src_file = fs.open(src, &FsOpenOptions::new().read(true))?;
    let mut dst_file = fs.open(dst, &FsOpenOptions::new().write(true).create_new(true))?;
    // Heap buffer (not a 64 KiB stack array) - keeps the cold-path clone off
    // the stack and satisfies the large-stack-array lint.
    let mut buf = vec![0u8; 64 * 1024].into_boxed_slice();

    // Run the copy in a closure so any failure leaves the original error AND
    // lets us best-effort remove the partial `dst` before propagating —
    // otherwise a mid-copy ENOSPC/EIO leaves a partial file and a retry trips
    // `create_new`'s AlreadyExists for an unrelated reason. Reads retry on
    // EINTR so a signal during the copy doesn't spuriously fail it. Mirrors
    // `checkpoint::link_or_copy_cross_fs`'s streamed-copy contract.
    let result: io::Result<()> = (|| {
        loop {
            let n = match src_file.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            };
            let chunk = buf.get(..n).ok_or_else(|| {
                io::Error::other("read returned more bytes than the buffer holds")
            })?;
            dst_file.write_all(chunk)?;
        }
        dst_file.sync_all()?;
        Ok(())
    })();

    if let Err(e) = result {
        drop(dst_file);
        let _ = fs.remove_file(dst);
        return Err(e);
    }
    Ok(())
}
