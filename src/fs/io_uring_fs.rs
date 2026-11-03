// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

//! `io_uring`-backed [`Fs`] implementation for high-throughput I/O on Linux.
//!
//! Requires the `io-uring` feature flag and Linux 5.6+. Uses a dedicated
//! I/O thread that owns the `io_uring` ring instance. Submissions from
//! multiple threads are batched opportunistically — when several threads
//! submit I/O concurrently, their SQEs are combined into a single
//! `io_uring_enter` syscall.
//!
//! Batched reads go through the ring. Everything else delegates to
//! [`std::fs`] on the caller's thread: a lone read, write or sync has nothing
//! to batch with, and a trip to the ring thread costs more than the syscall.

use super::{
    BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions, QueuedRead, ReadDone, ReadQueue,
};
use crate::HashMap;
use core::sync::atomic::AtomicU64;
use io_uring::{IoUring, opcode, types};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::{Arc, Mutex, mpsc};
use std::thread;

/// Default number of `io_uring` submission queue entries.
const DEFAULT_SQ_ENTRIES: u32 = 256;

/// Probes whether `io_uring` is supported on the running kernel.
///
/// Creates a minimal 2-entry ring and immediately drops it. This tests
/// kernel support without hitting `memlock` rlimits that a full-sized
/// ring might exceed in constrained environments (containers, etc.).
/// [`IoUringFs::new`] may still fail if the default ring size exceeds
/// the process's resource limits.
#[must_use]
pub fn is_io_uring_available() -> bool {
    IoUring::new(2).is_ok()
}

// ---------------------------------------------------------------------------
// IoUringFs
// ---------------------------------------------------------------------------

/// `io_uring`-backed [`Fs`] implementation.
///
/// Routes batched reads through a dedicated `io_uring` ring thread. Every
/// other operation delegates to [`std::fs`] on the caller's thread.
///
/// Multiple `IoUringFs` clones and all the file handles opened
/// through them share the same ring thread.
///
/// # Example
///
/// ```no_run
/// use lsm_tree::fs::IoUringFs;
///
/// let fs = IoUringFs::new().expect("io_uring not available");
/// // Use as Config::new_with_fs(path, fs)
/// ```
pub struct IoUringFs {
    inner: Arc<RingThread>,
}

impl IoUringFs {
    /// Creates a new `IoUringFs` with the default ring size (256 entries).
    ///
    /// # Errors
    ///
    /// Returns an error if `io_uring` is not available on this kernel.
    pub fn new() -> io::Result<Self> {
        Self::with_ring_size(DEFAULT_SQ_ENTRIES)
    }

    /// Creates a new `IoUringFs` with the specified submission queue size.
    ///
    /// Larger rings allow more in-flight operations before the SQ fills.
    /// Powers of two are most efficient (the kernel rounds up regardless).
    ///
    /// # Errors
    ///
    /// Returns an error if `io_uring` is not available on this kernel.
    pub fn with_ring_size(sq_entries: u32) -> io::Result<Self> {
        let inner = RingThread::spawn(sq_entries)?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// [`Fs::read_blocks_batched`], and with `on_read`
    /// [`Fs::read_blocks_batched_each`].
    fn read_blocks(
        &self,
        reqs: &mut [BlockRead<'_>],
        mut on_read: Option<OnRead<'_>>,
    ) -> crate::io::Result<()> {
        // Every request that has an fd goes to the one shared ring in a single
        // batched submission (the kernel fans each read out to its file's
        // device). A request without one (a non-io_uring file mixed into the
        // batch) cannot be submitted, so it is read serially — but only it:
        // degrading the whole batch would forfeit the ring for the requests
        // that were perfectly submittable. A lone request has nothing to batch
        // with, and is read by its descriptor on this thread.
        if let [req] = reqs
            && let Some(fd) = req.file.backing_fd()
        {
            return read_lone(fd, req, on_read);
        }
        if reqs.iter().all(|r| r.file.backing_fd().is_some()) {
            // The common case: nothing to split, and no borrow-gathering pass.
            // Every request is in the one group, so its position IS its index
            // and nothing has to be mapped back.
            return self
                .inner
                .submit_reads_multi(reqs, on_read)
                .map_err(|(_, e)| crate::io::Error::from(e));
        }

        // Indices travel with the requests: the contract is "the FIRST failing
        // block's error", and first means first in the caller's order, not
        // first in whichever group happened to run earlier. Splitting the batch
        // must not reorder which failure wins.
        let (mut submittable, mut serial): (IndexedReads<'_, '_>, IndexedReads<'_, '_>) = reqs
            .iter_mut()
            .enumerate()
            .partition(|(_, r)| r.file.backing_fd().is_some());
        log::debug!(
            "io_uring batched read: {} of {} requests lack a descriptor and are read serially",
            serial.len(),
            submittable.len() + serial.len(),
        );

        // The ring group runs first and is waited out inside
        // `submit_reads_multi` (it drains every completion before returning, so
        // no buffer is still being written while the serial reads run). Its
        // verdict is held rather than returned: a serial request EARLIER in the
        // caller's order may fail too, and that one owns the result.
        let ring_failure = if submittable.is_empty() {
            None
        } else {
            // The caller-order index of each group position: the submission
            // reports a failure, and hands a finished request over, by its
            // position in the group. An earlier request that succeeded must
            // not be blamed for a later one's failure.
            let caller_index: Vec<usize> = submittable.iter().map(|(i, _)| *i).collect();
            let mut group: Vec<&mut BlockRead<'_>> =
                submittable.iter_mut().map(|(_, r)| &mut **r).collect();
            let mut in_caller_order = on_read.as_deref_mut().map(|on_read| {
                any_request(move |position, req| {
                    if let Some(&index) = caller_index.get(position) {
                        on_read(index, req);
                    }
                })
            });
            self.inner
                .submit_reads_multi(
                    &mut group,
                    in_caller_order
                        .as_mut()
                        .map(|f| f as &mut dyn FnMut(usize, &BlockRead<'_>)),
                )
                .err()
                .map(|(at, e)| {
                    let caller_index = submittable.get(at).map_or(usize::MAX, |(i, _)| *i);
                    (caller_index, crate::io::Error::from(e))
                })
        };

        let mut serial_failure: Option<(usize, crate::io::Error)> = None;
        for (idx, req) in &mut serial {
            // A partly filled destination owns the block's first `filled`
            // bytes already; complete it from `offset + filled`.
            let Some(offset) = req.offset.checked_add(req.buf.filled() as u64) else {
                serial_failure = Some((
                    *idx,
                    crate::io::Error::from(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "read_blocks_batched: request offset overflows",
                    )),
                ));
                break;
            };
            let file = req.file;
            match req
                .buf
                .fill_unfilled(|target| file.read_at_buf(target, offset))
            {
                Ok(()) => {
                    if !req.buf.is_full() {
                        serial_failure = Some((
                            *idx,
                            crate::io::Error::from(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "read_blocks_batched: short read on a fixed-size block",
                            )),
                        ));
                        break;
                    }
                    if let Some(on_read) = on_read.as_deref_mut() {
                        on_read(*idx, req);
                    }
                }
                Err(e) => {
                    serial_failure = Some((*idx, e));
                    break;
                }
            }
        }

        match (ring_failure, serial_failure) {
            (Some((ring_at, ring_err)), Some((serial_at, serial_err))) => {
                if serial_at < ring_at {
                    Err(serial_err)
                } else {
                    Err(ring_err)
                }
            }
            (Some((_, e)), None) | (None, Some((_, e))) => Err(e),
            (None, None) => Ok(()),
        }
    }
}

impl Clone for IoUringFs {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for IoUringFs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IoUringFs").finish_non_exhaustive()
    }
}

/// The callback [`Fs::read_blocks_batched_each`] hands each finished request to.
type OnRead<'f> = &'f mut dyn for<'a, 'b> FnMut(usize, &'a BlockRead<'b>);

/// Fills `req` from `fd`, its descriptor, on this thread.
fn read_lone(
    fd: i32,
    req: &mut BlockRead<'_>,
    on_read: Option<OnRead<'_>>,
) -> crate::io::Result<()> {
    // A partly filled destination owns the block's first `filled` bytes
    // already; complete it from `offset + filled`.
    let Some(offset) = req.offset.checked_add(req.buf.filled() as u64) else {
        return Err(crate::io::Error::from(io::Error::new(
            io::ErrorKind::InvalidInput,
            "read_blocks_batched: request offset overflows",
        )));
    };
    let want = pread_exact(fd, req.buf.unfilled_uninit_mut(), offset)?;
    // SAFETY: `pread_exact` wrote all `want` bytes after those filled.
    #[expect(unsafe_code, reason = "see safety")]
    unsafe {
        req.buf.assume_written(want);
    }
    if let Some(on_read) = on_read {
        on_read(0, req);
    }
    Ok(())
}

/// Fills `dst` from `fd` at `offset` with `pread`, which only writes it, so
/// memory never written needs no zeroing first; returns `dst.len()`.
///
/// # Errors
///
/// The read's error; a file ending before `dst` is full is
/// [`io::ErrorKind::UnexpectedEof`], an offset past the largest a file can
/// have [`io::ErrorKind::InvalidInput`].
fn pread_exact(
    fd: i32,
    dst: &mut [core::mem::MaybeUninit<u8>],
    offset: u64,
) -> crate::io::Result<usize> {
    let mut done = 0;
    while let Some(rest) = dst.get_mut(done..).filter(|rest| !rest.is_empty()) {
        let at = u64::try_from(done)
            .ok()
            .and_then(|done| offset.checked_add(done))
            .and_then(|at| libc::off_t::try_from(at).ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "read offset past the last file offset",
                )
            })?;
        // SAFETY: the kernel writes at most `rest.len()` bytes of `rest`,
        // memory the caller holds for the call, and reads none of it.
        #[expect(unsafe_code, reason = "see safety")]
        let n = unsafe { libc::pread(fd, rest.as_mut_ptr().cast(), rest.len(), at) };
        match usize::try_from(n) {
            Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
            Ok(n) => done += n,
            Err(_) => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error.into());
                }
            }
        }
    }
    Ok(done)
}

/// Makes `f` a callback over requests of any lifetime, which a closure only
/// infers when its signature is fixed from outside.
fn any_request<F: FnMut(usize, &BlockRead<'_>)>(f: F) -> F {
    f
}

/// Requests paired with their position in the caller's batch. The position is
/// what decides which failure a split batch reports: the contract is the FIRST
/// failing block in the caller's order, which survives the split only if each
/// group remembers where its requests came from.
type IndexedReads<'a, 'b> = Vec<(usize, &'a mut BlockRead<'b>)>;

// ---------------------------------------------------------------------------
// Fs for IoUringFs
// ---------------------------------------------------------------------------

impl Fs for IoUringFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn FsFile>> {
        let mut builder = OpenOptions::new();
        builder
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .create_new(opts.create_new)
            .truncate(opts.truncate)
            .append(opts.append);

        // Gate matches the `mod open_flags;` declaration in `fs/mod.rs`
        // — the submodule only exists when `feature = "std"` is on.
        // The whole `io_uring_fs` module is already gated by
        // `cfg(io_uring_fs)`, and
        // the `io-uring` feature transitively enables `std` (see the
        // feature declaration in Cargo.toml), so this extra
        // `feature = "std"` predicate is logically redundant here.
        // Kept for consistency with `StdFs::open` and to make the
        // dependency on open_flags' gate explicit at the call site,
        // so feature-gate audits don't have to chase the implication
        // through Cargo.toml.
        #[cfg(feature = "std")]
        super::open_flags::apply_open_flags(&mut builder, opts);

        let file = builder.open(path)?;
        #[cfg(feature = "std")]
        super::open_flags::finish_open(&file, opts)?;

        // When opened in append mode, io_uring writes use an explicit offset
        // so the kernel's O_APPEND semantics don't apply. Initialize the
        // cursor to EOF so that Write trait calls append correctly.
        // Note: concurrent appends from separate handles are NOT atomic
        // (unlike O_APPEND). This is acceptable — lsm-tree uses single-
        // writer-per-file for SSTs, journals, and blob files.
        let cursor = if opts.append {
            file.metadata()?.len()
        } else {
            0
        };

        Ok(Box::new(IoUringFile {
            file,
            cursor: AtomicU64::new(cursor),
            is_append: opts.append,
            ring: Arc::clone(&self.inner),
        }))
    }

    fn read_blocks_batched(&self, reqs: &mut [BlockRead<'_>]) -> crate::io::Result<()> {
        self.read_blocks(reqs, None)
    }

    fn read_blocks_batched_each(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: &mut dyn FnMut(usize, &BlockRead<'_>),
    ) -> crate::io::Result<()> {
        self.read_blocks(reqs, Some(on_read))
    }

    fn read_queue(&self) -> Box<dyn ReadQueue + '_> {
        Box::new(UringReadQueue::new(&self.inner))
    }

    // The ring reads every request with a descriptor by that descriptor; one
    // without is read serially through `read_at`.
    fn reads_by_descriptor(&self) -> bool {
        true
    }

    // What the ring's pre-pass refuses before submitting any of the group
    // (see `submit_reads_multi`).
    fn check_descriptor_batch(&self, reqs: &[BlockRead<'_>]) -> crate::io::Result<()> {
        for req in reqs {
            if req.buf.capacity() == 0 || req.file.backing_fd().is_none() {
                continue;
            }
            if i32::try_from(req.buf.capacity() - req.buf.filled()).is_err() {
                return Err(crate::io::Error::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "buffer exceeds i32::MAX",
                )));
            }
            if req.offset.checked_add(req.buf.filled() as u64).is_none() {
                return Err(crate::io::Error::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "submit_reads_multi: request offset overflows",
                )));
            }
        }
        Ok(())
    }

    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        std::fs::create_dir_all(path).map_err(crate::io::Error::from)
    }

    fn create_dir(&self, path: &Path) -> crate::io::Result<()> {
        std::fs::create_dir(path).map_err(crate::io::Error::from)
    }

    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<FsDirEntry>> {
        // Delegate to std::fs — directory listing doesn't benefit from io_uring.
        std::fs::read_dir(path)?
            .map(|res| {
                let entry = res?;
                let file_type = entry.file_type()?;
                let file_name_os = entry.file_name();
                let file_name = file_name_os.into_string().map_err(|os| {
                    #[expect(
                        clippy::unnecessary_debug_formatting,
                        reason = "OsString has no Display impl — Debug is required"
                    )]
                    let msg = format!("non-UTF-8 filename in directory {}: {os:?}", path.display());
                    io::Error::new(io::ErrorKind::InvalidData, msg)
                })?;
                Ok(FsDirEntry {
                    path: entry.path(),
                    file_name,
                    is_dir: file_type.is_dir(),
                })
            })
            .collect::<io::Result<Vec<_>>>()
            .map_err(crate::io::Error::from)
    }

    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        std::fs::remove_file(path).map_err(crate::io::Error::from)
    }

    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        std::fs::remove_dir_all(path).map_err(crate::io::Error::from)
    }

    fn remove_dir(&self, path: &Path) -> crate::io::Result<()> {
        std::fs::remove_dir(path).map_err(crate::io::Error::from)
    }

    fn same_file(&self, a: &Path, b: &Path) -> crate::io::Result<bool> {
        // Filesystem OBJECT identity (device + inode), same contract as the
        // std backend: bind-mount aliases keep distinct canonical spellings
        // over one underlying file, and a probe failure propagates instead
        // of guessing "distinct".
        use std::os::unix::fs::MetadataExt;
        let ma = std::fs::metadata(a).map_err(crate::io::Error::from)?;
        let mb = std::fs::metadata(b).map_err(crate::io::Error::from)?;
        Ok(ma.dev() == mb.dev() && ma.ino() == mb.ino())
    }

    fn read_link(&self, path: &Path) -> crate::io::Result<Option<crate::path::PathBuf>> {
        super::StdFs.read_link(path)
    }

    fn symlink(&self, target: &Path, link: &Path, to_dir: bool) -> crate::io::Result<()> {
        super::StdFs.symlink(target, link, to_dir)
    }

    fn symlink_to_dir(&self, path: &Path) -> crate::io::Result<bool> {
        super::StdFs.symlink_to_dir(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        std::fs::rename(from, to).map_err(crate::io::Error::from)
    }

    fn metadata(&self, path: &Path) -> crate::io::Result<FsMetadata> {
        let m = std::fs::metadata(path)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn available_space(&self, path: &Path) -> crate::io::Result<u64> {
        // Free-space probe is a cold-path stat; delegate to the shared statvfs
        // helper (this backend is Linux-only, so libc statvfs is available).
        super::statvfs_available_space(path).map_err(crate::io::Error::from)
    }

    fn allocated_size(&self, path: &Path) -> crate::io::Result<Option<u64>> {
        // Cold-path stat; the shared unix helper reads `st_blocks` (this backend
        // is Linux-only, so `std::fs::metadata` is available). A stat failure
        // propagates as `Err` rather than being masked as `None` (unpunched).
        Ok(super::unix_allocated_size(path)?)
    }

    fn extent_is_hole(
        &self,
        path: &Path,
        offset: u64,
        len: u64,
    ) -> crate::io::Result<Option<bool>> {
        // Same kernel, same probe as the std backend: this is a cold diagnostic
        // path, so it delegates there rather than going through the ring.
        Fs::extent_is_hole(&super::StdFs, path, offset, len)
    }

    fn extent_contains_hole(
        &self,
        path: &Path,
        offset: u64,
        len: u64,
    ) -> crate::io::Result<Option<bool>> {
        // Same delegation as `extent_is_hole`, for the same reason.
        Fs::extent_contains_hole(&super::StdFs, path, offset, len)
    }

    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        let dir = File::open(path)?;
        if !dir.metadata()?.is_dir() {
            return Err(crate::io::Error::new(
                crate::io::ErrorKind::InvalidInput,
                "sync_directory: path is not a directory",
            ));
        }
        dir.sync_all().map_err(crate::io::Error::from)
    }

    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        path.try_exists().map_err(crate::io::Error::from)
    }

    fn hard_link(&self, src: &Path, dst: &Path) -> crate::io::Result<()> {
        // Hard linking is a metadata-only operation; io_uring offers no
        // throughput benefit, so delegate to [`StdFs`] for the EXDEV
        // fallback logic.
        super::StdFs.hard_link(src, dst)
    }

    fn backend_id(&self) -> Option<u64> {
        // `IoUringFs` resolves paths through the host kernel just like
        // `StdFs`, so it MUST report the same namespace ID — otherwise
        // the checkpoint driver would needlessly stream-copy between the
        // two backends.
        super::StdFs.backend_id()
    }

    fn volume_id(&self, path: &Path) -> Option<u64> {
        // Same kernel mount as `StdFs` — free space is a property of the mount,
        // not the I/O submission path.
        super::StdFs.volume_id(path)
    }

    // The file system's capabilities, and the operations they gate, are the
    // mount's and go through the same syscalls as on `StdFs`; the ring has no
    // opcode for any of them. `sync_directory_with`, `sync_device` and the
    // files' `sync_*_with` keep the trait defaults: on Linux every `SyncMode`
    // is the same `fsync` / `fdatasync`, which these handles issue on the ring.

    fn capabilities(&self, path: &Path) -> super::FsCapabilities {
        super::StdFs.capabilities(path)
    }

    fn try_disable_cow(&self, path: &Path) -> crate::io::Result<()> {
        super::StdFs.try_disable_cow(path)
    }

    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> crate::io::Result<()> {
        super::StdFs.punch_hole(path, offset, len)
    }

    fn reflink_file(&self, src: &Path, dst: &Path) -> crate::io::Result<()> {
        super::StdFs.reflink_file(src, dst)
    }

    fn hard_link_count(&self, path: &Path) -> crate::io::Result<u64> {
        super::StdFs.hard_link_count(path)
    }
}

// ---------------------------------------------------------------------------
// IoUringFile
// ---------------------------------------------------------------------------

/// File handle whose batched reads go through the shared `io_uring` ring.
///
/// Wraps a [`std::fs::File`], which serves every operation that comes alone:
/// a single read, a write, a sync. Only a read of several regions has anything
/// for the ring to batch.
pub(crate) struct IoUringFile {
    /// Underlying [`std::fs::File`] — owns the fd, used for metadata, `set_len`, lock.
    file: File,

    /// Tracked cursor position for [`Read`]/[`Write`]/[`Seek`] impls.
    /// Only accessed via `get_mut()` (those traits take `&mut self`) or
    /// not at all ([`FsFile::read_at`] uses an explicit offset).
    /// `AtomicU64` could be replaced with plain `u64` (which is already
    /// `Sync`), but is kept for consistency with the interior-mutability
    /// pattern and to allow potential future shared cursor access.
    cursor: AtomicU64,

    /// Whether the file was opened in append mode. When true, writes
    /// always go to current EOF regardless of cursor/seek position.
    is_append: bool,

    /// Shared reference to the ring thread.
    ring: Arc<RingThread>,
}

impl FsFile for IoUringFile {
    fn sync_all(&self) -> crate::io::Result<()> {
        FsFile::sync_all(&self.file)
    }

    fn sync_data(&self) -> crate::io::Result<()> {
        FsFile::sync_data(&self.file)
    }

    fn metadata(&self) -> crate::io::Result<FsMetadata> {
        let m = self.file.metadata()?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn hard_link_count(&self) -> crate::io::Result<u64> {
        use std::os::unix::fs::MetadataExt as _;
        Ok(self.file.metadata()?.nlink())
    }

    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.file.set_len(size).map_err(crate::io::Error::from)
    }

    // A lone operation stays on the caller's thread: the ring has nothing to
    // batch it with, and a trip to its thread and back costs far more than the
    // syscall.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        FsFile::read_at(&self.file, buf, offset)
    }

    // Batched read: submit every region to the ring at once (one coalesced
    // submission, reads overlap in flight) instead of the trait's serial
    // read_at loop. This is the io_uring win for multi-block reads.
    fn read_many(&self, regions: &mut [(u64, &mut [u8])]) -> crate::io::Result<()> {
        if regions.len() < 2 {
            return FsFile::read_many(&self.file, regions);
        }
        self.ring
            .submit_reads(self.file.as_raw_fd(), regions)
            .map_err(crate::io::Error::from)
    }

    fn read_at_buf(&self, buf: &mut super::ReadBuf<'_>, offset: u64) -> crate::io::Result<()> {
        // SAFETY: `read_at` is `File::read_at`, which only has the kernel write
        // the slice and returns how much of it was.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            buf.fill_with(|bytes| FsFile::read_at(self, bytes, offset))
        }
    }

    fn read_many_buf(&self, regions: &mut [(u64, super::ReadBuf<'_>)]) -> crate::io::Result<()> {
        if regions.len() < 2 {
            return FsFile::read_many_buf(&self.file, regions);
        }
        self.ring
            .submit_reads(self.file.as_raw_fd(), regions)
            .map_err(crate::io::Error::from)?;
        // SAFETY: `submit_reads` fails unless every region was filled, and
        // only the kernel wrote them.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            super::read_buf::assume_regions_filled(regions);
        }
        Ok(())
    }

    // Exposes the fd so `IoUringFs::read_blocks_batched` can submit reads across
    // many files (SSTs) to this handle's shared ring in one batch.
    fn backing_fd(&self) -> Option<i32> {
        Some(self.file.as_raw_fd())
    }

    fn start_writeback(&self, offset: u64, len: u64) -> crate::io::Result<()> {
        FsFile::start_writeback(&self.file, offset, len)
    }

    fn set_write_lifetime(&self, lifetime: super::WriteLifetime) -> crate::io::Result<()> {
        FsFile::set_write_lifetime(&self.file, lifetime)
    }

    fn hint(&self, hint: super::FileHint) -> crate::io::Result<()> {
        // `posix_fadvise` on the descriptor the ring reads through.
        FsFile::hint(&self.file, hint)
    }

    fn lock_exclusive(&self) -> crate::io::Result<()> {
        // Delegate to the platform-specific FsFile impl for std::fs::File.
        FsFile::lock_exclusive(&self.file)
    }

    fn try_lock_exclusive(&self) -> crate::io::Result<bool> {
        // io_uring wraps a real on-disk file, so it needs the genuine
        // non-blocking OS lock, not the trivial-acquire default.
        FsFile::try_lock_exclusive(&self.file)
    }
}

impl Read for IoUringFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let cursor = self.cursor.get_mut();
        let n = std::os::unix::fs::FileExt::read_at(&self.file, buf, *cursor)?;
        *cursor += n as u64;
        Ok(n)
    }
}

impl Write for IoUringFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        let cursor = self.cursor.get_mut();
        // In append mode, write at current EOF to match O_APPEND semantics.
        // fstat per write is ~100ns — negligible for journal/SST append patterns.
        // Cursor-based tracking would break if seek() is called before write().
        if self.is_append {
            *cursor = self.file.metadata()?.len();
        }
        let n = std::os::unix::fs::FileExt::write_at(&self.file, buf, *cursor)?;
        *cursor += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        // No userspace buffer to flush: the data is already in the kernel. Use
        // sync_data()/sync_all() for durable persistence.
        Ok(())
    }
}

impl Seek for IoUringFile {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let cursor = self.cursor.get_mut();
        let new_pos = match pos {
            SeekFrom::Start(n) => n,
            SeekFrom::Current(n) => if n >= 0 {
                cursor.checked_add(n.unsigned_abs())
            } else {
                cursor.checked_sub(n.unsigned_abs())
            }
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "seek position out of range")
            })?,
            SeekFrom::End(n) => {
                let len = self.file.metadata()?.len();
                if n >= 0 {
                    len.checked_add(n.unsigned_abs())
                } else {
                    len.checked_sub(n.unsigned_abs())
                }
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "seek position out of range")
                })?
            }
        };
        // Note: new_pos may exceed i64::MAX (kernel loff_t range). This
        // matches std::fs::File::seek which also returns u64. The kernel
        // will reject out-of-range offsets at the actual I/O syscall.
        *cursor = new_pos;
        Ok(new_pos)
    }
}

// ---------------------------------------------------------------------------
// Ring thread internals
// ---------------------------------------------------------------------------

/// Newtype wrapper for sending a `*mut u8` across threads.
///
/// # Safety
///
/// The caller must ensure the pointed-to memory remains valid until the
/// `io_uring` operation completes. This is upheld because the submitting
/// thread blocks on an `mpsc::Receiver` and cannot drop the buffer until
/// the CQE is received.
struct UnsafeSendMutPtr(*mut u8);

// SAFETY: see struct-level docs. The raw pointer is guaranteed valid for the
// duration of the io_uring op because the caller blocks until the CQE is
// received.
#[expect(unsafe_code, reason = "marking raw-pointer wrapper as Send")]
unsafe impl Send for UnsafeSendMutPtr {}

/// One read of a batch. Its completion is reported with its position in the
/// batch, so one channel serves the whole batch.
struct BatchRead {
    fd: i32,
    buf: UnsafeSendMutPtr,
    len: u32,
    offset: u64,
}

/// What the ring thread is sent: a whole batch of reads handed over in one
/// message, so the caller takes the submission lock and sends once per batch
/// rather than once per read.
enum Submission {
    Batch {
        reads: Vec<BatchRead>,
        /// Receives `(position, result)` for each read, in completion order.
        /// Bounded by the batch size, so the ring thread never blocks on it.
        done: mpsc::SyncSender<(usize, i32)>,
    },
    /// Reads of a [`UringReadQueue`], each reporting to the queue's sink with
    /// its position in the queue.
    Queued {
        reads: Vec<BatchRead>,
        /// The queue position of the first read; the others follow it.
        first: usize,
        sink: Arc<QueueSink>,
    },
}

/// Why [`RingThread::try_send`] sent nothing.
enum TrySend {
    /// The submission channel has no room; the submission was dropped
    /// unsent, so no read it described reached the kernel.
    Full,
    /// The ring thread is gone.
    Failed(io::Error),
}

/// Where the ring thread reports a [`UringReadQueue`]'s reads.
struct QueueSink {
    /// Unbounded: a queue has reads of several submissions in flight at
    /// once, and the ring thread must never block on a completion.
    done: mpsc::Sender<(usize, i32)>,
    /// Woken after each completion is sent, once the queue was given one.
    wake: Option<Arc<dyn crate::fs::ReadWake>>,
}

impl QueueSink {
    /// Hands `result` for the read at `position` to the queue, then wakes
    /// whoever waits on it: the completion is on the channel by the time the
    /// wake lands.
    fn deliver(&self, position: usize, result: i32) {
        if self.done.send((position, result)).is_ok()
            && let Some(wake) = &self.wake
        {
            wake.wake();
        }
    }
}

/// Where the ring thread delivers one operation's result.
enum Completion {
    InBatch {
        done: mpsc::SyncSender<(usize, i32)>,
        position: usize,
    },
    InQueue {
        sink: Arc<QueueSink>,
        position: usize,
    },
}

impl Completion {
    /// Hands `result` to the waiting caller. A caller that has gone away
    /// (only possible once it saw the ring fail) needs nothing more.
    fn deliver(self, result: i32) {
        match self {
            Self::InBatch { done, position } => done.send((position, result)).ok(),
            Self::InQueue { sink, position } => {
                sink.deliver(position, result);
                None
            }
        };
    }
}

/// Per-ring counts of what callers spend before the kernel sees a request:
/// completion channels created and submission-lock acquisitions. Test-only.
#[cfg(test)]
#[derive(Default)]
struct SubmitCounts {
    channels: core::sync::atomic::AtomicUsize,
    locks: core::sync::atomic::AtomicUsize,
}

/// Dedicated thread that owns the `io_uring` ring.
///
/// Operations are submitted via bounded `mpsc::SyncSender` (sized to match
/// the ring). A single operation's result comes back on its own channel; a
/// batch's results share one channel, tagged with each read's position.
struct RingThread {
    tx: Mutex<Option<mpsc::SyncSender<Submission>>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
    /// Whether the kernel refused a read that must not wait (`RWF_NOWAIT`)
    /// through this backend; once it has, every read goes to the ring.
    nowait_refused: core::sync::atomic::AtomicBool,
    #[cfg(test)]
    counts: SubmitCounts,
}

impl RingThread {
    fn spawn(sq_entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(sq_entries)?;
        // Bound the submission channel to ring capacity — provides
        // natural backpressure when callers outpace the I/O thread.
        let (tx, rx) = mpsc::sync_channel(sq_entries as usize);

        // If event_loop panics after submitting SQEs, those SQEs still
        // reference caller buffers. catch_unwind + abort is used, and
        // pending is wrapped in ManuallyDrop inside event_loop so that
        // SyncSenders are NOT dropped during unwind — callers stay blocked
        // until abort kills the process.
        let handle = thread::Builder::new()
            .name("lsm-io-uring".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    Self::event_loop(ring, rx);
                }));
                if result.is_err() {
                    log::error!("io_uring ring thread panicked; aborting to avoid UB");
                    std::process::abort();
                }
            })?;

        Ok(Self {
            tx: Mutex::new(Some(tx)),
            handle: Mutex::new(Some(handle)),
            nowait_refused: core::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            counts: SubmitCounts::default(),
        })
    }

    /// Main event loop for the I/O thread.
    ///
    /// 1. Block on `recv()` when idle (no in-flight ops).
    /// 2. Batch additional ops via `try_recv()`.
    /// 3. Submit to kernel and wait for at least one completion.
    /// 4. Dispatch CQE results to callers.
    // Coverage: error paths (EINTR, fatal ring failure, SQ overflow, channel
    // disconnect with pending ops) require kernel fault injection to exercise.
    // The happy path IS covered by all IoUringFs tests.
    #[cfg_attr(coverage_nightly, coverage(off))]
    #[expect(
        clippy::needless_pass_by_value,
        reason = "rx is moved into the spawned thread — must be owned"
    )]
    fn event_loop(mut ring: IoUring, rx: mpsc::Receiver<Submission>) {
        // ManuallyDrop ensures that on panic, pending's SyncSenders are NOT
        // dropped during stack unwinding. This keeps callers blocked on their
        // result channels until catch_unwind + abort kills the process,
        // preventing them from dropping buffers that the kernel may still access.
        let mut pending = std::mem::ManuallyDrop::new(HashMap::<u64, Completion>::default());
        let mut next_id: u64 = 0;

        loop {
            // Phase 1: collect operations.
            let first = if pending.is_empty() {
                match rx.recv() {
                    Ok(op) => Some(op),
                    Err(mpsc::RecvError) => break,
                }
            } else {
                match rx.try_recv() {
                    Ok(op) => Some(op),
                    Err(mpsc::TryRecvError::Empty) => None,
                    Err(mpsc::TryRecvError::Disconnected) => {
                        if pending.is_empty() {
                            break;
                        }
                        None
                    }
                }
            };

            if let Some(submission) = first {
                Self::accept(&mut ring, &mut pending, &mut next_id, submission);

                // Batch: drain as many additional submissions as available.
                while let Ok(submission) = rx.try_recv() {
                    Self::accept(&mut ring, &mut pending, &mut next_id, submission);
                }
            }

            if pending.is_empty() {
                continue;
            }

            // Phase 2: submit to kernel, retry on EINTR.
            // Errno constants are inlined to avoid a libc dependency
            // (consistent with StdFs which uses raw FFI for flock).
            loop {
                match ring.submit_and_wait(1) {
                    Ok(_) => break,
                    Err(ref e) if e.raw_os_error() == Some(4 /* EINTR */) => {}
                    Err(e) => {
                        // Fatal ring error. Previously submitted SQEs may
                        // still be in-flight referencing caller buffers.
                        // Draining `pending` would unblock callers and let
                        // them drop those buffers — UB if the kernel still
                        // touches them. Abort to avoid unsoundness.
                        log::error!(
                            "io_uring submit_and_wait failed: {e}; aborting process to avoid UB"
                        );
                        std::process::abort();
                    }
                }
            }

            // Phase 3: harvest completions.
            for cqe in ring.completion() {
                if let Some(completion) = pending.remove(&cqe.user_data()) {
                    completion.deliver(cqe.result());
                }
            }
        }

        // Normal exit (channel closed) — no in-flight SQEs remain, safe to
        // drop pending's SyncSenders. Without this, ManuallyDrop would leak.
        #[expect(unsafe_code, reason = "ManuallyDrop cleanup on normal exit path")]
        // SAFETY: we only reach here after the loop breaks (channel disconnected),
        // meaning no more SQEs can be submitted and all completions are harvested.
        unsafe {
            std::mem::ManuallyDrop::drop(&mut pending);
        }
    }

    /// Queues every operation of `submission`, each with where its result goes.
    fn accept(
        ring: &mut IoUring,
        pending: &mut HashMap<u64, Completion>,
        next_id: &mut u64,
        submission: Submission,
    ) {
        match submission {
            Submission::Batch { reads, done } => {
                for (position, read) in reads.into_iter().enumerate() {
                    let completion = Completion::InBatch {
                        done: done.clone(),
                        position,
                    };
                    Self::enqueue(ring, pending, next_id, &read, completion);
                }
            }
            Submission::Queued { reads, first, sink } => {
                for (index, read) in reads.into_iter().enumerate() {
                    let completion = Completion::InQueue {
                        sink: Arc::clone(&sink),
                        position: first + index,
                    };
                    Self::enqueue(ring, pending, next_id, &read, completion);
                }
            }
        }
    }

    /// Builds an SQE from `read` and pushes it onto the submission queue.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn enqueue(
        ring: &mut IoUring,
        pending: &mut HashMap<u64, Completion>,
        next_id: &mut u64,
        read: &BatchRead,
        completion: Completion,
    ) {
        let id = *next_id;
        *next_id = next_id.wrapping_add(1);

        let sqe = opcode::Read::new(types::Fd(read.fd), read.buf.0, read.len)
            .offset(read.offset)
            .build()
            .user_data(id);

        // SAFETY: SQE references memory that the calling thread keeps alive
        // (blocked on the result channel — see UnsafeSend safety contract).
        #[expect(unsafe_code, reason = "io_uring SQE push")]
        unsafe {
            while ring.submission().push(&sqe).is_err() {
                // SQ full — wait for at least one completion to free a slot.
                // Since the Fs API is synchronous, callers are already blocking;
                // backpressure here is natural, not an error.
                loop {
                    match ring.submit_and_wait(1) {
                        Ok(_) => break,
                        Err(ref e) if e.raw_os_error() == Some(4 /* EINTR */) => {}
                        Err(e) => {
                            // Fatal ring error — same as Phase 2 handler.
                            log::error!(
                                "io_uring submit_and_wait failed in SQ retry: {e}; aborting"
                            );
                            std::process::abort();
                        }
                    }
                }
                for cqe in ring.completion() {
                    if let Some(done) = pending.remove(&cqe.user_data()) {
                        done.deliver(cqe.result());
                    }
                }
            }
        }

        pending.insert(id, completion);
    }

    // -- Submission helpers --------------------------------------------------

    /// A completion channel for `bound` results.
    #[cfg_attr(
        not(test),
        expect(
            clippy::unused_self,
            reason = "self carries the test-only channel count"
        )
    )]
    fn channel<T>(&self, bound: usize) -> (mpsc::SyncSender<T>, mpsc::Receiver<T>) {
        #[cfg(test)]
        self.counts
            .channels
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        mpsc::sync_channel(bound)
    }

    /// Hands `submission` to the ring thread under one acquisition of the
    /// submission lock. On error nothing was queued: the submission is dropped
    /// with the failed send, so no read it describes ever reaches the kernel.
    fn send(&self, submission: Submission) -> io::Result<()> {
        #[cfg(test)]
        self.counts
            .locks
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        // Mutex guards Option<Sender> for clean shutdown (Drop sets to None).
        // Lock is held only for send() duration (~ns) — negligible vs I/O
        // latency (~µs). A lock-free channel would eliminate this but adds
        // an external dependency for no measurable gain.
        self.tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread shut down"))?
            .send(submission)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))
    }

    /// [`Self::send`] without waiting for room: a full submission channel
    /// sends nothing, so a caller that must not block keeps its reads for a
    /// later send. Nor does it wait for the submission lock, which a blocking
    /// sender holds while it waits for room: a lock held elsewhere counts as
    /// no room.
    fn try_send(&self, submission: Submission) -> Result<(), TrySend> {
        #[cfg(test)]
        self.counts
            .locks
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let guard = match self.tx.try_lock() {
            Ok(guard) => guard,
            Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return Err(TrySend::Full),
        };
        let sent = guard.as_ref().map(|tx| tx.try_send(submission));
        drop(guard);
        match sent {
            Some(Ok(())) => Ok(()),
            Some(Err(mpsc::TrySendError::Full(_))) => Err(TrySend::Full),
            Some(Err(mpsc::TrySendError::Disconnected(_))) => Err(TrySend::Failed(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring thread exited",
            ))),
            None => Err(TrySend::Failed(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "io_uring thread shut down",
            ))),
        }
    }

    /// Submits `reads` as one batch and waits for every one of them; see
    /// [`drain_batch`] for the verdict and `on_read`. `expected[i]` is the byte
    /// count `reads[i]` must return.
    fn submit_batch(
        &self,
        reads: Vec<BatchRead>,
        expected: &[usize],
        short_read: &'static str,
        on_read: impl FnMut(usize),
    ) -> Result<(), (usize, io::Error)> {
        if reads.is_empty() {
            return Ok(());
        }
        let (done, results) = self.channel(reads.len());
        // A failed send drops the batch with it, so nothing was submitted and
        // no buffer is in the kernel's hands; the failure belongs to the
        // batch's first read.
        self.send(Submission::Batch { reads, done })
            .map_err(|e| (0, e))?;
        drain_batch(expected, || results.recv().ok(), short_read, on_read)
    }

    /// Submits every read in `regions` to the ring BEFORE waiting on any of
    /// them, then waits for all completions. Because the event loop drains all
    /// queued ops into one `submit_and_wait`, the reads coalesce into far fewer
    /// `io_uring_enter` calls and overlap in flight (one batched submission
    /// instead of one blocking read per block).
    ///
    /// Fills each region completely; a short read on any region fails the whole
    /// batch (block reads are fixed-size, so the caller treats a failure as
    /// "could not batch-read" and falls back to per-block reads).
    fn submit_reads<R: super::read_buf::ReadRegion>(
        &self,
        fd: i32,
        regions: &mut [R],
    ) -> io::Result<()> {
        // Every read holds a raw pointer into the caller's buffer; `regions` is
        // borrowed for the whole call and the wait below covers every read, so
        // every buffer outlives the kernel's access (the UnsafeSend safety
        // contract).
        // Phase 1: validate EVERY region length BEFORE submitting anything. A
        // length over the i32 SQE cap must be rejected here: once the batch is
        // sent the kernel may be writing its buffers, and the batch goes out
        // whole, so there is no later point at which to refuse one read of it.
        // (SQE length is u32, CQE result i32 — block reads are KBs, never near
        // the cap, so this only fires on misuse of the general API.)
        let mut lens: Vec<u32> = Vec::with_capacity(regions.len());
        for region in regions.iter_mut() {
            // SAFETY: the bytes go only to the kernel, which only writes them.
            #[expect(unsafe_code, reason = "see safety")]
            let (_, buf) = unsafe { region.target() };
            let len = if buf.is_empty() {
                0
            } else {
                i32::try_from(buf.len())
                    .map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX")
                    })?
                    .unsigned_abs()
            };
            lens.push(len);
        }

        // Phase 2: the whole batch in one message, one completion channel for
        // it, and a wait for every read (see `drain_batch`).
        let mut reads = Vec::with_capacity(regions.len());
        let mut expected = Vec::with_capacity(regions.len());
        for (region, &len) in regions.iter_mut().zip(&lens) {
            // SAFETY: as above.
            #[expect(unsafe_code, reason = "see safety")]
            let (offset, buf) = unsafe { region.target() };
            if buf.is_empty() {
                continue;
            }
            reads.push(BatchRead {
                fd,
                buf: UnsafeSendMutPtr(buf.as_mut_ptr()),
                len,
                offset,
            });
            expected.push(buf.len());
        }
        self.submit_batch(
            reads,
            &expected,
            "io_uring read_many: short read on a fixed-size block region",
            |_| {},
        )
        .map_err(|(_, e)| e)
    }

    /// Like [`Self::submit_reads`], but each request carries its OWN fd, so
    /// reads from DIFFERENT files (SSTs, and on a multi-device layout different
    /// devices) coalesce into one submission to the shared ring. Callers
    /// guarantee every request has an fd (`read_blocks_batched` checks first).
    /// Generic over how the caller holds each request — `BlockRead` directly
    /// when the whole batch is submittable, `&mut BlockRead` when only a subset
    /// is and the rest are read serially. Monomorphised, so the common
    /// whole-batch path pays nothing for the subset one and neither has to
    /// gather its requests into a fresh allocation.
    ///
    /// With `on_read`, each request is counted as filled and handed over the
    /// moment its read completes, while the others may still be in flight;
    /// without it, the requests are counted only once every read succeeded, so
    /// a failed batch leaves none of them counted.
    fn submit_reads_multi<'r, T: core::borrow::BorrowMut<BlockRead<'r>>>(
        &self,
        reqs: &mut [T],
        on_read: Option<OnRead<'_>>,
    ) -> Result<(), (usize, io::Error)> {
        // Pre-pass: resolve every request's fd and validate its length BEFORE
        // submitting any op (same un-drained-in-flight hazard as submit_reads: a
        // missing fd or over-cap length returning via `?` mid-loop would strand
        // earlier sends with the kernel still writing their buffers). `None`
        // entries mark empty-buffer requests the send loop skips.
        let mut metas: Vec<Option<(i32, u32, u64)>> = Vec::with_capacity(reqs.len());
        for (at, req) in reqs.iter().map(core::borrow::Borrow::borrow).enumerate() {
            if req.buf.capacity() == 0 {
                metas.push(None);
                continue;
            }
            let fd = req.file.backing_fd().ok_or_else(|| {
                (
                    at,
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "submit_reads_multi: request without fd",
                    ),
                )
            })?;
            // From the UNFILLED region, which is where the pointer below points
            // too. Taking it from the capacity would describe a span starting at
            // the fill point and running a whole buffer's length, so a request
            // that arrived partly filled would have the kernel write past the
            // end of the allocation.
            let len: u32 = i32::try_from(req.buf.capacity() - req.buf.filled())
                .map_err(|_| {
                    (
                        at,
                        io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX"),
                    )
                })?
                .unsigned_abs();
            // A partly filled destination owns the block's first `filled` bytes
            // already, so the suffix the SQE describes lives `filled` bytes into
            // the block on disk. Submitting the original offset would duplicate
            // the block's beginning into the suffix instead.
            let offset = req
                .offset
                .checked_add(req.buf.filled() as u64)
                .ok_or_else(|| {
                    (
                        at,
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "submit_reads_multi: request offset overflows",
                        ),
                    )
                })?;
            metas.push(Some((fd, len, offset)));
        }

        // An empty request is complete without a read and never reaches the
        // ring; it is handed over as every other backend hands it, before any
        // buffer is given to the kernel.
        let mut on_read = on_read;
        if let Some(on_read) = on_read.as_deref_mut() {
            for (at, (req, meta)) in reqs
                .iter()
                .map(core::borrow::Borrow::borrow)
                .zip(&metas)
                .enumerate()
            {
                if meta.is_none() {
                    on_read(at, req);
                }
            }
        }

        // One read per non-empty request, in request order, with the position
        // each occupies in `reqs`: a failure is attributed to the request that
        // failed, not to the batch it travelled in.
        let mut reads = Vec::with_capacity(reqs.len());
        let mut expected = Vec::with_capacity(reqs.len());
        let mut ats = Vec::with_capacity(reqs.len());
        for (at, (req, meta)) in reqs
            .iter_mut()
            .map(core::borrow::BorrowMut::borrow_mut)
            .zip(&metas)
            .enumerate()
        {
            let Some((fd, len, offset)) = *meta else {
                continue;
            };
            expected.push(req.buf.capacity() - req.buf.filled());
            // The kernel only writes the span; it is counted filled only once
            // its completion reports it whole.
            let dst = req.buf.unfilled_uninit_mut().as_mut_ptr().cast::<u8>();
            reads.push(BatchRead {
                fd,
                buf: UnsafeSendMutPtr(dst),
                len,
                offset,
            });
            ats.push(at);
        }

        const SHORT_READ: &str = "io_uring read_blocks_batched: short read on a fixed-size block";
        let blame =
            |(position, e): (usize, io::Error)| (ats.get(position).copied().unwrap_or_default(), e);

        // The kernel filled a request exactly when its completion matched the
        // length it asked for; counting it covers the same unfilled span the
        // submission described.
        let count_filled = |req: &mut BlockRead<'r>| {
            let remaining = req.buf.capacity() - req.buf.filled();
            // SAFETY: called only for a request whose completion matched the
            // length it asked for, or for an empty one.
            #[expect(unsafe_code, reason = "see safety")]
            unsafe {
                req.buf.assume_written(remaining);
            }
        };

        if let Some(on_read) = on_read {
            return self
                .submit_batch(reads, &expected, SHORT_READ, |position| {
                    // Only this request's own buffer is touched: the others
                    // may still have the kernel writing into theirs.
                    if let Some(&at) = ats.get(position)
                        && let Some(req) = reqs.get_mut(at)
                    {
                        let req = req.borrow_mut();
                        count_filled(req);
                        on_read(at, req);
                    }
                })
                .map_err(blame);
        }
        self.submit_batch(reads, &expected, SHORT_READ, |_| {})
            .map_err(blame)?;
        for req in reqs.iter_mut().map(core::borrow::BorrowMut::borrow_mut) {
            count_filled(req);
        }
        Ok(())
    }
}

/// The [`ReadQueue`] of an [`IoUringFs`]: the reads submitted since the last
/// wait go to the ring in one message when the queue is waited on, and each
/// is handed back the moment its completion arrives, while reads submitted
/// earlier or later may still be in flight.
///
/// The queue owns every buffer on the ring until its completion is received,
/// and dropping it receives the completions still owed first, so no buffer is
/// freed while the kernel may still write into it. That receive cannot hang:
/// the queue borrows the backend, which keeps the ring thread alive, and the
/// ring thread delivers every completion it was handed.
struct UringReadQueue<'r> {
    ring: &'r RingThread,
    /// Where the reads it sends report; replaced when the queue is given a
    /// wake, which reads sent earlier do not ring.
    sink: Arc<QueueSink>,
    done_rx: mpsc::Receiver<(usize, i32)>,
    /// Not yet on the ring: submitted since the last wait, or kept by a wait
    /// for none that found no room or no lock to send them.
    submitted: Vec<QueuedRead>,
    /// The reads sent to the ring, by the position their completion reports
    /// less `base`; `None` once handed back. Handed-back reads at either end
    /// are dropped, so a long-lived queue keeps the span of its reads in
    /// flight, not one slot per read it ever sent.
    sent: std::collections::VecDeque<Option<QueuedRead>>,
    /// The position of `sent`'s first slot.
    base: usize,
    /// Reads on the ring whose completion has not arrived.
    on_ring: usize,
    /// Reads finished without the ring (empty, read serially, or refused),
    /// waiting to be handed back.
    finished: Vec<ReadDone>,
    /// Reads of a file with no descriptor the ring can take: read serially
    /// on the calling thread, so only by a wait for at least one read, never
    /// by a look at what is ready.
    serial: Vec<QueuedRead>,
    /// The smallest read whose cached bytes are read on the calling thread.
    cached_min: usize,
}

impl<'r> UringReadQueue<'r> {
    fn new(ring: &'r RingThread) -> Self {
        let (done, done_rx) = mpsc::channel();
        Self {
            ring,
            sink: Arc::new(QueueSink { done, wake: None }),
            done_rx,
            submitted: Vec::new(),
            sent: std::collections::VecDeque::new(),
            base: 0,
            on_ring: 0,
            finished: Vec::new(),
            serial: Vec::new(),
            cached_min: CACHED_MIN,
        }
    }

    /// A queue that reads the cached bytes of reads from `cached_min` up on
    /// the calling thread; `usize::MAX` sends every read to the ring.
    #[cfg(test)]
    fn with_cached_min(ring: &'r RingThread, cached_min: usize) -> Self {
        let mut queue = Self::new(ring);
        queue.cached_min = cached_min;
        queue
    }

    /// Reads the reads held for a serial read, each into its own buffer.
    fn read_serial(&mut self) {
        for mut read in core::mem::take(&mut self.serial) {
            let Some(offset) = resume_offset(&read) else {
                self.finished.push(done(read, Err(past_last_offset())));
                continue;
            };
            let file = &read.file;
            let result = read
                .buf
                .fill_with(|buf| buf.fill_unfilled(|target| file.read_at_buf(target, offset)));
            let result = match result {
                Ok(()) if read.buf.is_full() => Ok(()),
                Ok(()) => Err(short_read()),
                Err(error) => Err(error),
            };
            self.finished.push(done(read, result));
        }
    }

    /// Sends every read submitted since the last wait to the ring in one
    /// message. A read the ring cannot take is finished here or held: an
    /// empty one is read, one without a descriptor is held for a serial read,
    /// one too long for an SQE fails. Without `block`, a submission channel
    /// with no room keeps the reads submitted, for the next wait to send.
    fn issue(&mut self, block: bool) {
        if self.submitted.is_empty() {
            return;
        }
        let first = self.base + self.sent.len();
        let mut reads = Vec::with_capacity(self.submitted.len());
        // A wait for none reads nothing serially: a read without a descriptor
        // stays submitted, in its place among any the channel has no room
        // for, for the wait that reads it or a wrapper that takes it back.
        let mut deferred: Vec<(usize, QueuedRead)> = Vec::new();
        // The submission place of each read sent below, in order.
        let mut places: Vec<usize> = Vec::new();
        for (place, mut read) in core::mem::take(&mut self.submitted).into_iter().enumerate() {
            if read.buf.capacity() == 0 {
                self.finished.push(done(read, Ok(())));
                continue;
            }
            let Some(fd) = read.file.backing_fd() else {
                if block {
                    self.serial.push(read);
                } else {
                    deferred.push((place, read));
                }
                continue;
            };
            // What the page cache holds is copied here, on the reader's core,
            // so its bytes are checked while still in that core's cache and no
            // thread is woken for it; what it lacks goes to the ring.
            if read.buf.capacity() >= self.cached_min
                && read_cached(fd, &mut read, &self.ring.nowait_refused)
            {
                self.finished.push(done(read, Ok(())));
                continue;
            }
            let Some(offset) = resume_offset(&read) else {
                self.finished.push(done(read, Err(past_last_offset())));
                continue;
            };
            let Ok(len) = i32::try_from(read.buf.capacity() - read.buf.filled()) else {
                self.finished.push(done(
                    read,
                    Err(crate::io::Error::from(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "buffer exceeds i32::MAX",
                    ))),
                ));
                continue;
            };
            // The pointer is into the buffer's heap allocation, which moving
            // the read into `sent` does not move; the buffer is not touched
            // again until its completion is received (see the type's docs).
            // SAFETY: the kernel only writes the span, and the buffer is
            // counted filled only once its completion reports it whole.
            #[expect(unsafe_code, reason = "see safety")]
            let (dst, _) = unsafe { read.buf.unfilled_ptr() };
            reads.push(BatchRead {
                fd,
                buf: UnsafeSendMutPtr(dst),
                len: len.unsigned_abs(),
                offset,
            });
            self.sent.push_back(Some(read));
            places.push(place);
        }
        if reads.is_empty() {
            self.submitted
                .extend(deferred.into_iter().map(|(_, read)| read));
            return;
        }
        let count = reads.len();
        let submission = Submission::Queued {
            reads,
            first,
            sink: Arc::clone(&self.sink),
        };
        let sent = if block {
            self.ring.send(submission)
        } else {
            match self.ring.try_send(submission) {
                Ok(()) => Ok(()),
                Err(TrySend::Failed(error)) => Err(error),
                // Not sent, and the submission dropped here, so no buffer is
                // in the kernel's hands: the reads go back to wait their turn.
                Err(TrySend::Full) => {
                    let kept = self.sent.split_off(first - self.base);
                    let mut back: Vec<(usize, QueuedRead)> = places
                        .into_iter()
                        .zip(kept.into_iter().flatten())
                        .chain(deferred)
                        .collect();
                    back.sort_unstable_by_key(|(place, _)| *place);
                    self.submitted
                        .extend(back.into_iter().map(|(_, read)| read));
                    return;
                }
            }
        };
        self.submitted
            .extend(deferred.into_iter().map(|(_, read)| read));
        match sent {
            Ok(()) => self.on_ring += count,
            // Nothing was queued: the submission went down with the send, so
            // no buffer is in the kernel's hands.
            Err(error) => {
                for slot in self.sent.iter_mut().skip(first - self.base) {
                    if let Some(read) = slot.take() {
                        let failure = io::Error::new(error.kind(), error.to_string());
                        self.finished
                            .push(done(read, Err(crate::io::Error::from(failure))));
                    }
                }
                self.trim();
            }
        }
    }

    /// Takes the read sent at `position` out of its slot.
    fn take_sent(&mut self, position: usize) -> Option<QueuedRead> {
        let read = self
            .sent
            .get_mut(position.checked_sub(self.base)?)?
            .take()?;
        self.trim();
        Some(read)
    }

    /// Drops the handed-back slots at either end, keeping the positions of
    /// the reads still in flight.
    fn trim(&mut self) {
        while self.sent.front().is_some_and(Option::is_none) {
            self.sent.pop_front();
            self.base += 1;
        }
        while self.sent.back().is_some_and(Option::is_none) {
            self.sent.pop_back();
        }
    }

    /// The read at `position` with the ring's `result` for it.
    fn complete(&mut self, position: usize, result: i32) -> Option<ReadDone> {
        let mut read = self.take_sent(position)?;
        self.on_ring -= 1;
        let want = read.buf.capacity() - read.buf.filled();
        let result = match usize::try_from(result) {
            Ok(n) if n == want => {
                // SAFETY: the completion reports the kernel wrote all `want`
                // bytes of the span it was given.
                #[expect(unsafe_code, reason = "see safety")]
                unsafe {
                    read.buf.assume_written(want);
                }
                Ok(())
            }
            Ok(_) => Err(short_read()),
            // A negative completion is `-errno`, never `i32::MIN`.
            Err(_) => Err(crate::io::Error::from(io::Error::from_raw_os_error(
                -result,
            ))),
        };
        Some(done(read, result))
    }
}

/// The smallest read a queue copies from the page cache on the calling
/// thread. Below it the syscall costs more than the copy, and the ring takes
/// a batch of such reads in one submission: copying every read in place made
/// a blob tree's multi-get of small values 1.7x dearer, a floor of 64 KiB
/// left part of that, 256 KiB none.
const CACHED_MIN: usize = 256 << 10;

/// Where `read` goes on from: past what its buffer already holds, as a read
/// handed back to wait its turn keeps what it was given before. `None` past
/// the last offset a file can have.
fn resume_offset(read: &QueuedRead) -> Option<u64> {
    read.offset
        .checked_add(u64::try_from(read.buf.filled()).ok()?)
}

/// A read whose resumed offset passes the last one a file can have.
fn past_last_offset() -> crate::io::Error {
    crate::io::Error::from(io::Error::new(
        io::ErrorKind::InvalidInput,
        "read offset past the last file offset",
    ))
}

/// Copies into `read` what the page cache holds of it, without waiting for
/// the device: `true` when the buffer is full, `false` when the rest is for
/// the ring, which also tells the end of the file apart. A kernel refusing
/// such reads sets `refused`, and every later read of the backend goes to
/// the ring.
fn read_cached(fd: i32, read: &mut QueuedRead, refused: &core::sync::atomic::AtomicBool) -> bool {
    use core::sync::atomic::Ordering;
    if refused.load(Ordering::Relaxed) {
        return false;
    }
    // The whole span must lie below the largest offset: a kernel answers a
    // read ending past it with `EINVAL`, which would read as a refusal of
    // reads that must not wait and turn the fast path off.
    let rest = read.buf.capacity() - read.buf.filled();
    let Some(offset) = resume_offset(read)
        .filter(|&at| {
            u64::try_from(rest)
                .ok()
                .and_then(|rest| at.checked_add(rest))
                .is_some_and(|end| libc::c_long::try_from(end).is_ok())
        })
        .and_then(|offset| libc::c_long::try_from(offset).ok())
    else {
        return false;
    };
    // A file read past the page cache would wait for the device here: the
    // ring overlaps those.
    // SAFETY: F_GETFL only reads the descriptor's flags.
    #[expect(unsafe_code, reason = "see safety")]
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || flags & libc::O_DIRECT != 0 {
        return false;
    }
    // SAFETY: the kernel only writes the span, and only the bytes it reports
    // are counted filled below.
    #[expect(unsafe_code, reason = "see safety")]
    let (dst, len) = unsafe { read.buf.unfilled_ptr() };
    let iov = libc::iovec {
        iov_base: dst.cast(),
        iov_len: len,
    };
    // The system call, not glibc's `preadv2` wrapper: the wrapper only exists
    // from glibc 2.26, and the ring itself needs nothing of the C library. On
    // the 64-bit targets this backend builds for, the kernel takes the whole
    // offset from `pos_l` and ignores `pos_h` (`pos_from_hilo`, fs/read_write.c).
    // SAFETY: `iov` names `len` writable bytes of the buffer, alive for the
    // call.
    #[expect(unsafe_code, reason = "see safety")]
    let n = unsafe {
        libc::syscall(
            libc::SYS_preadv2,
            libc::c_long::from(fd),
            &raw const iov,
            libc::c_long::from(1u8),
            offset,
            libc::c_long::from(0u8),
            libc::c_long::from(libc::RWF_NOWAIT),
        )
    };
    let Ok(n) = usize::try_from(n) else {
        let errno = io::Error::last_os_error().raw_os_error();
        if matches!(errno, Some(libc::EOPNOTSUPP | libc::EINVAL | libc::ENOSYS)) {
            refused.store(true, Ordering::Relaxed);
        }
        // Not cached (EAGAIN), or a failure the ring reports as it would.
        return false;
    };
    // SAFETY: the kernel wrote the `n` bytes it reports.
    #[expect(unsafe_code, reason = "see safety")]
    unsafe {
        read.buf.assume_written(n);
    }
    // A part read, or nothing, may only be the rest being out of the cache:
    // some kernels (5.9, 5.10) answer a read that must not wait with zero
    // bytes before the end of the file. The ring reads on, and reports the
    // end of the file as a short read.
    read.buf.is_full()
}

/// `read` handed back with `result`.
fn done(read: QueuedRead, result: crate::io::Result<()>) -> ReadDone {
    ReadDone {
        tag: read.tag,
        buf: read.buf,
        result,
    }
}

/// A read that returned fewer bytes than its fixed-size block.
fn short_read() -> crate::io::Error {
    crate::io::Error::from(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "io_uring read queue: short read on a fixed-size block",
    ))
}

impl ReadQueue for UringReadQueue<'_> {
    fn submit(&mut self, read: QueuedRead) {
        self.submitted.push(read);
    }

    fn outstanding(&self) -> usize {
        self.submitted.len() + self.on_ring + self.finished.len() + self.serial.len()
    }

    fn held(&self) -> usize {
        // Reads not yet on the ring, kept by a wait for none that found no
        // room or no lock, are sent only by a later wait: no completion
        // wakes a caller for them.
        self.serial.len() + self.submitted.len()
    }

    fn issues_at_wait_for_none(&self) -> bool {
        // It sends what fits on the ring; a read that does not fit waits for
        // the next wait.
        true
    }

    // What a wait for none put back for want of room on the channel: a wait
    // sends everything submitted, so nothing else is left there.
    fn take_kept(&mut self) -> Vec<QueuedRead> {
        core::mem::take(&mut self.submitted)
    }

    fn wait(&mut self, min: usize, on_done: &mut dyn FnMut(ReadDone)) {
        // A wait for none only looks at what is ready: it sends what fits
        // and keeps the rest rather than wait for room on the channel.
        self.issue(min > 0);
        // A serial read blocks the calling thread, which a wait for none
        // must not.
        if min > 0 {
            self.read_serial();
        }
        let mut handed = 0usize;
        for read in core::mem::take(&mut self.finished) {
            on_done(read);
            handed += 1;
        }
        // What has arrived, then, while fewer than `min` were handed over,
        // what arrives next.
        while self.on_ring > 0 {
            let next = if handed < min {
                self.done_rx.recv().ok()
            } else {
                self.done_rx.try_recv().ok()
            };
            let Some((position, result)) = next else {
                break;
            };
            if let Some(read) = self.complete(position, result) {
                on_done(read);
                handed += 1;
            }
        }
    }

    fn set_wake(&mut self, wake: Arc<dyn crate::fs::ReadWake>) -> bool {
        // A read already sent reports to the sink it went with, which has no
        // wake: declined, so no caller sleeps waiting for it.
        if self.on_ring > 0 {
            return false;
        }
        self.sink = Arc::new(QueueSink {
            done: self.sink.done.clone(),
            wake: Some(wake),
        });
        true
    }
}

impl Drop for UringReadQueue<'_> {
    fn drop(&mut self) {
        // The kernel may still be writing into the buffers of reads on the
        // ring: receive their completions before `sent` frees them.
        while self.on_ring > 0 {
            let Ok((position, _)) = self.done_rx.recv() else {
                break;
            };
            if self.take_sent(position).is_some() {
                self.on_ring -= 1;
            }
        }
    }
}

/// Waits out a batch of `expected.len()` reads whose `(position, result)`
/// completions `next` yields in whatever order they finish, and returns the
/// failure at the LOWEST position, if any, with that position.
///
/// Every completion is awaited before returning, even once a failure is
/// known: a read not yet completed may still have the kernel writing into its
/// buffer, and returning early would let the caller free it mid-write. `next`
/// returning `None` means the ring thread is gone, which it only is once no
/// read of its is still in flight; a read that never completed is then
/// reported as a broken pipe.
///
/// The verdict does not depend on the arrival order. Completions arrive in
/// whatever order the kernel finishes them, so taking the first failure
/// OBSERVED would make one batch with one set of failures report different
/// errors from run to run; the lowest position is a rule over the requests.
///
/// `on_read` is called with a read's position as soon as that read has
/// returned every byte it asked for, while later ones may still be in flight.
/// When it panics, the completions still owed are received before the panic
/// carries on, since the unwind frees the buffers those reads write into.
fn drain_batch<N: FnMut() -> Option<(usize, i32)>>(
    expected: &[usize],
    next: N,
    short_read: &'static str,
    mut on_read: impl FnMut(usize),
) -> Result<(), (usize, io::Error)> {
    /// The completions not yet received; receiving them is what dropping it
    /// does, which an unwind out of `on_read` does too.
    struct Owed<N: FnMut() -> Option<(usize, i32)>> {
        next: N,
        count: usize,
    }
    impl<N: FnMut() -> Option<(usize, i32)>> Drop for Owed<N> {
        fn drop(&mut self) {
            while self.count > 0 && (self.next)().is_some() {
                self.count -= 1;
            }
        }
    }

    let mut owed = Owed {
        next,
        count: expected.len(),
    };
    let mut results: Vec<Option<i32>> = vec![None; expected.len()];
    while owed.count > 0 {
        let Some((position, result)) = (owed.next)() else {
            break;
        };
        owed.count -= 1;
        if let (Some(slot), Some(&want)) = (results.get_mut(position), expected.get(position)) {
            *slot = Some(result);
            if usize::try_from(result).is_ok_and(|n| n == want) {
                on_read(position);
            }
        }
    }
    // Nothing more is owed: either every completion arrived or the ring is gone.
    owed.count = 0;

    for (position, (result, &want)) in results.into_iter().zip(expected).enumerate() {
        let failure = match result {
            None => io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"),
            Some(result) if result < 0 => io::Error::from_raw_os_error(-result),
            #[expect(clippy::cast_sign_loss, reason = "guarded by result < 0 above")]
            Some(result) if result as usize != want => {
                io::Error::new(io::ErrorKind::UnexpectedEof, short_read)
            }
            Some(_) => continue,
        };
        return Err((position, failure));
    }
    Ok(())
}

impl Drop for RingThread {
    // Coverage: poison recovery branches require panic injection to reach.
    // The normal (non-poison) path is exercised by every test that drops IoUringFs.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn drop(&mut self) {
        // Drop the sender to close the channel — this unblocks the event
        // loop's recv() and lets it drain remaining in-flight ops.
        // Handle poison gracefully: during shutdown we only need to clear
        // the sender and join the thread, regardless of prior panics.
        let tx = match self.tx.get_mut() {
            Ok(tx) => tx,
            Err(poisoned) => poisoned.into_inner(),
        };
        *tx = None;

        let handle_slot = match self.handle.get_mut() {
            Ok(h) => h,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(handle) = handle_slot.take()
            && handle.join().is_err()
        {
            log::error!("io_uring ring thread panicked during shutdown");
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests;
