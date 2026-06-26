// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! `io_uring`-backed [`Fs`] implementation for high-throughput I/O on Linux.
//!
//! Requires the `io-uring` feature flag and Linux 5.6+. Uses a dedicated
//! I/O thread that owns the `io_uring` ring instance. Submissions from
//! multiple threads are batched opportunistically — when several threads
//! submit I/O concurrently, their SQEs are combined into a single
//! `io_uring_enter` syscall.
//!
//! Hot-path operations (read, write, fsync) go through the ring.
//! Cold-path operations (mkdir, readdir, stat, rename, unlink) delegate
//! to [`std::fs`] since they do not benefit from `io_uring`.

use super::{BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
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
/// Routes hot-path I/O operations (read, write, fsync) through a
/// dedicated `io_uring` ring thread. Directory and metadata operations
/// delegate to [`std::fs`] since they do not benefit from `io_uring`.
///
/// Multiple `IoUringFs` clones and all [`IoUringFile`] handles opened
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

        // Gate matches the `mod direct_io;` declaration in `fs/mod.rs`
        // — the submodule only exists when `feature = "std"` is on.
        // The whole `io_uring_fs` module is already gated by
        // `cfg(all(target_os = "linux", feature = "io-uring"))`, and
        // the `io-uring` feature transitively enables `std` (see the
        // feature declaration in Cargo.toml), so this extra
        // `feature = "std"` predicate is logically redundant here.
        // Kept for consistency with `StdFs::open` and to make the
        // dependency on direct_io's gate explicit at the call site,
        // so feature-gate audits don't have to chase the implication
        // through Cargo.toml.
        #[cfg(feature = "std")]
        super::direct_io::apply_direct_io_flag(&mut builder, opts.direct_io);

        let file = builder.open(path)?;

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
        // Every request that has an fd goes to the one shared ring in a single
        // batched submission (the kernel fans each read out to its file's
        // device). If any request lacks an fd (a non-io_uring file mixed in),
        // fall back to serial reads for the whole batch.
        if reqs.iter().any(|r| r.file.backing_fd().is_none()) {
            for req in reqs.iter_mut() {
                let n = req.file.read_at(req.buf, req.offset)?;
                if n != req.buf.len() {
                    return Err(crate::io::Error::from(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "read_blocks_batched: short read on a fixed-size block",
                    )));
                }
            }
            return Ok(());
        }
        self.inner
            .submit_reads_multi(reqs)
            .map_err(crate::io::Error::from)
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

    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        let dir = File::open(path)?;
        if !dir.metadata()?.is_dir() {
            return Err(crate::io::Error::new(
                crate::io::ErrorKind::InvalidInput,
                "sync_directory: path is not a directory",
            ));
        }
        self.inner.submit_fsync(dir.as_raw_fd(), false)?;
        Ok(())
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
}

// ---------------------------------------------------------------------------
// IoUringFile
// ---------------------------------------------------------------------------

/// File handle that routes I/O through an `io_uring` ring thread.
///
/// Wraps a [`std::fs::File`] for fd ownership and cold-path operations
/// (metadata, truncate, lock), while routing reads, writes, and fsyncs
/// through the shared `io_uring` ring.
pub struct IoUringFile {
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
        self.ring.submit_fsync(self.file.as_raw_fd(), false)?;
        Ok(())
    }

    fn sync_data(&self) -> crate::io::Result<()> {
        self.ring.submit_fsync(self.file.as_raw_fd(), true)?;
        Ok(())
    }

    fn metadata(&self) -> crate::io::Result<FsMetadata> {
        let m = self.file.metadata()?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        self.file.set_len(size).map_err(crate::io::Error::from)
    }

    // Fill-or-EOF: loop until buf is full or we hit EOF (0-byte read).
    // Retries on EINTR internally so callers can rely on short read = EOF.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let fd = self.file.as_raw_fd();
        let mut total_read: usize = 0;

        while total_read < buf.len() {
            let remaining = buf.get_mut(total_read..).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "read_at offset out of bounds")
            })?;
            let current_offset = offset.checked_add(total_read as u64).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "read_at offset overflow")
            })?;

            let n = loop {
                match self.ring.submit_read(fd, remaining, current_offset) {
                    Ok(n) => break n,
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e.into()),
                }
            };

            if n == 0 {
                break; // EOF
            }
            total_read += n as usize;
        }

        Ok(total_read)
    }

    // Batched read: submit every region to the ring at once (one coalesced
    // submission, reads overlap in flight) instead of the trait's serial
    // read_at loop. This is the io_uring win for multi-block reads.
    fn read_many(&self, regions: &mut [(u64, &mut [u8])]) -> crate::io::Result<()> {
        self.ring
            .submit_reads(self.file.as_raw_fd(), regions)
            .map_err(crate::io::Error::from)
    }

    // Exposes the fd so `IoUringFs::read_blocks_batched` can submit reads across
    // many files (SSTs) to this handle's shared ring in one batch.
    fn backing_fd(&self) -> Option<i32> {
        Some(self.file.as_raw_fd())
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
        let n = self.ring.submit_read(self.file.as_raw_fd(), buf, *cursor)?;
        *cursor += u64::from(n);
        Ok(n as usize)
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
        let n = self
            .ring
            .submit_write(self.file.as_raw_fd(), buf, *cursor)?;
        *cursor += u64::from(n);
        Ok(n as usize)
    }

    fn flush(&mut self) -> io::Result<()> {
        // No userspace buffer to flush — data goes directly to the kernel
        // via io_uring. Use sync_data()/sync_all() for durable persistence.
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

/// Newtype wrapper for sending a `*const u8` across threads.
///
/// See [`UnsafeSendMutPtr`] for safety contract.
struct UnsafeSendConstPtr(*const u8);

// SAFETY: see struct-level docs. The raw pointers are guaranteed valid
// for the duration of the io_uring op because the caller blocks until
// the CQE is received.
#[expect(unsafe_code, reason = "marking raw-pointer wrapper as Send")]
unsafe impl Send for UnsafeSendMutPtr {}

#[expect(unsafe_code, reason = "marking raw-pointer wrapper as Send")]
unsafe impl Send for UnsafeSendConstPtr {}

/// An I/O operation to submit to the ring.
enum OpKind {
    Read {
        fd: i32,
        buf: UnsafeSendMutPtr,
        len: u32,
        offset: u64,
    },
    Write {
        fd: i32,
        buf: UnsafeSendConstPtr,
        len: u32,
        offset: u64,
    },
    Fsync {
        fd: i32,
        datasync: bool,
    },
}

/// A submitted operation with its result channel.
struct Op {
    kind: OpKind,
    result_tx: mpsc::SyncSender<i32>,
}

/// Dedicated thread that owns the `io_uring` ring.
///
/// Operations are submitted via bounded `mpsc::SyncSender` (sized to match
/// the ring) and results are returned through per-operation channels.
struct RingThread {
    tx: Mutex<Option<mpsc::SyncSender<Op>>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
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
    fn event_loop(mut ring: IoUring, rx: mpsc::Receiver<Op>) {
        // ManuallyDrop ensures that on panic, pending's SyncSenders are NOT
        // dropped during stack unwinding. This keeps callers blocked on their
        // result channels until catch_unwind + abort kills the process,
        // preventing them from dropping buffers that the kernel may still access.
        let mut pending =
            std::mem::ManuallyDrop::new(HashMap::<u64, mpsc::SyncSender<i32>>::default());
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

            if let Some(op) = first {
                Self::enqueue(&mut ring, &mut pending, &mut next_id, op);

                // Batch: drain as many additional ops as available.
                while let Ok(op) = rx.try_recv() {
                    Self::enqueue(&mut ring, &mut pending, &mut next_id, op);
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
                let id = cqe.user_data();
                if let Some(tx) = pending.remove(&id) {
                    let _ = tx.send(cqe.result());
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

    /// Builds an SQE from `op` and pushes it onto the submission queue.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn enqueue(
        ring: &mut IoUring,
        pending: &mut HashMap<u64, mpsc::SyncSender<i32>>,
        next_id: &mut u64,
        op: Op,
    ) {
        let id = *next_id;
        *next_id = next_id.wrapping_add(1);

        let sqe = match op.kind {
            OpKind::Read {
                fd,
                buf,
                len,
                offset,
            } => opcode::Read::new(types::Fd(fd), buf.0, len)
                .offset(offset)
                .build()
                .user_data(id),

            OpKind::Write {
                fd,
                buf,
                len,
                offset,
            } => opcode::Write::new(types::Fd(fd), buf.0, len)
                .offset(offset)
                .build()
                .user_data(id),

            OpKind::Fsync { fd, datasync } => {
                let mut entry = opcode::Fsync::new(types::Fd(fd));
                if datasync {
                    entry = entry.flags(types::FsyncFlags::DATASYNC);
                }
                entry.build().user_data(id)
            }
        };

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
                    let cid = cqe.user_data();
                    if let Some(tx) = pending.remove(&cid) {
                        let _ = tx.send(cqe.result());
                    }
                }
            }
        }

        pending.insert(id, op.result_tx);
    }

    // -- Submission helpers --------------------------------------------------

    /// Submits a pread to the ring and blocks until completion.
    fn submit_read(&self, fd: i32, buf: &mut [u8], offset: u64) -> io::Result<u32> {
        // SQE length is u32, but CQE result is i32 — cap at i32::MAX
        // to ensure the byte count is always representable. In practice
        // LSM block reads are 4-64 KB, so the cap is never reached.
        let len: u32 = i32::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX"))?
            .unsigned_abs();
        let (tx, rx) = mpsc::sync_channel(1);
        let op = Op {
            kind: OpKind::Read {
                fd,
                buf: UnsafeSendMutPtr(buf.as_mut_ptr()),
                len,
                offset,
            },
            result_tx: tx,
        };
        self.send_and_wait(op, &rx)
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
    fn submit_reads(&self, fd: i32, regions: &mut [(u64, &mut [u8])]) -> io::Result<()> {
        // Phase 1: send every op, collecting one result receiver per region. The
        // op holds a raw pointer into the caller's buffer; `regions` is borrowed
        // for the whole call and we block on the receivers below, so every buffer
        // outlives the kernel's access (the UnsafeSend safety contract).
        let mut receivers: Vec<(mpsc::Receiver<i32>, usize)> = Vec::with_capacity(regions.len());
        for (offset, buf) in regions.iter_mut() {
            if buf.is_empty() {
                continue;
            }
            // SQE length is u32, CQE result is i32 (cap at i32::MAX so the byte
            // count stays representable; block reads are KBs, never near the cap).
            let len: u32 = i32::try_from(buf.len())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX")
                })?
                .unsigned_abs();
            let (tx, rx) = mpsc::sync_channel(1);
            let op = Op {
                kind: OpKind::Read {
                    fd,
                    buf: UnsafeSendMutPtr(buf.as_mut_ptr()),
                    len,
                    offset: *offset,
                },
                result_tx: tx,
            };
            // Send without waiting so the next op queues behind it; the event
            // loop's try_recv drain coalesces them. The bounded channel applies
            // backpressure past ring capacity, and the loop keeps draining +
            // submitting, so a send beyond capacity makes progress (no deadlock).
            self.tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread shut down")
                })?
                .send(op)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;
            receivers.push((rx, buf.len()));
        }

        // Phase 2: wait for every completion. Each region must be fully read.
        for (rx, expected) in receivers {
            let result = rx
                .recv()
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;
            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }
            #[expect(clippy::cast_sign_loss, reason = "guarded by result < 0 above")]
            let n = result as usize;
            if n != expected {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "io_uring read_many: short read on a fixed-size block region",
                ));
            }
        }
        Ok(())
    }

    /// Like [`Self::submit_reads`], but each request carries its OWN fd, so
    /// reads from DIFFERENT files (SSTs, and on a multi-device layout different
    /// devices) coalesce into one submission to the shared ring. Callers
    /// guarantee every request has an fd (`read_blocks_batched` checks first).
    fn submit_reads_multi(&self, reqs: &mut [BlockRead<'_>]) -> io::Result<()> {
        let mut receivers: Vec<(mpsc::Receiver<i32>, usize)> = Vec::with_capacity(reqs.len());
        for req in reqs.iter_mut() {
            if req.buf.is_empty() {
                continue;
            }
            let fd = req.file.backing_fd().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "submit_reads_multi: request without fd",
                )
            })?;
            let len: u32 = i32::try_from(req.buf.len())
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX")
                })?
                .unsigned_abs();
            let (tx, rx) = mpsc::sync_channel(1);
            let expected = req.buf.len();
            let op = Op {
                kind: OpKind::Read {
                    fd,
                    buf: UnsafeSendMutPtr(req.buf.as_mut_ptr()),
                    len,
                    offset: req.offset,
                },
                result_tx: tx,
            };
            self.tx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread shut down")
                })?
                .send(op)
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;
            receivers.push((rx, expected));
        }

        for (rx, expected) in receivers {
            let result = rx
                .recv()
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;
            if result < 0 {
                return Err(io::Error::from_raw_os_error(-result));
            }
            #[expect(clippy::cast_sign_loss, reason = "guarded by result < 0 above")]
            let n = result as usize;
            if n != expected {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "io_uring read_blocks_batched: short read on a fixed-size block",
                ));
            }
        }
        Ok(())
    }

    /// Submits a pwrite to the ring and blocks until completion.
    fn submit_write(&self, fd: i32, buf: &[u8], offset: u64) -> io::Result<u32> {
        // SQE length is u32, but CQE result is i32 — cap at i32::MAX
        // to ensure the byte count is always representable. In practice
        // LSM block writes are 4-64 KB, so the cap is never reached.
        let len: u32 = i32::try_from(buf.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "buffer exceeds i32::MAX"))?
            .unsigned_abs();
        let (tx, rx) = mpsc::sync_channel(1);
        let op = Op {
            kind: OpKind::Write {
                fd,
                buf: UnsafeSendConstPtr(buf.as_ptr()),
                len,
                offset,
            },
            result_tx: tx,
        };
        self.send_and_wait(op, &rx)
    }

    /// Submits an fsync or fdatasync and blocks until completion.
    fn submit_fsync(&self, fd: i32, datasync: bool) -> io::Result<u32> {
        let (tx, rx) = mpsc::sync_channel(1);
        let op = Op {
            kind: OpKind::Fsync { fd, datasync },
            result_tx: tx,
        };
        self.send_and_wait(op, &rx)
    }

    /// Sends an operation to the ring thread and blocks on the result.
    ///
    /// Returns the non-negative CQE result as `u32`. Negative results
    /// (kernel errors) are converted to [`io::Error`].
    fn send_and_wait(&self, op: Op, rx: &mpsc::Receiver<i32>) -> io::Result<u32> {
        // Mutex guards Option<Sender> for clean shutdown (Drop sets to None).
        // Lock is held only for send() duration (~ns) — negligible vs I/O
        // latency (~µs). A lock-free channel would eliminate this but adds
        // an external dependency for no measurable gain.
        self.tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread shut down"))?
            .send(op)
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;

        let result = rx
            .recv()
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "io_uring thread exited"))?;

        if result >= 0 {
            // CQE result is non-negative — `as u32` is lossless.
            #[expect(clippy::cast_sign_loss, reason = "guarded by result >= 0 check above")]
            Ok(result as u32)
        } else {
            Err(io::Error::from_raw_os_error(-result))
        }
    }
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
