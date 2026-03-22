// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

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

use super::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use io_uring::{opcode, types, IoUring};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;

/// Default number of `io_uring` submission queue entries.
const DEFAULT_SQ_ENTRIES: u32 = 256;

/// Probes whether `io_uring` is supported on the running kernel.
///
/// Creates a minimal ring with 2 entries and immediately drops it.
/// Returns `false` on any failure (old kernel, seccomp restrictions, etc.).
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
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> io::Result<Box<dyn FsFile>> {
        let file = OpenOptions::new()
            .read(opts.read)
            .write(opts.write)
            .create(opts.create)
            .create_new(opts.create_new)
            .truncate(opts.truncate)
            .append(opts.append)
            .open(path)?;

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
            ring: Arc::clone(&self.inner),
        }))
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<FsDirEntry>> {
        // Delegate to std::fs — directory listing doesn't benefit from io_uring.
        std::fs::read_dir(path)?
            .map(|res| {
                let entry = res?;
                let file_type = entry.file_type()?;
                let file_name = entry.file_name().into_string().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "non-UTF-8 filename")
                })?;
                Ok(FsDirEntry {
                    path: entry.path(),
                    file_name,
                    is_dir: file_type.is_dir(),
                })
            })
            .collect()
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn metadata(&self, path: &Path) -> io::Result<FsMetadata> {
        let m = std::fs::metadata(path)?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        let dir = File::open(path)?;
        if !dir.metadata()?.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sync_directory: path is not a directory",
            ));
        }
        self.inner.submit_fsync(dir.as_raw_fd(), false)?;
        Ok(())
    }

    fn exists(&self, path: &Path) -> io::Result<bool> {
        path.try_exists()
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
    /// `AtomicU64` is used instead of plain `u64` so that `IoUringFile`
    /// is `Sync` — required by the [`FsFile`] trait bound.
    cursor: AtomicU64,

    /// Shared reference to the ring thread.
    ring: Arc<RingThread>,
}

impl FsFile for IoUringFile {
    fn sync_all(&self) -> io::Result<()> {
        self.ring.submit_fsync(self.file.as_raw_fd(), false)?;
        Ok(())
    }

    fn sync_data(&self) -> io::Result<()> {
        self.ring.submit_fsync(self.file.as_raw_fd(), true)?;
        Ok(())
    }

    fn metadata(&self) -> io::Result<FsMetadata> {
        let m = self.file.metadata()?;
        Ok(FsMetadata {
            len: m.len(),
            is_dir: m.is_dir(),
            is_file: m.is_file(),
        })
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }

    // Fill-or-EOF: loop until buf is full or we hit EOF (0-byte read).
    // Retries on EINTR internally so callers can rely on short read = EOF.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let fd = self.file.as_raw_fd();
        let mut total_read: usize = 0;

        while total_read < buf.len() {
            let remaining = &mut buf[total_read..];
            let current_offset = offset + total_read as u64;

            let n = loop {
                match self.ring.submit_read(fd, remaining, current_offset) {
                    Ok(n) => break n,
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        if total_read > 0 {
                            return Ok(total_read);
                        }
                        return Err(e);
                    }
                }
            };

            if n == 0 {
                break; // EOF
            }
            total_read += n as usize;
        }

        Ok(total_read)
    }

    fn lock_exclusive(&self) -> io::Result<()> {
        // Delegate to the platform-specific FsFile impl for std::fs::File.
        FsFile::lock_exclusive(&self.file)
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
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow"))?,
            SeekFrom::End(n) => {
                let len = self.file.metadata()?.len();
                if n >= 0 {
                    len.checked_add(n.unsigned_abs())
                } else {
                    len.checked_sub(n.unsigned_abs())
                }
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "seek position overflow")
                })?
            }
        };
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
/// Operations are submitted via `mpsc::Sender` and results are returned
/// through per-operation `mpsc::SyncSender` channels.
struct RingThread {
    tx: Mutex<Option<mpsc::Sender<Op>>>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl RingThread {
    fn spawn(sq_entries: u32) -> io::Result<Self> {
        let ring = IoUring::new(sq_entries)?;
        let (tx, rx) = mpsc::channel();

        let handle = thread::Builder::new()
            .name("lsm-io-uring".into())
            .spawn(move || Self::event_loop(ring, rx))?;

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
    #[expect(
        clippy::needless_pass_by_value,
        reason = "rx is moved into the spawned thread — must be owned"
    )]
    fn event_loop(mut ring: IoUring, rx: mpsc::Receiver<Op>) {
        let mut pending: HashMap<u64, mpsc::SyncSender<i32>> = HashMap::new();
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
                        let errno = e.raw_os_error().unwrap_or(5 /* EIO */);
                        for (_, tx) in pending.drain() {
                            let _ = tx.send(-errno);
                        }
                        log::error!("io_uring submit_and_wait failed: {e}");
                        return;
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
    }

    /// Builds an SQE from `op` and pushes it onto the submission queue.
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
            if ring.submission().push(&sqe).is_err() {
                // SQ full — submit pending SQEs to the kernel and harvest
                // any already-completed CQEs to free SQ slots. This is
                // best-effort: if no completions are available the retry
                // below will fail with EBUSY, which is the correct outcome
                // for a genuinely saturated ring.
                if let Err(e) = ring.submit() {
                    let errno = e.raw_os_error().unwrap_or(5 /* EIO */);
                    let _ = op.result_tx.send(-errno);
                    return;
                }
                for cqe in ring.completion() {
                    let cid = cqe.user_data();
                    if let Some(tx) = pending.remove(&cid) {
                        let _ = tx.send(cqe.result());
                    }
                }
                if ring.submission().push(&sqe).is_err() {
                    // Still full after flush — io_uring ring is saturated.
                    let _ = op.result_tx.send(-16 /* EBUSY */);
                    return;
                }
            }
        }

        pending.insert(id, op.result_tx);
    }

    // -- Submission helpers --------------------------------------------------

    /// Submits a pread to the ring and blocks until completion.
    fn submit_read(&self, fd: i32, buf: &mut [u8], offset: u64) -> io::Result<u32> {
        // io_uring SQE length field is u32. In practice LSM block reads
        // are 4-64 KB, so the cap is never reached.
        let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
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

    /// Submits a pwrite to the ring and blocks until completion.
    fn submit_write(&self, fd: i32, buf: &[u8], offset: u64) -> io::Result<u32> {
        let len = u32::try_from(buf.len()).unwrap_or(u32::MAX);
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
        if let Some(handle) = handle_slot.take() {
            let _ = handle.join();
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Arc;
    // Shadows #[test] to enable log capture in test output.
    use test_log::test;

    /// Returns an `IoUringFs` or `None` if not available (e.g. old kernel,
    /// container without io_uring access). Tests that call this gracefully
    /// skip when io_uring is unavailable.
    fn try_io_uring() -> Option<IoUringFs> {
        match IoUringFs::new() {
            Ok(fs) => Some(fs),
            Err(e) => {
                eprintln!("skipping io_uring test: {e}");
                None
            }
        }
    }

    #[test]
    fn probe_availability() {
        // Just exercises the probe — result depends on the kernel.
        let available = is_io_uring_available();
        eprintln!("io_uring available: {available}");
    }

    #[test]
    fn create_read_write() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("test.txt");
        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        FsFile::sync_all(&file)?;
        drop(file);

        let opts = FsOpenOptions::new().read(true);
        let mut file = fs.open(&path, &opts)?;
        let mut buf = String::new();
        file.read_to_string(&mut buf)?;
        assert_eq!(buf, "hello world");

        Ok(())
    }

    #[test]
    fn read_at_pread_semantics() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("pread.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        FsFile::sync_data(&file)?;

        let mut buf = [0u8; 5];
        let n = FsFile::read_at(&file, &mut buf, 6)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"world");

        let n = FsFile::read_at(&file, &mut buf, 0)?;
        assert_eq!(n, 5);
        assert_eq!(&buf, b"hello");

        Ok(())
    }

    #[test]
    fn directory_operations() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let nested = dir.path().join("a").join("b").join("c");
        fs.create_dir_all(&nested)?;
        assert!(fs.exists(&nested)?);

        let file_path = nested.join("data.bin");
        let opts = FsOpenOptions::new().write(true).create_new(true);
        let mut file = fs.open(&file_path, &opts)?;
        file.write_all(b"data")?;
        drop(file);

        let entries: Vec<_> = fs.read_dir(&nested)?.collect::<io::Result<Vec<_>>>()?;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].file_name, "data.bin");

        let meta = fs.metadata(&file_path)?;
        assert!(meta.is_file);
        assert_eq!(meta.len, 4);

        fs.remove_file(&file_path)?;
        assert!(!fs.exists(&file_path)?);

        let top = dir.path().join("a");
        fs.remove_dir_all(&top)?;
        assert!(!fs.exists(&top)?);

        Ok(())
    }

    #[test]
    fn rename() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let src = dir.path().join("src.txt");
        let dst = dir.path().join("dst.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&src, &opts)?;
        file.write_all(b"content")?;
        drop(file);

        fs.rename(&src, &dst)?;
        assert!(!fs.exists(&src)?);
        assert!(fs.exists(&dst)?);

        Ok(())
    }

    #[test]
    fn sync_directory() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        fs.sync_directory(dir.path())?;
        Ok(())
    }

    #[test]
    fn file_metadata() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("meta.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"12345")?;

        let meta = FsFile::metadata(&file)?;
        assert!(meta.is_file);
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    fn file_set_len() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("truncate.bin");
        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        FsFile::set_len(&file, 5)?;

        let meta = FsFile::metadata(&file)?;
        assert_eq!(meta.len, 5);

        Ok(())
    }

    #[test]
    fn lock_exclusive() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;

        let path = dir.path().join("lockfile");
        let opts = FsOpenOptions::new().write(true).create(true);
        let file = fs.open(&path, &opts)?;
        FsFile::lock_exclusive(&file)?;

        Ok(())
    }

    #[test]
    fn truncate_and_append() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("trunc.txt");

        let opts = FsOpenOptions::new().write(true).create(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;
        drop(file);

        let opts = FsOpenOptions::new().write(true).truncate(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hi")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 2);

        let opts = FsOpenOptions::new().write(true).append(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"!")?;
        drop(file);

        let meta = fs.metadata(&path)?;
        assert_eq!(meta.len, 3);

        Ok(())
    }

    #[test]
    fn seek_operations() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("seek.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        file.write_all(b"hello world")?;

        // Seek to start and re-read
        file.seek(SeekFrom::Start(0))?;
        let mut buf = [0u8; 5];
        file.read_exact(&mut buf)?;
        assert_eq!(&buf, b"hello");

        // Seek from current (+1 to skip space)
        file.seek(SeekFrom::Current(1))?;
        file.read_exact(&mut buf)?;
        assert_eq!(&buf, b"world");

        // Seek from end
        let pos = file.seek(SeekFrom::End(-5))?;
        assert_eq!(pos, 6);

        Ok(())
    }

    #[test]
    fn concurrent_read_at() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("concurrent.bin");

        let opts = FsOpenOptions::new().write(true).create(true).read(true);
        let mut file = fs.open(&path, &opts)?;
        // Write 1000 bytes: each byte = (offset % 256)
        let data: Vec<u8> = (0..1000).map(|i| (i % 256) as u8).collect();
        file.write_all(&data)?;
        FsFile::sync_all(&file)?;

        let file = Arc::new(file);
        let mut handles = Vec::new();

        for chunk_start in (0..1000).step_by(100) {
            let file = Arc::clone(&file);
            handles.push(thread::spawn(move || -> io::Result<()> {
                let mut buf = [0u8; 100];
                let n = FsFile::read_at(file.as_ref(), &mut buf, chunk_start as u64)?;
                assert_eq!(n, 100);
                for (i, &byte) in buf.iter().enumerate() {
                    assert_eq!(byte, ((chunk_start + i) % 256) as u8);
                }
                Ok(())
            }));
        }

        for h in handles {
            match h.join() {
                Ok(result) => result?,
                Err(_) => return Err(io::Error::new(io::ErrorKind::Other, "thread panicked")),
            }
        }

        Ok(())
    }

    #[test]
    fn metadata_directory() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let dir = tempfile::tempdir()?;
        let meta = fs.metadata(dir.path())?;
        assert!(meta.is_dir);
        assert!(!meta.is_file);

        Ok(())
    }

    #[test]
    fn object_safety() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let fs: Arc<dyn Fs> = Arc::new(fs);
        let dir = tempfile::tempdir()?;
        let bogus = dir.path().join("nonexistent");
        assert!(!fs.exists(&bogus)?);
        Ok(())
    }

    #[test]
    fn clone_shares_ring() -> io::Result<()> {
        let Some(fs) = try_io_uring() else {
            return Ok(());
        };
        let fs2 = fs.clone();
        let dir = tempfile::tempdir()?;

        // Both clones should work with the same ring thread.
        let p1 = dir.path().join("a.txt");
        let p2 = dir.path().join("b.txt");
        let opts = FsOpenOptions::new().write(true).create(true);

        let mut f1 = fs.open(&p1, &opts)?;
        let mut f2 = fs2.open(&p2, &opts)?;
        f1.write_all(b"one")?;
        f2.write_all(b"two")?;
        FsFile::sync_all(&f1)?;
        FsFile::sync_all(&f2)?;

        assert_eq!(fs.metadata(&p1)?.len, 3);
        assert_eq!(fs2.metadata(&p2)?.len, 3);

        Ok(())
    }
}
