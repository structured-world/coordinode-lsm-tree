// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Pure-Rust, `no_std` `io_uring` driver core on raw Linux syscalls.
//!
//! The std-bound [`IoUringFs`](super::io_uring_fs) backend wraps the `io-uring`
//! crate, which pulls in `std` (it returns `std::io::Error` and consumes
//! `std::os::unix::io::RawFd`). This module drives the kernel ring directly via
//! the `syscalls` crate's raw `syscall!` and its own `mmap`'d ring management,
//! so a Linux `no_std + alloc` consumer gets kernel async I/O with no `std`.
//!
//! The core sets up a ring, submits a batch of SQEs (as many in flight as the
//! rings hold) and reaps their CQEs; one operation is a batch of one. The
//! [`Fs`] / [`FsFile`] impls build on it: single reads, writes
//! and syncs, and batched reads across files.
//!
//! Linux-only by construction (`io_uring` is a Linux kernel API); the module is
//! `cfg(all(target_os = "linux", feature = "io-uring-raw"))` at its declaration.

// This module is a thin Linux syscall / mmap ABI surface. Descriptors (`i32`)
// and ring offsets (`u64`) are cast to the register-width `usize` the syscall
// layer takes, the ring fd from `io_uring_setup` is narrowed back to `i32`, and
// the mmap'd ring fields are read through pointers cast from the page-aligned
// mapping base. These casts are inherent to the ABI and individually sound
// (non-negative fds, bounded `IORING_OFF_*` offsets, kernel-natural field
// alignment within a page-aligned mapping), so the cast lints are expected here.
#![expect(
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_ptr_alignment,
    reason = "Linux syscall/mmap ABI: register-width arg casts + page-aligned ring field reads"
)]

use alloc::boxed::Box;
use alloc::ffi::CString;
#[cfg(not(feature = "std"))]
use alloc::format;
use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use core::sync::atomic::{Ordering, fence};

use spin::Mutex;
use syscalls::{Errno, Sysno, syscall1, syscall2, syscall3, syscall4, syscall5, syscall6};

use crate::fs::{BlockRead, Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
use crate::io::{Error, ErrorKind, SeekFrom};
use crate::path::Path;

// ---- Linux io_uring ABI constants (stable kernel uapi, linux/io_uring.h) ----

/// `mmap` offset for the submission-queue ring.
const IORING_OFF_SQ_RING: u64 = 0;
/// `mmap` offset for the completion-queue ring.
const IORING_OFF_CQ_RING: u64 = 0x0800_0000;
/// `mmap` offset for the SQE array.
const IORING_OFF_SQES: u64 = 0x1000_0000;

/// `io_uring_enter` flag: wait for at least `min_complete` completions.
const IORING_ENTER_GETEVENTS: u32 = 1;

/// `io_uring_params.features` bit: the SQ and CQ rings live in a single `mmap`
/// (every kernel since 5.4). When set, we map the CQ ring as part of the SQ
/// ring mapping instead of a second `mmap`.
const IORING_FEAT_SINGLE_MMAP: u32 = 1;

/// No-op opcode: completes immediately with `res == 0`. Used to smoke-test the
/// ring without touching a file.
const IORING_OP_NOP: u8 = 0;
/// `fsync` opcode: flush the file's data + metadata (or just data, with the
/// `IORING_FSYNC_DATASYNC` flag) to stable storage.
const IORING_OP_FSYNC: u8 = 3;
/// `read` opcode: read into a single buffer at an explicit offset (the `addr` /
/// `len` form, not the iovec `readv`).
const IORING_OP_READ: u8 = 22;
/// `write` opcode: write a single buffer at an explicit offset.
const IORING_OP_WRITE: u8 = 23;
/// `op_flags` bit for [`IORING_OP_FSYNC`]: flush data only (skip non-essential
/// metadata), matching `fdatasync(2)`.
const IORING_FSYNC_DATASYNC: u32 = 1;

/// `openat` dir-fd sentinel meaning "resolve relative paths against the cwd".
const AT_FDCWD: i32 = -100;

// `open(2)` access-mode / creation flags for [`open_raw`] callers.
//
// The access modes (O_RDONLY/O_WRONLY/O_RDWR = 0/1/2) are universal across every
// Linux architecture. O_CREAT and O_TRUNC, however, are among the handful of
// open() flags whose bit values are arch-specific: x86/x86_64, ARM, AArch64,
// RISC-V, PowerPC, etc. use the asm-generic values, while MIPS and SPARC define
// their own (verified against each arch's kernel <asm/fcntl.h>). Hardcoding the
// asm-generic values unconditionally would silently pass the wrong flag on a
// MIPS or SPARC build, so they are selected per target_arch below.

/// Open read-only.
pub const O_RDONLY: i32 = 0;
/// Open write-only.
pub const O_WRONLY: i32 = 1;
/// Open read-write.
pub const O_RDWR: i32 = 2;

/// Create the file if it does not exist. Asm-generic value (`0o100`); MIPS and
/// SPARC override below.
#[cfg(not(any(
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    target_arch = "sparc",
    target_arch = "sparc64"
)))]
pub const O_CREAT: i32 = 0o100;
/// Create the file if it does not exist (MIPS: `0x100`).
#[cfg(any(
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6"
))]
pub const O_CREAT: i32 = 0x100;
/// Create the file if it does not exist (SPARC: `0x200`).
#[cfg(any(target_arch = "sparc", target_arch = "sparc64"))]
pub const O_CREAT: i32 = 0x200;

/// Truncate the file to zero length on open. Asm-generic and MIPS share `0o1000`
/// (`0x200`); only SPARC overrides below.
#[cfg(not(any(target_arch = "sparc", target_arch = "sparc64")))]
pub const O_TRUNC: i32 = 0o1000;
/// Truncate the file to zero length on open (SPARC: `0x400`).
#[cfg(any(target_arch = "sparc", target_arch = "sparc64"))]
pub const O_TRUNC: i32 = 0x400;

// `mmap` protection / flags.
const PROT_READ: usize = 0x1;
const PROT_WRITE: usize = 0x2;
const MAP_SHARED: usize = 0x01;
const MAP_POPULATE: usize = 0x0_8000;

// ---- ABI structs (repr(C), field order per linux/io_uring.h) ----

/// Submission-queue ring offsets, filled in by the kernel on `io_uring_setup`.
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct SqRingOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    flags: u32,
    dropped: u32,
    array: u32,
    resv1: u32,
    resv2: u64,
}

/// Completion-queue ring offsets, filled in by the kernel on `io_uring_setup`.
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct CqRingOffsets {
    head: u32,
    tail: u32,
    ring_mask: u32,
    ring_entries: u32,
    overflow: u32,
    cqes: u32,
    flags: u32,
    resv1: u32,
    resv2: u64,
}

/// Parameters passed to (and filled in by) `io_uring_setup`.
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoUringParams {
    sq_entries: u32,
    cq_entries: u32,
    flags: u32,
    sq_thread_cpu: u32,
    sq_thread_idle: u32,
    features: u32,
    wq_fd: u32,
    resv: [u32; 3],
    sq_off: SqRingOffsets,
    cq_off: CqRingOffsets,
}

/// Submission-queue entry (64 bytes). Only the fields the NOP / file opcodes
/// need are named; the rest are kept as padding so the struct matches the
/// kernel layout exactly.
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoUringSqe {
    opcode: u8,
    flags: u8,
    ioprio: u16,
    fd: i32,
    off: u64,
    addr: u64,
    len: u32,
    op_flags: u32,
    user_data: u64,
    buf_index: u16,
    personality: u16,
    splice_fd_in: i32,
    pad2: [u64; 2],
}

/// Completion-queue entry (16 bytes).
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct IoUringCqe {
    user_data: u64,
    res: i32,
    flags: u32,
}

/// The Linux `errno` numbers the I/O paths can surface. 1 to 34
/// (`asm-generic/errno-base.h`) are shared by every architecture; the rest
/// take their asm-generic values on most, and MIPS and SPARC define their own
/// (verified against each arch's kernel `<asm/errno.h>`), so those are
/// selected per `target_arch`, as the open flags above are.
mod errno {
    pub(super) const EPERM: i32 = 1;
    pub(super) const ENOENT: i32 = 2;
    pub(super) const EINTR: i32 = 4;
    /// Read only by the hole probes, which are 64-bit only.
    #[cfg(target_pointer_width = "64")]
    pub(super) const ENXIO: i32 = 6;
    pub(super) const EBADF: i32 = 9;
    pub(super) const EAGAIN: i32 = 11;
    pub(super) const EBUSY: i32 = 16;
    pub(super) const ENOMEM: i32 = 12;
    pub(super) const EACCES: i32 = 13;
    pub(super) const EEXIST: i32 = 17;
    pub(super) const EXDEV: i32 = 18;
    pub(super) const EINVAL: i32 = 22;
    pub(super) const ENFILE: i32 = 23;
    pub(super) const EMFILE: i32 = 24;
    /// Read only by the copy-on-write switch, on the asm-generic ioctl
    /// architectures.
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    ))]
    pub(super) const ENOTTY: i32 = 25;
    pub(super) const ENOSPC: i32 = 28;
    pub(super) const EROFS: i32 = 30;
    pub(super) const EPIPE: i32 = 32;
    pub(super) use arch::*;

    /// asm-generic.
    #[cfg(not(any(
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6",
        target_arch = "sparc",
        target_arch = "sparc64"
    )))]
    mod arch {
        pub(in super::super) const EOPNOTSUPP: i32 = 95;
        pub(in super::super) const ENETDOWN: i32 = 100;
        pub(in super::super) const ENETUNREACH: i32 = 101;
        pub(in super::super) const ECONNABORTED: i32 = 103;
        pub(in super::super) const ECONNRESET: i32 = 104;
        pub(in super::super) const ENOTCONN: i32 = 107;
        pub(in super::super) const ETIMEDOUT: i32 = 110;
        pub(in super::super) const ECONNREFUSED: i32 = 111;
        pub(in super::super) const EHOSTUNREACH: i32 = 113;
        pub(in super::super) const ESTALE: i32 = 116;
        pub(in super::super) const EDQUOT: i32 = 122;
    }

    /// MIPS.
    #[cfg(any(
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "mips64",
        target_arch = "mips64r6"
    ))]
    mod arch {
        pub(in super::super) const EOPNOTSUPP: i32 = 122;
        pub(in super::super) const ENETDOWN: i32 = 127;
        pub(in super::super) const ENETUNREACH: i32 = 128;
        pub(in super::super) const ECONNABORTED: i32 = 130;
        pub(in super::super) const ECONNRESET: i32 = 131;
        pub(in super::super) const ENOTCONN: i32 = 134;
        pub(in super::super) const ETIMEDOUT: i32 = 145;
        pub(in super::super) const ECONNREFUSED: i32 = 146;
        pub(in super::super) const EHOSTUNREACH: i32 = 148;
        pub(in super::super) const ESTALE: i32 = 151;
        pub(in super::super) const EDQUOT: i32 = 1133;
    }

    /// SPARC.
    #[cfg(any(target_arch = "sparc", target_arch = "sparc64"))]
    mod arch {
        pub(in super::super) const EOPNOTSUPP: i32 = 45;
        pub(in super::super) const ENETDOWN: i32 = 50;
        pub(in super::super) const ENETUNREACH: i32 = 51;
        pub(in super::super) const ECONNABORTED: i32 = 53;
        pub(in super::super) const ECONNRESET: i32 = 54;
        pub(in super::super) const ENOTCONN: i32 = 57;
        pub(in super::super) const ETIMEDOUT: i32 = 60;
        pub(in super::super) const ECONNREFUSED: i32 = 61;
        pub(in super::super) const EHOSTUNREACH: i32 = 65;
        pub(in super::super) const ESTALE: i32 = 70;
        pub(in super::super) const EDQUOT: i32 = 69;
    }
}

/// Maps a raw `errno` to the crate's `no_std` [`ErrorKind`]. Only the ones the
/// I/O paths can realistically surface are mapped; the rest fold into
/// `Other`, and the numeric errno is preserved in the message for diagnosis.
fn errno_to_kind(code: i32) -> ErrorKind {
    match code {
        errno::EPERM | errno::EACCES => ErrorKind::PermissionDenied,
        errno::ENOENT => ErrorKind::NotFound,
        errno::EINTR => ErrorKind::Interrupted,
        errno::EBADF | errno::EINVAL => ErrorKind::InvalidInput,
        // EWOULDBLOCK is EAGAIN on every Linux architecture.
        errno::EAGAIN => ErrorKind::WouldBlock,
        errno::ENOMEM => ErrorKind::OutOfMemory,
        errno::EEXIST => ErrorKind::AlreadyExists,
        errno::EXDEV => ErrorKind::CrossesDevices,
        errno::ENFILE | errno::EMFILE => ErrorKind::TooManyOpenFiles,
        errno::ENOSPC => ErrorKind::StorageFull,
        errno::EROFS => ErrorKind::ReadOnlyFilesystem,
        errno::EPIPE => ErrorKind::BrokenPipe,
        errno::EOPNOTSUPP => ErrorKind::Unsupported,
        errno::ENETDOWN => ErrorKind::NetworkDown,
        errno::ENETUNREACH => ErrorKind::NetworkUnreachable,
        errno::ECONNABORTED => ErrorKind::ConnectionAborted,
        errno::ECONNRESET => ErrorKind::ConnectionReset,
        errno::ENOTCONN => ErrorKind::NotConnected,
        errno::ETIMEDOUT => ErrorKind::TimedOut,
        errno::ECONNREFUSED => ErrorKind::ConnectionRefused,
        errno::EHOSTUNREACH => ErrorKind::HostUnreachable,
        errno::ESTALE => ErrorKind::StaleNetworkFileHandle,
        errno::EDQUOT => ErrorKind::QuotaExceeded,
        _ => ErrorKind::Other,
    }
}

/// Converts a `syscalls::Errno` into a crate [`Error`], preserving the numeric
/// errno in the message.
fn err(syscall: &str, e: Errno) -> Error {
    let raw = e.into_raw();
    Error::new(errno_to_kind(raw), format!("{syscall} failed: errno {raw}"))
}

/// `io_uring_setup(entries, params)` -> ring fd.
///
/// # Safety
/// `params` must point to a valid, writable [`IoUringParams`] the kernel fills
/// in. The caller owns the returned fd and must `close` it.
unsafe fn io_uring_setup(entries: u32, params: *mut IoUringParams) -> Result<i32, Error> {
    // SAFETY: forwarded to the kernel; `params` validity is the caller's
    // contract (documented above). The Result form maps `-errno` to `Err`.
    let fd = unsafe { syscall2(Sysno::io_uring_setup, entries as usize, params as usize) }
        .map_err(|e| err("io_uring_setup", e))?;
    Ok(fd as i32)
}

/// `io_uring_enter(fd, to_submit, min_complete, flags, NULL, 0)`, returning
/// how many SQEs the kernel took, or the raw `errno`, which the caller sorts
/// into the ones to enter again on and the ones that refuse the SQEs.
///
/// # Safety
/// `fd` must be a live ring fd from [`io_uring_setup`].
unsafe fn io_uring_enter_raw(
    fd: i32,
    to_submit: u32,
    min_complete: u32,
    flags: u32,
) -> Result<u32, Errno> {
    // SAFETY: `fd` is a live ring fd (caller contract). `sig`/`sigsz` are
    // NULL/0 — we never block on a signal mask.
    let n = unsafe {
        syscall6(
            Sysno::io_uring_enter,
            fd as usize,
            to_submit as usize,
            min_complete as usize,
            flags as usize,
            0,
            0,
        )
    }?;
    Ok(n as u32)
}

/// Ends the process: the ring failed to report completions it owes, so the
/// kernel may still write into buffers a return would hand back to their
/// owners, and nothing can say when it stops.
// Coverage: needs `io_uring_enter` to fail while waiting, which a healthy
// kernel never does for these arguments.
#[cfg_attr(coverage_nightly, coverage(off))]
fn ring_lost(error: Errno) -> ! {
    log::error!("io_uring_enter failed with reads in flight: {error}; aborting");
    #[cfg(feature = "std")]
    std::process::abort();
    // no-std: no abort in `core`; leaving the process at once is what an
    // abort does here, with the status a SIGABRT death reports.
    #[cfg(not(feature = "std"))]
    loop {
        // SAFETY: `exit_group` takes a status and never returns.
        let _ = unsafe { syscall1(Sysno::exit_group, 134) };
    }
}

/// One read of a batch handed to [`IoUringRaw::read_batch`]: `len` bytes of
/// `fd` from `offset` into `buf`.
#[derive(Clone, Copy)]
struct RawRead {
    fd: i32,
    buf: *mut u8,
    len: u32,
    offset: u64,
}

/// `mmap` wrapper returning the mapped address or a typed error.
///
/// # Safety
/// Standard `mmap` contract. The caller owns the mapping and must `munmap` it.
// Coverage: the `Err` arm requires an `mmap` failure (address-space exhaustion
// or a kernel fault), which cannot be provoked deterministically in CI. The
// success arm IS covered by every ring setup.
#[cfg_attr(coverage_nightly, coverage(off))]
unsafe fn mmap(
    len: usize,
    prot: usize,
    flags: usize,
    fd: i32,
    offset: u64,
) -> Result<*mut u8, Error> {
    // A 32-bit target maps through `mmap2`, whose offset counts 4096-byte
    // units: its `mmap` takes a 32-bit offset, and on x86 is the old form that
    // reads its arguments through a pointer. Every `IORING_OFF_*` is a
    // multiple of 4096.
    #[cfg(target_pointer_width = "64")]
    let (nr, off) = (Sysno::mmap, offset as usize);
    #[cfg(target_pointer_width = "32")]
    let (nr, off) = {
        debug_assert!(
            offset.is_multiple_of(4096),
            "mmap2 offset is in 4096-byte units"
        );
        (Sysno::mmap2, (offset / 4096) as usize)
    };
    // SAFETY: forwarded to the kernel. The raw `mmap` syscall returns `-errno`
    // (in `[-4095, -1]`) on failure, which the `syscalls` Result form maps to
    // `Err` for us; a success is the actual mapping address (page 0 included,
    // though the kernel does not hand it out for a NULL hint).
    let ret = unsafe { syscall6(nr, 0, len, prot, flags, fd as usize, off) };
    match ret {
        Ok(addr) => Ok(addr as *mut u8),
        Err(e) => Err(err("mmap", e)),
    }
}

/// `munmap(addr, len)`. Errors are swallowed (best-effort teardown).
///
/// # Safety
/// `addr`/`len` must come from a prior [`mmap`] of exactly this region.
unsafe fn munmap(addr: *mut u8, len: usize) {
    // SAFETY: caller passes a region from a prior `mmap`. Teardown is
    // best-effort: a failed unmap cannot be meaningfully handled in `Drop`.
    let _ = unsafe { syscall2(Sysno::munmap, addr as usize, len) };
}

/// `close(fd)`, best-effort.
///
/// # Safety
/// `fd` must be a live descriptor this code owns.
unsafe fn close(fd: i32) {
    // SAFETY: `fd` is owned by the caller (the ring fd in `Drop`).
    let _ = unsafe { syscall1(Sysno::close, fd as usize) };
}

/// Opens a file via the raw `openat(AT_FDCWD, path, flags, mode)` syscall,
/// returning the new descriptor.
///
/// Path resolution is relative to the process cwd; `mode` only applies when
/// `flags` requests creation (`O_CREAT`). File open is a one-shot control
/// operation, so it goes through a plain blocking syscall rather than the
/// ring; the ring is reserved for the hot read / write / fsync data path.
///
/// # Errors
/// Returns an [`Error`] if the `openat` syscall fails.
pub fn open_raw(path: &core::ffi::CStr, flags: i32, mode: u32) -> Result<i32, Error> {
    // SAFETY: `path` is a valid, NUL-terminated C string for the call's
    // duration (borrowed `&CStr`); the kernel only reads it.
    let fd = unsafe {
        syscall4(
            Sysno::openat,
            AT_FDCWD as usize,
            path.as_ptr() as usize,
            (flags | O_LARGEFILE) as usize,
            mode as usize,
        )
    }
    .map_err(|e| err("openat", e))?;
    Ok(fd as i32)
}

/// Closes a descriptor returned by [`open_raw`].
///
/// # Errors
/// Returns an [`Error`] if `close` fails (e.g. a write-back error flushed at
/// close time on some filesystems).
pub fn close_raw(fd: i32) -> Result<(), Error> {
    // SAFETY: `fd` is a descriptor the caller owns.
    unsafe { syscall1(Sysno::close, fd as usize) }.map_err(|e| err("close", e))?;
    Ok(())
}

// ---- Cold-path file syscalls (metadata / truncate / lock / seek) ----
//
// The std `io_uring` backend delegates these to `std::fs::File`; the `no_std`
// raw backend cannot, so they go through plain blocking syscalls (only the
// hot read / write / fsync data path uses the ring).

/// `lseek` whence: position relative to the end (used to resolve file size).
const SEEK_END: i32 = 2;

/// `flock` operation: exclusive lock.
const LOCK_EX: i32 = 2;
/// `flock` operation bit: non-blocking (fail with `EWOULDBLOCK` instead of waiting).
const LOCK_NB: i32 = 4;

/// `statx` flag: operate on `dirfd` itself when the path is empty (stat an fd).
const AT_EMPTY_PATH: i32 = 0x1000;
/// `statx` mask: request the basic stat fields (type, mode, size, …).
const STATX_BASIC_STATS: u32 = 0x0000_07ff;
/// `statx` mask bit: `stx_ino` was filled.
const STATX_INO: u32 = 0x0000_0100;
/// `st_mode` file-type mask and the regular-file / directory type bits.
const S_IFMT: u16 = 0o170_000;
const S_IFDIR: u16 = 0o040_000;
const S_IFREG: u16 = 0o100_000;

/// A `statx_timestamp` (`linux/stat.h`).
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct StatxTimestamp {
    tv_sec: i64,
    tv_nsec: u32,
    _reserved: i32,
}

/// The kernel `struct statx` (256 bytes, architecture-independent — unlike the
/// arch-specific `struct stat`, which is why `statx` is used here). Only
/// `stx_mode` and `stx_size` are read; the remaining fields keep the layout the
/// exact size the kernel writes into.
#[repr(C)]
#[derive(Default, Clone, Copy)]
struct Statx {
    stx_mask: u32,
    stx_blksize: u32,
    stx_attributes: u64,
    stx_nlink: u32,
    stx_uid: u32,
    stx_gid: u32,
    stx_mode: u16,
    _spare0: u16,
    stx_ino: u64,
    stx_size: u64,
    stx_blocks: u64,
    stx_attributes_mask: u64,
    stx_atime: StatxTimestamp,
    stx_btime: StatxTimestamp,
    stx_ctime: StatxTimestamp,
    stx_mtime: StatxTimestamp,
    stx_rdev_major: u32,
    stx_rdev_minor: u32,
    stx_dev_major: u32,
    stx_dev_minor: u32,
    stx_mnt_id: u64,
    stx_dio_mem_align: u32,
    stx_dio_offset_align: u32,
    _spare3: [u64; 12],
}

/// File type + size, the subset of `statx` the [`Fs`] metadata
/// surface needs.
pub(crate) struct RawMetadata {
    /// File length in bytes (`stx_size`).
    pub(crate) size: u64,
    /// Number of hard links to the underlying inode (`stx_nlink`). A count above
    /// 1 means another name shares the same bytes, so in-place mutation would
    /// rewrite that snapshot too.
    pub(crate) nlink: u64,
    /// Whether the entry is a directory (`S_IFDIR`).
    pub(crate) is_dir: bool,
    /// Whether the entry is a regular file (`S_IFREG`).
    pub(crate) is_file: bool,
}

/// `statx(fd, "", AT_EMPTY_PATH, STATX_BASIC_STATS, &buf)` — stats an open
/// descriptor without a path lookup.
///
/// # Errors
/// Returns an [`Error`] if the `statx` syscall fails.
pub(crate) fn fstat_raw(fd: i32) -> Result<RawMetadata, Error> {
    let mut buf = Statx::default();
    // Empty path + AT_EMPTY_PATH makes statx operate on `fd` directly.
    let empty: &core::ffi::CStr = c"";
    // SAFETY: `buf` is a valid, writable Statx the kernel fills; `empty` is a
    // valid NUL-terminated C string read by the kernel.
    unsafe {
        syscall5(
            Sysno::statx,
            fd as usize,
            empty.as_ptr() as usize,
            AT_EMPTY_PATH as usize,
            STATX_BASIC_STATS as usize,
            &raw mut buf as usize,
        )
    }
    .map_err(|e| err("statx", e))?;
    let kind = buf.stx_mode & S_IFMT;
    Ok(RawMetadata {
        size: buf.stx_size,
        nlink: u64::from(buf.stx_nlink),
        is_dir: kind == S_IFDIR,
        is_file: kind == S_IFREG,
    })
}

/// `ftruncate(fd, length)` — set the file size (extend with zeros or truncate).
///
/// # Errors
/// Returns an [`Error`] if the `ftruncate` syscall fails.
pub(crate) fn ftruncate_raw(fd: i32, length: u64) -> Result<(), Error> {
    // SAFETY: `fd` is an owned writable descriptor; `length` is a plain value.
    #[cfg(target_pointer_width = "64")]
    let ret = unsafe { syscall2(Sysno::ftruncate, fd as usize, length as usize) };
    // A 32-bit `ftruncate` takes a 32-bit length; `ftruncate64` takes it in
    // a register pair, which arm, MIPS and powerpc start at an even register
    // (glibc `ftruncate64.c`: `__ALIGNMENT_ARG SYSCALL_LL64 (length)`).
    #[cfg(target_pointer_width = "32")]
    let ret = {
        let (a, b) = reg_pair(length);
        // SAFETY: as above.
        unsafe {
            if ALIGNED_REGISTER_PAIRS {
                syscall4(Sysno::ftruncate64, fd as usize, 0, a, b)
            } else {
                syscall3(Sysno::ftruncate64, fd as usize, a, b)
            }
        }
    };
    ret.map_err(|e| err("ftruncate", e))?;
    Ok(())
}

/// Whether a 64-bit syscall argument starts at an even register on this
/// 32-bit target (glibc `__ASSUME_ALIGNED_REGISTER_PAIRS`).
#[cfg(target_pointer_width = "32")]
const ALIGNED_REGISTER_PAIRS: bool = cfg!(any(
    target_arch = "arm",
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "powerpc"
));

/// The two registers a 64-bit syscall argument takes on a 32-bit target, in
/// the target's byte order (glibc `SYSCALL_LL64`).
#[cfg(target_pointer_width = "32")]
fn reg_pair(v: u64) -> (usize, usize) {
    let (lo, hi) = (v as u32 as usize, (v >> 32) as usize);
    if cfg!(target_endian = "little") {
        (lo, hi)
    } else {
        (hi, lo)
    }
}

/// `sync_file_range(fd, offset, len, SYNC_FILE_RANGE_WRITE)`: starts writing
/// back the range without waiting for it. 64-bit targets only, where each
/// 64-bit argument is one register.
///
/// # Errors
/// Returns an [`Error`] if the `sync_file_range` syscall fails.
#[cfg(target_pointer_width = "64")]
pub(crate) fn start_writeback_raw(fd: i32, offset: u64, len: u64) -> Result<(), Error> {
    /// `SYNC_FILE_RANGE_WRITE` from `<linux/fs.h>`.
    const SYNC_FILE_RANGE_WRITE: usize = 2;
    // SAFETY: `fd` is an owned descriptor; offset, length and flags are plain
    // values the kernel bounds-checks.
    unsafe {
        syscall4(
            Sysno::sync_file_range,
            fd as usize,
            offset as usize,
            len as usize,
            SYNC_FILE_RANGE_WRITE,
        )
    }
    .map_err(|e| err("sync_file_range", e))?;
    Ok(())
}

/// `fcntl(fd, F_SET_RW_HINT, &hint)`: tells the device how long the data
/// written through `fd` is expected to live. The constants are Linux uapi
/// `include/uapi/linux/fcntl.h`; the argument is a pointer to a `u64`.
///
/// # Errors
/// Returns an [`Error`] if the `fcntl` syscall fails.
pub(crate) fn set_write_lifetime_raw(fd: i32, lifetime: super::WriteLifetime) -> Result<(), Error> {
    /// `F_LINUX_SPECIFIC_BASE + 12`.
    const F_SET_RW_HINT: usize = 1024 + 12;
    let hint: u64 = match lifetime {
        super::WriteLifetime::Short => 2,
        super::WriteLifetime::Medium => 3,
        super::WriteLifetime::Long => 4,
        super::WriteLifetime::Extreme => 5,
    };
    // SAFETY: `fd` is an owned descriptor and `hint` outlives the call; the
    // kernel only reads the `u64` the pointer names.
    unsafe {
        syscall3(
            Sysno::fcntl,
            fd as usize,
            F_SET_RW_HINT,
            (&raw const hint) as usize,
        )
    }
    .map_err(|e| err("fcntl(F_SET_RW_HINT)", e))?;
    Ok(())
}

/// Clears `O_NONBLOCK` from `fd`'s status flags, which a `no_wait` open set
/// for the open alone.
///
/// # Errors
/// Returns an [`Error`] if an `fcntl` syscall fails.
pub(crate) fn clear_nonblock_raw(fd: i32) -> Result<(), Error> {
    /// `F_GETFL`, the same on every Linux architecture.
    const F_GETFL: usize = 3;
    /// `F_SETFL`, the same on every Linux architecture.
    const F_SETFL: usize = 4;
    // SAFETY: `fd` is an owned descriptor; `F_GETFL` takes no argument.
    let flags = unsafe { syscall3(Sysno::fcntl, fd as usize, F_GETFL, 0) }
        .map_err(|e| err("fcntl(F_GETFL)", e))?;
    let cleared = flags & !(O_NONBLOCK.unsigned_abs() as usize);
    // SAFETY: as above; `F_SETFL` takes the flags as an integer.
    unsafe { syscall3(Sysno::fcntl, fd as usize, F_SETFL, cleared) }
        .map_err(|e| err("fcntl(F_SETFL)", e))?;
    Ok(())
}

/// `lseek(fd, offset, whence)` — reposition; returns the resulting absolute
/// offset. Used to resolve the file size (`SEEK_END`) for append / `Seek::End`.
///
/// # Errors
/// Returns an [`Error`] if the `lseek` syscall fails.
pub(crate) fn lseek_raw(fd: i32, offset: i64, whence: i32) -> Result<u64, Error> {
    lseek_errno(fd, offset, whence).map_err(|e| err("lseek", e))
}

/// [`lseek_raw`], handing back the raw `errno` for a caller that tells
/// failures apart (the hole probes read `ENXIO` as an answer).
#[cfg(target_pointer_width = "64")]
fn lseek_errno(fd: i32, offset: i64, whence: i32) -> Result<u64, Errno> {
    // SAFETY: `fd` is an owned descriptor; offset/whence are plain values.
    let pos = unsafe { syscall3(Sysno::lseek, fd as usize, offset as usize, whence as usize) }?;
    Ok(pos as u64)
}

/// [`lseek_raw`] on a 32-bit target, where `lseek` takes and returns a 32-bit
/// offset: `_llseek` takes the offset high word first and writes the 64-bit
/// result through a pointer (glibc `lseek64.c`).
#[cfg(target_pointer_width = "32")]
fn lseek_errno(fd: i32, offset: i64, whence: i32) -> Result<u64, Errno> {
    #[cfg(target_arch = "riscv32")]
    let nr = Sysno::llseek;
    #[cfg(not(target_arch = "riscv32"))]
    let nr = Sysno::_llseek;
    let mut pos: u64 = 0;
    // SAFETY: `fd` is an owned descriptor; the offset words and `whence` are
    // plain values, and `pos` outlives the call, which writes only it.
    unsafe {
        syscall5(
            nr,
            fd as usize,
            ((offset as u64) >> 32) as usize,
            offset as u32 as usize,
            (&raw mut pos) as usize,
            whence as usize,
        )
    }?;
    Ok(pos)
}

/// `flock(fd, LOCK_EX[|LOCK_NB])` — take an advisory exclusive lock. With
/// `non_blocking`, returns `Ok(false)` instead of waiting when the lock is held.
///
/// # Errors
/// Returns an [`Error`] if the `flock` syscall fails for a reason other than the
/// non-blocking contention case.
pub(crate) fn flock_exclusive_raw(fd: i32, non_blocking: bool) -> Result<bool, Error> {
    let op = if non_blocking {
        LOCK_EX | LOCK_NB
    } else {
        LOCK_EX
    };
    loop {
        // SAFETY: `fd` is an owned descriptor; `op` is a valid flock operation.
        match unsafe { syscall2(Sysno::flock, fd as usize, op as usize) } {
            Ok(_) => return Ok(true),
            // EINTR (4): interrupted by a signal mid-syscall — retry, so a stray
            // signal does not spuriously fail the lock (matches std's flock).
            Err(e) if e.into_raw() == 4 => {}
            // EWOULDBLOCK / EAGAIN (11): held by someone else, non-blocking request.
            Err(e) if non_blocking && e.into_raw() == 11 => return Ok(false),
            Err(e) => return Err(err("flock", e)),
        }
    }
}

/// Resolves the current file size via `lseek(fd, 0, SEEK_END)`.
///
/// # Errors
/// Returns an [`Error`] if the `lseek` syscall fails.
pub(crate) fn file_size_raw(fd: i32) -> Result<u64, Error> {
    lseek_raw(fd, 0, SEEK_END)
}

// ---- Directory / path syscalls (for the `Fs` backend) ----
//
// `O_APPEND` and `O_EXCL` take the asm-generic values, which every realistic
// `io_uring` target shares. The flags MIPS and SPARC move are not remapped
// here, matching the `direct_io` policy documented on `FsOpenOptions`.
// `O_DIRECTORY` and `O_NOFOLLOW` differ on arm, aarch64, powerpc and m68k,
// which keep their own `fcntl.h` (`arch/arm64/include/uapi/asm/fcntl.h`:
// `O_DIRECTORY 040000`, `O_NOFOLLOW 0100000`, `O_DIRECT 0200000`): the
// asm-generic `O_DIRECTORY` is `O_DIRECT` there.

/// Append: writes always land at end of file.
const O_APPEND: i32 = 0o2000;
/// Do not block in the open on a named pipe with no other end. Asm-generic
/// (`0o4000`, arm, aarch64 and powerpc alike); MIPS and SPARC override below.
#[cfg(not(any(
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    target_arch = "sparc",
    target_arch = "sparc64"
)))]
const O_NONBLOCK: i32 = 0o4000;
/// Do not block in the open on a named pipe with no other end (MIPS: `0x80`).
#[cfg(any(
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6"
))]
const O_NONBLOCK: i32 = 0x80;
/// Do not block in the open on a named pipe with no other end (SPARC:
/// `0x4000`).
#[cfg(any(target_arch = "sparc", target_arch = "sparc64"))]
const O_NONBLOCK: i32 = 0x4000;
/// Fail `open` with `EEXIST` if the path already exists (with `O_CREAT`).
const O_EXCL: i32 = 0o200;
/// Require the opened path to be a directory (arm, aarch64, powerpc, m68k).
#[cfg(any(
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "m68k"
))]
const O_DIRECTORY: i32 = 0o40_000;
/// Require the opened path to be a directory.
#[cfg(not(any(
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "m68k"
)))]
const O_DIRECTORY: i32 = 0o200_000;
/// Fail `open` with `ELOOP` if the last path component is a symbolic link
/// (arm, aarch64, powerpc, m68k).
#[cfg(any(
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "m68k"
))]
const O_NOFOLLOW: i32 = 0o100_000;
/// Fail `open` with `ELOOP` if the last path component is a symbolic link.
#[cfg(not(any(
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "powerpc",
    target_arch = "powerpc64",
    target_arch = "m68k"
)))]
const O_NOFOLLOW: i32 = 0o400_000;
/// Lets a 32-bit process open and write a file of 2 GiB or more; a 64-bit
/// kernel sets it on every open itself. Asm-generic value (`0o100000`).
#[cfg(all(
    target_pointer_width = "32",
    not(any(
        target_arch = "arm",
        target_arch = "powerpc",
        target_arch = "mips",
        target_arch = "mips32r6",
        target_arch = "sparc"
    ))
))]
const O_LARGEFILE: i32 = 0o100_000;
/// Lets a 32-bit process open and write a file of 2 GiB or more (arm).
#[cfg(all(target_pointer_width = "32", target_arch = "arm"))]
const O_LARGEFILE: i32 = 0o400_000;
/// Lets a 32-bit process open and write a file of 2 GiB or more (powerpc).
#[cfg(all(target_pointer_width = "32", target_arch = "powerpc"))]
const O_LARGEFILE: i32 = 0o200_000;
/// Lets a 32-bit process open and write a file of 2 GiB or more (MIPS).
#[cfg(all(
    target_pointer_width = "32",
    any(target_arch = "mips", target_arch = "mips32r6")
))]
const O_LARGEFILE: i32 = 0x2000;
/// Lets a 32-bit process open and write a file of 2 GiB or more (SPARC).
#[cfg(all(target_pointer_width = "32", target_arch = "sparc"))]
const O_LARGEFILE: i32 = 0x4_0000;
/// Set by a 64-bit kernel on every open, so nothing to add.
#[cfg(target_pointer_width = "64")]
const O_LARGEFILE: i32 = 0;

#[cfg(all(
    target_pointer_width = "32",
    any(target_arch = "x86", target_arch = "arm")
))]
const _: () = assert!(O_LARGEFILE == libc::O_LARGEFILE);

// The open flags this module spells out match the C library's on the target,
// checked as it builds: several differ per architecture, and a wrong one
// opens with another flag's meaning. A comparison of constants, so nothing of
// the C library is linked. MIPS and SPARC are left out, their divergent
// `O_APPEND` / `O_EXCL` / `O_DIRECTORY` / `O_NOFOLLOW` not being remapped here.
#[cfg(not(any(
    target_arch = "mips",
    target_arch = "mips32r6",
    target_arch = "mips64",
    target_arch = "mips64r6",
    target_arch = "sparc",
    target_arch = "sparc64"
)))]
const _: () = {
    assert!(O_CREAT == libc::O_CREAT);
    assert!(O_TRUNC == libc::O_TRUNC);
    assert!(O_APPEND == libc::O_APPEND);
    assert!(O_EXCL == libc::O_EXCL);
    assert!(O_DIRECTORY == libc::O_DIRECTORY);
    assert!(O_NOFOLLOW == libc::O_NOFOLLOW);
    assert!(O_NONBLOCK == libc::O_NONBLOCK);
};

/// `unlinkat` flag: remove a directory (`rmdir`) instead of a file.
const AT_REMOVEDIR: i32 = 0x200;

/// `linux_dirent64.d_type`: directory.
const DT_DIR: u8 = 4;

/// `mkdirat(AT_FDCWD, path, mode)` — create a directory.
///
/// # Errors
/// Returns an [`Error`] if the `mkdirat` syscall fails.
pub(crate) fn mkdirat_raw(path: &core::ffi::CStr, mode: u32) -> Result<(), Error> {
    // SAFETY: `path` is a valid NUL-terminated C string the kernel only reads.
    unsafe {
        syscall3(
            Sysno::mkdirat,
            AT_FDCWD as usize,
            path.as_ptr() as usize,
            mode as usize,
        )
    }
    .map_err(|e| err("mkdirat", e))?;
    Ok(())
}

/// `unlinkat(AT_FDCWD, path, flags)` — remove a file (`flags == 0`) or directory
/// (`flags == AT_REMOVEDIR`).
///
/// # Errors
/// Returns an [`Error`] if the `unlinkat` syscall fails.
pub(crate) fn unlinkat_raw(path: &core::ffi::CStr, remove_dir: bool) -> Result<(), Error> {
    let flags = if remove_dir { AT_REMOVEDIR } else { 0 };
    // SAFETY: `path` is a valid NUL-terminated C string the kernel only reads.
    unsafe {
        syscall3(
            Sysno::unlinkat,
            AT_FDCWD as usize,
            path.as_ptr() as usize,
            flags as usize,
        )
    }
    .map_err(|e| err("unlinkat", e))?;
    Ok(())
}

/// `symlinkat(target, AT_FDCWD, link)`: makes `link` a symbolic link holding
/// `target`, which is stored as given and need not exist.
///
/// # Errors
/// Returns an [`Error`] if the `symlinkat` syscall fails.
pub(crate) fn symlinkat_raw(target: &core::ffi::CStr, link: &core::ffi::CStr) -> Result<(), Error> {
    // SAFETY: both paths are valid NUL-terminated C strings the kernel reads.
    unsafe {
        syscall3(
            Sysno::symlinkat,
            target.as_ptr() as usize,
            AT_FDCWD as usize,
            link.as_ptr() as usize,
        )
    }
    .map_err(|e| err("symlinkat", e))?;
    Ok(())
}

/// `renameat2(AT_FDCWD, from, AT_FDCWD, to, 0)` — rename, replacing the
/// destination (the plain `rename(2)` behaviour).
///
/// # Errors
/// Returns an [`Error`] if the `renameat2` syscall fails.
pub(crate) fn renameat2_raw(from: &core::ffi::CStr, to: &core::ffi::CStr) -> Result<(), Error> {
    // SAFETY: both paths are valid NUL-terminated C strings the kernel reads.
    unsafe {
        syscall5(
            Sysno::renameat2,
            AT_FDCWD as usize,
            from.as_ptr() as usize,
            AT_FDCWD as usize,
            to.as_ptr() as usize,
            0,
        )
    }
    .map_err(|e| err("renameat2", e))?;
    Ok(())
}

/// `statx(AT_FDCWD, path, 0, STATX_BASIC_STATS, &buf)` — stat a path (follows
/// symlinks). Returns [`None`] if the path does not exist (`ENOENT`).
///
/// # Errors
/// Returns an [`Error`] if `statx` fails for a reason other than `ENOENT`.
pub(crate) fn statx_path_raw(path: &core::ffi::CStr) -> Result<Option<RawMetadata>, Error> {
    Ok(statx_at(path)?.map(|buf| {
        let kind = buf.stx_mode & S_IFMT;
        RawMetadata {
            size: buf.stx_size,
            nlink: u64::from(buf.stx_nlink),
            is_dir: kind == S_IFDIR,
            is_file: kind == S_IFREG,
        }
    }))
}

/// The whole `statx` of a path (following symlinks), or [`None`] if the path
/// does not exist (`ENOENT`).
///
/// # Errors
/// Returns an [`Error`] if `statx` fails for a reason other than `ENOENT`.
fn statx_at(path: &core::ffi::CStr) -> Result<Option<Statx>, Error> {
    let mut buf = Statx::default();
    // SAFETY: `path` is a valid NUL-terminated C string; `buf` is a valid,
    // writable Statx the kernel fills.
    let r = unsafe {
        syscall5(
            Sysno::statx,
            AT_FDCWD as usize,
            path.as_ptr() as usize,
            0,
            STATX_BASIC_STATS as usize,
            &raw mut buf as usize,
        )
    };
    match r {
        Ok(_) => Ok(Some(buf)),
        // The path does not exist: a normal "not found" answer.
        Err(e) if e.into_raw() == errno::ENOENT => Ok(None),
        Err(e) => Err(err("statx", e)),
    }
}

/// The file a path names, following symlinks: its device (major, minor) and
/// inode, from `statx(AT_FDCWD, path, 0, STATX_BASIC_STATS, &buf)`.
///
/// # Errors
/// Returns an [`Error`] if `statx` fails, `ENOENT` included: a name that does
/// not exist has no identity to compare.
pub(crate) fn statx_identity_raw(path: &core::ffi::CStr) -> Result<(u32, u32, u64), Error> {
    let mut buf = Statx::default();
    // SAFETY: `path` is a valid NUL-terminated C string; `buf` is a valid,
    // writable Statx the kernel fills.
    unsafe {
        syscall5(
            Sysno::statx,
            AT_FDCWD as usize,
            path.as_ptr() as usize,
            0,
            STATX_BASIC_STATS as usize,
            &raw mut buf as usize,
        )
    }
    .map_err(|e| err("statx", e))?;
    // A filesystem that cannot report an inode leaves `stx_ino` zero; two
    // files would then compare as one, so the identity is not known.
    if buf.stx_mask & STATX_INO == 0 {
        return Err(Error::new(
            ErrorKind::Unsupported,
            "statx: the filesystem reports no inode",
        ));
    }
    Ok((buf.stx_dev_major, buf.stx_dev_minor, buf.stx_ino))
}

/// The longest symlink target read: `PATH_MAX` on Linux.
const PATH_MAX: usize = 4096;

/// `readlinkat(AT_FDCWD, path, buf, PATH_MAX)` — the target of a symlink, read
/// without following it. Returns [`None`] when `path` does not exist
/// (`ENOENT`) or is not a symlink (`EINVAL`).
///
/// # Errors
/// Returns an [`Error`] if `readlinkat` fails for another reason, or the
/// target fills the whole buffer (`ENAMETOOLONG`: it may have been cut).
pub(crate) fn readlinkat_raw(path: &core::ffi::CStr) -> Result<Option<Vec<u8>>, Error> {
    let mut buf = alloc::vec![0u8; PATH_MAX];
    // SAFETY: `path` is a valid NUL-terminated C string the kernel reads;
    // `buf` is a valid, writable buffer of `buf.len()` bytes.
    let r = unsafe {
        syscall4(
            Sysno::readlinkat,
            AT_FDCWD as usize,
            path.as_ptr() as usize,
            buf.as_mut_ptr() as usize,
            buf.len(),
        )
    };
    match r {
        // A target that fills the buffer may continue past it.
        Ok(n) if n >= buf.len() => Err(Error::new(
            ErrorKind::InvalidInput,
            "readlinkat: symlink target longer than PATH_MAX",
        )),
        Ok(n) => {
            buf.truncate(n);
            Ok(Some(buf))
        }
        // ENOENT (2): no such entry; EINVAL (22): the entry is not a symlink.
        Err(e) if matches!(e.into_raw(), 2 | 22) => Ok(None),
        Err(e) => Err(err("readlinkat", e)),
    }
}

/// The kernel `struct statfs` for x86-64 / aarch64 Linux. Unlike `statx`, this
/// struct is architecture-dependent: the field widths below (and the syscall's
/// expected buffer layout) match the verified x86-64 / aarch64 ABI, so it is
/// gated to exactly those targets. Other 64-bit Linux architectures (e.g.
/// s390x) place `f_bavail` / `f_frsize` at different offsets, so reading this
/// layout there would yield bogus free space; they fall back to the trait
/// default (`u64::MAX`, no disk-pressure signal). A 32-bit build would likewise
/// read the kernel's 32-bit `statfs` into the wrong layout. Only `f_frsize` and
/// `f_bavail` are read.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
#[repr(C)]
#[derive(Default, Clone, Copy)]
#[allow(
    clippy::struct_field_names,
    reason = "field names mirror the kernel `struct statfs` (f_type, f_bsize, …) verbatim"
)]
struct Statfs {
    f_type: i64,
    f_bsize: i64,
    f_blocks: u64,
    f_bfree: u64,
    f_bavail: u64,
    f_files: u64,
    f_ffree: u64,
    f_fsid: [i32; 2],
    f_namelen: i64,
    f_frsize: i64,
    f_flags: i64,
    f_spare: [i64; 4],
}

/// `statfs(path, &buf)` — bytes available to an unprivileged process on the
/// filesystem backing `path`: `f_bavail * f_frsize`. Raw syscall (no libc),
/// matching this backend's no-libc design. x86-64 / aarch64 only (see
/// [`Statfs`]).
///
/// `f_frsize` (fundamental fragment size), not `f_bsize` (preferred transfer
/// block size), to match `available_space`'s contract and the `statvfs`-based
/// std backend (`f_bavail * f_frsize`).
///
/// # Errors
/// Returns an [`Error`] if the `statfs` syscall fails.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub(crate) fn statfs_available_raw(path: &core::ffi::CStr) -> Result<u64, Error> {
    let mut buf = Statfs::default();
    // SAFETY: `path` is a valid NUL-terminated C string the kernel reads;
    // `buf` is a valid, writable Statfs the kernel fills on success.
    unsafe { syscall2(Sysno::statfs, path.as_ptr() as usize, &raw mut buf as usize) }
        .map_err(|e| err("statfs", e))?;
    // `f_bavail` counts blocks free to a non-root caller; `f_frsize` is the
    // fundamental block size. Saturate so an implausible value can never wrap.
    let frsize = buf.f_frsize as u64;
    Ok(buf.f_bavail.saturating_mul(frsize))
}

/// The `f_type` magic of the file system backing `path`, from `statfs(path,
/// &buf)`. x86-64 / aarch64 only (see [`Statfs`]).
///
/// # Errors
/// Returns an [`Error`] if the `statfs` syscall fails.
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
fn statfs_type_raw(path: &core::ffi::CStr) -> Result<i64, Error> {
    let mut buf = Statfs::default();
    // SAFETY: `path` is a valid NUL-terminated C string the kernel reads;
    // `buf` is a valid, writable Statfs the kernel fills on success.
    unsafe { syscall2(Sysno::statfs, path.as_ptr() as usize, &raw mut buf as usize) }
        .map_err(|e| err("statfs", e))?;
    Ok(buf.f_type)
}

// ---- File system capability syscalls ----
//
// The operations `StdFs` reaches through libc on Linux, issued here as plain
// syscalls with the same semantics: the same flags, the same errno read as an
// answer or as "cannot tell". The constants are the kernel uapi values; those
// libc also defines are checked against it as the crate builds.

/// `statx` mask bit: `stx_blocks` was filled.
const STATX_BLOCKS: u32 = 0x0000_0400;
/// `fallocate` mode bits: keep the file size, deallocate the range.
#[cfg(target_pointer_width = "64")]
const FALLOC_FL_KEEP_SIZE: i32 = 0x01;
#[cfg(target_pointer_width = "64")]
const FALLOC_FL_PUNCH_HOLE: i32 = 0x02;
/// `lseek` whences: the next byte backed by data, the next hole.
#[cfg(target_pointer_width = "64")]
const SEEK_DATA: i32 = 3;
#[cfg(target_pointer_width = "64")]
const SEEK_HOLE: i32 = 4;
/// `linkat` flags: do not follow a symlink at the source, as `link(2)`.
const LINKAT_NO_FOLLOW: usize = 0;

/// `posix_fadvise` advice values. `POSIX_FADV_DONTNEED` is 6 on s390x, 4
/// elsewhere.
#[cfg(target_pointer_width = "64")]
const POSIX_FADV_NORMAL: usize = 0;
#[cfg(target_pointer_width = "64")]
const POSIX_FADV_RANDOM: usize = 1;
#[cfg(target_pointer_width = "64")]
const POSIX_FADV_SEQUENTIAL: usize = 2;
#[cfg(all(target_pointer_width = "64", not(target_arch = "s390x")))]
const POSIX_FADV_DONTNEED: usize = 4;
#[cfg(all(target_pointer_width = "64", target_arch = "s390x"))]
const POSIX_FADV_DONTNEED: usize = 6;

/// An `ioctl` request in the asm-generic encoding (direction in bits 30-31,
/// argument size in bits 16-29, type, number), which these architectures use;
/// powerpc, mips and sparc lay the bits out otherwise and keep the trait
/// defaults.
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
const fn ioctl_request(direction: u32, kind: u8, number: u8, size: usize) -> u32 {
    (direction << 30) | ((size as u32) << 16) | ((kind as u32) << 8) | number as u32
}

/// `FICLONE` (`_IOW(0x94, 9, int)`): clone the source descriptor's blocks into
/// the target.
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
const FICLONE: u32 = ioctl_request(1, 0x94, 9, core::mem::size_of::<i32>());
/// `FS_IOC_GETFLAGS` / `FS_IOC_SETFLAGS` (`_IOR` / `_IOW('f', 1|2, long)`):
/// the request carries the target's `long` width, though the kernel moves an
/// `int`.
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
const FS_IOC_GETFLAGS: u32 = ioctl_request(2, b'f', 1, core::mem::size_of::<core::ffi::c_long>());
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
const FS_IOC_SETFLAGS: u32 = ioctl_request(1, b'f', 2, core::mem::size_of::<core::ffi::c_long>());
/// The inode flag that turns per-file copy-on-write off.
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
const FS_NOCOW_FL: i32 = 0x0080_0000;

// The values above that the C library also defines match it on the target,
// checked as it builds, as the open flags are: a comparison of constants,
// nothing of the C library is linked.
#[cfg(target_pointer_width = "64")]
const _: () = {
    assert!(FALLOC_FL_KEEP_SIZE == libc::FALLOC_FL_KEEP_SIZE);
    assert!(FALLOC_FL_PUNCH_HOLE == libc::FALLOC_FL_PUNCH_HOLE);
    assert!(SEEK_DATA == libc::SEEK_DATA);
    assert!(SEEK_HOLE == libc::SEEK_HOLE);
    assert!(POSIX_FADV_NORMAL == libc::POSIX_FADV_NORMAL as usize);
    assert!(POSIX_FADV_RANDOM == libc::POSIX_FADV_RANDOM as usize);
    assert!(POSIX_FADV_SEQUENTIAL == libc::POSIX_FADV_SEQUENTIAL as usize);
    assert!(POSIX_FADV_DONTNEED == libc::POSIX_FADV_DONTNEED as usize);
};
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
#[cfg_attr(
    target_pointer_width = "32",
    expect(
        clippy::unnecessary_cast,
        reason = "libc::Ioctl is u32 on a 32-bit target and u64 on a 64-bit one"
    )
)]
const _: () = {
    assert!(FICLONE == libc::FICLONE as u32);
    assert!(FS_IOC_GETFLAGS == libc::FS_IOC_GETFLAGS as u32);
    assert!(FS_IOC_SETFLAGS == libc::FS_IOC_SETFLAGS as u32);
};

/// A descriptor opened for one probe, closed when the probe is over. Its users
/// are the 64-bit range syscalls and the asm-generic ioctls.
#[cfg(any(
    target_pointer_width = "64",
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "riscv32"
))]
struct ProbeFd(i32);

#[cfg(any(
    target_pointer_width = "64",
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "riscv32"
))]
impl ProbeFd {
    /// `path` opened with `flags`.
    fn open(path: &core::ffi::CStr, flags: i32) -> Result<Self, Error> {
        open_raw(path, flags, 0).map(Self)
    }
}

#[cfg(any(
    target_pointer_width = "64",
    target_arch = "x86",
    target_arch = "arm",
    target_arch = "riscv32"
))]
impl Drop for ProbeFd {
    fn drop(&mut self) {
        // Best-effort: nothing was written through it.
        let _ = close_raw(self.0);
    }
}

/// The `st_dev` the C library builds from a `statx` device pair, as `std`
/// reports it: two paths on one mount share it.
fn makedev(major: u32, minor: u32) -> u64 {
    let (major, minor) = (u64::from(major), u64::from(minor));
    ((major & 0xffff_f000) << 32)
        | ((major & 0x0000_0fff) << 8)
        | ((minor & 0xffff_ff00) << 12)
        | (minor & 0x0000_00ff)
}

/// A byte range as the `off_t` pair a syscall takes, and its end.
///
/// # Errors
/// The offset or the length exceeds `i64::MAX`, or the end overflows: no real
/// file extent.
#[cfg(target_pointer_width = "64")]
fn extent(offset: u64, len: u64) -> Result<(i64, i64, u64), Error> {
    let (Ok(off), Ok(length), Some(end)) = (
        i64::try_from(offset),
        i64::try_from(len),
        offset.checked_add(len),
    ) else {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "extent past the largest file offset",
        ));
    };
    Ok((off, length, end))
}

/// `fallocate(PUNCH_HOLE | KEEP_SIZE)` over `[offset, offset + len)` of the
/// file at `path`: the blocks are freed, the length is kept, the range reads
/// back as zeros. 64-bit targets only, where each argument is one register.
///
/// # Errors
/// Returns an [`Error`] if the file does not open or `fallocate` fails (e.g.
/// `EOPNOTSUPP` on a file system that cannot punch).
#[cfg(target_pointer_width = "64")]
fn punch_hole_raw(path: &core::ffi::CStr, offset: u64, len: u64) -> Result<(), Error> {
    let (off, length, _) = extent(offset, len)?;
    let fd = ProbeFd::open(path, O_WRONLY)?;
    // SAFETY: `fd` is a descriptor this call owns; mode and range are plain
    // values the kernel checks.
    unsafe {
        syscall4(
            Sysno::fallocate,
            fd.0 as usize,
            (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE) as usize,
            off as usize,
            length as usize,
        )
    }
    .map_err(|e| err("fallocate", e))?;
    Ok(())
}

/// Whether `[offset, offset + len)` of the file at `path` is entirely a hole:
/// `lseek(SEEK_DATA)` finds the next byte backed by data, and the range is a
/// hole when that lies at or past its end. `ENXIO` (no data follows) is a hole
/// to the end of the file; a file system without `SEEK_DATA` cannot answer.
///
/// # Errors
/// Returns an [`Error`] if the file does not open or `lseek` fails otherwise.
#[cfg(target_pointer_width = "64")]
fn extent_is_hole_raw(
    path: &core::ffi::CStr,
    offset: u64,
    len: u64,
) -> Result<Option<bool>, Error> {
    if len == 0 {
        return Ok(Some(false));
    }
    let (off, _, end) = extent(offset, len)?;
    let fd = ProbeFd::open(path, O_RDONLY)?;
    match lseek_errno(fd.0, off, SEEK_DATA) {
        Ok(next_data) => Ok(Some(next_data >= end)),
        Err(e) if e.into_raw() == errno::ENXIO => Ok(Some(true)),
        Err(e) if matches!(e.into_raw(), errno::EINVAL | errno::EOPNOTSUPP) => Ok(None),
        Err(e) => Err(err("lseek(SEEK_DATA)", e)),
    }
}

/// Whether `[offset, offset + len)` of the file at `path` contains a hole:
/// `lseek(SEEK_HOLE)` finds the next hole, and one before the range end proves
/// it. The hole every file ends in is excluded by clamping the end to the file
/// length; `ENXIO` (offset at or past the end) holds no byte of the file.
///
/// # Errors
/// Returns an [`Error`] if the file does not open or a syscall fails otherwise.
#[cfg(target_pointer_width = "64")]
fn extent_contains_hole_raw(
    path: &core::ffi::CStr,
    offset: u64,
    len: u64,
) -> Result<Option<bool>, Error> {
    if len == 0 {
        return Ok(Some(false));
    }
    let (off, _, end) = extent(offset, len)?;
    let fd = ProbeFd::open(path, O_RDONLY)?;
    let file_len = fstat_raw(fd.0)?.size;
    match lseek_errno(fd.0, off, SEEK_HOLE) {
        Ok(next_hole) => Ok(Some(next_hole < end.min(file_len))),
        Err(e) if e.into_raw() == errno::ENXIO => Ok(Some(false)),
        Err(e) if matches!(e.into_raw(), errno::EINVAL | errno::EOPNOTSUPP) => Ok(None),
        Err(e) => Err(err("lseek(SEEK_HOLE)", e)),
    }
}

/// `linkat(AT_FDCWD, src, AT_FDCWD, dst, 0)`: `dst` becomes a second name of
/// the inode `src` names. A link across mounts fails with `EXDEV`, reported as
/// [`ErrorKind::CrossesDevices`].
///
/// # Errors
/// Returns an [`Error`] if `linkat` fails.
fn linkat_raw(src: &core::ffi::CStr, dst: &core::ffi::CStr) -> Result<(), Error> {
    // SAFETY: both paths are valid NUL-terminated C strings the kernel reads.
    unsafe {
        syscall5(
            Sysno::linkat,
            AT_FDCWD as usize,
            src.as_ptr() as usize,
            AT_FDCWD as usize,
            dst.as_ptr() as usize,
            LINKAT_NO_FOLLOW,
        )
    }
    .map_err(|e| err("linkat", e))?;
    Ok(())
}

/// `ioctl(dst, FICLONE, src)` on a `dst` created for it: the clone shares the
/// source's blocks copy-on-write. Ok(Err(errno)) when the clone is refused,
/// with the empty `dst` removed, so the caller can tell a file system that
/// cannot clone (and copy instead) from a genuine failure.
///
/// # Errors
/// Returns an [`Error`] if `src` does not open or `dst` cannot be created.
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
fn ficlone_raw(src: &core::ffi::CStr, dst: &core::ffi::CStr) -> Result<Result<(), Errno>, Error> {
    let source = ProbeFd::open(src, O_RDONLY)?;
    let target = ProbeFd(open_raw(dst, O_WRONLY | O_CREAT | O_EXCL, 0o644)?);
    // SAFETY: both descriptors are owned for the call; FICLONE takes the
    // source descriptor as its argument.
    let cloned = unsafe {
        syscall3(
            Sysno::ioctl,
            target.0 as usize,
            FICLONE as usize,
            source.0 as usize,
        )
    };
    if let Err(e) = cloned {
        drop(target);
        // The empty target goes, so a copy in its place can create it again.
        let _ = unlinkat_raw(dst, false);
        return Ok(Err(e));
    }
    Ok(Ok(()))
}

/// Turns per-file copy-on-write off for the file at `path` (`FS_NOCOW_FL`
/// through `FS_IOC_SETFLAGS`). A file system without the inode-flags ioctl, or
/// without the flag, leaves it as it is: an optimization, never a requirement.
///
/// # Errors
/// Returns an [`Error`] if the file does not open or the ioctl fails for
/// another reason.
#[cfg(any(
    target_arch = "x86",
    target_arch = "x86_64",
    target_arch = "arm",
    target_arch = "aarch64",
    target_arch = "riscv32",
    target_arch = "riscv64",
    target_arch = "loongarch64",
    target_arch = "s390x"
))]
fn disable_cow_raw(path: &core::ffi::CStr) -> Result<(), Error> {
    /// A refusal that means "not applicable here".
    fn not_applicable(e: Errno) -> bool {
        matches!(
            e.into_raw(),
            errno::ENOTTY | errno::EOPNOTSUPP | errno::EINVAL
        )
    }
    let fd = ProbeFd::open(path, O_RDWR)?;
    let mut flags: i32 = 0;
    // SAFETY: `fd` is owned for the call; GETFLAGS writes one `int` through
    // the pointer, which outlives the call.
    let got = unsafe {
        syscall3(
            Sysno::ioctl,
            fd.0 as usize,
            FS_IOC_GETFLAGS as usize,
            (&raw mut flags) as usize,
        )
    };
    match got {
        Ok(_) => {}
        Err(e) if not_applicable(e) => return Ok(()),
        Err(e) => return Err(err("ioctl(FS_IOC_GETFLAGS)", e)),
    }
    if flags & FS_NOCOW_FL != 0 {
        return Ok(());
    }
    flags |= FS_NOCOW_FL;
    // SAFETY: as above; SETFLAGS reads one `int` through the pointer.
    let set = unsafe {
        syscall3(
            Sysno::ioctl,
            fd.0 as usize,
            FS_IOC_SETFLAGS as usize,
            (&raw const flags) as usize,
        )
    };
    match set {
        Ok(_) => Ok(()),
        Err(e) if not_applicable(e) => Ok(()),
        Err(e) => Err(err("ioctl(FS_IOC_SETFLAGS)", e)),
    }
}

/// `fadvise64(fd, 0, 0, advice)`: access advice for the whole file. Advice the
/// kernel does not know (`EINVAL`) is advisory and dropped, as `StdFs` drops
/// it. 64-bit targets only, where each argument is one register.
///
/// # Errors
/// Returns an [`Error`] if `fadvise64` refuses the descriptor (a pipe).
#[cfg(target_pointer_width = "64")]
fn fadvise_raw(fd: i32, hint: super::FileHint) -> Result<(), Error> {
    let advice = match hint {
        super::FileHint::Default => POSIX_FADV_NORMAL,
        super::FileHint::Sequential => POSIX_FADV_SEQUENTIAL,
        super::FileHint::Random => POSIX_FADV_RANDOM,
        super::FileHint::WriteOnce => POSIX_FADV_DONTNEED,
    };
    // SAFETY: `fd` is a descriptor the caller owns; the range and the advice
    // are plain values.
    match unsafe { syscall4(Sysno::fadvise64, fd as usize, 0, 0, advice) } {
        Ok(_) => Ok(()),
        Err(e) if e.into_raw() == errno::EINVAL => Ok(()),
        Err(e) => Err(err("fadvise64", e)),
    }
}

// SAFETY: `IoUringRaw`'s raw pointers address `mmap` regions it owns for its
// whole lifetime, and its ring fd is process-global; moving it to another
// thread is sound because every access is serialized through the `Mutex` that
// wraps it in `IoUringRawFile` (no concurrent unsynchronized use). It is only
// `Send` (moved across threads), never shared by `&` directly.
unsafe impl Send for IoUringRaw {}

/// A [`FsFile`] backed by the raw `no_std` `io_uring` driver.
///
/// The hot read / write / fsync path goes through a shared ring (serialized by a
/// `Mutex`), while cold operations (size, truncate, lock) use plain blocking
/// syscalls. It slots into the [`Fs`] abstraction like the
/// std-bound backends.
pub struct IoUringRawFile {
    /// Shared submission/completion ring. `Mutex` because the ring ops take
    /// `&mut self` while [`FsFile`] hands out `&self`, and one ring is shared
    /// across the files an `Fs` opens.
    ring: Arc<Mutex<IoUringRaw>>,
    /// The open file descriptor (owned: closed on [`Drop`]).
    fd: i32,
    /// Byte cursor for the sequential `Read` / `Write` / `Seek` path. `read_at`
    /// ignores it (explicit offset).
    cursor: u64,
    /// `O_APPEND` semantics: every write goes to the current end of file.
    is_append: bool,
}

impl IoUringRawFile {
    /// Wraps an open descriptor `fd` (from [`open_raw`]) with the shared `ring`.
    /// `is_append` selects `O_APPEND`-style writes (always at EOF). Takes
    /// ownership of `fd` (closed on [`Drop`]).
    #[must_use]
    pub fn new(ring: Arc<Mutex<IoUringRaw>>, fd: i32, is_append: bool) -> Self {
        Self {
            ring,
            fd,
            cursor: 0,
            is_append,
        }
    }

    /// Adds a signed delta to a base offset, erroring on overflow / underflow
    /// (a seek before byte 0 or past `u64::MAX`).
    fn offset_add(base: u64, delta: i64) -> Result<u64, Error> {
        let r = if delta >= 0 {
            base.checked_add(delta as u64)
        } else {
            base.checked_sub(delta.unsigned_abs())
        };
        r.ok_or_else(|| Error::new(ErrorKind::InvalidInput, "seek position out of range"))
    }

    fn read_impl(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        let n = self.ring.lock().read_at(self.fd, buf, self.cursor)?;
        // The cursor is bounded by the file size (n is the bytes just
        // read/written), so a plain add cannot overflow u64.
        self.cursor += n as u64;
        Ok(n)
    }

    fn write_impl(&mut self, buf: &[u8]) -> Result<usize, Error> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.is_append {
            // O_APPEND: write with the io_uring "use the file offset" sentinel
            // (`-1`), so the kernel performs the append atomically against the
            // O_APPEND descriptor. Resolving the offset in userspace
            // (`lseek` + positioned write) would race with other appenders,
            // because a positioned write ignores O_APPEND and two writers could
            // land at the same offset, silently losing one.
            return self.ring.lock().write_at(self.fd, buf, u64::MAX);
        }
        let n = self.ring.lock().write_at(self.fd, buf, self.cursor)?;
        // The cursor is bounded by the file size (n is the bytes just
        // read/written), so a plain add cannot overflow u64.
        self.cursor += n as u64;
        Ok(n)
    }

    fn seek_impl(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        let target = match pos {
            SeekFrom::Start(o) => o,
            SeekFrom::Current(d) => Self::offset_add(self.cursor, d)?,
            SeekFrom::End(d) => Self::offset_add(file_size_raw(self.fd)?, d)?,
        };
        self.cursor = target;
        Ok(target)
    }
}

impl Drop for IoUringRawFile {
    fn drop(&mut self) {
        // Best-effort close; a write-back error at close is not actionable here.
        let _ = close_raw(self.fd);
    }
}

impl FsFile for IoUringRawFile {
    fn sync_all(&self) -> crate::io::Result<()> {
        self.ring.lock().fsync(self.fd, false)
    }

    fn sync_data(&self) -> crate::io::Result<()> {
        self.ring.lock().fsync(self.fd, true)
    }

    fn metadata(&self) -> crate::io::Result<FsMetadata> {
        let m = fstat_raw(self.fd)?;
        Ok(FsMetadata {
            len: m.size,
            is_dir: m.is_dir,
            is_file: m.is_file,
        })
    }

    fn hard_link_count(&self) -> crate::io::Result<u64> {
        // `statx` on the open descriptor reports `stx_nlink`, so the in-place
        // heal path gets a real count instead of the `Unsupported` default that
        // would make it conservatively skip every reclaim.
        Ok(fstat_raw(self.fd)?.nlink)
    }

    fn set_len(&self, size: u64) -> crate::io::Result<()> {
        ftruncate_raw(self.fd, size)
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Fill-or-EOF: loop until the buffer is full or a 0-byte read signals
        // EOF, retrying the ring op on EINTR (mirrors the std io_uring backend).
        let mut total = 0usize;
        while total < buf.len() {
            let remaining = buf
                .get_mut(total..)
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "read_at slice out of range"))?;
            let at = offset
                .checked_add(total as u64)
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "read_at offset overflow"))?;
            let n = loop {
                match self.ring.lock().read_at(self.fd, remaining, at) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            };
            if n == 0 {
                break;
            }
            total += n;
        }
        Ok(total)
    }

    // Every region goes to the ring in one batch instead of the trait's serial
    // `read_at` loop, so they overlap in flight.
    fn read_many(&self, regions: &mut [(u64, &mut [u8])]) -> crate::io::Result<()> {
        let mut reads = Vec::with_capacity(regions.len());
        for (offset, buf) in regions.iter_mut() {
            if buf.is_empty() {
                continue;
            }
            reads.push(RawRead {
                fd: self.fd,
                buf: buf.as_mut_ptr(),
                len: batch_read_len(buf.len())?,
                offset: *offset,
            });
        }
        let mut results = alloc::vec![0i32; reads.len()];
        // SAFETY: every pointer is into a region `regions` borrows for this
        // whole call, and `read_batch` returns only once every read completed.
        unsafe { self.ring.lock().read_batch(&reads, &mut results) };
        // Every read has completed, so the first failure in region order
        // decides, whatever order they finished in.
        reads
            .iter()
            .zip(results)
            .try_for_each(|(read, res)| read_verdict(res, read.len))
    }

    fn read_at_buf(&self, buf: &mut super::ReadBuf<'_>, offset: u64) -> crate::io::Result<()> {
        // SAFETY: the ring only has the kernel write the slice, and `read_at`
        // returns how much of it was.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            buf.fill_with(|bytes| FsFile::read_at(self, bytes, offset))
        }
    }

    fn read_many_buf(&self, regions: &mut [(u64, super::ReadBuf<'_>)]) -> crate::io::Result<()> {
        // SAFETY: as in `read_at_buf`; `read_many` fails unless every region
        // was filled.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            super::read_buf::fill_regions_with(regions, |bytes| FsFile::read_many(self, bytes))
        }
    }

    // Exposes the descriptor so `IoUringRawFs::read_blocks_batched` can read
    // several files through one ring batch.
    fn backing_fd(&self) -> Option<i32> {
        Some(self.fd)
    }

    fn lock_exclusive(&self) -> crate::io::Result<()> {
        flock_exclusive_raw(self.fd, false)?;
        Ok(())
    }

    fn try_lock_exclusive(&self) -> crate::io::Result<bool> {
        flock_exclusive_raw(self.fd, true)
    }

    // 32-bit targets keep the no-op default: the call passes its 64-bit
    // offsets split across register pairs, which this raw layer does not.
    #[cfg(target_pointer_width = "64")]
    fn start_writeback(&self, offset: u64, len: u64) -> crate::io::Result<()> {
        start_writeback_raw(self.fd, offset, len)
    }

    fn set_write_lifetime(&self, lifetime: super::WriteLifetime) -> crate::io::Result<()> {
        set_write_lifetime_raw(self.fd, lifetime)
    }

    // 32-bit targets keep the no-op default, as for `start_writeback`.
    #[cfg(target_pointer_width = "64")]
    fn hint(&self, hint: super::FileHint) -> crate::io::Result<()> {
        fadvise_raw(self.fd, hint)
    }
}

#[cfg(feature = "std")]
impl std::io::Read for IoUringRawFile {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_impl(buf).map_err(Into::into)
    }
}
#[cfg(not(feature = "std"))]
impl crate::io::Read for IoUringRawFile {
    fn read(&mut self, buf: &mut [u8]) -> crate::io::Result<usize> {
        self.read_impl(buf)
    }
}

#[cfg(feature = "std")]
impl std::io::Write for IoUringRawFile {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.write_impl(buf).map_err(Into::into)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
#[cfg(not(feature = "std"))]
impl crate::io::Write for IoUringRawFile {
    fn write(&mut self, buf: &[u8]) -> crate::io::Result<usize> {
        self.write_impl(buf)
    }
    fn flush(&mut self) -> crate::io::Result<()> {
        Ok(())
    }
}

#[cfg(feature = "std")]
impl std::io::Seek for IoUringRawFile {
    fn seek(&mut self, pos: std::io::SeekFrom) -> std::io::Result<u64> {
        self.seek_impl(pos.into()).map_err(Into::into)
    }
}
#[cfg(not(feature = "std"))]
impl crate::io::Seek for IoUringRawFile {
    fn seek(&mut self, pos: SeekFrom) -> crate::io::Result<u64> {
        self.seek_impl(pos)
    }
}

/// Converts a path to a NUL-terminated C string for the path syscalls. Errors
/// on a non-UTF-8 path or an interior NUL. Uses `to_str` (common to the std and
/// `no_std` `Path`), not the `no_std`-only `as_str`.
fn path_to_cstring(path: &Path) -> Result<CString, Error> {
    let s = path
        .to_str()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "path is not valid UTF-8"))?;
    CString::new(s.as_bytes())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains an interior NUL"))
}

/// Maps [`FsOpenOptions`] to `open(2)` flags (see the directory-syscall note
/// for the per-architecture values and the MIPS / SPARC caveat).
fn open_flags(opts: &FsOpenOptions) -> i32 {
    let mut flags = if opts.read && (opts.write || opts.append) {
        O_RDWR
    } else if opts.write || opts.append {
        O_WRONLY
    } else {
        O_RDONLY
    };
    if opts.create || opts.create_new {
        flags |= O_CREAT;
    }
    if opts.create_new {
        flags |= O_EXCL;
    }
    if opts.truncate {
        flags |= O_TRUNC;
    }
    if opts.append {
        flags |= O_APPEND;
    }
    if opts.no_follow {
        flags |= O_NOFOLLOW;
    }
    if opts.no_wait {
        flags |= O_NONBLOCK;
    }
    flags
}

/// Reads every entry of an open directory `fd` via `getdents64`, returning
/// `(file_name, is_dir)` pairs and skipping `.` / `..`.
fn read_dir_entries(fd: i32) -> Result<Vec<(String, bool)>, Error> {
    let mut entries = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        // SAFETY: `buf` is a valid writable region of `buf.len()` bytes that the
        // kernel fills with packed `linux_dirent64` records.
        let n = unsafe {
            syscall3(
                Sysno::getdents64,
                fd as usize,
                buf.as_mut_ptr() as usize,
                buf.len(),
            )
        }
        .map_err(|e| err("getdents64", e))?;
        if n == 0 {
            break; // end of directory
        }
        let n = n as usize;
        let mut off = 0usize;
        // linux_dirent64: d_ino(8) d_off(8) d_reclen(u16 @16) d_type(u8 @18)
        // d_name(@19, NUL-terminated). Walk record by record via d_reclen.
        while off + 19 <= n {
            let reclen = {
                let b: [u8; 2] = buf
                    .get(off + 16..off + 18)
                    .and_then(|s| s.try_into().ok())
                    .ok_or_else(|| {
                        Error::new(ErrorKind::InvalidData, "dirent reclen out of range")
                    })?;
                // `d_reclen` is a native-endian `u16` (the kernel writes the
                // struct in host byte order), so decode native-endian — not
                // little-endian, which would be wrong on big-endian targets.
                usize::from(u16::from_ne_bytes(b))
            };
            if reclen < 19 || off + reclen > n {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    "dirent record length invalid",
                ));
            }
            let d_type = *buf
                .get(off + 18)
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "dirent type out of range"))?;
            let name_region = buf
                .get(off + 19..off + reclen)
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "dirent name out of range"))?;
            let name_len = name_region
                .iter()
                .position(|&c| c == 0)
                .unwrap_or(name_region.len());
            let name_bytes = name_region
                .get(..name_len)
                .ok_or_else(|| Error::new(ErrorKind::InvalidData, "dirent name slice"))?;
            let name = core::str::from_utf8(name_bytes)
                .map_err(|_| Error::new(ErrorKind::InvalidData, "dirent name is not UTF-8"))?;
            if name != "." && name != ".." {
                entries.push((name.to_string(), d_type == DT_DIR));
            }
            off += reclen;
        }
    }
    Ok(entries)
}

/// An [`Fs`] backend over the raw `no_std` `io_uring` driver.
///
/// Opened files share one ring (the hot read / write / fsync path); directory
/// operations use plain blocking syscalls. Pure syscalls throughout — no
/// `io-uring` crate and no `std::fs`, unlike the std-bound `IoUringFs`.
pub struct IoUringRawFs {
    ring: Arc<Mutex<IoUringRaw>>,
}

impl IoUringRawFs {
    /// Creates a backend with its own ring sized for `ring_entries` SQEs (the
    /// kernel rounds up to a power of two).
    ///
    /// # Errors
    /// Returns an [`Error`] if `io_uring_setup` / `mmap` fails.
    pub fn new(ring_entries: u32) -> Result<Self, Error> {
        Ok(Self {
            ring: Arc::new(Mutex::new(IoUringRaw::new(ring_entries)?)),
        })
    }

    /// Reads `reqs`: every one with a descriptor through the ring in one
    /// batch, then the others serially through `read_at`, stopping at the
    /// first of those that fails. Each request filled is handed to `on_read`
    /// once every read is over, in request order, whatever the call returns.
    ///
    /// # Errors
    ///
    /// The batch [`Fs::check_descriptor_batch`] refuses, before anything is
    /// read; else the failure of the lowest-indexed request that failed.
    fn read_blocks(
        &self,
        reqs: &mut [BlockRead<'_>],
        on_read: Option<OnRead<'_>>,
    ) -> crate::io::Result<()> {
        self.check_descriptor_batch(reqs)?;
        let mut reads = Vec::with_capacity(reqs.len());
        let mut ats = Vec::with_capacity(reqs.len());
        for (at, req) in reqs.iter_mut().enumerate() {
            let Some(fd) = req.file.backing_fd() else {
                continue;
            };
            let want = req.buf.capacity() - req.buf.filled();
            if want == 0 {
                continue;
            }
            // A partly filled destination owns the block's first `filled`
            // bytes already, so its read covers the suffix from there; the
            // check above proved the offset and the length fit.
            let offset = req.offset + req.buf.filled() as u64;
            reads.push(RawRead {
                fd,
                buf: req.buf.unfilled_mut().as_mut_ptr(),
                len: batch_read_len(want)?,
                offset,
            });
            ats.push(at);
        }
        let mut results = alloc::vec![0i32; reads.len()];
        if !reads.is_empty() {
            // SAFETY: every pointer is into a destination `reqs` borrows for
            // this whole call, and `read_batch` returns only once every read
            // completed; nothing touches a destination until then.
            unsafe { self.ring.lock().read_batch(&reads, &mut results) };
        }

        let mut failure: Option<(usize, Error)> = None;
        for ((&at, read), res) in ats.iter().zip(&reads).zip(results) {
            match read_verdict(res, read.len) {
                // The kernel filled exactly the unfilled span the read named.
                Ok(()) => {
                    if let Some(req) = reqs.get_mut(at) {
                        req.buf.advance(read.len as usize);
                    }
                }
                Err(error) => {
                    if failure.as_ref().is_none_or(|(held, _)| at < *held) {
                        failure = Some((at, error));
                    }
                }
            }
        }

        // A file with no descriptor this ring can take is read on its own.
        for (at, req) in reqs.iter_mut().enumerate() {
            if req.file.backing_fd().is_some() || req.buf.is_full() {
                continue;
            }
            // A lower request already failed: the call's verdict is decided.
            if failure.as_ref().is_some_and(|(held, _)| *held < at) {
                break;
            }
            let Some(offset) = req.offset.checked_add(req.buf.filled() as u64) else {
                failure = Some((
                    at,
                    Error::new(
                        ErrorKind::InvalidInput,
                        "read_blocks_batched: request offset overflows",
                    ),
                ));
                break;
            };
            let dst = req.buf.unfilled_mut();
            let want = dst.len();
            match req.file.read_at(dst, offset) {
                Ok(n) => {
                    req.buf.advance(n);
                    if n != want {
                        failure = Some((at, short_block_read()));
                        break;
                    }
                }
                Err(error) => {
                    failure = Some((at, error));
                    break;
                }
            }
        }

        if let Some(on_read) = on_read {
            for (at, req) in reqs.iter().enumerate() {
                if req.buf.is_full() {
                    on_read(at, req);
                }
            }
        }
        failure.map_or(Ok(()), |(_, error)| Err(error))
    }
}

/// The callback [`Fs::read_blocks_batched_each`] hands each filled request to.
type OnRead<'f> = &'f mut dyn for<'a, 'b> FnMut(usize, &'a BlockRead<'b>);

/// The SQE length of a batched read of `len` bytes: its completion reports
/// the count as an `i32`, so a longer read could not say it finished.
fn batch_read_len(len: usize) -> Result<u32, Error> {
    i32::try_from(len)
        .map(i32::unsigned_abs)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "buffer exceeds i32::MAX"))
}

/// What a batched read's completion `res` says about its fixed-size block of
/// `len` bytes: a short count is a block cut off, not an end of file.
fn read_verdict(res: i32, len: u32) -> Result<(), Error> {
    if res < 0 {
        return Err(Error::new(
            errno_to_kind(-res),
            format!("io_uring read completed with errno {}", -res),
        ));
    }
    if res.unsigned_abs() != len {
        return Err(short_block_read());
    }
    Ok(())
}

/// A batched read that returned fewer bytes than its fixed-size block.
fn short_block_read() -> Error {
    Error::new(
        ErrorKind::UnexpectedEof,
        "io_uring read_blocks_batched: short read on a fixed-size block",
    )
}

impl Fs for IoUringRawFs {
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

    // The ring reads every request with a descriptor by that descriptor; one
    // without is read serially through `read_at`.
    fn reads_by_descriptor(&self) -> bool {
        true
    }

    // What `read_blocks` refuses before it reads any of the batch.
    fn check_descriptor_batch(&self, reqs: &[BlockRead<'_>]) -> crate::io::Result<()> {
        for req in reqs {
            let want = req.buf.capacity() - req.buf.filled();
            if want == 0 || req.file.backing_fd().is_none() {
                continue;
            }
            batch_read_len(want)?;
            if req.offset.checked_add(req.buf.filled() as u64).is_none() {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "read_blocks_batched: request offset overflows",
                ));
            }
        }
        Ok(())
    }

    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn FsFile>> {
        let cpath = path_to_cstring(path)?;
        let fd = open_raw(&cpath, open_flags(opts), 0o644)?;
        // Owns `fd` from here, so a failure below closes it.
        let file = IoUringRawFile::new(Arc::clone(&self.ring), fd, opts.append);
        if opts.no_wait {
            clear_nonblock_raw(fd)?;
        }
        Ok(Box::new(file))
    }

    fn create_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        // Create each ancestor first; an already-existing component is success.
        if let Some(parent) = path.parent()
            && !parent.to_str().unwrap_or("").is_empty()
        {
            self.create_dir_all(parent)?;
        }
        match self.create_dir(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn create_dir(&self, path: &Path) -> crate::io::Result<()> {
        mkdirat_raw(&path_to_cstring(path)?, 0o755)
    }

    fn read_dir(&self, path: &Path) -> crate::io::Result<Vec<FsDirEntry>> {
        let cpath = path_to_cstring(path)?;
        let fd = open_raw(&cpath, O_RDONLY | O_DIRECTORY, 0)?;
        let result = read_dir_entries(fd);
        let _ = close_raw(fd);
        let names = result?;
        Ok(names
            .into_iter()
            .map(|(name, is_dir)| FsDirEntry {
                path: path.join(&name),
                file_name: name,
                is_dir,
            })
            .collect())
    }

    fn remove_file(&self, path: &Path) -> crate::io::Result<()> {
        unlinkat_raw(&path_to_cstring(path)?, false)
    }

    fn remove_dir_all(&self, path: &Path) -> crate::io::Result<()> {
        // Depth-first: remove children (recursing into subdirectories) before
        // the directory itself, since the kernel rejects rmdir on a non-empty
        // directory.
        for entry in self.read_dir(path)? {
            let child = path.join(&entry.file_name);
            if entry.is_dir {
                self.remove_dir_all(&child)?;
            } else {
                self.remove_file(&child)?;
            }
        }
        unlinkat_raw(&path_to_cstring(path)?, true)
    }

    fn remove_dir(&self, path: &Path) -> crate::io::Result<()> {
        // rmdir(2) through unlinkat: the kernel refuses a non-empty directory.
        unlinkat_raw(&path_to_cstring(path)?, true)
    }

    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        renameat2_raw(&path_to_cstring(from)?, &path_to_cstring(to)?)
    }

    fn same_file(&self, a: &Path, b: &Path) -> crate::io::Result<bool> {
        // Filesystem OBJECT identity (device + inode), as the other kernel
        // backends answer it; a probe failure propagates rather than read as
        // "distinct".
        let ia = statx_identity_raw(&path_to_cstring(a)?)?;
        let ib = statx_identity_raw(&path_to_cstring(b)?)?;
        Ok(ia == ib)
    }

    // Linux records no kind with a link: `to_dir` matters only on Windows.
    fn symlink(&self, target: &Path, link: &Path, _to_dir: bool) -> crate::io::Result<()> {
        symlinkat_raw(&path_to_cstring(target)?, &path_to_cstring(link)?)
    }

    fn read_link(&self, path: &Path) -> crate::io::Result<Option<crate::path::PathBuf>> {
        let Some(target) = readlinkat_raw(&path_to_cstring(path)?)? else {
            return Ok(None);
        };
        // A target is any byte string; the std path type holds every one.
        #[cfg(feature = "std")]
        {
            use std::os::unix::ffi::OsStringExt as _;
            Ok(Some(std::ffi::OsString::from_vec(target).into()))
        }
        // The no-std path type is a string, so a target must be UTF-8.
        #[cfg(not(feature = "std"))]
        {
            let target = String::from_utf8(target).map_err(|_| {
                Error::new(
                    ErrorKind::InvalidInput,
                    "readlinkat: symlink target is not valid UTF-8",
                )
            })?;
            Ok(Some(crate::path::PathBuf::from(target)))
        }
    }

    fn metadata(&self, path: &Path) -> crate::io::Result<FsMetadata> {
        match statx_path_raw(&path_to_cstring(path)?)? {
            Some(m) => Ok(FsMetadata {
                len: m.size,
                is_dir: m.is_dir,
                is_file: m.is_file,
            }),
            None => Err(Error::new(ErrorKind::NotFound, "path not found")),
        }
    }

    // x86-64 / aarch64 only: the raw `statfs` ABI struct is verified for those
    // layouts. On any other target (a different 64-bit arch like s390x, or a
    // 32-bit build) this method is absent, so the trait default (u64::MAX = no
    // disk-pressure signal) applies — correct, since admission must not gate on
    // a value it cannot read from a matching layout.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    fn available_space(&self, path: &Path) -> crate::io::Result<u64> {
        statfs_available_raw(&path_to_cstring(path)?)
    }

    fn sync_directory(&self, path: &Path) -> crate::io::Result<()> {
        let cpath = path_to_cstring(path)?;
        let fd = open_raw(&cpath, O_RDONLY | O_DIRECTORY, 0)?;
        let r = self.ring.lock().fsync(fd, false);
        let _ = close_raw(fd);
        r
    }

    fn exists(&self, path: &Path) -> crate::io::Result<bool> {
        Ok(statx_path_raw(&path_to_cstring(path)?)?.is_some())
    }

    // The file system's capabilities, and the operations they gate, answer as
    // `StdFs` answers them on Linux: the same syscalls, issued directly.

    fn hard_link(&self, src: &Path, dst: &Path) -> crate::io::Result<()> {
        linkat_raw(&path_to_cstring(src)?, &path_to_cstring(dst)?)
    }

    // Paths resolve through the host kernel, as through `StdFs`, so a
    // checkpoint links across the two instead of copying.
    fn backend_id(&self) -> Option<u64> {
        Some(super::KERNEL_BACKEND_ID)
    }

    // The mount's device, as `StdFs` reports it; a path that cannot be stat-ed
    // proves no independence.
    fn volume_id(&self, path: &Path) -> Option<u64> {
        let stat = statx_at(&path_to_cstring(path).ok()?).ok()??;
        Some(makedev(stat.stx_dev_major, stat.stx_dev_minor))
    }

    // x86-64 / aarch64 only, where the raw `statfs` layout is verified; other
    // targets keep the conservative default.
    #[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
    fn capabilities(&self, path: &Path) -> super::FsCapabilities {
        path_to_cstring(path)
            .and_then(|path| statfs_type_raw(&path))
            .map_or_else(
                |_| super::FsCapabilities::default(),
                super::linux_fs_type_capabilities,
            )
    }

    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    ))]
    fn try_disable_cow(&self, path: &Path) -> crate::io::Result<()> {
        disable_cow_raw(&path_to_cstring(path)?)
    }

    #[cfg(target_pointer_width = "64")]
    fn punch_hole(&self, path: &Path, offset: u64, len: u64) -> crate::io::Result<()> {
        punch_hole_raw(&path_to_cstring(path)?, offset, len)
    }

    // A file system that cannot clone (no reflink, another mount, a clone it
    // rejects) gets the streamed copy, as through `StdFs`; any other failure
    // is reported.
    #[cfg(any(
        target_arch = "x86",
        target_arch = "x86_64",
        target_arch = "arm",
        target_arch = "aarch64",
        target_arch = "riscv32",
        target_arch = "riscv64",
        target_arch = "loongarch64",
        target_arch = "s390x"
    ))]
    fn reflink_file(&self, src: &Path, dst: &Path) -> crate::io::Result<()> {
        match ficlone_raw(&path_to_cstring(src)?, &path_to_cstring(dst)?)? {
            Ok(()) => Ok(()),
            Err(e)
                if matches!(
                    e.into_raw(),
                    errno::EOPNOTSUPP | errno::EXDEV | errno::EINVAL
                ) =>
            {
                super::copy_file_streamed(self, src, dst)
            }
            Err(e) => Err(err("ioctl(FICLONE)", e)),
        }
    }

    fn hard_link_count(&self, path: &Path) -> crate::io::Result<u64> {
        statx_path_raw(&path_to_cstring(path)?)?
            .map(|stat| stat.nlink)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "path not found"))
    }

    // A stat failure is an error, never "cannot tell": repair reads `None` as
    // an unpunched file.
    fn allocated_size(&self, path: &Path) -> crate::io::Result<Option<u64>> {
        let stat = statx_at(&path_to_cstring(path)?)?
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "path not found"))?;
        if stat.stx_mask & STATX_BLOCKS == 0 {
            return Ok(None);
        }
        // `stx_blocks` counts 512-byte units; a file's blocks stay far below
        // `u64::MAX / 512`.
        Ok(Some(stat.stx_blocks * 512))
    }

    #[cfg(target_pointer_width = "64")]
    fn extent_is_hole(
        &self,
        path: &Path,
        offset: u64,
        len: u64,
    ) -> crate::io::Result<Option<bool>> {
        extent_is_hole_raw(&path_to_cstring(path)?, offset, len)
    }

    #[cfg(target_pointer_width = "64")]
    fn extent_contains_hole(
        &self,
        path: &Path,
        offset: u64,
        len: u64,
    ) -> crate::io::Result<Option<bool>> {
        extent_contains_hole_raw(&path_to_cstring(path)?, offset, len)
    }
}

/// A minimal, `no_std` `io_uring` instance: an `mmap`'d submission ring, an
/// `mmap`'d completion ring, the SQE array, and the ring fd.
///
/// This owns its mappings and the ring fd; [`Drop`] unmaps and closes them.
pub struct IoUringRaw {
    ring_fd: i32,

    /// Base of the SQ-ring mmap (also holds the CQ ring when the kernel reports
    /// `IORING_FEAT_SINGLE_MMAP`).
    sq_ptr: *mut u8,
    sq_len: usize,
    /// Base of the CQ-ring mmap when it is a SEPARATE mapping; `null` under
    /// single-mmap (the CQ ring lives inside `sq_ptr`).
    cq_ptr: *mut u8,
    cq_len: usize,
    /// Base of the SQE-array mmap.
    sqes: *mut IoUringSqe,
    sqes_len: usize,

    sq_entries: u32,
    cq_entries: u32,
    /// How many times this ring entered the kernel.
    #[cfg(test)]
    enters: usize,

    // Cached pointers into the SQ ring (kernel-shared, accessed volatile).
    sq_khead: *const u32,
    sq_ktail: *mut u32,
    sq_ring_mask: u32,
    sq_array: *mut u32,

    // Cached pointers into the CQ ring.
    cq_khead: *mut u32,
    cq_ktail: *const u32,
    cq_ring_mask: u32,
    cqes: *const IoUringCqe,
}

impl IoUringRaw {
    /// Sets up a ring sized for `entries` (rounded up to a power of two by the
    /// kernel) and maps its submission and completion rings.
    ///
    /// # Errors
    /// Returns an [`Error`] if `io_uring_setup` or any `mmap` fails.
    // Coverage: the post-setup `mmap`-failure unwind arms and the pre-5.4
    // separate-CQ-mmap layout require a kernel fault / an old kernel to exercise
    // and cannot be reached in CI (the runner kernel always reports SINGLE_MMAP).
    // The setup-success path and the zero-entry setup error ARE covered by tests.
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub fn new(entries: u32) -> Result<Self, Error> {
        let mut params = IoUringParams::default();

        // SAFETY: `params` is a valid writable struct the kernel fills in.
        let ring_fd = unsafe { io_uring_setup(entries, &raw mut params) }?;

        // On any setup failure past this point, close the ring fd.
        let guard = FdGuard(ring_fd);

        let sq_entries = params.sq_entries;
        let single_mmap = params.features & IORING_FEAT_SINGLE_MMAP != 0;

        // SQ ring length covers the array of u32 indices past `sq_off.array`.
        let sq_ring_sz = params.sq_off.array as usize + sq_entries as usize * size_of::<u32>();
        // CQ ring length covers the CQE array past `cq_off.cqes`.
        let cq_ring_sz =
            params.cq_off.cqes as usize + params.cq_entries as usize * size_of::<IoUringCqe>();

        // Under single-mmap the two rings share one mapping sized to the larger.
        let sq_len = if single_mmap {
            sq_ring_sz.max(cq_ring_sz)
        } else {
            sq_ring_sz
        };

        // SAFETY: standard ring mmap per the io_uring setup protocol.
        let sq_ptr = unsafe {
            mmap(
                sq_len,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE,
                ring_fd,
                IORING_OFF_SQ_RING,
            )
        }?;

        let (cq_ptr, cq_len, cq_base) = if single_mmap {
            (core::ptr::null_mut(), 0usize, sq_ptr)
        } else {
            // SAFETY: separate CQ mapping for pre-5.4 kernels.
            let p = match unsafe {
                mmap(
                    cq_ring_sz,
                    PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE,
                    ring_fd,
                    IORING_OFF_CQ_RING,
                )
            } {
                Ok(p) => p,
                Err(e) => {
                    // SAFETY: unwind the SQ mapping before bailing.
                    unsafe { munmap(sq_ptr, sq_len) };
                    return Err(e);
                }
            };
            (p, cq_ring_sz, p)
        };

        // SAFETY: SQE array mmap.
        let sqes = match unsafe {
            mmap(
                sq_entries as usize * size_of::<IoUringSqe>(),
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_POPULATE,
                ring_fd,
                IORING_OFF_SQES,
            )
        } {
            Ok(p) => p.cast::<IoUringSqe>(),
            Err(e) => {
                // SAFETY: unwind prior mappings before bailing.
                unsafe {
                    munmap(sq_ptr, sq_len);
                    if !cq_ptr.is_null() {
                        munmap(cq_ptr, cq_len);
                    }
                }
                return Err(e);
            }
        };

        // Setup succeeded — disarm the fd guard; `Drop` owns teardown now.
        guard.disarm();

        // SAFETY: the offsets come from the kernel and address fields inside the
        // mapped rings; the casts compute in-bounds pointers per the protocol.
        let sqes_len = sq_entries as usize * size_of::<IoUringSqe>();
        Ok(unsafe {
            Self {
                ring_fd,
                sq_ptr,
                sq_len,
                cq_ptr,
                cq_len,
                sqes,
                sqes_len,
                sq_entries,
                cq_entries: params.cq_entries,
                #[cfg(test)]
                enters: 0,
                sq_khead: sq_ptr.add(params.sq_off.head as usize).cast(),
                sq_ktail: sq_ptr.add(params.sq_off.tail as usize).cast(),
                sq_ring_mask: *(sq_ptr.add(params.sq_off.ring_mask as usize).cast::<u32>()),
                sq_array: sq_ptr.add(params.sq_off.array as usize).cast(),
                cq_khead: cq_base.add(params.cq_off.head as usize).cast(),
                cq_ktail: cq_base.add(params.cq_off.tail as usize).cast(),
                cq_ring_mask: *(cq_base.add(params.cq_off.ring_mask as usize).cast::<u32>()),
                cqes: cq_base.add(params.cq_off.cqes as usize).cast(),
            }
        })
    }

    /// Submits a single no-op and waits for its completion, returning the CQE
    /// `res` (`0` on success). Exercises the full submit/complete cycle.
    ///
    /// # Errors
    /// Returns an [`Error`] if `io_uring_enter` fails or the completion carries
    /// a negative `res` (a `-errno`).
    pub fn nop(&mut self, user_data: u64) -> Result<i32, Error> {
        let sqe = IoUringSqe {
            opcode: IORING_OP_NOP,
            user_data,
            ..IoUringSqe::default()
        };
        self.submit_and_reap_one(&sqe)
    }

    /// Reads up to `buf.len()` bytes from `fd` starting at `offset` through the
    /// ring, returning the number of bytes read (`0` at end of file).
    ///
    /// # Errors
    /// Returns an [`Error`] if the read completes with a negative `-errno`.
    pub fn read_at(&mut self, fd: i32, buf: &mut [u8], offset: u64) -> Result<usize, Error> {
        let len = u32::try_from(buf.len()).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "read length exceeds the 4 GiB io_uring single-op limit",
            )
        })?;
        let sqe = IoUringSqe {
            opcode: IORING_OP_READ,
            fd,
            addr: buf.as_mut_ptr() as u64,
            len,
            off: offset,
            ..IoUringSqe::default()
        };
        let res = self.submit_and_reap_one(&sqe)?;
        Ok(res as usize)
    }

    /// Writes up to `buf.len()` bytes to `fd` starting at `offset` through the
    /// ring, returning the number of bytes written.
    ///
    /// # Errors
    /// Returns an [`Error`] if the write completes with a negative `-errno`.
    pub fn write_at(&mut self, fd: i32, buf: &[u8], offset: u64) -> Result<usize, Error> {
        let len = u32::try_from(buf.len()).map_err(|_| {
            Error::new(
                ErrorKind::InvalidInput,
                "write length exceeds the 4 GiB io_uring single-op limit",
            )
        })?;
        let sqe = IoUringSqe {
            opcode: IORING_OP_WRITE,
            fd,
            addr: buf.as_ptr() as u64,
            len,
            off: offset,
            ..IoUringSqe::default()
        };
        let res = self.submit_and_reap_one(&sqe)?;
        Ok(res as usize)
    }

    /// Flushes `fd` to stable storage through the ring. With `datasync`, only
    /// the file data (and the metadata needed to read it back) is flushed,
    /// matching `fdatasync(2)`; otherwise all metadata is flushed too.
    ///
    /// # Errors
    /// Returns an [`Error`] if the fsync completes with a negative `-errno`.
    pub fn fsync(&mut self, fd: i32, datasync: bool) -> Result<(), Error> {
        let sqe = IoUringSqe {
            opcode: IORING_OP_FSYNC,
            fd,
            op_flags: if datasync { IORING_FSYNC_DATASYNC } else { 0 },
            ..IoUringSqe::default()
        };
        self.submit_and_reap_one(&sqe)?;
        Ok(())
    }

    /// Reads every one of `reads` through the ring, as many in flight at once
    /// as the rings hold, and stores each one's completion in `results` at its
    /// index: the byte count, or `-errno`.
    ///
    /// Every read has completed when this returns, so the buffers are the
    /// caller's again; no caller code runs while any is in flight.
    ///
    /// # Safety
    /// Every `buf` is valid for writes of its `len` bytes until this returns.
    unsafe fn read_batch(&mut self, reads: &[RawRead], results: &mut [i32]) {
        // SAFETY: the buffers are the caller's contract, forwarded.
        unsafe {
            self.submit_all(
                &mut |at| {
                    reads.get(at).map(|read| IoUringSqe {
                        opcode: IORING_OP_READ,
                        fd: read.fd,
                        addr: read.buf as u64,
                        len: read.len,
                        off: read.offset,
                        ..IoUringSqe::default()
                    })
                },
                results,
            );
        }
    }

    /// Submits one SQE and reaps its completion, returning its `res`.
    ///
    /// # Errors
    /// Returns an [`Error`] if the ring refuses the SQE or the completion's
    /// `res` is a negative `-errno`.
    fn submit_and_reap_one(&mut self, sqe: &IoUringSqe) -> Result<i32, Error> {
        let mut res = [0i32];
        // SAFETY: the memory `sqe` names is the opcode helper's borrow, which
        // outlives this call, and the call returns only once it completed.
        unsafe { self.submit_all(&mut |at| (at == 0).then_some(*sqe), &mut res) };
        let [res] = res;
        if res < 0 {
            return Err(Error::new(
                errno_to_kind(-res),
                format!("io_uring op completed with errno {}", -res),
            ));
        }
        Ok(res)
    }

    /// Submits the SQEs `sqe_at` builds for `0..results.len()` and stores the
    /// `res` of each one's completion in `results` at its index.
    ///
    /// At most as many go in flight as the completion ring holds, so no
    /// completion is ever dropped. An SQE the kernel refuses to take, and every
    /// one not yet published by then, is completed with the refusal's
    /// `-errno`; one `sqe_at` cannot build is completed with `-EINVAL`. It
    /// returns only once every SQE the kernel took has completed: until then
    /// the kernel may still write into the memory they name.
    ///
    /// # Safety
    /// The memory each SQE names stays valid until this returns.
    // Coverage: the refusal and lost-ring arms need `io_uring_enter` to fail
    // for a reason other than a signal or a full ring, which a healthy kernel
    // never does for these arguments.
    #[cfg_attr(coverage_nightly, coverage(off))]
    unsafe fn submit_all(
        &mut self,
        sqe_at: &mut dyn FnMut(usize) -> Option<IoUringSqe>,
        results: &mut [i32],
    ) {
        let count = results.len();
        let room = self.sq_entries.min(self.cq_entries) as usize;
        // The next SQE to publish.
        let mut next = 0usize;
        // Published, and not taken by the kernel yet.
        let mut untaken = 0u32;
        // Published, and not completed yet.
        let mut owed = 0usize;
        while next < count || owed > 0 {
            // SAFETY: single-threaded driver (the ring is behind the caller's
            // lock). Read the kernel-advanced SQ head (Acquire) for the free
            // slots, write each SQE into `tail & mask`, then publish the new
            // tail (Release) so the kernel sees fully written SQEs before the
            // indices it reads.
            unsafe {
                let mut tail = core::ptr::read_volatile(self.sq_ktail);
                let head = core::ptr::read_volatile(self.sq_khead);
                fence(Ordering::Acquire);
                while next < count && owed < room && tail.wrapping_sub(head) < self.sq_entries {
                    let Some(sqe) = sqe_at(next) else {
                        if let Some(slot) = results.get_mut(next) {
                            *slot = -errno::EINVAL;
                        }
                        next += 1;
                        continue;
                    };
                    let index = tail & self.sq_ring_mask;
                    core::ptr::write(
                        self.sqes.add(index as usize),
                        IoUringSqe {
                            user_data: next as u64,
                            ..sqe
                        },
                    );
                    core::ptr::write(self.sq_array.add(index as usize), index);
                    tail = tail.wrapping_add(1);
                    next += 1;
                    owed += 1;
                    untaken += 1;
                }
                fence(Ordering::Release);
                core::ptr::write_volatile(self.sq_ktail, tail);
            }
            if owed == 0 {
                continue;
            }

            // With every SQE published, one wait covers the rest; while some
            // are still to come, a completion frees the room they need.
            let wait = if next == count {
                // At most `room`, itself a `u32`.
                owed as u32
            } else {
                1
            };
            #[cfg(test)]
            {
                self.enters += 1;
            }
            // SAFETY: `self.ring_fd` is the live ring fd owned by this `IoUringRaw`.
            match unsafe { io_uring_enter_raw(self.ring_fd, untaken, wait, IORING_ENTER_GETEVENTS) }
            {
                Ok(taken) => untaken -= taken.min(untaken),
                // A signal, or no room for the kernel's own work yet: enter again.
                Err(e) if matches!(e.into_raw(), errno::EINTR | errno::EAGAIN | errno::EBUSY) => {}
                Err(e) if untaken > 0 => {
                    // An `enter` that fails took none of the SQEs it was offered
                    // (one that took some reports how many instead), so the
                    // kernel never reads them: they are taken back off the
                    // ring, and they and the ones never published fail with
                    // the refusal.
                    let refusal = -e.into_raw();
                    // SAFETY: no SQ polling thread, so the kernel reads the
                    // tail only inside `enter`, and it took none of these.
                    unsafe {
                        let tail = core::ptr::read_volatile(self.sq_ktail);
                        core::ptr::write_volatile(self.sq_ktail, tail.wrapping_sub(untaken));
                    }
                    let first = next - untaken as usize;
                    for slot in results.get_mut(first..).into_iter().flatten() {
                        *slot = refusal;
                    }
                    owed -= untaken as usize;
                    untaken = 0;
                    next = count;
                }
                Err(e) => ring_lost(e),
            }

            // SAFETY: read the kernel-advanced CQ tail (Acquire) before any
            // CQE, so no slot is read before the kernel published it, and
            // release the reads (Release) before the advanced head lets the
            // kernel reuse the slots.
            unsafe {
                let mut head = core::ptr::read_volatile(self.cq_khead);
                let tail = core::ptr::read_volatile(self.cq_ktail);
                fence(Ordering::Acquire);
                while head != tail {
                    let cqe = core::ptr::read(self.cqes.add((head & self.cq_ring_mask) as usize));
                    head = head.wrapping_add(1);
                    owed -= 1;
                    if let Some(slot) = usize::try_from(cqe.user_data)
                        .ok()
                        .and_then(|at| results.get_mut(at))
                    {
                        *slot = cqe.res;
                    }
                }
                fence(Ordering::Release);
                core::ptr::write_volatile(self.cq_khead, head);
            }
        }
    }

    /// The submission-queue depth the kernel allocated.
    #[must_use]
    pub fn sq_entries(&self) -> u32 {
        self.sq_entries
    }
}

impl Drop for IoUringRaw {
    fn drop(&mut self) {
        // SAFETY: every pointer/length here came from this struct's own `mmap` /
        // `io_uring_setup` and is unmapped/closed exactly once.
        unsafe {
            munmap(self.sqes.cast(), self.sqes_len);
            if !self.cq_ptr.is_null() {
                munmap(self.cq_ptr, self.cq_len);
            }
            munmap(self.sq_ptr, self.sq_len);
            close(self.ring_fd);
        }
    }
}

/// Closes a fd on drop unless disarmed. Used to release the ring fd if `mmap`
/// fails between `io_uring_setup` and the `IoUringRaw` value taking ownership.
struct FdGuard(i32);

impl FdGuard {
    fn disarm(self) {
        core::mem::forget(self);
    }
}

impl Drop for FdGuard {
    // Coverage: this fires only when `mmap` fails AFTER `io_uring_setup`
    // succeeds — an allocation fault that cannot be provoked in CI. On the
    // covered paths the guard is either disarmed (setup + mmap succeed) or never
    // built (setup itself fails).
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn drop(&mut self) {
        // SAFETY: `self.0` is the live ring fd; only reached on the setup-error
        // path where nothing else owns it yet.
        unsafe { close(self.0) };
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
