// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Pure-Rust, `no_std` `io_uring` driver core on raw Linux syscalls.
//!
//! The std-bound [`IoUringFs`](super::io_uring_fs) backend wraps the `io-uring`
//! crate, which pulls in `std` (it returns `std::io::Error` and consumes
//! `std::os::unix::io::RawFd`). This module drives the kernel ring directly via
//! the `syscalls` crate's raw `syscall!` and its own `mmap`'d ring management,
//! so a Linux `no_std + alloc` consumer gets kernel async I/O with no `std`.
//!
//! This is the driver CORE: it sets up a ring, submits a single SQE, and reaps
//! its CQE. The `IORING_OP_NOP` round-trip exercised by the tests proves the
//! whole submission/completion machinery (ring `mmap`, SQE encode, tail/head
//! handoff, `io_uring_enter`, CQE decode) end to end. File-operation opcodes
//! (READ / WRITE / FSYNC) and the [`Fs`](crate::fs::Fs) / [`FsFile`] impls layer
//! on top of this core in follow-up work.
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

use crate::fs::{Fs, FsDirEntry, FsFile, FsMetadata, FsOpenOptions};
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

/// Maps a raw `errno` to the crate's `no_std` [`ErrorKind`].
fn errno_to_kind(errno: i32) -> ErrorKind {
    // libc errno numbers (Linux generic). Only the ones the I/O paths can
    // realistically surface are mapped explicitly; the rest fold into `Other`,
    // and the numeric errno is preserved in the message for diagnosis.
    match errno {
        1 | 13 => ErrorKind::PermissionDenied, // EPERM / EACCES
        2 => ErrorKind::NotFound,              // ENOENT
        4 => ErrorKind::Interrupted,           // EINTR
        9 | 22 => ErrorKind::InvalidInput,     // EBADF / EINVAL
        11 => ErrorKind::WouldBlock,           // EAGAIN / EWOULDBLOCK
        17 => ErrorKind::AlreadyExists,        // EEXIST
        95 => ErrorKind::Unsupported,          // EOPNOTSUPP
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

/// `io_uring_enter(fd, to_submit, min_complete, flags, NULL, 0)`.
///
/// # Safety
/// `fd` must be a live ring fd from [`io_uring_setup`].
unsafe fn io_uring_enter(
    fd: i32,
    to_submit: u32,
    min_complete: u32,
    flags: u32,
) -> Result<u32, Error> {
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
    }
    .map_err(|e| err("io_uring_enter", e))?;
    Ok(n as u32)
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
    // SAFETY: forwarded to the kernel. The raw `mmap` syscall returns `-errno`
    // (in `[-4095, -1]`) on failure, which the `syscalls` Result form maps to
    // `Err` for us; a success is the actual mapping address (page 0 included,
    // though the kernel does not hand it out for a NULL hint).
    let ret = unsafe {
        syscall6(
            Sysno::mmap,
            0,
            len,
            prot,
            flags,
            fd as usize,
            offset as usize,
        )
    };
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
            flags as usize,
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

/// File type + size, the subset of `statx` the [`Fs`](crate::fs::Fs) metadata
/// surface needs.
pub struct RawMetadata {
    /// File length in bytes (`stx_size`).
    pub size: u64,
    /// Whether the entry is a directory (`S_IFDIR`).
    pub is_dir: bool,
    /// Whether the entry is a regular file (`S_IFREG`).
    pub is_file: bool,
}

/// `statx(fd, "", AT_EMPTY_PATH, STATX_BASIC_STATS, &buf)` — stats an open
/// descriptor without a path lookup.
///
/// # Errors
/// Returns an [`Error`] if the `statx` syscall fails.
pub fn fstat_raw(fd: i32) -> Result<RawMetadata, Error> {
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
        is_dir: kind == S_IFDIR,
        is_file: kind == S_IFREG,
    })
}

/// `ftruncate(fd, length)` — set the file size (extend with zeros or truncate).
///
/// # Errors
/// Returns an [`Error`] if the `ftruncate` syscall fails.
pub fn ftruncate_raw(fd: i32, length: u64) -> Result<(), Error> {
    // SAFETY: `fd` is an owned writable descriptor; `length` is a plain value.
    unsafe { syscall2(Sysno::ftruncate, fd as usize, length as usize) }
        .map_err(|e| err("ftruncate", e))?;
    Ok(())
}

/// `lseek(fd, offset, whence)` — reposition; returns the resulting absolute
/// offset. Used to resolve the file size (`SEEK_END`) for append / `Seek::End`.
///
/// # Errors
/// Returns an [`Error`] if the `lseek` syscall fails.
pub fn lseek_raw(fd: i32, offset: i64, whence: i32) -> Result<u64, Error> {
    // SAFETY: `fd` is an owned descriptor; offset/whence are plain values.
    let pos = unsafe { syscall3(Sysno::lseek, fd as usize, offset as usize, whence as usize) }
        .map_err(|e| err("lseek", e))?;
    Ok(pos as u64)
}

/// `flock(fd, LOCK_EX[|LOCK_NB])` — take an advisory exclusive lock. With
/// `non_blocking`, returns `Ok(false)` instead of waiting when the lock is held.
///
/// # Errors
/// Returns an [`Error`] if the `flock` syscall fails for a reason other than the
/// non-blocking contention case.
pub fn flock_exclusive_raw(fd: i32, non_blocking: bool) -> Result<bool, Error> {
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
pub fn file_size_raw(fd: i32) -> Result<u64, Error> {
    lseek_raw(fd, 0, SEEK_END)
}

// ---- Directory / path syscalls (for the `Fs` backend) ----
//
// These use the asm-generic flag values, authoritative on x86/x86_64, ARM,
// AArch64, RISC-V, PowerPC, etc. (the realistic `io_uring` targets). The handful
// of flags that diverge on MIPS / SPARC (O_APPEND / O_EXCL / O_DIRECTORY) are not
// remapped here, matching the `direct_io` policy documented on `FsOpenOptions`.

/// Append: writes always land at end of file.
const O_APPEND: i32 = 0o2000;
/// Fail `open` with `EEXIST` if the path already exists (with `O_CREAT`).
const O_EXCL: i32 = 0o200;
/// Require the opened path to be a directory.
const O_DIRECTORY: i32 = 0o200_000;

/// `unlinkat` flag: remove a directory (`rmdir`) instead of a file.
const AT_REMOVEDIR: i32 = 0x200;

/// `linux_dirent64.d_type`: directory.
const DT_DIR: u8 = 4;

/// `mkdirat(AT_FDCWD, path, mode)` — create a directory.
///
/// # Errors
/// Returns an [`Error`] if the `mkdirat` syscall fails.
pub fn mkdirat_raw(path: &core::ffi::CStr, mode: u32) -> Result<(), Error> {
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
pub fn unlinkat_raw(path: &core::ffi::CStr, remove_dir: bool) -> Result<(), Error> {
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

/// `renameat2(AT_FDCWD, from, AT_FDCWD, to, 0)` — rename, replacing the
/// destination (the plain `rename(2)` behaviour).
///
/// # Errors
/// Returns an [`Error`] if the `renameat2` syscall fails.
pub fn renameat2_raw(from: &core::ffi::CStr, to: &core::ffi::CStr) -> Result<(), Error> {
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
pub fn statx_path_raw(path: &core::ffi::CStr) -> Result<Option<RawMetadata>, Error> {
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
        Ok(_) => {
            let kind = buf.stx_mode & S_IFMT;
            Ok(Some(RawMetadata {
                size: buf.stx_size,
                is_dir: kind == S_IFDIR,
                is_file: kind == S_IFREG,
            }))
        }
        // ENOENT (2): the path does not exist — a normal "not found" answer.
        Err(e) if e.into_raw() == 2 => Ok(None),
        Err(e) => Err(err("statx", e)),
    }
}

/// The kernel `struct statfs` for 64-bit Linux (x86-64 / aarch64). Unlike
/// `statx`, this struct is architecture-dependent; this backend is gated to
/// 64-bit Linux, where the layout below is stable. Only `f_bsize` and
/// `f_bavail` are read.
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
/// filesystem backing `path`: `f_bavail * f_bsize`. Raw syscall (no libc),
/// matching this backend's no-libc design.
///
/// # Errors
/// Returns an [`Error`] if the `statfs` syscall fails.
pub fn statfs_available_raw(path: &core::ffi::CStr) -> Result<u64, Error> {
    let mut buf = Statfs::default();
    // SAFETY: `path` is a valid NUL-terminated C string the kernel reads;
    // `buf` is a valid, writable Statfs the kernel fills on success.
    unsafe { syscall2(Sysno::statfs, path.as_ptr() as usize, &raw mut buf as usize) }
        .map_err(|e| err("statfs", e))?;
    // `f_bavail` counts blocks free to a non-root caller; `f_bsize` is the
    // block size. Saturate so an implausible value can never wrap.
    let bsize = buf.f_bsize as u64;
    Ok(buf.f_bavail.saturating_mul(bsize))
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
/// syscalls. It slots into the [`Fs`](crate::fs::Fs) abstraction like the
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
        self.cursor = self.cursor.saturating_add(n as u64);
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
        self.cursor = self.cursor.saturating_add(n as u64);
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

    fn lock_exclusive(&self) -> crate::io::Result<()> {
        flock_exclusive_raw(self.fd, false)?;
        Ok(())
    }

    fn try_lock_exclusive(&self) -> crate::io::Result<bool> {
        flock_exclusive_raw(self.fd, true)
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

/// Maps [`FsOpenOptions`] to `open(2)` flags (asm-generic values; see the
/// directory-syscall note for the MIPS / SPARC caveat on the divergent flags).
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
/// `io-uring` crate and no `std::fs`, unlike the std-bound [`IoUringFs`].
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
}

impl Fs for IoUringRawFs {
    fn open(&self, path: &Path, opts: &FsOpenOptions) -> crate::io::Result<Box<dyn FsFile>> {
        let cpath = path_to_cstring(path)?;
        let fd = open_raw(&cpath, open_flags(opts), 0o644)?;
        Ok(Box::new(IoUringRawFile::new(
            Arc::clone(&self.ring),
            fd,
            opts.append,
        )))
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

    fn rename(&self, from: &Path, to: &Path) -> crate::io::Result<()> {
        renameat2_raw(&path_to_cstring(from)?, &path_to_cstring(to)?)
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

    /// Submits one SQE and reaps exactly one completion, returning its `res`.
    ///
    /// This is the shared submit/complete path the opcode helpers build on. It
    /// follows the full `io_uring` ring protocol: a submit-side overflow guard
    /// against the kernel-advanced SQ head, an `io_uring_enter` that submits one
    /// and blocks for one completion, and a reap that consults the
    /// kernel-advanced CQ tail before reading the CQE.
    ///
    /// # Errors
    /// Returns an [`Error`] if the SQ is full, `io_uring_enter` fails, no
    /// completion is visible after the wait, or the completion's `res` is a
    /// negative `-errno`.
    // Coverage: the SQ-full and no-completion guards are defensive and
    // unreachable through this one-submit-one-reap driver (it never lets the SQ
    // fill or `io_uring_enter` return without a completion); they would need
    // kernel fault injection. The submit/enter/reap success path and the
    // negative-`res` error path ARE covered by tests.
    #[cfg_attr(coverage_nightly, coverage(off))]
    fn submit_and_reap_one(&mut self, sqe: &IoUringSqe) -> Result<i32, Error> {
        // SAFETY: single-threaded driver. Read the kernel-advanced SQ head
        // (Acquire) to confirm the ring has a free slot, write the SQE into
        // `tail & mask`, then publish the new tail (Release) so the kernel sees
        // a fully-written SQE before the index it reads.
        unsafe {
            let tail = core::ptr::read_volatile(self.sq_ktail);
            let head = core::ptr::read_volatile(self.sq_khead);
            fence(Ordering::Acquire);
            if tail.wrapping_sub(head) >= self.sq_entries {
                return Err(Error::new(
                    ErrorKind::Other,
                    "io_uring submission queue is full",
                ));
            }
            let index = tail & self.sq_ring_mask;
            core::ptr::write(self.sqes.add(index as usize), *sqe);
            core::ptr::write(self.sq_array.add(index as usize), index);
            fence(Ordering::Release);
            core::ptr::write_volatile(self.sq_ktail, tail.wrapping_add(1));
        }

        // Submit 1, wait for 1 completion. Retry on EINTR: the SQE is already
        // published to the SQ (the tail was advanced above), so a
        // signal-interrupted `enter` must be re-entered to reap that in-flight
        // submission rather than returning and leaking it (the next call would
        // then reap this completion against the wrong `user_data`). The kernel
        // submits `min(to_submit, pending)` SQEs, so a retry after the first
        // `enter` already consumed the entry simply waits for the completion.
        loop {
            // SAFETY: `self.ring_fd` is the live ring fd owned by this `IoUringRaw`.
            match unsafe { io_uring_enter(self.ring_fd, 1, 1, IORING_ENTER_GETEVENTS) } {
                Ok(_) => break,
                // EINTR: fall through to re-enter the ring on the next iteration.
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }

        // SAFETY: read the kernel-advanced CQ tail (Acquire) before touching the
        // CQE so we never read a slot the kernel has not published. After a
        // successful `enter` with `min_complete == 1` a completion is present,
        // but the explicit tail check keeps the reaper correct and ready to
        // drain multiple completions in later opcode work.
        let res = unsafe {
            let head = core::ptr::read_volatile(self.cq_khead);
            let ktail = core::ptr::read_volatile(self.cq_ktail);
            fence(Ordering::Acquire);
            if head == ktail {
                return Err(Error::new(
                    ErrorKind::Other,
                    "io_uring_enter returned with no completion",
                ));
            }
            let cqe = core::ptr::read(self.cqes.add((head & self.cq_ring_mask) as usize));
            // Release the CQE read before publishing the advanced head: the
            // (non-volatile) read above must complete before the kernel can see
            // the new head and reuse the slot, otherwise the read could be
            // reordered after the head store and observe a torn / overwritten
            // entry. Matters once a future reaper drains multiple CQEs.
            fence(Ordering::Release);
            core::ptr::write_volatile(self.cq_khead, head.wrapping_add(1));
            cqe.res
        };
        if res < 0 {
            return Err(Error::new(
                errno_to_kind(-res),
                format!("io_uring op completed with errno {}", -res),
            ));
        }
        Ok(res)
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
mod tests {
    use super::*;

    // These tests require a Linux kernel with io_uring; they are compiled only
    // under `cfg(target_os = "linux")` (the module gate) and run on the Linux
    // CI / bench runner, not on the macOS dev host.

    #[test]
    fn raw_file_fsfile_round_trips() {
        // Exercises the full IoUringRawFile FsFile surface: sequential write
        // (ring), positioned read_at (ring, fill-or-EOF), metadata (statx),
        // Seek + sequential Read (cursor), set_len (ftruncate), fsync (ring),
        // and try_lock_exclusive (flock) — all over the raw no_std driver.
        use std::io::{Read, Seek, Write};

        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("iou_rawfile.bin");
        let cpath = std::ffi::CString::new(path.to_str().expect("utf8 path"))
            .expect("path has no interior NUL");

        let fd =
            open_raw(&cpath, O_CREAT | O_RDWR | O_TRUNC, 0o600).expect("openat should succeed");
        let ring = Arc::new(Mutex::new(IoUringRaw::new(8).expect("ring setup")));
        let mut file = IoUringRawFile::new(ring, fd, false);

        let payload = b"raw io_uring file round-trip payload";
        let written = Write::write(&mut file, payload).expect("write");
        assert_eq!(written, payload.len(), "short write");

        // statx-backed metadata reflects the written length.
        assert_eq!(
            FsFile::metadata(&file).expect("metadata").len,
            payload.len() as u64,
            "metadata len must match what was written"
        );

        // Positioned fill-or-EOF read returns the exact bytes.
        let mut rb = vec![0u8; payload.len()];
        let n = FsFile::read_at(&file, &mut rb, 0).expect("read_at");
        assert_eq!(n, payload.len(), "short read_at");
        assert_eq!(&rb, payload, "read_at bytes must match");

        // Seek + sequential Read from an offset uses the userspace cursor.
        file.seek(std::io::SeekFrom::Start(4)).expect("seek");
        let mut tail = vec![0u8; payload.len() - 4];
        let mut got = 0;
        while got < tail.len() {
            let chunk = tail.get_mut(got..).expect("in-bounds");
            let r = Read::read(&mut file, chunk).expect("read");
            if r == 0 {
                break;
            }
            got += r;
        }
        assert_eq!(
            tail.get(..got).expect("in-bounds"),
            payload.get(4..).expect("in-bounds"),
            "seek+read bytes must match"
        );

        FsFile::sync_all(&file).expect("sync_all");
        FsFile::sync_data(&file).expect("sync_data");

        // ftruncate shrinks the file; metadata reflects it.
        FsFile::set_len(&file, 4).expect("set_len");
        assert_eq!(
            FsFile::metadata(&file).expect("metadata after set_len").len,
            4,
            "set_len must shrink the file"
        );

        // A fresh file is unlocked, so a non-blocking exclusive lock succeeds.
        assert!(
            FsFile::try_lock_exclusive(&file).expect("try_lock_exclusive"),
            "exclusive lock on a fresh file must succeed"
        );

        drop(file); // closes the fd; `tmp` drops at end of scope, removing the dir
    }

    #[test]
    fn raw_fs_directory_and_file_ops() {
        // Exercises the IoUringRawFs Fs surface end to end: create_dir_all
        // (mkdirat), open + write (ring), metadata + exists (statx), read_dir
        // (getdents64), rename (renameat2), remove_file + remove_dir_all
        // (unlinkat) — all over the raw no_std driver.
        use crate::fs::Fs;
        use crate::path::Path;
        use std::io::Write;

        // A subdirectory under a `tempfile::tempdir()` guard: `create_dir_all`
        // creates it, and the guard removes the whole tree on drop even if an
        // assertion panics before the explicit `remove_dir_all` below.
        let tmp = tempfile::tempdir().expect("tempdir");
        let base = tmp.path().join("rawfs");
        let base_s = base.to_str().expect("utf8 temp path");
        let fs = IoUringRawFs::new(8).expect("fs setup");
        let dir = Path::new(base_s);

        fs.create_dir_all(dir).expect("create_dir_all");
        assert!(fs.exists(dir).expect("dir exists"));
        assert!(fs.metadata(dir).expect("dir metadata").is_dir);

        // Open + write a file through the ring.
        let fpath = dir.join("a.txt");
        let mut file = fs
            .open(&fpath, &FsOpenOptions::new().write(true).create(true))
            .expect("open");
        file.write_all(b"hello").expect("write");
        file.sync_all().expect("sync");
        drop(file);

        // statx-backed metadata + getdents64 listing see the file.
        assert_eq!(fs.metadata(&fpath).expect("file metadata").len, 5);
        let entries = fs.read_dir(dir).expect("read_dir");
        assert!(
            entries.iter().any(|e| e.file_name == "a.txt" && !e.is_dir),
            "read_dir must list the written file"
        );

        // rename (renameat2) moves it.
        let fpath2 = dir.join("b.txt");
        fs.rename(&fpath, &fpath2).expect("rename");
        assert!(!fs.exists(&fpath).expect("old gone"));
        assert!(fs.exists(&fpath2).expect("new present"));

        // unlinkat removes the file, then the (now empty) directory.
        fs.remove_file(&fpath2).expect("remove_file");
        assert!(!fs.exists(&fpath2).expect("file gone"));
        fs.remove_dir_all(dir).expect("remove_dir_all");
        assert!(!fs.exists(dir).expect("dir gone"));
    }

    #[test]
    fn raw_file_append_writes_accumulate_at_eof() {
        // O_APPEND writes always land at end of file: two sequential appends
        // (across reopens) both persist, in order — exercising the kernel's
        // atomic-append path (offset -1) the write impl uses for append mode.
        use crate::fs::Fs;
        use crate::path::Path;
        use std::io::Write;

        let tmp = tempfile::tempdir().expect("tempdir");
        let p = tmp.path().join("log.bin");
        let ps = p.to_str().expect("utf8 temp path");
        let fs = IoUringRawFs::new(8).expect("fs setup");
        let path = Path::new(ps);

        {
            let mut f = fs
                .open(path, &FsOpenOptions::new().append(true).create(true))
                .expect("open append");
            f.write_all(b"aaa").expect("first append");
            f.sync_all().expect("sync");
        }
        {
            let mut f = fs
                .open(path, &FsOpenOptions::new().append(true).create(true))
                .expect("reopen append");
            f.write_all(b"bbb").expect("second append");
            f.sync_all().expect("sync");
        }

        let f = fs
            .open(path, &FsOpenOptions::new().read(true))
            .expect("open read");
        let mut buf = [0u8; 6];
        let n = f.read_at(&mut buf, 0).expect("read_at");
        assert_eq!(n, 6, "both appends must be present");
        assert_eq!(&buf, b"aaabbb", "appends land in order at EOF");
    }

    #[test]
    fn ring_setup_and_nop_round_trips() {
        let mut ring = IoUringRaw::new(8).expect("io_uring_setup + mmap should succeed");
        assert!(
            ring.sq_entries() >= 8,
            "kernel rounds entries up to >= request"
        );
        // A NOP completes immediately with res == 0 and echoes our user_data
        // slot through the full submit -> enter -> complete cycle.
        let res = ring
            .nop(0xDEAD_BEEF)
            .expect("NOP submit/complete should succeed");
        assert_eq!(res, 0, "NOP res must be 0");
    }

    #[test]
    fn multiple_nops_reuse_slots() {
        let mut ring = IoUringRaw::new(4).expect("setup");
        // Submit more NOPs than the ring depth to exercise tail/head wraparound
        // and slot reuse across several enter calls.
        for i in 0..16u64 {
            let res = ring.nop(i).expect("nop");
            assert_eq!(res, 0);
        }
    }

    #[test]
    fn file_write_fsync_read_round_trips_through_the_ring() {
        // Real data path: open a file (raw openat), write a payload at an
        // offset through the ring, fsync it through the ring, read it back
        // through the ring, and verify the bytes — then close (raw) + clean up.
        let path = std::env::temp_dir().join(format!(
            "iou_raw_rt_{}_{}.bin",
            std::process::id(),
            // a per-test suffix so parallel runs do not collide
            line!()
        ));
        let cpath = std::ffi::CString::new(path.to_str().expect("utf8 path"))
            .expect("path has no interior NUL");

        let fd =
            open_raw(&cpath, O_CREAT | O_RDWR | O_TRUNC, 0o600).expect("openat should succeed");

        let mut ring = IoUringRaw::new(8).expect("ring setup");

        let payload = b"io_uring raw round-trip payload";
        let offset = 4096u64; // a non-zero offset to exercise positioned I/O

        let written = ring
            .write_at(fd, payload, offset)
            .expect("write_at should succeed");
        assert_eq!(written, payload.len(), "short write");

        ring.fsync(fd, false).expect("fsync should succeed");

        let mut readback = vec![0u8; payload.len()];
        let read = ring
            .read_at(fd, &mut readback, offset)
            .expect("read_at should succeed");
        assert_eq!(read, payload.len(), "short read");
        assert_eq!(
            &readback, payload,
            "read-back bytes must match what we wrote"
        );

        // Reading past EOF returns 0 bytes (the file is exactly offset+len long).
        let mut tail = [0u8; 8];
        let eof = ring
            .read_at(fd, &mut tail, offset + payload.len() as u64)
            .expect("read at EOF should succeed");
        assert_eq!(eof, 0, "read past EOF must return 0");

        close_raw(fd).expect("close should succeed");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn errno_maps_to_expected_error_kinds() {
        // Every explicitly-mapped errno lands on its kind; anything else folds
        // into `Other`. Covers the whole `errno_to_kind` table without needing
        // to provoke each real failure.
        assert_eq!(errno_to_kind(1), ErrorKind::PermissionDenied); // EPERM
        assert_eq!(errno_to_kind(2), ErrorKind::NotFound); // ENOENT
        assert_eq!(errno_to_kind(4), ErrorKind::Interrupted); // EINTR
        assert_eq!(errno_to_kind(9), ErrorKind::InvalidInput); // EBADF
        assert_eq!(errno_to_kind(11), ErrorKind::WouldBlock); // EAGAIN
        assert_eq!(errno_to_kind(13), ErrorKind::PermissionDenied); // EACCES
        assert_eq!(errno_to_kind(17), ErrorKind::AlreadyExists); // EEXIST
        assert_eq!(errno_to_kind(22), ErrorKind::InvalidInput); // EINVAL
        assert_eq!(errno_to_kind(95), ErrorKind::Unsupported); // EOPNOTSUPP
        assert_eq!(errno_to_kind(132), ErrorKind::Other); // unmapped
    }

    #[test]
    fn ring_setup_with_zero_entries_is_rejected() {
        // `io_uring_setup` rejects a zero-entry ring with EINVAL; this covers
        // the setup error path (and the fd guard never arming). `IoUringRaw`
        // is not `Debug`, so match rather than `expect_err`.
        match IoUringRaw::new(0) {
            Ok(_) => panic!("zero-entry ring must be rejected"),
            Err(e) => assert_eq!(e.kind(), ErrorKind::InvalidInput),
        }
    }

    #[test]
    fn open_raw_missing_file_returns_not_found() {
        // Opening a missing path read-only (no O_CREAT) fails with ENOENT,
        // exercising `open_raw`'s error path and the `err` -> `ErrorKind`
        // conversion.
        let cpath = std::ffi::CString::new("/proc/does-not-exist/iou_raw_missing")
            .expect("no interior NUL");
        let err = open_raw(&cpath, O_RDONLY, 0).expect_err("missing file must fail to open");
        assert_eq!(err.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn ring_op_on_bad_fd_surfaces_completion_error() {
        // A write against a descriptor that is not open completes with -EBADF;
        // the reaper must surface that negative `res` as an error (covering the
        // `res < 0` branch of `submit_and_reap_one`).
        let mut ring = IoUringRaw::new(4).expect("ring setup");
        let err = ring
            .write_at(1 << 30, b"x", 0)
            .expect_err("write to a non-open fd must fail");
        assert_eq!(err.kind(), ErrorKind::InvalidInput); // EBADF -> InvalidInput
    }
}
