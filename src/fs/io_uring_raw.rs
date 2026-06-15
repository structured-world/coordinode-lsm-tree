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

#[cfg(not(feature = "std"))]
use alloc::format;

use core::sync::atomic::{Ordering, fence};

use syscalls::{Errno, Sysno, syscall1, syscall2, syscall6};

use crate::io::{Error, ErrorKind};

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

// `mmap` protection / flags.
const PROT_READ: usize = 0x1;
const PROT_WRITE: usize = 0x2;
const MAP_SHARED: usize = 0x01;
const MAP_POPULATE: usize = 0x0_8000;
/// `mmap` sentinel return for failure (`MAP_FAILED` is `(void *) -1`).
const MAP_FAILED: usize = usize::MAX;

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
unsafe fn mmap(
    len: usize,
    prot: usize,
    flags: usize,
    fd: i32,
    offset: u64,
) -> Result<*mut u8, Error> {
    // SAFETY: forwarded to the kernel. `mmap` returns `MAP_FAILED` (`-1` as
    // usize) on error rather than `-errno`, so check that sentinel explicitly;
    // the syscalls Result form would otherwise treat the huge address as Ok.
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
        Ok(addr) if addr != MAP_FAILED && addr != 0 => Ok(addr as *mut u8),
        Ok(_) => Err(Error::new(ErrorKind::Other, "mmap returned MAP_FAILED")),
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

        // Submit 1, wait for 1 completion.
        // SAFETY: `self.ring_fd` is the live ring fd owned by this `IoUringRaw`.
        let _ = unsafe { io_uring_enter(self.ring_fd, 1, 1, IORING_ENTER_GETEVENTS) }?;

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
}
