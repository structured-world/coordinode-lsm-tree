// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A read target that may start uninitialized.
//!
//! `FsFile::read_at` takes `&mut [u8]`, and a byte slice over memory nothing
//! has written is not one, so a [`ReadBuf`] hands unwritten memory out only as
//! `MaybeUninit` bytes. It keeps count of what is initialized: safe code can
//! only ask for zeroed bytes and mark as filled what it was given, so the
//! zeroing happens only where the code doing the read is unknown, and a backend
//! whose read only writes (a `pread`, a kernel ring read, a copy) fills the
//! memory as it is, through [`ReadBuf::fill_with`].

use core::mem::MaybeUninit;

use crate::io;

/// What [`FsFile::read_at_buf`](crate::fs::FsFile::read_at_buf) returns.
pub type ReadResult = io::Result<()>;

/// A buffer for one read, tracking how much of it is initialized and filled.
///
/// Only this crate makes one: an implementation that could make its own could
/// swap it in behind the `&mut` it was given, and report filled a buffer other
/// than the one the caller is about to trust.
///
/// # Examples
///
/// A backend whose read only writes, in the body of its
/// [`read_at_buf`](crate::fs::FsFile::read_at_buf), spares the zeroing:
///
/// ```
/// use lsm_tree::fs::ReadBuf;
///
/// fn read_at_buf(buf: &mut ReadBuf<'_>, device: &[u8]) -> lsm_tree::fs::ReadResult {
///     // SAFETY: the closure writes only initialized bytes, and the first `n`
///     // of them, which it returns.
///     unsafe {
///         buf.fill_with(|memory| {
///             let n = memory.len().min(device.len());
///             memory[..n].write_copy_of_slice(&device[..n]);
///             Ok(n)
///         })
///     }
/// }
/// ```
pub struct ReadBuf<'a> {
    buf: &'a mut [MaybeUninit<u8>],
    /// The bytes from the start known to be initialized.
    init: usize,
    /// The bytes from the start the read filled, never past `init`.
    filled: usize,
}

impl<'a> ReadBuf<'a> {
    /// A buffer over bytes that may be uninitialized.
    pub(crate) fn uninit(buf: &'a mut [MaybeUninit<u8>]) -> Self {
        Self::partly_init(buf, 0)
    }

    /// A buffer over bytes of which the first `init` are initialized.
    pub(crate) fn partly_init(buf: &'a mut [MaybeUninit<u8>], init: usize) -> Self {
        debug_assert!(init <= buf.len(), "initialized past the buffer");
        Self {
            buf,
            init,
            filled: 0,
        }
    }

    /// The bytes from the start known to be initialized.
    pub(crate) fn initialized(&self) -> usize {
        self.init
    }

    /// The bytes the buffer can take.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// The bytes filled so far.
    #[must_use]
    pub fn filled(&self) -> usize {
        self.filled
    }

    /// The whole buffer as initialized bytes, zeroing what is not yet.
    pub fn init_mut(&mut self) -> &mut [u8] {
        // `init` never passes the end, so the rest is always there.
        let rest = self.buf.get_mut(self.init..).unwrap_or_default();
        rest.fill(MaybeUninit::new(0));
        self.init = self.buf.len();
        // SAFETY: every byte is initialized as of the line above, and
        // `MaybeUninit<u8>` has the layout of `u8`.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            core::slice::from_raw_parts_mut(self.buf.as_mut_ptr().cast::<u8>(), self.buf.len())
        }
    }

    /// Marks the first `filled` bytes as filled.
    ///
    /// # Errors
    ///
    /// `filled` exceeds what is initialized, as a read reporting more bytes
    /// than its buffer holds does.
    pub fn set_filled(&mut self, filled: usize) -> io::Result<()> {
        if filled > self.init {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "read reported more bytes than its buffer holds",
            ));
        }
        self.filled = filled;
        Ok(())
    }

    /// Marks the first `filled` bytes as filled and initialized.
    ///
    /// # Safety
    ///
    /// The first `filled` bytes must have been written, and `filled` must not
    /// exceed the capacity.
    pub unsafe fn assume_filled(&mut self, filled: usize) {
        debug_assert!(filled <= self.buf.len(), "filled past the buffer");
        self.filled = filled;
        self.init = self.init.max(filled);
    }

    /// Fills the buffer with `read`, which is given the whole buffer as it is,
    /// initialized or not, and returns the bytes it wrote from its start.
    ///
    /// # Safety
    ///
    /// `read` must write only initialized bytes into the memory it is given
    /// (never `MaybeUninit::uninit()`, which would undo bytes counted
    /// initialized), and must have written as many bytes from its start as
    /// it returns.
    ///
    /// # Errors
    ///
    /// The error of `read`.
    pub unsafe fn fill_with(
        &mut self,
        read: impl FnOnce(&mut [MaybeUninit<u8>]) -> io::Result<usize>,
    ) -> io::Result<()> {
        let filled = read(&mut *self.buf)?;
        // SAFETY: the caller promises `read` wrote `filled` bytes.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            self.assume_filled(filled);
        }
        Ok(())
    }

    /// Fills the next `bytes.len()` bytes with `bytes`.
    ///
    /// # Panics
    ///
    /// Panics if they do not fit.
    pub(crate) fn append(&mut self, bytes: &[u8]) {
        let end = self.filled + bytes.len();
        #[expect(clippy::indexing_slicing, reason = "panics by contract")]
        let target = &mut self.buf[self.filled..end];
        for (to, from) in target.iter_mut().zip(bytes) {
            to.write(*from);
        }
        self.filled = end;
        self.init = self.init.max(end);
    }

    /// The `len` bytes at `at`, as a buffer of their own, none of them filled.
    ///
    /// # Panics
    ///
    /// Panics if the range lies past the buffer.
    pub(crate) fn portion(&mut self, at: usize, len: usize) -> ReadBuf<'_> {
        let init = (self.init.max(at) - at).min(len);
        ReadBuf {
            #[expect(clippy::indexing_slicing, reason = "panics by contract")]
            buf: &mut self.buf[at..at + len],
            init,
            filled: 0,
        }
    }

    /// Takes the filled bytes of a [`Self::portion`] of this buffer starting
    /// at `self.filled()`, which `portion` was.
    pub(crate) fn extend_from_portion(&mut self, filled: usize) {
        // SAFETY: a portion counts what its read filled, in the bytes right
        // past what this buffer has filled.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            self.assume_filled(self.filled + filled);
        }
    }
}

/// Reads `regions` one after the other through
/// [`read_at_buf`](crate::fs::FsFile::read_at_buf), for a backend with no batch.
pub(crate) fn read_each(
    file: &(impl crate::fs::FsFile + ?Sized),
    regions: &mut [(u64, ReadBuf<'_>)],
) -> io::Result<()> {
    for (offset, buf) in regions {
        file.read_at_buf(buf, *offset)?;
        if buf.filled() != buf.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read_many: short read on a fixed-size block region",
            ));
        }
    }
    Ok(())
}

/// One region of a batched read, so a ring backend takes initialized and
/// uninitialized regions alike without gathering them into another list.
#[cfg(any(io_uring_fs, all(target_os = "linux", feature = "io-uring-raw")))]
pub(crate) trait ReadRegion {
    /// The region's file offset, and where and how long its memory is: a
    /// pointer for the kernel to write, never a byte slice over memory that
    /// may not be initialized.
    fn target(&mut self) -> (u64, *mut u8, usize);
}

#[cfg(any(io_uring_fs, all(target_os = "linux", feature = "io-uring-raw")))]
impl ReadRegion for (u64, &mut [u8]) {
    fn target(&mut self) -> (u64, *mut u8, usize) {
        (self.0, self.1.as_mut_ptr(), self.1.len())
    }
}

#[cfg(any(io_uring_fs, all(target_os = "linux", feature = "io-uring-raw")))]
impl ReadRegion for (u64, ReadBuf<'_>) {
    fn target(&mut self) -> (u64, *mut u8, usize) {
        (
            self.0,
            self.1.buf.as_mut_ptr().cast::<u8>(),
            self.1.buf.len(),
        )
    }
}

/// `bytes` as memory a read may write, for a backend whose read of
/// [`MaybeUninit`] memory serves its `read_at` too.
///
/// # Safety
///
/// The caller must write only initialized bytes into it: the caller of
/// `read_at` lent initialized bytes and may read them all afterwards.
pub(crate) unsafe fn as_uninit_mut(bytes: &mut [u8]) -> &mut [MaybeUninit<u8>] {
    // SAFETY: `MaybeUninit<u8>` has the layout of `u8`, and the caller writes
    // nothing uninitialized.
    #[expect(unsafe_code, reason = "see safety")]
    unsafe {
        core::slice::from_raw_parts_mut(bytes.as_mut_ptr().cast::<MaybeUninit<u8>>(), bytes.len())
    }
}

/// Marks every region filled.
///
/// # Safety
///
/// Every region must have been written whole.
#[cfg(any(io_uring_fs, all(target_os = "linux", feature = "io-uring-raw")))]
pub(crate) unsafe fn assume_regions_filled(regions: &mut [(u64, ReadBuf<'_>)]) {
    for (_, buf) in regions {
        // SAFETY: the caller promises it.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            buf.assume_filled(buf.capacity());
        }
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;

#[cfg(all(test, feature = "std"))]
#[expect(clippy::expect_used, clippy::indexing_slicing, reason = "test code")]
mod backend_tests;
