// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A read target that may start uninitialized.
//!
//! `FsFile::read_at` takes `&mut [u8]`, and a safe implementation may read that
//! slice, so handing it unwritten memory would be undefined behaviour reachable
//! from safe code. A [`ReadBuf`] keeps count of what is initialized: safe code
//! can only ask for zeroed bytes and mark as filled what it was given, so the
//! zeroing happens only where the code doing the read is unknown, and a backend
//! known to only write skips it through the `unsafe` methods.

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
///     // SAFETY: the closure writes the slice, reads none of it, and returns
///     // how much it wrote.
///     unsafe {
///         buf.fill_with(|bytes| {
///             let n = bytes.len().min(device.len());
///             bytes[..n].copy_from_slice(&device[..n]);
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
        Self {
            buf,
            init: 0,
            filled: 0,
        }
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
        if let Some(rest) = self.buf.get_mut(self.init..) {
            rest.fill(MaybeUninit::new(0));
        }
        self.init = self.buf.len();
        // SAFETY: every byte is initialized as of the line above.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            self.bytes_mut_unchecked()
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

    /// The whole buffer as bytes, whether initialized or not.
    ///
    /// # Safety
    ///
    /// The caller must not read a byte of it before writing that byte, nor
    /// hand it to code that might.
    #[must_use]
    pub unsafe fn bytes_mut_unchecked(&mut self) -> &mut [u8] {
        // SAFETY: `MaybeUninit<u8>` has the layout of `u8`, and the caller
        // promises to read nothing it did not write.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            core::slice::from_raw_parts_mut(self.buf.as_mut_ptr().cast::<u8>(), self.buf.len())
        }
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

    /// Fills the buffer with `read`, which returns the bytes it wrote, without
    /// zeroing it first.
    ///
    /// # Safety
    ///
    /// `read` must only write the slice it is given, and must have written as
    /// many bytes from its start as it returns.
    ///
    /// # Errors
    ///
    /// The error of `read`.
    pub unsafe fn fill_with(
        &mut self,
        read: impl FnOnce(&mut [u8]) -> io::Result<usize>,
    ) -> io::Result<()> {
        // SAFETY: the caller promises `read` only writes.
        #[expect(unsafe_code, reason = "see safety")]
        let filled = read(unsafe { self.bytes_mut_unchecked() })?;
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
#[cfg(all(
    target_os = "linux",
    any(feature = "io-uring", feature = "io-uring-raw")
))]
pub(crate) trait ReadRegion {
    /// The region's file offset and bytes.
    ///
    /// # Safety
    ///
    /// The caller must only write the bytes, as a kernel read does.
    unsafe fn target(&mut self) -> (u64, &mut [u8]);
}

#[cfg(all(
    target_os = "linux",
    any(feature = "io-uring", feature = "io-uring-raw")
))]
impl ReadRegion for (u64, &mut [u8]) {
    unsafe fn target(&mut self) -> (u64, &mut [u8]) {
        (self.0, &mut *self.1)
    }
}

#[cfg(all(
    target_os = "linux",
    any(feature = "io-uring", feature = "io-uring-raw")
))]
impl ReadRegion for (u64, ReadBuf<'_>) {
    unsafe fn target(&mut self) -> (u64, &mut [u8]) {
        // SAFETY: the caller only writes the bytes.
        #[expect(unsafe_code, reason = "see safety")]
        let bytes = unsafe { self.1.bytes_mut_unchecked() };
        (self.0, bytes)
    }
}

/// Marks every region filled.
///
/// # Safety
///
/// Every region must have been written whole.
#[cfg(all(
    target_os = "linux",
    any(feature = "io-uring", feature = "io-uring-raw")
))]
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
