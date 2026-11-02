// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! The destination of a queued block read, which becomes the block's
//! [`Slice`] once filled.

use super::{BlockBuf, BlockProgress, FsFile};
use crate::Slice;
use crate::slice::UnfilledSlice;
use alloc::boxed::Box;
use core::mem::MaybeUninit;

/// The destination of one [`QueuedRead`](super::QueuedRead): memory allocated
/// unzeroed, which a read fills in place and which then becomes the block's
/// [`Slice`] without a copy.
///
/// It keeps count of how much of it is initialized and how much was filled,
/// as a [`BlockBuf`] does, and fills only through one: see
/// [`fill_with`](Self::fill_with).
///
/// # Examples
///
/// ```
/// use lsm_tree::fs::QueuedBuf;
///
/// let mut buf = QueuedBuf::new(5);
/// buf.fill_with(|block| block.append(b"hello"));
/// assert!(buf.is_full());
/// assert_eq!(buf.into_slice().as_deref(), Some(&b"hello"[..]));
/// ```
pub struct QueuedBuf {
    storage: Storage,
    /// The bytes from the start known to be initialized.
    init: usize,
    filled: usize,
}

/// Where the bytes are. A short slice holds its bytes inline, so it is boxed
/// here: a read in flight writes through a pointer taken at submission, and
/// the buffer moves between the queue's lists meanwhile.
enum Storage {
    Direct(UnfilledSlice),
    Boxed(Box<UnfilledSlice>),
}

impl Storage {
    fn get(&self) -> &UnfilledSlice {
        match self {
            Self::Direct(slice) => slice,
            Self::Boxed(slice) => slice,
        }
    }

    fn get_mut(&mut self) -> &mut UnfilledSlice {
        match self {
            Self::Direct(slice) => slice,
            Self::Boxed(slice) => slice,
        }
    }
}

impl QueuedBuf {
    /// A destination of `len` bytes, none of them written.
    #[must_use]
    pub fn new(len: usize) -> Self {
        let slice = UnfilledSlice::new(len);
        let storage = if slice.bytes_move_with_it() {
            Storage::Boxed(Box::new(slice))
        } else {
            Storage::Direct(slice)
        };
        Self {
            storage,
            init: 0,
            filled: 0,
        }
    }

    /// How many bytes the read wants.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.storage.get().uninit().len()
    }

    /// How many bytes have been written so far.
    #[must_use]
    pub fn filled(&self) -> usize {
        self.filled
    }

    /// Whether the read has been filled completely.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.filled == self.capacity()
    }

    /// The bytes written so far: the whole block once [`is_full`](Self::is_full).
    #[must_use]
    pub fn filled_bytes(&self) -> &[u8] {
        let bytes = self.storage.get().uninit();
        // SAFETY: the first `filled` bytes are initialized (`filled <= init`),
        // and `MaybeUninit<u8>` has the layout of `u8`.
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            core::slice::from_raw_parts(bytes.as_ptr().cast::<u8>(), self.filled)
        }
    }

    /// Lends the buffer to `fill` as a [`BlockBuf`], and counts here what
    /// `fill` wrote and counted there.
    ///
    /// A [`BlockBuf`] that comes back over memory other than this buffer's
    /// (`fill` swapped in one of its own) counts nothing.
    pub fn fill_with<R>(&mut self, fill: impl FnOnce(&mut BlockBuf<'_>) -> R) -> R {
        let mut buf = self.block_buf();
        let result = fill(&mut buf);
        let progress = buf.progress();
        self.settle(&progress);
        result
    }

    /// Fills what is left of the buffer from `file`, starting `filled()`
    /// bytes past `offset`, with one [`FsFile::read_at_buf`]: unzeroed where
    /// the file only writes.
    ///
    /// # Errors
    ///
    /// The read's error; a file ending before the buffer is full is
    /// [`ErrorKind::UnexpectedEof`](crate::io::ErrorKind::UnexpectedEof).
    ///
    /// # Examples
    ///
    /// ```
    /// use lsm_tree::fs::{Fs, FsFile, FsOpenOptions, MemFs, QueuedBuf};
    /// use std::io::Write;
    /// use std::path::Path;
    ///
    /// let fs = MemFs::new();
    /// fs.create_dir_all(Path::new("/d"))?;
    /// let path = Path::new("/d/f");
    /// fs.open(path, &FsOpenOptions::new().write(true).create(true))?
    ///     .write_all(b"hello, world")?;
    /// let file = fs.open(path, &FsOpenOptions::new().read(true))?;
    ///
    /// let mut buf = QueuedBuf::new(5);
    /// buf.read_from(file.as_ref(), 7)?;
    /// assert_eq!(buf.filled_bytes(), b"world");
    /// # Ok::<(), lsm_tree::io::Error>(())
    /// ```
    pub fn read_from(&mut self, file: &dyn FsFile, offset: u64) -> crate::io::Result<()> {
        let at = offset.checked_add(self.filled as u64).ok_or_else(|| {
            crate::io::Error::new(
                crate::io::ErrorKind::InvalidInput,
                "QueuedBuf::read_from: offset past the largest file position",
            )
        })?;
        self.fill_with(|block| block.fill_unfilled(|target| file.read_at_buf(target, at)))?;
        if !self.is_full() {
            return Err(crate::io::Error::new(
                crate::io::ErrorKind::UnexpectedEof,
                "QueuedBuf::read_from: the file ends before the block does",
            ));
        }
        Ok(())
    }

    /// The buffer as a [`BlockBuf`], from where it stands. What is filled
    /// through it counts here once [`Self::settle`] is given its progress.
    pub(crate) fn block_buf(&mut self) -> BlockBuf<'_> {
        let (init, filled) = (self.init, self.filled);
        BlockBuf::resume(self.uninit_mut(), init, filled)
    }

    /// Counts what a [`BlockBuf`] from [`Self::block_buf`] was filled with,
    /// if it is still over this buffer's memory.
    pub(crate) fn settle(&mut self, progress: &BlockProgress) {
        let bytes = self.storage.get().uninit();
        if progress.at != bytes.as_ptr() || progress.len != bytes.len() {
            return;
        }
        self.init = progress.init;
        self.filled = progress.filled;
    }

    /// The unfilled part, whether initialized or not, for a read that only
    /// writes it, such as one a kernel ring carries out.
    ///
    /// # Safety
    ///
    /// The caller must not read a byte of it before writing that byte, and
    /// must not use the pointer once this buffer is dropped or turned into a
    /// slice.
    #[cfg(io_uring_fs)]
    pub(crate) unsafe fn unfilled_ptr(&mut self) -> (*mut u8, usize) {
        let filled = self.filled;
        let rest = self.uninit_mut().get_mut(filled..).unwrap_or_default();
        (rest.as_mut_ptr().cast::<u8>(), rest.len())
    }

    /// Counts `n` more bytes as filled.
    ///
    /// # Safety
    ///
    /// The `n` bytes after those filled so far must have been written.
    #[cfg(io_uring_fs)]
    pub(crate) unsafe fn assume_written(&mut self, n: usize) {
        let remaining = self.capacity() - self.filled;
        self.filled += n.min(remaining);
        self.init = self.init.max(self.filled);
    }

    /// The address of the bytes, which stays put while the buffer moves: a
    /// queue tells its reads apart by it.
    #[cfg(feature = "std")]
    pub(crate) fn addr(&self) -> usize {
        self.storage.get().uninit().as_ptr() as usize
    }

    /// The block, once the buffer is filled; `None` while it is not.
    #[must_use]
    pub fn into_slice(self) -> Option<Slice> {
        if !self.is_full() {
            return None;
        }
        let slice = match self.storage {
            Storage::Direct(slice) => slice,
            Storage::Boxed(slice) => *slice,
        };
        // SAFETY: every byte was filled, and a filled byte is initialized.
        #[expect(unsafe_code, reason = "see safety")]
        Some(unsafe { slice.assume_filled() })
    }

    fn uninit_mut(&mut self) -> &mut [MaybeUninit<u8>] {
        self.storage.get_mut().uninit_mut()
    }
}

impl core::fmt::Debug for QueuedBuf {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("QueuedBuf")
            .field("capacity", &self.capacity())
            .field("init", &self.init)
            .field("filled", &self.filled)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "test code")]
mod tests;
