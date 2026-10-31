// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use crate::byteview::ByteView;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

pub use crate::byteview::Builder;

/// A [`Slice`] allocated and not written yet, for a read to fill.
pub(crate) struct UnfilledSlice(ByteView);

impl UnfilledSlice {
    pub(crate) fn new(len: usize) -> Self {
        // SAFETY: the bytes are reached only as `MaybeUninit` until
        // `assume_filled`, whose caller promises they were written.
        Self(unsafe { ByteView::with_size_unzeroed(len) })
    }

    /// The whole allocation as a read target.
    pub(crate) fn target(&mut self) -> crate::fs::ReadBuf<'_> {
        crate::fs::ReadBuf::uninit(self.0.unfilled_mut())
    }

    /// The slice.
    ///
    /// # Safety
    ///
    /// Every byte must have been written.
    pub(crate) unsafe fn assume_filled(mut self) -> Slice {
        self.0.update_prefix();
        Slice(self.0)
    }
}

/// An immutable byte slice that can be cloned without additional heap allocation
///
/// There is no guarantee of any sort of alignment for zero-copy (de)serialization.
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) ByteView);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(bytes.into())
    }

    #[doc(hidden)]
    #[must_use]
    pub fn empty() -> Self {
        Self(ByteView::new(&[]))
    }

    /// Wraps an existing [`ByteView`] without copying (e.g. a zero-copy view
    /// into an arena block).
    #[must_use]
    pub(crate) fn from_view(view: ByteView) -> Self {
        Self(view)
    }

    #[doc(hidden)]
    #[must_use]
    pub unsafe fn builder_unzeroed(len: usize) -> Builder {
        // SAFETY: callers opt into the uninitialized builder contract via this
        // unsafe API and must fully initialize the returned buffer before any read.
        unsafe { ByteView::builder_unzeroed(len) }
    }

    pub(crate) fn slice(&self, range: impl core::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    pub(crate) fn fused(left: &[u8], right: &[u8]) -> Self {
        Self(ByteView::fused(left, right))
    }

    /// Whether both hold the same bytes. Unlike a comparison of the byte
    /// slices, it rules out most different keys on the length and the first
    /// bytes held inline, without reaching the bytes behind a pointer.
    #[inline]
    pub(crate) fn same_bytes(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    #[doc(hidden)]
    #[cfg(feature = "std")]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        ByteView::from_reader(reader, len).map(Self)
    }

    /// `no_std` mirror of [`Slice::from_reader`] over [`crate::io::Read`].
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurred.
    #[doc(hidden)]
    #[cfg(not(feature = "std"))]
    pub fn from_reader<R: crate::io::Read>(reader: &mut R, len: usize) -> crate::io::Result<Self> {
        ByteView::from_reader(reader, len).map(Self)
    }
}

// Arc::from<Vec<u8>> is specialized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(ByteView::from(value))
    }
}

// Arc::from<Vec<String>> is specialized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(ByteView::from(value.into_bytes()))
    }
}

impl From<ByteView> for Slice {
    fn from(value: ByteView) -> Self {
        Self(value)
    }
}

impl From<Slice> for ByteView {
    fn from(value: Slice) -> Self {
        value.0
    }
}
