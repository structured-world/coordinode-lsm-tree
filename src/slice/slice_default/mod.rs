// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::byteview::ByteView;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

pub use crate::byteview::Builder;

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
