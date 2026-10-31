// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};
use bytes::Bytes;

pub use bytes::BytesMut as Builder;

/// An immutable byte slice that can be cloned without additional heap allocation
///
/// There is no guarantee of any sort of alignment for zero-copy (de)serialization.
#[derive(Debug, Clone, Eq, Hash, Ord)]
pub struct Slice(pub(super) Bytes);

impl Slice {
    /// Construct a [`Slice`] from a byte slice.
    #[must_use]
    pub fn new(bytes: &[u8]) -> Self {
        Self(Bytes::copy_from_slice(bytes))
    }

    #[doc(hidden)]
    #[must_use]
    pub fn empty() -> Self {
        Self(Bytes::from_static(&[]))
    }

    #[doc(hidden)]
    #[must_use]
    pub unsafe fn builder_unzeroed(len: usize) -> Builder {
        // Use `with_capacity` & `set_len`` to avoid zeroing the buffer
        let mut builder = Builder::with_capacity(len);

        // SAFETY: we just allocated `len` bytes, and `read_exact` will fail if
        // it doesn't fill the buffer, subsequently dropping the uninitialized
        // BytesMut object
        #[expect(unsafe_code, reason = "see safety")]
        unsafe {
            builder.set_len(len);
        }

        builder
    }

    #[doc(hidden)]
    #[must_use]
    pub fn slice(&self, range: impl core::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    /// Whether both hold the same bytes.
    #[inline]
    pub(crate) fn same_bytes(&self, other: &Self) -> bool {
        self.0 == other.0
    }

    #[doc(hidden)]
    #[must_use]
    pub fn fused(left: &[u8], right: &[u8]) -> Self {
        let mut builder = Builder::with_capacity(left.len() + right.len());
        builder.extend_from_slice(left);
        builder.extend_from_slice(right);
        Self(builder.freeze())
    }

    /// Constructs a [`Slice`] from an I/O reader by pulling in `len` bytes.
    ///
    /// The buffer is zeroed: `R` is any safe reader, free to look at it.
    #[doc(hidden)]
    #[cfg(feature = "std")]
    pub fn from_reader<R: std::io::Read>(reader: &mut R, len: usize) -> std::io::Result<Self> {
        let mut builder = Builder::zeroed(len);
        reader.read_exact(&mut builder)?;
        Ok(Self(builder.freeze()))
    }

    /// `no_std` mirror of [`Slice::from_reader`] over [`crate::io::Read`].
    ///
    /// # Errors
    ///
    /// Returns an error if an I/O error occurred.
    #[doc(hidden)]
    #[cfg(not(feature = "std"))]
    pub fn from_reader<R: crate::io::Read>(reader: &mut R, len: usize) -> crate::io::Result<Self> {
        let mut builder = Builder::zeroed(len);
        reader.read_exact(&mut builder)?;
        Ok(Self(builder.freeze()))
    }
}

impl From<Bytes> for Slice {
    fn from(value: Bytes) -> Self {
        Self(value)
    }
}

impl From<Slice> for Bytes {
    fn from(value: Slice) -> Self {
        value.0
    }
}

// Bytes::from<Vec<u8>> is zero-copy optimized
impl From<Vec<u8>> for Slice {
    fn from(value: Vec<u8>) -> Self {
        Self(Bytes::from(value))
    }
}

// Bytes::from<String> is zero-copy optimized
impl From<String> for Slice {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}
