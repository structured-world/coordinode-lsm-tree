// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a write hands the memtable as a value.

use crate::path::{Path, PathBuf};
use crate::{Slice, UserValue};
use alloc::{string::String, sync::Arc, vec::Vec};

/// A value as a write hands it to the memtable: bytes it copies in, or a
/// value buffer it keeps as is.
pub enum InsertBytes<'a> {
    /// Bytes the memtable copies into its own memory: no allocation per
    /// value, and nothing to free one by one when the memtable is dropped.
    Borrowed(&'a [u8]),

    /// A ready value buffer the memtable keeps, without copying its bytes.
    Shared(UserValue),
}

impl InsertBytes<'_> {
    /// The value's bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Shared(value) => value,
        }
    }
}

/// A value [`AbstractTree::insert`](crate::AbstractTree::insert) and
/// [`AbstractTree::merge`](crate::AbstractTree::merge) accept.
///
/// Byte slices, strings, vectors and arrays are copied into the memtable,
/// which holds small values in its own memory: a write allocates nothing per
/// value, as the `RocksDB` memtable does. A [`Slice`] (and a `bytes::Bytes` under
/// the `bytes_1` feature) is kept as the buffer it is, so a caller that already
/// holds one pays no copy.
///
/// # Examples
///
/// ```
/// # let folder = tempfile::tempdir()?;
/// use lsm_tree::{AbstractTree, Config, SequenceNumberCounter, Slice};
///
/// let tree = Config::new(folder.path(), SequenceNumberCounter::default(), SequenceNumberCounter::default()).open()?;
/// tree.insert("a", b"copied into the memtable", 0);
/// tree.insert("b", Slice::from(b"kept as it is"), 1);
/// assert_eq!(&*tree.get("b", 2)?.unwrap(), b"kept as it is");
/// # Ok::<(), lsm_tree::Error>(())
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot be written as a value",
    label = "not a value a write accepts",
    note = "pass bytes (`&[u8]`, `Vec<u8>`, `&str`, `String`, an array) or a `Slice`, or implement `InsertValue` for `{Self}`"
)]
pub trait InsertValue {
    /// Hands the value to `f`, as bytes to copy or as a buffer to keep.
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R;
}

/// Values handed over as bytes the memtable copies.
macro_rules! copied {
    ($($ty:ty => |$v:ident| $bytes:expr;)*) => {
        $(
            impl InsertValue for $ty {
                #[inline]
                fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
                    let $v = self;
                    f(InsertBytes::Borrowed($bytes))
                }
            }
        )*
    };
}

copied! {
    &[u8] => |v| v;
    &Vec<u8> => |v| v.as_slice();
    Vec<u8> => |v| v.as_slice();
    &str => |v| v.as_bytes();
    &String => |v| v.as_bytes();
    String => |v| v.as_bytes();
    Arc<[u8]> => |v| &v;
    Arc<str> => |v| v.as_bytes();
}

#[cfg(feature = "std")]
copied! {
    &Path => |v| v.as_os_str().as_encoded_bytes();
    PathBuf => |v| v.as_os_str().as_encoded_bytes();
}

// no-std: the crate's path is UTF-8, so its `&str` bytes are the key bytes.
#[cfg(not(feature = "std"))]
copied! {
    &Path => |v| v.as_os_str().as_bytes();
    PathBuf => |v| v.as_os_str().as_bytes();
}

impl<const N: usize> InsertValue for [u8; N] {
    #[inline]
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
        f(InsertBytes::Borrowed(&self))
    }
}

impl<const N: usize> InsertValue for &[u8; N] {
    #[inline]
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
        f(InsertBytes::Borrowed(self))
    }
}

impl InsertValue for Slice {
    #[inline]
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
        f(InsertBytes::Shared(self))
    }
}

/// Another handle on the same buffer: no bytes are copied.
impl InsertValue for &Slice {
    #[inline]
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
        f(InsertBytes::Shared(self.clone()))
    }
}

#[cfg(not(feature = "bytes_1"))]
impl InsertValue for crate::byteview::ByteView {
    #[inline]
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
        f(InsertBytes::Shared(Slice::from(self)))
    }
}

#[cfg(feature = "bytes_1")]
impl InsertValue for bytes::Bytes {
    #[inline]
    fn with_insert_bytes<R>(self, f: impl FnOnce(InsertBytes<'_>) -> R) -> R {
        f(InsertBytes::Shared(Slice::from(self)))
    }
}

#[cfg(test)]
mod tests;
