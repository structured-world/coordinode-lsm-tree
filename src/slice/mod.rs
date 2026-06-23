// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

// Using tokio bytes
#[cfg(feature = "bytes_1")]
mod slice_bytes;

// Using byteview
#[cfg(not(feature = "bytes_1"))]
mod slice_default;

use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{string::String, vec::Vec};

use crate::path::{Path, PathBuf};

#[cfg(not(feature = "bytes_1"))]
pub use slice_default::{Builder, Slice};

#[cfg(feature = "bytes_1")]
pub use slice_bytes::{Builder, Slice};

impl AsRef<[u8]> for Slice {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for Slice {
    fn from(value: &[u8]) -> Self {
        #[cfg(not(feature = "bytes_1"))]
        {
            Self(crate::byteview::ByteView::new(value))
        }

        #[cfg(feature = "bytes_1")]
        {
            Self(bytes::Bytes::from(value.to_vec()))
        }
    }
}

impl From<Arc<[u8]>> for Slice {
    fn from(value: Arc<[u8]>) -> Self {
        Self::from(&*value)
    }
}

impl From<&Vec<u8>> for Slice {
    fn from(value: &Vec<u8>) -> Self {
        Self::from(value.as_slice())
    }
}

impl From<&str> for Slice {
    fn from(value: &str) -> Self {
        Self::from(value.as_bytes())
    }
}

impl From<&String> for Slice {
    fn from(value: &String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<&Path> for Slice {
    fn from(value: &Path) -> Self {
        // std: `OsStr::as_encoded_bytes`. no_std: the key is UTF-8, so
        // `as_os_str()` is a `&str` and `as_bytes()` yields the same bytes.
        #[cfg(feature = "std")]
        {
            Self::from(value.as_os_str().as_encoded_bytes())
        }
        #[cfg(not(feature = "std"))]
        {
            Self::from(value.as_os_str().as_bytes())
        }
    }
}

impl From<PathBuf> for Slice {
    fn from(value: PathBuf) -> Self {
        #[cfg(feature = "std")]
        {
            Self::from(value.as_os_str().as_encoded_bytes())
        }
        #[cfg(not(feature = "std"))]
        {
            Self::from(value.as_os_str().as_bytes())
        }
    }
}

impl From<Arc<str>> for Slice {
    fn from(value: Arc<str>) -> Self {
        Self::from(&*value)
    }
}

impl<const N: usize> From<[u8; N]> for Slice {
    fn from(value: [u8; N]) -> Self {
        Self::from(value.as_slice())
    }
}

impl<const N: usize> From<&[u8; N]> for Slice {
    fn from(value: &[u8; N]) -> Self {
        Self::from(value.as_slice())
    }
}

impl FromIterator<u8> for Slice {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = u8>,
    {
        Vec::from_iter(iter).into()
    }
}

impl core::ops::Deref for Slice {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl core::borrow::Borrow<[u8]> for Slice {
    fn borrow(&self) -> &[u8] {
        self
    }
}

impl<T> PartialEq<T> for Slice
where
    T: AsRef<[u8]>,
{
    fn eq(&self, other: &T) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl PartialEq<Slice> for &[u8] {
    fn eq(&self, other: &Slice) -> bool {
        *self == other.as_ref()
    }
}

impl<T> PartialOrd<T> for Slice
where
    T: AsRef<[u8]>,
{
    fn partial_cmp(&self, other: &T) -> Option<core::cmp::Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}

impl PartialOrd<Slice> for &[u8] {
    fn partial_cmp(&self, other: &Slice) -> Option<core::cmp::Ordering> {
        (*self).partial_cmp(other.as_ref())
    }
}

#[cfg(test)]
#[expect(clippy::expect_used)]
mod tests;
