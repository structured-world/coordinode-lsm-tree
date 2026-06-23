// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::ByteView;
use alloc::{string::String, sync::Arc};
use core::ops::Deref;

/// An immutable, UTF-8–encoded string slice
///
/// Will be inlined (no pointer dereference or heap allocation)
/// if it is 20 characters or shorter (on a 64-bit system).
///
/// A single heap allocation will be shared between multiple strings.
/// Even substrings of that heap allocation can be cloned without additional heap allocation.
///
/// Uses [`ByteView`] internally, but derefs as [`&str`].
#[repr(C)]
#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StrView(ByteView);

impl core::fmt::Display for StrView {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{}", &**self)
    }
}

impl core::fmt::Debug for StrView {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "{:?}", &**self)
    }
}

impl Deref for StrView {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        // SAFETY: Constructor takes a &str
        unsafe { core::str::from_utf8_unchecked(&self.0) }
    }
}

impl core::hash::Hash for StrView {
    fn hash<H: core::hash::Hasher>(&self, state: &mut H) {
        self.deref().hash(state);
    }
}

impl StrView {
    /// Creates a new string from an existing byte string.
    ///
    /// Will heap-allocate the string if it has at least length 13.
    ///
    /// # Panics
    ///
    /// Panics if the length does not fit in a u32 (4 GiB).
    #[must_use]
    pub fn new(s: &str) -> Self {
        Self(ByteView::new(s.as_bytes()))
    }

    #[doc(hidden)]
    #[must_use]
    #[allow(clippy::missing_const_for_fn)]
    pub unsafe fn from_raw(view: ByteView) -> Self {
        Self(view)
    }

    /// Clones the contents of this string into an independently tracked string.
    #[must_use]
    pub fn to_detached(&self) -> Self {
        Self::new(self)
    }

    /// Clones the given range of the existing string without heap allocation.
    #[must_use]
    pub fn slice(&self, range: impl core::ops::RangeBounds<usize>) -> Self {
        Self(self.0.slice(range))
    }

    /// Returns `true` if the string is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the amount of bytes in the string.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns `true` if `needle` is a prefix of the string or equal to the string.
    #[must_use]
    pub fn starts_with(&self, needle: &str) -> bool {
        self.0.starts_with(needle.as_bytes())
    }
}

impl core::borrow::Borrow<str> for StrView {
    fn borrow(&self) -> &str {
        self
    }
}

impl AsRef<str> for StrView {
    fn as_ref(&self) -> &str {
        self
    }
}

impl From<&str> for StrView {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for StrView {
    fn from(value: String) -> Self {
        Self::new(&value)
    }
}

impl From<Arc<str>> for StrView {
    fn from(value: Arc<str>) -> Self {
        Self::new(&value)
    }
}

impl TryFrom<ByteView> for StrView {
    type Error = core::str::Utf8Error;

    fn try_from(value: ByteView) -> Result<Self, Self::Error> {
        core::str::from_utf8(&value)?;
        Ok(Self(value))
    }
}

impl From<StrView> for ByteView {
    fn from(val: StrView) -> Self {
        val.0
    }
}

#[cfg(feature = "serde")]
mod serde {
    use super::StrView;
    use core::fmt;
    use core::ops::Deref;
    use serde::de::{self, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    impl Serialize for StrView {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(self.deref())
        }
    }

    impl<'de> Deserialize<'de> for StrView {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct StrViewVisitor;

            impl<'de> Visitor<'de> for StrViewVisitor {
                type Value = StrView;

                fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                    formatter.write_str("a string")
                }

                fn visit_str<E>(self, v: &str) -> Result<StrView, E>
                where
                    E: de::Error,
                {
                    Ok(StrView::new(v))
                }
            }

            deserializer.deserialize_str(StrViewVisitor)
        }
    }
}

#[cfg(test)]
mod tests;
