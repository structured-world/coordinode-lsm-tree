// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Zero-copy value reference that keeps the decompressed block buffer alive.
//!
//! [`PinnableSlice`] is inspired by `RocksDB`'s `PinnableSlice`
//! (`include/rocksdb/slice.h:179-263`). It wraps a value that was read from
//! the LSM tree and indicates whether the underlying data shares the
//! decompressed block buffer or is independently owned (e.g. from a memtable
//! or merge result).
//!
//! When the value comes from an on-disk data block, holding a
//! `PinnableSlice::Pinned` keeps the block's decompressed buffer alive
//! (via the refcounted [`Slice`] / `ByteView` backing) for the duration of
//! the reference. The value bytes are a sub-slice of that buffer — no copy
//! is performed. Note: this does **not** prevent the block cache from
//! evicting its entry; it only ensures the backing memory remains valid.
//!
//! Memtable and blob-resolved values use the `Owned` variant.

use crate::{Slice, UserValue, table::Block};

/// A value reference that may share the decompressed block buffer.
///
/// Use [`PinnableSlice::as_ref`] to access the raw bytes regardless of variant.
///
/// # Lifetime
///
/// The `Pinned` variant holds a [`Block`] clone whose `data` field is a
/// refcounted [`Slice`]. As long as the `PinnableSlice` is alive, the
/// decompressed block buffer remains valid. Dropping it releases the
/// reference count on the underlying `ByteView` allocation.
#[derive(Clone)]
pub enum PinnableSlice {
    /// Value sharing the decompressed block buffer — zero copy.
    ///
    /// The [`Block`] keeps the decompressed data alive via refcounted
    /// `Slice` / `ByteView`. `value` is a sub-slice created via
    /// [`Slice::slice`], sharing the same backing allocation.
    Pinned {
        /// Keeps the decompressed block buffer alive via refcount.
        _block: Block,
        /// Zero-copy sub-slice into the block's decompressed data.
        value: Slice,
    },

    /// Value owned independently (memtable, blob, merge result).
    Owned(UserValue),
}

impl PinnableSlice {
    /// Creates a pinned value sharing the decompressed block buffer.
    #[must_use]
    pub fn pinned(block: Block, value: Slice) -> Self {
        Self::Pinned {
            _block: block,
            value,
        }
    }

    /// Creates an owned value (not sharing any block buffer).
    #[must_use]
    pub fn owned(value: UserValue) -> Self {
        Self::Owned(value)
    }

    /// Returns `true` if this value shares the decompressed block buffer.
    #[must_use]
    pub fn is_pinned(&self) -> bool {
        matches!(self, Self::Pinned { .. })
    }

    /// Returns the raw value bytes.
    #[must_use]
    pub fn value(&self) -> &[u8] {
        self.as_ref()
    }

    /// Returns the length of the value in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.as_ref().len()
    }

    /// Returns `true` if the value is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    /// Converts this `PinnableSlice` into an owned `UserValue`.
    ///
    /// For the `Pinned` variant, the `Block` is dropped but the returned
    /// `Slice` still shares the same `ByteView` backing allocation.
    /// For the `Owned` variant, the value is returned directly.
    #[must_use]
    pub fn into_value(self) -> UserValue {
        match self {
            Self::Pinned { value, .. } => value,
            Self::Owned(v) => v,
        }
    }
}

impl std::fmt::Debug for PinnableSlice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pinned { value, .. } => {
                f.debug_struct("Pinned").field("len", &value.len()).finish()
            }
            Self::Owned(v) => f.debug_tuple("Owned").field(&v.len()).finish(),
        }
    }
}

impl AsRef<[u8]> for PinnableSlice {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Pinned { value, .. } => value.as_ref(),
            Self::Owned(v) => v.as_ref(),
        }
    }
}

impl PartialEq<[u8]> for PinnableSlice {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_ref() == other
    }
}

impl PartialEq<&[u8]> for PinnableSlice {
    fn eq(&self, other: &&[u8]) -> bool {
        self.as_ref() == *other
    }
}

impl From<PinnableSlice> for UserValue {
    fn from(ps: PinnableSlice) -> Self {
        ps.into_value()
    }
}
