// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Moving a scan's sources past the rest of one key's versions without handing
//! them out one by one.
//!
//! A scan resolves a key from its newest version and drops the older ones.
//! Walking a long chain of them costs a merge step, a decoded entry and a
//! materialized key per version; a source that can jump instead passes a data
//! block holding nothing but the key's versions without loading it, and a
//! memtable reaches the next key with one search.
//!
//! no-std: `core + alloc` only (read path).

/// A source a scan can move past the rest of one key's versions.
pub trait SkipKey {
    /// Moves the front cursor past every entry whose user key is `key`, so the
    /// next `next()` yields the first entry of a later key, or nothing.
    ///
    /// The caller has resolved `key` from the merge, so no source's front holds
    /// an entry of an earlier key: what is passed is `key`'s versions only. They
    /// are dropped unread, a damaged block holding nothing else included, as a
    /// point read never reads a version older than the one it returns.
    ///
    /// Returns `false` when this source cannot, its position unchanged; the
    /// caller then walks the versions as before.
    ///
    /// # Errors
    ///
    /// An error reading the index on the way. The source is then failed as it
    /// is when `next()` meets the error.
    fn skip_front_past(&mut self, key: &[u8]) -> crate::Result<bool>;
}

impl<S: SkipKey + ?Sized> SkipKey for alloc::boxed::Box<S> {
    fn skip_front_past(&mut self, key: &[u8]) -> crate::Result<bool> {
        (**self).skip_front_past(key)
    }
}
