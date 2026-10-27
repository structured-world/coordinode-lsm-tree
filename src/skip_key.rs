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
#[diagnostic::on_unimplemented(
    message = "`{Self}` cannot move a scan past a key's versions",
    label = "not a version-skipping scan source",
    note = "table, run and memtable readers implement it; wrappers forward to the source they hold"
)]
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

    /// Moves the back cursor up to the newest version of `key` a snapshot at
    /// `seqno` sees, so the next `next_back()` yields it rather than the
    /// older versions before it.
    ///
    /// Read backward, a key's versions come oldest first, so the version a
    /// scan resolves is the last of them. The caller is walking `key`'s
    /// versions from the back: what is dropped is only older versions of
    /// `key` that the snapshot sees, each with a newer one left after it.
    /// Versions the snapshot does not see stay; the caller's filter drops them. A
    /// source may stop short of the newest version: what it leaves the
    /// caller walks.
    ///
    /// Returns `true` when the next `next_back()` yields a version of `key`
    /// the snapshot sees. `false` when it does not, or the source cannot
    /// jump: nothing was dropped then.
    ///
    /// # Errors
    ///
    /// An error reading the index or a block on the way. The source is then
    /// failed as it is when `next_back()` meets the error.
    fn skip_back_to_newest(&mut self, key: &[u8], seqno: crate::SeqNo) -> crate::Result<bool>;
}

impl<S: SkipKey + ?Sized> SkipKey for alloc::boxed::Box<S> {
    fn skip_front_past(&mut self, key: &[u8]) -> crate::Result<bool> {
        (**self).skip_front_past(key)
    }

    fn skip_back_to_newest(&mut self, key: &[u8], seqno: crate::SeqNo) -> crate::Result<bool> {
        (**self).skip_back_to_newest(key, seqno)
    }
}
