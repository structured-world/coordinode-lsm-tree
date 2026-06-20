// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! In-place reposition support for the seekable range pipeline.
//!
//! The seekable range iterator is built once over a union range; every
//! reposition ([`crate::range::SeekableTreeIter::seek_to`] and friends) moves
//! the leaf cursors to a new sub-range WITHOUT rebuilding the merge stack
//! (loser-tree merger, MVCC stream, range-tombstone filter). [`Reseekable`] is
//! the propagation hook: each layer resets its own per-position state and
//! forwards the new bounds to the layer below, down to the leaf readers.
//!
//! Reposition is infallible by design: leaf readers defer all I/O to the next
//! `next()` / `next_back()` pull (the SST reader only re-seeks its in-memory
//! index here), so a corrupt-block error surfaces lazily through the normal
//! pull path rather than at reseek time.
//!
//! no-std: this module is `core + alloc` only (read path).

use crate::UserKey;
use crate::key::InternalKey;
use core::ops::Bound;

/// New bounds for an in-place reposition, in both the user-key and internal-key
/// domains. The two are the same interval translated for the two reader
/// families: SST / run readers seek on user keys, while memtable readers range
/// over [`InternalKey`] bounds. The translation mirrors
/// [`crate::range::TreeIter::create_range`]'s bound construction.
pub struct ReseekCtx {
    /// User-key bounds — what SST / run leaf readers seek to.
    pub user: (Bound<UserKey>, Bound<UserKey>),
    /// Internal-key bounds — what memtable leaf readers range over.
    pub internal: (Bound<InternalKey>, Bound<InternalKey>),
}

/// A merge-stack layer that can be repositioned in place to a fresh sub-range.
///
/// Implemented by the leaf sources and by every wrapper in the seekable
/// pipeline; each impl resets its own cursor / lookahead state and forwards the
/// reposition to the layer it wraps.
pub trait Reseekable {
    /// Reposition to the bounds in `ctx`, resetting all per-position state so
    /// the next forward / backward pull starts fresh at the new sub-range.
    fn reseek(&mut self, ctx: &ReseekCtx);
}
