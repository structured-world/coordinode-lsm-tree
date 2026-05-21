// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Seekable source iterator trait for the merge-iterator path.
//!
//! `MergeSource` is what `SeekingMerger` consumes: a stream that
//! can be advanced in either direction AND repositioned to a
//! specific key. The seek primitive is what makes RocksDB-style
//! direction switching possible — without it, a backward-only path
//! cannot recover from prior forward consumption on iterators whose
//! front and back cursors don't share state (LSM SST scanners
//! empirically don't).
//!
//! # Contract
//!
//! - [`Self::next`] yields items in ascending `InternalKey` order
//!   from the source's "front" cursor.
//! - [`Self::next_back`] yields items in descending order from the
//!   source's "back" cursor.
//! - [`Self::seek`] guarantees depend on the source's cursor model:
//!
//!   **Independent-cursor sources** (LSM SST scanners, `RunReader`s,
//!   where calling `next` repeatedly does NOT advance the back
//!   cursor and vice versa) — seek MUST reposition so that:
//!     * the next `next()` yields the first item with `key >= target`
//!     * the next `next_back()` yields the first item with `key < target`
//!
//!     If no such item exists in a direction, that direction returns
//!     `None` on the next call. Repositioning is the only way for
//!     `SeekingMerger` to guarantee the no-overlap invariant when
//!     direction switches.
//!
//!   **Coherent-cursor sources** (those marked [`CoherentMergeSource`]
//!   — `alloc::vec::IntoIter`, `alloc::collections::VecDeque`, range
//!   iterators from `alloc::collections::BTreeMap`, and our
//!   `CoherentIterSource` wrapper) — seek
//!   MAY be a no-op. The shared front/back cursor discipline
//!   already maintains the "no item yielded twice" invariant for
//!   mixed-direction consumption without any explicit reposition;
//!   the no-op satisfies the contract trivially. The current MVP
//!   `SeekingMerger` does NOT actually invoke `seek()` — its
//!   `DoubleEndedIterator` impl is type-gated on
//!   `CoherentMergeSource` and relies on the marker's coherence
//!   promise. The seek-aware direction-switch path that would
//!   exercise `seek()` for independent-cursor sources is the
//!   follow-up tracked as issue #280.
//!
//! # Why a custom trait, not just `DoubleEndedIterator + Seek`
//!
//! Rust's standard library has no `Seek` trait for iterators
//! (`io::Seek` is for byte streams). We need a domain-specific
//! seek that targets an `InternalKey`. Combining this with
//! `DoubleEndedIterator` would over-constrain the trait: not every
//! `MergeSource` impl needs the full `DoubleEndedIterator` API
//! surface, and adapter wrappers (filter, map) need their own
//! seek handling. Keeping `next` / `next_back` / `seek` as three
//! distinct methods on one trait keeps the wrapper story simple.

use crate::{InternalValue, key::InternalKey};
use alloc::boxed::Box;

/// What every merge source yields.
pub type IterItem = crate::Result<InternalValue>;

/// A source iterator consumable from either end, repositionable by
/// key. Used by `SeekingMerger` (RocksDB-style merging iterator with
/// O(n log shard) direction switches and O(log n) per-step
/// `replace_*`).
pub trait MergeSource: Send {
    /// Yield the next item in ascending key order from the front
    /// cursor, or `None` if exhausted in the forward direction.
    fn next(&mut self) -> Option<IterItem>;

    /// Yield the next item in descending key order from the back
    /// cursor, or `None` if exhausted in the backward direction.
    fn next_back(&mut self) -> Option<IterItem>;

    /// Reposition cursors for a subsequent direction switch.
    ///
    /// Implementations with INDEPENDENT front/back cursors (LSM SST
    /// scanners, `RunReader`s) MUST reposition so that:
    ///
    /// - the next `next()` yields the first item with `key >= target`, and
    /// - the next `next_back()` yields the first item with `key < target`.
    ///
    /// Implementations marked [`CoherentMergeSource`] — whose
    /// `next()` / `next_back()` already shrink a single shared
    /// remaining range from both ends — MAY treat seek as a no-op.
    /// Their cursor discipline already guarantees "no item yielded
    /// twice" under mixed-direction consumption without any
    /// explicit reposition.
    ///
    /// The current MVP `SeekingMerger` does NOT actually invoke
    /// `seek()` at all — `DoubleEndedIterator` is type-gated on
    /// `CoherentMergeSource` and relies on the marker's coherence
    /// promise. Once the seek-aware direction-switch path (issue
    /// #280) lands, `SeekingMerger` WILL invoke `seek()` on
    /// non-coherent sources at the direction-switch boundary;
    /// coherent sources will still pay nothing for the no-op.
    ///
    /// Returns `Err` if seek requires I/O (SST scanner reseek, run
    /// header re-read) and that I/O fails. Corruption errors
    /// surface here rather than being stashed for a later
    /// `next()` / `next_back()` call, so the caller knows
    /// precisely when the failure happened.
    fn seek(&mut self, target: &InternalKey) -> crate::Result<()>;
}

/// Marker for `MergeSource` impls whose `next()` and `next_back()`
/// share cursor state.
///
/// Such sources guarantee that mixed forward/backward consumption
/// never yields the same item twice. Iterators backed by
/// `alloc::vec::IntoIter`, `alloc::collections::VecDeque`, or
/// range iterators from `alloc::collections::BTreeMap` qualify
/// (their `DoubleEndedIterator` impls shrink a single remaining
/// range from both ends).
///
/// **Why a marker trait:** `SeekingMerger` can skip seek-based
/// reseeks for sources whose front/back cursors already shrink a
/// single shared remaining range. Sources with independent cursors
/// (LSM SST scanners, run readers — the intended follow-up impls)
/// should implement `MergeSource` with a real `seek`, but MUST NOT
/// implement `CoherentMergeSource` unless mixed forward/backward
/// consumption already guarantees no item is yielded twice. The
/// marker is the type-system encoding of that promise — without
/// it, `SeekingMerger`'s `DoubleEndedIterator` impl is unavailable
/// at compile time.
pub trait CoherentMergeSource: MergeSource {}

/// Pass-through impl so callers can build `Vec<Box<dyn MergeSource +
/// 'a>>` and pass that to `SeekingMerger`. Each method just
/// dispatches to the inner trait object.
impl<S: MergeSource + ?Sized> MergeSource for Box<S> {
    fn next(&mut self) -> Option<IterItem> {
        (**self).next()
    }
    fn next_back(&mut self) -> Option<IterItem> {
        (**self).next_back()
    }
    fn seek(&mut self, target: &InternalKey) -> crate::Result<()> {
        (**self).seek(target)
    }
}

/// Adapter that wraps any `DoubleEndedIterator<Item = IterItem>` as
/// a `MergeSource` with a no-op `seek`.
///
/// Useful for source iterators whose front and back cursors share
/// state natively (`alloc::vec::IntoIter`,
/// `alloc::collections::VecDeque`, range iterators from
/// `alloc::collections::BTreeMap`)
/// — those don't need seek to maintain the "no item yielded twice"
/// invariant under mixed forward/backward consumption.
///
/// For source iterators with independent front/back cursors (LSM
/// SST scanners), use a dedicated `MergeSource` impl that uses
/// real `seek` instead of this adapter.
pub struct CoherentIterSource<I> {
    iter: I,
}

impl<I> CoherentIterSource<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I> CoherentMergeSource for CoherentIterSource<I> where
    I: DoubleEndedIterator<Item = IterItem> + Send
{
}

impl<S: CoherentMergeSource + ?Sized> CoherentMergeSource for Box<S> {}

impl<I> MergeSource for CoherentIterSource<I>
where
    I: DoubleEndedIterator<Item = IterItem> + Send,
{
    fn next(&mut self) -> Option<IterItem> {
        Iterator::next(&mut self.iter)
    }
    fn next_back(&mut self) -> Option<IterItem> {
        DoubleEndedIterator::next_back(&mut self.iter)
    }
    fn seek(&mut self, _target: &InternalKey) -> crate::Result<()> {
        // No-op: callers using this adapter promise the iterator's
        // cursors are coherent (e.g., std vec/btree iters). Direction
        // switching relies on the iterator's own front/back cursor
        // discipline, not on a seek call. Always Ok — there's no I/O
        // to fail.
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "test assertions")]
mod tests {
    use super::*;
    use crate::ValueType::Value;
    use crate::comparator::default_comparator;
    use alloc::vec;
    use test_log::test;

    fn make_iv(key: &[u8], seqno: u64) -> InternalValue {
        InternalValue::from_components(key, b"", seqno, Value)
    }

    #[test]
    fn coherent_iter_source_forward_and_backward() {
        let items: Vec<IterItem> = vec![
            Ok(make_iv(b"a", 0)),
            Ok(make_iv(b"b", 0)),
            Ok(make_iv(b"c", 0)),
        ];
        let mut src = CoherentIterSource::new(items.into_iter());
        // Forward + backward interleave correctly via std::vec::IntoIter's
        // shared front/back cursors.
        assert_eq!(src.next().unwrap().unwrap().key.user_key.as_ref(), b"a");
        assert_eq!(
            src.next_back().unwrap().unwrap().key.user_key.as_ref(),
            b"c"
        );
        assert_eq!(src.next().unwrap().unwrap().key.user_key.as_ref(), b"b");
        assert!(src.next().is_none());
        assert!(src.next_back().is_none());
    }

    #[test]
    fn coherent_iter_source_seek_is_no_op() {
        // seek() does not advance or rewind; the iter keeps its
        // original position.
        let items: Vec<IterItem> = vec![Ok(make_iv(b"a", 0)), Ok(make_iv(b"b", 0))];
        let mut src = CoherentIterSource::new(items.into_iter());
        let target = make_iv(b"zzz", 0);
        src.seek(&target.key).unwrap();
        // Still yields from the start.
        assert_eq!(src.next().unwrap().unwrap().key.user_key.as_ref(), b"a");
    }

    #[test]
    fn boxed_merge_source_dispatches_through_blanket_impl() {
        // Build a Box<dyn MergeSource> via the blanket impl and
        // exercise all three trait methods.
        let items: Vec<IterItem> = vec![Ok(make_iv(b"x", 0)), Ok(make_iv(b"y", 0))];
        let mut boxed: Box<dyn MergeSource> = Box::new(CoherentIterSource::new(items.into_iter()));
        // .next() through the box
        assert_eq!(
            MergeSource::next(&mut boxed)
                .unwrap()
                .unwrap()
                .key
                .user_key
                .as_ref(),
            b"x"
        );
        // .seek() through the box (no-op on the underlying)
        let target = make_iv(b"target", 0);
        MergeSource::seek(&mut boxed, &target.key).unwrap();
        // .next_back() through the box
        assert_eq!(
            MergeSource::next_back(&mut boxed)
                .unwrap()
                .unwrap()
                .key
                .user_key
                .as_ref(),
            b"y"
        );
        assert!(MergeSource::next(&mut boxed).is_none());
    }

    #[test]
    fn coherent_iter_source_propagates_errors() {
        // Test that error items pass through both forward and backward.
        let items: Vec<IterItem> = vec![Ok(make_iv(b"a", 0)), Err(crate::Error::Unrecoverable)];
        let mut src = CoherentIterSource::new(items.into_iter());
        assert!(src.next().unwrap().is_ok());
        assert!(src.next().unwrap().is_err());
        assert!(src.next().is_none());
        // Sanity: comparator import works in the test
        let _ = default_comparator();
    }

    #[test]
    fn empty_coherent_iter_source() {
        let items: Vec<IterItem> = vec![];
        let mut src = CoherentIterSource::new(items.into_iter());
        assert!(src.next().is_none());
        assert!(src.next_back().is_none());
        let target = make_iv(b"k", 0);
        src.seek(&target.key).unwrap(); // no panic on empty
    }
}
