// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Seekable source iterator trait for the merge-iterator path.
//!
//! `MergeSource` is what `SeekingMerger` consumes: a stream that
//! can be advanced in either direction, AND that exposes a key-
//! addressed [`MergeSource::seek`] primitive whose strength varies
//! by source flavour. Independent-cursor sources MUST treat `seek`
//! as a real reposition (the only way they can maintain the
//! no-duplicates invariant under mixed direction); coherent-cursor
//! sources (see [`CoherentMergeSource`]) MAY treat it as a no-op
//! because their shared `next`/`next_back` cursor discipline
//! already enforces no-duplicates without external reseek (the
//! `CoherentIterSource` adapter ships such a no-op `seek`).
//!
//! `SeekingMerger` itself does NOT invoke [`MergeSource::seek`] on
//! direction switches — its two-tree architecture relies on each
//! source's own `(front_idx, back_idx)` window (or equivalent
//! self-coordination between `next` and `next_back`) to keep mixed
//! direction duplicate-free. The seek primitive is exposed on the
//! trait for user-initiated repositioning (range scan starting
//! key, jump to a known key, etc.), not as a direction-switch
//! reconciliation mechanism.
//!
//! # Contract
//!
//! - [`MergeSource::next`] yields items in ascending `InternalKey` order
//!   from the source's "front" cursor.
//! - [`MergeSource::next_back`] yields items in descending order from the
//!   source's "back" cursor.
//! - [`MergeSource::seek`] guarantees depend on the source's cursor model:
//!
//!   **Independent-cursor sources** (LSM SST scanners, `RunReader`s,
//!   where calling `next` repeatedly does NOT advance the back
//!   cursor and vice versa) — seek MUST reposition so that:
//!     * the next `next()` yields the first item with `key >= target`
//!     * the next `next_back()` yields the first item with `key < target`
//!
//!     If no such item exists in a direction, that direction returns
//!     `None` on the next call. NOTE: `SeekingMerger` does NOT call
//!     `seek` on direction switches — its two-tree architecture
//!     relies on the source's own `(front_idx, back_idx)` window
//!     (or equivalent self-coordination) to keep mixed direction
//!     duplicate-free. `seek` is exposed here for user-initiated
//!     repositioning (range scan starting key, etc.).
//!
//!   **Coherent-cursor sources** (those marked [`CoherentMergeSource`]
//!   — `alloc::vec::IntoIter`, `alloc::collections::VecDeque`, range
//!   iterators from `alloc::collections::BTreeMap`, and our
//!   `CoherentIterSource` wrapper) — seek MAY be a no-op. The shared
//!   front/back cursor discipline already maintains the "no item
//!   yielded twice" invariant for mixed-direction consumption
//!   without any explicit reposition; the no-op satisfies the
//!   contract trivially.
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

    /// User-initiated cursor reposition to a target key.
    ///
    /// `seek` is a public reposition primitive — typically called
    /// once at the start of a range scan, but valid at any point
    /// during iteration as an explicit jump. It is NOT the hook
    /// `SeekingMerger` uses to handle direction switches (see the
    /// paragraph on direction-switch handling below).
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
    /// `SeekingMerger` does NOT invoke `seek()` on direction
    /// switches — its two-tree architecture relies on the source
    /// being [`CoherentMergeSource`] (either literally-shared
    /// cursor state, or a self-coordinating index window — see
    /// that marker's docs for the two flavours). Sources with
    /// genuinely independent front/back cursors (no shared state,
    /// no window guard — currently no in-tree impl, but a
    /// straight-line file scanner with separate read offsets would
    /// be the canonical example) must NOT implement
    /// [`CoherentMergeSource`] and therefore cannot be used through
    /// `SeekingMerger`'s `DoubleEndedIterator` impl. `seek` is
    /// exposed for user-initiated repositioning — typically at the
    /// start of a range scan, but also valid mid-iteration as an
    /// explicit jump to a known key.
    ///
    /// The [`CoherentMergeSource`] marker's no-duplicates promise
    /// covers mixed `next` / `next_back` consumption WITHOUT an
    /// intervening user-initiated `seek`. A user `seek` is an
    /// explicit reposition, so observing previously-yielded items
    /// after a seek is expected behaviour, not a contract
    /// violation. Impls are free to hard-reset their cursors.
    ///
    /// Returns `Err` if seek requires I/O (SST scanner reseek, run
    /// header re-read) and that I/O fails. Corruption errors
    /// surface here rather than being stashed for a later
    /// `next()` / `next_back()` call, so the caller knows
    /// precisely when the failure happened.
    fn seek(&mut self, target: &InternalKey) -> crate::Result<()>;
}

/// Marker that promises a `MergeSource` impl's `next()` and
/// `next_back()` will never yield the same item twice under mixed
/// forward/backward consumption — regardless of HOW the impl
/// achieves that.
///
/// Two flavours qualify:
///
/// - **Literally shared cursor state**: `alloc::vec::IntoIter`,
///   `alloc::collections::VecDeque`, range iterators from
///   `alloc::collections::BTreeMap`. Their `DoubleEndedIterator`
///   impls shrink a single remaining range from both ends.
///   `CoherentIterSource` wraps these.
///
/// - **Self-coordinating index window**: an impl backed by a
///   sorted buffer plus `(front_idx, back_idx)` pointers that
///   refuses to yield once `front_idx >= back_idx`. SST scanners
///   and `RunReader`-style impls qualify ONLY IF they actually
///   enforce a single shrinking window — i.e. `next()` advances
///   `front_idx` and `next_back()` retreats `back_idx`, and either
///   refuses to yield when the indices cross. An impl with two
///   genuinely independent cursors (each side has its own offset
///   that the other never reads) is NOT coherent and MUST NOT
///   implement this marker, even though it might happen to behave
///   correctly under a specific consumption pattern.
///
/// **What this marker gates:** `SeekingMerger`'s
/// `DoubleEndedIterator` impl is bounded on this trait — sources
/// without the promise cannot use mixed direction through the
/// merger.
///
/// **What this marker says about `seek`:** the no-duplicates
/// promise covers mixed `next` / `next_back` consumption WITHOUT
/// an intervening user-initiated `seek`. A user `seek` is an
/// explicit reposition: observing previously-yielded items after
/// a seek is expected behaviour, not a contract violation. Impls
/// are free to hard-reset their cursors on `seek` — a production
/// independent-cursor source typically will. The marker promise
/// re-engages on the next forward / backward step pair.
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
    // `#[inline]` on the per-step forwards — when this adapter is
    // used through a concrete type (not boxed behind `dyn`), the
    // hot merger loop sees through to `I::next` / `I::next_back`
    // and the wrapper compiles away. Doesn't help the boxed path
    // (vtable call is intrinsic there) but is free to add.
    #[inline]
    fn next(&mut self) -> Option<IterItem> {
        Iterator::next(&mut self.iter)
    }
    #[inline]
    fn next_back(&mut self) -> Option<IterItem> {
        DoubleEndedIterator::next_back(&mut self.iter)
    }
    #[inline]
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
mod tests;
