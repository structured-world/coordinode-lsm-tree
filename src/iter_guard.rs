use crate::{
    KvPair, UserKey, UserValue, blob_tree::Guard as BlobGuard, tree::Guard as StandardGuard,
};
use enum_dispatch::enum_dispatch;

/// Guard to access key-value pairs
#[enum_dispatch]
pub trait IterGuard {
    /// Accesses the key-value pair if the predicate returns `true`.
    ///
    /// The predicate receives the key - if returning false, the value
    /// may not be loaded if the tree is key-value separated.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn into_inner_if(
        self,
        pred: impl Fn(&UserKey) -> bool,
    ) -> crate::Result<(UserKey, Option<UserValue>)>;

    /// Accesses the key-value pair.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn into_inner(self) -> crate::Result<KvPair>;

    /// Accesses the key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn key(self) -> crate::Result<UserKey>;

    /// Returns the value size.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn size(self) -> crate::Result<u32>;

    /// Accesses the value.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    fn value(self) -> crate::Result<UserValue>
    where
        Self: Sized,
    {
        self.into_inner().map(|(_, v)| v)
    }
}

/// Generic iterator value
#[enum_dispatch(IterGuard)]
pub enum IterGuardImpl {
    /// Iterator value of a standard LSM-tree
    Standard(StandardGuard),

    /// Iterator value of a key-value separated tree
    Blob(BlobGuard),
}

/// A range iterator that can reposition (seek) in place, without reopening its
/// per-SST readers.
///
/// Returned by [`AbstractTree::range_seekable`](crate::AbstractTree::range_seekable).
/// The per-SST setup is paid once when the iterator is created; each
/// [`seek_to`](Self::seek_to) / [`seek_to_for_prev`](Self::seek_to_for_prev)
/// only rebuilds the cheap merge pipeline, so scanning many disjoint key
/// sub-intervals amortizes that setup instead of paying it per interval.
///
/// Seeks are valid mid-iteration (an explicit jump to a known key), which
/// enables data-dependent scan patterns — merge / zig-zag joins, skip-scan —
/// where the next seek target is computed from rows already returned.
pub trait SeekableGuardIter: DoubleEndedIterator<Item = IterGuardImpl> + Send {
    /// Reposition so the next [`Iterator::next`] yields the first entry with
    /// user key `>= key` (`RocksDB` `Seek`).
    fn seek_to(&mut self, key: &[u8]);

    /// Reposition so the next [`DoubleEndedIterator::next_back`] yields the last
    /// entry with user key `<= key` (`RocksDB` `SeekForPrev`).
    fn seek_to_for_prev(&mut self, key: &[u8]);

    /// Return the current key (the key the next [`Iterator::next`] would yield)
    /// without consuming it; `None` once the range is exhausted.
    ///
    /// A leapfrog / zig-zag join reads each input's current key to compute the
    /// next seek target before advancing any of them.
    fn peek_key(&mut self) -> Option<crate::Result<UserKey>>;
}

/// A [`SeekableGuardIter`] that failed before it could open its snapshot
/// (the requested seqno is at or below the retention floor).
///
/// It carries the failure as its single item, from either end, so the
/// consumer meets it through the same per-row `Result` it already handles;
/// seeks are accepted and ignored, since there is no pipeline to reposition.
/// [`peek_key`](SeekableGuardIter::peek_key) consumes the error, as it does
/// on the live iterator (`crate::Error` is not `Clone`).
#[expect(
    clippy::redundant_pub_crate,
    reason = "reached from tree / blob_tree as crate::iter_guard::FailedSeekable"
)]
pub(crate) struct FailedSeekable {
    err: Option<crate::Error>,
}

impl FailedSeekable {
    pub(crate) fn new(err: crate::Error) -> Self {
        Self { err: Some(err) }
    }
}

impl Iterator for FailedSeekable {
    type Item = IterGuardImpl;

    fn next(&mut self) -> Option<Self::Item> {
        self.err.take().map(crate::tree::error_guard)
    }
}

impl DoubleEndedIterator for FailedSeekable {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next()
    }
}

impl SeekableGuardIter for FailedSeekable {
    fn seek_to(&mut self, _key: &[u8]) {}

    fn seek_to_for_prev(&mut self, _key: &[u8]) {}

    fn peek_key(&mut self) -> Option<crate::Result<UserKey>> {
        self.err.take().map(Err)
    }
}
