// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Which keys a flush finds written more than once, the observed sign that
//! their values are short-lived.
//!
//! The flush stream drops the versions no snapshot reads, so by the time a
//! value reaches the blob writer the versions that would show the key was
//! overwritten are gone. [`MarkOverwritten`] sits under that stream, on the
//! merge of the memtables, where every version still passes, and queues each
//! key it sees more than once in [`OverwrittenKeys`]. The writer then asks the
//! queue about the key it is writing.

use crate::{InternalValue, UserKey, comparator::SharedComparator};
use alloc::collections::VecDeque;
use core::{cell::RefCell, cmp::Ordering, iter::Peekable};

/// Keys seen with more than one version, in key order, from the position the
/// flush writer has reached up to the position the merge has read.
///
/// Named by the hidden flush hook of the sealed tree types; nothing outside
/// the crate can reach or build one.
#[derive(Default)]
#[doc(hidden)]
pub struct OverwrittenKeys(RefCell<VecDeque<UserKey>>);

impl OverwrittenKeys {
    /// Whether `key` was seen with more than one version. Keys before it are
    /// forgotten: the writer moves forward in key order and never asks again.
    pub(crate) fn take_before_and_check(&self, key: &[u8], comparator: &SharedComparator) -> bool {
        let mut queue = self.0.borrow_mut();
        while let Some(front) = queue.front() {
            match comparator.compare(front, key) {
                Ordering::Less => {
                    queue.pop_front();
                }
                Ordering::Equal => return true,
                Ordering::Greater => return false,
            }
        }
        false
    }

    fn mark(&self, key: &UserKey) {
        let mut queue = self.0.borrow_mut();
        // The versions of a key are adjacent, so a key with three or more is
        // seen again right after it was queued.
        if queue
            .back()
            .is_none_or(|last| !crate::comparator::same_user_key(last, key))
        {
            queue.push_back(key.clone());
        }
    }
}

/// Passes the merge of the memtables through, queueing every key that has a
/// further version right behind the one passed.
///
/// A key is queued before its newest version leaves this iterator, so the
/// writer, which receives that version later, always finds it queued.
pub struct MarkOverwritten<'q, I: Iterator<Item = crate::Result<InternalValue>>> {
    inner: Peekable<I>,
    keys: Option<&'q OverwrittenKeys>,
}

impl<'q, I: Iterator<Item = crate::Result<InternalValue>>> MarkOverwritten<'q, I> {
    /// Marks into `keys`, or passes `inner` through untouched when `None`.
    pub(crate) fn new(inner: I, keys: Option<&'q OverwrittenKeys>) -> Self {
        Self {
            inner: inner.peekable(),
            keys,
        }
    }
}

impl<I: Iterator<Item = crate::Result<InternalValue>>> Iterator for MarkOverwritten<'_, I> {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        if let (Some(keys), Ok(current)) = (self.keys, &item)
            && let Some(Ok(next)) = self.inner.peek()
            && crate::comparator::same_user_key(&next.key.user_key, &current.key.user_key)
        {
            keys.mark(&current.key.user_key);
        }
        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests;
