// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use crate::{SeqNo, UserKey, ValueType, comparator::UserComparator};
use core::cmp::Reverse;

#[derive(Clone, Eq)]
pub struct InternalKey {
    pub user_key: UserKey,
    pub seqno: SeqNo,
    pub value_type: ValueType,
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.seqno == other.seqno
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl core::fmt::Debug for InternalKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{:?}:{}:{}",
            self.user_key,
            self.seqno,
            match self.value_type {
                ValueType::Value => "V",
                ValueType::Tombstone => "T",
                ValueType::WeakTombstone => "W",
                ValueType::MergeOperand => "M",
                ValueType::Indirection => "Vb",
            },
        )
    }
}

impl InternalKey {
    pub fn new<K: Into<UserKey>>(user_key: K, seqno: SeqNo, value_type: ValueType) -> Self {
        let user_key = user_key.into();

        assert!(
            u16::try_from(user_key.len()).is_ok(),
            "keys can be 65535 bytes in length",
        );

        Self {
            user_key,
            seqno,
            value_type,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        self.value_type.is_tombstone()
    }

    /// Compares two internal keys using a custom user key comparator.
    ///
    /// User keys are compared via the given comparator; ties are broken
    /// by sequence number in descending order (higher seqno = "smaller"
    /// in sort order), matching the invariant of [`Ord for InternalKey`].
    // Generic over the comparator type so concrete-typed callers
    // (e.g. SeekingMerger<_, DefaultUserComparator>) get monomorphised
    // dispatch instead of going through `&dyn UserComparator` vtables
    // on every key compare. `?Sized` keeps the older
    // `&dyn UserComparator` callers source-compatible — pass
    // `&*shared_comparator` (deref Arc<dyn> to `dyn UserComparator`)
    // and it matches `C = dyn UserComparator`. No behaviour change for
    // existing dyn paths.
    pub(crate) fn compare_with<C: UserComparator + ?Sized>(
        &self,
        other: &Self,
        cmp: &C,
    ) -> core::cmp::Ordering {
        cmp.compare(&self.user_key, &other.user_key)
            .then_with(|| Reverse(self.seqno).cmp(&Reverse(other.seqno)))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<core::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Order by user key, THEN by sequence number
// This is one of the most important functions
// Otherwise queries will not match expected behaviour
impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> core::cmp::Ordering {
        (&self.user_key, Reverse(self.seqno)).cmp(&(&other.user_key, Reverse(other.seqno)))
    }
}
