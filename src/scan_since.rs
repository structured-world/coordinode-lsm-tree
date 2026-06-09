// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

//! Change-data-capture event stream for [`Tree::scan_since_seqno`].
//!
//! [`Tree::scan_since_seqno`]: crate::Tree::scan_since_seqno

use crate::{SeqNo, Slice};

/// A single change event emitted by [`Tree::scan_since_seqno`].
///
/// Each event carries the sequence number at which the change was committed.
/// Events are emitted in increasing seqno order, so a downstream consumer
/// (replica, Kafka connector, Debezium-style pipeline) can replay them in
/// order to reconstruct the source's history. Superseded versions are **not**
/// collapsed: a key updated three times after the target seqno yields three
/// events, mirroring the source's full change history rather than just its
/// latest visible state.
///
/// # Replay semantics
///
/// Applying events in seqno order reconstructs the state delta. An
/// `Insert(K, V1, s=150)` followed by a `PointTombstone(K, s=200)` means "K was
/// inserted with V1 at 150, then deleted at 200" — the net effect on a replica
/// starting before 150 is "create K with V1, then delete K", matching the
/// source.
///
/// # Merge operands
///
/// A store using a [`MergeOperator`](crate::MergeOperator) records partial
/// updates as [`MergeOperand`](Self::MergeOperand) events rather than resolved
/// values: the consumer applies the same merge operator to reproduce the
/// source's state. Emitting a merge as an `Insert` would make a replica
/// overwrite instead of merge, diverging from the source; resolving the merge
/// chain here would require reading the full base+operand history and defeat
/// the block-skip optimization, so the raw operand is surfaced instead.
///
/// # KV-separated (blob) values
///
/// When a value is stored out-of-line in a blob file, the blob is resolved and
/// the real value is carried in the [`Insert`](Self::Insert) event, so the
/// consumer never needs access to the source's blob files to replicate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanSinceEvent {
    /// A record was written (or overwritten) at `seqno`.
    ///
    /// Covers both inline values and values resolved from a blob file.
    Insert {
        /// User key that was written.
        key: Slice,
        /// Value written at `seqno` (resolved from a blob file if the entry
        /// was KV-separated).
        value: Slice,
        /// Sequence number at which the write was committed.
        seqno: SeqNo,
    },

    /// A merge operand was written at `seqno`.
    ///
    /// The consumer must apply the source's [`MergeOperator`](crate::MergeOperator)
    /// to combine this operand with the prior value / operands, exactly as the
    /// source does.
    MergeOperand {
        /// User key the operand applies to.
        key: Slice,
        /// Raw merge operand bytes, to be combined via the merge operator.
        operand: Slice,
        /// Sequence number at which the operand was committed.
        seqno: SeqNo,
    },

    /// A single key was deleted at `seqno`.
    ///
    /// Covers both regular and weak (single-delete) tombstones; both reduce to
    /// "this key is gone as of `seqno`" for replay purposes.
    PointTombstone {
        /// User key that was deleted.
        key: Slice,
        /// Sequence number at which the deletion was committed.
        seqno: SeqNo,
    },

    /// A half-open key range `[start_key, end_key)` was deleted at `seqno`.
    RangeTombstone {
        /// Inclusive lower bound of the deleted range.
        start_key: Slice,
        /// Exclusive upper bound of the deleted range.
        end_key: Slice,
        /// Sequence number at which the range deletion was committed.
        seqno: SeqNo,
    },
}

impl ScanSinceEvent {
    /// Sequence number at which this change was committed.
    ///
    /// Events from [`Tree::scan_since_seqno`](crate::Tree::scan_since_seqno)
    /// arrive in increasing order of this value.
    #[must_use]
    pub fn seqno(&self) -> SeqNo {
        match self {
            Self::Insert { seqno, .. }
            | Self::MergeOperand { seqno, .. }
            | Self::PointTombstone { seqno, .. }
            | Self::RangeTombstone { seqno, .. } => *seqno,
        }
    }
}
