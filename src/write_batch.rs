// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! Write batch for bulk memtable insertion with shared seqno.
//!
//! A [`WriteBatch`] collects multiple write operations (insert, remove, merge)
//! and applies them to the active memtable with a single seqno.
//! This reduces per-operation overhead:
//!
//! - **One version-history lock** acquisition instead of N
//! - **Batch size accounting**: single `fetch_add` for total size
//! - **Shared seqno**: all entries in a batch share the same sequence number
//!
//! **Visibility contract:** entries are inserted into the memtable one at a time
//! and become individually visible to concurrent readers as they are written.
//! Atomic batch visibility requires the **caller** to publish the batch seqno
//! (via `visible_seqno.fetch_max(batch_seqno + 1)`) only **after**
//! [`AbstractTree::apply_batch`] returns. This is the same pattern used by
//! fjall's keyspace for single-writer batches.

use crate::{UserKey, UserValue, ValueType, value::InternalValue};

/// A single entry in a [`WriteBatch`].
#[derive(Clone, Debug)]
enum WriteBatchEntry {
    /// Insert or update a key-value pair.
    Insert { key: UserKey, value: UserValue },

    /// Delete a key (standard tombstone).
    Remove { key: UserKey },

    /// Delete a key (weak/single-delete tombstone).
    RemoveWeak { key: UserKey },

    /// Write a merge operand for a key.
    Merge { key: UserKey, value: UserValue },
}

/// Batch of write operations applied atomically with a shared seqno.
///
/// # Examples
///
/// ```
/// use lsm_tree::WriteBatch;
///
/// let mut batch = WriteBatch::new();
/// batch.insert("key1", "value1");
/// batch.insert("key2", "value2");
/// batch.remove("key3");
///
/// assert_eq!(batch.len(), 3);
/// assert!(!batch.is_empty());
/// ```
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    entries: Vec<WriteBatchEntry>,
}

impl WriteBatch {
    /// Creates an empty write batch.
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Creates an empty write batch with the given capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Inserts a key-value pair into the batch.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(&mut self, key: K, value: V) {
        self.entries.push(WriteBatchEntry::Insert {
            key: key.into(),
            value: value.into(),
        });
    }

    /// Adds a delete (tombstone) for a key.
    pub fn remove<K: Into<UserKey>>(&mut self, key: K) {
        self.entries
            .push(WriteBatchEntry::Remove { key: key.into() });
    }

    /// Adds a weak delete (single-delete tombstone) for a key.
    pub fn remove_weak<K: Into<UserKey>>(&mut self, key: K) {
        self.entries
            .push(WriteBatchEntry::RemoveWeak { key: key.into() });
    }

    /// Adds a merge operand for a key.
    pub fn merge<K: Into<UserKey>, V: Into<UserValue>>(&mut self, key: K, value: V) {
        self.entries.push(WriteBatchEntry::Merge {
            key: key.into(),
            value: value.into(),
        });
    }

    /// Returns the number of operations in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the batch contains no operations.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clears the batch, removing all entries.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Materializes all entries into [`InternalValue`]s with the given seqno.
    #[doc(hidden)]
    #[must_use]
    pub fn materialize(self, seqno: crate::SeqNo) -> Vec<InternalValue> {
        self.entries
            .into_iter()
            .map(|entry| match entry {
                WriteBatchEntry::Insert { key, value } => {
                    InternalValue::from_components(key, value, seqno, ValueType::Value)
                }
                WriteBatchEntry::Remove { key } => InternalValue::new_tombstone(key, seqno),
                WriteBatchEntry::RemoveWeak { key } => {
                    InternalValue::new_weak_tombstone(key, seqno)
                }
                WriteBatchEntry::Merge { key, value } => {
                    InternalValue::new_merge_operand(key, value, seqno)
                }
            })
            .collect()
    }
}
