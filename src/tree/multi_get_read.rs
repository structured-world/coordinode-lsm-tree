// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! A multi-get as a machine: the memtables answer what they hold when it
//! starts, the version's levels are read for the rest, and each key's newest
//! version is then turned into its value, a merge or a blob read handed out
//! as a job. It holds one version for the whole read and never opens, reads
//! or waits itself.

use super::Tree;
use super::level_resolve::{JobDone, LevelJob, LevelWork, ReadMachine};
use super::tables_read::TablesRead;
use crate::merge_operator::MergeOperator;
use crate::version::SuperVersion;
use crate::{InternalValue, SeqNo, UserValue};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// How a key's newest version becomes its value.
pub enum Values<'a> {
    /// Stored inline: the version is the value.
    Inline,
    /// A blob tree's: an indirection is followed into the value log.
    Blob {
        tree_id: crate::TreeId,
        config: &'a crate::Config,
        #[cfg(feature = "metrics")]
        metrics: &'a crate::metrics::Metrics,
    },
}

/// The read of `keys` at the snapshot `seqno` of `super_version`.
pub(super) struct MultiGetRead<'a, 'k, K> {
    super_version: &'a SuperVersion,
    keys: &'k [K],
    seqno: SeqNo,
    comparator: &'a dyn crate::comparator::UserComparator,
    merge_operator: Option<&'a Arc<dyn MergeOperator>>,
    values: Values<'a>,
    /// Each key's newest version, from the memtables or the levels.
    entries: Vec<Option<InternalValue>>,
    /// `(duplicate index, representative index)` of keys asked more than
    /// once: the levels are read for the representative only.
    duplicates: Vec<(usize, usize)>,
    /// The read of the levels, while it goes on.
    tables: Option<TablesRead<'a, 'k, K>>,
    /// Whether every entry was turned into a value or handed out as a job.
    resolved: bool,
    results: Vec<Option<UserValue>>,
    /// Value jobs not yet back.
    jobs: usize,
    /// The failure of the lowest key a value job failed for: the one a
    /// resolve in key order would report.
    failure: Option<(usize, crate::Error)>,
}

impl<'a, 'k, K: AsRef<[u8]>> MultiGetRead<'a, 'k, K> {
    /// The read of `keys` at `seqno` in `super_version`: the memtables are
    /// read here, and the keys they do not hold are sorted under
    /// `comparator` for the levels, whose filter and index blocks are held
    /// within `metadata_budget`.
    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n, always in bounds"
    )]
    pub(super) fn new(
        super_version: &'a SuperVersion,
        keys: &'k [K],
        seqno: SeqNo,
        comparator: &'a dyn crate::comparator::UserComparator,
        merge_operator: Option<&'a Arc<dyn MergeOperator>>,
        values: Values<'a>,
        metadata_budget: u64,
    ) -> Self {
        let n = keys.len();
        // The memtables first, unsorted: a lookup there is O(log n) per key
        // regardless of order, so a batch they answer whole skips the sort and
        // the hashing.
        let mut entries: Vec<Option<InternalValue>> = alloc::vec![None; n];
        let mut remaining: Vec<usize> = Vec::with_capacity(n);
        for idx in 0..n {
            let key = keys[idx].as_ref();
            if let Some(entry) = super_version.active_memtable.get(key, seqno) {
                entries[idx] = Some(entry);
                continue;
            }
            if let Some(entry) =
                Tree::get_internal_entry_from_sealed_memtables(super_version, key, seqno)
            {
                entries[idx] = Some(entry);
                continue;
            }
            remaining.push(idx);
        }

        let mut duplicates = Vec::new();
        let tables = (!remaining.is_empty()).then(|| {
            remaining.sort_by(|&a, &b| comparator.compare(keys[a].as_ref(), keys[b].as_ref()));
            // The levels take strictly sorted, unique keys: an equal key asked
            // again is answered from its representative.
            let (miss_keys, dups) = Tree::dedup_sorted_miss_keys(&remaining, keys, comparator);
            duplicates = dups;
            TablesRead::new(
                &super_version.version,
                keys,
                miss_keys,
                seqno,
                comparator,
                metadata_budget,
            )
        });

        Self {
            super_version,
            keys,
            seqno,
            comparator,
            merge_operator,
            values,
            entries,
            duplicates,
            tables,
            resolved: false,
            results: alloc::vec![None; n],
            jobs: 0,
            failure: None,
        }
    }

    /// Turns every key's newest version into its value: a tombstone, or a
    /// version a range tombstone covers, has none; a merge operand, with a
    /// merge operator, and a blob indirection are handed out as jobs.
    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n, always in bounds"
    )]
    fn resolve(&mut self, out: &mut LevelWork<'a, 'k>) {
        for idx in 0..self.entries.len() {
            let Some(entry) = self.entries[idx].take() else {
                continue;
            };
            if entry.is_tombstone() {
                continue;
            }
            let key = self.keys[idx].as_ref();
            if Tree::is_suppressed_by_range_tombstones(
                self.super_version,
                key,
                entry.key.seqno,
                self.seqno,
                self.comparator,
            ) {
                continue;
            }
            if entry.key.value_type.is_merge_operand() {
                // Without a merge operator the operand is the value, as a
                // single-key read returns it. Operands are stored inline in a
                // blob tree too, so the merge result is a plain value.
                if let Some(merge_operator) = self.merge_operator {
                    out.jobs.push(LevelJob::Merge {
                        idx,
                        key,
                        super_version: self.super_version,
                        seqno: self.seqno,
                        merge_operator,
                    });
                    self.jobs += 1;
                } else {
                    self.results[idx] = Some(entry.value);
                }
                continue;
            }
            match &self.values {
                Values::Blob {
                    tree_id,
                    config,
                    #[cfg(feature = "metrics")]
                    metrics,
                } if entry.key.value_type.is_indirection() => {
                    out.jobs.push(LevelJob::Blob {
                        idx,
                        item: entry,
                        tree_id: *tree_id,
                        config,
                        version: &self.super_version.version,
                        #[cfg(feature = "metrics")]
                        metrics,
                    });
                    self.jobs += 1;
                }
                _ => self.results[idx] = Some(entry.value),
            }
        }
    }
}

impl<'a, 'k, K: AsRef<[u8]>> ReadMachine<'a, 'k> for MultiGetRead<'a, 'k, K> {
    /// Each key's value, in the order the keys were asked.
    type Output = crate::Result<Vec<Option<UserValue>>>;

    fn pump(&mut self, out: &mut LevelWork<'a, 'k>) -> Option<Self::Output> {
        if let Some(tables) = &mut self.tables {
            let read = tables.pump(&mut self.entries, out)?;
            self.tables = None;
            if let Err(error) = read {
                return Some(Err(error));
            }
            Tree::fan_out_duplicates(&self.duplicates, &mut self.entries);
        }
        if !self.resolved {
            self.resolved = true;
            self.resolve(out);
        }
        if self.jobs > 0 {
            return None;
        }
        if let Some((_, error)) = self.failure.take() {
            return Some(Err(error));
        }
        Some(Ok(core::mem::take(&mut self.results)))
    }

    fn job_done(&mut self, done: JobDone) {
        match done {
            JobDone::Value { idx, value } => {
                self.jobs -= 1;
                match value {
                    Ok(value) => {
                        if let Some(slot) = self.results.get_mut(idx) {
                            *slot = value;
                        }
                    }
                    Err(error) => {
                        if self.failure.as_ref().is_none_or(|(held, _)| idx < *held) {
                            self.failure = Some((idx, error));
                        }
                    }
                }
            }
            done => {
                if let Some(tables) = &mut self.tables {
                    tables.job_done(done);
                } else {
                    debug_assert!(false, "a job handed back to a read that did not ask for it");
                }
            }
        }
    }

    fn read_done(&mut self, done: crate::fs::ReadDone) {
        if let Some(tables) = &mut self.tables {
            tables.read_done(done);
        } else {
            debug_assert!(
                false,
                "a read handed back to a read that did not ask for it"
            );
        }
    }
}
