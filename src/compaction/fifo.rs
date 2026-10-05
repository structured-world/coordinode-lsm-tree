// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{Choice, CompactionStrategy};
use crate::{
    HashMap, HashSet, KvPair, TableId, compaction::state::CompactionState, config::Config,
    table::Table, time::unix_timestamp, version::Version, vlog::BlobFileId,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[doc(hidden)]
pub const NAME: &str = "FifoCompaction";

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest table(s)
/// when the threshold is reached. Tables are dropped whole, oldest data first,
/// from whichever level they are in, so a tree that was major-compacted keeps
/// its limit, and overlapping tables are fine: the older one goes first. A
/// table's age is its `created_at`: the write time of a flushed table, and for
/// a compaction output the newest age among the inputs its key range meets,
/// so a compaction neither makes data look newer nor restarts its TTL. Tables
/// of one age, as the outputs that split one input, go by their highest
/// sequence number, and in key order once a compaction has zeroed it. A table
/// another compaction is working on is left for a later round, and no newer
/// table is dropped ahead of it.
///
/// Additionally, a (lazy) TTL can be configured to drop old tables. It is off
/// while the clock reads zero, which is no clock.
///
/// ###### Caution
///
/// Only use it for specific workloads where:
///
/// 1) You only want to store recent data (unimportant logs, ...)
/// 2) The key order of inserts is strictly monotonically increasing or decreasing
/// 3) You only insert new data (no updates/deletes)
#[derive(Clone)]
pub struct Strategy {
    /// Data set size limit in bytes
    pub limit: u64,

    /// TTL in seconds, will be disabled if 0 or None
    pub ttl_seconds: Option<u64>,
}

impl Strategy {
    /// Configures a new `Fifo` compaction strategy
    #[must_use]
    pub fn new(limit: u64, ttl_seconds: Option<u64>) -> Self {
        Self { limit, ttl_seconds }
    }
}

impl CompactionStrategy for Strategy {
    fn get_name(&self) -> &'static str {
        NAME
    }

    fn get_config(&self) -> Vec<KvPair> {
        vec![
            (
                crate::UserKey::from("fifo_limit"),
                crate::UserValue::from(self.limit.to_le_bytes()),
            ),
            (
                crate::UserKey::from("fifo_ttl"),
                crate::UserValue::from(if self.ttl_seconds.is_some() {
                    [1u8]
                } else {
                    [0u8]
                }),
            ),
            (
                crate::UserKey::from("fifo_ttl_seconds"),
                crate::UserValue::from(self.ttl_seconds.map(u64::to_le_bytes).unwrap_or_default()),
            ),
        ]
    }

    fn choose(&self, version: &Version, _: &Config, state: &CompactionState) -> Choice {
        // Early return avoids unnecessary work and keeps FIFO a no-op when there is nothing to do.
        if version.iter_tables().next().is_none() {
            return Choice::DoNothing;
        }

        // Account for both table file bytes and value-log (blob) bytes to enforce the true space
        // limit. A table another compaction holds still occupies its space, so it counts here,
        // but only free tables can be dropped below.
        // Summed on-disk sizes cannot overflow u64.
        let db_size = version.iter_tables().map(Table::file_size).sum::<u64>()
            + version.blob_files.on_disk_size();
        let hidden = state.hidden_set();

        let mut ids_to_drop: HashSet<_> = HashSet::default();

        // Compute TTL cutoff once and perform a single pass to mark expired tables and
        // accumulate their sizes. Also collect non-expired tables for possible size-based drops.
        let now = unix_timestamp();
        let ttl_cutoff = match self.ttl_seconds {
            // A clock at zero is no clock (a `no_std` build before one is
            // registered): TTL is off then, as tables written meanwhile carry
            // time zero too and would all look expired.
            Some(s) if s > 0 && !now.is_zero() => Some(
                // Clamp-to-zero: a TTL longer than the wall clock leaves no
                // expiry cutoff rather than wrapping.
                now.as_nanos()
                    .saturating_sub(u128::from(s) * 1_000_000_000u128),
            ),
            _ => None,
        };
        // A table whose blob references cannot be read leaves the bytes it
        // frees unknown: wait for a round that can read them.
        let Some(mut blob_credit) = BlobCredit::new(version) else {
            return Choice::DoNothing;
        };

        let mut ttl_dropped_bytes = 0u64;
        // Every table not expired, held ones included: they keep their place in
        // the age order below.
        let mut alive = Vec::new();

        for table in version.iter_tables() {
            let held = hidden.is_hidden(table.id());
            let expired = !held
                && ttl_cutoff.is_some_and(|cutoff| u128::from(table.metadata.created_at) <= cutoff);

            if expired {
                ids_to_drop.insert(table.id());
                // Accumulated dropped-byte total, bounded by the on-disk size;
                // cannot overflow u64.
                ttl_dropped_bytes += table.file_size() + blob_credit.drop_table(table.id());
            } else {
                alive.push((table, held));
            }
        }

        // Subtract TTL-selected bytes to see if we're still over the limit.
        let size_after_ttl = db_size.saturating_sub(ttl_dropped_bytes);

        // If we still exceed the limit, drop additional oldest tables until within the limit.
        if size_after_ttl > self.limit {
            let overshoot = size_after_ttl - self.limit;

            let mut collected_bytes = 0u64;

            // Oldest data first, by age: a compaction output carries the
            // newest age of the inputs its keys came from, so the order holds
            // after a major compaction, even one that zeroed the sequence
            // numbers. The highest sequence number orders tables of one age:
            // FIFO admits only inserts, so it follows insertion while kept.
            alive.sort_by_key(|(t, _)| (t.metadata.created_at, t.get_highest_seqno(), t.id()));

            for (table, held) in alive {
                if collected_bytes >= overshoot {
                    break;
                }
                // A held table still counts against the limit, and dropping
                // newer tables in its place would lose recent data while the
                // older one stays: wait for the round after its compaction.
                if held {
                    break;
                }

                ids_to_drop.insert(table.id());

                // Accumulated collected-byte total, bounded by the on-disk size;
                // cannot overflow u64.
                collected_bytes += table.file_size() + blob_credit.drop_table(table.id());
            }
        }

        if ids_to_drop.is_empty() {
            Choice::DoNothing
        } else {
            Choice::Drop(ids_to_drop)
        }
    }
}

/// What dropping tables frees in blob files. A blob file goes only with the
/// last table that references it, so several tables sharing one, as the
/// outputs of a compaction do, free its bytes together and none of them
/// alone.
struct BlobCredit<'v> {
    version: &'v Version,
    /// The blob files each table references.
    files_of: HashMap<TableId, Vec<BlobFileId>>,
    /// How many tables, dropped or not yet, still reference each blob file.
    refs: HashMap<BlobFileId, usize>,
}

impl<'v> BlobCredit<'v> {
    /// The references of every table in `version`, or `None` when one cannot
    /// be read. A version without blob files reads none.
    fn new(version: &'v Version) -> Option<Self> {
        let mut files_of = HashMap::default();
        let mut refs: HashMap<BlobFileId, usize> = HashMap::default();
        if version.blob_files.len() > 0 {
            for table in version.iter_tables() {
                let files: Vec<BlobFileId> = table
                    .blob_links()
                    .ok()?
                    .iter()
                    .map(|file| file.blob_file_id)
                    .collect();
                for &file in &files {
                    *refs.entry(file).or_insert(0) += 1;
                }
                files_of.insert(table.id(), files);
            }
        }
        Some(Self {
            version,
            files_of,
            refs,
        })
    }

    /// The blob bytes freed by dropping `table` after the tables dropped
    /// before it: those of every blob file it was the last to reference.
    fn drop_table(&mut self, table: TableId) -> u64 {
        let Some(files) = self.files_of.remove(&table) else {
            return 0;
        };
        let mut freed = 0;
        for file in files {
            let Some(count) = self.refs.get_mut(&file) else {
                continue;
            };
            // Counted from the same lists, once per referencing table, and a
            // table's list leaves `files_of` on its first drop.
            debug_assert!(*count > 0, "blob file {file} released twice");
            *count -= 1;
            if *count == 0 {
                freed += self
                    .version
                    .blob_files
                    .get(file)
                    .map_or(0, |blob_file| blob_file.meta().total_compressed_bytes);
            }
        }
        freed
    }
}

#[cfg(test)]
mod tests;
