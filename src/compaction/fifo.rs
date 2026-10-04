// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use super::{Choice, CompactionStrategy};
use crate::{
    HashSet, KvPair, compaction::state::CompactionState, config::Config, table::Table,
    time::unix_timestamp, version::Version,
};
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

#[doc(hidden)]
pub const NAME: &str = "FifoCompaction";

/// FIFO-style compaction
///
/// Limits the tree size to roughly `limit` bytes, deleting the oldest table(s)
/// when the threshold is reached. Tables are dropped whole, oldest first by
/// creation time, from whichever level they are in, so a tree that was
/// major-compacted keeps its limit, and overlapping tables are fine: the
/// older one goes first. Tables another compaction is working on are left
/// for a later round.
///
/// Additionally, a (lazy) TTL can be configured to drop old tables.
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
        let ttl_cutoff = match self.ttl_seconds {
            Some(s) if s > 0 => Some(
                // Clamp-to-zero: a TTL longer than the wall clock leaves no
                // expiry cutoff rather than wrapping.
                unix_timestamp()
                    .as_nanos()
                    .saturating_sub(u128::from(s) * 1_000_000_000u128),
            ),
            _ => None,
        };

        let mut ttl_dropped_bytes = 0u64;
        let mut alive = Vec::new();

        for table in version.iter_tables() {
            if hidden.is_hidden(table.id()) {
                continue;
            }
            let expired =
                ttl_cutoff.is_some_and(|cutoff| u128::from(table.metadata.created_at) <= cutoff);

            if expired {
                ids_to_drop.insert(table.id());
                let linked_blob_file_bytes = table.referenced_blob_bytes().unwrap_or_default();
                // Accumulated dropped-byte total, bounded by the on-disk size;
                // cannot overflow u64.
                ttl_dropped_bytes += table.file_size() + linked_blob_file_bytes;
            } else {
                alive.push(table);
            }
        }

        // Subtract TTL-selected bytes to see if we're still over the limit.
        let size_after_ttl = db_size.saturating_sub(ttl_dropped_bytes);

        // If we still exceed the limit, drop additional oldest tables until within the limit.
        if size_after_ttl > self.limit {
            let overshoot = size_after_ttl - self.limit;

            let mut collected_bytes = 0u64;

            // Oldest-first list by creation time from the non-expired set.
            alive.sort_by_key(|t| t.metadata.created_at);

            for table in alive {
                if collected_bytes >= overshoot {
                    break;
                }

                ids_to_drop.insert(table.id());

                let linked_blob_file_bytes = table.referenced_blob_bytes().unwrap_or_default();
                // Accumulated collected-byte total, bounded by the on-disk size;
                // cannot overflow u64.
                collected_bytes += table.file_size() + linked_blob_file_bytes;
            }
        }

        if ids_to_drop.is_empty() {
            Choice::DoNothing
        } else {
            Choice::Drop(ids_to_drop)
        }
    }
}

#[cfg(test)]
mod tests;
