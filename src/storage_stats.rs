// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Read-only storage introspection: how much is stored, the average shape of a
//! stored entry, and an estimate of how many more entries fit in a byte budget.
//!
//! Computed from the live version's table + blob-file metadata plus one
//! size-stat per live file (the same accounting `Tree::create_checkpoint`
//! uses), so it never touches the data blocks. See
//! [`crate::AbstractTree::storage_stats`].

use crate::version::Version;

/// Coarse storage state of a tree.
///
/// With storage admission gating off (no configured quota and a backend that
/// cannot report free space) a tree reports [`Self::Healthy`] or, mid-run,
/// [`Self::CompactionInProgress`]. Once gating is active (bounded capacity), an
/// idle tree instead reports compaction availability:
/// [`Self::FullCompactionAvailable`] when a full compaction has working room,
/// [`Self::TightCompactionAvailable`] when only the opt-in tight-space mode
/// would fit, and [`Self::ReadOnlyOutOfSpace`] when the write gate is closed
/// (this takes precedence over a concurrent compaction).
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum StorageStatus {
    /// Normal operation: writes and a full compaction are available.
    Healthy,
    /// Enough free space for a normal (full) compaction.
    FullCompactionAvailable,
    /// Not enough space for a full compaction, but the opt-in tight-space
    /// (incremental-reclaim) compaction mode can still run.
    TightCompactionAvailable,
    /// Out of space: the tree is read-only until space is freed or the quota
    /// is raised.
    ReadOnlyOutOfSpace,
    /// A compaction is currently running.
    CompactionInProgress,
}

/// A point-in-time snapshot of a tree's on-disk storage footprint and the
/// average shape of a stored entry.
///
/// All byte figures are on-disk (post-compression, including any per-block
/// overhead and blob files). Averages are over every stored entry version, so
/// they pair with [`Self::item_count`].
#[must_use]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct StorageStats {
    /// Total on-disk bytes of all live SSTs plus blob files: how much is
    /// **occupied**. Pairs with [`Self::capacity_bytes`] / [`Self::available_bytes`]
    /// for an "X of Y used" view in a single call.
    pub used_bytes: u64,

    /// Total bytes the tree may occupy: the tighter of a configured byte quota
    /// (`storage_limit_bytes`) and the physical disk headroom (free space plus
    /// what is already used), across every volume the tree writes to. `None`
    /// when unbounded: no quota set AND the backend cannot report free space.
    pub capacity_bytes: Option<u64>,

    /// Free room left before the tree turns read-only: `capacity_bytes - used_bytes`
    /// (saturating). `None` exactly when [`Self::capacity_bytes`] is `None`
    /// (unbounded).
    pub available_bytes: Option<u64>,

    /// Whether a compaction can still run given the remaining free space (it
    /// needs working room to write merged output). `true` when unbounded or
    /// when at least [`Self::tight_compaction_bytes`] of free space remains;
    /// `false` when the disk is too full for a compaction to make progress. The
    /// finer full-vs-tight distinction is carried by [`Self::status`].
    pub compaction_possible: bool,

    /// Estimated free space (bytes) a FULL compaction needs for its transient
    /// output while the inputs still exist: the largest level's on-disk size
    /// (an upper bound on a single merge's input set). A full compaction has
    /// room when [`Self::available_bytes`] `>=` this. Pair with `used_bytes` /
    /// `capacity_bytes` to draw a capacity gauge: `used` → `used + tight_compaction_bytes`
    /// → `used + full_compaction_bytes` → `capacity`.
    pub full_compaction_bytes: u64,

    /// Estimated free space (bytes) a minimal (tight) space-reclaiming
    /// compaction needs to make forward progress: the reserved working floor.
    /// Tight compaction has room when [`Self::available_bytes`] `>=` this.
    pub tight_compaction_bytes: u64,

    /// Number of live entries (all versions) across all live SSTs.
    pub item_count: u64,

    /// Number of live SSTs.
    pub table_count: u64,

    /// Average on-disk bytes per entry (`used_bytes / item_count`), or `0` when
    /// the tree is empty. This is the figure
    /// [`Self::estimated_remaining_entries`] divides a budget by.
    pub avg_entry_on_disk_bytes: u64,

    /// Average user-key byte length per entry, or `None` if any live table was
    /// written before per-table key/value byte sums were recorded (the average
    /// key/value split is only exact when every table carries the figures).
    pub avg_key_bytes: Option<u64>,

    /// Average value byte length per entry, or `None` under the same condition
    /// as [`Self::avg_key_bytes`].
    pub avg_value_bytes: Option<u64>,

    /// Estimated bytes a full compaction could reclaim, from the
    /// weak-tombstone-reclaimable entry count times the average on-disk entry
    /// size. An estimate, not an exact figure.
    pub reclaimable_bytes_estimate: u64,

    /// Coarse storage state.
    pub status: StorageStatus,
}

impl StorageStats {
    /// Approximately how many more average-shaped entries fit in `budget_bytes`,
    /// using [`Self::avg_entry_on_disk_bytes`].
    ///
    /// Returns `0` when the average entry size is unknown (an empty tree), since
    /// there is no basis for the estimate.
    #[must_use]
    pub fn estimated_remaining_entries(&self, budget_bytes: u64) -> u64 {
        if self.avg_entry_on_disk_bytes == 0 {
            0
        } else {
            budget_bytes / self.avg_entry_on_disk_bytes
        }
    }
}

/// Sums the true physical on-disk size of every live table and blob file in
/// `version` (one metadata stat per file).
///
/// This is the same physical basis [`compute_storage_stats`] reports as
/// `used_bytes` and that `Tree::create_checkpoint` totals, so the storage
/// admission gate agrees with both. It deliberately does NOT use
/// `Metadata::file_size` (undercounts by the meta block / footer) or
/// `disk_space()` (metadata `Level::size`, which also omits blob files).
///
/// # Errors
///
/// Returns an error if a live table or blob file's size cannot be stat-ed.
pub(crate) fn compute_used_bytes(version: &Version) -> crate::Result<u64> {
    // Sum of on-disk file sizes, bounded by the filesystem capacity → cannot
    // overflow u64; plain arithmetic.
    let mut used_bytes = 0u64;
    for table in version.iter_tables() {
        used_bytes += table.fs.metadata(&table.path)?.len;
    }
    for blob in version.blob_files.iter() {
        used_bytes += blob.0.fs.metadata(&blob.0.path)?.len;
    }
    Ok(used_bytes)
}

/// The transient-output bound a full compaction's space check uses: the largest
/// level's on-disk size (the `full_compaction_bytes` gauge figure), an upper
/// bound on a single merge's input set. `0` for an empty tree.
///
/// This is the DEMAND. The destination VOLUME is a separate concern: a full
/// compaction writes its output to the last configured level
/// (`level_count - 1`), not to whichever level is currently largest, so callers
/// pass the last level as the destination to the per-volume space check (the two
/// differ only under tiered routing, where they can be different filesystems).
pub(crate) fn full_compaction_demand_bytes(version: &Version) -> u64 {
    version
        .iter_levels()
        .map(crate::version::Level::size)
        .max()
        .unwrap_or(0)
}

/// Computes [`StorageStats`] from a live version's table + blob-file metadata.
///
/// `is_compacting` selects [`StorageStatus::CompactionInProgress`] vs
/// [`StorageStatus::Healthy`]; the caller supplies it because compaction state
/// is engine-internal.
///
/// `value_bytes_are_user_values` must be `false` for a KV-separated
/// (`BlobTree`) tree: there the SST records a small indirection pointer per
/// large value, not the user value, so the per-table value-byte sum measures
/// pointers and the value average would misreport. When `false`,
/// [`StorageStats::avg_value_bytes`] is forced to `None`. Key bytes are never
/// separated, so [`StorageStats::avg_key_bytes`] stays exact either way.
///
/// `used_bytes` is the true on-disk file size of every live table and blob
/// file (one metadata stat per file), not the writer's `Metadata::file_size`
/// or [`crate::version::Version::blob_files`]' compressed-payload sum: those
/// undercount the physical file by the meta block / footer / blob trailer.
/// Statting matches the figure `Tree::create_checkpoint` reports, so the two
/// agree on disk reality.
///
/// # Errors
///
/// Returns an error if a live table or blob file's size cannot be stat-ed.
pub(crate) fn compute_storage_stats(
    version: &Version,
    is_compacting: bool,
    value_bytes_are_user_values: bool,
) -> crate::Result<StorageStats> {
    let mut used_bytes = 0u64;
    let mut item_count = 0u64;
    let mut table_count = 0u64;
    let mut reclaimable_entries = 0u64;
    let mut sum_key = 0u64;
    let mut sum_value = 0u64;
    // The key/value split is only exact when EVERY live table records the byte
    // sums; a single legacy table without them makes the average unrepresentable.
    let mut all_have_shape = true;

    // Every running total below is a sum of on-disk byte sizes or live item
    // counts; both are bounded by the filesystem capacity / the live entry count
    // and cannot overflow u64, so plain arithmetic is correct (a debug-overflow
    // would itself signal a corrupt metadata read).
    for table in version.iter_tables() {
        let m = &table.metadata;
        // Physical file size, NOT m.file_size (which undercounts — see above).
        let on_disk = table.fs.metadata(&table.path)?.len;
        used_bytes += on_disk;
        item_count += m.item_count;
        table_count += 1;
        reclaimable_entries += m.weak_tombstone_reclaimable;
        match (m.sum_user_key_bytes, m.sum_value_bytes) {
            (Some(k), Some(v)) => {
                sum_key += k;
                sum_value += v;
            }
            _ => all_have_shape = false,
        }
    }

    // Physical blob-file size (metadata + trailer included), NOT
    // BlobFileList::on_disk_size() which sums only the compressed payload.
    for blob in version.blob_files.iter() {
        used_bytes += blob.0.fs.metadata(&blob.0.path)?.len;
    }

    let avg_entry_on_disk_bytes = if item_count == 0 {
        0
    } else {
        used_bytes / item_count
    };

    let have_shape = all_have_shape && item_count > 0;
    let avg_key_bytes = have_shape.then(|| sum_key / item_count);
    // Value bytes are only meaningful when not KV-separated (see param doc).
    let avg_value_bytes =
        (have_shape && value_bytes_are_user_values).then(|| sum_value / item_count);

    // reclaimable_entries ≤ item_count and avg_entry_on_disk_bytes = used / item_count,
    // so the product is ≤ used_bytes (bounded by disk capacity): plain multiply.
    let reclaimable_bytes_estimate = reclaimable_entries * avg_entry_on_disk_bytes;

    // A full compaction's transient output is bounded by its input set; the
    // largest single merge is bounded by the largest level's on-disk size, so
    // that is the free space a full compaction needs.
    let full_compaction_bytes = full_compaction_demand_bytes(version);
    // A minimal (tight) space-reclaiming merge needs only the reserved working
    // floor to make forward progress.
    let tight_compaction_bytes = crate::tree::MIN_RESERVED_HEADROOM;

    let status = if is_compacting {
        StorageStatus::CompactionInProgress
    } else {
        StorageStatus::Healthy
    };

    Ok(StorageStats {
        used_bytes,
        // Capacity is disk-aware (quota + free-space probe) and lives at the
        // tree layer; this version-only computation leaves it unbounded. The
        // caller (`Tree::storage_stats`) fills the real figures.
        capacity_bytes: None,
        available_bytes: None,
        compaction_possible: true,
        full_compaction_bytes,
        tight_compaction_bytes,
        item_count,
        table_count,
        avg_entry_on_disk_bytes,
        avg_key_bytes,
        avg_value_bytes,
        reclaimable_bytes_estimate,
        status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stats_with_avg(avg_entry_on_disk_bytes: u64) -> StorageStats {
        StorageStats {
            used_bytes: 0,
            capacity_bytes: None,
            available_bytes: None,
            compaction_possible: true,
            full_compaction_bytes: 0,
            tight_compaction_bytes: 0,
            item_count: 0,
            table_count: 0,
            avg_entry_on_disk_bytes,
            avg_key_bytes: None,
            avg_value_bytes: None,
            reclaimable_bytes_estimate: 0,
            status: StorageStatus::Healthy,
        }
    }

    #[test]
    fn estimated_remaining_entries_divides_budget_by_average() {
        // budget / avg_entry_on_disk: 1000 bytes at 50 bytes/entry = 20 entries.
        let stats = stats_with_avg(50);
        assert_eq!(stats.estimated_remaining_entries(1000), 20);
        // Partial entries round down (integer division).
        assert_eq!(stats.estimated_remaining_entries(1049), 20);
        assert_eq!(stats.estimated_remaining_entries(0), 0);
    }

    #[test]
    fn estimated_remaining_entries_is_zero_when_average_is_unknown() {
        // An empty tree has no average to extrapolate from, so any budget
        // yields 0 rather than dividing by zero.
        let stats = stats_with_avg(0);
        assert_eq!(stats.estimated_remaining_entries(1_000_000), 0);
    }

    #[test]
    fn compute_on_empty_version_maps_compaction_flag_to_status() {
        use crate::TreeType;
        use crate::version::Version;

        // An empty version has no tables, so no file is stat-ed: the call is
        // pure and exercises only the status mapping and the zero-table path.
        let version = Version::new(0, TreeType::Standard);

        #[expect(
            clippy::unwrap_used,
            reason = "compute_storage_stats cannot fail on an empty in-memory version (no file to stat)"
        )]
        let busy = compute_storage_stats(&version, true, true).unwrap();
        assert_eq!(busy.status, StorageStatus::CompactionInProgress);
        assert_eq!(busy.used_bytes, 0);
        assert_eq!(busy.item_count, 0);
        assert_eq!(busy.table_count, 0);
        assert_eq!(busy.avg_key_bytes, None);
        assert_eq!(busy.estimated_remaining_entries(1_000_000), 0);

        #[expect(
            clippy::unwrap_used,
            reason = "compute_storage_stats cannot fail on an empty in-memory version (no file to stat)"
        )]
        let idle = compute_storage_stats(&version, false, true).unwrap();
        assert_eq!(idle.status, StorageStatus::Healthy);
    }
}
