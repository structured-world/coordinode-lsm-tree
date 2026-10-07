// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A read-only view of a tree's on-disk state, for the offline converter that
//! carries a tree to the next major format.
//!
//! Every reader here decodes what this crate wrote with the code that wrote it
//! and touches nothing: no write, no delete, no manifest rotation, no orphan
//! sweep. That is why it exists beside [`Tree::open`](crate::Tree), which
//! repairs and cleans the directory it opens. The manifest is read under
//! [`crate::config::ManifestRecoveryMode::AbsoluteConsistency`]: a converter must refuse a
//! store it cannot read exactly, not carry a guess forward.

use crate::config::ManifestRecoveryMode;
use crate::encryption::EncryptionProvider;
use crate::fs::Fs;
use crate::path::Path;
use crate::{SeqNo, TableId, TreeType, UserKey, vlog::BlobFileId};
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

mod blob;
mod table;

pub use blob::{BlobFileExport, BlobFrame};
pub use table::{
    BlockRef, BurrKind, BurrLayer, BurrSolution, Filter, FilterPartition, Locator, RangeDelete,
    Section, TableContext, TableExport, VerifiedFrame, decode_burr,
};

#[cfg(test)]
#[expect(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    reason = "test code"
)]
mod tests;

/// One table as the manifest places it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableRecord {
    /// The table's id, which is also its file name in the tables folder.
    pub id: TableId,
    /// The checksum of the whole table file.
    pub checksum: u128,
    /// The seqno added to every entry of the table on read (non-zero only for
    /// an ingested table).
    pub global_seqno: SeqNo,
}

/// One blob file the manifest names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobFileRecord {
    /// The blob file's id, which is also its file name in the blobs folder.
    pub id: BlobFileId,
    /// The checksum of the blob file, from its first live byte.
    pub checksum: u128,
}

/// What a blob file holds that no table references any more.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobGcStats {
    /// The blob file's id.
    pub id: BlobFileId,
    /// Unreferenced blobs.
    pub stale_items: u64,
    /// Unreferenced blob bytes, as stored.
    pub stale_bytes: u64,
    /// Unreferenced bytes that freeing would return to the disk.
    pub stale_on_disk_bytes: u64,
}

/// A tree's current state as its manifest records it: the snapshot the
/// `CURRENT` pointer names, with every edit of its log applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestState {
    /// The name of the comparator the tree was written under; tables are read
    /// only under a comparator of that name (see [`TableContext::new`]).
    pub comparator_name: String,
    /// The number of levels the tree was created with.
    pub level_count: u8,
    /// Whether the tree separates values into blob files.
    pub tree_type: TreeType,
    /// The version the state describes: the snapshot's id advanced by every
    /// edit replayed on it.
    pub version_id: u64,
    /// Tables by level, then run (newest run first in level 0), then key order
    /// within the run. A level that holds nothing is present and empty.
    pub levels: Vec<Vec<Vec<TableRecord>>>,
    /// Blob files, by id.
    pub blob_files: Vec<BlobFileRecord>,
    /// Garbage statistics of the blob files that have any, by id.
    pub blob_gc_stats: Vec<BlobGcStats>,
    /// Tables a tight-space compaction restricted, by id, with the first key
    /// each still serves.
    pub restrictions: Vec<(TableId, UserKey)>,
    /// Blob files whose consumed prefix was reclaimed, by id, with the offset
    /// of each one's first live byte.
    pub blob_restrictions: Vec<(BlobFileId, u64)>,
    /// The highest snapshot seqno the tree can no longer serve.
    pub retention_floor: SeqNo,
    /// The compression dictionaries the tree registers, by id.
    pub dicts: Vec<u32>,
}

/// Reads every compression dictionary stored in the tree in `folder`.
/// Dictionaries a tree was opened with but never stored are not on disk and
/// have to be supplied by the caller.
///
/// # Errors
///
/// Propagates the folder scan and fails on a dictionary whose bytes no longer
/// hash to its name.
#[cfg(zstd_any)]
pub fn read_dictionaries(
    folder: &Path,
    fs: &dyn Fs,
    encryption: Option<&dyn EncryptionProvider>,
) -> crate::Result<crate::compression::ZstdDictionaries> {
    crate::dicts::read_all(fs, &folder.join(crate::file::DICTS_FOLDER), encryption)
}

/// Reads the current manifest state of the tree in `folder`.
///
/// # Errors
///
/// Returns the error a strict open would: any defect in the snapshot or its
/// edit log, a torn trailing edit included, and a snapshot whose counts do not
/// describe its tables ([`crate::Error::ManifestTablesUnaccounted`]).
pub fn read_manifest(
    folder: &Path,
    fs: &dyn Fs,
    encryption: Option<Arc<dyn EncryptionProvider>>,
) -> crate::Result<ManifestState> {
    // The snapshot's header first, as an open reads it: the format version,
    // level count and filter hash this build reads, and the comparator the
    // tree was written under.
    let header = {
        let snapshot_id =
            crate::version::recovery::get_current_version(folder, fs, encryption.clone())?;
        let mut archive = crate::manifest_blocks::reader::ManifestArchiveReader::open(
            &folder.join(format!("v{snapshot_id}")),
            fs,
            Arc::new(crate::runtime_config::RuntimeConfig::default()),
            encryption.clone(),
        )?;
        crate::manifest::Manifest::decode_from(&mut archive)?
    };
    match header.version {
        crate::FormatVersion::V5 => {}
    }

    let recovery = crate::version::recovery::recover(
        folder,
        fs,
        ManifestRecoveryMode::AbsoluteConsistency,
        encryption,
    )?;

    let levels = recovery
        .table_ids
        .iter()
        .map(|level| {
            level
                .iter()
                .map(|run| {
                    run.iter()
                        .map(|t| TableRecord {
                            id: t.id,
                            checksum: t.checksum.into_u128(),
                            global_seqno: t.global_seqno,
                        })
                        .collect()
                })
                .collect()
        })
        .collect();

    let mut blob_files: Vec<BlobFileRecord> = recovery
        .blob_file_ids
        .iter()
        .map(|(id, checksum)| BlobFileRecord {
            id: *id,
            checksum: checksum.into_u128(),
        })
        .collect();
    blob_files.sort_unstable_by_key(|b| b.id);

    let mut blob_gc_stats: Vec<BlobGcStats> = recovery
        .gc_stats
        .iter()
        .map(|(id, entry)| BlobGcStats {
            id: *id,
            stale_items: entry.len as u64,
            stale_bytes: entry.bytes,
            stale_on_disk_bytes: entry.on_disk_bytes,
        })
        .collect();
    blob_gc_stats.sort_unstable_by_key(|s| s.id);

    let mut restrictions: Vec<(TableId, UserKey)> = recovery.restrictions.into_iter().collect();
    restrictions.sort_unstable_by_key(|(id, _)| *id);

    let mut blob_restrictions: Vec<(BlobFileId, u64)> =
        recovery.blob_restrictions.into_iter().collect();
    blob_restrictions.sort_unstable_by_key(|(id, _)| *id);

    Ok(ManifestState {
        comparator_name: header.comparator_name,
        level_count: header.level_count,
        tree_type: recovery.tree_type,
        version_id: recovery.curr_version_id,
        levels,
        blob_files,
        blob_gc_stats,
        restrictions,
        blob_restrictions,
        retention_floor: recovery.retention_floor,
        dicts: recovery.dicts,
    })
}
