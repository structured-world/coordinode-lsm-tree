// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Structured World Foundation

//! Out-of-band inspection of a single SST file.
//!
//! Companion to [`crate::verify`]: while `verify_block_checksums` walks
//! every block and checks per-block XXH3, this module exposes a public
//! read-only view of an SST's stored metadata ([`TableProperties`]) for
//! diagnostic tooling like `sst-dump properties` that needs the
//! metadata fields without spinning up a [`Tree`](crate::Tree) or
//! recovering the manifest.
//!
//! The reader follows the same TAIL-first / MID-fallback pattern as
//! [`Table::recover`](crate::Table) (see PR #295): if the canonical
//! `meta` section at the file tail fails to decode, the MID-mirror
//! `meta_mid` section is attempted, and only if both copies are
//! unreadable does the call return an error.

use crate::CompressionType;
use crate::fs::{Fs, FsOpenOptions, StdFs};
use crate::key_range::KeyRange;
use crate::table::TableId;
use crate::table::meta::ParsedMeta;
use crate::table::regions::ParsedRegions;
use std::path::Path;

/// Read-only snapshot of an SST file's stored metadata.
///
/// Constructed by [`read_table_properties`]; not directly creatable by
/// external callers. Fields mirror the on-disk meta block (see
/// `src/table/writer/mod.rs::write_meta_section`). `#[non_exhaustive]`
/// so new fields can be added in a minor version bump.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TableProperties {
    /// Per-tree unique table id (the SST file's logical identifier).
    pub id: TableId,
    /// Logical size of the data-blocks region as recorded by the
    /// writer. This is `*self.meta.file_pos` at the moment the meta
    /// section was flushed — i.e. the byte offset just past the last
    /// data block — and does NOT include the index / filter /
    /// range-tombstone / linked-blob / meta / SFA-trailer sections
    /// that follow. To get the actual on-disk file size, `stat` the
    /// file directly. See `write_meta_section`'s `file_size` field
    /// for the writer-side definition.
    pub file_size: u64,
    /// Smallest and largest user keys present in the table.
    pub key_range: KeyRange,
    /// Number of live (non-tombstone) KV entries.
    pub item_count: u64,
    /// Number of strong tombstone entries (delete markers that
    /// suppress all older versions of a key).
    pub tombstone_count: u64,
    /// Number of weak tombstone entries (single-version delete
    /// markers).
    pub weak_tombstone_count: u64,
    /// Number of weak tombstones eligible for reclamation during
    /// compaction (already paired with their matching value entry).
    pub weak_tombstone_reclaimable: u64,
    /// Number of data blocks emitted by the writer.
    pub data_block_count: u64,
    /// Number of index blocks. For full-index tables this is 1
    /// (the TLI itself acts as the index); for partitioned-index
    /// tables this is the count of leaf index blocks under the TLI.
    pub index_block_count: u64,
    /// Codec used to compress data blocks.
    pub data_block_compression: CompressionType,
    /// Codec used to compress index blocks. Often `None` since the
    /// TLI is small and compression overhead dominates the win.
    pub index_block_compression: CompressionType,
    /// Wall-clock nanoseconds since the Unix epoch when the writer
    /// finalised the table. Recovered identically from MID or TAIL
    /// meta — the writer snapshots `unix_timestamp()` once and emits
    /// the same value to both copies.
    pub created_at_nanos: u128,
}

/// Reads `path` and returns its on-disk metadata as
/// [`TableProperties`].
///
/// Opens the file through [`StdFs`] (sst-dump is a real-filesystem
/// tool; `MemFs` / `IoUring` trees never have a real path), parses the
/// SFA trailer to locate the `meta` and optional `meta_mid` sections,
/// then decodes the meta block via the same `ParsedMeta` machinery
/// `Table::recover` uses on a live tree open.
///
/// Recovery semantics mirror [`Table::recover`](crate::Table):
/// the canonical tail `meta` section is tried first; on
/// decode/checksum/decrypt failure the MID mirror `meta_mid` is tried.
/// Both copies failing returns the original tail error. Tables
/// written before the meta-mirror change (#295) have no `meta_mid`
/// section and the reader falls straight through to the tail copy.
///
/// Encryption: this function does not accept an encryption provider
/// because it is intended for out-of-band diagnostic use. Encrypted
/// SSTs will fail to decode their meta block here. Encrypted-aware
/// property inspection is tracked under #251 / #256 once the
/// AAD-bound wire format lands.
#[cfg(feature = "std")]
pub fn read_table_properties(path: &Path) -> crate::Result<TableProperties> {
    let fs = StdFs;
    let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;

    let sfa_reader = sfa::Reader::from_reader(&mut file)?;
    let toc = sfa_reader.toc();
    let regions = ParsedRegions::parse_from_toc(toc)?;

    // TAIL first (authoritative copy by convention). On decode /
    // decrypt / checksum failure, fall back to MID. Mirrors
    // `Table::recover` so a corrupted-tail SST that the live open
    // path can still recover also produces inspectable properties
    // here.
    let meta = match ParsedMeta::load_with_handle(&*file, &regions.metadata, None) {
        Ok(m) => m,
        Err(tail_err) => {
            if let Some(mid_handle) = regions.metadata_mid {
                match ParsedMeta::load_with_handle(&*file, &mid_handle, None) {
                    Ok(mid) => mid,
                    Err(_) => return Err(tail_err),
                }
            } else {
                return Err(tail_err);
            }
        }
    };

    Ok(TableProperties {
        id: meta.id,
        file_size: meta.file_size,
        key_range: meta.key_range,
        item_count: meta.item_count,
        tombstone_count: meta.tombstone_count,
        weak_tombstone_count: meta.weak_tombstone_count,
        weak_tombstone_reclaimable: meta.weak_tombstone_reclaimable,
        data_block_count: meta.data_block_count,
        index_block_count: meta.index_block_count,
        data_block_compression: meta.data_block_compression,
        index_block_compression: meta.index_block_compression,
        created_at_nanos: *meta.created_at,
    })
}
