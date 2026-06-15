// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

use super::{BlockHandle, BlockOffset};
use crate::sfa::TocEntry;

/// Converts a [`crate::sfa::TocEntry`] to our [`BlockHandle`] struct.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidHeader`] if the on-disk region length does not
/// fit a `u32` (a region >= 4 GiB), surfacing a corrupt TOC as a typed recovery
/// error instead of panicking during table open.
fn toc_entry_to_handle(entry: &TocEntry) -> crate::Result<BlockHandle> {
    let size: u32 = entry
        .len()
        .try_into()
        .map_err(|_| crate::Error::InvalidHeader("Toc"))?;
    Ok(BlockHandle::new(BlockOffset(entry.pos()), size))
}

/// The regions block stores offsets to the different table file "regions"
///
/// ```text
/// --------------------
/// |       data       | <- implicitly start at 0
/// |------------------|
/// |       index      | <- partitioned only: sub-index blocks
/// |                  |    (full-index tables emit only `tli` below)
/// |------------------|
/// |        tli       | <- head copy (top-level index)
/// |------------------|
/// |      filter      | <- may not exist
/// |------------------|
/// |    filter_tli    | <- partitioned filter only
/// |------------------|
/// | range_tombstones | <- may not exist
/// |------------------|
/// |     meta_mid     | <- mirror of meta
/// |------------------|
/// | linked_blob_files| <- may not exist
/// |------------------|
/// |   table_version  |
/// |------------------|
/// |  meta_separator  | <- 4 KiB zero padding
/// |------------------|
/// |     tli_tail     | <- mirror of tli
/// |------------------|
/// |       meta       |
/// |------------------|
/// |        toc       |
/// |------------------|
/// |      trailer     | <- fixed size
/// |------------------|
/// ```
///
/// Writer emission order matches the diagram top-to-bottom. For the
/// partitioned index path (`PartitionedIndexWriter::finish`) the
/// `index` section is written first, then `tli`; the full-index
/// path (`FullIndexWriter::finish`) skips `index` and emits only
/// `tli`. Same pattern for filters: partitioned writes `filter`
/// then `filter_tli`, full writes only `filter`.
#[derive(Copy, Clone, Debug, Default)]
pub struct ParsedRegions {
    pub tli: BlockHandle,
    /// Tail-side mirror of the TLI block. Head copy lives in the
    /// `tli` section earlier in the file (after `data` and the
    /// partitioned `index` sub-blocks if any, before `filter` /
    /// `filter_tli` / `range_tombstones`); this copy lives near the
    /// file tail, after `meta_separator`
    /// and before `meta`. A torn-write or bad sector at either
    /// position leaves the other copy intact. Reader prefers the
    /// tail copy on open and transparently falls back to the head
    /// copy on decode/checksum/decrypt failure. Absent on tables
    /// written before the TLI-mirror change.
    pub tli_tail: Option<BlockHandle>,
    pub index: Option<BlockHandle>,
    pub filter_tli: Option<BlockHandle>,
    pub filter: Option<BlockHandle>,
    pub range_tombstones: Option<BlockHandle>,
    /// Optional inner zstd-block layout index (the `block_layout` section).
    /// Present only when the table has at least one data block that compressed
    /// into >= 2 inner zstd blocks; powers range-query partial decode.
    pub block_layout: Option<BlockHandle>,
    /// Optional retrieval-ribbon locator section (the `locator` section).
    /// Present only when the level's locator policy is enabled and the per-SST
    /// widths fit; maps each key to its data block and slot for O(1) point
    /// reads. Absent on tables written without the locator feature.
    pub locator: Option<BlockHandle>,
    pub linked_blob_files: Option<BlockHandle>,
    pub metadata: BlockHandle,
    /// Mid-file backup of the meta block. Writer order:
    /// `data` → `index?` → `tli` → `filter?` → `filter_tli?` →
    /// `range_tombstones?` → **`meta_mid`** →
    /// `linked_blob_files?` → `table_version` → `meta_separator` →
    /// `tli_tail` → `meta`. Absent on tables written before the
    /// meta-mirror change.
    /// Defends against torn-write at the file tail (incomplete fsync):
    /// MID lives several KiB before TAIL, with a 4 KiB
    /// `meta_separator` section between them to guarantee a fresh
    /// filesystem sector boundary.
    pub metadata_mid: Option<BlockHandle>,
}

impl ParsedRegions {
    pub fn parse_from_toc(toc: &crate::sfa::Toc) -> crate::Result<Self> {
        Ok(Self {
            filter_tli: toc
                .section(b"filter_tli")
                .map(toc_entry_to_handle)
                .transpose()?,
            tli: toc
                .section(b"tli")
                .map(toc_entry_to_handle)
                .transpose()?
                .ok_or_else(|| {
                    log::error!("TLI should exist");
                    crate::Error::Unrecoverable
                })?,
            tli_tail: toc
                .section(b"tli_tail")
                .map(toc_entry_to_handle)
                .transpose()?,
            index: toc.section(b"index").map(toc_entry_to_handle).transpose()?,
            filter: toc
                .section(b"filter")
                .map(toc_entry_to_handle)
                .transpose()?,
            range_tombstones: toc
                .section(b"range_tombstones")
                .map(toc_entry_to_handle)
                .transpose()?,
            block_layout: toc
                .section(b"block_layout")
                .map(toc_entry_to_handle)
                .transpose()?,
            locator: toc
                .section(b"locator")
                .map(toc_entry_to_handle)
                .transpose()?,
            linked_blob_files: toc
                .section(b"linked_blob_files")
                .map(toc_entry_to_handle)
                .transpose()?,
            metadata: toc
                .section(b"meta")
                .map(toc_entry_to_handle)
                .transpose()?
                .ok_or_else(|| {
                    log::error!("Metadata should exist");
                    crate::Error::Unrecoverable
                })?,
            metadata_mid: toc
                .section(b"meta_mid")
                .map(toc_entry_to_handle)
                .transpose()?,
        })
    }
}
