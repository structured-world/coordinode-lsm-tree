// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Structured World Foundation

pub mod block;
pub(crate) mod block_index;
pub(crate) mod block_layout;
#[cfg(feature = "columnar")]
pub mod columnar;
#[cfg(feature = "columnar")]
pub mod columnar_predicate;
pub mod data_block;
pub mod delete_bitmap;
pub mod filter;
mod id;
mod index_block;
mod inner;
pub(crate) mod iter;
#[cfg(feature = "zstd")]
pub(crate) mod lazy_block;
pub(crate) mod locator;
pub(crate) mod meta;
pub(crate) mod multi_writer;
pub(crate) mod regions;
#[cfg(feature = "std")]
mod relocate;
mod scanner;
pub(crate) mod seqno_bounds;
pub mod util;
pub mod writer;
pub(crate) mod zone_map;

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::useless_vec,
    clippy::needless_borrows_for_generic_args,
    reason = "test code"
)]
mod tests;

pub use block::{Block, BlockOffset};
pub use data_block::DataBlock;
pub use id::{GlobalTableId, TableId};
pub use index_block::{BlockHandle, IndexBlock, KeyedBlockHandle};
pub use scanner::Scanner;
pub use writer::Writer;

use crate::{
    Checksum, CompressionType, InternalValue, SeqNo, TreeId, UserKey,
    cache::Cache,
    comparator::SharedComparator,
    descriptor_table::DescriptorTable,
    file_accessor::FileAccessor,
    fs::{Fs, FsFile, FsOpenOptions},
    range_tombstone::RangeTombstone,
    table::{
        block::{BlockType, ParsedItem},
        block_index::{BlockIndex, FullBlockIndex, TwoLevelBlockIndex, VolatileBlockIndex},
        filter::block::FilterBlock,
        regions::ParsedRegions,
        writer::LinkedFile,
    },
};
use alloc::borrow::Cow;
use alloc::sync::Arc;
#[cfg(not(feature = "std"))]
use alloc::{boxed::Box, vec::Vec};
use block_index::BlockIndexImpl;
use core::ops::{Bound, RangeBounds};
use inner::Inner;
use iter::Iter;

use crate::path::PathBuf;
use portable_atomic::AtomicU64;
use util::load_block;

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

pub type TableInner = Inner;

/// Plan produced by [`Table::plan_block_tasks`]: the SST's file handle, the
/// table-local read seqno, whether the blocks need the special load path
/// (Page-ECC / columnar), and per data block its handle plus the positions
/// (into the input key batch) of the keys that fall in it.
pub(crate) type BlockTaskPlan = (
    Arc<dyn crate::fs::FsFile>,
    SeqNo,
    bool,
    Vec<(BlockHandle, Vec<usize>)>,
);

/// How [`Table::recover_inner`] treats degraded sidecars and the metadata id
/// cross-check.
#[derive(Clone, Copy)]
pub(crate) enum RecoveryMode {
    /// Live-tree open: a corrupt delete-bitmap / unreadable zone map fails
    /// closed, and the caller's durable id (manifest entry / file name) is
    /// cross-checked against the meta payload (with MID-mirror fallback on a
    /// bad TAIL copy).
    Live,
    /// Salvage open: a corrupt delete-bitmap / missing zone map degrades
    /// instead of failing, so a damaged sidecar still opens. For an
    /// UNENCRYPTED source the id cross-check uses `expected_id`: `Some` when
    /// the caller knows the durable id out-of-band (repair — from the SST
    /// file name), so a forged TAIL id falls back to the intact MID mirror
    /// instead of poisoning the recovered copy's identity; `None` for a
    /// standalone recovery reader, where the source's own stored id IS the
    /// identity to recover under. An ENCRYPTED open always cross-checks the
    /// caller's id — the meta block's AAD binds it regardless.
    Salvage {
        /// The durable table id known out-of-band, or `None` when the
        /// source's stored id is authoritative.
        expected_id: Option<TableId>,
        /// Load the MID meta mirror FIRST (tail as fallback), inverting the
        /// default tail-first order. Set by the salvage arbitration when the
        /// two mirrors decode to DIVERGENT contents: neither copy can be
        /// proven genuine, so salvage attempts both orders and keeps the
        /// attempt that recovers more.
        prefer_mid_meta: bool,
    },
}

/// Cached outcome of the heal's lazy hard-link probe/detach (see
/// [`Table::ensure_unshared_for_write`]): the probe and copy run at most
/// once per scan, and only when a write-back is actually needed.
#[cfg(feature = "page_ecc")]
enum UnshareState {
    /// No write attempted yet: the link count has not been probed.
    Unprobed,
    /// The handle is safe to write through (exclusive, or already detached).
    Ready,
    /// The unshare failed; every write is refused with this reason.
    Failed(alloc::string::String),
}

/// A disk segment (a.k.a. `Table`, `SSTable`, `SST`, `sorted string table`) that is located on disk
///
/// A table is an immutable list of key-value pairs, split into compressed blocks.
/// A reference to the block (`block handle`) is saved in the "block index".
///
/// Deleted entries are represented by tombstones.
///
/// Tables can be merged together to improve read performance and free unneeded disk space by removing outdated item versions.
#[doc(alias("sstable", "sst", "sorted string table"))]
#[derive(Clone)]
pub struct Table(
    Arc<Inner>,
    /// Tight-space restriction: when `Some(bound)`, this version's view of the
    /// table is clamped to keys `>= bound`. The on-disk data blocks below
    /// `bound` have been punched out ([`crate::fs::Fs::punch_hole`]) and their
    /// content lives in a freshly merged output table that supersedes them, so
    /// reads must not touch the punched prefix. Carried on the `Table` wrapper
    /// (not the shared `Arc<Inner>`) so an older snapshot keeps its own
    /// unrestricted view of the same physical SST. `None` on the common path.
    Option<UserKey>,
    /// Refreshed full-file checksum: when `Some`, an in-place heal changed the
    /// file's bytes AFTER it was recovered, and this digest supersedes the one
    /// captured at recovery. Carried on the wrapper (like the restriction) so
    /// the version diff sees the change and persists it to the manifest, while
    /// older snapshots keep the digest they were recovered under. `None` on
    /// the common path.
    Option<Checksum>,
);

impl core::ops::Deref for Table {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg_attr(coverage_nightly, coverage(off))]
impl core::fmt::Debug for Table {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Table:{}({:?})", self.id(), self.metadata.key_range)
    }
}

/// How an SST's rows are visible at a query snapshot, returned by
/// [`Table::seqno_visibility`]. Drives whether the tree-level columnar scan can
/// stream a segment verbatim ([`All`](Self::All)) or must apply a per-row seqno
/// mask ([`Partial`](Self::Partial)); [`None`](Self::None) segments are dropped.
#[cfg(feature = "columnar")]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SeqnoVisibility {
    /// No row is visible at the snapshot (the SST postdates it).
    None,
    /// Every row is visible (the SST entirely predates the snapshot).
    All,
    /// The snapshot straddles the SST's seqno range; visibility is per-row.
    Partial,
}

/// Result of a bloom filter check.
enum BloomResult {
    /// Bloom says key is definitely absent — skip point read.
    Skip,
    /// Point read should proceed.
    Proceed {
        /// Whether a filter was present (used for metrics accounting).
        #[cfg_attr(
            not(feature = "metrics"),
            expect(
                dead_code,
                reason = "read by BloomResult::has_filter under metrics feature"
            )
        )]
        has_filter: bool,
    },
}

impl BloomResult {
    fn should_skip(&self) -> bool {
        matches!(self, Self::Skip)
    }

    #[cfg(feature = "metrics")]
    fn has_filter(&self) -> bool {
        matches!(self, Self::Proceed { has_filter: true })
    }
}

/// Re-applies a bulk-ingested table's base sequence number to a table-local
/// seqno, translating it back to the global coordinate that callers compare
/// across tables.
///
/// The sum never overflows on any reachable input: a row reaches this
/// translation only when it is visible at the query snapshot `Q`, which (by the
/// exclusive MVCC check `local < Q - global`) requires `local + global < Q`, and
/// `Q <= SeqNo::MAX`. The `checked_add` therefore always succeeds; an overflow
/// would mean the invariant was violated, so it aborts loudly in both debug and
/// release builds rather than wrapping to a subtly wrong seqno (the silent-
/// corruption class `saturating_add` shares). For a non-ingested table `global`
/// is `0` and this is the identity.
#[inline]
fn apply_global_seqno(local: SeqNo, global: SeqNo) -> SeqNo {
    local.checked_add(global).unwrap_or_else(|| {
        unreachable!(
            "apply_global_seqno: table-local seqno + global base overflowed SeqNo::MAX, \
             but a row is only translated here when visible (local + global < query snapshot)"
        )
    })
}

/// Result of [`Table::salvage_load_block`]: the decoded block plus, when it read
/// back cleanly, its raw on-disk bytes for a verbatim copy.
pub(crate) struct SalvageBlock {
    /// The decoded (decompressed / decrypted / ECC-healed) block: the source of
    /// the per-row entries the salvage walk accounts and, on the re-encode path,
    /// re-serializes.
    pub block: Block,
    /// `Some((raw_on_disk_bytes, header, inner_layout))` when the block read back
    /// cleanly (no ECC recovery): the walk byte-copies these verbatim. `None` when
    /// ECC recovery healed the block, so the faulty on-disk bytes must not be
    /// propagated and the caller re-encodes the healed payload instead.
    pub verbatim: Option<VerbatimCopy>,
}

/// Raw on-disk frame captured for a verbatim block copy:
/// `(raw_on_disk_bytes, header, inner_layout)`. See [`SalvageBlock::verbatim`].
pub(crate) type VerbatimCopy = (
    alloc::vec::Vec<u8>,
    crate::table::block::Header,
    alloc::vec::Vec<u32>,
);

impl Table {
    #[must_use]
    pub fn global_seqno(&self) -> SeqNo {
        self.0.global_seqno
    }

    /// Classifies how this SST's rows are visible at query snapshot `seqno`,
    /// using the same exclusive MVCC rule as [`Self::point_read`]: a row is
    /// visible iff its effective seqno (`local + global_seqno`) is `< seqno`.
    ///
    /// Bulk-ingested columnar SSTs carry a uniform per-row seqno (every local
    /// seqno is `0`, sharing one `global_seqno`), so they classify as wholly
    /// [`All`](SeqnoVisibility::All) or wholly [`None`](SeqnoVisibility::None);
    /// only a flush-produced multi-seqno SST whose seqno range straddles the
    /// snapshot is [`Partial`](SeqnoVisibility::Partial), which the tree-level
    /// columnar scan resolves with a per-row seqno mask.
    #[cfg(feature = "columnar")]
    pub(crate) fn seqno_visibility(&self, seqno: SeqNo) -> SeqnoVisibility {
        // Translate the query snapshot into this table's local seqno space; a
        // snapshot below the base predates every row.
        let Some(local_threshold) = seqno.checked_sub(self.global_seqno()) else {
            return SeqnoVisibility::None;
        };
        // seqnos are (min, max) local; visible rows satisfy `local < threshold`.
        if self.metadata.seqnos.0 >= local_threshold {
            return SeqnoVisibility::None;
        }
        if self.metadata.seqnos.1 < local_threshold {
            SeqnoVisibility::All
        } else {
            SeqnoVisibility::Partial
        }
    }

    pub fn referenced_blob_bytes(&self) -> crate::Result<u64> {
        let cached = self
            .0
            .cached_blob_bytes
            .load(core::sync::atomic::Ordering::Acquire);
        if cached != u64::MAX {
            return Ok(cached);
        }

        let sum = self
            .list_blob_file_references()?
            .map(|bf| bf.iter().map(|f| f.on_disk_bytes).sum::<u64>())
            .unwrap_or_default();

        self.0
            .cached_blob_bytes
            .store(sum, core::sync::atomic::Ordering::Release);
        Ok(sum)
    }

    pub fn list_blob_file_references(&self) -> crate::Result<Option<Vec<LinkedFile>>> {
        use crate::io::{LE, ReadBytesExt};

        Ok(if let Some(handle) = &self.regions.linked_blob_files {
            let table_id = self.global_id();

            let (fd, _) = self
                .file_accessor
                .get_or_open_table(&table_id, &self.path)?;

            // Read the exact region using pread-style helper
            let buf =
                crate::file::read_exact(fd.as_ref(), *handle.offset(), handle.size() as usize)?;

            // Parse the buffer
            let mut reader = &buf[..];
            let len = reader.read_u32::<LE>()?;
            // Bound the declared record count by the bytes that remain BEFORE
            // reserving: each record is 4 u64s (32 bytes), so a corrupt or
            // forged count header (e.g. u32::MAX) must fail as invalid data
            // here rather than trigger a multi-GB Vec pre-allocation (which
            // aborts the process on allocators that don't overcommit).
            const RECORD_SIZE: usize = 4 * core::mem::size_of::<u64>();
            if len as usize > reader.len() / RECORD_SIZE {
                return Err(crate::Error::InvalidHeader(
                    "linked_blob_files: declared record count exceeds section size",
                ));
            }
            let mut blob_files = Vec::with_capacity(len as usize);

            for _ in 0..len {
                let blob_file_id = reader.read_u64::<LE>()?;
                let len = reader.read_u64::<LE>()?;
                let bytes = reader.read_u64::<LE>()?;
                let on_disk_bytes = reader.read_u64::<LE>()?;

                #[expect(
                    clippy::cast_possible_truncation,
                    reason = "truncation is not expected to happen"
                )]
                blob_files.push(LinkedFile {
                    blob_file_id,
                    bytes,
                    len: len as usize,
                    on_disk_bytes,
                });
            }

            Some(blob_files)
        } else {
            None
        })
    }

    /// Gets the global table ID.
    #[must_use]
    fn global_id(&self) -> GlobalTableId {
        (self.tree_id, self.id()).into()
    }

    #[must_use]
    pub fn filter_size(&self) -> u32 {
        self.regions.filter.map(|x| x.size()).unwrap_or_default()
    }

    #[must_use]
    pub fn pinned_filter_size(&self) -> usize {
        self.pinned_filter_block
            .as_ref()
            .map(FilterBlock::size)
            .unwrap_or_default()
    }

    #[must_use]
    pub fn pinned_block_index_size(&self) -> usize {
        match &*self.block_index {
            BlockIndexImpl::Full(full_block_index) => full_block_index.inner().inner.size(),
            BlockIndexImpl::VolatileFull(_) | BlockIndexImpl::Closed => 0,
            BlockIndexImpl::TwoLevel(two_level_block_index) => {
                two_level_block_index.top_level_index.inner.size()
            }
        }
    }

    /// Gets the table ID.
    ///
    /// The table ID is unique for this tree, but not
    /// across multiple trees, use [`Table::global_id`] for that.
    #[must_use]
    pub fn id(&self) -> TableId {
        self.metadata.id
    }

    /// This segment's positional delete-bitmap (rows deleted by position),
    /// loaded on open. Empty when the segment has no materialized deletes, in
    /// which case a scan applies no mask.
    #[must_use]
    pub fn delete_bitmap(&self) -> &crate::table::delete_bitmap::DeleteBitmap {
        self.delete_bitmap.as_ref()
    }

    /// Whether this segment was written WITH a positional delete bitmap (it
    /// carries a `delete_bitmap` section), independent of whether that bitmap is
    /// currently loaded. This stays `true` even when a salvage-mode open degraded
    /// a corrupt bitmap to empty, so the salvage walk can tell "no deletes ever"
    /// apart from "deletes whose bitmap was lost" — and must NOT byte-copy a block
    /// of the latter verbatim (which would resurrect positionally-deleted rows
    /// the recovered copy no longer masks).
    ///
    /// `pub(crate)` for the salvage walk ([`crate::salvage`]); only the columnar
    /// copy-through path consults it, so it is gated to that feature.
    #[cfg(feature = "columnar")]
    pub(crate) fn has_delete_bitmap_section(&self) -> bool {
        self.regions.delete_bitmap.is_some()
    }

    /// Whether the loaded delete-bitmap's CONTENTS match the meta-recorded
    /// `descriptor#delete_bitmap_hash` (and length). The section's own block
    /// checksum only proves the bytes are self-consistent; an equal-cardinality
    /// substitution (a different, checksum-valid bitmap) passes both that and the
    /// positional cross-check, yet masks the WRONG rows. Salvage has no original
    /// whole-file digest to compare against, so it MUST authenticate the contents
    /// before masking. A present section with a missing hash (a table written
    /// before the field) cannot be authenticated → `false` (fail closed). A table
    /// with no delete-bitmap section has nothing to authenticate → `true`.
    #[cfg(feature = "columnar")]
    pub(crate) fn delete_bitmap_authenticated(&self) -> bool {
        if !self.has_delete_bitmap_section() {
            return true;
        }
        match self.metadata.delete_bitmap_hash {
            Some(recorded) => {
                let bitmap = self.delete_bitmap();
                crate::hash::hash128(&bitmap.encode()) == recorded
                    && self.metadata.delete_bitmap_len == Some(bitmap.len())
            }
            None => false,
        }
    }

    /// Whether this segment carries a parallel `zone_map` section, i.e. it was
    /// written with the zone-map policy on and held at least one data block.
    /// The section powers predicate-based block-skip; absence means scans read
    /// every block. Read-transparent either way, so this is the only way to
    /// observe that a flush actually persisted zone maps.
    #[must_use]
    pub fn has_zone_map(&self) -> bool {
        self.regions.zone_map.is_some()
    }

    /// Whether this segment carries a parallel `seqno_bounds` section. Unlike
    /// the loaded map (best-effort at recover time — an unreadable section
    /// degrades it to empty), this reflects actual SECTION presence, so a
    /// source with rotted bounds still salvages into a copy WITH the section
    /// (the writer re-derives the ranges from the re-emitted entries).
    #[must_use]
    pub fn has_seqno_bounds(&self) -> bool {
        self.regions.seqno_bounds.is_some()
    }

    /// The fraction of this segment's rows masked by its positional
    /// delete-bitmap, as a percentage in `0..=100`, or `None` when the segment
    /// carries no delete-bitmap (nothing masked).
    ///
    /// Drives the density-based rewrite policy: a segment whose masked fraction
    /// has grown past the adaptive purge threshold is worth physically rewriting
    /// (dropping the masked rows and clearing the bitmap) rather than paying the
    /// merge-on-read mask cost on every scan.
    #[must_use]
    pub fn delete_density(&self) -> Option<u8> {
        let deleted = self.delete_bitmap().len();
        if deleted == 0 {
            return None;
        }
        let total = self.metadata.item_count.max(1);
        // Widen to u128 so `* 100` cannot overflow regardless of the (already
        // bounded) inputs; the quotient is clamped to the 0..=100 percentage range.
        let percent = (u128::from(deleted) * 100 / u128::from(total)).min(100);
        Some(u8::try_from(percent).unwrap_or(100))
    }

    /// Iterates every data-block handle in block-index (key) order; each item
    /// carries the block's last key. The salvage walk ([`crate::salvage`]) uses
    /// this to enumerate the blocks to recover one at a time; a corrupt index
    /// entry surfaces as an `Err` item rather than aborting the iteration.
    // std-only: the sole consumer is the std-gated salvage walk.
    #[cfg(feature = "std")]
    pub(crate) fn data_block_handles(&self) -> block_index::BlockIndexIterImpl {
        use block_index::BlockIndex;
        self.block_index.iter()
    }

    fn load_block(
        &self,
        handle: &BlockHandle,
        block_type: BlockType,
        compression: CompressionType,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<Block> {
        load_block(
            self.global_id(),
            &self.path,
            &self.file_accessor,
            &self.cache,
            handle,
            block_type,
            compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            zstd_dict,
            self.heal_hints.get().map(AsRef::as_ref),
            #[cfg(feature = "metrics")]
            &self.metrics,
        )
    }

    /// Loads a data-carrying block STRAIGHT FROM THE FILE, bypassing the
    /// block cache in both directions, for the semantic reconcile gates: a
    /// block read before an on-disk alteration leaves its pristine copy
    /// cached, and a gate served that stale original would judge bytes
    /// other than the ones the digest refresh is about to trust. Reuses the
    /// cached file descriptor but never consults or populates the block
    /// cache (the cold verification blocks must not evict the live working
    /// set either). Decodes under the table's data-block codec context, so
    /// it fits every gate that walks `block_index` (Data or Columnar role).
    // Compiled under no_std alongside its `verify_kv_checksums` consumer,
    // which is itself dead there (the verify/scrub caller is std-gated).
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "gate-only loader; the verify/scrub consumers are std-gated"
        )
    )]
    fn load_block_from_disk(
        &self,
        handle: &BlockHandle,
        block_type: BlockType,
    ) -> crate::Result<Block> {
        let (fd, _cache_event) = self
            .file_accessor
            .get_or_open_table(&self.global_id(), &self.path)?;
        let transform = crate::table::util::build_block_transform(
            self.metadata.data_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        let block = Block::from_file(
            fd.as_ref(),
            *handle,
            crate::table::block::BlockIdentity {
                table_id: self.metadata.id,
                block_type,
                dict_id: self.metadata.data_block_compression.dict_id(),
                window_log: 0,
            },
            &transform,
        )?;
        // Swap-defence role check, mirroring `load_block`.
        if block.header.block_type != block_type {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }
        Ok(block)
    }

    /// Frames the block starting at `offset` by reading its HEADER straight
    /// from the file: the on-disk span is header + payload + parity trailer
    /// (SST blocks carry no `block_flags` byte, so the trailer is sized from
    /// the per-SST descriptor scheme). The writer emits blocks back-to-back,
    /// making the physical tiling ground truth: the salvage gap walk frames
    /// index-omitted bytes with this, and the TLI mirror gate compares each
    /// decoded handle against the frame its header derives. A header that
    /// fails to decode, or a span leaving `section_end`, means the bytes are
    /// not frameable as a block.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] on an undecodable header or an
    /// out-of-section span; any I/O error from the read.
    #[cfg(feature = "std")]
    pub(crate) fn probe_block_handle_at(
        &self,
        offset: u64,
        section_end: u64,
    ) -> crate::Result<BlockHandle> {
        let file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        self.probe_block_handle_in(&*file, offset, section_end)
    }

    /// As [`probe_block_handle_at`] but reads through an ALREADY-OPEN handle, so
    /// a caller scanning many offsets (the salvage resync loop, which steps one
    /// byte at a time because block starts are not aligned) pays a single
    /// `open` instead of one per probed offset.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] on an undecodable header or an
    /// out-of-section span; any I/O error from the read.
    #[cfg(feature = "std")]
    pub(crate) fn probe_block_handle_in(
        &self,
        file: &dyn crate::fs::FsFile,
        offset: u64,
        section_end: u64,
    ) -> crate::Result<BlockHandle> {
        use crate::coding::Decode;
        use crate::table::block::Header;

        // Positional read of the largest possible header (block_flags-bearing
        // types are one byte longer than the SST minimum); a short read only
        // matters if it cuts into the bytes `decode_from` actually consumes.
        let mut buf = [0u8; Header::MAX_LEN];
        let got = file.read_at(&mut buf, offset)?;
        let mut cursor = buf.get(..got).ok_or(crate::Error::InvalidHeader(
            "block header read out of bounds",
        ))?;
        let header = Header::decode_from(&mut cursor)?;
        let header_len = Header::header_len(header.block_type) as u64;
        let parity_len = self.metadata.ecc_params.map_or(0, |scheme| {
            u64::from(crate::table::block::expected_parity_len(
                header.data_length,
                scheme,
            ))
        });
        let total = header_len
            .checked_add(u64::from(header.data_length))
            .and_then(|t| t.checked_add(parity_len))
            .ok_or(crate::Error::InvalidHeader("block span overflows the file"))?;
        let end = offset
            .checked_add(total)
            .ok_or(crate::Error::InvalidHeader("block span overflows the file"))?;
        if end > section_end {
            return Err(crate::Error::InvalidHeader(
                "block extends past its section",
            ));
        }
        let size = u32::try_from(total)
            .map_err(|_| crate::Error::InvalidHeader("block span exceeds the block size limit"))?;
        Ok(BlockHandle::new(BlockOffset(offset), size))
    }

    /// Loads (and, for columnar SSTs, reconstructs + delete-masks) a data block.
    /// Returns `Ok(None)` when a columnar block is wholly deleted by the
    /// positional mask, so the caller treats it as carrying no keys.
    ///
    /// `pub(crate)` so the salvage walk ([`crate::salvage`]) can attempt each
    /// data block individually and quarantine the ones that fail to load.
    pub(crate) fn load_data_block(&self, handle: &BlockHandle) -> crate::Result<Option<DataBlock>> {
        // Columnar SSTs store each data block as a PAX `ColumnBatch`; reconstruct
        // the row entries on load so every row read path works unchanged.
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            return self.load_columnar_data_block(handle);
        }
        // `from_loaded` transparently strips the per-KV checksum footer when
        // this SST carries one. Footer presence is a per-SST property
        // (`kv_checksum_algo`), not a per-block header flag — data blocks omit
        // the block_flags byte — so the descriptor supplies it here.
        let has_kv_footer = self.metadata.kv_checksum_algo.is_some();
        self.load_block(
            handle,
            BlockType::Data,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )
        .and_then(|block| DataBlock::from_loaded(block, has_kv_footer))
        .map(Some)
    }

    /// Loads a columnar data block and reconstructs it as a row-major
    /// [`DataBlock`]: decode the `ColumnBatch`, rebuild the entries, and
    /// re-encode them row-major in memory so the existing point-read / iterator
    /// machinery is reused verbatim. The native column-projection read path
    /// (decode only the referenced columns) is a later optimization.
    ///
    /// Returns `Ok(None)` when the positional delete-bitmap deletes every row of
    /// the block, so the caller treats it as carrying no keys.
    #[cfg(feature = "columnar")]
    fn load_columnar_data_block(&self, handle: &BlockHandle) -> crate::Result<Option<DataBlock>> {
        let block = self.load_block(
            handle,
            BlockType::Columnar,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        let restart = self.metadata.data_block_restart_interval;
        match self
            .delete_block_starts
            .as_ref()
            .and_then(|starts| starts.get(&handle.offset().0))
        {
            // The segment has materialized deletes and this block has a recorded
            // start position: drop the deleted rows during reconstruction. The
            // start-row map is built at open from the zone map (every block), so
            // an unmapped block is unreachable; it falls through to the whole-block
            // reconstruction below rather than masking against the wrong positions.
            Some(&start) => DataBlock::from_columnar_block_masked(
                &block.data,
                restart,
                &self.delete_bitmap,
                start,
            ),
            // No materialized deletes (or, unreachably, an unmapped block):
            // reconstruct the whole block.
            None => DataBlock::from_columnar_block(&block.data, restart).map(Some),
        }
    }

    /// Loads a columnar data block as a delete-masked
    /// [`ColumnBatch`](crate::table::columnar::ColumnBatch), preserving its
    /// per-field value sub-columns (and per-row seqnos) instead of reconstructing
    /// rows. Salvage re-emits the result verbatim so a recovered columnar SST
    /// keeps its sub-columns and MVCC versions; `Ok(None)` when the positional
    /// delete-bitmap removes every row of the block.
    ///
    /// `pub(crate)` for the salvage walk ([`crate::salvage`]).
    #[cfg(feature = "columnar")]
    pub(crate) fn load_columnar_block_masked(
        &self,
        handle: &BlockHandle,
    ) -> crate::Result<Option<crate::table::columnar::ColumnBatch>> {
        let block = self.load_block(
            handle,
            BlockType::Columnar,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        let batch = crate::table::columnar::ColumnBatch::decode(&block.data)?;
        // A real writer never emits an empty data block (the ingest path skips
        // the write entirely), so a checksum-clean ZERO-ROW batch is malformed
        // input. Reject it here rather than return it as "live": the writer
        // primitives emit nothing for an empty batch, and a caller counting it
        // as recovered would misreport an unrecovered block as salvaged.
        if batch.row_count == 0 {
            return Err(crate::Error::InvalidHeader("columnar: zero-row data block"));
        }
        let Some(start) = self
            .delete_block_starts
            .as_ref()
            .and_then(|starts| starts.get(&handle.offset().0))
            .copied()
        else {
            // No materialized deletes: the whole block is live.
            return Ok(Some(batch));
        };
        // Drop positionally-deleted rows: this block's rows occupy global
        // positions `[start, start + row_count)` in write order. A corrupt zone
        // map can make `start` large enough that `start + i` overflows the u32
        // row-position space and wraps back to the start of the bitmap, masking
        // unrelated rows. Fail closed on overflow: the salvage walk drops and
        // re-emits this block as corrupt rather than applying deletes at wrong
        // positions.
        let keep: alloc::vec::Vec<bool> = (0..batch.row_count)
            .map(|i| {
                let pos = start.checked_add(i).ok_or(crate::Error::InvalidHeader(
                    "columnar delete position overflow",
                ))?;
                Ok(!self.delete_bitmap.contains(pos))
            })
            .collect::<crate::Result<_>>()?;
        let masked = crate::table::columnar_predicate::filter_batch(&batch, &keep);
        if masked.row_count == 0 {
            Ok(None)
        } else {
            Ok(Some(masked))
        }
    }

    /// Cross-checks the positional delete mask against the ACTUAL per-block
    /// row counts. `delete_block_starts` is derived from the zone map, and a
    /// zone map that decodes but carries wrong counts (a checksum-repatched
    /// tamper) shifts every later block's claimed start — the mask would then
    /// delete the WRONG rows, silently. Walks the data blocks in index order
    /// and requires each block's claimed start to equal the running sum of
    /// actual decoded row counts. An UNREADABLE block fails the verification
    /// outright: its actual count is unknowable, and trusting the zone map's
    /// claim for it would let a repatched count on exactly that block shift
    /// every later mask undetected. Trivially `true` when the segment has no
    /// materialized deletes.
    ///
    /// `pub(crate)` for the salvage walk ([`crate::salvage`]), which must not
    /// mask against unverified positions.
    ///
    /// # Errors
    ///
    /// Propagates a transient [`crate::Error::Io`] from an index / block read.
    /// `Ok(false)` is reserved for a STRUCTURAL failure (a reordered index, a
    /// count mismatch, an undecodable or zero-row block): folding a flaky read
    /// into `false` would classify a retryable fault as a persistent
    /// unpositionable mask, and — with the default `allow_delete_resurrection ==
    /// false` — abort salvage, letting `repair_with_salvage` rebuild the manifest
    /// WITHOUT a table a retry could have recovered faithfully.
    #[cfg(feature = "columnar")]
    pub(crate) fn delete_positions_verified(&self) -> crate::Result<bool> {
        let Some(starts) = self.delete_block_starts.as_deref() else {
            // No materialized deletes: there is nothing to position.
            return Ok(true);
        };
        let mut cumulative: u32 = 0;
        // Anchor the walk to PHYSICAL block order. `delete_block_starts` is built
        // by walking this same index, so a forged TLI that REORDERS the handles
        // rebuilds the starts in that reordered sequence and self-validates
        // against them, yet the bitmap positions were assigned in the writer's
        // physical block order, so the salvage walk (which sorts blocks by
        // offset) would mask against the wrong starts. Requiring strictly
        // increasing offsets rejects the reorder: a genuine index is always in
        // offset order (the writer emits blocks back-to-back).
        let mut prev_offset: Option<u64> = None;
        for keyed in self.block_index.iter() {
            let keyed = match keyed {
                Ok(keyed) => keyed,
                // Only a TRANSIENT read propagates (a retry could verify the
                // mask); a PERSISTENT index failure makes every later position
                // unverifiable, so degrade to an unpositionable mask (`Ok(false)`)
                // and let the caller's resurrection opt-in decide.
                Err(crate::Error::Io(e)) if e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                Err(_) => return Ok(false),
            };
            let offset = keyed.offset().0;
            if prev_offset.is_some_and(|prev| offset <= prev) {
                // Out-of-order (or duplicate) offset: the index was reordered, so
                // the physical positions cannot be trusted.
                return Ok(false);
            }
            prev_offset = Some(offset);
            if starts.get(&offset) != Some(&cumulative) {
                return Ok(false);
            }
            let handle = BlockHandle::new(keyed.offset(), keyed.size());
            let block = match self.load_block(
                &handle,
                BlockType::Columnar,
                self.metadata.data_block_compression,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            ) {
                Ok(block) => block,
                // Only a TRANSIENT read propagates; a PERSISTENT load failure
                // leaves the block's actual count unknowable, so every later
                // position is unverifiable — degrade to an unpositionable mask
                // (`Ok(false)`) rather than trust the (potentially tampered)
                // zone-map claim for it, and let the resurrection opt-in decide.
                Err(crate::Error::Io(e)) if e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                Err(_) => return Ok(false),
            };
            // FULLY decode the batch rather than trusting the leading LE u32
            // row count: a checksum-repatched tamper can keep those four bytes
            // intact while breaking the column framing. The salvage walk would
            // drop such a block as undecodable — but its ACTUAL row count is
            // then just as unknowable as an unreadable block's, so accepting
            // the claimed count here would let the mask land on unproven
            // positions for every later block. Fail closed on any decode
            // failure.
            let Ok(batch) = crate::table::columnar::ColumnBatch::decode(&block.data) else {
                // A decode failure is STRUCTURAL (the salvage walk drops such a
                // block), so its actual count is unknowable — fail closed.
                return Ok(false);
            };
            // A ZERO-ROW batch is malformed input (a real writer never emits
            // an empty block) and the salvage walk DROPS it — so accepting it
            // here (a tampered zone map can claim 0 for exactly that block,
            // keeping the chain self-consistent) would verify positions the
            // bitmap was never built against for every later block. Reject
            // it like the rest of the salvage pipeline does.
            if batch.row_count == 0 {
                return Ok(false);
            }
            let advance = batch.row_count;
            // `wrapping_add` matches how the open path builds the starts map,
            // so the comparison chain stays consistent (the salvage read mask
            // separately rejects positions that would overflow).
            cumulative = cumulative.wrapping_add(advance);
        }
        Ok(true)
    }

    /// Salvage helper: load one data block recovery-aware and, when it reads back
    /// cleanly, also capture its raw on-disk bytes for a verbatim copy.
    ///
    /// Bypasses the block cache so the returned recovery status reflects THIS read
    /// of the medium (a cached block hides whether the on-disk bytes needed ECC
    /// repair). On a clean read ([`verbatim`](SalvageBlock::verbatim) is `Some`),
    /// the salvage walk byte-copies the raw bytes into the recovered SST instead of
    /// decoding + re-encoding the block; on an ECC-recovered read (`None`) the
    /// faulty on-disk bytes must not be propagated, so the caller re-encodes the
    /// healed payload in [`block`](SalvageBlock::block) instead.
    ///
    /// `pub(crate)` for the salvage walk ([`crate::salvage`]).
    pub(crate) fn salvage_load_block(
        &self,
        handle: &BlockHandle,
        block_type: BlockType,
    ) -> crate::Result<SalvageBlock> {
        let table_id = self.global_id();
        let (fd, _) = self
            .file_accessor
            .get_or_open_table(&table_id, &self.path)?;
        let transform = crate::table::util::build_block_transform(
            self.metadata.data_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        let (block, status, recovery) = crate::table::block::Block::from_file_with_recovery(
            fd.as_ref(),
            *handle,
            crate::table::block::BlockIdentity {
                table_id: table_id.table_id(),
                block_type,
                dict_id: self.metadata.data_block_compression.dict_id(),
                window_log: 0,
            },
            &transform,
        )?;
        // A wrong block type means a swapped / corrupt index entry pointed us at
        // the wrong bytes; surface it rather than salvage the wrong block.
        if block.header.block_type != block_type {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }
        // A table whose ECC descriptor this build cannot interpret still reads
        // cleanly (`EccStatus::Unrecognized`, `recovery.is_none()`), but its raw
        // bytes carry an opaque parity trailer the salvage writer's mirrored ECC
        // (`ecc_params = None`) does not account for. A verbatim copy of those
        // bytes would fail the writer's on-disk-size check and abort the whole
        // salvage, so force the re-encode path (which emits a trailer-free block
        // from the decoded payload) for such tables. The same applies to a
        // RECOGNIZED scheme on a build without `page_ecc`: the salvage writer
        // mirrors ECC as `None` there (it cannot emit parity), so raw bytes
        // with a trailer must be re-encoded trailer-free, not byte-copied.
        let ecc_verbatim_ok = cfg!(feature = "page_ecc") || self.metadata.ecc_params.is_none();
        // Clean read: capture the raw on-disk bytes (and inner layout) for a
        // verbatim copy. A second pread of a cold block costs far less than the
        // re-compression the copy avoids.
        //
        // `Err` from this function is reserved for the initial VERIFIED read:
        // by this point that read has already produced a recoverable decoded
        // block, so ANY failure of the re-read — a transient I/O error or a
        // corrupt header, same as a checksum / parity mismatch below — must
        // not drop the block; it just disqualifies the byte-copy and falls
        // back to re-encoding the verified payload (`verbatim = None`).
        let capture_verbatim = || -> Option<VerbatimCopy> {
            use crate::coding::Decode;
            let raw =
                crate::file::read_exact(fd.as_ref(), *handle.offset(), handle.size() as usize)
                    .ok()?;
            let header = crate::table::block::Header::decode_from(&mut &raw[..]).ok()?;
            let layout = self.inner_block_layout(handle.offset().0);
            // A recorded MULTI-INNER layout is the untrusted `block_layout`
            // section itself: a checksum-consistent forge of it routes an
            // otherwise-readable zstd SST through salvage, and copying the block
            // verbatim would re-emit the same unauthenticated inner boundaries,
            // so partial range reads keep omitting keys even though salvage
            // reports success. Disqualify verbatim for such blocks; the re-encode
            // path below rebuilds the frame from the verified decoded payload and
            // records a fresh, self-consistent layout. Single-inner blocks (the
            // common case, empty layout) are unaffected.
            if !layout.is_empty() {
                return None;
            }
            // The bytes COPIED are this second read, not the verified first
            // one, so validate this exact frame before marking it
            // verbatim-safe (a transient fault or concurrent mutation between
            // the reads must not persist unchecked bytes):
            // - the re-read header must equal the verified read's header;
            // - the re-read payload must hash to the header's stored checksum
            //   (the checksum covers the on-disk payload bytes uniformly —
            //   pre-decrypt, pre-decompress);
            // - for an ECC table, the parity trailer must match freshly
            //   computed parity (a clean payload checksum never validates the
            //   trailer — parity is only consulted on a mismatch — so bit rot
            //   confined to the trailer otherwise reads as a clean block).
            // Any mismatch falls back to the re-encode path, which emits the
            // VERIFIED first read's decoded payload with fresh framing.
            let header_len = crate::table::block::Header::header_len(header.block_type);
            // checked_add: a forged/rotted data_length can overflow the
            // payload-end sum on 32-bit targets; overflow = not verbatim-safe.
            let payload_checksum_ok = header_len
                .checked_add(header.data_length as usize)
                .and_then(|payload_end| raw.get(header_len..payload_end))
                .is_some_and(|payload| {
                    crate::hash::hash128(payload) == header.checksum.into_u128()
                });
            (header == block.header
                && payload_checksum_ok
                && self.raw_block_parity_verifies(&raw, &header))
            .then(|| (raw.to_vec(), header, layout))
        };
        // The PER-BLOCK status must be Ok too: `EccStatus::Unrecognized` means
        // the frame carried trailing bytes the transform could not attribute
        // (e.g. an over-sized forged index handle leaking the next section's
        // bytes into the read) — the payload verified, but the raw frame is
        // longer than the header's on-disk size and a verbatim copy would be
        // rejected by the writer, dropping a recoverable block. Re-encode the
        // verified payload instead.
        let verbatim = if recovery.is_none()
            && status == crate::table::block::EccStatus::Ok
            && !self.metadata.ecc_unrecognized
            && ecc_verbatim_ok
        {
            capture_verbatim()
        } else {
            None
        };
        Ok(SalvageBlock { block, verbatim })
    }

    /// Whether `raw`'s parity trailer matches freshly computed parity over its
    /// payload — the verbatim-copy eligibility check for a block of an
    /// ECC-carrying table (see [`Self::salvage_load_block`]). Trivially `true`
    /// for a table without a recognized ECC scheme; a build without `page_ecc`
    /// never reaches this for an ECC table (the caller's gate already routes
    /// those to the re-encode path).
    #[cfg_attr(
        not(feature = "page_ecc"),
        expect(
            clippy::unused_self,
            reason = "without page_ecc the caller's gate excludes ECC tables, so there is no parity to check"
        )
    )]
    fn raw_block_parity_verifies(&self, raw: &[u8], header: &crate::table::block::Header) -> bool {
        #[cfg(feature = "page_ecc")]
        {
            matches!(self.raw_block_parity_delta(raw, header), Ok(None))
        }
        #[cfg(not(feature = "page_ecc"))]
        {
            let _ = (raw, header);
            true
        }
    }

    /// Compares `raw`'s parity trailer against freshly computed parity over
    /// its payload. `Ok(None)` — the trailer matches (or the table carries no
    /// recognized ECC scheme); `Ok(Some(fresh))` — MISMATCH, `fresh` is the
    /// parity the trailer should hold (the in-place heal persists it);
    /// `Err(())` — the frame is inconsistent or the encoder rejected the
    /// shape, so the trailer is unverifiable.
    #[cfg(feature = "page_ecc")]
    fn raw_block_parity_delta(
        &self,
        raw: &[u8],
        header: &crate::table::block::Header,
    ) -> Result<Option<alloc::vec::Vec<u8>>, ()> {
        let Some(params) = self.metadata.ecc_params else {
            return Ok(None);
        };
        let header_len = crate::table::block::Header::header_len(header.block_type);
        // Checked: `data_length` comes from a re-read header, so a forged
        // value must fail as "unverifiable", never wrap (32-bit `usize`).
        let Some(payload_end) = header_len.checked_add(header.data_length as usize) else {
            return Err(());
        };
        // Treat any frame inconsistency as "unverifiable" rather than panicking.
        let (Some(payload), Some(trailer)) =
            (raw.get(header_len..payload_end), raw.get(payload_end..))
        else {
            return Err(());
        };
        let fresh = match params {
            crate::table::block::EccParams::Secded => crate::secded::encode_block_parity(payload),
            crate::table::block::EccParams::Shard { .. } => {
                let (ds, ps) = params.as_shards();
                match crate::ecc::encode_parity(payload, ds, ps) {
                    Ok(p) => p,
                    Err(_) => return Err(()),
                }
            }
        };
        // The heal writes `fresh` back AT the trailer's offset, so the frame
        // must hold EXACTLY that many trailer bytes: a shorter (or longer)
        // on-disk trailer means the frame is malformed, and "healing" it
        // would write past the frame's end into the next block's bytes,
        // breaking the size-preserving heal contract. Unverifiable, not
        // healable.
        if trailer.len() != fresh.len() {
            return Err(());
        }
        if trailer == fresh {
            Ok(None)
        } else {
            Ok(Some(fresh))
        }
    }

    /// The inner-zstd block layout (cumulative decompressed end offsets) for the
    /// data block at `offset`, or empty when the block has a single inner block
    /// (the common case) — and always, on a build without zstd, where data blocks
    /// never split. Salvage's verbatim copy passes this through so a multi-inner
    /// block keeps its layout at its new file offset (the offsets are
    /// decompressed-space and block-relative, so they stay valid after the move).
    #[cfg_attr(
        not(feature = "zstd"),
        expect(
            clippy::unused_self,
            reason = "the layout lookup is zstd-only; without zstd no data block splits, so the layout is always empty and `self` is unused"
        )
    )]
    fn inner_block_layout(&self, offset: u64) -> alloc::vec::Vec<u32> {
        #[cfg(feature = "zstd")]
        {
            self.block_layout
                .ends_for(offset)
                .map(<[u32]>::to_vec)
                .unwrap_or_default()
        }
        #[cfg(not(feature = "zstd"))]
        {
            let _ = offset;
            alloc::vec::Vec::new()
        }
    }

    /// Point-read counterpart to [`Self::load_columnar_data_block`]: decodes the
    /// columnar block once and rebuilds only `needle`'s rows (its MVCC versions,
    /// minus any masked by the positional delete-bitmap) into a tiny row block, or
    /// `Ok(None)` when the key is absent / wholly deleted. The caller runs the
    /// normal seqno-aware point read on the result, so a columnar point read
    /// touches one key's rows instead of untransposing + re-encoding the whole
    /// block per lookup.
    #[cfg(feature = "columnar")]
    fn load_columnar_point_block(
        &self,
        handle: &BlockHandle,
        needle: &[u8],
    ) -> crate::Result<Option<DataBlock>> {
        let block = self.load_block(
            handle,
            BlockType::Columnar,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        let deletes = self
            .delete_block_starts
            .as_ref()
            .and_then(|starts| starts.get(&handle.offset().0))
            .map(|&start| (self.delete_bitmap.as_ref(), start));
        DataBlock::columnar_point_block(
            &block.data,
            needle,
            &self.comparator,
            self.metadata.data_block_restart_interval,
            deletes,
        )
    }

    /// Loads the data block to point-read for `needle`: for a columnar SST the
    /// key-aware fast path that rebuilds only the matching key's rows; for a row
    /// SST the whole block. `Ok(None)` means the block carries no row for `needle`
    /// (columnar: absent / wholly deleted), so the caller moves on.
    fn load_point_block(
        &self,
        handle: &BlockHandle,
        needle: &[u8],
    ) -> crate::Result<Option<DataBlock>> {
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            return self.load_columnar_point_block(handle, needle);
        }
        let _ = needle;
        self.load_data_block(handle)
    }

    /// Loads a columnar data block and decodes only the projected columns,
    /// stepping over the rest without decoding them. The returned batch carries
    /// the requested columns for this block's rows. This is the projection read
    /// the vectorized scan uses, distinct from the whole-block reconstruction
    /// that the row read paths use.
    #[cfg(feature = "columnar")]
    fn load_columnar_block_projected(
        &self,
        handle: &BlockHandle,
        projection: &[u16],
    ) -> crate::Result<crate::table::columnar::ColumnBatch> {
        let block = self.load_block(
            handle,
            BlockType::Columnar,
            self.metadata.data_block_compression,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        crate::table::columnar::ColumnBatch::decode_projected(&block.data, projection)
    }

    /// Returns the (possibly compressed) file size.
    pub(crate) fn file_size(&self) -> u64 {
        self.metadata.file_size
    }

    /// The on-disk ROLE of this table's data blocks: a columnar segment's
    /// writer seals them as [`BlockType::Columnar`], a row-major one as
    /// [`BlockType::Data`]. Scrub / heal walks pass this as the expected type
    /// so the per-block role check (swap-defence against a misdirected index
    /// entry) matches what the writer actually emitted.
    #[cfg(feature = "std")]
    fn data_block_role(&self) -> BlockType {
        if self.metadata.columnar {
            BlockType::Columnar
        } else {
            BlockType::Data
        }
    }

    /// Patrol-scrubs every data block of this table: a cache-bypassing read that
    /// runs the Page-ECC verify+correct path, recording a heal hint (when
    /// `auto_heal` is on) on a confirmed-persistent correction.
    ///
    /// Returns a partial [`PatrolScrubReport`](crate::scrub::PatrolScrubReport)
    /// for this SST (`sst_files_scanned == 1`) so the caller can merge it across
    /// the tree. Always runs to completion: an uncorrectable / unreadable block
    /// is recorded (and logged), not silently skipped, and the next block is
    /// still scrubbed. A block-index walk failure stops this table early (later
    /// offsets are untrustworthy) but other tables still scrub.
    #[cfg(feature = "std")]
    pub(crate) fn scrub_data_blocks(&self) -> crate::scrub::PatrolScrubReport {
        use crate::scrub::{PatrolScrubReport, ScrubError};
        use crate::table::util::{BlockScrubOutcome, scrub_block};

        let mut report = PatrolScrubReport {
            sst_files_scanned: 1,
            ..PatrolScrubReport::default()
        };

        for entry in self.block_index.iter() {
            let keyed = match entry {
                Ok(h) => h,
                Err(e) => {
                    // A structural index error means later offsets can't be
                    // trusted — stop this table, record it, let others run.
                    log::error!(
                        "patrol scrub: block index of table {} at {} unreadable: {e:?}",
                        self.id(),
                        self.path.display(),
                    );
                    report.errors.push(ScrubError::BlockIndexUnreadable {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!("{e:?}"),
                    });
                    break;
                }
            };

            // Tight-space restriction: skip a block whose last key is below the
            // bound: it sits in the punched-out prefix a superseding output
            // table now owns, so scrubbing its reclaimed bytes would report
            // spurious corruption for a restricted view (see `Table::range`).
            if let Some(bound) = &self.1
                && self.comparator.compare(keyed.end_key(), bound) == core::cmp::Ordering::Less
            {
                continue;
            }
            let block_offset = keyed.offset().0;
            let handle = BlockHandle::new(keyed.offset(), keyed.size());
            report.blocks_scanned += 1;

            match scrub_block(
                self.global_id(),
                &self.path,
                &self.file_accessor,
                &handle,
                self.data_block_role(),
                self.metadata.data_block_compression,
                self.encryption.as_deref(),
                self.metadata.ecc_params,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
                self.heal_hints.get().map(AsRef::as_ref),
                #[cfg(feature = "metrics")]
                &self.metrics,
            ) {
                Ok(BlockScrubOutcome::Clean) => {}
                Ok(BlockScrubOutcome::Corrected { scheduled }) => {
                    report.corrections_applied += 1;
                    if scheduled {
                        // heal_hints dedups per SST, so `scheduled` is true at
                        // most once per table — this counts distinct SSTs.
                        report.ssts_scheduled_for_rewrite += 1;
                    }
                }
                Err(e) => {
                    report.uncorrectable_blocks += 1;
                    log::error!(
                        "patrol scrub: uncorrectable block at offset {block_offset} in table {} \
                         at {}: {e:?}",
                        self.id(),
                        self.path.display(),
                    );
                    report.errors.push(ScrubError::UncorrectableBlock {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        block_offset,
                        reason: alloc::format!("{e:?}"),
                    });
                }
            }
        }

        report
    }

    /// Ensures the heal's handle may be WRITTEN through: probes the link
    /// count on first use and detaches a multiply-linked inode onto a
    /// private copy via [`Self::unshare_for_heal`]. The outcome is cached in
    /// `state`, so the probe and copy run at most once per scan and only
    /// when a write is actually needed (a clean scan never detaches a
    /// checkpoint link). A failed link-count query is treated as shared
    /// (fail closed) — the copy path is safe either way.
    ///
    /// # Errors
    ///
    /// The unshare failure reason when the write must NOT happen (the inode
    /// may still be shared); repeated calls keep returning it.
    #[cfg(feature = "page_ecc")]
    fn ensure_unshared_for_write(
        &self,
        file: &mut Box<dyn crate::fs::FsFile>,
        state: &mut UnshareState,
        sync_mode: crate::fs::SyncMode,
    ) -> Result<(), alloc::string::String> {
        match state {
            UnshareState::Ready => Ok(()),
            UnshareState::Failed(reason) => Err(reason.clone()),
            UnshareState::Unprobed => {
                let shared = match file.hard_link_count() {
                    Ok(n) => n > 1,
                    Err(e) => {
                        log::warn!(
                            "in-place heal: link-count query failed for table {} at {}: {e}; \
                             assuming the inode is shared",
                            self.id(),
                            self.path.display(),
                        );
                        true
                    }
                };
                if shared {
                    // A pinned FD (no descriptor cache) is bound to its
                    // inode for the table's lifetime: after the unshare's
                    // copy + rename, every later read, scrub probe, and
                    // digest check would keep resolving the DEAD inode
                    // while the manifest points at the healed live path.
                    // Refuse the detach — the blocked write-backs surface
                    // as findings and every link stays byte-identical.
                    if !self.file_accessor.can_retarget() {
                        let reason = alloc::string::String::from(
                            "the inode is multiply linked and the pinned file \
                             descriptor cannot be retargeted at a detached copy; \
                             refusing the in-place write",
                        );
                        *state = UnshareState::Failed(reason.clone());
                        return Err(reason);
                    }
                    match self.unshare_for_heal(file.as_ref(), sync_mode) {
                        Ok(fresh) => *file = fresh,
                        Err(reason) => {
                            *state = UnshareState::Failed(reason.clone());
                            return Err(reason);
                        }
                    }
                }
                *state = UnshareState::Ready;
                Ok(())
            }
        }
    }

    /// Detaches this table's live path from a multiply-linked inode so an
    /// in-place heal can write without touching the other links (checkpoint
    /// snapshots): streams the file into a sibling `*.healtmp-{n}` copy, syncs
    /// it, and atomically renames it over the live path. The returned handle
    /// is the copy's (still valid after the rename), open read+write.
    ///
    /// The temp name carries a process-wide sequence number so two concurrent
    /// heal scans of the same table can never remove or rename each other's
    /// in-progress copy (cross-process exclusion comes from the directory
    /// lock). Every pre-rename failure removes the copy before returning —
    /// recovery refuses to open a tree whose `tables/` holds a file it cannot
    /// parse as a table id, so an abandoned artifact must never outlive the
    /// heal (recovery still sweeps `*.healtmp-*` left by a hard crash).
    #[cfg(feature = "page_ecc")]
    pub(crate) fn unshare_for_heal(
        &self,
        source: &dyn crate::fs::FsFile,
        sync_mode: crate::fs::SyncMode,
    ) -> Result<Box<dyn crate::fs::FsFile>, alloc::string::String> {
        use std::io::{Seek, SeekFrom, Write};

        static HEAL_TMP_SEQ: core::sync::atomic::AtomicU64 = core::sync::atomic::AtomicU64::new(0);

        let len = source
            .metadata()
            .map_err(|e| alloc::format!("metadata: {e}"))?
            .len;

        // A tight-space-restricted view has reclaimed the DATA blocks below its
        // frontier, but NOT the whole `[0, punch)` span: live index / filter blocks
        // are interleaved among those data blocks (partitioned index / filter) and
        // the reopen path reads them, so `Inner::drop` punches each data block
        // INDIVIDUALLY and leaves the metadata blocks intact. Reproduce that exact
        // hole pattern in the heal copy — copy the live blocks, leave the reclaimed
        // data-block extents as holes — instead of materializing the zeros (which
        // would re-allocate the reclaimed space, an ENOSPC risk on the near-full disk
        // tight-space runs on) OR zeroing the whole prefix (which would corrupt the
        // interleaved metadata). `punch` is `0` for a normal (unrestricted) table, so
        // its copy is byte-for-byte as before.
        let punch = self
            .punch_offset()
            .map_err(|e| alloc::format!("punch offset: {e}"))?;
        let sparse = punch > 0 && self.fs.capabilities(&self.path).punch_hole;
        let seq = HEAL_TMP_SEQ.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        let tmp_path = self.path.with_extension(alloc::format!("healtmp-{seq}"));
        let mut tmp = self
            .fs
            .open(
                &tmp_path,
                &FsOpenOptions::new().read(true).write(true).create_new(true),
            )
            .map_err(|e| alloc::format!("create heal copy: {e}"))?;

        // Any failure past this point must take the copy with it (see above).
        let mut copy = || -> Result<(), alloc::string::String> {
            // The reclaimed DATA-block extents below the frontier — exactly what
            // tight-space punched (via `Inner::drop`). The block index yields ONLY
            // data-block handles, so live index / filter blocks interleaved below the
            // frontier are NOT in this set and stay in the copied complement.
            let mut holes: alloc::vec::Vec<(u64, u64)> = alloc::vec::Vec::new();
            if sparse {
                use crate::table::block_index::BlockIndex;
                for handle in self.block_index.iter() {
                    let handle = handle.map_err(|e| alloc::format!("block index iter: {e}"))?;
                    let block_off = handle.offset().0;
                    if block_off < punch {
                        holes.push((block_off, u64::from(handle.size())));
                    }
                }
                holes.sort_unstable();
                // Establish the full size so the un-written data-block extents stay
                // sparse holes.
                tmp.set_len(len)
                    .map_err(|e| alloc::format!("set heal copy length: {e}"))?;
            }

            // Live ranges to stream = the complement of the data-block holes across
            // `[0, len)`. With no holes (a normal table) this is the whole file.
            let mut live: alloc::vec::Vec<(u64, u64)> = alloc::vec::Vec::new();
            let mut cursor = 0u64;
            for &(h_off, h_len) in &holes {
                if h_off > cursor {
                    live.push((cursor, h_off));
                }
                cursor = cursor.max(h_off + h_len);
            }
            if cursor < len {
                live.push((cursor, len));
            }

            let mut buf = alloc::vec![0u8; 1 << 20];
            for &(start, end) in &live {
                tmp.seek(SeekFrom::Start(start))
                    .map_err(|e| alloc::format!("seek heal copy to {start}: {e}"))?;
                let mut off = start;
                while off < end {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "the u64 min() is taken first, so the value is bounded by buf.len()"
                    )]
                    let want = (end - off).min(buf.len() as u64) as usize;
                    let Some(chunk) = buf.get_mut(..want) else {
                        return Err(alloc::string::String::from("chunk within buffer"));
                    };
                    let n = source
                        .read_at(chunk, off)
                        .map_err(|e| alloc::format!("read source at {off}: {e}"))?;
                    if n < want {
                        // Fill-or-EOF contract: a short read here means the file
                        // shrank underneath us — abort rather than install a
                        // truncated copy.
                        return Err(alloc::format!(
                            "short read at {off}: got {n} of {want} bytes"
                        ));
                    }
                    tmp.write_all(chunk)
                        .map_err(|e| alloc::format!("write heal copy at {off}: {e}"))?;
                    off += want as u64;
                }
            }
            // Deallocate the data-block holes even where `set_len` zero-allocated, so
            // the reclaimed space is not silently re-consumed by the heal copy.
            for &(h_off, h_len) in &holes {
                self.fs
                    .punch_hole(&tmp_path, h_off, h_len)
                    .map_err(|e| alloc::format!("punch heal copy data block at {h_off}: {e}"))?;
            }
            // sync_all, not sync_data: the copy is a NEW file, its size must
            // be durable before the rename publishes it. Mode-aware: the
            // caller selected the tree's durability (Normal skips the macOS
            // F_FULLFSYNC hardware barrier, same as the flush path).
            tmp.sync_all_with(sync_mode)
                .map_err(|e| alloc::format!("sync heal copy: {e}"))?;
            self.fs
                .rename(&tmp_path, &self.path)
                .map_err(|e| alloc::format!("rename heal copy into place: {e}"))
        };
        if let Err(reason) = copy() {
            if let Err(e) = self.fs.remove_file(&tmp_path) {
                log::warn!(
                    "failed to remove abandoned heal copy {}: {e}",
                    tmp_path.display(),
                );
            }
            return Err(reason);
        }
        // The rename has replaced the live path, so the descriptor cache now
        // holds the OLD inode's fd: a later heal (or read) resolving through
        // it would scrub the detached inode while writes target the live one,
        // misreporting a live-only fault as an unexplained (uncorrectable)
        // mismatch. Drop the stale entry IMMEDIATELY — before the durability
        // sync below, whose failure must not leave the cache pinned to the
        // dead inode — so the next access reopens the live path.
        self.file_accessor.remove_for_table(&self.global_id());
        if let Some(parent) = self.path.parent() {
            self.fs
                .sync_directory_with(parent, sync_mode)
                .map_err(|e| alloc::format!("sync directory after rename: {e}"))?;
        }
        // The handle survives the rename (same inode, now the live path).
        Ok(tmp)
    }

    /// In-place ECC autoheal: like [`Self::scrub_data_blocks`], but PERSISTS each
    /// correction by writing the corrected block back at its existing offset
    /// (size-preserving) instead of scheduling a full-file healing rewrite.
    /// Healthy blocks are never rewritten, so the cost is O(damage), not O(file).
    ///
    /// Opens the SST read+write through the tree's `Fs` (bypassing the block
    /// cache, like scrub), and for each data block that fails its checksum but
    /// Page-ECC recovers it, writes back `header ++ recovered_data ++ recomputed
    /// parity` and `sync_data`s it before moving on, so a crash mid-heal leaves
    /// the block in its prior, still-RS-correctable state. Uncorrectable /
    /// unreadable blocks are recorded as findings and left for block salvage.
    ///
    /// HARD-LINK SAFE by unsharing: checkpoints hard-link SSTs, and a
    /// checkpoint's manifest records the digest of the bytes AT SNAPSHOT TIME,
    /// which the checkpoint (immutable by design) can never reconcile the way
    /// the live tree does. Writing through a shared inode would therefore
    /// desynchronize the snapshot from its own manifest whenever the recorded
    /// digest is not the original file's (a manifest rebuilt over rotted
    /// bytes). Right before the FIRST write-back (lazily — a clean scan of a
    /// linked healthy table performs zero writes and keeps the checkpoint's
    /// disk sharing), the link count is probed and a multiply-linked live
    /// path is detached onto a private copy (copy + atomic rename), which is
    /// then healed, leaving every other link byte-identical to what its
    /// manifest describes. Cached read-only descriptors may keep serving the
    /// old inode until they are reopened; its bytes are unchanged, so reads
    /// stay correct (ECC-corrected on the fly as before).
    ///
    /// Returns the scrub report plus an ATTRIBUTION flag: `true` when the
    /// file's digest was computed right before this pass's FIRST write and it
    /// matched the manifest digest — every byte the file now differs from
    /// the manifest state by is provably one of this pass's verified
    /// corrections. The digest reconciliation uses it to decide whether a
    /// post-heal mismatch may cover sections that cannot be semantically
    /// authenticated (deletion metadata). `false` whenever nothing was
    /// written, the pre-write digest could not be computed, or it already
    /// disagreed with the manifest.
    ///
    /// `pub(crate)` for [`crate::scrub::patrol_scrub`] (heal-in-place enabled).
    /// `sync_mode` is the tree's configured durability
    /// ([`Config::sync_mode`](crate::config::Config::sync_mode)): every
    /// write-back, the unshare copy, and the post-rename directory sync
    /// honor it, so a patrol with many corrections does not pay a macOS
    /// `F_FULLFSYNC` hardware barrier per block when the caller selected
    /// [`SyncMode::Normal`](crate::fs::SyncMode::Normal).
    ///
    /// `manifest_checksum` is the table's CURRENT manifest digest, read by
    /// the caller under the per-table heal lock: a concurrent patrol may
    /// have refreshed the manifest after this view was captured, and the
    /// attribution probe must compare against what the manifest says NOW.
    #[cfg(feature = "page_ecc")]
    pub(crate) fn heal_data_blocks_in_place(
        &self,
        sync_mode: crate::fs::SyncMode,
        manifest_checksum: Checksum,
    ) -> (crate::scrub::PatrolScrubReport, bool) {
        use crate::scrub::{PatrolScrubReport, ScrubError};
        use std::io::{Seek, SeekFrom, Write};

        let mut report = PatrolScrubReport {
            sst_files_scanned: 1,
            ..PatrolScrubReport::default()
        };
        // Whether any write-back was ATTEMPTED. A write_all that errors after a
        // partial write, or a full write whose sync then fails, may already have
        // changed the on-disk bytes even though `blocks_healed_in_place` stays 0,
        // so the marker (the only attribution for a later digest refresh) must be
        // KEPT in that case; it is removed only when no mutation ever began.
        let mut write_attempted = false;

        // NEITHER exclusion is taken here — both are held by the patrol
        // scrub across this scan AND the digest reconciliation that follows
        // it, because re-acquiring either inside would deadlock:
        // - the per-table `heal_lock` (a Mutex) serializes same-table heals
        //   for the whole scan-to-reconcile span, so one patrol cannot
        //   install a digest computed before another patrol's heal;
        // - the `DeletionPause` mutation window excludes a checkpoint's link
        //   pass for the same span, so a checkpoint cannot link healed bytes
        //   under a stale digest.

        // A single read+write handle for both the recovery read and the
        // write-back, opened directly (not via the read-only descriptor cache) so
        // the heal sees the medium and can mutate it.
        let mut file = match self
            .fs
            .open(&self.path, &FsOpenOptions::new().read(true).write(true))
        {
            Ok(f) => f,
            Err(e) => {
                // Cannot persist corrections (a read-only replica, restrictive
                // permissions) — but the table must still get its integrity
                // CHECKED. Record the failed open, then fall back to the
                // read-only scrub so corruption still surfaces instead of
                // returning a healthy-looking report with zero blocks scanned.
                report.errors.push(ScrubError::BlockIndexUnreadable {
                    table_id: self.id(),
                    path: self.path.to_path_buf(),
                    reason: alloc::format!("open read+write for heal: {e}"),
                });
                report.merge(self.scrub_data_blocks());
                // Both passes stamped this SST as scanned; it is one file.
                report.sst_files_scanned = 1;
                return (report, false);
            }
        };

        // A multiply-linked inode (a checkpoint shares it) must not be healed
        // through — but detaching costs a full-file copy and permanently
        // ends the checkpoint's disk sharing, so it happens LAZILY: the link
        // count is probed (and a shared inode detached onto a private copy)
        // only right before the FIRST write-back. A clean scan of a linked
        // healthy table therefore stays O(damage) = zero writes. The probe
        // stays honest under the caller's mutation window: a checkpoint
        // cannot link this SST anywhere inside the scan-to-write span.
        let mut unshare_state = UnshareState::Unprobed;

        let transform = match crate::table::util::build_block_transform(
            self.metadata.data_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        ) {
            Ok(t) => t,
            Err(e) => {
                report.errors.push(ScrubError::BlockIndexUnreadable {
                    table_id: self.id(),
                    path: self.path.to_path_buf(),
                    reason: alloc::format!("build transform for heal: {e:?}"),
                });
                return (report, false);
            }
        };

        // Attribution + crash-recovery marker, computed UP FRONT while the file
        // still holds its pre-heal bytes. The heal is deterministic, so the
        // digest of the file with every correction applied is known before any
        // write lands: bind THAT into the attestation (not just `pre ==
        // manifest`), so a marker a crash leaves behind can only ever
        // re-authorize the exact healed bytes, never an unrelated later forge.
        // A clean scan (no corrections) writes nothing. `pre_heal_matched`
        // still drives the returned attribution flag and the zero-heal marker
        // cleanup below; the per-block correction is recomputed identically in
        // the write loop (both call `heal_correction_for_block`).
        // A restricted view predicts (and later digests) only its live suffix;
        // its corrections all lie there, and the punched prefix is not hashed.
        // A transient failure resolving that bound must ABORT, not fall back to
        // offset 0: predicting (and attesting) over the WHOLE physical file while
        // the manifest and reconciliation hash only the suffix would, after a
        // crash mid-heal, leave a completed marker whose digest can never match
        // the suffix — permanently stranding the healed table.
        let heal_start = match self.restrict_lower_bound() {
            Some(bound) => match self.punch_offset_for(bound) {
                Ok(off) => off,
                Err(e) => {
                    report.errors.push(ScrubError::ChecksumRefreshFailed {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!(
                            "could not resolve the restricted heal start; heal skipped to \
                             keep the table reconcilable: {e:?}"
                        ),
                    });
                    return (report, false);
                }
            },
            None => 0,
        };
        // Predict the post-heal digest AND the exact set of offsets the heal
        // will touch, streaming so a broadly damaged table does not materialize
        // every corrected frame at once. The write loop below applies ONLY these
        // offsets, so a fault appearing after this pass is never healed under a
        // digest that did not attest it.
        let (predicted_digest, predicted_offsets) =
            match self.predict_heal_digest_and_offsets(file.as_ref(), &transform, heal_start) {
                Ok(pair) => pair,
                Err(e) => {
                    report.errors.push(ScrubError::ChecksumRefreshFailed {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!(
                            "could not predict the post-heal digest; heal skipped to keep the \
                             table reconcilable: {e:?}"
                        ),
                    });
                    return (report, false);
                }
            };
        // The pre-heal digest probe result: `None` when there is nothing to
        // heal; otherwise `Some(matched)` where `matched` says whether the
        // current bytes still match the manifest (the ATTRIBUTABLE path). A
        // probe FAILURE aborts before the first write: writing corrections with
        // no completed marker can permanently strand a table whose manifest
        // legitimately described the pre-heal bytes.
        let pre_heal_matched: Option<bool> = if predicted_offsets.is_empty() {
            None
        } else {
            let matched = match self.pre_heal_digest_matches(manifest_checksum) {
                Ok(matched) => matched,
                Err(e) => {
                    report.errors.push(ScrubError::ChecksumRefreshFailed {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!(
                            "pre-heal digest probe failed; heal skipped to keep the \
                             table reconcilable: {e:?}"
                        ),
                    });
                    return (report, false);
                }
            };
            if matched {
                // ATTRIBUTABLE path: the heal is about to change bytes the
                // manifest digest still matches, so the crash-recovery marker
                // MUST be durable BEFORE the first mutation. If the post-heal
                // digest cannot be predicted, or the attestation cannot be
                // persisted, do NOT heal: a crash after a corrected block syncs
                // but before the in-process manifest refresh would leave healed
                // bytes under the stale digest with no marker, and fail-closed
                // reconciliation rejects that mismatch forever, permanently
                // stranding a table that was reconcilable a moment earlier.
                // Leaving the block corrupt keeps the table reconcilable; the
                // next patrol retries once the marker can be written. (A
                // non-crash run also reconciles directly, but the marker is the
                // ONLY thing that survives a crash, so its durability gates the
                // mutation.)
                if let Err(e) = crate::scrub::heal_attest::write(
                    &*self.fs,
                    &self.path,
                    self.encryption.as_deref(),
                    self.id(),
                    manifest_checksum,
                    Checksum::from_raw(predicted_digest),
                ) {
                    report.errors.push(ScrubError::ChecksumRefreshFailed {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!(
                            "could not persist the heal attestation; heal skipped to keep the \
                             table reconcilable: {e}"
                        ),
                    });
                    return (report, false);
                }
                Some(true)
            } else {
                // NOT-matched pre-heal: the current bytes already differ from the
                // manifest digest. If the predicted heal RESTORES the manifest
                // digest (the common restorative case: a rotted file healing back to
                // exactly what the manifest describes), the write window STILL needs
                // a durable marker BEFORE the first mutation. A crash after syncing
                // SOME of several corrections leaves the file matching neither the
                // manifest nor the predicted digest; with no marker a checkpoint
                // hard-links those intermediate bytes under the stale manifest
                // digest, producing a permanently inconsistent checkpoint. The
                // marker records `(manifest, predicted)` (here equal), so a reconcile
                // trusts the file only once it hashes to the restored digest. (The
                // DIVERGING sub-case, predicted != manifest, is gated separately
                // below against an EXISTING completed marker via `attests_post`.)
                if Checksum::from_raw(predicted_digest) == manifest_checksum
                    && let Err(e) = crate::scrub::heal_attest::write(
                        &*self.fs,
                        &self.path,
                        self.encryption.as_deref(),
                        self.id(),
                        manifest_checksum,
                        Checksum::from_raw(predicted_digest),
                    )
                {
                    report.errors.push(ScrubError::ChecksumRefreshFailed {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!(
                            "could not persist the restorative heal attestation; heal \
                             skipped to keep the table reconcilable: {e}"
                        ),
                    });
                    return (report, false);
                }
                Some(false)
            }
        };

        // A DIVERGING heal on a stale-manifest file must NOT proceed: when the
        // current bytes do not match the manifest AND the predicted post-heal
        // digest would not match it either, the file is drifting away from the
        // manifest (e.g. a prior heal crashed after writing its completed
        // attestation but before refreshing the manifest, then a fresh fault
        // appeared). Writing these corrections would move the bytes off any
        // existing attestation's `post` digest with no chaining marker, so the
        // reconcile that runs right after this returns could not attribute the
        // result and would strip the marker — leaving future scrubs and
        // checkpoints unable to reconcile the table. Leave it untouched (fail
        // closed): the reconcile still reconciles an existing marker against the
        // manifest, and a later patrol heals the fault once the bytes line up.
        //
        // A not-matched file whose predicted heal RESTORES the manifest digest
        // (a plain rotted file healing back to what the manifest describes), or
        // restores a digest an existing COMPLETED marker already attests (a fresh
        // fault on the just-healed bytes, which that marker still reconciles), is
        // NOT diverging, so it proceeds. `None` (nothing to heal) is unaffected.
        if pre_heal_matched == Some(false)
            && Checksum::from_raw(predicted_digest) != manifest_checksum
        {
            use crate::scrub::heal_attest::AttestResult;
            match crate::scrub::heal_attest::attests_post(
                &*self.fs,
                &self.path,
                self.encryption.as_deref(),
                self.id(),
                Checksum::from_raw(predicted_digest),
            ) {
                // An existing COMPLETED marker already attests the predicted post:
                // the heal restores an attested digest, not a divergence — proceed.
                AttestResult::Attests => {}
                // No attesting marker: healing would drift the file off any
                // marker's post with no chaining attribution, so the reconcile
                // could not attribute it and would strip the marker. Fail closed.
                AttestResult::Absent => return (report, false),
                // The attestation probe hit a TRANSIENT read. Collapsing that to
                // "does not attest" (the old behavior) would skip the heal, leaving
                // the file diverged from a marker that MAY attest the predicted
                // post; the reconcile then rereads the now-readable marker, finds
                // it no longer matches the current bytes, and deletes it — stranding
                // the table. Record a finding so the report is non-clean (a
                // checkpoint's `reconcile_pending_heals` then aborts instead of
                // removing) and skip; the next patrol retries once the probe reads
                // cleanly and the marker's verdict is conclusive.
                AttestResult::Inconclusive => {
                    report.errors.push(ScrubError::ChecksumRefreshFailed {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: "heal attestation probe was inconclusive (transient read); heal \
                                 skipped to preserve the existing marker for the next patrol"
                            .to_string(),
                    });
                    return (report, false);
                }
            }
        }

        for entry in self.block_index.iter() {
            let keyed = match entry {
                Ok(h) => h,
                Err(e) => {
                    // A structural index error means later offsets can't be
                    // trusted — stop this table, record it.
                    report.errors.push(ScrubError::BlockIndexUnreadable {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        reason: alloc::format!("{e:?}"),
                    });
                    break;
                }
            };
            // Tight-space restriction: a block whose last key is below the bound
            // sits in the punched-out (reclaimed) prefix that a superseding
            // output table now owns. Its bytes are gone, so reading it reports a
            // spurious uncorrectable error that would suppress the digest refresh
            // for a real correction in the LIVE suffix. Skip it: the read path
            // clamps scans the same way (see `Table::range`).
            if let Some(bound) = &self.1
                && self.comparator.compare(keyed.end_key(), bound) == core::cmp::Ordering::Less
            {
                continue;
            }
            let block_offset = keyed.offset().0;
            let handle = BlockHandle::new(keyed.offset(), keyed.size());
            report.blocks_scanned += 1;

            // Verify the block through the SAME full read the scrub path uses
            // (checksum + decode + ECC recovery), not just a bare frame check.
            // This detects a checksum-clean-but-undecodable block (e.g. a corrupt
            // `uncompressed_length`) and a corrupt block in a non-ECC segment,
            // reporting it as uncorrectable instead of silently clean. `heal_hints
            // = None`: this path persists the correction IN PLACE, so it must not
            // also queue a full-file healing rewrite. The metric is recorded inside
            // `scrub_block`.
            let outcome = crate::table::util::scrub_block(
                self.global_id(),
                &self.path,
                &self.file_accessor,
                &handle,
                self.data_block_role(),
                self.metadata.data_block_compression,
                self.encryption.as_deref(),
                self.metadata.ecc_params,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
                None,
                #[cfg(feature = "metrics")]
                &self.metrics,
            );
            match outcome {
                // Verified clean: nothing to persist.
                // Verified clean — but a clean PAYLOAD checksum never
                // validates the parity trailer (parity is only consulted on a
                // mismatch), so rot confined to the trailer would silently
                // leave dead ECC on disk: a later payload fault in this block
                // could no longer be recovered. This pass holds the read+write
                // handle and the payload is untouched, so a size-preserving
                // trailer rebuild is exactly the heal it exists to perform.
                Ok(crate::table::util::BlockScrubOutcome::Clean) => {
                    let raw = match crate::file::read_exact(
                        file.as_ref(),
                        block_offset,
                        keyed.size() as usize,
                    ) {
                        Ok(raw) => raw,
                        Err(e) => {
                            report.uncorrectable_blocks += 1;
                            report.errors.push(ScrubError::UncorrectableBlock {
                                table_id: self.id(),
                                path: self.path.to_path_buf(),
                                block_offset,
                                reason: alloc::format!(
                                    "in-place heal: parity re-read failed: {e:?}"
                                ),
                            });
                            continue;
                        }
                    };
                    use crate::coding::Decode;
                    let Ok(raw_header) = crate::table::block::Header::decode_from(&mut &raw[..])
                    else {
                        // The scrub just read this frame cleanly; a header that
                        // no longer decodes is an inconsistency worth surfacing.
                        report.uncorrectable_blocks += 1;
                        report.errors.push(ScrubError::UncorrectableBlock {
                            table_id: self.id(),
                            path: self.path.to_path_buf(),
                            block_offset,
                            reason: alloc::string::String::from(
                                "in-place heal: block scrubbed clean but its header no \
                                 longer decodes",
                            ),
                        });
                        continue;
                    };
                    // The bytes examined below are this SECOND read, not the
                    // frame the scrub just verified: a transient fault or
                    // fresh rot between the reads would otherwise feed the
                    // parity comparison an unverified payload, and the
                    // rebuild arm would PERSIST parity computed over those
                    // corrupt bytes — turning a recoverable block into one
                    // whose ECC agrees with the corruption. Verify the
                    // re-read header's ROLE and its payload against the
                    // header checksum first; a mismatch is surfaced, never
                    // acted on.
                    if raw_header.block_type != self.data_block_role() {
                        report.uncorrectable_blocks += 1;
                        report.errors.push(ScrubError::UncorrectableBlock {
                            table_id: self.id(),
                            path: self.path.to_path_buf(),
                            block_offset,
                            reason: alloc::string::String::from(
                                "in-place heal: block scrubbed clean but its re-read \
                                 header carries a different block role",
                            ),
                        });
                        continue;
                    }
                    let header_len = crate::table::block::Header::header_len(raw_header.block_type);
                    // checked_add: a forged/rotted data_length can overflow
                    // the payload-end sum on 32-bit targets; overflow is an
                    // uncorrectable finding, not a panic.
                    let payload_ok = header_len
                        .checked_add(raw_header.data_length as usize)
                        .and_then(|payload_end| raw.get(header_len..payload_end))
                        .is_some_and(|payload| {
                            crate::hash::hash128(payload) == raw_header.checksum.into_u128()
                        });
                    if !payload_ok {
                        report.uncorrectable_blocks += 1;
                        report.errors.push(ScrubError::UncorrectableBlock {
                            table_id: self.id(),
                            path: self.path.to_path_buf(),
                            block_offset,
                            reason: alloc::string::String::from(
                                "in-place heal: block scrubbed clean but its re-read \
                                 payload does not match its checksum",
                            ),
                        });
                        continue;
                    }
                    match self.raw_block_parity_delta(&raw, &raw_header) {
                        // Trailer matches (or no ECC): nothing to persist.
                        Ok(None) => {}
                        // Trailer rot: persist the freshly computed parity at
                        // its on-disk position (header + payload unchanged).
                        Ok(Some(fresh)) => {
                            let trailer_offset = block_offset
                                + crate::table::block::Header::header_len(raw_header.block_type)
                                    as u64
                                + u64::from(raw_header.data_length);
                            // Apply ONLY corrections the prediction pass attested.
                            // A trailer rebuild that appears now but was not
                            // predicted (fresh rot between the two reads) is left
                            // as-is: healing it would put bytes on disk the
                            // marker's digest never covered, and a later
                            // checkpoint would snapshot them under the stale
                            // digest. It stays RS-correctable for the next patrol.
                            if !predicted_offsets.contains(&trailer_offset) {
                                continue;
                            }
                            // Attribution + the crash-recovery marker were
                            // captured UP FRONT (before this loop), so nothing is
                            // done here. First write: make sure no checkpoint link
                            // shares the inode (lazy detach).
                            if let Err(reason) = self.ensure_unshared_for_write(
                                &mut file,
                                &mut unshare_state,
                                sync_mode,
                            ) {
                                report.uncorrectable_blocks += 1;
                                report.errors.push(ScrubError::UncorrectableBlock {
                                    table_id: self.id(),
                                    path: self.path.to_path_buf(),
                                    block_offset,
                                    reason: alloc::format!(
                                        "unshare hard-linked SST for heal: {reason}"
                                    ),
                                });
                                continue;
                            }
                            let write_back = file
                                .seek(SeekFrom::Start(trailer_offset))
                                .and_then(|_| file.write_all(&fresh));
                            // The write_all may have written some bytes even if it
                            // then errored; mark the file as possibly mutated so a
                            // later failure does not drop the attestation.
                            write_attempted = true;
                            let durable = match write_back {
                                Ok(()) => file
                                    .sync_data_with(sync_mode)
                                    .map_err(|e| alloc::format!("sync: {e}")),
                                Err(e) => Err(alloc::format!("write: {e}")),
                            };
                            if let Err(reason) = durable {
                                report.uncorrectable_blocks += 1;
                                report.errors.push(ScrubError::UncorrectableBlock {
                                    table_id: self.id(),
                                    path: self.path.to_path_buf(),
                                    block_offset,
                                    reason: alloc::format!("in-place parity rebuild {reason}"),
                                });
                                continue;
                            }
                            report.blocks_healed_in_place += 1;
                        }
                        Err(()) => {
                            report.uncorrectable_blocks += 1;
                            report.errors.push(ScrubError::UncorrectableBlock {
                                table_id: self.id(),
                                path: self.path.to_path_buf(),
                                block_offset,
                                reason: alloc::string::String::from(
                                    "in-place heal: parity trailer unverifiable on a \
                                     checksum-clean block",
                                ),
                            });
                        }
                    }
                }
                // Recovered from parity: persist the corrected frame in place.
                Ok(crate::table::util::BlockScrubOutcome::Corrected { .. }) => {
                    // Apply ONLY corrections the prediction pass attested. A block
                    // that recovers now but was not in the predicted set (a fault
                    // that appeared after the prediction) is left corrupt: writing
                    // it would heal bytes the marker's digest never covered, so a
                    // later checkpoint could snapshot them under the stale digest.
                    // It stays RS-correctable, so the next patrol retries.
                    if !predicted_offsets.contains(&block_offset) {
                        continue;
                    }
                    let frame = match crate::table::block::Block::heal_frame(
                        file.as_ref(),
                        handle,
                        &transform,
                    ) {
                        Ok(Some((frame, _kind))) => frame,
                        // The scrub read corrected a fault but the confirming
                        // re-read is already clean: a TRANSIENT fault the first
                        // read hit and the re-read did not. This mirrors the
                        // schedule path (`maybe_record_persistent_heal`), which
                        // treats a clean confirmation re-read as transient and
                        // does not act — there is nothing on disk to persist.
                        Ok(None) => {
                            log::debug!(
                                "in-place heal: transient correction on block at offset \
                                 {block_offset} in table {} at {}; re-read clean, nothing to \
                                 persist",
                                self.id(),
                                self.path.display(),
                            );
                            continue;
                        }
                        // A real read/decode error on the re-read: surface it
                        // rather than silently skip the write-back.
                        Err(e) => {
                            report.uncorrectable_blocks += 1;
                            report.errors.push(ScrubError::UncorrectableBlock {
                                table_id: self.id(),
                                path: self.path.to_path_buf(),
                                block_offset,
                                reason: alloc::format!(
                                    "in-place heal: block scrubbed as corrected but the heal \
                                     re-read failed: {e:?}"
                                ),
                            });
                            continue;
                        }
                    };
                    // Attribution + the crash-recovery marker were captured UP
                    // FRONT (before this loop). First write: make sure no
                    // checkpoint link shares the inode (lazy detach).
                    if let Err(reason) =
                        self.ensure_unshared_for_write(&mut file, &mut unshare_state, sync_mode)
                    {
                        report.uncorrectable_blocks += 1;
                        report.errors.push(ScrubError::UncorrectableBlock {
                            table_id: self.id(),
                            path: self.path.to_path_buf(),
                            block_offset,
                            reason: alloc::format!("unshare hard-linked SST for heal: {reason}"),
                        });
                        continue;
                    }
                    // Seek + write (std::io) and sync (crate::io) carry different
                    // error types, so each is handled separately; both render to
                    // text for the finding. `sync_data` (not `sync_all`): the file
                    // size is unchanged, so only the data needs flushing, and it
                    // must land before the next block so a crash leaves the block
                    // in its prior, still-RS-correctable state.
                    let write_back = file
                        .seek(SeekFrom::Start(block_offset))
                        .and_then(|_| file.write_all(&frame));
                    // The write_all may have written some bytes even if it then
                    // errored (a partial write); mark the file as possibly mutated
                    // so a later failure does not drop the attestation.
                    write_attempted = true;
                    let durable = match write_back {
                        Ok(()) => file
                            .sync_data_with(sync_mode)
                            .map_err(|e| alloc::format!("sync: {e}")),
                        Err(e) => Err(alloc::format!("write: {e}")),
                    };
                    if let Err(reason) = durable {
                        report.uncorrectable_blocks += 1;
                        log::error!(
                            "in-place heal: write-back failed for block at offset \
                             {block_offset} in table {} at {}: {reason}",
                            self.id(),
                            self.path.display(),
                        );
                        report.errors.push(ScrubError::UncorrectableBlock {
                            table_id: self.id(),
                            path: self.path.to_path_buf(),
                            block_offset,
                            reason: alloc::format!("in-place heal {reason}"),
                        });
                        continue;
                    }
                    report.corrections_applied += 1;
                    report.blocks_healed_in_place += 1;
                }
                Err(e) => {
                    report.uncorrectable_blocks += 1;
                    log::error!(
                        "in-place heal: uncorrectable block at offset {block_offset} in table \
                         {} at {}: {e:?}",
                        self.id(),
                        self.path.display(),
                    );
                    report.errors.push(ScrubError::UncorrectableBlock {
                        table_id: self.id(),
                        path: self.path.to_path_buf(),
                        block_offset,
                        reason: alloc::format!("{e:?}"),
                    });
                }
            }
        }

        // The in-progress marker is written speculatively before the first
        // block write; if NO block was actually healed (every candidate turned
        // out uncorrectable) AND no write was ever attempted, the file is
        // unchanged and the marker attests to a heal that never happened. Remove
        // it so it cannot later authorize an unrelated digest mismatch (its
        // `pre == manifest` binding does not expire on its own). But when a
        // write WAS attempted and then failed (a partial write, or a full write
        // whose sync failed), the on-disk bytes may already differ from the
        // manifest digest: KEEP the marker so a later patrol can still attribute
        // and refresh the altered table rather than stranding it.
        let healed = pre_heal_matched == Some(true);
        if healed && report.blocks_healed_in_place == 0 && !write_attempted {
            crate::scrub::heal_attest::remove(&*self.fs, &self.path);
        }

        (report, healed)
    }

    /// The correction the in-place heal would apply to one data block, computed
    /// WITHOUT writing. [`Self::predict_heal_digest_and_offsets`] calls this to
    /// PREDICT the post-heal digest and the offsets to touch; the write loop in
    /// [`Self::heal_data_blocks_in_place`] performs the same decision inline and
    /// applies it. The heal is deterministic (RS recovery / parity rebuild over
    /// the same pre-heal bytes), so the prediction and the later application
    /// are byte-identical. This MIRRORS the write loop's arms; a drift only
    /// weakens crash recovery (a predicted digest that no longer matches the
    /// applied bytes fails the marker CLOSED, never authorizing wrong bytes),
    /// it is never a correctness hazard.
    ///
    /// # Errors
    ///
    /// Propagates a TRANSIENT read failure (the confirming block / frame re-read)
    /// rather than mapping it to "no correction": a swallowed read would omit the
    /// block from the prediction's offset set, and the write loop's gate would then
    /// skip a correction it re-discovers, reporting a clean pass over known damage.
    #[cfg(feature = "page_ecc")]
    fn heal_correction_for_block(
        &self,
        file: &dyn crate::fs::FsFile,
        keyed: &KeyedBlockHandle,
        transform: &crate::table::block::BlockTransform<'_>,
    ) -> crate::Result<Option<(u64, alloc::vec::Vec<u8>)>> {
        use crate::coding::Decode;
        let block_offset = keyed.offset().0;
        let handle = BlockHandle::new(keyed.offset(), keyed.size());
        let outcome = crate::table::util::scrub_block(
            self.global_id(),
            &self.path,
            &self.file_accessor,
            &handle,
            self.data_block_role(),
            self.metadata.data_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
            None,
            #[cfg(feature = "metrics")]
            &self.metrics,
        );
        match outcome {
            Ok(crate::table::util::BlockScrubOutcome::Clean) => {
                // A clean-payload block still needs a parity-trailer rebuild if its
                // trailer rotted. The confirming re-read PROPAGATES on a transient
                // failure (see the doc), but a decode / structural inconsistency
                // leaves the bytes unchanged (`Ok(None)`), matching the write loop's
                // report-and-skip.
                let raw = crate::file::read_exact(file, block_offset, keyed.size() as usize)?;
                let Ok(raw_header) = crate::table::block::Header::decode_from(&mut &raw[..]) else {
                    return Ok(None);
                };
                if raw_header.block_type != self.data_block_role() {
                    return Ok(None);
                }
                let header_len = crate::table::block::Header::header_len(raw_header.block_type);
                let payload_ok = header_len
                    .checked_add(raw_header.data_length as usize)
                    .and_then(|payload_end| raw.get(header_len..payload_end))
                    .is_some_and(|payload| {
                        crate::hash::hash128(payload) == raw_header.checksum.into_u128()
                    });
                if !payload_ok {
                    return Ok(None);
                }
                match self.raw_block_parity_delta(&raw, &raw_header) {
                    Ok(Some(fresh)) => {
                        let trailer_offset = block_offset
                            + crate::table::block::Header::header_len(raw_header.block_type) as u64
                            + u64::from(raw_header.data_length);
                        Ok(Some((trailer_offset, fresh)))
                    }
                    // Trailer matches (no ECC / already fresh) or is
                    // unverifiable: nothing to persist.
                    Ok(None) | Err(()) => Ok(None),
                }
            }
            Ok(crate::table::util::BlockScrubOutcome::Corrected { .. }) => {
                match crate::table::block::Block::heal_frame(file, handle, transform) {
                    Ok(Some((frame, _kind))) => Ok(Some((block_offset, frame))),
                    // A confirming re-read that is now clean: a transient fault the
                    // first read hit and this one did not, so nothing to persist.
                    Ok(None) => Ok(None),
                    // A real read / decode error on the confirming read PROPAGATES:
                    // mapping it to "no correction" would let the write loop's gate
                    // skip a correction it re-discovers and report a clean pass.
                    Err(e) => Err(e),
                }
            }
            // A TRANSIENT read (Io) during this prediction PROPAGATES: swallowing
            // it drops the block from the predicted offset set, so the write pass
            // would skip a correction it re-discovers on an ECC-correctable block
            // and report a clean pass over the fault still on disk. An
            // UNCORRECTABLE / structural failure leaves the bytes as they are (the
            // scrub already recorded the finding and the write pass re-surfaces
            // it), so there is no correction to predict.
            Err(e @ crate::Error::Io(_)) => Err(e),
            Err(_) => Ok(None),
        }
    }

    /// Read-only pass predicting the post-heal whole-file digest AND the set of
    /// write offsets the heal will touch, WITHOUT materializing every corrected
    /// frame. The predicted digest is streamed: each correction (see
    /// [`Self::heal_correction_for_block`]) is spliced into a running hash and
    /// then DROPPED, so a broadly damaged multi-gigabyte table costs only one
    /// correction frame of heap at a time instead of the whole repaired file.
    ///
    /// The returned offset set is what the write loop gates on: it applies ONLY
    /// corrections whose offset was predicted here, so a fault that appears
    /// AFTER this pass (transient rot, a byte flipped between the two reads) is
    /// left unwritten rather than healed under a digest that never attested it.
    ///
    /// The streamed digest is byte-identical to
    /// [`crate::repair::compute_table_checksum_with_overrides`] fed the same
    /// corrections: the file is hashed from `heal_start` to EOF with each
    /// size-preserving correction substituted at its offset. Corrections are
    /// non-overlapping and strictly increasing (block index order; one
    /// correction per block, within that block), so a single forward pass with a
    /// discard-the-replaced-bytes cursor reproduces exactly that byte stream.
    /// Skips a restricted view's punched prefix exactly as the write loop does.
    ///
    /// # Errors
    ///
    /// Any I/O error opening or streaming the file.
    #[cfg(feature = "page_ecc")]
    fn predict_heal_digest_and_offsets(
        &self,
        file: &dyn crate::fs::FsFile,
        transform: &crate::table::block::BlockTransform<'_>,
        heal_start: u64,
    ) -> crate::Result<(u128, crate::HashSet<u64>)> {
        use std::io::{Read, Seek, SeekFrom};

        let mut hasher = xxhash_rust::xxh3::Xxh3Default::new();
        let mut offsets = crate::HashSet::default();
        // A dedicated sequential reader for the digest stream: it walks forward
        // from `heal_start` while `file` still serves the per-block scrub reads.
        let mut rdr = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        rdr.seek(SeekFrom::Start(heal_start))?;
        let mut buf = alloc::vec![0u8; 256 * 1024];
        // Absolute file position the sequential reader has consumed up to.
        let mut pos = heal_start;

        // Reads exactly `count` bytes from `rdr`, hashing them when `hash` is set
        // and discarding them (the region a correction replaces) otherwise.
        let consume = |rdr: &mut alloc::boxed::Box<dyn crate::fs::FsFile>,
                       hasher: &mut xxhash_rust::xxh3::Xxh3Default,
                       buf: &mut [u8],
                       mut count: u64,
                       hash: bool|
         -> crate::Result<()> {
            while count > 0 {
                let want = usize::try_from(count.min(buf.len() as u64)).unwrap_or(buf.len());
                let Some(window) = buf.get_mut(..want) else {
                    break;
                };
                rdr.read_exact(window)?;
                if hash {
                    hasher.update(window);
                }
                count -= want as u64;
            }
            Ok(())
        };

        for entry in self.block_index.iter() {
            // Propagate a transient index-read failure rather than `break`: a
            // truncated prediction would return a digest and an offset set that
            // omit every later block, and the write loop's `predicted_offsets`
            // guard would then silently skip any correctable fault it finds there,
            // reporting a clean heal while leaving known damage on disk. Aborting
            // makes the patrol report the failed pass and retry.
            let keyed = entry?;
            if let Some(bound) = &self.1
                && self.comparator.compare(keyed.end_key(), bound) == core::cmp::Ordering::Less
            {
                continue;
            }
            let Some((write_offset, bytes)) =
                self.heal_correction_for_block(file, &keyed, transform)?
            else {
                continue;
            };
            // Corrections are strictly increasing and non-overlapping; a
            // regression would mean overlapping splices, so bail rather than
            // underflow the gap.
            let Some(gap) = write_offset.checked_sub(pos) else {
                break;
            };
            consume(&mut rdr, &mut hasher, &mut buf, gap, true)?;
            hasher.update(&bytes);
            let replaced = bytes.len() as u64;
            consume(&mut rdr, &mut hasher, &mut buf, replaced, false)?;
            pos = write_offset + replaced;
            offsets.insert(write_offset);
            // `bytes` is dropped here: only its offset is retained.
        }

        // Hash the untouched tail from the last correction to EOF.
        loop {
            let n = rdr.read(&mut buf)?;
            if n == 0 {
                break;
            }
            let Some(window) = buf.get(..n) else { break };
            hasher.update(window);
        }

        Ok((hasher.digest128(), offsets))
    }

    /// Whether the file's CURRENT digest equals `manifest_checksum` — the
    /// attribution probe [`Self::heal_data_blocks_in_place`] takes right
    /// before its first write-back. The caller supplies the CURRENT
    /// manifest digest (read under the per-table heal lock), not this
    /// view's snapshot: a concurrent patrol may have refreshed the manifest
    /// after this view was captured, and comparing against the stale
    /// snapshot would mark a legitimate heal unattributable.
    ///
    /// # Errors
    ///
    /// A failed digest read PROPAGATES rather than grading `false`: the caller
    /// must abort the heal on a probe failure, because proceeding would write
    /// corrections with no completed attestation, and if the manifest
    /// legitimately described the pre-heal bytes the healed digest would then be
    /// permanently unreconcilable.
    #[cfg(feature = "page_ecc")]
    fn pre_heal_digest_matches(&self, manifest_checksum: Checksum) -> crate::Result<bool> {
        // Restriction-aware: a restricted view's manifest digest covers only its
        // live suffix, so probe the same region.
        Ok(self.live_region_checksum()? == manifest_checksum)
    }

    /// Whether the ON-DISK file carries deletion metadata the digest
    /// reconciliation cannot semantically authenticate: a `range_tombstones`
    /// or `delete_bitmap` section. These are AUTHORITATIVE — nothing in-file
    /// can re-derive which rows or ranges were genuinely deleted — so unlike
    /// the derived sections (zone map, seqno bounds, locator, filter) a
    /// re-stamped payload has no cross-check. Read from the file's TOC, not
    /// the recover-time regions, for the same distrust-of-memory reason as
    /// the other gates.
    ///
    /// # Errors
    ///
    /// Any I/O / decode error from reading the SFA trailer.
    #[cfg(feature = "std")]
    pub(crate) fn has_deletion_metadata(&self) -> crate::Result<bool> {
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        Ok(regions.range_tombstones.is_some() || regions.delete_bitmap.is_some())
    }

    /// Scrub: verifies the per-KV checksum footer of every data block in this
    /// table, decoding each block and recomputing each entry's logical-content
    /// digest.
    ///
    /// Footer presence is a per-SST property read from the descriptor
    /// (`metadata.kv_checksum_algo`), not a per-block header flag — SST data
    /// blocks omit the `block_flags` byte. When the descriptor reports no
    /// footers the whole scrub is a no-op; otherwise every data block is
    /// verified under the descriptor's algorithm. This is the paranoid /
    /// offline integrity path — the live read path does NOT verify per-entry
    /// digests (the block-level checksum already covers the on-disk bytes).
    /// Stops and returns on the first detected mismatch.
    ///
    /// # Errors
    ///
    /// - [`crate::Error::ChecksumMismatch`] if any entry's recomputed digest
    ///   disagrees with the stored value (corruption of the entry bytes or
    ///   the stored digest).
    /// - Any I/O / decode error encountered while loading a block.
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "core+alloc per-KV scrub over the table; the verify/scrub consumer is std-gated, so unused under no_std"
        )
    )]
    pub(crate) fn verify_kv_checksums(&self) -> crate::Result<()> {
        // Footer presence is a per-SST property recorded in the descriptor
        // (`kv_checksum_algo`); data blocks omit the block_flags byte, so the
        // descriptor is the authoritative source. When it reports no footers,
        // there is nothing to scrub.
        let Some(expected_algo) = self.metadata.kv_checksum_algo else {
            return Ok(());
        };

        // Descriptor declares this SST footer-bearing, and an SST is
        // homogeneous — every data block carries a footer under `expected_algo`.
        // A restricted view's punched prefix blocks are dead and read as zeros,
        // so skip them (only the live suffix is footer-verified).
        let punch = self.punch_offset()?;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            // Load the RAW block (footer intact) — do NOT route through
            // `load_data_block`, which strips the footer via `from_loaded` —
            // and DISK-FRESH: a pristine copy cached before an on-disk
            // re-stamp would otherwise pass this gate for a file whose
            // stale footer no longer matches its altered value bytes.
            let block = self.load_block_from_disk(&block_handle, BlockType::Data)?;
            DataBlock::verify_kv_checked(
                &block.data,
                block.header,
                self.comparator.clone(),
                Some(expected_algo),
            )?;
        }
        Ok(())
    }

    /// Cross-checks the recorded `linked_blob_files` section against the
    /// COMPLETE accounting derived from this table's indirection entries
    /// (per blob id: reference count, logical bytes, on-disk bytes). The
    /// section carries NO per-section checksum, so structurally valid rot —
    /// a flipped id byte OR a flipped counter byte — is invisible to the
    /// out-of-band walk; the entries themselves are the only integrity
    /// source, and the counters feed fragmentation math (a forged total can
    /// make a blob file look dead while another table still references it).
    /// Duplicate records for one id are rejected too. A no-op (`Ok`) for
    /// tables without the section.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the recorded records disagree
    /// with the derived ones; any I/O / decode error from the full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_blob_links(&self) -> crate::Result<()> {
        use crate::coding::Decode;
        use alloc::collections::BTreeMap;

        // A restricted view cannot be blob-link-cross-checked: the recorded
        // `linked_blob_files` section aggregates the WHOLE table's indirections
        // (per-blob-id counts, not per-block), but its punched prefix is
        // unscannable, so the derived accounting can only cover the live suffix
        // and would never match. This gate exists to stop a heal from laundering
        // a re-stamped section, but an in-place heal rewrites DATA blocks only
        // (it never touches `linked_blob_files`), so skipping it here forfeits no
        // heal-attribution guarantee for a restricted table.
        if self.restrict_lower_bound().is_some() {
            return Ok(());
        }

        // Derive the indirection accounting FIRST, even when the section is
        // absent: a table that still carries `ValueHandle` indirections but
        // advertises no `linked_blob_files` section must NOT pass. Blob GC
        // consults `list_blob_file_references()` to decide whether other tables
        // reference a blob, so an accepted table with hidden indirections lets
        // GC rewrite / drop a blob file this table still points into.
        // (len, bytes, on_disk_bytes) per blob id, accumulated exactly the
        // way the writer folds them from indirections.
        let mut derived: BTreeMap<crate::vlog::BlobFileId, (usize, u64, u64)> = BTreeMap::new();
        for kv in self.scan()? {
            let kv = kv?;
            if kv.key.value_type == crate::ValueType::Indirection {
                let mut cursor = &kv.value[..];
                let ind = crate::blob_tree::handle::BlobIndirection::decode_from(&mut cursor)?;
                let slot = derived.entry(ind.vhandle.blob_file_id).or_insert((0, 0, 0));
                slot.0 += 1;
                slot.1 += u64::from(ind.size);
                slot.2 += u64::from(ind.vhandle.on_disk_size);
            }
        }
        let Some(recorded) = self.list_blob_file_references()? else {
            // No section is valid ONLY for a table with no indirections; a
            // non-empty derived map with no recorded section is a dropped /
            // renamed section hiding live blob references.
            if !derived.is_empty() {
                return Err(crate::Error::InvalidHeader(
                    "table carries indirection entries but no linked_blob_files section",
                ));
            }
            return Ok(());
        };
        // The writer OMITS the linked_blob_files section when there are no blob
        // references, so a PRESENT but empty (zero-count) section is a forgery,
        // not a legitimate no-op: e.g. a delete_bitmap replaced by a four-byte
        // zero-count linked_blob_files. Both `derived` and `recorded_map` would
        // then be empty and the equality check below would pass, keeping the
        // table after its deletion metadata vanished. Reject it here.
        if recorded.is_empty() {
            return Err(crate::Error::InvalidHeader(
                "linked_blob_files section is present but records no blob references",
            ));
        }
        let mut recorded_map: BTreeMap<crate::vlog::BlobFileId, (usize, u64, u64)> =
            BTreeMap::new();
        for link in &recorded {
            if recorded_map
                .insert(
                    link.blob_file_id,
                    (link.len, link.bytes, link.on_disk_bytes),
                )
                .is_some()
            {
                return Err(crate::Error::InvalidHeader(
                    "linked_blob_files carries duplicate records for one blob id",
                ));
            }
        }
        if derived != recorded_map {
            return Err(crate::Error::InvalidHeader(
                "linked_blob_files disagrees with the table's indirection entries",
            ));
        }
        Ok(())
    }

    /// Compares the two DECODED TLI mirrors (head `tli` vs `tli_tail`) and
    /// validates the decoded handle list against the PHYSICAL section
    /// layout. Each copy is independently checksum- (and parity-)consistent,
    /// so a forged tail that encodes a DIFFERENT handle list passes every
    /// byte-level check — and BOTH mirrors forged to the SAME list pass the
    /// equality comparison too, since two forged copies prove nothing about
    /// the data blocks. The writer emits blocks strictly back-to-back, so
    /// the decoded handles must exactly TILE their section: in full-index
    /// mode the handles tile the `data` section; in partitioned mode the TLI
    /// handles tile the `index` section and the partitions' concatenated
    /// data handles tile the `data` section. An omitted, redirected, or
    /// duplicated handle leaves a gap or an overlap.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the decoded mirrors differ or
    /// the handle list does not tile the physical sections; any I/O / decode
    /// error while loading either copy (an unreadable mirror is equally
    /// untrustworthy for the callers' restamp / keep decisions).
    #[cfg(feature = "std")]
    pub(crate) fn verify_tli_mirrors(&self) -> crate::Result<()> {
        self.verify_tli_mirrors_inner(true)
    }

    /// Whether the index STRUCTURE alone authenticates its offsets as original
    /// block boundaries: the mirrors agree, the binary-index pointers verify,
    /// the partition separators are consistent, and the handles TILE their
    /// section. This is the block-boundary provenance the salvage walk needs to
    /// trust an indexed offset without re-reading each (possibly corrupt) data
    /// block — [`verify_tli_mirrors`](Self::verify_tli_mirrors) additionally
    /// frames and decodes every data block, which a bit-rotted-but-index-intact
    /// table (the salvage case) would fail spuriously.
    ///
    /// # Errors
    ///
    /// Propagates only a TRANSIENT [`crate::Error::Io`] from opening / reading
    /// the index mirrors: folding a flaky read into `false` would make the
    /// salvage walk fall back to physical-chain provenance and surrender every
    /// block past the first header break, dropping otherwise healthy keys the
    /// intact TLI could have anchored on retry. `Ok(false)` covers a STRUCTURAL
    /// authentication failure (mirrors disagree, pointers do not verify, handles
    /// do not tile) AND a PERSISTENT read failure of one mirror: both mean the
    /// index cannot be trusted here, so the walk should fall back to the readable
    /// data section rather than abort and recover nothing.
    #[cfg(feature = "std")]
    pub(crate) fn tli_structure_authenticated(&self) -> crate::Result<bool> {
        match self.verify_tli_mirrors_inner(false) {
            Ok(()) => Ok(true),
            // Only a TRANSIENT read propagates (so the salvage walk aborts for a
            // retry rather than surrendering an intact index to a flaky read). A
            // PERSISTENT mirror failure (a bad-sector `UnexpectedEof` on one
            // mirror while the other and the data section stay readable) is
            // untrusted input, not a reason to abort: fold it into `false` so the
            // walk falls back to physical-chain provenance and still recovers the
            // readable data blocks.
            Err(crate::Error::Io(e)) if e.kind().is_transient() => Err(crate::Error::Io(e)),
            Err(_) => Ok(false),
        }
    }

    /// Shared body. `frame_blocks` additionally frames and decodes each
    /// addressed DATA block (the full integrity check the scrub reconcile
    /// wants); with it `false` only the index structure is authenticated, for
    /// the salvage walk that runs over corrupt data blocks by design.
    #[cfg(feature = "std")]
    fn verify_tli_mirrors_inner(&self, frame_blocks: bool) -> crate::Result<()> {
        use crate::table::block::ParsedItem as _;

        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        let head = Self::read_tli_at(
            &*file,
            regions.tli,
            self.metadata.id,
            self.metadata.index_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
        )?;
        if let Some(tail_handle) = regions.tli_tail {
            let tail = Self::read_tli_at(
                &*file,
                tail_handle,
                self.metadata.id,
                self.metadata.index_block_compression,
                self.encryption.as_deref(),
                self.metadata.ecc_params,
            )?;
            if head.as_slice() != tail.as_slice() {
                return Err(crate::Error::InvalidHeader(
                    "tli_tail decodes to a different handle list than the head tli",
                ));
            }
        }

        // Structural coverage: the decoded handles must tile their section.
        let data_section = trailer
            .toc()
            .section(b"data")
            .ok_or(crate::Error::InvalidHeader("data section missing"))?;
        head.try_iter(self.comparator.clone())?;
        // The sequential walk below never reads the BINARY INDEX pointers,
        // so a pointer redirected to another restart head passes mirror
        // equality, tiling, and every separator check — yet the seek path
        // trusts it after reopen. Authenticate the pointers against the
        // sequentially derived restart heads.
        head.verify_binary_index()?;
        let keyed: alloc::vec::Vec<KeyedBlockHandle> = head
            .iter(self.comparator.clone())
            .map(|i| i.materialize(head.as_slice()))
            .collect();
        let handles: alloc::vec::Vec<BlockHandle> = keyed.iter().map(|k| *k.as_ref()).collect();
        if let Some(index_handle) = regions.index {
            // Partitioned: the TLI addresses index partitions inside the
            // `index` section; the partitions address the data blocks.
            let index_section = trailer
                .toc()
                .section(b"index")
                .ok_or(crate::Error::InvalidHeader("index section missing"))?;
            let _ = index_handle;
            if !Self::frames_tile_section(&handles, index_section.pos(), index_section.len()) {
                return Err(crate::Error::InvalidHeader(
                    "tli handles do not tile the index section",
                ));
            }
            self.verify_handles_frame_blocks(&handles, index_section.pos(), index_section.len())?;
            let mut data_handles = alloc::vec::Vec::new();
            for top in &keyed {
                let part = Self::read_tli_at(
                    &*file,
                    *top.as_ref(),
                    self.metadata.id,
                    self.metadata.index_block_compression,
                    self.encryption.as_deref(),
                    self.metadata.ecc_params,
                )?;
                part.try_iter(self.comparator.clone())?;
                // Same pointer authentication as the TLI above: a partition
                // seek trusts its binary index too.
                part.verify_binary_index()?;
                let part_keyed: alloc::vec::Vec<KeyedBlockHandle> = part
                    .iter(self.comparator.clone())
                    .map(|i| i.materialize(part.as_slice()))
                    .collect();
                if frame_blocks {
                    let punch = self.punch_offset()?;
                    for k in &part_keyed {
                        // Punched prefix blocks of a restricted view decode to
                        // zeros; skip their separator cross-check (they are dead).
                        if k.offset().0 < punch {
                            continue;
                        }
                        self.verify_separator_matches_block(k)?;
                    }
                }
                // The TLI's top-level SEPARATOR for this partition must equal the
                // partition's LAST data-block separator. `frames_tile_section`
                // and the per-block separator checks ignore the top-level keys,
                // so a re-stamped partition boundary (in both mirrors) passes
                // them all — yet `TwoLevelBlockIndex::forward_reader` seeks by the
                // top-level separator and would route reads to the wrong
                // partition, skipping the keys the real partition holds.
                let Some(part_last) = part_keyed.last() else {
                    return Err(crate::Error::InvalidHeader(
                        "tli addresses an index partition that decodes to zero entries",
                    ));
                };
                if self
                    .comparator
                    .compare(top.end_key().as_ref(), part_last.end_key().as_ref())
                    != core::cmp::Ordering::Equal
                {
                    return Err(crate::Error::InvalidHeader(
                        "tli separator disagrees with its partition's last separator",
                    ));
                }
                data_handles.extend(part_keyed.iter().map(|k| *k.as_ref()));
            }
            if !Self::frames_tile_section(&data_handles, data_section.pos(), data_section.len()) {
                return Err(crate::Error::InvalidHeader(
                    "index partitions' data handles do not tile the data section",
                ));
            }
            if frame_blocks {
                self.verify_handles_frame_blocks(
                    &data_handles,
                    data_section.pos(),
                    data_section.len(),
                )?;
            }
        } else {
            if !Self::frames_tile_section(&handles, data_section.pos(), data_section.len()) {
                return Err(crate::Error::InvalidHeader(
                    "tli data handles do not tile the data section",
                ));
            }
            if frame_blocks {
                self.verify_handles_frame_blocks(&handles, data_section.pos(), data_section.len())?;
                let punch = self.punch_offset()?;
                for k in &keyed {
                    // Punched prefix blocks of a restricted view decode to zeros;
                    // skip their separator cross-check (they are dead).
                    if k.offset().0 < punch {
                        continue;
                    }
                    self.verify_separator_matches_block(k)?;
                }
            }
        }
        Ok(())
    }

    /// Confirms a leaf index entry's SEPARATOR (its `end_key`) equals the
    /// addressed data block's actual decoded last key. A forged separator
    /// re-stamped to another still-sorted value passes the mirror comparison
    /// and section tiling (both ignore the keys), yet the index binary
    /// search routes keys in `(forged_separator, real_last_key]` to the wrong
    /// block — `point_read` then misses existing keys. The in-memory
    /// reachability probe does not catch this on the heal path (the live
    /// table keeps its correct recovery-time index), so the disk-fresh
    /// separator must be checked against the block's decoded final key here.
    #[cfg(feature = "std")]
    fn verify_separator_matches_block(&self, keyed: &KeyedBlockHandle) -> crate::Result<()> {
        let block_handle = BlockHandle::new(keyed.offset(), keyed.size());
        let entries = self.decode_block_entries(&block_handle)?;
        let Some(last) = entries.last() else {
            return Err(crate::Error::InvalidHeader(
                "tli separator addresses a data block that decodes to zero entries",
            ));
        };
        if self
            .comparator
            .compare(keyed.end_key().as_ref(), last.key.user_key.as_ref())
            != core::cmp::Ordering::Equal
        {
            return Err(crate::Error::InvalidHeader(
                "tli separator does not match the addressed block's decoded last key",
            ));
        }
        Ok(())
    }

    /// Requires every handle's SIZE to equal the physical frame its block's
    /// on-disk header derives (header + payload + parity). The cumulative
    /// tiling check cannot tell ONE handle spanning several back-to-back
    /// blocks from the real per-block layout: the spanned frame still
    /// decodes its FIRST payload (the tail reads as an unrecognized trailer
    /// on a non-ECC block), so the separator cross-check passes too — yet
    /// every later physical block is unreachable through the index and
    /// reads silently miss its keys.
    #[cfg(feature = "std")]
    fn verify_handles_frame_blocks(
        &self,
        handles: &[BlockHandle],
        pos: u64,
        len: u64,
    ) -> crate::Result<()> {
        // checked, not saturating: a re-stamped TOC could overflow `pos + len`,
        // and a saturated `u64::MAX` bound would then accept a forged oversized
        // handle instead of rejecting the corrupt section.
        let section_end = pos
            .checked_add(len)
            .ok_or(crate::Error::InvalidHeader("data section length overflows"))?;
        // A restricted view's punched prefix blocks read as zeros, so their
        // physical frame can no longer be probed. They tile the section by
        // length (punch preserves file size) but are dead, so skip framing
        // them. Index-section handles sit above the (data-section) punch
        // offset and are never skipped.
        let punch = self.punch_offset()?;
        for handle in handles {
            if handle.offset().0 < punch {
                continue;
            }
            let probed = self.probe_block_handle_at(handle.offset().0, section_end)?;
            if probed.size() != handle.size() {
                return Err(crate::Error::InvalidHeader(
                    "an index handle's size disagrees with its block's physical frame",
                ));
            }
        }
        Ok(())
    }

    /// Whether `handles`, in order, exactly tile `[pos, pos + len)`: the
    /// first starts at `pos`, each next starts where the previous ended, and
    /// the last ends at `pos + len`. The writer emits blocks back-to-back,
    /// so any omitted, redirected, or duplicated handle breaks the tiling.
    #[cfg(feature = "std")]
    fn frames_tile_section(handles: &[BlockHandle], pos: u64, len: u64) -> bool {
        let mut at = pos;
        for handle in handles {
            if handle.offset().0 != at {
                return false;
            }
            at = match at.checked_add(u64::from(handle.size())) {
                Some(v) => v,
                None => return false,
            };
        }
        pos.checked_add(len) == Some(at)
    }

    /// Cross-checks the recorded `seqno_bounds` section against the ACTUAL
    /// per-block seqno ranges derived from decoding every data block's
    /// entries. The section's block is checksum-clean to the out-of-band
    /// walk even when its PAYLOAD was re-stamped to another structurally
    /// valid map, and `scan_since_seqno` trusts it to SKIP blocks — a forged
    /// range silently omits a block's live entries from every CDC /
    /// incremental scan. Every recorded block must exist, every data block
    /// must be recorded, and each recorded range must equal the decoded one.
    /// A no-op for tables without the section.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the recorded map disagrees with
    /// the decoded entries; any I/O / decode error from the full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_seqno_bounds(&self) -> crate::Result<()> {
        use crate::table::block::ParsedItem as _;

        // Re-read the section FROM DISK: the in-memory map was loaded at
        // recover time, so an on-disk re-stamp after the open (the very
        // forge this check exists for) would be invisible to it. Unlike the
        // best-effort recover load, an unreadable section here is an error —
        // the callers are deciding whether to trust the file's bytes.
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        let Some(sb_handle) = regions.seqno_bounds else {
            return Ok(());
        };
        let seqno_bounds = {
            let block = Block::from_file(
                &*file,
                sb_handle,
                crate::table::block::BlockIdentity {
                    table_id: self.metadata.id,
                    block_type: BlockType::SeqnoBounds,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
            if block.header.block_type != BlockType::SeqnoBounds {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }
            crate::table::seqno_bounds::SeqnoBoundsMap::decode(&block.data)?
        };
        // No early return on an empty map: a PRESENT-but-empty section on a
        // table with data blocks is a forgery (every writer emits one entry per
        // block). The per-block loop below rejects it — the first block's
        // `bounds_for` returns `None` — while a genuinely empty table (no data
        // blocks) still passes, since the loop runs zero times and `checked`
        // equals the empty map's length.
        // Skip a restricted view's punched (dead) prefix blocks; count only the
        // LIVE seqno_bounds entries below so the cross-check covers the suffix.
        let punch = self.punch_offset()?;
        let mut checked = 0usize;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            let Some(recorded) = seqno_bounds.bounds_for(block_handle.offset().0) else {
                return Err(crate::Error::InvalidHeader(
                    "seqno_bounds is missing a data block's entry",
                ));
            };
            let derived = {
                #[cfg(feature = "columnar")]
                if self.metadata.columnar {
                    let block = self.load_block_from_disk(&block_handle, BlockType::Columnar)?;
                    let batch = crate::table::columnar::ColumnBatch::decode(&block.data)?;
                    let entries = crate::table::columnar::column_batch_to_entries(&batch)?;
                    let mut seqnos = entries.iter().map(|e| e.key.seqno);
                    let Some(first) = seqnos.next() else {
                        return Err(crate::Error::InvalidHeader(
                            "columnar data block decodes to zero rows",
                        ));
                    };
                    Some(seqnos.fold((first, first), |(lo, hi), s| (lo.min(s), hi.max(s))))
                } else {
                    None
                }
                #[cfg(not(feature = "columnar"))]
                None::<(SeqNo, SeqNo)>
            };
            let derived = if let Some(d) = derived {
                d
            } else {
                let block = self.load_block_from_disk(&block_handle, BlockType::Data)?;
                let data_block =
                    DataBlock::from_loaded(block, self.metadata.kv_checksum_algo.is_some())?;
                let mut seqnos = data_block
                    .try_iter(self.comparator.clone())?
                    .map(|p| p.seqno());
                let Some(first) = seqnos.next() else {
                    return Err(crate::Error::InvalidHeader(
                        "row data block decodes to zero entries",
                    ));
                };
                seqnos.fold((first, first), |(lo, hi), s| (lo.min(s), hi.max(s)))
            };
            if derived != recorded {
                return Err(crate::Error::InvalidHeader(
                    "seqno_bounds disagrees with the block's decoded entries",
                ));
            }
            checked = checked
                .checked_add(1)
                .ok_or(crate::Error::InvalidHeader("seqno_bounds"))?;
        }
        // Every recorded entry matched some walked block (offsets are unique
        // on both sides), so equal counts mean the map records EXACTLY the
        // table's blocks — a forged extra entry cannot hide among them.
        if checked != seqno_bounds.live_len(punch) {
            return Err(crate::Error::InvalidHeader(
                "seqno_bounds carries entries for blocks the index does not hold",
            ));
        }
        Ok(())
    }

    /// Fully DECODES every data block and checks its entry count against the
    /// trailer's declared item count. The out-of-band walk verifies only the
    /// outer frame, and `verify_kv_checksums` is a no-op for a footer-less
    /// SST, so a checksum- and parity-consistent block with a valid prefix
    /// followed by a malformed entry is otherwise graded clean: the entry
    /// decoder turns a mid-stream parse failure into an ordinary end of
    /// iteration, so a later scan silently omits the malformed tail. Decoding
    /// to fewer (or more) entries than the trailer declares is corruption.
    /// Covers both row-major and columnar blocks.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when a block decodes to a different
    /// entry count than its trailer declares; any I/O / decode error from the
    /// full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_block_entry_counts(&self) -> crate::Result<()> {
        // A restricted view's punched prefix blocks are dead (read as zeros);
        // only its live suffix blocks are entry-count-verified.
        let punch = self.punch_offset()?;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            #[cfg(feature = "columnar")]
            if self.metadata.columnar {
                let block = self.load_block_from_disk(&block_handle, BlockType::Columnar)?;
                // `column_batch_to_entries` fully materializes the rows, so a
                // malformed batch fails here rather than truncating silently.
                let batch = crate::table::columnar::ColumnBatch::decode(&block.data)?;
                let entries = crate::table::columnar::column_batch_to_entries(&batch)?;
                if entries.len() != batch.row_count as usize {
                    return Err(crate::Error::InvalidHeader(
                        "columnar block decodes to fewer rows than its batch declares",
                    ));
                }
                continue;
            }
            let block = self.load_block_from_disk(&block_handle, BlockType::Data)?;
            let data_block =
                DataBlock::from_loaded(block, self.metadata.kv_checksum_algo.is_some())?;
            let declared = data_block.len();
            let decoded = data_block.try_iter(self.comparator.clone())?.count();
            if decoded != declared {
                return Err(crate::Error::InvalidHeader(
                    "data block decodes to fewer entries than its trailer declares",
                ));
            }
        }
        Ok(())
    }

    /// Cross-checks the recorded `zone_map` section against the ACTUAL
    /// per-block statistics derived from decoding every data block. The
    /// section's block is checksum-clean to the out-of-band walk even when
    /// its PAYLOAD was re-stamped to another structurally valid map, and
    /// `columnar_scan` trusts its min/max to SKIP blocks — a forged range
    /// silently omits matching rows. A row block records one synthetic column
    /// (whole-block key range + row count); a columnar block records one entry
    /// per stored column, re-derived from the decoded batch. Every block must
    /// be recorded and its recorded stats must equal the decoded ones. A no-op
    /// for tables without the section.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the recorded map disagrees with
    /// the decoded blocks; any I/O / decode error from the full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_zone_map(&self) -> crate::Result<()> {
        // Re-read the section FROM DISK: the in-memory map is best-effort at
        // recover time, so an on-disk re-stamp after the open is invisible to
        // it. An unreadable section is an error here (the caller is deciding
        // whether to trust the file's bytes), unlike the best-effort load.
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        let Some(zm_handle) = regions.zone_map else {
            return Ok(());
        };
        let zone_map = {
            let block = Block::from_file(
                &*file,
                zm_handle,
                crate::table::block::BlockIdentity {
                    table_id: self.metadata.id,
                    block_type: BlockType::ZoneMap,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
            if block.header.block_type != BlockType::ZoneMap {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }
            crate::table::zone_map::ZoneMap::decode(&block.data)?
        };
        // The writer emits one zone-map entry per data block whenever the
        // section exists, so a PRESENT-but-empty map on a table with data blocks
        // is a forgery — e.g. a delete_bitmap relabeled and re-roled to an empty
        // zone_map, which drops the deletion metadata while every semantic gate
        // (tiling, block role, parsed deletion state) still passes. Reject it,
        // as the other rebuildable-section checks do.
        if zone_map.is_empty() {
            if self.block_index.iter().next().is_some() {
                return Err(crate::Error::InvalidHeader(
                    "zone_map section is present but empty on a table with data blocks",
                ));
            }
            return Ok(());
        }
        // A restricted view's punched prefix blocks are DEAD (never read), and
        // the zone_map still carries their entries (the file is punched, not
        // rewritten). Skip those blocks and count only the LIVE zone_map entries
        // below, so the cross-check authenticates exactly the suffix.
        let punch = self.punch_offset()?;
        let mut checked = 0usize;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            let Some(recorded) = zone_map.columns_for(block_handle.offset().0) else {
                return Err(crate::Error::InvalidHeader(
                    "zone_map is missing a data block's entry",
                ));
            };
            // Authenticate the recorded entry against the block's ACTUAL decoded
            // statistics. A columnar block records one entry per stored column,
            // re-derived here by the SAME function the writer used, so any
            // divergence (a re-stamped range, an added / dropped column, a
            // flipped id) is a forgery. A row block records a single synthetic
            // whole-block key range (checked in `verify_row_block_zone_entry`).
            #[cfg(feature = "columnar")]
            if self.metadata.columnar {
                let block = self.load_block_from_disk(&block_handle, BlockType::Columnar)?;
                let batch = crate::table::columnar::ColumnBatch::decode(&block.data)?;
                if batch.row_count == 0 {
                    return Err(crate::Error::InvalidHeader(
                        "columnar data block decodes to zero rows",
                    ));
                }
                if recorded != batch.zone_stats().as_slice() {
                    return Err(crate::Error::InvalidHeader(
                        "zone_map disagrees with the columnar block's per-column statistics",
                    ));
                }
            } else {
                self.verify_row_block_zone_entry(recorded, &block_handle)?;
            }
            #[cfg(not(feature = "columnar"))]
            self.verify_row_block_zone_entry(recorded, &block_handle)?;

            checked = checked
                .checked_add(1)
                .ok_or(crate::Error::InvalidHeader("zone_map"))?;
        }
        if checked != zone_map.live_len(punch) {
            return Err(crate::Error::InvalidHeader(
                "zone_map carries entries for blocks the index does not hold",
            ));
        }
        Ok(())
    }

    /// Cross-checks a ROW data block's recorded zone-map entry: it must be
    /// exactly one synthetic whole-block column (`column_id == 0`, zero type /
    /// codec / null fields) whose `min` / `max` / `row_count` equal the block's
    /// decoded key range and row count.
    ///
    /// Authenticates the column's IDENTITY, not just its key bounds. A
    /// re-stamped map that keeps the checked min / max / `row_count` but changes
    /// the id to a consumer value-column id would let
    /// [`ColumnRangePredicate::can_skip_block`](crate::table::columnar_predicate::ColumnRangePredicate::can_skip_block)
    /// read those key bounds as value-column statistics and skip blocks holding
    /// matching rows. Split out of [`Self::verify_zone_map`] so the columnar
    /// path can authenticate its per-column entry instead.
    #[cfg(feature = "std")]
    fn verify_row_block_zone_entry(
        &self,
        recorded: &[crate::table::zone_map::ColumnStats],
        block_handle: &BlockHandle,
    ) -> crate::Result<()> {
        let [col] = recorded else {
            return Err(crate::Error::InvalidHeader(
                "zone_map block does not carry exactly one synthetic column",
            ));
        };
        if col.column_id != 0 || col.type_tag != 0 || col.codec_id != 0 || col.null_count != 0 {
            return Err(crate::Error::InvalidHeader(
                "zone_map synthetic column identity disagrees with the \
                 writer's whole-block column (id / type / codec / null)",
            ));
        }
        let (min_key, max_key, rows) = self.zone_stats_from_row_block(block_handle)?;
        if col.min != min_key || col.max != max_key || col.row_count as usize != rows {
            return Err(crate::Error::InvalidHeader(
                "zone_map disagrees with the block's decoded key range or row count",
            ));
        }
        Ok(())
    }

    /// Derives `(min_user_key, max_user_key, row_count)` from a decoded ROW
    /// data block, for the [`Self::verify_zone_map`] cross-check.
    #[cfg(feature = "std")]
    fn zone_stats_from_row_block(
        &self,
        block_handle: &BlockHandle,
    ) -> crate::Result<(Vec<u8>, Vec<u8>, usize)> {
        use crate::table::block::ParsedItem as _;
        let block = self.load_block_from_disk(block_handle, BlockType::Data)?;
        let data_block = DataBlock::from_loaded(block, self.metadata.kv_checksum_algo.is_some())?;
        let entries: Vec<_> = data_block
            .try_iter(self.comparator.clone())?
            .map(|p| p.materialize(data_block.as_slice()))
            .collect();
        let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
            return Err(crate::Error::InvalidHeader(
                "row data block decodes to zero entries",
            ));
        };
        Ok((
            first.key.user_key.to_vec(),
            last.key.user_key.to_vec(),
            entries.len(),
        ))
    }

    /// Publishes this table's tight-space restriction lower bound to its
    /// `.restrict-bound` sidecar, so manifest repair recovers the exact bound
    /// WITHOUT the SST itself being mutated (which would invalidate the whole-file
    /// checksum the manifest still holds for it). MUST be called STRICTLY AFTER the
    /// slice's version install commits and BEFORE the prefix is hole-punched: the
    /// post-commit ordering makes a sidecar on disk always denote a committed
    /// restriction (so repair honors it without a commit protocol), and writing it
    /// before the punch gives every punched input a recoverable exact bound. The
    /// atomic `temp + rename` write leaves the sidecar fully present or absent
    /// across a crash, beside an untouched SST.
    ///
    /// # Errors
    ///
    /// Propagates encryption / filesystem failures from the atomic sidecar write.
    #[cfg(feature = "std")]
    pub(crate) fn write_restrict_sidecar(
        &self,
        bound: &[u8],
        sync_mode: crate::fs::SyncMode,
    ) -> crate::Result<()> {
        crate::restrict_bound::write(
            &*self.fs,
            &self.path,
            self.encryption.as_deref(),
            self.metadata.id,
            bound,
            sync_mode,
        )
    }

    /// Reads one data block's RAW on-disk bytes and reports whether they are all
    /// zero — the signature of a hole-punched (reclaimed) block. Used to
    /// authenticate a restriction against physical punch evidence: a real punch
    /// zeroes the reclaimed prefix, so a genuinely restricted table's below-bound
    /// blocks read as zeros while a forged / stale sidecar over an unpunched file
    /// finds intact data there.
    ///
    /// # Errors
    ///
    /// Propagates the positioned read failure.
    #[cfg(feature = "std")]
    fn block_is_zeroed(&self, block_handle: &BlockHandle) -> crate::Result<bool> {
        let file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let bytes = crate::file::read_exact(
            &*file,
            block_handle.offset().0,
            block_handle.size() as usize,
        )?;
        Ok(bytes.iter().all(|&b| b == 0))
    }

    /// Whether the ENTIRE prefix below `bound` is physically hole-punched (every
    /// data block whose keys all sit below `bound` reads as zeros) — the
    /// independent punch evidence repair requires before trusting a
    /// `.restrict-bound` sidecar.
    ///
    /// Checking only the FIRST below-bound block is unsound: an earlier tight-space
    /// slice may have punched `[0, B1)` while a LATER slice wrote a larger sidecar
    /// bound `B2 > B1` whose `upgrade_version` never committed (crash / install
    /// failure). The blocks in `[B1, B2)` are then still LIVE, yet the first
    /// below-`B2` block (at offset 0) is punched — so a first-block-only check
    /// would accept `B2` and permanently omit the live keys between `B1` and `B2`.
    /// Requiring EVERY below-bound block to be zeroed rejects such a partially
    /// punched extent (any readable below-bound block → `false`), failing closed.
    ///
    /// A real tight-space punch reclaimed `[0, punch_offset)`, so all below-bound
    /// blocks read as zeros; a forged or stale sidecar over an UNPUNCHED file finds
    /// intact data and is rejected. Returns `false` when no block sits entirely
    /// below `bound` (nothing was reclaimable, so a restriction would be spurious).
    ///
    /// # Errors
    ///
    /// Propagates a block-index or positioned-read failure.
    #[cfg(feature = "std")]
    pub(crate) fn prefix_is_punched(&self, bound: &[u8]) -> crate::Result<bool> {
        let mut saw_below_bound = false;
        for handle in self.block_index.iter() {
            let handle = handle?;
            // Blocks are yielded in key order, so once a block's keys reach the
            // bound, no later block sits entirely below it either.
            if self.comparator.compare(handle.end_key(), bound) != core::cmp::Ordering::Less {
                break;
            }
            saw_below_bound = true;
            if !self.block_is_zeroed(&BlockHandle::new(handle.offset(), handle.size()))? {
                // A live block below the claimed bound: the punched extent does not
                // reach it, so the bound is unbacked — fail closed.
                return Ok(false);
            }
        }
        Ok(saw_below_bound)
    }

    /// Whether this table's FIRST data block is hole-punched (reads as zeros).
    /// A tight-space punch always reclaims a `[0, punch_offset)` prefix that
    /// includes the first data block, so this is the punch test for the case
    /// where the sidecar bound itself could not be read: a zeroed first block
    /// means the SST is genuinely punched (bound lost → quarantine), while an
    /// intact first block means an unpunched file carrying a spurious sidecar.
    ///
    /// # Errors
    ///
    /// Propagates a block-index or positioned-read failure.
    #[cfg(feature = "std")]
    pub(crate) fn first_data_block_is_punched(&self) -> crate::Result<bool> {
        match self.block_index.iter().next() {
            Some(handle) => {
                let handle = handle?;
                self.block_is_zeroed(&BlockHandle::new(handle.offset(), handle.size()))
            }
            None => Ok(false),
        }
    }

    /// The conservative restriction bound derived from the PHYSICAL punch alone,
    /// for a punched SST whose exact `.restrict-bound` sidecar is not trustworthy
    /// (lost / corrupt / unbacked) and whose manifest restriction is also gone.
    ///
    /// Walks the data blocks in key order and returns the END key of the FIRST
    /// block that does NOT read as zeros. Restricting to that key drops the
    /// punched (zeroed) prefix AND the first readable block, which may STRADDLE
    /// the true (mid-block) bound: since the end key is that block's maximum, it
    /// is at or above the true bound, so every served key is live and NO
    /// superseded key is resurrected. The cost is at most that one block's live
    /// suffix, which is exactly the trade the resurrection flag governs; the
    /// resurrection path keeps the whole readable region instead.
    ///
    /// `None` when EVERY block reads as zeros: no live data survives the punch, so
    /// the caller excludes the table (no recoverable live data to lose).
    ///
    /// # Errors
    ///
    /// Propagates a block-index or positioned-read failure.
    #[cfg(feature = "std")]
    pub(crate) fn derive_restriction_bound(&self) -> crate::Result<Option<UserKey>> {
        for handle in self.block_index.iter() {
            let handle = handle?;
            if !self.block_is_zeroed(&BlockHandle::new(handle.offset(), handle.size()))? {
                return Ok(Some(handle.end_key().clone()));
            }
        }
        Ok(None)
    }

    /// The GREEDY counterpart of [`derive_restriction_bound`](Self::derive_restriction_bound)
    /// for RESURRECTION mode: returns the FIRST (lowest) key of the first readable
    /// block, so restricting to it keeps the WHOLE straddling block — resurrecting
    /// its sub-bound keys — while still excluding the punched (zeroed) blocks below
    /// it. Returning the punched table UNRESTRICTED instead would route a read to a
    /// zeroed, physically-missing block and fail after a supposedly successful
    /// repair. Wholly-empty readable blocks (e.g. a columnar block fully masked by
    /// its delete bitmap) are skipped. `None` when every block is zeroed or empty.
    ///
    /// # Errors
    ///
    /// Propagates a block-index, positioned-read, or block-decode failure.
    #[cfg(feature = "std")]
    pub(crate) fn derive_resurrection_bound(&self) -> crate::Result<Option<UserKey>> {
        for handle in self.block_index.iter() {
            let handle = handle?;
            let bh = BlockHandle::new(handle.offset(), handle.size());
            if self.block_is_zeroed(&bh)? {
                continue;
            }
            if let Some(db) = self.load_data_block(&bh)?
                && let Some(first) = db.first_user_key(self.comparator.clone())?
            {
                return Ok(Some(first));
            }
        }
        Ok(None)
    }

    /// Cross-checks the recorded `locator` section against the ACTUAL
    /// key → newest-version-block mapping derived from decoding every data
    /// block. A checksum- and parity-consistent forged locator is accepted by
    /// the out-of-band walk on its block role alone, but `point_read_inner`
    /// trusts its answer and reads the addressed block directly: a locator
    /// redirected from a key's newest-version block to a LATER block holding
    /// an OLDER version returns that stale value without falling back to the
    /// sorted index. A correctly-built locator answers every in-table key
    /// with the block holding its newest version (the FIRST block covering
    /// it, since blocks are sorted by key then descending seqno), so any key
    /// whose locator answer points at a different block is corruption. A
    /// no-op for tables without the section.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when a key's locator answer disagrees
    /// with its decoded newest-version block; any I/O / decode error from the
    /// full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_locator(&self) -> crate::Result<()> {
        // Re-read the section FROM DISK and pair it with the ordinal → handle
        // map (the writer's block_id order == index order), mirroring the open
        // path. An unreadable section is an error here (the caller is deciding
        // whether to trust the bytes), unlike the best-effort open load.
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        let Some(loc_handle) = regions.locator else {
            return Ok(());
        };
        let block = Block::from_file(
            &*file,
            loc_handle,
            crate::table::block::BlockIdentity {
                table_id: self.metadata.id,
                block_type: BlockType::Locator,
                dict_id: 0,
                window_log: 0,
            },
            &{
                let t = match self.encryption.as_deref() {
                    Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                    None => crate::table::block::BlockTransform::PLAIN,
                };
                if let Some(ecc) = self.metadata.ecc_params {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        )?;
        if block.header.block_type != BlockType::Locator {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }
        let blocks: Vec<BlockHandle> = self
            .block_index
            .iter()
            .map(|r| r.map(|kbh| *kbh.as_ref()))
            .collect::<crate::Result<Vec<_>>>()?;
        let locator = crate::table::locator::LoadedLocator::new(block.data, blocks);

        // Walk blocks in index (block_id) order; the FIRST time a user key
        // appears is its newest version, so that block is the locator's
        // expected answer. `seen` dedups across blocks (a key's older
        // versions in later blocks must not overwrite the expectation).
        let mut seen: crate::HashSet<Vec<u8>> = crate::HashSet::default();
        // A restricted view's punched prefix blocks are dead; skip them. Their
        // keys are superseded, so a suffix key's newest version is in the suffix,
        // and reads never consult the locator's prefix answers.
        let punch = self.punch_offset()?;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            // Load the disk-fresh row block once so a `Restart` / `Entry`
            // slot hint can be probed against it below. Columnar blocks carry
            // no in-block slot semantics (rows are reconstructed on load), so
            // there the locator is per-block and only the block-id is checked.
            #[cfg(feature = "columnar")]
            let row_block = if self.metadata.columnar {
                None
            } else {
                Some(DataBlock::from_loaded(
                    self.load_block_from_disk(&block_handle, BlockType::Data)?,
                    self.metadata.kv_checksum_algo.is_some(),
                )?)
            };
            #[cfg(not(feature = "columnar"))]
            let row_block = Some(DataBlock::from_loaded(
                self.load_block_from_disk(&block_handle, BlockType::Data)?,
                self.metadata.kv_checksum_algo.is_some(),
            )?);
            // Reuse the already-loaded row block for row tables (its entries
            // are what `decode_block_entries` would re-decode); only the
            // columnar path needs the separate reconstruction decode.
            use crate::table::block::ParsedItem as _;
            let entries: Vec<InternalValue> = match row_block.as_ref() {
                Some(block) => block
                    .try_iter(self.comparator.clone())?
                    .map(|p| p.materialize(block.as_slice()))
                    .collect(),
                None => self.decode_block_entries(&block_handle)?,
            };
            for entry in entries {
                let user_key = entry.key.user_key.to_vec();
                if !seen.insert(user_key.clone()) {
                    continue;
                }
                let key_hash = crate::hash::hash64(&user_key);
                // The writer OMITS the locator section when it cannot build one
                // and otherwise encodes EVERY unique key, so a present locator
                // that gives NO answer for a decoded key is a forgery — e.g. a
                // delete_bitmap relabeled/re-roled to a locator that resolves
                // nothing. The read path falls back to the sorted index on a
                // miss, but a verifier deciding whether to trust the bytes must
                // treat the unanswered key as corrupt (otherwise the relabel
                // records no degradation and the deleted rows come back live).
                let Some((located, hint)) = locator.locate_block(key_hash)? else {
                    return Err(crate::Error::InvalidHeader(
                        "locator gives no answer for a decoded key it should resolve",
                    ));
                };
                if located.offset() != block_handle.offset() {
                    return Err(crate::Error::InvalidHeader(
                        "locator resolves a key to a block other than its newest-version block",
                    ));
                }
                // A `Restart` / `Entry` slot hint sends `point_read_at_slot`
                // straight to that in-block position: a checksum-clean
                // locator can keep the right block id yet redirect the slot
                // to a later restart interval holding an OLDER version of a
                // multi-version key, and the read returns the stale value
                // without falling back to the sorted index. Probe the hint
                // and require the NEWEST version (the first decoded entry for
                // the key, which is `entry` here since `located` == this
                // block). A `None` hint (per-block precision) has no slot to
                // validate.
                if let (Some((slot, is_entry)), Some(block)) = (hint, row_block.as_ref()) {
                    let found = block.point_read_at_slot(
                        slot,
                        is_entry,
                        user_key.as_ref(),
                        crate::seqno::MAX_SEQNO,
                        &self.comparator,
                    )?;
                    let disagrees = match &found {
                        Some(v) => {
                            v.key.seqno != entry.key.seqno
                                || v.key.value_type != entry.key.value_type
                                || v.value != entry.value
                        }
                        None => true,
                    };
                    if disagrees {
                        return Err(crate::Error::InvalidHeader(
                            "locator slot hint does not resolve a key's newest version",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Confirms every decoded key is RETRIEVABLE through its own block's
    /// in-block indexes, judged on the DISK-FRESH bytes. A data block's
    /// embedded HASH INDEX is checksum-clean to the out-of-band walk even
    /// when a bucket was re-stamped to `MARKER_FREE`: the sequential decode
    /// gates still see every entry, but `point_read` trusts the index and
    /// returns `None` for the affected keys. Each block is re-read from the
    /// file and probed through ITS OWN `point_read` — the table-level probe
    /// this replaces went through the recovery-time in-memory index and the
    /// block cache, so a pristine cached copy masked the on-disk forge.
    /// Filter, locator, and TLI misdirection have their own disk-fresh
    /// gates (`verify_filter`, `verify_locator`, `verify_tli_mirrors`).
    /// Columnar blocks carry no in-block key index (rows are reconstructed
    /// on load), so they have nothing to probe.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when a decoded key is not retrievable
    /// from its block; any I/O / decode error from the full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_point_read_reachability(&self) -> crate::Result<()> {
        use crate::table::block::ParsedItem as _;
        // Columnar blocks carry no in-block key index to probe, but the GLOBAL
        // internal-key sort order (user key ASC, then seqno DESC per key) is still
        // an invariant the read path relies on: `column_batch_match_entries`
        // binary-searches the key column assuming it is sorted, so a
        // checksum-restamped block with reordered keys — which every other
        // columnar check (count / zone-bound comparisons) tolerates — would make
        // point reads miss a key or return a stale version. Enforce the order over
        // the live suffix (the punched prefix is dead); there is no point_read to
        // run, so this is the only order gate for a columnar table.
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            let punch = self.punch_offset()?;
            let mut prev_internal: Option<(UserKey, SeqNo)> = None;
            for handle in self.block_index.iter() {
                let handle = handle?;
                let block_handle = BlockHandle::new(handle.offset(), handle.size());
                if block_handle.offset().0 < punch {
                    continue;
                }
                for entry in self.decode_block_entries(&block_handle)? {
                    if let Some((pk, ps)) = &prev_internal {
                        let out_of_order = match self.comparator.compare(&entry.key.user_key, pk) {
                            core::cmp::Ordering::Greater => false,
                            core::cmp::Ordering::Less => true,
                            core::cmp::Ordering::Equal => entry.key.seqno >= *ps,
                        };
                        if out_of_order {
                            return Err(crate::Error::InvalidHeader(
                                "columnar entries are out of order (a user key decreased, or an \
                                 equal key's seqno did not strictly decrease) across the walk",
                            ));
                        }
                    }
                    prev_internal = Some((entry.key.user_key.clone(), entry.key.seqno));
                }
            }
            return Ok(());
        }
        // The internal-key sort order (user key ASC, then seqno DESC per key) is
        // a GLOBAL invariant across the whole table, not just within a block.
        // Carry the last decoded internal key ACROSS block boundaries so a
        // checksum-restamped later block cannot raise the seqno of a key that
        // also ends the preceding block: both blocks would decode and probe
        // cleanly on their own, yet after reopen the index seeks the first block
        // and a later compaction could persist the stale (lower-seqno) version.
        // A restricted view's punched prefix blocks are dead; skip them. The
        // global sort order still holds across the LIVE suffix, so `prev_internal`
        // simply starts at the first live block.
        let punch = self.punch_offset()?;
        let mut prev_internal: Option<(UserKey, SeqNo)> = None;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            let block = self.load_block_from_disk(&block_handle, BlockType::Data)?;
            let data_block =
                DataBlock::from_loaded(block, self.metadata.kv_checksum_algo.is_some())?;
            // A block whose keys all resolve through its HASH index never
            // exercises the binary index in the probes below, yet range
            // seeks still trust it — authenticate the pointers directly.
            data_block.verify_binary_index()?;
            let entries: Vec<InternalValue> = data_block
                .try_iter(self.comparator.clone())?
                .map(|p| p.materialize(data_block.as_slice()))
                .collect();
            // A key's versions are adjacent within the block (sorted by key
            // then descending seqno), so the FIRST occurrence is the newest
            // and one probe per distinct key suffices.
            let mut prev_key: Option<UserKey> = None;
            for entry in entries {
                // Enforce the global sort order on EVERY entry (before the
                // per-key dedup below), tracking the previous entry across block
                // boundaries: the user key must strictly increase, or (for the
                // same user key) the seqno must strictly decrease.
                if let Some((pk, ps)) = &prev_internal {
                    let out_of_order = match self.comparator.compare(&entry.key.user_key, pk) {
                        core::cmp::Ordering::Greater => false,
                        core::cmp::Ordering::Less => true,
                        core::cmp::Ordering::Equal => entry.key.seqno >= *ps,
                    };
                    if out_of_order {
                        return Err(crate::Error::InvalidHeader(
                            "data-block entries are out of order (a user key decreased, or an \
                             equal key's seqno did not strictly decrease) across the walk",
                        ));
                    }
                }
                prev_internal = Some((entry.key.user_key.clone(), entry.key.seqno));

                if prev_key.as_ref() == Some(&entry.key.user_key) {
                    continue;
                }
                // Require the probe to return the NEWEST version, not merely
                // SOME version. A key spanning restart intervals has a
                // conflict-marked hash bucket; re-stamping that bucket to a
                // later interval holding an OLDER version still yields
                // `Some`, so an `is_none` check would pass — yet point reads
                // after reopen would return the stale value. Match the
                // decoded newest entry's seqno, value type, and bytes.
                let found = data_block.point_read(
                    entry.key.user_key.as_ref(),
                    crate::seqno::MAX_SEQNO,
                    &self.comparator,
                )?;
                let disagrees = match &found {
                    Some(v) => {
                        v.key.seqno != entry.key.seqno
                            || v.key.value_type != entry.key.value_type
                            || v.value != entry.value
                    }
                    None => true,
                };
                if disagrees {
                    return Err(crate::Error::InvalidHeader(
                        "a decoded key's point_read does not return its newest version \
                         (an in-block index disagrees with the entries)",
                    ));
                }
                prev_key = Some(entry.key.user_key);
            }
        }
        Ok(())
    }

    /// Whether the salvage-mode open degraded a REBUILDABLE side section
    /// (filter / `filter_tli`, seqno bounds, zone map, locator) because its
    /// block did not decode as the claimed type — see
    /// [`Inner::rebuildable_section_degraded`](crate::table::inner::Inner). Used
    /// by salvage to fail closed on a table whose degraded section may be a
    /// relabeled deletion it would otherwise discard and resurrect.
    ///
    /// `std`-gated because its only consumer is the salvage path (`std`-only).
    #[cfg(feature = "std")]
    pub(crate) fn salvage_degraded_a_rebuildable_section(&self) -> bool {
        self.rebuildable_section_degraded
    }

    /// Probes every decoded data key against the on-disk `filter` section:
    /// each key the table holds must be reported as POSSIBLY PRESENT. A
    /// checksum- and parity-consistent forged filter is accepted by the
    /// out-of-band walk on its framing and role alone — the walk never
    /// probes it — but `check_bloom` trusts it to SKIP point reads, so a key
    /// made into a false negative silently disappears from every read. A
    /// false positive is unprovable (it is the filter's normal error mode),
    /// but a false NEGATIVE on an existing key is corruption by
    /// construction. A no-op for tables without a filter.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the filter reports an existing
    /// key as definitely absent; any I/O / decode error from the full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_filter(
        &self,
        prefix_extractor: Option<&alloc::sync::Arc<dyn crate::prefix::PrefixExtractor>>,
    ) -> crate::Result<()> {
        // Re-read the filter FROM DISK: the open path PINS the filter (or
        // its partition index) in memory at recover time, so an on-disk
        // re-stamp after the open (the very forge this check exists for)
        // would be invisible to `check_bloom`. An unreadable filter is an
        // error here (the caller is deciding whether to trust the bytes),
        // unlike the read path's permissive empty-payload sentinel.
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        if regions.filter.is_none() && regions.filter_tli.is_none() {
            return Ok(());
        }
        let filter_transform = {
            let t = match self.encryption.as_deref() {
                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                None => crate::table::block::BlockTransform::PLAIN,
            };
            if let Some(ecc) = self.metadata.ecc_params {
                t.with_ecc(ecc)
            } else {
                t
            }
        };
        let load_filter_block =
            |handle: BlockHandle| -> crate::Result<crate::table::filter::block::FilterBlock> {
                let block = Block::from_file(
                    &*file,
                    handle,
                    crate::table::block::BlockIdentity {
                        table_id: self.metadata.id,
                        block_type: BlockType::Filter,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &filter_transform,
                )?;
                if block.header.block_type != BlockType::Filter {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                Ok(crate::table::filter::block::FilterBlock::new(block))
            };

        // Partitioned mode: the partition index maps a key to its filter
        // block. Loaded from disk for the same reason as the filter itself.
        let filter_index = if let Some(idx_handle) = regions.filter_tli {
            let block = Block::from_file(
                &*file,
                idx_handle,
                crate::table::block::BlockIdentity {
                    table_id: self.metadata.id,
                    block_type: BlockType::Index,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = crate::table::block::BlockTransform::from_parts(
                        self.metadata.index_block_compression,
                        self.encryption.as_deref(),
                        #[cfg(zstd_any)]
                        None,
                    )?;
                    if let Some(ecc) = self.metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
            if block.header.block_type != BlockType::Index {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }
            let idx = IndexBlock::new(block);
            idx.try_iter(self.comparator.clone())?;
            Some(idx)
        } else {
            None
        };
        let full_filter = if filter_index.is_none() {
            regions.filter.map(load_filter_block).transpose()?
        } else {
            None
        };
        // A present full filter whose payload decodes to the empty "no filter"
        // sentinel is a forgery on a table with data blocks: the writer omits
        // the section entirely when filtering is disabled, so it never emits a
        // present-but-empty full filter. Left unrejected, the read-path probe
        // below reports Ok(true) for EVERY key (the permissive empty sentinel),
        // so a delete_bitmap renamed and re-roled to an empty filter passes the
        // whole check, the SST is kept, and reopening resurrects the rows the
        // hidden bitmap deleted.
        if let Some(filter) = &full_filter
            && filter.is_empty()
            && self.block_index.iter().next().is_some()
        {
            return Err(crate::Error::InvalidHeader(
                "filter section is present but empty on a table with data blocks",
            ));
        }

        // Partition blocks are shared by many keys; memoize by file offset so
        // the probe loop reads each partition once.
        let mut partitions: alloc::collections::BTreeMap<
            u64,
            crate::table::filter::block::FilterBlock,
        > = alloc::collections::BTreeMap::new();

        // A restricted view's punched prefix blocks decode to zeros; skip them.
        // Their keys are superseded, so the live filter is only obligated to
        // report the suffix keys present.
        let punch = self.punch_offset()?;
        let mut prev_key: Option<Vec<u8>> = None;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            if block_handle.offset().0 < punch {
                continue;
            }
            let entries = self.decode_block_entries(&block_handle)?;
            for entry in entries {
                // Blocks are sorted by key then descending seqno, so a key's
                // older versions are always adjacent — one probe per key.
                if prev_key.as_deref() == Some(entry.key.user_key.as_ref()) {
                    continue;
                }
                let user_key = entry.key.user_key.to_vec();
                let key_hash = crate::hash::hash64(&user_key);
                let maybe_present = if let Some(idx) = &filter_index {
                    let mut iter = idx.iter(self.comparator.clone());
                    iter.seek(&user_key, crate::seqno::MAX_SEQNO);
                    let Some(part_handle) = iter.next() else {
                        // A key past the last partition is a definite miss on
                        // the read path — a false negative by construction.
                        return Err(crate::Error::InvalidHeader(
                            "filter partition index does not cover an existing key",
                        ));
                    };
                    let part_handle = part_handle.materialize(idx.as_slice()).into_inner();
                    let filter = match partitions.entry(part_handle.offset().0) {
                        alloc::collections::btree_map::Entry::Occupied(e) => e.into_mut(),
                        alloc::collections::btree_map::Entry::Vacant(e) => {
                            e.insert(load_filter_block(part_handle)?)
                        }
                    };
                    // The partition index only addresses partitions the writer
                    // filled with keys, so a partition that decodes to the empty
                    // "no filter" sentinel yet is sought for an existing key is a
                    // forgery (a delete_bitmap relabeled to an empty filter
                    // partition under a re-stamped filter_tli). Left unrejected,
                    // the probe below reports Ok(true) permissively for every
                    // key and the relabel passes as a filter-less table.
                    if filter.is_empty() {
                        return Err(crate::Error::InvalidHeader(
                            "filter partition is present but empty for an existing key",
                        ));
                    }
                    filter.maybe_contains_hash(key_hash)?
                } else if let Some(filter) = &full_filter {
                    filter.maybe_contains_hash(key_hash)?
                } else {
                    true
                };
                if !maybe_present {
                    return Err(crate::Error::InvalidHeader(
                        "filter reports an existing key as definitely absent",
                    ));
                }
                // A full filter also indexes every PREFIX the extractor
                // emits for a key, and `maybe_contains_prefix` trusts it to
                // SKIP whole tables on a prefix scan. A filter rebuilt from
                // complete-key hashes alone (a salvage without the extractor)
                // passes the key probe above yet turns every prefix into a
                // false negative — the table silently vanishes from prefix
                // scans. Probe each emitted prefix hash too. Partitioned
                // filters stay conservative (their prefix probe is
                // deliberately best-effort), so this runs only for a full
                // filter with a configured extractor.
                if let (Some(filter), Some(extractor)) = (&full_filter, prefix_extractor) {
                    for prefix in extractor.prefixes(&user_key) {
                        let prefix_hash = crate::hash::hash64(prefix);
                        if !filter.maybe_contains_hash(prefix_hash)? {
                            return Err(crate::Error::InvalidHeader(
                                "filter reports an existing key's prefix as definitely absent",
                            ));
                        }
                    }
                }
                prev_key = Some(user_key);
            }
        }
        Ok(())
    }

    /// Cross-checks the recorded metadata BOUNDS against the table's decoded
    /// contents. Both meta mirrors re-stamped CONSISTENTLY (fresh checksums
    /// and parity) pass every byte-level check and the mirror comparison,
    /// yet run selection trusts `key#min`/`key#max` to route reads AROUND
    /// this table — a narrowed range silently hides real keys (and the range
    /// tombstones that mask older tables). Checks, against the ON-DISK meta:
    /// - the recorded key range COVERS the decoded first/last data keys and
    ///   every recorded range tombstone's bounds (covers, not equals: the
    ///   writer legitimately widens the range over tombstone-only spans);
    /// - the recorded `item_count` equals the decoded entry count (the
    ///   tombstone sentinel is an on-disk entry counted on both sides);
    /// - the recorded `data_block_count` equals the indexed block count
    ///   ([`Table::scan`] hands it to the compaction scanner, which stops
    ///   after that many blocks — a smaller forge drops the tail);
    /// - the disk-fresh per-KV footer descriptor matches the recovery-time
    ///   one (a re-stamped `None` would misread footer bytes as the trailer
    ///   after reopen);
    /// - for tables WITHOUT a range-tombstone sentinel, the recorded
    ///   `seqno#min` is at or below the decoded minimum and `seqno#kv_max`
    ///   is at or above the decoded maximum (a raised min / lowered max
    ///   hides visible versions from snapshot reads). RT-bearing tables skip
    ///   this: the writer records the KV range EXCLUDING the sentinel's
    ///   synthetic RT-derived seqno, so the decoded range legitimately
    ///   differs — and those tables are already gated by the fail-closed
    ///   deletion-metadata attribution rule.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the recorded bounds disagree
    /// with the decoded contents; any I/O / decode error from the full scan.
    #[cfg(feature = "std")]
    pub(crate) fn verify_metadata_bounds(&self) -> crate::Result<()> {
        // Re-read the meta FROM DISK: the in-memory copy was parsed at
        // recover time, so an on-disk re-stamp after the open (the very
        // forge this check exists for) would be invisible to it.
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        // Tail-first with MID fallback, mirroring recovery: the next open
        // trusts the same copy, and a table living off its intact MID mirror
        // (unreadable forged tail) must be judged by THAT copy's bounds.
        let meta = match crate::table::meta::ParsedMeta::load_with_handle(
            &*file,
            &regions.metadata,
            Some(self.metadata.id),
            self.encryption.as_deref(),
        ) {
            Ok(meta) => meta,
            Err(tail_err) => {
                let Some(mid_handle) = regions.metadata_mid else {
                    return Err(tail_err);
                };
                crate::table::meta::ParsedMeta::load_with_handle(
                    &*file,
                    &mid_handle,
                    Some(self.metadata.id),
                    self.encryption.as_deref(),
                )?
            }
        };

        // Nothing on the heal path legitimately rewrites the meta sections,
        // so the disk-fresh copy must equal the recovery-time one FIELD FOR
        // FIELD. A single-field comparison is not enough: any descriptor
        // re-stamped consistently in both mirrors passes the mirror walk,
        // and fields no gate can re-derive from the entries stay
        // authoritative-only — `descriptor#kv_checksum` flipped to `None`
        // makes every reader misread footer bytes as the block trailer
        // after reopen, and a back-dated `created_at` lets FIFO compaction
        // drop the live SST as TTL-expired. The recovery-time copy is
        // trustworthy (the blocks decoded under it at open), so any
        // disk-fresh divergence is a post-open re-stamp.
        if meta != self.metadata {
            return Err(crate::Error::InvalidHeader(
                "disk-fresh meta disagrees with the recovery-time copy",
            ));
        }

        // Load the range tombstones UP FRONT: the synthetic sentinel an RT-ONLY
        // table records is derived from them and must be EXCLUDED from the KV
        // seqno bounds below (its seqno is RT-derived and lies outside the
        // recorded KV range), and the same list feeds the key-range coverage
        // check further down.
        let tombstones = if let Some(rt_handle) = regions.range_tombstones {
            let block = Block::from_file(
                &*file,
                rt_handle,
                crate::table::block::BlockIdentity {
                    table_id: self.metadata.id,
                    block_type: BlockType::RangeTombstone,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
            if block.header.block_type != BlockType::RangeTombstone {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }
            Some(Self::decode_range_tombstones(
                &block,
                self.comparator.as_ref(),
            )?)
        } else {
            None
        };
        // The `(start, seqno)` of the synthetic sentinel entry (a weak tombstone
        // at the (seqno, start)-minimal tombstone), or `None` when the table is
        // not RT-only. Excluded ONCE from the decoded KV seqno bounds below.
        //
        // Only an RT-ONLY table (no real KV items) carries a synthetic sentinel,
        // written SOLELY to give an otherwise KV-empty range-tombstone table one
        // index entry (the writer's `finish`), so its `item_count` is exactly 1.
        // Gate on that — NOT on a seqno comparison against `highest_kv_seqno`: the
        // sentinel exclusion feeds the seqno-bounds check below, and
        // `highest_kv_seqno` is precisely the field that check validates, so using
        // it to identify the sentinel is circular — an attacker who lowers a
        // re-stamped `highest_kv_seqno` below a REAL weak tombstone would make it
        // look synthetic, exclude it, and slip the forged bound past. `item_count`
        // is instead cross-checked against the DECODED entry count below, so a
        // forged `item_count == 1` on a multi-entry table is rejected there; a
        // genuine single-real-KV table whose sole weak tombstone matches the
        // RT-minimal `(key, seqno)` is unreachable (the covering RT would have to
        // start at that sole key and end past it, exceeding the one-key range the
        // coverage check enforces, and an empty point tombstone is rejected at
        // decode), so this never wrongly excludes a real entry.
        let sentinel = tombstones
            .as_deref()
            .filter(|_| meta.item_count == 1)
            .and_then(crate::range_tombstone::RangeTombstone::sentinel)
            .map(|(k, s)| (k.clone(), s));
        let mut sentinel_excluded = false;

        // A restricted (tight-space) view's `[0, punch)` prefix blocks are
        // punched to zeros: the index still LISTS them (so counting index
        // entries yields the whole-table block count, which `meta` records),
        // but they no longer DECODE. Count every block toward `data_block_count`
        // without decoding, and aggregate entries / keys / seqnos over the live
        // suffix only. `meta.item_count` describes the whole table, so its
        // exact-equality check relaxes to a subset check for a restricted view.
        let punch = self.punch_offset()?;
        let restricted = self.restrict_lower_bound().is_some();
        let mut count: u64 = 0;
        let mut block_count: u64 = 0;
        let mut first_key: Option<UserKey> = None;
        let mut last_key: Option<UserKey> = None;
        let mut seqno_lo: Option<SeqNo> = None;
        let mut seqno_hi: Option<SeqNo> = None;
        for handle in self.block_index.iter() {
            let handle = handle?;
            let block_handle = BlockHandle::new(handle.offset(), handle.size());
            block_count = block_count
                .checked_add(1)
                .ok_or(crate::Error::InvalidHeader("meta bounds"))?;
            if block_handle.offset().0 < punch {
                continue;
            }
            let entries = self.decode_block_entries(&block_handle)?;
            count = count
                .checked_add(entries.len() as u64)
                .ok_or(crate::Error::InvalidHeader("meta bounds"))?;
            if first_key.is_none() {
                first_key = entries.first().map(|e| e.key.user_key.clone());
            }
            if let Some(last) = entries.last() {
                last_key = Some(last.key.user_key.clone());
            }
            for entry in &entries {
                // Exclude the synthetic RT sentinel (once): its RT-derived seqno
                // is deliberately outside the recorded KV range, so folding it in
                // would break the KV seqno-bound cross-check for an RT-only table.
                if !sentinel_excluded
                    && let Some((sentinel_key, sentinel_seqno)) = &sentinel
                    && entry.key.seqno == *sentinel_seqno
                    && entry.key.value_type == crate::ValueType::WeakTombstone
                    && entry.key.user_key.as_ref() == sentinel_key.as_ref()
                {
                    sentinel_excluded = true;
                    continue;
                }
                let s = entry.key.seqno;
                seqno_lo = Some(seqno_lo.map_or(s, |lo| lo.min(s)));
                seqno_hi = Some(seqno_hi.map_or(s, |hi| hi.max(s)));
            }
        }
        if restricted {
            // The live suffix is a subset of the whole table `meta` describes;
            // it must not decode to MORE entries than the recorded whole-table
            // count (that would be a grafted-in forge, not a punched prefix).
            if count > meta.item_count {
                return Err(crate::Error::InvalidHeader(
                    "restricted view decodes to more entries than meta item_count",
                ));
            }
        } else if count != meta.item_count {
            return Err(crate::Error::InvalidHeader(
                "meta item_count disagrees with the decoded entry count",
            ));
        }

        // Seqno bounds route snapshot visibility: `get_for_key_cmp` skips this
        // table when the query snapshot is at or below the recorded minimum,
        // and treats it as fully visible above the recorded maximum. Both
        // meta mirrors re-stamped with `seqno#min` raised (or `seqno#kv_max`
        // lowered) pass every other gate yet silently hide older / newer
        // visible versions after reopen. Cross-check against the decoded seqnos.
        // The synthetic RT sentinel was already excluded above (its RT-derived
        // seqno lies outside the recorded KV range), so this runs for RT-bearing
        // tables too: an RT-ONLY table's sole entry drops out, leaving no bounds
        // to check, while an RT+KV table's real entries are checked. A recorded
        // minimum ABOVE the real minimum, or a recorded KV maximum BELOW the
        // real maximum, is corruption.
        if let (Some(lo), Some(hi)) = (seqno_lo, seqno_hi) {
            if meta.seqnos.0 > lo {
                return Err(crate::Error::InvalidHeader(
                    "meta seqno#min is above the decoded minimum seqno",
                ));
            }
            if meta.highest_kv_seqno < hi {
                return Err(crate::Error::InvalidHeader(
                    "meta seqno#kv_max is below the decoded maximum seqno",
                ));
            }
        }
        // `Table::scan` hands the recorded count to the compaction scanner,
        // which stops after that many blocks: a count re-stamped SMALLER
        // silently drops every key in the omitted tail from rewrites.
        if block_count != meta.data_block_count {
            return Err(crate::Error::InvalidHeader(
                "meta data_block_count disagrees with the indexed block count",
            ));
        }
        let cmp = &self.comparator;
        let covers = |key: &[u8]| {
            cmp.compare(meta.key_range.min().as_ref(), key) != core::cmp::Ordering::Greater
                && cmp.compare(meta.key_range.max().as_ref(), key) != core::cmp::Ordering::Less
        };
        if let (Some(first), Some(last)) = (&first_key, &last_key)
            && !(covers(first.as_ref()) && covers(last.as_ref()))
        {
            return Err(crate::Error::InvalidHeader(
                "meta key range does not cover the decoded data keys",
            ));
        }

        // The decoded tombstone count must match the recorded
        // `range_tombstone_count`. A re-stamped RT block that decodes to a
        // SUBSET, or a dropped section, passes the coverage check below for its
        // surviving entries while the missing ranges no longer mask lower-level
        // data — reads then resurrect the keys those tombstones deleted. The
        // count lives in the meta block (already cross-checked field-for-field
        // against the recovery-time copy above), so it is the trustworthy side.
        let decoded_rt_count = tombstones.as_ref().map_or(0, alloc::vec::Vec::len) as u64;
        if decoded_rt_count != meta.range_tombstone_count {
            return Err(crate::Error::InvalidHeader(
                "range_tombstones count disagrees with the recorded range_tombstone_count",
            ));
        }

        // The recorded positional-delete count must match the readable
        // delete_bitmap section EXACTLY, in both directions. The section is
        // OPTIONAL (the writer omits it, and records the count as 0, precisely
        // when the bitmap is empty), so its effective length is its decoded
        // length when present and 0 when absent. A re-stamped TOC can break the
        // agreement either way:
        //   - a `> 0` count with the section RENAMED away (or REPLACED by
        //     another valid optional section, e.g. a full filter that passes
        //     every probe) leaves the table reporting no deletion, resurrecting
        //     every positionally-deleted row;
        //   - a `0` count with a live non-empty section GRAFTED on (the
        //     no-delete count is genuine, but a bitmap from another table is
        //     spliced in) makes reads apply that mask and drop live rows.
        // Comparing effective length to the recorded count catches both, like
        // the range-tombstone count check above. The count lives in the meta
        // block (already cross-checked field-for-field against the recovery-time
        // copy above and against the mirror), so it is the trustworthy side.
        // `None` is an older table without the field: nothing to cross-check.
        // Columnar-only: the bitmap is a columnar-layout section.
        #[cfg(feature = "columnar")]
        if let Some(recorded) = meta.delete_bitmap_len {
            let effective = if self.has_delete_bitmap_section() {
                self.delete_bitmap().len()
            } else {
                0
            };
            if effective != recorded {
                return Err(crate::Error::InvalidHeader(
                    "delete_bitmap count disagrees with the recorded \
                     descriptor#delete_bitmap_len (the section was hidden, \
                     replaced, or grafted on)",
                ));
            }
        }
        // Authenticate the delete-bitmap CONTENTS, not just its cardinality: the
        // count check above accepts an equal-cardinality checksum-valid bitmap
        // substituted for the real one, which — during manifest repair, with no
        // original whole-file digest to compare against — would resurrect the
        // originally-deleted rows and drop different live ones. The meta-bound
        // hash of the section's encoded bytes catches any content substitution;
        // the meta block is itself checksum- and mirror-verified, so a forger
        // cannot restamp the hash without failing meta integrity. Re-encoding the
        // decoded bitmap is byte-identical to the on-disk section (the container
        // kind and its contents round-trip verbatim), so it matches the writer's
        // hash.
        //
        // This gate runs ONLY during repair / heal reconciliation, never on an
        // ordinary read (which the manifest's whole-file digest already
        // protects). Those paths have no original digest, so a PRESENT bitmap
        // section MUST carry a content hash to be authenticated here: a table
        // written before this field (`None`) cannot be authenticated, so it fails
        // closed rather than accepting a possibly-substituted equal-cardinality
        // mask. Ordinary reads of such an older table are unaffected.
        #[cfg(feature = "columnar")]
        if self.has_delete_bitmap_section() {
            match meta.delete_bitmap_hash {
                Some(recorded_hash) => {
                    let actual_hash = crate::hash::hash128(&self.delete_bitmap().encode());
                    if actual_hash != recorded_hash {
                        return Err(crate::Error::InvalidHeader(
                            "delete_bitmap contents disagree with the recorded \
                             descriptor#delete_bitmap_hash (an equal-cardinality bitmap \
                             was substituted)",
                        ));
                    }
                }
                None => {
                    return Err(crate::Error::InvalidHeader(
                        "delete_bitmap section present without a \
                         descriptor#delete_bitmap_hash; its contents cannot be \
                         authenticated during repair / heal",
                    ));
                }
            }
        }
        // Range tombstones mask entries in OLDER tables during reads and
        // merges, so a key range narrowed below a tombstone's extent routes
        // reads around this table and resurrects the data it deletes. Reuse the
        // list loaded up front (for the sentinel derivation) rather than
        // re-reading the block.
        if let Some(tombstones) = &tombstones {
            for rt in tombstones {
                if !(covers(rt.start.as_ref()) && covers(rt.end.as_ref())) {
                    return Err(crate::Error::InvalidHeader(
                        "meta key range does not cover a recorded range tombstone",
                    ));
                }
            }
        }
        Ok(())
    }

    /// Cross-checks the recorded `block_layout` section against the ACTUAL
    /// inner-block boundaries of each zstd data frame, derived by stepwise
    /// partial decodes. The section's block is checksum-clean to the walk
    /// even when a cumulative end was re-stamped to another structurally
    /// valid value, and the partial range-read path trusts it to bound
    /// decompression — a mis-mapped boundary silently omits keys from the
    /// affected span. Every recorded entry must belong to a real data block,
    /// its ends must be strictly increasing, each prefix decode must land
    /// exactly on its recorded boundary, and the final end must exhaust the
    /// frame. A no-op for tables without the section, for encrypted tables
    /// (the lazy path never engages there — the plaintext frame requires a
    /// whole-block decrypt), and on builds without zstd.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] when the recorded layout disagrees
    /// with the frames; any I/O / decode error from the frame reads.
    #[cfg(feature = "std")]
    pub(crate) fn verify_block_layout(&self) -> crate::Result<()> {
        // The section-presence + EMPTINESS check runs on EVERY build (including
        // non-zstd, where the per-frame cross-check below is compiled out): a
        // relabeled delete_bitmap→empty block_layout drops the deletion metadata
        // regardless of zstd, so the forgery must be caught either way.
        //
        // Re-read the section FROM DISK: the in-memory map was loaded at recover
        // time, so an on-disk re-stamp after the open (the very forge this check
        // exists for) would be invisible to it.
        let mut file = self.fs.open(&self.path, &FsOpenOptions::new().read(true))?;
        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = regions::ParsedRegions::parse_from_toc(trailer.toc())?;
        let Some(bl_handle) = regions.block_layout else {
            return Ok(());
        };
        // Decode the map with an encryption-aware transform: on an encrypted
        // table the section is AEAD-sealed, so both the emptiness check below
        // AND catching a relabeled delete_bitmap (its AAD binds the block type,
        // so decoding it as a BlockLayout fails the AEAD open) need the provider.
        let map = {
            let block = Block::from_file(
                &*file,
                bl_handle,
                crate::table::block::BlockIdentity {
                    table_id: self.metadata.id,
                    block_type: BlockType::BlockLayout,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match self.encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = self.metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;
            if block.header.block_type != BlockType::BlockLayout {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }
            crate::table::block_layout::BlockLayoutMap::decode(&block.data)?
        };
        // The writer emits the block_layout section ONLY when it has at least
        // one multi-inner-block frame to record, so a PRESENT-but-empty map on a
        // table with data blocks is a forgery — e.g. a delete_bitmap renamed and
        // re-roled to an empty block_layout, which drops the deletion metadata
        // and resurrects positionally-deleted rows. (The per-block loop below
        // cannot catch it: a block absent from the map is skipped, so an empty
        // map trivially "agrees" with every block.)
        if map.is_empty() {
            if self.block_index.iter().next().is_some() {
                return Err(crate::Error::InvalidHeader(
                    "block_layout section is present but empty on a table with data blocks",
                ));
            }
            return Ok(());
        }
        // The per-frame cross-check decodes whole zstd DATA frames: it needs
        // zstd (the lazy partial-decode path) and cannot run on an encrypted
        // table (the plaintext frame requires a whole-block decrypt the lazy
        // path never performs). The emptiness forgery above is already rejected
        // on every build, so a non-zstd / encrypted table stops here.
        #[cfg(feature = "zstd")]
        {
            if self.encryption.is_some() {
                return Ok(());
            }
            const ERR: crate::Error =
                crate::Error::InvalidHeader("block_layout disagrees with the frames' inner blocks");

            let transform = crate::table::util::build_block_transform(
                self.metadata.data_block_compression,
                None,
                self.metadata.ecc_params,
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            )?;
            // A restricted view's punched prefix frames read as zeros and cannot
            // be frame-decoded; the map still records them (it is not rewritten
            // on reopen), so cross-check `recorded_seen` against the LIVE map
            // entries (offset >= punch) below instead of the whole map length.
            let punch = self.punch_offset()?;
            let mut recorded_seen = 0usize;
            for handle in self.block_index.iter() {
                let handle = handle?;
                let block_handle = BlockHandle::new(handle.offset(), handle.size());
                if block_handle.offset().0 < punch {
                    continue;
                }
                let Some(ends) = map.ends_for(block_handle.offset().0) else {
                    continue;
                };
                recorded_seen = recorded_seen
                    .checked_add(1)
                    .ok_or(crate::Error::InvalidHeader("block_layout"))?;
                if !ends.iter().zip(ends.iter().skip(1)).all(|(a, b)| a < b) {
                    return Err(ERR);
                }
                let (header, frame, _recovery) =
                    Block::read_data_frame(&*file, block_handle, &transform)?;
                if ends.last() != Some(&header.uncompressed_length) {
                    return Err(ERR);
                }
                // Stepwise COLD prefix decodes: each recorded boundary must
                // be exactly where the frame's k-th inner block ends. The
                // per-block quadratic cost is bounded by the handful of
                // inner blocks a data block splits into.
                let mut src = std::io::Cursor::new(frame.as_ref());
                let mut decoder = structured_zstd::decoding::FrameDecoder::new();
                for (idx, &end) in ends.iter().enumerate() {
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "inner-block index is bounded by ends.len(), well within u32"
                    )]
                    let end_block = (idx + 1) as u32;
                    src.set_position(0);
                    // A zstd reset / decode failure on a checksum-clean frame is
                    // DETERMINISTIC codec corruption (a forge that kept the block
                    // checksum but broke the inner framing), NOT transient I/O:
                    // map it to the structural `ERR` so repair's is_corruption
                    // gate routes the table through salvage (which drops / re-
                    // encodes the block) instead of aborting for a retry that can
                    // never succeed. `Error::Io` stays reserved for real fs reads
                    // (the frame read above still propagates its own I/O).
                    decoder.reset(&mut src).map_err(|_| ERR)?;
                    let pd = decoder
                        .decode_blocks_partial(&mut src, 0, end_block, None, false)
                        .map_err(|_| ERR)?;
                    if pd.stopped_at.is_some()
                        || pd.blocks_decoded != end_block
                        || pd.data.len() != end as usize
                    {
                        return Err(ERR);
                    }
                    // The final recorded end must EXHAUST the frame: extra
                    // unrecorded inner blocks would hide data past the map.
                    if idx + 1 == ends.len() && !pd.frame_finished {
                        return Err(ERR);
                    }
                }
            }
            // Every recorded LIVE entry matched a walked block (offsets are
            // unique on both sides), so equal counts mean the map records ONLY
            // the table's blocks. A restricted view compares against the live
            // map entries; the punched prefix's entries are legitimately present.
            if recorded_seen != map.live_len(punch) {
                return Err(crate::Error::InvalidHeader(
                    "block_layout carries entries for blocks the index does not hold",
                ));
            }
        }
        Ok(())
    }

    /// Decodes one data block's entries (row or columnar) into
    /// [`InternalValue`]s, for the semantic cross-check gates. Reads the
    /// block DISK-FRESH ([`Self::load_block_from_disk`]): the gates judge
    /// the file being reconciled, and a pristine copy cached before an
    /// on-disk alteration must not stand in for the altered bytes.
    #[cfg(feature = "std")]
    fn decode_block_entries(
        &self,
        block_handle: &BlockHandle,
    ) -> crate::Result<Vec<InternalValue>> {
        use crate::table::block::ParsedItem as _;
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            let block = self.load_block_from_disk(block_handle, BlockType::Columnar)?;
            let batch = crate::table::columnar::ColumnBatch::decode(&block.data)?;
            return crate::table::columnar::column_batch_to_entries(&batch);
        }
        let block = self.load_block_from_disk(block_handle, BlockType::Data)?;
        let data_block = DataBlock::from_loaded(block, self.metadata.kv_checksum_algo.is_some())?;
        Ok(data_block
            .try_iter(self.comparator.clone())?
            .map(|p| p.materialize(data_block.as_slice()))
            .collect())
    }

    /// Loads the filter block (if any) and checks the bloom filter.
    ///
    /// Returns `Ok(BloomResult::Skip)` if the bloom filter says the key is definitely absent
    /// (and updates metrics accordingly), `Ok(BloomResult::Proceed { has_filter })` otherwise.
    fn check_bloom(&self, key: &[u8], key_hash: u64) -> crate::Result<BloomResult> {
        debug_assert_eq!(
            key_hash,
            crate::hash::hash64(key),
            "key_hash must match the hash of the provided key"
        );

        let filter_block = if let Some(block) = &self.pinned_filter_block {
            Some(Cow::Borrowed(block))
        } else if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter(self.comparator.clone());
            // Filter partitions are written with seqno=0, making the seqno
            // parameter irrelevant to partition selection. Use MAX_SEQNO
            // consistently to match the index-block seek in Table::range().
            iter.seek(key, crate::seqno::MAX_SEQNO);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None,
                    #[cfg(zstd_any)]
                    None,
                )?;
                Some(Cow::Owned(FilterBlock::new(block)))
            } else {
                // Key sorts past the last filter partition — definite miss.
                #[cfg(feature = "metrics")]
                {
                    use core::sync::atomic::Ordering::Relaxed;
                    self.metrics.filter_queries.fetch_add(1, Relaxed);
                    self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
                }
                return Ok(BloomResult::Skip);
            }
        } else if let Some(_filter_tli_handle) = &self.regions.filter_tli {
            unimplemented!("unpinned filter TLI not supported");
        } else if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None,
                #[cfg(zstd_any)]
                None,
            )?;
            Some(Cow::Owned(FilterBlock::new(block)))
        } else {
            None
        };

        let has_filter = filter_block.is_some();

        if let Some(filter_block) = &filter_block
            && !filter_block.maybe_contains_hash(key_hash)?
        {
            #[cfg(feature = "metrics")]
            {
                use core::sync::atomic::Ordering::Relaxed;
                self.metrics.filter_queries.fetch_add(1, Relaxed);
                self.metrics.io_skipped_by_filter.fetch_add(1, Relaxed);
            }
            return Ok(BloomResult::Skip);
        }

        Ok(BloomResult::Proceed { has_filter })
    }

    /// Records a data-consulting point read for per-segment tiering / placement
    /// stats: a single `Relaxed` counter bump plus, on `std`, the access time.
    /// Called only after the seqno-range + bloom gates pass, so bloom misses do
    /// not inflate the count. Raw counter; the consumer derives a rate / EMA from
    /// successive polls.
    fn record_access(&self) {
        self.read_count
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        #[cfg(feature = "std")]
        self.last_access_secs.store(
            crate::time::unix_timestamp().as_secs(),
            core::sync::atomic::Ordering::Relaxed,
        );
    }

    pub fn get(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<InternalValue>> {
        // Tight-space restriction: this version sees the table only at keys
        // `>= bound` (the prefix below it is punched out and superseded by a
        // merged output table). Keys below `bound` must miss here so the read
        // falls through to that output; the punched blocks are never touched.
        if self.is_below_restriction(key) {
            return Ok(None);
        }

        let global_seqno = self.global_seqno();
        // A query snapshot below this table's base seqno predates the table, so
        // none of its rows are visible. `checked_sub` yields `None` there (where a
        // saturating sub would silently clamp to 0); the seqno-range gate below
        // then handles the in-range case.
        let Some(seqno) = seqno.checked_sub(global_seqno) else {
            return Ok(None);
        };

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        // Access accounting after the seqno-range + bloom gates, so a segment
        // that excludes the key (seqno range) or rejects it (bloom miss) is not
        // counted as serving it.
        self.record_access();

        // Row-cache fast path: a prior latest-version read cached this key's
        // resolved value for this (immutable) SST, so we can skip the index walk
        // + data-block decode. The cached value is in table-local seqno space
        // (same as `point_read`). Use it only when the cached newest version is
        // visible at the query snapshot; otherwise fall through, because an older
        // version may apply at this snapshot.
        if let Some(mut iv) = self.cache.get_row(self.global_id(), key_hash, key) {
            // Snapshot reads are exclusive: a version is visible iff its seqno is
            // strictly less than the query seqno. Only serve the cached newest
            // version when it is visible; otherwise fall through (an older
            // version may apply at this snapshot).
            if iv.key.seqno < seqno {
                iv.key.seqno = apply_global_seqno(iv.key.seqno, global_seqno);
                return Ok(Some(iv));
            }
        }

        let item = self.point_read(key, seqno, key_hash)?;

        // Populate the row cache only when this read could see the SST's newest
        // version (`seqno > max`, exclusive), so the resolved value is the SST's
        // newest version for this key — which keeps the seqno-visibility check
        // above correct for later snapshot reads. SSTs are immutable, so the
        // entry stays valid until the SST is compacted away.
        if seqno > self.metadata.seqnos.1
            && let Some(iv) = &item
        {
            self.cache
                .insert_row(self.global_id(), key_hash, iv.clone());
        }

        // Translate table-local seqno back to global coordinate so callers
        // can compare across tables/memtables (L0 best-selection, RT suppression).
        let item = item.map(|mut iv| {
            iv.key.seqno = apply_global_seqno(iv.key.seqno, global_seqno);
            iv
        });

        #[cfg(feature = "metrics")]
        {
            use core::sync::atomic::Ordering::Relaxed;
            // NOTE: `check_bloom()` accounts for lookups rejected by the filter
            // (skip I/O entirely). This path accounts for negative point lookups
            // that still reached storage even though a filter was present, so
            // `filter_queries` remains interpretable alongside `filter_efficiency()`.
            // https://github.com/fjall-rs/lsm-tree/issues/246
            if item.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(item)
    }

    /// Value-only point read: `(value_type, seqno, value)` without
    /// reconstructing the entry key. Used by the value-returning `get` path,
    /// which never reads the matched key (the caller has the needle), so the
    /// delta-key fusion in [`DataBlock::point_read`] is skipped. The value is a
    /// zero-copy slice of the cached block.
    pub(crate) fn get_value(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(crate::ValueType, SeqNo, crate::Slice)>> {
        // Tight-space restriction (mirrors `Table::get`): a key below the bound
        // misses so the read falls through to the superseding output.
        if self.is_below_restriction(key) {
            return Ok(None);
        }

        let global_seqno = self.global_seqno();
        // A query snapshot below this table's base seqno predates the table, so
        // none of its rows are visible. `checked_sub` yields `None` there (where a
        // saturating sub would silently clamp to 0); the seqno-range gate below
        // then handles the in-range case.
        let Some(seqno) = seqno.checked_sub(global_seqno) else {
            return Ok(None);
        };

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        // Access accounting after the seqno-range + bloom gates (mirrors `Table::get`).
        self.record_access();

        // Row-cache fast path (mirrors `Table::get`): serve the value tuple from
        // a prior cached point-read result, skipping the index walk + block
        // decode, when the cached newest version is visible at this snapshot.
        if let Some(iv) = self.cache.get_row(self.global_id(), key_hash, key) {
            // Exclusive snapshot visibility (see `Table::get`): serve only when
            // the cached newest version is strictly older than the query seqno.
            if iv.key.seqno < seqno {
                let s = apply_global_seqno(iv.key.seqno, global_seqno);
                return Ok(Some((iv.key.value_type, s, iv.value)));
            }
        }

        let item = self.point_read_value(key, seqno, key_hash)?;

        // Populate only when this read could see the SST's newest version
        // (`seqno > max`, exclusive), mirroring `Table::get`. The value path does
        // not reconstruct the matched key, so rebuild the `InternalValue` from
        // the query key (the needle) + the resolved `(value_type, seqno, value)`.
        if seqno > self.metadata.seqnos.1
            && let Some((vt, s, v)) = &item
        {
            let iv = InternalValue {
                key: crate::key::InternalKey::new(crate::UserKey::from(key), *s, *vt),
                value: v.clone(),
            };
            self.cache.insert_row(self.global_id(), key_hash, iv);
        }

        // Translate table-local seqno back to the global coordinate, mirroring
        // `Table::get`.
        let item = item.map(|(vt, s, v)| (vt, apply_global_seqno(s, global_seqno), v));

        #[cfg(feature = "metrics")]
        {
            use core::sync::atomic::Ordering::Relaxed;
            if item.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(item)
    }

    /// Value-only block-index walk: companion to [`Table::point_read_inner`]
    /// that reads each candidate data block with
    /// [`DataBlock::point_read_value`] (no key fusion, no retained block).
    fn point_read_value(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(crate::ValueType, SeqNo, crate::Slice)>> {
        // Fast path: retrieval-ribbon locator (see `point_read_inner` for the
        // MVCC-correctness argument). A located-block miss falls through to the
        // index walk below.
        if let Some((handle, hint)) = self.locator_block(key_hash)?
            && let Some(data_block) = self.load_point_block(&handle, key)?
        {
            // A columnar block is narrowed to this key's rows by load_point_block,
            // so the full-block slot hint does not apply.
            let is_columnar = cfg!(feature = "columnar") && self.metadata.columnar;
            let found = match hint {
                Some((slot, is_entry)) if !is_columnar => data_block.point_read_value_at_slot(
                    slot,
                    is_entry,
                    key,
                    seqno,
                    &self.comparator,
                )?,
                _ => data_block.point_read_value(key, seqno, &self.comparator)?,
            };
            if let Some(found) = found {
                return Ok(Some(found));
            }
        }

        let Some(iter) = self.block_index.point_read_reader(key, seqno) else {
            return Ok(None);
        };

        for block_handle in iter {
            let block_handle = block_handle?;

            // A columnar block carrying no row for this key (absent / wholly
            // deleted) returns None here; still honor the end-key cutoff before
            // skipping, so an absent key below this block's end key stops the
            // scan instead of probing every later candidate block.
            let Some(data_block) = self.load_point_block(block_handle.as_ref(), key)? else {
                if self.comparator.compare(block_handle.end_key(), key)
                    == core::cmp::Ordering::Greater
                {
                    return Ok(None);
                }
                continue;
            };

            if let Some(found) = data_block.point_read_value(key, seqno, &self.comparator)? {
                return Ok(Some(found));
            }

            if self.comparator.compare(block_handle.end_key(), key) == core::cmp::Ordering::Greater
            {
                return Ok(None);
            }
        }

        Ok(None)
    }

    /// Like [`Table::get`], but also returns the [`Block`] containing the value.
    ///
    /// Used by `get_pinned()` to construct `PinnableSlice::Pinned`.
    ///
    pub(crate) fn get_with_block(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(InternalValue, Block)>> {
        // Tight-space restriction (mirrors `Table::get`): a key below the bound
        // misses so the read falls through to the superseding output and never
        // touches the punched-out prefix.
        if self.is_below_restriction(key) {
            return Ok(None);
        }

        let global_seqno = self.global_seqno();
        // A query snapshot below this table's base seqno predates the table, so
        // none of its rows are visible. `checked_sub` yields `None` there (where a
        // saturating sub would silently clamp to 0); the seqno-range gate below
        // then handles the in-range case.
        let Some(seqno) = seqno.checked_sub(global_seqno) else {
            return Ok(None);
        };

        if self.metadata.seqnos.0 >= seqno {
            return Ok(None);
        }

        let bloom = self.check_bloom(key, key_hash)?;
        if bloom.should_skip() {
            return Ok(None);
        }

        // Access accounting after the seqno-range + bloom gates (mirrors `Table::get`).
        self.record_access();

        let result = self.point_read_with_block(key, seqno, key_hash)?;

        // Translate table-local seqno back to global coordinate (see Table::get).
        let result = result.map(|(mut iv, block)| {
            iv.key.seqno = apply_global_seqno(iv.key.seqno, global_seqno);
            (iv, block)
        });

        #[cfg(feature = "metrics")]
        {
            use core::sync::atomic::Ordering::Relaxed;
            if result.is_none() && bloom.has_filter() {
                self.metrics.filter_queries.fetch_add(1, Relaxed);
            }
        }

        Ok(result)
    }

    /// Shared block-index walk for point reads. Returns the matching entry
    /// together with the [`DataBlock`] it was found in, so callers that need
    /// the block (e.g. for [`PinnableSlice`]) can keep it alive.
    /// Resolve the data block holding `key_hash`'s newest version (plus an
    /// optional in-block slot hint) via the retrieval-ribbon locator, if one is
    /// loaded. `Ok(None)` means no locator or the ribbon could not answer → the
    /// caller uses the sorted-index walk.
    fn locator_block(
        &self,
        key_hash: u64,
    ) -> crate::Result<Option<crate::table::locator::Located>> {
        match &self.locator_index {
            Some(loc) => loc.locate_block(key_hash),
            None => Ok(None),
        }
    }

    fn point_read_inner(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(InternalValue, DataBlock)>> {
        // Fast path: a retrieval-ribbon locator resolves the key to its data
        // block in O(1), skipping the index-block binary search. The located
        // block holds the newest version (the run's highest-seqno prefix), so a
        // hit returns the correct MVCC answer and a miss (absent key, or the
        // visible version lives in a later block) safely falls through to the
        // index walk below.
        if let Some((handle, hint)) = self.locator_block(key_hash)?
            && let Some(data_block) = self.load_point_block(&handle, key)?
        {
            // A columnar block is narrowed to this key's rows by load_point_block,
            // so the full-block slot hint does not apply.
            let is_columnar = cfg!(feature = "columnar") && self.metadata.columnar;
            let found = match hint {
                Some((slot, is_entry)) if !is_columnar => {
                    data_block.point_read_at_slot(slot, is_entry, key, seqno, &self.comparator)?
                }
                _ => data_block.point_read(key, seqno, &self.comparator)?,
            };
            if let Some(item) = found {
                return Ok(Some((item, data_block)));
            }
        }

        // Borrowing point-read seek: avoids cloning the index block + reuses
        // the trailer metadata parsed at table open (see
        // `BlockIndexImpl::point_read_reader`).
        let Some(iter) = self.block_index.point_read_reader(key, seqno) else {
            return Ok(None);
        };

        for block_handle in iter {
            let block_handle = block_handle?;

            // A columnar block carrying no row for this key (absent / wholly
            // deleted) returns None here; still honor the end-key cutoff before
            // skipping, so an absent key below this block's end key stops the
            // scan instead of probing every later candidate block.
            let Some(data_block) = self.load_point_block(block_handle.as_ref(), key)? else {
                if self.comparator.compare(block_handle.end_key(), key)
                    == core::cmp::Ordering::Greater
                {
                    return Ok(None);
                }
                continue;
            };

            if let Some(item) = data_block.point_read(key, seqno, &self.comparator)? {
                return Ok(Some((item, data_block)));
            }

            // NOTE: If the last block key is higher than ours,
            // our key cannot be in the next block
            if self.comparator.compare(block_handle.end_key(), key) == core::cmp::Ordering::Greater
            {
                return Ok(None);
            }
        }

        Ok(None)
    }

    fn point_read(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<InternalValue>> {
        self.point_read_inner(key, seqno, key_hash)
            .map(|opt| opt.map(|(iv, _)| iv))
    }

    /// Like [`Table::point_read`], but also returns the underlying [`Block`].
    ///
    /// Holding on to the returned [`Block`] (e.g. for [`PinnableSlice`]) keeps the
    /// block data alive while the value is in use, but does not guarantee that the
    /// cache will retain its own entry for that block.
    fn point_read_with_block(
        &self,
        key: &[u8],
        seqno: SeqNo,
        key_hash: u64,
    ) -> crate::Result<Option<(InternalValue, Block)>> {
        self.point_read_inner(key, seqno, key_hash)
            .map(|opt| opt.map(|(iv, db)| (iv, db.inner)))
    }

    /// Batch point-read variant of [`Table::get`].
    ///
    /// Each input pair is `(key, key_hash)`. The slice **must be
    /// strictly sorted ascending by `key` under this table's
    /// comparator** — duplicate adjacent keys are a caller bug
    /// (callers should dedup before batching; a duplicate
    /// suggests a logic error in the query construction) and
    /// are rejected by a `debug_assert!` in debug builds.
    /// Returns one `Option<InternalValue>` per input pair in
    /// input order: `Some(_)` for found values (including
    /// tombstones — callers distinguish via [`InternalValue`]'s
    /// value type), `None` for absent keys.
    ///
    /// # Hash contract
    ///
    /// `key_hash` **must** equal `crate::hash::hash64(key)` — the
    /// same function the writer used when populating the bloom
    /// filter. The bloom probe consumes the hash; the
    /// key↔hash agreement check is a `debug_assert!` only, so
    /// release builds trust the caller. Passing a wrong hash in
    /// release produces false-negative skips: the corresponding
    /// `results[i]` slot stays `None` as if the key weren't in
    /// the table (the result vector itself is always
    /// `sorted_keys.len()` long — nothing is dropped from it).
    /// Callers should derive both values from the same
    /// `(&[u8], u64) = (key, hash64(key))` expression at the
    /// same scope to make the agreement trivially auditable.
    ///
    /// In partitioned-filter mode (`pinned_filter_index` /
    /// `filter_tli`), `check_bloom` ALSO uses the raw `key`
    /// bytes — not the hash — to select which filter partition
    /// to probe. The hash drives the bit probes inside the
    /// selected partition. Bottom line: BOTH inputs are
    /// load-bearing in the partitioned case; only the
    /// monolithic-filter case is "hash-only".
    ///
    /// # Why this exists vs. calling [`Table::get`] in a loop
    ///
    /// Sequential per-key calls each pay:
    ///
    /// 1. Bloom-filter dereference + N hash probes — duplicated
    ///    across calls.
    /// 2. Block-index seek from scratch — every call walks
    ///    `forward_reader(key, seqno)` and re-pays the index
    ///    binary search even when the previous call already
    ///    landed inside the same data block.
    /// 3. Block load — every call re-fetches the data block
    ///    from cache, so cache hits still pay a hashmap lookup
    ///    + Arc clone per call.
    ///
    /// `batch_get` collapses all three:
    ///
    /// 1. Filter probed once per key in a tight loop. For
    ///    monolithic filters (the default) the filter block is
    ///    fetched once and the loop just checks N hashes
    ///    against it. For partitioned filters
    ///    (`pinned_filter_index` / `filter_tli`), each probe
    ///    still seeks the partition index and may load the
    ///    relevant partition block lazily — so "one filter
    ///    fetch total" only holds in the monolithic case;
    ///    partitioned filters amortise loads across keys that
    ///    land in the same partition rather than across the
    ///    whole batch.
    /// 2. Block-index seek runs once at the smallest passing
    ///    key, then the iterator walks forward across the
    ///    sorted input — no re-seek per key.
    /// 3. Each data block is loaded at most once for the entire
    ///    batch. Multiple input keys that fall in the same block
    ///    share a single load.
    ///
    /// The wire-format is identical to N independent `get()`
    /// calls; the savings are purely call-overhead.
    ///
    /// # Sort requirement
    ///
    /// Sorting is the caller's responsibility because the
    /// `batch_get_from_tables` driver already maintains the
    /// remaining-keys list in comparator order between L1+ runs
    /// (re-sorted after each `covered_miss` split). Re-sorting
    /// inside `batch_get` would be redundant work; passing
    /// pre-sorted input lets the implementation rely on a
    /// monotone two-pointer walk between input keys and block
    /// boundaries.
    ///
    /// # Errors
    ///
    /// Propagates any I/O / corruption error from the filter
    /// fetch, block-index read, or data-block load. On error
    /// the partial `results` vector is discarded — callers
    /// observe an all-or-nothing outcome per call.
    #[expect(
        clippy::indexing_slicing,
        reason = "every index access in this routine is bounded by construction: \
                  `passing` indices are produced from enumerate(sorted_keys) so they're \
                  < sorted_keys.len() == results.len(); `passing[p]` is guarded by \
                  `p < passing.len()` on every loop iteration; `passing[0]` is read \
                  only after an explicit emptiness check above."
    )]
    pub fn batch_get(
        &self,
        sorted_keys: &[(&[u8], u64)],
        seqno: SeqNo,
    ) -> crate::Result<Vec<Option<InternalValue>>> {
        let mut results: Vec<Option<InternalValue>> = vec![None; sorted_keys.len()];

        if sorted_keys.is_empty() {
            return Ok(results);
        }

        // Debug-time guard for the sorted-input contract.
        // Unsorted input would silently return wrong Nones
        // (the two-pointer walk between block_iter and the
        // input slice assumes monotone keys); catch the
        // accidental misuse before it ships to a release
        // benchmark. Strict-monotone is the contract — equal
        // adjacent keys would be a duplicate query, also a
        // caller bug.
        debug_assert!(
            sorted_keys
                .windows(2)
                .all(|w| self.comparator.compare(w[0].0, w[1].0) == core::cmp::Ordering::Less),
            "batch_get input must be strictly sorted ascending by key under \
             the table's comparator; unsorted/duplicate input produces silent \
             None misses because the two-pointer walk assumes monotone keys"
        );

        let global_seqno = self.global_seqno();
        // A query snapshot below this table's base seqno predates the table, so no
        // key is visible. `checked_sub` yields `None` there (a saturating sub would
        // clamp to 0); the seqno-range gate below handles the in-range case.
        let Some(table_seqno) = seqno.checked_sub(global_seqno) else {
            return Ok(results);
        };

        // Table is entirely above the snapshot — no key is visible.
        if self.metadata.seqnos.0 >= table_seqno {
            return Ok(results);
        }

        // Filter the input through the bloom filter once. The
        // filter resource (mmap / Arc) is fetched lazily by
        // check_bloom on the first call; subsequent calls reuse
        // it through the table-internal cache.
        let mut passing: Vec<usize> = Vec::with_capacity(sorted_keys.len());
        #[cfg(feature = "metrics")]
        let mut had_filter = false;
        for (i, (key, hash)) in sorted_keys.iter().enumerate() {
            let bloom = self.check_bloom(key, *hash)?;
            if !bloom.should_skip() {
                passing.push(i);
                #[cfg(feature = "metrics")]
                if bloom.has_filter() {
                    had_filter = true;
                }
            }
        }
        if passing.is_empty() {
            return Ok(results);
        }

        // Seek the block index once at the smallest passing key.
        // forward_reader returns the first block whose end_key
        // can cover that key; everything past it walks forward.
        let first_key = sorted_keys[passing[0]].0;
        let Some(mut block_iter) = self.block_index.forward_reader(first_key, table_seqno) else {
            // No block can contain the smallest passing key — every
            // passing key is "negative with filter present" for
            // metrics accounting purposes, mirroring Table::get
            // where a bloom-passing key that point_read can't find
            // increments filter_queries. Falling through to the
            // shared metrics block below ensures the batch path
            // doesn't under-report compared to N independent get()s.
            #[cfg(feature = "metrics")]
            {
                // Use core::* rather than std::* re-exports: the
                // `metrics` feature isn't std-gated in Cargo.toml,
                // and `Ordering` lives in `core::sync::atomic`
                // unchanged — keeps this hot-path import no-std
                // friendly without any runtime impact (the std
                // path is just a re-export of the core symbol).
                use core::sync::atomic::Ordering::Relaxed;
                if had_filter && !passing.is_empty() {
                    self.metrics
                        .filter_queries
                        .fetch_add(passing.len(), Relaxed);
                }
            }
            return Ok(results);
        };

        // Two-pointer walk: outer loop advances block_iter, inner
        // loop drains passing keys that fall inside the current
        // block's range. Both sides are monotone (sorted by the
        // same comparator), so each side advances at most once
        // per pair.
        let mut p = 0_usize;
        while p < passing.len() {
            let Some(handle_result) = block_iter.next() else {
                break;
            };
            let block_handle = handle_result?;
            let end_key = block_handle.end_key();

            // Lazy load: only fetch the data block if at least
            // one passing key falls into this block's range.
            // Most blocks will contain at least one key (we
            // seeked here precisely because the first key did),
            // but bloom may have skipped enough later keys that
            // the next passing one is in a later block — in
            // which case we skip the load.
            let first_in_block = sorted_keys[passing[p]].0;
            if self.comparator.compare(first_in_block, end_key) == core::cmp::Ordering::Greater {
                // The next passing key is BEYOND this block's
                // range. Skip the load and advance to the next
                // block in the index.
                continue;
            }

            // A wholly-deleted columnar block carries no keys; skip it.
            let Some(data_block) = self.load_data_block(block_handle.as_ref())? else {
                continue;
            };

            // Drain passing keys that fall inside [..end_key].
            //
            // Three-way handling mirrors Table::point_read_inner's
            // end-key boundary check:
            //   - Greater (key > end_key): key belongs to a later
            //     block. Break inner loop, advance outer.
            //   - Less    (key < end_key): key is strictly inside
            //     this block. point_read decides; either way the
            //     key cannot continue into the next block (block
            //     keys are sorted, and a later block's first key
            //     is > this block's end_key), so we always advance
            //     p — set Some on hit, leave None on miss.
            //   - Equal   (key == end_key): block end_key matches
            //     the query exactly. point_read may return None
            //     even when a visible version of THIS user key
            //     exists in the NEXT block (same-key spans block
            //     boundary — common with MVCC versions of a hot
            //     key). On None, do NOT advance p — break out so
            //     the next outer iteration loads the next block
            //     and retries the same key.
            while p < passing.len() {
                let key_idx = passing[p];
                let key = sorted_keys[key_idx].0;
                match self.comparator.compare(key, end_key) {
                    core::cmp::Ordering::Greater => break,
                    core::cmp::Ordering::Less => {
                        if let Some(mut item) =
                            data_block.point_read(key, table_seqno, &self.comparator)?
                        {
                            // Translate table-local seqno back to
                            // the global coordinate so callers can
                            // compare results across tables /
                            // memtables (matches Table::get's
                            // contract).
                            item.key.seqno = apply_global_seqno(item.key.seqno, global_seqno);
                            results[key_idx] = Some(item);
                        }
                        p += 1;
                    }
                    core::cmp::Ordering::Equal => {
                        if let Some(mut item) =
                            data_block.point_read(key, table_seqno, &self.comparator)?
                        {
                            item.key.seqno = apply_global_seqno(item.key.seqno, global_seqno);
                            results[key_idx] = Some(item);
                            p += 1;
                        } else {
                            // Same user key may continue in the
                            // next block — leave p in place so the
                            // outer loop's next iteration retries
                            // this key against the next block.
                            break;
                        }
                    }
                }
            }
        }

        #[cfg(feature = "metrics")]
        {
            // core::* (vs the std re-export) for no-std friendliness;
            // see the comment on the matching import above.
            use core::sync::atomic::Ordering::Relaxed;
            // Mirror Table::get's accounting: count negative
            // point lookups that reached storage despite a
            // filter being present. Only keys that passed bloom
            // AND came back empty count.
            if had_filter {
                let negative_with_filter =
                    passing.iter().filter(|&&i| results[i].is_none()).count();
                if negative_with_filter > 0 {
                    // filter_queries is AtomicUsize; the count is
                    // already a usize, no conversion needed.
                    self.metrics
                        .filter_queries
                        .fetch_add(negative_with_filter, Relaxed);
                }
            }
        }

        Ok(results)
    }

    /// Shared setup for the prewarm and chunked block planners: rejects a table
    /// that cannot contribute (empty input, or entirely above the read snapshot),
    /// bloom-filters `sorted_keys` to the passing positions, and opens a forward
    /// block-index reader at the first passing key.
    ///
    /// `Ok(Some(..))` carries the passing positions, the reader, and the
    /// table-local read seqno. `Ok(None)` means the table genuinely contributes no
    /// block (nothing passes the snapshot/bloom/index). `Err` is a real
    /// [`Table::check_bloom`] failure (a partitioned filter's block read) and is
    /// propagated so the authoritative chunked planner surfaces it instead of
    /// mistaking it for a miss; the best-effort prewarm planner maps it back to
    /// `None`.
    #[expect(
        clippy::indexing_slicing,
        reason = "passing[0] is valid after the emptiness check"
    )]
    fn plan_block_walk_setup(
        &self,
        sorted_keys: &[(&[u8], u64)],
        seqno: SeqNo,
    ) -> crate::Result<Option<(Vec<usize>, block_index::BlockIndexIterImpl, SeqNo)>> {
        if sorted_keys.is_empty() {
            return Ok(None);
        }
        let global_seqno = self.global_seqno();
        let Some(table_seqno) = seqno.checked_sub(global_seqno) else {
            return Ok(None);
        };
        if self.metadata.seqnos.0 >= table_seqno {
            return Ok(None);
        }
        let mut passing: Vec<usize> = Vec::with_capacity(sorted_keys.len());
        for (i, (key, hash)) in sorted_keys.iter().enumerate() {
            if !self.check_bloom(key, *hash)?.should_skip() {
                passing.push(i);
            }
        }
        if passing.is_empty() {
            return Ok(None);
        }
        let Some(block_iter) = self
            .block_index
            .forward_reader(sorted_keys[passing[0]].0, table_seqno)
        else {
            return Ok(None);
        };
        Ok(Some((passing, block_iter, table_seqno)))
    }

    /// Plans the COLD (uncached) data blocks [`Table::batch_get`] will read for
    /// `sorted_keys`, returning this table's file handle alongside them so the
    /// caller can read the blocks of MANY SSTs in one cross-file batch (see the
    /// multi-get level prewarm). Returns `None` when there is nothing to prewarm:
    /// no cold block, or a Page-ECC SST (the serial path observes auto-heal) or a
    /// columnar SST (its blocks are reconstructed on the load path).
    ///
    /// Best-effort: an over- or under-estimate only affects warming, never a
    /// query result, since `batch_get` re-reads every block authoritatively.
    #[expect(
        clippy::indexing_slicing,
        reason = "`passing` positions index into `sorted_keys` (< its len); `passing[p]` \
                  is guarded by `p < passing.len()` each iteration."
    )]
    pub(crate) fn plan_prewarm(
        &self,
        sorted_keys: &[(&[u8], u64)],
        seqno: SeqNo,
    ) -> Option<(Arc<dyn crate::fs::FsFile>, Vec<BlockHandle>)> {
        if self.metadata.ecc_params.is_some() {
            return None;
        }
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            return None;
        }
        // Best-effort warming: a bloom-probe error here just skips this table's
        // prewarm (`.ok().flatten()` maps it to None; the authoritative resolve
        // re-probes and surfaces it).
        let (passing, mut block_iter, _table_seqno) = self
            .plan_block_walk_setup(sorted_keys, seqno)
            .ok()
            .flatten()?;

        // Conservative block-boundary walk (mirrors batch_get's span-retry),
        // collecting only the COLD (uncached) blocks.
        let mut handles: Vec<BlockHandle> = Vec::new();
        let mut p = 0_usize;
        while p < passing.len() {
            let Some(Ok(block_handle)) = block_iter.next() else {
                break;
            };
            let end_key = block_handle.end_key();
            let first_in_block = sorted_keys[passing[p]].0;
            if self.comparator.compare(first_in_block, end_key) == core::cmp::Ordering::Greater {
                continue;
            }
            let handle = *block_handle.as_ref();
            if self
                .cache
                .get_block(self.global_id(), handle.offset())
                .is_none()
            {
                handles.push(handle);
            }
            while p < passing.len() {
                let key = sorted_keys[passing[p]].0;
                match self.comparator.compare(key, end_key) {
                    core::cmp::Ordering::Greater | core::cmp::Ordering::Equal => break,
                    core::cmp::Ordering::Less => p += 1,
                }
            }
        }
        if handles.is_empty() {
            return None;
        }

        let (file, _) = self
            .file_accessor
            .get_or_open_table(&self.global_id(), &self.path)
            .ok()?;
        Some((file, handles))
    }

    /// Decodes blocks read by the level prewarm into the cache (`buffers[i]` is
    /// the on-disk bytes of `handles[i]`, both from [`Table::plan_prewarm`]).
    pub(crate) fn decode_prewarmed(&self, handles: &[BlockHandle], buffers: &[Vec<u8>]) {
        crate::table::util::decode_prewarmed_blocks(
            self.global_id(),
            &self.cache,
            handles,
            buffers,
            BlockType::Data,
            self.metadata.data_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        );
    }

    /// Capacity in bytes of this table's (shared) block cache, for the level
    /// prewarm's eviction-avoiding size bound.
    pub(crate) fn cache_capacity(&self) -> u64 {
        self.cache.capacity()
    }

    /// Whether this table's data blocks need the special load path rather than a
    /// plain bytes decode: Page-ECC (re-read recovery) or columnar (PAX
    /// reconstruction). The chunked `multi_get` resolver reads these via
    /// [`Table::load_data_block`] instead of [`Table::decode_data_block_from_bytes`].
    pub(crate) fn is_chunk_special(&self) -> bool {
        if self.metadata.ecc_params.is_some() {
            return true;
        }
        #[cfg(feature = "columnar")]
        if self.metadata.columnar {
            return true;
        }
        false
    }

    /// Plans the data blocks [`Table::batch_get`] would read for `sorted_keys`
    /// (bloom + block-index walk, no decode), returning this table's file handle,
    /// the table-local read seqno, whether it needs the special load path, and per
    /// block the positions (into `sorted_keys`) of the keys that fall in it.
    ///
    /// Used by the chunked `multi_get` resolver to read MANY SSTs' blocks in one
    /// batch and point-read directly, without the cache. Conservative at block
    /// boundaries: a key equal to a block's end key is listed in that block AND
    /// the next (an MVCC version of the same key can span the boundary), mirroring
    /// `batch_get`'s span-retry; the higher-seqno hit wins at resolution.
    ///
    /// `Ok(None)` means this table covers none of `sorted_keys`. Unlike the
    /// best-effort prewarm planner, a bloom-probe or table-open failure is
    /// PROPAGATED (not swallowed to `None`): the chunked resolver is authoritative
    /// for its level, so a swallowed error would let a stale lower level answer.
    ///
    /// # Errors
    ///
    /// Propagates a bloom-probe ([`Table::check_bloom`]) or table-open failure.
    #[expect(
        clippy::indexing_slicing,
        reason = "`passing` positions index into `sorted_keys` (< its len); `passing[p]` \
                  is guarded by `p < passing.len()` each iteration."
    )]
    pub(crate) fn plan_block_tasks(
        &self,
        sorted_keys: &[(&[u8], u64)],
        seqno: SeqNo,
    ) -> crate::Result<Option<BlockTaskPlan>> {
        let Some((passing, mut block_iter, table_seqno)) =
            self.plan_block_walk_setup(sorted_keys, seqno)?
        else {
            return Ok(None);
        };

        let mut blocks: Vec<(BlockHandle, Vec<usize>)> = Vec::new();
        let mut p = 0_usize;
        while p < passing.len() {
            // None ends the index; an Err (index-read / decode failure) is
            // PROPAGATED, not treated as end-of-index. Swallowing it would skip
            // the rest of this table and let a lower level answer a key the
            // failed table actually covers (same `?` contract as `batch_get`).
            let Some(handle_result) = block_iter.next() else {
                break;
            };
            let block_handle = handle_result?;
            let end_key = block_handle.end_key();
            let first_in_block = sorted_keys[passing[p]].0;
            if self.comparator.compare(first_in_block, end_key) == core::cmp::Ordering::Greater {
                continue;
            }
            let handle = *block_handle.as_ref();
            let mut block_keys: Vec<usize> = Vec::new();
            while p < passing.len() {
                let pos = passing[p];
                match self.comparator.compare(sorted_keys[pos].0, end_key) {
                    core::cmp::Ordering::Greater => break,
                    core::cmp::Ordering::Less => {
                        block_keys.push(pos);
                        p += 1;
                    }
                    // Equal: list in THIS block and (by not advancing p) the next,
                    // since a version of this key may continue across the boundary.
                    core::cmp::Ordering::Equal => {
                        block_keys.push(pos);
                        break;
                    }
                }
            }
            blocks.push((handle, block_keys));
        }
        if blocks.is_empty() {
            return Ok(None);
        }

        let (file, _) = self
            .file_accessor
            .get_or_open_table(&self.global_id(), &self.path)?;
        Ok(Some((file, table_seqno, self.is_chunk_special(), blocks)))
    }

    /// Decodes a data block from its on-disk bytes (read by the chunked resolver),
    /// using the same path as [`Table::load_data_block`] for a non-special table
    /// ([`Block::from_reader`] shares the header / decrypt helpers), so the block
    /// is byte-identical. Not for Page-ECC / columnar tables ([`is_chunk_special`]).
    ///
    /// # Errors
    ///
    /// Propagates a corruption / decode error (the resolver surfaces it).
    pub(crate) fn decode_data_block_from_bytes(
        &self,
        bytes: &[u8],
    ) -> crate::Result<Option<DataBlock>> {
        let transform = crate::table::util::build_block_transform(
            self.metadata.data_block_compression,
            self.encryption.as_deref(),
            self.metadata.ecc_params,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )?;
        let identity = crate::table::block::BlockIdentity {
            table_id: self.global_id().table_id(),
            block_type: BlockType::Data,
            dict_id: self.metadata.data_block_compression.dict_id(),
            window_log: 0,
        };
        let block = Block::from_reader(&mut crate::io::Cursor::new(bytes), identity, &transform)?;
        if block.header.block_type != BlockType::Data {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }
        let has_kv_footer = self.metadata.kv_checksum_algo.is_some();
        DataBlock::from_loaded(block, has_kv_footer).map(Some)
    }

    /// Point-reads `key` in an already-decoded `block`, translating the
    /// table-local seqno of any hit back to the global coordinate (matching
    /// [`Table::batch_get`]'s contract).
    ///
    /// # Errors
    ///
    /// Propagates a decode / corruption error from the point read.
    pub(crate) fn point_read_translated(
        &self,
        block: &DataBlock,
        key: &[u8],
        table_seqno: SeqNo,
    ) -> crate::Result<Option<InternalValue>> {
        let global_seqno = self.global_seqno();
        Ok(block
            .point_read(key, table_seqno, &self.comparator)?
            .map(|mut item| {
                item.key.seqno = apply_global_seqno(item.key.seqno, global_seqno);
                item
            }))
    }

    /// Creates a scanner over the `Table`.
    ///
    /// The scanner is ĺogically the same as a normal iter(),
    /// however it uses its own file descriptor, does not look into the block cache
    /// and uses buffered I/O.
    ///
    /// Used for compactions and thus not available to a user.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[doc(hidden)]
    pub fn scan(&self) -> crate::Result<Scanner> {
        #[expect(
            clippy::expect_used,
            reason = "there shouldn't be 4 billion data blocks in a single table"
        )]
        let block_count = self
            .metadata
            .data_block_count
            .try_into()
            .expect("data block count should fit");

        Scanner::new(
            &self.fs,
            &self.path,
            block_count,
            self.metadata.data_block_compression,
            self.global_seqno(),
            self.encryption.clone(),
            self.metadata.ecc_params,
            self.metadata.kv_checksum_algo.is_some(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            self.metadata.id,
            self.metadata.columnar,
            self.metadata.data_block_restart_interval,
        )
    }

    /// Scans this columnar SST block by block, returning one [`ColumnBatch`] per
    /// data block that survives the optional predicate, each carrying only the
    /// projected columns.
    ///
    /// `projection` lists the column ids to decode; every other column is
    /// stepped over without decoding. When `predicate` is set, a block whose
    /// zone-map proves it out of range is skipped without being loaded, and each
    /// surviving block is filtered to the rows that match.
    ///
    /// [`ColumnBatch`]: crate::table::columnar::ColumnBatch
    ///
    /// # Errors
    ///
    /// Returns an error if this SST is not columnar, or on a block read / decode
    /// failure.
    #[cfg(feature = "columnar")]
    pub fn columnar_scan(
        &self,
        projection: &[u16],
        predicate: Option<&crate::table::columnar_predicate::ColumnRangePredicate>,
    ) -> crate::Result<Vec<crate::table::columnar::ColumnBatch>> {
        if !self.metadata.columnar {
            return Err(crate::Error::FeatureUnsupported("columnar"));
        }
        // The predicate must see its own column, even when the caller did not
        // project it; decode it too and drop it from each output batch, so a
        // predicate on an unprojected column still filters instead of matching
        // every row.
        let mut decode_projection = projection.to_vec();
        let added_predicate_column = match predicate {
            Some(pred) if !decode_projection.contains(&pred.column_id) => {
                decode_projection.push(pred.column_id);
                Some(pred.column_id)
            }
            _ => None,
        };
        // Positional deletes are masked at scan time. The block index yields
        // blocks in key (= write) order, the same order the writer assigned row
        // positions, so `row_base` is each block's first global row position.
        let has_deletes = !self.delete_bitmap.is_empty();
        let mut row_base: u32 = 0;
        let mut out = Vec::new();
        for keyed in self.block_index.iter() {
            let keyed = keyed?;
            // Zone-map block skip: prove the block is out of range and never
            // load it. A missing entry is conservative (cannot skip).
            if let Some(pred) = predicate
                && let Some(stats) = self.zone_map.columns_for(*keyed.offset())
                && pred.can_skip_block(stats)
            {
                // Advance the position cursor by the skipped block's row count
                // (from its zone-map stats) so later blocks still map to the
                // right delete positions. Skipped rows are predicate-excluded, so
                // whether they are deleted does not affect the output.
                if has_deletes && let Some(first) = stats.first() {
                    row_base = row_base.wrapping_add(first.row_count);
                }
                continue;
            }
            let handle = BlockHandle::new(keyed.offset(), keyed.size());
            let batch = self.load_columnar_block_projected(&handle, &decode_projection)?;
            let row_count = batch.row_count;
            let mut batch = if predicate.is_some() || has_deletes {
                let mut keep = match predicate {
                    Some(pred) => pred.matching_rows(&batch),
                    None => alloc::vec![true; row_count as usize],
                };
                if has_deletes {
                    let mut pos = row_base;
                    for k in &mut keep {
                        if self.delete_bitmap.contains(pos) {
                            *k = false;
                        }
                        pos = pos.wrapping_add(1);
                    }
                }
                crate::table::columnar_predicate::filter_batch(&batch, &keep)
            } else {
                batch
            };
            row_base = row_base.wrapping_add(row_count);
            if let Some(column_id) = added_predicate_column {
                batch.columns.retain(|c| c.column_id != column_id);
            }
            out.push(batch);
        }
        Ok(out)
    }

    /// Creates an iterator over the `Table`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[doc(hidden)]
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + use<> {
        self.range(..)
    }

    /// Collects every entry in this SST with `seqno >= target_seqno`,
    /// applying the per-block seqno-bounds skip when the SST carries it.
    ///
    /// A data block whose `seqno_bounds` section entry reports
    /// `seqno_max < target_seqno` cannot hold a qualifying record, so it is
    /// skipped without being read. When the SST has no `seqno_bounds` section
    /// (the feature was off), every block is read and filtered per entry, so
    /// the result is correct regardless. Entries come back in the SST's stored
    /// order (key-ascending,
    /// seqno-descending within a key); ordering across sources is the caller's
    /// job.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index or a data block fails.
    #[doc(hidden)]
    pub fn scan_since_seqno(&self, target_seqno: SeqNo) -> crate::Result<Vec<InternalValue>> {
        self.scan_seqno_range(target_seqno, SeqNo::MAX, true)
    }

    /// Like [`Self::scan_since_seqno`] but also bounds the result above:
    /// collects entries whose global seqno is in `[target_seqno, end_seqno)`.
    /// The upper bound lets the tree-level scan pin a stable snapshot watermark
    /// so a concurrent write cannot leak in mid-scan.
    ///
    /// `block_skip` enables the per-block seqno-bounds optimization (skip data
    /// blocks whose recorded `[seqno_min, seqno_max]` cannot overlap the
    /// window). Pass `false` for a paranoid full scan that reads every block and
    /// filters per entry, so even an undetected-corrupt seqno bound (one that
    /// somehow slipped past the block XXH3 checksum) cannot cause a qualifying
    /// record to be skipped.
    ///
    /// # Errors
    ///
    /// Returns `Err` if reading the index or a data block fails.
    #[doc(hidden)]
    pub fn scan_seqno_range(
        &self,
        target_seqno: SeqNo,
        end_seqno: SeqNo,
        block_skip: bool,
    ) -> crate::Result<Vec<InternalValue>> {
        // Bulk-ingested tables store entries at LOCAL seqno coordinates with a
        // `global_seqno` offset; the on-disk seqno bounds and per-entry seqnos
        // are all local. Translate the incoming global target down to local
        // for the comparisons, then translate matched record seqnos back up to
        // global before returning — exactly as `Table::get` does. For a
        // non-ingested table `global_seqno` is 0 and both translations are
        // no-ops.
        let global_seqno = self.global_seqno();
        // Here the saturating clamp to 0 is the INTENDED result, not the silent
        // overflow-masking the point-read path avoids: a lower bound below the
        // offset means "start at the table's first entry", so clamping the
        // translated lower bound to 0 is exactly right.
        let local_target = target_seqno.saturating_sub(global_seqno);
        // Upper bound in local coords. `SeqNo::MAX` (the unbounded case) maps to
        // `MAX - global_seqno`, still far above any reachable local seqno, so every
        // entry passes (effectively unbounded); a real watermark below the offset
        // clamps to 0, which (via the empty-window check below) correctly excludes
        // the whole table. The clamp is intentional, hence saturating not checked.
        let local_end = end_seqno.saturating_sub(global_seqno);

        // Empty window (e.g. a caught-up CDC poller whose target equals the
        // current watermark): nothing can qualify, so skip walking the index
        // entirely. Without this a legacy SST (no per-block seqno bounds) would
        // load + filter every block to return nothing on every poll.
        if local_target >= local_end {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();

        for handle in self.block_index.iter() {
            let handle = handle?;

            // Tight-space restriction: a block whose last key is below the bound
            // sits entirely in the punched-out (zeroed) prefix — never read it.
            // The block straddling the bound is intact (punch starts at its
            // offset) and is filtered per entry in the loop below.
            if let Some(bound) = &self.1
                && self.comparator.compare(handle.end_key(), bound) == core::cmp::Ordering::Less
            {
                continue;
            }

            // Block-skip: look this block's seqno bounds up in the parallel
            // `seqno_bounds` section (keyed by file offset). If its (local) min
            // exceeds the upper bound, or its (local) max is below the target, it
            // cannot reference a qualifying record — skip the data-block read.
            // Bounds live in the section, NOT inline in the index entry, so a
            // point read never pays for them. Disabled in paranoid full-scan
            // mode (`block_skip == false`); absent for legacy/off tables → no
            // skip, full filter (correct regardless).
            if block_skip
                && let Some((seqno_min, seqno_max)) =
                    self.seqno_bounds.bounds_for(handle.as_ref().offset().0)
                && (seqno_max < local_target || seqno_min >= local_end)
            {
                continue;
            }

            // A wholly-deleted columnar block carries no keys; skip it.
            let Some(block) = self.load_data_block(handle.as_ref())? else {
                continue;
            };
            let data = &block.inner.data;
            for item in block.iter(self.comparator.clone()) {
                let mut value = item.materialize(data);
                // Drop entries below the restriction bound in the straddling
                // block (their authoritative copy lives in the superseding
                // output table).
                if let Some(bound) = &self.1
                    && self.comparator.compare(&value.key.user_key, bound)
                        == core::cmp::Ordering::Less
                {
                    continue;
                }
                if value.key.seqno >= local_target && value.key.seqno < local_end {
                    value.key.seqno = apply_global_seqno(value.key.seqno, global_seqno);
                    out.push(value);
                }
            }
        }

        Ok(out)
    }

    /// Creates a ranged iterator over the `Table`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[must_use]
    #[doc(hidden)]
    pub fn range<R: RangeBounds<UserKey> + Send>(
        &self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = crate::Result<InternalValue>> + Send + use<R> {
        self.range_iter(range)
    }

    /// Builds the positional delete mask for a columnar iterator from the
    /// on-open cache: the delete-bitmap plus each block's first global row
    /// position. `None` when the segment has no deletes, so a delete-free table
    /// pays nothing. Cheap (two `Arc` clones); the cumulative row counts were
    /// computed once on open.
    fn build_delete_mask(&self) -> Option<iter::DeleteMask> {
        let block_start_rows = self.delete_block_starts.clone()?;
        Some(iter::DeleteMask {
            bitmap: self.delete_bitmap.clone(),
            block_start_rows,
        })
    }

    /// Like [`Self::range`] but returns the concrete [`iter::Iter`] reader.
    ///
    /// The seekable range pipeline holds the concrete type so it can re-position
    /// the reader in place via [`Self::reseek_range`] instead of rebuilding it.
    pub(crate) fn range_iter<R: RangeBounds<UserKey> + Send>(&self, range: R) -> iter::Iter {
        let index_iter = self.block_index.iter();

        let mut iter = Iter::new(
            self.global_id(),
            self.global_seqno(),
            self.path.clone(),
            index_iter,
            self.file_accessor.clone(),
            self.cache.clone(),
            self.metadata.data_block_compression,
            self.encryption.clone(),
            self.metadata.ecc_params,
            self.heal_hints.get().cloned(),
            self.metadata.kv_checksum_algo.is_some(),
            self.metadata.columnar,
            self.build_delete_mask(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            #[cfg(feature = "zstd")]
            self.block_layout.clone(),
            #[cfg(feature = "zstd")]
            self.metadata.data_block_restart_interval,
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        );

        match range.start_bound() {
            Bound::Included(key) => iter.set_lower_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_lower_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        // Tight-space restriction: raise the scan's lower bound up to `bound`
        // when this version restricts the table, so the iterator never walks
        // index entries pointing into the punched-out (zeroed) prefix below
        // `bound`. Only raises (never lowers) the requested start: a request
        // already at or above `bound` is left untouched.
        if let Some(bound) = &self.1 {
            let raise = match range.start_bound() {
                Bound::Included(key) | Bound::Excluded(key) => {
                    self.comparator.compare(bound, key) == core::cmp::Ordering::Greater
                }
                Bound::Unbounded => true,
            };
            if raise {
                iter.set_lower_bound(iter::Bound::Included(bound.clone()));
            }
        }

        match range.end_bound() {
            Bound::Included(key) => iter.set_upper_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_upper_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        iter
    }

    /// Re-position an existing [`iter::Iter`] (produced by [`Self::range`] on
    /// this same table) to a fresh `range`, reusing its owned index iterator and
    /// `Arc` handles instead of constructing a new reader.
    ///
    /// Applies the exact same bound translation as [`Self::range`] (including the
    /// tight-space lower-bound raise), so the re-seeked iterator yields the same
    /// entries a freshly-built `self.range(range)` would. Used by the seekable
    /// range pipeline to move leaf cursors without per-seek allocation.
    #[doc(hidden)]
    pub fn reseek_range<R: RangeBounds<UserKey> + Send>(&self, iter: &mut iter::Iter, range: R) {
        iter.reset_for_reseek();

        match range.start_bound() {
            Bound::Included(key) => iter.set_lower_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_lower_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }

        // Mirror `range()`'s tight-space restriction: raise the scan's lower
        // bound up to `bound` when this version restricts the table.
        if let Some(bound) = &self.1 {
            let raise = match range.start_bound() {
                Bound::Included(key) | Bound::Excluded(key) => {
                    self.comparator.compare(bound, key) == core::cmp::Ordering::Greater
                }
                Bound::Unbounded => true,
            };
            if raise {
                iter.set_lower_bound(iter::Bound::Included(bound.clone()));
            }
        }

        match range.end_bound() {
            Bound::Included(key) => iter.set_upper_bound(iter::Bound::Included(key.clone())),
            Bound::Excluded(key) => iter.set_upper_bound(iter::Bound::Excluded(key.clone())),
            Bound::Unbounded => {}
        }
    }

    fn read_tli(
        regions: &ParsedRegions,
        file: &dyn FsFile,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> crate::Result<IndexBlock> {
        // Tail copy first (preferred): if a fresh `tli_tail` exists it
        // landed after the head `tli`, so it's the most-recently
        // fsynced copy. On any decode / decrypt / checksum failure
        // fall back to the head `tli` if present.
        //
        // Both copies encode the same handles list (the writer hands
        // a single `tli_bytes` buffer to both sites) and both are
        // written under the same `CompressionType`
        // (`metadata.index_block_compression`); the block header does
        // not record a compression tag, so this single value decodes
        // either copy. Encryption nonce differs per copy (fresh per
        // `Block::write_into`) and the ciphertext therefore differs
        // byte-for-byte, but both decrypt to the same plaintext
        // IndexBlock.
        //
        // Tables written before the TLI-mirror change have no
        // `tli_tail`; reader falls straight through to the head copy.
        if let Some(tail_handle) = regions.tli_tail {
            log::trace!("Reading TLI tail mirror, with tli_tail_ptr={tail_handle:?}");
            match Self::read_tli_at(file, tail_handle, table_id, compression, encryption, ecc) {
                Ok(idx) => return Ok(idx),
                Err(tail_err) => {
                    log::warn!(
                        "TLI tail mirror unreadable ({tail_err}); falling back to TLI head copy at {:?}",
                        regions.tli,
                    );
                    // Match the meta-mirror pattern: when BOTH
                    // copies fail, surface the original `tail_err`
                    // (callers care about the authoritative /
                    // preferred copy's failure mode). The head
                    // failure goes to the log so it's not silently
                    // dropped from diagnostics.
                    log::trace!("Reading TLI head copy, with tli_ptr={:?}", regions.tli);
                    return match Self::read_tli_at(
                        file,
                        regions.tli,
                        table_id,
                        compression,
                        encryption,
                        ecc,
                    ) {
                        Ok(idx) => Ok(idx),
                        Err(head_err) => {
                            log::warn!(
                                "TLI head copy also unreadable ({head_err}); returning original tail error",
                            );
                            Err(tail_err)
                        }
                    };
                }
            }
        }

        log::trace!("Reading TLI head copy, with tli_ptr={:?}", regions.tli);
        Self::read_tli_at(file, regions.tli, table_id, compression, encryption, ecc)
    }

    fn read_tli_at(
        file: &dyn FsFile,
        handle: BlockHandle,
        table_id: TableId,
        compression: CompressionType,
        encryption: Option<&dyn crate::encryption::EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
    ) -> crate::Result<IndexBlock> {
        let block = Block::from_file(
            file,
            handle,
            crate::table::block::BlockIdentity {
                table_id,
                block_type: BlockType::Index,
                dict_id: 0,
                window_log: 0,
            },
            &{
                // Index blocks are SST blocks that omit the block_flags byte,
                // so ECC presence comes from the per-SST descriptor: upgrade
                // to the `*Ecc` transform when this table was written with
                // Page ECC. Identity without the feature.
                let t = crate::table::block::BlockTransform::from_parts(
                    compression,
                    encryption,
                    #[cfg(zstd_any)]
                    None,
                )?;
                if let Some(ecc) = ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        )?;

        if block.header.block_type != BlockType::Index {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        Ok(IndexBlock::new(block))
    }

    /// Tries to recover a table from a file.
    ///
    /// A corrupt delete-bitmap fails recovery rather than silently resurrecting
    /// deleted rows; see [`Self::recover_inner`] for the salvage variant that
    /// degrades to "all rows live" instead.
    #[expect(
        clippy::too_many_arguments,
        reason = "recovery requires many context parameters"
    )]
    pub fn recover(
        file_path: PathBuf,
        checksum: Checksum,
        global_seqno: SeqNo,
        tree_id: TreeId,
        table_id: TableId,
        cache: Arc<Cache>,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
        pin_filter: bool,
        pin_index: bool,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
    ) -> crate::Result<Self> {
        Self::recover_inner(
            file_path,
            checksum,
            global_seqno,
            tree_id,
            table_id,
            cache,
            descriptor_table,
            fs,
            pin_filter,
            pin_index,
            encryption,
            #[cfg(zstd_any)]
            zstd_dictionary,
            comparator,
            #[cfg(feature = "metrics")]
            metrics,
            RecoveryMode::Live,
        )
    }

    /// Recovers a table, optionally in **salvage mode** (see [`RecoveryMode`]).
    ///
    /// In salvage mode a corrupt or truncated delete-bitmap degrades to empty
    /// ("all rows live, pending recompaction") and a delete-bitmap with an
    /// unreadable zone map is ignored rather than erroring, so a columnar
    /// segment with a damaged sidecar still opens and its data blocks can be
    /// recovered. Normal recovery ([`RecoveryMode::Live`]) fails closed on
    /// both, to avoid resurrecting deleted rows. Used by [`crate::salvage`].
    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "recovery requires many context parameters and is inherently complex"
    )]
    pub(crate) fn recover_inner(
        file_path: PathBuf,
        checksum: Checksum,
        global_seqno: SeqNo,
        tree_id: TreeId,
        table_id: TableId,
        cache: Arc<Cache>,
        descriptor_table: Option<Arc<DescriptorTable>>,
        fs: Arc<dyn Fs>,
        pin_filter: bool,
        pin_index: bool,
        encryption: Option<Arc<dyn crate::encryption::EncryptionProvider>>,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        #[cfg(feature = "metrics")] metrics: Arc<Metrics>,
        mode: RecoveryMode,
    ) -> crate::Result<Self> {
        use core::sync::atomic::AtomicBool;
        use meta::ParsedMeta;
        use regions::ParsedRegions;

        let salvage = matches!(mode, RecoveryMode::Salvage { .. });

        log::debug!("Recovering table from file {}", file_path.display());
        let mut file = fs.open(&file_path, &FsOpenOptions::new().read(true))?;
        let file_path = Arc::new(file_path);

        #[cfg(feature = "metrics")]
        metrics
            .table_file_opened_uncached
            .fetch_add(1, core::sync::atomic::Ordering::Relaxed);

        let trailer = crate::sfa::Reader::from_reader(&mut file)?;
        let regions = ParsedRegions::parse_from_toc(trailer.toc())?;

        log::trace!("Reading meta block, with meta_ptr={:?}", regions.metadata);
        // The expected-id cross-check rejects a swapped / wrong-id file in a
        // LIVE tree, where the manifest / file name is the durable identity.
        // A salvage open with a caller-known id (repair — from the file name)
        // keeps the check so a forged TAIL id falls back to the MID mirror; a
        // STANDALONE salvage reader has no out-of-band id, so the SOURCE's
        // own stored id is the identity and the check is skipped (None). An
        // ENCRYPTED open always passes the caller's id — the meta block's AAD
        // binds it, so decryption itself requires the right id.
        let expected_id = match mode {
            RecoveryMode::Live => Some(table_id),
            RecoveryMode::Salvage { expected_id, .. } => {
                if encryption.is_some() {
                    Some(table_id)
                } else {
                    expected_id
                }
            }
        };
        // Salvage arbitration may invert the mirror order (see
        // `RecoveryMode::Salvage::prefer_mid_meta`); live opens always load
        // tail-first.
        let prefer_mid_meta = matches!(
            mode,
            RecoveryMode::Salvage {
                prefer_mid_meta: true,
                ..
            }
        );
        // TAIL first (authoritative copy by convention; physically
        // identical content to MID — same `file_size`, same
        // `created_at`, same KV map — the only difference is which
        // SFA section is loaded). On any decode/decrypt/checksum
        // failure fall back to the MID copy if present. Under salvage
        // arbitration (`prefer_mid_meta`) the order is inverted: MID
        // first, tail as the fallback.
        let (first_handle, first_name, second, second_name) =
            if prefer_mid_meta && let Some(mid_handle) = regions.metadata_mid {
                (mid_handle, "MID", Some(regions.metadata), "TAIL")
            } else {
                (regions.metadata, "TAIL", regions.metadata_mid, "MID")
            };
        let metadata = match ParsedMeta::load_with_handle(
            &*file,
            &first_handle,
            expected_id,
            encryption.as_deref(),
        ) {
            Ok(m) => m,
            Err(first_err) => {
                if let Some(second_handle) = second {
                    log::warn!(
                        "{first_name} meta block unreadable for {} ({first_err}); \
                         falling back to {second_name} copy",
                        file_path.display(),
                    );
                    // Match the PR contract: when BOTH copies fail,
                    // surface the FIRST error (callers care about the
                    // preferred copy's failure mode). The fallback
                    // failure goes to the log so it's not silently
                    // dropped from diagnostics.
                    // MID and TAIL are byte-identical: same `file_size`
                    // (= `*self.meta.file_pos`, only bumped inside
                    // `spill_block`, unchanged between the two writes),
                    // same `created_at` (snapshotted once in
                    // `finish()`), same KV map. Either payload is usable
                    // directly — no sentinel patching, no
                    // `std::fs::metadata` (which would also bypass the
                    // pluggable `Fs` backend).
                    match ParsedMeta::load_with_handle(
                        &*file,
                        &second_handle,
                        expected_id,
                        encryption.as_deref(),
                    ) {
                        Ok(m) => m,
                        Err(second_err) => {
                            log::warn!(
                                "{second_name} meta block also unreadable for {}: {second_err}; \
                                 returning original {first_name} error",
                                file_path.display(),
                            );
                            return Err(first_err);
                        }
                    }
                } else {
                    return Err(first_err);
                }
            }
        };

        // Fail-fast: if this table was written with dictionary compression,
        // verify the caller provided the matching dictionary. Without this
        // check, reopening with the wrong dictionary (or None) would only
        // surface as a decompression error on the first data-block read.
        #[cfg(zstd_any)]
        if let CompressionType::ZstdDict { dict_id, .. } = metadata.data_block_compression {
            let got = zstd_dictionary.as_ref().map(|d| d.id());
            if got != Some(dict_id) {
                return Err(crate::Error::ZstdDictMismatch {
                    expected: dict_id,
                    got,
                });
            }
        }

        let file_handle: Arc<dyn FsFile> = Arc::from(file);

        let file_accessor = if let Some(dt) = descriptor_table {
            FileAccessor::DescriptorTable {
                table: dt,
                fs: fs.clone(),
            }
        } else {
            FileAccessor::File(file_handle.clone())
        };

        let block_index = if regions.index.is_some() {
            log::trace!(
                "Creating partitioned block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(
                &regions,
                file_handle.as_ref(),
                metadata.id,
                metadata.index_block_compression,
                encryption.as_deref(),
                metadata.ecc_params,
            )?;

            BlockIndexImpl::TwoLevel(TwoLevelBlockIndex {
                top_level_index: block,
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                path: Arc::clone(&file_path),
                file_accessor: file_accessor.clone(),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                ecc: metadata.ecc_params,
                comparator: comparator.clone(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        } else if pin_index {
            log::trace!(
                "Creating pinned, full block index, with tli_ptr={:?}",
                regions.tli,
            );

            let block = Self::read_tli(
                &regions,
                file_handle.as_ref(),
                metadata.id,
                metadata.index_block_compression,
                encryption.as_deref(),
                metadata.ecc_params,
            )?;
            BlockIndexImpl::Full(FullBlockIndex::new(block, comparator.clone())?)
        } else {
            log::trace!("Creating volatile, full block index");

            BlockIndexImpl::VolatileFull(VolatileBlockIndex {
                cache: cache.clone(),
                compression: metadata.index_block_compression,
                file_accessor: file_accessor.clone(),
                handle: regions.tli,
                path: Arc::clone(&file_path),
                table_id: (tree_id, metadata.id).into(),
                encryption: encryption.clone(),
                ecc: metadata.ecc_params,
                comparator: comparator.clone(),

                #[cfg(feature = "metrics")]
                metrics: metrics.clone(),
            })
        };

        // Set when the salvage-mode open DEGRADES a rebuildable side section
        // (filter / filter_tli, seqno bounds, zone map, locator) because its
        // block did not decode as the claimed type. Salvage re-derives every
        // such section from the recovered entries, so a section that is present
        // but does not decode may be a `range_tombstones` / `delete_bitmap`
        // relabeled to a rebuildable name and re-roled — which salvage would
        // discard, resurrecting the suppressed rows. This is a purely
        // STRUCTURAL signal (each decode reads its own section's bytes,
        // independent of the data blocks), so a corrupt DATA block does not
        // trip it. `block_layout` is excluded: it fails the open outright, so a
        // relabel to it is quarantined by the failed recovery rather than
        // salvaged.
        let mut rebuildable_section_degraded = false;

        let pinned_filter_index = if let Some(filter_tli_handle) = regions.filter_tli {
            let load = || -> crate::Result<IndexBlock> {
                let block = Block::from_file(
                    file_handle.as_ref(),
                    filter_tli_handle,
                    crate::table::block::BlockIdentity {
                        table_id: metadata.id,
                        block_type: BlockType::Index,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &{
                        // Filter TLI is an Index (SST) block: no block_flags byte,
                        // so ECC presence comes from the per-SST descriptor.
                        let t = crate::table::block::BlockTransform::from_parts(
                            metadata.index_block_compression,
                            encryption.as_deref(),
                            #[cfg(zstd_any)]
                            None,
                        )?;
                        if let Some(ecc) = metadata.ecc_params {
                            t.with_ecc(ecc)
                        } else {
                            t
                        }
                    },
                )?;
                if block.header.block_type != BlockType::Index {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                let idx = IndexBlock::new(block);
                // Validate filter index trailer eagerly (same as FullBlockIndex::new)
                // so later iter() calls cannot panic on malformed blocks.
                idx.try_iter(comparator.clone())?;
                Ok(idx)
            };
            match load() {
                Ok(idx) => Some(idx),
                // Only a TRANSIENT read propagates so repair retries; a PERSISTENT
                // I/O failure (bad sector, truncation) degrades under salvage like
                // the seqno-bounds / zone-map / delete-bitmap / locator loaders —
                // turning a one-shot failure into a permanent drop of the whole
                // (recoverable) table would be wrong.
                Err(crate::Error::Io(e)) if e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                // Salvage never consults the source's filter — the
                // destination writer rebuilds it from the recovered keys —
                // so a STRUCTURALLY or PERSISTENTLY unreadable filter index must not
                // cost the recoverable data (a live open still fails closed).
                Err(e) if salvage => {
                    log::warn!(
                        "filter index for table {:?} is unreadable ({e}); salvaging \
                         without it (the recovered copy re-derives its filter)",
                        metadata.id
                    );
                    rebuildable_section_degraded = true;
                    None
                }
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        // TODO: FilterBlock newtype
        //
        // In SALVAGE mode the source filter is never PINNED (the destination
        // rebuilds it from the recovered keys), but a present full filter must
        // still be PROBED even when `pin_filter` is false: a delete_bitmap
        // renamed and re-roled to a `filter` (an empty sentinel, or a
        // checksum/parity-valid but structurally broken BuRR payload) launders
        // the deletion metadata. Loading and parsing it here trips the
        // rebuildable-section degradation, which the salvage guard turns into a
        // fail-closed quarantine. A live open (pin_filter, not salvage) keeps
        // its exact prior behaviour.
        let pinned_filter_block = if pinned_filter_index.is_none() && (pin_filter || salvage) {
            let loaded = regions
                .filter
                .map(|filter_handle| {
                    log::debug!(
                        "Loading and pinning filter block, with filter_ptr={filter_handle:?}"
                    );

                    let block = Block::from_file(
                        file_handle.as_ref(),
                        filter_handle,
                        crate::table::block::BlockIdentity {
                            table_id: metadata.id,
                            block_type: BlockType::Filter,
                            dict_id: 0,
                            window_log: 0,
                        },
                        // Filter blocks are never written compressed, so the
                        // transform is Plain or Encrypted depending on whether
                        // the table is keyed. Filter is an SST block (no
                        // block_flags byte), so ECC presence comes from the
                        // per-SST descriptor: upgrade to `*Ecc` when page_ecc.
                        &{
                            let t = match encryption.as_deref() {
                                Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                                None => crate::table::block::BlockTransform::PLAIN,
                            };
                            if let Some(ecc) = metadata.ecc_params {
                                t.with_ecc(ecc)
                            } else {
                                t
                            }
                        },
                    )
                    .and_then(|block| {
                        if block.header.block_type == BlockType::Filter {
                            Ok(block)
                        } else {
                            Err(crate::Error::InvalidTag((
                                "BlockType",
                                block.header.block_type.into(),
                            )))
                        }
                    })?;

                    Ok::<_, crate::Error>(FilterBlock::new(block))
                })
                .transpose();
            match loaded {
                Ok(Some(filter)) => {
                    // A present full filter that decodes to the empty sentinel,
                    // or whose BuRR payload does not parse, is not something the
                    // writer ever emits (it omits the section when filtering is
                    // disabled). In salvage mode treat it as a degraded
                    // rebuildable section so a relabeled delete_bitmap cannot
                    // pass as a filter-less table and resurrect deleted rows.
                    if salvage && (filter.is_empty() || filter.maybe_contains_hash(0).is_err()) {
                        log::warn!(
                            "filter block for table {:?} is empty or unparsable; salvaging \
                             as a degraded rebuildable section",
                            metadata.id
                        );
                        rebuildable_section_degraded = true;
                    }
                    // Never PIN in salvage (the destination rebuilds the
                    // filter); a live open with pin_filter keeps it.
                    if pin_filter { Some(filter) } else { None }
                }
                Ok(None) => None,
                // An InvalidTag here is STRUCTURAL, not corruption: the block
                // loaded and verified its own PAYLOAD checksum, it is just the
                // WRONG role. `Header::checksum` covers the payload, not the
                // header, so a `block_type` byte that flips to another valid SST
                // discriminant does NOT fail that checksum — it reaches this
                // role check exactly like a TOC rename (a delete_bitmap renamed
                // to `filter` without re-roling its header). Both are a valid
                // block of the wrong name (the relabel signature), so degrade
                // like the empty / unparsable payload above and fail closed.
                //
                // Any OTHER load failure (payload checksum / AEAD) is GENUINE
                // bit-rot: a re-stamped relabel produces a checksum-VALID block,
                // so a broken payload checksum is real corruption, rebuilt from
                // the recovered keys. A delete-free table with a bit-rotted
                // filter must auto-repair, not quarantine, so salvaging
                // continues without degrading.
                Err(e) if salvage => {
                    if matches!(e, crate::Error::InvalidTag(_)) {
                        log::warn!(
                            "filter block for table {:?} has the wrong role ({e}); salvaging \
                             as a degraded rebuildable section",
                            metadata.id
                        );
                        rebuildable_section_degraded = true;
                    } else {
                        log::warn!(
                            "filter block for table {:?} is unreadable ({e}); salvaging \
                             without it (the recovered copy re-derives its filter)",
                            metadata.id
                        );
                    }
                    None
                }
                Err(e) => return Err(e),
            }
        } else {
            None
        };

        // Load range tombstones (if present)
        let range_tombstones = if let Some(rt_handle) = regions.range_tombstones {
            log::trace!("Loading range tombstone block, with rt_ptr={rt_handle:?}");
            let block = Block::from_file(
                file_handle.as_ref(),
                rt_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::RangeTombstone,
                    dict_id: 0,
                    window_log: 0,
                },
                // Range-tombstone blocks are always uncompressed; the
                // transform is Plain or Encrypted depending on whether the
                // table is keyed. RangeTombstone is an SST block (no
                // block_flags byte), so ECC presence comes from the per-SST
                // descriptor: upgrade to `*Ecc` when page_ecc.
                &{
                    let t = match encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;

            if block.header.block_type != BlockType::RangeTombstone {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }

            let mut rts = Self::decode_range_tombstones(&block, comparator.as_ref())?;
            // Sort range tombstones by (start asc, seqno desc) using the
            // user comparator so the order matches the tree's key ordering.
            // The seqno-desc tiebreaker ensures higher-seqno RTs are checked
            // first when multiple share the same start key.
            let cmp = &comparator;
            rts.sort_unstable_by(|a, b| {
                cmp.compare(&a.start, &b.start)
                    .then_with(|| b.seqno.cmp(&a.seqno))
            });
            rts
        } else {
            Vec::new()
        };

        // Load the optional inner-block layout section (present only when the
        // table has data blocks that split into >= 2 inner zstd blocks). Mirrors
        // the range-tombstone loader: same Plain/Encrypted (+ optional ECC)
        // transform the writer used for this uncompressed meta section.
        let block_layout = if let Some(bl_handle) = regions.block_layout {
            let block = Block::from_file(
                file_handle.as_ref(),
                bl_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::BlockLayout,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            )?;

            if block.header.block_type != BlockType::BlockLayout {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    block.header.block_type.into(),
                )));
            }

            let map = crate::table::block_layout::BlockLayoutMap::decode(&block.data)?;
            log::trace!(
                "Loaded block-layout index with {} multi-inner-block entries",
                map.len(),
            );
            map
        } else {
            crate::table::block_layout::BlockLayoutMap::default()
        };

        // Load the optional seqno-bounds section (parallel to the index; powers
        // the scan_since_seqno block-skip). Absent unless seqno_in_index was on.
        //
        // Best-effort, like the zone map below: the seqno-bounds section is
        // derived, non-authoritative metadata, so a corrupt / unreadable section
        // disables the block-skip (falling back to a full per-entry filter)
        // rather than failing the whole table open.
        let seqno_bounds = if let Some(sb_handle) = regions.seqno_bounds {
            let load = || -> crate::Result<crate::table::seqno_bounds::SeqnoBoundsMap> {
                let block = Block::from_file(
                    file_handle.as_ref(),
                    sb_handle,
                    crate::table::block::BlockIdentity {
                        table_id: metadata.id,
                        block_type: BlockType::SeqnoBounds,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &{
                        let t = match encryption.as_deref() {
                            Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                            None => crate::table::block::BlockTransform::PLAIN,
                        };
                        if let Some(ecc) = metadata.ecc_params {
                            t.with_ecc(ecc)
                        } else {
                            t
                        }
                    },
                )?;
                if block.header.block_type != BlockType::SeqnoBounds {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                crate::table::seqno_bounds::SeqnoBoundsMap::decode(&block.data)
            };
            match load() {
                Ok(m) => m,
                // Only a TRANSIENT read propagates so repair retries; a PERSISTENT
                // failure (bad sector, truncation) degrades this rebuildable,
                // derived section to an empty map (seqno block-skip disabled)
                // rather than failing the whole open — turning an optimization's
                // bit-rot into a hard availability loss would be wrong.
                Err(crate::Error::Io(e)) if e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                Err(e) => {
                    log::warn!(
                        "seqno-bounds section for table {:?} is unreadable ({e}); disabling seqno block-skip",
                        metadata.id
                    );
                    rebuildable_section_degraded = true;
                    crate::table::seqno_bounds::SeqnoBoundsMap::default()
                }
            }
        } else {
            crate::table::seqno_bounds::SeqnoBoundsMap::default()
        };
        if !seqno_bounds.is_empty() {
            log::trace!("Loaded {} seqno-bounds entries", seqno_bounds.len());
        }

        // Load the optional zone-map section (parallel to the index; powers the
        // predicate-based block-skip). Absent unless the zone-map policy was on.
        //
        // Best-effort: the zone map is DERIVED, non-authoritative metadata. A
        // corrupt or unreadable section disables block-skip for this table (an
        // empty map) rather than failing the whole `Table::recover` — turning an
        // optimization's bit-rot into a hard availability loss would be wrong.
        let zone_map = if let Some(zm_handle) = regions.zone_map {
            let load = || -> crate::Result<crate::table::zone_map::ZoneMap> {
                let block = Block::from_file(
                    file_handle.as_ref(),
                    zm_handle,
                    crate::table::block::BlockIdentity {
                        table_id: metadata.id,
                        block_type: BlockType::ZoneMap,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &{
                        let t = match encryption.as_deref() {
                            Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                            None => crate::table::block::BlockTransform::PLAIN,
                        };
                        if let Some(ecc) = metadata.ecc_params {
                            t.with_ecc(ecc)
                        } else {
                            t
                        }
                    },
                )?;
                if block.header.block_type != BlockType::ZoneMap {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                crate::table::zone_map::ZoneMap::decode(&block.data)
            };
            match load() {
                Ok(m) => m,
                // Only a TRANSIENT read propagates so repair retries; a PERSISTENT
                // failure degrades this rebuildable, derived section to an empty
                // map (block-skip disabled) rather than failing the whole open.
                Err(crate::Error::Io(e)) if e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                Err(e) => {
                    log::warn!(
                        "zone-map section for table {:?} is unreadable ({e}); disabling block-skip",
                        metadata.id
                    );
                    rebuildable_section_degraded = true;
                    crate::table::zone_map::ZoneMap::default()
                }
            }
        } else {
            crate::table::zone_map::ZoneMap::default()
        };

        // Load the optional positional delete-bitmap section. Unlike the zone
        // map (a skip optimization that degrades safely to empty), the delete
        // bitmap is correctness data: silently dropping an unreadable one would
        // resurrect deleted rows, so normal recovery propagates the error and
        // fails. In salvage mode it degrades to empty ("all rows live, pending
        // recompaction") so the segment's data is still recoverable.
        let mut delete_bitmap_degraded = false;
        let mut delete_bitmap = if let Some(db_handle) = regions.delete_bitmap {
            let load = || -> crate::Result<crate::table::delete_bitmap::DeleteBitmap> {
                let block = Block::from_file(
                    file_handle.as_ref(),
                    db_handle,
                    crate::table::block::BlockIdentity {
                        table_id: metadata.id,
                        block_type: BlockType::DeleteBitmap,
                        dict_id: 0,
                        window_log: 0,
                    },
                    &{
                        let t = match encryption.as_deref() {
                            Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                            None => crate::table::block::BlockTransform::PLAIN,
                        };
                        if let Some(ecc) = metadata.ecc_params {
                            t.with_ecc(ecc)
                        } else {
                            t
                        }
                    },
                )?;
                if block.header.block_type != BlockType::DeleteBitmap {
                    return Err(crate::Error::InvalidTag((
                        "BlockType",
                        block.header.block_type.into(),
                    )));
                }
                crate::table::delete_bitmap::DeleteBitmap::decode(&block.data)
            };
            match load() {
                Ok(db) => db,
                // A TRANSIENT read propagates so repair retries: degrading the
                // delete mask to "all rows live" on a one-shot fault would
                // resurrect deleted rows in the rebuilt table. A PERSISTENT read
                // failure in salvage mode instead falls through to the
                // degradation branch below, where the caller's
                // `allow_delete_resurrection` opt-in is honored downstream (a
                // non-salvage open still fails closed via the final arm).
                Err(crate::Error::Io(e)) if e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                Err(e) if salvage => {
                    log::warn!(
                        "delete-bitmap for table {:?} is unreadable ({e}); salvaging all rows as live",
                        metadata.id
                    );
                    delete_bitmap_degraded = true;
                    crate::table::delete_bitmap::DeleteBitmap::default()
                }
                Err(e) => return Err(e),
            }
        } else {
            crate::table::delete_bitmap::DeleteBitmap::default()
        };

        // A delete-bitmap is positional; masking it on the read path needs each
        // block's row count, which comes from the zone map. The writer co-writes
        // both, so a bitmap without a zone map is a malformed SST (masking would
        // resolve every block to position 0 and corrupt visibility).
        if !delete_bitmap.is_empty() && zone_map.is_empty() {
            if salvage {
                // Without a readable zone map the bitmap cannot be positioned
                // (every block would resolve to row 0), so ignore it: show all
                // rows live rather than masking against the wrong positions.
                log::warn!(
                    "salvage: delete-bitmap for table {:?} has no readable zone map; ignoring it (all rows live)",
                    metadata.id
                );
                delete_bitmap_degraded = true;
                delete_bitmap = crate::table::delete_bitmap::DeleteBitmap::default();
            } else {
                return Err(crate::Error::InvalidHeader(
                    "delete-bitmap SST is missing its zone map",
                ));
            }
        }

        // Cache each data block's first global row position (file offset -> start
        // row) once on open, so positional delete masking is O(1) per block on
        // every read instead of recomputing cumulative row counts. Empty when the
        // segment has no deletes.
        let delete_block_starts = if delete_bitmap.is_empty() {
            None
        } else {
            let mut map = crate::HashMap::default();
            let mut start: u32 = 0;
            for keyed in block_index.iter() {
                let keyed = keyed?;
                map.insert(keyed.offset().0, start);
                let row_count = zone_map
                    .columns_for(keyed.offset().0)
                    .and_then(|stats| stats.first())
                    .map_or(0, |col| col.row_count);
                start = start.wrapping_add(row_count);
            }
            Some(alloc::sync::Arc::new(map))
        };
        let delete_bitmap = alloc::sync::Arc::new(delete_bitmap);

        // Load the optional retrieval-ribbon locator section and pair it with an
        // ordinal → data-block-handle map (the index yields handles in key/write
        // order, which is the writer's block_id ordering). Only when the section
        // exists, so non-locator tables pay nothing.
        // Load the optional retrieval-ribbon locator as a BEST-EFFORT point-read
        // accelerator: any failure (corrupt locator section, unexpected block
        // type, or a corrupt sub-index block hit while walking the index to pair
        // locators with their data-block handles) degrades to `None` rather than
        // failing the table open. Point reads then use the sorted-index path,
        // which isolates a corrupt sub-index partition to its own keys — so
        // enabling the locator by default does NOT widen the blast radius of a
        // partitioned-index corruption from "one partition" back to "whole SST".
        let locator_block = match regions.locator {
            None => None,
            Some(loc_handle) => match Block::from_file(
                file_handle.as_ref(),
                loc_handle,
                crate::table::block::BlockIdentity {
                    table_id: metadata.id,
                    block_type: BlockType::Locator,
                    dict_id: 0,
                    window_log: 0,
                },
                &{
                    let t = match encryption.as_deref() {
                        Some(enc) => crate::table::block::BlockTransform::Encrypted(enc),
                        None => crate::table::block::BlockTransform::PLAIN,
                    };
                    if let Some(ecc) = metadata.ecc_params {
                        t.with_ecc(ecc)
                    } else {
                        t
                    }
                },
            ) {
                Ok(block) => Some(block),
                // A TRANSIENT locator read during salvage must PROPAGATE, not
                // degrade: `rebuildable_section_degraded` makes `salvage_attempt`
                // read a delete-free table as possibly hiding deletion metadata and
                // fail the whole SST (`FeatureUnsupported`), quarantining an
                // otherwise-salvageable table instead of retrying the retryable
                // read. Mirrors the zone-map / seqno-bounds / delete-bitmap loaders.
                // A non-salvage open keeps the best-effort accelerator behavior
                // (degrade to the sorted-index path), so a flaky read there never
                // fails the open.
                Err(crate::Error::Io(e)) if salvage && e.kind().is_transient() => {
                    return Err(crate::Error::Io(e));
                }
                Err(e) => {
                    log::warn!("retrieval-ribbon locator disabled: section load failed: {e:?}");
                    rebuildable_section_degraded = true;
                    None
                }
            },
        };
        let locator_index = locator_block.and_then(|block| {
            if block.header.block_type != BlockType::Locator {
                log::warn!(
                    "retrieval-ribbon locator disabled: unexpected block type {:?}",
                    block.header.block_type
                );
                rebuildable_section_degraded = true;
                return None;
            }
            let blocks: Vec<BlockHandle> = block_index
                .iter()
                .map(|r| r.map(|kbh| *kbh.as_ref()))
                .collect::<crate::Result<Vec<_>>>()
                .inspect_err(|e| {
                    log::warn!("retrieval-ribbon locator disabled: index walk failed: {e:?}");
                })
                .ok()?;
            log::trace!(
                "Loaded retrieval-ribbon locator over {} blocks",
                blocks.len()
            );
            Some(crate::table::locator::LoadedLocator::new(
                block.data, blocks,
            ))
        });

        log::debug!(
            "Recovered table #{} from {}",
            metadata.id,
            file_path.display(),
        );

        Ok(Self(
            Arc::new(Inner {
                path: file_path,
                tree_id,

                metadata,
                regions,

                cache,

                file_accessor,
                fs,

                block_index: Arc::new(block_index),

                pinned_filter_index,

                pinned_filter_block,

                is_deleted: AtomicBool::default(),
                punch_on_drop: AtomicU64::new(u64::MAX),

                checksum,
                global_seqno,

                comparator,

                #[cfg(feature = "metrics")]
                metrics,

                cached_blob_bytes: AtomicU64::new(u64::MAX),
                read_count: AtomicU64::new(0),
                last_access_secs: AtomicU64::new(0),
                range_tombstones,
                block_layout,
                seqno_bounds,
                zone_map,
                delete_bitmap,
                delete_block_starts,
                delete_bitmap_degraded,
                rebuildable_section_degraded,
                locator_index,
                encryption,

                #[cfg(zstd_any)]
                zstd_dictionary,

                deletion_pause: once_cell::race::OnceBox::new(),

                #[cfg(feature = "std")]
                background_deleter: once_cell::race::OnceBox::new(),

                heal_hints: once_cell::race::OnceBox::new(),

                #[cfg(all(feature = "std", feature = "page_ecc"))]
                heal_lock: once_cell::race::OnceBox::new(),
            }),
            None,
            None,
        ))
    }

    /// The tight-space restriction lower bound for this version's view of the
    /// table, or `None` on the common path. `Some(bound)` means the data below
    /// `bound` has been punched out and superseded by a merged output table, so
    /// reads route keys `< bound` elsewhere and clamp this table's scans to
    /// start at `bound` (its index still references the punched prefix).
    #[must_use]
    pub(crate) fn restrict_lower_bound(&self) -> Option<&UserKey> {
        self.1.as_ref()
    }

    /// True when `key` is below this version's tight-space restriction bound, so
    /// a point read must miss here and fall through to the output table that
    /// superseded the punched-out prefix. Every point-read entry point
    /// ([`get`](Self::get), [`get_value`](Self::get_value),
    /// [`get_with_block`](Self::get_with_block)) consults this first.
    #[inline]
    fn is_below_restriction(&self, key: &[u8]) -> bool {
        self.1
            .as_ref()
            .is_some_and(|bound| self.comparator.compare(key, bound) == core::cmp::Ordering::Less)
    }

    /// Returns a view of this table restricted to keys `>= lower`, for
    /// tight-space compaction. Shares the same `Arc<Inner>` (no file re-open,
    /// no extra handle, no [`Drop`] interaction), so the original and the
    /// restricted view are one physical SST seen by different versions. The
    /// caller punches the data blocks below `lower` only after this view is
    /// durably installed.
    #[must_use]
    pub(crate) fn with_restriction(&self, lower: UserKey) -> Self {
        Self(self.0.clone(), Some(lower), self.2)
    }

    /// Re-opens this table as a DISTINCT [`Inner`](inner::Inner) (its own file
    /// handle and fresh drop / punch-on-drop atomics) restricted to keys
    /// `>= lower`. Used by tight-space compaction so the PRIOR unrestricted view
    /// can drop — and punch its consumed prefix on that drop — independently of
    /// this restricted view, which keeps serving the suffix. Heavier than
    /// [`with_restriction`](Self::with_restriction) (re-reads the footer + block
    /// index), which is acceptable on the opt-in, emergency tight-space path.
    ///
    /// Opens with its own file handle (no shared descriptor table) so the old
    /// view's handle lifecycle stays fully separate.
    ///
    /// The suffix digest captured here must stay consistent with what the caller
    /// installs in the manifest, so the caller MUST hold this table's
    /// [`heal_lock_arc`](Self::heal_lock_arc) across BOTH this call and the
    /// version edit that installs the returned view. Without it a concurrent
    /// patrol heal could refresh a suffix block between the capture and the
    /// install, binding the restricted manifest to a pre-heal digest that the
    /// post-restriction patrol can neither match nor re-attribute (its
    /// attestation binds whole-file, not suffix, digests).
    ///
    /// # Errors
    ///
    /// Propagates any error from re-opening the SST file.
    // std-only: computes the suffix digest via `crate::repair` (file I/O) and is
    // reached only from tight-space compaction, which is itself std-gated.
    #[cfg(feature = "std")]
    pub(crate) fn reopen_restricted(&self, lower: UserKey) -> crate::Result<Self> {
        // The restricted view's digest is the LIVE SUFFIX only: its
        // `[0, punch_offset)` prefix is hole-punched right after this view is
        // installed, so a whole-file digest (what `self.checksum()` holds) would
        // never match the punched file. Compute the suffix digest over the
        // CURRENT bytes NOW, while the file is still whole — the suffix is
        // untouched by the punch, and reading it fresh also folds in any in-place
        // heal that refreshed this table (so a tight-space swap installs the
        // healed suffix digest, never a stale pre-heal one).
        let punch_offset = self.punch_offset_for(&lower)?;
        let restricted_checksum = crate::Checksum::from_raw(
            crate::repair::compute_table_checksum_from(&*self.fs, &self.path, punch_offset)?,
        );
        let reopened = Self::recover(
            (*self.path).clone(),
            restricted_checksum,
            self.global_seqno,
            self.tree_id,
            self.metadata.id,
            self.cache.clone(),
            None,
            self.fs.clone(),
            self.pinned_filter_size() > 0,
            self.pinned_block_index_size() > 0,
            self.encryption.clone(),
            #[cfg(zstd_any)]
            self.zstd_dictionary.clone(),
            self.comparator.clone(),
            #[cfg(feature = "metrics")]
            self.metrics.clone(),
        )?;
        // The reopened `Inner` is DISTINCT from this one, so it starts without
        // the tree-installed shared gates. Carry them forward, or the restricted
        // view would lose them: without the checkpoint deletion pause a
        // checkpoint could link healed bytes under a stale digest, and without
        // the shared heal lock two patrols could heal + reconcile the same SST
        // concurrently and leave a clean file mismatched with the manifest.
        if let Some(pause) = self.0.deletion_pause.get() {
            reopened.install_deletion_pause(Arc::clone(pause));
        }
        #[cfg(all(feature = "std", feature = "page_ecc"))]
        reopened.install_heal_lock(self.heal_lock_arc());
        Ok(reopened.with_restriction(lower))
    }

    /// Marks this view to punch `[0, offset)` when its last `Arc` drops (see
    /// [`Inner::punch_on_drop`](inner::Inner::punch_on_drop)). Set on the PRIOR
    /// unrestricted view once a tight-space slice has been installed, so the
    /// consumed prefix is reclaimed exactly when no reader can still see it.
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "tight-space punch hook; its compaction consumer is std-gated, so unused under no_std"
        )
    )]
    pub(crate) fn mark_punch_on_drop(&self, offset: u64) {
        self.0
            .punch_on_drop
            .store(offset, core::sync::atomic::Ordering::Release);
    }

    /// Byte offset of the first data block whose last key reaches `key`. Punching
    /// `[0, offset)` reclaims every data block strictly below `key` while leaving
    /// the straddling block and the index / footer (which follow all data blocks)
    /// intact. When `key` is past the last block's keys, returns the end of the
    /// data region (every data block is punchable).
    ///
    /// # Errors
    ///
    /// Propagates a block-index read error.
    #[cfg_attr(
        not(feature = "std"),
        allow(
            dead_code,
            reason = "tight-space punch offset; its compaction consumer is std-gated, so unused under no_std"
        )
    )]
    pub(crate) fn punch_offset_for(&self, key: &[u8]) -> crate::Result<u64> {
        let mut data_end = 0u64;
        for handle in self.block_index.iter() {
            let handle = handle?;
            if self.comparator.compare(handle.end_key(), key) != core::cmp::Ordering::Less {
                return Ok(handle.offset().0);
            }
            data_end = handle.offset().0 + u64::from(handle.size());
        }
        Ok(data_end)
    }

    /// Byte offset of this view's first LIVE data block: `0` for a normal table,
    /// or the punch offset for a tight-space RESTRICTED view (its `[0, offset)`
    /// data blocks are hole-punched and read as zeros). A data block or section
    /// entry at a lower offset is DEAD (superseded, never read), so the
    /// disk-fresh verification gates skip it.
    ///
    /// # Errors
    ///
    /// Propagates a restricted view's punch-offset lookup failure instead of
    /// grading it `0`: falling back to `0` would make the verification gates walk
    /// the hole-punched prefix (which reads as zeros), report its blocks as
    /// structural corruption, and — on the heal reconcile path — strip a valid
    /// heal attestation for what is really a transient partitioned-index read.
    /// Propagating keeps a transient failure inconclusive (the marker survives for
    /// the next patrol). A normal (unrestricted) table never calls the fallible
    /// lookup, so it always returns `Ok(0)`.
    pub(crate) fn punch_offset(&self) -> crate::Result<u64> {
        match self.restrict_lower_bound() {
            Some(bound) => self.punch_offset_for(bound),
            None => Ok(0),
        }
    }

    /// The whole-file digest for a normal table, or the LIVE-SUFFIX digest for a
    /// tight-space RESTRICTED view: its `[0, punch_offset)` prefix is hole-
    /// punched once a superseding output table owns those keys, so hashing the
    /// whole physical file would fold the punched (zeroed) prefix into the
    /// digest and never match the manifest. Digesting only `[punch_offset, end)`
    /// keeps the checksum stable across the punch, and it is what
    /// [`reopen_restricted`](Self::reopen_restricted) records and what
    /// verification / heal reconciliation must recompute for a restricted view.
    #[cfg(feature = "std")]
    pub(crate) fn live_region_checksum(&self) -> crate::Result<Checksum> {
        let start = match self.restrict_lower_bound() {
            Some(bound) => self.punch_offset_for(bound)?,
            None => 0,
        };
        crate::repair::compute_table_checksum_from(&*self.fs, &self.path, start)
            .map(Checksum::from_raw)
    }

    /// Installs the tree-wide deletion pause used by checkpoints.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree
    /// after recovery and after compaction registers freshly-built tables.
    pub(crate) fn install_deletion_pause(&self, pause: Arc<crate::deletion_pause::DeletionPause>) {
        let _ = self.0.deletion_pause.set(Box::new(pause));
    }

    /// The shared heal-serialization lock for this table, lazily created on
    /// first use. Held by the patrol scrub across the whole scan-to-reconcile
    /// span so two overlapping heals cannot race the link-count probe or the
    /// digest reconciliation. Shared by STABLE table identity:
    /// [`reopen_restricted`](Self::reopen_restricted) propagates it into the
    /// distinct `Inner` it creates.
    #[cfg(all(feature = "std", feature = "page_ecc"))]
    pub(crate) fn heal_lock_arc(&self) -> Arc<parking_lot::Mutex<()>> {
        Arc::clone(
            self.0
                .heal_lock
                .get_or_init(|| Box::new(Arc::new(parking_lot::Mutex::new(())))),
        )
    }

    /// Installs a shared heal lock, so a re-opened view serializes heals against
    /// the original. Idempotent: a second call is a no-op.
    #[cfg(all(feature = "std", feature = "page_ecc"))]
    pub(crate) fn install_heal_lock(&self, lock: Arc<parking_lot::Mutex<()>>) {
        let _ = self.0.heal_lock.set(Box::new(lock));
    }

    /// Installs the tree-wide background file deleter.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree after
    /// recovery and after compaction registers freshly-built tables, so an
    /// obsolete SST's `unlink` runs off the foreground path while its blocks
    /// are reclaimed synchronously at Drop.
    #[cfg(feature = "std")]
    pub(crate) fn install_background_deleter(&self, deleter: Arc<crate::BackgroundDeleter>) {
        let _ = self.0.background_deleter.set(Box::new(deleter));
    }

    /// Installs the tree-wide ECC heal-hint sink.
    ///
    /// Idempotent: a second call is a no-op. Called by the owning tree after
    /// recovery and after compaction registers freshly-built tables, so a
    /// confirmed-persistent ECC correction on a read can queue this SST for a
    /// healing recompaction.
    pub(crate) fn install_heal_hints(&self, hints: Arc<crate::heal_hints::HealHints>) {
        let _ = self.0.heal_hints.set(Box::new(hints));
    }

    #[must_use]
    pub fn checksum(&self) -> Checksum {
        // The refreshed digest (an in-place heal changed the bytes after
        // recovery) supersedes the one captured at recovery.
        self.2.unwrap_or(self.0.checksum)
    }

    /// A view of this table whose full-file checksum is `checksum`: an
    /// in-place heal changed the file's bytes, and installing this view into
    /// a new version makes the diff persist the refreshed digest to the
    /// manifest (see [`crate::Version::with_refreshed_table_checksum`]).
    #[must_use]
    pub(crate) fn with_refreshed_checksum(&self, checksum: Checksum) -> Self {
        Self(self.0.clone(), self.1.clone(), Some(checksum))
    }

    /// Read `len` bytes from the cursor position with checked arithmetic.
    /// Uses `.get()` instead of direct indexing to satisfy `clippy::indexing_slicing`.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block sizes are bounded well within usize on all supported platforms"
    )]
    fn read_checked_slice(
        cursor: &mut crate::io::Cursor<&[u8]>,
        field: &'static str,
        len: usize,
    ) -> crate::Result<Vec<u8>> {
        let offset = cursor.position();
        let data = cursor.get_ref();
        let pos = offset as usize;
        let end_pos = pos
            .checked_add(len)
            .ok_or(crate::Error::RangeTombstoneDecode { field, offset })?;
        let buf = data
            .get(pos..end_pos)
            .ok_or(crate::Error::RangeTombstoneDecode { field, offset })?
            .to_vec();
        cursor.set_position(end_pos as u64);
        Ok(buf)
    }

    /// Decodes range tombstones from a raw block.
    ///
    /// Wire format (repeated): `[start_len:u16_le][start][end_len:u16_le][end][seqno:u64_le]`
    ///
    /// # Errors
    ///
    /// Will return `Err` if the block data is malformed.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "block sizes are bounded well within usize on all supported platforms"
    )]
    fn decode_range_tombstones(
        block: &Block,
        comparator: &dyn crate::comparator::UserComparator,
    ) -> crate::Result<Vec<RangeTombstone>> {
        use crate::io::{Cursor, LE, ReadBytesExt};

        let mut tombstones = Vec::new();
        let data = block.data.as_ref();

        // A dedicated RT block with empty payload is corruption — the writer
        // only creates an RT block handle when at least one tombstone exists.
        if data.is_empty() {
            log::error!("Range tombstone block: missing start_len");
            return Err(crate::Error::RangeTombstoneDecode {
                field: "start_len",
                offset: 0,
            });
        }

        let mut cursor = Cursor::new(data);

        while (cursor.position() as usize) < data.len() {
            let entry_offset = cursor.position();
            let start_len_offset = entry_offset;
            let start_len =
                cursor
                    .read_u16::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "start_len",
                        offset: start_len_offset,
                    })? as usize;

            // Validate length against remaining data before allocating
            let remaining = data.len() - cursor.position() as usize;
            if start_len > remaining {
                log::error!(
                    "Range tombstone block: start_len {start_len} exceeds remaining {remaining}"
                );
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "start_len",
                    offset: start_len_offset,
                });
            }

            // Extract validated slice from cursor position.
            // Using .get() instead of direct indexing to satisfy clippy::indexing_slicing.
            let start_buf = Self::read_checked_slice(&mut cursor, "start", start_len)?;

            let end_len_offset = cursor.position();
            let end_len =
                cursor
                    .read_u16::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "end_len",
                        offset: end_len_offset,
                    })? as usize;

            let remaining = data.len() - cursor.position() as usize;
            if end_len > remaining {
                log::error!(
                    "Range tombstone block: end_len {end_len} exceeds remaining {remaining}"
                );
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "end_len",
                    offset: end_len_offset,
                });
            }

            let end_buf = Self::read_checked_slice(&mut cursor, "end", end_len)?;

            let seqno_offset = cursor.position();
            let seqno =
                cursor
                    .read_u64::<LE>()
                    .map_err(|_| crate::Error::RangeTombstoneDecode {
                        field: "seqno",
                        offset: seqno_offset,
                    })?;

            let start = UserKey::from(start_buf);
            let end = UserKey::from(end_buf);

            // Validate invariant: start < end using the tree's comparator
            // (reject corrupted or misordered intervals)
            if comparator.compare(&start, &end) != core::cmp::Ordering::Less {
                log::error!("Range tombstone block: invalid interval (start >= end)");
                return Err(crate::Error::RangeTombstoneDecode {
                    field: "interval",
                    offset: entry_offset,
                });
            }

            tombstones.push(RangeTombstone::new(start, end, seqno));
        }

        Ok(tombstones)
    }

    /// Returns the range tombstones stored in this table.
    #[must_use]
    pub(crate) fn range_tombstones(&self) -> &[RangeTombstone] {
        &self.0.range_tombstones
    }

    pub(crate) fn mark_as_deleted(&self) {
        self.0
            .is_deleted
            .store(true, core::sync::atomic::Ordering::Release);
    }

    /// Checks if a key range overlaps (partially or fully) with this table's key range.
    pub(crate) fn check_key_range_overlap_cmp(
        &self,
        bounds: &(Bound<&[u8]>, Bound<&[u8]>),
        cmp: &dyn crate::comparator::UserComparator,
    ) -> bool {
        if !self
            .metadata
            .key_range
            .overlaps_with_bounds_cmp(bounds, cmp)
        {
            return false;
        }

        // Tight-space restriction: the live range is `[bound, hi]`. If the
        // query's upper bound is strictly below `bound`, the query targets only
        // the punched-out prefix (now served by a superseding output table), so
        // this table does not overlap.
        if let Some(bound) = &self.1 {
            match bounds.1 {
                Bound::Included(end) => {
                    if cmp.compare(end, bound) == core::cmp::Ordering::Less {
                        return false;
                    }
                }
                Bound::Excluded(end) => {
                    // end <= bound: every key the query can reach is below the
                    // live range.
                    if cmp.compare(end, bound) != core::cmp::Ordering::Greater {
                        return false;
                    }
                }
                Bound::Unbounded => {}
            }
        }

        true
    }

    /// Checks the full-table bloom filter for a hash value.
    ///
    /// Returns `Ok(true)` if the hash may exist in the filter (or if no full
    /// filter is available), `Ok(false)` if the hash is definitely absent.
    ///
    /// Handles full (non-partitioned) filters directly. Partitioned / TLI
    /// filters are keyed by user key, not raw hash, so this method returns
    /// `Ok(true)` conservatively for those types.
    fn bloom_may_contain_hash(&self, hash: u64) -> crate::Result<bool> {
        // Full (non-partitioned) filter — single bloom covers the entire table
        if let Some(block) = &self.pinned_filter_block {
            return block.maybe_contains_hash(hash);
        }

        // Partitioned / TLI filters: partition index is keyed by user key, not
        // raw hash — we would need to scan ALL partitions to check,
        // which is O(partitions) I/O and defeats the purpose of bloom skip.
        // Returning Ok(true) is correct (conservative: segment is NOT skipped).
        if self.pinned_filter_index.is_some() || self.regions.filter_tli.is_some() {
            return Ok(true);
        }

        // Unpinned full filter — load from disk.
        // Safe: if we reach here, filter_tli is None (no partitioned filter),
        // so regions.filter is a single full-table bloom, not a concatenation.
        if let Some(filter_block_handle) = &self.regions.filter {
            let block = self.load_block(
                filter_block_handle,
                BlockType::Filter,
                CompressionType::None, // NOTE: Filter blocks are never compressed (crate invariant)
                #[cfg(zstd_any)]
                None,
            )?;
            let block = FilterBlock::new(block);
            return block.maybe_contains_hash(hash);
        }

        // No filter available — cannot rule out the hash
        Ok(true)
    }

    /// Checks the bloom filter for a prefix hash.
    ///
    /// Returns `Ok(true)` if the prefix may exist in this table (or if no
    /// filter is available), `Ok(false)` if the prefix is definitely absent.
    ///
    /// This is used by prefix scans to skip segments that contain no keys
    /// with a matching prefix. The prefix must have been indexed at write
    /// time via a [`PrefixExtractor`](crate::PrefixExtractor).
    pub(crate) fn maybe_contains_prefix(&self, prefix_hash: u64) -> crate::Result<bool> {
        self.bloom_may_contain_hash(prefix_hash)
    }

    /// Checks the bloom filter for a precomputed key hash.
    ///
    /// Returns `Ok(true)` if the key may exist in this table (or if no
    /// filter is available), `Ok(false)` if the key is definitely absent.
    ///
    /// Used by the point-read merge pipeline to pre-filter disk tables
    /// before building range iterators. For partitioned or TLI filter
    /// configurations, the underlying check returns `Ok(true)` conservatively,
    /// so pre-filtering is best-effort and configuration-dependent.
    pub(crate) fn bloom_may_contain_key_hash(&self, key_hash: u64) -> crate::Result<bool> {
        self.bloom_may_contain_hash(key_hash)
    }

    /// Checks the bloom filter for a key, with partition-aware seeking.
    ///
    /// Unlike [`bloom_may_contain_key_hash`](Self::bloom_may_contain_key_hash)
    /// which falls back to `Ok(true)` for partitioned filters, this method
    /// uses the user key to seek the partition index and check only the
    /// matching partition's bloom filter.
    ///
    /// `key_hash` must be the xxh3 hash of `key` (pre-computed by the caller
    /// to avoid redundant hashing — same pattern as [`Table::get`]).
    pub(crate) fn bloom_may_contain_key(&self, key: &[u8], key_hash: u64) -> crate::Result<bool> {
        debug_assert_eq!(
            crate::hash::hash64(key),
            key_hash,
            "bloom_may_contain_key: key_hash must be crate::hash::hash64(key)"
        );

        // Full (non-partitioned) filter — delegate to hash-only path.
        // A table has either pinned_filter_block (full) or pinned_filter_index
        // (partitioned), never both — checked at construction time.
        if self.pinned_filter_block.is_some() {
            return self.bloom_may_contain_hash(key_hash);
        }

        // Partitioned filter with pinned TLI — seek to the matching partition
        if let Some(filter_idx) = &self.pinned_filter_index {
            let mut iter = filter_idx.iter(self.comparator.clone());
            iter.seek(key, crate::seqno::MAX_SEQNO);

            if let Some(filter_block_handle) = iter.next() {
                let filter_block_handle = filter_block_handle.materialize(filter_idx.as_slice());

                let block = self.load_block(
                    &filter_block_handle.into_inner(),
                    BlockType::Filter,
                    CompressionType::None,
                    #[cfg(zstd_any)]
                    None,
                )?;
                let block = FilterBlock::new(block);
                return block.maybe_contains_hash(key_hash);
            }

            // iter.next() == None means the key is beyond all partition
            // boundaries (seek found no ceiling entry in the TLI, which is
            // ordered by each partition's last user key). The key cannot
            // exist in this table. Same logic as Table::get (line ~265).
            return Ok(false);
        }

        // Unpinned filter — fall through to hash-only path (handles both
        // unpinned full filters and the no-filter case)
        self.bloom_may_contain_hash(key_hash)
    }

    /// Returns the highest effective sequence number in the table.
    ///
    /// For tables produced by flush/compaction (`global_seqno == 0`), this
    /// returns the highest item seqno directly.
    ///
    /// For tables produced by bulk ingestion (`global_seqno > 0`), items
    /// are written with local seqno 0 and the table carries a global offset.
    /// The effective seqno of each item is `global_seqno + local_seqno`,
    /// which mirrors the translation in [`Table::get`].
    #[must_use]
    pub fn get_highest_seqno(&self) -> SeqNo {
        self.metadata.seqnos.1 + self.global_seqno()
    }

    /// The highest LOCAL (on-disk, pre-`global_seqno`) sequence number of any
    /// entry, `0` for an empty table. Unlike [`get_highest_seqno`], it does NOT
    /// add the offset. Manifest repair uses it as the LEGACY bulk-ingest
    /// signature: a table of unknown provenance whose entries all sit at local
    /// seqno 0 may itself be a legacy bulk-ingested table.
    ///
    /// [`get_highest_seqno`]: Self::get_highest_seqno
    #[must_use]
    pub(crate) fn max_local_seqno(&self) -> SeqNo {
        self.metadata.seqnos.1
    }

    /// Returns the highest sequence number from KV entries only,
    /// excluding range tombstone seqnos.
    ///
    /// This enables more aggressive table-skip: a covering RT stored
    /// in the same table can trigger skip because its seqno may exceed
    /// the KV-only max even though it doesn't exceed the overall max.
    ///
    /// For tables written before this field was introduced, falls back
    /// to `get_highest_seqno()` (conservative but correct).
    #[must_use]
    pub fn get_highest_kv_seqno(&self) -> SeqNo {
        self.metadata.highest_kv_seqno + self.global_seqno()
    }

    /// Returns the number of tombstone markers in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_count(&self) -> u64 {
        self.metadata.tombstone_count
    }

    /// Returns the number of weak (single delete) tombstones in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn weak_tombstone_count(&self) -> u64 {
        self.metadata.weak_tombstone_count
    }

    /// Returns the number of value entries reclaimable once weak tombstones can be GC'd.
    #[must_use]
    #[doc(hidden)]
    pub fn weak_tombstone_reclaimable(&self) -> u64 {
        self.metadata.weak_tombstone_reclaimable
    }

    /// Returns the ratio of tombstone markers in the `Table`.
    #[must_use]
    #[doc(hidden)]
    pub fn tombstone_ratio(&self) -> f32 {
        todo!()

        //  self.metadata.tombstone_count as f32 / self.metadata.key_count as f32
    }
}
