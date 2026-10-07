// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! One table's sections, read with the decoders the read path uses.

use super::TableRecord;
use crate::cache::Cache;
use crate::comparator::SharedComparator;
use crate::encryption::EncryptionProvider;
use crate::fs::Fs;
use crate::path::Path;
use crate::table::{RecoverParams, Table};
use crate::{Checksum, InternalValue, SeqNo, TableId, UserKey, UserValue};
use alloc::sync::Arc;

/// One entry of a table's table of contents.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    /// The section's name, as stored.
    pub name: Vec<u8>,
    /// Where the section starts in the file.
    pub offset: u64,
    /// The section's length in bytes.
    pub len: u64,
}

/// A block an index addresses: the last key and seqno it holds and where it
/// lies in the file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockRef {
    /// The user key of the block's last entry.
    pub end_key: UserKey,
    /// The seqno of the block's last entry.
    pub seqno: SeqNo,
    /// Where the block's frame starts in the file.
    pub offset: u64,
    /// The frame's length on disk, header and parity trailer included.
    pub size: u32,
}

/// One block as the read path verifies it, still compressed.
///
/// The checksum is checked, damage the parity trailer covers is repaired and
/// the payload is decrypted. A converter frames this payload anew, so a block
/// healed here is carried forward healed and a block the parity cannot
/// repair stops the conversion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifiedFrame {
    /// The block's role.
    pub block_type: crate::table::block::BlockType,
    /// The payload's length once decompressed.
    pub uncompressed_length: u32,
    /// The verified payload, compressed as stored.
    pub payload: Vec<u8>,
    /// What the parity trailer found.
    pub ecc_status: crate::table::block::EccStatus,
    /// How the payload was repaired, when it was.
    pub ecc_recovery: Option<crate::table::block::EccRecoveryKind>,
}

/// A range tombstone: every key in `[start, end)` deleted at `seqno`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RangeDelete {
    /// The first key deleted.
    pub start: UserKey,
    /// The first key past the deleted range.
    pub end: UserKey,
    /// The tombstone's local seqno.
    pub seqno: SeqNo,
}

/// What a `BuRR` solution stores per key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BurrKind {
    /// A fingerprint: the solution answers membership.
    Membership,
    /// A caller value: the solution answers retrieval (the locator).
    Retrieval,
}

/// One layer of a `BuRR` solution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BurrLayer {
    /// The layer's slot count.
    pub m: u32,
    /// One bumping threshold per block of slots.
    pub thresholds: Vec<u8>,
    /// The solution rows, one per slot, each holding its `r` result bits.
    pub rows: Vec<u64>,
}

/// A `BuRR` solution: the parameters every layer shares and the layers, in
/// the order a probe walks them. A layer's seed derives from `root_seed` and
/// the layer's position.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BurrSolution {
    /// What the solution stores.
    pub kind: BurrKind,
    /// Result bits per key.
    pub r: u8,
    /// Band width.
    pub w: u8,
    /// Slots per threshold block.
    pub b: u8,
    /// The seed the layer seeds derive from.
    pub root_seed: u64,
    /// The layers.
    pub layers: Vec<BurrLayer>,
}

/// One partition of a partitioned membership filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterPartition {
    /// The partition's entry in the filter's partition index: the last key
    /// and seqno it covers and where its block lies.
    pub entry: BlockRef,
    /// The partition's filter.
    pub solution: BurrSolution,
}

/// A table's membership filter.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Filter {
    /// One filter over the whole table.
    Full(BurrSolution),
    /// Filters over key ranges, in key order.
    Partitioned(Vec<FilterPartition>),
}

/// A table's retrieval locator: for every key, the data block (and slot)
/// holding its newest version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Locator {
    /// What a slot addresses, as stored: 0 = a restart index, 1 = an entry
    /// index, 2 = none (the block alone).
    pub precision: u8,
    /// Bits of a located value that hold the block ordinal.
    pub block_id_bits: u8,
    /// Bits of a located value that hold the slot.
    pub slot_bits: u8,
    /// The retrieval solution.
    pub solution: BurrSolution,
}

/// What every table of one tree is read with.
pub struct TableContext {
    pub(crate) fs: Arc<dyn Fs>,
    pub(crate) encryption: Option<Arc<dyn EncryptionProvider>>,
    #[cfg(zstd_any)]
    pub(crate) dictionaries: crate::compression::ZstdDictionaries,
    pub(crate) comparator: SharedComparator,
}

impl TableContext {
    /// The context for reading the tables of the tree whose manifest `state`
    /// describes, through `fs`, decrypting with `encryption`, decompressing
    /// with `dictionaries`, ordering keys with `comparator`.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::ComparatorMismatch`] when `comparator` is not the
    /// one the tree was written under: its indexes and blocks would be read in
    /// the wrong order.
    pub fn new(
        state: &super::ManifestState,
        fs: Arc<dyn Fs>,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        #[cfg(zstd_any)] dictionaries: crate::compression::ZstdDictionaries,
        comparator: SharedComparator,
    ) -> crate::Result<Self> {
        let supplied = comparator.name();
        if state.comparator_name != supplied {
            return Err(crate::Error::ComparatorMismatch {
                stored: state.comparator_name.clone(),
                supplied,
            });
        }
        Ok(Self {
            fs,
            encryption,
            #[cfg(zstd_any)]
            dictionaries,
            comparator,
        })
    }
}

/// A table opened for export: nothing is cached, pinned or written, and the
/// file's digest has been checked against the manifest's.
pub struct TableExport {
    table: Table,
}

impl TableExport {
    /// Opens the table at `path` that the manifest records as `record`,
    /// restricted to keys at or above `restriction` when the manifest holds a
    /// tight-space bound for it.
    ///
    /// # Errors
    ///
    /// Returns any error opening the table would, and
    /// [`crate::Error::ChecksumMismatch`] when the file (its live suffix, for
    /// a restricted table) does not hash to the manifest's checksum, even
    /// with every repair its blocks' parity can make applied, and no heal
    /// attestation binds what it does hash to to that checksum; and
    /// [`crate::Error::PageEccUnrecoverable`] for a mismatching file holding a
    /// block its parity cannot repair.
    pub fn open(
        path: &Path,
        record: &TableRecord,
        restriction: Option<&UserKey>,
        context: &TableContext,
    ) -> crate::Result<Self> {
        let mut params = RecoverParams::new(
            path.to_path_buf(),
            Checksum::from_raw(record.checksum),
            record.id,
            context.fs.clone(),
            context.comparator.clone(),
            Arc::new(Cache::with_capacity_bytes(0)),
        );
        params.global_seqno = record.global_seqno;
        params.encryption.clone_from(&context.encryption);
        #[cfg(zstd_any)]
        {
            params.zstd_dictionaries = context.dictionaries.clone();
        }
        let table = Table::recover(params)?;
        let table = match restriction {
            Some(bound) => table.with_restriction(bound.clone()),
            None => table,
        };

        let live_from = table.punch_offset()?;
        let digest = crate::repair::compute_table_checksum_from(&*context.fs, path, live_from)?;
        // The bytes the manifest describes, or the bytes an in-place heal that
        // crashed before its manifest refresh left behind, which its
        // attestation binds to the manifest's digest.
        let describes = |candidate: u128| {
            candidate == record.checksum
                || matches!(
                    crate::scrub::heal_attest::attests(
                        &*context.fs,
                        path,
                        context.encryption.as_deref(),
                        record.id,
                        Checksum::from_raw(candidate),
                        Checksum::from_raw(record.checksum),
                    ),
                    crate::scrub::heal_attest::AttestResult::Attests
                )
        };
        // Damage the blocks' parity repairs is accounted for too: every read
        // here hands those blocks back repaired. Without the parity codec no
        // damage can be.
        #[cfg(feature = "page_ecc")]
        let accounted = describes(digest) || describes(table.export_repaired_digest()?);
        #[cfg(not(feature = "page_ecc"))]
        let accounted = describes(digest);
        if !accounted {
            return Err(crate::Error::ChecksumMismatch {
                got: Checksum::from_raw(digest),
                expected: Checksum::from_raw(record.checksum),
            });
        }
        Ok(Self { table })
    }

    /// The table's id.
    #[must_use]
    pub fn id(&self) -> TableId {
        self.table.id()
    }

    /// Whether the table's data blocks are columnar.
    #[must_use]
    pub fn is_columnar(&self) -> bool {
        self.table.metadata.columnar
    }

    /// The offset of the first data block that holds live data: `0`, or for a
    /// restricted table the start of the first block its bound keeps. The
    /// blocks before it were punched out and read as zeros.
    ///
    /// # Errors
    ///
    /// Propagates the index walk that finds the bound's block.
    pub fn live_from(&self) -> crate::Result<u64> {
        self.table.punch_offset()
    }

    /// The table of contents, in file order.
    ///
    /// # Errors
    ///
    /// Propagates the read and decode of the trailer.
    pub fn sections(&self) -> crate::Result<Vec<Section>> {
        self.table.export_sections()
    }

    /// Every key and value of the meta block, in key order, from the copy the
    /// open read.
    ///
    /// # Errors
    ///
    /// Propagates the block read, and fails when neither meta copy decodes to
    /// the metadata the open read.
    pub fn meta(&self) -> crate::Result<Vec<(UserKey, UserValue)>> {
        self.table.export_meta()
    }

    /// Every data block, in key order, as the index lists it, punched prefix
    /// included.
    ///
    /// # Errors
    ///
    /// Propagates the index walk.
    pub fn data_blocks(&self) -> crate::Result<Vec<BlockRef>> {
        self.table.export_data_blocks()
    }

    /// Every entry `block` holds, in stored order, with its local seqno (the
    /// manifest's global seqno not applied) and no delete mask: a columnar
    /// block's rows come back whether or not the delete bitmap marks them.
    ///
    /// # Errors
    ///
    /// Propagates the block read, its verification and decode.
    pub fn rows(&self, block: &BlockRef) -> crate::Result<Vec<InternalValue>> {
        self.table.export_rows(block)
    }

    /// The decoded column batch of the columnar data block `block`, every row
    /// included whether or not the delete bitmap marks it.
    ///
    /// # Errors
    ///
    /// Propagates the block read and decode, and refuses a row table.
    #[cfg(feature = "columnar")]
    pub fn columnar_batch(
        &self,
        block: &BlockRef,
    ) -> crate::Result<crate::table::columnar::ColumnBatch> {
        self.table.export_columnar_batch(block)
    }

    /// The rows of the columnar data block `block` the delete bitmap marks
    /// deleted, as indexes into the block's batch, ascending. Local to the
    /// block, so they stay valid for a restricted table whose blocks before
    /// [`live_from`](Self::live_from) cannot be read.
    ///
    /// # Errors
    ///
    /// Propagates the block read, and fails when the table has deletes but no
    /// first row recorded for the block.
    #[cfg(feature = "columnar")]
    pub fn deleted_rows_in(&self, block: &BlockRef) -> crate::Result<Vec<u32>> {
        self.table.export_deleted_rows_in(block)
    }

    /// The table's range tombstones, as stored (not clamped to a restriction).
    #[must_use]
    pub fn range_tombstones(&self) -> Vec<RangeDelete> {
        self.table.export_range_tombstones()
    }

    /// The blob files the table's values point into, with the bytes they
    /// reference in each, or `None` when the table records none.
    ///
    /// # Errors
    ///
    /// Propagates the section read and rejects a malformed section.
    pub fn linked_blob_files(
        &self,
    ) -> crate::Result<Option<Vec<crate::table::writer::LinkedFile>>> {
        self.table.list_blob_file_references()
    }

    /// The block of `block_type` whose frame spans `size` bytes at `offset`,
    /// verified, repaired and decrypted by the read path's own block read, and
    /// still compressed.
    ///
    /// # Errors
    ///
    /// Propagates the read, a checksum failure the parity cannot repair, a
    /// decrypt failure, and rejects a block that carries another role.
    pub fn frame(
        &self,
        offset: u64,
        size: u32,
        block_type: crate::table::block::BlockType,
    ) -> crate::Result<VerifiedFrame> {
        self.table.export_frame(offset, size, block_type)
    }

    /// The membership filter, or `None` when the table has none.
    ///
    /// # Errors
    ///
    /// Propagates the section reads, and rejects a filter block whose payload
    /// is not a membership solution.
    pub fn filter(&self) -> crate::Result<Option<Filter>> {
        self.table.export_filter()
    }

    /// The retrieval locator, or `None` when the table has none.
    ///
    /// # Errors
    ///
    /// Propagates the section read, and rejects a section whose header or
    /// payload does not decode.
    pub fn locator(&self) -> crate::Result<Option<Locator>> {
        self.table.export_locator()
    }
}

/// Decodes a `BuRR` payload of the given kind.
///
/// # Errors
///
/// Returns [`crate::Error::InvalidHeader`] or [`crate::Error::InvalidTag`]
/// for a payload that is truncated, inconsistent, or of the other kind.
pub fn decode_burr(bytes: &[u8], kind: BurrKind) -> crate::Result<BurrSolution> {
    use crate::table::filter::ribbon::burr::wire;

    let tag = match kind {
        BurrKind::Membership => wire::BURR_FILTER_TYPE_BYTE,
        BurrKind::Retrieval => wire::BURR_RETRIEVAL_TYPE_BYTE,
    };
    let decoded = wire::decode_as(bytes, tag)?;
    let layers = decoded
        .layers
        .iter()
        .map(|layer| {
            let m = u32::try_from(layer.m)
                .map_err(|_| crate::Error::InvalidHeader("BurrFilter layer m"))?;
            // The decode checked the payload is exactly `m` words of eight bytes.
            let rows = layer
                .z_bytes
                .chunks_exact(8)
                .map(|word| {
                    <[u8; 8]>::try_from(word)
                        .map(u64::from_le_bytes)
                        .map_err(|_| crate::Error::InvalidHeader("BurrFilter layer payload"))
                })
                .collect::<crate::Result<Vec<u64>>>()?;
            Ok(BurrLayer {
                m,
                thresholds: layer.thresholds.to_vec(),
                rows,
            })
        })
        .collect::<crate::Result<Vec<BurrLayer>>>()?;
    Ok(BurrSolution {
        kind,
        r: decoded.r,
        w: decoded.w,
        b: decoded.b,
        root_seed: decoded.root_seed,
        layers,
    })
}
