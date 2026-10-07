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
    /// The filesystem the tables are reachable through.
    pub fs: Arc<dyn Fs>,
    /// The tree's at-rest encryption provider.
    pub encryption: Option<Arc<dyn EncryptionProvider>>,
    /// Every dictionary the tree's tables may name.
    #[cfg(zstd_any)]
    pub dictionaries: crate::compression::ZstdDictionaries,
    /// The tree's key ordering.
    pub comparator: SharedComparator,
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
    /// a restricted table) does not hash to the manifest's checksum.
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
        if digest != record.checksum {
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
