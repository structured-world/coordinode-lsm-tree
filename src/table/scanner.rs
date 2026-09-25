// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use alloc::boxed::Box;
use alloc::sync::Arc;

use super::{Block, DataBlock};
use crate::io::BufReader;
use crate::path::Path;
use crate::{
    CompressionType, InternalValue, SeqNo,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    fs::{FileHint, Fs, FsFile, FsOpenOptions},
    table::{block::BlockType, iter::OwnedDataBlockIter},
};

/// Table reader that is optimized for consuming an entire table
pub struct Scanner {
    reader: BufReader<Box<dyn FsFile>>,
    iter: OwnedDataBlockIter,

    compression: CompressionType,
    block_count: usize,
    read_count: usize,

    global_seqno: SeqNo,

    encryption: Option<Arc<dyn EncryptionProvider>>,
    comparator: SharedComparator,

    /// Per-SST Page-ECC scheme from table metadata: data blocks omit the
    /// `block_flags` byte, so the scanner's block-read transform is told
    /// here the parity scheme (sizing + recovery). `None` = no parity.
    ecc: Option<crate::table::block::EccParams>,
    /// Per-SST per-KV-footer flag (`kv_checksum_algo.is_some()`): supplied to
    /// `from_loaded` so it strips the footer (data blocks omit the byte).
    has_kv_footer: bool,

    #[cfg(zstd_any)]
    zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,

    /// Table id of the SST being scanned; threaded through to
    /// per-block reads via `BlockIdentity`.
    table_id: crate::TableId,

    /// Whether this SST stores columnar (PAX) row groups. When set, each
    /// group is read as its directory and pages and reconstructed into a
    /// row-major block so the scan iterator is unchanged.
    columnar: bool,
    /// Data-block restart interval, used to re-encode a reconstructed columnar
    /// block (matches the value the SST recorded).
    restart_interval: u8,

    /// Tight-space restriction lower bound of the view being scanned. The
    /// caller already positions the scan at the first live block; this filter
    /// drops the sub-bound entries of that STRADDLING block (their
    /// authoritative copies live in the superseding slice output).
    lower_bound: Option<crate::UserKey>,
    /// `true` while entries may still fall below `lower_bound`. Keys ascend,
    /// so once one entry reaches the bound the comparison is retired.
    filtering_below_bound: bool,

    /// The tags the index names the groups still to be read by, in order: a
    /// columnar scan streams the data without the index, and refuses a group
    /// whose directory does not carry the tag its entry names. Empty for a
    /// row-major table.
    group_tags: alloc::vec::IntoIter<Option<core::num::NonZeroU64>>,
}

impl Scanner {
    #[expect(
        clippy::too_many_arguments,
        reason = "scanner ctor takes one local per piece of state it needs to thread \
                  through fetch_next_block; collapsing them into a config struct would \
                  add an indirection without removing any per-call decision the caller \
                  makes about the values"
    )]
    pub fn new(
        fs: &Arc<dyn Fs>,
        path: &Path,
        block_count: usize,
        compression: CompressionType,
        global_seqno: SeqNo,
        encryption: Option<Arc<dyn EncryptionProvider>>,
        ecc: Option<crate::table::block::EccParams>,
        has_kv_footer: bool,
        #[cfg(zstd_any)] zstd_dictionary: Option<Arc<crate::compression::ZstdDictionary>>,
        comparator: SharedComparator,
        table_id: crate::TableId,
        columnar: bool,
        restart_interval: u8,
        start_offset: u64,
        lower_bound: Option<crate::UserKey>,
        group_tags: alloc::vec::Vec<Option<core::num::NonZeroU64>>,
    ) -> crate::Result<Self> {
        // 2 MiB buffer matches RocksDB's `compaction_readahead_size`
        // default and is large enough that the kernel can fold the
        // sequential-scan readahead heuristic into a single big
        // userspace fill per ~500 typical (4 KiB) data blocks instead
        // of one syscall every 8 blocks. Picked to dominate any
        // pre-#133-Phase1c micro-cost: the loss is at most a few MB
        // of allocator overhead per concurrent compaction, and
        // compaction concurrency is bounded by the scheduler. HDD-
        // tuning beyond this would benefit from a configurable knob,
        // tracked as the configurable-readahead follow-up under #133.
        const SCANNER_READAHEAD_BYTES: usize = 2 * 1024 * 1024;
        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        // The scanner walks every block in order — tell the kernel so
        // it can ramp readahead aggressively and evict already-read
        // pages instead of pinning them. Best-effort: hint() is
        // advisory and any failure here would just leave the kernel
        // on the default heuristic, so drop the error rather than
        // failing the open.
        let _ = file.hint(FileHint::Sequential);
        // A tight-space restricted view starts at its first LIVE block: the
        // blocks below it are hole-punched (they read as zeros and cannot
        // decode). `block_count` was reduced by the caller to match.
        if start_offset > 0 {
            #[cfg(not(feature = "std"))]
            use crate::io::{Seek, SeekFrom};
            #[cfg(feature = "std")]
            use std::io::{Seek, SeekFrom};
            file.seek(SeekFrom::Start(start_offset))?;
        }
        let mut reader = BufReader::with_capacity(SCANNER_READAHEAD_BYTES, file);
        let mut group_tags = group_tags.into_iter();

        let block = Self::fetch_next_block(
            &mut reader,
            table_id,
            compression,
            encryption.as_deref(),
            ecc,
            has_kv_footer,
            columnar,
            restart_interval,
            group_tags.next().flatten(),
            #[cfg(zstd_any)]
            zstd_dictionary.as_deref(),
        )?;
        let cmp = comparator.clone();
        let iter = OwnedDataBlockIter::try_new(block, |b| b.try_iter(cmp))?;

        Ok(Self {
            reader,
            iter,

            compression,
            block_count,
            read_count: 1,

            global_seqno,
            encryption,
            comparator,

            ecc,
            has_kv_footer,

            #[cfg(zstd_any)]
            zstd_dictionary,

            table_id,
            columnar,
            restart_interval,

            filtering_below_bound: lower_bound.is_some(),
            lower_bound,
            group_tags,
        })
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "per-block read threads each SST-level read parameter through; a config struct would add indirection without removing a caller decision"
    )]
    fn fetch_next_block(
        reader: &mut BufReader<Box<dyn FsFile>>,
        table_id: crate::TableId,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        has_kv_footer: bool,
        columnar: bool,
        restart_interval: u8,
        group_tag: Option<core::num::NonZeroU64>,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        if columnar {
            return Self::fetch_next_row_group(
                reader,
                table_id,
                compression,
                encryption,
                ecc,
                restart_interval,
                group_tag,
                #[cfg(zstd_any)]
                zstd_dict,
            );
        }
        let block = Self::read_block(
            reader,
            table_id,
            BlockType::Data,
            compression,
            encryption,
            ecc,
            #[cfg(zstd_any)]
            zstd_dict,
        )?;
        // A row block must be BlockType::Data. `from_loaded` strips the per-KV
        // checksum footer when this SST carries one (per-SST `has_kv_footer`,
        // since data blocks omit the byte).
        if block.header.block_type != BlockType::Data {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }
        DataBlock::from_loaded(block, has_kv_footer)
    }

    /// Reads the next block from the stream under `block_type`'s identity.
    fn read_block(
        reader: &mut BufReader<Box<dyn FsFile>>,
        table_id: crate::TableId,
        block_type: BlockType,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<Block> {
        Block::from_reader(
            reader,
            crate::table::block::BlockIdentity {
                table_id,
                block_type,
                dict_id: compression.dict_id(),
                window_log: 0,
            },
            &{
                // SST blocks omit the block_flags byte, so ECC presence is a
                // per-SST property: upgrade the transform to its `*Ecc`
                // variant when this table was written with Page ECC, so the
                // reader expects the parity trailer. Identity without the
                // `page_ecc` feature.
                let t = crate::table::block::BlockTransform::from_parts(
                    compression,
                    encryption,
                    #[cfg(zstd_any)]
                    zstd_dict,
                )?;
                if let Some(ecc) = ecc {
                    t.with_ecc(ecc)
                } else {
                    t
                }
            },
        )
    }

    /// Reads one columnar row group from the stream — its directory, then its
    /// pages, which follow it back to back, then its zone blocks if it has any
    /// — and reconstructs it into a row-major [`DataBlock`].
    ///
    /// The directory must carry `group_tag`, the tag the group's index entry
    /// names it by: a group consistent in itself but read in another group's
    /// place is refused rather than streamed out of order. Each page is then
    /// checked against the directory: a page whose on-disk length differs
    /// from the length the directory records means the stream and the
    /// directory disagree about where the next block starts, and every block
    /// read after that point would be misframed.
    #[cfg(feature = "columnar")]
    #[expect(
        clippy::too_many_arguments,
        reason = "per-group read threads each SST-level read parameter through, as `fetch_next_block` does"
    )]
    fn fetch_next_row_group(
        reader: &mut BufReader<Box<dyn FsFile>>,
        table_id: crate::TableId,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        restart_interval: u8,
        group_tag: Option<core::num::NonZeroU64>,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        let directory_block = Self::read_block(
            reader,
            table_id,
            BlockType::ColumnPageDirectory,
            CompressionType::None,
            encryption,
            ecc,
            #[cfg(zstd_any)]
            None,
        )?;
        if directory_block.header.block_type != BlockType::ColumnPageDirectory {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                directory_block.header.block_type.into(),
            )));
        }
        let directory = crate::table::column_page::PageDirectory::decode(&directory_block.data)?;
        crate::table::row_group::check_group_tag(group_tag, &directory)?;
        let mut pages = Vec::with_capacity(directory.entries().len());
        for entry in directory.entries() {
            let page = Self::read_block(
                reader,
                table_id,
                BlockType::ColumnPage,
                compression,
                encryption,
                ecc,
                #[cfg(zstd_any)]
                zstd_dict,
            )?;
            if page.header.block_type != BlockType::ColumnPage {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    page.header.block_type.into(),
                )));
            }
            if page.header.on_disk_size_with(ecc) != entry.length {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page length disagrees with its directory entry",
                ));
            }
            pages.push(Some(page));
        }
        // The zone blocks, when the group has any, close the group. A scan of
        // every row has no use for them, but they are read and verified like
        // the rest, both to reach the next group and so that a stream that
        // ends or diverges there is refused rather than misframed.
        for zone_block in directory.zone_blocks() {
            let zones = Self::read_block(
                reader,
                table_id,
                BlockType::ColumnZones,
                CompressionType::None,
                encryption,
                ecc,
                #[cfg(zstd_any)]
                None,
            )?;
            if zones.header.block_type != BlockType::ColumnZones {
                return Err(crate::Error::InvalidTag((
                    "BlockType",
                    zones.header.block_type.into(),
                )));
            }
            if zones.header.on_disk_size_with(ecc) != zone_block.length {
                return Err(crate::Error::InvalidHeader(
                    "columnar: zone block length disagrees with its directory",
                ));
            }
        }
        // The scanner feeds compaction, which is maintenance and outside the
        // read counters, so neither the page copies nor the rebuilt values are
        // charged.
        let pages = crate::table::row_group::RowGroupBlocks {
            directory: alloc::sync::Arc::new(directory),
            pages,
            zones: None,
        }
        .to_row_pages(&crate::table::row_group::PageWant::ALL, &mut 0)?;
        DataBlock::from_column_batch(pages.batches, restart_interval, &mut 0)
    }

    /// Without the `columnar` feature a columnar SST cannot be read.
    #[cfg(not(feature = "columnar"))]
    #[cfg_attr(
        zstd_any,
        expect(
            clippy::too_many_arguments,
            reason = "mirrors the columnar build's signature"
        )
    )]
    fn fetch_next_row_group(
        _reader: &mut BufReader<Box<dyn FsFile>>,
        _table_id: crate::TableId,
        _compression: CompressionType,
        _encryption: Option<&dyn EncryptionProvider>,
        _ecc: Option<crate::table::block::EccParams>,
        _restart_interval: u8,
        _group_tag: Option<core::num::NonZeroU64>,
        #[cfg(zstd_any)] _zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        Err(crate::Error::FeatureUnsupported("columnar"))
    }
}

impl Iterator for Scanner {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(mut item) = self.iter.next() {
                // Sub-bound entries of the straddling first block belong to
                // the slice output that superseded the punched prefix; keys
                // ascend, so the comparison retires at the first live entry.
                if self.filtering_below_bound {
                    if let Some(bound) = &self.lower_bound
                        && self.comparator.compare(&item.key.user_key, bound.as_ref())
                            == core::cmp::Ordering::Less
                    {
                        continue;
                    }
                    self.filtering_below_bound = false;
                }
                item.key.seqno += self.global_seqno;
                return Some(Ok(item));
            }

            if self.read_count >= self.block_count {
                return None;
            }

            // Init new block
            let block = match Self::fetch_next_block(
                &mut self.reader,
                self.table_id,
                self.compression,
                self.encryption.as_deref(),
                self.ecc,
                self.has_kv_footer,
                self.columnar,
                self.restart_interval,
                self.group_tags.next().flatten(),
                #[cfg(zstd_any)]
                self.zstd_dictionary.as_deref(),
            ) {
                Ok(block) => block,
                Err(e) => {
                    self.read_count = self.block_count;
                    return Some(Err(e));
                }
            };
            let cmp = self.comparator.clone();
            match OwnedDataBlockIter::try_new(block, |b| b.try_iter(cmp)) {
                Ok(iter) => {
                    self.iter = iter;
                    self.read_count += 1;
                }
                Err(e) => {
                    // Poison the scanner so callers cannot silently skip
                    // the corrupt block and resume on later blocks.
                    self.read_count = self.block_count;
                    return Some(Err(e));
                }
            }
        }
    }
}
