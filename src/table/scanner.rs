// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2024-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

use alloc::sync::Arc;

use super::{Block, DataBlock};
use crate::path::Path;
use crate::{
    CompressionType, InternalValue, SeqNo,
    comparator::SharedComparator,
    encryption::EncryptionProvider,
    fs::{FileHint, Fs, FsOpenOptions},
    table::{block::BlockType, iter::OwnedDataBlockIter, util::TimedFile},
};

/// What a read through the stream returns in this build.
#[cfg(feature = "std")]
type IoResult<T> = std::io::Result<T>;
#[cfg(not(feature = "std"))]
type IoResult<T> = crate::io::Result<T>;

/// 2 MiB matches the `compaction_readahead_size` default of `RocksDB`: one read
/// from the file per ~500 typical (4 KiB) data blocks. A chunk is never
/// larger than what is left of the file, so a small table reads only itself.
const SCANNER_READAHEAD_BYTES: usize = 2 * 1024 * 1024;

/// The data section read as a stream of blocks, with the offset the next one
/// starts at: every block is verified at the place it was read from, so a
/// block of the right shape written at another block's place is refused
/// rather than streamed out of order.
///
/// The file is read a chunk at a time straight into a buffer nothing zeroes,
/// and a block's payload is handed out as a view of its chunk, not copied out
/// of it. A view keeps its whole chunk alive, which a scan's consumers, all of
/// them streaming, hold only until they move on; a block reaching past its
/// chunk starts the next one, which takes over the bytes not yet handed out
/// and those of the block being read, so a block's frame lies in one chunk.
struct BlockStream {
    file: TimedFile,
    /// The bytes read from the file: those from `at` on are not handed out yet.
    chunk: crate::Slice,
    at: usize,
    /// Where in `chunk` the block being read starts, once
    /// [`Self::begin_block`] marked one: a chunk read meanwhile keeps its
    /// bytes, so its frame lies in one chunk.
    mark: Option<usize>,
    /// The file offset the block being read starts at.
    mark_position: u64,
    /// The bytes of the file not read into a chunk yet.
    unread: u64,
    /// The length a chunk is read at, unless less of the file is left or a
    /// block needs more.
    readahead: usize,
    position: u64,
}

impl BlockStream {
    /// A stream over `file`, positioned `start` bytes into its `len`.
    fn new(file: TimedFile, start: u64, len: u64, readahead: usize) -> Self {
        Self {
            file,
            chunk: crate::Slice::empty(),
            at: 0,
            mark: None,
            mark_position: start,
            // A start past the end leaves nothing to read: the first block
            // read then fails as a truncated file.
            unread: len.saturating_sub(start),
            readahead,
            position: start,
        }
    }

    /// The bytes read and not handed out yet.
    fn pending(&self) -> &[u8] {
        self.chunk.get(self.at..).unwrap_or_default()
    }

    /// Marks the start of the next block.
    fn begin_block(&mut self) {
        self.mark = Some(self.at);
        self.mark_position = self.position;
    }

    /// The block read since [`Self::begin_block`], which it ends: the file
    /// offset it starts at and its frame, as a view of the chunk holding it.
    /// `None` when no block was marked.
    fn block_frame(&mut self) -> Option<(u64, crate::Slice)> {
        let mark = self.mark.take()?;
        Some((self.mark_position, self.chunk.slice(mark..self.at)))
    }

    /// Reads the next chunk, at least `need` bytes past what the block being
    /// read has taken, with that block's bytes and those not handed out yet at
    /// its front. One shorter than `need` is a truncated file.
    fn refill(&mut self, need: usize) -> IoResult<()> {
        let mark = self.mark.unwrap_or(self.at);
        let kept = self.chunk.get(mark..).unwrap_or_default();
        let taken = self.at - mark;
        let pending_len = kept.len() - taken;
        // Bounded by the file's length, a u64.
        let left = self.unread + pending_len as u64;
        let len = usize::try_from(left)
            .map_or(self.readahead, |left| left.min(self.readahead))
            .max(need);
        // `need` exceeds what is pending, so the chunk reads past it.
        let read = len - pending_len;
        let chunk = crate::Slice::from_reader(
            &mut PendingThenFile {
                pending: kept,
                file: &mut self.file,
            },
            taken + len,
        )?;
        self.unread = self.unread.saturating_sub(read as u64);
        self.chunk = chunk;
        self.mark = self.mark.map(|_| 0);
        self.at = taken;
        Ok(())
    }

    /// The next `len` bytes, as a view of the chunk holding them.
    fn view(&mut self, len: usize) -> IoResult<crate::Slice> {
        if self.pending().len() < len {
            self.refill(len)?;
        }
        let end = self.at + len;
        let view = self.chunk.slice(self.at..end);
        self.at = end;
        Ok(view)
    }

    fn read_pending(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.pending().is_empty() {
            if self.unread == 0 || buf.is_empty() {
                return Ok(0);
            }
            self.refill(1)?;
        }
        let pending = self.pending();
        let n = pending.len().min(buf.len());
        let (out, _) = buf.split_at_mut(n);
        let (bytes, _) = pending.split_at(n);
        out.copy_from_slice(bytes);
        self.at += n;
        Ok(n)
    }
}

#[cfg(feature = "std")]
impl std::io::Read for BlockStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_pending(buf)
    }
}

#[cfg(not(feature = "std"))]
impl crate::io::Read for BlockStream {
    fn read(&mut self, buf: &mut [u8]) -> crate::io::Result<usize> {
        self.read_pending(buf)
    }
}

/// The bytes a chunk takes over from the last one, then the file.
struct PendingThenFile<'a> {
    pending: &'a [u8],
    file: &'a mut TimedFile,
}

impl PendingThenFile<'_> {
    fn read_next(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        #[cfg(not(feature = "std"))]
        use crate::io::Read;
        #[cfg(feature = "std")]
        use std::io::Read;

        if self.pending.is_empty() {
            return self.file.read(buf);
        }
        let n = self.pending.len().min(buf.len());
        let (out, _) = buf.split_at_mut(n);
        let (bytes, rest) = self.pending.split_at(n);
        out.copy_from_slice(bytes);
        self.pending = rest;
        Ok(n)
    }
}

#[cfg(feature = "std")]
impl std::io::Read for PendingThenFile<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.read_next(buf)
    }
}

#[cfg(not(feature = "std"))]
impl crate::io::Read for PendingThenFile<'_> {
    fn read(&mut self, buf: &mut [u8]) -> crate::io::Result<usize> {
        self.read_next(buf)
    }
}

/// Table reader that is optimized for consuming an entire table
pub struct Scanner {
    reader: BlockStream,
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

    /// The index entries of the groups still to be read, in order: a columnar
    /// scan streams the data without the index, and refuses a group whose
    /// directory does not carry the tag and lengths its entry records. Empty
    /// for a row-major table.
    groups: alloc::vec::IntoIter<super::BlockHandle>,
    /// The index entry of the group whose rows are being handed out.
    current_group: Option<super::BlockHandle>,
    /// The extent and frame of the row-major block whose rows are being
    /// handed out, until a carry takes them; `None` for a columnar table and
    /// once handed on.
    frame: Option<(super::BlockHandle, crate::Slice)>,
    /// Hands out each group's or block's rows from its own list and records it
    /// as a candidate for a whole copy, when a compaction asked for it.
    carry: Option<super::group_carry::GroupCarry>,
}

impl Scanner {
    #[expect(
        clippy::too_many_arguments,
        reason = "scanner ctor takes one local per piece of state it needs to thread \
                  through fetch_next_block; collapsing them into a config struct would \
                  add an indirection without removing any per-call decision the caller \
                  makes about the values"
    )]
    pub(crate) fn new(
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
        groups: alloc::vec::Vec<super::BlockHandle>,
        pace: Option<crate::table::util::Pacer>,
    ) -> crate::Result<Self> {
        let mut file = fs.open(path, &FsOpenOptions::new().read(true))?;
        let len = file.metadata()?.len;
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
        let mut timed = TimedFile::new(file);
        if let Some(pace) = pace {
            timed.set_pace(pace);
        }
        let mut reader = BlockStream::new(timed, start_offset, len, SCANNER_READAHEAD_BYTES);
        let mut groups = groups.into_iter();
        let current_group = groups.next();

        let block = Self::fetch_next_block(
            &mut reader,
            table_id,
            compression,
            encryption.as_deref(),
            ecc,
            has_kv_footer,
            columnar,
            restart_interval,
            current_group,
            #[cfg(zstd_any)]
            zstd_dictionary.as_deref(),
        )?;
        let frame = Self::row_block_frame(&mut reader, columnar);
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
            groups,
            current_group,
            frame,
            carry: None,
        })
    }

    /// The extent and frame of the block just read, when the table is
    /// row-major: one view of the chunk the stream read it from.
    fn row_block_frame(
        reader: &mut BlockStream,
        columnar: bool,
    ) -> Option<(super::BlockHandle, crate::Slice)> {
        let (offset, frame) = reader.block_frame()?;
        if columnar {
            return None;
        }
        let size = u32::try_from(frame.len()).ok()?;
        Some((
            super::BlockHandle::new(crate::table::block::BlockOffset(offset), size),
            frame,
        ))
    }

    /// Records every row group or data block this scan reads in `carry`'s
    /// queue, with the rows it hands out for it. Only the scan of a table
    /// whose groups or blocks can be carried takes one (see
    /// `Table::carries_row_groups` and `Table::carries_data_blocks`): a table
    /// read whole and as stored, since a restricted view drops rows of its
    /// first block and an ingested table shifts their seqnos, so what it hands
    /// out is not what the bytes hold.
    #[must_use]
    pub(crate) fn with_carry(mut self, mut carry: super::group_carry::GroupCarry) -> Self {
        debug_assert!(
            self.lower_bound.is_none() && self.global_seqno == 0,
            "only a table whose groups or blocks can be carried records them",
        );
        let (group, frame) = match self.frame.take() {
            Some((block, frame)) => (Some(block), Some(frame)),
            None => (self.current_group, None),
        };
        carry.start(group, frame, self.iter.by_ref());
        self.carry = Some(carry);
        self
    }

    /// Hands `rows`, the block of `group` just read, to the carry of a scan
    /// that records what it reads: recorded, its rows are handed out from the
    /// carry's list, each group's or block's taken at once so the candidate
    /// holds them all before the first goes; unrecorded, they come straight
    /// from the block, as a scan that records nothing reads them.
    fn start_carried(&mut self, group: Option<super::BlockHandle>, rows: OwnedDataBlockIter) {
        let (group, frame) = match Self::row_block_frame(&mut self.reader, self.columnar) {
            Some((block, frame)) => (Some(block), Some(frame)),
            None => (group, None),
        };
        let Some(carry) = self.carry.as_mut() else {
            return;
        };
        if carry.skips_next() {
            // A scan bounded to a range filters the block's rows by it.
            self.filtering_below_bound = carry.filters_passed_rows();
            self.iter = rows;
        } else {
            carry.start(group, frame, rows);
        }
    }

    /// Whether `item`, read from the current block, is handed out: one of a
    /// restricted view's straddling block at or past its lower bound, or one
    /// of a group a bounded carry passes within the carry's range. Called only
    /// while `filtering_below_bound` is set.
    #[cold]
    fn hands_out(&mut self, item: &InternalValue) -> bool {
        if let Some(carry) = &self.carry {
            return carry.hands_out(item);
        }
        // Sub-bound entries of the straddling first block belong to the slice
        // output that superseded the punched prefix; keys ascend, so the
        // comparison retires at the first live entry.
        if let Some(bound) = &self.lower_bound
            && self.comparator.compare(&item.key.user_key, bound.as_ref())
                == core::cmp::Ordering::Less
        {
            return false;
        }
        self.filtering_below_bound = false;
        true
    }

    /// Reads and opens the block of `group`, the next in the stream. A block
    /// that fails poisons the scan, so that no caller skips it and resumes on
    /// the blocks after it.
    fn read_group(
        &mut self,
        group: Option<super::BlockHandle>,
    ) -> crate::Result<OwnedDataBlockIter> {
        let block = Self::fetch_next_block(
            &mut self.reader,
            self.table_id,
            self.compression,
            self.encryption.as_deref(),
            self.ecc,
            self.has_kv_footer,
            self.columnar,
            self.restart_interval,
            group,
            #[cfg(zstd_any)]
            self.zstd_dictionary.as_deref(),
        )
        .and_then(|block| {
            let cmp = self.comparator.clone();
            OwnedDataBlockIter::try_new(block, |b| b.try_iter(cmp))
        });
        match block {
            Ok(_) => self.read_count += 1,
            Err(_) => self.read_count = self.block_count,
        }
        block
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "per-block read threads each SST-level read parameter through; a config struct would add indirection without removing a caller decision"
    )]
    fn fetch_next_block(
        reader: &mut BlockStream,
        table_id: crate::TableId,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        has_kv_footer: bool,
        columnar: bool,
        restart_interval: u8,
        group: Option<super::BlockHandle>,
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
                group,
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

    /// Reads the next block from the stream under `block_type`'s identity,
    /// verified at the offset it was read from, and advances past it.
    fn read_block(
        stream: &mut BlockStream,
        table_id: crate::TableId,
        block_type: BlockType,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<Block> {
        stream.begin_block();
        let at = crate::table::block::ChecksumAt::table(table_id, stream.position);
        let block = Block::from_reader_taking(
            stream,
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
            at,
            // The scanner feeds compaction, which is maintenance and outside
            // the read counters.
            &mut 0,
            |stream, len| Ok(stream.view(len)?),
        )?;
        // The block consumed its header, payload and parity trailer; an SST
        // block's trailer is sized by the table's scheme, not its header.
        stream.position += u64::from(block.header.on_disk_size_with(ecc));
        Ok(block)
    }

    /// Reads one columnar row group from the stream — its directory, then its
    /// pages, which follow it back to back, then its zone blocks if it has any
    /// — and reconstructs it into a row-major [`DataBlock`].
    ///
    /// The directory must carry the tag, and be the length and frame the head
    /// zone block and group the length, that `group`, the group's index
    /// entry, records: a group consistent in itself but read in another
    /// group's place, or misdescribed by its entry, is refused rather than
    /// streamed out of order or rewritten. Each page is then
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
        reader: &mut BlockStream,
        table_id: crate::TableId,
        compression: CompressionType,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        restart_interval: u8,
        group: Option<super::BlockHandle>,
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
        crate::table::row_group::check_group_tag(
            group.and_then(|group| group.group_tag()),
            &directory,
        )?;
        // The tag check refused a missing entry. The lengths are checked as an
        // indexed read checks them, so that a group its entry misdescribes is
        // refused here too rather than rewritten by the compaction.
        if let Some(group) = &group {
            let directory_len = directory_block.header.on_disk_size_with(ecc);
            crate::table::row_group::check_directory_len(
                crate::table::row_group::indexed_directory_len(group)?,
                directory_len,
            )?;
            crate::table::row_group::check_group_extent(group, directory_len, &directory)?;
            crate::table::row_group::check_head_zones_len(group, &directory)?;
        }
        // The zone blocks a scan of every row has no use for are read and
        // verified like the rest, both to reach the next block and so that a
        // stream that ends or diverges there is refused rather than
        // misframed: the head one between the directory and the pages, the
        // others after the pages.
        if let Some(head) = directory.head_zone_block() {
            Self::check_zone_block(reader, table_id, encryption, ecc, &directory, head)?;
        }
        let mut pages = alloc::vec::Vec::with_capacity(directory.entries().len());
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
            pages.push(page);
        }
        for &zone_block in directory.zone_blocks() {
            Self::check_zone_block(reader, table_id, encryption, ecc, &directory, zone_block)?;
        }
        // The scanner feeds compaction, which is maintenance and outside the
        // read counters, so neither the page copies nor the rebuilt values are
        // charged.
        let pages =
            crate::table::row_group::RowGroupBlocks::whole(alloc::sync::Arc::new(directory), pages)
                .to_row_pages(&mut 0)?;
        DataBlock::from_column_batch(pages.batches, restart_interval, &mut 0)
    }

    /// Reads the next block as the zone block `block` names and verifies it,
    /// then decodes it against `directory` as an indexed read does: a scan has
    /// no use for the zones, but one that took a zone block of another group
    /// or column for this one would rewrite the group with statistics that
    /// hide the damage.
    #[cfg(feature = "columnar")]
    fn check_zone_block(
        reader: &mut BlockStream,
        table_id: crate::TableId,
        encryption: Option<&dyn EncryptionProvider>,
        ecc: Option<crate::table::block::EccParams>,
        directory: &crate::table::column_page::PageDirectory,
        block: crate::table::column_page::ZoneBlock,
    ) -> crate::Result<()> {
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
        if zones.header.on_disk_size_with(ecc) != block.length {
            return Err(crate::Error::InvalidHeader(
                "columnar: zone block length disagrees with its directory",
            ));
        }
        directory.decode_zone_block(block.column_id, &zones.data)?;
        Ok(())
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
        _reader: &mut BlockStream,
        _table_id: crate::TableId,
        _compression: CompressionType,
        _encryption: Option<&dyn EncryptionProvider>,
        _ecc: Option<crate::table::block::EccParams>,
        _restart_interval: u8,
        _group: Option<super::BlockHandle>,
        #[cfg(zstd_any)] _zstd_dict: Option<&crate::compression::ZstdDictionary>,
    ) -> crate::Result<DataBlock> {
        Err(crate::Error::FeatureUnsupported("columnar"))
    }
}

#[cfg(test)]
#[expect(clippy::indexing_slicing, reason = "test code")]
mod tests;

impl Iterator for Scanner {
    type Item = crate::Result<InternalValue>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // A scan that records what it reads hands out a recorded group's
            // rows from its carry, so the rows of the block come first only
            // where nothing is recorded: the path every other scan takes.
            if let Some(mut item) = self.iter.next() {
                if self.filtering_below_bound && !self.hands_out(&item) {
                    continue;
                }
                item.key.seqno += self.global_seqno;
                return Some(Ok(item));
            }
            if let Some(carry) = self.carry.as_mut() {
                if let Some(row) = carry.next_row() {
                    return Some(Ok(row));
                }
                // The group passed unrecorded, if one was, is done.
                self.filtering_below_bound = false;
            }

            if self.read_count >= self.block_count {
                return None;
            }

            let group = self.groups.next();
            let iter = match self.read_group(group) {
                Ok(iter) => iter,
                Err(e) => return Some(Err(e)),
            };
            self.current_group = group;
            if self.carry.is_some() {
                self.start_carried(group, iter);
            } else {
                // A scan that carries nothing keeps no frame.
                self.frame = None;
                self.iter = iter;
            }
        }
    }
}
