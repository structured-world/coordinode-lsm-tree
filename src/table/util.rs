// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025-present, fjall-rs
// Copyright (c) 2026-present, Dmitry Prudnikov

#[cfg(not(feature = "std"))]
use alloc::vec::Vec;

use super::{Block, BlockHandle, GlobalTableId};
use crate::path::Path;
use crate::{
    Cache, CompressionType, KeyRange, Table, encryption::EncryptionProvider,
    file_accessor::FileAccessor, table::block::BlockType, version::run::Ranged,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// Returns the bounding key range of a table slice.
///
/// Takes `first().min()` and `last().max()` — no comparison needed because
/// callers pass tables that are already sorted in comparator order (via
/// `push_cmp` / `sort_by_cmp`). Works correctly for any comparator.
#[must_use]
pub fn aggregate_run_key_range(tables: &[Table]) -> KeyRange {
    #[expect(clippy::expect_used, reason = "runs are never empty by definition")]
    let lo = tables.first().expect("run should never be empty");
    #[expect(clippy::expect_used, reason = "runs are never empty by definition")]
    let hi = tables.last().expect("run should never be empty");
    KeyRange::new((lo.key_range().min().clone(), hi.key_range().max().clone()))
}

/// [start, end] slice indexes
#[derive(Debug)]
pub struct SliceIndexes(pub usize, pub usize);

/// Loads a block from disk or block cache, if cached.
///
/// Also handles file descriptor opening and caching.
///
/// When this read recovers the payload from its Page-ECC parity (a checksum
/// mismatch the parity repaired) the returned bytes are correct, but the on-disk
/// copy still carries the fault. If `heal_hints` is `Some`, the loader then
/// re-reads the same block straight from disk to tell a persistent medium fault
/// (re-read again recovers) from a transient read-path glitch (re-read clean),
/// and records the SST in the sink on confirmed persistence so the compaction
/// picker can rewrite it clean. `None` disables that scheduling (the payload is
/// still healed and returned). A cache hit never triggers this path: the cached
/// bytes were already verified on their original read.
///
/// `charge` says whose read this is: only a foreground read reaches the read
/// and load counters, and a report neither fills the cache nor promotes what
/// it finds there. Page-ECC recoveries are counted for every reader.
#[expect(
    clippy::too_many_arguments,
    reason = "block loading requires table id, path, file accessor, cache, handle, block type, compression, and heal context"
)]
pub fn load_block(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    heal_hints: Option<&crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    charge: ReadCharge,
) -> crate::Result<Block> {
    #[cfg(feature = "metrics")]
    use core::sync::atomic::Ordering::Relaxed;

    log::trace!("load {block_type:?} block {handle:?}");

    // Invariant: manifest Blocks have their own reader path and
    // never reach the SST block cache. Surface a typed error
    // (rather than panic) so a caller that wires up an SST loader
    // with a manifest BlockType gets a routable failure instead
    // of a process abort. The check stays outside the metrics
    // cfg so the contract holds on every build.
    if matches!(block_type, BlockType::Manifest | BlockType::ManifestFooter) {
        return Err(crate::Error::InvalidTag(("BlockType", block_type.into())));
    }

    let cached = if charge.touches_cache() {
        cache.get_block(table_id, handle.offset())
    } else {
        cache.peek_block(table_id, handle.offset())
    };
    if let Some(block) = cached {
        // Per-KV checking is a header flag, not a block type, so a data
        // block is always BlockType::Data on disk — an exact role match is
        // the right swap-defence check here.
        if block.header.block_type != block_type {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            )));
        }

        #[cfg(feature = "metrics")]
        if charge.is_counted() {
            record_block_load_cached(metrics, block_type);
        }

        return Ok(block);
    }

    // An untraced read leaves the descriptor cache as it found it, like the
    // block cache above.
    let (fd, cache_event) = if charge.touches_cache() {
        file_accessor.get_or_open_table(&table_id, path)?
    } else {
        (file_accessor.peek_or_open_table(&table_id, path)?, None)
    };

    // Only track descriptor-table cache metrics; pinned FDs (None) are not cache events.
    #[cfg(feature = "metrics")]
    if let Some(hit) = cache_event
        && charge.is_counted()
    {
        if hit {
            metrics.table_file_opened_cached.fetch_add(1, Relaxed);
        } else {
            metrics.table_file_opened_uncached.fetch_add(1, Relaxed);
        }
    }

    #[cfg(not(feature = "metrics"))]
    let _ = cache_event;

    let transform = build_block_transform(
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    // Charged as the read is issued, before it is validated: a block that then
    // fails its checksum, decryption or decompression was still asked of the
    // filesystem, while a handle refused before reading asked nothing.
    let mut produced = 0;
    let read = Block::from_file_issuing(
        fd.as_ref(),
        *handle,
        crate::table::block::BlockIdentity {
            table_id: table_id.table_id(),
            block_type,
            dict_id: compression.dict_id(),
            window_log: 0,
        },
        &transform,
        || {
            #[cfg(feature = "metrics")]
            if charge.is_counted() {
                record_block_read(metrics, block_type, handle.size().into());
            }
        },
        &mut produced,
    );
    // What the transform produced, counted once per block that actually ran
    // one. Paired with the per-role `*_io_requested` above: those record what
    // was asked of the filesystem, this records what came out the other side
    // of decompression, decryption and ECC. The ratio is the compression the
    // read actually paid for, and its absolute value is what separates a
    // physical projection from a cosmetic one. Charged before the result is
    // judged: a transform whose output the length or role check then refuses
    // still ran.
    #[cfg(feature = "metrics")]
    record_block_decoded(metrics, charge, produced);
    let (block, ecc_status, recovery) = read?;
    admit_read_block(
        table_id,
        path,
        file_accessor,
        cache,
        handle,
        block_type,
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
        heal_hints,
        #[cfg(feature = "metrics")]
        metrics,
        charge,
        block,
        ecc_status,
        recovery,
    )
}

/// Takes a block just read from disk into the engine: counts it, checks its
/// role, schedules a heal when ECC had to correct it, and caches it.
///
/// Everything [`load_block`] does after the read and its decode charge, split
/// out so a caller that read several blocks in one request — a columnar row
/// group's directory and pages — accounts for each of them exactly as a block
/// read on its own is accounted for. Two paths that disagreed here would make
/// the counters, the cache and auto-heal depend on how a block happened to be
/// fetched.
#[expect(
    clippy::too_many_arguments,
    reason = "the whole block read context, plus the block it admits"
)]
pub(crate) fn admit_read_block(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    cache: &Cache,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    heal_hints: Option<&crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    charge: ReadCharge,
    block: Block,
    ecc_status: crate::table::block::EccStatus,
    recovery: Option<crate::table::block::EccRecoveryKind>,
) -> crate::Result<Block> {
    // Count the on-read ECC recovery (by mechanism) at this primary read site,
    // whoever the reader is: it is a health signal about the medium. The
    // persistence-confirming re-read below goes through a path that does NOT
    // count, so a single fault is counted exactly once.
    #[cfg(feature = "metrics")]
    if let Some(kind) = recovery {
        metrics.record_ecc_recovery(kind);
    }
    #[cfg(not(feature = "metrics"))]
    let _ = recovery;
    let corrected = matches!(ecc_status, crate::table::block::EccStatus::Corrected);

    if block.header.block_type != block_type {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    #[cfg(feature = "metrics")]
    if charge.is_counted() {
        record_block_loaded(metrics, block_type);
    }

    // ECC recovered this block's payload from parity. The bytes returned below
    // are correct, but the on-disk copy is still faulty: when auto-heal is on,
    // confirm the fault is persistent and queue the SST for a healing rewrite.
    if corrected {
        maybe_record_persistent_heal(
            table_id,
            path,
            file_accessor,
            handle,
            block_type,
            compression,
            encryption,
            ecc,
            #[cfg(zstd_any)]
            zstd_dict,
            heal_hints,
            #[cfg(feature = "metrics")]
            metrics,
            charge,
        );
    }

    if charge.touches_cache() {
        cache.insert_block(table_id, handle.offset(), block.clone());
    }

    Ok(block)
}

/// Counts one block served from the block cache under its role.
///
/// A columnar page directory counts as index: it is what turns a row group
/// into the addresses of its pages, which is the index's job one level down.
/// Counting it as data would make the data-block hit rate report an address
/// lookup as a payload read.
#[cfg(feature = "metrics")]
fn record_block_load_cached(metrics: &Metrics, block_type: BlockType) {
    use core::sync::atomic::Ordering::Relaxed;
    let cached = match block_type {
        BlockType::Filter => &metrics.filter_block_load_cached,
        BlockType::Index | BlockType::ColumnPageDirectory => &metrics.index_block_load_cached,
        BlockType::RangeTombstone => &metrics.range_tombstone_block_load_cached,
        BlockType::Data | BlockType::Meta | BlockType::ColumnPage => {
            &metrics.data_block_load_cached
        }
        // Manifest variants are rejected by `load_block`'s guard before any
        // cache lookup; the remaining sections are loaded once on open via
        // `Block::from_file`, never through this cached path.
        BlockType::Manifest
        | BlockType::ManifestFooter
        | BlockType::BlockLayout
        | BlockType::Locator
        | BlockType::SeqnoBounds
        | BlockType::ZoneMap
        | BlockType::DeleteBitmap => return,
    };
    cached.fetch_add(1, Relaxed);
}

/// Charges one uncached block read to its role's requested-bytes counter:
/// `on_disk` bytes asked of the filesystem. Every path that reads a block
/// without going through the block cache calls this at the point the read is
/// issued, so a read that then fails validation is still counted. Whether the
/// block then loaded is a separate count, [`record_block_loaded`].
#[cfg(feature = "metrics")]
pub(crate) fn record_block_read(metrics: &Metrics, block_type: BlockType, on_disk: u64) {
    use core::sync::atomic::Ordering::Relaxed;
    let requested = match block_type {
        BlockType::Filter => &metrics.filter_block_io_requested,
        BlockType::Index | BlockType::ColumnPageDirectory => &metrics.index_block_io_requested,
        BlockType::RangeTombstone => &metrics.range_tombstone_block_io_requested,
        BlockType::Data | BlockType::Meta | BlockType::ColumnPage => {
            &metrics.data_block_io_requested
        }
        // Manifest variants never reach a table read path; the remaining
        // sections are loaded once on open via `Block::from_file`, outside
        // these per-read counters.
        BlockType::Manifest
        | BlockType::ManifestFooter
        | BlockType::BlockLayout
        | BlockType::Locator
        | BlockType::SeqnoBounds
        | BlockType::ZoneMap
        | BlockType::DeleteBitmap => return,
    };
    requested.fetch_add(on_disk, Relaxed);
}

/// Counts one block that came back from disk usable, after its checksum,
/// transform and type checks passed. A read that failed any of them asked the
/// filesystem for bytes ([`record_block_read`]) but loaded nothing, so it must
/// not lower the cache hit rates the load counters feed.
#[cfg(feature = "metrics")]
fn record_block_loaded(metrics: &Metrics, block_type: BlockType) {
    use core::sync::atomic::Ordering::Relaxed;
    let loads = match block_type {
        BlockType::Filter => &metrics.filter_block_load_io,
        BlockType::Index | BlockType::ColumnPageDirectory => &metrics.index_block_load_io,
        BlockType::RangeTombstone => &metrics.range_tombstone_block_load_io,
        BlockType::Data | BlockType::Meta | BlockType::ColumnPage => &metrics.data_block_load_io,
        BlockType::Manifest
        | BlockType::ManifestFooter
        | BlockType::BlockLayout
        | BlockType::Locator
        | BlockType::SeqnoBounds
        | BlockType::ZoneMap
        | BlockType::DeleteBitmap => return,
    };
    loads.fetch_add(1, Relaxed);
}

/// Charges what one block's transform produced to `bytes_decoded`, for a
/// counted read. Called as soon as the transform ran, before its output is
/// judged: a transform whose output the length or role check then refuses
/// still ran.
#[cfg(feature = "metrics")]
fn record_block_decoded(metrics: &Metrics, charge: ReadCharge, produced: usize) {
    if charge.is_counted() {
        metrics
            .block_bytes_decoded
            .fetch_add(produced as u64, core::sync::atomic::Ordering::Relaxed);
    }
}

/// A columnar row group as read: its directory and its pages, each verified
/// and cached as the block it is.
#[cfg(feature = "columnar")]
pub(crate) struct RowGroupBlocks {
    pub directory: crate::table::column_page::PageDirectory,
    /// One block per directory entry, in the directory's order.
    pub pages: Vec<Block>,
}

/// Loads a columnar row group: its page directory, then its pages.
///
/// `group` is the index entry, which spans the whole group. When the
/// directory and every page are cached nothing is read. Otherwise the whole
/// group is read in ONE request — what reading a columnar block cost before
/// it had pages — and each block in it is verified and admitted separately
/// ([`admit_read_block`]), so each is cached under its own offset with its own
/// buffer. A page cached as a view of the shared read would keep the whole
/// group alive while the cache charged it as one page.
///
/// `charge` is applied as [`load_block`] applies it. The one request is
/// charged to the data role when it is issued, since the index entry it serves
/// names a data block and the group's split into directory and pages is known
/// only once the bytes are in; each block's load and decode are then counted
/// under its own role as it is verified.
///
/// # Errors
///
/// Any block's verification error, or [`crate::Error::InvalidHeader`] when the
/// directory describes a layout that does not fill the group exactly: a gap
/// or an overrun means the index entry and the directory disagree about where
/// the group ends, and neither can be trusted to name the right bytes.
#[cfg(feature = "columnar")]
#[expect(
    clippy::too_many_arguments,
    reason = "the whole block read context, like load_block"
)]
pub(crate) fn load_row_group(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    cache: &Cache,
    group: &BlockHandle,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    heal_hints: Option<&crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    charge: ReadCharge,
) -> crate::Result<RowGroupBlocks> {
    use crate::coding::Decode;
    use crate::table::column_page::PageDirectory;

    if let Some(cached) = cached_row_group(table_id, cache, group, ecc, charge)? {
        #[cfg(feature = "metrics")]
        if charge.is_counted() {
            record_block_load_cached(metrics, BlockType::ColumnPageDirectory);
            for _ in &cached.pages {
                record_block_load_cached(metrics, BlockType::ColumnPage);
            }
        }
        return Ok(cached);
    }

    // An untraced read leaves the descriptor cache as it found it, as in
    // `load_block`.
    let (fd, cache_event) = if charge.touches_cache() {
        file_accessor.get_or_open_table(&table_id, path)?
    } else {
        (file_accessor.peek_or_open_table(&table_id, path)?, None)
    };
    #[cfg(feature = "metrics")]
    if let Some(hit) = cache_event
        && charge.is_counted()
    {
        use core::sync::atomic::Ordering::Relaxed;
        if hit {
            metrics.table_file_opened_cached.fetch_add(1, Relaxed);
        } else {
            metrics.table_file_opened_uncached.fetch_add(1, Relaxed);
        }
    }
    #[cfg(not(feature = "metrics"))]
    let _ = cache_event;

    #[cfg(feature = "metrics")]
    if charge.is_counted() {
        record_block_read(metrics, BlockType::ColumnPage, group.size().into());
    }
    let frame = crate::file::read_exact(fd.as_ref(), *group.offset(), group.size() as usize)?;

    // The directory is the first block; its own header says how long it is.
    let directory_header = crate::table::block::Header::decode_from(&mut &frame[..])?;
    let directory_len = directory_header.on_disk_size_with(ecc);
    let directory_handle = BlockHandle::new(group.offset(), directory_len);
    let directory_block = verify_and_admit(
        &frame,
        0,
        &directory_handle,
        BlockType::ColumnPageDirectory,
        CompressionType::None,
        table_id,
        path,
        file_accessor,
        cache,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        None,
        heal_hints,
        #[cfg(feature = "metrics")]
        metrics,
        charge,
    )?;
    let directory = PageDirectory::decode(&directory_block.data)?;
    check_group_extent(group, directory_len, &directory)?;

    let mut pages = Vec::with_capacity(directory.entries().len());
    for entry in directory.entries() {
        // Both terms were proved to fit inside the group by the extent check.
        let at = directory_len as usize + entry.offset as usize;
        let handle = BlockHandle::new(
            crate::table::block::BlockOffset(*group.offset() + at as u64),
            entry.length,
        );
        pages.push(verify_and_admit(
            &frame,
            at,
            &handle,
            BlockType::ColumnPage,
            compression,
            table_id,
            path,
            file_accessor,
            cache,
            encryption,
            ecc,
            #[cfg(zstd_any)]
            zstd_dict,
            heal_hints,
            #[cfg(feature = "metrics")]
            metrics,
            charge,
        )?);
    }
    Ok(RowGroupBlocks { directory, pages })
}

#[cfg(feature = "columnar")]
impl RowGroupBlocks {
    /// Decodes the pages whose column `wanted` selects — every page when
    /// `None` — into a batch, in write order.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a zero-row group, a page naming an
    /// encoding part this build does not decode, or a page whose own column
    /// id disagrees with the one the directory filed it under. The last two
    /// fail the group rather than the page: a reader that skipped what it
    /// could not interpret, or trusted a page to be what the directory said
    /// without checking, would return a batch that is quietly missing or
    /// substituting a column.
    ///
    /// Adds to `copied` what decoding the pages copied out of them, as each
    /// copy is made.
    pub(crate) fn to_batch(
        &self,
        wanted: Option<&[u16]>,
        copied: &mut usize,
    ) -> crate::Result<crate::table::columnar::ColumnBatch> {
        let row_count = self.directory.row_count();
        if row_count == 0 {
            return Err(crate::Error::InvalidHeader("columnar: zero-row data block"));
        }
        let mut columns = Vec::with_capacity(self.pages.len());
        for (entry, page) in self.directory.entries().iter().zip(&self.pages) {
            // Every column's encoding names a single part today. A part this
            // build does not decode is refused rather than skipped: skipping
            // it would hand back a column assembled from some of its parts.
            if entry.id.part != 0 {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page holds an encoding part this build does not decode",
                ));
            }
            if wanted.is_some_and(|w| !w.contains(&entry.id.column_id)) {
                continue;
            }
            let column =
                crate::table::columnar::Column::decode_page(&page.data, row_count, copied)?;
            if column.column_id != entry.id.column_id {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page column disagrees with its directory entry",
                ));
            }
            columns.push(column);
        }
        Ok(crate::table::columnar::ColumnBatch { row_count, columns })
    }
}

/// A row group served entirely from the cache, or `None` when any of its
/// blocks is missing. Partly cached is treated as not cached: the group is one
/// request either way, and reading it whole refreshes every block at once.
/// A read that must not touch the cache (`charge`) looks without promoting.
#[cfg(feature = "columnar")]
fn cached_row_group(
    table_id: GlobalTableId,
    cache: &Cache,
    group: &BlockHandle,
    ecc: Option<crate::table::block::EccParams>,
    charge: ReadCharge,
) -> crate::Result<Option<RowGroupBlocks>> {
    let lookup = |offset| {
        if charge.touches_cache() {
            cache.get_block(table_id, offset)
        } else {
            cache.peek_block(table_id, offset)
        }
    };
    let Some(directory_block) = lookup(group.offset()) else {
        return Ok(None);
    };
    if directory_block.header.block_type != BlockType::ColumnPageDirectory {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            directory_block.header.block_type.into(),
        )));
    }
    let directory = crate::table::column_page::PageDirectory::decode(&directory_block.data)?;
    let directory_len = directory_block.header.on_disk_size_with(ecc);
    check_group_extent(group, directory_len, &directory)?;

    let mut pages = Vec::with_capacity(directory.entries().len());
    for entry in directory.entries() {
        let offset = *group.offset() + u64::from(directory_len) + u64::from(entry.offset);
        let Some(page) = lookup(crate::table::block::BlockOffset(offset)) else {
            return Ok(None);
        };
        if page.header.block_type != BlockType::ColumnPage {
            return Err(crate::Error::InvalidTag((
                "BlockType",
                page.header.block_type.into(),
            )));
        }
        pages.push(page);
    }
    Ok(Some(RowGroupBlocks { directory, pages }))
}

/// Proves the directory describes a layout that fills the group exactly.
///
/// The index entry and the directory are two independent statements of where
/// the group ends. A writer makes them agree by construction, so a
/// disagreement is corruption, and it is refused before any page offset
/// derived from the directory is used to slice the group.
#[cfg(feature = "columnar")]
pub(crate) fn check_group_extent(
    group: &BlockHandle,
    directory_len: u32,
    directory: &crate::table::column_page::PageDirectory,
) -> crate::Result<()> {
    let described = directory_len.checked_add(directory.pages_len());
    if described != Some(group.size()) {
        return Err(crate::Error::InvalidHeader(
            "columnar: page directory does not fill its row group",
        ));
    }
    // Contiguity, not only total length: a page offset that skips ahead would
    // leave unverified bytes inside the group, and the sum alone would not
    // see it if another page made up the difference.
    let mut expected = 0_u32;
    for entry in directory.entries() {
        if entry.offset != expected {
            return Err(crate::Error::InvalidHeader(
                "columnar: row group pages are not contiguous",
            ));
        }
        expected = expected.wrapping_add(entry.length);
    }
    Ok(())
}

/// Verifies the block at `at` inside a row group's `frame` and admits it.
#[cfg(feature = "columnar")]
#[expect(
    clippy::too_many_arguments,
    reason = "the whole block read context, plus where the block sits in the frame"
)]
fn verify_and_admit(
    frame: &crate::Slice,
    at: usize,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    cache: &Cache,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    heal_hints: Option<&crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    charge: ReadCharge,
) -> crate::Result<Block> {
    let bytes = frame
        .get(at..at + handle.size() as usize)
        .ok_or(crate::Error::InvalidHeader(
            "columnar: page outside its row group",
        ))?;
    let transform = build_block_transform(
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    let identity = crate::table::block::BlockIdentity {
        table_id: table_id.table_id(),
        block_type,
        dict_id: compression.dict_id(),
        window_log: 0,
    };
    // An owned copy per block, so each cached block holds exactly its own
    // bytes rather than a view that pins the whole group.
    let (header, payload, ecc_status, recovery) =
        Block::verify_frame(bytes.to_vec(), *handle, identity, &transform)?;
    let mut produced = 0;
    let data = Block::decompress_payload(&header, payload, &transform, &mut produced);
    #[cfg(feature = "metrics")]
    record_block_decoded(metrics, charge, produced);
    let data = data?;
    admit_read_block(
        table_id,
        path,
        file_accessor,
        cache,
        handle,
        block_type,
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
        heal_hints,
        #[cfg(feature = "metrics")]
        metrics,
        charge,
        Block { header, data },
        ecc_status,
        recovery,
    )
}

/// Decodes pre-read block bytes into the cache: the decode half of a batched
/// prewarm.
///
/// The bytes were read in ONE cross-file
/// [`Fs::read_blocks_batched`](crate::fs::Fs::read_blocks_batched) (the reads of
/// many SSTs, possibly fanned out across devices, coalesced into one
/// submission). `buffers[i]` holds the on-disk bytes of `handles[i]`.
///
/// Decodes each with the same path [`load_block`] uses ([`Block::from_reader`]
/// shares the header / ECC / decrypt helpers), so the cached block is
/// byte-identical to what the read walk would produce, then inserts it. Purely
/// an I/O optimization: it never changes which bytes a later [`load_block`]
/// returns. A block whose decode would need a re-read recovery is left uncached
/// for the read walk to handle authoritatively.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors load_block's decode context (id, cache, type, compression, encryption, ecc, dict)"
)]
pub fn decode_prewarmed_blocks(
    table_id: GlobalTableId,
    cache: &Cache,
    handles: &[BlockHandle],
    buffers: &[&[u8]],
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
) {
    let Ok(transform) = build_block_transform(
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
    ) else {
        return;
    };
    let identity = crate::table::block::BlockIdentity {
        table_id: table_id.table_id(),
        block_type,
        dict_id: compression.dict_id(),
        window_log: 0,
    };

    // Invariant: prewarm only ever caches NON-ECC blocks. `Table::plan_prewarm`
    // returns `None` for any table whose `ecc_params` is set, so the prewarm pass
    // never gathers an ECC block's bytes, and `ecc` here is always `None`. This
    // matters for data integrity: `from_reader` repairs an ECC-corrected payload
    // silently (it returns no `EccStatus`), so caching a corrected block as clean
    // would let later `load_block` cache hits skip `from_file_with_recovery` /
    // `maybe_record_persistent_heal`, leaving the latent on-disk fault unscheduled
    // for healing. With `ecc` always `None` the insert below cannot capture a
    // corrected block. The assert pins the invariant: if prewarm is ever extended
    // to ECC tables, this trips and the decode must first move to a
    // status-returning path that leaves corrected blocks uncached.
    debug_assert!(
        ecc.is_none(),
        "prewarm must not cache ECC blocks: plan_prewarm gates ECC tables out, and \
         from_reader would silently cache an ECC-corrected block as if it were clean"
    );

    for (handle, buf) in handles.iter().zip(buffers.iter()) {
        // Same decode as load_block (from_reader shares the header / ECC /
        // decrypt helpers), so the cached block is byte-identical to what the
        // read walk would produce. A decode error (e.g. a block needing a re-read
        // recovery) just leaves it uncached for the walk to read authoritatively.
        let mut reader = crate::io::Cursor::new(&buf[..]);
        // The read was charged when the batch was submitted; the decode is
        // charged here, where the transform ran, before the result is judged:
        // a block refused by its length or role check was still decoded, and
        // the read walk that then reads it authoritatively charges its own
        // decode on top.
        let mut produced = 0;
        let decoded = Block::from_reader_counting(&mut reader, identity, &transform, &mut produced);
        #[cfg(feature = "metrics")]
        metrics
            .block_bytes_decoded
            .fetch_add(produced as u64, core::sync::atomic::Ordering::Relaxed);
        let Ok(block) = decoded else {
            continue;
        };
        if block.header.block_type == block_type {
            cache.insert_block(table_id, handle.offset(), block);
        }
    }
}

/// Schedules a healing recompaction for `table_id` when a just-read block was
/// ECC-corrected and the fault is confirmed persistent.
///
/// Call this on any read path that recovers a block from parity (full
/// [`load_block`], the zstd partial-decode path, and [`scrub_block`] all feed
/// it). No-op unless `heal_hints` is `Some` and enabled (`auto_heal`). On a
/// corrected read it re-reads the block straight from disk (cache-bypassing) to
/// tell a persistent medium fault from a transient read-path glitch, recording
/// the SST only on confirmed persistence. A re-read error is treated as
/// non-persistent: a genuinely faulty medium resurfaces on the next read, and
/// this must never mask a live-path I/O failure.
///
/// Returns `true` when this call newly queued `table_id` for healing (confirmed
/// persistent fault, scheduling enabled, not already queued). Read paths ignore
/// the return; the patrol scrub uses it to count distinct SSTs it scheduled.
///
/// `charge` says whose read this confirms: a foreground read's confirming
/// re-read goes into the read-byte counters like the read itself, and is
/// charged only once the re-read is actually issued; an untraced one, such as
/// a patrol scrub's, is neither counted nor allowed to touch the caches.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors the block read context needed for the confirming re-read"
)]
pub(crate) fn maybe_record_persistent_heal(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    heal_hints: Option<&crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    charge: ReadCharge,
) -> bool {
    let Some(hints) = heal_hints else {
        return false;
    };
    if !hints.is_enabled() {
        return false;
    }
    // The confirming re-read of a foreground read is a second request for the
    // whole block, charged as issued like the first, and so is what its
    // transform produced, whether or not the block then checks out.
    let mut decoded = 0;
    let reread = reread_block_is_corrected(
        table_id,
        path,
        file_accessor,
        handle,
        block_type,
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
        #[cfg(feature = "metrics")]
        metrics,
        charge,
        &mut decoded,
    );
    #[cfg(feature = "metrics")]
    if charge.is_counted() {
        metrics
            .block_bytes_decoded
            .fetch_add(decoded as u64, core::sync::atomic::Ordering::Relaxed);
    }
    match reread {
        Ok(corrected) => {
            if !corrected {
                log::debug!(
                    "Transient ECC correction on table {table_id:?} block {handle:?}; \
                     re-read clean, not scheduling"
                );
                return false;
            }
            if hints.record(table_id) {
                #[cfg(feature = "metrics")]
                metrics
                    .ecc_auto_heal_scheduled
                    .fetch_add(1, core::sync::atomic::Ordering::Relaxed);
                log::warn!(
                    "Persistent ECC correction on table {table_id:?} block {handle:?}; \
                     queued for healing recompaction"
                );
                return true;
            }
            false
        }
        Err(e) => {
            log::debug!(
                "ECC re-read confirmation for table {table_id:?} block {handle:?} failed: {e:?}"
            );
            false
        }
    }
}

/// Whose read a block read belongs to, which decides whether it is charged to
/// the read counters and whether it may fill the block cache.
///
/// Page-ECC recoveries are counted whoever read the block: they are a health
/// signal about the medium, not a cost of the read.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReadCharge {
    /// A read made for a caller: counted, and cached like any read.
    Foreground,
    /// Maintenance, such as compaction or a patrol scrub: not counted.
    Maintenance,
    /// A read that must leave no trace: not counted, and it neither fills the
    /// block or descriptor cache nor touches the recency of what they hold,
    /// and skips the partial-decode tier, which lives in the block cache. A
    /// monitoring report
    /// (polling it leaves nothing a later read can see), and verification that
    /// must not displace the workload's blocks.
    Untraced,
}

impl ReadCharge {
    /// Whether this read is charged to the read counters.
    #[cfg(feature = "metrics")]
    #[must_use]
    pub(crate) const fn is_counted(self) -> bool {
        matches!(self, Self::Foreground)
    }

    /// Whether this read may leave a trace in the block cache.
    #[must_use]
    pub(crate) const fn touches_cache(self) -> bool {
        !matches!(self, Self::Untraced)
    }
}

/// Outcome of patrol-scrubbing a single block via [`scrub_block`].
//
// Std-gated: the sole consumer (`Table::scrub_data_blocks` → `crate::scrub`)
// is std-only, so gating keeps the no_std build free of dead code.
#[cfg(feature = "std")]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) enum BlockScrubOutcome {
    /// The block read clean: its checksum verified with no parity correction
    /// (covers [`EccStatus::Ok`](crate::table::block::EccStatus::Ok) and
    /// [`EccStatus::Unrecognized`](crate::table::block::EccStatus::Unrecognized)
    /// — in both cases the payload verified against its stored checksum).
    Clean,
    /// The block was recovered from its Page-ECC parity (a latent on-disk fault
    /// was corrected in-flight). `scheduled` is `true` when this read confirmed
    /// the fault is persistent and newly queued the owning SST for a healing
    /// rewrite (`auto_heal` on and the SST not already queued).
    Corrected {
        /// `true` iff this block newly scheduled the SST for healing.
        scheduled: bool,
    },
}

/// Reads one block straight from disk (bypassing the block cache) and runs the
/// full Page-ECC verify+correct path, recording a heal hint on a confirmed
/// persistent correction.
///
/// This is the per-block primitive of the patrol scrub: it proactively reads a
/// (typically cold) block so latent single-block bit-rot is corrected on read
/// and the SST scheduled for a clean rewrite before a second fault in the same
/// block exceeds the parity budget. Unlike [`load_block`] it deliberately
/// **bypasses the block cache** in both directions: it neither serves a cached
/// (already-clean) copy that would hide the on-disk fault, nor inserts the cold
/// block and evicts the live working set.
///
/// Returns the [`BlockScrubOutcome`], or `Err` when the block is uncorrectable
/// (checksum failed and parity could not recover it) or unreadable; the caller
/// records that as an uncorrectable finding rather than silently skipping it.
///
/// Scope: FRAME integrity only — checksum, decompress, decrypt, ECC. The
/// block body (restart trailer, entry framing, value-type tags) is
/// deliberately NOT decoded here: a scrub is a lightweight bit-rot patrol,
/// and any body-level fault in a checksum-clean block is (a) not producible
/// by media corruption (the checksum covers the payload) and (b) the domain
/// of callers that fully decode and validate block bodies (e.g. the salvage
/// walk's row materialization). [`crate::verify::verify_sst_file`] is likewise
/// a frame-level verifier. Both the plain patrol scrub and the in-place heal
/// share exactly this depth.
#[cfg(feature = "std")]
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors load_block's read context minus the cache"
)]
pub(crate) fn scrub_block(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    heal_hints: Option<&crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
) -> crate::Result<BlockScrubOutcome> {
    // The patrol leaves the descriptor cache as it leaves the block cache:
    // walking every table must not evict the workload's descriptors.
    let fd = file_accessor.peek_or_open_table(&table_id, path)?;
    let transform = build_block_transform(
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    let (block, ecc_status, recovery) = Block::from_file_with_recovery(
        fd.as_ref(),
        *handle,
        crate::table::block::BlockIdentity {
            table_id: table_id.table_id(),
            block_type,
            dict_id: compression.dict_id(),
            window_log: 0,
        },
        &transform,
    )?;
    // Role check, mirroring `load_block`'s swap-defence: an index entry
    // misdirected at another (checksum-valid) block of a different role must
    // surface as an uncorrectable finding, not scrub clean.
    if block.header.block_type != block_type {
        return Err(crate::Error::InvalidTag((
            "BlockType",
            block.header.block_type.into(),
        )));
    }

    Ok(match ecc_status {
        crate::table::block::EccStatus::Corrected => {
            // Primary read for the scrub: count the recovery by mechanism here
            // (the confirming re-read inside maybe_record_persistent_heal does
            // not count).
            #[cfg(feature = "metrics")]
            if let Some(kind) = recovery {
                metrics.record_ecc_recovery(kind);
            }
            #[cfg(not(feature = "metrics"))]
            let _ = recovery;
            let scheduled = maybe_record_persistent_heal(
                table_id,
                path,
                file_accessor,
                handle,
                block_type,
                compression,
                encryption,
                ecc,
                #[cfg(zstd_any)]
                zstd_dict,
                heal_hints,
                #[cfg(feature = "metrics")]
                metrics,
                ReadCharge::Untraced,
            );
            BlockScrubOutcome::Corrected { scheduled }
        }
        crate::table::block::EccStatus::Ok | crate::table::block::EccStatus::Unrecognized => {
            BlockScrubOutcome::Clean
        }
    })
}

/// Builds the [`BlockTransform`](crate::table::block::BlockTransform) for a
/// block read from its per-SST codec context.
///
/// ECC presence is a per-SST descriptor property (`ecc`): the transform is
/// upgraded to its `*Ecc` variant when this SST was written with a recognized
/// Page ECC scheme. On a build WITHOUT the `page_ecc` feature `with_ecc` is the
/// identity function — the parity trailer then reads as an unrecognized opaque
/// trailer (the read frames the payload by `data_length`, verifies its
/// checksum, and reports
/// [`EccStatus::Unrecognized`](crate::table::block::EccStatus::Unrecognized)),
/// so the data still loads without ECC recovery rather than failing closed.
// The lifetime is only elidable when the dictionary parameter is compiled
// out: with it two input references share `'a`, and the transform must borrow
// from both for exactly as long.
#[cfg_attr(
    not(zstd_any),
    expect(
        clippy::elidable_lifetime_names,
        reason = "named to stay valid in the zstd build, where a second reference parameter shares it"
    )
)]
pub(crate) fn build_block_transform<'a>(
    compression: CompressionType,
    encryption: Option<&'a dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&'a crate::compression::ZstdDictionary>,
) -> crate::Result<crate::table::block::BlockTransform<'a>> {
    let t = crate::table::block::BlockTransform::from_parts(
        compression,
        encryption,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    Ok(if let Some(ecc) = ecc {
        t.with_ecc(ecc)
    } else {
        t
    })
}

/// Re-reads a block straight from disk (bypassing the block cache) and reports
/// whether its on-disk bytes are *still* ECC-corrected.
///
/// Used to confirm that a correction observed on a cache-miss read reflects a
/// **persistent** on-disk fault rather than a transient read-path glitch (bad
/// RAM / DMA / cable during the first read): a second independent read of the
/// same offset that again recovers from parity proves the bytes on the medium
/// are faulty. Returns whether the re-read was itself ECC-corrected (`false`
/// when it read clean, a transient fault, or carried no recognized parity),
/// and the length the re-read decoded to, for the caller's byte counters.
///
/// Runs only on the cold corrected-read path, so the extra disk read costs
/// nothing on clean reads.
#[expect(
    clippy::too_many_arguments,
    reason = "mirrors load_block's read context minus the cache"
)]
fn reread_block_is_corrected(
    table_id: GlobalTableId,
    path: &Path,
    file_accessor: &FileAccessor,
    handle: &BlockHandle,
    block_type: BlockType,
    compression: CompressionType,
    encryption: Option<&dyn EncryptionProvider>,
    ecc: Option<crate::table::block::EccParams>,
    #[cfg(zstd_any)] zstd_dict: Option<&crate::compression::ZstdDictionary>,
    #[cfg(feature = "metrics")] metrics: &Metrics,
    charge: ReadCharge,
    decoded: &mut usize,
) -> crate::Result<bool> {
    // The confirmation belongs to the read it confirms: an untraced one leaves
    // the descriptor cache as it found it.
    let fd = if charge.touches_cache() {
        file_accessor.get_or_open_table(&table_id, path)?.0
    } else {
        file_accessor.peek_or_open_table(&table_id, path)?
    };
    let transform = build_block_transform(
        compression,
        encryption,
        ecc,
        #[cfg(zstd_any)]
        zstd_dict,
    )?;
    let (_block, ecc_status, _recovery) = Block::from_file_issuing(
        fd.as_ref(),
        *handle,
        crate::table::block::BlockIdentity {
            table_id: table_id.table_id(),
            block_type,
            dict_id: compression.dict_id(),
            window_log: 0,
        },
        &transform,
        // Charged when the read is issued, not before the descriptor opens.
        || {
            #[cfg(feature = "metrics")]
            if charge.is_counted() {
                record_block_read(metrics, block_type, handle.size().into());
            }
        },
        decoded,
    )?;
    Ok(matches!(
        ecc_status,
        crate::table::block::EccStatus::Corrected
    ))
}

// Cached, `no_std`-friendly CPU-feature tokens for the SIMD dispatch below.
// `cpufeatures::new!` generates a module whose `get()` runs CPUID once (atomic
// load thereafter) AND verifies OS AVX-state enablement via XCR0, so the AVX
// paths cannot SIGILL on a CPU/OS that does not actually support them. These
// work identically under `std` and `no_std` — which is why the dispatch is no
// longer `std`-gated and `no_std` x86 builds keep the wide SIMD lanes instead
// of dropping to the scalar tail.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
cpufeatures::new!(cpu_avx512bw, "avx512bw");
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
cpufeatures::new!(cpu_avx2, "avx2");
// SSE2 is the mandatory `x86_64` ISA baseline, so it is only runtime-detected on
// 32-bit x86 (pre-Pentium-4 may lack it); `x86_64` takes the SSE2 arm directly.
#[cfg(target_arch = "x86")]
cpufeatures::new!(cpu_sse2, "sse2");

/// Returns the length of the longest shared byte prefix of `s1` and `s2`.
///
/// This is on the hot path of block encoding during flush and compaction —
/// every truncated entry pays one call against the restart base key.
///
/// Dispatch:
/// - **`x86_64` / `x86` with AVX-512BW** (runtime-detected): 64-byte vectorized lanes via
///   `_mm512_cmpeq_epi8_mask`. Checked first so AVX-512 hosts use the widest lane.
/// - **`x86_64` / `x86` with AVX2** (runtime-detected): 32-byte vectorized lanes via `_mm256_cmpeq_epi8`.
/// - **`x86_64` with SSE2**: 16-byte lanes via `_mm_cmpeq_epi8` — SSE2 is the mandatory `x86_64`
///   ISA baseline, so this path needs no runtime check, only the AVX2 negative result.
/// - **`x86` (32-bit) with SSE2** (runtime-detected): same 16-byte kernel, but SSE2 is *not*
///   guaranteed on 32-bit x86 (pre-Pentium-4 lacks it), so it is runtime-detected; pre-SSE2
///   hosts fall through to the scalar kernel below.
/// - **`aarch64` little-endian**: 16-byte vectorized lanes via NEON (`ARMv8` baseline — no runtime check).
/// - **Everything else** (incl. big-endian aarch64, pre-SSE2 32-bit x86, riscv, powerpc): 8-byte word
///   stride via XOR. First-mismatch position uses `trailing_zeros() / 8` on little-endian
///   targets and `leading_zeros() / 8` on big-endian, so the byte ordering of the word matches
///   the byte ordering of the source slice on either endianness.
///
/// CPU-feature detection is cached (one CPUID on first call via `cpufeatures`,
/// an atomic load thereafter) and works under `no_std`, so the per-call dispatch
/// cost is one to three cached atomic loads on x86 and the wide lanes are taken
/// on capable hosts regardless of whether the `std` feature is enabled.
///
/// This convenience wrapper resolves the kernel on every call. In a hot loop,
/// resolve once with [`resolve_lsp_kernel`] at loop entry and call the returned
/// pointer per item so feature detection does not run per comparison.
#[must_use]
pub fn longest_shared_prefix_length(s1: &[u8], s2: &[u8]) -> usize {
    // SAFETY: `resolve_lsp_kernel` only returns a kernel whose required CPU
    // feature it has just verified present on this host (or the always-safe
    // scalar kernel), so invoking it on this same host cannot execute an
    // unsupported instruction.
    unsafe { resolve_lsp_kernel()(s1, s2) }
}

/// Function-pointer type for the longest-shared-prefix kernel.
///
/// `unsafe` because the SIMD kernels require their target CPU feature to be
/// present; [`resolve_lsp_kernel`] only ever hands back a pointer whose feature
/// it verified (or the always-safe scalar kernel), so a caller invoking the
/// returned pointer on the same host upholds that contract.
pub type LspKernel = unsafe fn(&[u8], &[u8]) -> usize;

/// Resolves the best longest-shared-prefix kernel for the current CPU **once**
/// and returns it as a function pointer.
///
/// Call this at the entry to a hot loop (e.g. when a block encoder is built) and
/// invoke the returned pointer per item, so CPU-feature detection runs a single
/// time instead of on every comparison. Selection order matches
/// [`longest_shared_prefix_length`]: AVX-512BW, then AVX2, then SSE2 (the
/// `x86_64` baseline; runtime-detected on 32-bit `x86`), then NEON on LE
/// `aarch64`, then the portable scalar kernel.
#[must_use]
pub fn resolve_lsp_kernel() -> LspKernel {
    // Each `cfg` block below is a tail expression and exactly one compiles per
    // target, so there is no `return`/unreachable-tail bookkeeping. The x86
    // path detects features at runtime and defers the actual choice to the pure
    // `select_lsp_kernel`, which is exhaustively unit-tested on any host (the
    // detection itself can only exercise the current CPU's lane).
    #[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
    {
        select_lsp_kernel(LspCpuFeatures {
            avx512bw: cpu_avx512bw::get(),
            avx2: cpu_avx2::get(),
            // SSE2 is the mandatory x86_64 ISA baseline (always present); on
            // 32-bit x86 it is runtime-detected.
            #[cfg(target_arch = "x86_64")]
            sse2: true,
            #[cfg(target_arch = "x86")]
            sse2: cpu_sse2::get(),
        })
    }
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    {
        // NEON is mandatory in the ARMv8 baseline `target_arch = "aarch64"` implies.
        lsp_neon
    }
    #[cfg(not(any(
        target_arch = "x86_64",
        target_arch = "x86",
        all(target_arch = "aarch64", target_endian = "little")
    )))]
    {
        // Portable fallback (big-endian aarch64, riscv, powerpc, …).
        lsp_scalar
    }
}

/// Detected x86 SIMD features the longest-shared-prefix dispatch selects on.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[derive(Clone, Copy)]
struct LspCpuFeatures {
    avx512bw: bool,
    avx2: bool,
    sse2: bool,
}

/// Pure kernel selection from detected x86 features: widest lane first
/// (AVX-512BW, then AVX2, then SSE2), else the portable scalar kernel.
///
/// Split out from [`resolve_lsp_kernel`]'s runtime detection so every branch is
/// exhaustively unit-testable on any host — the detection can only ever
/// exercise the lane the test CPU happens to expose.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
fn select_lsp_kernel(f: LspCpuFeatures) -> LspKernel {
    if f.avx512bw {
        lsp_avx512
    } else if f.avx2 {
        lsp_avx2
    } else if f.sse2 {
        lsp_sse2
    } else {
        lsp_scalar
    }
}

/// 8-byte word-stride scalar implementation — works on every platform, no intrinsics.
///
/// Compares 8 bytes at a time via `u64` XOR and locates the first mismatching byte
/// using an endian-aware bit-count:
/// - **Little-endian** (`target_endian = "little"`): `trailing_zeros() / 8` — the
///   lowest-numbered bit in the XOR word corresponds to the first source byte.
/// - **Big-endian** (`target_endian = "big"`): `leading_zeros() / 8` — the highest-numbered
///   bit in the XOR word corresponds to the first source byte.
///
/// Tail shorter than 8 bytes falls back to a byte-by-byte loop.
// On little-endian aarch64 the NEON kernel is selected unconditionally (NEON is
// in the ARMv8 baseline), so the portable scalar fallback has no caller on that
// target alone — it is still the live kernel on x86 without SSE2, big-endian
// aarch64, riscv, powerpc, etc. `expect` (not `allow`) keeps this honest: it
// errors if the fallback ever becomes reachable here, or if it stops being dead.
#[cfg_attr(
    all(target_arch = "aarch64", target_endian = "little", not(test)),
    expect(
        dead_code,
        reason = "portable LSP fallback; aarch64-LE always selects the NEON kernel \
                  (unit tests still call it directly, hence not(test))"
    )
)]
#[must_use]
pub(crate) fn lsp_scalar(s1: &[u8], s2: &[u8]) -> usize {
    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 8 <= min_len {
        // SAFETY: i + 8 <= min_len <= s{1,2}.len() — both 8-byte reads are in-bounds.
        // `read_unaligned` documents that the pointer needs no alignment, so the
        // `*const u8 -> *const u64` cast is sound. Clippy's `cast_ptr_alignment`
        // does NOT fire here (verified across all CI targets including BE powerpc64)
        // because the cast feeds directly into `read_unaligned`, which clippy
        // recognises as an unaligned-load idiom.
        #[expect(unsafe_code, reason = "bounds checked by loop guard above")]
        let (a, b) = unsafe {
            (
                s1.as_ptr().add(i).cast::<u64>().read_unaligned(),
                s2.as_ptr().add(i).cast::<u64>().read_unaligned(),
            )
        };
        let diff = a ^ b;
        if diff != 0 {
            // Endian-independent: position of first byte-level difference.
            // On LE the lowest mismatching byte is at trailing_zeros / 8;
            // on BE it is at leading_zeros / 8. Use the matching primitive.
            #[cfg(target_endian = "little")]
            let byte_off = (diff.trailing_zeros() / 8) as usize;
            #[cfg(target_endian = "big")]
            let byte_off = (diff.leading_zeros() / 8) as usize;
            return i + byte_off;
        }
        i += 8;
    }

    while i < min_len {
        // SAFETY: i < min_len <= s{1,2}.len()
        #[expect(unsafe_code, reason = "i < min_len bounds-checked above")]
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// AVX2 implementation — 32 bytes per iteration via `_mm256_cmpeq_epi8`.
///
/// # Safety
///
/// Caller must ensure the host CPU supports AVX2 (`cpu_avx2::get()`).
// Compiled on every x86 / x86_64 build (std and no_std): the runtime dispatch
// above selects it via cached CPU-feature detection, and the #[cfg(test)]
// kernel tests below exercise it directly.
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "avx2")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_avx2(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8,
    };

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 32 <= min_len {
        // SAFETY: i + 32 <= min_len ≤ s{1,2}.len() — both 32-byte loads are in-bounds.
        // `_mm256_loadu_si256` is the *unaligned* load, so the u8→__m256i pointer cast
        // does not require 32-byte alignment (the pointer is only used by `loadu`).
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "_mm256_loadu_si256 explicitly performs an unaligned 32-byte load"
        )]
        let (va, vb) = unsafe {
            (
                _mm256_loadu_si256(s1.as_ptr().add(i).cast::<__m256i>()),
                _mm256_loadu_si256(s2.as_ptr().add(i).cast::<__m256i>()),
            )
        };
        // Register-only AVX2 intrinsics under #[target_feature(enable = "avx2")] —
        // no `unsafe` block needed; the function-level `unsafe` covers their availability.
        let cmp = _mm256_cmpeq_epi8(va, vb);
        // `_mm256_movemask_epi8` returns the byte-mask as a signed `i32`. We treat the
        // bit pattern as `u32` for trailing-zeros math — `cast_unsigned()` makes the
        // sign-preserving reinterpretation explicit.
        let mask = _mm256_movemask_epi8(cmp).cast_unsigned();
        if mask != u32::MAX {
            return i + (!mask).trailing_zeros() as usize;
        }
        i += 32;
    }

    // Tail: byte-stride (≤31 bytes left, not worth dispatching a narrower kernel).
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// AVX-512BW implementation — 64 bytes per iteration via `_mm512_cmpeq_epi8_mask`.
///
/// The widest `x86_64` lane: one iteration consumes a full 64-byte cache line, so
/// keys that share a long prefix (time-series, tenant-prefixed, sorted UUIDs)
/// settle in half the iterations of the AVX2 kernel. `_mm512_cmpeq_epi8_mask`
/// folds the 64-lane byte comparison directly into a `__mmask64`, avoiding the
/// separate `movemask` step the AVX2/SSE2 kernels need.
///
/// # Safety
///
/// Caller must ensure the host CPU supports AVX-512BW (`cpu_avx512bw::get()`).
/// BW implies the F subset, so the 512-bit load and the byte-granular
/// compare-mask are both available.
// See `lsp_avx2`: compiled on every x86 build (std and no_std).
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
// List both ISA features the body relies on: `_mm512_loadu_si512` is AVX-512F,
// `_mm512_cmpeq_epi8_mask` is AVX-512BW. BW implies F (so `avx512bw` alone would
// compile), but naming both keeps the gate matching the actual requirements and
// guards against a future edit dropping the BW-only compare without noticing the
// F load is still gated. Runtime detection on `avx512bw` is sufficient because
// any CPU exposing BW necessarily implements F.
#[target_feature(enable = "avx512bw,avx512f")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_avx512(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 64 <= min_len {
        // SAFETY: i + 64 <= min_len ≤ s{1,2}.len() — both 64-byte loads are in-bounds.
        // `_mm512_loadu_si512` is the *unaligned* load, so the u8→__m512i pointer cast
        // does not require 64-byte alignment (the pointer is only used by `loadu`).
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "_mm512_loadu_si512 explicitly performs an unaligned 64-byte load"
        )]
        let (va, vb) = unsafe {
            (
                _mm512_loadu_si512(s1.as_ptr().add(i).cast::<__m512i>()),
                _mm512_loadu_si512(s2.as_ptr().add(i).cast::<__m512i>()),
            )
        };
        // `_mm512_cmpeq_epi8_mask` yields a 64-bit mask: bit j is set iff byte j is
        // equal. A full-match lane is `u64::MAX`; the first mismatch is the lowest
        // zero bit of the mask, i.e. the lowest set bit of its complement.
        let mask = _mm512_cmpeq_epi8_mask(va, vb);
        if mask != u64::MAX {
            return i + (!mask).trailing_zeros() as usize;
        }
        i += 64;
    }

    // Tail: byte-stride (≤63 bytes left, not worth dispatching a narrower kernel).
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// SSE2 implementation — 16 bytes per iteration via `_mm_cmpeq_epi8`.
///
/// Used on `x86_64` hosts that lack AVX2 (older Intel Atoms, some sandboxed
/// VMs / containers, AMD pre-Excavator, low-power embedded `x86_64`) and on
/// 32-bit `x86` hosts with SSE2 but without AVX2.
///
/// # Safety
///
/// Caller must ensure the host supports SSE2. On `x86_64` this is the mandatory
/// ISA baseline (always true); on 32-bit `x86` it must be runtime-detected via
/// `cpu_sse2::get()` because pre-Pentium-4 CPUs lack it.
// See `lsp_avx2`: compiled on every x86 build (std and no_std).
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[target_feature(enable = "sse2")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_sse2(s1: &[u8], s2: &[u8]) -> usize {
    #[cfg(target_arch = "x86")]
    use core::arch::x86::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};
    #[cfg(target_arch = "x86_64")]
    use core::arch::x86_64::{__m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    while i + 16 <= min_len {
        // SAFETY: i + 16 <= min_len ≤ s{1,2}.len() — both 16-byte loads are in-bounds.
        // `_mm_loadu_si128` is the *unaligned* load, so the u8→__m128i pointer cast
        // does not require 16-byte alignment (the pointer is only used by `loadu`).
        #[expect(
            clippy::cast_ptr_alignment,
            reason = "_mm_loadu_si128 explicitly performs an unaligned 16-byte load"
        )]
        let (va, vb) = unsafe {
            (
                _mm_loadu_si128(s1.as_ptr().add(i).cast::<__m128i>()),
                _mm_loadu_si128(s2.as_ptr().add(i).cast::<__m128i>()),
            )
        };
        // Register-only SSE2 intrinsics under #[target_feature(enable = "sse2")] —
        // safe in stable Rust without an inner `unsafe` block.
        let cmp = _mm_cmpeq_epi8(va, vb);
        // `_mm_movemask_epi8` returns the 16-bit byte-mask as a signed `i32`
        // (low 16 bits used, high 16 zero). Reinterpret as `u32` for trailing-zeros math.
        let mask = _mm_movemask_epi8(cmp).cast_unsigned();
        // SSE2 mask is 16 bits, so a full-match lane is `0xFFFF`, not `u32::MAX`.
        if mask != 0xFFFF {
            return i + (!mask).trailing_zeros() as usize;
        }
        i += 16;
    }

    // Tail: byte-stride (≤15 bytes left).
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// NEON implementation — 16 bytes per iteration via `vceqq_u8` + byte-wise mask reduction.
///
/// Restricted to **little-endian** aarch64 because the lane-to-memory mapping of
/// `vgetq_lane_u64` and the `trailing_zeros() / 8` mismatch-position math both
/// assume LE byte order. Big-endian aarch64 falls back to the scalar kernel.
///
/// # Safety
///
/// NEON is part of the `ARMv8` baseline and is always available on `target_arch = "aarch64"`,
/// so no runtime detection is needed. The `unsafe` is required only because the intrinsics
/// themselves are `unsafe fn`.
#[cfg(all(target_arch = "aarch64", target_endian = "little"))]
#[target_feature(enable = "neon")]
#[expect(unsafe_code, reason = "intrinsics require unsafe")]
#[must_use]
unsafe fn lsp_neon(s1: &[u8], s2: &[u8]) -> usize {
    use core::arch::aarch64::{
        vandq_u8, vceqq_u8, vdupq_n_u8, vgetq_lane_u64, vld1q_u8, vreinterpretq_u64_u8,
    };

    let min_len = s1.len().min(s2.len());
    let mut i = 0;

    // 16-byte equality mask: lanes are 0xFF when bytes match, 0x00 when they differ.
    // Reduce to a 128-bit value and inspect its halves as u64 for first-mismatch position.
    while i + 16 <= min_len {
        // SAFETY: i + 16 <= min_len ≤ s{1,2}.len() — both 16-byte loads are in-bounds.
        let (va, vb) = unsafe { (vld1q_u8(s1.as_ptr().add(i)), vld1q_u8(s2.as_ptr().add(i))) };
        // Register-only NEON intrinsics — safe in stable Rust under the `neon` target feature.
        let cmp = vceqq_u8(va, vb);
        // Trim to bit-per-byte mask via AND with 0xFF (no-op for the equality result,
        // but keeps the intent explicit); reinterpret as two u64 halves.
        let masked = vandq_u8(cmp, vdupq_n_u8(0xFF));
        let as_u64 = vreinterpretq_u64_u8(masked);
        let lo = vgetq_lane_u64(as_u64, 0);
        let hi = vgetq_lane_u64(as_u64, 1);

        if lo != u64::MAX {
            // First mismatching byte is in the low half.
            return i + (!lo).trailing_zeros() as usize / 8;
        }
        if hi != u64::MAX {
            // First mismatching byte is in the high half.
            return i + 8 + (!hi).trailing_zeros() as usize / 8;
        }
        i += 16;
    }

    // Tail: byte-stride for the ≤15 remaining bytes.
    while i < min_len {
        // SAFETY: i < min_len ≤ s{1,2}.len()
        let (a, b) = unsafe { (*s1.get_unchecked(i), *s2.get_unchecked(i)) };
        if a != b {
            return i;
        }
        i += 1;
    }

    min_len
}

/// Compares the conceptual concatenation `prefix + suffix` against `needle`
/// using the given comparator.
///
/// For the default lexicographic comparator this performs a zero-allocation
/// bytewise comparison. Custom comparators fall back to concatenating prefix
/// and suffix into a temporary `Vec` so that `UserComparator::compare` always
/// receives a complete key.
#[must_use]
pub fn compare_prefixed_slice(
    prefix: &[u8],
    suffix: &[u8],
    needle: &[u8],
    cmp: &dyn crate::comparator::UserComparator,
) -> core::cmp::Ordering {
    // Fast path: zero-allocation bytewise comparison for the default
    // (lexicographic) comparator. This is the hot path for block index
    // and data block binary searches.
    if cmp.is_lexicographic() {
        return compare_prefixed_slice_lexicographic(prefix, suffix, needle);
    }

    // Slow path: materialize prefix+suffix into a contiguous buffer for
    // custom comparators. Uses a stack buffer for typical key sizes to
    // avoid heap allocation on the hot binary-search path.
    let total_len = prefix.len() + suffix.len();

    if total_len <= 256 {
        let mut buf = [0_u8; 256];

        // SAFETY (indexing): total_len <= 256 == buf.len(), and
        // prefix.len() + suffix.len() == total_len, so all slices are in bounds.
        #[expect(clippy::indexing_slicing, reason = "total_len <= 256 checked above")]
        {
            buf[..prefix.len()].copy_from_slice(prefix);
            buf[prefix.len()..total_len].copy_from_slice(suffix);
        }

        #[expect(clippy::indexing_slicing, reason = "total_len <= 256 checked above")]
        return cmp.compare(&buf[..total_len], needle);
    }

    // Fallback for unusually large keys: allocate a temporary Vec.
    let mut full_key = Vec::with_capacity(total_len);
    full_key.extend_from_slice(prefix);
    full_key.extend_from_slice(suffix);
    cmp.compare(&full_key, needle)
}

/// Zero-allocation lexicographic comparison of `prefix + suffix` against `needle`.
#[must_use]
fn compare_prefixed_slice_lexicographic(
    prefix: &[u8],
    suffix: &[u8],
    needle: &[u8],
) -> core::cmp::Ordering {
    use core::cmp::Ordering::{Equal, Greater};

    if needle.is_empty() {
        let combined_len = prefix.len() + suffix.len();
        return if combined_len > 0 { Greater } else { Equal };
    }

    let max_pfx_len = prefix.len().min(needle.len());

    {
        // SAFETY: max_pfx_len = min(prefix.len(), needle.len()), so both
        // slices [0..max_pfx_len] are within bounds by construction.
        #[expect(
            unsafe_code,
            reason = "max_pfx_len <= prefix.len() && max_pfx_len <= needle.len()"
        )]
        let pfx = unsafe { prefix.get_unchecked(0..max_pfx_len) };

        #[expect(
            unsafe_code,
            reason = "max_pfx_len <= prefix.len() && max_pfx_len <= needle.len()"
        )]
        let ndl = unsafe { needle.get_unchecked(0..max_pfx_len) };

        match pfx.cmp(ndl) {
            Equal => {}
            ordering => return ordering,
        }
    }

    // Clamp-to-zero: when `needle` is longer than `prefix` there is no remainder
    // (the prefix is exhausted), which the `> 0` check below treats correctly.
    let rest_len = prefix.len().saturating_sub(needle.len());
    if rest_len > 0 {
        return Greater;
    }

    // SAFETY: rest_len == 0 means prefix.len() <= needle.len(), so
    // max_pfx_len == prefix.len() <= needle.len() and needle[max_pfx_len..] is in-bounds.
    #[expect(
        unsafe_code,
        reason = "max_pfx_len <= needle.len() guaranteed by rest_len == 0 guard above"
    )]
    let remaining_needle = unsafe { needle.get_unchecked(max_pfx_len..) };
    suffix.cmp(remaining_needle)
}

#[cfg(test)]
mod tests;
