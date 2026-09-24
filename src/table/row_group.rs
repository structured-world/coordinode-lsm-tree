// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! Reading a columnar row group: its page directory, then the pages a read
//! wants.
//!
//! The layout this reads is specified in `docs/columnar-page-format.md`. A
//! full read takes the group in one request; a projection reads the
//! directory, then only the pages of the columns it asked for.

use alloc::{sync::Arc, vec::Vec};

use super::util::{ReadCharge, admit_read_block, build_block_transform};
#[cfg(feature = "metrics")]
use super::util::{record_block_decoded, record_block_load_cached, record_block_read};
use super::{Block, BlockHandle, BlockOffset, GlobalTableId};
use crate::coding::Decode;
use crate::fs::FsFile;
use crate::path::Path;
use crate::table::block::{BlockType, EccParams, Header};
use crate::table::column_page::{PageDirectory, PageEntry};
use crate::{
    Cache, CompressionType, Slice, encryption::EncryptionProvider, file_accessor::FileAccessor,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// A columnar row group as read: its directory and the pages that were
/// fetched, each verified and cached as the block it is.
pub struct RowGroupBlocks {
    pub directory: PageDirectory,
    /// One slot per directory entry, in the directory's order: the page's
    /// block when it was fetched, `None` when the read did not want it.
    pub pages: Vec<Option<Block>>,
}

/// How much of a row group a selective read fetches before it knows the
/// directory's length.
///
/// The index entry spans the whole group and does not record where the
/// directory ends, but the directory's block header, at the front, does. One
/// request of this size covers the directory of any realistic schema and,
/// since the key page follows it, usually the key page too, which is what
/// every read of the group needs next. A directory longer than this is
/// completed by a second request.
const DIRECTORY_PREFIX: usize = 4 * 1_024;

/// The pages a read of a row group wants: the columns it projects, times the
/// row pages that hold the rows it asks for.
#[derive(Clone, Debug, Default)]
pub struct PageWant<'a> {
    /// The column ids read, or every column when `None`.
    pub columns: Option<&'a [u16]>,
    /// The row pages read, half-open, or every row page when `None`.
    pub row_pages: Option<core::ops::Range<u16>>,
}

impl<'a> PageWant<'a> {
    /// Every page of the group.
    pub const ALL: Self = Self {
        columns: None,
        row_pages: None,
    };

    /// Every row page of the columns `columns` names.
    #[must_use]
    pub const fn columns(columns: &'a [u16]) -> Self {
        Self {
            columns: Some(columns),
            row_pages: None,
        }
    }

    /// Whether this is a read of the whole group.
    const fn is_all(&self) -> bool {
        self.columns.is_none() && self.row_pages.is_none()
    }

    /// Whether the read needs the page `entry` names.
    fn wants(&self, entry: &PageEntry) -> bool {
        self.columns.is_none_or(|w| w.contains(&entry.id.column_id))
            && self
                .row_pages
                .as_ref()
                .is_none_or(|r| r.contains(&entry.row_page))
    }
}

/// A row group decoded row page by row page: one batch per row page read, in
/// row order, each holding the columns the read projected.
#[derive(Debug)]
pub struct RowPages {
    /// The ordinal of the first row page read.
    pub first_page: u16,
    /// The group row the first row page read starts at.
    pub first_row: u32,
    /// One batch per row page read, in row order.
    pub batches: Vec<crate::table::columnar::ColumnBatch>,
}

impl RowPages {
    /// The rows the batches hold between them.
    #[must_use]
    pub fn row_count(&self) -> u32 {
        // The directory proved the row pages sum to the group's `u32` row
        // count, so a subset of them cannot overflow.
        self.batches.iter().map(|b| b.row_count).sum()
    }

    /// The batches joined into one ([`ColumnBatch::concat`]), for a consumer
    /// that takes the group whole.
    ///
    /// [`ColumnBatch::concat`]: crate::table::columnar::ColumnBatch::concat
    ///
    /// # Errors
    ///
    /// As [`ColumnBatch::concat`].
    pub fn into_batch(self) -> crate::Result<crate::table::columnar::ColumnBatch> {
        crate::table::columnar::ColumnBatch::concat(self.batches)
    }
}

/// Everything a row group read needs to fetch, verify, count and cache the
/// blocks of one group: the context [`crate::table::util::load_block`] takes
/// for one block, plus the group's index entry.
pub struct GroupRead<'a> {
    pub table_id: GlobalTableId,
    pub path: &'a Path,
    pub file_accessor: &'a FileAccessor,
    pub cache: &'a Cache,
    /// The index entry, which spans the whole group.
    pub group: &'a BlockHandle,
    pub compression: CompressionType,
    pub encryption: Option<&'a dyn EncryptionProvider>,
    pub ecc: Option<EccParams>,
    #[cfg(zstd_any)]
    pub zstd_dict: Option<&'a crate::compression::ZstdDictionary>,
    pub heal_hints: Option<&'a crate::heal_hints::HealHints>,
    #[cfg(feature = "metrics")]
    pub metrics: &'a Metrics,
    /// Whose read this is, applied as `load_block` applies it.
    pub charge: ReadCharge,
}

impl GroupRead<'_> {
    /// Loads the group's directory and the pages `want` selects.
    ///
    /// Nothing is read for a block already cached. Every block read is
    /// verified and admitted separately ([`admit_read_block`]), so each is
    /// cached under its own offset with its own buffer: a page cached as a
    /// view of a shared read would keep the whole read alive while the cache
    /// charged it as one page.
    ///
    /// When the directory is cached, only the wanted pages that are not are
    /// read, each run of consecutive ones as one request: a full read after a
    /// point read has fetched the key page reads the rest of the group, not the
    /// key page again. When it is not, `wanted == None` reads the group in ONE
    /// request, as a full scan wants, and `Some` reads the directory first (a
    /// [`DIRECTORY_PREFIX`] of the group, extended if the directory is longer)
    /// and then the wanted pages the same way, reusing any bytes the prefix
    /// brought in.
    ///
    /// Each request is charged when it is issued: the whole group and page
    /// runs to the data role, the directory prefix to the index role. Each
    /// block's load and decode are then counted under its own role as it is
    /// verified.
    ///
    /// # Errors
    ///
    /// Any block's verification error, or [`crate::Error::InvalidHeader`] when
    /// the directory describes a layout that does not fill the group exactly:
    /// a gap or an overrun means the index entry and the directory disagree
    /// about where the group ends, and neither can be trusted to name the
    /// right bytes.
    pub(crate) fn load(&self, want: &PageWant<'_>) -> crate::Result<RowGroupBlocks> {
        if let Some(directory_block) =
            self.lookup(self.group.offset(), BlockType::ColumnPageDirectory)?
        {
            let directory = PageDirectory::decode(&directory_block.data)?;
            let directory_len = directory_block.header.on_disk_size_with(self.ecc);
            check_group_extent(self.group, directory_len, &directory)?;
            let pages = self.cached_pages(&directory, directory_len, want)?;
            self.count_cached(true, &pages);
            let complete = pages
                .iter()
                .zip(directory.entries())
                .all(|(page, entry)| page.is_some() || !want.wants(entry));
            if complete {
                return Ok(RowGroupBlocks { directory, pages });
            }
            let fd = self.open()?;
            let pages = self.fetch_missing(
                fd.as_ref(),
                &directory,
                directory_len,
                &Slice::empty(),
                pages,
                want,
            )?;
            return Ok(RowGroupBlocks { directory, pages });
        }

        let fd = self.open()?;
        if want.is_all() {
            self.read_whole(fd.as_ref())
        } else {
            self.read_selective(fd.as_ref(), want)
        }
    }

    /// The whole group in one request, every block verified and admitted.
    fn read_whole(&self, fd: &dyn FsFile) -> crate::Result<RowGroupBlocks> {
        let frame = self.read(fd, 0, self.group.size() as usize, BlockType::ColumnPage)?;
        let (directory, directory_len) = self.admit_directory(&frame)?;
        let pages = self.fetch_missing(
            fd,
            &directory,
            directory_len,
            &frame,
            alloc::vec![None; directory.entries().len()],
            &PageWant::ALL,
        )?;
        Ok(RowGroupBlocks { directory, pages })
    }

    /// The directory from a prefix of the group, then the wanted pages.
    fn read_selective(
        &self,
        fd: &dyn FsFile,
        want: &PageWant<'_>,
    ) -> crate::Result<RowGroupBlocks> {
        let group_len = self.group.size() as usize;
        let prefix = self.read(
            fd,
            0,
            group_len.min(DIRECTORY_PREFIX),
            BlockType::ColumnPageDirectory,
        )?;
        let directory_len = Header::decode_from(&mut &prefix[..])?.on_disk_size_with(self.ecc);
        let directory_end = directory_len as usize;
        if directory_end > group_len {
            return Err(crate::Error::InvalidHeader(
                "columnar: page directory overruns its row group",
            ));
        }
        // A directory longer than the prefix: fetch the rest of it, and keep
        // one buffer so the page reads below see a single contiguous prefix.
        let prefix = if directory_end > prefix.len() {
            let rest = self.read(
                fd,
                prefix.len(),
                directory_end - prefix.len(),
                BlockType::ColumnPageDirectory,
            )?;
            Slice::fused(&prefix, &rest)
        } else {
            prefix
        };
        let (directory, directory_len) = self.admit_directory(&prefix)?;
        let pages = self.cached_pages(&directory, directory_len, want)?;
        self.count_cached(false, &pages);
        let pages = self.fetch_missing(fd, &directory, directory_len, &prefix, pages, want)?;
        Ok(RowGroupBlocks { directory, pages })
    }

    /// Verifies and admits the directory at the front of `front`, then proves
    /// it fills the group. Returns it with its on-disk length.
    fn admit_directory(&self, front: &Slice) -> crate::Result<(PageDirectory, u32)> {
        let directory_len = Header::decode_from(&mut &front[..])?.on_disk_size_with(self.ecc);
        let handle = BlockHandle::new(self.group.offset(), directory_len);
        let bytes = front
            .get(..directory_len as usize)
            .ok_or(crate::Error::InvalidHeader(
                "columnar: page directory overruns its row group",
            ))?;
        let block = self.verify_and_admit(
            bytes,
            &handle,
            BlockType::ColumnPageDirectory,
            CompressionType::None,
            false,
        )?;
        let directory = PageDirectory::decode(&block.data)?;
        check_group_extent(self.group, directory_len, &directory)?;
        Ok((directory, directory_len))
    }

    /// Fetches every page `want` selects that `pages` does not hold yet.
    ///
    /// Pages are contiguous in directory order (the extent check proved it),
    /// so a run of consecutive missing pages is one byte range and is read in
    /// one request. `front` holds the group's first bytes when an earlier
    /// read brought them in, and a range it covers is served from it rather
    /// than asked for again.
    fn fetch_missing(
        &self,
        fd: &dyn FsFile,
        directory: &PageDirectory,
        directory_len: u32,
        front: &Slice,
        mut pages: Vec<Option<Block>>,
        want: &PageWant<'_>,
    ) -> crate::Result<Vec<Option<Block>>> {
        let entries = directory.entries();
        let runs = {
            let missing = |i: usize| {
                pages.get(i).is_some_and(Option::is_none)
                    && entries.get(i).is_some_and(|e| want.wants(e))
            };
            let mut runs = Vec::new();
            let mut i = 0;
            while i < entries.len() {
                if !missing(i) {
                    i += 1;
                    continue;
                }
                let first = i;
                while i < entries.len() && missing(i) {
                    i += 1;
                }
                runs.push(first..i);
            }
            runs
        };
        // Every offset below is within the group: the extent check proved
        // that the directory and its pages fill it exactly, and the group's
        // length is a `u32`.
        let page_at = |entry: &PageEntry| directory_len as usize + entry.offset as usize;
        for run in runs {
            let (Some(first), Some(last)) = (entries.get(run.start), entries.get(run.end - 1))
            else {
                continue;
            };
            let start = page_at(first);
            let end = page_at(last) + last.length as usize;
            let span = self.span(fd, front, start, end)?;
            for index in run {
                let Some(entry) = entries.get(index) else {
                    continue;
                };
                let at = page_at(entry) - start;
                let bytes =
                    span.get(at..at + entry.length as usize)
                        .ok_or(crate::Error::InvalidHeader(
                            "columnar: page outside its row group",
                        ))?;
                let handle = BlockHandle::new(
                    BlockOffset(*self.group.offset() + page_at(entry) as u64),
                    entry.length,
                );
                let page = self.verify_and_admit(
                    bytes,
                    &handle,
                    BlockType::ColumnPage,
                    self.compression,
                    true,
                )?;
                if let Some(slot) = pages.get_mut(index) {
                    *slot = Some(page);
                }
            }
        }
        Ok(pages)
    }

    /// The group's bytes `[start, end)`, served from `front` as far as it
    /// reaches and read for the rest.
    fn span(
        &self,
        fd: &dyn FsFile,
        front: &Slice,
        start: usize,
        end: usize,
    ) -> crate::Result<Slice> {
        if end <= front.len() {
            return Ok(front.slice(start..end));
        }
        if start >= front.len() {
            return self.read(fd, start, end - start, BlockType::ColumnPage);
        }
        let tail = self.read(fd, front.len(), end - front.len(), BlockType::ColumnPage)?;
        Ok(Slice::fused(&front.slice(start..), &tail))
    }

    /// Reads `len` bytes at `at` within the group, charged to `role` as the
    /// request is issued.
    fn read(
        &self,
        fd: &dyn FsFile,
        at: usize,
        len: usize,
        role: BlockType,
    ) -> crate::Result<Slice> {
        #[cfg(feature = "metrics")]
        if self.charge.is_counted() {
            record_block_read(self.metrics, role, len as u64);
        }
        #[cfg(not(feature = "metrics"))]
        let _ = role;
        Ok(crate::file::read_exact(
            fd,
            *self.group.offset() + at as u64,
            len,
        )?)
    }

    /// The pages `want` selects that the cache already holds.
    fn cached_pages(
        &self,
        directory: &PageDirectory,
        directory_len: u32,
        want: &PageWant<'_>,
    ) -> crate::Result<Vec<Option<Block>>> {
        let mut pages = Vec::with_capacity(directory.entries().len());
        for entry in directory.entries() {
            let page = if want.wants(entry) {
                let offset =
                    *self.group.offset() + u64::from(directory_len) + u64::from(entry.offset);
                self.lookup(BlockOffset(offset), BlockType::ColumnPage)?
            } else {
                None
            };
            pages.push(page);
        }
        Ok(pages)
    }

    /// A cached block at `offset`, looked up without promoting it when this
    /// read must not touch the cache, and refused when it is not `expected`.
    fn lookup(&self, offset: BlockOffset, expected: BlockType) -> crate::Result<Option<Block>> {
        let block = if self.charge.touches_cache() {
            self.cache.get_block(self.table_id, offset)
        } else {
            self.cache.peek_block(self.table_id, offset)
        };
        match block {
            Some(block) if block.header.block_type != expected => Err(crate::Error::InvalidTag((
                "BlockType",
                block.header.block_type.into(),
            ))),
            other => Ok(other),
        }
    }

    /// Counts the cached blocks a read served: the directory when it came
    /// from the cache, and each page it holds.
    #[cfg_attr(
        not(feature = "metrics"),
        expect(clippy::unused_self, reason = "the counters are behind `metrics`")
    )]
    fn count_cached(&self, directory: bool, pages: &[Option<Block>]) {
        #[cfg(feature = "metrics")]
        if self.charge.is_counted() {
            if directory {
                record_block_load_cached(self.metrics, BlockType::ColumnPageDirectory);
            }
            for _ in pages.iter().flatten() {
                record_block_load_cached(self.metrics, BlockType::ColumnPage);
            }
        }
        #[cfg(not(feature = "metrics"))]
        let _ = (directory, pages);
    }

    /// Opens the table's file, leaving the descriptor cache as it found it
    /// for a read that must leave no trace, as `load_block` does.
    fn open(&self) -> crate::Result<Arc<dyn FsFile>> {
        let (fd, cache_event) = if self.charge.touches_cache() {
            self.file_accessor
                .get_or_open_table(&self.table_id, self.path)?
        } else {
            (
                self.file_accessor
                    .peek_or_open_table(&self.table_id, self.path)?,
                None,
            )
        };
        #[cfg(feature = "metrics")]
        if let Some(hit) = cache_event
            && self.charge.is_counted()
        {
            use core::sync::atomic::Ordering::Relaxed;
            if hit {
                self.metrics.table_file_opened_cached.fetch_add(1, Relaxed);
            } else {
                self.metrics
                    .table_file_opened_uncached
                    .fetch_add(1, Relaxed);
            }
        }
        #[cfg(not(feature = "metrics"))]
        let _ = cache_event;
        Ok(fd)
    }

    /// Verifies one block's on-disk `bytes` and admits it. `with_dict` says
    /// whether the table's zstd dictionary applies: pages are compressed
    /// under it, the directory is never compressed.
    fn verify_and_admit(
        &self,
        bytes: &[u8],
        handle: &BlockHandle,
        block_type: BlockType,
        compression: CompressionType,
        with_dict: bool,
    ) -> crate::Result<Block> {
        #[cfg(zstd_any)]
        let zstd_dict = if with_dict { self.zstd_dict } else { None };
        #[cfg(not(zstd_any))]
        let _ = with_dict;
        let transform = build_block_transform(
            compression,
            self.encryption,
            self.ecc,
            #[cfg(zstd_any)]
            zstd_dict,
        )?;
        let identity = crate::table::block::BlockIdentity {
            table_id: self.table_id.table_id(),
            block_type,
            dict_id: compression.dict_id(),
            window_log: 0,
        };
        // An owned copy per block, so each cached block holds exactly its own
        // bytes rather than a view that pins the read it came in with.
        let (header, payload, ecc_status, recovery) =
            Block::verify_frame(bytes.to_vec(), *handle, identity, &transform)?;
        let mut produced = 0;
        let data = Block::decompress_payload(&header, payload, &transform, &mut produced);
        #[cfg(feature = "metrics")]
        record_block_decoded(self.metrics, self.charge, produced);
        let data = data?;
        admit_read_block(
            self.table_id,
            self.path,
            self.file_accessor,
            self.cache,
            handle,
            block_type,
            compression,
            self.encryption,
            self.ecc,
            #[cfg(zstd_any)]
            zstd_dict,
            self.heal_hints,
            #[cfg(feature = "metrics")]
            self.metrics,
            self.charge,
            Block { header, data },
            ecc_status,
            recovery,
        )
    }
}

impl RowGroupBlocks {
    /// Decodes the pages `want` selects into one batch per row page, in row
    /// order, each page a view of its own block.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a zero-row group, a row page range
    /// outside the group, a page naming an encoding part this build does not
    /// decode, a wanted page the read did not fetch, a page whose stamp names
    /// another group, part or row page, or a page whose own column id
    /// disagrees with the one the directory filed it under. They fail the
    /// group rather than the page: a reader that skipped what it could not
    /// interpret, or trusted a page to be what the directory said without
    /// checking, would return a batch that is quietly missing or substituting
    /// a column.
    ///
    /// Adds to `copied` what decoding the pages copied out of them, as each
    /// copy is made.
    pub(crate) fn to_row_pages(
        &self,
        want: &PageWant<'_>,
        copied: &mut usize,
    ) -> crate::Result<RowPages> {
        use crate::table::columnar::{Column, ColumnBatch};

        if self.directory.row_count() == 0 {
            return Err(crate::Error::InvalidHeader("columnar: zero-row data block"));
        }
        let outside = || crate::Error::InvalidHeader("columnar: row page range outside the group");
        // The directory holds at most `u16::MAX` row pages.
        let all = u16::try_from(self.directory.row_pages().len()).map_err(|_| outside())?;
        let range = match &want.row_pages {
            None => 0..all,
            Some(r) if r.start < r.end && r.end <= all => r.clone(),
            Some(_) => return Err(outside()),
        };
        let first_page = range.start;
        let first_row = self
            .directory
            .row_page_start(first_page)
            .ok_or_else(outside)?;
        // One column list per row page read, filled in one pass over the
        // directory: its entries are column-major, so each row page's columns
        // arrive in write order.
        let mut columns: Vec<Vec<Column>> = range.clone().map(|_| Vec::new()).collect();
        for (entry, page) in self.directory.entries().iter().zip(&self.pages) {
            // Every column's encoding names a single part today. A part this
            // build does not decode is refused rather than skipped: skipping
            // it would hand back a column assembled from some of its parts.
            if entry.id.part != 0 {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page holds an encoding part this build does not decode",
                ));
            }
            if !want.wants(entry) {
                continue;
            }
            let Some(page) = page else {
                return Err(crate::Error::InvalidHeader(
                    "columnar: a wanted page was not read",
                ));
            };
            let rows =
                self.directory
                    .row_page_rows(entry.row_page)
                    .ok_or(crate::Error::InvalidHeader(
                        "columnar: page names a row page the group does not have",
                    ))?;
            let column =
                Column::decode_page(&page.data, rows, self.directory.stamp_for(entry), copied)?;
            if column.column_id != entry.id.column_id {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page column disagrees with its directory entry",
                ));
            }
            let slot = entry
                .row_page
                .checked_sub(range.start)
                .and_then(|i| columns.get_mut(usize::from(i)))
                .ok_or(crate::Error::InvalidHeader(
                    "columnar: page names a row page the group does not have",
                ))?;
            slot.push(column);
        }
        let batches = columns
            .into_iter()
            .zip(range)
            .map(|(columns, index)| {
                let row_count = self.directory.row_page_rows(index).ok_or_else(outside)?;
                Ok(ColumnBatch { row_count, columns })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(RowPages {
            first_page,
            first_row,
            batches,
        })
    }
}

/// Proves the directory describes a layout that fills the group exactly.
///
/// The index entry and the directory are two independent statements of where
/// the group ends. A writer makes them agree by construction, so a
/// disagreement is corruption, and it is refused before any page offset
/// derived from the directory is used to slice the group.
pub fn check_group_extent(
    group: &BlockHandle,
    directory_len: u32,
    directory: &PageDirectory,
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
