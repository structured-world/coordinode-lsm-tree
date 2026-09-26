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
use crate::table::column_page::{PageDirectory, PageEntry, PageStamp, PageZones};
use crate::{
    Cache, CompressionType, Slice, encryption::EncryptionProvider, file_accessor::FileAccessor,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;

/// A columnar row group as read: its directory and the pages that were
/// fetched, each verified and cached as the block it is.
pub struct RowGroupBlocks {
    /// The group's directory, shared with the cache that keeps it decoded.
    pub directory: Arc<PageDirectory>,
    /// One slot per directory entry, in the directory's order: the page's
    /// block when it was fetched, `None` when the read did not want it.
    pub pages: Vec<Option<Block>>,
    /// The zones of the zone block the read needed to select its row pages,
    /// when it needed one: those of the column it pruned on.
    pub zones: Option<PageZones>,
}

/// Which row pages of a group a read wants.
#[derive(Clone, Debug, Default)]
pub enum RowPageSelect<'a> {
    /// Every row page.
    #[default]
    All,
    /// The row pages of this half-open range of ordinals.
    Range(core::ops::Range<u16>),
    /// The row pages whose statistics zone for `column_id` may hold a
    /// non-null value within the inclusive `[lower, upper]`, either side
    /// unbounded when `None`. A row page the column has no zone for is kept:
    /// only a zone can prove a page holds no match.
    Zone {
        /// The column the zones are read for.
        column_id: u16,
        /// Inclusive lower bound, or `None` for none.
        lower: Option<&'a [u8]>,
        /// Inclusive upper bound, or `None` for none.
        upper: Option<&'a [u8]>,
    },
}

/// The pages a read of a row group wants: the columns it projects, times the
/// row pages that hold the rows it asks for.
#[derive(Clone, Debug, Default)]
pub struct PageWant<'a> {
    /// The column ids read, or every column when `None`.
    pub columns: Option<&'a [u16]>,
    /// The row pages read.
    pub row_pages: RowPageSelect<'a>,
    /// Whether the read expects to want every page: a group that fits the
    /// I/O buffer is then taken in one request instead of its directory
    /// first, and the pages it does not want are neither verified nor cached.
    pub whole: bool,
}

impl<'a> PageWant<'a> {
    /// Every page of the group.
    pub const ALL: Self = Self {
        columns: None,
        row_pages: RowPageSelect::All,
        whole: true,
    };

    /// The pages of `columns` on the row pages `row_pages` selects, read
    /// directory first.
    #[must_use]
    pub const fn projected(columns: &'a [u16], row_pages: RowPageSelect<'a>) -> Self {
        Self {
            columns: Some(columns),
            row_pages,
            whole: false,
        }
    }

    /// The column this want selects row pages by, when it selects by zones.
    fn zone_column(&self) -> Option<u16> {
        match self.row_pages {
            RowPageSelect::Zone { column_id, .. } => Some(column_id),
            RowPageSelect::All | RowPageSelect::Range(_) => None,
        }
    }

    /// The column whose zone block resolving this want against `directory`
    /// needs: it selects by a column whose zones the directory does not keep,
    /// and the group has a zone block for it.
    fn zone_block_column(&self, directory: &PageDirectory) -> Option<u16> {
        self.zone_column().filter(|&column_id| {
            !directory.head_zones().describes(column_id)
                && directory.zone_block(column_id).is_some()
        })
    }

    /// This want resolved against `directory` and, when it was read, the
    /// group's zone block: the row pages it selects, one flag per row page.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for an empty range or one past the
    /// group's row pages: a caller names a range from what it read of this
    /// group, so one outside it is a reader error, not an empty selection.
    fn resolve(
        &self,
        directory: &PageDirectory,
        block_zones: Option<&PageZones>,
    ) -> crate::Result<Wanted<'a>> {
        let count = directory.row_pages().len();
        let row_pages = match &self.row_pages {
            RowPageSelect::All => alloc::vec![true; count],
            RowPageSelect::Range(range) => {
                if range.start >= range.end || usize::from(range.end) > count {
                    return Err(crate::Error::InvalidHeader(
                        "columnar: row page range outside the group",
                    ));
                }
                // A directory holds at most `u16::MAX` row pages, so the
                // ordinals below cover every one.
                (0u16..).take(count).map(|i| range.contains(&i)).collect()
            }
            RowPageSelect::Zone {
                column_id,
                lower,
                upper,
            } => (0u16..)
                .zip(directory.row_pages())
                .map(|(ordinal, &rows)| {
                    directory
                        .head_zones()
                        .zone(ordinal, *column_id)
                        .or_else(|| block_zones.and_then(|z| z.zone(ordinal, *column_id)))
                        .is_none_or(|zone| zone_may_match(zone, rows, *lower, *upper))
                })
                .collect(),
        };
        Ok(Wanted {
            columns: self.columns,
            row_pages,
        })
    }
}

/// Whether a row page whose column has `zone` over `rows` rows may hold a
/// non-null value within `[lower, upper]`.
fn zone_may_match(
    zone: crate::table::column_page::Zone<'_>,
    rows: u32,
    lower: Option<&[u8]>,
    upper: Option<&[u8]>,
) -> bool {
    if zone.null_count == rows {
        // Every row is null, and a null is within no range.
        return false;
    }
    if let (Some(lower), Some(max)) = (lower, zone.max)
        && max < lower
    {
        return false;
    }
    if let Some(upper) = upper
        && zone.min > upper
    {
        return false;
    }
    true
}

/// A [`PageWant`] resolved against a group's directory.
struct Wanted<'a> {
    columns: Option<&'a [u16]>,
    /// One flag per row page.
    row_pages: Vec<bool>,
}

impl Wanted<'_> {
    /// Whether the read needs the page `entry` names.
    fn wants(&self, entry: &PageEntry) -> bool {
        self.columns.is_none_or(|w| w.contains(&entry.id.column_id))
            && self
                .row_pages
                .get(usize::from(entry.row_page))
                .copied()
                .unwrap_or(false)
    }
}

/// A row group decoded row page by row page: one batch per row page read, in
/// row order, each holding the columns the read projected.
#[derive(Debug)]
pub struct RowPages {
    /// Rows in the whole group, the row pages not read included.
    pub group_rows: u32,
    /// Each row page's ordinal, one per batch.
    pub ordinals: Vec<u16>,
    /// The group row each row page starts at, one per batch.
    pub starts: Vec<u32>,
    /// One batch per row page read, in row order.
    pub batches: Vec<crate::table::columnar::ColumnBatch>,
    /// Whether the read wanted every page of the group: a scan that sees it
    /// can expect the next group to be wanted whole too.
    pub every_page: bool,
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
    /// How much one request may ask for and how many go in flight at once.
    pub budget: crate::config::ReadBudget,
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
    /// read, each run of consecutive ones as requests of up to the budget's
    /// I/O buffer: a full read after a point read has fetched the key page
    /// reads the rest of the group, not the key page again. When it is not, a
    /// read that expects every page ([`PageWant::whole`]) of a group that fits
    /// the I/O buffer takes it in ONE request, as a full scan wants, and any
    /// other read takes exactly the directory first, whose length the index
    /// entry records, and then the wanted pages the same way. Requests go out
    /// the budget's in-flight count at a time.
    ///
    /// Each request is charged when it is issued: the whole group and page
    /// runs to the data role, the directory to the index role. Each block's
    /// load and decode are then counted under its own role as it is verified.
    ///
    /// # Errors
    ///
    /// Any block's verification error, or [`crate::Error::InvalidHeader`] when
    /// the index entry names no row group, when the directory's length is not
    /// the one the index entry records, or when the directory describes a
    /// layout that does not fill the group exactly: a gap or an overrun means
    /// the index entry and the directory disagree about where the group ends,
    /// and neither can be trusted to name the right bytes.
    pub(crate) fn load(&self, want: &PageWant<'_>) -> crate::Result<RowGroupBlocks> {
        if let Some((directory, directory_len)) = self.cache.get_directory(
            self.table_id,
            self.group.offset(),
            self.charge.touches_cache(),
        ) {
            // Decoded and checked when it was cached; its length, extent and
            // tag are checked again against this read's index entry, which is
            // what names the group here.
            check_directory_len(indexed_directory_len(self.group)?, directory_len)?;
            check_group_extent(self.group, directory_len, &directory)?;
            check_head_zones_len(self.group, &directory)?;
            self.check_group_tag(&directory)?;
            let mut fd = None;
            // A directory cached by a read that did not select by its head
            // zone block's column lacks those zones: the first read that does
            // reads the block and caches the directory with them.
            let directory = if want
                .zone_column()
                .is_some_and(|column_id| directory.lacks_head_zones_for(column_id))
            {
                let directory =
                    self.with_head_zones(&mut fd, &directory, directory_len, &Slice::empty())?;
                if self.charge.touches_cache() {
                    self.cache.insert_directory(
                        self.table_id,
                        self.group.offset(),
                        Arc::clone(&directory),
                        directory_len,
                    );
                }
                directory
            } else {
                directory
            };
            let zones = match want.zone_block_column(&directory) {
                Some(column_id) => Some(self.zone_block(
                    &mut fd,
                    &directory,
                    directory_len,
                    column_id,
                    &Slice::empty(),
                    true,
                )?),
                None => None,
            };
            let wanted = want.resolve(&directory, zones.as_ref())?;
            let pages = self.cached_pages(&directory, directory_len, &wanted)?;
            self.count_cached(true, &pages);
            let complete = pages
                .iter()
                .zip(directory.entries())
                .all(|(page, entry)| page.is_some() || !wanted.wants(entry));
            if complete {
                return Ok(RowGroupBlocks {
                    directory,
                    pages,
                    zones,
                });
            }
            let fd = match fd {
                Some(fd) => fd,
                None => self.open()?,
            };
            let pages = self.fetch_missing(
                fd.as_ref(),
                &directory,
                directory_len,
                &Slice::empty(),
                pages,
                &wanted,
            )?;
            return Ok(RowGroupBlocks {
                directory,
                pages,
                zones,
            });
        }

        let fd = self.open()?;
        self.read_selective(fd, want)
    }

    /// `directory` with its head zone block's zones kept on it, the block
    /// read, verified and admitted like any zone block. `front` holds the
    /// group's first bytes when an earlier read brought them in.
    fn with_head_zones(
        &self,
        fd: &mut Option<Arc<dyn FsFile>>,
        directory: &Arc<PageDirectory>,
        directory_len: u32,
        front: &Slice,
    ) -> crate::Result<Arc<PageDirectory>> {
        let Some(head) = directory.head_zone_block() else {
            return Ok(Arc::clone(directory));
        };
        // Kept decoded on the directory, so the block itself is not cached.
        let zones = self.zone_block(fd, directory, directory_len, head.column_id, front, false)?;
        Ok(Arc::new(
            PageDirectory::clone(directory).with_head_zones(zones)?,
        ))
    }

    /// The group's zone block, from the cache or read, verified and admitted
    /// like a page, decoded and checked against `directory`, and cached when
    /// `cache` says so. `front` holds the group's first bytes when an earlier
    /// read brought them in; `fd` is opened here when the block has to be
    /// read and it is not yet.
    fn zone_block(
        &self,
        fd: &mut Option<Arc<dyn FsFile>>,
        directory: &PageDirectory,
        directory_len: u32,
        column_id: u16,
        front: &Slice,
        cache: bool,
    ) -> crate::Result<PageZones> {
        let (after_directory, length) =
            directory
                .zone_block(column_id)
                .ok_or(crate::Error::InvalidHeader(
                    "columnar: the column has no zone block",
                ))?;
        // The extent check proved the directory, its zone blocks and its
        // pages fill the group, and the group's length is a `u32`.
        let start = directory_len as usize + after_directory as usize;
        let end = start + length as usize;
        let handle = BlockHandle::new(BlockOffset(*self.group.offset() + start as u64), length);
        if let Some(block) = self.lookup(handle.offset(), BlockType::ColumnZones)? {
            #[cfg(feature = "metrics")]
            if self.charge.is_counted() {
                record_block_load_cached(self.metrics, BlockType::ColumnZones);
            }
            return directory.decode_zone_block(column_id, &block.data);
        }
        let bytes = if end <= front.len() {
            front.slice(start..end)
        } else {
            self.check_block_len(length)?;
            let fd = match fd {
                Some(fd) => fd,
                None => fd.insert(self.open()?),
            };
            self.read(fd.as_ref(), start, end - start, BlockType::ColumnZones)?
        };
        let block = self.verify_and_admit(
            &bytes,
            &handle,
            BlockType::ColumnZones,
            CompressionType::None,
            false,
        )?;
        let zones = directory.decode_zone_block(column_id, &block.data)?;
        if cache {
            self.cache_block(&handle, block);
        }
        Ok(zones)
    }

    /// [`check_group_tag`] against this read's index entry. Checked before the
    /// directory's zones are used: they can prune every page, and then no
    /// page stamp is ever checked against the directory.
    fn check_group_tag(&self, directory: &PageDirectory) -> crate::Result<()> {
        check_group_tag(self.group.group_tag(), directory)
    }

    /// Refuses a block length the directory declares but no block of this
    /// table can have, before a read allocates a buffer for it.
    fn check_block_len(&self, length: u32) -> crate::Result<()> {
        crate::table::block::check_on_disk_size(u64::from(length), self.encryption, self.ecc)
    }

    /// Caches a verified page or zone block under its own offset, unless this
    /// read must leave the cache as it found it.
    fn cache_block(&self, handle: &BlockHandle, block: Block) {
        if self.charge.touches_cache() {
            self.cache
                .insert_block(self.table_id, handle.offset(), block);
        }
    }

    /// The directory, then the wanted pages. A read that expects every page
    /// and fits the I/O buffer takes the whole group in one request, so the
    /// pages are served from it; any other read takes exactly the directory,
    /// with the head zone block after it when it selects by the key column,
    /// whose zones a writer puts there.
    fn read_selective(
        &self,
        fd: Arc<dyn FsFile>,
        want: &PageWant<'_>,
    ) -> crate::Result<RowGroupBlocks> {
        let group_len = self.group.size() as usize;
        let io_buffer = self.budget.io_buffer() as usize;
        let directory_len = indexed_directory_len(self.group)?;
        self.check_block_len(directory_len)?;
        let head_len = if want.zone_column() == Some(crate::table::columnar::COL_USER_KEY) {
            self.group
                .row_group()
                .map_or(0, |group| group.head_zones_len)
        } else {
            0
        };
        if head_len > 0 {
            self.check_block_len(head_len)?;
        }
        // Both are u32s, and the index entry refused a sum past the group.
        let head_end = directory_len as usize + head_len as usize;
        if head_end > group_len {
            return Err(crate::Error::InvalidHeader(
                "columnar: page directory overruns its row group",
            ));
        }
        let prefix = if want.whole && group_len <= io_buffer {
            self.read(fd.as_ref(), 0, group_len, BlockType::ColumnPage)?
        } else {
            self.read(fd.as_ref(), 0, head_end, BlockType::ColumnPageDirectory)?
        };
        let directory = self.admit_directory(&prefix, directory_len)?;
        let mut fd = Some(fd);
        let directory = if want
            .zone_column()
            .is_some_and(|column_id| directory.lacks_head_zones_for(column_id))
        {
            self.with_head_zones(&mut fd, &directory, directory_len, &prefix)?
        } else {
            directory
        };
        let zones = match want.zone_block_column(&directory) {
            Some(column_id) => Some(self.zone_block(
                &mut fd,
                &directory,
                directory_len,
                column_id,
                &prefix,
                true,
            )?),
            None => None,
        };
        let fd = match fd {
            Some(fd) => fd,
            None => self.open()?,
        };
        let wanted = want.resolve(&directory, zones.as_ref())?;
        let pages = if prefix.len() == group_len {
            // Every page is in hand already: a cache lookup per page would
            // cost more than verifying it from the bytes just read.
            alloc::vec![None; directory.entries().len()]
        } else {
            let pages = self.cached_pages(&directory, directory_len, &wanted)?;
            self.count_cached(false, &pages);
            pages
        };
        let pages = self.fetch_missing(
            fd.as_ref(),
            &directory,
            directory_len,
            &prefix,
            pages,
            &wanted,
        )?;
        // Cached only now that the blocks it named carried the stamps it
        // implies: another group's directory read in this one's place fails
        // the read without staying behind to refuse every healthy page later.
        if self.charge.touches_cache() {
            self.cache.insert_directory(
                self.table_id,
                self.group.offset(),
                Arc::clone(&directory),
                directory_len,
            );
        }
        Ok(RowGroupBlocks {
            directory,
            pages,
            zones,
        })
    }

    /// Verifies and admits the directory of `directory_len` bytes, the length
    /// the index entry records, at the front of `front`, decodes it and proves
    /// it fills the group. The caller caches it once the blocks it names have
    /// checked out.
    fn admit_directory(
        &self,
        front: &Slice,
        directory_len: u32,
    ) -> crate::Result<Arc<PageDirectory>> {
        check_directory_len(
            directory_len,
            Header::decode_from(&mut &front[..])?.on_disk_size_with(self.ecc),
        )?;
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
        let directory = Arc::new(PageDirectory::decode(&block.data)?);
        check_group_extent(self.group, directory_len, &directory)?;
        check_head_zones_len(self.group, &directory)?;
        self.check_group_tag(&directory)?;
        Ok(directory)
    }

    /// Fetches every page `want` selects that `pages` does not hold yet.
    ///
    /// Pages are contiguous in directory order (the extent check proved it),
    /// so a run of consecutive missing pages is one byte range, read in
    /// requests of up to the budget's I/O buffer: a run within it is one
    /// request, a longer one is cut between pages, and a page larger than it
    /// is a request of its own. Requests go out the budget's in-flight count
    /// at a time. `front` holds the group's first bytes when an earlier read
    /// brought them in, and a range it covers is served from it rather than
    /// asked for again.
    fn fetch_missing(
        &self,
        fd: &dyn FsFile,
        directory: &PageDirectory,
        directory_len: u32,
        front: &Slice,
        mut pages: Vec<Option<Block>>,
        want: &Wanted<'_>,
    ) -> crate::Result<Vec<Option<Block>>> {
        let entries = directory.entries();
        let io_buffer = self.budget.io_buffer() as usize;
        let requests = {
            let missing = |i: usize| {
                pages.get(i).is_some_and(Option::is_none)
                    && entries.get(i).is_some_and(|e| want.wants(e))
            };
            let mut requests = Vec::new();
            let mut i = 0;
            while i < entries.len() {
                if !missing(i) {
                    i += 1;
                    continue;
                }
                let first = i;
                let mut bytes = 0usize;
                while i < entries.len() && missing(i) {
                    let length = entries.get(i).map_or(0, |e| e.length);
                    // A page longer than any block can be is refused before a
                    // request allocates a buffer for it.
                    self.check_block_len(length)?;
                    let length = length as usize;
                    if i > first && bytes + length > io_buffer {
                        break;
                    }
                    bytes += length;
                    i += 1;
                }
                requests.push(first..i);
            }
            requests
        };
        // Every offset below is within the group: the extent check proved
        // that the directory, its zone blocks and its pages fill it exactly,
        // and the group's length is a `u32`.
        let pages_at = directory_len as usize + directory.pages_start() as usize;
        let page_at = |entry: &PageEntry| pages_at + entry.offset as usize;
        let span_of = |run: &core::ops::Range<usize>| {
            let first = entries.get(run.start)?;
            let last = entries.get(run.end.checked_sub(1)?)?;
            Some((page_at(first), page_at(last) + last.length as usize))
        };
        for batch in requests.chunks(usize::from(self.budget.in_flight())) {
            let spans = batch
                .iter()
                .map(|run| {
                    span_of(run).ok_or(crate::Error::InvalidHeader(
                        "columnar: page outside its row group",
                    ))
                })
                .collect::<crate::Result<Vec<_>>>()?;
            // The requests the bytes already in hand do not reach at all go
            // out together; one they reach, in whole or in part, is served
            // from them.
            let beyond: Vec<(usize, usize)> = spans
                .iter()
                .filter(|&&(start, _)| start >= front.len())
                .map(|&(start, end)| (start, end - start))
                .collect();
            let mut read = self.read_many(fd, &beyond)?.into_iter();
            for (run, &(start, end)) in batch.iter().zip(&spans) {
                let span = if start >= front.len() {
                    read.next().ok_or(crate::Error::InvalidHeader(
                        "columnar: a request came back missing",
                    ))?
                } else {
                    self.span(fd, front, start, end)?
                };
                self.admit_pages(
                    directory,
                    directory_len,
                    run.clone(),
                    start,
                    &span,
                    &mut pages,
                )?;
            }
        }
        Ok(pages)
    }

    /// Verifies, admits and caches the pages `run` names out of `span`, the
    /// group's bytes from `start`, into their slots of `pages`.
    ///
    /// A page is cached only once its stamp is the one `directory` implies
    /// for its slot: a page read in another page's place (a misdirected or
    /// transient read) fails the read without staying behind in the cache to
    /// fail every later read of the group.
    fn admit_pages(
        &self,
        directory: &PageDirectory,
        directory_len: u32,
        run: core::ops::Range<usize>,
        start: usize,
        span: &Slice,
        pages: &mut [Option<Block>],
    ) -> crate::Result<()> {
        let entries = directory.entries();
        let pages_at = directory_len as usize + directory.pages_start() as usize;
        let page_at = |entry: &PageEntry| pages_at + entry.offset as usize;
        for index in run {
            let Some(entry) = entries.get(index) else {
                continue;
            };
            let at = page_at(entry) - start;
            let Some(bytes) = span.get(at..at + entry.length as usize) else {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page outside its row group",
                ));
            };
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
            let stamped = page
                .data
                .first_chunk::<{ PageStamp::LEN }>()
                .is_some_and(|stamp| PageStamp::decode(*stamp) == directory.stamp_for(entry));
            if !stamped {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page belongs to another row group, column part or row page",
                ));
            }
            self.cache_block(&handle, page.clone());
            if let Some(slot) = pages.get_mut(index) {
                *slot = Some(page);
            }
        }
        Ok(())
    }

    /// Reads the `(at, len)` ranges of the group in one batched request, each
    /// charged to the data role as the request is issued.
    fn read_many(&self, fd: &dyn FsFile, ranges: &[(usize, usize)]) -> crate::Result<Vec<Slice>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }
        #[cfg(feature = "metrics")]
        if self.charge.is_counted() {
            for &(_, len) in ranges {
                record_block_read(self.metrics, BlockType::ColumnPage, len as u64);
            }
        }
        let regions: Vec<(u64, usize)> = ranges
            .iter()
            .map(|&(at, len)| (*self.group.offset() + at as u64, len))
            .collect();
        Ok(crate::file::read_exact_many(fd, &regions)?)
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
        want: &Wanted<'_>,
    ) -> crate::Result<Vec<Option<Block>>> {
        let mut pages = Vec::with_capacity(directory.entries().len());
        let pages_at =
            *self.group.offset() + u64::from(directory_len) + u64::from(directory.pages_start());
        for entry in directory.entries() {
            let page = if want.wants(entry) {
                let offset = pages_at + u64::from(entry.offset);
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
        let wanted = want.resolve(&self.directory, self.zones.as_ref())?;
        // The selected row pages, in row order, and each one's place among
        // them. A directory holds at most `u16::MAX` row pages, so the
        // ordinals cover every one.
        let mut ordinals = Vec::new();
        let mut slot_of: Vec<Option<usize>> = Vec::with_capacity(wanted.row_pages.len());
        for (ordinal, &selected) in (0u16..).zip(&wanted.row_pages) {
            slot_of.push(selected.then_some(ordinals.len()));
            if selected {
                ordinals.push(ordinal);
            }
        }
        // One column list per row page read, filled in one pass over the
        // directory: its entries are column-major, so each row page's columns
        // arrive in write order.
        let mut columns: Vec<Vec<Column>> = ordinals.iter().map(|_| Vec::new()).collect();
        let outside = || crate::Error::InvalidHeader("columnar: row page outside the group");
        let unknown_row_page = || {
            crate::Error::InvalidHeader("columnar: page names a row page the group does not have")
        };
        let mut every_page = true;
        for (entry, page) in self.directory.entries().iter().zip(&self.pages) {
            // Every column's encoding names a single part today. A part this
            // build does not decode is refused rather than skipped: skipping
            // it would hand back a column assembled from some of its parts.
            if entry.id.part != 0 {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page holds an encoding part this build does not decode",
                ));
            }
            if !wanted.wants(entry) {
                every_page = false;
                continue;
            }
            let Some(page) = page else {
                return Err(crate::Error::InvalidHeader(
                    "columnar: a wanted page was not read",
                ));
            };
            let rows = self
                .directory
                .row_page_rows(entry.row_page)
                .ok_or_else(unknown_row_page)?;
            let column =
                Column::decode_page(&page.data, rows, self.directory.stamp_for(entry), copied)?;
            if column.column_id != entry.id.column_id {
                return Err(crate::Error::InvalidHeader(
                    "columnar: page column disagrees with its directory entry",
                ));
            }
            let slot = slot_of
                .get(usize::from(entry.row_page))
                .copied()
                .flatten()
                .and_then(|i| columns.get_mut(i))
                .ok_or_else(unknown_row_page)?;
            slot.push(column);
        }
        let mut starts = Vec::with_capacity(ordinals.len());
        let mut batches = Vec::with_capacity(ordinals.len());
        for (columns, &ordinal) in columns.into_iter().zip(&ordinals) {
            starts.push(self.directory.row_page_start(ordinal).ok_or_else(outside)?);
            let row_count = self.directory.row_page_rows(ordinal).ok_or_else(outside)?;
            batches.push(ColumnBatch { row_count, columns });
        }
        Ok(RowPages {
            group_rows: self.directory.row_count(),
            ordinals,
            starts,
            batches,
            every_page,
        })
    }
}

/// Refuses a directory whose tag is not `expected`, the one the group's index
/// entry names it by, and a group whose entry names none.
///
/// A directory verifies as a block wherever it is read, so another group's,
/// read in this one's place, would otherwise be taken for this group's: every
/// read of a group's blocks (an indexed read, a scan, verification, salvage)
/// checks it before trusting anything the directory says.
pub fn check_group_tag(
    expected: Option<core::num::NonZeroU64>,
    directory: &PageDirectory,
) -> crate::Result<()> {
    match expected {
        Some(tag) if tag.get() == directory.group_tag() => Ok(()),
        Some(_) => Err(crate::Error::InvalidHeader(
            "columnar: page directory belongs to another row group",
        )),
        None => Err(crate::Error::InvalidHeader(
            "columnar: the index entry names no row group",
        )),
    }
}

/// The on-disk length of `group`'s directory, as its index entry records it.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] when the entry names no row group, which a
/// columnar table's entries always do.
pub fn indexed_directory_len(group: &BlockHandle) -> crate::Result<u32> {
    group
        .row_group()
        .map(|group| group.directory_len.get())
        .ok_or(crate::Error::InvalidHeader(
            "columnar: the index entry names no row group",
        ))
}

/// Refuses a directory whose on-disk length, `found`, is not the `indexed`
/// one its group's index entry records.
///
/// A reader reads exactly the indexed length, so a directory of another
/// length in the group's place would either be cut short or run into the
/// first page; the two are independent statements a writer makes agree.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] when they differ.
pub fn check_directory_len(indexed: u32, found: u32) -> crate::Result<()> {
    if indexed == found {
        Ok(())
    } else {
        Err(crate::Error::InvalidHeader(
            "columnar: page directory length disagrees with its index entry",
        ))
    }
}

/// Refuses a directory whose head zone block is not the length `group`'s index
/// entry records: a read that selects by the key column takes that many bytes
/// after the directory as the block, so a disagreement would cut it short or
/// run it into the first page.
///
/// # Errors
///
/// [`crate::Error::InvalidHeader`] when they differ, or when the entry names
/// no row group.
pub fn check_head_zones_len(group: &BlockHandle, directory: &PageDirectory) -> crate::Result<()> {
    let indexed = group
        .row_group()
        .ok_or(crate::Error::InvalidHeader(
            "columnar: the index entry names no row group",
        ))?
        .head_zones_len;
    if indexed == directory.pages_start() {
        Ok(())
    } else {
        Err(crate::Error::InvalidHeader(
            "columnar: head zone block length disagrees with its index entry",
        ))
    }
}

/// Proves the directory describes a layout that fills the group exactly: the
/// directory, its pages back to back (which a [`PageDirectory`] holds by
/// construction), then its zone blocks.
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
    if directory.group_len(directory_len) == Some(group.size()) {
        Ok(())
    } else {
        Err(crate::Error::InvalidHeader(
            "columnar: page directory does not fill its row group",
        ))
    }
}
