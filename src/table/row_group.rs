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

/// Whether a read with projection `wanted` needs the page `entry` names.
fn is_wanted(wanted: Option<&[u16]>, entry: &PageEntry) -> bool {
    wanted.is_none_or(|w| w.contains(&entry.id.column_id))
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
    /// Loads the group's directory and the pages `wanted` selects by column
    /// id, or every page when `None`.
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
    pub(crate) fn load(&self, wanted: Option<&[u16]>) -> crate::Result<RowGroupBlocks> {
        if let Some(directory_block) =
            self.lookup(self.group.offset(), BlockType::ColumnPageDirectory)?
        {
            let directory = PageDirectory::decode(&directory_block.data)?;
            let directory_len = directory_block.header.on_disk_size_with(self.ecc);
            check_group_extent(self.group, directory_len, &directory)?;
            let pages = self.cached_pages(&directory, directory_len, wanted)?;
            self.count_cached(true, &pages);
            let complete = pages
                .iter()
                .zip(directory.entries())
                .all(|(page, entry)| page.is_some() || !is_wanted(wanted, entry));
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
                wanted,
            )?;
            return Ok(RowGroupBlocks { directory, pages });
        }

        let fd = self.open()?;
        match wanted {
            None => self.read_whole(fd.as_ref()),
            Some(_) => self.read_selective(fd.as_ref(), wanted),
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
            None,
        )?;
        Ok(RowGroupBlocks { directory, pages })
    }

    /// The directory from a prefix of the group, then the wanted pages.
    fn read_selective(
        &self,
        fd: &dyn FsFile,
        wanted: Option<&[u16]>,
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
        let pages = self.cached_pages(&directory, directory_len, wanted)?;
        self.count_cached(false, &pages);
        let pages = self.fetch_missing(fd, &directory, directory_len, &prefix, pages, wanted)?;
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

    /// Fetches every page `wanted` selects that `pages` does not hold yet.
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
        wanted: Option<&[u16]>,
    ) -> crate::Result<Vec<Option<Block>>> {
        let entries = directory.entries();
        let runs = {
            let missing = |i: usize| {
                pages.get(i).is_some_and(Option::is_none)
                    && entries.get(i).is_some_and(|e| is_wanted(wanted, e))
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

    /// The pages `wanted` selects that the cache already holds.
    fn cached_pages(
        &self,
        directory: &PageDirectory,
        directory_len: u32,
        wanted: Option<&[u16]>,
    ) -> crate::Result<Vec<Option<Block>>> {
        let mut pages = Vec::with_capacity(directory.entries().len());
        for entry in directory.entries() {
            let page = if is_wanted(wanted, entry) {
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
    /// Decodes the pages whose column `wanted` selects — every page when
    /// `None` — into a batch, in write order.
    ///
    /// # Errors
    ///
    /// [`crate::Error::InvalidHeader`] for a zero-row group, a page naming an
    /// encoding part this build does not decode, a wanted page the read did
    /// not fetch, a page whose stamp names another group or part, or a page
    /// whose own column id disagrees with the one the directory filed it
    /// under. They fail the group rather than the page: a reader that skipped
    /// what it could not interpret, or trusted a page to be what the directory
    /// said without checking, would return a batch that is quietly missing or
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
            if !is_wanted(wanted, entry) {
                continue;
            }
            let Some(page) = page else {
                return Err(crate::Error::InvalidHeader(
                    "columnar: a wanted page was not read",
                ));
            };
            let column = crate::table::columnar::Column::decode_page(
                &page.data,
                row_count,
                self.directory.stamp_for(entry),
                copied,
            )?;
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
