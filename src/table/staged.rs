// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! A read of one table's blocks for a batch of keys, advanced one dependent
//! stage at a time: the filter blocks the keys are checked against, then the
//! index blocks that locate the keys that passed, and last the data blocks
//! that hold them.
//!
//! Every stage's blocks are known from what the stage before it returned,
//! before any of them is read, so a caller holding many tables' reads can ask
//! for the blocks of one stage of all of them together and read them in one
//! batch. The read never reads a block itself and never waits: it names the
//! blocks it lacks and holds the ones it is given. What it holds is what it
//! answers from, so a block the cache evicts between two stages is not read
//! twice; the blocks it decodes go into the cache as well.

use alloc::vec::Vec;

use super::block_index::{BlockIndexImpl, iter::OwnedIndexBlockIter};
use super::filter::block::FilterBlock;
use super::probe_stats::PlanCounts;
use super::{Block, BlockHandle, BlockType, FilterSource, IndexBlock, KeyedBlockHandle, Table};
use crate::SeqNo;

/// How a table enters a staged read of a key batch.
pub enum StagedStart<'t> {
    /// The table holds nothing any key of the batch can read: the batch is
    /// empty, or the table lies above the snapshot.
    Nothing,
    /// The table is read serially: its blocks need the load path's own
    /// recovery (Page-ECC) or reconstruction (columnar).
    Serial,
    /// The table is read in stages.
    Staged(StagedRead<'t>),
}

/// Which blocks a stage reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Stage {
    /// The filter blocks the keys are checked against.
    Filter,
    /// The index blocks locating the keys that passed.
    Index,
    /// Planned: the data blocks and the keys in each are known.
    Done,
}

/// A staged read of one table for a key batch; see the module docs.
pub struct StagedRead<'t> {
    table: &'t Table,
    /// The snapshot in the table's local seqno space.
    table_seqno: SeqNo,
    stage: Stage,
    /// The positions (into the key batch) still being read.
    passing: Vec<usize>,
    /// The blocks this stage lacks, cold in the cache.
    need: Vec<BlockHandle>,
    /// The blocks held, by their offset in the table file.
    held: Vec<(u64, Block)>,
    /// What the filters answered, counted once the plan is.
    tally: PlanCounts,
    /// Once planned: each data block and the positions of the keys in it.
    blocks: Vec<(BlockHandle, Vec<usize>)>,
}

impl<'t> StagedRead<'t> {
    /// Opens the staged read of `table` for `sorted_keys` (each with its hash,
    /// sorted by the table's comparator) at the snapshot `seqno`.
    pub(crate) fn start(
        table: &'t Table,
        sorted_keys: &[(&[u8], u64)],
        seqno: SeqNo,
    ) -> StagedStart<'t> {
        if table.is_chunk_special() {
            return StagedStart::Serial;
        }
        if sorted_keys.is_empty() {
            return StagedStart::Nothing;
        }
        let Some(table_seqno) = seqno.checked_sub(table.global_seqno()) else {
            return StagedStart::Nothing;
        };
        if table.metadata.seqnos.0 >= table_seqno {
            return StagedStart::Nothing;
        }
        let mut read = Self {
            table,
            table_seqno,
            stage: Stage::Filter,
            passing: (0..sorted_keys.len()).collect(),
            need: Vec::new(),
            held: Vec::new(),
            tally: PlanCounts::default(),
            blocks: Vec::new(),
        };
        for (key, _) in sorted_keys {
            match table.filter_source(key) {
                FilterSource::Block(handle) => read.want(handle, BlockType::Filter),
                FilterSource::None | FilterSource::Pinned(_) | FilterSource::PastPartitions => {}
            }
        }
        StagedStart::Staged(read)
    }

    /// The blocks this stage lacks and their type; none once it has them all,
    /// when [`Self::advance`] moves the read on.
    pub(crate) fn need(&self) -> (BlockType, &[BlockHandle]) {
        let block_type = match self.stage {
            Stage::Filter => BlockType::Filter,
            Stage::Index | Stage::Done => BlockType::Index,
        };
        (block_type, &self.need)
    }

    /// Whether the read is planned: [`Self::into_plan`] has its answer.
    pub(crate) fn is_done(&self) -> bool {
        self.stage == Stage::Done
    }

    /// Holds `bytes`, the on-disk bytes of the needed block at `handle`, and
    /// puts the decoded block in the cache.
    ///
    /// # Errors
    ///
    /// Propagates a decode or corruption error; the read is then to be done
    /// serially.
    pub(crate) fn supply(&mut self, handle: BlockHandle, bytes: &[u8]) -> crate::Result<()> {
        let (block_type, _) = self.need();
        let offset = *handle.offset();
        let block = self
            .table
            .decode_block_from_bytes(bytes, offset, block_type)?;
        self.table
            .cache
            .insert_block(self.table.global_id(), handle.offset(), block.clone());
        self.need.retain(|h| h.offset() != handle.offset());
        self.held.push((offset, block));
        Ok(())
    }

    /// Moves the read on once this stage holds every block it needs: the
    /// filters answer, the index is walked, and the next stage's blocks
    /// become what the read lacks. A stage whose blocks are all at hand is
    /// passed at once.
    ///
    /// # Errors
    ///
    /// Propagates a filter or index decode error; the read is then to be done
    /// serially.
    pub(crate) fn advance(&mut self, sorted_keys: &[(&[u8], u64)]) -> crate::Result<()> {
        debug_assert!(self.need.is_empty(), "a stage advanced before its blocks");
        loop {
            match self.stage {
                Stage::Filter => {
                    self.check_filters(sorted_keys)?;
                    // The filters are answered: none of their blocks is read
                    // again, so none is kept beyond what the cache keeps.
                    self.held
                        .retain(|(_, block)| block.header.block_type != BlockType::Filter);
                    if self.passing.is_empty() {
                        self.stage = Stage::Done;
                        self.held.clear();
                        return Ok(());
                    }
                    self.stage = Stage::Index;
                    self.want_index(sorted_keys);
                }
                Stage::Index => {
                    if self.walk_index(sorted_keys)? {
                        // Planned: the index blocks are not walked again.
                        self.stage = Stage::Done;
                        self.held.clear();
                    }
                }
                Stage::Done => return Ok(()),
            }
            if !self.need.is_empty() {
                return Ok(());
            }
        }
    }

    /// The planned data blocks, each with the positions of the keys in it,
    /// and what the filters answered.
    pub(crate) fn into_plan(self) -> (SeqNo, Vec<(BlockHandle, Vec<usize>)>, PlanCounts) {
        (self.table_seqno, self.blocks, self.tally)
    }

    /// Records that the stage needs the block at `handle`, unless it is held
    /// or asked for already, or the cache has it, in which case it is held.
    fn want(&mut self, handle: BlockHandle, block_type: BlockType) {
        let offset = *handle.offset();
        if self.held.iter().any(|(at, _)| *at == offset)
            || self.need.iter().any(|h| *h.offset() == offset)
        {
            return;
        }
        match self.table.cached_block(&handle, block_type) {
            Some(block) => self.held.push((offset, block)),
            None => self.need.push(handle),
        }
    }

    /// The held block at `offset`.
    fn held(&self, offset: u64) -> Option<&Block> {
        self.held
            .iter()
            .find(|(at, _)| *at == offset)
            .map(|(_, block)| block)
    }

    /// Checks every key against its filter, keeping the ones that pass.
    fn check_filters(&mut self, sorted_keys: &[(&[u8], u64)]) -> crate::Result<()> {
        let mut passing = Vec::with_capacity(self.passing.len());
        for &pos in &self.passing {
            let Some(&(key, hash)) = sorted_keys.get(pos) else {
                continue;
            };
            let answer = match self.table.filter_source(key) {
                FilterSource::None => Table::answer_bloom(None, hash)?,
                FilterSource::Pinned(block) => Table::answer_bloom(Some(block), hash)?,
                FilterSource::Block(handle) => {
                    let block = self.held(*handle.offset()).ok_or(NOT_HELD)?;
                    Table::answer_bloom(Some(&FilterBlock::new(block.clone())), hash)?
                }
                FilterSource::PastPartitions => super::BloomResult::PastPartitions,
            };
            Table::tally_bloom(&mut self.tally, &answer);
            if !answer.should_skip() {
                passing.push(pos);
            }
        }
        self.passing = passing;
        Ok(())
    }

    /// Asks for the index blocks that locate the passing keys.
    fn want_index(&mut self, sorted_keys: &[(&[u8], u64)]) {
        match &*self.table.block_index {
            BlockIndexImpl::VolatileFull(index) => {
                self.want(index.handle, BlockType::Index);
            }
            BlockIndexImpl::TwoLevel(index) => {
                let comparator = self.table.comparator.clone();
                let keys: Vec<&[u8]> = self
                    .passing
                    .iter()
                    .filter_map(|&pos| sorted_keys.get(pos).map(|(key, _)| *key))
                    .collect();
                for key in keys {
                    let partition = OwnedIndexBlockIter::from_block_with_bounds(
                        index.top_level_index.clone(),
                        comparator.clone(),
                        Some((key, self.table_seqno)),
                        None,
                    )
                    .ok()
                    .flatten()
                    .and_then(|mut it| it.next());
                    if let Some(partition) = partition {
                        self.want(*partition.as_ref(), BlockType::Index);
                    }
                }
            }
            BlockIndexImpl::Full(_) | BlockIndexImpl::Closed => {}
        }
    }

    /// Walks the index over the passing keys, planning the data blocks: it is
    /// sought at each key that lies past the entry before it, so a sparse
    /// batch over a large table reads and decodes only the entries and
    /// partitions its keys fall in, and it steps to the next entry only where
    /// a key equal to an entry's end may continue into it. `Ok(false)` when
    /// the walk reached an index partition not yet held, which is then what
    /// the read lacks.
    fn walk_index(&mut self, sorted_keys: &[(&[u8], u64)]) -> crate::Result<bool> {
        // A partition the cache has is held at once and the walk starts over,
        // in this loop rather than by recursion, however many cached
        // partitions the keys cross.
        loop {
            match self.walk_index_once(sorted_keys)? {
                Walk::Planned => return Ok(true),
                Walk::Lacks => return Ok(false),
                Walk::Again => {}
            }
        }
    }

    /// One walk of the index for [`Self::walk_index`].
    fn walk_index_once(&mut self, sorted_keys: &[(&[u8], u64)]) -> crate::Result<Walk> {
        let Some(&first) = self.passing.first() else {
            return Ok(Walk::Planned);
        };
        let Some(&(first_key, _)) = sorted_keys.get(first) else {
            return Ok(Walk::Planned);
        };
        let seqno = self.table_seqno;
        let comparator = self.table.comparator.clone();
        let mut plan = DataPlan::new(&self.passing);
        let mut lacks: Option<BlockHandle> = None;

        match &*self.table.block_index {
            BlockIndexImpl::Full(index) => {
                let mut walk = index.forward_reader(first_key, seqno);
                while let Some(handle) = walk.as_mut().and_then(Iterator::next) {
                    let handle = handle?;
                    if !plan.feed(&handle, sorted_keys, self.table) {
                        break;
                    }
                    if let Some(key) = plan.seek_past(&handle, sorted_keys, self.table) {
                        walk = index.forward_reader(key, seqno);
                    }
                }
            }
            BlockIndexImpl::VolatileFull(index) => {
                let block =
                    IndexBlock::new(self.held(*index.handle.offset()).ok_or(NOT_HELD)?.clone());
                let open = |key: &[u8]| {
                    OwnedIndexBlockIter::from_block_with_bounds(
                        block.clone(),
                        comparator.clone(),
                        Some((key, seqno)),
                        None,
                    )
                };
                let mut walk = open(first_key)?;
                while let Some(handle) = walk.as_mut().and_then(Iterator::next) {
                    if !plan.feed(&handle, sorted_keys, self.table) {
                        break;
                    }
                    if let Some(key) = plan.seek_past(&handle, sorted_keys, self.table) {
                        walk = open(key)?;
                    }
                }
            }
            BlockIndexImpl::TwoLevel(index) => {
                let held = &self.held;
                let partition_of = |handle: &KeyedBlockHandle| {
                    let handle = *handle.as_ref();
                    held.iter()
                        .find(|(at, _)| *at == *handle.offset())
                        .map(|(_, block)| IndexBlock::new(block.clone()))
                        .ok_or(handle)
                };
                // The top-level entries from the partition being walked on,
                // and the entries of that partition.
                let mut partitions: Option<OwnedIndexBlockIter> = None;
                let mut entries: Option<OwnedIndexBlockIter> = None;
                let mut seek: Option<&[u8]> = Some(first_key);
                'walk: loop {
                    if let Some(key) = seek.take() {
                        let bound = Some((key, seqno));
                        partitions = OwnedIndexBlockIter::from_block_with_bounds(
                            index.top_level_index.clone(),
                            comparator.clone(),
                            bound,
                            None,
                        )?;
                        let Some(partition) = partitions.as_mut().and_then(Iterator::next) else {
                            break;
                        };
                        match partition_of(&partition) {
                            Ok(block) => {
                                entries = OwnedIndexBlockIter::from_block_with_bounds(
                                    block,
                                    comparator.clone(),
                                    bound,
                                    None,
                                )?;
                            }
                            Err(handle) => {
                                lacks = Some(handle);
                                break;
                            }
                        }
                    }
                    let handle = loop {
                        if let Some(handle) = entries.as_mut().and_then(Iterator::next) {
                            break handle;
                        }
                        // The partition is walked: its next one continues it.
                        let Some(partition) = partitions.as_mut().and_then(Iterator::next) else {
                            break 'walk;
                        };
                        match partition_of(&partition) {
                            Ok(block) => {
                                entries = OwnedIndexBlockIter::from_block_with_bounds(
                                    block,
                                    comparator.clone(),
                                    None,
                                    None,
                                )?;
                            }
                            Err(handle) => {
                                lacks = Some(handle);
                                break 'walk;
                            }
                        }
                    };
                    if !plan.feed(&handle, sorted_keys, self.table) {
                        break;
                    }
                    seek = plan.seek_past(&handle, sorted_keys, self.table);
                }
            }
            BlockIndexImpl::Closed => {}
        }

        if let Some(handle) = lacks {
            self.want(handle, BlockType::Index);
            // The cache had it and it is held now: walk again.
            return Ok(if self.need.is_empty() {
                Walk::Again
            } else {
                Walk::Lacks
            });
        }
        let blockless = plan.blockless();
        self.table
            .tally_blockless(&mut self.tally, self.table_seqno, blockless);
        self.blocks = plan.blocks;
        Ok(Walk::Planned)
    }
}

/// How one walk of the index ended.
enum Walk {
    /// The data blocks are planned.
    Planned,
    /// An index partition is to be read.
    Lacks,
    /// An index partition the cache held was taken: the walk starts over.
    Again,
}

/// A block the stage needs was not supplied before it advanced.
const NOT_HELD: crate::Error =
    crate::Error::InvalidHeader("staged read: a stage advanced without a block it needs");

/// The data blocks a walk of the index plans for the passing keys, with the
/// same boundary rule as the serial planner: a key equal to a block's end key
/// is listed in that block and the next, since a version of it may continue
/// across the boundary.
struct DataPlan<'p> {
    passing: &'p [usize],
    /// The next passing key to place.
    p: usize,
    blocks: Vec<(BlockHandle, Vec<usize>)>,
}

impl<'p> DataPlan<'p> {
    fn new(passing: &'p [usize]) -> Self {
        Self {
            passing,
            p: 0,
            blocks: Vec::new(),
        }
    }

    /// Where the walk continues after `handle`: the next passing key to place
    /// when it lies past `handle`'s end, so the index is sought there instead
    /// of walked entry by entry; `None` to take the entry after `handle`,
    /// which a key equal to its end continues into.
    fn seek_past<'k>(
        &self,
        handle: &KeyedBlockHandle,
        sorted_keys: &[(&'k [u8], u64)],
        table: &Table,
    ) -> Option<&'k [u8]> {
        let &pos = self.passing.get(self.p)?;
        let &(key, _) = sorted_keys.get(pos)?;
        (table.comparator.compare(key, handle.end_key()) == core::cmp::Ordering::Greater)
            .then_some(key)
    }

    /// Places the keys the index entry `handle` covers; `false` once every
    /// passing key is placed and the walk can stop.
    fn feed(
        &mut self,
        handle: &KeyedBlockHandle,
        sorted_keys: &[(&[u8], u64)],
        table: &Table,
    ) -> bool {
        let Some(&first) = self.passing.get(self.p) else {
            return false;
        };
        let Some(&(first_key, _)) = sorted_keys.get(first) else {
            return false;
        };
        let end_key = handle.end_key();
        if table.comparator.compare(first_key, end_key) == core::cmp::Ordering::Greater {
            return true;
        }
        let mut block_keys = Vec::new();
        while let Some(&pos) = self.passing.get(self.p) {
            let Some(&(key, _)) = sorted_keys.get(pos) else {
                break;
            };
            match table.comparator.compare(key, end_key) {
                core::cmp::Ordering::Greater => break,
                core::cmp::Ordering::Less => {
                    block_keys.push(pos);
                    self.p += 1;
                }
                core::cmp::Ordering::Equal => {
                    block_keys.push(pos);
                    break;
                }
            }
        }
        self.blocks.push((*handle.as_ref(), block_keys));
        self.p < self.passing.len()
    }

    /// The passing keys past the last block, none of which a block holds; a
    /// key equal to the last block's end key is listed in it and not counted.
    fn blockless(&self) -> usize {
        let listed = self.blocks.last().map(|(_, keys)| keys.as_slice());
        self.passing
            .iter()
            .skip(self.p)
            .filter(|pos| !listed.is_some_and(|keys| keys.contains(pos)))
            .count()
    }
}

#[cfg(test)]
mod tests;
