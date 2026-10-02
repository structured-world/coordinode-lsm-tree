// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! The staged read of a level's tables as a machine: it decides which filter
//! and index blocks each table needs next and takes them back as they arrive,
//! and never submits, reads or waits itself. Whoever drives it carries the
//! reads out and hands each result back.

use super::{LevelRead, LevelTable, MetaHeld, Tree};
use crate::table::staged::{StagedRead, StagedStart};
use crate::{SeqNo, Table, table::BlockHandle};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// One block read a level asks for: `buf` is to be filled from `file` at
/// `offset`, and handed back with `tag` through [`LevelStages::complete`].
pub(super) struct StageRead<'a> {
    pub(super) tag: usize,
    pub(super) table: &'a Table,
    pub(super) file: Arc<dyn crate::fs::FsFile>,
    pub(super) offset: u64,
    pub(super) buf: Vec<u8>,
}

/// The staged read of a level's `tables` for their spans of `batch`.
pub(super) struct LevelStages<'a, 't, 'b> {
    tables: &'t mut [LevelTable<'a>],
    batch: &'b [(&'b [u8], u64)],
    seqno: SeqNo,
    metadata_budget: u64,
    /// What each read asked for is for: its tag is its position here.
    asked: Vec<(usize, BlockHandle)>,
    /// Each table's reads still in flight.
    waiting: Vec<usize>,
    /// At most as many tables hold a file open for their stages as the
    /// descriptor cache keeps open: a level wider than that, read stage by
    /// stage all at once, would otherwise open a file per table and run the
    /// process out of descriptors. A table whose stages are over lets its
    /// file go and its place with it, so the tables waiting for a place
    /// always get one.
    open_cap: usize,
    staged: Vec<bool>,
    in_stage: usize,
    meta: MetaHeld,
    /// Whether the last pass left a table waiting for a place under the cap,
    /// or for room under the budget.
    deferred: bool,
}

impl<'a, 't, 'b> LevelStages<'a, 't, 'b> {
    /// A table's read begins, at the snapshot `seqno`, over its span of
    /// `batch` once it has a place to read in.
    pub(super) fn new(
        tables: &'t mut [LevelTable<'a>],
        batch: &'b [(&'b [u8], u64)],
        seqno: SeqNo,
        metadata_budget: u64,
    ) -> Self {
        let open_cap = Tree::open_cap(tables.iter().map(|entry| entry.table));
        let count = tables.len();
        Self {
            tables,
            batch,
            seqno,
            metadata_budget,
            asked: Vec::new(),
            waiting: alloc::vec![0; count],
            open_cap,
            staged: alloc::vec![false; count],
            in_stage: 0,
            meta: MetaHeld::new(count),
            deferred: false,
        }
    }

    /// Whether the last pass left a table waiting: one that will move once a
    /// table in flight is back, or once nothing is.
    pub(super) fn deferred(&self) -> bool {
        self.deferred
    }

    /// One pass over the level: every table with none of its blocks in flight
    /// moves on as far as the blocks it holds take it, and asks for the next
    /// stage's blocks once its read lacks some, as long as it has a place
    /// under the cap and room under the budget. The reads asked for are
    /// appended to `out`.
    #[expect(
        clippy::indexing_slicing,
        reason = "`at` indexes the level's tables, which every per-table vector is sized to; a table's span lies in `batch`"
    )]
    pub(super) fn pump(&mut self, out: &mut Vec<StageRead<'a>>) {
        self.deferred = false;
        // Whether a table before the one at hand holds metadata blocks: the
        // first that does is never held back, so the level advances.
        let mut held_before = false;
        for at in 0..self.tables.len() {
            let first_holder = !held_before;
            if self.waiting[at] > 0 {
                held_before = true;
                continue;
            }
            let entry = &mut self.tables[at];
            let keys = &self.batch[entry.keys.clone()];
            if matches!(entry.read, LevelRead::Pending) {
                if self.in_stage >= self.open_cap {
                    self.deferred = true;
                    continue;
                }
                entry.read = match StagedRead::start(entry.table, keys, self.seqno) {
                    StagedStart::Nothing => LevelRead::Nothing,
                    StagedStart::Serial => LevelRead::Serial,
                    StagedStart::Staged(read) => LevelRead::Staged(read),
                };
            }
            while let LevelRead::Staged(read) = &mut entry.read
                && !read.is_done()
            {
                let (block_type, need) = read.need();
                if need.is_empty() {
                    // A stage passed lets go of the blocks it alone read.
                    if read.advance(keys).is_ok() {
                        self.meta.settle(at, read.held_bytes());
                    } else {
                        entry.read = LevelRead::Serial;
                        self.meta.settle(at, 0);
                    }
                    continue;
                }
                let table: &'a Table = entry.table;
                // A table with no place under the cap waits before any of its
                // stage's buffers is allocated: it is passed over again on
                // every pass until a place frees.
                if entry.file.is_none() && self.in_stage >= self.open_cap {
                    self.deferred = true;
                    break;
                }
                // Nor is any allocated for a stage past the budget, unless no
                // table before this one holds blocks: then it goes on even
                // alone above the budget, or the level would stop.
                let asked_bytes: u64 = need.iter().map(|handle| u64::from(handle.size())).sum();
                if !first_holder && self.meta.total + asked_bytes > self.metadata_budget {
                    self.deferred = true;
                    break;
                }
                // A size no block can have is refused before any buffer is
                // allocated for it; the serial planner then reports the
                // corruption as the load path does.
                let Ok(buffers) = need
                    .iter()
                    .map(|handle| table.block_buffer(handle))
                    .collect::<crate::Result<Vec<_>>>()
                else {
                    entry.read = LevelRead::Serial;
                    self.meta.settle(at, 0);
                    break;
                };
                let file = if let Some(file) = &entry.file {
                    Arc::clone(file)
                } else {
                    let Ok(file) = table.open_file() else {
                        entry.read = LevelRead::Serial;
                        self.meta.settle(at, 0);
                        break;
                    };
                    entry.file = Some(Arc::clone(&file));
                    self.staged[at] = true;
                    self.in_stage += 1;
                    file
                };
                table.record_batched_read(block_type, need);
                self.meta.in_flight[at] += asked_bytes;
                self.meta.settle(at, read.held_bytes());
                for (handle, buf) in need.iter().zip(buffers) {
                    out.push(StageRead {
                        tag: self.asked.len(),
                        table,
                        file: Arc::clone(&file),
                        offset: *handle.offset(),
                        buf,
                    });
                    self.asked.push((at, *handle));
                    self.waiting[at] += 1;
                }
                break;
            }
            // A table whose stages are over, or that is left to the serial
            // planner, needs its file no more: its data blocks are read
            // through the file their chunk opens, and holding it would keep a
            // descriptor per table of the level open until the whole level is
            // read.
            if self.waiting[at] == 0 && entry.read.is_over() {
                entry.file = None;
                if self.staged[at] {
                    self.staged[at] = false;
                    self.in_stage -= 1;
                }
                // Planned, or left to the serial planner: the blocks its
                // stages held are its read's no more.
                self.meta.settle(at, 0);
            }
            held_before |= self.meta.held[at] > 0;
        }
    }

    /// Takes back the read tagged `tag`: `result` is how it went, and `buf`
    /// holds the block when it went well. A read that loses a block is
    /// dropped, and its table planned serially.
    #[expect(
        clippy::indexing_slicing,
        reason = "a tag indexes `asked`, whose entries index the level's tables and `waiting`, all by construction"
    )]
    pub(super) fn complete(&mut self, tag: usize, result: crate::io::Result<()>, buf: &[u8]) {
        let (at, handle) = self.asked[tag];
        self.waiting[at] -= 1;
        // Back from flight: held from here on decoded, if at all.
        self.meta.in_flight[at] -= u64::from(handle.size());
        let LevelRead::Staged(read) = &mut self.tables[at].read else {
            self.meta.settle(at, 0);
            return;
        };
        let supplied = match result {
            Ok(()) => read.supply(handle, buf).is_ok(),
            Err(error) => {
                log::debug!(
                    "a staged level read lost a block, its table is planned serially: {error}"
                );
                false
            }
        };
        if supplied {
            self.meta.settle(at, read.held_bytes());
        } else {
            self.tables[at].read = LevelRead::Serial;
            self.meta.settle(at, 0);
        }
    }
}
