// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! What a read is about and what it asks its driver for. A read holds its
//! context (the version it reads, its keys, how a version becomes a value)
//! and hands out block reads and jobs; a job names tables by their place in
//! the version and keys by their index, so it holds no borrow and can be
//! carried out on any thread against the same context.

use super::Tree;
use crate::comparator::SharedComparator;
use crate::merge_operator::MergeOperator;
use crate::table::probe_stats::PlanCounts;
use crate::table::{BlockHandle, BlockTaskPlan, DataBlock};
use crate::version::{SnapshotRef, Version};
use crate::{InternalValue, SeqNo, Table, UserValue, fs::FsFile};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// How a key's newest version becomes its value.
pub enum Values {
    /// Stored inline: the version is the value.
    Inline,
    /// A blob tree's: an indirection is followed into the value log.
    Blob(crate::blob_tree::BlobSource),
}

/// What a read is about: the version it holds for its whole life, its keys,
/// and how it reads them.
pub struct ReadCtx<K> {
    pub super_version: SnapshotRef,
    pub keys: Vec<K>,
    pub seqno: SeqNo,
    pub comparator: SharedComparator,
    pub merge_operator: Option<Arc<dyn MergeOperator>>,
    /// Where a merge reads a base kept in the value log; set for a blob
    /// tree and its index.
    pub merge_base: Option<crate::blob_tree::BlobSource>,
    pub values: Values,
    /// The most filter and index bytes a level's staged read holds.
    pub metadata_budget: u64,
}

impl<K: AsRef<[u8]>> ReadCtx<K> {
    /// The version the read holds.
    pub(super) fn version(&self) -> &Version {
        &self.super_version.version
    }

    /// The key at `idx`; empty for an index no key has, which no read hands
    /// out.
    pub(super) fn key(&self, idx: usize) -> &[u8] {
        self.keys.get(idx).map_or(&[], AsRef::as_ref)
    }
}

/// A table's place in a version: its level, its run in the level, and its
/// position in the run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TableAt {
    pub(super) level: usize,
    pub(super) run: usize,
    pub(super) pos: usize,
}

impl TableAt {
    /// The table at this place in `version`.
    pub(super) fn table(self, version: &Version) -> Option<&Table> {
        version.level(self.level)?.get(self.run)?.get(self.pos)
    }
}

/// A table no longer at the place a job names: the job's version is the
/// one it was planned against, so this is a defect, reported rather than
/// panicking.
fn misplaced() -> crate::Error {
    crate::Error::Io(crate::io::Error::new(
        crate::io::ErrorKind::Other,
        "a read job names a table its version does not hold",
    ))
}

/// A block read: `buf` filled whole from `file` at `offset`, and handed back
/// with `tag`.
pub(super) struct BlockRequest<'a> {
    pub(super) tag: usize,
    pub(super) table: &'a Table,
    pub(super) file: Arc<dyn FsFile>,
    pub(super) offset: u64,
    pub(super) buf: Vec<u8>,
}

/// Work a read asks for that is not one block read: it may block, so its
/// driver runs it wherever it likes, against the read's context, and hands
/// back what [`Self::run`] returns.
pub(super) enum Job {
    /// Opens a table's file.
    Open { tag: usize, table: TableAt },
    /// Plans a table serially for the keys of `keys`: its blocks need the
    /// load path, or a stage of it failed.
    Plan {
        at: usize,
        table: TableAt,
        keys: Vec<(usize, u64)>,
    },
    /// Loads a Page-ECC or columnar table's data block through the table's
    /// own path, which heals a corrected ECC block and reconstructs a
    /// columnar one.
    Load {
        index: usize,
        table: TableAt,
        handle: BlockHandle,
    },
    /// Resolves the keys of `remaining` against a level key by key, run by
    /// run: the level's staged resolve could not plan a table of it or read
    /// a block of its plan.
    Serial {
        level: usize,
        remaining: Vec<(usize, u64)>,
    },
    /// Resolves the merge operands of the key at `idx`, reading every
    /// version of it below the newest.
    Merge { idx: usize },
    /// Reads the value the indirection `item`, the newest version of the key
    /// at `idx`, points to in the value log.
    Blob { idx: usize, item: InternalValue },
}

/// A finished [`Job`].
pub(super) enum JobDone {
    Opened {
        tag: usize,
        file: crate::Result<Arc<dyn FsFile>>,
    },
    Planned {
        at: usize,
        plan: crate::Result<Option<BlockTaskPlan>>,
        tally: PlanCounts,
    },
    Loaded {
        index: usize,
        block: crate::Result<Option<DataBlock>>,
    },
    Serial {
        result: crate::Result<super::tables_read::SerialLevel>,
    },
    /// The value of the key at `idx`, from a merge or a blob read.
    Value {
        idx: usize,
        value: crate::Result<Option<UserValue>>,
    },
}

impl Job {
    /// Carries the job out on the calling thread, against the context of the
    /// read that asked for it.
    pub(super) fn run<K: AsRef<[u8]>>(self, ctx: &ReadCtx<K>) -> JobDone {
        let version = ctx.version();
        match self {
            Self::Open { tag, table } => JobDone::Opened {
                tag,
                file: table
                    .table(version)
                    .ok_or_else(misplaced)
                    .and_then(Table::open_file),
            },
            Self::Plan { at, table, keys } => {
                let mut tally = PlanCounts::default();
                let batch: Vec<(&[u8], u64)> = keys
                    .iter()
                    .map(|&(idx, hash)| (ctx.key(idx), hash))
                    .collect();
                let plan = table
                    .table(version)
                    .ok_or_else(misplaced)
                    .and_then(|table| table.plan_block_tasks(&batch, ctx.seqno, &mut tally));
                JobDone::Planned { at, plan, tally }
            }
            Self::Load {
                index,
                table,
                handle,
            } => JobDone::Loaded {
                index,
                block: table
                    .table(version)
                    .ok_or_else(misplaced)
                    .and_then(|table| table.load_data_block(&handle)),
            },
            Self::Serial { level, remaining } => JobDone::Serial {
                result: version.level(level).ok_or_else(misplaced).and_then(|on| {
                    Tree::resolve_level_serially(
                        level == 0,
                        on,
                        remaining,
                        &ctx.keys,
                        ctx.seqno,
                        ctx.comparator.as_ref(),
                    )
                }),
            },
            Self::Merge { idx } => JobDone::Value {
                idx,
                value: match &ctx.merge_operator {
                    Some(merge_operator) => Tree::resolve_merge_via_pipeline(
                        (*ctx.super_version).clone(),
                        ctx.key(idx),
                        ctx.seqno,
                        Arc::clone(merge_operator),
                        ctx.merge_base.clone(),
                    ),
                    None => Err(misplaced()),
                },
            },
            Self::Blob { idx, item } => JobDone::Value {
                idx,
                value: match &ctx.values {
                    Values::Blob(source) => source.value(version, item).map(Some),
                    Values::Inline => Ok(Some(item.value)),
                },
            },
        }
    }
}

/// Where a read's asks go as it makes them. A driver that submits a block
/// read the moment it is asked for takes it here directly, with no list in
/// between; a job is carried out only after the pass that asked for it,
/// since it hands its result back to the read.
pub(super) trait ReadSink<'a> {
    /// Takes a job the read asks for.
    fn job(&mut self, job: Job);

    /// Takes a block read the read asks for.
    fn read(&mut self, read: BlockRequest<'a>);
}

/// What a read asks for, kept until its driver takes it: jobs, then reads.
pub(super) struct ReadWork<'a> {
    pub(super) jobs: Vec<Job>,
    pub(super) reads: Vec<BlockRequest<'a>>,
}

impl ReadWork<'_> {
    pub(super) const fn new() -> Self {
        Self {
            jobs: Vec::new(),
            reads: Vec::new(),
        }
    }
}

impl<'a> ReadSink<'a> for ReadWork<'a> {
    fn job(&mut self, job: Job) {
        self.jobs.push(job);
    }

    fn read(&mut self, read: BlockRequest<'a>) {
        self.reads.push(read);
    }
}

/// A read the caller drives: it asks for work through `pump`, and takes each
/// piece back as it finishes, until `pump` hands out its answer.
pub(super) trait ReadMachine<'a> {
    /// What the read answers once it is over.
    type Output;

    /// Moves the read on as far as what is back takes it, asking `out` for
    /// the work it needs next; `Some` once the read is over and nothing of it
    /// is out.
    fn pump<S: ReadSink<'a>>(&mut self, out: &mut S) -> Option<Self::Output>;

    /// Takes back a finished job, asking `out` for the work it unblocks: a
    /// file back is read through at once, without waiting for the next pump.
    fn job_done<S: ReadSink<'a>>(&mut self, done: JobDone, out: &mut S);

    /// Takes back a finished block read.
    fn read_done(&mut self, done: crate::fs::ReadDone);
}
