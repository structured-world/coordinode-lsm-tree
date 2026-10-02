// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! A multi-get its caller drives, which never blocks, sleeps or waits.
//!
//! The read hands out the block reads and the jobs it needs, the caller
//! carries them out however and wherever it likes, hands each result back
//! and resumes the read, until it answers.

use super::multi_get_read::MultiGetRead;
use super::read_job::{self, ReadCtx, ReadMachine, ReadWork};
use crate::fs::{Fs, FsFile};
use crate::{UserKey, UserValue};
use alloc::sync::Arc;
use alloc::vec::Vec;
use self_cell::self_cell;

/// The machine of a read, borrowing the context its cell owns.
struct Machine<'a> {
    read: MultiGetRead<'a, UserKey>,
    work: ReadWork<'a>,
}

self_cell!(
    struct Cell {
        owner: Arc<ReadCtx<UserKey>>,

        #[covariant]
        dependent: Machine,
    }
);

/// A multi-get suspended until the work it handed out is back.
///
/// Obtained from `start_multi_get` on a tree. Each [`resume`](Self::resume)
/// moves the read on as far as the results handed back take it. The read
/// holds the version it started on for its whole life, so compaction and
/// garbage collection running while it is suspended do not change its
/// answer.
///
/// Dropping it gives up the read: the buffers of the reads it handed out
/// belong to the caller, so dropping neither frees a buffer a device may
/// still write into nor waits for one.
pub struct ResumableMultiGet {
    cell: Cell,
    /// The backends the reads handed out go through, each once, by the id a
    /// [`BlockRequest`] names.
    backends: Vec<Arc<dyn Fs>>,
}

/// Where a multi-get stands after it is started or resumed.
pub enum Step {
    /// Work is out: carry out what [`ResumableMultiGet::take_reads`] and
    /// [`ResumableMultiGet::take_jobs`] hand out, hand each result back, and
    /// resume.
    Pending(ResumableMultiGet),
    /// The read is over: each key's value, in the order the keys were
    /// given, or the failure that ended it.
    Done(crate::Result<Vec<Option<UserValue>>>),
}

/// Names a block read to its read when it is handed back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadTag(usize);

/// A block read: `buf.len()` bytes of `file` from `offset`, the whole of
/// them, into `buf`.
///
/// The caller owns the buffer until it hands it back through
/// [`ResumableMultiGet::complete_read`] with the read's `tag`.
pub struct BlockRequest {
    /// Names the read when it is handed back.
    pub tag: ReadTag,
    /// The backend the file was opened through, by the id
    /// [`ResumableMultiGet::backend`] resolves: reads through one backend can
    /// be submitted together, for example through [`Fs::read_queue`].
    pub backend: usize,
    /// The file to read.
    pub file: Arc<dyn FsFile>,
    /// Where the block starts in `file`.
    pub offset: u64,
    /// The destination, exactly as long as the block.
    pub buf: Vec<u8>,
}

/// Work a read needs that is not one block read.
///
/// Opening a table's file, planning or loading the blocks of a Page-ECC or
/// columnar table, a merge, a blob value: it may block on I/O, so the caller
/// runs it on a thread it can spare, for example on a blocking pool, and
/// hands back what [`run`](Self::run) returns through
/// [`ResumableMultiGet::complete_job`].
pub struct ReadJob {
    ctx: Arc<ReadCtx<UserKey>>,
    job: read_job::Job,
}

impl ReadJob {
    /// Carries the job out on the calling thread.
    #[must_use]
    pub fn run(self) -> JobOutcome {
        JobOutcome(self.job.run(&self.ctx))
    }
}

/// A finished [`ReadJob`], to hand back to its read.
pub struct JobOutcome(read_job::JobDone);

impl ResumableMultiGet {
    /// The read of `ctx`, started.
    pub(crate) fn start(ctx: ReadCtx<UserKey>) -> Step {
        let cell = Cell::new(Arc::new(ctx), |ctx| Machine {
            read: MultiGetRead::new(ctx),
            work: ReadWork::new(),
        });
        Self {
            cell,
            backends: Vec::new(),
        }
        .resume()
    }

    /// Moves the read on as far as the results handed back take it.
    #[must_use]
    pub fn resume(mut self) -> Step {
        let answer = self
            .cell
            .with_dependent_mut(|_, machine| machine.read.pump(&mut machine.work));
        match answer {
            Some(answer) => Step::Done(answer),
            None => Step::Pending(self),
        }
    }

    /// The block reads handed out since the last call.
    pub fn take_reads(&mut self) -> Vec<BlockRequest> {
        let backends = &mut self.backends;
        self.cell.with_dependent_mut(|_, machine| {
            machine
                .work
                .reads
                .drain(..)
                .map(|read| {
                    let backend = backends
                        .iter()
                        .position(|fs| Arc::ptr_eq(fs, &read.table.fs))
                        .unwrap_or_else(|| {
                            backends.push(Arc::clone(&read.table.fs));
                            backends.len() - 1
                        });
                    BlockRequest {
                        tag: ReadTag(read.tag),
                        backend,
                        file: read.file,
                        offset: read.offset,
                        buf: read.buf,
                    }
                })
                .collect()
        })
    }

    /// The jobs handed out since the last call.
    pub fn take_jobs(&mut self) -> Vec<ReadJob> {
        self.cell.with_dependent_mut(|ctx, machine| {
            machine
                .work
                .jobs
                .drain(..)
                .map(|job| ReadJob {
                    ctx: Arc::clone(ctx),
                    job,
                })
                .collect()
        })
    }

    /// The backend a [`BlockRequest`] names by `id`.
    #[must_use]
    pub fn backend(&self, id: usize) -> Option<&Arc<dyn Fs>> {
        self.backends.get(id)
    }

    /// Hands back the block read tagged `tag`: `buf` holds the whole block
    /// when `result` is `Ok`. A read that failed or came back short makes the
    /// read take another path to the same answer, never a wrong one.
    pub fn complete_read(&mut self, tag: ReadTag, result: crate::io::Result<()>, buf: Vec<u8>) {
        self.cell.with_dependent_mut(|_, machine| {
            machine.read.read_done(crate::fs::ReadDone {
                tag: tag.0,
                buf,
                result,
            });
        });
    }

    /// Hands back a finished job.
    pub fn complete_job(&mut self, outcome: JobOutcome) {
        self.cell
            .with_dependent_mut(|_, machine| machine.read.job_done(outcome.0));
    }
}
