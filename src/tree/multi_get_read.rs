// Copyright (c) 2026-present, Dmitry Prudnikov
// This source code is licensed under the Apache 2.0 License
// (found in the LICENSE-APACHE file in the repository)

//! A multi-get as a machine: the memtables answer what they hold when it
//! starts, the version's levels are read for the rest, and each key's newest
//! version is then turned into its value, a merge or a blob read handed out
//! as a job. It reads the one version its context holds for the whole read
//! and never opens, reads or waits itself.

use super::Tree;
use super::read_job::{BlockRequest, Job, JobDone, ReadCtx, ReadMachine, ReadSink, Values};
use super::tables_read::TablesRead;
use crate::blob_tree::{ValuePlan, ValueRead};
use crate::fs::FsFile;
use crate::{InternalValue, UserValue};
use alloc::sync::Arc;
use alloc::vec::Vec;

/// A separated value read through the read's queue: the key it is for, its
/// indirection, the record to read and the blob file it is in.
struct BlobRead<'a> {
    idx: usize,
    item: InternalValue,
    read: ValueRead<'a>,
    /// The slot of its blob file in [`MultiGetRead::blob_files`].
    slot: usize,
}

/// The multi-get of the keys of its context.
pub(super) struct MultiGetRead<'a, K> {
    ctx: &'a ReadCtx<K>,
    /// Each key's newest version, from the memtables or the levels.
    entries: Vec<Option<InternalValue>>,
    /// `(duplicate index, representative index)` of keys asked more than
    /// once: the levels are read for the representative only.
    duplicates: Vec<(usize, usize)>,
    /// The read of the levels, while it goes on.
    tables: Option<TablesRead<'a, K>>,
    /// Whether every entry was turned into a value or handed out as a job.
    resolved: bool,
    results: Vec<Option<UserValue>>,
    /// Value jobs not yet back.
    jobs: usize,
    /// The failure of the lowest key a value job failed for: the one a
    /// resolve in key order would report.
    failure: Option<(usize, crate::Error)>,
    /// The value reads out, by tag; `None` once back.
    blob_reads: Vec<Option<BlobRead<'a>>>,
    /// The blob files those reads go through, each opened once: `None` while
    /// its open is out.
    blob_files: Vec<(crate::vlog::BlobFileId, Option<Arc<dyn FsFile>>)>,
    /// Values whose read failed or did not check out, to read on their own
    /// path, which reports what is wrong as a single read would.
    alone: Vec<(usize, InternalValue)>,
}

impl<'a, K: AsRef<[u8]>> MultiGetRead<'a, K> {
    /// The multi-get of `ctx`'s keys: the memtables are read here, and the
    /// keys they do not hold are sorted under the read's comparator for the
    /// levels.
    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n, always in bounds"
    )]
    pub(super) fn new(ctx: &'a ReadCtx<K>) -> Self {
        let keys = &ctx.keys;
        let n = keys.len();
        let super_version = &ctx.super_version;
        let comparator = ctx.comparator.as_ref();
        // The memtables first, unsorted: a lookup there is O(log n) per key
        // regardless of order, so a batch they answer whole skips the sort and
        // the hashing.
        let mut entries: Vec<Option<InternalValue>> = alloc::vec![None; n];
        let mut remaining: Vec<usize> = Vec::with_capacity(n);
        for idx in 0..n {
            let key = keys[idx].as_ref();
            if let Some(entry) = super_version.active_memtable.get(key, ctx.seqno) {
                entries[idx] = Some(entry);
                continue;
            }
            if let Some(entry) =
                Tree::get_internal_entry_from_sealed_memtables(super_version, key, ctx.seqno)
            {
                entries[idx] = Some(entry);
                continue;
            }
            remaining.push(idx);
        }

        let mut duplicates = Vec::new();
        let tables = (!remaining.is_empty()).then(|| {
            remaining.sort_by(|&a, &b| comparator.compare(keys[a].as_ref(), keys[b].as_ref()));
            // The levels take strictly sorted, unique keys: an equal key asked
            // again is answered from its representative.
            let (miss_keys, dups) = Tree::dedup_sorted_miss_keys(&remaining, keys, comparator);
            duplicates = dups;
            TablesRead::new(ctx, miss_keys)
        });

        Self {
            ctx,
            entries,
            duplicates,
            tables,
            resolved: false,
            results: alloc::vec![None; n],
            jobs: 0,
            failure: None,
            blob_reads: Vec::new(),
            blob_files: Vec::new(),
            alone: Vec::new(),
        }
    }

    /// Turns every key's newest version into its value: a tombstone, or a
    /// version a range tombstone covers, has none; a merge operand, with a
    /// merge operator, and a blob indirection are handed out as jobs.
    #[expect(
        clippy::indexing_slicing,
        reason = "indices are generated from 0..n, always in bounds"
    )]
    fn resolve(&mut self, out: &mut impl ReadSink<'a>) {
        let ctx = self.ctx;
        for idx in 0..self.entries.len() {
            let Some(entry) = self.entries[idx].take() else {
                continue;
            };
            if entry.is_tombstone() {
                continue;
            }
            if Tree::is_suppressed_by_range_tombstones(
                &ctx.super_version,
                ctx.key(idx),
                entry.key.seqno,
                ctx.seqno,
                ctx.comparator.as_ref(),
            ) {
                continue;
            }
            if entry.key.value_type.is_merge_operand() {
                // Without a merge operator the operand is the value, as a
                // single-key read returns it. Operands are stored inline in a
                // blob tree too, so the merge result is a plain value.
                if ctx.merge_operator.is_some() {
                    out.job(Job::Merge { idx });
                    self.jobs += 1;
                } else {
                    self.results[idx] = Some(entry.value);
                }
                continue;
            }
            // A cell row resolves its references from the value log as an
            // indirection does.
            let Values::Blob(source) = &ctx.values else {
                self.results[idx] = Some(entry.value);
                continue;
            };
            if !entry.key.value_type.is_indirection() && !entry.key.value_type.is_cell_row() {
                self.results[idx] = Some(entry.value);
                continue;
            }
            match source.plan_value(ctx.version(), &entry) {
                ValuePlan::Ready(value) => self.results[idx] = Some(value),
                ValuePlan::Alone => {
                    out.job(Job::Blob { idx, item: entry });
                    self.jobs += 1;
                }
                ValuePlan::Read(read) => {
                    let id = read.vhandle.blob_file_id;
                    let held = self.blob_files.iter().position(|&(held, _)| held == id);
                    let slot = held.unwrap_or_else(|| {
                        out.job(Job::OpenBlob {
                            tag: self.blob_files.len(),
                            blob_file: id,
                        });
                        self.blob_files.push((id, None));
                        self.blob_files.len() - 1
                    });
                    self.blob_reads.push(Some(BlobRead {
                        idx,
                        item: entry,
                        read,
                        slot,
                    }));
                    self.jobs += 1;
                }
            }
        }
    }

    /// Takes back the blob file opened for `slot` and hands `out` the value
    /// reads waiting on it; reads of a file that would not open go on their
    /// own path, which reports why.
    fn blob_opened<S: ReadSink<'a>>(
        &mut self,
        slot: usize,
        file: crate::Result<Arc<dyn FsFile>>,
        out: &mut S,
    ) {
        let Values::Blob(source) = &self.ctx.values else {
            debug_assert!(false, "a blob file opened for a read of inline values");
            return;
        };
        let version = self.ctx.version();
        match file {
            Ok(file) => {
                for (tag, pending) in self.blob_reads.iter().enumerate() {
                    let Some(pending) = pending.as_ref().filter(|p| p.slot == slot) else {
                        continue;
                    };
                    source.count_issued(version, &pending.read);
                    out.read(BlockRequest {
                        tag,
                        fs: &pending.read.blob_file.0.fs,
                        cache: &source.cache,
                        file: Arc::clone(&file),
                        offset: pending.read.vhandle.offset,
                        buf: alloc::vec![0u8; pending.read.len],
                    });
                }
                if let Some((_, held)) = self.blob_files.get_mut(slot) {
                    *held = Some(file);
                }
            }
            Err(_) => {
                for pending in &mut self.blob_reads {
                    if let Some(read) = pending.take_if(|p| p.slot == slot) {
                        self.alone.push((read.idx, read.item));
                    }
                }
            }
        }
    }

    /// Takes back a value read: checked and taken as the key's value, or, when
    /// it failed or does not check out, left to the value's own path.
    fn blob_read_done(&mut self, done: crate::fs::ReadDone) {
        let Some(pending) = self.blob_reads.get_mut(done.tag).and_then(Option::take) else {
            debug_assert!(false, "a value read handed back twice or never asked for");
            return;
        };
        let file = self
            .blob_files
            .get(pending.slot)
            .and_then(|(_, file)| file.as_ref());
        let value = match (&self.ctx.values, file, done.result) {
            (Values::Blob(source), Some(file), Ok(())) => source
                .finish_value(
                    self.ctx.version(),
                    &pending.item.key.user_key,
                    &pending.read,
                    file.as_ref(),
                    &crate::Slice::from(done.buf),
                )
                .ok(),
            _ => None,
        };
        match value {
            Some(value) => {
                self.jobs -= 1;
                if let Some(slot) = self.results.get_mut(pending.idx) {
                    *slot = Some(value);
                }
            }
            None => self.alone.push((pending.idx, pending.item)),
        }
    }
}

impl<'a, K: AsRef<[u8]>> ReadMachine<'a> for MultiGetRead<'a, K> {
    /// Each key's value, in the order the keys were asked.
    type Output = crate::Result<Vec<Option<UserValue>>>;

    fn pump<S: ReadSink<'a>>(&mut self, out: &mut S) -> Option<Self::Output> {
        if let Some(tables) = &mut self.tables {
            let read = tables.pump(&mut self.entries, out)?;
            self.tables = None;
            if let Err(error) = read {
                return Some(Err(error));
            }
            Tree::fan_out_duplicates(&self.duplicates, &mut self.entries);
        }
        if !self.resolved {
            self.resolved = true;
            self.resolve(out);
        }
        // Counted among the jobs already, as the reads they replace were.
        for (idx, item) in self.alone.drain(..) {
            out.job(Job::Blob { idx, item });
        }
        if self.jobs > 0 {
            return None;
        }
        if let Some((_, error)) = self.failure.take() {
            return Some(Err(error));
        }
        Some(Ok(core::mem::take(&mut self.results)))
    }

    fn job_done<S: ReadSink<'a>>(&mut self, done: JobDone, out: &mut S) {
        match done {
            JobDone::Value { idx, value } => {
                self.jobs -= 1;
                match value {
                    Ok(value) => {
                        if let Some(slot) = self.results.get_mut(idx) {
                            *slot = value;
                        }
                    }
                    Err(error) => {
                        if self.failure.as_ref().is_none_or(|(held, _)| idx < *held) {
                            self.failure = Some((idx, error));
                        }
                    }
                }
            }
            done => {
                if let Some(tables) = &mut self.tables {
                    tables.job_done(done, out);
                } else if let JobDone::Opened { tag, file } = done {
                    self.blob_opened(tag, file, out);
                } else {
                    debug_assert!(false, "a job handed back to a read that did not ask for it");
                }
            }
        }
    }

    // The levels are read before any value is, so a read back while they
    // are being read is theirs, and one back after is a value's.
    fn read_done(&mut self, done: crate::fs::ReadDone) {
        if let Some(tables) = &mut self.tables {
            tables.read_done(done);
        } else {
            self.blob_read_done(done);
        }
    }
}
