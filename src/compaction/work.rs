// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a compaction's work costs, by the kind of work each byte takes:
//! estimated from metadata before the compaction runs, and measured as it
//! reads its inputs and installs its outputs.
//!
//! A byte of compaction work costs one of four things. A table given another
//! level where it lies costs a version edit. An encoded unit copied from an
//! input into an output costs its read and its write, and no decoding. A copy
//! authenticated or protected again under the output's identity costs that on
//! top. A value encoded from decoded rows costs the codec as well. The four
//! are counted apart because a choice among compactions weighs them apart: a
//! merge that copies most of what it writes costs a fraction of one that
//! encodes it all, though both move the same bytes.

use crate::comparator::UserComparator;
use crate::range_tombstone::RangeTombstone;
use crate::{BlobFile, Table, blob_tree::FragmentationMap};
use alloc::sync::Arc;
use portable_atomic::{AtomicU64, Ordering::Relaxed};

#[cfg(feature = "std")]
use parking_lot::Mutex;
// no-std: spin mirrors parking_lot's Mutex API without an allocator.
#[cfg(not(feature = "std"))]
use spin::Mutex;

/// One figure per kind of compaction work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Work<T> {
    /// Bytes of tables given another level where they lie: the version
    /// records them there, and nothing is read or written.
    pub moved: T,
    /// Encoded bytes copied from an input into an output without being
    /// decoded: the row groups and column pages a merge left as they were,
    /// the blocks of a relocated segment, blob frames.
    pub carried: T,
    /// Copied bytes authenticated or protected again under the output's
    /// identity, their payload neither decoded nor compressed again. No
    /// compaction makes such a copy: the units of an encrypted table, or of
    /// one with parity, are bound to it, and a merge decodes its rows.
    pub reprotected: T,
    /// Output bytes encoded from decoded rows.
    pub reencoded: T,
    /// Bytes read from the inputs' files.
    pub read: T,
    /// Bytes written to the outputs: the output tables' files and the value
    /// frames of the output blob files.
    pub written: T,
}

impl<T> Work<T> {
    /// Pairs each figure of `self` with the same figure of `other`.
    fn zip<U, V>(self, other: Work<U>, f: impl Fn(T, U) -> V) -> Work<V> {
        Work {
            moved: f(self.moved, other.moved),
            carried: f(self.carried, other.carried),
            reprotected: f(self.reprotected, other.reprotected),
            reencoded: f(self.reencoded, other.reencoded),
            read: f(self.read, other.read),
            written: f(self.written, other.written),
        }
    }
}

/// What a compaction is expected to cost, worked out from metadata before it
/// runs: no input is read to make it and no compaction filter is called.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkEstimate {
    /// The work expected.
    pub expected: Work<u64>,
    /// The most each kind of work can come to, or `None` where nothing known
    /// before the run bounds it: what a merge reads and writes depends on the
    /// rows it drops, on what a compaction filter or merge operator makes of
    /// the rest and on how the output's codec encodes them. The least is zero
    /// for every kind, since a filter may drop every row.
    pub ceiling: Work<Option<u64>>,
}

impl WorkEstimate {
    /// An estimate of exactly `work`, every figure its own bound.
    fn exact(work: Work<u64>) -> Self {
        Self {
            expected: work,
            ceiling: work.zip(work, |figure, _| Some(figure)),
        }
    }

    /// Whether every figure of `measured` lies within the bounds.
    #[must_use]
    pub fn contains(&self, measured: &Work<u64>) -> bool {
        let within = measured.zip(self.ceiling, |figure, ceiling| {
            ceiling.is_none_or(|ceiling| figure <= ceiling)
        });
        within.moved
            && within.carried
            && within.reprotected
            && within.reencoded
            && within.read
            && within.written
    }
}

/// A compaction's work, as estimated before it ran and as measured.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WorkReport {
    /// The estimate the compaction was planned against.
    pub estimate: WorkEstimate,
    /// The work it did: what it read, and what it installed.
    pub measured: Work<u64>,
}

impl WorkReport {
    /// The estimate's error per kind of work: the measured figure less the
    /// expected one.
    #[must_use]
    pub fn error(&self) -> Work<i128> {
        self.measured
            .zip(self.estimate.expected, |measured, expected| {
                i128::from(measured) - i128::from(expected)
            })
    }
}

/// What one install put in the version: the bytes its outputs hold, the part
/// of them copied rather than encoded, and the bytes read outside the run's
/// paced input reads.
#[derive(Clone, Copy, Debug, Default)]
pub struct Installed {
    pub written: u64,
    pub carried: u64,
    pub read: u64,
}

/// The work of one compaction run as it goes: the estimate it was planned
/// against, the bytes its reads returned and what its installs put in the
/// version. Shared by every thread of the run.
#[derive(Default)]
pub struct WorkLedger {
    estimate: Mutex<WorkEstimate>,
    moved: AtomicU64,
    carried: AtomicU64,
    written: AtomicU64,
    read: AtomicU64,
}

impl WorkLedger {
    /// Records the estimate the run is planned against.
    pub(crate) fn plan(&self, estimate: WorkEstimate) {
        *self.estimate.lock() = estimate;
    }

    /// Records tables given another level where they lie.
    pub(crate) fn moved(&self, bytes: u64) {
        self.moved.fetch_add(bytes, Relaxed);
    }

    /// Records the bytes a read of an input returned.
    pub(crate) fn read(&self, bytes: u64) {
        self.read.fetch_add(bytes, Relaxed);
    }

    /// Records what an install put in the version.
    pub(crate) fn installed(&self, installed: Installed) {
        self.written.fetch_add(installed.written, Relaxed);
        self.carried.fetch_add(installed.carried, Relaxed);
        self.read.fetch_add(installed.read, Relaxed);
    }

    /// The run's work so far, against its estimate.
    pub(crate) fn report(&self) -> WorkReport {
        let written = self.written.load(Relaxed);
        let carried = self.carried.load(Relaxed);
        debug_assert!(
            carried <= written,
            "every carried byte is a byte of an output"
        );
        WorkReport {
            estimate: *self.estimate.lock(),
            measured: Work {
                moved: self.moved.load(Relaxed),
                carried,
                reprotected: 0,
                // Every carried byte lies in an output the same install
                // counted as written.
                reencoded: written - carried,
                read: self.read.load(Relaxed),
                written,
            },
        }
    }
}

/// The pacer of a compaction's input reads. It tells the limiter's latency
/// backoff how long the reads take and charges nothing: a compaction pays its
/// limiter for what it writes, and the reads of its inputs only steer the
/// rate. Active while the limiter has a backoff to steer, so without one the
/// reads go as they would unpaced. Every read it hears of is counted to the
/// run's work.
pub struct InputPacer {
    // no-std: reads are timed only with std (see `ReadPacer::read_took`)
    #[cfg(feature = "std")]
    limiter: Arc<crate::rate_limiter::RateLimiter>,
    work: Arc<WorkLedger>,
}

impl InputPacer {
    /// A pacer steering `limiter` and counting to `work`.
    pub(crate) fn new(
        limiter: &Arc<crate::rate_limiter::RateLimiter>,
        work: &Arc<WorkLedger>,
    ) -> Self {
        #[cfg(not(feature = "std"))]
        let _ = limiter;
        Self {
            #[cfg(feature = "std")]
            limiter: Arc::clone(limiter),
            work: Arc::clone(work),
        }
    }
}

impl crate::table::util::ReadPacer for InputPacer {
    fn active(&self) -> bool {
        #[cfg(feature = "std")]
        return self.limiter.steers_by_latency();
        #[cfg(not(feature = "std"))]
        false
    }

    fn pace(&self, _offset: u64, _len: u64) {}

    #[cfg(feature = "std")]
    fn read_took(&self, elapsed: core::time::Duration) {
        self.limiter.record_read_latency(elapsed);
    }

    fn read_bytes(&self, len: u64) {
        self.work.read(len);
    }
}

/// The work of giving `tables` another level where they lie.
pub fn estimate_move<'a>(tables: impl IntoIterator<Item = &'a Table>) -> WorkEstimate {
    WorkEstimate::exact(Work {
        moved: tables.into_iter().map(Table::file_len).sum(),
        ..Work::default()
    })
}

/// What a merge's estimate is worked out from.
pub struct MergePlan<'a> {
    /// The tables merged.
    pub inputs: &'a [Table],
    /// The blob files the merge relocates.
    pub blob_files: &'a [BlobFile],
    /// The garbage the version charged to each blob file.
    pub gc_stats: &'a FragmentationMap,
    /// Whether the rows pass through user code, a compaction filter or a
    /// merge operator, that may change any of them.
    pub user_code: bool,
    /// The range tombstones the merge deletes the keys they cover by.
    pub range_tombstones: &'a [RangeTombstone],
    /// The tree's key order.
    pub comparator: &'a dyn UserComparator,
}

/// The work of the merge `plan` describes, with `carries` saying whether the
/// merge records a table's row groups to copy them.
///
/// A table's row groups are expected to be copied when nothing the merge does
/// can change a row of them: no other input holds a key within its range, it
/// holds one version per key and no tombstone, no range tombstone the merge
/// applies covers any of its keys and no user code sees its rows. The rest of
/// the inputs is expected to be written again whole, and every frame of a
/// relocated blob file that the version did not charge as garbage copied.
pub fn estimate_merge(plan: &MergePlan<'_>, carries: impl Fn(&Table) -> bool) -> WorkEstimate {
    let mut expected = Work::<u64>::default();
    let mut carried_ceiling = 0;
    for (at, table) in plan.inputs.iter().enumerate() {
        let size = table.file_len();
        expected.written += size;
        carried_ceiling += size;
        // The whole file is read by its scan.
        expected.read += size;
        if !carries(table) || !unchanged_by_merge(plan, at, table) {
            continue;
        }
        let groups = table.data_section_len();
        expected.carried += groups;
        // A copied group is read again, as it lies.
        expected.read += groups;
    }
    for blob_file in plan.blob_files {
        let frames = blob_file.meta().total_compressed_bytes;
        let garbage = plan
            .gc_stats
            .get(&blob_file.id())
            .map_or(0, |entry| entry.on_disk_bytes);
        // A file charged past its frames holds no live frame: zero is the
        // answer, not a bound hit.
        let live = frames.saturating_sub(garbage);
        expected.carried += live;
        expected.written += live;
        expected.read += frames;
        carried_ceiling += frames;
    }
    // Every copied byte is one of the bytes expected written.
    expected.reencoded = expected.written - expected.carried;
    WorkEstimate {
        expected,
        ceiling: Work {
            moved: Some(0),
            // A copy takes a unit of an input as it lies, each at most once.
            carried: Some(carried_ceiling),
            reprotected: Some(0),
            reencoded: None,
            read: None,
            written: None,
        },
    }
}

/// Whether the merge `plan` describes leaves every row of `table`, its input
/// `at`, as it is.
fn unchanged_by_merge(plan: &MergePlan<'_>, at: usize, table: &Table) -> bool {
    let meta = &table.metadata;
    let (min, max) = (meta.key_range.min(), meta.key_range.max());
    let below = |a: &[u8], b: &[u8]| plan.comparator.compare(a, b) == core::cmp::Ordering::Less;
    !plan.user_code
        && meta.tombstone_count == 0
        && meta.weak_tombstone_count == 0
        && meta.range_tombstone_count == 0
        && meta.key_count == Some(meta.item_count)
        && plan.inputs.iter().enumerate().all(|(other_at, other)| {
            let other = &other.metadata.key_range;
            other_at == at || below(other.max(), min) || below(max, other.min())
        })
        // A tombstone covers `start <= key < end`.
        && plan
            .range_tombstones
            .iter()
            .all(|rt| below(max, &rt.start) || !below(min, &rt.end))
}

#[cfg(test)]
mod tests;
