//! Mixed-layout scenarios: the instrument the read-path work is measured with.
//!
//! The sibling `lifecycle-zstd22` workload walks a real write / rewrite /
//! delete / compact / read cycle and reports ops/sec, but it is row-major throughout —
//! one opaque value per key, no projection, no columnar segment, no blob — so
//! a change that halves the bytes read for a two-field projection over a wide
//! record is invisible to it. This workload holds the shapes that change is
//! about, and reports the byte counters rather than the rate.
//!
//! # What is being reported
//!
//! Ops/sec is the least interesting number here. What each scenario prints is
//! **bytes read, bytes decoded and bytes copied**, normalised per emitted row,
//! because those are the quantities the read-path issues state their
//! acceptance in. Their definitions are in `docs/BENCHMARKING.md` and are
//! pinned by `tests/read_byte_counters.rs`; the short form is that read is
//! what was asked of the filesystem, decoded is what the block transform
//! produced from it, and copied is what gathers moved.
//!
//! # Every scenario checks what it read
//!
//! A scenario that only counted rows would report a flattering figure for a
//! build that stopped resolving versions, since skipping work is fast. Each
//! read pass compares every value against the [`fixtures::Oracle`] its fixture
//! derived from the write history, so a fast number from a wrong build fails
//! the run instead of being published.
//!
//! # Scenarios that cannot run yet say so
//!
//! Several shapes need capabilities that have not landed. A scenario whose
//! native path does not exist reports **unsupported** and contributes no
//! number. It is never quietly run through a fallback path under the same
//! name: publishing a figure that measures something else is worse than
//! publishing none, because the series looks continuous across the change
//! that was supposed to move it.
//!
//! Their fixtures exist regardless, and are exercised by this module's tests.
//! That is deliberate: the issues that add those capabilities need the fixture
//! and its expected data to already be there, so enabling a scenario is a
//! change of one line here rather than a fresh argument about what the
//! expected result is.

pub mod fixtures;

#[cfg(test)]
mod tests;

use crate::config::{BenchConfig, DEFAULT_KEY_SIZE, DEFAULT_VALUE_SIZE};
use crate::reporter::{Reporter, Suite};
use crate::workloads::Workload;
use fixtures::{Fixture, FixtureFn};
use lsm_tree::table::columnar::{COL_USER_KEY, COL_VALUE};
use lsm_tree::table::columnar_predicate::{ColumnRangePredicate, PredicateApply};
use lsm_tree::{AbstractTree, AnyTree, Guard, SeqNo};
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

pub struct MixedLayout;

/// One measured read pass over a built fixture, returning the rows it emitted.
///
/// It verifies as it goes and panics on disagreement: a measurement taken over
/// wrong results is not a slower or faster number, it is not a number. A read
/// that FAILS is returned instead, so the run reports the engine's error.
type ReadFn = fn(&Fixture) -> lsm_tree::Result<u64>;

/// A measured pass of a scan that yields in batches, reporting besides its
/// rows what it took to reach the first batch and what it held at most.
type ScanFn = fn(&Fixture) -> lsm_tree::Result<ScanPass>;

/// What a batch scan measured beyond the tree's counters.
struct ScanPass {
    rows: u64,
    /// Wall time and bytes read from the scan's creation to its first batch,
    /// or `None` when it yielded none.
    first_batch: Option<(Duration, u64)>,
    /// The most page bytes the scan held at once, as the engine counts it.
    retained: u64,
}

/// A pass repeated while something else runs against the tree, reporting
/// each repetition's latency besides its rows.
type LatencyFn = fn(&Fixture) -> lsm_tree::Result<LatencyPass>;

/// What a repeated pass measured: the rows of all its repetitions and how
/// long each took.
struct LatencyPass {
    rows: u64,
    latencies: Vec<Duration>,
}

/// A pass that rewrites rows, reporting besides the rows it updated the blob
/// payload bytes the rewrite wrote.
type UpdateFn = fn(&Fixture) -> lsm_tree::Result<UpdatePass>;

/// What an update pass measured beyond the tree's counters.
struct UpdatePass {
    rows: u64,
    /// Blob bytes written while the rows were rewritten and flushed.
    payload_written: u64,
}

/// Whether a scenario's native path exists in this build.
enum Support {
    /// Runs, through this read pass, and its figures mean what the scenario
    /// says they mean.
    Native(ReadFn),
    /// Runs, through this batch scan, which also reports its first batch and
    /// what it held.
    Scan(ScanFn),
    /// Runs, through this repeated pass, which also reports the P50 and P99
    /// of its repetitions.
    Latency(LatencyFn),
    /// Rewrites rows through this pass and reports the payload bytes the
    /// rewrite wrote per updated row.
    Update(UpdateFn),
    /// Rewrites the fixture in rounds of flushes and compactions, reports what
    /// collecting the stale blobs cost, then measures a full scan of what is
    /// left, so placement's two costs come from one run.
    Churn,
    /// The capability it measures has not landed. Carries the reason, which
    /// names the missing piece rather than saying "skipped". There is no read
    /// pass to hold, which is the point of pairing the two in one enum: a
    /// scenario cannot be marked native without one, or unsupported with one.
    Missing(&'static str),
}

/// What one scenario measured. Every field is either counted by the engine or
/// timed here — nothing is estimated, because an estimated figure cannot be
/// compared across a change that alters the estimate's inputs.
struct Readings {
    /// Keys the fixture built, which its own cap may hold below `--num`.
    keys: u64,
    rows: u64,
    bytes_read: u64,
    bytes_decoded: u64,
    bytes_copied: u64,
    /// What projected scans handed out, per row returned.
    bytes_materialized: u64,
    /// Of the payload read late, the chosen rows' cells and what came along.
    payload_useful: u64,
    payload_incidental: u64,
    /// The blob bytes read ahead in coalesced spans.
    blob_prefetched: u64,
    elapsed: std::time::Duration,
    /// For a batch scan: its first batch and what it held.
    scan: Option<ScanFigures>,
}

/// What a batch scan reports beyond the counters every scenario has.
struct ScanFigures {
    /// Wall time and bytes read until the first batch.
    first_batch: Option<(Duration, u64)>,
    /// The most page bytes the scan held at once.
    retained: u64,
}

impl Readings {
    /// Takes a before/after difference of the tree's counters around `body`.
    ///
    /// Differences, not absolutes: the fixture build reads as it compacts, and
    /// charging that to the scenario's read would bury the figure the scenario
    /// exists to report under the cost of creating its own input.
    fn measure(
        tree: &AnyTree,
        keys: u64,
        body: impl FnOnce() -> lsm_tree::Result<u64>,
    ) -> lsm_tree::Result<Self> {
        let m = tree.metrics();
        let (r0, d0, c0) = (m.bytes_read(), m.bytes_decoded(), m.bytes_copied());
        let (mat0, use0, inc0, pre0) = (
            m.bytes_materialized(),
            m.payload_bytes_useful(),
            m.payload_bytes_incidental(),
            m.blob_bytes_prefetched(),
        );
        let start = Instant::now();
        let rows = body()?;
        let elapsed = start.elapsed();
        Ok(Self {
            keys,
            rows,
            bytes_read: m.bytes_read() - r0,
            bytes_decoded: m.bytes_decoded() - d0,
            bytes_copied: m.bytes_copied() - c0,
            bytes_materialized: m.bytes_materialized() - mat0,
            payload_useful: m.payload_bytes_useful() - use0,
            payload_incidental: m.payload_bytes_incidental() - inc0,
            blob_prefetched: m.blob_bytes_prefetched() - pre0,
            elapsed,
            scan: None,
        })
    }

    /// [`Self::measure`] around a batch scan, keeping what it reports of its
    /// first batch and of what it held.
    fn measure_scan(
        tree: &AnyTree,
        keys: u64,
        body: impl FnOnce() -> lsm_tree::Result<ScanPass>,
    ) -> lsm_tree::Result<Self> {
        let mut figures = None;
        let mut readings = Self::measure(tree, keys, || {
            let pass = body()?;
            figures = Some(ScanFigures {
                first_batch: pass.first_batch,
                retained: pass.retained,
            });
            Ok(pass.rows)
        })?;
        readings.scan = figures;
        Ok(readings)
    }

    /// Bytes one counter moved per emitted row, or `None` when the scenario
    /// emitted no row and the cost per row is undefined.
    #[expect(
        clippy::cast_precision_loss,
        reason = "ratios over counts far below f64's exact range"
    )]
    fn per_row(&self, bytes: u64) -> Option<f64> {
        (self.rows != 0).then(|| bytes as f64 / self.rows as f64)
    }

    /// Publishes the three counters as bytes per emitted row, all costs, and
    /// carries the raw readings in the annotation.
    ///
    /// Per row rather than per byte of another counter, so every series has
    /// the same denominator, and one that is legitimately zero (a read that
    /// gathers nothing copies nothing) is the best value of its series rather
    /// than a division by zero. A scenario that emitted no row publishes
    /// nothing: its cost per row does not exist, and a stand-in value would
    /// draw a point the run did not measure.
    fn publish(&self, scenario: &str, reporter: &mut Reporter) {
        let (Some(read), Some(decoded), Some(copied)) = (
            self.per_row(self.bytes_read),
            self.per_row(self.bytes_decoded),
            self.per_row(self.bytes_copied),
        ) else {
            return;
        };
        // `keys` is the working set the scenario built. Each fixture caps it
        // on its own, so it is the size the series describes, not `--num`.
        let annotation = format!(
            "keys: {} | rows: {} | read: {} B | decoded: {} B | copied: {} B | \
             materialized: {} B | payload useful: {} B | payload incidental: {} B | \
             blob prefetched: {} B | elapsed: {:?}",
            self.keys,
            self.rows,
            self.bytes_read,
            self.bytes_decoded,
            self.bytes_copied,
            self.bytes_materialized,
            self.payload_useful,
            self.payload_incidental,
            self.blob_prefetched,
            self.elapsed,
        );
        for (counter, value) in [("read", read), ("decoded", decoded), ("copied", copied)] {
            reporter.publish_series(
                format!("{scenario} bytes {counter} per row"),
                value,
                "B/row",
                annotation.clone(),
                Suite::Costs,
            );
        }
        // A projected scan's own figures, published only where one ran: a row
        // read materialises nothing through a scan and reads no payload late.
        if let Some(materialized) = self
            .per_row(self.bytes_materialized)
            .filter(|_| self.bytes_materialized > 0)
        {
            reporter.publish_series(
                format!("{scenario} bytes materialized per row"),
                materialized,
                "B/row",
                annotation.clone(),
                Suite::Costs,
            );
        }
        if self.payload_useful + self.payload_incidental > 0 {
            for (counter, value) in [
                ("useful", self.payload_useful),
                ("incidental", self.payload_incidental),
            ] {
                if let Some(value) = self.per_row(value) {
                    reporter.publish_series(
                        format!("{scenario} payload bytes {counter} per row"),
                        value,
                        "B/row",
                        annotation.clone(),
                        Suite::Costs,
                    );
                }
            }
        }
        let Some(scan) = &self.scan else {
            return;
        };
        // Bytes are counted by the engine and go with the costs; the time to
        // the first batch is timed on this host, so it goes to the host's own
        // timings.
        #[expect(
            clippy::cast_precision_loss,
            reason = "byte counts far below f64's exact range"
        )]
        let mut figures = vec![("retained payload", scan.retained as f64, "B", Suite::Costs)];
        if let Some((time, bytes)) = scan.first_batch {
            #[expect(
                clippy::cast_precision_loss,
                reason = "byte counts far below f64's exact range"
            )]
            figures.extend([
                (
                    "time to first batch",
                    time.as_secs_f64() * 1e6,
                    "us",
                    Suite::Timings,
                ),
                ("bytes read to first batch", bytes as f64, "B", Suite::Costs),
            ]);
        }
        for (figure, value, unit, suite) in figures {
            reporter.publish_series(
                format!("{scenario} {figure}"),
                value,
                unit,
                annotation.clone(),
                suite,
            );
        }
    }

    /// The human-readable line. On stderr, like every other line the harness
    /// narrates with: stdout carries the machine-readable report and nothing
    /// else, so `--github-json` stays parseable.
    ///
    /// Besides the published figures it prints two diagnostics that are not
    /// series: decoded over read (the compression the read paid for) and
    /// copied over decoded (how much of what the transform produced was then
    /// moved again). Either is `n/a` when its denominator is zero, as it is
    /// for a read served wholly from cache.
    fn report(&self, scenario: &str) {
        let per_row = |bytes| fmt_ratio(self.per_row(bytes), 1);
        eprintln!(
            "  {scenario:<34} keys={:<9} rows={:<9} read/row={:<9} decoded/row={:<9} \
             copied/row={:<9} decoded/read={:<5} copied/decoded={:<5} {:?}",
            self.keys,
            self.rows,
            per_row(self.bytes_read),
            per_row(self.bytes_decoded),
            per_row(self.bytes_copied),
            fmt_ratio(ratio(self.bytes_decoded, self.bytes_read), 2),
            fmt_ratio(ratio(self.bytes_copied, self.bytes_decoded), 2),
            self.elapsed,
        );
        if self.bytes_materialized + self.payload_useful + self.payload_incidental > 0 {
            eprintln!(
                "  {:<34} materialized/row={:<9} payload useful={} B incidental={} B \
                 blob prefetched={} B",
                "",
                per_row(self.bytes_materialized),
                self.payload_useful,
                self.payload_incidental,
                self.blob_prefetched,
            );
        }
        if let Some(scan) = &self.scan {
            let first = scan.first_batch.map_or_else(
                || "no batch".to_string(),
                |(time, bytes)| format!("{time:?} after {bytes} B read"),
            );
            eprintln!(
                "  {:<34} first batch: {first}, retained: {} B",
                "", scan.retained,
            );
        }
    }
}

/// `num / den`, or `None` when `den` is zero.
#[expect(
    clippy::cast_precision_loss,
    reason = "ratios over counts far below f64's exact range"
)]
fn ratio(num: u64, den: u64) -> Option<f64> {
    (den != 0).then(|| num as f64 / den as f64)
}

/// A ratio for the text report, `n/a` where it is undefined.
fn fmt_ratio(value: Option<f64>, decimals: usize) -> String {
    value.map_or_else(|| "n/a".to_string(), |v| format!("{v:.decimals$}"))
}

/// Reads every key the fixture wrote and checks it against the oracle.
///
/// The shared pass for the shapes whose scenario is "read it all back": it
/// asserts presence, absence and content, so a build that lost a version, kept
/// a deleted key or returned a neighbouring row fails here rather than
/// publishing a fast number.
fn verify_point_reads(fixture: &Fixture) -> lsm_tree::Result<u64> {
    let mut rows = 0_u64;
    for row in &fixture.oracle.rows {
        let got = fixture.tree.get(&*row.key, SeqNo::MAX)?;
        match (&row.expect, got) {
            (Some(expected), Some(actual)) => {
                assert_eq!(
                    &*actual,
                    fixture.read_bytes(*expected)?.as_slice(),
                    "value disagrees with the write history for key {:?}",
                    String::from_utf8_lossy(&row.key),
                );
                rows += 1;
            }
            (None, None) => {}
            (Some(_), None) => panic!(
                "key {:?} was written and not deleted, but reads as absent",
                String::from_utf8_lossy(&row.key),
            ),
            (None, Some(_)) => panic!(
                "key {:?} was deleted, but still reads as present",
                String::from_utf8_lossy(&row.key),
            ),
        }
    }
    assert_eq!(
        rows,
        fixture.oracle.visible(),
        "the pass emitted a different number of rows than the write history \
         says are visible",
    );
    Ok(rows)
}

/// Checks a scan's rows against the rows of the write history it should
/// return, one at a time and in order, so a missing, duplicated, reordered or
/// wrong row fails at the first place it diverges; a count alone would let one
/// omission and one wrong row cancel out.
struct Lockstep<'a, I: Iterator<Item = (&'a [u8], fixtures::Value)>> {
    expected: I,
    rows: u64,
}

impl<'a, I: Iterator<Item = (&'a [u8], fixtures::Value)>> Lockstep<'a, I> {
    /// Checks the next emitted row, whose value must equal `expect`'s
    /// rendering of the version the write history holds.
    fn check(
        &mut self,
        key: &[u8],
        value: &[u8],
        expect: impl FnOnce(fixtures::Value) -> lsm_tree::Result<Vec<u8>>,
    ) -> lsm_tree::Result<()> {
        let Some((want_key, want)) = self.expected.next() else {
            panic!(
                "the scan emitted {:?} after the last row the write history selects",
                String::from_utf8_lossy(key),
            );
        };
        assert_eq!(
            key,
            want_key,
            "the scan emitted {:?} where the write history has {:?} next",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(want_key),
        );
        assert_eq!(
            value,
            expect(want)?.as_slice(),
            "value disagrees with the write history for key {:?}",
            String::from_utf8_lossy(key),
        );
        self.rows += 1;
        Ok(())
    }

    /// The number of rows checked, once the scan has ended with nothing the
    /// write history selects left unreturned.
    fn finish(mut self) -> u64 {
        if let Some((missed, _)) = self.expected.next() {
            panic!(
                "the scan ended before {:?}, which the write history selects",
                String::from_utf8_lossy(missed),
            );
        }
        self.rows
    }
}

/// The rows of the write history a scan should return, in key order: the
/// visible ones whose version `keep` selects.
fn lockstep(
    fixture: &Fixture,
    keep: impl Fn(fixtures::Value) -> bool,
) -> Lockstep<'_, impl Iterator<Item = (&[u8], fixtures::Value)>> {
    Lockstep {
        expected: fixture
            .oracle
            .rows
            .iter()
            .filter_map(move |r| r.expect.filter(|&v| keep(v)).map(|v| (r.key.as_slice(), v))),
        rows: 0,
    }
}

/// Scans a split-value fixture with the predicate handed to the engine, and
/// checks what comes back against the rows the write history says it selects.
///
/// The predicate runs inside the columnar scan, over the field's own
/// sub-column, so the rows it rejects are the engine's to skip: a block the
/// zone map rules out is never read, and a rejected row is never gathered.
/// Filtering returned rows here instead would make every selectivity cost the
/// same engine work and only change the row count the figures divide by. The
/// expected set comes from the seeds, not from the stored field, so a scan
/// that filtered on the wrong column or kept the wrong rows disagrees with it.
#[expect(
    clippy::expect_used,
    reason = "a scan missing a projected column or row is a wrong result, and a verify pass panics on one"
)]
fn verify_predicate_scan(
    fixture: &Fixture,
    predicate: &ColumnRangePredicate,
    selects: fn(u64) -> bool,
) -> lsm_tree::Result<u64> {
    assert_eq!(
        fixture.shape,
        fixtures::Shape::Split,
        "a predicate needs its field in a sub-column of its own",
    );
    let mut check = lockstep(fixture, |v| selects(v.seed));
    for batch in fixture.tree.columnar_scan(
        &[COL_USER_KEY, fixtures::COL_ROW],
        Some(predicate),
        SeqNo::MAX,
        ..,
    )? {
        let batch = batch?;
        let column = |id| {
            batch
                .columns
                .iter()
                .find(|c| c.column_id == id)
                .expect("the scan returns every projected column")
        };
        let (keys, values) = (column(COL_USER_KEY), column(fixtures::COL_ROW));
        for row in 0..batch.row_count {
            let cell = |c| {
                fixtures::bytes_cell(c, batch.row_count, row)
                    .expect("a returned column holds every row it counts")
            };
            // The row sub-column holds the value whole, so the cell is
            // compared with the value itself rather than with the framed row
            // a row read builds around it.
            check.check(cell(keys), cell(values), |v| Ok(v.bytes()))?;
        }
    }
    Ok(check.finish())
}

/// An inclusive predicate on one of the fields, whose sub-columns hold them
/// big-endian so bytewise order is numeric order.
fn field_range(column_id: u16, lo: u64, hi: u64) -> ColumnRangePredicate {
    ColumnRangePredicate {
        column_id,
        lower: Some(lo.to_be_bytes().to_vec()),
        upper: Some(hi.to_be_bytes().to_vec()),
        apply: PredicateApply::Filter,
    }
}

/// ~1% selectivity: the case where materializing a row before the predicate
/// runs wastes almost all of the work.
fn scan_sparse(fixture: &Fixture) -> lsm_tree::Result<u64> {
    let rows = verify_predicate_scan(fixture, &field_range(fixtures::COL_GROUP, 0, 0), |seed| {
        fixtures::group_of(seed) == 0
    })?;
    assert_eq!(
        rows,
        fixture.oracle.selected(),
        "the sparse predicate must select what the fixture marked",
    );
    Ok(rows)
}

/// ~90% selectivity: the case where deferring materialization buys almost
/// nothing and its bookkeeping can cost more than it saves. The right answer
/// differs from the sparse case, which is why both are measured.
fn scan_near_full(fixture: &Fixture) -> lsm_tree::Result<u64> {
    verify_predicate_scan(fixture, &field_range(fixtures::COL_BUCKET, 1, 9), |seed| {
        fixtures::bucket_of(seed) != 0
    })
}

/// Where a cell-row scan's predicate is applied.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Filter {
    /// In the engine, before the payload of a dropped row is read.
    Engine,
    /// By the caller, over rows the engine returned with every projected
    /// field: the sequential pass a late read must not lose to.
    Caller,
}

/// Scans a cell-row fixture with a predicate on one of its filter fields,
/// projecting the key, the sparse field and, when `payload`, the value, and
/// checks each row against the write history.
///
/// With [`Filter::Engine`] the predicate runs over the field's own column and
/// the payload of a row it drops is never read, from its page or from its
/// blob file, which is what the late-materialization scenarios measure; with
/// [`Filter::Caller`] every row comes back whole and the caller drops it.
#[expect(
    clippy::expect_used,
    reason = "a scan missing a projected column or row is a wrong result, and a verify pass panics on one"
)]
fn verify_cells_scan(
    fixture: &Fixture,
    predicate: Option<&ColumnRangePredicate>,
    filter: Filter,
    selects: impl Fn(u64) -> bool,
    payload: bool,
) -> lsm_tree::Result<u64> {
    use lsm_tree::{Absent, ProjectedField, Projection};

    let fixtures::Shape::Cells { spread } = fixture.shape else {
        panic!("a cell-row scan needs a cell-row fixture");
    };
    let mut projection = Projection::new()
        .column(COL_USER_KEY)
        .field(ProjectedField::new(
            fixtures::CELL_GROUP,
            fixtures::u64_be(),
            Absent::Error,
        )?);
    if payload {
        projection = projection.field(ProjectedField::new(
            fixtures::CELL_PAYLOAD,
            lsm_tree::table::columnar::TypeTag::Bytes,
            Absent::Error,
        )?);
    }
    // A blob tree filters on a field it is told the type of: the predicate's
    // field is declared too, and comes back beside the others.
    if let Some(predicate) = predicate
        && predicate.column_id != fixtures::CELL_GROUP
    {
        projection = projection.field(ProjectedField::new(
            predicate.column_id,
            fixtures::u64_be(),
            Absent::Error,
        )?);
    }
    let in_engine = predicate.filter(|_| filter == Filter::Engine);
    let mut check = lockstep(fixture, |v| selects(v.seed));
    for batch in fixture
        .tree
        .columnar_scan(projection, in_engine, SeqNo::MAX, ..)?
    {
        let batch = batch?;
        let column = |id| {
            batch
                .columns
                .iter()
                .find(|c| c.column_id == id)
                .expect("the scan returns every projected column")
        };
        let (keys, groups) = (column(COL_USER_KEY), column(fixtures::CELL_GROUP));
        for row in 0..batch.row_count {
            if filter == Filter::Caller
                && let Some(predicate) = predicate
            {
                let at = row as usize * 8;
                let value = column(predicate.column_id)
                    .data
                    .get(at..at + 8)
                    .expect("a filter field per row");
                // Big-endian u64 bounds order as the values do.
                let kept = predicate.lower.as_deref().is_none_or(|lo| value >= lo)
                    && predicate.upper.as_deref().is_none_or(|hi| value <= hi);
                if !kept {
                    continue;
                }
            }
            let key = fixtures::bytes_cell(keys, batch.row_count, row)
                .expect("a returned column holds every row it counts");
            let at = row as usize * 8;
            let group = groups.data.get(at..at + 8).expect("a group per row");
            let mut seen = group.to_vec();
            if payload {
                seen.extend_from_slice(
                    fixtures::bytes_cell(column(fixtures::CELL_PAYLOAD), batch.row_count, row)
                        .expect("a returned column holds every row it counts"),
                );
            }
            check.check(key, &seen, |v| {
                let [group, ..] = fixtures::cell_fields(v.seed, spread);
                let mut want = group.to_vec();
                if payload {
                    want.extend_from_slice(&v.bytes());
                }
                Ok(want)
            })?;
        }
    }
    Ok(check.finish())
}

/// ~1% of a cell-row tree, in a few runs of neighbouring keys: the payload
/// of the few pages holding them is all a late read takes.
fn cells_sparse_clustered(fixture: &Fixture) -> lsm_tree::Result<u64> {
    verify_cells_scan(
        fixture,
        Some(&field_range(fixtures::CELL_CLUSTER, 0, 0)),
        Filter::Engine,
        |seed| fixtures::cluster_of(seed) == 0,
        true,
    )
}

/// About one row in each row page, filtered by `filter`: every page of the
/// payload holds a kept row, so the late read can spare no page, and must not
/// cost more than the eager one; what it spares is materialising the rows
/// dropped.
fn cells_one_per_page(fixture: &Fixture, filter: Filter) -> lsm_tree::Result<u64> {
    let fixtures::Shape::Cells { spread } = fixture.shape else {
        panic!("a cell-row scan needs a cell-row fixture");
    };
    verify_cells_scan(
        fixture,
        Some(&field_range(fixtures::CELL_SPREAD, 0, 0)),
        filter,
        move |seed| seed % spread == 0,
        true,
    )
}

/// One row per page, filtered in the engine.
fn cells_sparse_one_per_page(fixture: &Fixture) -> lsm_tree::Result<u64> {
    cells_one_per_page(fixture, Filter::Engine)
}

/// One row per page, every row read whole and filtered by the caller: the
/// time the engine's late read is held against.
fn cells_sparse_one_per_page_eager(fixture: &Fixture) -> lsm_tree::Result<u64> {
    cells_one_per_page(fixture, Filter::Caller)
}

/// ~90% of a cell-row tree, filtered by `filter`: nearly every page holds a
/// kept row.
fn cells_dense(fixture: &Fixture, filter: Filter) -> lsm_tree::Result<u64> {
    verify_cells_scan(
        fixture,
        Some(&field_range(fixtures::CELL_BUCKET, 1, 9)),
        filter,
        |seed| fixtures::bucket_of(seed) != 0,
        true,
    )
}

/// ~90% of a cell-row tree, filtered in the engine.
fn cells_near_full(fixture: &Fixture) -> lsm_tree::Result<u64> {
    cells_dense(fixture, Filter::Engine)
}

/// ~90% of a cell-row tree, every row read whole and filtered by the caller:
/// the sequential pass the engine's density decision must not lose to.
fn cells_near_full_eager(fixture: &Fixture) -> lsm_tree::Result<u64> {
    cells_dense(fixture, Filter::Caller)
}

/// ~1% of a cell-row tree whose predicate field is kept in a blob file,
/// filtered by `filter`: every row's object is read to judge it, and the
/// inline payload of the rows dropped is what the engine spares.
fn cells_ref_filtered(fixture: &Fixture, filter: Filter) -> lsm_tree::Result<u64> {
    verify_cells_scan(
        fixture,
        Some(&field_range(fixtures::CELL_CLUSTER, 0, 0)),
        filter,
        |seed| fixtures::cluster_of(seed) == 0,
        true,
    )
}

/// A predicate on a field kept in a blob file, filtered in the engine.
fn cells_ref_filtered_late(fixture: &Fixture) -> lsm_tree::Result<u64> {
    cells_ref_filtered(fixture, Filter::Engine)
}

/// A predicate on a field kept in a blob file, every row read whole and
/// filtered by the caller.
fn cells_ref_filtered_eager(fixture: &Fixture) -> lsm_tree::Result<u64> {
    cells_ref_filtered(fixture, Filter::Caller)
}

/// The header field of every wide cell row, without its payload: no blob is
/// read at all.
fn cells_projected(fixture: &Fixture) -> lsm_tree::Result<u64> {
    let blobs = fixture.tree.metrics().blob_read_count();
    let rows = verify_cells_scan(fixture, None, Filter::Engine, |_| true, false)?;
    assert_eq!(
        fixture.tree.metrics().blob_read_count(),
        blobs,
        "a projection of the header fields read a payload"
    );
    Ok(rows)
}

/// The near-full field after a metadata-only update: a value no row held
/// before, so a read that returned the old version disagrees.
fn updated_bucket(seed: u64) -> u64 {
    (fixtures::bucket_of(seed) + 1) % 10
}

/// Rewrites every visible wide cell row with a new near-full field, keeping
/// its payload by reference, flushes, and counts the blob bytes the flush
/// wrote: a metadata-only update should write none.
///
/// Each row is then read back and checked against the write history with the
/// new field, so an update that lost its payload or kept the old field fails
/// instead of reporting zero.
#[expect(
    clippy::expect_used,
    reason = "a visible row that does not read back, or a payload that is not a reference, is a wrong result, and a verify pass panics on one"
)]
fn cells_metadata_update(fixture: &Fixture) -> lsm_tree::Result<UpdatePass> {
    use lsm_tree::blob_tree::field_row::{Cell, Field};

    let AnyTree::Blob(blob) = &fixture.tree else {
        panic!("a metadata-only update needs a blob tree");
    };
    let fixtures::Shape::Cells { spread } = fixture.shape else {
        panic!("a metadata-only update needs a cell-row fixture");
    };
    let (files_before, bytes_before): (Vec<_>, _) = {
        let version = blob.current_version();
        (
            version.blob_files.list_ids().copied().collect(),
            version.blob_files.on_disk_size(),
        )
    };

    let mut seqno = blob.get_highest_seqno().map_or(0, |s| s + 1);
    let mut rows = 0_u64;
    for row in &fixture.oracle.rows {
        let Some(value) = row.expect else { continue };
        let read = blob
            .get_cells(&*row.key, SeqNo::MAX)?
            .expect("a visible row reads back");
        let bucket = updated_bucket(value.seed).to_be_bytes();
        let mut update: Vec<Field<'_>> = read.fields()?;
        for field in &mut update {
            if field.column == fixtures::CELL_BUCKET {
                field.cell = Cell::Value(&bucket);
            }
        }
        assert!(
            update
                .iter()
                .any(|f| f.column == fixtures::CELL_PAYLOAD && matches!(f.cell, Cell::Ref(_))),
            "the payload of key {:?} is not in a blob file, so the update measures nothing",
            String::from_utf8_lossy(&row.key),
        );
        blob.insert_cells(row.key.clone(), &update, seqno)?;
        seqno += 1;
        rows += 1;
    }
    blob.flush_active_memtable(0)?;

    let after = blob.current_version();
    assert!(
        files_before
            .iter()
            .all(|&id| after.blob_files.contains_key(id)),
        "a blob file went during the update, so the size difference is not what it wrote",
    );
    let payload_written = after.blob_files.on_disk_size() - bytes_before;

    for row in &fixture.oracle.rows {
        let Some(value) = row.expect else { continue };
        let [group, _, cluster, spread_field] = fixtures::cell_fields(value.seed, spread);
        let bucket = updated_bucket(value.seed).to_be_bytes();
        let payload = value.bytes();
        let expected = lsm_tree::table::columnar::frame_value_cells(&[
            (fixtures::u64_be(), &group),
            (fixtures::u64_be(), &bucket),
            (fixtures::u64_be(), &cluster),
            (fixtures::u64_be(), &spread_field),
            (lsm_tree::table::columnar::TypeTag::Bytes, &payload),
        ])?;
        assert_eq!(
            blob.get(&*row.key, SeqNo::MAX)?.as_deref(),
            Some(expected.as_slice()),
            "key {:?} does not read back with the updated field and its payload",
            String::from_utf8_lossy(&row.key),
        );
    }
    Ok(UpdatePass {
        rows,
        payload_written,
    })
}

/// ~1% of the scattered blobs: the payload of the rows the predicate drops is
/// never fetched.
fn cells_blobs_filtered(fixture: &Fixture) -> lsm_tree::Result<u64> {
    verify_cells_scan(
        fixture,
        Some(&field_range(fixtures::CELL_GROUP, 0, 0)),
        Filter::Engine,
        |seed| fixtures::group_of(seed) == 0,
        true,
    )
}

/// The sparse projected scan of the scattered cell rows, repeated while a
/// second thread rewrites other rows, flushes and compacts, so blob files are
/// relocated and dropped under the scans: each repetition is verified
/// against the write history and timed.
///
/// The rows the other thread writes hold a sparse field the predicate drops,
/// so the write history of the scanned rows stays what the fixture recorded.
fn cells_scan_under_compaction(fixture: &Fixture) -> lsm_tree::Result<LatencyPass> {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Repetitions of the scan.
    const SCANS: usize = 40;
    let stop = Arc::new(AtomicBool::new(false));
    let tree = fixture.tree.clone();
    let churning = Arc::clone(&stop);
    let churn = std::thread::spawn(move || -> lsm_tree::Result<u64> {
        let AnyTree::Blob(blob) = &tree else {
            return Ok(0);
        };
        let mut seqno = tree.get_highest_seqno().map_or(0, |s| s + 1);
        let mut rounds = 0;
        // A row the predicate drops: its sparse field is never zero.
        let group = 1u64.to_be_bytes();
        let payload = vec![b'c'; 8_192];
        while !churning.load(Ordering::Relaxed) {
            for j in 0..64u64 {
                let fields = [
                    lsm_tree::blob_tree::field_row::Field {
                        column: fixtures::CELL_GROUP,
                        tag: fixtures::u64_be(),
                        cell: lsm_tree::blob_tree::field_row::Cell::Value(&group),
                    },
                    lsm_tree::blob_tree::field_row::Field::bytes(fixtures::CELL_PAYLOAD, &payload),
                ];
                blob.insert_cells(format!("zz{j:04}"), &fields, seqno)?;
                seqno += 1;
            }
            tree.flush_active_memtable(0)?;
            tree.major_compact(64 * 1_024 * 1_024, SeqNo::MAX)?;
            rounds += 1;
        }
        Ok(rounds)
    });

    let mut latencies = Vec::with_capacity(SCANS);
    let mut rows = 0;
    let scanned = (|| -> lsm_tree::Result<()> {
        for _ in 0..SCANS {
            let start = Instant::now();
            rows += cells_blobs_filtered(fixture)?;
            latencies.push(start.elapsed());
        }
        Ok(())
    })();
    stop.store(true, Ordering::Relaxed);
    let rounds = churn
        .join()
        .map_err(|_| lsm_tree::Error::FeatureUnsupported("the compacting thread panicked"))??;
    scanned?;
    eprintln!(
        "  {:<34} {rounds} rewrite and compaction rounds ran under the scans",
        ""
    );
    Ok(LatencyPass { rows, latencies })
}

/// Every visible row, resolved in key order.
///
/// The blob scenarios read through this rather than through point reads: a
/// scan is where neighbouring blobs are fetched ahead and adjacent records
/// merge into one read, so it is the only pass on which blob placement can
/// move the byte counters. A point read fetches one record whatever sits
/// next to it.
fn scan_all(fixture: &Fixture) -> lsm_tree::Result<u64> {
    let mut check = lockstep(fixture, |_| true);
    for guard in fixture.tree.iter(SeqNo::MAX, None) {
        let (key, value) = guard.into_inner()?;
        check.check(&key, &value, |v| fixture.read_bytes(v))?;
    }
    Ok(check.finish())
}

/// Every visible row of a columnar fixture, through the projected scan of its
/// key and value, checked against the write history as it streams.
///
/// Times the first batch from the scan's creation, because a scan that reads
/// the whole group before yielding pays for it there, and reports the most
/// page bytes the engine says it held.
#[expect(
    clippy::expect_used,
    reason = "a scan missing a projected column or row is a wrong result, and a verify pass panics on one"
)]
fn scan_columnar(fixture: &Fixture) -> lsm_tree::Result<ScanPass> {
    let metrics = fixture.tree.metrics();
    let (start, read_before) = (Instant::now(), metrics.bytes_read());
    let mut scan = fixture
        .tree
        .columnar_scan(&[COL_USER_KEY, COL_VALUE], None, SeqNo::MAX, ..)?;
    let mut check = lockstep(fixture, |_| true);
    let mut first_batch = None;
    for batch in &mut scan {
        let batch = batch?;
        first_batch.get_or_insert_with(|| (start.elapsed(), metrics.bytes_read() - read_before));
        let column = |id| {
            batch
                .columns
                .iter()
                .find(|c| c.column_id == id)
                .expect("the scan returns every projected column")
        };
        let (keys, values) = (column(COL_USER_KEY), column(COL_VALUE));
        for row in 0..batch.row_count {
            let cell = |c| {
                fixtures::bytes_cell(c, batch.row_count, row)
                    .expect("a returned column holds every row it counts")
            };
            check.check(cell(keys), cell(values), |v| Ok(v.bytes()))?;
        }
    }
    Ok(ScanPass {
        rows: check.finish(),
        first_batch,
        retained: scan.peak_payload_bytes(),
    })
}

/// A scenario: a fixture, and either the read pass that measures it or the
/// reason there is not one yet.
struct Scenario {
    name: &'static str,
    fixture: FixtureFn,
    support: Support,
}

/// Why the blob-placement scenarios cannot run on a zero-capacity cache: the
/// scan's blob prefetch holds what it fetches in the cache, so with no cache
/// every row reads its blob alone and placement cannot move the counters.
const PLACEMENT_NEEDS_CACHE: &str = "placement shows only through the scan's blob \
     prefetch, which holds what it fetches in the cache; --cache-mb 0 turns it off";

/// The blob-placement read pass, unless the run's cache turns the prefetch
/// off, in which case the figure would measure the fixtures' version
/// histories rather than placement.
fn placement_support(config: &BenchConfig) -> Support {
    if config.cache_mb == 0 {
        Support::Missing(PLACEMENT_NEEDS_CACHE)
    } else {
        Support::Native(scan_all)
    }
}

/// The churn pass, under the same condition as the placement scans: the scan
/// it ends with shows placement only through the blob prefetch.
fn churn_support(config: &BenchConfig) -> Support {
    if config.cache_mb == 0 {
        Support::Missing(PLACEMENT_NEEDS_CACHE)
    } else {
        Support::Churn
    }
}

/// Builds a churn scenario's fixture, rewrites it with [`fixtures::churn`],
/// reports the relocated bytes per reclaimed byte, and measures a verified
/// full scan of the result.
fn run_churn(
    name: &str,
    fixture: FixtureFn,
    config: &BenchConfig,
    seqno: &AtomicU64,
    dir: &Path,
    reporter: &mut Reporter,
) -> lsm_tree::Result<()> {
    let mut fixture = fixture(config, seqno, dir)?;
    let t = Instant::now();
    let churned = fixtures::churn(&mut fixture, seqno)?;
    let keys = fixture.oracle.rows.len() as u64;
    let readings = Readings::measure(&fixture.tree, keys, || scan_all(&fixture))?;
    reporter.record_duration(t.elapsed());
    readings.report(name);
    readings.publish(name, reporter);

    let blob_files = fixture.tree.current_version().blob_files.len();
    #[expect(
        clippy::cast_precision_loss,
        reason = "byte counts far below f64's exact range"
    )]
    let per_reclaimed =
        (churned.reclaimed > 0).then(|| churned.relocated as f64 / churned.reclaimed as f64);
    eprintln!(
        "  {:<34} relocated={} B reclaimed={} B relocated/reclaimed={} blob files={blob_files}",
        "",
        churned.relocated,
        churned.reclaimed,
        fmt_ratio(per_reclaimed, 3),
    );
    if let Some(value) = per_reclaimed {
        reporter.publish_series(
            format!("{name} relocated bytes per reclaimed byte"),
            value,
            "B/B",
            format!(
                "keys: {keys} | relocated: {} B | reclaimed: {} B | blob files: {blob_files}",
                churned.relocated, churned.reclaimed,
            ),
            Suite::Costs,
        );
    }
    Ok(())
}

/// Every scenario, in the order the report prints them.
///
/// The unsupported verdicts are the honest statement of where the engine is.
/// They are named after the capability they wait for rather than after an
/// issue number, which would rot.
fn scenarios(config: &BenchConfig) -> Vec<Scenario> {
    vec![
        Scenario {
            name: "narrow-records",
            fixture: fixtures::narrow,
            support: Support::Native(verify_point_reads),
        },
        Scenario {
            name: "wide-records-full-read",
            fixture: fixtures::wide,
            support: Support::Native(verify_point_reads),
        },
        Scenario {
            name: "mixed-value-sizes",
            fixture: fixtures::mixed_sizes,
            support: Support::Native(verify_point_reads),
        },
        Scenario {
            name: "row-updates-over-columnar-base",
            fixture: fixtures::columnar_base_row_updates,
            support: Support::Native(verify_point_reads),
        },
        Scenario {
            name: "row-updates-over-columnar-base-scan",
            fixture: fixtures::columnar_base_row_updates,
            support: Support::Scan(scan_columnar),
        },
        Scenario {
            name: "versions-deletes-tombstones",
            fixture: fixtures::versions_deletes_tombstones,
            support: Support::Native(verify_point_reads),
        },
        Scenario {
            name: "selective-scan-sparse",
            fixture: fixtures::selectivity,
            support: Support::Native(scan_sparse),
        },
        Scenario {
            name: "selective-scan-near-full",
            fixture: fixtures::selectivity,
            support: Support::Native(scan_near_full),
        },
        Scenario {
            name: "columnar-scan-one-segment",
            fixture: fixtures::columnar_segment,
            support: Support::Scan(scan_columnar),
        },
        Scenario {
            name: "columnar-scan-overlap-8",
            fixture: fixtures::columnar_overlap,
            support: Support::Scan(scan_columnar),
        },
        Scenario {
            name: "blobs-well-placed",
            fixture: fixtures::blobs_well_placed,
            support: placement_support(config),
        },
        Scenario {
            name: "blobs-scattered",
            fixture: fixtures::blobs_scattered,
            support: placement_support(config),
        },
        // The pairs below compare engine byte counters, not time, and each
        // builds its own tree in its own directory with its own cache, so the
        // order they run in moves none of their figures.
        Scenario {
            name: "blobs-well-placed-churn",
            fixture: fixtures::blobs_well_placed,
            support: churn_support(config),
        },
        Scenario {
            name: "blobs-well-placed-churn-one-group",
            fixture: fixtures::blobs_well_placed_one_group,
            support: churn_support(config),
        },
        Scenario {
            name: "blobs-scattered-churn",
            fixture: fixtures::blobs_scattered,
            support: churn_support(config),
        },
        Scenario {
            name: "blobs-scattered-churn-one-group",
            fixture: fixtures::blobs_scattered_one_group,
            support: churn_support(config),
        },
        Scenario {
            name: "blobs-filtered-before-fetch",
            fixture: fixtures::cells_scattered,
            support: Support::Native(cells_blobs_filtered),
        },
        Scenario {
            name: "wide-cells-projected",
            fixture: fixtures::cells_wide,
            support: Support::Native(cells_projected),
        },
        Scenario {
            name: "metadata-only-update",
            fixture: fixtures::cells_wide,
            support: Support::Update(cells_metadata_update),
        },
        Scenario {
            name: "cells-scan-sparse-clustered",
            fixture: fixtures::cells_inline,
            support: Support::Native(cells_sparse_clustered),
        },
        Scenario {
            name: "cells-scan-sparse-one-per-page",
            fixture: fixtures::cells_inline,
            support: Support::Native(cells_sparse_one_per_page),
        },
        Scenario {
            name: "cells-scan-sparse-one-per-page-eager",
            fixture: fixtures::cells_inline,
            support: Support::Native(cells_sparse_one_per_page_eager),
        },
        Scenario {
            name: "cells-scan-near-full",
            fixture: fixtures::cells_inline,
            support: Support::Native(cells_near_full),
        },
        Scenario {
            name: "cells-scan-near-full-eager",
            fixture: fixtures::cells_inline,
            support: Support::Native(cells_near_full_eager),
        },
        Scenario {
            name: "cells-scan-ref-filtered",
            fixture: fixtures::cells_ref_filter,
            support: Support::Native(cells_ref_filtered_late),
        },
        Scenario {
            name: "cells-scan-ref-filtered-eager",
            fixture: fixtures::cells_ref_filter,
            support: Support::Native(cells_ref_filtered_eager),
        },
        Scenario {
            name: "cells-scan-under-compaction",
            fixture: fixtures::cells_scattered,
            support: Support::Latency(cells_scan_under_compaction),
        },
    ]
}

/// The latency at percentile `p` of `latencies`, in microseconds.
#[expect(
    clippy::cast_precision_loss,
    reason = "a percentile of a few dozen durations, far below f64's exact range"
)]
fn percentile_us(latencies: &[Duration], p: f64) -> f64 {
    let mut sorted: Vec<Duration> = latencies.to_vec();
    sorted.sort_unstable();
    let Some(last) = sorted.len().checked_sub(1) else {
        return 0.0;
    };
    // Nearest rank, the definition a short series is read by.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "a rank within the series, between 0 and its length"
    )]
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted
        .get(rank.saturating_sub(1).min(last))
        .map_or(0.0, |d| d.as_secs_f64() * 1e6)
}

/// Builds one scenario's fixture beneath `dir` and measures `read` over it,
/// returning the readings and the read's wall time (the build is not timed).
///
/// The readings name the keys the fixture BUILT: every fixture caps `--num`,
/// so the request would label a smaller working set as the requested one.
fn measure_scenario(
    fixture: FixtureFn,
    read: ReadFn,
    config: &BenchConfig,
    seqno: &AtomicU64,
    dir: &Path,
) -> lsm_tree::Result<(Readings, Duration)> {
    let fixture = fixture(config, seqno, dir)?;
    let t = Instant::now();
    let keys = fixture.oracle.rows.len() as u64;
    let readings = Readings::measure(&fixture.tree, keys, || read(&fixture))?;
    Ok((readings, t.elapsed()))
}

impl Workload for MixedLayout {
    // The report is labelled with the run's settings, so a setting the
    // scenarios do not use is refused rather than recorded. They run one after
    // another on the calling thread and report bytes per row, which
    // concurrency does not move; and each fixes its own key format, value
    // lengths and tree kind, since those are what distinguish the scenarios.
    // Cache, compression, block size and metadata placement do reach every
    // fixture's tree.
    fn check_config(&self, config: &BenchConfig) -> Result<(), String> {
        let mut ignored = Vec::new();
        if config.threads != 1 {
            ignored.push(format!("--threads {}", config.threads));
        }
        if config.key_size != DEFAULT_KEY_SIZE {
            ignored.push(format!("--key-size {}", config.key_size));
        }
        if config.value_size != DEFAULT_VALUE_SIZE {
            ignored.push(format!("--value-size {}", config.value_size));
        }
        if config.use_blob_tree {
            ignored.push("--use-blob-tree".to_string());
        }
        if ignored.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "mixed-layout runs each scenario on one thread with its own key format, \
                 value sizes and tree kind; it does not use {}",
                ignored.join(", "),
            ))
        }
    }

    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Each scenario builds its own tree: they differ in value shape, in
        // layout and in write history, so one shared tree would make every
        // figure a blend. The harness's tree holds no data for the same
        // reason; its directory is where `--db` put the run, so the fixtures
        // are built beneath it and measure the device that was asked for.
        let fixtures_in = tree.tree_config().path.as_path();
        reporter.start();

        let mut unsupported = 0_usize;
        for scenario in scenarios(config) {
            let name = scenario.name;
            match scenario.support {
                Support::Native(read) => {
                    let (readings, elapsed) =
                        measure_scenario(scenario.fixture, read, config, seqno, fixtures_in)?;
                    reporter.record_duration(elapsed);
                    readings.report(name);
                    readings.publish(name, reporter);
                }
                Support::Scan(scan) => {
                    let fixture = (scenario.fixture)(config, seqno, fixtures_in)?;
                    let t = Instant::now();
                    let keys = fixture.oracle.rows.len() as u64;
                    let readings = Readings::measure_scan(&fixture.tree, keys, || scan(&fixture))?;
                    reporter.record_duration(t.elapsed());
                    readings.report(name);
                    readings.publish(name, reporter);
                }
                Support::Latency(pass) => {
                    let fixture = (scenario.fixture)(config, seqno, fixtures_in)?;
                    let t = Instant::now();
                    let keys = fixture.oracle.rows.len() as u64;
                    let mut latencies = Vec::new();
                    let readings = Readings::measure(&fixture.tree, keys, || {
                        let measured = pass(&fixture)?;
                        latencies = measured.latencies;
                        Ok(measured.rows)
                    })?;
                    reporter.record_duration(t.elapsed());
                    // The counters are the tree's, and the compacting thread
                    // reads and copies through them during the pass: printed
                    // for reference, not published as the scan's per-row cost.
                    readings.report(name);
                    let (p50, p99) = (
                        percentile_us(&latencies, 50.0),
                        percentile_us(&latencies, 99.0),
                    );
                    eprintln!("  {:<34} scan P50={p50:.1}us P99={p99:.1}us", "");
                    // Timed on this host, so the host's own timings.
                    for (figure, value) in [("scan P50", p50), ("scan P99", p99)] {
                        reporter.publish_series(
                            format!("{name} {figure}"),
                            value,
                            "us",
                            format!("keys: {keys} | scans: {}", latencies.len()),
                            Suite::Timings,
                        );
                    }
                }
                Support::Update(pass) => {
                    let fixture = (scenario.fixture)(config, seqno, fixtures_in)?;
                    let t = Instant::now();
                    let keys = fixture.oracle.rows.len() as u64;
                    let mut payload_written = 0;
                    let readings = Readings::measure(&fixture.tree, keys, || {
                        let measured = pass(&fixture)?;
                        payload_written = measured.payload_written;
                        Ok(measured.rows)
                    })?;
                    reporter.record_duration(t.elapsed());
                    readings.report(name);
                    readings.publish(name, reporter);
                    // Engine-counted bytes, so a cost; zero is the expected
                    // value, and anything above it is payload a metadata-only
                    // update rewrote.
                    if let Some(per_row) = readings.per_row(payload_written) {
                        eprintln!(
                            "  {:<34} payload written={payload_written} B ({per_row:.1} B/row)",
                            ""
                        );
                        reporter.publish_series(
                            format!("{name} payload bytes written per row"),
                            per_row,
                            "B/row",
                            format!(
                                "keys: {keys} | rows: {} | payload written: {payload_written} B",
                                readings.rows
                            ),
                            Suite::Costs,
                        );
                    }
                }
                Support::Churn => {
                    run_churn(name, scenario.fixture, config, seqno, fixtures_in, reporter)?;
                }
                Support::Missing(reason) => {
                    // The fixture is NOT built here. It exists, and the tests
                    // build it, but building it in the benchmark would spend
                    // the run's time writing data nothing can read yet.
                    unsupported += 1;
                    eprintln!("  {name:<34} UNSUPPORTED — {reason}");
                }
            }
        }

        if unsupported > 0 {
            eprintln!(
                "  {unsupported} scenario(s) have no native path and reported no figure; \
                 their fixtures exist and are enabled as the capabilities land."
            );
        }

        reporter.stop();
        Ok(())
    }
}
