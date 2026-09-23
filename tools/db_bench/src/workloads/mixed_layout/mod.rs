//! Mixed-layout scenarios: the instrument the read-path work is measured with.
//!
//! The sibling `mixed` workload walks a real write / rewrite / delete /
//! compact / read cycle and reports ops/sec, but it is row-major throughout —
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

use crate::config::BenchConfig;
use crate::reporter::{Direction, Reporter};
use crate::workloads::Workload;
use fixtures::{Fixture, FixtureFn};
use lsm_tree::table::columnar::COL_USER_KEY;
use lsm_tree::table::columnar_predicate::ColumnRangePredicate;
use lsm_tree::{AbstractTree, AnyTree, Guard, SeqNo};
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct MixedLayout;

/// One measured read pass over a built fixture, returning the rows it emitted.
///
/// It verifies as it goes and panics on disagreement: a measurement taken over
/// wrong results is not a slower or faster number, it is not a number. A read
/// that FAILS is returned instead, so the run reports the engine's error.
type ReadFn = fn(&Fixture) -> lsm_tree::Result<u64>;

/// Whether a scenario's native path exists in this build.
enum Support {
    /// Runs, through this read pass, and its figures mean what the scenario
    /// says they mean.
    Native(ReadFn),
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
    rows: u64,
    bytes_read: u64,
    bytes_decoded: u64,
    bytes_copied: u64,
    elapsed: std::time::Duration,
}

impl Readings {
    /// Takes a before/after difference of the tree's counters around `body`.
    ///
    /// Differences, not absolutes: the fixture build reads as it compacts, and
    /// charging that to the scenario's read would bury the figure the scenario
    /// exists to report under the cost of creating its own input.
    fn measure(
        tree: &AnyTree,
        body: impl FnOnce() -> lsm_tree::Result<u64>,
    ) -> lsm_tree::Result<Self> {
        let m = tree.metrics();
        let (r0, d0, c0) = (m.bytes_read(), m.bytes_decoded(), m.bytes_copied());
        let start = Instant::now();
        let rows = body()?;
        let elapsed = start.elapsed();
        Ok(Self {
            rows,
            bytes_read: m.bytes_read() - r0,
            bytes_decoded: m.bytes_decoded() - d0,
            bytes_copied: m.bytes_copied() - c0,
            elapsed,
        })
    }

    /// Rows emitted per KiB the scenario moved, for one counter: a yield, so
    /// more rows out of the same kibibyte is the improvement.
    #[expect(
        clippy::cast_precision_loss,
        reason = "ratios over counts far below f64's exact range"
    )]
    fn rows_per_kib(&self, bytes: u64) -> f64 {
        self.rows as f64 / (bytes.max(1) as f64 / 1024.0)
    }

    /// Bytes gathers moved per byte the transform produced: the copy
    /// amplification of the read, the quantity the acceptance rule says may
    /// not regress. Zero where nothing is gathered, which a smaller-is-better
    /// series draws honestly; a read served wholly from cache decodes nothing,
    /// so its copies are counted against one byte rather than divided by zero.
    #[expect(
        clippy::cast_precision_loss,
        reason = "ratios over counts far below f64's exact range"
    )]
    fn copy_amplification(&self) -> f64 {
        self.bytes_copied as f64 / self.bytes_decoded.max(1) as f64
    }

    /// Publishes the series that state the read path's cost, and carries the
    /// raw readings in the annotation.
    ///
    /// Read and decoded are yields (rows per KiB). Copied is a cost that is
    /// legitimately zero for a read that gathers nothing, so it is published
    /// as an amplification in the smaller-is-better suite, where zero is the
    /// best value rather than a division by zero.
    fn publish(&self, scenario: &str, reporter: &mut Reporter) {
        let annotation = format!(
            "rows: {} | read: {} B | decoded: {} B | copied: {} B | elapsed: {:?}",
            self.rows, self.bytes_read, self.bytes_decoded, self.bytes_copied, self.elapsed,
        );
        reporter.publish_series(
            format!("{scenario} rows per KiB read"),
            self.rows_per_kib(self.bytes_read),
            "rows/KiB",
            annotation.clone(),
            Direction::BiggerIsBetter,
        );
        reporter.publish_series(
            format!("{scenario} rows per KiB decoded"),
            self.rows_per_kib(self.bytes_decoded),
            "rows/KiB",
            annotation.clone(),
            Direction::BiggerIsBetter,
        );
        reporter.publish_series(
            format!("{scenario} bytes copied per byte decoded"),
            self.copy_amplification(),
            "B/B",
            annotation,
            Direction::SmallerIsBetter,
        );
    }

    /// The human-readable line. On stderr, like every other line the harness
    /// narrates with: stdout carries the machine-readable report and nothing
    /// else, so `--github-json` stays parseable.
    #[expect(
        clippy::cast_precision_loss,
        reason = "ratios over counts far below f64's exact range"
    )]
    fn report(&self, scenario: &str) {
        let rows = self.rows.max(1) as f64;
        eprintln!(
            "  {scenario:<34} rows={:<9} read/row={:<9.1} decoded/row={:<9.1} \
             copied/row={:<9.1} expand={:<5.2} {:?}",
            self.rows,
            self.bytes_read as f64 / rows,
            self.bytes_decoded as f64 / rows,
            self.bytes_copied as f64 / rows,
            // Decoded over read: the compression this read actually paid for.
            // A projection that loads whole wide blocks to return two columns
            // shows a high expansion with a low row count.
            self.bytes_decoded as f64 / (self.bytes_read.max(1) as f64),
            self.elapsed,
        );
    }
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
            name: "wide-records-projected",
            fixture: fixtures::wide,
            support: Support::Missing(
                "needs a projection that reads the header fields without the \
                 payload; today a read returns the whole value",
            ),
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
            name: "blobs-well-placed",
            fixture: fixtures::blobs_well_placed,
            support: placement_support(config),
        },
        Scenario {
            name: "blobs-scattered",
            fixture: fixtures::blobs_scattered,
            support: placement_support(config),
        },
        Scenario {
            name: "blobs-filtered-before-fetch",
            fixture: fixtures::blobs_scattered,
            support: Support::Missing(
                "needs materialization deferred past the filter, so the blob \
                 reads of discarded rows are never issued",
            ),
        },
    ]
}

impl Workload for MixedLayout {
    fn run(
        &self,
        _tree: &AnyTree,
        config: &BenchConfig,
        seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // Each scenario builds its own tree: they differ in value shape, in
        // layout and in write history, so one shared tree would make every
        // figure a blend. The harness's tree is unused for the same reason.
        reporter.start();

        let mut unsupported = 0_usize;
        for scenario in scenarios(config) {
            let name = scenario.name;
            match scenario.support {
                Support::Native(read) => {
                    let fixture = (scenario.fixture)(config, seqno)?;
                    let t = Instant::now();
                    let readings = Readings::measure(&fixture.tree, || read(&fixture))?;
                    reporter.record_duration(t.elapsed());
                    readings.report(name);
                    readings.publish(name, reporter);
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
