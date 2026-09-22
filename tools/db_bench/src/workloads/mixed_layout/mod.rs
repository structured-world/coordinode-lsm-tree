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
use crate::reporter::Reporter;
use crate::workloads::Workload;
use fixtures::{Fixture, FixtureFn, field_bucket, field_group};
use lsm_tree::{AbstractTree, AnyTree, Guard, SeqNo};
use std::sync::atomic::AtomicU64;
use std::time::Instant;

pub struct MixedLayout;

/// One measured read pass over a built fixture, returning the rows it emitted.
///
/// It verifies as it goes and panics on disagreement: a measurement taken over
/// wrong results is not a slower or faster number, it is not a number.
type ReadFn = fn(&Fixture) -> u64;

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
    fn measure(tree: &AnyTree, body: impl FnOnce() -> u64) -> Self {
        let m = tree.metrics();
        let (r0, d0, c0) = (m.bytes_read(), m.bytes_decoded(), m.bytes_copied());
        let start = Instant::now();
        let rows = body();
        let elapsed = start.elapsed();
        Self {
            rows,
            bytes_read: m.bytes_read() - r0,
            bytes_decoded: m.bytes_decoded() - d0,
            bytes_copied: m.bytes_copied() - c0,
            elapsed,
        }
    }

    /// Rows emitted per KiB the scenario moved, for one counter.
    ///
    /// The dashboard draws every series bigger-is-better, and all three
    /// counters improve by SHRINKING, so the published quantity is inverted:
    /// more rows out of the same kibibyte is the improvement. Bytes per row
    /// would draw the same measurement upside down on a chart that cannot say
    /// so.
    #[expect(
        clippy::cast_precision_loss,
        reason = "ratios over counts far below f64's exact range"
    )]
    fn rows_per_kib(&self, bytes: u64) -> f64 {
        self.rows as f64 / (bytes.max(1) as f64 / 1024.0)
    }

    /// Publishes the two series that state the read path's cost, and carries
    /// the rest of the readings in the annotation.
    ///
    /// Copied is annotation rather than a series because it is legitimately
    /// zero for a scan that gathers nothing, and rows-per-KiB of zero bytes is
    /// not a point on a chart. It is still printed, still pinned by
    /// `tests/read_byte_counters.rs`, and becomes a series the day a scenario
    /// gathers.
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
        );
        reporter.publish_series(
            format!("{scenario} rows per KiB decoded"),
            self.rows_per_kib(self.bytes_decoded),
            "rows/KiB",
            annotation,
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
fn verify_point_reads(fixture: &Fixture) -> u64 {
    let mut rows = 0_u64;
    for row in &fixture.oracle.rows {
        let got = fixture
            .tree
            .get(&*row.key, SeqNo::MAX)
            .expect("get must not fail");
        match (&row.expect, got) {
            (Some(expected), Some(actual)) => {
                assert_eq!(
                    &*actual,
                    expected.bytes().as_slice(),
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
    rows
}

/// Scans the whole key space, keeps the rows a predicate over the VALUE
/// selects, and checks the selection against the oracle.
///
/// The predicate reads the field back out of what the scan returned, so a path
/// that returned the wrong row's header cannot pass by agreeing with a
/// predicate evaluated over the key.
fn verify_selective_scan(fixture: &Fixture, predicate: fn(&[u8]) -> bool, expected: u64) -> u64 {
    let mut rows = 0_u64;
    for guard in fixture.tree.iter(SeqNo::MAX, None) {
        let (_, value) = guard.into_inner().expect("scan must not fail");
        if predicate(&value) {
            rows += 1;
        }
    }
    assert_eq!(
        rows, expected,
        "the predicate selected {rows} rows, the write history says {expected}",
    );
    rows
}

/// ~1% selectivity: the case where materializing a row before the predicate
/// runs wastes almost all of the work.
fn scan_sparse(fixture: &Fixture) -> u64 {
    let expected = fixture.oracle.selected();
    verify_selective_scan(fixture, |v| field_group(v) == Some(0), expected)
}

/// ~90% selectivity: the case where deferring materialization buys almost
/// nothing and its bookkeeping can cost more than it saves. The right answer
/// differs from the sparse case, which is why both are measured.
fn scan_near_full(fixture: &Fixture) -> u64 {
    let expected = fixture
        .oracle
        .rows
        .iter()
        .filter(|r| r.expect.is_some_and(|v| fixtures::bucket_of(v.seed) != 0))
        .count() as u64;
    verify_selective_scan(fixture, |v| field_bucket(v) != Some(0), expected)
}

/// A scenario: a fixture, and either the read pass that measures it or the
/// reason there is not one yet.
struct Scenario {
    name: &'static str,
    fixture: FixtureFn,
    support: Support,
}

/// Every scenario, in the order the report prints them.
///
/// The unsupported verdicts are the honest statement of where the engine is.
/// They are named after the capability they wait for rather than after an
/// issue number, which would rot.
fn scenarios() -> Vec<Scenario> {
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
            support: Support::Native(verify_point_reads),
        },
        Scenario {
            name: "blobs-scattered",
            fixture: fixtures::blobs_scattered,
            support: Support::Native(verify_point_reads),
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
        for scenario in scenarios() {
            let name = scenario.name;
            match scenario.support {
                Support::Native(read) => {
                    let fixture = (scenario.fixture)(config, seqno)?;
                    let t = Instant::now();
                    let readings = Readings::measure(&fixture.tree, || read(&fixture));
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
