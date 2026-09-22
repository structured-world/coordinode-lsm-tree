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
//! # Scenarios that cannot run yet say so
//!
//! Several shapes need capabilities that have not landed. A scenario whose
//! native path does not exist reports **unsupported** and contributes no
//! number. It is never quietly run through a fallback path under the same
//! name: publishing a figure that measures something else is worse than
//! publishing none, because the series looks continuous across the change
//! that was supposed to move it.

use crate::config::{BenchConfig, create_tree};
use crate::reporter::Reporter;
use crate::workloads::Workload;
use lsm_tree::{AbstractTree, AnyTree, SeqNo};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub struct MixedLayout;

/// Whether a scenario's native path exists in this build.
enum Support {
    /// Runs, and its figures mean what the scenario says they mean.
    Native,
    /// The capability it measures has not landed. Carries the reason, which
    /// names the missing piece rather than saying "skipped".
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

    #[expect(
        clippy::cast_precision_loss,
        reason = "ratios over counts far below f64's exact range"
    )]
    /// The human-readable line. On stderr, like every other line the harness
    /// narrates with: stdout carries the machine-readable report and nothing
    /// else, so `--github-json` stays parseable.
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

fn key(i: u64) -> Vec<u8> {
    format!("k{i:012}").into_bytes()
}

/// A wide record: several small fields a predicate can filter on, then one
/// large payload. Laid out as one opaque value here — the point of the shape
/// is that a projection SHOULD be able to read the small fields without the
/// payload, and measuring how far the engine is from that is the exercise.
fn wide_value(i: u64, payload_len: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(32 + payload_len);
    v.extend_from_slice(&i.to_be_bytes());
    v.extend_from_slice(&(i % 97).to_be_bytes());
    v.extend_from_slice(&(i % 7).to_be_bytes());
    v.extend_from_slice(&(i % 3).to_be_bytes());
    v.resize(32 + payload_len, b'p');
    v
}

/// Narrow records: a handful of small fields and nothing else. The control
/// against which every wide-record figure is read — whatever a projection
/// costs, it should not cost more per row than reading a narrow row whole.
fn scenario_narrow(config: &BenchConfig, seqno: &AtomicU64) -> lsm_tree::Result<Readings> {
    let dir = tempfile::tempdir()?;
    let tree = create_tree(dir.path(), config)?;
    let n = config.num.min(200_000);
    for i in 0..n {
        tree.insert(
            key(i),
            &i.to_be_bytes()[..],
            seqno.fetch_add(1, Ordering::Relaxed),
        );
    }
    tree.flush_active_memtable(0)?;

    Ok(Readings::measure(&tree, || {
        let mut rows = 0_u64;
        for i in 0..n {
            if tree.get(key(i), SeqNo::MAX).expect("get").is_some() {
                rows += 1;
            }
        }
        rows
    }))
}

/// Wide records read whole. The baseline the projection scenarios are
/// compared against: this is what it costs when nothing is projected away,
/// so a projection that reports the same decoded-per-row has bought nothing.
fn scenario_wide_full(config: &BenchConfig, seqno: &AtomicU64) -> lsm_tree::Result<Readings> {
    let dir = tempfile::tempdir()?;
    let tree = create_tree(dir.path(), config)?;
    let n = config.num.min(50_000);
    for i in 0..n {
        tree.insert(
            key(i),
            wide_value(i, 4_096),
            seqno.fetch_add(1, Ordering::Relaxed),
        );
    }
    tree.flush_active_memtable(0)?;

    Ok(Readings::measure(&tree, || {
        let mut rows = 0_u64;
        for i in 0..n {
            if tree.get(key(i), SeqNo::MAX).expect("get").is_some() {
                rows += 1;
            }
        }
        rows
    }))
}

/// Short and long values sharing one key space, which is what makes a single
/// block geometry a compromise: the same block size that fits many short rows
/// holds a fraction of a long one.
fn scenario_mixed_sizes(config: &BenchConfig, seqno: &AtomicU64) -> lsm_tree::Result<Readings> {
    let dir = tempfile::tempdir()?;
    let tree = create_tree(dir.path(), config)?;
    let n = config.num.min(100_000);
    for i in 0..n {
        let value = if i % 10 == 0 {
            vec![b'L'; 8_192]
        } else {
            vec![b's'; 24]
        };
        tree.insert(key(i), value, seqno.fetch_add(1, Ordering::Relaxed));
    }
    tree.flush_active_memtable(0)?;

    Ok(Readings::measure(&tree, || {
        let mut rows = 0_u64;
        for i in 0..n {
            if tree.get(key(i), SeqNo::MAX).expect("get").is_some() {
                rows += 1;
            }
        }
        rows
    }))
}

/// Historical versions with deletes: several versions per key, a fifth of
/// them deleted, read at MAX so the resolution work is paid in full.
///
/// The expectation is derived from the write history rather than from a
/// second read of the same tree: a key is visible exactly when its last
/// operation was an insert. Two readers sharing one faulty resolution routine
/// would agree with each other and both be wrong, which is why the oracle is
/// the history and not the engine.
fn scenario_versions_and_deletes(
    config: &BenchConfig,
    seqno: &AtomicU64,
) -> lsm_tree::Result<Readings> {
    let dir = tempfile::tempdir()?;
    let tree = create_tree(dir.path(), config)?;
    let n = config.num.min(100_000);

    for i in 0..n {
        tree.insert(
            key(i),
            vec![b'v'; 64],
            seqno.fetch_add(1, Ordering::Relaxed),
        );
    }
    tree.flush_active_memtable(0)?;
    for i in (0..n).step_by(3) {
        tree.insert(
            key(i),
            vec![b'w'; 64],
            seqno.fetch_add(1, Ordering::Relaxed),
        );
    }
    for i in (0..n).step_by(5) {
        tree.remove(key(i), seqno.fetch_add(1, Ordering::Relaxed));
    }
    tree.flush_active_memtable(0)?;

    // Derived from the history above, not from a read: deleted iff the last
    // operation on the key was the remove, i.e. divisible by 5.
    let expected_visible = |i: u64| !i.is_multiple_of(5);

    let readings = Readings::measure(&tree, || {
        let mut rows = 0_u64;
        for i in 0..n {
            let got = tree.get(key(i), SeqNo::MAX).expect("get");
            assert_eq!(
                got.is_some(),
                expected_visible(i),
                "key {i}: visibility disagrees with the write history",
            );
            if got.is_some() {
                rows += 1;
            }
        }
        rows
    });
    Ok(readings)
}

/// The body of a scenario: builds its own fixture, then measures one read pass
/// over it.
type ScenarioFn = fn(&BenchConfig, &AtomicU64) -> lsm_tree::Result<Readings>;

struct Scenario {
    name: &'static str,
    support: Support,
    run: ScenarioFn,
}

/// Every scenario, with the support verdict that decides whether it runs.
///
/// The verdicts are the honest statement of where the engine is: the shapes
/// that need a projection over mixed layouts, or a blob read deferred past a
/// filter, have no native path yet and are named after the capability they
/// wait for rather than after the issue number, which would rot.
fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            name: "narrow-records",
            support: Support::Native,
            run: scenario_narrow,
        },
        Scenario {
            name: "wide-records-full-read",
            support: Support::Native,
            run: scenario_wide_full,
        },
        Scenario {
            name: "mixed-value-sizes",
            support: Support::Native,
            run: scenario_mixed_sizes,
        },
        Scenario {
            name: "versions-and-deletes",
            support: Support::Native,
            run: scenario_versions_and_deletes,
        },
        Scenario {
            name: "wide-records-projected",
            support: Support::Missing(
                "needs a projected scan that spans row and columnar segments; \
                 today the columnar scan refuses a mixed-layout tree",
            ),
            run: scenario_wide_full,
        },
        Scenario {
            name: "row-updates-over-columnar-base",
            support: Support::Missing(
                "needs the same mixed-layout projected scan: a row-format \
                 update over a columnar base is exactly what it refuses",
            ),
            run: scenario_wide_full,
        },
        Scenario {
            name: "blobs-filtered-before-fetch",
            support: Support::Missing(
                "needs materialization deferred past the filter, so the blob \
                 reads of discarded rows are never issued",
            ),
            run: scenario_wide_full,
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
        // Each scenario builds its own tree: they differ in value shape and in
        // write history, so one shared tree would make every figure a blend.
        // The harness's tree is unused for the same reason.
        reporter.start();

        let mut unsupported = 0_usize;
        for scenario in scenarios() {
            let name = scenario.name;
            match scenario.support {
                Support::Native => {
                    let t = Instant::now();
                    let readings = (scenario.run)(config, seqno)?;
                    reporter.record_duration(t.elapsed());
                    readings.report(name);
                    readings.publish(name, reporter);
                }
                Support::Missing(reason) => {
                    unsupported += 1;
                    eprintln!("  {name:<34} UNSUPPORTED — {reason}");
                }
            }
        }

        if unsupported > 0 {
            eprintln!(
                "  {unsupported} scenario(s) have no native path and reported no figure; \
                 they are enabled as the capabilities land."
            );
        }

        reporter.stop();
        Ok(())
    }
}
