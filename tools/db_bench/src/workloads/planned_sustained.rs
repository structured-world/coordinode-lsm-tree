//! Sustained random overwrites with mixed point reads and range scans into
//! three trees side by side, given the same key stream and the same compaction
//! budget: one under the planner for query throughput with its default model,
//! one under the planner with a model calibrated from what it observes as it
//! runs, and one under leveled compaction.
//!
//! Compaction runs as it does in a deployment: in the background, on threads
//! of its own, throttled by the tree's compaction rate limit, while the writer
//! goes on and queries are served. Every tree gets the same limit, a multiple
//! of the paced write rate, so a gain bought by spending more of the shared
//! budget does not show as one; the reads they serve contend with their
//! compactions for the machine. The series are taken during the stream, not
//! after it drains: the queries served per second of read time, the time of
//! point reads and of scans, the sorted runs a read meets and the debt each
//! strategy reports at every flush, the time of each flush, and the bytes and
//! thread time compaction spent. The trees take each step in turn, which goes
//! first rotating, so a machine warming or slowing over the run weighs on all
//! alike.
//!
//! Each run reads the trees with one of several mixes: point reads and scans
//! of a hundred keys spread evenly, point reads skewed to a hot set with short
//! scans, long scans, and runs piling up in L0 before they go down. Beside the
//! throughput, both planned trees check the planner's model step by step: what
//! it predicts a query costs with the runs the main level holds, scaled to the
//! observed cost on the first half of the steps and judged on the second, for
//! the default model and the calibrated one alike, and how often each model's
//! plan changes.

mod calibration;

use crate::config::BenchConfig;
use crate::db::make_sequential_key;
use crate::reporter::{Reporter, Suite};
use crate::workloads::Workload;
use calibration::{Calibrated, Observed, StepSample, Timing};
use hdrhistogram::Histogram;
use lsm_tree::compaction::planned::{Model, PlanView, dp::Plan};
use lsm_tree::compaction::{CompactionAction, CompactionStrategy, Leveled, Planned};
use lsm_tree::{AbstractTree, AnyTree, Guard, UserKey, UserValue};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub struct PlannedSustained;

#[cfg(test)]
mod tests;

/// The key stream's seed: every arm writes the same keys in the same order.
const SEED: u64 = 0x5EED_0692;

/// The memtable bytes written between two flushes.
const FLUSH_BYTES: u64 = 1 << 20;

/// The target size of a table, small enough for a run of a few million
/// writes to fill several levels.
const TABLE_TARGET: u64 = 256 << 10;

/// Bytes per second the writer is paced to, into each tree.
const WRITE_RATE: u64 = 8 << 20;

/// Bytes per second each tree's compactions may write, the same budget for
/// all, as multiples of the write rate. At 8 every strategy keeps up, and what
/// tells them apart is what their compactions take from the reads meanwhile;
/// at 2 none does, and what tells them apart is how they fall behind.
const COMPACTION_RATES: [u64; 2] = [8 * WRITE_RATE, 2 * WRITE_RATE];

/// Compactions each tree may run at once.
const COMPACTION_THREADS: usize = 2;

/// How long a compaction thread with nothing to do waits before asking again.
const IDLE_WAIT: Duration = Duration::from_millis(2);

/// How the reads taken after each flush are mixed.
#[derive(Clone, Copy, Debug)]
struct ReadMix {
    name: &'static str,
    /// Point reads, and scans, taken after each flush.
    points: u64,
    scans: u64,
    /// The keys a scan reads.
    scan_length: usize,
    /// Whether reads go to a hot set of keys rather than evenly to all.
    skewed: bool,
    /// The L0 table count that sends the top level down, `None` for the
    /// strategies' default.
    l0_tables: Option<u16>,
}

impl ReadMix {
    /// The share of reads that are scans.
    fn scan_share(self) -> f64 {
        self.scans as f64 / (self.points + self.scans).max(1) as f64
    }
}

/// The mixes a run reads with. The first is the one the bench published
/// before the others were added.
const MIXES: [ReadMix; 4] = [
    ReadMix {
        name: "even",
        points: 100,
        scans: 100,
        scan_length: 100,
        skewed: false,
        l0_tables: None,
    },
    ReadMix {
        name: "hot points",
        points: 180,
        scans: 20,
        scan_length: 16,
        skewed: true,
        l0_tables: None,
    },
    ReadMix {
        name: "long scans",
        points: 150,
        scans: 50,
        scan_length: 1_000,
        skewed: false,
        l0_tables: None,
    },
    ReadMix {
        name: "32 L0 runs",
        points: 100,
        scans: 100,
        scan_length: 100,
        skewed: false,
        l0_tables: Some(32),
    },
];

/// Which strategy a tree compacts with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// The planner with its default model.
    Planned,
    /// The planner with a model calibrated as the tree runs.
    Calibrated,
    Leveled,
}

impl Kind {
    const ALL: [Self; 3] = [Self::Planned, Self::Calibrated, Self::Leveled];

    fn name(self) -> &'static str {
        match self {
            Self::Planned => "planned",
            Self::Calibrated => "calibrated",
            Self::Leveled => "leveled",
        }
    }
}

/// How well a model predicted the queries of one tree.
#[derive(Default)]
struct ModelCheck {
    /// The predicted and the observed mean query time of each step.
    pairs: Vec<(f64, f64)>,
    /// Steps whose plan differed from the step before.
    plan_changes: u64,
    last_plan: Option<Arc<Plan>>,
}

impl ModelCheck {
    /// Records what `view` predicts a query costs with `main_runs` runs in the
    /// main level beside the `observed` cost, and whether its plan changed.
    fn record(&mut self, view: &PlanView, main_runs: u32, observed: f64) {
        self.pairs
            .push((1.0 / view.inputs.speed(main_runs), observed));
        if self
            .last_plan
            .as_ref()
            .is_some_and(|last| **last != *view.plan)
        {
            self.plan_changes += 1;
        }
        self.last_plan = Some(Arc::clone(&view.plan));
    }
}

/// What one arm measured.
struct Arm {
    /// Bytes of keys and values written.
    written: u64,
    /// Bytes of the tables compactions wrote.
    compacted: u64,
    /// Thread time spent compacting, in nanoseconds.
    compaction_nanos: u64,
    /// Merges run.
    merges: u64,
    /// Sorted runs a read meets at each flush, summed and at their peak.
    runs_sum: u64,
    runs_peak: u64,
    /// Debt the arm's strategy reports at each flush.
    debt_peak: u64,
    debt_sum: u64,
    steps: u64,
    /// Point read and scan times, in nanoseconds.
    points: Histogram<u64>,
    scans: Histogram<u64>,
    /// Time of all reads and scans together, in nanoseconds.
    query_nanos: u64,
    /// Time of each flush, in nanoseconds: the time the writer is held.
    flushes: Histogram<u64>,
    /// What the tree observed, and the model calibrated from it; for a
    /// planned tree under the default model a shadow nothing runs by.
    observed: Observed,
    calibrated: Calibrated,
    /// The default and the calibrated model judged on this tree's queries.
    default_check: ModelCheck,
    calibrated_check: ModelCheck,
}

impl Arm {
    /// Nothing measured yet.
    fn new() -> Self {
        #[expect(clippy::expect_used, reason = "constant histogram params")]
        Self {
            written: 0,
            compacted: 0,
            compaction_nanos: 0,
            merges: 0,
            runs_sum: 0,
            runs_peak: 0,
            debt_peak: 0,
            debt_sum: 0,
            steps: 0,
            points: Histogram::new_with_max(10_000_000_000, 3).expect("valid histogram params"),
            scans: Histogram::new_with_max(10_000_000_000, 3).expect("valid histogram params"),
            query_nanos: 0,
            flushes: Histogram::new_with_max(1_000_000_000_000, 3).expect("valid histogram params"),
            observed: Observed::default(),
            calibrated: Calibrated::new(Model::default()),
            default_check: ModelCheck::default(),
            calibrated_check: ModelCheck::default(),
        }
    }
}

/// What a tree's compaction threads share with the writer.
#[derive(Default)]
struct Background {
    stop: AtomicBool,
    /// The sequence number below which every version may go: no snapshot is
    /// held, so the writer's.
    watermark: AtomicU64,
    merges: AtomicU64,
    nanos: AtomicU64,
    /// Compactions running now, so a query knows whether one ran beside it.
    running: AtomicU64,
    /// Bytes the merges wrote and the thread time they took.
    merge_bytes: AtomicU64,
    merge_nanos: AtomicU64,
}

/// The planner under each model after a step, and what the step's queries took.
struct Models {
    default: Planned,
    calibrated: Planned,
    /// Whether the calibrated model moved in the step.
    moved: bool,
    /// The step's mean query time.
    query_nanos: f64,
}

/// The strategy a tree's compaction threads take for each compaction: the
/// calibrated tree swaps in a new one as its model moves.
type StrategySlot = Arc<RwLock<Arc<dyn CompactionStrategy>>>;

/// One of the trees and what it measured so far.
struct ArmRun {
    kind: Kind,
    tree: AnyTree,
    strategy: StrategySlot,
    arm: Arm,
    /// The keys read after each step: the same stream for every tree.
    lookups: StdRng,
    background: Arc<Background>,
    workers: Vec<JoinHandle<lsm_tree::Result<()>>>,
    /// Removed when the run ends.
    _folder: tempfile::TempDir,
}

/// The planner told the tables' size, the L0 table count of `mix` and the
/// rates it runs at; with `calibrated`, that model and the scan share it gives
/// `mix`.
fn planned(compaction_rate: u64, mix: ReadMix, calibrated: Option<&Calibrated>) -> Planned {
    let strategy = Planned::default().with_table_target_size(TABLE_TARGET);
    let strategy = match mix.l0_tables {
        Some(tables) => strategy.with_l0_file_threshold(tables),
        None => strategy,
    };
    let prior = Model {
        write_rate: WRITE_RATE,
        encode_rate: compaction_rate,
        ..Model::default()
    };
    match calibrated {
        None => strategy.with_model(prior),
        Some(calibrated) => strategy
            .with_model(calibrated.model)
            .with_scan_share(Some(calibrated.scan_share(mix.scan_share()))),
    }
}

/// Leveled compaction told the tables' size and the L0 table count of `mix`.
fn leveled(mix: ReadMix) -> Leveled {
    let strategy = Leveled::default().with_table_target_size(TABLE_TARGET);
    match mix.l0_tables {
        Some(tables) => strategy.with_l0_threshold(u8::try_from(tables).unwrap_or(u8::MAX)),
        None => strategy,
    }
}

/// The prior every planned tree's calibration starts from: the model the
/// default planner is given.
fn prior(compaction_rate: u64) -> Model {
    Model {
        write_rate: WRITE_RATE,
        encode_rate: compaction_rate,
        ..Model::default()
    }
}

impl ArmRun {
    fn open(
        base: &Path,
        config: &BenchConfig,
        kind: Kind,
        mix: ReadMix,
        compaction_rate: u64,
    ) -> lsm_tree::Result<Self> {
        let folder = tempfile::Builder::new()
            .prefix("planned-sustained-")
            .tempdir_in(base)?;
        let tree = crate::config::tree_builder(folder.path(), config)?
            .table_target_size(TABLE_TARGET)
            .compaction_rate_limit(compaction_rate)
            .open()?;
        let strategy: Arc<dyn CompactionStrategy> = match kind {
            Kind::Planned | Kind::Calibrated => Arc::new(planned(compaction_rate, mix, None)),
            Kind::Leveled => Arc::new(leveled(mix)),
        };
        let strategy: StrategySlot = Arc::new(RwLock::new(strategy));
        let mut arm = Arm::new();
        arm.calibrated = Calibrated::new(prior(compaction_rate));
        let background = Arc::new(Background::default());
        let workers = (0..COMPACTION_THREADS)
            .map(|_| {
                let (tree, strategy, background) =
                    (tree.clone(), Arc::clone(&strategy), Arc::clone(&background));
                std::thread::spawn(move || compact_in_background(&tree, &strategy, &background))
            })
            .collect();
        Ok(Self {
            kind,
            tree,
            strategy,
            arm,
            lookups: StdRng::seed_from_u64(SEED ^ 1),
            background,
            workers,
            _folder: folder,
        })
    }

    /// The strategy the tree compacts with now.
    fn current_strategy(&self) -> Arc<dyn CompactionStrategy> {
        Arc::clone(
            &self
                .strategy
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    /// The key a read goes to: any of `keys` alike, or under a skewed mix by
    /// a Zipfian heat, scattered over the key space so the hot set is not one
    /// range.
    fn lookup_key(&mut self, keys: u64, mix: ReadMix) -> u64 {
        if !mix.skewed {
            return self.lookups.random_range(0..keys);
        }
        // A rank drawn log-uniformly from `1..=keys` falls on `x` with a
        // density of `1/x`: the heat of a Zipf law of exponent one.
        let draw: f64 = self.lookups.random();
        let rank = ((keys as f64).powf(draw) as u64).clamp(1, keys) - 1;
        // A large odd multiplier spreads neighbouring ranks over the key
        // space; two ranks landing on one key only make it hotter.
        rank.wrapping_mul(0x9E37_79B9_7F4A_7C15) % keys
    }

    /// Flushes what was written since the last step, then samples runs, debt
    /// and queries while compaction goes on in the background, and for a
    /// planned tree checks the models against what the queries took.
    fn step(
        &mut self,
        seqno: u64,
        config: &BenchConfig,
        mix: ReadMix,
        compaction_rate: u64,
    ) -> lsm_tree::Result<()> {
        let keys = key_space(config);
        self.background.watermark.store(seqno, Ordering::Relaxed);
        let flush = Instant::now();
        self.tree.flush_active_memtable(seqno)?;
        self.arm
            .flushes
            .saturating_record(u64::try_from(flush.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let version = self.tree.current_version();
        let runs = version
            .iter_levels()
            .map(|level| level.run_count() as u64)
            .sum::<u64>();
        let main_runs = version.level(1).map_or(0, |main| {
            u32::try_from(main.run_count()).unwrap_or(u32::MAX)
        });
        self.arm.runs_peak = self.arm.runs_peak.max(runs);
        self.arm.runs_sum += runs;
        let debt = self.current_strategy().pending_compaction_bytes(&version);
        self.arm.debt_peak = self.arm.debt_peak.max(debt);
        self.arm.debt_sum += debt;
        self.arm.steps += 1;
        let (mut step_points, mut step_scans) = (Timing::default(), Timing::default());
        // Point reads and scans interleaved, so neither meets a tree the
        // other has warmed alone.
        for _ in 0..mix.points.max(mix.scans) {
            for scan in [false, true] {
                let (taken, wanted) = if scan {
                    (self.arm.scans.len(), mix.scans)
                } else {
                    (self.arm.points.len(), mix.points)
                };
                if taken >= wanted * self.arm.steps {
                    continue;
                }
                let key = make_sequential_key(self.lookup_key(keys, mix), config.key_size);
                let beside_merge = self.background.running.load(Ordering::Relaxed) > 0;
                let at = Instant::now();
                if scan {
                    for item in self
                        .tree
                        .range(key.as_slice().., seqno, None)
                        .take(mix.scan_length)
                    {
                        item.value()?;
                    }
                } else {
                    self.tree.get(&key, seqno)?;
                }
                let nanos = u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX);
                self.arm.query_nanos += nanos;
                // Above the histogram's range a read is recorded at its top.
                if scan {
                    self.arm.scans.saturating_record(nanos);
                    step_scans.record(nanos);
                } else {
                    self.arm.points.saturating_record(nanos);
                    step_points.record(nanos);
                }
                if beside_merge {
                    self.arm.observed.during.record(nanos);
                } else {
                    self.arm.observed.idle.record(nanos);
                }
            }
        }
        if self.kind != Kind::Leveled {
            let models = self.observe(main_runs, step_points, step_scans, mix, compaction_rate);
            let config = self.tree.tree_config();
            let views = (
                models.default.plan_for(&version, config),
                models.calibrated.plan_for(&version, config),
            );
            self.judge(models, views, main_runs);
        }
        Ok(())
    }

    /// Records what the step observed and moves the calibrated model toward
    /// it, returning the strategies of both models as they now are.
    fn observe(
        &mut self,
        main_runs: u32,
        points: Timing,
        scans: Timing,
        mix: ReadMix,
        compaction_rate: u64,
    ) -> Models {
        let observed = &mut self.arm.observed;
        observed.merge_bytes = self.background.merge_bytes.load(Ordering::Relaxed);
        observed.merge_nanos = self.background.merge_nanos.load(Ordering::Relaxed);
        observed.merges = self.background.merges.load(Ordering::Relaxed);
        // Point reads ask the filters; a scan reads every run it meets, and a
        // compaction reads its inputs whole.
        let metrics = self.tree.metrics();
        observed.filter_queries = metrics.filter_queries() as u64;
        observed.filter_skips = metrics.io_skipped_by_filter() as u64;
        let mean = |timing: Timing| timing.nanos as f64 / timing.count.max(1) as f64;
        if points.count > 0 && scans.count > 0 {
            observed.steps.push(StepSample {
                main_runs,
                scan_nanos: mean(scans),
                point_nanos: mean(points),
            });
        }
        let fitted = calibration::fit(observed);
        let before = self.arm.calibrated;
        self.arm.calibrated.update(&fitted);
        Models {
            default: planned(compaction_rate, mix, None),
            calibrated: planned(compaction_rate, mix, Some(&self.arm.calibrated)),
            moved: self.arm.calibrated != before,
            query_nanos: (points.nanos + scans.nanos) as f64
                / (points.count + scans.count).max(1) as f64,
        }
    }

    /// Judges both models' plans of the step against the queries it took, and
    /// under the calibrated strategy compacts by the model as it now is.
    fn judge(
        &mut self,
        models: Models,
        (default, calibrated): (Option<PlanView>, Option<PlanView>),
        main_runs: u32,
    ) {
        if let Some(view) = default {
            self.arm
                .default_check
                .record(&view, main_runs, models.query_nanos);
        }
        if let Some(view) = calibrated {
            self.arm
                .calibrated_check
                .record(&view, main_runs, models.query_nanos);
        }
        if self.kind == Kind::Calibrated && models.moved {
            *self
                .strategy
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Arc::new(models.calibrated);
        }
    }

    /// Stops the compaction threads and waits for them.
    fn stop_workers(&mut self) -> lsm_tree::Result<()> {
        self.background.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            worker
                .join()
                .map_err(|_| lsm_tree::Error::Unrecoverable)??;
        }
        Ok(())
    }

    /// Drains what the stream left without a limit, once every tree's
    /// compaction threads are stopped, so all trees end at one their strategy
    /// has nothing more to do for and their compaction bytes compare.
    fn finish(mut self, seqno: u64) -> lsm_tree::Result<Arm> {
        self.stop_workers()?;
        self.tree.compaction_rate_limiter().set_rate(0);
        let strategy = self.current_strategy();
        let draining = Instant::now();
        loop {
            let result = self.tree.compact(Arc::clone(&strategy), seqno)?;
            match result.action {
                CompactionAction::Nothing => break,
                CompactionAction::Merged => self.arm.merges += 1,
                CompactionAction::Moved | CompactionAction::Dropped => {}
            }
        }
        self.arm.compaction_nanos += u64::try_from(draining.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            + self.background.nanos.load(Ordering::Relaxed);
        self.arm.merges += self.background.merges.load(Ordering::Relaxed);
        // A fresh tree: everything its compactions wrote is this run's.
        self.arm.compacted = self.tree.metrics().compaction_work_measured().written;
        Ok(std::mem::replace(&mut self.arm, Arm::new()))
    }
}

impl Drop for ArmRun {
    fn drop(&mut self) {
        // A run that failed midway still stops its threads before its folder
        // goes.
        self.background.stop.store(true, Ordering::Relaxed);
        for worker in self.workers.drain(..) {
            drop(worker.join());
        }
    }
}

/// One compaction thread: compacts with the tree's current strategy while
/// there is work, waits a moment when there is none, until told to stop.
fn compact_in_background(
    tree: &AnyTree,
    strategy: &StrategySlot,
    background: &Background,
) -> lsm_tree::Result<()> {
    while !background.stop.load(Ordering::Relaxed) {
        let current = Arc::clone(
            &strategy
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        background.running.fetch_add(1, Ordering::Relaxed);
        let at = Instant::now();
        let result = tree.compact(current, background.watermark.load(Ordering::Relaxed));
        let nanos = u64::try_from(at.elapsed().as_nanos()).unwrap_or(u64::MAX);
        background.running.fetch_sub(1, Ordering::Relaxed);
        let result = result?;
        // Every call counts, one that finds nothing to do as well: choosing
        // is compaction work, and the planner's choice is its plan. The wait
        // after an idle one does not.
        background.nanos.fetch_add(nanos, Ordering::Relaxed);
        match result.action {
            CompactionAction::Nothing => std::thread::sleep(IDLE_WAIT),
            CompactionAction::Merged => {
                background.merges.fetch_add(1, Ordering::Relaxed);
                background
                    .merge_bytes
                    .fetch_add(result.work.measured.written, Ordering::Relaxed);
                background.merge_nanos.fetch_add(nanos, Ordering::Relaxed);
            }
            CompactionAction::Moved | CompactionAction::Dropped => {}
        }
    }
    Ok(())
}

/// Overwrites over half as many keys as writes reach a steady state where
/// every level is rewritten, not just appended to.
fn key_space(config: &BenchConfig) -> u64 {
    (config.num / 2).max(1)
}

/// What a run measured, one arm per [`Kind`] in its order.
type Arms = [Arm; 3];

/// Writes the key stream into a tree of each [`Kind`] beneath `base` at
/// [`WRITE_RATE`], each compacting at `compaction_rate` and read with `mix`, a
/// step every [`FLUSH_BYTES`] and one for what is left at the end, the trees
/// taking each step in turn with the first rotating. The drains go in turn
/// too, starting with the tree at `first_drain`.
fn run_arms(
    base: &Path,
    config: &BenchConfig,
    mix: ReadMix,
    compaction_rate: u64,
    first_drain: usize,
) -> lsm_tree::Result<Arms> {
    // An arm that fails to open fails the run; those opened before it stop
    // their threads as they drop.
    let mut arms = Kind::ALL
        .into_iter()
        .map(|kind| ArmRun::open(base, config, kind, mix, compaction_rate))
        .collect::<lsm_tree::Result<Vec<_>>>()?;
    let keys = key_space(config);
    let entry = (config.key_size + config.value_size) as u64;
    let value = UserValue::from(vec![b'v'; config.value_size]);
    let mut stream = StdRng::seed_from_u64(SEED);
    let mut seqno = 1u64;
    let mut pending = 0u64;
    let mut streamed = 0u64;
    let mut steps = 0usize;
    let started = Instant::now();
    let mut step = |arms: &mut [ArmRun], seqno| -> lsm_tree::Result<()> {
        let count = arms.len();
        for offset in 0..count {
            if let Some(arm) = arms.get_mut((steps + offset) % count) {
                arm.step(seqno, config, mix, compaction_rate)?;
            }
        }
        steps += 1;
        Ok(())
    };
    for _ in 0..config.num {
        // Shared by every tree: a clone is a reference count, not a copy.
        let key = UserKey::from(make_sequential_key(
            stream.random_range(0..keys),
            config.key_size,
        ));
        for arm in &mut arms {
            arm.tree.insert(key.clone(), value.clone(), seqno);
            arm.arm.written += entry;
        }
        seqno += 1;
        pending += entry;
        streamed += entry;
        if pending >= FLUSH_BYTES {
            step(&mut arms, seqno)?;
            pending = 0;
            // Paced: the stream is ahead of its rate, so the writer waits.
            let due = Duration::from_secs_f64(streamed as f64 / WRITE_RATE as f64);
            if let Some(ahead) = due.checked_sub(started.elapsed()) {
                std::thread::sleep(ahead);
            }
        }
    }
    // The writes after the last full step are a step of their own, so every
    // byte counted as written reaches the tables and is measured as the rest.
    if pending > 0 {
        step(&mut arms, seqno)?;
    }
    // Every tree's threads stop before any drains: a drain runs with no
    // compaction of another tree beside it, whichever drains first.
    for arm in &mut arms {
        arm.stop_workers()?;
    }
    // The drains take turns going first across the runs, as the steps do
    // within one: a later one drains on a warmer cache.
    let mut runs: Vec<Option<ArmRun>> = arms.into_iter().map(Some).collect();
    let mut finished: [Option<Arm>; 3] = [None, None, None];
    for offset in 0..runs.len() {
        let index = (first_drain + offset) % runs.len();
        if let (Some(run), Some(slot)) = (
            runs.get_mut(index).and_then(Option::take),
            finished.get_mut(index),
        ) {
            *slot = Some(run.finish(seqno)?);
        }
    }
    match finished {
        [Some(planned), Some(calibrated), Some(leveled)] => Ok([planned, calibrated, leveled]),
        // One arm per kind was opened and every one drained.
        _ => unreachable!("every arm drains"),
    }
}

/// Publishes `arm`'s series, a tree of `kind` read with `mix` and compacted
/// at `compaction_rate`.
fn publish(
    reporter: &mut Reporter,
    kind: Kind,
    arm: &Arm,
    mix: ReadMix,
    config: &BenchConfig,
    compaction_rate: u64,
) {
    let name = format!(
        "{} / {} at {}x",
        mix.name,
        kind.name(),
        compaction_rate / WRITE_RATE
    );
    let calibrated = &arm.calibrated;
    let extra = format!(
        "num: {} | value_size: {} | steps: {} | merges: {} | points: {} | scans: {} of {} | \
         skewed: {} | write rate: {} MiB/s | compaction rate: {} MiB/s | calibrated encode \
         rate: {} MiB/s | calibrated merge query share: {:.3} | calibrated scan weight: {:.3}",
        config.num,
        config.value_size,
        arm.steps,
        arm.merges,
        mix.points,
        mix.scans,
        mix.scan_length,
        mix.skewed,
        WRITE_RATE >> 20,
        compaction_rate >> 20,
        calibrated.model.encode_rate >> 20,
        calibrated.model.merge_query_share,
        calibrated.scan_weight,
    );
    let ratio = |a: u64, b: u64| if b == 0 { 0.0 } else { a as f64 / b as f64 };
    let queries = arm.points.len() + arm.scans.len();
    let series: [(&str, f64, &str, Suite); 10] = [
        (
            "queries per second of read time",
            ratio(queries * 1_000_000_000, arm.query_nanos),
            "ops/s",
            Suite::Timings,
        ),
        (
            "point read p99",
            arm.points.value_at_quantile(0.99) as f64 / 1_000.0,
            "us",
            Suite::Timings,
        ),
        (
            "scan p99",
            arm.scans.value_at_quantile(0.99) as f64 / 1_000.0,
            "us",
            Suite::Timings,
        ),
        (
            "flush p99",
            arm.flushes.value_at_quantile(0.99) as f64 / 1_000_000.0,
            "ms",
            Suite::Timings,
        ),
        (
            "compaction thread time",
            arm.compaction_nanos as f64 / 1_000_000_000.0,
            "s",
            Suite::Timings,
        ),
        (
            "compaction bytes per written byte",
            ratio(arm.compacted, arm.written),
            "x",
            Suite::Costs,
        ),
        (
            "mean sorted runs",
            ratio(arm.runs_sum, arm.steps),
            "runs",
            Suite::Costs,
        ),
        (
            "peak sorted runs",
            arm.runs_peak as f64,
            "runs",
            Suite::Costs,
        ),
        (
            "mean debt",
            ratio(arm.debt_sum, arm.steps),
            "bytes",
            Suite::Costs,
        ),
        ("peak debt", arm.debt_peak as f64, "bytes", Suite::Costs),
    ];
    if kind != Kind::Leveled {
        for (model, check) in [
            ("default", &arm.default_check),
            ("calibrated", &arm.calibrated_check),
        ] {
            if let Some(error) = calibration::held_out_error(&check.pairs) {
                reporter.publish_series(
                    format!("{name} / {model} model held-out query time error"),
                    error,
                    "x",
                    extra.clone(),
                    Suite::Costs,
                );
            }
            reporter.publish_series(
                format!("{name} / {model} model plan changes"),
                check.plan_changes as f64,
                "steps",
                extra.clone(),
                Suite::Costs,
            );
        }
    }
    for (label, value, unit, suite) in series {
        reporter.publish_series(
            format!("{name} / {label}"),
            value,
            unit,
            extra.clone(),
            suite,
        );
    }
}

impl Workload for PlannedSustained {
    fn check_config(&self, config: &BenchConfig) -> Result<(), String> {
        if config.threads != 1 {
            return Err("planned-sustained runs one writer; pass --threads 1".to_string());
        }
        // Compaction bytes are counted in tables; a blob tree writes most of
        // its bytes to blob files, which the budget would leave out.
        if config.use_blob_tree {
            return Err(
                "planned-sustained measures table compaction; drop --use-blob-tree".to_string(),
            );
        }
        Ok(())
    }

    fn run(
        &self,
        tree: &AnyTree,
        config: &BenchConfig,
        _seqno: &AtomicU64,
        reporter: &mut Reporter,
    ) -> lsm_tree::Result<()> {
        // The harness's tree holds no data; its directory is where `--db` put
        // the run, so every tree is built beneath it, on the device asked for.
        reporter.start();
        let mut run = 0usize;
        for mix in MIXES {
            for compaction_rate in COMPACTION_RATES {
                let arms = run_arms(
                    &tree.tree_config().path,
                    config,
                    mix,
                    compaction_rate,
                    run % Kind::ALL.len(),
                )?;
                for (kind, arm) in Kind::ALL.into_iter().zip(&arms) {
                    publish(reporter, kind, arm, mix, config, compaction_rate);
                }
                run += 1;
            }
        }
        reporter.stop();
        Ok(())
    }
}
