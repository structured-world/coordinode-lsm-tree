// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! One sensor trace under four compaction governors, on a real filesystem
//! (the one under `TMPDIR`): what each writes, how far each lets the tree's
//! shape and its reads go, and what the write budget reports.
//!
//! The trace runs on a simulated clock, keyed by sensor first so every flush
//! overlaps the tables before it. Flushed every ten simulated minutes and
//! compacted until nothing is left to do; once an hour the readings older
//! than the retention go under one range tombstone per sensor. Phases:
//!
//! 1. a quiet night: 12 h of `QUIET_SENSORS` sensors at 1 Hz;
//! 2. a restart: the tree closes and reopens, and a budget resumes from its
//!    checkpoint on a new ledger;
//! 3. a burst: 6 h of `BURST_SENSORS` sensors at 1 Hz;
//! 4. load above the budget: the allowance is cut, at run time, to a quarter,
//!    below what the flushes alone write, and the burst goes on for 6 h.
//!
//! Arms:
//!
//! - `none`: leveled compaction, nothing governs it;
//! - `rate`: leveled compaction under a compaction rate limit, in real time:
//!   it spreads compaction over time and bounds no bytes, so it writes what
//!   `none` writes, only later;
//! - `budget`: leveled compaction under a write budget on the medium;
//! - `planned`: planned compaction under the same budget, its model told the
//!   burst's write rate and its units sized to the trace (a mebibyte).
//!
//! The tree's backpressure stops writers at `L0_STOP` runs, which lets a
//! merge overrun the budget. Every ten simulated minutes `READS` point reads
//! of the last hour's readings are timed. Per arm it prints the bytes written
//! (compaction's apart), the host write amplification, the most L0 runs and
//! compaction debt seen, the read p50/p99, and the budget's refusals,
//! overrides and unsustainable hours, with one `budget,<arm>,<field>,<value>`
//! line per figure. `BUDGET_ARM` runs one arm only.

use lsm_tree::compaction::planned::Model;
use lsm_tree::compaction::{CompactionAction, CompactionStrategy, Leveled, Planned};
use lsm_tree::fs::{CountingFs, Fs, StdFs, WriteIoStats, WriteOrigin};
use lsm_tree::write_budget::{Allowance, WriteBudget};
use lsm_tree::{
    AbstractTree, AnyTree, BackpressureThresholds, Clock, Config, SeqNo, SequenceNumberCounter,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const QUIET_SENSORS: u32 = 4;
const BURST_SENSORS: u32 = 64;
const QUIET_HOURS: u64 = 12;
const BURST_HOURS: u64 = 6;
const OVER_HOURS: u64 = 6;
const FLUSH_EVERY_SECONDS: u64 = 600;
const RETENTION_HOURS: u64 = 3;
const VALUE_BYTES: usize = 24;
const READS: usize = 256;
const L0_STOP: usize = 16;
const DAY: Duration = Duration::from_secs(86_400);

/// The budget's allowance: three times what the burst's flushes write a day.
const ALLOWANCE_PER_DAY: u64 = 3 * BURST_SENSORS as u64 * (12 + VALUE_BYTES as u64) * 86_400;

/// The rate limit of the `rate` arm, bytes per second: enough that it does
/// not stretch the run much.
const RATE_LIMIT: u64 = 8 << 20;

/// A wall clock the trace moves.
#[derive(Default)]
struct TraceClock(AtomicU64);

impl Clock for TraceClock {
    fn unix_time(&self) -> Duration {
        Duration::from_secs(self.0.load(Ordering::Relaxed))
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Arm {
    None,
    Rate,
    Budget,
    Planned,
}

impl Arm {
    const ALL: [Self; 4] = [Self::None, Self::Rate, Self::Budget, Self::Planned];

    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Rate => "rate",
            Self::Budget => "budget",
            Self::Planned => "planned",
        }
    }

    fn budgeted(self) -> bool {
        matches!(self, Self::Budget | Self::Planned)
    }

    fn strategy(self) -> Arc<dyn CompactionStrategy> {
        match self {
            // Units of a mebibyte: at its default 64 MiB a tree of the trace's
            // size never fills a unit, and its top level never comes down.
            Self::Planned => Arc::new(
                Planned::default()
                    .with_table_target_size(1 << 20)
                    .with_model(Model {
                        write_rate: u64::from(BURST_SENSORS) * (12 + VALUE_BYTES as u64),
                        ..Model::default()
                    }),
            ),
            _ => Arc::new(Leveled::default()),
        }
    }
}

fn key(second: u64, sensor: u32) -> Vec<u8> {
    [&sensor.to_be_bytes()[..], &second.to_be_bytes()[..]].concat()
}

/// A reading: the same for the same sensor and second, and not compressible
/// past what a sensor's raw samples are.
fn reading(second: u64, sensor: u32) -> [u8; VALUE_BYTES] {
    let mut s = (second << 20 ^ u64::from(sensor)).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = [0; VALUE_BYTES];
    for byte in &mut out {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        *byte = (s >> 56) as u8;
    }
    out
}

/// What one arm saw.
#[derive(Default)]
struct Outcome {
    payload: u64,
    written: u64,
    compaction: u64,
    max_l0_runs: usize,
    max_debt: u64,
    reads_ns: Vec<u64>,
    refused: u64,
    backpressure_overrides: u64,
    forced_overrides: u64,
    space_overrides: u64,
    unsustainable_checks: u64,
    elapsed: Duration,
    cpu: Duration,
}

/// The medium of one arm: its backend, and the budget on it when it has one.
struct Medium {
    fs: Arc<dyn Fs>,
    ledger: Arc<WriteIoStats>,
    budget: Option<Arc<WriteBudget>>,
}

impl Medium {
    fn new(arm: Arm, clock: &Arc<TraceClock>, resumed: Option<&Arc<WriteBudget>>) -> Self {
        let ledger = Arc::new(WriteIoStats::default());
        if !arm.budgeted() {
            let fs = CountingFs::with_stats(Arc::new(StdFs), Arc::clone(&ledger));
            return Self {
                fs: Arc::new(fs),
                ledger,
                budget: None,
            };
        }
        let now = clock.unix_time();
        let budget = match resumed {
            Some(before) => WriteBudget::resume_at(
                Arc::clone(&ledger),
                before.allowance(),
                before.checkpoint(),
                0,
                now,
            ),
            None => WriteBudget::new_at(
                Arc::clone(&ledger),
                Allowance::new(ALLOWANCE_PER_DAY, DAY).with_burst(ALLOWANCE_PER_DAY / 24),
                now,
            ),
        };
        let budget = Arc::new(budget.with_clock(Arc::clone(clock) as Arc<dyn Clock>));
        Self {
            fs: Arc::new(CountingFs::with_budget(
                Arc::new(StdFs),
                Arc::clone(&budget),
            )),
            ledger,
            budget: Some(budget),
        }
    }

    fn bytes(&self) -> (u64, u64) {
        let reading = self.ledger.snapshot();
        let compaction = reading.origin(WriteOrigin::CompactionTable).bytes
            + reading.origin(WriteOrigin::CompactionBlob).bytes;
        (reading.files().bytes, compaction)
    }
}

fn open(
    arm: Arm,
    path: &std::path::Path,
    seqno: &SequenceNumberCounter,
    medium: &Medium,
) -> lsm_tree::Result<AnyTree> {
    let mut config = Config::new(path, seqno.clone(), SequenceNumberCounter::default())
        .with_shared_fs(Arc::clone(&medium.fs));
    if arm == Arm::Rate {
        config = config.compaction_rate_limit(RATE_LIMIT);
    }
    let tree = config.open()?;
    let AnyTree::Standard(standard) = &tree else {
        unreachable!("the trace separates no blobs")
    };
    standard.update_runtime_config(|c| {
        c.backpressure = BackpressureThresholds {
            l0_stop: Some(L0_STOP),
            ..BackpressureThresholds::OFF
        };
    })?;
    Ok(tree)
}

/// Flushes, then compacts until the strategy has nothing left it may run.
fn settle(
    tree: &AnyTree,
    seqno: &SequenceNumberCounter,
    strategy: &Arc<dyn CompactionStrategy>,
    outcome: &mut Outcome,
) -> lsm_tree::Result<()> {
    tree.flush_active_memtable(seqno.get())?;
    outcome.max_l0_runs = outcome.max_l0_runs.max(tree.l0_run_count());
    outcome.max_debt = outcome
        .max_debt
        .max(tree.compaction_debt(strategy.as_ref()));
    while tree.compact(Arc::clone(strategy), seqno.get())?.action != CompactionAction::Nothing {}
    Ok(())
}

/// Times `READS` point reads of the last hour of readings, of the sensors
/// that report in every phase.
fn time_reads(tree: &AnyTree, now: u64, outcome: &mut Outcome) -> lsm_tree::Result<()> {
    let mut s = now | 1;
    for _ in 0..READS {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        // The first hour reads back to second 0 at the earliest.
        let second = now - (s % 3_600).min(now);
        let sensor = (s >> 32) as u32 % QUIET_SENSORS;
        let start = Instant::now();
        let found = tree.get(key(second, sensor), SeqNo::MAX)?;
        outcome.reads_ns.push(start.elapsed().as_nanos() as u64);
        assert!(found.is_some(), "a reading of the last hour is retained");
    }
    Ok(())
}

fn account(medium: &Medium, outcome: &mut Outcome) {
    let (written, compaction) = medium.bytes();
    outcome.written += written;
    outcome.compaction += compaction;
    if let Some(budget) = &medium.budget {
        let report = budget.report();
        outcome.refused += report.refused;
        outcome.backpressure_overrides += report.backpressure_overrides.reservations;
        outcome.forced_overrides += report.forced_overrides.reservations;
        outcome.space_overrides += report.space_overrides.reservations;
    }
}

fn run(arm: Arm) -> lsm_tree::Result<Outcome> {
    let dir = tempfile::tempdir()?;
    let seqno = SequenceNumberCounter::default();
    let clock = Arc::new(TraceClock::default());
    let strategy = arm.strategy();
    let mut outcome = Outcome::default();
    let started = Instant::now();
    let cpu_started = cpu_time::ProcessTime::now();

    let mut medium = Medium::new(arm, &clock, None);
    let mut tree = open(arm, dir.path(), &seqno, &medium)?;
    let phases = [
        (QUIET_HOURS, QUIET_SENSORS),
        (BURST_HOURS, BURST_SENSORS),
        (OVER_HOURS, BURST_SENSORS),
    ];
    let mut second = 0u64;
    for (phase, (hours, sensors)) in phases.into_iter().enumerate() {
        if phase == 1 {
            // A restart: the tree reopens, the budget resumes on a new ledger.
            account(&medium, &mut outcome);
            drop(tree);
            medium = Medium::new(arm, &clock, medium.budget.as_ref());
            tree = open(arm, dir.path(), &seqno, &medium)?;
        }
        if phase == 2
            && let Some(budget) = &medium.budget
        {
            budget.set_allowance(
                Allowance::new(ALLOWANCE_PER_DAY / 4, DAY).with_burst(ALLOWANCE_PER_DAY / 96),
            );
        }
        for _ in 0..hours * 3_600 {
            for sensor in 0..sensors {
                let (key, value) = (key(second, sensor), reading(second, sensor));
                outcome.payload += (key.len() + value.len()) as u64;
                tree.insert(key, value, seqno.next());
            }
            second += 1;
            clock.0.store(second, Ordering::Relaxed);
            if second.is_multiple_of(FLUSH_EVERY_SECONDS) {
                settle(&tree, &seqno, &strategy, &mut outcome)?;
                time_reads(&tree, second - 1, &mut outcome)?;
                if medium
                    .budget
                    .as_ref()
                    .is_some_and(|b| b.report().unsustainable)
                {
                    outcome.unsustainable_checks += 1;
                }
            }
            if second.is_multiple_of(3_600)
                && let Some(expired) = (second / 3_600).checked_sub(RETENTION_HOURS + 1)
            {
                let (start, end) = (
                    (expired * 3_600).to_be_bytes(),
                    ((expired + 1) * 3_600).to_be_bytes(),
                );
                let at = seqno.next();
                for sensor in 0..BURST_SENSORS {
                    let sensor = sensor.to_be_bytes();
                    tree.remove_range(
                        [&sensor[..], &start[..]].concat(),
                        [&sensor[..], &end[..]].concat(),
                        at,
                    );
                }
                settle(&tree, &seqno, &strategy, &mut outcome)?;
            }
        }
    }
    account(&medium, &mut outcome);
    outcome.elapsed = started.elapsed();
    outcome.cpu = cpu_started.elapsed();
    Ok(outcome)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    let at = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted.get(at).copied().unwrap_or(0)
}

fn main() -> lsm_tree::Result<()> {
    let only = std::env::var("BUDGET_ARM").ok();
    let mib = |b: u64| b as f64 / (1_024.0 * 1_024.0);
    println!(
        "{QUIET_HOURS} h of {QUIET_SENSORS} sensors, a restart, {BURST_HOURS} h of {BURST_SENSORS}, \
         then {OVER_HOURS} h more with the allowance cut to a quarter; budget {:.1} MiB a day\n",
        mib(ALLOWANCE_PER_DAY),
    );
    println!(
        "{:<8} {:>11} {:>14} {:>6} {:>8} {:>13} {:>9} {:>9} {:>8} {:>9} {:>14} {:>9} {:>7}",
        "arm",
        "written MiB",
        "compacted MiB",
        "WA",
        "max L0",
        "max debt MiB",
        "read p50",
        "read p99",
        "refused",
        "overrides",
        "unsustainable",
        "wall s",
        "CPU s",
    );
    for arm in Arm::ALL {
        if only.as_deref().is_some_and(|name| name != arm.name()) {
            continue;
        }
        let mut outcome = run(arm)?;
        outcome.reads_ns.sort_unstable();
        let (p50, p99) = (
            percentile(&outcome.reads_ns, 0.50),
            percentile(&outcome.reads_ns, 0.99),
        );
        let overrides =
            outcome.backpressure_overrides + outcome.forced_overrides + outcome.space_overrides;
        println!(
            "{:<8} {:>11.2} {:>14.2} {:>6.2} {:>8} {:>13.2} {:>7}ns {:>7}ns {:>8} {:>9} {:>12} h {:>9.1} {:>7.1}",
            arm.name(),
            mib(outcome.written),
            mib(outcome.compaction),
            outcome.written as f64 / outcome.payload as f64,
            outcome.max_l0_runs,
            mib(outcome.max_debt),
            p50,
            p99,
            outcome.refused,
            overrides,
            outcome.unsustainable_checks * FLUSH_EVERY_SECONDS / 3_600,
            outcome.elapsed.as_secs_f64(),
            outcome.cpu.as_secs_f64(),
        );
        let name = arm.name();
        for (field, value) in [
            ("payload_bytes", outcome.payload),
            ("written_bytes", outcome.written),
            ("compaction_bytes", outcome.compaction),
            ("max_l0_runs", outcome.max_l0_runs as u64),
            ("max_debt_bytes", outcome.max_debt),
            ("read_p50_ns", p50),
            ("read_p99_ns", p99),
            ("refused", outcome.refused),
            ("backpressure_overrides", outcome.backpressure_overrides),
            ("forced_overrides", outcome.forced_overrides),
            ("space_overrides", outcome.space_overrides),
            ("unsustainable_checks", outcome.unsustainable_checks),
            ("wall_ms", outcome.elapsed.as_millis() as u64),
            ("cpu_ms", outcome.cpu.as_millis() as u64),
        ] {
            println!("budget,{name},{field},{value}");
        }
    }
    Ok(())
}
