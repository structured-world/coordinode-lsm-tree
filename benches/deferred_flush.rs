// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! What a flush costs when its durability waits for a barrier: a long-lived
//! tree takes a few rows, flushes them and goes on, the way a caller with its
//! own log flushes, under each flush durability.
//!
//! | arm | each flush | barrier |
//! |---|---|---|
//! | `synced` | syncs its table and the manifest | none needed |
//! | `deferred` | syncs nothing | never |
//! | `deferred_persist16` | syncs nothing | `persist_flushes` after every 16th |
//!
//! The arms take their flushes in alternation, so drift on the machine falls
//! on all of them. Reported per flush in microseconds: the mean (a barrier's
//! cost spread over the flushes it covers), the p50 and the p99.
//!
//! `DEFERRED_ROWS` (comma-separated, default `16,1000`) sets the rows per
//! flush, `DEFERRED_ROUNDS` (default 400) the flushes per arm and row count,
//! `DEFERRED_DIR` the folder the trees are made in (default the system
//! temporary folder).

#![expect(
    clippy::expect_used,
    reason = "benchmark setup favors concise panic messages"
)]

use lsm_tree::config::FlushDurability;
use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter};
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// Flushes a barrier covers in the `deferred_persist16` arm.
const BARRIER_EVERY: u64 = 16;

/// One tree and the times of its flushes, each with its share of a barrier.
struct Arm {
    label: &'static str,
    barrier: bool,
    _dir: TempDir,
    tree: AnyTree,
    seqno: SequenceNumberCounter,
    flushes: Vec<Duration>,
}

impl Arm {
    fn open(
        label: &'static str,
        base: Option<&str>,
        durability: FlushDurability,
        barrier: bool,
    ) -> Self {
        let dir = match base {
            Some(base) => TempDir::new_in(base),
            None => TempDir::new(),
        }
        .expect("tempdir");
        let seqno = SequenceNumberCounter::default();
        let tree = Config::new(dir.path(), seqno.clone(), SequenceNumberCounter::default())
            .flush_durability(durability)
            .open()
            .expect("open");
        Self {
            label,
            barrier,
            _dir: dir,
            tree,
            seqno,
            flushes: Vec::new(),
        }
    }

    /// Writes `rows` rows of round `round` and flushes them, timing the
    /// flush, and the barrier when this round ends a batch of them.
    fn round(&mut self, round: u64, rows: u32) {
        let value = [0xAB_u8; 256];
        for i in 0..rows {
            let key = format!("r{round:08}:{i:05}");
            self.tree.insert(key, value, self.seqno.next());
        }
        let started = Instant::now();
        self.tree.flush_active_memtable(0).expect("flush");
        if self.barrier && (round + 1).is_multiple_of(BARRIER_EVERY) {
            self.tree.persist_flushes().expect("persist");
        }
        self.flushes.push(started.elapsed());
    }
}

/// The `pct`-th percentile of `v` (nearest rank), which is not empty.
fn percentile(v: &[Duration], pct: usize) -> Duration {
    let mut v = v.to_vec();
    v.sort();
    let rank = (v.len() * pct).div_ceil(100).max(1);
    *v.get(rank - 1)
        .expect("a percentile of at least one sample")
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let rows: Vec<u32> = std::env::var("DEFERRED_ROWS")
        .unwrap_or_else(|_| "16,1000".to_owned())
        .split(',')
        .map(|s| {
            s.trim()
                .parse()
                .expect("DEFERRED_ROWS: comma-separated counts")
        })
        .collect();
    let rounds: u64 = env_or("DEFERRED_ROUNDS", 400);
    assert!(rounds > 0, "DEFERRED_ROUNDS: at least one round");
    let base = std::env::var("DEFERRED_DIR").ok();

    for rows in rows {
        let mut arms = [
            Arm::open("synced", base.as_deref(), FlushDurability::Synced, false),
            Arm::open(
                "deferred",
                base.as_deref(),
                FlushDurability::Deferred,
                false,
            ),
            Arm::open(
                "deferred_persist16",
                base.as_deref(),
                FlushDurability::Deferred,
                true,
            ),
        ];
        for round in 0..rounds {
            // Rotate which arm goes first, so none always follows the others'
            // writeback.
            let first = usize::try_from(round).expect("round fits usize") % arms.len();
            for place in 0..arms.len() {
                arms.get_mut((first + place) % 3)
                    .expect("an arm")
                    .round(round, rows);
            }
        }
        for arm in &arms {
            let total: Duration = arm.flushes.iter().sum();
            let mean = total / u32::try_from(arm.flushes.len()).expect("rounds fit u32");
            println!(
                "deferred_flush/{rows}/{}: mean {:.1} us, p50 {:.1} us, p99 {:.1} us ({} flushes)",
                arm.label,
                mean.as_secs_f64() * 1e6,
                percentile(&arm.flushes, 50).as_secs_f64() * 1e6,
                percentile(&arm.flushes, 99).as_secs_f64() * 1e6,
                arm.flushes.len(),
            );
        }
    }
}
