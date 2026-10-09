// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026-present, Dmitry Prudnikov

//! `compaction_rate_limit` paces a real compaction: a `major_compact` over a
//! known number of bytes takes as long as the configured rate says, alone and
//! when trees share one limiter, and a live retune moves a compaction already
//! waiting.

#![cfg(feature = "std")]

use lsm_tree::{AbstractTree, AnyTree, Config, SequenceNumberCounter, rate_limiter::RateLimiter};
use std::sync::Arc;
use std::time::{Duration, Instant};
use test_log::test;

/// The limiter's rate in these tests. Its burst is one second of it, so a
/// compaction that moves `bytes` owes `(bytes - RATE) / RATE` seconds.
const RATE: u64 = 256 * 1_024;

/// Writes `flushes` tables of `keys_per_flush` distinct keys with
/// `value_len`-byte values and returns the bytes a compaction over them is
/// charged: one key plus one value per emitted entry (distinct keys, so every
/// entry is emitted).
fn fill(tree: &AnyTree, tag: &str, flushes: u64, keys_per_flush: u64, value_len: usize) -> u64 {
    let value = vec![b'v'; value_len];
    let mut charged = 0u64;
    let mut seqno = 0u64;
    for flush in 0..flushes {
        for i in 0..keys_per_flush {
            let key = format!("{tag}-{flush:02}-{i:06}");
            charged += (key.len() + value.len()) as u64;
            tree.insert(key, value.clone(), seqno);
            seqno += 1;
        }
        tree.flush_active_memtable(0).expect("flush");
    }
    charged
}

fn open(dir: &std::path::Path, configure: impl FnOnce(Config) -> Config) -> AnyTree {
    configure(Config::new(
        dir,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    ))
    .open()
    .expect("open")
}

/// Seconds the limiter must hold back `bytes` at `RATE`, past its one-second
/// burst.
fn owed(bytes: u64) -> Duration {
    assert!(
        bytes > RATE,
        "the fixture must exceed the burst to owe anything"
    );
    Duration::from_secs_f64((bytes - RATE) as f64 / RATE as f64)
}

/// A limited tree's major compaction takes at least what the rate owes for the
/// bytes it moved, and not much more: a debit charged twice, or a wait that
/// never ends, would push it far past.
#[test]
fn a_limited_major_compaction_runs_at_the_configured_rate() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = open(dir.path(), |c| c.compaction_rate_limit(RATE));
    let charged = fill(&tree, "a", 4, 256, 1_000);
    let tables_before = tree.table_count();

    let start = Instant::now();
    tree.major_compact(64 * 1_024 * 1_024, u64::MAX)?;
    let took = start.elapsed();

    let floor = owed(charged);
    assert!(
        took >= floor.mul_f64(0.95),
        "{charged} B at {RATE} B/s owe {floor:?} past the burst, the compaction took {took:?}",
    );
    assert!(
        took <= floor.mul_f64(1.5) + Duration::from_secs(1),
        "{charged} B at {RATE} B/s owe {floor:?}, the compaction took {took:?}: \
         charged more than once, or held past its debt",
    );
    assert!(tree.table_count() < tables_before, "the compaction ran");
    assert_eq!(
        tree.len(lsm_tree::MAX_SEQNO, None)?,
        4 * 256,
        "nothing lost to the pacing"
    );
    Ok(())
}

/// The same compaction without a limit does not wait out that debt: the
/// pacing above comes from the limit, not from the work.
#[test]
fn an_unlimited_major_compaction_does_not_wait() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    let tree = open(dir.path(), |c| c);
    let charged = fill(&tree, "a", 4, 256, 1_000);

    let start = Instant::now();
    tree.major_compact(64 * 1_024 * 1_024, u64::MAX)?;
    let took = start.elapsed();

    assert!(
        took < owed(charged).mul_f64(0.5),
        "an unlimited compaction of {charged} B took {took:?}",
    );
    Ok(())
}

/// Two trees compacting at once on one shared limiter are paced together:
/// their combined bytes owe the shared rate, not each tree's alone.
#[test]
fn trees_sharing_a_limiter_compact_at_the_shared_rate() -> lsm_tree::Result<()> {
    let shared = Arc::new(RateLimiter::new(RATE));
    let (a_dir, b_dir) = (tempfile::tempdir()?, tempfile::tempdir()?);
    // Each tree's own figure would let it run nearly unthrottled; the shared
    // limiter overrides it.
    let a = open(a_dir.path(), |c| {
        c.compaction_rate_limit(RATE * 100)
            .compaction_rate_limiter(Arc::clone(&shared))
    });
    let b = open(b_dir.path(), |c| {
        c.compaction_rate_limiter(Arc::clone(&shared))
    });
    let charged = fill(&a, "a", 2, 256, 1_000) + fill(&b, "b", 2, 256, 1_000);

    let start = Instant::now();
    std::thread::scope(|s| -> lsm_tree::Result<()> {
        let on_a = s.spawn(|| a.major_compact(64 * 1_024 * 1_024, u64::MAX));
        let on_b = s.spawn(|| b.major_compact(64 * 1_024 * 1_024, u64::MAX));
        on_a.join().expect("tree a")?;
        on_b.join().expect("tree b")?;
        Ok(())
    })?;
    let took = start.elapsed();

    let floor = owed(charged);
    assert!(
        took >= floor.mul_f64(0.95),
        "{charged} B over one shared {RATE} B/s owe {floor:?}, both compactions \
         finished in {took:?}",
    );
    Ok(())
}

/// A compaction already waiting on a low rate finishes once the rate is raised
/// through the tree's limiter: the retune reaches the waiter, not only the
/// next compaction.
#[test]
fn a_raised_rate_releases_a_waiting_compaction() -> lsm_tree::Result<()> {
    let dir = tempfile::tempdir()?;
    // At 1 KiB/s the compaction below would owe about a quarter of an hour.
    let tree = open(dir.path(), |c| c.compaction_rate_limit(1_024));
    fill(&tree, "a", 4, 256, 1_000);

    let start = Instant::now();
    std::thread::scope(|s| -> lsm_tree::Result<()> {
        let compaction = s.spawn(|| tree.major_compact(64 * 1_024 * 1_024, u64::MAX));
        std::thread::sleep(Duration::from_millis(300));
        assert!(!compaction.is_finished(), "the low rate holds it back");
        tree.compaction_rate_limiter().set_rate(64 * 1_024 * 1_024);
        compaction.join().expect("compaction")?;
        Ok(())
    })?;
    let took = start.elapsed();

    assert!(
        took < Duration::from_secs(10),
        "the raised rate did not reach the waiting compaction: {took:?}",
    );
    assert_eq!(tree.len(lsm_tree::MAX_SEQNO, None)?, 4 * 256);
    Ok(())
}
